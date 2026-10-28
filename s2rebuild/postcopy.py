"""s2rebuild postcopy: views, AUTO_INCREMENT counters and the O7 stale check; the O7 and O8 handlers.

Author: John Bergamini

Section 10.11 and 10.10 of the architecture. Views run under their definer and C4 removes the
runner, so the runner never creates them. With the owner profile they are created through the
owner session in rounds, until a round creates none: the original DEFINER is kept when that
account still exists and is not the runner, else the clause is removed and the owner account
becomes the definer; when the server refuses the kept definer (error 1227: naming another
account as DEFINER needs SUPER, which no account holds on Helios), the view is created again without
the clause and the owner is the definer; the choice is recorded per view. Without the owner profile O7 prints and
writes the O7 block for the admin, runs the stale check and the counters, and ends pending
until a rerun finds the views. A view whose definition names the source database is refused
before any view statement. For every AUTO_INCREMENT column the replacement counter is set to the
larger of the source counter and MAX plus one and synced on every aggregator (ALL), after the last
re-copy of the stale check, so a re-copy never leaves a counter computed before it. A re-copy
round that completes (every chunk done, count proof all match) marks O6 passed again. Before an O7 or O8 re-copy mark, the
O10 block files of the database get a STOP (ownersql.retire_o10_blocks). An O7 re-copy round
holds the copy lock (copy.copy_lock) from its mark through its copy, so it never runs while an
O10 of another process waits for its go. O8 runs the tiers of verify.py; with --yes and no
chunk copied since the last O8 run it accepts the recorded result without a rerun, and with
--fingerprint TABLE or --full TABLE it runs tier 3 for the named tables only. Statements issued
through the owner guard of O7 naming the replacement: USE and CREATE VIEW, with SELECT on
information_schema.VIEWS and USER_PRIVILEGES. Through the runner guard naming the replacement:
SELECT MAX and COUNT, SHOW CREATE TABLE, ALTER TABLE AUTO_INCREMENT, AGGREGATOR SYNC
AUTO_INCREMENT ... ALL, SELECT on information_schema.VIEWS and the SELECT statements of the stale check.
"""
import json
import re

from . import copy as copy_module
from . import estimate, inventory, ownersql, projections, report_md, report_xlsx, routines, steps, verify
from .common import EXIT_GATE, EXIT_INCOMPLETE, EXIT_MISMATCH, EXIT_REFUSED, KitError
from .sqlutil import ident, qualified, read_guard, runner_guard, strip_literals, strip_quoted

STALE_CHECK_ROUNDS = 3  # re-copy rounds before O7 gives up; the check itself runs once more after the last round
KEPT, OWNER_DEFINER, FOUND = 'kept', 'owner', 'found'  # the definer choice recorded per view
COUNTER_MAX, COUNTER_SOURCE = 'MAX+1', 'source counter'  # the counter decision recorded per table
VIEW_DEFINER_SQL = 'SELECT DEFINER FROM information_schema.VIEWS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s'
_CAPTURED_DEFINER = re.compile(r'^`([^`]*)`@`([^`]*)`$')


def view_exists_sql():
    """COUNT of a view in information_schema.VIEWS (parameters: schema, name)."""
    return 'SELECT COUNT(*) AS n FROM information_schema.VIEWS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s'


def view_statement(view, target_db, keep_definer=False):
    """CREATE VIEW in the replacement from the captured text, the view name qualified.

    keep_definer keeps the captured DEFINER clause where the server wrote it; otherwise the
    clause is removed and the account that executes the statement becomes the definer.
    """
    text = view['definition'] if keep_definer else inventory.strip_definer(view['definition'])
    name = view['name']
    text = text.replace('VIEW %s' % ident(name), 'VIEW %s' % qualified(target_db, name), 1)
    if 'VIEW %s' % qualified(target_db, name) not in text:
        raise KitError('Captured definition of view %s does not name the view as expected' % name)
    return text


def names_database(text, database):
    """True when a statement names database as a qualifier (`database`. or database.) outside its string literals. Issues nothing."""
    if re.search(r'`%s`\s*\.' % re.escape(database), strip_literals(text or '')):
        return True
    return bool(re.search(r'(?<![A-Za-z0-9_$.])%s\s*\.' % re.escape(database), strip_quoted(text or '')))


def dependency_order(views):
    """The views with each one after the captured views its definition names; capture order otherwise. Issues nothing.

    The printed O7 block runs in order and stops at the first error, so a view on another view
    must come after it. A name that only looks like a reference (a column alias) or a cycle
    keeps capture order; the kit route also creates in rounds (create_views).
    """
    names = [v['name'] for v in views]
    needs = {}
    for v in views:
        text = strip_literals(v.get('definition') or '')
        needs[v['name']] = {n for n in names if n != v['name'] and re.search(r'(?<![A-Za-z0-9_$])%s(?![A-Za-z0-9_$])' % re.escape(n), text)}
    ordered, placed, left = [], set(), list(views)
    while left:
        ready = [v for v in left if needs[v['name']] <= placed] or left
        ordered.extend(ready)
        placed.update(v['name'] for v in ready)
        left = [v for v in left if v['name'] not in placed]
    return ordered


def definer_account(definer):
    """(user, host) of a captured DEFINER value such as `app`@`%`, or None. Issues nothing."""
    m = _CAPTURED_DEFINER.match(str(definer or ''))
    return (m.group(1), m.group(2)) if m else None


def _count(rows):
    if not rows:
        return 0
    row = rows[0]
    return int((row['n'] if isinstance(row, dict) else row[0]) or 0)


def definer_choice(session, view, runner_user):
    """(KEPT or OWNER_DEFINER, reason) for one captured view; issues the USER_PRIVILEGES COUNT for the captured definer.

    The original DEFINER is kept when that account still exists in the workspace and is not the
    runner, which C4 removes; otherwise the clause is removed and the owner session that runs
    the CREATE becomes the definer. Existence is read from information_schema.USER_PRIVILEGES
    (the rows of 'user'@'host') through the owner session, which sees every account. An account
    without a row gets the owner, which stays. Confirmed live on October 27, 2026: Helios refuses
    another account as DEFINER (error 1227), so create_views falls back to the owner (_create).
    """
    account = definer_account(view.get('definer'))
    if account is None:
        return OWNER_DEFINER, 'no definer captured; the owner account is the definer'
    if account[0] == runner_user:
        return OWNER_DEFINER, 'the original definer is the runner, which C4 removes; the owner account is the definer'
    if _count(session.query(ownersql.privileges_count_sql(), ("'%s'@'%s'" % account,))) > 0:
        return KEPT, 'original definer kept'
    return OWNER_DEFINER, 'the original definer no longer exists; the owner account is the definer'


def _exists(session, target_db, name):
    return _count(session.query(view_exists_sql(), (target_db, name))) > 0


def _recorded_views(journal):
    """The view names the journal holds a definer choice for."""
    return {json.loads(d).get('view') for d in journal.events('view')}


def record_found(session, target_db, view, journal):
    """Record a view found in the replacement with the DEFINER the server shows; issues the information_schema.VIEWS read."""
    rows = session.query(VIEW_DEFINER_SQL, (target_db, view['name']))
    row = rows[0] if rows else None
    observed = (row['DEFINER'] if isinstance(row, dict) else row[0]) if row else None
    journal.event('view', json.dumps({'view': view['name'], 'original_definer': view.get('definer'), 'definer': FOUND,
                                      'observed_definer': observed, 'reason': 'found in the replacement; definer %s' % observed}))


DEFINER_REFUSED = 1227
DEFINER_REFUSED_REASON = ('the server refused the original definer (error 1227: another account as DEFINER needs SUPER, which no account '
                          'holds on Helios); the owner account is the definer')


def _create(session, view, target_db, choice, reason):
    """CREATE VIEW with the chosen definer; when the server refuses a kept definer (1227), again without the clause. Returns (choice, reason)."""
    try:
        session.execute(view_statement(view, target_db, keep_definer=(choice == KEPT)))
        return choice, reason
    except KitError:
        raise
    except Exception as exc:  # noqa: BLE001 - only the definer refusal falls back; every other error goes to the caller
        args = getattr(exc, 'args', ())
        if choice != KEPT or not args or args[0] != DEFINER_REFUSED:
            raise
    session.execute(view_statement(view, target_db))
    return OWNER_DEFINER, DEFINER_REFUSED_REASON


def _error_text(exc):
    args = getattr(exc, 'args', ())
    return 'server error %s (%s)' % (args[0] if args and isinstance(args[0], int) else 'unknown', type(exc).__name__)


def create_views(session, target_db, views, journal, log, runner_user=None):
    """Create every captured view missing from the replacement, in rounds until a round creates none; returns the names created.

    Issues USE of the replacement, then per view the existence COUNT on information_schema.VIEWS,
    the USER_PRIVILEGES COUNT of definer_choice, CREATE VIEW naming the replacement and the
    existence COUNT again as its check after (a view the server does not list refuses O7); session
    is the owner session of O7. A view that exists is skipped; when the journal holds no choice
    for it (the reply to its CREATE was lost, or the process stopped before the journal write),
    record_found records the DEFINER the server shows. A server error on one view keeps its error
    number only and the view is tried again in the next round, so a view on a later-named view is
    created once the other exists. Raises KitError naming every view still missing with its last
    error when a round creates none. journal None (dry run) records nothing.
    """
    created = []
    if not views:
        return created
    session.execute('USE ' + ident(target_db))
    recorded = _recorded_views(journal) if journal is not None else set()
    pending, errors, rounds = list(views), {}, 0
    while pending:
        rounds += 1
        left = []
        for view in pending:
            name = view['name']
            if _exists(session, target_db, name):
                log('view %s already exists in %s; skipped' % (name, target_db))
                if journal is not None and name not in recorded:
                    record_found(session, target_db, view, journal)
                    recorded.add(name)
                continue
            choice, reason = definer_choice(session, view, runner_user)
            try:
                choice, reason = _create(session, view, target_db, choice, reason)
            except KitError:
                raise
            except Exception as exc:  # noqa: BLE001 - one view's server error is kept and that view is tried again next round
                errors[name] = _error_text(exc)
                log('view %s not created in round %d: %s; it is tried again after the others' % (name, rounds, errors[name]))
                if journal is not None:
                    journal.event('view_failed', json.dumps({'view': name, 'round': rounds, 'error': errors[name]}))
                left.append(view)
                continue
            found = _exists(session, target_db, name)
            if journal is not None and not found:  # a dry run prints the check and creates nothing
                raise KitError('O7 stopped at check "view %s exists": CREATE VIEW returned without an error, but information_schema.VIEWS of %s '
                               'does not list it; inspect the replacement before any further statement' % (name, target_db))
            if journal is not None:
                journal.event('view', json.dumps({'view': name, 'original_definer': view.get('definer'), 'definer': choice, 'reason': reason}))
                recorded.add(name)
            log('created view %s (%s)' % (name, reason))
            created.append(name)
        if len(left) == len(pending):
            raise KitError('O7 stopped before the stale check: round %d created none of the views still missing: %s. Fix the cause, then '
                           'rerun O7; views already created are skipped' % (rounds, ', '.join('%s (%s)' % (v['name'], errors[v['name']]) for v in left)))
        pending = left
    return created


def view_block(views, target_db):
    """The O7 block for the admin without the owner profile: USE, then per view in dependency order a CREATE VIEW and its check.

    Each CREATE is skipped when the view exists and followed by a check that it does, so the
    block can be run again. The DEFINER clause is removed, so the admin account that runs the
    block becomes the definer (the runner cannot see whether the original account still
    exists, and C4 removes the runner). Issues nothing (the block is printed).
    """
    block = ownersql.Block('O7', 'Create the views in the replacement as the admin')
    block.note('Run this block as the admin, then rerun O7. Each view is created without its DEFINER clause, so the admin account that '
               'runs the block becomes the definer.')
    block.statement('USE %s' % ident(target_db), 'work inside the replacement')
    for view in dependency_order(views):
        params = (target_db, view['name'])
        block.statement(view_statement(view, target_db), 'create view %s' % view['name'], skip_if=(view_exists_sql(), params))
        block.check('view %s exists' % view['name'], view_exists_sql(), params, ownersql.Expect('column_equals', 'n', 1))
    return block


def place_views(ctx, views, reader):
    """Create the views with the owner profile, or print the O7 block without it; returns the names still missing.

    With the owner profile create_views runs through the owner guard of O7. Without it the
    runner (reader) reads which views are missing and records a view found without a journal
    choice; when any is missing, the O7 block is printed and written to owner-sql/O7.sql. A dry
    run prints the statements of the configured route and records nothing.
    """
    if not views:
        return []
    journal = None if ctx.dry_run else ctx.journal
    if ctx.owner_configured:
        owner = ctx.owner_session(steps.guard_for('O7', ctx.db, steps.OWNER))
        try:
            create_views(owner, ctx.db.replacement, views, journal, ctx.log, ctx.config.runner.user)
        finally:
            owner.close()
        return []
    recorded = _recorded_views(journal) if journal is not None else set()
    missing = []
    for view in views:
        if not _exists(reader, ctx.db.replacement, view['name']):
            missing.append(view['name'])
        elif journal is not None and view['name'] not in recorded:
            record_found(reader, ctx.db.replacement, view, journal)
    if missing:
        block = view_block(views, ctx.db.replacement)
        ownersql.guard_check_block(block, ctx)
        ownersql.print_block(block, ctx, ownersql.owner_sql_path(ctx, 'O7'))
    return missing


def max_count_sql(db, table, column):
    """SELECT MAX(column), COUNT(*) for the counter."""
    return 'SELECT MAX(%s) AS m, COUNT(*) AS n FROM %s' % (ident(column), qualified(db, table))


def alter_counter_sql(db, table, value):
    """ALTER TABLE ... AUTO_INCREMENT = value."""
    return 'ALTER TABLE %s AUTO_INCREMENT = %d' % (qualified(db, table), int(value))


def sync_counter_sql(db, table):
    """AGGREGATOR SYNC AUTO_INCREMENT ON the table ALL: the sync runs on every aggregator, not only the one of the session."""
    return 'AGGREGATOR SYNC AUTO_INCREMENT ON %s ALL' % qualified(db, table)


def counter_value(source_counter, max_plus_one):
    """(value, decision) for one counter: the larger of the source counter and MAX plus one. Issues nothing.

    decision is COUNTER_SOURCE when the source counter is above MAX plus one (rows deleted at the
    top of the source, or an empty table with a counter), else COUNTER_MAX. Either may be None
    (no counter captured, or no rows); (None, None) when both are.
    """
    if source_counter is not None and (max_plus_one is None or int(source_counter) > max_plus_one):
        return int(source_counter), COUNTER_SOURCE
    if max_plus_one is not None:
        return max_plus_one, COUNTER_MAX
    return None, None


def _counter_of(session, db, table):
    rows = session.query('SHOW CREATE TABLE ' + qualified(db, table))
    if not rows:
        return None
    row = rows[0]
    return inventory.auto_increment_counter(row['Create Table'] if isinstance(row, dict) else row[1])


COUNTER_REFUSED = 1749
COUNTER_REFUSED_ACTION = ('the server does not set the counter of a sharded table (error 1749); AGGREGATOR SYNC moved every aggregator '
                          'past MAX')


def _alter_counter(session, target_db, table, value):
    """ALTER TABLE ... AUTO_INCREMENT; False when the server refuses it for a sharded table (1749), every other error goes to the caller."""
    try:
        session.execute(alter_counter_sql(target_db, table, value))
        return True
    except KitError:
        raise
    except Exception as exc:  # noqa: BLE001 - only the sharded-table refusal is expected
        args = getattr(exc, 'args', ())
        if not args or args[0] != COUNTER_REFUSED:
            raise
    return False


def set_counters(session, target_db, tables, journal, log):
    """For every table with an AUTO_INCREMENT column: the larger of the source counter and MAX plus one, then the sync on every aggregator.

    Issues per table SELECT MAX and COUNT, SHOW CREATE TABLE, ALTER TABLE ... AUTO_INCREMENT and
    AGGREGATOR SYNC AUTO_INCREMENT ... ALL, then SHOW CREATE TABLE again. The source counter is the
    one the inventory captured (auto_increment.counter). An empty table without a source counter
    keeps the default. tables are the inventory table dicts. Returns the list of actions recorded
    in the journal: both values, the decision and the counter before and after. Helios does not
    set the counter of a sharded table (error 1749, October 27, 2026): its values carry the
    aggregator id, so the source counter cannot be carried over, and the sync alone moves every
    aggregator past MAX; the action records that.
    """
    actions = []
    for t in tables:
        auto = t.get('auto_increment') or {}
        column = auto.get('column')
        if not column:
            continue
        rows = session.query(max_count_sql(target_db, t['name'], column))
        row = rows[0] if rows else {'m': None, 'n': 0}
        m = row['m'] if isinstance(row, dict) else row[0]
        n = int((row['n'] if isinstance(row, dict) else row[1]) or 0)
        before = _counter_of(session, target_db, t['name'])
        max_plus_one = None if n == 0 or m is None else int(m) + 1
        value, decision = counter_value(auto.get('counter'), max_plus_one)
        action = {'table': t['name'], 'column': column, 'source_counter': auto.get('counter'), 'target_before': before,
                  'rows': n, 'max_value': None if max_plus_one is None else int(m), 'max_plus_one': max_plus_one, 'decision': decision}
        if value is None:
            action.update(target_after=before, action='table empty, counter left at default')
        else:
            altered = _alter_counter(session, target_db, t['name'], value)
            session.execute(sync_counter_sql(target_db, t['name']))
            action.update(target_after=_counter_of(session, target_db, t['name']),
                          action=('counter set to %s and synced on every aggregator' % ('the source counter' if decision == COUNTER_SOURCE else 'MAX+1'))
                          if altered else COUNTER_REFUSED_ACTION)
        journal.event('counter', json.dumps(action))
        log('%s: %s (%d rows, MAX(%s)=%s, source counter %s)' % (t['name'], action['action'], n, column, m, auto.get('counter')))
        actions.append(action)
    return actions


def _write_final_reports(ctx, data, plans, baseline):
    """Write the final workbook and the Markdown report for the run; returns the workbook path."""
    reports = ctx.config.runs_dir() / ctx.run_id / ctx.db.source / 'reports'
    xlsx = report_xlsx.write_final_workbook(reports / 'final.xlsx', data, plans, ctx.journal, ctx.config, ctx.db, ctx.run_id, baseline)
    report_md.write_run_report(reports / 'run-report.md', data, plans, ctx.journal, ctx.config, ctx.db, ctx.run_id)
    ctx.log('final workbook %s and run report %s written' % (xlsx, reports / 'run-report.md'))
    return xlsx


def reopen_copy_step(journal, round_number, log):
    """Mark O6 passed again after a completed re-copy round of the stale check.

    mark_recopy set O6 pending; the round redid the O6 work for the marked buckets and ended with
    every chunk done and the count proof all matching, which is the O6 gate. Writes the journal only.
    """
    if journal.step_status('O6') == 'passed':
        return
    journal.step_end('O6', 'passed', 'every chunk done; per-table count proof all match (re-copy round %d of the O7 stale check)' % round_number)
    log('O6 passed again: re-copy round %d completed with every chunk done and the count proof all matching' % round_number)


def handle_o7(step, ctx):
    """O7 handler: views (place_views), then the procedures (routines.place), the stale check with re-copy of mismatching tables until
    zero mismatches, then the counters.

    A view or a procedure whose definition names the source database refuses the step before any
    statement. Without the owner profile the step ends pending while a view is missing, after the
    stale check and the counters ran; the procedures wait for the views.
    """
    try:
        data, plans, directory = copy_module.load_plans(ctx)
    except KitError as exc:
        return copy_module._no_plans(step, ctx, exc)
    statements = copy_module.statements_for(ctx.db)
    if not ctx.dry_run and ctx.journal.is_empty():
        return steps.Outcome(steps.REFUSED, 'the copy has not run in this journal; run O5 and O6 first', EXIT_REFUSED)
    views = data.get('views') or []
    named = sorted(v['name'] for v in views if names_database(v.get('definition'), ctx.db.source))
    if named:
        return steps.Outcome(steps.REFUSED, 'view(s) %s name the source database %s in their definition; no view statement was issued. Edit each '
                             'definition in the capture so it names no database, then rerun O7' % (', '.join(named), ctx.db.source), EXIT_REFUSED)
    refusal = routines.refuse_source_names(data, ctx.config, ctx.db, routines.O7_KINDS) or projections.refuse(data)
    if refusal:
        return steps.Outcome(steps.REFUSED, refusal, EXIT_REFUSED)
    guard = runner_guard(ctx.db.replacement, 'runner O7')
    session = ctx.runner_session(guard)
    try:
        if ctx.dry_run:
            place_views(ctx, views, session)
            routines.place(ctx, data, routines.O7_KINDS, 'O7')
            projections.place(ctx, data, 'O7')
            for t in data['tables']:
                if (t.get('auto_increment') or {}).get('column'):
                    session.query(max_count_sql(ctx.db.replacement, t['name'], t['auto_increment']['column']))
                    ctx.log.plain('    DRY RUN: %s' % alter_counter_sql(ctx.db.replacement, t['name'], 0).replace('= 0', '= <larger of source counter and MAX+1>'))
                    ctx.log.plain('    DRY RUN: %s' % sync_counter_sql(ctx.db.replacement, t['name']))
            verify.dry_run_statements(session, ctx.db.source, ctx.db.replacement, plans, ctx.config, tiers=[verify.TIER_COUNT, verify.TIER_NEWEST])
            return steps.Outcome(steps.PASSED, 'dry run: view, counter and stale check statements printed')
        ctx.journal.check_plan(plans, statements.source_db, statements.target_db)
        missing = place_views(ctx, views, session)
        if not missing:
            # the procedures after the views, since a procedure may read a view; the gate records the ROUTINES count for S3
            outcome = routines.place(ctx, data, routines.O7_KINDS, 'O7')
            if outcome is not None:
                return outcome
        baseline = estimate.load_baseline(ctx.config.resolve(ctx.config.estimate.baseline) if ctx.config.estimate.baseline else None)
        recopies = 0
        while True:
            verifier = verify.Verifier(session, ctx.db.source, ctx.db.replacement, plans, ctx.journal, ctx.config, baseline, ctx.log)
            results = verify.stale_check(verifier)
            for line in verify.summary_lines(results):
                ctx.log('stale check %d: %s' % (recopies + 1, line))
            targets = verify.recopy_targets(results, plans)
            if not targets:
                ctx.journal.event('stale_check', 'check %d: 0 mismatches after %d re-copy round(s)' % (recopies + 1, recopies))
                break
            if recopies >= STALE_CHECK_ROUNDS:
                return steps.Outcome(steps.MISMATCH, 'stale check still finds mismatches after %d re-copy rounds: %s; investigate the writers, then rerun O7' % (
                    STALE_CHECK_ROUNDS, ', '.join(sorted({t for t, _ in targets}))), EXIT_MISMATCH)
            tables = sorted({t for t, _ in targets})
            # the round holds the copy lock from its mark through its copy, so no O10 of another process waits for its go meanwhile
            with copy_module.copy_lock(ctx.journal_path, 'O7'):
                for line in ownersql.retire_o10_blocks(ctx, tables, 'the O7 stale check re-copies %s; O8 must verify again' % ', '.join(tables)):
                    ctx.log(line)
                for table, bucket in targets:
                    ctx.journal.mark_recopy(table, bucket)
                recopies += 1
                ctx.log('stale check: %d table(s) marked for re-copy (round %d): %s' % (len(targets), recopies, ', '.join(tables)))
                runner = copy_module.Runner(plans, statements, ctx.journal, ctx.runner_connector(), ctx.log, ctx.config.copy,
                                            abort_at=copy_module.resolve_abort_at(ctx.journal, ctx.config,
                                                                                  copy_module.longest_chunk_seconds(plans, ctx.config, ctx.journal)),
                                            stop_file=copy_module.stop_file_for(ctx.journal_path), retry_failed=True)
                complete = runner.copy()
            if not complete:
                return steps.Outcome(steps.INCOMPLETE, 're-copy after the stale check did not complete; O6 stays pending: rerun O6 to finish '
                                     'the marked buckets, then O7 (s2rebuild run --from O6)', EXIT_INCOMPLETE)
            reopen_copy_step(ctx.journal, recopies, ctx.log)
        # the one counter pass, after the last re-copy round, so every counter follows the rows now in the replacement
        set_counters(session, ctx.db.replacement, data['tables'], ctx.journal, ctx.log)
        # the projections last, each built once over its full table; the gate records the PROJECTIONS count for S3
        outcome = projections.place(ctx, data, 'O7')
        if outcome is not None:
            return outcome
        if missing:
            return steps.Outcome(steps.PENDING, 'stale check 0 mismatches after %d re-copy round(s), counters set; view(s) %s not created: no owner '
                                 'profile. Run the O7 block in %s as the admin, then rerun O7 (s2rebuild step O7); the rerun finds the views and passes'
                                 % (recopies, ', '.join(missing), ownersql.owner_sql_path(ctx, 'O7')), EXIT_GATE)
        return steps.Outcome(steps.PASSED, 'views created, stale check 0 mismatches after %d re-copy round(s), counters set, %s' % (
            recopies, projections.summary(data)))
    finally:
        session.close()


def _o8_rerun(verifier, plans, ctx):
    """Tier 3 for the tables named by --fingerprint TABLE (sampled at --percent N or verify.sample_percent) and --full TABLE."""
    sampled = list(ctx.options.get('fingerprint') or [])
    full = list(ctx.options.get('full') or [])
    percent = ctx.options.get('percent')
    both = sorted(set(sampled) & set(full))
    if both:
        raise KitError('%s named by both --fingerprint and --full; name a table once' % ', '.join(both))
    results = []
    if sampled:
        results.extend(verifier.rerun(sampled, percent=percent))
    if full:
        results.extend(verifier.rerun(full, full=True))
    return results


def handle_o8(step, ctx):
    """O8 handler: the configured tiers over every table, journal rows, the final workbook and report.

    --fingerprint TABLE (--percent N) and --full TABLE run tier 3 for the named tables only.
    --yes accepts the recorded result without a rerun when no chunk was copied since the last
    O8 run (verify.recorded_state), and records an accepted event per sampled or not
    fingerprinted table; a table sampled or left out by the budget otherwise ends the step pending.
    """
    try:
        data, plans, directory = copy_module.load_plans(ctx)
    except KitError as exc:
        return copy_module._no_plans(step, ctx, exc)
    if not ctx.dry_run and ctx.journal.is_empty():
        return steps.Outcome(steps.REFUSED, 'the copy has not run in this journal; run O5 to O7 first', EXIT_REFUSED)
    named = bool(ctx.options.get('fingerprint') or ctx.options.get('full'))
    session = ctx.runner_session(read_guard('runner O8'))
    try:
        if ctx.dry_run:
            verify.dry_run_statements(session, ctx.db.source, ctx.db.replacement, plans, ctx.config)
            return steps.Outcome(steps.PASSED, 'dry run: verification statements for %d tables printed' % len(plans))
        ctx.journal.check_plan(plans, ctx.db.source, ctx.db.replacement)
        baseline = estimate.load_baseline(ctx.config.resolve(ctx.config.estimate.baseline) if ctx.config.estimate.baseline else None)
        tiers = list(ctx.config.verify.tiers)
        verifier = verify.Verifier(session, ctx.db.source, ctx.db.replacement, plans, ctx.journal, ctx.config, baseline, ctx.log,
                                   connector=ctx.runner_connector())
        if named:
            results = _o8_rerun(verifier, plans, ctx)
        else:
            recorded, reason = verify.recorded_state(ctx.journal, plans, tiers) if ctx.yes else (None, 'no --yes')
            if recorded is not None:
                to_accept, _ = verify.tables_to_accept(ctx.journal, plans, tiers)
                lines = verify.accept_recorded(ctx.journal, to_accept, ctx.log)
                _write_final_reports(ctx, data, plans, baseline)
                return steps.Outcome(steps.PASSED, 'recorded result accepted without a rerun: no chunk was copied since the last O8 run%s' % (
                    ('; ' + '; '.join(lines)) if lines else ''))
            if ctx.yes:
                ctx.log('O8 runs the tiers again: ' + reason)
            results = verifier.run()
        for line in verify.summary_lines(results):
            ctx.log(line)
        _write_final_reports(ctx, data, plans, baseline)
        targets = verify.recopy_targets(results, plans)
        if targets:
            tables = sorted({t for t, _ in targets})
            for line in ownersql.retire_o10_blocks(ctx, tables, 'O8 found a mismatch and marks %s for re-copy; rerun O6 to O8 '
                                                   '(s2rebuild run --from O6)' % ', '.join(tables)):
                ctx.log(line)
            for table, bucket in targets:
                ctx.journal.mark_recopy(table, bucket)
            hint = '; '.join('mark-recopy %s%s' % (t, '' if b is None else ' --bucket %d' % b) for t, b in targets)
            return steps.Outcome(steps.MISMATCH, 'verification mismatch; buckets marked for re-copy (%s); rerun O6 to O8 '
                                 '(s2rebuild run --from O6): the re-copy reopens O7 for the counters and the stale check' % hint, EXIT_MISMATCH)
        to_accept, missing = verify.tables_to_accept(ctx.journal, plans, tiers)
        if missing:
            return steps.Outcome(steps.PENDING, 'tier 3 of the named table(s) recorded; %s have no tier 3 result at the current re-copy epoch: '
                                 'run step O8 without --fingerprint or --full%s' % (', '.join(missing), steps.run_flag(ctx.run_id)), steps.EXIT_GATE)
        sampled = sorted(t for t, r in to_accept.items() if r['result'] == verify.SAMPLED)
        budget = sorted(t for t, r in to_accept.items() if r['result'] == verify.NOT_FINGERPRINTED)
        if to_accept and not ctx.yes:
            parts = []
            if sampled:
                parts.append('tier 3 was sampled on ' + ', '.join('%s (%s rows, WHERE %s)' % (t, to_accept[t].get('sample_rows'), to_accept[t].get('predicate'))
                                                                  for t in sampled))
            if budget:
                parts.append('not fingerprinted within verify.fingerprint_budget_minutes: ' + ', '.join(budget))
            return steps.Outcome(steps.PENDING, 'tier 1 and 2 pass; %s; rerun step O8 --yes%s to accept the recorded result, step O8 --full TABLE%s '
                                 'for a full fingerprint of one table, or raise verify.full_fingerprint_max_rows or '
                                 'verify.fingerprint_budget_minutes and rerun' % ('; '.join(parts), steps.run_flag(ctx.run_id), steps.run_flag(ctx.run_id)),
                                 steps.EXIT_GATE)
        lines = verify.accept_recorded(ctx.journal, to_accept, ctx.log)
        return steps.Outcome(steps.PASSED, 'every tier passed (%d tables%s)' % (len(plans), ('; ' + '; '.join(lines)) if lines else ''))
    finally:
        session.close()


steps.register('O7', handle_o7)
steps.register('O8', handle_o8)
