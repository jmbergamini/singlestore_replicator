"""s2rebuild postcopy: views, AUTO_INCREMENT counters and the O7 stale check; the O7 and O8 handlers.

Author: John Bergamini

Section 10.11 and 10.10 of the architecture. Views are created from the captured definitions
with the DEFINER clause removed (the executing account becomes the definer; the original is
recorded per view). For every AUTO_INCREMENT column the replacement counter is set to MAX plus
one and synced across aggregators when rows exist. Statements issued through the runner guard
naming the replacement: USE, CREATE VIEW, SELECT MAX and COUNT, SHOW CREATE TABLE, ALTER TABLE
AUTO_INCREMENT, AGGREGATOR SYNC AUTO_INCREMENT, and the SELECT statements of the stale check.
"""
import json

from . import copy as copy_module
from . import estimate, inventory, report_md, report_xlsx, steps, verify
from .common import EXIT_INCOMPLETE, EXIT_MISMATCH, EXIT_REFUSED, KitError
from .sqlutil import ident, qualified, read_guard, runner_guard

STALE_CHECK_ROUNDS = 3  # re-copy rounds before O7 gives up; the check itself runs once more after the last round


def view_exists_sql():
    """COUNT of a view in information_schema.VIEWS (parameters: schema, name)."""
    return 'SELECT COUNT(*) AS n FROM information_schema.VIEWS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s'


def view_statement(view, target_db):
    """CREATE VIEW in the replacement from the captured text: DEFINER removed, the view name qualified."""
    text = inventory.strip_definer(view['definition'])
    name = view['name']
    text = text.replace('VIEW %s' % ident(name), 'VIEW %s' % qualified(target_db, name), 1)
    if 'VIEW %s' % qualified(target_db, name) not in text:
        raise KitError('Captured definition of view %s does not name the view as expected' % name)
    return text


def create_views(session, target_db, views, journal, log):
    """Create every captured view that does not exist in the replacement; returns the names created."""
    created = []
    if not views:
        return created
    session.execute('USE ' + ident(target_db))
    for view in views:
        rows = session.query(view_exists_sql(), (target_db, view['name']))
        if rows and int(rows[0]['n'] if isinstance(rows[0], dict) else rows[0][0]):
            log('view %s already exists in %s; skipped' % (view['name'], target_db))
            continue
        session.execute(view_statement(view, target_db))
        journal.event('view', json.dumps({'view': view['name'], 'definer_removed': view.get('definer')}))
        log('created view %s%s' % (view['name'], (' (definer %s removed)' % view['definer']) if view.get('definer') else ''))
        created.append(view['name'])
    return created


def max_count_sql(db, table, column):
    """SELECT MAX(column), COUNT(*) for the counter."""
    return 'SELECT MAX(%s) AS m, COUNT(*) AS n FROM %s' % (ident(column), qualified(db, table))


def alter_counter_sql(db, table, value):
    """ALTER TABLE ... AUTO_INCREMENT = value."""
    return 'ALTER TABLE %s AUTO_INCREMENT = %d' % (qualified(db, table), int(value))


def sync_counter_sql(db, table):
    """AGGREGATOR SYNC AUTO_INCREMENT ON the table."""
    return 'AGGREGATOR SYNC AUTO_INCREMENT ON ' + qualified(db, table)


def _counter_of(session, db, table):
    rows = session.query('SHOW CREATE TABLE ' + qualified(db, table))
    if not rows:
        return None
    row = rows[0]
    return inventory.auto_increment_counter(row['Create Table'] if isinstance(row, dict) else row[1])


def set_counters(session, target_db, tables, journal, log):
    """For every table with an AUTO_INCREMENT column: MAX plus one, then sync; empty tables keep the default.

    tables are the inventory table dicts. Returns the list of actions recorded in the journal.
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
        action = {'table': t['name'], 'column': column, 'source_counter': auto.get('counter'), 'target_before': before,
                  'rows': n}
        if n == 0 or m is None:
            action.update(target_after=before, action='table empty, counter left at default')
        else:
            session.execute(alter_counter_sql(target_db, t['name'], int(m) + 1))
            session.execute(sync_counter_sql(target_db, t['name']))
            action.update(target_after=_counter_of(session, target_db, t['name']), action='counter set to MAX+1 and synced',
                          max_value=int(m))
        journal.event('counter', json.dumps(action))
        log('%s: %s (%d rows, MAX(%s)=%s)' % (t['name'], action['action'], n, column, m))
        actions.append(action)
    return actions


def _write_final_reports(ctx, data, plans, baseline):
    """Write the final workbook and the Markdown report for the run; returns the workbook path."""
    reports = ctx.config.runs_dir() / ctx.run_id / ctx.db.source / 'reports'
    xlsx = report_xlsx.write_final_workbook(reports / 'final.xlsx', data, plans, ctx.journal, ctx.config, ctx.db, ctx.run_id, baseline)
    report_md.write_run_report(reports / 'run-report.md', data, plans, ctx.journal, ctx.config, ctx.db, ctx.run_id)
    ctx.log('final workbook %s and run report %s written' % (xlsx, reports / 'run-report.md'))
    return xlsx


def handle_o7(step, ctx):
    """O7 handler: views, counters, then the stale check with re-copy of mismatching tables until zero mismatches."""
    try:
        data, plans, directory = copy_module.load_plans(ctx)
    except KitError as exc:
        return copy_module._no_plans(step, ctx, exc)
    statements = copy_module.statements_for(ctx.db)
    if not ctx.dry_run and ctx.journal.is_empty():
        return steps.Outcome(steps.REFUSED, 'the copy has not run in this journal; run O5 and O6 first', EXIT_REFUSED)
    guard = runner_guard(ctx.db.replacement, 'runner O7')
    session = ctx.runner_session(guard)
    try:
        if ctx.dry_run:
            if data.get('views'):
                session.execute('USE ' + ident(ctx.db.replacement))
                for view in data['views']:
                    session.execute(view_statement(view, ctx.db.replacement))
            for t in data['tables']:
                if (t.get('auto_increment') or {}).get('column'):
                    session.query(max_count_sql(ctx.db.replacement, t['name'], t['auto_increment']['column']))
                    ctx.log.plain('    DRY RUN: %s' % alter_counter_sql(ctx.db.replacement, t['name'], 0).replace('= 0', '= <MAX+1>'))
                    ctx.log.plain('    DRY RUN: %s' % sync_counter_sql(ctx.db.replacement, t['name']))
            verify.dry_run_statements(session, ctx.db.source, ctx.db.replacement, plans, ctx.config, tiers=[verify.TIER_COUNT, verify.TIER_NEWEST])
            return steps.Outcome(steps.PASSED, 'dry run: view, counter and stale check statements printed')
        ctx.journal.check_plan(plans, statements.source_db, statements.target_db)
        create_views(session, ctx.db.replacement, data.get('views') or [], ctx.journal, ctx.log)
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
            for table, bucket in targets:
                ctx.journal.mark_recopy(table, bucket)
            recopies += 1
            ctx.log('stale check: %d table(s) marked for re-copy (round %d): %s' % (len(targets), recopies, ', '.join(sorted({t for t, _ in targets}))))
            with copy_module.copy_lock(ctx.journal_path):
                runner = copy_module.Runner(plans, statements, ctx.journal, ctx.runner_connector(), ctx.log, ctx.config.copy,
                                            abort_at=copy_module.resolve_abort_at(ctx.journal, ctx.config, copy_module.longest_chunk_seconds(plans, ctx.config)),
                                            stop_file=copy_module.stop_file_for(ctx.journal_path), retry_failed=True)
                complete = runner.copy()
            if not complete:
                return steps.Outcome(steps.INCOMPLETE, 're-copy after the stale check did not complete; rerun O7', EXIT_INCOMPLETE)
        set_counters(session, ctx.db.replacement, data['tables'], ctx.journal, ctx.log)
        return steps.Outcome(steps.PASSED, 'views created, stale check 0 mismatches after %d re-copy round(s), counters set' % recopies)
    finally:
        session.close()


def handle_o8(step, ctx):
    """O8 handler: the configured tiers over every table, journal rows, the final workbook and report."""
    try:
        data, plans, directory = copy_module.load_plans(ctx)
    except KitError as exc:
        return copy_module._no_plans(step, ctx, exc)
    if not ctx.dry_run and ctx.journal.is_empty():
        return steps.Outcome(steps.REFUSED, 'the copy has not run in this journal; run O5 to O7 first', EXIT_REFUSED)
    session = ctx.runner_session(read_guard('runner O8'))
    try:
        if ctx.dry_run:
            verify.dry_run_statements(session, ctx.db.source, ctx.db.replacement, plans, ctx.config)
            return steps.Outcome(steps.PASSED, 'dry run: verification statements for %d tables printed' % len(plans))
        ctx.journal.check_plan(plans, ctx.db.source, ctx.db.replacement)
        baseline = estimate.load_baseline(ctx.config.resolve(ctx.config.estimate.baseline) if ctx.config.estimate.baseline else None)
        verifier = verify.Verifier(session, ctx.db.source, ctx.db.replacement, plans, ctx.journal, ctx.config, baseline, ctx.log)
        results = verifier.run()
        for line in verify.summary_lines(results):
            ctx.log(line)
        summary = verify.summarize(results)
        _write_final_reports(ctx, data, plans, baseline)
        targets = verify.recopy_targets(results, plans)
        if targets:
            for table, bucket in targets:
                ctx.journal.mark_recopy(table, bucket)
            hint = '; '.join('mark-recopy %s%s' % (t, '' if b is None else ' --bucket %d' % b) for t, b in targets)
            return steps.Outcome(steps.MISMATCH, 'verification mismatch; buckets marked for re-copy (%s); rerun O6 then O8' % hint, EXIT_MISMATCH)
        sampled = summary.get(verify.TIER_FINGERPRINT, {}).get('sampled', [])
        if sampled and not ctx.yes:
            return steps.Outcome(steps.PENDING, 'tier 1 and 2 pass; tier 3 was sampled beyond the budget on %s; rerun step O8 --yes to accept, '
                                 'or raise verify.fingerprint_budget_minutes' % ', '.join(sampled), steps.EXIT_GATE)
        return steps.Outcome(steps.PASSED, 'every tier passed (%d tables%s)' % (len(plans), ('; sampled accepted: ' + ', '.join(sampled)) if sampled else ''))
    finally:
        session.close()


steps.register('O7', handle_o7)
steps.register('O8', handle_o8)
