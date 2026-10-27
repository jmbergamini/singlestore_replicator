"""s2rebuild routines: functions, table-valued functions, aggregates and procedures captured and recreated.

Author: John Bergamini

Section 10.22 of the architecture. The runner sees no rows in information_schema.ROUTINES
(Helios fact 21), so every routine read goes through the owner session: ROUTINES for the
functions, table-valued functions and procedures, SHOW AGGREGATES for the aggregates, then
SHOW CREATE FUNCTION, SHOW CREATE AGGREGATE or SHOW CREATE PROCEDURE per routine. P3 and O4
store each definition under inventory/<step>/definitions/routines/<name>.<kind>.sql (mode
0600). The replacement gets them in order, through the owner session: functions and TVFs at
O5 after the tables, then the aggregates (which name their functions), then the procedures at
O7 after the views, so a procedure that reads a view compiles; O9 runs after that, so a load
target of INTO PROCEDURE exists. The DEFINER policy is the one of the views (postcopy): the
original definer is kept while that account exists and is not the runner, else the owner; on
error 1227 the routine is created again without the clause and the reason is recorded. A body
that names the source database is refused before any statement. objects.routines.ignore lists
the routines left behind on purpose. The gate per kind: every expected routine exists and its
normalized definition equals the capture. Statements issued through the owner guard of O5
and O7, naming the replacement: USE, CREATE FUNCTION, CREATE AGGREGATE, CREATE PROCEDURE;
reads: SELECT on information_schema.ROUTINES and USER_PRIVILEGES, SHOW AGGREGATES, the three
SHOW CREATE forms.
"""
import json
import re

from . import steps
from .common import EXIT_REFUSED, KitError
from .sqlutil import ident, qualified

FUNCTION, TVF, AGGREGATE, PROCEDURE = 'function', 'tvf', 'aggregate', 'procedure'
KINDS = (FUNCTION, TVF, AGGREGATE, PROCEDURE)
KIND_LABELS = {FUNCTION: 'functions', TVF: 'TVFs', AGGREGATE: 'aggregates', PROCEDURE: 'procedures'}
# the creation order: functions and TVFs after the tables (O5), then the aggregates that name them, then the procedures after the views (O7)
O5_KINDS = (FUNCTION, TVF, AGGREGATE)
O7_KINDS = (PROCEDURE,)
SHOW_CREATE = {FUNCTION: 'SHOW CREATE FUNCTION', TVF: 'SHOW CREATE FUNCTION', AGGREGATE: 'SHOW CREATE AGGREGATE',
               PROCEDURE: 'SHOW CREATE PROCEDURE'}
ROUTINES_SQL = ('SELECT ROUTINE_NAME, ROUTINE_TYPE, DEFINER FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = %s '
                'ORDER BY ROUTINE_NAME')
ROUTINE_COUNT_SQL = 'SELECT COUNT(*) AS n FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = %s'
ROUTINE_EXISTS_SQL = 'SELECT COUNT(*) AS n FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = %s AND ROUTINE_NAME = %s'
AGGREGATES_SQL = 'SHOW AGGREGATES'
IGNORE_KEY = 'objects.routines.ignore'
DEFINITIONS_SUBDIR = 'routines'
KEPT, OWNER_DEFINER = 'kept', 'owner'
DEFINER_REFUSED = 1227
DEFINER_REFUSED_REASON = ('the server refused the original definer (error 1227: another account as DEFINER needs SUPER, which no '
                          'account holds on Helios); the owner account is the definer')
ROUTINE_COUNT_META = 'routine_count'  # the ROUTINES row count of the replacement after O7, the number S3 expects on the live name
_DEFINER = re.compile(r"DEFINER\s*=\s*(`[^`]*`|'[^']*')@(`[^`]*`|'[^']*')")
_HEAD = re.compile(r'\b(FUNCTION|AGGREGATE|PROCEDURE)\s+(?:`([A-Za-z0-9_]+)`|([A-Za-z0-9_]+))\s*\(', re.I)
_TABLE_VALUED = re.compile(r'\bRETURNS\s+(?:TABLE|QUERY)\b', re.I)


def kind_of(routine_type, definition=''):
    """The kit's kind (function, tvf, aggregate, procedure) for a ROUTINE_TYPE value and the SHOW CREATE text. Issues nothing.

    PROCEDURE and AGGREGATE are their own kinds; a FUNCTION whose type or text says it returns a
    table (RETURNS TABLE, RETURNS QUERY, or a ROUTINE_TYPE naming TABLE) is a TVF; the rest are
    functions.
    """
    text = str(routine_type or '').upper()
    if 'PROCEDURE' in text:
        return PROCEDURE
    if 'AGGREGATE' in text:
        return AGGREGATE
    if 'TABLE' in text or _TABLE_VALUED.search(str(definition or '')):
        return TVF
    return FUNCTION


def definer_of(definition, column_value=None):
    """The captured DEFINER as `user`@`host`: from the DEFINER clause of the text, else from the ROUTINES.DEFINER value (user@host); None without either."""
    m = _DEFINER.search(str(definition or ''))
    if m:
        return '`%s`@`%s`' % (m.group(1).strip("`'"), m.group(2).strip("`'"))
    text = str(column_value or '').strip()
    if not text or '@' not in text:
        return None
    user, _, host = text.rpartition('@')
    return '`%s`@`%s`' % (user.strip("`'"), host.strip("`'"))


def strip_definer(definition):
    """The routine text without its DEFINER clause, so the account that executes it becomes the definer. Issues nothing."""
    return re.sub(r'\s*DEFINER\s*=\s*(?:`[^`]*`|\'[^\']*\')@(?:`[^`]*`|\'[^\']*\')', '', str(definition or ''), count=1)


def normalize(definition):
    """Whitespace-insensitive routine text without its DEFINER clause and without a trailing semicolon; the gate compares these."""
    text = ' '.join(strip_definer(definition).split())
    return text[:-1].rstrip() if text.endswith(';') else text


def statement(routine, target_db, keep_definer=False):
    """The CREATE statement of a captured routine in the replacement: its name qualified, the DEFINER clause kept or removed.

    SHOW CREATE names the routine unqualified (quoted or not) right after FUNCTION, AGGREGATE or
    PROCEDURE; that first occurrence becomes `target_db`.`name`, so the guard sees the database.
    Raises KitError when the text does not name the routine where the server writes it.
    """
    text = routine['definition'] if keep_definer else strip_definer(routine['definition'])
    name = routine['name']
    m = _HEAD.search(text)
    if not m or (m.group(2) or m.group(3)) != name:
        raise KitError('Captured definition of routine %s does not name the routine as expected' % name)
    head = text[:m.start()] + '%s %s(' % (m.group(1).upper(), qualified(target_db, name))
    return head + text[m.end():]


def names_database(definition, database):
    """True when a routine text names database as a qualifier outside its string literals (postcopy.names_database). Issues nothing."""
    from . import postcopy
    return postcopy.names_database(definition, database)


def ignored(config, db, name):
    """True when objects.routines.ignore lists the routine by name, or as <database>.<name> for this database only."""
    ignore = set(config.objects.routines.ignore) if config.objects is not None else set()
    return name in ignore or '%s.%s' % (db.source, name) in ignore


def label(routine):
    """'<kind> <name>' for log lines, the plan sheet and the workbook."""
    return '%s %s' % (routine.get('kind') or kind_of(routine.get('type'), routine.get('definition')), routine['name'])


def split(config, db, routines, kinds=KINDS):
    """(to_create, ignored_labels) of the captured routines of the given kinds; no statement is issued."""
    keep, left = [], []
    for r in routines:
        kind = r.get('kind') or kind_of(r.get('type'), r.get('definition'))
        if kind not in kinds:
            continue
        if ignored(config, db, r['name']):
            left.append(label(r))
        else:
            keep.append(dict(r, kind=kind))
    return keep, left


def summary(config, db, inventory):
    """'N routines (f functions, t TVFs, a aggregates, p procedures), M ignored by configuration', or why none were captured."""
    if not inventory.get('routines_captured'):
        return 'not captured (%s)' % (inventory.get('routines_note') or 'no owner profile')
    routines = inventory.get('routines') or []
    by_kind = {k: 0 for k in KINDS}
    for r in routines:
        by_kind[r.get('kind') or kind_of(r.get('type'), r.get('definition'))] += 1
    left = split(config, db, routines)[1]
    text = '%d routines (%s), %d ignored by configuration' % (
        len(routines), ', '.join('%d %s' % (by_kind[k], KIND_LABELS[k]) for k in KINDS), len(left))
    return text + (': ' + ', '.join(left) if left else '')


def pipeline_target_refusals(config, db, inventory):
    """Why the capture cannot be replayed: a captured load target INTO PROCEDURE whose procedure is ignored or not captured. Issues nothing."""
    pipelines = (inventory.get('pipelines') or {}).get('items') or []
    names = {r['name'] for r in inventory.get('routines') or []}
    out = []
    for item in pipelines:
        target = str(item.get('target') or '')
        if not target.startswith('procedure '):
            continue
        proc = target.split(' ', 1)[1]
        if ignored(config, db, proc):
            out.append('%s loads into %s, which %s lists, so O9 would find no target' % (item['name'], target, IGNORE_KEY))
        elif inventory.get('routines_captured') and proc not in names:
            out.append('%s loads into %s, which the capture does not hold' % (item['name'], target))
    return out


def refuse_pipeline_targets(inventory, config, db, step, journal=None):
    """Raise KitError when a captured load target names a procedure the replacement will not hold (pipeline_target_refusals).

    With a journal the step is started and recorded failed first, so an earlier pass of the
    step no longer counts. Returns the ignored routine labels when nothing is refused.
    """
    refusals = pipeline_target_refusals(config, db, inventory)
    if not refusals:
        return split(config, db, inventory.get('routines') or [])[1]
    note = ('%s refused for %s: %s. Remove the name from %s, or list the load target as application managed, then capture again' % (
        step, db.source, '; '.join(refusals), IGNORE_KEY))
    if journal is not None:
        journal.step_start(step)
        journal.step_end(step, 'failed', note)
    raise KitError(note)


# ---------------------------------------------------------------------------
# Capture (P2, P3, O4), owner session
# ---------------------------------------------------------------------------
def _first_value(row):
    return str(list(row.values())[0]) if isinstance(row, dict) else str(row[0])


def _text(rows, what):
    if not rows:
        raise KitError('%s returned nothing; capture again with the owner profile' % what)
    row = rows[0]
    if isinstance(row, dict):
        values = list(row.values())
        return str(values[1] if len(values) > 1 else values[0])
    return str(row[1] if len(row) > 1 else row[0])


def aggregate_names(session, db_name):
    """The aggregate names SHOW AGGREGATES lists for a database (USE first, since it lists the current database). Issues USE and SHOW AGGREGATES."""
    session.query('USE ' + ident(db_name))
    return [_first_value(r) for r in session.query(AGGREGATES_SQL)]


def capture(session, db_name):
    """Every routine of a database with its definition, through the owner session.

    Issues the ROUTINES listing, USE and SHOW AGGREGATES, then SHOW CREATE AGGREGATE per
    aggregate and SHOW CREATE FUNCTION or SHOW CREATE PROCEDURE per routine. A name SHOW
    AGGREGATES lists is an aggregate whatever ROUTINES says of it. Returns the list of
    {'name', 'kind', 'type', 'definer', 'definition'} in name order, aggregates included.
    """
    items = {}
    for name in aggregate_names(session, db_name):
        text = _text(session.query('%s %s' % (SHOW_CREATE[AGGREGATE], qualified(db_name, name))), 'SHOW CREATE AGGREGATE of ' + name)
        items[name] = {'name': name, 'kind': AGGREGATE, 'type': 'AGGREGATE', 'definer': definer_of(text), 'definition': text}
    for r in session.query(ROUTINES_SQL, (db_name,)):
        name = str(r['ROUTINE_NAME'])
        if name in items:
            continue
        routine_type = str(r.get('ROUTINE_TYPE') or '')
        kind = kind_of(routine_type)
        if kind == AGGREGATE:
            continue
        text = _text(session.query('%s %s' % (SHOW_CREATE[kind], qualified(db_name, name))), '%s of %s' % (SHOW_CREATE[kind], name))
        items[name] = {'name': name, 'kind': kind_of(routine_type, text), 'type': routine_type,
                       'definer': definer_of(text, r.get('DEFINER')), 'definition': text}
    return [items[n] for n in sorted(items)]


def file_name(routine):
    """<name>.<kind>.sql under definitions/routines/."""
    return '%s.%s.sql' % (routine['name'], routine.get('kind') or kind_of(routine.get('type'), routine.get('definition')))


def drift(before, after):
    """(added, removed, changed) routine labels between two captures; a change is a normalized definition that differs. Issues nothing."""
    old = {r['name']: r for r in before.get('routines') or []}
    new = {r['name']: r for r in after.get('routines') or []}
    added = sorted(label(new[n]) for n in set(new) - set(old))
    removed = sorted(label(old[n]) for n in set(old) - set(new))
    changed = sorted(label(new[n]) for n in set(old) & set(new)
                     if normalize(old[n].get('definition')) != normalize(new[n].get('definition')))
    return added, removed, changed


# ---------------------------------------------------------------------------
# Creation (O5, O7), owner session
# ---------------------------------------------------------------------------
def _count(rows):
    if not rows:
        return 0
    row = rows[0]
    return int((row['n'] if isinstance(row, dict) else row[0]) or 0)


def _exists(session, target_db, routine, aggregates):
    if routine['kind'] == AGGREGATE:
        return routine['name'] in aggregates
    return _count(session.query(ROUTINE_EXISTS_SQL, (target_db, routine['name']))) > 0


def _error_text(exc):
    args = getattr(exc, 'args', ())
    return 'server error %s (%s)' % (args[0] if args and isinstance(args[0], int) else 'unknown', type(exc).__name__)


def _create(session, routine, target_db, choice, reason):
    """CREATE with the chosen definer; when the server refuses a kept definer (1227), again without the clause. Returns (choice, reason)."""
    try:
        session.execute(statement(routine, target_db, keep_definer=(choice == KEPT)))
        return choice, reason
    except KitError:
        raise
    except Exception as exc:  # noqa: BLE001 - only the definer refusal falls back; every other error goes to the caller
        args = getattr(exc, 'args', ())
        if choice != KEPT or not args or args[0] != DEFINER_REFUSED:
            raise
    session.execute(statement(routine, target_db))
    return OWNER_DEFINER, DEFINER_REFUSED_REASON


def _recorded(journal):
    return {json.loads(d).get('routine') for d in journal.events('routine')} if journal is not None else set()


def create(session, target_db, routines, journal, log, runner_user=None, step='O5'):
    """Create every routine missing from the replacement, kind by kind in creation order, in rounds; returns the names created.

    Issues USE of the replacement, then per routine the existence check (ROUTINES, or SHOW
    AGGREGATES for an aggregate), the USER_PRIVILEGES COUNT of the definer choice, the CREATE
    naming the replacement and the existence check again. The kinds come in KINDS order
    (functions and TVFs, aggregates, procedures), so an aggregate finds its functions; inside a
    kind a routine whose server error names a missing dependency is tried again after the
    others, until a round creates none, which raises KitError with each routine left and its
    error number. A routine that exists is skipped. journal None (dry run) records nothing.
    """
    from . import postcopy
    created = []
    if not routines:
        return created
    session.execute('USE ' + ident(target_db))
    recorded = _recorded(journal)
    for kind in KINDS:
        pending = [r for r in routines if r['kind'] == kind]
        errors, rounds = {}, 0
        while pending:
            rounds += 1
            aggregates = aggregate_names(session, target_db) if kind == AGGREGATE else ()
            left = []
            for routine in pending:
                name = routine['name']
                if _exists(session, target_db, routine, aggregates):
                    log('%s %s already exists in %s; skipped' % (kind, name, target_db))
                    if journal is not None and name not in recorded:
                        journal.event('routine', json.dumps({'routine': name, 'kind': kind, 'original_definer': routine.get('definer'),
                                                             'definer': 'found', 'reason': 'found in the replacement'}))
                        recorded.add(name)
                    continue
                choice, reason = postcopy.definer_choice(session, routine, runner_user)
                try:
                    choice, reason = _create(session, routine, target_db, choice, reason)
                except KitError:
                    raise
                except Exception as exc:  # noqa: BLE001 - one routine's server error is kept and that routine is tried again next round
                    errors[name] = _error_text(exc)
                    log('%s %s not created in round %d: %s; it is tried again after the others' % (kind, name, rounds, errors[name]))
                    if journal is not None:
                        journal.event('routine_failed', json.dumps({'routine': name, 'kind': kind, 'round': rounds, 'error': errors[name]}))
                    left.append(routine)
                    continue
                found = _exists(session, target_db, routine, aggregate_names(session, target_db) if kind == AGGREGATE else ())
                if journal is not None and not found:  # a dry run prints the check and creates nothing
                    raise KitError('%s stopped at check "%s %s exists": the CREATE returned without an error, but the server does not list it in %s; '
                                   'inspect the replacement before any further statement' % (step, kind, name, target_db))
                if journal is not None:
                    journal.event('routine', json.dumps({'routine': name, 'kind': kind, 'original_definer': routine.get('definer'),
                                                         'definer': choice, 'reason': reason}))
                    recorded.add(name)
                log('created %s %s (%s)' % (kind, name, reason))
                created.append(name)
            if len(left) == len(pending):
                raise KitError('%s stopped: round %d created none of the %s still missing: %s. Fix the cause, then rerun %s; routines already '
                               'created are skipped' % (step, rounds, KIND_LABELS[kind], ', '.join('%s (%s)' % (r['name'], errors[r['name']]) for r in left), step))
            pending = left
    return created


def verify(session, target_db, routines, log, step='O5'):
    """The gate per kind: every expected routine exists in the replacement and its normalized definition equals the capture.

    Issues the ROUTINES listing of the replacement, USE and SHOW AGGREGATES, and one SHOW
    CREATE per expected routine. Raises KitError naming every routine missing or differing.
    Returns the ROUTINES row count the server reports for the replacement.
    """
    listed = {str(r['ROUTINE_NAME']) for r in session.query(ROUTINES_SQL, (target_db,))}
    aggregates = set(aggregate_names(session, target_db))
    problems = []
    for routine in routines:
        kind = routine['kind']
        present = routine['name'] in (aggregates if kind == AGGREGATE else listed)
        if not present:
            problems.append('%s %s missing' % (kind, routine['name']))
            continue
        live = _text(session.query('%s %s' % (SHOW_CREATE[kind], qualified(target_db, routine['name']))), '%s of %s' % (SHOW_CREATE[kind], routine['name']))
        if normalize(live) != normalize(routine['definition']):
            problems.append('%s %s differs from the capture' % (kind, routine['name']))
    if problems:
        raise KitError('%s gate failed on the routines of %s: %s. Compare definitions/routines/ of the capture with SHOW CREATE in the '
                       'replacement, fix the cause, then rerun %s' % (step, target_db, '; '.join(problems), step))
    by_kind = {}
    for r in routines:
        by_kind[r['kind']] = by_kind.get(r['kind'], 0) + 1
    log('routines gate: %s equal the capture' % (', '.join('%d %s' % (by_kind[k], KIND_LABELS[k]) for k in KINDS if by_kind.get(k)) or 'none expected'))
    return _count(session.query(ROUTINE_COUNT_SQL, (target_db,)))


def refuse_source_names(inventory, config, db, kinds):
    """The refusal text when a routine to create names the source database in its body; None otherwise. Issues nothing."""
    keep = split(config, db, inventory.get('routines') or [], kinds)[0]
    named = sorted(label(r) for r in keep if names_database(r.get('definition'), db.source))
    if not named:
        return None
    return ('%s name the source database %s in their definition; no routine statement was issued. Edit each definition in the capture '
            '(definitions/routines/) so it names no database, then rerun the step' % (', '.join(named), db.source))


def place(ctx, inventory, kinds, step):
    """Create the routines of the given kinds in the replacement with the owner profile, then run the gate; returns an Outcome or None.

    None means the step goes on: nothing captured, nothing to create, or everything created and
    verified. A refusal (a body naming the source) or a missing owner profile with routines to
    create returns the Outcome the step ends with. A dry run prints the statements and records
    nothing. The ROUTINES row count of the replacement after the O7 gate is recorded in the
    journal (routine_count) for the S3 check of the switch.
    """
    refusal = refuse_source_names(inventory, ctx.config, ctx.db, kinds)
    if refusal:
        return steps.Outcome(steps.REFUSED, refusal, EXIT_REFUSED)
    if not inventory.get('routines_captured'):
        ctx.log('routines: %s; none created at %s' % (inventory.get('routines_note') or 'not captured', step))
        return None
    keep, left = split(ctx.config, ctx.db, inventory.get('routines') or [], kinds)
    if left:
        ctx.log('routines ignored by %s at %s: %s' % (IGNORE_KEY, step, ', '.join(left)))
    if not keep:
        return None
    if not ctx.owner_configured and not ctx.dry_run:
        return steps.Outcome(steps.PENDING, '%d routine(s) to create at %s (%s) need the owner profile; configure connections.owner, then rerun %s'
                             % (len(keep), step, ', '.join(label(r) for r in keep), step), steps.EXIT_GATE)
    journal = None if ctx.dry_run else ctx.journal
    owner = ctx.owner_session(steps.guard_for(step, ctx.db, steps.OWNER))
    try:
        create(owner, ctx.db.replacement, keep, journal, ctx.log, ctx.config.runner.user, step)
        if not ctx.dry_run:
            count = verify(owner, ctx.db.replacement, keep, ctx.log, step)
            if step == 'O7':
                ctx.journal.set_meta(ROUTINE_COUNT_META, str(count))
    finally:
        owner.close()
    return None


def expected_count(journal):
    """The ROUTINES row count S3 expects on the live name (recorded by the O7 gate), or None when no gate recorded one."""
    value = journal.get_meta(ROUTINE_COUNT_META)
    try:
        return int(value) if value not in (None, '') else None
    except ValueError:
        return None
