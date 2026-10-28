"""s2rebuild sample: build a database as a percent sample of another, for the rehearsal and the lab.

Author: John Bergamini

s2rebuild sample-database SOURCE NAME --percent P [--tables LIST] (section 10.18 of the architecture;
release 1.0 W12). The runner captures SOURCE (inventory.capture; the owner session reads the routines
and the write-capable accounts), creates NAME with the partition layout of SOURCE, CREATE TABLE ... LIKE
for every table, then one INSERT ... SELECT per table: the rows whose CRC32 of the copy key (the key
columns, or every column of a keyless table) MOD 100 is under P for a table with at least
copy.min_rows_for_chunking rows; the whole table for a reference table, for a smaller table and for
every table at 100 percent. A reference table of a READ ONLY source goes through a sharded staging table
in NAME, as the copy does at O6 (fact 24). Then the owner session creates the views and the routines of
the capture in NAME with the definer policy of O5 and O7 (postcopy.create_views, routines.create;
objects.routines.ignore applies); without the owner profile they are left out and the summary says so.
--tables LIST fills only the named tables; every table is still created. Nothing is journaled: the
sample is a tool, not a step. rehearse --from-database NAME --percent P builds the rehearsal source
with build(). Statements issued through the sample guard, the runner write kinds on NAME only: CREATE
DATABASE, CREATE TABLE LIKE, CREATE TABLE (the staging table), INSERT ... SELECT, DROP TABLE (the
staging table); through the owner guard on NAME: USE, CREATE VIEW, CREATE FUNCTION, CREATE AGGREGATE,
CREATE PROCEDURE; and the reads of the capture.
"""
import dataclasses

from . import copy as copy_module
from . import inventory, postcopy, projections, routines
from .common import KitError
from .sqlutil import Guard, RUNNER_KINDS, ident, owner_guard, qualified, read_guard

PERCENT_MIN, PERCENT_MAX = 1, 100
SAMPLED, WHOLE = 'sampled', 'whole'
OWNER_KINDS = ('USE', 'CREATE VIEW', 'CREATE FUNCTION', 'CREATE AGGREGATE', 'CREATE PROCEDURE', 'CREATE PROJECTION')


def check_percent(percent):
    """The sample percent as an integer from 1 to 100; KitError otherwise."""
    try:
        value = int(percent)
    except (TypeError, ValueError):
        raise KitError('--percent must be a whole number from %d to %d' % (PERCENT_MIN, PERCENT_MAX))
    if isinstance(percent, bool) or not PERCENT_MIN <= value <= PERCENT_MAX:
        raise KitError('--percent must be a whole number from %d to %d, got %s' % (PERCENT_MIN, PERCENT_MAX, percent))
    return value


def sample_guard(name):
    """The runner allowance of the sample: the runner write kinds (CREATE DATABASE and CREATE TABLE among them) on NAME only."""
    return Guard('sample', RUNNER_KINDS, (name,))


def owner_sample_guard(name):
    """The owner allowance for the views and routines of the sample: USE and the CREATE kinds of O5 and O7, on NAME only."""
    return owner_guard('owner sample', OWNER_KINDS, (name,))


def whole_table(plan, config, percent):
    """True when the sample takes every row: 100 percent, a reference table, or fewer estimated rows than copy.min_rows_for_chunking."""
    return int(percent) >= PERCENT_MAX or plan.reference or plan.est_rows < int(config.copy.min_rows_for_chunking)


def predicate(plan, percent):
    """CRC32(<key expression>) MOD 100 < P: the rows the sample takes from a table it does not copy whole."""
    return 'CRC32(%s) MOD 100 < %d' % (plan.key_expr, int(percent))


def insert_sql(source, name, plan, percent, config):
    """INSERT INTO NAME.table SELECT the copy columns FROM SOURCE.table, with the sample predicate unless the table is copied whole."""
    cols = plan.column_list()
    sql = 'INSERT INTO %s (%s) SELECT %s FROM %s' % (qualified(name, plan.name), cols, cols, qualified(source, plan.name))
    if not whole_table(plan, config, percent):
        sql += ' WHERE ' + predicate(plan, percent)
    return sql


def spec_for(config, source, name):
    """The DatabaseSpec the capture and the plan of SOURCE use: the first configured database with SOURCE as its source and NAME as its replacement."""
    return dataclasses.replace(config.databases[0], source=source, replacement=name)


def source_read_only(session, source):
    """True when SOURCE is attached READ ONLY (its MV_CLOUD_ATTACHED_DATABASES row says READONLY); issues one read."""
    rows = session.query(inventory.ATTACHMENT_SQL, (source,))
    return bool(rows) and str(dict(rows[0]).get('ATTACH_TYPE') or '').upper() == 'READONLY'


def check_names(source, name, config=None):
    """Refuse a name that is not an identifier, a sample named like its source, or a sample named like a replacement or retained name of the run.

    A sample may take the name of a configured source: that is how a lab source is built for its own run. build
    refuses a NAME that exists, so a live source is never written.
    """
    ident(source)
    ident(name)
    if source == name:
        raise KitError('The sample needs a name of its own; SOURCE and NAME are both %s' % name)
    if config is not None and name in [n for db in config.databases for n in (db.replacement, db.retained)]:
        raise KitError('%s is a replacement or retained name of the configured run; a sample takes another name' % name)


def _stage(session, statements, plan, planned, log):
    """A reference table of a READ ONLY source: every row through a sharded staging table in the sample; returns the rows inserted."""
    stage = copy_module.stage_name(plan, planned)
    if copy_module.one_value(session.query(statements.table_exists(), (statements.target_db, stage))):
        session.execute(statements.drop_table(stage))
    session.execute(statements.create_stage(plan, stage))
    try:
        staged = session.execute(statements.insert_stage(plan, stage, 0))
        rows = session.execute(statements.insert_from_stage(plan, stage))
    finally:
        session.execute(statements.drop_table(stage))
    if rows != staged:
        raise KitError('Reference table %s: %d rows staged but %d inserted into the sample' % (plan.name, staged, rows))
    log('    %s: %d rows, whole (reference table, through the staging table %s)' % (plan.name, rows, stage))
    return rows


def build(open_runner, open_owner, source, name, percent, config, log, tables=None):
    """Create NAME as a PERCENT sample of SOURCE; returns the summary dict.

    open_runner(guard) and open_owner(guard) open guarded sessions (a StepContext's runner_session
    and owner_session, or a SessionFactory's runner and owner); open_owner is None without the
    owner profile. The capture is taken in memory, the sample is created, every table by LIKE, the
    rows by one INSERT ... SELECT per table (the staging path for a reference table of a READ ONLY
    source), then the views and routines through the owner session. tables, when given, names the
    tables that get rows; an unknown name refuses before any statement. Raises KitError when NAME
    exists already or SOURCE is not attached.
    """
    percent = check_percent(percent)
    check_names(source, name, config)
    wanted = [str(t) for t in (tables or [])]
    for table in wanted:
        ident(table)
    spec = spec_for(config, source, name)
    runner = open_runner(sample_guard(name))
    try:
        if runner.query(inventory.LAYOUT_SQL, (name,)):
            raise KitError('A database named %s exists already; choose another name for the sample' % name)
        layouts = runner.query(inventory.LAYOUT_SQL, (source,))
        if not layouts:
            raise KitError('Source database %s is not attached to the workspace; nothing was created' % source)
        layout = dict(layouts[0])
        read_only = source_read_only(runner, source)
        owner_reader = open_owner(read_guard('owner sample')) if open_owner is not None else None
        try:
            data = inventory.capture(runner, spec, config, owner_reader, log=log, step='sample', samples=1, sleep=lambda s: None)
        finally:
            if owner_reader is not None:
                owner_reader.close()
        plans = inventory.plan_tables(data, config, spec)
        planned = [p.name for p in plans]
        unknown = sorted(set(wanted) - set(planned))
        if unknown:
            raise KitError('--tables names %s, which %s does not hold; its tables are %s' % (', '.join(unknown), source, ', '.join(sorted(planned))))
        statements = copy_module.Statements(source, name, layout['NUM_PARTITIONS'], layout['NUM_SUB_PARTITIONS'])
        runner.execute(statements.create_database())
        log('sample %s created with the layout of %s (%s partitions, %s sub-partitions); source attached %s' % (
            name, source, layout['NUM_PARTITIONS'], layout['NUM_SUB_PARTITIONS'], 'READ ONLY' if read_only else 'read write'))
        for plan in plans:
            runner.execute(statements.create_table_like(plan.name))
        rows, sampled, whole = {}, [], []
        for plan in plans:
            if wanted and plan.name not in wanted:
                continue
            if plan.reference and read_only:
                rows[plan.name] = _stage(runner, statements, plan, planned, log)
                whole.append(plan.name)
                continue
            rows[plan.name] = runner.execute(insert_sql(source, name, plan, percent, config))
            if whole_table(plan, config, percent):
                whole.append(plan.name)
                log('    %s: %d rows, whole (%s)' % (plan.name, rows[plan.name], 'reference table' if plan.reference else
                                                       'under copy.min_rows_for_chunking' if percent < PERCENT_MAX else '100 percent'))
            else:
                sampled.append(plan.name)
                log('    %s: %d rows, sampled (%s)' % (plan.name, rows[plan.name], predicate(plan, percent)))
    finally:
        runner.close()
    views = data.get('views') or []
    captured_routines = data.get('routines') or [] if data.get('routines_captured') else []
    captured_projections = data.get('projections') or [] if data.get('projections_captured') else []
    created_views, created_routines, created_projections, ignored = [], [], [], []
    if open_owner is not None and (views or captured_routines or captured_projections):
        owner = open_owner(owner_sample_guard(name))
        try:
            if views:
                created_views = postcopy.create_views(owner, name, views, None, log, config.runner.user)
            keep, ignored = routines.split(config, spec, captured_routines)
            if ignored:
                log('    routines ignored by %s in the sample: %s' % (routines.IGNORE_KEY, ', '.join(ignored)))
            if keep:
                created_routines = routines.create(owner, name, keep, None, log, config.runner.user, step='sample')
            created_projections = projections.create(owner, name, captured_projections, None, log, step='sample')
        finally:
            owner.close()
    elif views or captured_routines:
        log('    views and routines not created in %s: no owner profile (%d views, %d routines captured)' % (name, len(views), len(captured_routines)))
    return {'source': source, 'name': name, 'percent': percent, 'tables': len(plans), 'rows': rows, 'sampled': sampled, 'whole': whole,
            'views': len(created_views), 'views_captured': len(views), 'routines': len(created_routines),
            'routines_captured': len(captured_routines), 'routines_ignored': ignored, 'read_only': read_only,
            'projections': len(created_projections), 'projections_captured': len(captured_projections),
            'owner': open_owner is not None, 'layout': {'partitions': layout['NUM_PARTITIONS'], 'sub_partitions': layout['NUM_SUB_PARTITIONS']}}


def summary_lines(summary):
    """The lines the command prints after build: the counts, then the rows per table."""
    lines = ['sample %s built from %s at %d percent: %d tables (%d sampled by CRC32 of the key, %d whole), %d of %d views, %d of %d routines, '
             '%d of %d projections%s'
             % (summary['name'], summary['source'], summary['percent'], summary['tables'], len(summary['sampled']), len(summary['whole']),
                summary['views'], summary['views_captured'], summary['routines'], summary['routines_captured'],
                summary.get('projections', 0), summary.get('projections_captured', 0),
                '' if summary['owner'] else '; views and routines need the owner profile')]
    for name in sorted(summary['rows']):
        lines.append('    %s: %d rows (%s)' % (name, summary['rows'][name], SAMPLED if name in summary['sampled'] else WHOLE))
    return lines
