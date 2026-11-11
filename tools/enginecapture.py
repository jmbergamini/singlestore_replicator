"""s2rebuild tools/enginecapture: a read-only record of the engine that the test fake is built from (a development tool).

Author: John Bergamini

Every live bug of the first end-to-end run sat where the fake disagreed with the engine. This
module reads what the fake needs to agree on and writes it to tests/engine/helios-<version>.json:
the engine version, the column list of every information_schema view the kit reads, the columns
of SHOW AGGREGATES, the PASSWORD_ variables of the workspace policy, the ATTACH forms and id
shapes as patterns, and, with a fixture database, what the runner sees against the owner (the
ROUTINES and PIPELINES row counts). The error table is kept by hand in ERROR_TABLE and carried
over from an existing file. No host, workspace id, database name or credential goes into the
file: the fixture database name is replaced by a placeholder. Statements issued, all through a
read guard: SELECT on information_schema.COLUMNS, GLOBAL_VARIABLES, ROUTINES and PIPELINES,
SELECT @@memsql_version, SELECT ... LIMIT 1 on a view that COLUMNS does not list, USE and SHOW
AGGREGATES in the fixture database.

Usage from the kit root: .venv/bin/python tools/enginecapture.py [--config PATH] [--fixture-database NAME] [--out FILE] [--dry-run]
"""
import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from s2rebuild.common import KitError, iso, utc_now  # noqa: E402 - the kit root goes on the path first
from s2rebuild.sqlutil import ident  # noqa: E402

# the views the kit reads (work order section 4, W1)
VIEWS = ('TABLES', 'COLUMNS', 'STATISTICS', 'TABLE_STATISTICS', 'MV_COLUMNSTORE_FILES', 'ROUTINES', 'PROJECTIONS', 'VIEWS', 'USER_PRIVILEGES',
         'SCHEMA_PRIVILEGES', 'TABLE_PRIVILEGES', 'PROCESSLIST', 'MV_PROCESSLIST', 'PIPELINES', 'PIPELINES_OFFSETS',
         'PIPELINES_BATCHES_SUMMARY', 'PIPELINES_ERRORS', 'MV_CLOUD_ATTACHED_DATABASES', 'MV_CLOUD_BRANCHES', 'DISTRIBUTED_DATABASES',
         'MV_NODES', 'MV_EVENTS', 'GLOBAL_VARIABLES')
VERSION_SQL = 'SELECT @@memsql_version AS v'
VIEW_COLUMNS_SQL = ("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'information_schema' AND TABLE_NAME = %s "
                    'ORDER BY ORDINAL_POSITION')
PASSWORD_SQL = "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME LIKE 'PASSWORD_%'"
ROUTINE_COUNT_SQL = 'SELECT COUNT(*) AS n FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = %s'
PIPELINE_COUNT_SQL = 'SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s'
FIXTURE_PLACEHOLDER = '<fixture database>'

ATTACH = {
    'types': ['create', 'resume', 'pitr'],
    'syntax_forms': {
        'attached': 'ATTACH DATABASE `<name>` STORAGEID <storage_id> COMPUTEID <compute_id>',
        'detached': "ATTACH DATABASE `<name>` AT MILESTONE '<YYYY-MM-DD HH_MM_SS.ffffff>' STORAGEID <storage_id> COMPUTEID <compute_id>",
        'dropped': 'ATTACH DROPPED DATABASE `<name>` STORAGEID <storage_id> COMPUTEID <compute_id>',
    },
    'read_only_position': 'READ ONLY follows the names and comes before AT MILESTONE, STORAGEID and COMPUTEID; after STORAGEID it is error 1064',
    'storage_id_pattern': '^[0-9]+:[0-9]+$',
    'compute_id_pattern': '^[0-9]+:[0-9]+$',
    'note': ('ATTACH ... AS from a detached record adds a resume row and leaves the old row detached; from an attached database it adds a '
             'pitr row with a new COMPUTE_ID and leaves the old row attached; a detached record re-attaches by its own ATTACH_SYNTAX; '
             'ROW_NUMBER is not stable between listings'),
}

# hand-maintained: the error numbers the kit handles, with the text the server or the driver gives (section 4, W1)
ERROR_TABLE = {
    '1064': {'when': 'a wrong ATTACH form: a quoted STORAGEID, or READ ONLY after STORAGEID',
             'message': "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right "
                        "syntax to use near 'READ ONLY' at line 1", 'source': 'fake, from the live run of October 26, 2026'},
    '1144': {'when': 'CREATE USER with a password that misses the workspace password policy (PASSWORD_% variables)',
             'message': 'The password does not satisfy the password policy', 'source': 'kit wording from fact 20; confirm the text live'},
    '1205': {'when': 'an INSERT or DELETE waits past lock_wait_timeout on a leaf table lock',
             'message': 'Lock wait timeout exceeded; try restarting transaction', 'source': 'fake'},
    '1227': {'when': 'CREATE VIEW with a DEFINER other than the executing account',
             'message': 'Access denied; you need (at least one of) the SUPER privilege(s) for this operation',
             'source': 'fake, from the live run of October 27, 2026'},
    '1290': {'when': 'a write on a READ ONLY attachment (the O3 write probe expects it)', 'message': 'The database is attached read only',
             'source': 'fake'},
    '1305': {'when': 'CREATE AGGREGATE naming a function that does not exist yet, a load target INTO PROCEDURE that does not exist, '
                     'or SHOW CREATE of a routine the database does not hold (routines are created in dependency order)',
             'message': '<kind> <name> does not exist', 'source': 'kit wording from the MySQL number; confirm the number and text live'},
    '1836': {'when': 'a write on a READ ONLY attachment, the second number the server uses for it', 'message': 'Running in read-only mode',
             'source': 'kit wording; freeze.READ_ONLY_ERRNOS accepts 1290 and 1836'},
    '1749': {'when': 'ALTER TABLE ... AUTO_INCREMENT = N on a sharded table',
             'message': "Forwarding Error (node-master-0:3306): Feature 'ALTER TABLE...AUTO_INCREMENT=X for sharded tables' is not supported "
                        'by SingleStore Distributed.', 'source': 'fake, from the live run of October 27, 2026'},
    '1777': {'when': 'an INSERT into a reference table that reads a READ ONLY source',
             'message': 'Forwarding Error (node-master-0:3306): Partitions of <db> has no master instance. This is likely because the node or '
                        'nodes that hold a copy of the partition are down.', 'source': 'fake, from the live run of October 27, 2026'},
    '1997': {'when': 'a chunk in flight while autoscale replaces a node', 'message': 'Transaction was already in its commit phase',
             'source': 'LESSONS fact 30 (work order appendix), wording from the log'},
    '2720': {'when': 'DROP DATABASE of a READ ONLY attachment; detach it and attach it read write first',
             'message': 'Cannot drop a database that is attached read only',
             'source': 'kit wording; the live text of October 27, 2026 reads "is attached as a readonly database ... cannot be dropped. Please try '
                       'DETACH DATABASE instead or DROP from the primary copy"'},
    '2719': {'when': 'an INSERT on a READ ONLY attachment (the retained copy); the write probe accepts it by its message (readonly)',
             'message': 'Leaf Error (<leaf>): Database `<db>` is not attached or is attached as a readonly database on workspace `<workspace>` '
                        'and is unavailable for write operations', 'source': 'live, I0b, October 28, 2026, engine 10.8.0'},
    '1008': {'when': 'DROP DATABASE of a detached record', 'message': "Forwarding Error (<master>): Can't drop database '<db>'; database doesn't exist",
             'source': 'live, I0b, October 28, 2026, engine 10.8.0'},
    '2003': {'when': 'the server is unreachable as a session opens', 'message': "Can't connect to MySQL server", 'source': 'driver'},
    '2004': {'when': 'the client cannot create a socket', 'message': "Can't create TCP/IP socket", 'source': 'driver'},
    '2006': {'when': 'the connection was closed by the server between two statements', 'message': 'MySQL server has gone away',
             'source': 'driver'},
    '2013': {'when': 'the connection dropped during a statement (the kit cannot tell whether it was applied)',
             'message': 'Lost connection to MySQL server during query', 'source': 'driver'},
}


def _rows(session, sql, params=()):
    return [dict(r) if isinstance(r, dict) else r for r in session.query(sql, params)]


def _count(session, sql, name):
    rows = _rows(session, sql, (name,))
    return int(rows[0].get('n') or 0) if rows else None


def view_columns(session, view):
    """The column names of one information_schema view: from COLUMNS, else the keys of one row (values discarded); [] when neither answers."""
    rows = _rows(session, VIEW_COLUMNS_SQL, (view,))
    columns = [str(r.get('COLUMN_NAME')) for r in rows if r.get('COLUMN_NAME')]
    if columns:
        return columns
    rows = _rows(session, 'SELECT * FROM information_schema.%s LIMIT 1' % view)
    return list(rows[0].keys()) if rows else []


def capture(owner, fixture_database=None, runner=None):
    """Read the engine through the owner session (and the runner for the fixture comparison); returns the capture mapping.

    Issues: SELECT @@memsql_version; per view the COLUMNS query (and a one-row SELECT when it
    lists nothing); the PASSWORD_% variables; with a fixture database USE and SHOW AGGREGATES,
    and the ROUTINES and PIPELINES counts as the owner and, when a runner session is given, as
    the runner. Nothing that names a host, a workspace or a credential goes into the result.
    """
    version_rows = _rows(owner, VERSION_SQL)
    version = str(version_rows[0].get('v')) if version_rows else 'unknown'
    out = {'engine_version': version, 'captured': iso(utc_now()), 'seeded': None, 'views': {}, 'warnings': []}
    for view in VIEWS:
        columns = view_columns(owner, view)
        out['views'][view] = columns
        if not columns:
            out['warnings'].append('%s: no column listed by information_schema.COLUMNS and no row to read; add its columns by hand' % view)
    aggregates = {'columns': None, 'note': 'captured in the fixture database with --fixture-database NAME'}
    if fixture_database:
        owner.query('USE %s' % ident(fixture_database))
        rows = _rows(owner, 'SHOW AGGREGATES')
        aggregates['columns'] = [re.sub(r'_in_\w+$', '_in_%s' % FIXTURE_PLACEHOLDER, k) for k in rows[0].keys()] if rows else None
        if not rows:
            out['warnings'].append('SHOW AGGREGATES listed nothing in the fixture database; its columns are not captured')
    out['show_aggregates'] = aggregates
    policy = {str(r.get('VARIABLE_NAME')): str(r.get('VARIABLE_VALUE')) for r in _rows(owner, PASSWORD_SQL)}
    out['password_variables'] = {'names_known': sorted(policy), 'values': policy or None,
                                 'note': 'GLOBAL_VARIABLES PASSWORD_% holds the workspace password policy (fact 20); the values are per workspace'}
    counts = {'fixture_database': FIXTURE_PLACEHOLDER if fixture_database else None, 'ROUTINES': {'runner': None, 'owner': None},
              'PIPELINES': {'runner': None, 'owner': None},
              'note': 'facts 10 and 21: an account without routine or pipeline privileges sees no rows in ROUTINES or PIPELINES; the owner sees them all'}
    if fixture_database:
        counts['ROUTINES']['owner'] = _count(owner, ROUTINE_COUNT_SQL, fixture_database)
        counts['PIPELINES']['owner'] = _count(owner, PIPELINE_COUNT_SQL, fixture_database)
        if runner is not None:
            counts['ROUTINES']['runner'] = _count(runner, ROUTINE_COUNT_SQL, fixture_database)
            counts['PIPELINES']['runner'] = _count(runner, PIPELINE_COUNT_SQL, fixture_database)
    out['runner_versus_owner'] = counts
    out['attach'] = dict(ATTACH)
    out['errors'] = dict(ERROR_TABLE)
    return out


def default_path(root, version):
    """tests/engine/helios-<version>.json under the kit root; the version keeps letters, digits, dots and dashes only."""
    safe = re.sub(r'[^A-Za-z0-9.-]+', '-', str(version)).strip('-') or 'unknown'
    return root / 'tests' / 'engine' / ('helios-%s.json' % safe)


def write_capture(data, path):
    """Write the capture as JSON; an existing file's hand-maintained error table is carried over. Returns the path."""
    path = path.resolve()
    if path.exists():
        try:
            with open(path, encoding='utf8') as f:
                previous = json.load(f)
        except (OSError, ValueError) as exc:
            raise KitError('%s exists but cannot be read as JSON (%s); move it away or pass --out' % (path, type(exc).__name__))
        if isinstance(previous.get('errors'), dict):
            merged = dict(previous['errors'])
            merged.update({k: v for k, v in data.get('errors', {}).items() if k not in merged})
            data['errors'] = merged
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + '\n', encoding='utf8')
    return path


def summary_lines(data, path):
    """What the command prints: the version, the view count, the fixture counts and the warnings."""
    views = data.get('views', {})
    lines = ['engine-capture: engine %s, %d views with columns, %d without, written to %s' % (
        data.get('engine_version'), sum(1 for v in views.values() if v), sum(1 for v in views.values() if not v), path)]
    counts = data.get('runner_versus_owner', {})
    if counts.get('fixture_database'):
        lines.append('fixture database: ROUTINES owner %s runner %s; PIPELINES owner %s runner %s' % (
            counts['ROUTINES']['owner'], counts['ROUTINES']['runner'], counts['PIPELINES']['owner'], counts['PIPELINES']['runner']))
    for warning in data.get('warnings', []):
        lines.append('warning: ' + warning)
    return lines


def main(argv=None):
    """Read the engine with the owner profile and write tests/engine/helios-<version>.json (reads only; no run is opened).

    The owner session reads the version, the view columns, the PASSWORD_ variables and, with
    --fixture-database, SHOW AGGREGATES and the ROUTINES and PIPELINES counts; the runner session
    counts the same two views, so the file records what the runner sees against the owner. A dry
    run prints the statements and writes nothing.
    """
    from s2rebuild.config import load_config
    from s2rebuild.connect import Log, Redactor, SessionFactory
    from s2rebuild.sqlutil import read_guard
    parser = argparse.ArgumentParser(prog='enginecapture')
    parser.add_argument('--config', metavar='PATH', help='configuration file (default: $S2REBUILD_CONFIG or customer/config.yaml)')
    parser.add_argument('--fixture-database', metavar='NAME',
                        help='a database to read SHOW AGGREGATES in and to count ROUTINES and PIPELINES rows as the owner and as the runner')
    parser.add_argument('--out', metavar='FILE', help='write the capture here instead of tests/engine/helios-<version>.json')
    parser.add_argument('--dry-run', action='store_true', help='print the reads, open no connection, write nothing')
    args = parser.parse_args(argv)
    import os
    config = load_config(args.config, env=os.environ)
    if config.owner is None and not args.dry_run:
        raise KitError('enginecapture needs connections.owner in the configuration: the view columns and SHOW AGGREGATES are read with the owner profile')
    fixture = args.fixture_database
    if fixture is not None:
        ident(fixture)  # the shape check before any connection opens; the name itself never reaches the file
    redactor = Redactor([config.workspace.host], soft=[config.runner.user] + ([config.owner.user] if config.owner else []))
    log = Log(redactor, stream=sys.stdout)
    sessions = SessionFactory(config, os.environ, log, args.dry_run)
    try:
        owner = sessions.owner(read_guard('owner engine-capture'))
        runner = sessions.runner(read_guard('runner engine-capture')) if fixture else None
        data = capture(owner, fixture, runner)
    finally:
        sessions.close()
    if args.dry_run:
        print('DRY RUN: enginecapture issued the reads above and writes nothing')
        return 0
    path = Path(args.out) if args.out else default_path(Path(__file__).resolve().parent.parent, data['engine_version'])
    written = write_capture(data, path)
    for line in summary_lines(data, written):
        print(redactor(line))
    return 0


if __name__ == '__main__':
    sys.exit(main())
