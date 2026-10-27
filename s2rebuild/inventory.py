"""s2rebuild inventory: read-only capture of a database, the copy plan and the drift report.

Author: John Bergamini

Steps P3 and O4 (section 10.5 of the architecture). Statements issued: SELECT on
information_schema views, SHOW CREATE TABLE, SHOW CREATE VIEW and, with the owner profile,
SHOW CREATE PIPELINE ... EXTENDED. Nothing else. Files that hold definitions are written with
mode 0600 and the CREDENTIALS clause of a pipeline is replaced by a placeholder before it is
written. The copy plan (TablePlan, hash buckets) built here is what copy.py executes.
A reference table (CREATE [ROWSTORE] REFERENCE TABLE in its definition) is recorded with
reference true and an exact COUNT(*) at P3 and O4, because TABLE_STATISTICS reports no rows for
it (Helios fact 36); its plan is one bucket through the staging path of copy.py.
Routines (functions, table-valued functions, aggregates, procedures) and the write-capable
account list are read with the owner session (routines.py), since the runner sees no rows in
ROUTINES and no other account's grants; each routine definition goes to
definitions/routines/<name>.<kind>.sql. Without an owner session the capture says so and the
plan sheet prints "not captured". A load target INTO PROCEDURE whose procedure is ignored by
objects.routines.ignore, or absent from the capture, refuses P3 and O4, since O9 would fail.
"""
import csv
import datetime as dt
import json
import math
import os
import re
import time
from pathlib import Path

from . import routines, steps
from .common import KitError, iso, utc_now
from .sqlutil import ident, qualified, strip_literals

INVENTORY_FILE = 'inventory.json'
TABLES_CSV = 'tables.csv'
DEFINITIONS_DIR = 'definitions'
PIPELINES_DIR = 'pipelines'
RECREATE_FILE = 'recreate-with-placeholders.sql'
CREDENTIALS_PLACEHOLDER = '<<CREDENTIALS>>'
PRIVATE_MODE = 0o600
WRITE_PRIVILEGES = ('INSERT', 'UPDATE', 'DELETE', 'ALTER', 'CREATE', 'DROP', 'INDEX', 'LOCK TABLES', 'EXECUTE',
                    'CREATE PIPELINE', 'ALTER PIPELINE', 'DROP PIPELINE', 'START PIPELINE', 'CREATE VIEW', 'DROP VIEW',
                    'ALTER VIEW', 'ALL PRIVILEGES', 'SUPER')
_AUTO_INCREMENT_OPTION = re.compile(r'\bAUTO_INCREMENT=(\d+)')
_DEFINER = re.compile(r'DEFINER=(`[^`]*`@`[^`]*`)')
_VARCHAR = re.compile(r'(?:var)?char\((\d+)\)', re.I)
_REFERENCE = re.compile(r'\s*CREATE\s+(?:ROWSTORE\s+)?REFERENCE\s+TABLE\b', re.I)
_ROWSTORE = re.compile(r'\s*CREATE\s+ROWSTORE\s+(?:REFERENCE\s+)?TABLE\b', re.I)
# SORT KEY [`name`] (`a`, `b`) and KEY `name` (`a`) USING CLUSTERED COLUMNSTORE: the columnstore sort order
_SORT_KEY = re.compile(r'\bSORT\s+KEY\s*(?:`[^`]*`\s*)?\(([^)]*)\)', re.I)
_CLUSTERED_KEY = re.compile(r'\bKEY\s*(?:`[^`]*`\s*)?\(([^)]*)\)\s*USING\s+CLUSTERED\s+COLUMNSTORE\b', re.I)
COPY_PATH_STAGING = 'staging'
COPY_PATH_DIRECT = 'direct'
_CREDENTIALS = re.compile(r"""(CREDENTIALS\s+)(?:'(?:[^'\\]|\\.|'')*'|"(?:[^"\\]|\\.|"")*")""", re.I)
_CREDENTIALS_TOKEN = re.compile(r'(CREDENTIALS\s+)<[^<>]*REDACTED[^<>]*>', re.I)
_CREDENTIALS_CLAUSE = re.compile(r"""\bCREDENTIALS\s*(?:['"]|<(?!-))""", re.I)
_REDACTED = re.compile(r'(<redacted>|\*{3,}|REDACTED|<hidden>)', re.I)
_SOURCE = re.compile(r'LOAD\s+DATA\s+(KAFKA|S3|AZURE|GCS|FS|HDFS|LINK)\s+(\'(?:[^\'\\]|\\.)*\'|"(?:[^"\\]|\\.)*")?', re.I)
_TARGET = re.compile(r'INTO\s+(TABLE|PROCEDURE)\s+`?([A-Za-z0-9_]+)`?', re.I)
_BATCH = re.compile(r'BATCH_INTERVAL\s+(\d+)', re.I)
_DUPLICATE = re.compile(r'\b(REPLACE|IGNORE|SKIP\s+(?:DUPLICATE\s+KEY|ALL|CONSTRAINT|PARSER)\s+ERRORS)\s+INTO\b', re.I)

# ---------------------------------------------------------------------------
# Queries (Appendix A). Every one is a SELECT or a SHOW.
# ---------------------------------------------------------------------------
LAYOUT_SQL = ('SELECT NUM_PARTITIONS, NUM_SUB_PARTITIONS, IS_SYNC, IS_SYNC_DURABILITY, IS_SYNC_DURABILITY_ON_REF_DB_ONLY '
              'FROM information_schema.DISTRIBUTED_DATABASES WHERE DATABASE_NAME = %s')
ATTACHMENT_SQL = ('SELECT DATABASE_NAME, ATTACH_TYPE, STORAGE_ID, COMPUTE_ID, ATTACH_SYNTAX '
                  'FROM information_schema.MV_CLOUD_ATTACHED_DATABASES WHERE DATABASE_NAME = %s')
OBJECTS_SQL = 'SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s ORDER BY TABLE_NAME'
COLUMNS_SQL = ('SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, '
               'EXTRA, GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = %s '
               'ORDER BY TABLE_NAME, ORDINAL_POSITION')
KEYS_SQL = ('SELECT TABLE_NAME, NON_UNIQUE, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, INDEX_TYPE '
            'FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = %s ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX')
ROWS_SQL = ('SELECT TABLE_NAME, SUM(ROWS) AS estimated_rows FROM information_schema.TABLE_STATISTICS '
            "WHERE DATABASE_NAME = %s AND NODE_TYPE = 'LEAF' AND PARTITION_TYPE = 'Master' GROUP BY TABLE_NAME")
SIZE_SQL = ('SELECT f.TABLE_NAME, SUM(f.SIZE) / POW(1024, 3) AS primary_files_gib '
            'FROM information_schema.MV_COLUMNSTORE_FILES f JOIN information_schema.TABLE_STATISTICS t '
            'ON f.DATABASE_NAME = t.DATABASE_NAME AND f.TABLE_NAME = t.TABLE_NAME AND f.HOST = t.HOST AND f.PORT = t.PORT '
            "AND f.PARTITION = t.ORDINAL WHERE f.DATABASE_NAME = %s AND t.NODE_TYPE = 'LEAF' AND t.PARTITION_TYPE = 'Master' "
            'GROUP BY f.TABLE_NAME')
USER_PRIVILEGES_SQL = 'SELECT GRANTEE, PRIVILEGE_TYPE FROM information_schema.USER_PRIVILEGES'
SCHEMA_PRIVILEGES_SQL = 'SELECT GRANTEE, TABLE_SCHEMA, PRIVILEGE_TYPE FROM information_schema.SCHEMA_PRIVILEGES WHERE TABLE_SCHEMA = %s'
TABLE_PRIVILEGES_SQL = 'SELECT GRANTEE, TABLE_NAME, PRIVILEGE_TYPE FROM information_schema.TABLE_PRIVILEGES WHERE TABLE_SCHEMA = %s'
PROCESSLIST_SQL = ('SELECT ID, USER, HOST, DB, COMMAND, TIME, LEFT(INFO, 160) AS INFO FROM information_schema.PROCESSLIST '
                   'WHERE (DB = %s OR INFO LIKE %s) AND INFO IS NOT NULL')
PIPELINES_SQL = 'SELECT PIPELINE_NAME, STATE FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s ORDER BY PIPELINE_NAME'
OFFSETS_SQL = ('SELECT PIPELINE_NAME, SOURCE_TYPE, SOURCE_PARTITION_ID, EARLIEST_OFFSET, LATEST_OFFSET, LATEST_LOADED_OFFSET, '
               'LATEST_SUCCESSFUL_OFFSET FROM information_schema.PIPELINES_OFFSETS WHERE DATABASE_NAME = %s')
# Helios 10.8: START_TIME is the batch timestamp (BATCH_TIME is its duration in seconds); errors carry ERROR_UNIX_TIMESTAMP
BATCHES_SQL = ('SELECT PIPELINE_NAME, BATCH_STATE, START_TIME, ROWS_INSERTED, ROWS_UPDATED '
               'FROM information_schema.PIPELINES_BATCHES_SUMMARY WHERE DATABASE_NAME = %s AND START_TIME >= %s')
ERRORS_SQL = ('SELECT PIPELINE_NAME, ERROR_UNIX_TIMESTAMP, ERROR_MESSAGE FROM information_schema.PIPELINES_ERRORS '
              'WHERE DATABASE_NAME = %s AND ERROR_UNIX_TIMESTAMP >= UNIX_TIMESTAMP(%s)')
ALL_LAYOUTS_SQL = ('SELECT DATABASE_NAME, NUM_PARTITIONS, NUM_SUB_PARTITIONS FROM information_schema.DISTRIBUTED_DATABASES '
                   'ORDER BY DATABASE_NAME')
ALL_ROWS_SQL = ('SELECT DATABASE_NAME, TABLE_NAME, SUM(ROWS) AS estimated_rows FROM information_schema.TABLE_STATISTICS '
                "WHERE NODE_TYPE = 'LEAF' AND PARTITION_TYPE = 'Master' GROUP BY DATABASE_NAME, TABLE_NAME")
ALL_SIZES_SQL = ('SELECT f.DATABASE_NAME, SUM(f.SIZE) / POW(1024, 3) AS primary_files_gib '
                 'FROM information_schema.MV_COLUMNSTORE_FILES f JOIN information_schema.TABLE_STATISTICS t '
                 'ON f.DATABASE_NAME = t.DATABASE_NAME AND f.TABLE_NAME = t.TABLE_NAME AND f.HOST = t.HOST AND f.PORT = t.PORT '
                 "AND f.PARTITION = t.ORDINAL WHERE t.NODE_TYPE = 'LEAF' AND t.PARTITION_TYPE = 'Master' GROUP BY f.DATABASE_NAME")
ALL_PIPELINES_SQL = 'SELECT DATABASE_NAME, COUNT(*) AS pipelines FROM information_schema.PIPELINES GROUP BY DATABASE_NAME'


def write_private(path, text):
    """Write text to path with mode 0600, creating parent folders; used for every file that holds definitions."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, PRIVATE_MODE)
    with os.fdopen(fd, 'w', encoding='utf8') as fh:
        fh.write(text)
    return path


_write_private = write_private


def _int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _epoch_text(value):
    """'YYYY-MM-DD HH:MM:SS' in UTC for a Unix timestamp, or '' when it is missing."""
    try:
        return dt.datetime.fromtimestamp(float(value), dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError, OverflowError, OSError):
        return ''


def _float(value):
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def auto_increment_counter(definition):
    """The AUTO_INCREMENT table option of a CREATE TABLE text, or None."""
    m = _AUTO_INCREMENT_OPTION.search(str(definition or ''))
    return int(m.group(1)) if m else None


def normalize_definition(sql):
    """Whitespace-insensitive CREATE TABLE text with the AUTO_INCREMENT counter removed (state, not structure)."""
    return ' '.join(re.sub(r'\bAUTO_INCREMENT=\d+\s*', '', str(sql or '')).split())


def storage_type(definition):
    """The storage kind of a CREATE TABLE text: rowstore when its CREATE line says ROWSTORE, columnstore when it
    carries a SORT KEY or a clustered columnstore index, else rowstore; None for an empty text."""
    text = str(definition or '').upper()
    if not text:
        return None
    if _ROWSTORE.match(text):
        return 'rowstore'
    if 'SORT KEY' in text or 'CLUSTERED COLUMNSTORE' in text:
        return 'columnstore'
    return 'rowstore'


def is_reference(definition):
    """True for a CREATE [ROWSTORE] REFERENCE TABLE text (a full copy on every node)."""
    return bool(_REFERENCE.match(str(definition or '')))


def sort_key_columns(definition):
    """The columnstore sort key columns of a CREATE TABLE text, in order: SORT KEY (...) or KEY ... USING CLUSTERED COLUMNSTORE.

    An empty list for a rowstore table, an unordered columnstore (SORT KEY ()) or no text. The
    first column is the range column of a sampled fingerprint (verify.range_column).
    """
    text = str(definition or '')
    m = _SORT_KEY.search(text) or _CLUSTERED_KEY.search(text)
    if not m:
        return []
    return [c.strip().strip('`') for c in m.group(1).split(',') if c.strip().strip('`')]


def view_definer(definition):
    """The DEFINER clause value of a CREATE VIEW text, or None."""
    m = _DEFINER.search(str(definition or ''))
    return m.group(1) if m else None


def strip_definer(definition):
    """The CREATE VIEW text without its DEFINER clause (the executing account becomes the definer)."""
    return re.sub(r'\s*DEFINER=`[^`]*`@`[^`]*`', '', str(definition or ''), count=1)


def redact_credentials(statement, name='pipeline'):
    """Replace the CREDENTIALS literal of a pipeline statement with the placeholder.

    Handled: the bare token the server returns for every identity (CREDENTIALS <CREDENTIALS
    REDACTED>), and single-quoted or double-quoted literals with backslash or doubled-quote
    escapes. When an unreplaced CREDENTIALS clause remains afterwards (the word outside a string
    literal followed by a quote or a <, other than the placeholder), the text is refused rather
    than written, so no unrecognized form reaches a file. The word elsewhere (a topic, table or
    column named credentials, or a <- column mapping) is not a clause and is kept.
    """
    text = _CREDENTIALS_TOKEN.sub(lambda m: m.group(1) + "'" + CREDENTIALS_PLACEHOLDER + "'", str(statement or ''))
    text = _CREDENTIALS.sub(lambda m: m.group(1) + "'" + CREDENTIALS_PLACEHOLDER + "'", text)
    if _CREDENTIALS_CLAUSE.search(strip_literals(text.replace("'" + CREDENTIALS_PLACEHOLDER + "'", ''))):
        raise KitError('Pipeline %s: the CREDENTIALS clause has a form the kit cannot replace; refusing to write its definition' % name)
    return text


def has_redaction_marker(text):
    """True when the server replaced a value with a redaction marker."""
    return bool(_REDACTED.search(str(text or '')))


# ---------------------------------------------------------------------------
# Capture
# ---------------------------------------------------------------------------
def _one(rows, what, required=True):
    if not rows:
        if required:
            raise KitError(what + ' is not reported by the workspace')
        return None
    return {k: (v if not isinstance(v, bytes) else v.decode('utf8', 'replace')) for k, v in rows[0].items()}


def _group_columns(column_rows):
    by_table = {}
    for c in sorted(column_rows, key=lambda r: (r['TABLE_NAME'], _int(r['ORDINAL_POSITION']))):
        by_table.setdefault(c['TABLE_NAME'], []).append({
            'name': c['COLUMN_NAME'], 'ordinal': _int(c['ORDINAL_POSITION']), 'data_type': c.get('DATA_TYPE'),
            'column_type': c.get('COLUMN_TYPE'), 'nullable': str(c.get('IS_NULLABLE', '')).upper() == 'YES',
            'default': None if c.get('COLUMN_DEFAULT') is None else str(c.get('COLUMN_DEFAULT')),
            'extra': c.get('EXTRA') or '', 'generation_expression': c.get('GENERATION_EXPRESSION') or ''})
    return by_table


def _group_keys(key_rows):
    by_table = {}
    order = {}
    for r in key_rows:
        key = (r['TABLE_NAME'], r['INDEX_NAME'])
        if key not in order:
            order[key] = {'name': r['INDEX_NAME'], 'unique': _int(r['NON_UNIQUE'], 1) == 0, 'type': r.get('INDEX_TYPE') or '',
                          'columns': []}
            by_table.setdefault(r['TABLE_NAME'], []).append(order[key])
        order[key]['columns'].append((_int(r['SEQ_IN_INDEX']), r['COLUMN_NAME']))
    for keys in by_table.values():
        for k in keys:
            k['columns'] = [c for _, c in sorted(k['columns'])]
    return by_table


def _write_accounts(session, db_name, protected):
    """Grantees holding a write privilege globally, on the database or on one of its tables."""
    accounts = {}

    def _add(grantee, privilege, scope):
        if privilege.upper() not in WRITE_PRIVILEGES:
            return
        entry = accounts.setdefault(grantee, {'grantee': grantee, 'privileges': set(), 'scopes': set()})
        entry['privileges'].add(privilege.upper())
        entry['scopes'].add(scope)
    for r in session.query(USER_PRIVILEGES_SQL):
        _add(r['GRANTEE'], r['PRIVILEGE_TYPE'], 'global')
    for r in session.query(SCHEMA_PRIVILEGES_SQL, (db_name,)):
        _add(r['GRANTEE'], r['PRIVILEGE_TYPE'], 'database')
    for r in session.query(TABLE_PRIVILEGES_SQL, (db_name,)):
        _add(r['GRANTEE'], r['PRIVILEGE_TYPE'], 'table:' + str(r['TABLE_NAME']))
    out = []
    for grantee in sorted(accounts):
        user = grantee.split('@')[0].strip("'\"`")
        if user in protected:
            continue
        entry = accounts[grantee]
        out.append({'grantee': grantee, 'user': user, 'privileges': sorted(entry['privileges']), 'scopes': sorted(entry['scopes'])})
    return out


def _sessions(session, db_name, samples, sleep):
    """PROCESSLIST sampled several times, aggregated by user and host with the statement kinds seen."""
    seen = {}
    for i in range(samples):
        rows = session.query(PROCESSLIST_SQL, (db_name, '%' + db_name + '%'))
        for r in rows:
            key = (str(r.get('USER')), str(r.get('HOST', '')).split(':')[0])
            entry = seen.setdefault(key, {'user': key[0], 'host': key[1], 'samples_seen': 0, 'statement_kinds': set()})
            entry['samples_seen'] += 1
            info = str(r.get('INFO') or '').strip()
            if info:
                entry['statement_kinds'].add(info.split(' ', 1)[0].upper())
        if i + 1 < samples:
            sleep(1)
    out = []
    for key in sorted(seen):
        entry = seen[key]
        out.append(dict(entry, statement_kinds=sorted(entry['statement_kinds'])))
    return out


def parse_pipeline_statements(rows):
    """Split SHOW CREATE PIPELINE ... EXTENDED rows into (create_statement, alter_statements).

    Every non-empty value of every row except the Pipeline name column is a statement; the
    one starting with CREATE is the definition, the others are ALTER PIPELINE offset statements.
    """
    statements = []
    for row in rows:
        for key, value in row.items():
            if str(key).lower() == 'pipeline' or value is None:
                continue
            text = str(value).strip()
            if text:
                statements.append(text)
    create = next((s for s in statements if s.upper().startswith('CREATE')), '')
    alters = [s for s in statements if s.upper().startswith('ALTER')]
    return create, alters


def describe_pipeline(create):
    """Source type, source location, target, batch interval and duplicate handling from a CREATE PIPELINE text."""
    m = _SOURCE.search(create or '')
    source_type = m.group(1).upper() if m else 'UNKNOWN'
    location = (m.group(2) or '').strip('\'"') if m else ''
    t = _TARGET.search(create or '')
    target = ('%s %s' % (t.group(1).lower(), t.group(2))) if t else 'unknown'
    b = _BATCH.search(create or '')
    d = _DUPLICATE.search(create or '')
    duplicate = ' '.join(d.group(1).upper().split()) if d else 'INSERT'
    return {'source_type': source_type, 'source_location': location, 'target': target,
            'batch_interval': int(b.group(1)) if b else None, 'duplicate_handling': duplicate}


def _pipelines(owner_session, db_name, app_managed, now):
    since = (now - dt.timedelta(hours=24)).replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
    pipelines = owner_session.query(PIPELINES_SQL, (db_name,))
    offsets = owner_session.query(OFFSETS_SQL, (db_name,))
    batches = owner_session.query(BATCHES_SQL, (db_name, since))
    errors = owner_session.query(ERRORS_SQL, (db_name, since))
    by_name = {}
    for r in pipelines:
        name = r['PIPELINE_NAME']
        rows = owner_session.query('SHOW CREATE PIPELINE %s EXTENDED' % qualified(db_name, name))
        create, alters = parse_pipeline_statements(rows)
        if not create:
            raise KitError('Pipeline %s: SHOW CREATE PIPELINE returned no CREATE statement; refusing to store an empty definition; '
                           'capture again with the owner profile' % name)
        definition = redact_credentials(create, name)
        # the marker is read on the stored definition, the text O9 checks, so the workbook names the pipelines O9 will refuse
        item = {'name': name, 'state': r['STATE'], 'definition': definition,
                'alter_statements': [redact_credentials(a, name) for a in alters],
                'redaction_marker': has_redaction_marker(definition), 'app_managed': name in app_managed,
                'offsets': [], 'last_batch_time': None, 'rows_24h': 0, 'errors_24h': 0, 'last_error': None}
        item.update(describe_pipeline(create))
        by_name[name] = item
    for o in offsets:
        item = by_name.get(o['PIPELINE_NAME'])
        if item is not None:
            item['offsets'].append({'partition': str(o['SOURCE_PARTITION_ID']), 'earliest': _int(o.get('EARLIEST_OFFSET'), None),
                                    'latest': _int(o.get('LATEST_OFFSET'), None), 'loaded': _int(o.get('LATEST_LOADED_OFFSET'), None),
                                    'successful': _int(o.get('LATEST_SUCCESSFUL_OFFSET'), None)})
    for b in batches:
        item = by_name.get(b['PIPELINE_NAME'])
        if item is not None:
            when = str(b.get('START_TIME'))
            if item['last_batch_time'] is None or when > item['last_batch_time']:
                item['last_batch_time'] = when
            item['rows_24h'] += _int(b.get('ROWS_INSERTED')) + _int(b.get('ROWS_UPDATED'))
    for e in errors:
        item = by_name.get(e['PIPELINE_NAME'])
        if item is not None:
            item['errors_24h'] += 1
            when = _epoch_text(e.get('ERROR_UNIX_TIMESTAMP'))
            if item['last_error'] is None or when > item['last_error']['time']:
                item['last_error'] = {'time': when, 'message': str(e.get('ERROR_MESSAGE') or '')[:500]}
    return {'captured': True, 'note': '%d pipelines captured with the owner profile' % len(by_name),
            'items': [by_name[n] for n in sorted(by_name)]}


ROUTINES_NOT_CAPTURED = 'routines not captured: no owner profile; the runner sees none in ROUTINES'
PIPELINES_READ_ONLY_NOTE = ('pipelines not listed: PIPELINES is empty on a READ ONLY attachment (fact 23); '
                            'O9 replays the O2 capture taken after the stop')


def capture(session, db, config, owner_session=None, exact_counts=False, log=None, step='P3', samples=3, sleep=time.sleep):
    """Capture the inventory of db.source through a read-only runner session.

    Inputs: a Session with the read guard, the DatabaseSpec, the Config, an optional owner
    Session for pipelines, routines and the write-capable accounts (the runner sees neither
    the routines nor other accounts' grants). Output: the inventory dict (see write for the
    file layout). Statements issued: the SELECT and SHOW statements at the top of this module,
    and those of routines.capture through the owner session.
    """
    now = utc_now()
    name = db.source
    protected = {config.runner.user} | ({config.owner.user} if config.owner else set()) | set(config.freeze.lockout.protected_accounts)
    layout = _one(session.query(LAYOUT_SQL, (name,)), 'Source database %s' % name)
    attachment = _one(session.query(ATTACHMENT_SQL, (name,)), 'attachment', required=False)
    objects = session.query(OBJECTS_SQL, (name,))
    columns = _group_columns(session.query(COLUMNS_SQL, (name,)))
    keys = _group_keys(session.query(KEYS_SQL, (name,)))
    estimates = {r['TABLE_NAME']: _int(r['estimated_rows']) for r in session.query(ROWS_SQL, (name,))}
    sizes = {r['TABLE_NAME']: _float(r['primary_files_gib']) for r in session.query(SIZE_SQL, (name,))}
    tables, views = [], []
    for o in objects:
        obj = o['TABLE_NAME']
        if str(o['TABLE_TYPE']).upper() == 'VIEW':
            rows = session.query('SHOW CREATE VIEW ' + qualified(name, obj))
            definition = ''
            if rows:
                row = rows[0]
                definition = str(row.get('Create View') or row.get('Create Table') or list(row.values())[1])
            views.append({'name': obj, 'definition': definition, 'definer': view_definer(definition)})
            continue
        rows = session.query('SHOW CREATE TABLE ' + qualified(name, obj))
        if not rows:
            raise KitError('SHOW CREATE TABLE returned nothing for ' + obj)
        row = rows[0]
        definition = str(row.get('Create Table') or list(row.values())[1])
        cols = columns.get(obj, [])
        auto = next((c['name'] for c in cols if 'auto_increment' in str(c['extra']).lower()), None)
        reference = is_reference(definition)
        exact = None
        if exact_counts or reference:
            # TABLE_STATISTICS reports no rows for a reference table (fact 36), so its count is always exact
            count_rows = session.query('SELECT COUNT(*) AS n FROM ' + qualified(name, obj))
            exact = _int(count_rows[0]['n']) if count_rows else None
            if reference and exact is None:
                raise KitError('Reference table %s: COUNT(*) returned no row, so its plan would have no row count; '
                               'check that the runner can read it and capture again' % obj)
        tables.append({
            'name': obj, 'columns': cols, 'keys': keys.get(obj, []), 'definition': definition,
            'storage_type': storage_type(definition), 'reference': reference,
            'auto_increment': {'column': auto, 'counter': auto_increment_counter(definition)} if auto else None,
            'est_rows': exact if reference else estimates.get(obj, 0), 'exact_rows': exact, 'size_gib': sizes.get(obj),
            'computed_columns': [c['name'] for c in cols if c['generation_expression']],
        })
    if owner_session is not None:
        captured = routines.capture(owner_session, name)
        routines_note = '%d routines captured with the owner profile' % len(captured)
    else:
        captured, routines_note = [], ROUTINES_NOT_CAPTURED
    inventory = {
        'kit': 's2rebuild', 'version': 1, 'database': name, 'captured_utc': iso(now), 'step': step,
        'layout': {k: _int(v) for k, v in layout.items()}, 'attachment': attachment,
        'tables': tables, 'views': views, 'routines': captured,
        'routines_captured': owner_session is not None, 'routines_note': routines_note,
        'accounts': _write_accounts(owner_session if owner_session is not None else session, name, protected),
        'sessions': _sessions(session, name, samples, sleep),
        'size_gib_total': round(sum(t['size_gib'] or 0.0 for t in tables), 3),
        'est_rows_total': sum(t['est_rows'] for t in tables),
    }
    if owner_session is not None and attachment is not None and str(attachment.get('ATTACH_TYPE')).upper() == 'READONLY':
        # fact 23: PIPELINES shows no rows on a READ ONLY attachment, so a listing now would read as every pipeline
        # removed; O9 replays the O2 capture taken after the stop, and the drift report says so instead
        inventory['pipelines'] = {'captured': False, 'items': [], 'read_only': True, 'note': PIPELINES_READ_ONLY_NOTE}
    elif owner_session is not None:
        inventory['pipelines'] = _pipelines(owner_session, name, set(db.app_managed_pipelines), now)
    else:
        inventory['pipelines'] = {'captured': False, 'items': [],
                                  'note': 'pipelines not captured: no owner profile; the O9 gate cannot pass without a capture'}
    if log is not None:
        log('inventory of %s: %d tables (%d reference, counted exactly), %d views, %s, %d write-capable accounts, %s' % (
            name, len(tables), sum(1 for t in tables if t['reference']), len(views), routines_note, len(inventory['accounts']),
            inventory['pipelines']['note']))
    return inventory


# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------
def write(inventory, directory, plans=None):
    """Write inventory.json, tables.csv, definitions/ with definitions/routines/ (0600) and pipelines/ (0600) under directory."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    write_private(directory / INVENTORY_FILE, json.dumps(inventory, indent=1, sort_keys=False, default=str))
    for t in inventory['tables']:
        _write_private(directory / DEFINITIONS_DIR / (t['name'] + '.sql'), t['definition'] + '\n')
    for v in inventory['views']:
        _write_private(directory / DEFINITIONS_DIR / (v['name'] + '.view.sql'), (v['definition'] or '') + '\n')
    for r in inventory.get('routines') or []:
        if r.get('definition'):
            _write_private(directory / DEFINITIONS_DIR / routines.DEFINITIONS_SUBDIR / routines.file_name(r), r['definition'] + '\n')
    plans = plans if plans is not None else []
    by_name = {p.name: p for p in plans}
    with (directory / TABLES_CSV).open('w', newline='', encoding='utf8') as fh:
        w = csv.writer(fh)
        w.writerow(['database', 'table_name', 'estimated_rows', 'exact_rows', 'size_gib', 'storage_type', 'reference', 'copy_path',
                    'key_kind', 'key_columns', 'buckets', 'time_column', 'unique_key_lock_risk', 'auto_increment_column', 'computed_columns'])
        for t in inventory['tables']:
            p = by_name.get(t['name'])
            reference = bool(t.get('reference', is_reference(t.get('definition'))))
            w.writerow([inventory['database'], t['name'], t['est_rows'], '' if t['exact_rows'] is None else t['exact_rows'],
                        '' if t['size_gib'] is None else round(t['size_gib'], 3), t.get('storage_type') or '',
                        1 if reference else 0, COPY_PATH_STAGING if reference else COPY_PATH_DIRECT,
                        p.key_kind if p else '', ','.join(p.key_columns) if p else '', p.n_buckets if p else '',
                        (p.time_column or '') if p else '', (1 if p.unique_key_lock_risk else 0) if p else '',
                        (t['auto_increment'] or {}).get('column') or '', ','.join(t.get('computed_columns') or [])])
    pipelines = inventory.get('pipelines') or {}
    if pipelines.get('captured'):
        blocks = []
        for item in pipelines['items']:
            text = '-- pipeline: %s | state: %s | source: %s | target: %s\n%s;\n' % (
                item['name'], item['state'], item['source_type'], item['target'], item['definition'])
            text += ''.join('%s;\n' % a for a in item['alter_statements'])
            _write_private(directory / PIPELINES_DIR / (item['name'] + '.sql'), text)
            blocks.append(text)
        _write_private(directory / PIPELINES_DIR / RECREATE_FILE,
                       '-- Every CREATE PIPELINE below carries %s in place of the credentials; fill it in before running.\n'
                       '-- Run inside the replacement database. Pipelines are created stopped. Never START here.\n\n' % CREDENTIALS_PLACEHOLDER
                       + '\n'.join(blocks))
    return directory


def load(directory):
    """Read inventory.json from a capture directory."""
    path = Path(directory) / INVENTORY_FILE
    if not path.is_file():
        raise KitError('No inventory at %s; run s2rebuild profile first or pass --inventory DIR' % path)
    try:
        inventory = json.loads(path.read_text(encoding='utf8'))
    except ValueError:
        raise KitError('Inventory file %s is not valid JSON' % path)
    if not isinstance(inventory, dict) or inventory.get('kit') != 's2rebuild' or 'tables' not in inventory:
        raise KitError('Inventory file %s is not an s2rebuild inventory' % path)
    return inventory


# ---------------------------------------------------------------------------
# Copy plan
# ---------------------------------------------------------------------------
class TablePlan:
    """One table's copy plan: columns, key expression, bucket count and the attributes the report shows."""

    def __init__(self, name, columns, est_rows, key_kind, key_columns, definition, n_buckets, time_column=None,
                 unique_key_lock_risk=False, concurrency=None, size_gib=None, chunk_rows=None, computed_columns=(),
                 column_types=None, sort_columns=None):
        self.name = name
        self.columns = list(columns)
        self.est_rows = int(est_rows)
        self.key_kind = key_kind
        self.key_columns = list(key_columns)
        self.definition = definition
        self.n_buckets = int(n_buckets)
        self.time_column = time_column
        self.unique_key_lock_risk = bool(unique_key_lock_risk)
        self.concurrency = concurrency
        self.size_gib = size_gib
        self.chunk_rows = chunk_rows
        self.computed_columns = list(computed_columns)
        # column name to lower-case DATA_TYPE (the verification picks hashable and orderable columns by it; unknown when None)
        self.column_types = dict(column_types or {})
        # the columnstore sort key columns in order, from the definition when not given
        self.sort_columns = list(sort_columns) if sort_columns is not None else sort_key_columns(definition)
        if not self.columns:
            raise KitError('Table %s has no copyable columns' % name)
        if len(self.columns) != len(set(self.columns)):
            raise KitError('Duplicate column names for table ' + name)
        for c in self.key_columns:
            if c not in self.columns:
                raise KitError('Key column %s of %s is not a copyable column' % (c, name))

    @property
    def reference(self):
        """True for a REFERENCE table (a full copy on every node), read from the captured definition.

        Helios runs an INSERT into a reference table on the master aggregator, which cannot read a
        READ ONLY source, so the copy stages its rows first (copy.Runner.copy_reference). The plan
        gives it one bucket, and its row count is the exact count the capture took (fact 36).
        """
        return is_reference(self.definition)

    @property
    def copy_path(self):
        """'staging' for a reference table (through a sharded staging table), 'direct' for INSERT ... SELECT by bucket."""
        return COPY_PATH_STAGING if self.reference else COPY_PATH_DIRECT

    @property
    def key_expr(self):
        """The hashed expression: the key column, CONCAT_WS of the key columns, or every column with COALESCE."""
        if self.key_kind == 'key':
            if len(self.key_columns) == 1:
                return ident(self.key_columns[0])
            return "CONCAT_WS('|', " + ', '.join(ident(c) for c in self.key_columns) + ')'
        return "CONCAT_WS('|', " + ', '.join("COALESCE(%s, '')" % ident(c) for c in self.columns) + ')'

    def predicate(self, bucket):
        """CRC32(key_expr) MOD B = bucket, complete and disjoint over 0 to B minus 1."""
        if not isinstance(bucket, int) or bucket < 0 or bucket >= self.n_buckets:
            raise KitError('Bucket out of range for ' + self.name)
        return 'CRC32(%s) MOD %d = %d' % (self.key_expr, self.n_buckets, bucket)

    def column_list(self):
        """The quoted, comma-separated copy column list in ordinal order."""
        return ', '.join(ident(c) for c in self.columns)

    def describe(self):
        """A dict for reports and the journal."""
        return {'table': self.name, 'estimated_rows': self.est_rows, 'buckets': self.n_buckets, 'key_kind': self.key_kind,
                'key_columns': self.key_columns, 'key_expr': self.key_expr, 'column_count': len(self.columns),
                'time_column': self.time_column, 'unique_key_lock_risk': self.unique_key_lock_risk,
                'concurrency': self.concurrency, 'size_gib': self.size_gib, 'chunk_rows': self.chunk_rows,
                'computed_columns': self.computed_columns, 'reference': self.reference, 'copy_path': self.copy_path,
                'sort_columns': self.sort_columns}


def buckets_for(est_rows, chunk_rows, min_rows_for_chunking, max_buckets_per_table):
    """B = 1 below min_rows_for_chunking, else min(max_buckets_per_table, ceil(rows / chunk_rows)) (section 10.6)."""
    if est_rows is None or est_rows < 0:
        raise KitError('Row estimate missing or negative')
    if est_rows < min_rows_for_chunking:
        return 1
    return max(1, min(int(max_buckets_per_table), math.ceil(est_rows / float(chunk_rows))))


def choose_key(table):
    """('key', columns) for the PRIMARY key or the first unique key with no nullable column, else ('row', [])."""
    nullable = {c['name'] for c in table['columns'] if c.get('nullable')}
    computed = set(table.get('computed_columns') or [])
    keys = table.get('keys') or []
    primary = [k for k in keys if str(k['name']).upper() == 'PRIMARY']
    candidates = primary + [k for k in keys if k.get('unique') and str(k['name']).upper() != 'PRIMARY'
                            and str(k.get('type', '')).upper() not in ('SHARD', 'SORT', 'COLUMNSTORE')]
    for k in candidates:
        cols = list(k['columns'])
        if cols and not any(c in nullable or c in computed for c in cols):
            return 'key', cols
    return 'row', []


def lock_risk(table, key_columns=None):
    """True when any unique key (PRIMARY included) has a varchar longer than 64 or is composite with a varchar (section 10.5).

    Columnstore inserts into such a table take a leaf table lock, so parallel chunks collide
    with error 1205. key_columns is accepted for callers that know only the hash key.
    """
    types = {c['name']: str(c.get('column_type') or '') for c in table['columns']}
    unique_keys = [list(k['columns']) for k in table.get('keys') or [] if k.get('unique')]
    if key_columns and list(key_columns) not in unique_keys:
        unique_keys.append(list(key_columns))
    for columns in unique_keys:
        varchars = []
        for c in columns:
            m = _VARCHAR.search(types.get(c, ''))
            if m:
                varchars.append(int(m.group(1)))
        if not varchars:
            continue
        if len(columns) > 1 or varchars[0] > 64:
            return True
    return False


def time_column_for(table, candidates):
    """The first configured candidate that exists on the table, or None."""
    names = {c['name'] for c in table['columns']}
    for candidate in candidates:
        if candidate in names:
            return candidate
    return None


def plan_tables(inventory, config, db):
    """Build the TablePlan list for a database from its inventory and the copy settings, largest table first."""
    plans = []
    for t in inventory['tables']:
        override = db.table_override(t['name'])
        chunk_rows = override.chunk_rows or config.copy.chunk_rows
        key_kind, key_columns = choose_key(t)
        columns = [c['name'] for c in t['columns'] if not c.get('generation_expression')]
        concurrency = override.concurrency if override.concurrency is not None else (1 if config.copy.table_concurrency == 1 else None)
        # a reference table is one bucket through the staging path, whatever its count: one staging table, two inserts
        n_buckets = 1 if is_reference(t['definition']) else buckets_for(
            t['est_rows'], chunk_rows, config.copy.min_rows_for_chunking, config.copy.max_buckets_per_table)
        plans.append(TablePlan(
            t['name'], columns, t['est_rows'], key_kind, key_columns, t['definition'], n_buckets,
            time_column=time_column_for(t, db.time_column_candidates), unique_key_lock_risk=lock_risk(t, key_columns),
            concurrency=concurrency, size_gib=t.get('size_gib'), chunk_rows=chunk_rows,
            computed_columns=t.get('computed_columns') or [],
            column_types={c['name']: str(c.get('data_type') or '').lower() for c in t['columns']}))
    names = [p.name for p in plans]
    if len(names) != len(set(names)):
        raise KitError('Duplicate table names in the inventory')
    plans.sort(key=lambda p: (-p.est_rows, p.name))
    return plans


def plan_summary(plans):
    """Totals: tables, buckets, estimated rows and the statement counts the copy will issue."""
    buckets = sum(p.n_buckets for p in plans)
    reference = sum(1 for p in plans if p.reference)
    return {'tables': len(plans), 'buckets': buckets, 'estimated_rows': sum(p.est_rows for p in plans),
            'size_gib': round(sum(p.size_gib or 0.0 for p in plans), 3), 'reference_tables': reference,
            'statements': {'create_database': 1, 'create_table_like': len(plans), 'show_create_table': len(plans),
                           'insert_select': buckets, 'staging_tables': reference}}


# ---------------------------------------------------------------------------
# Drift (O4 against P3)
# ---------------------------------------------------------------------------
def pipeline_drift(before, after):
    """(added, removed, changed, note) pipeline labels between two captures; a change is a definition whose text differs.

    Offsets and states are not drift (O2 stops the pipelines and their offsets move until then).
    When either capture holds no pipeline listing (no owner profile, or a READ ONLY attachment,
    fact 23) nothing is compared and note says why, so a frozen source never reads as every
    pipeline removed. Issues nothing.
    """
    old_cap, new_cap = before.get('pipelines') or {}, after.get('pipelines') or {}
    for cap in (new_cap, old_cap):
        if not cap.get('captured'):
            return [], [], [], cap.get('note') or 'pipelines not captured'
    old = {p['name']: p for p in old_cap.get('items') or []}
    new = {p['name']: p for p in new_cap.get('items') or []}
    added = sorted('pipeline %s' % n for n in set(new) - set(old))
    removed = sorted('pipeline %s' % n for n in set(old) - set(new))
    changed = sorted('pipeline %s' % n for n in set(old) & set(new)
                     if ' '.join(str(old[n].get('definition') or '').split()) != ' '.join(str(new[n].get('definition') or '').split()))
    return added, removed, changed, None


def drift(before, after):
    """Objects added or removed, columns added or removed and definitions changed between two inventories.

    Routines and pipelines count as objects (labels 'kind name'); pipelines_note carries the reason
    when the pipelines could not be compared.
    """
    old_objects = {t['name']: t for t in before['tables']}
    new_objects = {t['name']: t for t in after['tables']}
    old_views = {v['name']: v for v in before.get('views', [])}
    new_views = {v['name']: v for v in after.get('views', [])}
    routines_added, routines_removed, routines_changed = routines.drift(before, after)
    pipelines_added, pipelines_removed, pipelines_changed, pipelines_note = pipeline_drift(before, after)
    report = {
        'objects_added': sorted((set(new_objects) | set(new_views)) - (set(old_objects) | set(old_views))) + routines_added + pipelines_added,
        'objects_removed': sorted((set(old_objects) | set(old_views)) - (set(new_objects) | set(new_views))) + routines_removed + pipelines_removed,
        'columns_added': [], 'columns_removed': [], 'definitions_changed': [], 'rows_changed': [], 'pipelines_note': pipelines_note,
    }
    for name in sorted(set(old_objects) & set(new_objects)):
        old_cols = {c['name'] for c in old_objects[name]['columns']}
        new_cols = {c['name'] for c in new_objects[name]['columns']}
        report['columns_added'].extend('%s.%s' % (name, c) for c in sorted(new_cols - old_cols))
        report['columns_removed'].extend('%s.%s' % (name, c) for c in sorted(old_cols - new_cols))
        if normalize_definition(old_objects[name]['definition']) != normalize_definition(new_objects[name]['definition']):
            report['definitions_changed'].append(name)
        if old_objects[name]['est_rows'] != new_objects[name]['est_rows']:
            report['rows_changed'].append({'table': name, 'before': old_objects[name]['est_rows'], 'after': new_objects[name]['est_rows']})
    for name in sorted(set(old_views) & set(new_views)):
        if ' '.join(str(old_views[name]['definition']).split()) != ' '.join(str(new_views[name]['definition']).split()):
            report['definitions_changed'].append(name)
    report['definitions_changed'].extend(routines_changed + pipelines_changed)
    report['count'] = sum(len(report[k]) for k in ('objects_added', 'objects_removed', 'columns_added', 'columns_removed',
                                                   'definitions_changed'))
    return report


def drift_lines(report):
    """The drift report as log lines; structural differences first, the pipeline note, row estimate changes summarized."""
    lines = []
    for key, label in (('objects_added', 'object added'), ('objects_removed', 'object removed'), ('columns_added', 'column added'),
                       ('columns_removed', 'column removed'), ('definitions_changed', 'definition changed')):
        for item in report[key]:
            lines.append('DRIFT: %s: %s' % (label, item))
    if report.get('pipelines_note'):
        lines.append('drift: pipelines not compared: %s' % report['pipelines_note'])
    lines.append('drift: %d structural difference(s); row estimates changed on %d table(s)' % (report['count'], len(report['rows_changed'])))
    return lines


# ---------------------------------------------------------------------------
# System profile (--all-databases)
# ---------------------------------------------------------------------------
def system_profile(session, config, baseline, estimate_module):
    """One row per database in the workspace with layout, tables, rows, GiB, pipelines and a modeled copy time."""
    layouts = session.query(ALL_LAYOUTS_SQL)
    rows_by_db = {}
    for r in session.query(ALL_ROWS_SQL):
        rows_by_db.setdefault(r['DATABASE_NAME'], []).append(_int(r['estimated_rows']))
    sizes = {r['DATABASE_NAME']: _float(r['primary_files_gib']) for r in session.query(ALL_SIZES_SQL)}
    pipelines = {r['DATABASE_NAME']: _int(r['pipelines']) for r in session.query(ALL_PIPELINES_SQL)}
    out = []
    for lay in layouts:
        name = lay['DATABASE_NAME']
        table_rows = rows_by_db.get(name, [])
        jobs = [(n, buckets_for(n, config.copy.chunk_rows, config.copy.min_rows_for_chunking, config.copy.max_buckets_per_table))
                for n in table_rows]
        wall = estimate_module.simulate_rows(jobs, config.copy.workers, baseline).wall_seconds if jobs else 0.0
        out.append({'database': name, 'partitions': _int(lay['NUM_PARTITIONS']), 'sub_partitions': _int(lay['NUM_SUB_PARTITIONS']),
                    'tables': len(table_rows), 'estimated_rows': sum(table_rows), 'size_gib': sizes.get(name),
                    'pipelines': pipelines.get(name, 0), 'copy_minutes_expected': round(wall / 60.0, 1)})
    out.sort(key=lambda r: -r['estimated_rows'])
    return out


# ---------------------------------------------------------------------------
# Step handlers (registered with steps)
# ---------------------------------------------------------------------------
def inventory_dir(config, run_id, db, step):
    """customer/runs/<run>/<db>/inventory/<step>."""
    return config.runs_dir() / run_id / db.source / 'inventory' / step


def handle_p3(step, ctx):
    """P3 handler: the profile command does the capture; here the step reports whether a capture exists.

    A capture whose load target INTO PROCEDURE names a procedure the replacement will not hold is refused.
    """
    directory = inventory_dir(ctx.config, ctx.run_id or '', ctx.db, 'P3')
    if (directory / INVENTORY_FILE).is_file():
        routines.refuse_pipeline_targets(load(directory), ctx.config, ctx.db, 'P3')
        return steps.Outcome(steps.PASSED, 'inventory present at %s' % directory)
    note = 'run s2rebuild profile to capture the inventory and write the profile workbook'
    if ctx.dry_run:
        return steps.Outcome(steps.STUB, 'no capture yet; ' + note)
    return steps.Outcome(steps.REFUSED, note, steps.EXIT_REFUSED)


def handle_o4(step, ctx):
    """O4 with a capture present: check it and report it (cli.handle_o4 takes the capture inside run when none exists).

    A capture whose load target INTO PROCEDURE names a procedure the replacement will not hold is refused.
    """
    directory = inventory_dir(ctx.config, ctx.run_id or '', ctx.db, 'O4')
    if (directory / INVENTORY_FILE).is_file():
        routines.refuse_pipeline_targets(load(directory), ctx.config, ctx.db, 'O4')
        return steps.Outcome(steps.PASSED, 'freeze capture present at %s' % directory)
    note = 'run s2rebuild run --from O4, which takes the freeze capture (s2rebuild profile --step O4 for a rerun)'
    if ctx.dry_run:
        return steps.Outcome(steps.STUB, 'no freeze capture yet; ' + note)
    return steps.Outcome(steps.REFUSED, note, steps.EXIT_REFUSED)


CURRENT_SQL = 'SELECT CURRENT_USER() AS u, @@memsql_version AS v'
GRANTEE_PRIVILEGES_SQL = 'SELECT PRIVILEGE_TYPE FROM information_schema.USER_PRIVILEGES WHERE GRANTEE = %s'
LEAF_CORES_SQL = "SELECT SUM(NUM_CPUS) AS cores FROM information_schema.MV_NODES WHERE TYPE = 'LEAF'"
PIPELINE_COUNT_SQL = 'SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s'


def grantee_of(current_user):
    """The grantee string 'user'@'host' for a CURRENT_USER() value such as user@% (section 5)."""
    text = str(current_user or '')
    if text.startswith("'"):
        return text
    user, _, host = text.rpartition('@') if '@' in text else (text, '', '%')
    return "'%s'@'%s'" % (user, host or '%')


def preflight(runner, db, config, owner=None):
    """P2 checks; returns (ok, lines, facts).

    Connection and engine version; the runner's privileges against section 5, each missing one by
    name; whether the runner may create branches (recorded, not assumed); the source layout and
    STORAGE_ID; the replacement and retained names free; leaf cores times 8 against the target
    partition count when MV_NODES is readable; with the owner profile, its connection, the
    pipelines it can see and the routines by kind with the ones objects.routines.ignore lists
    (the runner sees none, so without the owner profile the line says so and never "none").
    Statements issued: SELECT, USE and SHOW only.
    """
    from .ownersql import RUNNER_PRIVILEGES
    ok, lines, facts = True, [], {}
    who = _one(runner.query(CURRENT_SQL), 'The runner connection')
    facts.update(runner_user=who.get('u'), engine_version=who.get('v'))
    lines.append('connection: runner %s, engine %s' % (who.get('u'), who.get('v')))
    held = {str(r['PRIVILEGE_TYPE']).upper() for r in runner.query(GRANTEE_PRIVILEGES_SQL, (grantee_of(who.get('u')),))}
    missing = [] if 'ALL PRIVILEGES' in held else [p for p in RUNNER_PRIVILEGES if p not in held]
    facts['missing_privileges'] = missing
    if missing:
        ok = False
        lines.append('privileges: FAILED, missing %s (grant them with s2rebuild account-sql)' % ', '.join(missing))
    else:
        lines.append('privileges: all %d runner privileges of section 5 present' % len(RUNNER_PRIVILEGES))
    facts['runner_can_branch'] = bool(held & {'ATTACH DATABASE', 'CREATE DATABASE', 'ALL PRIVILEGES'})
    lines.append('branching (ATTACH ... AS) by the runner: %s (recorded; the switch runs as the owner)' % (
        'possible, CREATE DATABASE or ATTACH DATABASE held' if facts['runner_can_branch'] else 'not possible'))
    layout = runner.query(LAYOUT_SQL, (db.source,))
    if not layout:
        ok = False
        lines.append('source layout: FAILED, %s is not reported by DISTRIBUTED_DATABASES' % db.source)
    else:
        lay = layout[0]
        facts['source_layout'] = {k: lay.get(k) for k in ('NUM_PARTITIONS', 'NUM_SUB_PARTITIONS')}
        lines.append('source layout: %s partitions, %s sub-partitions; target %d and %d' % (
            lay.get('NUM_PARTITIONS'), lay.get('NUM_SUB_PARTITIONS'), db.layout.partitions, db.layout.sub_partitions))
    attached = runner.query(ATTACHMENT_SQL, (db.source,))
    if not attached:
        ok = False
        lines.append('source STORAGE_ID: FAILED, no row in MV_CLOUD_ATTACHED_DATABASES for %s' % db.source)
    else:
        facts['source_attachment'] = dict(attached[0])
        lines.append('source STORAGE_ID: %s (%s), recorded' % (attached[0].get('STORAGE_ID'), attached[0].get('ATTACH_TYPE')))
    for role, name in (('replacement', db.replacement), ('retained', db.retained)):
        if runner.query(LAYOUT_SQL, (name,)) or runner.query(ATTACHMENT_SQL, (name,)):
            ok = False
            lines.append('%s name: FAILED, %s already exists; choose another name in the configuration' % (role, name))
        else:
            lines.append('%s name: %s is free' % (role, name))
    try:
        rows = runner.query(LEAF_CORES_SQL)
        cores = int(rows[0]['cores']) if rows and rows[0].get('cores') is not None else None
    except Exception:  # noqa: BLE001 - MV_NODES may not be readable; the result is recorded, not assumed
        cores = None
    facts['leaf_cores'] = cores
    if cores is None:
        lines.append('compute size: leaf cores are not readable here; confirm in the Portal that cores x 8 >= %d partitions' % db.layout.partitions)
    elif cores * 8 < db.layout.partitions:
        ok = False
        lines.append('compute size: FAILED, %d leaf cores x 8 = %d is below the %d target partitions' % (cores, cores * 8, db.layout.partitions))
    else:
        lines.append('compute size: %d leaf cores x 8 = %d covers the %d target partitions' % (cores, cores * 8, db.layout.partitions))
    if owner is not None:
        who2 = _one(owner.query(CURRENT_SQL), 'The owner connection')
        count = owner.query(PIPELINE_COUNT_SQL, (db.source,))
        facts['owner_user'] = who2.get('u')
        lines.append('owner: connected as %s; %s pipeline(s) visible on %s' % (who2.get('u'), count[0]['n'] if count else 0, db.source))
        captured = routines.capture(owner, db.source)
        snapshot = {'routines': captured, 'routines_captured': True}
        facts['routines'] = [{'name': r['name'], 'kind': r['kind']} for r in captured]
        facts['routines_ignored'] = routines.split(config, db, captured)[1]
        lines.append('routines: ' + routines.summary(config, db, snapshot))
    else:
        lines.append('owner: no owner profile configured; admin steps print their blocks and wait for recorded gates')
        facts['routines'] = None
        lines.append('routines: not visible to the runner; owner profile needed')
    return ok, lines, facts


def workspace_check(ctx):
    """The workspace part of P2: (ok, lines, facts) through the Management API when a key is configured, else the three questions.

    With workspace.management_api_key_keychain set, the state must be ACTIVE (workspace.preflight_lines);
    the facts go into the preflight record for the plan sheet and the O1 guard. Without a key the
    lines are the questions O1 records the answer to. Reads the API only; no statement.
    """
    from . import workspace
    if not workspace.api_configured(ctx.config):
        return True, workspace.question_lines(), {'source': 'questions'}
    facts = workspace.read_workspace(ctx.config, keychain=getattr(ctx.sessions, 'keychain', None), redactor=ctx.log.redactor)
    lines = workspace.preflight_lines(facts)
    return facts.get('state') == workspace.ACTIVE, lines, facts


def handle_p2(step, ctx):
    """P2 handler (also behind s2rebuild check): the preflight checks, the workspace state and settings, the source record in the journal.

    Refuses Path B with more than one configured database (steps.path_b_single_database) before any connection.
    """
    from .sqlutil import read_guard
    steps.path_b_single_database(ctx.config)
    runner = ctx.runner_session(read_guard('runner P2'))
    owner = ctx.owner_session(read_guard('owner P2')) if ctx.owner_configured else None
    try:
        if ctx.dry_run:
            for sql, args in ((CURRENT_SQL, ()), (GRANTEE_PRIVILEGES_SQL, ('<grantee>',)), (LAYOUT_SQL, (ctx.db.source,)),
                              (ATTACHMENT_SQL, (ctx.db.source,)), (LAYOUT_SQL, (ctx.db.replacement,)),
                              (LAYOUT_SQL, (ctx.db.retained,)), (LEAF_CORES_SQL, ())):
                runner.query(sql, args)
            if owner is not None:
                owner.query(routines.ROUTINES_SQL, (ctx.db.source,))
                owner.query(routines.AGGREGATES_SQL)
            return steps.Outcome(steps.PASSED, 'dry run: preflight queries printed')
        ok, lines, facts = preflight(runner, ctx.db, ctx.config, owner)
        record = None
        if facts.get('source_attachment'):
            from . import switch
            record = switch.identify(runner, ctx.db.source)  # the branch record of the source: the switch and the rollback go by it
    finally:
        runner.close()
        if owner is not None:
            owner.close()
    workspace_ok, workspace_lines, facts['workspace'] = workspace_check(ctx)
    ok = ok and workspace_ok
    lines.extend(workspace_lines)
    for line in lines:
        ctx.log('    ' + line)
    att = facts.get('source_attachment')
    if att:
        ctx.journal.record_attachment_row('source', att)
        if record is not None:
            ctx.journal.record_attachment_row('source', record)
    ctx.journal.set_meta('engine_version', str(facts.get('engine_version')))
    ctx.journal.set_meta('preflight', json.dumps(facts, default=str, sort_keys=True))
    failed = [l for l in lines if 'FAILED' in l]
    if not ok:
        return steps.Outcome(steps.FAILED, 'preflight failed: ' + '; '.join(failed), steps.EXIT_GATE)
    return steps.Outcome(steps.PASSED, 'preflight passed: connection, privileges, layout, STORAGE_ID recorded, target names free')


steps.register('P2', handle_p2)
steps.register('P3', handle_p3)
steps.register('O4', handle_o4)
