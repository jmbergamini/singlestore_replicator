"""s2rebuild journal: the SQLite run journal with migrations, step and chunk transitions.

Author: John Bergamini

One journal per run per database (section 9 of the architecture): WAL mode, synchronous
FULL, every write behind one lock. The schema carries a schema_version row in meta and a
migration list; a journal written by the reference runner (no version row) is upgraded in
place. Statements issued by this module: SQLite statements against the journal file only.
"""
import json
import sqlite3
import threading
from pathlib import Path

from .common import KitError, iso, utc_now

SCHEMA_VERSION = 1
MEMORY = ':memory:'
STEP_STATUSES = ('pending', 'running', 'passed', 'failed', 'skipped')
CHUNK_STATES = ('pending', 'started', 'done', 'failed')
TABLE_FLAGS = ('created_in_target', 'definition_ok')
TABLE_ATTRS = ('size_gib', 'time_column', 'unique_key_lock_risk', 'concurrency', 'verified_at_utc')
ATTACHMENT_ROLES = ('source', 'replacement', 'retained', 'live')
OFFSET_PHASES = ('capture', 'before_start', 'after_release')
PIPELINE_FIELDS = ('state_at_capture', 'source_type', 'source_location', 'target', 'batch_interval',
                   'duplicate_handling', 'definition_file', 'app_managed', 'created_in_replacement_utc',
                   'started_utc', 'state_after_start')

# Column lists drive both CREATE TABLE and the add-missing-column migration.
COLUMNS = {
    'meta': [('key', 'TEXT PRIMARY KEY'), ('value', 'TEXT NOT NULL')],
    'steps': [('step_id', 'TEXT PRIMARY KEY'), ('status', 'TEXT NOT NULL'), ('started_utc', 'TEXT'),
              ('ended_utc', 'TEXT'), ('note', 'TEXT'), ('evidence_path', 'TEXT')],
    'tables': [('table_name', 'TEXT PRIMARY KEY'), ('est_rows', 'INTEGER NOT NULL'), ('n_buckets', 'INTEGER NOT NULL'),
               ('key_kind', 'TEXT NOT NULL'), ('key_expr', 'TEXT NOT NULL'), ('columns_json', 'TEXT NOT NULL'),
               ('created_in_target', 'INTEGER NOT NULL DEFAULT 0'), ('definition_ok', 'INTEGER NOT NULL DEFAULT 0'),
               ('source_count', 'INTEGER'), ('target_count', 'INTEGER'), ('counted_utc', 'TEXT'),
               ('size_gib', 'REAL'), ('time_column', 'TEXT'), ('unique_key_lock_risk', 'INTEGER NOT NULL DEFAULT 0'),
               ('concurrency', 'INTEGER'), ('verified_at_utc', 'TEXT')],
    'chunks': [('table_name', 'TEXT NOT NULL'), ('bucket', 'INTEGER NOT NULL'), ('state', 'TEXT NOT NULL'),
               ('rows_inserted', 'INTEGER'), ('started_utc', 'TEXT'), ('ended_utc', 'TEXT'),
               ('attempts', 'INTEGER NOT NULL DEFAULT 0'), ('error', 'TEXT'), ('source_count', 'INTEGER'),
               ('target_count', 'INTEGER'), ('counted_utc', 'TEXT')],
    'verification': [('id', 'INTEGER PRIMARY KEY AUTOINCREMENT'), ('table_name', 'TEXT NOT NULL'), ('tier', 'TEXT NOT NULL'),
                     ('result', 'TEXT NOT NULL'), ('source_value', 'TEXT'), ('target_value', 'TEXT'),
                     ('sql_source', 'TEXT'), ('sql_target', 'TEXT'), ('detail', 'TEXT'), ('checked_utc', 'TEXT NOT NULL')],
    'attachments': [('id', 'INTEGER PRIMARY KEY AUTOINCREMENT'), ('role', 'TEXT NOT NULL'), ('database_name', 'TEXT NOT NULL'),
                    ('storage_id', 'TEXT'), ('compute_id', 'TEXT'), ('attach_type', 'TEXT'), ('observed_utc', 'TEXT NOT NULL')],
    'grants_snapshot': [('grantee', 'TEXT PRIMARY KEY'), ('privileges_json', 'TEXT NOT NULL'),
                        ('captured_utc', 'TEXT NOT NULL'), ('restored_utc', 'TEXT')],
    'pipelines': [('name', 'TEXT PRIMARY KEY'), ('state_at_capture', 'TEXT'), ('source_type', 'TEXT'),
                  ('source_location', 'TEXT'), ('target', 'TEXT'), ('batch_interval', 'INTEGER'),
                  ('duplicate_handling', 'TEXT'), ('definition_file', 'TEXT'), ('app_managed', 'INTEGER NOT NULL DEFAULT 0'),
                  ('created_in_replacement_utc', 'TEXT'), ('started_utc', 'TEXT'), ('state_after_start', 'TEXT')],
    'pipeline_offsets': [('id', 'INTEGER PRIMARY KEY AUTOINCREMENT'), ('pipeline_name', 'TEXT NOT NULL'),
                         ('source_partition', 'TEXT NOT NULL'), ('phase', 'TEXT NOT NULL'), ('earliest', 'INTEGER'),
                         ('latest', 'INTEGER'), ('loaded', 'INTEGER'), ('successful', 'INTEGER'), ('observed_utc', 'TEXT NOT NULL')],
    'events': [('ts', 'TEXT NOT NULL'), ('kind', 'TEXT NOT NULL'), ('detail', 'TEXT NOT NULL')],
}
TABLE_CONSTRAINTS = {'chunks': 'PRIMARY KEY (table_name, bucket)'}


def create_table_sql(name):
    """The CREATE TABLE IF NOT EXISTS statement for one journal table."""
    parts = ['%s %s' % col for col in COLUMNS[name]]
    if name in TABLE_CONSTRAINTS:
        parts.append(TABLE_CONSTRAINTS[name])
    return 'CREATE TABLE IF NOT EXISTS %s (%s)' % (name, ', '.join(parts))


def _plan_attr(plan, name, default=None):
    return getattr(plan, name, default)


def read_only_uri(path, immutable=False):
    """A percent-encoded file: URI for a read-only open (mode=ro, or immutable=1 when the folder is read-only)."""
    return Path(path).resolve().as_uri() + ('?immutable=1' if immutable else '?mode=ro')


def _open_read_only(path, **kwargs):
    """Connect read-only; fall back to immutable=1 when the WAL shared-memory file cannot be created."""
    try:
        conn = sqlite3.connect(read_only_uri(path), uri=True, **kwargs)
        conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        return conn
    except sqlite3.OperationalError:
        conn = sqlite3.connect(read_only_uri(path, immutable=True), uri=True, **kwargs)
        conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        return conn


def _open_error(path, exc):
    return KitError('Journal %s could not be opened (%s); it may be locked by another s2rebuild process, corrupt, '
                    'or in a folder that is not writable' % (path, type(exc).__name__))


class Journal:
    """The run journal. Open with a path, or MEMORY for a throwaway in-memory journal.

    read_only opens an existing journal without migrating it (status); a journal in an older
    schema is refused with the command that upgrades it. Every sqlite failure while opening
    becomes a KitError that names the path.
    """

    def __init__(self, path, read_only=False):
        self.path = str(path)
        self.lock = threading.RLock()
        try:
            if read_only:
                self.conn = _open_read_only(self.path, check_same_thread=False, isolation_level=None)
                self._check_read_only_schema()
            else:
                if self.path != MEMORY:
                    Path(self.path).parent.mkdir(parents=True, exist_ok=True)
                self.conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
                self.conn.execute('PRAGMA journal_mode=WAL')
                self.conn.execute('PRAGMA synchronous=FULL')
                self._migrate()
        except sqlite3.DatabaseError as exc:
            raise _open_error(self.path, exc)

    def _check_read_only_schema(self):
        tables = self._existing_tables()
        missing = sorted(set(COLUMNS) - tables)
        if missing:
            raise KitError('Journal %s uses an older schema (no %s table); open it once with a writable command '
                           'such as s2rebuild step or run to upgrade it' % (self.path, missing[0]))
        row = self.conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
        if row is not None and int(row[0]) > SCHEMA_VERSION:
            raise KitError('Journal schema version %s is newer than this kit supports (%d); upgrade the kit' % (row[0], SCHEMA_VERSION))

    @classmethod
    def dry_run_copy(cls, path):
        """An in-memory journal holding a copy of the file at path, or empty when there is none.

        A dry run reads the real state and writes nothing to disk.
        """
        journal = cls(MEMORY)
        if path is not None and Path(path).exists():
            try:
                src = _open_read_only(path)
                try:
                    src.backup(journal.conn)
                finally:
                    src.close()
                journal._migrate()
            except sqlite3.DatabaseError as exc:
                journal.close()
                raise _open_error(path, exc)
        return journal

    # -- schema -------------------------------------------------------------
    def _existing_tables(self):
        return {r[0] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    def _existing_columns(self, table):
        return [r[1] for r in self.conn.execute('PRAGMA table_info(%s)' % table)]

    def _migrate(self):
        with self.lock:
            tables = self._existing_tables()
            version = 0
            if 'meta' in tables:
                row = self.conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
                version = int(row[0]) if row else 0
            if version > SCHEMA_VERSION:
                raise KitError('Journal schema version %d is newer than this kit supports (%d); upgrade the kit' % (
                    version, SCHEMA_VERSION))
            if version < 1:
                self._migrate_to_1()
            self.conn.execute("INSERT INTO meta (key, value) VALUES ('schema_version', ?) "
                              'ON CONFLICT(key) DO UPDATE SET value = excluded.value', (str(SCHEMA_VERSION),))

    def _migrate_to_1(self):
        """Version 1: every table of section 9; add columns missing from a reference-runner journal."""
        self.conn.execute('BEGIN')
        try:
            existing = self._existing_tables()
            for name in COLUMNS:
                if name not in existing:
                    self.conn.execute(create_table_sql(name))
                else:
                    have = set(self._existing_columns(name))
                    for col, decl in COLUMNS[name]:
                        if col not in have:
                            self.conn.execute('ALTER TABLE %s ADD COLUMN %s %s' % (name, col, decl.replace(' PRIMARY KEY', '')))
            self.conn.execute('COMMIT')
        except Exception:
            self.conn.execute('ROLLBACK')
            raise

    def schema_version(self):
        """The schema version recorded in meta (0 for a journal written before versioning)."""
        return int(self.get_meta('schema_version') or 0)

    def close(self):
        """Close the SQLite connection."""
        with self.lock:
            self.conn.close()

    def _q(self, sql, args=()):
        with self.lock:
            return self.conn.execute(sql, args).fetchall()

    def _rows(self, sql, args=()):
        with self.lock:
            cur = self.conn.execute(sql, args)
            names = [d[0] for d in cur.description]
            return [dict(zip(names, r)) for r in cur.fetchall()]

    # -- meta and events ------------------------------------------------------
    def get_meta(self, key):
        """The meta value for key as text, or None."""
        rows = self._q('SELECT value FROM meta WHERE key = ?', (key,))
        return rows[0][0] if rows else None

    def set_meta(self, key, value):
        """Insert or replace one meta value (stored as text)."""
        self._q('INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value',
                (key, str(value)))

    def meta(self):
        """Every meta key and value as a dict."""
        return {k: v for k, v in self._q('SELECT key, value FROM meta ORDER BY key')}

    def event(self, kind, detail):
        """Append one timestamped event row (kind, detail)."""
        self._q('INSERT INTO events (ts, kind, detail) VALUES (?, ?, ?)', (iso(utc_now()), kind, str(detail)))

    def events(self, kind=None):
        """Every event as dicts, or the detail strings of one kind in order."""
        if kind is None:
            return self._rows('SELECT ts, kind, detail FROM events ORDER BY rowid')
        return [r['detail'] for r in self._rows('SELECT detail FROM events WHERE kind = ? ORDER BY rowid', (kind,))]

    # -- steps ----------------------------------------------------------------
    def step(self, step_id):
        """The step row as a dict (status, times, note, evidence_path), or None when never recorded."""
        rows = self._rows('SELECT * FROM steps WHERE step_id = ?', (step_id,))
        return rows[0] if rows else None

    def step_status(self, step_id):
        """The status text of a step, or None when never recorded."""
        row = self.step(step_id)
        return row['status'] if row else None

    def step_start(self, step_id, note=None):
        """Mark a step running. Clears the previous end time and keeps earlier evidence."""
        now = iso(utc_now())
        self._q('INSERT INTO steps (step_id, status, started_utc, ended_utc, note) VALUES (?, ?, ?, NULL, ?) '
                'ON CONFLICT(step_id) DO UPDATE SET status = excluded.status, started_utc = excluded.started_utc, '
                'ended_utc = NULL, note = excluded.note', (step_id, 'running', now, note))
        self.event('step_start', step_id)

    def step_end(self, step_id, status, note=None, evidence_path=None):
        """Record the result of a step attempt: passed, failed, skipped or pending (gate waiting)."""
        if status not in STEP_STATUSES or status == 'running':
            raise KitError('Invalid step status ' + str(status))
        now = iso(utc_now())
        self._q('INSERT INTO steps (step_id, status, started_utc, ended_utc, note, evidence_path) VALUES (?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(step_id) DO UPDATE SET status = excluded.status, ended_utc = excluded.ended_utc, '
                'note = COALESCE(excluded.note, steps.note), evidence_path = COALESCE(excluded.evidence_path, steps.evidence_path)',
                (step_id, status, now, now, note, evidence_path))
        self.event('step_' + status, '%s %s' % (step_id, note or ''))

    def record_gate(self, step_id, passed, note, evidence_path=None):
        """Record a gate decision made by a person; the evidence column is set to the file given now, or cleared."""
        self.step_end(step_id, 'passed' if passed else 'failed', note, evidence_path)
        self._q('UPDATE steps SET evidence_path = ? WHERE step_id = ?', (evidence_path, step_id))
        self.event('gate', '%s %s: %s' % (step_id, 'pass' if passed else 'fail', note))

    def steps_board(self):
        """Every step row keyed by step id."""
        return {r['step_id']: r for r in self._rows('SELECT * FROM steps')}

    # -- plan -----------------------------------------------------------------
    def is_empty(self):
        """True when no table plan has been written yet."""
        return not self._q('SELECT 1 FROM tables LIMIT 1')

    def init_plan(self, plans, source_db, target_db, inventory_dir=None):
        """Write the table and chunk plan. plans carry name, est_rows, n_buckets, key_kind, key_expr, columns."""
        with self.lock:
            self.conn.execute('BEGIN')
            try:
                self.set_meta('source_db', source_db)
                self.set_meta('replacement_db', target_db)
                self.set_meta('inventory_dir', str(inventory_dir or ''))
                self.set_meta('created_utc', iso(utc_now()))
                for p in plans:
                    self.conn.execute(
                        'INSERT INTO tables (table_name, est_rows, n_buckets, key_kind, key_expr, columns_json, size_gib, '
                        'time_column, unique_key_lock_risk, concurrency) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                        (p.name, int(p.est_rows), int(p.n_buckets), p.key_kind, p.key_expr, json.dumps(list(p.columns)),
                         _plan_attr(p, 'size_gib'), _plan_attr(p, 'time_column'),
                         1 if _plan_attr(p, 'unique_key_lock_risk') else 0, _plan_attr(p, 'concurrency')))
                    for b in range(int(p.n_buckets)):
                        self.conn.execute('INSERT INTO chunks (table_name, bucket, state) VALUES (?, ?, ?)', (p.name, b, 'pending'))
                self.conn.execute('COMMIT')
            except Exception:
                self.conn.execute('ROLLBACK')
                raise

    def check_plan(self, plans, source_db, target_db):
        """Refuse to resume with a journal that belongs to another pair or another chunk plan.

        Reads replacement_db, falling back to target_db for a journal written by the reference runner.
        """
        recorded_target = self.get_meta('replacement_db') or self.get_meta('target_db')
        if self.get_meta('source_db') != source_db or recorded_target != target_db:
            raise KitError('Journal belongs to a different source/replacement pair; refusing to resume with it')
        recorded = {r[0]: r for r in self._q('SELECT table_name, n_buckets, key_expr, columns_json FROM tables')}
        if set(recorded) != {p.name for p in plans}:
            raise KitError('Journal table set differs from the inventory; refusing to resume')
        for p in plans:
            _, n, expr, cols = recorded[p.name]
            if n != p.n_buckets or expr != p.key_expr or json.loads(cols) != list(p.columns):
                raise KitError('Journal chunk plan differs from the inventory for ' + p.name + '; refusing to resume')

    # -- tables ---------------------------------------------------------------
    def tables(self):
        """Every table row as a dict, largest estimated row count first."""
        return self._rows('SELECT * FROM tables ORDER BY est_rows DESC, table_name')

    def table_flags(self, table):
        """(created_in_target, definition_ok) for a table; (0, 0) when unknown."""
        rows = self._q('SELECT created_in_target, definition_ok FROM tables WHERE table_name = ?', (table,))
        return rows[0] if rows else (0, 0)

    def set_table_flag(self, table, column, value=1):
        """Set created_in_target or definition_ok for a table."""
        if column not in TABLE_FLAGS:
            raise KitError('Unknown table flag ' + str(column))
        self._q('UPDATE tables SET %s = ? WHERE table_name = ?' % column, (value, table))

    def set_table_attr(self, table, column, value):
        """Set one of size_gib, time_column, unique_key_lock_risk, concurrency or verified_at_utc."""
        if column not in TABLE_ATTRS:
            raise KitError('Unknown table attribute ' + str(column))
        self._q('UPDATE tables SET %s = ? WHERE table_name = ?' % column, (value, table))

    def record_table_counts(self, table, source_count, target_count):
        """Store the whole-table COUNT(*) of both sides with the time of the count."""
        self._q('UPDATE tables SET source_count = ?, target_count = ?, counted_utc = ? WHERE table_name = ?',
                (source_count, target_count, iso(utc_now()), table))

    # -- chunks ---------------------------------------------------------------
    def chunk_states(self, table=None):
        """(table_name, bucket, state, rows_inserted, attempts) rows, ordered."""
        if table is None:
            return self._q('SELECT table_name, bucket, state, rows_inserted, attempts FROM chunks ORDER BY table_name, bucket')
        return self._q('SELECT table_name, bucket, state, rows_inserted, attempts FROM chunks WHERE table_name = ? ORDER BY bucket',
                       (table,))

    def chunks(self, table=None):
        """Every chunk row as a dict, for one table or for all."""
        if table is None:
            return self._rows('SELECT * FROM chunks ORDER BY table_name, bucket')
        return self._rows('SELECT * FROM chunks WHERE table_name = ? ORDER BY bucket', (table,))

    def mark_started(self, table, bucket):
        """Committed before the INSERT runs, so a crash leaves the chunk 'started' for reconciliation."""
        self._q("UPDATE chunks SET state = 'started', started_utc = ?, ended_utc = NULL, attempts = attempts + 1, "
                'error = NULL WHERE table_name = ? AND bucket = ?', (iso(utc_now()), table, bucket))

    def mark_done(self, table, bucket, rows):
        """Record a finished chunk with the rows the INSERT reported."""
        self._q("UPDATE chunks SET state = 'done', rows_inserted = ?, ended_utc = ? WHERE table_name = ? AND bucket = ?",
                (rows, iso(utc_now()), table, bucket))

    def undo_attempt(self, table, bucket):
        """A lock wait does not count against the attempt limit."""
        self._q('UPDATE chunks SET attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END '
                'WHERE table_name = ? AND bucket = ?', (table, bucket))

    def mark_failed(self, table, bucket, error):
        """Record a failed attempt with the redacted error text (cut to 2000 characters)."""
        self._q("UPDATE chunks SET state = 'failed', ended_utc = ?, error = ? WHERE table_name = ? AND bucket = ?",
                (iso(utc_now()), str(error)[:2000], table, bucket))

    def mark_recopy(self, table, bucket=None):
        """Set buckets to failed with attempts 0 so the next copy run deletes and re-inserts them."""
        known = {r[0] for r in self._q('SELECT table_name FROM tables')}
        if table not in known:
            raise KitError('Table %s is not in this journal' % table)
        if bucket is None:
            cur_sql = ("UPDATE chunks SET state = 'failed', attempts = 0, error = 'marked for re-copy', ended_utc = NULL "
                       'WHERE table_name = ?')
            args = (table,)
        else:
            cur_sql = ("UPDATE chunks SET state = 'failed', attempts = 0, error = 'marked for re-copy', ended_utc = NULL "
                       'WHERE table_name = ? AND bucket = ?')
            args = (table, int(bucket))
        with self.lock:
            count = self.conn.execute(cur_sql, args).rowcount
        if count == 0:
            raise KitError('No bucket matched %s%s' % (table, '' if bucket is None else ' bucket %d' % bucket))
        self.event('mark_recopy', '%s%s' % (table, '' if bucket is None else ' bucket %d' % bucket))
        if self.step_status('O6') == 'passed':
            self.step_end('O6', 'pending', 'buckets marked for re-copy; rerun O6')
        return count

    def record_chunk_counts(self, table, bucket, source_count, target_count):
        """Store the per-bucket COUNT(*) of both sides with the time of the count."""
        self._q('UPDATE chunks SET source_count = ?, target_count = ?, counted_utc = ? WHERE table_name = ? AND bucket = ?',
                (source_count, target_count, iso(utc_now()), table, bucket))

    def totals(self):
        """({state: (chunks, rows)}, tables_done)."""
        rows = self._q('SELECT state, COUNT(*), COALESCE(SUM(rows_inserted), 0) FROM chunks GROUP BY state')
        out = {state: (0, 0) for state in CHUNK_STATES}
        for state, n, rows_ in rows:
            out[state] = (n, rows_)
        tables_done = self._q("SELECT COUNT(*) FROM tables t WHERE NOT EXISTS "
                              "(SELECT 1 FROM chunks c WHERE c.table_name = t.table_name AND c.state != 'done')")[0][0]
        return out, tables_done

    # -- verification -----------------------------------------------------------
    def record_verification(self, table, tier, result, source_value=None, target_value=None, sql_source=None,
                            sql_target=None, detail=None):
        """Append one verification row; a result of 'match' also stamps the table's verified_at_utc. Returns the time."""
        now = iso(utc_now())
        self._q('INSERT INTO verification (table_name, tier, result, source_value, target_value, sql_source, sql_target, '
                'detail, checked_utc) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (table, tier, result, None if source_value is None else str(source_value),
                 None if target_value is None else str(target_value), sql_source, sql_target, detail, now))
        if result == 'match':
            self.set_table_attr(table, 'verified_at_utc', now)
        return now

    def verifications(self, table=None, tier=None):
        """Verification rows as dicts, filtered by table and tier when given."""
        sql = 'SELECT * FROM verification WHERE 1 = 1'
        args = []
        if table is not None:
            sql += ' AND table_name = ?'
            args.append(table)
        if tier is not None:
            sql += ' AND tier = ?'
            args.append(tier)
        return self._rows(sql + ' ORDER BY id', tuple(args))

    # -- attachments, grants, pipelines -------------------------------------------
    def record_attachment(self, role, database_name, storage_id=None, compute_id=None, attach_type=None):
        """Append an observed attachment row for the role source, replacement, retained or live."""
        if role not in ATTACHMENT_ROLES:
            raise KitError('Unknown attachment role ' + str(role))
        self._q('INSERT INTO attachments (role, database_name, storage_id, compute_id, attach_type, observed_utc) '
                'VALUES (?, ?, ?, ?, ?, ?)', (role, database_name, storage_id, compute_id, attach_type, iso(utc_now())))

    def attachment(self, role):
        """The most recent attachment row for a role, or None."""
        rows = self._rows('SELECT * FROM attachments WHERE role = ? ORDER BY id DESC LIMIT 1', (role,))
        return rows[0] if rows else None

    def snapshot_grant(self, grantee, privileges):
        """Store the privileges of one grantee ({scope: [privileges]}) unless a row already exists for it.

        A lockout rerun must never replace the original snapshot with the already-revoked state,
        so an existing row is kept as it is. Returns True when a row was written.
        """
        value = {k: sorted(v) for k, v in privileges.items()} if isinstance(privileges, dict) else sorted(privileges)
        with self.lock:
            cur = self.conn.execute('INSERT INTO grants_snapshot (grantee, privileges_json, captured_utc) VALUES (?, ?, ?) '
                                    'ON CONFLICT(grantee) DO NOTHING', (grantee, json.dumps(value, sort_keys=True), iso(utc_now())))
            return cur.rowcount == 1

    def mark_grant_restored(self, grantee):
        """Stamp the time the grantee's privileges were restored."""
        self._q('UPDATE grants_snapshot SET restored_utc = ? WHERE grantee = ?', (iso(utc_now()), grantee))

    def grants_snapshot(self):
        """Every snapshot row keyed by grantee, with privileges decoded to a list."""
        return {r['grantee']: dict(r, privileges=json.loads(r['privileges_json']))
                for r in self._rows('SELECT * FROM grants_snapshot ORDER BY grantee')}

    def upsert_pipeline(self, name, **fields):
        """Create the pipeline row when missing and set the given fields (PIPELINE_FIELDS only)."""
        unknown = set(fields) - set(PIPELINE_FIELDS)
        if unknown:
            raise KitError('Unknown pipeline fields: ' + ', '.join(sorted(unknown)))
        with self.lock:
            self.conn.execute('INSERT INTO pipelines (name) VALUES (?) ON CONFLICT(name) DO NOTHING', (name,))
            for key, value in fields.items():
                self.conn.execute('UPDATE pipelines SET %s = ? WHERE name = ?' % key, (value, name))

    def pipelines(self):
        """Every pipeline row as a dict, by name."""
        return self._rows('SELECT * FROM pipelines ORDER BY name')

    def record_offsets(self, pipeline_name, source_partition, phase, earliest=None, latest=None, loaded=None, successful=None):
        """Append one offsets row for a pipeline and source partition in phase capture, before_start or after_release."""
        if phase not in OFFSET_PHASES:
            raise KitError('Unknown offset phase ' + str(phase))
        self._q('INSERT INTO pipeline_offsets (pipeline_name, source_partition, phase, earliest, latest, loaded, successful, '
                'observed_utc) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (pipeline_name, str(source_partition), phase, earliest, latest, loaded, successful, iso(utc_now())))

    def offsets(self, phase, pipeline_name=None):
        """Offset rows of one phase, for one pipeline or for all."""
        if pipeline_name is None:
            return self._rows('SELECT * FROM pipeline_offsets WHERE phase = ? ORDER BY pipeline_name, source_partition, id',
                              (phase,))
        return self._rows('SELECT * FROM pipeline_offsets WHERE phase = ? AND pipeline_name = ? ORDER BY source_partition, id',
                          (phase, pipeline_name))
