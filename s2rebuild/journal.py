"""s2rebuild journal: the SQLite run journal, step and chunk transitions.

Author: John Bergamini

One journal per run per database (section 9 of the architecture): WAL mode, synchronous
FULL, every write behind one lock. The schema carries a schema_version row in meta. A journal
belongs to one run, so there is one schema: a journal written by an older kit is refused, never
upgraded (OLDER_KIT). A re-copy (mark_recopy) reopens the evidence built on
the copy: it increments the table's recopy_epoch, and every verification row carries the epoch
it verified. A mark, the start of O6 to O8 and the go decision of O10 (record_go) each read the
switch state in their own BEGIN IMMEDIATE transaction, so of a re-copy and a go only the first
to commit goes ahead. The attachments table holds the branch record per role (source,
replacement, retained, live): name, STORAGE_ID, COMPUTE_ID, the attachment type, the record type
(create, resume, pitr), the ATTACH_SYNTAX and whether it is detached; a row of either attachment
view completes the latest row of the same record. A verification row carries the sampling
predicate and the rows in the sample when tier 3 sampled.
Statements issued by this module: SQLite statements against the journal file only.
"""
import json
import sqlite3
import threading
from pathlib import Path

from .common import KitError, iso, utc_now

SCHEMA_VERSION = 4
OLDER_KIT = 'This journal was written by an older kit; finish or abandon that run with the kit version that started it.'
MEMORY = ':memory:'
STEP_STATUSES = ('pending', 'running', 'passed', 'failed', 'skipped')
CHUNK_STATES = ('pending', 'started', 'done', 'failed')
TABLE_FLAGS = ('created_in_target', 'definition_ok')
TABLE_ATTRS = ('size_gib', 'time_column', 'unique_key_lock_risk', 'concurrency', 'verified_at_utc')
ATTACHMENT_ROLES = ('source', 'replacement', 'retained', 'live')
RECORD_TYPES = ('create', 'resume', 'pitr')
OFFSET_PHASES = ('capture', 'before_start', 'after_release')
PIPELINE_FIELDS = ('state_at_capture', 'source_type', 'source_location', 'target', 'batch_interval',
                   'duplicate_handling', 'definition_file', 'app_managed', 'created_in_replacement_utc',
                   'started_utc', 'state_after_start')

# Column lists drive CREATE TABLE.
COLUMNS = {
    'meta': [('key', 'TEXT PRIMARY KEY'), ('value', 'TEXT NOT NULL')],
    'steps': [('step_id', 'TEXT PRIMARY KEY'), ('status', 'TEXT NOT NULL'), ('started_utc', 'TEXT'),
              ('ended_utc', 'TEXT'), ('note', 'TEXT'), ('evidence_path', 'TEXT')],
    'tables': [('table_name', 'TEXT PRIMARY KEY'), ('est_rows', 'INTEGER NOT NULL'), ('n_buckets', 'INTEGER NOT NULL'),
               ('key_kind', 'TEXT NOT NULL'), ('key_expr', 'TEXT NOT NULL'), ('columns_json', 'TEXT NOT NULL'),
               ('created_in_target', 'INTEGER NOT NULL DEFAULT 0'), ('definition_ok', 'INTEGER NOT NULL DEFAULT 0'),
               ('source_count', 'INTEGER'), ('target_count', 'INTEGER'), ('counted_utc', 'TEXT'),
               ('size_gib', 'REAL'), ('time_column', 'TEXT'), ('unique_key_lock_risk', 'INTEGER NOT NULL DEFAULT 0'),
               ('concurrency', 'INTEGER'), ('verified_at_utc', 'TEXT'), ('recopy_epoch', 'INTEGER NOT NULL DEFAULT 0')],
    'chunks': [('table_name', 'TEXT NOT NULL'), ('bucket', 'INTEGER NOT NULL'), ('state', 'TEXT NOT NULL'),
               ('rows_inserted', 'INTEGER'), ('started_utc', 'TEXT'), ('ended_utc', 'TEXT'),
               ('attempts', 'INTEGER NOT NULL DEFAULT 0'), ('error', 'TEXT'), ('source_count', 'INTEGER'),
               ('target_count', 'INTEGER'), ('counted_utc', 'TEXT')],
    'verification': [('id', 'INTEGER PRIMARY KEY AUTOINCREMENT'), ('table_name', 'TEXT NOT NULL'), ('tier', 'TEXT NOT NULL'),
                     ('result', 'TEXT NOT NULL'), ('source_value', 'TEXT'), ('target_value', 'TEXT'),
                     ('sql_source', 'TEXT'), ('sql_target', 'TEXT'), ('detail', 'TEXT'), ('checked_utc', 'TEXT NOT NULL'),
                     ('epoch', 'INTEGER'), ('predicate', 'TEXT'), ('sample_rows', 'INTEGER')],
    'attachments': [('id', 'INTEGER PRIMARY KEY AUTOINCREMENT'), ('role', 'TEXT NOT NULL'), ('database_name', 'TEXT NOT NULL'),
                    ('storage_id', 'TEXT'), ('compute_id', 'TEXT'), ('attach_type', 'TEXT'), ('observed_utc', 'TEXT NOT NULL'),
                    ('record_type', 'TEXT'), ('attach_syntax', 'TEXT'), ('is_detached', 'INTEGER')],
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
# Steps whose evidence a re-copy invalidates, with the note mark_recopy records when it reopens them.
RECOPY_REOPENS = (('O6', 'buckets of %s marked for re-copy; rerun O6'),
                  ('O7', 'counters and stale check predate the re-copy of %s; rerun O7 after O6'),
                  ('O8', 'verification predates the re-copy of %s; rerun O8 after O7'))


def _text(value):
    return None if value is None else str(value)


def _same(a, b):
    return a is None or b is None or str(a) == str(b)


def attachment_fields(row):
    """The record_attachment keyword arguments for a view row: either attachment view, or a joined record.

    ATTACH_TYPE is READONLY or READWRITE in MV_CLOUD_ATTACHED_DATABASES and create, resume or pitr in
    MV_CLOUD_BRANCHES; the row's value goes to the field it belongs to. COMPUTE_ID comes from
    COMPUTE_ID or LATEST_COMPUTE_ID; a joined record carries ACCESS for the attachment type.
    """
    kind = row.get('ATTACH_TYPE')
    kind = None if kind is None else str(kind)
    record_type = kind.lower() if kind and kind.lower() in RECORD_TYPES else row.get('RECORD_TYPE')
    attach_type = kind.upper() if kind and kind.lower() not in RECORD_TYPES else row.get('ACCESS')
    detached = row.get('IS_DETACHED')
    return {'database_name': str(row.get('DATABASE_NAME')), 'storage_id': row.get('STORAGE_ID'),
            'compute_id': row.get('COMPUTE_ID') if row.get('COMPUTE_ID') is not None else row.get('LATEST_COMPUTE_ID'),
            'attach_type': attach_type, 'record_type': record_type, 'attach_syntax': row.get('ATTACH_SYNTAX'),
            'is_detached': None if detached is None else int(str(detached))}


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

    A new file gets every table and the schema version; an existing journal must carry this
    schema version (check_schema), else it is refused with OLDER_KIT. read_only opens an existing
    journal without writing (status). Every sqlite failure while opening becomes a KitError that
    names the path.
    """

    def __init__(self, path, read_only=False):
        self.path = str(path)
        self.lock = threading.RLock()
        try:
            if read_only:
                self.conn = _open_read_only(self.path, check_same_thread=False, isolation_level=None)
                self.check_schema()
            else:
                if self.path != MEMORY:
                    Path(self.path).parent.mkdir(parents=True, exist_ok=True)
                self.conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
                if self._existing_tables():
                    try:
                        self.check_schema()  # before the WAL pragma, so a refused journal is left byte for byte as it was
                    except KitError:
                        self.conn.close()
                        raise
                self.conn.execute('PRAGMA journal_mode=WAL')
                self.conn.execute('PRAGMA synchronous=FULL')
                self._create_or_check()
        except sqlite3.DatabaseError as exc:
            raise _open_error(self.path, exc)

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
                journal.check_schema()
            except sqlite3.DatabaseError as exc:
                journal.close()
                raise _open_error(path, exc)
        return journal

    # -- schema -------------------------------------------------------------
    def _existing_tables(self):
        return {r[0] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    def _existing_columns(self, table):
        return [r[1] for r in self.conn.execute('PRAGMA table_info(%s)' % table)]

    def _create_or_check(self):
        """A new file: every table of section 9 and the schema version, in one transaction. An existing journal: check_schema."""
        with self.lock:
            if self._existing_tables():
                self.check_schema()
                return
            self.conn.execute('BEGIN')
            try:
                for name in COLUMNS:
                    self.conn.execute(create_table_sql(name))
                self.conn.execute("INSERT INTO meta (key, value) VALUES ('schema_version', ?)", (str(SCHEMA_VERSION),))
                self.conn.execute('COMMIT')
            except Exception:
                self.conn.execute('ROLLBACK')
                raise

    def check_schema(self):
        """Refuse a journal of another schema: a newer one names the version, an older one (or one without every table) gets OLDER_KIT."""
        tables = self._existing_tables()
        row = self.conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone() if 'meta' in tables else None
        version = int(row[0]) if row else 0
        if version > SCHEMA_VERSION:
            raise KitError('Journal schema version %d is newer than this kit supports (%d); upgrade the kit' % (version, SCHEMA_VERSION))
        if version < SCHEMA_VERSION or set(COLUMNS) - tables:
            raise KitError('%s (%s)' % (OLDER_KIT, self.path))

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

    def accept_config_hash(self, old, new, note):
        """Record an accepted configuration change: a config_change event with both hashes and the note, then new as config_hash.

        One BEGIN IMMEDIATE transaction, so a crash part way leaves the old hash and no event. Refused,
        with nothing written, when the recorded hash is no longer old (another process accepted first).
        """
        with self.lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                recorded = self.get_meta('config_hash')
                if recorded != old:
                    raise KitError('The configuration hash of %s changed meanwhile (now %s); run the command again' % (
                        self.path, (recorded or 'none')[:12]))
                self.event('config_change', 'configuration hash %s replaced by %s after O1: %s' % (old, new, note))
                self.set_meta('config_hash', new)
                self.conn.execute('COMMIT')
            except BaseException:
                self.conn.execute('ROLLBACK')
                raise

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

    def step_start(self, step_id, note=None, unless=None):
        """Mark a step running. Clears the previous end time and keeps earlier evidence.

        unless, when given, is called inside the same BEGIN IMMEDIATE transaction before the write
        (run_step passes switch_began for O6 to O8). When it returns a reason, nothing is written and
        the reason is returned. So a go decision committed by another process is never missed, and
        a step started first is seen running by the go (record_go). Returns None when started.
        """
        if unless is None:
            self._step_start(step_id, note)
            return None
        with self.lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                reason = unless()
                if not reason:
                    self._step_start(step_id, note)
                self.conn.execute('COMMIT')
            except BaseException:
                self.conn.execute('ROLLBACK')
                raise
        return reason or None

    def _step_start(self, step_id, note):
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
        """Refuse to resume with a journal that belongs to another pair or another chunk plan."""
        recorded_target = self.get_meta('replacement_db')
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

    def mark_done(self, table, bucket, rows, error=None):
        """Record a finished chunk with the rows the INSERT reported; error, when given, tags how it was loaded (the fresh-table load)."""
        if error is not None:
            self._q("UPDATE chunks SET state = 'done', rows_inserted = ?, ended_utc = ?, error = ? WHERE table_name = ? AND bucket = ?",
                    (rows, iso(utc_now()), str(error)[:2000], table, bucket))
            return
        self._q("UPDATE chunks SET state = 'done', rows_inserted = ?, ended_utc = ? WHERE table_name = ? AND bucket = ?",
                (rows, iso(utc_now()), table, bucket))

    def mark_loaded(self, table, n_buckets, rows, error, meta_key, meta_value):
        """Record a whole-table load (the fresh-table load) in one BEGIN IMMEDIATE transaction.

        Every bucket of table is marked started, then done with error as its tag (bucket 0 with
        rows, the others with 0), then meta_key is set to meta_value. A crash part way leaves the
        journal as it was, so the meta value never reads done beside a bucket that is not.
        """
        with self.lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                for b in range(n_buckets):
                    self.mark_started(table, b)
                    self.mark_done(table, b, rows if b == 0 else 0, error=error)
                self.set_meta(meta_key, meta_value)
                self.conn.execute('COMMIT')
            except BaseException:
                self.conn.execute('ROLLBACK')
                raise

    def undo_attempt(self, table, bucket):
        """A lock wait does not count against the attempt limit."""
        self._q('UPDATE chunks SET attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END '
                'WHERE table_name = ? AND bucket = ?', (table, bucket))

    def mark_failed(self, table, bucket, error):
        """Record a failed attempt with the redacted error text (cut to 2000 characters)."""
        self._q("UPDATE chunks SET state = 'failed', ended_utc = ?, error = ? WHERE table_name = ? AND bucket = ?",
                (iso(utc_now()), str(error)[:2000], table, bucket))

    def has_table(self, table):
        """True when table is a planned table of this journal."""
        return bool(self._q('SELECT 1 FROM tables WHERE table_name = ?', (table,)))

    def check_recopy(self, table, bucket=None):
        """Refuse a re-copy mark before anything is written: a table not in this journal, a bucket not in the table, or a switch
        that may have begun (switch_began). mark_recopy runs it inside its own transaction, so a go decision committed by another
        process just before is seen; mark-recopy also runs it on every selected journal before it marks any of them. Reads the
        journal only."""
        if not self.has_table(table):
            raise KitError('Table %s is not in this journal' % table)
        began = self.switch_began()
        if began:
            raise KitError('mark-recopy refused for %s: the switch may have begun (%s); a re-copy now would change the copy behind the '
                           'live name and clear the target counts the rollback proof compares; nothing was marked. Finish the switch or '
                           'roll it back; a new copy is a new run' % (table, began))
        if bucket is not None and not self._q('SELECT 1 FROM chunks WHERE table_name = ? AND bucket = ?', (table, int(bucket))):
            raise KitError('No bucket matched %s bucket %d' % (table, int(bucket)))

    def mark_recopy(self, table, bucket=None):
        """Set buckets to failed with attempts 0 so the next copy run deletes and re-inserts them; reopen what the copy proved.

        One BEGIN IMMEDIATE transaction: check_recopy, the chunk rows, the mark_recopy event and
        _reopen_evidence (the table's verified_at_utc and target_count cleared, its recopy_epoch
        incremented, O6, O7 and O8 set to pending when they had passed). A crash part way leaves
        the journal as it was. Refused, with nothing written, by check_recopy (once the switch may
        have begun, among others); since the check reads inside the transaction, a mark and the go
        decision of O10 (record_go) are ordered: whichever commits second is refused.
        Returns the buckets marked.
        """
        if bucket is None:
            cur_sql = ("UPDATE chunks SET state = 'failed', attempts = 0, error = 'marked for re-copy', ended_utc = NULL "
                       'WHERE table_name = ?')
            args = (table,)
        else:
            cur_sql = ("UPDATE chunks SET state = 'failed', attempts = 0, error = 'marked for re-copy', ended_utc = NULL "
                       'WHERE table_name = ? AND bucket = ?')
            args = (table, int(bucket))
        with self.lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                self.check_recopy(table, bucket)
                count = self.conn.execute(cur_sql, args).rowcount
                if count == 0:
                    raise KitError('No bucket matched %s%s' % (table, '' if bucket is None else ' bucket %d' % bucket))
                self.event('mark_recopy', '%s%s' % (table, '' if bucket is None else ' bucket %d' % bucket))
                self._reopen_evidence(table)
                self.conn.execute('COMMIT')
            except BaseException:
                self.conn.execute('ROLLBACK')
                raise
        return count

    def reopen_evidence(self, table, reason):
        """Clear the evidence built on the rows of table before they are replaced another way (the fresh-table load).

        One transaction: an evidence_reopened event with the reason, then _reopen_evidence. A crash
        part way leaves the journal as it was.
        """
        with self.lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                self.event('evidence_reopened', '%s: %s' % (table, reason))
                self._reopen_evidence(table)
                self.conn.execute('COMMIT')
            except BaseException:
                self.conn.execute('ROLLBACK')
                raise

    def _reopen_evidence(self, table):
        """Inside the caller's transaction: target_count and verified_at_utc of table cleared, recopy_epoch + 1, and O6, O7 and
        O8 set to pending when passed or skipped, each note naming the table; a running or failed step keeps its status."""
        self.conn.execute('UPDATE tables SET target_count = NULL, recopy_epoch = recopy_epoch + 1 WHERE table_name = ?', (table,))
        self.set_table_attr(table, 'verified_at_utc', None)
        for step_id, note in RECOPY_REOPENS:
            if self.step_status(step_id) in ('passed', 'skipped'):
                self.step_end(step_id, 'pending', note % table)

    def record_go(self, detail, check):
        """Record the go decision of O10 (the switch_go event) in one BEGIN IMMEDIATE transaction with its last check.

        check(journal) returns the problems that refuse the go, read inside the transaction. A
        re-copy mark or a step start that another process committed first is seen, and one that
        commits after sees the go, since mark_recopy and run_step read switch_began in their own
        transactions. With a problem left nothing is written and KitError names every problem.
        """
        with self.lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                problems = list(check(self))
                if problems:
                    raise KitError('S0 refused at the go decision: %s; nothing was issued' % '; '.join(problems))
                self.event('switch_go', detail)
                self.conn.execute('COMMIT')
            except BaseException:
                self.conn.execute('ROLLBACK')
                raise

    def switch_began(self):
        """Why the switch may have begun, or None: O10 passed, the go decision recorded, an O10 gate recorded (pass or fail),
        a switch observed (switch_observed: in the evidence of a refused gate O10 --pass, or in the position O10 read before its
        go), a switched, rollback or release time recorded, or the retained or live attachment recorded.

        From then on the copy is what the live name resolves to and target_count is what the rollback
        proof compares, so mark_recopy and O6 to O8 refuse. The kit route records the go decision
        (record_go) before its first statement, so an O10 that stopped before the go (a Ctrl-C at the
        typed-name prompt, a lost owner connection, a stop during S0) issued nothing and does not count,
        even when it is failed or still running. Every O10 refusal that issues nothing comes before the
        go (S0, the S1 before-checks, the go transaction), so it
        does not count either. While it waits for the go, O10 holds the copy lock and
        record_go reads S0's journal part again, so a re-copy in that window is refused or refuses the
        go. A gate is how a person reports a hand or notebook O10, so any O10 gate counts. Reads the
        journal only.
        """
        if self.step_status('O10') == 'passed':
            return 'O10 is passed'
        go = self.events('switch_go')
        if go:
            return 'the go decision was recorded: %s' % go[-1]
        gates = [d for d in self.events('gate') if d.startswith('O10 ')]
        if gates:
            return 'gate O10 --%s was recorded' % gates[-1].split(' ')[1].rstrip(':')
        observed = self.events('switch_observed')
        if observed:
            return 'a switch was observed: %s' % observed[-1]
        for key in ('switched_utc', 'rollback_started_utc', 'rolled_back_utc', 'release_started_utc', 'released_utc'):
            if self.get_meta(key):
                return '%s is %s' % (key, self.get_meta(key))
        for role in ('retained', 'live'):
            if self.attachment(role):
                return 'the %s attachment is recorded' % role
        return None

    def recopy_epoch(self, table):
        """The table's re-copy epoch: 0 until mark_recopy first marks it, then one more per mark."""
        rows = self._q('SELECT recopy_epoch FROM tables WHERE table_name = ?', (table,))
        return int(rows[0][0] or 0) if rows else 0

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
                            sql_target=None, detail=None, predicate=None, sample_rows=None):
        """Append one verification row at the table's current recopy_epoch; a result of 'match' also stamps verified_at_utc.

        predicate is the WHERE text of a sampled fingerprint and sample_rows the source rows it
        covered; both stay NULL for a full fingerprint. Returns the time of the row.
        """
        now = iso(utc_now())
        self._q('INSERT INTO verification (table_name, tier, result, source_value, target_value, sql_source, sql_target, '
                'detail, checked_utc, epoch, predicate, sample_rows) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '(SELECT recopy_epoch FROM tables WHERE table_name = ?), ?, ?)',
                (table, tier, result, None if source_value is None else str(source_value),
                 None if target_value is None else str(target_value), sql_source, sql_target, detail, now, table, predicate,
                 None if sample_rows is None else int(sample_rows)))
        if result == 'match':
            self.set_table_attr(table, 'verified_at_utc', now)
        return now

    def verification_state(self):
        """Per planned table, by name: verified_at_utc, recopy_epoch, the chunks not done, the newest chunk ended_utc,
        and epoch_verified (1 when a match or sampled verification row carries the current recopy_epoch).

        A version 1 journal opened read only (owner-sql) has no epoch columns: every table reads
        epoch 0 and not epoch_verified, as after its upgrade, where the old rows carry no epoch.
        """
        if 'recopy_epoch' not in self._existing_columns('tables') or 'epoch' not in self._existing_columns('verification'):
            epoch, verified = '0 AS recopy_epoch', '0 AS epoch_verified'
        else:
            epoch = 't.recopy_epoch'
            verified = ("EXISTS (SELECT 1 FROM verification v WHERE v.table_name = t.table_name AND v.epoch = t.recopy_epoch "
                        "AND v.result IN ('match', 'sampled')) AS epoch_verified")
        return self._rows(
            'SELECT t.table_name, t.verified_at_utc, %s, ' % epoch +
            "(SELECT COUNT(*) FROM chunks c WHERE c.table_name = t.table_name AND c.state != 'done') AS chunks_open, "
            '(SELECT MAX(c.ended_utc) FROM chunks c WHERE c.table_name = t.table_name) AS last_chunk_end, '
            '%s FROM tables t ORDER BY t.table_name' % verified)

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
    def record_attachment(self, role, database_name, storage_id=None, compute_id=None, attach_type=None, record_type=None, attach_syntax=None,
                          is_detached=None):
        """Append an observed attachment row for the role source, replacement, retained or live.

        attach_type is READONLY or READWRITE (MV_CLOUD_ATTACHED_DATABASES); record_type is the
        branch record's create, resume or pitr, attach_syntax its ATTACH_SYNTAX and is_detached
        its IS_DETACHED (MV_CLOUD_BRANCHES). A field left None is filled from the latest row of
        the same role when that row names the same database, STORAGE_ID and COMPUTE_ID, so the
        two views' rows of one record complete each other.
        """
        if role not in ATTACHMENT_ROLES:
            raise KitError('Unknown attachment role ' + str(role))
        if record_type is not None and str(record_type) not in RECORD_TYPES:
            raise KitError('Unknown branch record type %s (create, resume or pitr)' % record_type)
        values = {'attach_type': attach_type, 'record_type': record_type, 'attach_syntax': attach_syntax, 'is_detached': is_detached}
        previous = self.attachment(role)
        if previous and previous.get('database_name') == database_name and _same(previous.get('storage_id'), storage_id) \
                and _same(previous.get('compute_id'), compute_id):
            for key, value in values.items():
                if value is None:
                    values[key] = previous.get(key)
        self._q('INSERT INTO attachments (role, database_name, storage_id, compute_id, attach_type, observed_utc, record_type, attach_syntax, is_detached) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', (role, database_name, _text(storage_id), _text(compute_id), values['attach_type'], iso(utc_now()),
                                                      values['record_type'], values['attach_syntax'],
                                                      None if values['is_detached'] is None else int(values['is_detached'])))

    def record_attachment_row(self, role, row):
        """Record a row of MV_CLOUD_ATTACHED_DATABASES, of MV_CLOUD_BRANCHES, or a joined record (switch.identify) for a role."""
        self.record_attachment(role, **attachment_fields(row))

    def attachment(self, role):
        """The most recent attachment row for a role, or None."""
        rows = self._rows('SELECT * FROM attachments WHERE role = ? ORDER BY id DESC LIMIT 1', (role,))
        return rows[0] if rows else None

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
