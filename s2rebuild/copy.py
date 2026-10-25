"""s2rebuild copy: create the replacement (O5) and copy every table in hash-bucket chunks (O6).

Author: John Bergamini

The reference runner generalized (section 10.9 of the architecture). Kept as it was: hash-bucket
predicates CRC32(key_expr) MOD B = k, server-side INSERT INTO replacement SELECT FROM source,
journal transitions committed before each statement, delete-then-reinsert reconciliation of a
started or failed bucket, the write guard, the redactor, the abort deadline, the stop file, lock
waits requeued without consuming an attempt, the definition diff that ignores only the
AUTO_INCREMENT counter, the refusal of a replacement holding rows the journal cannot explain,
and the count proof at the end. New: names and sizes from config, the bucket cap, a queue per
table with per-table concurrency and automatic serialization after a lock wait, the lock probe,
the fresh-table fallback and per-chunk timings for calibrate.

Statements issued, every one through the runner guard naming the replacement: CREATE DATABASE,
CREATE TABLE LIKE, SHOW CREATE TABLE, INSERT INTO replacement SELECT FROM source, DELETE FROM
replacement (reconciliation and lock probe), SELECT COUNT(*), SET lock_wait_timeout and, for the
fresh-table fallback, CREATE TABLE LIKE, INSERT SELECT without a predicate, DROP TABLE and
ALTER TABLE RENAME TO.
"""
import collections
import contextlib
import datetime as dt
import fcntl
import os
import threading
import time
import traceback
from pathlib import Path

from . import estimate, inventory, steps, transform
from .common import EXIT_INCOMPLETE, EXIT_MISMATCH, EXIT_REFUSED, KitError, iso, parse_utc, utc_now
from .sqlutil import ident, qualified, runner_guard

DURABILITY_FLAGS = ('IS_SYNC', 'IS_SYNC_DURABILITY', 'IS_SYNC_DURABILITY_ON_REF_DB_ONLY')
LOCK_WAIT_ERRNO = 1205
FRESH_SUFFIX = '__fresh'


@contextlib.contextmanager
def copy_lock(journal_path):
    """Hold <journal>.lock exclusively while a copy runs; refuse when another process holds it.

    Two runners on one journal would insert every pending bucket twice. The lock file carries
    the holder's pid. An in-memory journal (no path) takes no lock.
    """
    if not journal_path:
        yield None
        return
    path = Path(str(journal_path) + '.lock')
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            holder = ''
            try:
                holder = os.read(fd, 64).decode('utf8', 'replace').strip()
            except OSError:
                pass
            raise KitError('Another s2rebuild process holds the copy lock for this journal%s; wait for it to finish before rerunning O6' % (
                (' (pid %s)' % holder) if holder else ''))
        os.pwrite(fd, ('%-24d' % os.getpid()).encode('ascii'), 0)
        yield path
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def is_lock_wait(exc):
    """True for error 1205 (lock wait timeout) however the driver wraps it."""
    args = getattr(exc, 'args', ())
    return (bool(args) and args[0] == LOCK_WAIT_ERRNO) or 'Lock wait timeout' in str(exc)


def one_value(rows, key='n'):
    """The single integer a COUNT or MAX query returned (dict or tuple row)."""
    if not rows:
        raise KitError('Query returned no rows')
    row = rows[0]
    value = row[key] if isinstance(row, dict) else row[0]
    return int(value)


class Statements:
    """Every statement text the copy issues, built from the two database names and the target layout."""

    def __init__(self, source_db, target_db, partitions, sub_partitions):
        ident(source_db)
        ident(target_db)
        if source_db == target_db:
            raise KitError('Source and replacement must differ')
        self.source_db = source_db
        self.target_db = target_db
        self.partitions = int(partitions)
        self.sub_partitions = int(sub_partitions)

    def layout_query(self):
        """DISTRIBUTED_DATABASES row for one database (parameter: the name)."""
        return inventory.LAYOUT_SQL

    def tables_query(self):
        """Base table names of one schema (parameter: the name)."""
        return ("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'")

    def create_database(self):
        """CREATE DATABASE with the target layout."""
        return 'CREATE DATABASE %s PARTITIONS %d SUB_PARTITIONS %d' % (ident(self.target_db), self.partitions, self.sub_partitions)

    def create_table_like(self, table):
        """CREATE TABLE replacement.table LIKE source.table."""
        return 'CREATE TABLE %s LIKE %s' % (qualified(self.target_db, table), qualified(self.source_db, table))

    def show_create(self, table, db=None):
        """SHOW CREATE TABLE in the replacement (or db)."""
        return 'SHOW CREATE TABLE ' + qualified(db or self.target_db, table)

    def insert_chunk(self, plan, bucket):
        """INSERT INTO replacement SELECT FROM source for one bucket."""
        cols = plan.column_list()
        return 'INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s' % (
            qualified(self.target_db, plan.name), cols, cols, qualified(self.source_db, plan.name), plan.predicate(bucket))

    def delete_chunk(self, plan, bucket):
        """DELETE FROM replacement for one bucket (reconciliation and lock probe)."""
        return 'DELETE FROM %s WHERE %s' % (qualified(self.target_db, plan.name), plan.predicate(bucket))

    def count(self, db, table, predicate=None):
        """SELECT COUNT(*) with an optional predicate."""
        sql = 'SELECT COUNT(*) AS n FROM ' + qualified(db, table)
        if predicate:
            sql += ' WHERE ' + predicate
        return sql

    def set_lock_wait(self, seconds):
        """SET lock_wait_timeout for the session."""
        return 'SET lock_wait_timeout = %d' % int(seconds)

    def read_lock_wait(self):
        """SELECT the current lock_wait_timeout."""
        return 'SELECT @@lock_wait_timeout AS v'

    def create_fresh(self, plan):
        """CREATE TABLE replacement.table__fresh LIKE source.table."""
        return 'CREATE TABLE %s LIKE %s' % (qualified(self.target_db, plan.name + FRESH_SUFFIX), qualified(self.source_db, plan.name))

    def insert_all(self, plan):
        """INSERT INTO the fresh table SELECT every row of the source table."""
        cols = plan.column_list()
        return 'INSERT INTO %s (%s) SELECT %s FROM %s' % (qualified(self.target_db, plan.name + FRESH_SUFFIX), cols, cols,
                                                          qualified(self.source_db, plan.name))

    def drop_table(self, table):
        """DROP TABLE in the replacement (fresh-table fallback only)."""
        return 'DROP TABLE ' + qualified(self.target_db, table)

    def rename_fresh(self, plan):
        """ALTER TABLE the fresh table RENAME TO the planned name."""
        return 'ALTER TABLE %s RENAME TO %s' % (qualified(self.target_db, plan.name + FRESH_SUFFIX), ident(plan.name))


def statements_for(db_spec):
    """Statements for a DatabaseSpec (names and layout from config)."""
    return Statements(db_spec.source, db_spec.replacement, db_spec.layout.partitions, db_spec.layout.sub_partitions)


def layout_of(session, statements, db_name):
    """The DISTRIBUTED_DATABASES row of a database as ints, or None when it is not reported."""
    rows = session.query(statements.layout_query(), (db_name,))
    if len(rows) != 1:
        return None
    row = rows[0]
    items = row.items() if isinstance(row, dict) else zip(('NUM_PARTITIONS', 'NUM_SUB_PARTITIONS') + DURABILITY_FLAGS, row)
    return {k: int(v) for k, v in items}


# ---------------------------------------------------------------------------
# O5: create the replacement
# ---------------------------------------------------------------------------
def check_source(session, statements, log, expected=None):
    """The source must be reported; when expected (the capture's layout) is given, it must still match."""
    layout = layout_of(session, statements, statements.source_db)
    if layout is None:
        raise KitError('Source database %s is not reported by the workspace; refusing to start' % statements.source_db)
    if expected:
        for key in ('NUM_PARTITIONS',) + DURABILITY_FLAGS:
            if key in expected and int(expected[key]) != layout.get(key):
                raise KitError('Source %s differs from the capture (%s was %s, now %s); refusing to start' % (
                    statements.source_db, key, expected[key], layout.get(key)))
    log('Source %s reported: partitions=%d sub_partitions=%d durability=%s' % (
        statements.source_db, layout['NUM_PARTITIONS'], layout['NUM_SUB_PARTITIONS'], ','.join(str(layout[f]) for f in DURABILITY_FLAGS)))
    return layout


def ensure_target_database(session, statements, journal, log, source_layout):
    """Create the replacement with the target layout when missing; verify layout and durability afterwards."""
    layout = layout_of(session, statements, statements.target_db)
    if layout is None:
        log('Creating replacement database ' + statements.target_db)
        session.execute(statements.create_database())
        journal.event('create_database', statements.target_db)
        layout = layout_of(session, statements, statements.target_db)
        if layout is None:
            raise KitError('Replacement database not reported after creation')
    if layout['NUM_PARTITIONS'] != statements.partitions or layout['NUM_SUB_PARTITIONS'] != statements.sub_partitions:
        raise KitError('Replacement layout is %d partitions / %d sub-partitions, not the configured %d / %d; stopping' % (
            layout['NUM_PARTITIONS'], layout['NUM_SUB_PARTITIONS'], statements.partitions, statements.sub_partitions))
    for flag in DURABILITY_FLAGS:
        if layout.get(flag) != source_layout.get(flag):
            raise KitError('Replacement durability flag %s differs from the source; stopping' % flag)
    log('Replacement %s verified: partitions=%d sub_partitions=%d durability matches source' % (
        statements.target_db, layout['NUM_PARTITIONS'], layout['NUM_SUB_PARTITIONS']))
    return layout


def existing_target_tables(session, statements):
    """Base table names already present in the replacement."""
    rows = session.query(statements.tables_query(), (statements.target_db,))
    return {(r['TABLE_NAME'] if isinstance(r, dict) else r[0]) for r in rows}


def ensure_tables(session, statements, journal, log, plans, target_spec=None):
    """CREATE TABLE LIKE for every planned table that is missing, then compare each definition with the capture.

    The comparison goes through transform.apply (a pass-through in this pass) and ignores only
    the AUTO_INCREMENT counter. The first difference stops the step.
    """
    existing = existing_target_tables(session, statements)
    unknown = existing - {p.name for p in plans}
    if unknown:
        raise KitError('Replacement holds tables that are not in the plan: ' + ', '.join(sorted(unknown)))
    created = 0
    targets = []
    for p in plans:
        if p.name not in existing:
            session.execute(statements.create_table_like(p.name))
            created += 1
        journal.set_table_flag(p.name, 'created_in_target')
        rows = session.query(statements.show_create(p.name))
        if not rows:
            raise KitError('SHOW CREATE TABLE returned nothing for ' + p.name)
        row = rows[0]
        actual = row['Create Table'] if isinstance(row, dict) else row[1]
        target = transform.apply(p, target_spec)
        targets.append(target)
        if inventory.normalize_definition(actual) != inventory.normalize_definition(target.target):
            journal.event('definition_mismatch', p.name)
            raise KitError('Definition of %s in the replacement differs from the captured definition; stopping. '
                           'Review the live source schema against the capture before continuing.' % p.name)
        counter = inventory.auto_increment_counter(p.definition)
        if counter is not None:
            journal.event('auto_increment_counter', '%s saved AUTO_INCREMENT=%d; the counter is set at O7' % (p.name, counter))
        journal.set_table_flag(p.name, 'definition_ok')
    log('Tables verified: %d created now, %d already present, all %d definitions match the capture' % (
        created, len(plans) - created, len(plans)))
    return targets


def check_target_rows_accounted(session, statements, journal, plans):
    """Refuse a replacement that holds rows the journal cannot explain."""
    problems = []
    for p in plans:
        target_rows = one_value(session.query(statements.count(statements.target_db, p.name)))
        if target_rows == 0:
            continue
        expected = 0
        for _, bucket, state, rows_inserted, _ in journal.chunk_states(p.name):
            if state == 'done':
                expected += int(rows_inserted or 0)
            elif state in ('started', 'failed'):
                expected += one_value(session.query(statements.count(statements.target_db, p.name, p.predicate(bucket))))
        if expected != target_rows:
            problems.append('%s holds %d rows but the journal accounts for %d' % (p.name, target_rows, expected))
    if problems:
        raise KitError('Replacement holds rows the journal does not account for; refusing to run. ' + '; '.join(problems))


def init_or_check_plan(journal, plans, statements, log, inventory_dir=None):
    """Write the chunk plan into an empty journal, or verify a resumed journal matches it."""
    if journal.is_empty():
        journal.init_plan(plans, statements.source_db, statements.target_db, inventory_dir)
        log('Journal initialised with %d tables and %d buckets' % (len(plans), sum(p.n_buckets for p in plans)))
    else:
        journal.check_plan(plans, statements.source_db, statements.target_db)
        log('Journal matches the plan; resuming')


def create_replacement(session, statements, journal, log, plans, source_layout_expected=None, target_spec=None, inventory_dir=None):
    """The O5 body: source check, plan in the journal, replacement database, tables, definitions, row accounting."""
    source_layout = check_source(session, statements, log, source_layout_expected)
    init_or_check_plan(journal, plans, statements, log, inventory_dir)
    ensure_target_database(session, statements, journal, log, source_layout)
    targets = ensure_tables(session, statements, journal, log, plans, target_spec)
    check_target_rows_accounted(session, statements, journal, plans)
    return targets


# ---------------------------------------------------------------------------
# O6: the copy runner
# ---------------------------------------------------------------------------
class Runner:
    """Copies the planned chunks with a pool of workers, one connection each (section 10.9).

    A queue per table in plan order (largest first); a worker takes the next chunk of the first
    table whose in-flight count is below its limit. A lock wait (error 1205) does not consume an
    attempt: the chunk goes to the back of its table's queue, the table's limit becomes 1 when
    table_concurrency is auto, and the table is parked for lock_retry_seconds while the other
    tables continue. Before a chunk on a table that has waited for a lock, the reconciliation
    DELETE runs as a probe with lock_wait_timeout = lock_probe_timeout_seconds.
    """

    def __init__(self, plans, statements, journal, connect, log, copy_config, workers=None, abort_at=None, stop_file=None,
                 after_insert_hook=None, retry_failed=False, sleep=time.sleep, clock=time.monotonic):
        self.plans = plans
        self.by_name = {p.name: p for p in plans}
        self.st = statements
        self.journal = journal
        self.connect = connect
        self.log = log
        self.cfg = copy_config
        self.workers = max(1, int(workers or copy_config.workers))
        self.abort_at = abort_at
        self.stop_file = Path(stop_file) if stop_file else None
        self.after_insert_hook = after_insert_hook
        self.retry_failed = retry_failed
        self.sleep = sleep
        self.clock = clock
        self.lock = threading.Lock()
        self.stop_launching = threading.Event()
        self.queues = collections.OrderedDict((p.name, collections.deque()) for p in plans)
        self.limits = {}
        self.configured = {p.name for p in plans if p.concurrency}
        for p in plans:
            if p.concurrency:
                self.limits[p.name] = int(p.concurrency)
            elif copy_config.table_concurrency == 1:
                self.limits[p.name] = 1
        self.in_flight = {p.name: 0 for p in plans}
        self.peak_in_flight = {p.name: 0 for p in plans}
        self.parked_until = {}
        self.lock_waits = {p.name: 0 for p in plans}
        self.started_at = None
        self.rows_since_start = 0
        self.failed_this_run = 0
        self.connect_failures = 0
        self.interrupted = False
        self._seed_from_journal()

    def _seed_from_journal(self):
        """A table serialized after a lock wait in an earlier run stays serialized and probed on resume."""
        if self.cfg.table_concurrency != 'auto':
            return
        try:
            rows = self.journal.tables()
        except Exception:  # noqa: BLE001 - a journal without a plan yet has nothing to seed
            return
        for row in rows:
            name = row.get('table_name')
            if name in self.by_name and name not in self.configured and row.get('concurrency') == 1:
                self.limits[name] = 1
                self.lock_waits[name] = 1

    # -- queue ------------------------------------------------------------------
    def load_pending(self):
        """Fill the queues from the journal: pending, started and failed chunks (parked failures need retry_failed)."""
        total = 0
        for p in self.plans:
            for _, bucket, state, _, attempts in self.journal.chunk_states(p.name):
                if state == 'done':
                    continue
                if state == 'failed' and attempts >= self.cfg.max_attempts and not self.retry_failed:
                    continue
                self.queues[p.name].append((bucket, state))
                total += 1
        return total

    def remaining(self):
        """Chunks queued or in flight."""
        with self.lock:
            return sum(len(q) for q in self.queues.values()) + sum(self.in_flight.values())

    def should_stop_launching(self):
        """True after the abort deadline, when the stop file exists, or once a stop was requested."""
        if self.stop_launching.is_set():
            return True
        if self.abort_at is not None and utc_now() >= self.abort_at:
            self.log('Abort deadline reached; no new chunks will start')
            self.stop_launching.set()
            return True
        if self.stop_file is not None and self.stop_file.exists():
            self.log('Stop file present; no new chunks will start')
            self.stop_launching.set()
            return True
        return False

    def take(self):
        """The next (table, bucket, prior_state) a worker may run, or None when nothing is runnable now."""
        now = self.clock()
        with self.lock:
            for name, queue in self.queues.items():
                if not queue:
                    continue
                if self.parked_until.get(name, 0) > now:
                    continue
                limit = self.limits.get(name, self.workers)
                if self.in_flight[name] >= limit:
                    continue
                bucket, prior = queue.popleft()
                self.in_flight[name] += 1
                self.peak_in_flight[name] = max(self.peak_in_flight[name], self.in_flight[name])
                return name, bucket, prior
        return None

    def release(self, name):
        """Return a table's in-flight slot."""
        with self.lock:
            self.in_flight[name] -= 1

    def requeue_after_lock(self, name, bucket):
        """Put the chunk back (as failed, so it is reconciled), serialize the table in auto mode and park it."""
        with self.lock:
            self.queues[name].append((bucket, 'failed'))
            self.lock_waits[name] += 1
            if self.cfg.table_concurrency == 'auto' and name not in self.configured and self.limits.get(name, self.workers) != 1:
                self.limits[name] = 1
                self.journal.set_table_attr(name, 'concurrency', 1)
                self.journal.event('serialized', '%s limited to one chunk at a time after a lock wait' % name)
            self.parked_until[name] = self.clock() + self.cfg.lock_retry_seconds
            self.journal.event('park', '%s parked for %d s after a lock wait' % (name, self.cfg.lock_retry_seconds))

    # -- one chunk ----------------------------------------------------------------
    def run_chunk(self, session, table, bucket, prior_state):
        """Reconcile when needed (with the lock probe on a table that has waited), then insert the bucket."""
        p = self.by_name[table]
        self.journal.mark_started(table, bucket)
        probe = self.lock_waits[table] > 0
        if prior_state in ('started', 'failed') or probe:
            if probe:
                session.execute(self.st.set_lock_wait(self.cfg.lock_probe_timeout_seconds))
            try:
                removed = session.execute(self.st.delete_chunk(p, bucket))
            finally:
                if probe:
                    try:
                        session.execute(self.st.set_lock_wait(session.lock_wait_default))
                    except Exception as exc:  # noqa: BLE001 - the original error must keep propagating
                        self._safe_log('restoring lock_wait_timeout failed: %s' % exc)
            if prior_state in ('started', 'failed'):
                self.journal.event('reconcile_delete', '%s bucket %d removed %d rows before re-insert' % (table, bucket, removed))
                self.log('Reconciled %s bucket %d: removed %d rows left by an interrupted attempt' % (table, bucket, removed))
        rows = session.execute(self.st.insert_chunk(p, bucket))
        if self.after_insert_hook is not None:
            self.after_insert_hook(table, bucket)
        self.journal.mark_done(table, bucket, rows)
        with self.lock:
            self.rows_since_start += rows
        return rows

    def open_session(self):
        """A new worker session behind the runner guard."""
        session = _CopySession(self.connect(), self.st.target_db, self.log)
        return session

    def _safe_log(self, message):
        try:
            self.log(message)
        except Exception:  # noqa: BLE001 - a broken log must not take a worker down
            pass

    def _give_back(self, table, bucket, prior):
        """Return a taken chunk to the front of its queue untouched."""
        with self.lock:
            self.queues[table].appendleft((bucket, prior))
        self.release(table)

    def worker(self, idx):
        """Worker loop: take runnable chunks until the queues are empty or a stop is requested.

        A connection failure gives the chunk back untouched; three in a row stop the pool.
        Any failure while journaling an error is logged (redacted) and never ends the thread.
        """
        session = None
        try:
            while True:
                if self.should_stop_launching():
                    break
                item = self.take()
                if item is None:
                    if self.remaining() == 0:
                        break
                    self.sleep(0.2)
                    continue
                table, bucket, prior = item
                if session is None:
                    try:
                        session = self.open_session()
                    except Exception as exc:  # noqa: BLE001 - the chunk was never started
                        self._safe_log('worker %d cannot connect: %s; %s bucket %d stays pending' % (
                            idx, self.log.redactor('%s: %s' % (type(exc).__name__, exc)), table, bucket))
                        self._give_back(table, bucket, prior)
                        with self.lock:
                            self.connect_failures += 1
                            if self.connect_failures >= 3:
                                self.stop_launching.set()
                                self._safe_log('three connection failures; no new chunks will start')
                        self.sleep(1)
                        break
                try:
                    self.log('worker %d start %s bucket %d/%d (prior state %s)' % (idx, table, bucket, self.by_name[table].n_buckets, prior))
                    rows = self.run_chunk(session, table, bucket, prior)
                    self.log('worker %d done %s bucket %d/%d rows=%d' % (idx, table, bucket, self.by_name[table].n_buckets, rows))
                except Exception as exc:  # noqa: BLE001 - journal the failure and continue with the next chunk
                    detail = self.log.redactor('%s: %s' % (type(exc).__name__, exc))
                    try:
                        self.journal.mark_failed(table, bucket, detail)
                        if is_lock_wait(exc):
                            self.journal.undo_attempt(table, bucket)
                            self._safe_log('worker %d LOCK WAIT on %s bucket %d; parked for %d s: %s' % (
                                idx, table, bucket, self.cfg.lock_retry_seconds, detail))
                            self.requeue_after_lock(table, bucket)
                        else:
                            with self.lock:
                                self.failed_this_run += 1
                            self._safe_log('worker %d FAILED %s bucket %d: %s' % (idx, table, bucket, detail))
                    except Exception as inner:  # noqa: BLE001 - journal trouble is reported, not fatal
                        self._safe_log('worker %d could not journal the failure of %s bucket %d: %s' % (
                            idx, table, bucket, self.log.redactor('%s: %s' % (type(inner).__name__, inner))))
                    if session is not None:
                        session.close()
                        session = None
                finally:
                    self.release(table)
        finally:
            if session is not None:
                session.close()

    # -- progress and completion ------------------------------------------------------
    def progress_line(self):
        """The progress line: tables and buckets done, failed, rows, rows per hour, projected finish, deadline."""
        totals, tables_done = self.journal.totals()
        done_n, done_rows = totals['done']
        all_n = sum(v[0] for v in totals.values())
        elapsed = max(1.0, self.clock() - self.started_at)
        with self.lock:
            rows = self.rows_since_start
        per_hour = rows / elapsed * 3600
        remaining_est = sum(p.est_rows for p in self.plans) - done_rows
        if per_hour > 0 and remaining_est > 0:
            hours = remaining_est / per_hour
            finish = 'beyond a year' if hours > 24 * 365 else iso(utc_now() + dt.timedelta(hours=hours))
        elif remaining_est <= 0:
            finish = 'now'
        else:
            finish = 'unknown'
        deadline = (' deadline=%s' % iso(self.abort_at)) if self.abort_at else ''
        return 'progress tables_done=%d/%d buckets_done=%d/%d failed=%d rows_done=%d rows_per_hour=%.0f projected_finish=%s%s' % (
            tables_done, len(self.plans), done_n, all_n, totals['failed'][0], done_rows, per_hour, finish, deadline)

    def copy(self):
        """Run the pool until every chunk is done or a stop condition holds; returns True when the copy is complete."""
        self.started_at = self.clock()
        pending = self.load_pending()
        if not pending:
            self.log('No pending chunks; copy already complete')
            return self.finish()
        self.log('Starting copy: %d chunks pending, %d workers' % (pending, self.workers))
        threads = [threading.Thread(target=self.worker, args=(i,), name='worker-%d' % i, daemon=True) for i in range(self.workers)]
        old_hook = threading.excepthook
        threading.excepthook = self._thread_excepthook
        try:
            for t in threads:
                t.start()
            last = self.clock()
            try:
                while any(t.is_alive() for t in threads):
                    self.sleep(0.2)
                    if self.clock() - last >= self.cfg.progress_seconds:
                        self._safe_log(self._progress_or_note())
                        last = self.clock()
            except KeyboardInterrupt:
                self.interrupted = True
                self.stop_launching.set()
                with self.lock:
                    running = sum(self.in_flight.values())
                self._safe_log('Interrupted: no new chunk starts; waiting for %d in-flight chunk(s) to finish. A client that dies '
                               'mid-statement leaves a leaf table lock for 15 to 20 minutes, so the workers are left to finish.' % running)
            for t in threads:
                t.join()
        finally:
            threading.excepthook = old_hook
        self._safe_log(self._progress_or_note())
        return self.finish()

    def _progress_or_note(self):
        try:
            return self.progress_line()
        except Exception as exc:  # noqa: BLE001 - a progress line must never stop the pool
            return 'progress line unavailable: %s' % type(exc).__name__

    def _thread_excepthook(self, args):
        """Redacted report of an exception that escaped a worker thread."""
        text = ''.join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
        self._safe_log('worker thread %s died: %s' % (getattr(args.thread, 'name', '?'), self.log.redactor(text)))

    def finish(self):
        """Report incomplete buckets, or run the per-table count proof when every chunk is done."""
        totals, _ = self.journal.totals()
        incomplete = totals['pending'][0] + totals['started'][0] + totals['failed'][0]
        if incomplete:
            self.log('Copy incomplete: pending=%d started=%d failed=%d. Rerun the same step with the same run to resume.' % (
                totals['pending'][0], totals['started'][0], totals['failed'][0]))
            return False
        self.log('All %d tables copied. Writing per-table count proof.' % len(self.plans))
        return self.table_count_proof()

    def table_count_proof(self, report_path=None):
        """COUNT(*) on both sides for every table; journal the counts; write <journal>.counts.txt; True when all match."""
        session = self.open_session()
        lines = ['table\tsource_count\ttarget_count\tmatch']
        ok = True
        try:
            for p in self.plans:
                src = one_value(session.query(self.st.count(self.st.source_db, p.name)))
                tgt = one_value(session.query(self.st.count(self.st.target_db, p.name)))
                self.journal.record_table_counts(p.name, src, tgt)
                match = src == tgt
                ok = ok and match
                lines.append('%s\t%d\t%d\t%s' % (p.name, src, tgt, 'yes' if match else 'NO'))
        finally:
            session.close()
        lines.append('all_tables_match\t%s' % ('yes' if ok else 'NO'))
        if report_path or self.journal.path != ':memory:':
            path = Path(report_path) if report_path else Path(self.journal.path + '.counts.txt')
            path.write_text('\n'.join(lines) + '\n', encoding='utf8')
        self.journal.event('count_proof', 'all tables match' if ok else 'MISMATCH in per-table counts')
        self.log('Per-table count proof: all tables match = %s' % ('yes' if ok else 'NO'))
        return ok


class _CopySession:
    """A worker's connection behind the runner guard, remembering the server's lock_wait_timeout."""

    def __init__(self, conn, target_db, log):
        from .connect import Session
        self.inner = Session(conn, runner_guard(target_db, 'runner O6'), log)
        self.lock_wait_default = 60
        try:
            rows = self.inner.query('SELECT @@lock_wait_timeout AS v')
            if rows:
                row = rows[0]
                self.lock_wait_default = int(row['v'] if isinstance(row, dict) else row[0])
        except Exception:  # noqa: BLE001 - a server without the variable keeps the default
            pass

    def query(self, sql, args=()):
        """Query through the guarded session."""
        return self.inner.query(sql, args)

    def execute(self, sql, args=()):
        """Execute through the guarded session."""
        return self.inner.execute(sql, args)

    def close(self):
        """Close the worker connection."""
        self.inner.close()


def fresh_table_load(session, statements, journal, log, plan):
    """The fresh-table fallback: load one table in a new table and swap it in (section 10.9).

    Used when a target table is locked by a prepared transaction the kit cannot see. Every bucket
    of the table is marked done with the rows the single INSERT reported.
    """
    fresh = plan.name + FRESH_SUFFIX
    existing = existing_target_tables(session, statements)
    src = one_value(session.query(statements.count(statements.source_db, plan.name)))
    rows = None
    if fresh in existing:
        tgt = one_value(session.query(statements.count(statements.target_db, fresh)))
        if tgt == src:
            log('Fresh table for %s already holds the full %d rows; reload skipped' % (plan.name, src))
            rows = tgt
        else:
            session.execute(statements.drop_table(fresh))
    if rows is None:
        session.execute(statements.create_fresh(plan))
        rows = session.execute(statements.insert_all(plan))
        tgt = one_value(session.query(statements.count(statements.target_db, fresh)))
        if src != tgt:
            raise KitError('Fresh-table load of %s copied %d rows but the source holds %d; the fresh table is left for inspection' % (
                plan.name, tgt, src))
    if plan.name in existing:
        session.execute(statements.drop_table(plan.name))
    session.execute(statements.rename_fresh(plan))
    for b in range(plan.n_buckets):
        journal.mark_started(plan.name, b)
        journal.mark_done(plan.name, b, rows if b == 0 else 0)
    journal.record_table_counts(plan.name, src, tgt)
    journal.event('manual_load', '%s loaded through the fresh-table fallback with %d rows' % (plan.name, rows))
    log('Fresh-table load of %s done: %d rows, count proof matches' % (plan.name, rows))
    return rows


def resolve_abort_at(journal, config, longest_chunk_seconds=0):
    """The abort deadline: the earlier of the journal's O1 record and the window-derived deadline.

    The window deadline is end minus restart allowance minus buffer minus the longest modeled
    chunk (section 10.6). O1 may have recorded it before a capture existed, so O6 recomputes
    with the plan and keeps the earlier of the two. None without a record or a window.
    """
    candidates = []
    recorded = journal.get_meta('abort_deadline')
    if recorded:
        candidates.append(parse_utc(recorded, 'abort_deadline'))
    if config.window is not None:
        candidates.append(config.window.abort_deadline(longest_chunk_seconds))
    return min(candidates) if candidates else None


def baseline_for(config):
    """The configured baseline file when present, else the shipped default."""
    return estimate.load_baseline(config.resolve(config.estimate.baseline) if config.estimate.baseline else None)


def longest_chunk_seconds(plans, config):
    """The longest modeled chunk of a plan list, the allowance the deadline subtracts."""
    return estimate.estimate(plans, config, baseline_for(config)).schedule.longest_chunk_seconds


def stop_file_for(journal_path):
    """<journal>.stop next to the journal (None for an in-memory journal)."""
    return Path(str(journal_path) + '.stop') if journal_path else None


# ---------------------------------------------------------------------------
# Step handlers
# ---------------------------------------------------------------------------
def load_plans(ctx):
    """The freeze capture (O4) when present, else the profile (P3); returns (inventory_dict, plans, directory)."""
    for step_id in ('O4', 'P3'):
        directory = inventory.inventory_dir(ctx.config, ctx.run_id or '', ctx.db, step_id)
        if (directory / inventory.INVENTORY_FILE).is_file():
            data = inventory.load(directory)
            if data.get('database') != ctx.db.source:
                raise KitError('Capture at %s describes database %s, not %s; refusing to use it' % (directory, data.get('database'), ctx.db.source))
            return data, inventory.plan_tables(data, ctx.config, ctx.db), directory
    raise KitError('No capture for %s under this run; run s2rebuild profile (P3) and the O4 freeze capture first' % ctx.db.source)


def _no_plans(step, ctx, exc):
    if ctx.dry_run:
        return steps.Outcome(steps.STUB, 'no capture yet; ' + str(exc))
    return steps.Outcome(steps.REFUSED, str(exc), EXIT_REFUSED)


def handle_o5(step, ctx):
    """O5 handler: create the replacement and its tables from the capture, verify every definition."""
    try:
        data, plans, directory = load_plans(ctx)
    except KitError as exc:
        return _no_plans(step, ctx, exc)
    statements = statements_for(ctx.db)
    session = ctx.runner_session(runner_guard(ctx.db.replacement, 'runner O5'))
    try:
        if ctx.dry_run:
            session.execute(statements.create_database())
            for p in plans:
                session.execute(statements.create_table_like(p.name))
                session.query(statements.show_create(p.name))
            return steps.Outcome(steps.PASSED, 'dry run: %d statements for %d tables printed' % (1 + 2 * len(plans), len(plans)))
        create_replacement(session, statements, ctx.journal, ctx.log, plans, data.get('layout'), ctx.db.target, directory)
        ctx.journal.set_meta('plan_dir', str(directory))
        from . import switch as switch_module
        storage_id = switch_module.record_replacement(ctx, session)
        if storage_id:
            ctx.log('replacement %s recorded on STORAGE_ID %s for the switch' % (ctx.db.replacement, storage_id))
        else:
            ctx.log('replacement %s has no storage row visible to the runner; O10 reads its STORAGE_ID before S1' % ctx.db.replacement)
        return steps.Outcome(steps.PASSED, 'replacement %s verified with %d tables' % (ctx.db.replacement, len(plans)))
    finally:
        session.close()


def handle_o6(step, ctx):
    """O6 handler: copy every chunk (or one table through the fresh-table fallback), then the count proof.

    Live path: the copy lock, the source and row-accounting checks of the reference preflight,
    then the Runner with the abort deadline including the longest-chunk allowance.
    """
    try:
        data, plans, directory = load_plans(ctx)
    except KitError as exc:
        return _no_plans(step, ctx, exc)
    statements = statements_for(ctx.db)
    fresh = ctx.options.get('fresh_table')
    if ctx.dry_run:
        session = ctx.runner_session(runner_guard(ctx.db.replacement, 'runner O6'))
        try:
            if fresh:
                plan = next((p for p in plans if p.name == fresh), None)
                if plan is None:
                    return steps.Outcome(steps.REFUSED, 'Table %s is not in the plan' % fresh, EXIT_REFUSED)
                for sql in (statements.create_fresh(plan), statements.insert_all(plan), statements.count(statements.source_db, plan.name),
                            statements.count(statements.target_db, plan.name + FRESH_SUFFIX), statements.drop_table(plan.name),
                            statements.rename_fresh(plan)):
                    session.execute(sql) if not sql.startswith('SELECT') else session.query(sql)
                return steps.Outcome(steps.PASSED, 'dry run: fresh-table statements for %s printed' % fresh)
            for p in plans:
                for b in range(p.n_buckets):
                    session.execute(statements.insert_chunk(p, b))
            return steps.Outcome(steps.PASSED, 'dry run: %d INSERT statements over %d tables printed' % (sum(p.n_buckets for p in plans), len(plans)))
        finally:
            session.close()
    journal = ctx.journal
    if journal.is_empty():
        return steps.Outcome(steps.REFUSED, 'O5 has not written the plan into this journal; run O5 first', EXIT_REFUSED)
    journal.check_plan(plans, statements.source_db, statements.target_db)
    with copy_lock(ctx.journal_path):
        session = ctx.runner_session(runner_guard(ctx.db.replacement, 'runner O6'))
        try:
            check_source(session, statements, ctx.log, data.get('layout'))
            check_target_rows_accounted(session, statements, journal, [p for p in plans if p.name != fresh])
            if fresh:
                plan = next((p for p in plans if p.name == fresh), None)
                if plan is None:
                    return steps.Outcome(steps.REFUSED, 'Table %s is not in the plan' % fresh, EXIT_REFUSED)
                try:
                    fresh_table_load(session, statements, journal, ctx.log, plan)
                except KitError:
                    raise
                except Exception as exc:  # noqa: BLE001 - a driver error must leave a journaled, resumable state
                    detail = ctx.log.redactor('%s: %s' % (type(exc).__name__, exc))
                    journal.event('fresh_table_error', '%s: %s' % (fresh, detail))
                    return steps.Outcome(steps.INCOMPLETE, 'fresh-table load of %s failed (%s); rerun step O6 --fresh-table %s, the load resumes '
                                         'from the fresh table when it is complete' % (fresh, detail, fresh), EXIT_INCOMPLETE)
                totals, _ = journal.totals()
                left = totals['pending'][0] + totals['started'][0] + totals['failed'][0]
                if left:
                    return steps.Outcome(steps.INCOMPLETE, 'fresh-table load done; %d chunks still pending on other tables' % left, EXIT_INCOMPLETE)
        finally:
            session.close()
        abort_at = resolve_abort_at(journal, ctx.config, longest_chunk_seconds(plans, ctx.config))
        stop_file = stop_file_for(ctx.journal_path)
        if abort_at is not None:
            ctx.log('Abort deadline: %s UTC (includes the longest-chunk allowance). Stop file: %s' % (iso(abort_at), stop_file))
        runner = Runner(plans, statements, journal, ctx.runner_connector(), ctx.log, ctx.config.copy, abort_at=abort_at,
                        stop_file=stop_file, retry_failed=bool(ctx.options.get('retry_failed')))
        complete = runner.copy()
    if not complete:
        totals, _ = journal.totals()
        if totals['pending'][0] + totals['started'][0] + totals['failed'][0] == 0:
            return steps.Outcome(steps.MISMATCH, 'count proof found a per-table mismatch; see the journal counts', EXIT_MISMATCH)
        return steps.Outcome(steps.INCOMPLETE, 'copy incomplete: pending=%d started=%d failed=%d%s; rerun O6 with the same run to resume' % (
            totals['pending'][0], totals['started'][0], totals['failed'][0], ' (interrupted)' if runner.interrupted else ''), EXIT_INCOMPLETE)
    return steps.Outcome(steps.PASSED, 'every chunk done; per-table count proof all match')


steps.register('O5', handle_o5)
steps.register('O6', handle_o6)
