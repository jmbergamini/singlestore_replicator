"""s2rebuild verify: tier 1 counts, tier 2 newest values, tier 3 fingerprints in parallel and sampled by range, and the O7 stale check.

Author: John Bergamini

Section 10.10 of the architecture. Every table records verified_at_utc when a tier matches,
and every verification row carries the table's re-copy epoch; O10 refuses to switch unless
every table was verified after freeze_time and after its last chunk, at its current epoch. A
mismatching fingerprint group names the bucket to re-copy when the group count equals the
table's bucket count. Tier 3 runs on verify.sessions connections, largest table first, the
source and replacement sides of a table at the same time; a table over
verify.full_fingerprint_max_rows is sampled as a contiguous range on its first sort key column
or its time column, the same predicate on both sides; verify.fingerprint_budget_minutes is an
elapsed deadline after which a table is recorded not fingerprinted. Statements issued, all
through the read guard: SELECT COUNT(*), SELECT MAX(time column), SELECT MIN and MAX of the
first key column or of the range column, and the grouped fingerprint SELECT with CRC32 sums.
Nothing else.
"""
import collections
import dataclasses
import datetime as dt
import decimal
import re
import threading
import time

from . import estimate
from .common import KitError
from .sqlutil import ident, qualified, read_guard

TIER_COUNT, TIER_NEWEST, TIER_FINGERPRINT = 'count', 'newest', 'fingerprint'
MATCH, MISMATCH, COUNT_ONLY, SAMPLED, NOT_FINGERPRINTED = 'match', 'mismatch', 'count only', 'sampled', 'not fingerprinted'
VALUE_COLUMNS = 4
SIDE_SOURCE, SIDE_TARGET = 'source', 'replacement'
ACCEPTED_EVENT = 'accepted'
# DATA_TYPE values the value hash leaves out: CONCAT_WS has no stable text for them
UNSUPPORTED_TYPES = frozenset(('json', 'bson', 'geography', 'geographypoint', 'vector', 'blob', 'tinyblob', 'mediumblob',
                               'longblob', 'binary', 'varbinary'))
# DATA_TYPE values a contiguous range can be computed on: a number or a point in time
ORDERABLE_TYPES = frozenset(('tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint', 'decimal', 'numeric', 'float',
                             'double', 'real', 'date', 'datetime', 'timestamp'))
_TIME_FORMATS = ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d')
_ACCEPTED = re.compile(r'^accepted (sampled fingerprint for|not fingerprinted) (\S+) at re-copy epoch (\d+)')


def count_sql(db, table):
    """SELECT COUNT(*) for one table."""
    return 'SELECT COUNT(*) AS n FROM ' + qualified(db, table)


def newest_sql(db, table, time_column):
    """SELECT MAX(time column) for one table."""
    return 'SELECT MAX(%s) AS newest FROM %s' % (ident(time_column), qualified(db, table))


def key_range_sql(db, table, key_column):
    """SELECT MIN and MAX of one column: the first key column at tier 2, the range column of a sample."""
    return 'SELECT MIN(%s) AS lo, MAX(%s) AS hi FROM %s' % (ident(key_column), ident(key_column), qualified(db, table))


def supported(plan, column):
    """True when the value hash can take the column: its type is not one of UNSUPPORTED_TYPES (an unknown type counts)."""
    return str(getattr(plan, 'column_types', {}).get(column) or '').lower() not in UNSUPPORTED_TYPES


def orderable(plan, column):
    """True when a contiguous range can be computed on the column: a number or time type, or a type the plan does not know."""
    kind = str(getattr(plan, 'column_types', {}).get(column) or '').lower()
    return not kind or kind in ORDERABLE_TYPES


def value_columns_for(plan, limit=VALUE_COLUMNS):
    """The value hash columns: the time column, then up to limit supported non-key columns; the first column when nothing else."""
    columns = [plan.time_column] if plan.time_column else []
    others = [c for c in plan.columns if c not in plan.key_columns and c != plan.time_column and supported(plan, c)]
    columns.extend(others[:max(0, int(limit))])
    return columns or plan.columns[:1]


def value_expr(plan, limit=VALUE_COLUMNS):
    """The value hash input: CONCAT_WS of the time column and up to limit supported columns, each with COALESCE."""
    return "CONCAT_WS('|', " + ', '.join("COALESCE(%s, '')" % ident(c) for c in value_columns_for(plan, limit)) + ')'


def range_column(plan):
    """The column a sample ranges over: the first sort key column that is orderable, else an orderable time column, else None."""
    for column in getattr(plan, 'sort_columns', ()) or ():
        if column in plan.columns and orderable(plan, column):
            return column
    if plan.time_column and orderable(plan, plan.time_column):
        return plan.time_column
    return None


def _as_number(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, decimal.Decimal)):
        return value
    try:
        return int(str(value))
    except (TypeError, ValueError):
        pass
    try:
        return decimal.Decimal(str(value))
    except (decimal.InvalidOperation, TypeError, ValueError):
        return None


def _as_time(value):
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day)
    for fmt in _TIME_FORMATS:
        try:
            return dt.datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None


def sample_cut(lo, hi, percent):
    """The upper bound of a range that spans percent of the value span from lo: a number or a time, or None when the values
    cannot be spanned (text, or an unknown shape)."""
    n_lo, n_hi = _as_number(lo), _as_number(hi)
    if n_lo is not None and n_hi is not None:
        if isinstance(n_lo, int) and isinstance(n_hi, int):
            return n_lo + (n_hi - n_lo) * int(percent) // 100
        return decimal.Decimal(str(n_lo)) + (decimal.Decimal(str(n_hi)) - decimal.Decimal(str(n_lo))) * int(percent) / 100
    t_lo, t_hi = _as_time(lo), _as_time(hi)
    if t_lo is not None and t_hi is not None:
        return t_lo + (t_hi - t_lo) * int(percent) / 100
    return None


def sql_literal(value):
    """A number as it is, a time as a quoted 'YYYY-MM-DD HH:MM:SS.ffffff', anything else quoted with its quotes doubled."""
    if isinstance(value, bool):
        return '1' if value else '0'
    if isinstance(value, (int, float, decimal.Decimal)):
        return str(value)
    if isinstance(value, dt.datetime):
        return "'%s'" % value.strftime('%Y-%m-%d %H:%M:%S.%f')
    if isinstance(value, dt.date):
        return "'%s'" % value.isoformat()
    return "'%s'" % str(value).replace("'", "''")


def range_predicate(column, lo, cut):
    """The sample predicate, the same text on both sides: column >= lo AND column <= cut."""
    return '%s >= %s AND %s <= %s' % (ident(column), sql_literal(lo), ident(column), sql_literal(cut))


def fingerprint_groups(plan, config):
    """The table's bucket count, or verify.fingerprint_groups for a single-bucket table."""
    return plan.n_buckets if plan.n_buckets > 1 else int(config.verify.fingerprint_groups)


def fingerprint_sql(db, plan, groups, predicate=None, value_columns=VALUE_COLUMNS):
    """The grouped fingerprint SELECT of section 10.10, over the rows of predicate when one is given."""
    sql = ('SELECT CRC32(%s) MOD %d AS grp, COUNT(*) AS n, SUM(CRC32(%s)) AS key_hash, SUM(CRC32(%s)) AS value_hash FROM %s' % (
        plan.key_expr, groups, plan.key_expr, value_expr(plan, value_columns), qualified(db, plan.name)))
    if predicate:
        sql += ' WHERE ' + predicate
    return sql + ' GROUP BY grp'


def fingerprint_cost_seconds(plan, baseline):
    """Two scans of the table at the baseline scan rate."""
    return 2.0 * plan.est_rows * baseline.scan_s_per_row


def sessions_for(config):
    """verify.sessions, or copy.workers when the key is not set."""
    return int(config.verify.sessions or config.copy.workers)


@dataclasses.dataclass
class TierResult:
    """One tier's outcome for one table."""

    table: str
    tier: str
    result: str
    source_value: object = None
    target_value: object = None
    sql_source: str = None
    sql_target: str = None
    detail: str = ''
    mismatched_groups: tuple = ()
    sampled: bool = False
    predicate: str = None
    sample_rows: int = None
    seconds: float = None

    @property
    def ok(self):
        """True unless the result is a mismatch."""
        return self.result != MISMATCH


def _scalar(rows, key):
    if not rows:
        return None
    row = rows[0]
    return row[key] if isinstance(row, dict) else row[0]


def _pair(rows, first, second):
    if not rows:
        return None, None
    row = rows[0]
    return (row[first], row[second]) if isinstance(row, dict) else (row[0], row[1])


def _text(value):
    return None if value is None else str(value)


class Verifier:
    """Runs the tiers for a plan list and journals every result.

    Tiers 1 and 2 go through the one read-guarded session. Tier 3 runs on verify.sessions
    worker threads with a connection each from connector (ctx.runner_connector), largest table
    first, both sides of a table at the same time; without a connector it runs on the session,
    one table at a time. clock is the elapsed-time source of the budget deadline and the per-table
    seconds (time.monotonic; the tests inject one).
    """

    def __init__(self, session, source_db, target_db, plans, journal, config, baseline=None, log=None, connector=None,
                 clock=time.monotonic):
        self.session = session
        self.source_db = source_db
        self.target_db = target_db
        self.plans = plans
        self.journal = journal
        self.config = config
        self.baseline = baseline or estimate.DEFAULT_BASELINE
        self.log = log or (lambda msg: None)
        self.connector = connector
        self.clock = clock
        self.sessions = sessions_for(config) if connector is not None else 1
        self.value_columns = int(config.verify.value_columns)
        # (table, side, worker) in the order the workers took the fingerprint tasks
        self.schedule = []

    def _record(self, r):
        self.journal.record_verification(r.table, r.tier, r.result, r.source_value, r.target_value, r.sql_source, r.sql_target, r.detail,
                                         predicate=r.predicate, sample_rows=r.sample_rows)
        return r

    def tier1(self, plan):
        """Tier 1: COUNT(*) on both sides."""
        sql_s, sql_t = count_sql(self.source_db, plan.name), count_sql(self.target_db, plan.name)
        src = int(_scalar(self.session.query(sql_s), 'n') or 0)
        tgt = int(_scalar(self.session.query(sql_t), 'n') or 0)
        result = MATCH if src == tgt else MISMATCH
        self.journal.record_table_counts(plan.name, src, tgt)
        return self._record(TierResult(plan.name, TIER_COUNT, result, src, tgt, sql_s, sql_t,
                                       '' if result == MATCH else 'source %d, replacement %d' % (src, tgt)))

    def tier2(self, plan):
        """Tier 2: MAX(time column) and the first key column's MIN and MAX; count only without a time column."""
        results = []
        if plan.key_columns:
            sql_s, sql_t = key_range_sql(self.source_db, plan.name, plan.key_columns[0]), key_range_sql(self.target_db, plan.name, plan.key_columns[0])
            lo_s, hi_s = _pair(self.session.query(sql_s), 'lo', 'hi')
            lo_t, hi_t = _pair(self.session.query(sql_t), 'lo', 'hi')
            same = (_text(lo_s), _text(hi_s)) == (_text(lo_t), _text(hi_t))
            results.append(self._record(TierResult(plan.name, TIER_NEWEST, MATCH if same else MISMATCH,
                                                   '%s .. %s' % (lo_s, hi_s), '%s .. %s' % (lo_t, hi_t), sql_s, sql_t,
                                                   'key range of ' + plan.key_columns[0])))
        if plan.time_column:
            sql_s, sql_t = newest_sql(self.source_db, plan.name, plan.time_column), newest_sql(self.target_db, plan.name, plan.time_column)
            src, tgt = _scalar(self.session.query(sql_s), 'newest'), _scalar(self.session.query(sql_t), 'newest')
            same = _text(src) == _text(tgt)
            results.append(self._record(TierResult(plan.name, TIER_NEWEST, MATCH if same else MISMATCH, _text(src), _text(tgt), sql_s, sql_t,
                                                   'newest ' + plan.time_column)))
        else:
            results.append(self._record(TierResult(plan.name, TIER_NEWEST, COUNT_ONLY, None, None, None, None,
                                                   'no time column; count only')))
        return results

    # -- tier 3 -------------------------------------------------------------------
    def rows_of(self, plan):
        """The rows the sampling rule compares with the threshold: the journal's source count when tier 1 ran, else the estimate."""
        for t in self.journal.tables():
            if t['table_name'] == plan.name and t.get('source_count') is not None:
                return int(t['source_count'])
        return int(plan.est_rows)

    def sampling(self, plan, percent=None, full=False):
        """(predicate, note) for tier 3 of plan: a range over percent (default verify.sample_percent) of the value span of
        the range column when the table is over verify.full_fingerprint_max_rows (or percent is given); None with a note
        for a full fingerprint. Issues one MIN and MAX read on the source for a sampled table."""
        rows = self.rows_of(plan)
        threshold = int(self.config.verify.full_fingerprint_max_rows)
        if full or (percent is None and rows <= threshold):
            return None, ''
        percent = int(self.config.verify.sample_percent if percent is None else percent)
        if percent >= 100:
            return None, ''
        over = '%d rows over verify.full_fingerprint_max_rows %d' % (rows, threshold) if rows > threshold else 'sample of %d percent asked' % percent
        column = range_column(plan)
        if column is None:
            return None, 'full fingerprint: %s and no orderable sort key or time column' % over
        lo, hi = _pair(self.session.query(key_range_sql(self.source_db, plan.name, column)), 'lo', 'hi')
        if lo is None or hi is None:
            return None, 'full fingerprint: %s and %s holds no value' % (over, column)
        cut = sample_cut(lo, hi, percent)
        if cut is None:
            return None, 'full fingerprint: %s and the values of %s cannot be spanned' % (over, column)
        return range_predicate(column, lo, cut), 'sampled %d percent of the %s range' % (percent, column)

    def _open_session(self):
        if self.connector is None:
            return self.session
        from .connect import Session
        return Session(self.connector(), read_guard('runner O8 fingerprint'), self.log)

    def fingerprint_side(self, session, db, plan, groups, predicate):
        """One side of a table: (sql, {group: (n, key_hash, value_hash)})."""
        sql = fingerprint_sql(db, plan, groups, predicate, self.value_columns)
        return sql, {int(r['grp']): (int(r['n']), int(r['key_hash']), int(r['value_hash'])) for r in session.query(sql)}

    def compare(self, plan, sides, predicate, note, seconds):
        """Compare the two sides of a table group by group, journal the row and log the table line."""
        sql_s, side_s = sides[SIDE_SOURCE]
        sql_t, side_t = sides[SIDE_TARGET]
        bad = sorted(g for g in set(side_s) | set(side_t) if side_s.get(g) != side_t.get(g))
        sampled = predicate is not None
        result = MISMATCH if bad else (SAMPLED if sampled else MATCH)
        sample_rows = sum(n for n, _, _ in side_s.values()) if sampled else None
        detail = '%d groups compared' % len(set(side_s) | set(side_t))
        if note:
            detail += ', ' + note
        if sampled:
            detail += ', %d rows in the sample, WHERE %s' % (sample_rows, predicate)
        if bad:
            detail += ', mismatched groups ' + ', '.join(str(g) for g in bad)
        r = TierResult(plan.name, TIER_FINGERPRINT, result, len(side_s), len(side_t), sql_s, sql_t, detail, tuple(bad), sampled,
                       predicate, sample_rows, seconds)
        self._record(r)
        if result == SAMPLED:
            self.journal.set_table_attr(plan.name, 'verified_at_utc', self.journal.verifications(plan.name, TIER_FINGERPRINT)[-1]['checked_utc'])
        self.log('tier 3 %s: %s in %.1f s (%s)' % (plan.name, result, seconds, detail))
        return r

    def tier3_all(self, plans, percent=None, full=False, budget_minutes=None, use_budget=True):
        """Tier 3 over plans: largest first, both sides of a table at once, on self.sessions connections.

        The budget (verify.fingerprint_budget_minutes unless budget_minutes is given) is an
        elapsed deadline from the start of this call: a table whose first side is taken after
        it is recorded not fingerprinted. Results come back in table order, largest first.
        """
        ordered = sorted(plans, key=lambda p: (-p.est_rows, p.name))
        budget = self.config.verify.fingerprint_budget_minutes if budget_minutes is None else budget_minutes
        started = self.clock()
        deadline = started + float(budget) * 60.0 if use_budget and budget is not None else None
        jobs = collections.OrderedDict()
        queue = collections.deque()
        for plan in ordered:
            predicate, note = self.sampling(plan, percent, full)
            jobs[plan.name] = {'plan': plan, 'predicate': predicate, 'note': note, 'groups': fingerprint_groups(plan, self.config),
                               'sides': {}, 'started': None, 'result': None, 'skipped': False}
            queue.append((plan.name, SIDE_SOURCE))
            queue.append((plan.name, SIDE_TARGET))
        lock = threading.Lock()
        errors = []

        def _take(worker):
            with lock:
                while queue:
                    name, side = queue.popleft()
                    job = jobs[name]
                    if side == SIDE_SOURCE and deadline is not None and self.clock() > deadline:
                        # the table's first side comes after the deadline: its other side is next in the queue
                        job['skipped'] = True
                        queue.popleft()
                        continue
                    if job['started'] is None:
                        job['started'] = self.clock()
                    self.schedule.append((name, side, worker))
                    return name, side, job
            return None

        def _work(worker):
            # the open sits inside the try: a session that cannot open (2003 while the workspace resumes) is raised after the join, so O8
            # records failed with the driver line, and nothing escapes the thread unredacted
            session = None
            try:
                session = self._open_session()
                while not errors:
                    item = _take(worker)
                    if item is None:
                        return
                    name, side, job = item
                    db = self.source_db if side == SIDE_SOURCE else self.target_db
                    sql, groups = self.fingerprint_side(session, db, job['plan'], job['groups'], job['predicate'])
                    with lock:
                        job['sides'][side] = (sql, groups)
                        done = len(job['sides']) == 2
                    if done:
                        job['result'] = self.compare(job['plan'], job['sides'], job['predicate'], job['note'], self.clock() - job['started'])
            except BaseException as exc:  # noqa: BLE001 - raised again in the calling thread after the join
                with lock:
                    errors.append(exc)
            finally:
                if session is not None and session is not self.session:
                    session.close()

        if self.sessions == 1 and self.connector is None:
            _work(0)
        else:
            self.log('tier 3: %d tables on %d sessions, largest first%s' % (
                len(ordered), self.sessions, '' if deadline is None else ', budget %s minutes' % budget))
            threads = [threading.Thread(target=_work, args=(i,), name='fingerprint-%d' % i, daemon=True) for i in range(self.sessions)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        if errors:
            raise errors[0]
        results = []
        for name, job in jobs.items():
            if job['result'] is not None:
                results.append(job['result'])
            elif job['skipped']:
                detail = 'budget: verify.fingerprint_budget_minutes %s elapsed before this table' % budget
                results.append(self._record(TierResult(name, TIER_FINGERPRINT, NOT_FINGERPRINTED, None, None, None, None, detail)))
                self.log('tier 3 %s: %s (%s)' % (name, NOT_FINGERPRINTED, detail))
        return results

    def run(self, tiers=None, budget_minutes=None):
        """Run the configured tiers over every table: tiers 1 and 2 table by table, then tier 3 in parallel (tier3_all).

        A table with any mismatch in this run ends without a verified_at_utc stamp, whatever
        its other tiers said.
        """
        tiers = list(tiers if tiers is not None else self.config.verify.tiers)
        results = []
        for plan in self.plans:
            if TIER_COUNT in tiers:
                results.append(self.tier1(plan))
            if TIER_NEWEST in tiers:
                results.extend(self.tier2(plan))
        if TIER_FINGERPRINT in tiers:
            results.extend(self.tier3_all(self.plans, budget_minutes=budget_minutes))
        self._clear_mismatched(results)
        return results

    def rerun(self, names, percent=None, full=False):
        """Tier 3 for the named tables only (step O8 --fingerprint TABLE --percent N, --full TABLE), no budget, at the current epoch."""
        by_name = {p.name: p for p in self.plans}
        unknown = sorted(set(names) - set(by_name))
        if unknown:
            raise KitError('Not a planned table: %s; the plan holds %s' % (', '.join(unknown), ', '.join(sorted(by_name))))
        results = self.tier3_all([by_name[n] for n in names], percent=percent, full=full, use_budget=False)
        self._clear_mismatched(results)
        return results

    def _clear_mismatched(self, results):
        for table in sorted({r.table for r in results if r.result == MISMATCH}):
            self.journal.set_table_attr(table, 'verified_at_utc', None)


def recopy_targets(results, plans):
    """(table, bucket or None) pairs to mark for re-copy from the mismatches; a group maps to a bucket when counts agree."""
    by_name = {p.name: p for p in plans}
    out = []
    for r in results:
        if r.result != MISMATCH:
            continue
        plan = by_name[r.table]
        if r.tier == TIER_FINGERPRINT and plan.n_buckets > 1 and r.mismatched_groups:
            out.extend((r.table, g) for g in r.mismatched_groups)
        else:
            out.append((r.table, None))
    seen = set()
    unique = []
    for item in out:
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


RESULT_PRECEDENCE = (MISMATCH, NOT_FINGERPRINTED, SAMPLED, COUNT_ONLY, MATCH)


def table_result(results):
    """{(tier, table): result} with one result per table and tier; a mismatch wins over every other row."""
    out = {}
    for r in results:
        key = (r.tier, r.table)
        current = out.get(key)
        if current is None or RESULT_PRECEDENCE.index(r.result) < RESULT_PRECEDENCE.index(current):
            out[key] = r.result
    return out


def summarize(results):
    """Per tier: tables matched, mismatched, sampled, not fingerprinted, count only, one entry per table (mismatch wins)."""
    summary = {}
    for (tier, table), result in table_result(results).items():
        entry = summary.setdefault(tier, {MATCH: set(), MISMATCH: set(), SAMPLED: set(), NOT_FINGERPRINTED: set(), COUNT_ONLY: set()})
        entry[result].add(table)
    return {tier: {k: sorted(v) for k, v in entry.items()} for tier, entry in summary.items()}


def summary_lines(results):
    """Human lines, one per tier."""
    lines = []
    for tier, entry in summarize(results).items():
        lines.append('tier %s: %d match, %d mismatch, %d sampled, %d not fingerprinted, %d count only%s' % (
            tier, len(entry[MATCH]), len(entry[MISMATCH]), len(entry[SAMPLED]), len(entry[NOT_FINGERPRINTED]), len(entry[COUNT_ONLY]),
            (': mismatch on ' + ', '.join(entry[MISMATCH])) if entry[MISMATCH] else ''))
    return lines


# -- the recorded result: acceptance without a rerun ---------------------------------
def latest_rows(journal, plan_names, tiers):
    """{table: {tier: the newest verification row at the table's current re-copy epoch}} for the planned tables."""
    out = {}
    epochs = {t['table_name']: int(t.get('recopy_epoch') or 0) for t in journal.verification_state()}
    for name in plan_names:
        rows = [r for r in journal.verifications(name) if r.get('epoch') is not None and int(r['epoch']) == epochs.get(name, 0)]
        out[name] = {tier: next((r for r in reversed(rows) if r['tier'] == tier), None) for tier in tiers}
    return out


def accepted_tables(journal):
    """{table: epoch} of the accepted events (accept_recorded), the newest epoch per table."""
    out = {}
    for detail in journal.events(ACCEPTED_EVENT):
        m = _ACCEPTED.match(str(detail))
        if m:
            out[m.group(2)] = max(int(m.group(3)), out.get(m.group(2), -1))
    return out


def recorded_state(journal, plans, tiers):
    """(rows by table, None) when O8 can accept the recorded result without a rerun, else (None, reason).

    The recorded result stands when every planned table has every chunk done, a row of every
    configured tier at its current re-copy epoch, none of them a mismatch, and no chunk ended
    after the oldest of those rows was written (no chunk was copied since the last O8 run).
    """
    tiers = list(tiers)
    state = {t['table_name']: t for t in journal.verification_state()}
    rows = latest_rows(journal, [p.name for p in plans], tiers)
    for plan in plans:
        t = state.get(plan.name)
        if t is None:
            return None, '%s has no journal row' % plan.name
        if int(t.get('chunks_open') or 0):
            return None, '%s has chunks not done' % plan.name
        latest = rows[plan.name]
        for tier in tiers:
            row = latest.get(tier)
            if row is None:
                return None, '%s has no %s row at re-copy epoch %d' % (plan.name, tier, int(t.get('recopy_epoch') or 0))
            if row['result'] == MISMATCH:
                return None, '%s: the last %s row is a mismatch' % (plan.name, tier)
            if t.get('last_chunk_end') and str(row['checked_utc']) < str(t['last_chunk_end']):
                return None, '%s: a chunk ended at %s after its %s row of %s' % (plan.name, t['last_chunk_end'], tier, row['checked_utc'])
    return rows, None


def tables_to_accept(journal, plans, tiers):
    """(to accept, missing): the planned tables whose newest tier 3 row at the current epoch is sampled or not fingerprinted and
    not yet accepted, as {table: row}, and the tables with no tier 3 row at the current epoch (when tier 3 is configured)."""
    if TIER_FINGERPRINT not in tiers:
        return {}, []
    rows = latest_rows(journal, [p.name for p in plans], [TIER_FINGERPRINT])
    epochs = {t['table_name']: int(t.get('recopy_epoch') or 0) for t in journal.verification_state()}
    accepted = accepted_tables(journal)
    to_accept, missing = {}, []
    for plan in plans:
        row = rows[plan.name][TIER_FINGERPRINT]
        if row is None:
            missing.append(plan.name)
        elif row['result'] in (SAMPLED, NOT_FINGERPRINTED) and accepted.get(plan.name) != epochs.get(plan.name, 0):
            to_accept[plan.name] = row
    return to_accept, missing


def accept_recorded(journal, rows, log=None):
    """Record one accepted event per table of rows (tables_to_accept): accepted sampled fingerprint for TABLE at re-copy epoch N,
    or accepted not fingerprinted TABLE at re-copy epoch N (budget). Returns the lines recorded."""
    lines = []
    for table in sorted(rows):
        row = rows[table]
        epoch = int(row.get('epoch') or 0)
        if row['result'] == NOT_FINGERPRINTED:
            line = 'accepted not fingerprinted %s at re-copy epoch %d (budget)' % (table, epoch)
        else:
            line = 'accepted sampled fingerprint for %s at re-copy epoch %d (%d rows, WHERE %s)' % (
                table, epoch, int(row.get('sample_rows') or 0), row.get('predicate'))
        journal.event(ACCEPTED_EVENT, line)
        if log is not None:
            log(line)
        lines.append(line)
    return lines


def stale_check(verifier):
    """The O7 stale check: tiers 1 and 2 on every table; returns the results."""
    return verifier.run(tiers=[TIER_COUNT, TIER_NEWEST])


def dry_run_statements(session, source_db, target_db, plans, config, tiers=None):
    """Feed every verification statement the given tiers (default: the configured ones) would issue to a dry-run session.

    The fingerprint is printed in its full form: a sample's range is read from the source at run time.
    """
    tiers = list(config.verify.tiers if tiers is None else tiers)
    for plan in plans:
        for db in (source_db, target_db):
            if TIER_COUNT in tiers:
                session.query(count_sql(db, plan.name))
            if TIER_NEWEST in tiers and plan.key_columns:
                session.query(key_range_sql(db, plan.name, plan.key_columns[0]))
            if TIER_NEWEST in tiers and plan.time_column:
                session.query(newest_sql(db, plan.name, plan.time_column))
            if TIER_FINGERPRINT in tiers:
                session.query(fingerprint_sql(db, plan, fingerprint_groups(plan, config), value_columns=int(config.verify.value_columns)))
