"""s2rebuild estimate: throughput model, scheduling simulation, window fit and calibration.

Author: John Bergamini

Section 10.6 of the architecture. The model per chunk of a table with N rows in B buckets is
chunk_seconds = chunk_overhead_s + scan_s_per_row * N + insert_s_per_row * N / B. A reference
table is one bucket through a staging table, so its rows are inserted twice at the insert rate:
reference_seconds = chunk_overhead_s + scan_s_per_row * N + 2 * insert_s_per_row * N. The default
constants come from the reference run (4 workers, S-16 compute at 2x) and calibrate refits
them from a journal. Statements issued: none (calibrate reads a journal file with sqlite3,
read-only).
"""
import dataclasses
import datetime as dt
import heapq
import json
import sqlite3
from pathlib import Path

from .common import KitError, auto_table_limit, format_duration, iso, parse_utc, utc_now
from .inventory import unmigrated
from .journal import read_only_uri

DEFAULT_SCAN_S_PER_ROW = 7.62e-8
DEFAULT_INSERT_S_PER_ROW = 2.95e-6
DEFAULT_CHUNK_OVERHEAD_S = 2.0
DEFAULT_WORKERS = 4
DEFAULT_COMPUTE_SIZE = 'S-16'

# Fixed allowances per step, in minutes (section 10.6).
ALLOWANCE_MINUTES = {'freeze': 10, 'create_replacement': 2, 'post_copy': 5, 'switch': 10, 'release': 10}
PIPELINE_MINUTES_EACH = 1
# The error text of the chunk rows a fresh-table load writes (one whole-table INSERT, not a chunk timing); read_chunks skips them.
FRESH_LOAD_ERROR = 'fresh-table load'


@dataclasses.dataclass(frozen=True)
class Baseline:
    """The three model constants with their provenance."""

    scan_s_per_row: float = DEFAULT_SCAN_S_PER_ROW
    insert_s_per_row: float = DEFAULT_INSERT_S_PER_ROW
    chunk_overhead_s: float = DEFAULT_CHUNK_OVERHEAD_S
    workers: int = DEFAULT_WORKERS
    compute_size: str = DEFAULT_COMPUTE_SIZE
    run_id: str = 'reference'
    fitted_utc: str = None
    source: str = 'default'
    chunks: int = 349
    fitted_overhead_s: float = None

    @property
    def scan_rows_per_second(self):
        """Rows scanned per second per worker."""
        return 1.0 / self.scan_s_per_row

    @property
    def insert_rows_per_second(self):
        """Rows inserted per second per worker."""
        return 1.0 / self.insert_s_per_row

    def to_dict(self):
        """JSON-ready dict for customer/baseline.json."""
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data):
        """Build from a baseline.json mapping; unknown keys are refused."""
        fields = {f.name for f in dataclasses.fields(cls)}
        unknown = set(data) - fields
        if unknown:
            raise KitError('Baseline file holds unknown keys: ' + ', '.join(sorted(unknown)))
        try:
            baseline = cls(**data)
        except TypeError as exc:
            raise KitError('Baseline file is incomplete: %s' % exc)
        for name in ('scan_s_per_row', 'insert_s_per_row', 'chunk_overhead_s'):
            value = getattr(baseline, name)
            if not isinstance(value, (int, float)) or value <= 0 and name != 'chunk_overhead_s' or value < 0:
                raise KitError('Baseline %s must be a positive number' % name)
        return baseline


DEFAULT_BASELINE = Baseline()


def load_baseline(path=None):
    """The baseline from a JSON file, or the shipped default when path is None or the file is absent."""
    if path is None:
        return DEFAULT_BASELINE
    path = Path(path)
    if not path.is_file():
        return DEFAULT_BASELINE
    try:
        data = json.loads(path.read_text(encoding='utf8'))
    except ValueError:
        raise KitError('Baseline file %s is not valid JSON' % path)
    if not isinstance(data, dict):
        raise KitError('Baseline file %s must hold a mapping' % path)
    return Baseline.from_dict(dict(data, source=str(path)))


def write_baseline(path, baseline):
    """Write the baseline as JSON (source recorded as the file path)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = baseline.to_dict()
    data['source'] = str(path)
    path.write_text(json.dumps(data, indent=1) + '\n', encoding='utf8')
    return path


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
def chunk_seconds(baseline, rows, buckets):
    """Modeled seconds for one chunk of a table with rows rows split into buckets buckets."""
    return baseline.chunk_overhead_s + baseline.scan_s_per_row * rows + baseline.insert_s_per_row * rows / float(buckets)


def reference_seconds(baseline, rows):
    """Modeled seconds for a reference table: one scan of the source and two inserts at the insert rate.

    The copy stages the rows in a sharded staging table and inserts them again into the reference
    table (copy.Runner.copy_reference), so the rows pass the insert rate twice in one bucket.
    """
    return baseline.chunk_overhead_s + baseline.scan_s_per_row * rows + 2.0 * baseline.insert_s_per_row * rows


def table_worker_seconds(baseline, rows, buckets):
    """Modeled worker seconds for a whole table: buckets times the chunk time."""
    return buckets * chunk_seconds(baseline, rows, buckets)


@dataclasses.dataclass
class Schedule:
    """Result of the list-scheduling simulation."""

    wall_seconds: float
    worker_seconds: float
    longest_chunk_seconds: float
    per_table: dict

    @property
    def per_table_minutes(self):
        """{table: (start_min, end_min)} relative to the copy start."""
        return {name: (round(s / 60.0, 1), round(e / 60.0, 1)) for name, (s, e, _) in self.per_table.items()}


def simulate_rows(jobs, workers, baseline, limits=None, names=None):
    """List scheduling of (rows, buckets) jobs over workers with per-table concurrency limits.

    jobs: list of (rows, buckets) or (rows, buckets, reference), a reference table being one
    chunk at reference_seconds; names: optional table names in the same order (index when
    absent); limits: {name: max in-flight chunks}, else common.auto_table_limit(workers), as
    the copy does under table_concurrency auto. Chunks are offered in table order (largest
    table first, as the caller sorted them), each chunk taking its modeled seconds. A worker
    takes the first chunk whose table is below its limit. Returns a Schedule.
    """
    workers = max(1, int(workers))
    names = list(names) if names is not None else [str(i) for i in range(len(jobs))]
    limits = dict(limits or {})
    default_limit = auto_table_limit(workers)
    queue = []
    for name, job in zip(names, jobs):
        rows, buckets = job[0], job[1]
        reference = len(job) > 2 and bool(job[2])
        seconds = reference_seconds(baseline, rows) if reference else chunk_seconds(baseline, rows, buckets)
        for b in range(1 if reference else int(buckets)):
            queue.append((name, b, seconds))
    per_table = {name: [None, 0.0, 0.0] for name in names}
    in_flight = {name: 0 for name in names}
    free = [(0.0, w) for w in range(workers)]
    heapq.heapify(free)
    running = []  # (end_time, worker, table)
    now = 0.0
    longest = max((s for _, _, s in queue), default=0.0)
    worker_seconds = sum(s for _, _, s in queue)
    while queue or running:
        progressed = False
        while free and queue:
            idx = next((i for i, (name, _, _) in enumerate(queue)
                        if in_flight[name] < limits.get(name, default_limit)), None)
            if idx is None:
                break
            name, bucket, seconds = queue.pop(idx)
            start, worker = heapq.heappop(free)
            start = max(start, now)
            end = start + seconds
            in_flight[name] += 1
            entry = per_table[name]
            entry[0] = start if entry[0] is None else min(entry[0], start)
            entry[1] = max(entry[1], end)
            entry[2] += seconds
            heapq.heappush(running, (end, worker, name))
            progressed = True
        if running:
            end, worker, name = heapq.heappop(running)
            now = end
            in_flight[name] -= 1
            heapq.heappush(free, (end, worker))
        elif not progressed:
            break
    wall = max((e for _, e, _ in per_table.values()), default=0.0)
    return Schedule(wall_seconds=wall, worker_seconds=worker_seconds, longest_chunk_seconds=longest,
                    per_table={name: (s or 0.0, e, w) for name, (s, e, w) in per_table.items()})


def simulate(plans, workers, baseline):
    """Simulate the copy of TablePlan objects (name, est_rows, n_buckets, concurrency, reference)."""
    jobs = [(p.est_rows, p.n_buckets, getattr(p, 'reference', False)) for p in plans]
    limits = {p.name: getattr(p, 'concurrency', None) for p in plans if getattr(p, 'concurrency', None)}
    return simulate_rows(jobs, workers, baseline, limits, names=[p.name for p in plans])


# ---------------------------------------------------------------------------
# Estimate with allowances and bands
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class Estimate:
    """Copy wall time, allowances per step and the O2 to O11 total, each with low and high bands."""

    workers: int
    baseline: Baseline
    schedule: Schedule
    allowances: list
    band_percent: int
    tables: int
    rows: int
    size_gib: float

    @property
    def copy_seconds(self):
        """Expected copy wall seconds from the schedule."""
        return self.schedule.wall_seconds

    @property
    def allowance_seconds(self):
        """Sum of the fixed allowances."""
        return sum(s for _, s in self.allowances)

    @property
    def total_seconds(self):
        """Expected O2 to O11 time: copy plus every allowance."""
        return self.copy_seconds + self.allowance_seconds

    def band(self, seconds):
        """(low, expected, high) for a duration using the band percent."""
        f = self.band_percent / 100.0
        return seconds * (1 - f), seconds, seconds * (1 + f)

    @property
    def copy_band(self):
        """(low, expected, high) copy wall seconds."""
        return self.band(self.copy_seconds)

    @property
    def total_band(self):
        """(low, expected, high) O2 to O11 seconds."""
        return self.band(self.total_seconds)

    @property
    def rows_per_hour(self):
        """Modeled aggregate rows per hour of copy wall time."""
        return self.rows / self.copy_seconds * 3600 if self.copy_seconds else 0.0

    def table_minutes(self, name):
        """(low, expected, high) wall minutes for one table from the schedule."""
        start, end, _ = self.schedule.per_table.get(name, (0.0, 0.0, 0.0))
        low, expected, high = self.band(end - start)
        return round(low / 60.0, 1), round(expected / 60.0, 1), round(high / 60.0, 1)


def allowances(config, plans, baseline, workers, pipeline_count=0):
    """The fixed allowances of section 10.6 as (label, seconds), in step order."""
    total_rows = sum(p.est_rows for p in plans)
    items = [
        ('O3 freeze', ALLOWANCE_MINUTES['freeze'] * 60.0),
        ('O5 create replacement', ALLOWANCE_MINUTES['create_replacement'] * 60.0),
        ('O6 count proof', 2.0 * total_rows * baseline.scan_s_per_row / max(1, workers)),
        ('O7 post-copy', ALLOWANCE_MINUTES['post_copy'] * 60.0),
        ('O8 fingerprints (budget)', config.verify.fingerprint_budget_minutes * 60.0 if 'fingerprint' in config.verify.tiers else 0.0),
        ('O9 pipelines', PIPELINE_MINUTES_EACH * 60.0 * pipeline_count),
        ('O10 switch', ALLOWANCE_MINUTES['switch'] * 60.0),
        ('O11 release', ALLOWANCE_MINUTES['release'] * 60.0),
    ]
    return items


def estimate(plans, config, baseline=None, pipeline_count=0, workers=None):
    """Build the Estimate for a plan list under the configured copy settings."""
    baseline = baseline or DEFAULT_BASELINE
    workers = int(workers or config.copy.workers)
    schedule = simulate(plans, workers, baseline)
    return Estimate(workers=workers, baseline=baseline, schedule=schedule,
                    allowances=allowances(config, plans, baseline, workers, pipeline_count),
                    band_percent=config.estimate.band_percent, tables=len(plans), rows=sum(p.est_rows for p in plans),
                    size_gib=sum(p.size_gib or 0.0 for p in plans))


@dataclasses.dataclass
class WindowFit:
    """The estimate placed inside the window: deadline, finish times and spare."""

    window_start: dt.datetime
    window_end: dt.datetime
    deadline: dt.datetime
    copy_end: tuple
    total_end: tuple
    longest_chunk_seconds: float

    @property
    def fits(self):
        """True when the copy ends before the abort deadline even at the high band and O11 ends inside the window."""
        return self.copy_end[2] <= self.deadline and self.total_end[2] <= self.window_end

    @property
    def spare_seconds(self):
        """Seconds between the high-band copy end and the deadline (negative when it does not fit)."""
        return (self.deadline - self.copy_end[2]).total_seconds()


def window_fit(est, window):
    """Place an Estimate in a Window: deadline = end minus restart allowance minus buffer minus longest chunk."""
    deadline = window.abort_deadline(est.schedule.longest_chunk_seconds)
    before_copy = sum(s for label, s in est.allowances if label.startswith(('O3', 'O5')))
    copy_end = tuple(window.start + dt.timedelta(seconds=before_copy + s) for s in est.copy_band)
    total_end = tuple(window.start + dt.timedelta(seconds=s) for s in est.total_band)
    return WindowFit(window.start, window.end, deadline, copy_end, total_end, est.schedule.longest_chunk_seconds)


def fit_lines(est, fit):
    """Human lines for the plan sheet and the profile output."""
    low, expected, high = est.copy_band
    tl, te, th = est.total_band
    lines = ['Estimate   copy %s expected (%s low, %s high) for %d tables, %s rows, %.0f GiB, %d workers' % (
        format_duration(expected), format_duration(low), format_duration(high), est.tables, _human_rows(est.rows), est.size_gib, est.workers),
        '           total O2 to O11: %s expected (%s low, %s high)' % (format_duration(te), format_duration(tl), format_duration(th))]
    if fit is not None:
        verdict = 'FITS' if fit.fits else 'DOES NOT FIT'
        spare = fit.spare_seconds
        lines.append('           copy ends %s expected, %s at high; deadline %s: %s, %s %s at high' % (
            iso(fit.copy_end[1]), iso(fit.copy_end[2]), iso(fit.deadline), verdict,
            format_duration(abs(spare)), 'spare' if spare >= 0 else 'short'))
    return lines


def _human_rows(rows):
    if rows >= 1e9:
        return '%.2f B' % (rows / 1e9)
    if rows >= 1e6:
        return '%.1f M' % (rows / 1e6)
    return str(rows)


# ---------------------------------------------------------------------------
# Calibration (C2)
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class Calibration:
    """What calibrate measured and fitted from a journal."""

    baseline: Baseline
    chunks: int
    tables: int
    rows: int
    worker_seconds: float
    wall_seconds: float
    fitted_overhead_s: float
    residual_rms_s: float
    first_started: str
    last_ended: str

    @property
    def rows_per_hour(self):
        """Observed rows inserted per hour of wall time over the fitted chunks."""
        return self.rows / self.wall_seconds * 3600 if self.wall_seconds else 0.0

    def gib_per_hour(self, size_gib):
        """GiB per hour of wall time for a given total size, or None when the size is unknown."""
        if size_gib is None or not self.wall_seconds:
            return None
        return size_gib / self.wall_seconds * 3600


def _parse(ts):
    return dt.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=dt.timezone.utc)


def _solve3(m, v):
    rows = [list(r) + [val] for r, val in zip(m, v)]
    n = 3
    for i in range(n):
        pivot = max(range(i, n), key=lambda r: abs(rows[r][i]))
        rows[i], rows[pivot] = rows[pivot], rows[i]
        if abs(rows[i][i]) < 1e-300:
            raise KitError('Calibration needs more variety in chunk sizes; the fit is singular')
        for r in range(n):
            if r != i:
                factor = rows[r][i] / rows[i][i]
                rows[r] = [a - factor * b for a, b in zip(rows[r], rows[i])]
    return [rows[i][n] / rows[i][i] for i in range(n)]


def read_chunks(journal_path, until=None, first_pass_only=True):
    """(table_rows, rows_inserted, seconds, started, ended) for the done chunks of a journal, read-only.

    first_pass_only keeps chunks with one attempt (re-copies carry lock waits and pauses);
    until keeps chunks that ended before that UTC time. Chunk rows of a fresh-table load
    (error FRESH_LOAD_ERROR: bucket 0 holds every row, the duration is zero) are skipped.
    """
    path = Path(journal_path)
    if not path.is_file():
        raise KitError('Journal not found: %s' % path)
    try:
        conn = sqlite3.connect(read_only_uri(path), uri=True)
    except sqlite3.DatabaseError as exc:
        raise KitError('Journal %s could not be opened (%s)' % (path, type(exc).__name__))
    try:
        try:
            tables = {r[0]: int(r[1]) for r in conn.execute('SELECT table_name, est_rows FROM tables')}
            rows = conn.execute("SELECT table_name, rows_inserted, started_utc, ended_utc, attempts FROM chunks "
                                "WHERE state = 'done' AND started_utc IS NOT NULL AND ended_utc IS NOT NULL "
                                "AND (error IS NULL OR error <> ?)", (FRESH_LOAD_ERROR,)).fetchall()
        except sqlite3.DatabaseError as exc:
            raise KitError('Journal %s has no readable tables and chunks (%s)' % (path, type(exc).__name__))
    finally:
        conn.close()
    cut = parse_utc(until, 'until') if until else None
    out = []
    for table, inserted, started, ended, attempts in rows:
        if first_pass_only and int(attempts or 0) != 1:
            continue
        if table not in tables:
            continue
        s, e = _parse(started), _parse(ended)
        if cut is not None and e >= cut:
            continue
        out.append((tables[table], int(inserted or 0), (e - s).total_seconds(), s, e, table))
    return out


def calibrate(journal_path, until=None, first_pass_only=True, workers=None, compute_size=None, run_id=None,
              default_overhead=DEFAULT_CHUNK_OVERHEAD_S):
    """Fit scan, insert and overhead by least squares on a journal's first-pass chunks.

    duration = overhead + scan * table_rows + insert * rows_inserted, three free parameters.
    The stored chunk_overhead_s is the larger of the shipped default and the fitted value, so
    the model never gets less conservative than the default; the fitted value is kept as
    fitted_overhead_s. Returns a Calibration. Reads the journal file only.
    """
    points = read_chunks(journal_path, until, first_pass_only)
    if len(points) < 4:
        raise KitError('Calibration needs at least 4 first-pass chunks; found %d' % len(points))
    m = [[0.0] * 3 for _ in range(3)]
    v = [0.0] * 3
    for n, r, d, _, _, _ in points:
        xs = (float(n), float(r), 1.0)
        for i in range(3):
            v[i] += xs[i] * d
            for j in range(3):
                m[i][j] += xs[i] * xs[j]
    scan, insert, overhead = _solve3(m, v)
    if scan <= 0 or insert <= 0:
        raise KitError('Calibration produced a non-positive rate (scan %.3e, insert %.3e); check the journal timings' % (scan, insert))
    residuals = [d - (overhead + scan * n + insert * r) for n, r, d, _, _, _ in points]
    rms = (sum(x * x for x in residuals) / len(residuals)) ** 0.5
    baseline = Baseline(scan_s_per_row=scan, insert_s_per_row=insert, chunk_overhead_s=max(default_overhead, overhead),
                        workers=int(workers or DEFAULT_WORKERS), compute_size=compute_size or DEFAULT_COMPUTE_SIZE,
                        run_id=run_id or Path(journal_path).parent.name or 'journal', fitted_utc=iso(utc_now()),
                        source='calibrate', chunks=len(points), fitted_overhead_s=overhead)
    first = min(s for _, _, _, s, _, _ in points)
    last = max(e for _, _, _, _, e, _ in points)
    return Calibration(baseline=baseline, chunks=len(points), tables=len({t for *_, t in points}),
                       rows=sum(r for _, r, _, _, _, _ in points), worker_seconds=sum(d for _, _, d, _, _, _ in points),
                       wall_seconds=(last - first).total_seconds(), fitted_overhead_s=overhead, residual_rms_s=rms,
                       first_started=iso(first), last_ended=iso(last))


def calibration_lines(cal, size_gib=None):
    """Human summary of a calibration for the terminal and the log."""
    b = cal.baseline
    lines = [
        'calibrate: %d first-pass chunks over %d tables, %s rows, %.0f worker-seconds, %s wall (%s to %s)' % (
            cal.chunks, cal.tables, _human_rows(cal.rows), cal.worker_seconds, format_duration(cal.wall_seconds),
            cal.first_started, cal.last_ended),
        'fit: scan_s_per_row = %.3e (%.1f M rows per second per worker), insert_s_per_row = %.3e (%.0f k rows per second per worker)' % (
            b.scan_s_per_row, b.scan_rows_per_second / 1e6, b.insert_s_per_row, b.insert_rows_per_second / 1e3),
        'fit: chunk_overhead_s fitted %.2f, stored %.2f; residual rms %.1f s per chunk' % (
            cal.fitted_overhead_s, b.chunk_overhead_s, cal.residual_rms_s),
        'observed: %.2f billion rows per hour%s' % (cal.rows_per_hour / 1e9, (
            ', about %.0f GiB per hour' % cal.gib_per_hour(size_gib)) if size_gib else ', GiB per hour needs --size-gib or table sizes'),
    ]
    return lines


# ---------------------------------------------------------------------------
# Plan sheet (P4, Appendix B)
# ---------------------------------------------------------------------------
def plan_sheet(config, db, run_id, inventory, est, fit, rehearsal=None, env=None):
    """The plan sheet lines for one database."""
    env = {} if env is None else env
    lines = ['%s    run %s    database %s' % ('SingleStore Database Rebuild Kit', run_id, db.source)]
    window = config.window
    if window is not None:
        lines.append('Window     %s to %s (%s)' % (iso(window.start), iso(window.end), format_duration(window.duration.total_seconds())))
        if fit is not None:
            lines.append('Deadline   %s abort for the copy (end minus %d m restart, %d m buffer, %s longest chunk)' % (
                iso(fit.deadline), window.restart_allowance_minutes, window.buffer_minutes, format_duration(fit.longest_chunk_seconds)))
    else:
        lines.append('Window     none configured: no abort deadline; the copy and the switch run without a time limit')
    lines.append('Freeze     %s; application stop owner: <name recorded in the P4 gate note>' % (
        'Path A read-only attachment' if config.freeze.mode == 'readonly' else 'Path B lockout (revoke and kill loop)'))
    lines.extend(fit_lines(est, fit))
    pipelines = (inventory or {}).get('pipelines') or {}
    if pipelines.get('captured'):
        items = pipelines['items']
        by_type = {}
        for item in items:
            by_type[item['source_type']] = by_type.get(item['source_type'], 0) + 1
        routes = {'kit': '1 (kit replays)', 'customer': '2 (customer runs the placeholder file)'}
        creds = []
        for source_type in sorted(by_type):
            var = config.pipelines.credentials_env.get(source_type)
            creds.append('%s env %s' % (source_type, ('set' if var and env.get(var) else 'not set') if var else 'not configured'))
        lines.append('Pipelines  %d captured (%s); route %s; credentials: %s' % (
            len(items), ', '.join('%d %s' % (n, t) for t, n in sorted(by_type.items())), routes[config.pipelines.route], ', '.join(creds)))
        exposure = est.total_band[2]
        verdicts = []
        for source_type in sorted(by_type):
            hours = config.pipelines.retention_hours.get(source_type)
            if hours is None:
                verdicts.append('%s not stated' % source_type)
            else:
                verdicts.append('%s %s h, margin %s' % (source_type, hours, 'OK' if hours * 3600.0 > exposure else 'SHORT'))
        lines.append('           retention stated by customer: %s; stop to start at high: %s; app-managed: %d' % (
            '; '.join(verdicts), format_duration(exposure), sum(1 for i in items if i['app_managed'])))
    else:
        lines.append('Pipelines  not captured (%s)' % pipelines.get('note', 'no inventory'))
    accounts = (inventory or {}).get('accounts') or []
    lines.append('Accounts   write-capable: %s%s' % (', '.join(a['user'] for a in accounts) or 'none found',
                                                     ' (Path B would revoke these on every database)' if config.freeze.mode == 'lockout' else ''))
    listed, refusals = unmigrated(config, db, (inventory or {}).get('routines') or [], pipelines.get('items') or [])
    lines.append('Routines   %s' % ('; '.join(['%s not migrated' % label for label in listed] + ['REFUSED: ' + r for r in refusals]) or 'none'))
    lines.append('Rehearsal  %s' % (rehearsal or 'none recorded'))
    if config.closeout.retain_days < 90:
        lines.append('Retention  closeout.retain_days is %d, under the 90 day default: the retained copy %s may be deleted by the customer '
                     '(s2rebuild cleanup --delete) that soon after the switch' % (config.closeout.retain_days, db.retained))
    return lines
