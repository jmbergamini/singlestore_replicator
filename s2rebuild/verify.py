"""s2rebuild verify: tier 1 counts, tier 2 newest values, tier 3 grouped fingerprints, and the O7 stale check.

Author: John Bergamini

Section 10.10 of the architecture. Every table records verified_at_utc when a tier matches;
O10 refuses to switch a database whose newest verified_at_utc is older than freeze_time. A
mismatching fingerprint group names the bucket to re-copy when the group count equals the
table's bucket count. Statements issued, all through the read guard: SELECT COUNT(*), SELECT
MAX(time column), SELECT MIN and MAX of the first key column, and the grouped fingerprint
SELECT with CRC32 sums. Nothing else.
"""
import dataclasses

from . import estimate
from .common import KitError
from .sqlutil import ident, qualified

TIER_COUNT, TIER_NEWEST, TIER_FINGERPRINT = 'count', 'newest', 'fingerprint'
MATCH, MISMATCH, COUNT_ONLY, SAMPLED = 'match', 'mismatch', 'count only', 'sampled'
VALUE_COLUMNS = 4


def count_sql(db, table):
    """SELECT COUNT(*) for one table."""
    return 'SELECT COUNT(*) AS n FROM ' + qualified(db, table)


def newest_sql(db, table, time_column):
    """SELECT MAX(time column) for one table."""
    return 'SELECT MAX(%s) AS newest FROM %s' % (ident(time_column), qualified(db, table))


def key_range_sql(db, table, key_column):
    """SELECT MIN and MAX of the first key column."""
    return 'SELECT MIN(%s) AS lo, MAX(%s) AS hi FROM %s' % (ident(key_column), ident(key_column), qualified(db, table))


def value_expr(plan):
    """The value hash input: the time column, or up to four non-key columns, each with COALESCE."""
    if plan.time_column:
        columns = [plan.time_column]
    else:
        columns = [c for c in plan.columns if c not in plan.key_columns][:VALUE_COLUMNS] or plan.columns[:VALUE_COLUMNS]
    return "CONCAT_WS('|', " + ', '.join("COALESCE(%s, '')" % ident(c) for c in columns) + ')'


def fingerprint_groups(plan, config):
    """The table's bucket count, or verify.fingerprint_groups for a single-bucket table."""
    return plan.n_buckets if plan.n_buckets > 1 else int(config.verify.fingerprint_groups)


def fingerprint_sql(db, plan, groups, sample_percent=None):
    """The grouped fingerprint SELECT of section 10.10, optionally over a CRC32 MOD 100 sample."""
    sql = ('SELECT CRC32(%s) MOD %d AS grp, COUNT(*) AS n, SUM(CRC32(%s)) AS key_hash, SUM(CRC32(%s)) AS value_hash FROM %s' % (
        plan.key_expr, groups, plan.key_expr, value_expr(plan), qualified(db, plan.name)))
    if sample_percent is not None:
        sql += ' WHERE CRC32(%s) MOD 100 < %d' % (plan.key_expr, int(sample_percent))
    return sql + ' GROUP BY grp'


def fingerprint_cost_seconds(plan, baseline):
    """Two scans of the table at the baseline scan rate."""
    return 2.0 * plan.est_rows * baseline.scan_s_per_row


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

    @property
    def ok(self):
        """True unless the result is a mismatch."""
        return self.result != MISMATCH


def _scalar(rows, key):
    if not rows:
        return None
    row = rows[0]
    return row[key] if isinstance(row, dict) else row[0]


def _text(value):
    return None if value is None else str(value)


class Verifier:
    """Runs the tiers for a plan list through one read-guarded session and journals every result."""

    def __init__(self, session, source_db, target_db, plans, journal, config, baseline=None, log=None):
        self.session = session
        self.source_db = source_db
        self.target_db = target_db
        self.plans = plans
        self.journal = journal
        self.config = config
        self.baseline = baseline or estimate.DEFAULT_BASELINE
        self.log = log or (lambda msg: None)

    def _record(self, r):
        self.journal.record_verification(r.table, r.tier, r.result, r.source_value, r.target_value, r.sql_source, r.sql_target, r.detail)
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
            rs, rt = self.session.query(sql_s), self.session.query(sql_t)
            lo_s, hi_s = (_scalar(rs, 'lo'), (rs[0]['hi'] if rs and isinstance(rs[0], dict) else (rs[0][1] if rs else None)))
            lo_t, hi_t = (_scalar(rt, 'lo'), (rt[0]['hi'] if rt and isinstance(rt[0], dict) else (rt[0][1] if rt else None)))
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

    def tier3(self, plan, sampled=False):
        """Tier 3: grouped fingerprints on both sides, full or sampled beyond the budget."""
        groups = fingerprint_groups(plan, self.config)
        percent = int(self.config.verify.sample_percent_over_budget) if sampled else None
        sql_s = fingerprint_sql(self.source_db, plan, groups, percent)
        sql_t = fingerprint_sql(self.target_db, plan, groups, percent)
        side_s = {int(r['grp']): (int(r['n']), int(r['key_hash']), int(r['value_hash'])) for r in self.session.query(sql_s)}
        side_t = {int(r['grp']): (int(r['n']), int(r['key_hash']), int(r['value_hash'])) for r in self.session.query(sql_t)}
        bad = sorted(g for g in set(side_s) | set(side_t) if side_s.get(g) != side_t.get(g))
        result = MISMATCH if bad else (SAMPLED if sampled else MATCH)
        detail = '%d groups compared%s%s' % (len(set(side_s) | set(side_t)), ', sampled %d percent' % percent if sampled else '',
                                             (', mismatched groups ' + ', '.join(str(g) for g in bad)) if bad else '')
        r = TierResult(plan.name, TIER_FINGERPRINT, result, len(side_s), len(side_t), sql_s, sql_t, detail, tuple(bad), sampled)
        self._record(r)
        if result == SAMPLED:
            self.journal.set_table_attr(plan.name, 'verified_at_utc', self.journal.verifications(plan.name, TIER_FINGERPRINT)[-1]['checked_utc'])
        return r

    def run(self, tiers=None, budget_minutes=None):
        """Run the configured tiers over every table; tier 3 smallest first until the budget is spent, then sampled.

        A table with any mismatch in this run ends without a verified_at_utc stamp, whatever
        its other tiers said.
        """
        tiers = list(tiers if tiers is not None else self.config.verify.tiers)
        budget = (self.config.verify.fingerprint_budget_minutes if budget_minutes is None else budget_minutes) * 60.0
        results = []
        for plan in self.plans:
            if TIER_COUNT in tiers:
                results.append(self.tier1(plan))
            if TIER_NEWEST in tiers:
                results.extend(self.tier2(plan))
        if TIER_FINGERPRINT in tiers:
            spent = 0.0
            for plan in sorted(self.plans, key=lambda p: (p.est_rows, p.name)):
                cost = fingerprint_cost_seconds(plan, self.baseline)
                sampled = spent + cost > budget
                if not sampled:
                    spent += cost
                results.append(self.tier3(plan, sampled=sampled))
        for table in sorted({r.table for r in results if r.result == MISMATCH}):
            self.journal.set_table_attr(table, 'verified_at_utc', None)
        return results


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


RESULT_PRECEDENCE = (MISMATCH, SAMPLED, COUNT_ONLY, MATCH)


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
    """Per tier: tables matched, mismatched, sampled, count only, one entry per table (mismatch wins)."""
    summary = {}
    for (tier, table), result in table_result(results).items():
        entry = summary.setdefault(tier, {'match': set(), 'mismatch': set(), 'sampled': set(), 'count only': set()})
        entry[result].add(table)
    return {tier: {k: sorted(v) for k, v in entry.items()} for tier, entry in summary.items()}


def summary_lines(results):
    """Human lines, one per tier."""
    lines = []
    for tier, entry in summarize(results).items():
        lines.append('tier %s: %d match, %d mismatch, %d sampled, %d count only%s' % (
            tier, len(entry['match']), len(entry['mismatch']), len(entry['sampled']), len(entry['count only']),
            (': mismatch on ' + ', '.join(entry['mismatch'])) if entry['mismatch'] else ''))
    return lines


def stale_check(verifier):
    """The O7 stale check: tiers 1 and 2 on every table; returns the results."""
    return verifier.run(tiers=[TIER_COUNT, TIER_NEWEST])


def dry_run_statements(session, source_db, target_db, plans, config, tiers=None):
    """Feed every verification statement the given tiers (default: the configured ones) would issue to a dry-run session."""
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
                session.query(fingerprint_sql(db, plan, fingerprint_groups(plan, config)))
