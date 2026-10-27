"""s2rebuild tests test_verify: tier selection, grouped fingerprints and bucket mapping, parallel tier 3, range sampling, the budget
deadline, the rerun flags, acceptance without a rerun, verified_at rule.

Author: John Bergamini
"""
import functools
import unittest
from unittest import mock

from s2rebuild import copy as C
from s2rebuild import estimate as E
from s2rebuild import inventory as I
from s2rebuild import steps
from s2rebuild import verify as V
from s2rebuild.common import EXIT_DONE, KitError
from s2rebuild.connect import DryRunSession, Log
from s2rebuild.sqlutil import read_guard, runner_guard
from tests.fakedb import SimulatedCrash, crc32, key_text, multiset
from tests.helpers import FAULT_POINTS, crash_journal
from tests.test_copy import FIXTURE_TABLES, SRC, TGT, Harness

LATE_ROW = {'a': 'late', 'b': '2026-03-03 00:00:00.000000', 'c': 777, 'note': 'arrived after the copy'}


def copied(test, **kw):
    h = Harness(test, **kw)
    assert h.o5_o6().status == steps.PASSED, h.text()
    return h


def stale_copy(test):
    """A copied harness whose source t_comp gained a row after the copy, so the O7 stale check re-copies t_comp (one bucket)."""
    h = copied(test)
    h.fake.rows(SRC, 't_comp').append(dict(LATE_ROW))
    return h


def arm_recopy(test, h, table, statement, point):
    """Arm one of FAULT_POINTS around the re-copy DELETE or INSERT of a marked bucket of table.

    before: the server never sees the statement. after: the server applied it and the reply was
    lost. journal: the statement ran and the worker dies (SimulatedCrash) before the journal
    records it: the reconcile_delete event after the DELETE, mark_done after the INSERT.
    """
    if point == 'journal':
        if statement == 'DELETE':
            return crash_journal(test, h.journal, 'event', match=lambda kind, detail: kind == 'reconcile_delete' and detail.startswith(table + ' '))
        return crash_journal(test, h.journal, 'mark_done', match=lambda t, bucket, rows: t == table)
    h.fake.fail_statement(r'^%s (FROM|INTO) `%s`\.`%s`' % (statement, TGT, table), point)
    return h.fake.faults_fired


def table_writes(h, table, start=0):
    """The DELETE and INSERT statements the fake applied to the replacement table, from position start of its write list."""
    return [w.split(' ')[0] for w in h.fake.writes[start:] if w.startswith(('DELETE FROM `%s`.`%s`' % (TGT, table), 'INSERT INTO `%s`.`%s`' % (TGT, table)))]


def assert_reinserted_once(test, h, table, start):
    """Every re-copy INSERT of table follows its DELETE, and the replacement holds the source rows exactly once."""
    seen = table_writes(h, table, start)
    for i, kind in enumerate(seen):
        if kind == 'INSERT':
            test.assertEqual(seen[i - 1] if i else None, 'DELETE', 'an INSERT of a marked bucket always follows its DELETE: %s' % seen)
    test.assertEqual(multiset(h.fake.rows(TGT, table)), multiset(h.fake.rows(SRC, table)), 'no row lost or doubled')


def auto_increment_on_t_comp(inv):
    """Inventory mutation: t_comp has the AUTO_INCREMENT column c (source counter 500), so O7 sets its replacement counter."""
    for t in inv['tables']:
        if t['name'] == 't_comp':
            t['auto_increment'] = {'column': 'c', 'counter': 500}


def counter_writes(h, table, start=0):
    """The ALTER TABLE ... AUTO_INCREMENT and AGGREGATOR SYNC AUTO_INCREMENT statements the fake applied to the replacement table."""
    return [w for w in h.fake.writes[start:] if w.startswith(('ALTER TABLE `%s`.`%s` AUTO_INCREMENT' % (TGT, table),
                                                             'AGGREGATOR SYNC AUTO_INCREMENT ON `%s`.`%s`' % (TGT, table)))]


def assert_counter_follows(test, h, table, after_kind):
    """The newest counter event of table comes after the newest event of after_kind, and the replacement counter is the one it set."""
    import json
    events = h.journal.events()
    last = max(i for i, e in enumerate(events) if e['kind'] == after_kind)
    counters = [(i, json.loads(e['detail'])) for i, e in enumerate(events) if e['kind'] == 'counter']
    counters = [(i, c) for i, c in counters if c['table'] == table]
    test.assertTrue(counters and counters[-1][0] > last, 'a counter event of %s follows the %s event' % (table, after_kind))
    test.assertIsNotNone(counters[-1][1]['target_after'], 'the counter is set, not left at the default')
    test.assertEqual(h.fake.databases[TGT]['tables'][table]['auto_increment'], counters[-1][1]['target_after'])


SORTED_TABLES = dict(FIXTURE_TABLES)
_SINGLE = FIXTURE_TABLES['t_single']
# t_single as a columnstore table sorted on its integer column n, so a sampled fingerprint ranges over n
SORTED_TABLES['t_single'] = (_SINGLE[0], _SINGLE[1], _SINGLE[2], _SINGLE[3].replace('PRIMARY KEY (`id`)\n', 'PRIMARY KEY (`id`),\n  SORT KEY (`n`)\n'))


def sampled_harness(test, threshold, percent=5, time_column=None, **kw):
    """A copied harness whose t_single carries SORT KEY (`n`), with verify.full_fingerprint_max_rows at threshold and the sample percent."""
    def mutate(d):
        d.update(verify=dict({'full_fingerprint_max_rows': threshold, 'sample_percent': percent}, **kw))
        if time_column:
            d['databases'][0].update(time_column_candidates=[time_column])
    return copied(test, tables=SORTED_TABLES, mutate=mutate)


def fingerprints_since(h, start):
    """The fingerprint statements the fake saw from position start of its statement list."""
    return [s for s, _ in h.fake.statements[start:] if s.startswith('SELECT CRC32(')]


def run_from_o6(h, to_id='O8'):
    """The operator's rerun: s2rebuild run --from O6 --to to_id on the harness journal; returns the exit code."""
    return steps.run_steps(h.config, [h.db], {h.db.source: h.journal}, h.log, run_id=h.run_id, from_id='O6', to_id=to_id,
                           sessions=h.sessions)


class SqlTests(unittest.TestCase):
    def test_statement_shapes(self):
        plan = I.TablePlan('t', ['id', 'v', 'w', 'x', 'y', 'z'], 25_000_000, 'key', ['id'], '', 3, time_column=None)
        self.assertEqual(V.count_sql('d', 't'), 'SELECT COUNT(*) AS n FROM `d`.`t`')
        self.assertEqual(V.newest_sql('d', 't', 'u'), 'SELECT MAX(`u`) AS newest FROM `d`.`t`')
        self.assertEqual(V.key_range_sql('d', 't', 'id'), 'SELECT MIN(`id`) AS lo, MAX(`id`) AS hi FROM `d`.`t`')
        self.assertEqual(V.value_expr(plan), "CONCAT_WS('|', COALESCE(`v`, ''), COALESCE(`w`, ''), COALESCE(`x`, ''), COALESCE(`y`, ''))",
                         'up to four non-key columns')
        plan.time_column = 'u'
        self.assertEqual(V.value_expr(plan), "CONCAT_WS('|', COALESCE(`u`, ''), COALESCE(`v`, ''), COALESCE(`w`, ''), COALESCE(`x`, ''), "
                                             "COALESCE(`y`, ''))", 'the time column plus up to four value columns, always')
        self.assertEqual(V.value_expr(plan, 1), "CONCAT_WS('|', COALESCE(`u`, ''), COALESCE(`v`, ''))", 'verify.value_columns bounds the rest')
        plan.column_types = {'v': 'json', 'w': 'blob', 'x': 'double'}
        self.assertEqual(V.value_columns_for(plan), ['u', 'x', 'y', 'z'], 'unsupported types are left out')
        sql = V.fingerprint_sql('d', plan, 3, value_columns=1)
        self.assertEqual(sql, "SELECT CRC32(`id`) MOD 3 AS grp, COUNT(*) AS n, SUM(CRC32(`id`)) AS key_hash, "
                              "SUM(CRC32(CONCAT_WS('|', COALESCE(`u`, ''), COALESCE(`x`, '')))) AS value_hash FROM `d`.`t` GROUP BY grp")
        sampled = V.fingerprint_sql('d', plan, 64, V.range_predicate('id', 10, 19))
        self.assertTrue(sampled.endswith('WHERE `id` >= 10 AND `id` <= 19 GROUP BY grp'))
        for text in (sql, sampled, V.newest_sql('d', 't', 'u')):
            read_guard().check(text)
        self.assertEqual(V.sample_cut(0, 699, 10), 69, 'an integer span: the floor')
        self.assertEqual(str(V.sample_cut('1.5', '3.5', 50)), '2.5', 'a decimal span')
        self.assertEqual(V.sql_literal(V.sample_cut('2026-01-01 00:00:00', '2026-01-02 00:00:00', 25)), "'2026-01-01 06:00:00.000000'")
        self.assertIsNone(V.sample_cut('id-1', 'id-9', 5), 'text cannot be spanned')
        self.assertEqual(V.sql_literal("it's"), "'it''s'")
        from tests.helpers import config_from
        cfg = config_from()
        self.assertEqual(V.fingerprint_groups(plan, cfg), 3)
        self.assertEqual(V.fingerprint_groups(I.TablePlan('t', ['a'], 10, 'row', [], '', 1), cfg), 64)
        self.assertAlmostEqual(V.fingerprint_cost_seconds(plan, E.DEFAULT_BASELINE), 2 * 25_000_000 * E.DEFAULT_SCAN_S_PER_ROW)


class TierTests(unittest.TestCase):
    def test_all_tiers_match_after_a_clean_copy(self):
        h = copied(self)
        session = h.sessions.runner(read_guard())
        verifier = V.Verifier(session, SRC, TGT, h.plans(), h.journal, h.config, log=h.log)
        results = verifier.run()
        tiers = {r.tier for r in results}
        self.assertEqual(tiers, {V.TIER_COUNT, V.TIER_NEWEST, V.TIER_FINGERPRINT})
        self.assertFalse([r for r in results if r.result == V.MISMATCH])
        count_only = [r.table for r in results if r.result == V.COUNT_ONLY]
        self.assertEqual(sorted(count_only), ['t_comp', 't_empty', 't_nokey', 't_single'], 'no fixture table has a configured time column')
        rows = h.journal.verifications()
        self.assertEqual(len(rows), len(results))
        self.assertTrue(all(t['verified_at_utc'] for t in h.journal.tables()))
        self.assertEqual(V.recopy_targets(results, h.plans()), [])
        summary = V.summarize(results)
        self.assertEqual(summary[V.TIER_FINGERPRINT]['match'], ['t_comp', 't_empty', 't_nokey', 't_single'])
        lines = V.summary_lines(results)
        self.assertTrue(any(line.startswith('tier count: 4 match') for line in lines))

    def test_time_column_drives_tier2(self):
        tables = dict(FIXTURE_TABLES)
        h = copied(self, mutate=lambda d: d['databases'][0].update(time_column_candidates=['b']))
        session = h.sessions.runner(read_guard())
        results = V.Verifier(session, SRC, TGT, h.plans(), h.journal, h.config).run(tiers=[V.TIER_NEWEST])
        comp = [r for r in results if r.table == 't_comp']
        self.assertEqual([r.detail for r in comp], ['key range of a', 'newest b'])
        self.assertTrue(all(r.result == V.MATCH for r in comp))
        self.assertIn('2026-01-01 00:00:59', comp[1].source_value)

    def test_mismatch_maps_to_the_bucket(self):
        h = copied(self)
        plans = h.plans()
        single = next(p for p in plans if p.name == 't_single')
        victim = h.fake.rows(TGT, 't_single')[5]
        bucket = crc32(key_text('`id`', victim)) % single.n_buckets
        victim['payload'] = 'tampered'
        h.fake.rows(TGT, 't_nokey').pop()
        session = h.sessions.runner(read_guard())
        results = V.Verifier(session, SRC, TGT, plans, h.journal, h.config).run()
        bad = {(r.table, r.tier): r for r in results if r.result == V.MISMATCH}
        self.assertIn(('t_single', V.TIER_FINGERPRINT), bad)
        self.assertEqual(bad[('t_single', V.TIER_FINGERPRINT)].mismatched_groups, (bucket,))
        self.assertIn(('t_nokey', V.TIER_COUNT), bad)
        self.assertNotIn(('t_single', V.TIER_COUNT), bad, 'a changed value keeps the count equal')
        targets = V.recopy_targets(results, plans)
        self.assertIn(('t_single', bucket), targets)
        self.assertIn(('t_nokey', None), targets, 'a count mismatch marks the whole table')
        self.assertEqual(len(targets), len(set(targets)), 'duplicates collapse')
        self.assertEqual(len(targets), 3, 'the fingerprint mismatch of t_nokey also names its bucket')
        verified = {t['table_name']: t['verified_at_utc'] for t in h.journal.tables()}
        self.assertIsNone(verified['t_single'], 'a mismatching table carries no verified_at')
        self.assertIsNotNone(verified['t_comp'])
        for table, b in targets:
            h.journal.mark_recopy(table, b)
        self.assertEqual(h.step('O6', options={'retry_failed': True}).status, steps.PASSED)
        again = V.Verifier(session, SRC, TGT, plans, h.journal, h.config).run()
        self.assertFalse([r for r in again if r.result == V.MISMATCH])

    def test_budget_deadline_marks_the_tables_after_it_not_fingerprinted(self):
        """verify.fingerprint_budget_minutes is an elapsed deadline (a fake clock that steps 25 s per reading): the largest table is
        fingerprinted inside the minute, the three behind it are recorded not fingerprinted, O8 ends pending, and --yes accepts the
        recorded result without a rerun."""
        h = copied(self, mutate=lambda d: d.update(verify={'fingerprint_budget_minutes': 1}))
        self.assertEqual(h.step('O7').status, steps.PASSED, h.text())
        ticks = iter(range(0, 100000, 25))
        with mock.patch.object(V, 'Verifier', functools.partial(V.Verifier, clock=lambda: float(next(ticks)))):
            out8 = h.step('O8')
        self.assertEqual((out8.status, out8.exit_code), (steps.PENDING, 4), h.text())
        self.assertIn('not fingerprinted within verify.fingerprint_budget_minutes: t_comp, t_empty, t_nokey', out8.note)
        by = {r['table_name']: r for r in h.journal.verifications(tier=V.TIER_FINGERPRINT)}
        self.assertEqual(by['t_single']['result'], V.MATCH)
        self.assertEqual({t: by[t]['result'] for t in ('t_comp', 't_empty', 't_nokey')}, {t: V.NOT_FINGERPRINTED for t in ('t_comp', 't_empty', 't_nokey')})
        self.assertEqual(by['t_comp']['detail'], 'budget: verify.fingerprint_budget_minutes 1 elapsed before this table')
        self.assertIn('tier 3 t_single: match in 25.0 s', h.text(), 'one log line per table with the elapsed seconds')
        self.assertIn('tier fingerprint: 1 match, 0 mismatch, 0 sampled, 3 not fingerprinted', h.text())
        start = len(h.fake.statements)
        out = h.step('O8', yes=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual(h.fake.statements[start:], [], 'the acceptance issues no query')
        self.assertEqual(h.journal.events(V.ACCEPTED_EVENT), ['accepted not fingerprinted %s at re-copy epoch 0 (budget)' % t for t in ('t_comp', 't_empty', 't_nokey')])

    def test_tier2_newest_mismatch_with_equal_counts(self):
        h = copied(self, mutate=lambda d: d['databases'][0].update(time_column_candidates=['b']))
        rows = h.fake.rows(TGT, 't_comp')
        newest = max(rows, key=lambda r: r['b'])
        newest['b'] = '2030-01-01 00:00:00.000000'
        session = h.sessions.runner(read_guard())
        results = V.Verifier(session, SRC, TGT, h.plans(), h.journal, h.config).run(tiers=[V.TIER_COUNT, V.TIER_NEWEST])
        bad = [r for r in results if r.result == V.MISMATCH]
        self.assertEqual([(r.table, r.tier, r.detail) for r in bad], [('t_comp', V.TIER_NEWEST, 'newest b')])
        self.assertEqual(V.recopy_targets(results, h.plans()), [('t_comp', None)])
        self.assertIsNone(next(t for t in h.journal.tables() if t['table_name'] == 't_comp')['verified_at_utc'])
        self.assertEqual(V.summarize(results)[V.TIER_NEWEST]['mismatch'], ['t_comp'], 'a mismatch wins over the matching key range row')
        self.assertEqual(V.table_result(results)[(V.TIER_NEWEST, 't_single')], V.COUNT_ONLY)
        self.assertIsNotNone(next(t for t in h.journal.tables() if t['table_name'] == 't_single')['verified_at_utc'],
                             'a sampled match still stamps verified_at')

    def test_dry_run_statements_through_a_recording_session(self):
        h = Harness(self)
        session = DryRunSession(read_guard(), h.log)
        V.dry_run_statements(session, SRC, TGT, h.plans(), h.config)
        kinds = [s.split(' ')[1] for s in session.statements]
        self.assertEqual(len(session.statements), 2 * (4 + 2 + 4), 'count for 4 tables, key range for 2 keyed tables, fingerprints for 4, both sides')
        self.assertTrue(all(k in ('COUNT(*)', 'MIN(`id`),', 'MIN(`a`),', 'CRC32(`id`)') or k.startswith(('CRC32', 'MIN', 'COUNT')) for k in kinds))


class StaleCheckAndHandlersTests(unittest.TestCase):
    def test_o7_and_o8_handlers_pass_on_a_clean_copy_and_write_the_final_workbook(self):
        from openpyxl import load_workbook
        h = copied(self)
        out7 = h.step('O7')
        self.assertEqual(out7.status, steps.PASSED, h.text())
        self.assertIn('stale check 0 mismatches after 0 re-copy round(s), counters set', out7.note)
        out8 = h.step('O8')
        self.assertEqual(out8.status, steps.PASSED, h.text())
        reports = h.tmp / 'runs' / h.run_id / SRC / 'reports'
        self.assertTrue((reports / 'final.xlsx').is_file())
        self.assertTrue((reports / 'run-report.md').is_file())
        wb = load_workbook(str(reports / 'final.xlsx'))
        self.assertEqual(wb.sheetnames[0], 'Summary')
        self.assertEqual(wb.sheetnames[1:5], ['t_single', 't_nokey', 't_comp', 't_empty'])
        self.assertEqual(wb.sheetnames[5:], ['Views', 'Routines', 'Pipelines', 'Pipeline offsets'])
        summary = {str(r[0].value): r[1].value for r in wb['Summary'].iter_rows(min_col=1, max_col=2) if r[0].value}
        self.assertEqual(summary['Rows copied'], 1500)
        self.assertEqual(summary['Tier count'], '4 match')
        self.assertEqual(summary['Tier fingerprint'], '4 match')
        self.assertIn('O6', summary)
        single = {r[0].value: r[1].value for r in wb['t_single'].iter_rows(min_col=1, max_col=2) if r[0].value}
        self.assertEqual(single['Buckets done'], 3)
        self.assertEqual(single['Rows match'], 'yes')
        self.assertEqual(single['Result'], 'match')
        self.assertIn('SUM(CRC32', single['SQL source side'])
        self.assertIn('Copy wall expected', summary)
        self.assertIn('Rows per hour actual', summary)
        self.assertEqual(summary['Write-capable'], 'none found')
        self.assertEqual(summary['Path B action'], 'none (Path A read-only attachment)')
        self.assertIn('pass-through', summary['Replacement definition'])
        self.assertEqual(summary['t_single'], 'identical')
        self.assertIsInstance(single['Source count'], int, 'tier 1 counts are numbers')
        sql_cell = next(r[1] for r in wb['t_single'].iter_rows(min_col=1, max_col=2) if r[0].value == 'SQL source side')
        self.assertTrue(sql_cell.alignment.wrap_text)
        pipes_header = [c.value for c in wb['Pipelines'][1]]
        self.assertEqual(len(pipes_header), 22)
        self.assertIn('variable set', pipes_header)
        self.assertIn('offsets verified equal', pipes_header)
        md = (reports / 'run-report.md').read_text()
        self.assertIn('## Timeline', md)
        self.assertIn('count_proof', md)
        self.assertIn(str(h.journal_path), md)

    def test_o7_stale_check_recopies_a_changed_table_and_sets_counters_afterwards(self):
        import json
        def counter(inv):
            for t in inv['tables']:
                if t['name'] == 't_comp':
                    t['auto_increment'] = {'column': 'c', 'counter': 500}
        h = copied(self, mutate_inventory=counter)
        h.fake.refuse_sharded_counter = False  # the ALTER path of the counter logic: Helios 10.8.0 refuses it on a sharded table (1749, covered by its own test)
        h.fake.rows(SRC, 't_comp').append({'a': 'late', 'b': '2026-03-03 00:00:00.000000', 'c': 777, 'note': 'arrived after the copy'})
        out7 = h.step('O7')
        self.assertEqual(out7.status, steps.PASSED, h.text())
        self.assertIn('after 1 re-copy round(s)', out7.note)
        self.assertEqual(h.journal.events('mark_recopy'), ['t_comp'])
        from tests.fakedb import multiset
        self.assertEqual(multiset(h.fake.rows(TGT, 't_comp')), multiset(h.fake.rows(SRC, 't_comp')))
        actions = [json.loads(d) for d in h.journal.events('counter')]
        self.assertEqual(actions[0]['max_value'], 777, 'the counter is computed after the re-copy, not before')
        self.assertEqual(h.fake.databases[TGT]['tables']['t_comp']['auto_increment'], 778)
        events = [e['kind'] for e in h.journal.events()]
        self.assertLess(events.index('mark_recopy'), events.index('counter'))

    def test_f4_o7_repair_leaves_o6_passed_and_o8_runnable(self):
        """O7's own re-copy redid the O6 work: O6 ends passed, O8 runs without a rerun of O6, and its rows carry the new epoch."""
        h = stale_copy(self)
        out7 = h.step('O7', redo=False)
        self.assertEqual(out7.status, steps.PASSED, h.text())
        self.assertIn('after 1 re-copy round(s)', out7.note)
        row = h.journal.step('O6')
        self.assertEqual(row['status'], 'passed', 'the completed re-copy round is the O6 gate')
        self.assertIn('re-copy round 1 of the O7 stale check', row['note'])
        self.assertEqual(h.journal.recopy_epoch('t_comp'), 1)
        o7_rows = h.journal.verifications('t_comp')
        self.assertEqual({r['epoch'] for r in o7_rows if r['result'] == 'mismatch'}, {0}, 'the mismatch was found at epoch 0')
        self.assertEqual(o7_rows[-1]['epoch'], 1, 'the clean stale check after the re-copy ran at epoch 1')
        out8 = h.step('O8', redo=False)
        self.assertEqual(out8.status, steps.PASSED, h.text())
        self.assertEqual({r['epoch'] for r in h.journal.verifications('t_comp')[len(o7_rows):]}, {1}, 'O8 verified at the new epoch')
        state = {t['table_name']: t for t in h.journal.verification_state()}
        self.assertTrue(all(t['epoch_verified'] for t in state.values()), state)

    def test_f4_o7_recopy_fault_points_resume_from_o6_and_end_with_o6_to_o8_passed(self):
        """A failure before, after, or after and before the journal update, at the re-copy DELETE and at the re-copy INSERT of the
        bucket O7 marked: O7 stops with O6 pending; the operator's run --from O6 reconciles the bucket, O7 and O8 pass, no row doubled."""
        for statement in ('DELETE', 'INSERT'):
            for point in FAULT_POINTS:
                with self.subTest(statement=statement, point=point):
                    h = stale_copy(self)
                    start = len(h.fake.writes)
                    fired = arm_recopy(self, h, 't_comp', statement, point)
                    out7 = h.step('O7', redo=False)
                    self.assertEqual(len(fired), 1, 'the fault fired')
                    self.assertEqual(out7.status, steps.INCOMPLETE, h.text())
                    self.assertIn('rerun O6 to finish the marked buckets, then O7', out7.note)
                    self.assertEqual({s: h.journal.step_status(s) for s in ('O6', 'O7')}, {'O6': 'pending', 'O7': 'pending'})
                    self.assertEqual(h.journal.events('counter'), [], 'no counter is set before the re-copy completes')
                    self.assertEqual(run_from_o6(h), EXIT_DONE, h.text())
                    self.assertEqual({s: h.journal.step_status(s) for s in ('O6', 'O7', 'O8')}, {'O6': 'passed', 'O7': 'passed', 'O8': 'passed'})
                    assert_reinserted_once(self, h, 't_comp', start)
                    self.assertNotIn('[O7] Post-copy (python; runner) database %s: already passed' % SRC, h.text())

    def test_f4_o7_crash_after_its_recopy_round_resumes_from_o6_and_sets_the_counter_after_the_recopy(self):
        """A kill -9 stand-in at the journal write that marks O6 passed after O7's completed re-copy round, and one before, after, or
        after and before the journal event, at the counter ALTER that follows that round: O7 ends failed, and the operator's run
        --from O6 ends with O6, O7 and O8 passed, no row lost or doubled, and the counter set after the re-copy."""
        cases = [('O6 passed', None)] + [('counter', point) for point in FAULT_POINTS]
        for where, point in cases:
            with self.subTest(where=where, point=point):
                h = copied(self, mutate_inventory=auto_increment_on_t_comp)
                h.fake.refuse_sharded_counter = False  # the ALTER path of the counter logic: Helios 10.8.0 refuses it on a sharded table (1749, covered by its own test)
                h.fake.rows(SRC, 't_comp').append(dict(LATE_ROW))
                start = len(h.fake.writes)
                if where == 'O6 passed':
                    fired = crash_journal(self, h.journal, 'step_end',
                                          match=lambda step_id, status, note=None, evidence_path=None: step_id == 'O6' and status == 'passed')
                elif point == 'journal':
                    fired = crash_journal(self, h.journal, 'event', match=lambda kind, detail: kind == 'counter')
                else:
                    h.fake.fail_statement(r'^ALTER TABLE `%s`\.`t_comp` AUTO_INCREMENT' % TGT, point, crash=True)
                    fired = h.fake.faults_fired
                with self.assertRaises(SimulatedCrash):
                    h.step('O7', redo=False)
                self.assertEqual(len(fired), 1, 'the fault fired')
                self.assertEqual(h.journal.step_status('O7'), 'failed')
                self.assertEqual(h.journal.step_status('O6'), 'pending' if where == 'O6 passed' else 'passed',
                                 'O6 is passed again only once its journal write is done')
                self.assertEqual(run_from_o6(h), EXIT_DONE, h.text())
                self.assertEqual({s: h.journal.step_status(s) for s in ('O6', 'O7', 'O8')}, {'O6': 'passed', 'O7': 'passed', 'O8': 'passed'})
                assert_reinserted_once(self, h, 't_comp', start)
                assert_counter_follows(self, h, 't_comp', 'mark_recopy')
                self.assertGreater(h.fake.databases[TGT]['tables']['t_comp']['auto_increment'], LATE_ROW['c'],
                                   'the counter is above every id the re-copy brought')

    def test_o8_mismatch_marks_buckets_and_exits_3(self):
        h = copied(self)
        self.assertEqual(h.step('O7').status, steps.PASSED, h.text())
        h.fake.rows(TGT, 't_single')[0]['payload'] = 'tampered'
        out8 = h.step('O8')
        self.assertEqual((out8.status, out8.exit_code), (steps.MISMATCH, 3), h.text())
        self.assertIn('mark-recopy t_single --bucket', out8.note)
        self.assertEqual(h.journal.step_status('O8'), 'failed')
        self.assertEqual(h.journal.step_status('O7'), 'pending', 'the re-copy reopens O7 for the counters (F4)')
        self.assertTrue(any(c['state'] == 'failed' for c in h.journal.chunks('t_single')))
        self.assertEqual(h.step('O6', options={'retry_failed': True}).status, steps.PASSED)
        self.assertEqual(h.step('O7').status, steps.PASSED)
        self.assertEqual(h.step('O8').status, steps.PASSED)

    def test_o8_sampled_needs_yes(self):
        """A table over verify.full_fingerprint_max_rows is sampled and O8 ends pending until --yes; the threshold reads the journal's
        source count, so t_nokey (500 rows) at a threshold of 500 is a full fingerprint."""
        h = sampled_harness(self, threshold=500)
        self.assertEqual(h.step('O7').status, steps.PASSED, h.text())
        out8 = h.step('O8')
        self.assertEqual((out8.status, out8.exit_code), (steps.PENDING, 4), h.text())
        self.assertIn('rerun step O8 --yes', out8.note)
        by = {r['table_name']: r for r in h.journal.verifications(tier=V.TIER_FINGERPRINT)}
        self.assertEqual((by['t_single']['result'], by['t_nokey']['result'], by['t_nokey']['predicate']), (V.SAMPLED, V.MATCH, None))
        self.assertEqual(h.step('O8', yes=True).status, steps.PASSED)

    def test_handlers_refuse_before_the_copy_and_print_in_dry_run(self):
        h = Harness(self)
        from s2rebuild.common import KitError
        with self.assertRaisesRegex(KitError, 'Cannot run O7'):
            h.step('O7')
        for step_id in ('O5', 'O6'):
            h.journal.step_end(step_id, 'passed', 'marked for the test')
        out7 = h.step('O7')
        self.assertEqual(out7.status, steps.REFUSED)
        self.assertIn('copy has not run', out7.note)
        h.journal.step_end('O7', 'passed', 'marked for the test')
        out8 = h.step('O8')
        self.assertEqual(out8.status, steps.REFUSED)
        self.assertIn('copy has not run', out8.note)
        self.assertEqual(h.fake.connections, [], 'a refusal before the copy opens no connection')
        self.assertEqual(h.step('O7', dry_run=True).status, steps.PASSED)
        self.assertEqual(h.step('O8', dry_run=True).status, steps.PASSED)
        self.assertIn('DRY RUN: SELECT COUNT(*) AS n FROM `%s`.`t_single`' % TGT, h.text())
        self.assertEqual(h.fake.connections, [], 'a dry run opens no connection')


class ReworkTests(unittest.TestCase):
    """W7: tier 3 in parallel on its own sessions, sampled by a contiguous range, the rerun flags and acceptance without a rerun."""

    def test_tier3_runs_largest_first_both_sides_at_once_on_its_own_sessions(self):
        h = copied(self, mutate=lambda d: d.update(verify={'sessions': 3}))
        session = h.sessions.runner(read_guard())
        before, start = len(h.fake.connections), len(h.fake.statements)
        verifier = V.Verifier(session, SRC, TGT, h.plans(), h.journal, h.config, log=h.log, connector=h.sessions.runner_connector())
        results = verifier.run(tiers=[V.TIER_FINGERPRINT])
        self.assertEqual([r.table for r in results], ['t_single', 't_nokey', 't_comp', 't_empty'], 'largest first')
        self.assertTrue(all(r.result == V.MATCH for r in results))
        self.assertEqual(len(h.fake.connections) - before, 3, 'verify.sessions connections of their own, not copy.workers (2)')
        self.assertTrue(all(c.closed for c in h.fake.connections[before:]), 'closed after the tier')
        self.assertEqual([(t, side) for t, side, _ in verifier.schedule],
                         [(t, side) for t in ('t_single', 't_nokey', 't_comp', 't_empty') for side in (V.SIDE_SOURCE, V.SIDE_TARGET)],
                         'the two sides of a table are handed out together, so they run at the same time')
        self.assertEqual(sorted(fingerprints_since(h, start)), sorted([r.sql_source for r in results] + [r.sql_target for r in results]))
        self.assertEqual(session.statements, [], 'tier 3 never runs on the step session')
        for r in results:
            self.assertIsNotNone(r.seconds)
            self.assertIn('tier 3 %s: match in ' % r.table, h.text(), 'one log line per table with the elapsed seconds')
        self.assertIn('tier 3: 4 tables on 3 sessions, largest first', h.text())

    def test_threshold_samples_the_time_range_and_notes_a_table_without_an_orderable_column(self):
        """Over the threshold: t_comp (300 rows, time column b) gets a 5 percent range of b on both sides; t_single and t_nokey have no
        sort key or time column, so they get a full fingerprint with a note; the workbook shows the predicate and the rows."""
        from openpyxl import load_workbook
        h = copied(self, mutate=lambda d: (d['databases'][0].update(time_column_candidates=['b']),
                                           d.update(verify={'full_fingerprint_max_rows': 200})))
        self.assertEqual(h.step('O7').status, steps.PASSED, h.text())
        out8 = h.step('O8')
        self.assertEqual((out8.status, out8.exit_code), (steps.PENDING, 4), h.text())
        by = {r['table_name']: r for r in h.journal.verifications(tier=V.TIER_FINGERPRINT)}
        comp = by['t_comp']
        self.assertEqual((comp['result'], comp['sample_rows'], comp['epoch']), (V.SAMPLED, 15, 0))
        self.assertTrue(comp['predicate'].startswith("`b` >= '2026-01-01 00:00:00.000000' AND `b` <= '2026-01-01 00:00:02.95"), comp['predicate'])
        self.assertTrue(comp['sql_source'].endswith('WHERE %s GROUP BY grp' % comp['predicate']))
        self.assertTrue(comp['sql_target'].endswith('WHERE %s GROUP BY grp' % comp['predicate']), 'the same predicate on both sides')
        self.assertIn('sampled 5 percent of the b range, 15 rows in the sample', comp['detail'])
        self.assertIn("SUM(CRC32(CONCAT_WS('|', COALESCE(`b`, ''), COALESCE(`note`, ''))))", comp['sql_source'], 'the time column plus the value column')
        for name in ('t_single', 't_nokey'):
            self.assertEqual((by[name]['result'], by[name]['predicate']), (V.MATCH, None))
            self.assertIn('rows over verify.full_fingerprint_max_rows 200 and no orderable sort key or time column', by[name]['detail'])
        self.assertEqual(by['t_empty']['detail'], '64 groups compared'.replace('64', '0'), 'under the threshold: no note')
        self.assertIn('tier 3 was sampled on t_comp (15 rows, WHERE `b` >= ', out8.note)
        wb = load_workbook(str(h.tmp / 'runs' / h.run_id / SRC / 'reports' / 'final.xlsx'))
        tab = {r[0].value: r[1].value for r in wb['t_comp'].iter_rows(min_col=1, max_col=2) if r[0].value}
        self.assertEqual((tab['Sampled or full'], tab['Sample predicate'], tab['Rows in sample']), ('sampled', comp['predicate'], 15))
        summary = {str(r[0].value): r[1].value for r in wb['Summary'].iter_rows(min_col=1, max_col=2) if r[0].value}
        self.assertEqual(summary['Sampled tables'], 't_comp')

    def test_sort_key_gives_an_integer_range_and_a_change_inside_it_is_caught(self):
        h = sampled_harness(self, threshold=500, percent=10)
        plans = h.plans()
        single = next(p for p in plans if p.name == 't_single')
        self.assertEqual((single.sort_columns, V.range_column(single)), (['n'], 'n'))
        session = h.sessions.runner(read_guard())
        results = V.Verifier(session, SRC, TGT, plans, h.journal, h.config, log=h.log).run(tiers=[V.TIER_COUNT, V.TIER_FINGERPRINT])
        r = next(r for r in results if r.table == 't_single' and r.tier == V.TIER_FINGERPRINT)
        self.assertEqual((r.result, r.predicate, r.sample_rows), (V.SAMPLED, '`n` >= 0 AND `n` <= 69', 70), '10 percent of the span 0 to 699')
        self.assertTrue(r.sql_source.endswith('WHERE `n` >= 0 AND `n` <= 69 GROUP BY grp'))
        self.assertTrue(r.sql_target.endswith('WHERE `n` >= 0 AND `n` <= 69 GROUP BY grp'))
        self.assertIn('SELECT MIN(`n`) AS lo, MAX(`n`) AS hi FROM `%s`.`t_single`' % SRC, session.statements, 'the range comes from the source')
        nokey = next(r for r in results if r.table == 't_nokey' and r.tier == V.TIER_FINGERPRINT)
        self.assertEqual((nokey.result, nokey.predicate), (V.MATCH, None), '500 rows is not over 500')
        self.assertIsNotNone(next(t for t in h.journal.tables() if t['table_name'] == 't_single')['verified_at_utc'], 'a sampled match stamps verified_at')
        inside = next(row for row in h.fake.rows(TGT, 't_single') if row['n'] == 5)
        inside['payload'] = 'tampered'
        again = V.Verifier(session, SRC, TGT, plans, h.journal, h.config, log=h.log).run(tiers=[V.TIER_FINGERPRINT])
        bad = next(r for r in again if r.table == 't_single')
        self.assertEqual(bad.result, V.MISMATCH)
        self.assertEqual(bad.mismatched_groups, (crc32(key_text('`id`', inside)) % single.n_buckets,), 'the group maps to the bucket')
        self.assertIsNone(next(t for t in h.journal.tables() if t['table_name'] == 't_single')['verified_at_utc'])

    def test_yes_without_a_chunk_since_the_last_o8_accepts_the_recorded_result_without_a_query(self):
        h = sampled_harness(self, threshold=500)
        self.assertEqual(h.step('O7').status, steps.PASSED, h.text())
        self.assertEqual(h.step('O8').status, steps.PENDING, h.text())
        rows_before, start = len(h.journal.verifications()), len(h.fake.statements)
        out = h.step('O8', yes=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('recorded result accepted without a rerun: no chunk was copied since the last O8 run', out.note)
        self.assertEqual(h.fake.statements[start:], [], 'no fingerprint query, no count: nothing was issued')
        self.assertEqual(len(h.journal.verifications()), rows_before, 'no new verification row')
        self.assertEqual(h.journal.events(V.ACCEPTED_EVENT),
                         ['accepted sampled fingerprint for t_single at re-copy epoch 0 (35 rows, WHERE `n` >= 0 AND `n` <= 34)'])
        from s2rebuild import switch
        self.assertEqual(switch.unverified_tables(h.journal), [], 'S0 accepts the accepted sample')
        self.assertEqual(h.step('O8', redo=True, yes=True).status, steps.PASSED, 'a second --yes records nothing twice')
        self.assertEqual(len(h.journal.events(V.ACCEPTED_EVENT)), 1)

    def test_yes_after_a_recopy_runs_the_tiers_again_before_accepting(self):
        """The failure injection of the acceptance rule: a bucket copied since the last O8 run (a re-copy at a new epoch) makes --yes
        run every tier again, and the acceptance is recorded at the new epoch."""
        h = sampled_harness(self, threshold=500)
        self.assertEqual(h.step('O7').status, steps.PASSED, h.text())
        self.assertEqual(h.step('O8').status, steps.PENDING, h.text())
        h.journal.mark_recopy('t_comp', None)
        self.assertEqual(h.step('O6', options={'retry_failed': True}).status, steps.PASSED, h.text())
        self.assertEqual(h.step('O7').status, steps.PASSED, h.text())
        start = len(h.fake.statements)
        out = h.step('O8', yes=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertTrue(out.note.startswith('every tier passed (4 tables; accepted sampled fingerprint for t_single at re-copy epoch 0'), out.note)
        self.assertEqual(len(fingerprints_since(h, start)), 8, 'both sides of every table ran again')
        self.assertIn('O8 runs the tiers again: t_comp has no fingerprint row at re-copy epoch 1', h.text())
        self.assertEqual(h.journal.verifications('t_comp', V.TIER_FINGERPRINT)[-1]['epoch'], 1)

    def test_full_table_reruns_tier_3_for_that_table_only(self):
        h = sampled_harness(self, threshold=500)
        self.assertEqual(h.step('O7').status, steps.PASSED, h.text())
        self.assertEqual(h.step('O8').status, steps.PENDING, h.text())
        start = len(h.fake.statements)
        out = h.step('O8', options={'full': ['t_single']})
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertTrue(out.note.startswith('every tier passed (4 tables)'), 'nothing left to accept')
        issued = [s for s, _ in h.fake.statements[start:]]
        self.assertEqual(len(issued), 2, 'the two sides of t_single and nothing else: %s' % issued)
        self.assertTrue(all(s.startswith('SELECT CRC32(') and '`t_single`' in s and 'WHERE' not in s for s in issued))
        row = h.journal.verifications('t_single', V.TIER_FINGERPRINT)[-1]
        self.assertEqual((row['result'], row['predicate'], row['sample_rows'], row['epoch']), (V.MATCH, None, None, 0))
        self.assertEqual(h.journal.events(V.ACCEPTED_EVENT), [])
        out = h.step('O8', redo=True, options={'full': ['t_other']})
        self.assertEqual(out.status, steps.REFUSED)
        self.assertIn('Not a planned table: t_other', out.note)

    def test_fingerprint_table_with_percent_samples_that_table_and_the_flags_are_checked(self):
        import types
        from s2rebuild import cli
        h = sampled_harness(self, threshold=1_000_000_000, time_column='b')
        self.assertEqual(h.step('O7').status, steps.PASSED, h.text())
        self.assertEqual(h.step('O8').status, steps.PASSED, h.text())
        start = len(h.fake.statements)
        out = h.step('O8', redo=True, options={'fingerprint': ['t_comp'], 'percent': 50})
        self.assertEqual(out.status, steps.PENDING, h.text())
        self.assertIn('tier 3 was sampled on t_comp (150 rows, WHERE `b` >= ', out.note)
        issued = [s for s, _ in h.fake.statements[start:]]
        self.assertEqual([s[:13] for s in issued], ['SELECT MIN(`b', 'SELECT CRC32(', 'SELECT CRC32('], 'the range of b from the source, then both sides')
        row = h.journal.verifications('t_comp', V.TIER_FINGERPRINT)[-1]
        self.assertEqual((row['result'], row['sample_rows']), (V.SAMPLED, 150))
        self.assertIn('sampled 50 percent of the b range', row['detail'])
        self.assertEqual(h.step('O8', redo=True, yes=True).status, steps.PASSED, 'accepted without a rerun')
        self.assertEqual(len(h.journal.events(V.ACCEPTED_EVENT)), 1)
        o8, o6 = steps.REGISTRY['O8'], steps.REGISTRY['O6']
        self.assertEqual(cli._o8_options(types.SimpleNamespace(fingerprint=None, full=None, percent=None), o6), {})
        with self.assertRaisesRegex(KitError, '--percent needs --fingerprint TABLE'):
            cli._o8_options(types.SimpleNamespace(fingerprint=None, full=None, percent=5), o8)
        with self.assertRaisesRegex(KitError, '--percent must be between 1 and 100'):
            cli._o8_options(types.SimpleNamespace(fingerprint=['t'], full=None, percent=0), o8)
        with self.assertRaisesRegex(KitError, '--full applies to step O8 only'):
            cli._o8_options(types.SimpleNamespace(fingerprint=None, full=['t'], percent=None), o6)
        out = h.step('O8', redo=True, options={'fingerprint': ['t_comp'], 'full': ['t_comp']})
        self.assertEqual((out.status, out.note), (steps.REFUSED, 't_comp named by both --fingerprint and --full; name a table once'))


if __name__ == '__main__':
    unittest.main()
