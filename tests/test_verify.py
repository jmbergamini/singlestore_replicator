"""s2rebuild tests test_verify: tier selection, grouped fingerprints and bucket mapping, budget and sampling, verified_at rule.

Author: John Bergamini
"""
import unittest

from s2rebuild import copy as C
from s2rebuild import estimate as E
from s2rebuild import inventory as I
from s2rebuild import steps
from s2rebuild import verify as V
from s2rebuild.common import EXIT_DONE
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
        self.assertEqual(V.value_expr(plan), "CONCAT_WS('|', COALESCE(`u`, ''))", 'the time column alone when present')
        sql = V.fingerprint_sql('d', plan, 3)
        self.assertEqual(sql, "SELECT CRC32(`id`) MOD 3 AS grp, COUNT(*) AS n, SUM(CRC32(`id`)) AS key_hash, "
                              "SUM(CRC32(CONCAT_WS('|', COALESCE(`u`, '')))) AS value_hash FROM `d`.`t` GROUP BY grp")
        self.assertTrue(V.fingerprint_sql('d', plan, 64, 5).endswith('WHERE CRC32(`id`) MOD 100 < 5 GROUP BY grp'))
        for text in (sql, V.fingerprint_sql('d', plan, 64, 5), V.newest_sql('d', 't', 'u')):
            read_guard().check(text)
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

    def test_budget_smallest_first_then_sampled(self):
        h = copied(self, mutate=lambda d: d.update(verify={'fingerprint_budget_minutes': 400001, 'sample_percent_over_budget': 50}))
        session = h.sessions.runner(read_guard())
        baseline = E.Baseline(scan_s_per_row=1.0, insert_s_per_row=1.0, chunk_overhead_s=0)
        results = V.Verifier(session, SRC, TGT, h.plans(), h.journal, h.config, baseline).run(tiers=[V.TIER_FINGERPRINT])
        by = {r.table: r for r in results}
        self.assertEqual(by['t_empty'].result, V.MATCH, 'the empty table costs nothing')
        self.assertEqual(by['t_comp'].result, V.MATCH, '200 s fits; smallest first spends it before the 24 million second table')
        self.assertEqual(by['t_nokey'].result, V.SAMPLED, 'largest first would have fitted this one and sampled t_comp instead')
        self.assertEqual(by['t_single'].result, V.SAMPLED)
        self.assertIn('sampled 50 percent', by['t_single'].detail)
        self.assertTrue(by['t_single'].sql_source.endswith('WHERE CRC32(`id`) MOD 100 < 50 GROUP BY grp'))
        self.assertEqual(V.summarize(results)[V.TIER_FINGERPRINT]['sampled'], ['t_nokey', 't_single'])

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
        self.assertEqual(wb.sheetnames[5:], ['Views', 'Pipelines', 'Pipeline offsets'])
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
        h = copied(self, mutate=lambda d: d.update(verify={'fingerprint_budget_minutes': 0}))
        self.assertEqual(h.step('O7').status, steps.PASSED, h.text())
        out8 = h.step('O8')
        self.assertEqual((out8.status, out8.exit_code), (steps.PENDING, 4), h.text())
        self.assertIn('rerun step O8 --yes', out8.note)
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


if __name__ == '__main__':
    unittest.main()
