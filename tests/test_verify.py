"""s2rebuild tests test_verify: tier selection, grouped fingerprints and bucket mapping, budget and sampling, verified_at rule.

Author: John Bergamini
"""
import unittest

from s2rebuild import copy as C
from s2rebuild import estimate as E
from s2rebuild import inventory as I
from s2rebuild import steps
from s2rebuild import verify as V
from s2rebuild.connect import DryRunSession, Log
from s2rebuild.sqlutil import read_guard, runner_guard
from tests.fakedb import crc32, key_text
from tests.test_copy import FIXTURE_TABLES, SRC, TGT, Harness


def copied(test, **kw):
    h = Harness(test, **kw)
    assert h.o5_o6().status == steps.PASSED, h.text()
    return h


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

    def test_o8_mismatch_marks_buckets_and_exits_3(self):
        h = copied(self)
        self.assertEqual(h.step('O7').status, steps.PASSED, h.text())
        h.fake.rows(TGT, 't_single')[0]['payload'] = 'tampered'
        out8 = h.step('O8')
        self.assertEqual((out8.status, out8.exit_code), (steps.MISMATCH, 3), h.text())
        self.assertIn('mark-recopy t_single --bucket', out8.note)
        self.assertEqual(h.journal.step_status('O8'), 'failed')
        self.assertTrue(any(c['state'] == 'failed' for c in h.journal.chunks('t_single')))
        self.assertEqual(h.step('O6', options={'retry_failed': True}).status, steps.PASSED)
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
