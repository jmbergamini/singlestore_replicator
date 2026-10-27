"""s2rebuild tests test_estimate: the fit reproduces the reference constants, scheduling, bands, window fit.

Author: John Bergamini
"""
import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path

from s2rebuild import estimate as E
from s2rebuild import inventory as I
from s2rebuild.common import KitError, iso
from tests.helpers import ROOT, Plan, config_from

REFERENCE_JOURNAL = ROOT / 'tests' / 'fixtures' / 'reference-run' / 'copy-journal.sqlite'
FIXTURE_INVENTORY = ROOT / 'tests' / 'fixtures' / 'inventory'


class CalibrateTests(unittest.TestCase):
    def test_fit_reproduces_the_reference_constants_within_2_percent(self):
        cal = E.calibrate(REFERENCE_JOURNAL, until='2026-10-24T08:00:00Z')
        b = cal.baseline
        self.assertEqual(cal.chunks, 349)
        self.assertEqual(round(cal.worker_seconds), 23867)
        self.assertAlmostEqual(b.scan_s_per_row / E.DEFAULT_SCAN_S_PER_ROW, 1.0, delta=0.02)
        self.assertAlmostEqual(b.insert_s_per_row / E.DEFAULT_INSERT_S_PER_ROW, 1.0, delta=0.02)
        self.assertAlmostEqual(b.scan_rows_per_second / 1e6, 13.1, delta=0.2)
        self.assertAlmostEqual(b.insert_rows_per_second / 1e3, 339, delta=5)
        self.assertEqual(b.chunk_overhead_s, E.DEFAULT_CHUNK_OVERHEAD_S, 'a fitted overhead below the default keeps the default')
        self.assertLess(cal.fitted_overhead_s, E.DEFAULT_CHUNK_OVERHEAD_S)
        self.assertAlmostEqual(cal.rows_per_hour / 1e9, 1.43, delta=0.02)
        self.assertAlmostEqual(cal.gib_per_hour(938), 490, delta=5)
        self.assertEqual(cal.wall_seconds, 6913.0)
        self.assertEqual(b.source, 'calibrate')
        self.assertEqual(b.chunks, 349)
        lines = E.calibration_lines(cal, 938)
        self.assertIn('349 first-pass chunks', lines[0])
        self.assertIn('13.1 M rows per second per worker', lines[1])
        self.assertIn('339 k rows per second per worker', lines[1])
        self.assertIn('1.43 billion rows per hour, about 488 GiB per hour', lines[3])
        self.assertIn('GiB per hour needs --size-gib', E.calibration_lines(cal)[3])

    def test_fit_without_the_cut_includes_the_tail_and_excludes_recopies(self):
        cal = E.calibrate(REFERENCE_JOURNAL)
        self.assertEqual(cal.chunks, 360, 'first-pass chunks only; the 22 re-copied chunks are out')
        with self.assertRaisesRegex(KitError, 'non-positive rate'):
            E.calibrate(REFERENCE_JOURNAL, first_pass_only=False)
        points = E.read_chunks(REFERENCE_JOURNAL, first_pass_only=False)
        self.assertEqual(len(points), 382, 're-copied chunks carry hours of lock waits, so the fit on all attempts is refused')

    def test_calibrate_refusals(self):
        with self.assertRaisesRegex(KitError, 'Journal not found'):
            E.calibrate(ROOT / 'nowhere.sqlite')
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / 'bad.sqlite'
            bad.write_bytes(b'not a database' * 10)
            with self.assertRaisesRegex(KitError, 'could not be opened|no readable tables'):
                E.calibrate(bad)
        with self.assertRaisesRegex(KitError, 'at least 4'):
            E.calibrate(REFERENCE_JOURNAL, until='2026-10-24T06:01:30Z')
        with self.assertRaisesRegex(KitError, 'until must'):
            E.calibrate(REFERENCE_JOURNAL, until='yesterday')


class BaselineTests(unittest.TestCase):
    def test_default_and_file_round_trip(self):
        self.assertIs(E.load_baseline(None), E.DEFAULT_BASELINE)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'baseline.json'
            self.assertIs(E.load_baseline(path), E.DEFAULT_BASELINE, 'absent file means default')
            cal = E.calibrate(REFERENCE_JOURNAL, until='2026-10-24T08:00:00Z', workers=4, compute_size='S-16', run_id='r1')
            E.write_baseline(path, cal.baseline)
            data = json.loads(path.read_text())
            self.assertEqual(data['run_id'], 'r1')
            self.assertEqual(data['source'], str(path))
            loaded = E.load_baseline(path)
            self.assertAlmostEqual(loaded.scan_s_per_row, cal.baseline.scan_s_per_row)
            self.assertEqual(loaded.workers, 4)
            path.write_text(json.dumps(dict(data, bogus=1)))
            with self.assertRaisesRegex(KitError, 'unknown keys'):
                E.load_baseline(path)
            path.write_text(json.dumps(dict(data, scan_s_per_row=-1)))
            with self.assertRaisesRegex(KitError, 'positive'):
                E.load_baseline(path)
            path.write_text('[]')
            with self.assertRaisesRegex(KitError, 'mapping'):
                E.load_baseline(path)
            path.write_text('{nope')
            with self.assertRaisesRegex(KitError, 'not valid JSON'):
                E.load_baseline(path)


class ModelTests(unittest.TestCase):
    def test_chunk_and_table_seconds(self):
        b = E.DEFAULT_BASELINE
        self.assertAlmostEqual(E.chunk_seconds(b, 10_000_000, 1), 2 + 0.762 + 29.5, places=6)
        self.assertAlmostEqual(E.table_worker_seconds(b, 10_000_000, 2), 2 * (2 + 0.762 + 14.75), places=6)
        self.assertAlmostEqual(E.table_worker_seconds(E.Baseline(1e-6, 1e-6, 0), 1000, 4), 4 * (0.001 + 0.00025), places=9)

    def test_w4_a_reference_table_is_one_chunk_at_the_insert_rate_twice(self):
        """A reference table copies through a staging table: one chunk, the source scanned once, the rows inserted twice."""
        b = E.Baseline(scan_s_per_row=1e-6, insert_s_per_row=1e-3, chunk_overhead_s=2.0)
        self.assertAlmostEqual(E.reference_seconds(b, 1000), 2.0 + 0.001 + 2.0, places=9)
        plans = [Plan('ref', 1000, 1, reference=True), Plan('plain', 1000, 1, reference=False)]
        schedule = E.simulate(plans, 1, b)
        start, end, worker = schedule.per_table['ref']
        self.assertAlmostEqual(worker, 4.001, places=6)
        self.assertGreater(worker, schedule.per_table['plain'][2], 'the second insert costs more than the direct path')
        self.assertAlmostEqual(schedule.per_table['plain'][2], 3.001, places=6)
        self.assertAlmostEqual(schedule.worker_seconds, 7.002, places=6)
        forced = E.simulate_rows([(1000, 4, True)], 1, b, names=['ref'])
        self.assertAlmostEqual(forced.worker_seconds, 4.001, places=6, msg='a reference job is one chunk whatever its bucket count')
        self.assertAlmostEqual(E.simulate([Plan('legacy', 1000, 1)], 1, b).worker_seconds, 3.001, places=6, msg='a plan without the flag is direct')

    def test_schedule_respects_workers_and_limits(self):
        b = E.Baseline(scan_s_per_row=0.0, insert_s_per_row=1e-3, chunk_overhead_s=0.0)
        plans = [Plan('big', 4000, 4), Plan('small', 1000, 1)]
        one = E.simulate(plans, 1, b)
        self.assertAlmostEqual(one.wall_seconds, 5.0)
        self.assertAlmostEqual(one.worker_seconds, 5.0)
        four = E.simulate(plans, 4, b)
        self.assertAlmostEqual(four.wall_seconds, 2.0, msg='five one-second chunks on four workers take two rounds')
        plans[0].concurrency = 1
        serialized = E.simulate(plans, 4, b)
        self.assertAlmostEqual(serialized.wall_seconds, 4.0, msg='the big table runs one chunk at a time')
        start, end, worker = serialized.per_table['big']
        self.assertEqual((start, end, worker), (0.0, 4.0, 4.0))
        self.assertEqual(serialized.per_table_minutes['small'], (0.0, 0.0))
        self.assertEqual(serialized.longest_chunk_seconds, 1.0)
        empty = E.simulate([], 4, b)
        self.assertEqual(empty.wall_seconds, 0.0)

    def test_schedule_caps_one_table_at_half_the_workers_above_eight(self):
        b = E.Baseline(scan_s_per_row=0.0, insert_s_per_row=1e-3, chunk_overhead_s=0.0)
        big = [Plan('big', 12000, 12)]
        self.assertAlmostEqual(E.simulate(big, 12, b).wall_seconds, 2.0, msg='12 workers: 6 chunks of one table at a time')
        self.assertAlmostEqual(E.simulate(big, 8, b).wall_seconds, 2.0, msg='8 workers: no cap, 8 then 4')
        two = [Plan('big', 12000, 12), Plan('next', 6000, 6)]
        self.assertAlmostEqual(E.simulate(two, 12, b).wall_seconds, 2.0, msg='the other 6 workers copy the next table')

    def test_estimate_allowances_bands_and_fit(self):
        cfg = config_from()
        inv = I.load(FIXTURE_INVENTORY)
        plans = I.plan_tables(inv, cfg, cfg.databases[0])
        est = E.estimate(plans, cfg, E.DEFAULT_BASELINE, pipeline_count=2)
        self.assertEqual(est.workers, 4)
        self.assertEqual(est.tables, 6)
        self.assertEqual(est.rows, 639003650)
        labels = [label for label, _ in est.allowances]
        self.assertEqual(labels, ['O3 freeze', 'O5 create replacement', 'O6 count proof', 'O7 post-copy', 'O8 fingerprints (estimate)',
                                  'O9 pipelines', 'O10 switch', 'O11 release'])
        by = dict(est.allowances)
        self.assertEqual(by['O3 freeze'], 600.0)
        self.assertEqual(by['O9 pipelines'], 120.0)
        self.assertAlmostEqual(by['O8 fingerprints (estimate)'], 2 * 639003650 * E.DEFAULT_SCAN_S_PER_ROW / 4,
                               msg='no budget: two scans per table over copy.workers sessions')
        budgeted = config_from(lambda d: d.update(verify={'fingerprint_budget_minutes': 30, 'sessions': 8, 'full_fingerprint_max_rows': 100_000_000}))
        self.assertEqual(dict(E.estimate(plans, budgeted, E.DEFAULT_BASELINE).allowances)['O8 fingerprints (budget)'], 1800.0)
        self.assertAlmostEqual(E.fingerprint_seconds(plans, budgeted, E.DEFAULT_BASELINE, 8),
                               2 * (639003650 - 600_000_000 * 0.95) * E.DEFAULT_SCAN_S_PER_ROW / 8, msg='the table over the threshold at 5 percent')
        self.assertAlmostEqual(by['O6 count proof'], 2 * 639003650 * E.DEFAULT_SCAN_S_PER_ROW, msg='the count proof is serial, not divided by the workers')
        low, expected, high = est.copy_band
        self.assertAlmostEqual(low, expected * 0.85)
        self.assertAlmostEqual(high, expected * 1.15)
        self.assertGreater(est.total_seconds, est.copy_seconds)
        self.assertEqual(est.table_minutes('empty_table')[1], 0.0)
        self.assertGreater(est.table_minutes('order_items')[2], est.table_minutes('order_items')[0])
        fit = E.window_fit(est, cfg.window)
        self.assertEqual(fit.deadline, cfg.window.end - dt.timedelta(minutes=90, seconds=est.schedule.longest_chunk_seconds))
        self.assertTrue(fit.fits)
        self.assertGreater(fit.spare_seconds, 0)
        lines = E.fit_lines(est, fit)
        self.assertIn('FITS', lines[2])
        self.assertIn('639.0 M rows', lines[0])
        short = config_from(lambda d: d['window'].update(end='2026-11-01T05:10:00Z'))
        tight = E.window_fit(est, short.window)
        self.assertFalse(tight.fits)
        self.assertLess(tight.spare_seconds, 0)
        self.assertIn('DOES NOT FIT', E.fit_lines(est, tight)[2])
        no_fp = config_from(lambda d: d.update(verify={'tiers': ['count']}))
        self.assertEqual(dict(E.estimate(plans, no_fp, E.DEFAULT_BASELINE).allowances)['O8 fingerprints (estimate)'], 0.0)

    def test_plan_sheet(self):
        cfg = config_from()
        inv = I.load(FIXTURE_INVENTORY)
        plans = I.plan_tables(inv, cfg, cfg.databases[0])
        est = E.estimate(plans, cfg, E.DEFAULT_BASELINE, pipeline_count=2)
        fit = E.window_fit(est, cfg.window)
        lines = E.plan_sheet(cfg, cfg.databases[0], '20261101-0500', inv, est, fit, None, {'S2REBUILD_KAFKA_CREDENTIALS': 'x'})
        text = '\n'.join(lines)
        self.assertTrue(lines[0].startswith('SingleStore Database Rebuild Kit    run 20261101-0500    database example_orders_db'))
        self.assertIn('Window     2026-11-01T05:00:00Z to 2026-11-01T13:00:00Z (8 h 00 m)', text)
        self.assertIn('abort for the copy (end minus 60 m restart, 30 m buffer', text)
        self.assertIn('Freeze     Path A read-only attachment', text)
        self.assertIn('Pipelines  2 captured (1 KAFKA, 1 S3); route 1 (kit replays); credentials: KAFKA env set, S3 env not set', text)
        self.assertIn('retention stated by customer: KAFKA not stated; S3 not stated; stop to start at high:', text)
        self.assertIn('app-managed: 1', text)
        stated = config_from(lambda d: d.update(pipelines={'retention_hours': {'KAFKA': 168, 'S3': 1}}))
        text = '\n'.join(E.plan_sheet(stated, stated.databases[0], 'r', inv, E.estimate(plans, stated, pipeline_count=2), fit))
        self.assertIn('KAFKA 168 h, margin OK; S3 1 h, margin SHORT', text)
        self.assertIn('Accounts   write-capable: app_user, retention_job', text)
        self.assertIn('Rehearsal  none recorded', text)
        no_window = config_from(lambda d: d.pop('window'))
        text = '\n'.join(E.plan_sheet(no_window, no_window.databases[0], 'r', inv, E.estimate(plans, no_window), None))
        self.assertIn('Window     none configured: no abort deadline', text)


class ScalingTests(unittest.TestCase):
    """W10: the rates scaled by the leaf cores P2 recorded against the baseline's, the cap, the leaf cores in the baseline, one fit per run."""

    BASE = E.Baseline(scan_s_per_row=1e-6, insert_s_per_row=1e-3, chunk_overhead_s=2.0, leaf_cores=240)
    PLANS = [Plan('t', 1000, 1, size_gib=0.0)]

    def test_w10_rates_scale_down_by_leaf_cores_without_limit(self):
        """A workspace with a quarter of the baseline's leaf cores gets rates four times slower; the overhead stays; the count proof
        follows the scaled scan rate; no scaling when P2 or the baseline recorded no leaf cores."""
        cfg = config_from()
        est = E.estimate(self.PLANS, cfg, self.BASE, leaf_cores=60)
        self.assertEqual((est.scale, est.leaf_cores), (0.25, 60))
        self.assertAlmostEqual(est.rates.scan_s_per_row, 4e-6)
        self.assertAlmostEqual(est.rates.insert_s_per_row, 4e-3)
        self.assertEqual(est.rates.chunk_overhead_s, 2.0)
        self.assertIs(est.baseline, self.BASE, 'the Estimate keeps the baseline as read; the scaled rates are separate')
        self.assertAlmostEqual(est.copy_seconds, 2.0 + 0.004 + 4.0)
        self.assertAlmostEqual(dict(est.allowances)['O6 count proof'], 2 * 1000 * 4e-6)
        self.assertEqual(est.scale_note, "rates scaled by 0.25 for 60 leaf cores against the baseline's 240")
        line = E.fit_lines(est, None)[-1]
        self.assertTrue(line.startswith('           planning estimate: rates scaled by 0.25'), line)
        self.assertIn(E.PLANNING_NOTE, line)
        no_cores = E.estimate(self.PLANS, cfg, self.BASE, leaf_cores=None)
        self.assertEqual(no_cores.scale, 1.0)
        self.assertIn('P2 recorded no leaf cores', no_cores.scale_note)
        default = E.estimate(self.PLANS, cfg, E.DEFAULT_BASELINE, leaf_cores=60)
        self.assertEqual(default.scale, 1.0, 'the shipped constants carry no leaf cores, so nothing scales')
        self.assertIn('the baseline records no leaf cores', default.scale_note)
        self.assertIs(E.scaled(self.BASE, 1.0), self.BASE)

    def test_w10_scale_up_is_capped_by_the_configuration(self):
        """Twice the baseline's leaf cores gets no credit at the default cap of 1.0, 1.5 at a cap of 1.5; the cap is 1.0 to 4.0."""
        est = E.estimate(self.PLANS, config_from(), self.BASE, leaf_cores=480)
        self.assertEqual(est.scale, 1.0)
        self.assertEqual(est.scale_note, "rates scaled by 1.00 for 480 leaf cores against the baseline's 240 (estimate.scale_up_cap 1.0 gives no more credit)")
        self.assertAlmostEqual(est.copy_seconds, 2.0 + 0.001 + 1.0, msg='the baseline rates as they are')
        cap = config_from(lambda d: d.update(estimate={'scale_up_cap': 1.5}))
        est = E.estimate(self.PLANS, cap, self.BASE, leaf_cores=480)
        self.assertEqual(est.scale, 1.5)
        self.assertIn('(estimate.scale_up_cap 1.5 gives no more credit)', est.scale_note)
        self.assertAlmostEqual(est.rates.insert_s_per_row, 1e-3 / 1.5)
        under = E.estimate(self.PLANS, cap, self.BASE, leaf_cores=300)
        self.assertEqual(under.scale, 1.25, 'under the cap the ratio applies')
        self.assertIn('(within estimate.scale_up_cap 1.5)', under.scale_note)
        with self.assertRaisesRegex(KitError, 'scale_up_cap'):
            config_from(lambda d: d.update(estimate={'scale_up_cap': 0.5}))
        with self.assertRaisesRegex(KitError, 'scale_up_cap'):
            config_from(lambda d: d.update(estimate={'scale_up_cap': 5}))

    def test_w10_calibrate_records_leaf_cores_with_the_baseline(self):
        """calibrate keeps the leaf cores it is given with the compute label; the file round trip keeps them; an older file
        without the key loads None; the shipped default carries none."""
        cal = E.calibrate(REFERENCE_JOURNAL, until='2026-10-24T08:00:00Z', leaf_cores=240, compute_size='S-8')
        self.assertEqual((cal.baseline.leaf_cores, cal.baseline.compute_size), (240, 'S-8'))
        self.assertEqual(E.calibration_lines(cal)[4], 'recorded with the baseline: 4 workers, compute S-8, leaf cores 240')
        plain = E.calibrate(REFERENCE_JOURNAL, until='2026-10-24T08:00:00Z')
        self.assertIsNone(plain.baseline.leaf_cores)
        self.assertIn('leaf cores not recorded by P2 (no scaling from this baseline)', E.calibration_lines(plain)[4])
        self.assertIsNone(E.DEFAULT_BASELINE.leaf_cores)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'baseline.json'
            E.write_baseline(path, cal.baseline)
            data = json.loads(path.read_text())
            self.assertEqual(data['leaf_cores'], 240)
            self.assertEqual(E.load_baseline(path).leaf_cores, 240)
            del data['leaf_cores']
            path.write_text(json.dumps(data))
            self.assertIsNone(E.load_baseline(path).leaf_cores, 'a file written before W10 loads without scaling')
            path.write_text(json.dumps(dict(data, leaf_cores=0)))
            with self.assertRaisesRegex(KitError, 'leaf_cores'):
                E.load_baseline(path)

    def test_w10_window_fit_is_one_for_the_run(self):
        """Two databases in sequence: the copy and the allowances add up, the longest chunk is the largest, the fit line says so,
        and a window that fits one database does not fit two."""
        cfg = config_from()
        inv = I.load(FIXTURE_INVENTORY)
        plans = I.plan_tables(inv, cfg, cfg.databases[0])
        est = E.estimate(plans, cfg, E.DEFAULT_BASELINE, pipeline_count=2)
        self.assertIs(E.combine([est]), est)
        run = E.combine([est, est])
        self.assertEqual((run.databases, run.tables, run.rows), (2, 2 * est.tables, 2 * est.rows))
        self.assertAlmostEqual(run.copy_seconds, 2 * est.copy_seconds)
        self.assertEqual([label for label, _ in run.allowances], [label for label, _ in est.allowances])
        self.assertAlmostEqual(run.allowance_seconds, 2 * est.allowance_seconds)
        self.assertEqual(run.schedule.longest_chunk_seconds, est.schedule.longest_chunk_seconds)
        self.assertIsNone(E.run_fit([est, est], None))
        with self.assertRaisesRegex(KitError, 'No estimate'):
            E.combine([])
        fit = E.run_fit([est, est], cfg.window)
        self.assertEqual(fit.databases, 2)
        self.assertEqual(fit.deadline, E.window_fit(est, cfg.window).deadline)
        self.assertEqual(E.window_fit(est, cfg.window).databases, 1)
        self.assertIn('(the run: 2 databases in sequence)', E.fit_lines(est, fit)[2])
        self.assertNotIn('the run:', E.fit_lines(est, E.window_fit(est, cfg.window))[2])
        # a window of one high-band total plus the longest chunk and a minute, without restart or buffer allowance
        end = cfg.window.start + dt.timedelta(seconds=int(est.total_band[2] + est.schedule.longest_chunk_seconds) + 60)
        short = config_from(lambda d: d['window'].update(end=iso(end), restart_allowance_minutes=0, buffer_minutes=0))
        self.assertTrue(E.run_fit([est], short.window).fits, 'one database fits the shortened window')
        self.assertFalse(E.run_fit([est, est], short.window).fits, 'two in sequence do not')


if __name__ == '__main__':
    unittest.main()
