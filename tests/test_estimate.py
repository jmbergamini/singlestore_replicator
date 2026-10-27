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
from s2rebuild.common import KitError
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
        self.assertEqual(labels, ['O3 freeze', 'O5 create replacement', 'O6 count proof', 'O7 post-copy', 'O8 fingerprints (budget)',
                                  'O9 pipelines', 'O10 switch', 'O11 release'])
        by = dict(est.allowances)
        self.assertEqual(by['O3 freeze'], 600.0)
        self.assertEqual(by['O9 pipelines'], 120.0)
        self.assertEqual(by['O8 fingerprints (budget)'], 1800.0)
        self.assertAlmostEqual(by['O6 count proof'], 2 * 639003650 * E.DEFAULT_SCAN_S_PER_ROW / 4)
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
        self.assertEqual(dict(E.estimate(plans, no_fp, E.DEFAULT_BASELINE).allowances)['O8 fingerprints (budget)'], 0.0)

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


if __name__ == '__main__':
    unittest.main()
