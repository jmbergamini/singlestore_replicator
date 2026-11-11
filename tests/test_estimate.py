"""s2rebuild tests test_estimate: the estimate line and the plan sheet.

Author: John Bergamini
"""
import unittest

from s2rebuild import estimate as E
from s2rebuild import inventory as I
from tests.helpers import ROOT, config_from

FIXTURE_INVENTORY = ROOT / 'tests' / 'fixtures' / 'inventory'


class PlanSheetTests(unittest.TestCase):
    def test_plan_sheet(self):
        cfg = config_from()
        inv = I.load(FIXTURE_INVENTORY)
        plans = I.plan_tables(inv, cfg, cfg.databases[0])
        est = E.estimate(plans, cfg, pipeline_count=2)
        self.assertEqual(est.copy_seconds, est.rows / 2.4e9 * 3600)
        lines = E.plan_sheet(cfg, cfg.databases[0], '20261101-0500', inv, est, {'S2REBUILD_KAFKA_CREDENTIALS': 'x'})
        text = '\n'.join(lines)
        self.assertTrue(lines[0].startswith('SingleStore Database Rebuild Kit    run 20261101-0500    database example_orders_db'))
        self.assertIn('Window     2026-11-01T05:00:00Z to 2026-11-01T13:00:00Z (8 h 00 m); no step stops on it', text)
        self.assertIn('Freeze     read-only attachment', text)
        self.assertIn('Estimate   copy about ', text)
        self.assertIn('at 2.40 B rows per hour (estimate.rows_per_hour)', text)
        self.assertIn('Pipelines  2 captured (1 KAFKA, 1 S3); route 1 (kit replays); credentials: KAFKA env set, S3 env not set', text)
        self.assertIn('retention stated by customer: KAFKA not stated; S3 not stated; stop to start (the copy estimate):', text)
        self.assertIn('app-managed: 1', text)
        stated = config_from(lambda d: d.update(pipelines={'retention_hours': {'KAFKA': 168, 'S3': 0}}))
        text = '\n'.join(E.plan_sheet(stated, stated.databases[0], 'r', inv, E.estimate(plans, stated, pipeline_count=2)))
        self.assertIn('KAFKA 168 h, margin OK; S3 0 h, margin SHORT', text)
        self.assertIn('Accounts   write-capable: app_user, retention_job', text)
        self.assertIn('Workspace  answered at O1 with --workspace NOTE', text)
        no_window = config_from(lambda d: d.pop('window'))
        text = '\n'.join(E.plan_sheet(no_window, no_window.databases[0], 'r', inv, E.estimate(plans, no_window)))
        self.assertIn('Window     none configured; the copy and the switch run without a time limit', text)


if __name__ == '__main__':
    unittest.main()
