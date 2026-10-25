"""s2rebuild tests test_preflight: the P2 checks behind s2rebuild check, and the C2 report and calibrate handler.

Author: John Bergamini
"""
import unittest

from s2rebuild import estimate, inventory, report_xlsx, steps
from s2rebuild.common import EXIT_DONE
from tests.helpers import ROOT, run_cli, write_config
from tests.test_copy import SRC, TGT, Harness
from tests.test_pipelines import owner_harness

REFERENCE_JOURNAL = ROOT / 'tests' / 'fixtures' / 'reference-run' / 'copy-journal.sqlite'


def p2(h, **kw):
    """Run the P2 handler on the harness (P1 is recorded first, as the operator would)."""
    h.journal.record_gate('P1', True, 'test')
    return h.step('P2', **kw)


class PreflightTests(unittest.TestCase):
    def test_a_clean_workspace_passes_and_the_storage_id_is_recorded(self):
        h = Harness(self)
        out = p2(h)
        self.assertEqual(out.status, steps.PASSED, h.text())
        text = h.text()
        self.assertIn('connection: runner <redacted>@%, engine 8.9.0', text, 'log lines mask the SQL user names')
        self.assertIn('privileges: all 11 runner privileges of section 5 present', text)
        self.assertIn('source STORAGE_ID: sid-source (READWRITE), recorded', text)
        self.assertIn('replacement name: %s is free' % TGT, text)
        self.assertIn('compute size: 16 leaf cores x 8 = 128 covers the 32 target partitions', text)
        self.assertIn('owner: no owner profile configured', text)
        self.assertEqual(h.journal.attachment('source')['storage_id'], 'sid-source')
        self.assertEqual(h.journal.get_meta('engine_version'), '8.9.0')

    def test_each_failure_is_named(self):
        h = Harness(self)
        h.fake.privileges = [r for r in h.fake.privileges if r['PRIVILEGE_TYPE'] not in ('CREATE DATABASE', 'DROP DATABASE')]
        h.fake.databases[TGT] = {'layout': dict(h.fake.databases[SRC]['layout']), 'tables': {}, 'views': {}}
        h.fake.leaf_cores = 2
        out = p2(h)
        self.assertEqual((out.status, out.exit_code), (steps.FAILED, 4))
        self.assertIn('missing CREATE DATABASE, DROP DATABASE', out.note)
        self.assertIn('%s already exists' % TGT, out.note)
        self.assertIn('2 leaf cores x 8 = 16 is below the 32 target partitions', out.note)

    def test_unreadable_cores_are_reported_not_assumed_and_the_owner_is_checked(self):
        h = owner_harness(self)
        h.fake.leaf_cores = None
        h.fake.pipelines = [{'PIPELINE_NAME': 'p1', 'STATE': 'Running'}]
        out = p2(h)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('leaf cores are not readable here; confirm in the Portal that cores x 8 >= 32 partitions', h.text())
        self.assertIn('owner: connected as <redacted>@%; 1 pipeline(s) visible', h.text())

    def test_check_dry_run_prints_the_queries(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path, _ = write_config(tmp)
            code, text = run_cli(['check', '--dry-run', '--config', str(path)], env={'S2REBUILD_RUNNER_PASSWORD': 'x'})
            self.assertEqual(code, EXIT_DONE, text)
            self.assertIn('DRY RUN: SELECT CURRENT_USER() AS u, @@memsql_version AS v', text)
            self.assertIn('MV_NODES', text)
            self.assertFalse((tmp and __import__('pathlib').Path(tmp) / 'runs').exists(), 'a dry run writes nothing')


class ReportAndCalibrateTests(unittest.TestCase):
    def ready(self, h):
        """A copied harness with every step before C2 passed."""
        self.assertEqual(h.o5_o6().status, steps.PASSED)
        for step_id in ('O7', 'O8', 'O9', 'O10', 'O11', 'C1'):
            h.journal.step_end(step_id, 'passed', 'test')

    def test_reports_and_a_recalibrated_baseline(self):
        h = Harness(self, mutate=lambda d: d.update(estimate={'baseline': str('customer_baseline.json')}))
        self.ready(h)
        reference = estimate.calibrate(REFERENCE_JOURNAL, until='2026-10-24T08:00:00Z', workers=4)
        original = estimate.calibrate
        estimate.calibrate = lambda *a, **k: reference
        try:
            out = h.step('C2', redo=True)
        finally:
            estimate.calibrate = original
        self.assertEqual(out.status, steps.PASSED, h.text())
        reports = h.tmp / 'runs' / h.run_id / SRC / 'reports'
        self.assertTrue((reports / 'final.xlsx').is_file())
        self.assertTrue((reports / 'run-report.md').is_file())
        self.assertIn('baseline recalibrated and written to', out.note)
        self.assertTrue((h.tmp / 'customer_baseline.json').is_file())

    def test_too_few_chunks_keep_the_baseline_and_wait_for_a_person(self):
        h = Harness(self)
        self.ready(h)
        out = h.step('C2', redo=True)
        self.assertEqual((out.status, out.exit_code), (steps.PENDING, 4), h.text())
        self.assertIn('the baseline was not recalibrated', out.note)
        self.assertTrue((h.tmp / 'runs' / h.run_id / SRC / 'reports' / 'final.xlsx').is_file(), 'the reports are written anyway')


if __name__ == '__main__':
    unittest.main()
