"""s2rebuild tests test_preflight: the P2 checks behind s2rebuild check, and the C2 report and calibrate handler.

Author: John Bergamini
"""
import json
import unittest
from unittest import mock

import openpyxl

from s2rebuild import estimate, inventory, report_xlsx, steps
from s2rebuild.common import EXIT_DONE
from tests.fakedb import FakeDisconnect, SimulatedCrash
from tests.helpers import FAULT_POINTS, ROOT, crash_journal, run_cli, write_config
from tests.test_copy import FIXTURE_TABLES, SRC, TGT, Harness
from tests.test_pipelines import owner_harness

REFERENCE_JOURNAL = ROOT / 'tests' / 'fixtures' / 'reference-run' / 'copy-journal.sqlite'
ROUTINES = [{'ROUTINE_NAME': 'nightly_rollup', 'ROUTINE_TYPE': 'PROCEDURE'}, {'ROUTINE_NAME': 'tax_rate', 'ROUTINE_TYPE': 'FUNCTION'}]


def p2(h, **kw):
    """Run the P2 handler on the harness (P1 is recorded first, as the operator would)."""
    h.journal.record_gate('P1', True, 'test')
    return h.step('P2', **kw)


def listing(names):
    """A config mutation that sets objects.routines.ignore to names."""
    return lambda d: d.update(objects={'routines': {'ignore': list(names)}})


def routine_harness(test, names, routines=ROUTINES):
    """A harness whose source reports routines and whose configuration lists names in objects.routines.ignore."""
    h = Harness(test, mutate=listing(names))
    h.fake.routines = [dict(r) for r in routines]
    return h


class PreflightTests(unittest.TestCase):
    def test_a_clean_workspace_passes_and_the_storage_id_is_recorded(self):
        h = Harness(self)
        out = p2(h)
        self.assertEqual(out.status, steps.PASSED, h.text())
        text = h.text()
        self.assertIn('connection: runner <redacted>@%, engine 10.8.0', text, 'log lines mask the SQL user names')
        self.assertIn('privileges: all 11 runner privileges of section 5 present', text)
        self.assertIn('source STORAGE_ID: 1028600672070281646:7367158985892520834 (READWRITE), recorded', text)
        self.assertIn('replacement name: %s is free' % TGT, text)
        self.assertIn('compute size: 16 leaf cores x 8 = 128 covers the 32 target partitions', text)
        self.assertIn('owner: no owner profile configured', text)
        self.assertEqual(h.journal.attachment('source')['storage_id'], '1028600672070281646:7367158985892520834')
        self.assertEqual(h.journal.get_meta('engine_version'), '10.8.0', 'the fake reports the engine of the capture')

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


class RoutineTests(unittest.TestCase):
    def test_f8_an_unlisted_procedure_or_function_refuses_check(self):
        """check (the P2 handler) fails on each procedure and function objects.routines.ignore does not list, naming its type."""
        h = routine_harness(self, [])
        out = p2(h)
        self.assertEqual((out.status, out.exit_code), (steps.FAILED, 4), h.text())
        self.assertIn('routines: FAILED, not migrated in this pass: procedure nightly_rollup is not listed in objects.routines.ignore; '
                      'function tax_rate is not listed in objects.routines.ignore', out.note)
        self.assertEqual(h.journal.step_status('P2'), 'failed')
        h = routine_harness(self, ['nightly_rollup'])
        out = p2(h)
        self.assertEqual(out.status, steps.FAILED, 'the function is still not listed')
        self.assertIn('function tax_rate is not listed', out.note)
        self.assertNotIn('procedure nightly_rollup', out.note)

    def test_f8_listed_routines_pass_check_as_not_migrated(self):
        """A bare name and a database.name entry both list a routine; check passes and records them as not migrated."""
        h = routine_harness(self, ['nightly_rollup', SRC + '.tax_rate'])
        out = p2(h)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('routines: not migrated, listed in objects.routines.ignore: procedure nightly_rollup, function tax_rate', h.text())
        facts = json.loads(h.journal.get_meta('preflight'))
        self.assertEqual(facts['routines_not_migrated'], ['procedure nightly_rollup', 'function tax_rate'])

    def test_f8_check_after_a_fault_at_every_point_still_refuses_or_passes(self):
        """Failure points: the ROUTINES query fails before it runs ('before'), its reply is lost after it ran ('after'), and
        the P2 journal update (step_end) is killed after the query ('journal'). P2 is never left passed, and the rerun an
        operator makes refuses the unlisted procedure or passes the listed one."""
        for point in FAULT_POINTS:
            for names in ([], ['nightly_rollup']):
                with self.subTest(point=point, ignore=names):
                    h = routine_harness(self, names, ROUTINES[:1])
                    if point == 'journal':
                        fired = crash_journal(self, h.journal, 'step_end', match=lambda step_id, *a, **k: step_id == 'P2')
                        with self.assertRaises(SimulatedCrash):
                            p2(h)
                        self.assertEqual(len(fired), 1)
                    else:
                        h.fake.fail_statement(r'information_schema\.ROUTINES', point)
                        with self.assertRaises(FakeDisconnect):
                            p2(h)
                        self.assertEqual([when for when, _ in h.fake.faults_fired], [point])
                    self.assertNotEqual(h.journal.step_status('P2'), 'passed', 'a fault never leaves P2 passed')
                    out = p2(h)
                    if names:
                        self.assertEqual(out.status, steps.PASSED, h.text())
                        self.assertIn('routines: not migrated, listed in objects.routines.ignore: procedure nightly_rollup', h.text())
                    else:
                        self.assertEqual((out.status, out.exit_code), (steps.FAILED, 4), h.text())
                        self.assertIn('procedure nightly_rollup is not listed in objects.routines.ignore', out.note)
                    self.assertEqual(h.journal.step_status('P2'), 'passed' if names else 'failed')


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

    def test_f9_c2_with_three_chunks_passes_with_the_note(self):
        """Three first-pass chunks cannot be fitted: the baseline in use stays (no baseline file is written), the Summary of the
        final workbook and of the run report carry a Calibration line that says so, and C2 passes with that note, so C3 can run."""
        h = Harness(self, tables={'t_single': FIXTURE_TABLES['t_single']})
        self.ready(h)
        self.assertEqual(len(estimate.read_chunks(h.journal_path)), 3)
        out = h.step('C2', redo=False)
        self.assertEqual((out.status, out.exit_code), (steps.PASSED, EXIT_DONE), h.text())
        line = ('baseline not recalibrated (Calibration needs at least 4 first-pass chunks; found 3); '
                'the baseline in use stays: the shipped default')
        self.assertEqual(out.note, 'reports written; ' + line)
        self.assertEqual(h.journal.step_status('C2'), 'passed')
        self.assertFalse(h.config.resolve(h.config.estimate.baseline).exists(), 'the configured baseline file is not written')
        reports = h.tmp / 'runs' / h.run_id / SRC / 'reports'
        summary = {row[0]: row[1] for row in openpyxl.load_workbook(reports / 'final.xlsx')['Summary'].iter_rows(values_only=True)}
        self.assertEqual(summary['Calibration'], line)
        self.assertIn('- Calibration: ' + line, (reports / 'run-report.md').read_text())
        self.assertEqual(steps.unmet_preconditions(steps.REGISTRY['C3'], h.journal), [])

    def test_f9_c2_fault_points_around_the_baseline_write(self):
        """The three failure points around the one C2 write outside the reports, the fitted baseline file: a kill just before
        it, just after it, and after it but before the C2 journal update. Rerunning step C2 passes and the file holds the
        fitted baseline."""
        reference = estimate.calibrate(REFERENCE_JOURNAL, until='2026-10-24T08:00:00Z', workers=4)
        write = estimate.write_baseline
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = Harness(self, mutate=lambda d: d.update(estimate={'baseline': 'customer_baseline.json'}))
                self.ready(h)
                target = h.tmp / 'customer_baseline.json'

                def killed(path, baseline, point=point):
                    if point == 'after':
                        write(path, baseline)
                    raise SimulatedCrash('killed %s the baseline write' % point)
                with mock.patch.object(estimate, 'calibrate', return_value=reference):
                    if point == 'journal':
                        crash_journal(self, h.journal, 'step_end', match=lambda step_id, status, *rest: (step_id, status) == ('C2', 'passed'))
                        with self.assertRaises(SimulatedCrash):
                            h.step('C2', redo=False)
                        self.assertEqual(h.journal.step_status('C2'), 'running', 'killed before the journal update')
                        self.assertTrue(target.is_file())
                    else:
                        with mock.patch.object(estimate, 'write_baseline', killed), self.assertRaises(SimulatedCrash):
                            h.step('C2', redo=False)
                        self.assertEqual(h.journal.step_status('C2'), 'failed')
                        self.assertEqual(target.is_file(), point == 'after')
                    out = h.step('C2', redo=False)
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertEqual(out.note, 'reports written; baseline recalibrated and written to %s' % target)
                self.assertEqual(estimate.load_baseline(target).scan_s_per_row, reference.baseline.scan_s_per_row)
                self.assertEqual(h.journal.step_status('C2'), 'passed')


if __name__ == '__main__':
    unittest.main()
