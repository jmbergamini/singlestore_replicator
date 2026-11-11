"""s2rebuild tests test_preflight: the P2 checks behind s2rebuild check, and the C2 report handler.

Author: John Bergamini
"""
import json
import unittest

from s2rebuild import report_xlsx, steps
from s2rebuild.common import EXIT_DONE
from tests.fakedb import FakeDisconnect, SimulatedCrash
from tests.helpers import FAULT_POINTS, ROOT, crash_journal, run_cli, write_config
from tests.test_copy import SRC, TGT, Harness
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


def routine_harness(test, names, routines=ROUTINES, owner=True):
    """A harness whose source holds routines and whose configuration lists names in objects.routines.ignore; the owner profile by default,
    since the runner sees no routines (fact 21)."""
    h = owner_harness(test, mutate=listing(names)) if owner else Harness(test, mutate=listing(names))
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
        self.assertIn('workers: 2 configured (copy.workers), table concurrency auto, on 16 leaf cores', text, 'W11: the worker line')
        self.assertIn('owner: no owner profile configured', text)
        self.assertIn('workspace: answer before the window:', text)
        self.assertIn('1. is the workspace ACTIVE', text)
        self.assertIn('record the answers at O1 with --workspace NOTE', text)
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
    def test_routines_are_counted_by_kind_through_the_owner_profile_at_check(self):
        """check (the P2 handler) reads the routines with the owner session and prints them by kind; the runner sees none, so without
        the owner profile the line says so and never 'none'."""
        h = routine_harness(self, [])
        out = p2(h)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('routines: 2 routines (1 functions, 0 TVFs, 0 aggregates, 1 procedures), 0 ignored by configuration', h.text())
        facts = json.loads(h.journal.get_meta('preflight'))
        self.assertEqual(facts['routines'], [{'name': 'nightly_rollup', 'kind': 'procedure'}, {'name': 'tax_rate', 'kind': 'function'}])
        self.assertEqual(facts['routines_ignored'], [])
        h = routine_harness(self, [], owner=False)
        out = p2(h)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('routines: not visible to the runner; owner profile needed', h.text())
        self.assertNotIn('routines: none', h.text())
        self.assertIsNone(json.loads(h.journal.get_meta('preflight'))['routines'])

    def test_ignored_routines_are_named_at_check(self):
        """A bare name and a database.name entry both ignore a routine; check passes and names them as ignored by configuration."""
        h = routine_harness(self, ['nightly_rollup', SRC + '.tax_rate'])
        out = p2(h)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('routines: 2 routines (1 functions, 0 TVFs, 0 aggregates, 1 procedures), 2 ignored by configuration: '
                      'procedure nightly_rollup, function tax_rate', h.text())
        facts = json.loads(h.journal.get_meta('preflight'))
        self.assertEqual(facts['routines_ignored'], ['procedure nightly_rollup', 'function tax_rate'])

    def test_check_after_a_fault_at_every_point_still_counts_the_routines(self):
        """Failure points: the ROUTINES query fails before it runs ('before'), its reply is lost after it ran ('after'), and
        the P2 journal update (step_end) is killed after the query ('journal'). P2 is never left passed, and the rerun an
        operator makes counts the routines again, the ignored one named."""
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
                    self.assertEqual(out.status, steps.PASSED, h.text())
                    self.assertIn('routines: 1 routines (0 functions, 0 TVFs, 0 aggregates, 1 procedures), %d ignored by configuration%s' % (
                        len(names), ': procedure nightly_rollup' if names else ''), h.text())
                    self.assertEqual(h.journal.step_status('P2'), 'passed')


class ReportTests(unittest.TestCase):
    def ready(self, h):
        """A copied harness with every step before C2 passed."""
        self.assertEqual(h.o5_o6().status, steps.PASSED)
        for step_id in ('O7', 'O8', 'O9', 'O10', 'O11', 'C1'):
            h.journal.step_end(step_id, 'passed', 'test')

    def test_c2_writes_the_reports(self):
        h = Harness(self)
        self.assertEqual(p2(h).status, steps.PASSED, h.text())
        self.ready(h)
        out = h.step('C2', redo=True)
        self.assertEqual((out.status, out.note), (steps.PASSED, 'reports written'), h.text())
        reports = h.tmp / 'runs' / h.run_id / SRC / 'reports'
        self.assertTrue((reports / 'final.xlsx').is_file())
        self.assertTrue((reports / 'run-report.md').is_file())
        self.assertEqual(steps.unmet_preconditions(steps.REGISTRY['C3'], h.journal), [])

    def test_the_run_report_follows_c2_c3_and_c4(self):
        """The report on disk shows C2 passed, never running, and is written again when C3 and C4 are gated, by hand or by account-sql.
        A gate before C2 passed writes nothing, and a gate on another step leaves the report as it is."""
        h = Harness(self)
        self.assertEqual(p2(h).status, steps.PASSED, h.text())
        self.ready(h)
        report = h.tmp / 'runs' / h.run_id / SRC / 'reports' / 'run-report.md'
        self.assertIsNone(report_xlsx.refresh_reports(h.ctx(), 'C3'), 'C2 has not passed')
        self.assertFalse(report.exists())
        self.assertEqual(h.step('C2', redo=True).status, steps.PASSED, h.text())
        text = report.read_text(encoding='utf8')
        self.assertIn('- C2: passed', text)
        self.assertNotIn('- C2: running', text)
        self.assertNotIn('- C3:', text)
        self.assertIn('- Steps not passed: none', text)
        h.journal.record_gate('C3', True, 'two entries, write on the retained name refused')
        self.assertIsNone(report_xlsx.refresh_reports(h.ctx(), 'O11'), 'only C2 to C4 write the reports again')
        self.assertNotIn('- C3:', report.read_text(encoding='utf8'))
        self.assertEqual(report_xlsx.refresh_reports(h.ctx(), 'C3'), report)
        self.assertIn('- C3: passed', report.read_text(encoding='utf8'))
        base = ['--config', str(h.config_path), '--run', h.run_id]
        code, out = run_cli(['gate', 'C4', '--pass', '--note', 'DROP USER done, zero privileges remain'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, out)
        text = report.read_text(encoding='utf8')
        for step_id in ('C2', 'C3', 'C4'):
            self.assertIn('- %s: passed' % step_id, text)
        self.assertIn('gate C4 pass: DROP USER done, zero privileges remain', text)


if __name__ == '__main__':
    unittest.main()
