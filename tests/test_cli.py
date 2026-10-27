"""s2rebuild tests test_cli: help lists every command, stubs refuse, dry-run walk, gate and status cycle, one rule set per entry point,
the run the README sequence works on.

Author: John Bergamini
"""
import contextlib
import hashlib
import json
import os
import re
import shlex
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import yaml

from s2rebuild import cli, inventory, rehearse, steps
from s2rebuild.common import EXIT_DONE, EXIT_GATE, EXIT_INCOMPLETE, EXIT_REFUSED
from s2rebuild.journal import Journal
from tests.fakedb import SimulatedCrash
from tests.helpers import FAULT_POINTS, FIXTURE, ROOT, crash_journals, fake_driver, record_passed, run_cli, write_config

FIXTURE_INVENTORY = ROOT / 'tests' / 'fixtures' / 'inventory'
REFERENCE_JOURNAL = ROOT / 'tests' / 'fixtures' / 'reference-run' / 'copy-journal.sqlite'

STUB_ARGS = {'owner-sql': ['O10'], 'step': ['O6'], 'gate': ['O1', '--pass', '--note', 'x']}


class HelpTests(unittest.TestCase):
    def test_help_lists_every_command_of_section_8(self):
        code, text = run_cli(['--help'])
        self.assertEqual(code, 0)
        expected = ['check', 'account-sql', 'profile', 'plan', 'rehearse', 'sample-database', 'run', 'step', 'gate', 'status', 'watch',
                    'quiet-proof', 'lockout', 'owner-sql', 'owner-notebook', 'mark-recopy', 'pause', 'resume', 'abandon', 'report',
                    'calibrate', 'cleanup', 'bundle', 'engine-capture', 'docs-flags']
        self.assertEqual(cli.COMMAND_NAMES, expected)
        for name in expected:
            self.assertRegex(text, r'\n\s+%s\s' % re.escape(name), name)
        for flag in ('--config PATH', '--run ID', '--db NAME', '--dry-run', '--yes', '--verbose'):
            self.assertIn(flag, text)
        self.assertIn('SingleStore Database Rebuild Kit', text)
        self.assertIn('Exit codes: 0 done, 1 refused', ' '.join(text.split()))

    def test_every_command_has_help_and_a_dispatch_entry(self):
        for name in cli.COMMAND_NAMES:
            code, text = run_cli([name, '--help'])
            self.assertEqual(code, 0, name)
            self.assertIn('--dry-run', text, name)
            self.assertIn(name, cli.DISPATCH)

    def test_version(self):
        code, text = run_cli(['--version'])
        self.assertEqual(code, 0)
        self.assertIn('SingleStore Database Rebuild Kit 1.0.0', text)

    def test_usage_error_exits_1_not_2(self):
        code, text = run_cli(['run', '--nope'])
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('REFUSED: unrecognized arguments: --nope', text)
        code, text = run_cli([])
        self.assertEqual(code, EXIT_REFUSED)
        code, text = run_cli(['gate', 'O1', '--note', 'x'])
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('--pass', text)

    def test_docs_flags_table_covers_every_command(self):
        code, text = run_cli(['docs-flags'])
        self.assertEqual(code, EXIT_DONE)
        self.assertTrue(text.startswith('| Command | Flag | Default | Description |\n|---|---|---|---|\n'))
        for name in cli.COMMAND_NAMES:
            self.assertIn('| %s |' % name, text)
        self.assertIn('| (global) | `--dry-run` |', text)
        self.assertIn('| run | `--from ID` | O1 |', text)
        self.assertIn('| gate | `--note TEXT` |', text)
        self.assertNotIn(chr(0x2014), text)


class StubTests(unittest.TestCase):
    def test_unimplemented_commands_refuse_and_name_the_milestone(self):
        for name, label, help_text, milestone in cli.COMMANDS:
            if milestone is None:
                continue
            code, text = run_cli([name] + STUB_ARGS.get(name, []) + ['--config', str(FIXTURE)])
            self.assertEqual(code, EXIT_REFUSED, name)
            self.assertIn('arrives in %s' % milestone, text)
            self.assertNotIn('milestone M2', text, 'M2 is this build; nothing arrives in it')
            self.assertIn('Nothing was run', text)


class DryRunCliTests(unittest.TestCase):
    def test_run_dry_run_walks_o1_to_o11_on_the_fixture(self):
        code, text = run_cli(['run', '--dry-run', '--config', str(FIXTURE), '--from', 'O1', '--to', 'O11'])
        self.assertEqual(code, EXIT_DONE)
        self.assertIn('DRY RUN, example_orders_db', text)
        for step_id in ('O1', 'O2', 'O3', 'O4', 'O5', 'O6', 'O7', 'O8', 'O9', 'O10', 'O11'):
            self.assertIn('[%s]' % step_id, text)
        self.assertIn('gates pending: O1 example_orders_db, O2 example_orders_db, O3 example_orders_db, O9 example_orders_db, '
                      'O10 example_orders_db, O11 example_orders_db', text)
        self.assertIn('No connection was opened and nothing was written.', text)

    def test_global_flags_before_or_after_the_command(self):
        before, text_before = run_cli(['--dry-run', '--config', str(FIXTURE), 'run', '--from', 'O2', '--to', 'O2'])
        after, text_after = run_cli(['run', '--from', 'O2', '--to', 'O2', '--dry-run', '--config', str(FIXTURE)])
        self.assertEqual((before, after), (EXIT_DONE, EXIT_DONE))
        self.assertIn('[O2]', text_before)
        self.assertIn('[O2]', text_after)
        self.assertNotIn('[O3]', text_after)

    def test_db_flag_merges_across_positions(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp, lambda d: d['databases'].append(
                {'source': 'second_db', 'layout': {'partitions': 8, 'sub_partitions': 64}}))
            code, text = run_cli(['--db', 'example_orders_db', 'run', '--dry-run', '--config', str(path), '--to', 'O1', '--db', 'second_db'])
            self.assertEqual(code, EXIT_DONE)
            self.assertIn('database example_orders_db', text)
            self.assertIn('database second_db', text)

    def test_dry_run_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp)
            code, text = run_cli(['run', '--dry-run', '--config', str(path)])
            self.assertEqual(code, EXIT_DONE)
            self.assertFalse((Path(tmp) / 'runs').exists())

    def test_env_config_default(self):
        code, text = run_cli(['run', '--dry-run', '--to', 'O1'], env={'S2REBUILD_CONFIG': str(FIXTURE)})
        self.assertEqual(code, EXIT_DONE)
        code, text = run_cli(['run', '--dry-run', '--to', 'O1'], env={'S2REBUILD_CONFIG': '/nonexistent/config.yaml'})
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('Configuration file not found', text)

    def test_db_selection(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp, lambda d: d['databases'].append(
                {'source': 'second_db', 'layout': {'partitions': 8, 'sub_partitions': 64}}))
            code, text = run_cli(['run', '--dry-run', '--config', str(path), '--to', 'O1', '--db', 'second_db'])
            self.assertEqual(code, EXIT_DONE)
            self.assertIn('database second_db', text)
            self.assertNotIn('database example_orders_db', text)
            code, text = run_cli(['run', '--dry-run', '--config', str(path), '--to', 'O1', '--db', 'nope'])
            self.assertEqual(code, EXIT_REFUSED)
            self.assertIn('not configured', text)
            code, text = run_cli(['run', '--config', str(path), '--to', 'O1'])
            self.assertEqual(code, EXIT_REFUSED)
            self.assertIn('pass --db NAME or --yes', text)
            self.assertFalse((Path(tmp) / 'runs').exists(), 'refused before any journal is created')
            code, text = run_cli(['run', '--dry-run', '--config', str(path), '--to', 'O1', '--yes'])
            self.assertEqual(code, EXIT_DONE)
            self.assertIn('database example_orders_db', text)
            self.assertIn('database second_db', text)


class ProfileCliTests(unittest.TestCase):
    def test_profile_dry_run_on_the_fixture_inventory_writes_the_workbook(self):
        from openpyxl import load_workbook
        with tempfile.TemporaryDirectory() as tmp:
            config_path, _ = write_config(tmp)
            out = Path(tmp) / 'profile.xlsx'
            code, text = run_cli(['profile', '--dry-run', '--config', str(config_path), '--inventory', str(FIXTURE_INVENTORY), '--out', str(out)])
            self.assertEqual(code, EXIT_DONE, text)
            self.assertTrue(out.is_file())
            wb = load_workbook(str(out))
            self.assertEqual(wb.sheetnames[0], 'Summary')
            self.assertEqual(wb.sheetnames[1:7], ['order_items', 'orders', 'events', 'customers', 'totals', 'empty_table'])
            self.assertIn('workbook written to %s (Summary plus 6 table tabs)' % out, text)
            self.assertIn('FITS', text)
            self.assertIn('No connection was opened', text)
            self.assertFalse((ROOT / 'customer' / 'runs').exists(), 'a dry run on the fixture writes nothing under the kit')
            self.assertFalse((Path(tmp) / 'runs').exists(), 'a dry run writes only the workbook it was asked for')
            code, text = run_cli(['profile', '--dry-run', '--config', str(config_path), '--inventory', str(FIXTURE_INVENTORY),
                                  '--out', str(Path(tmp).parent / 'outside.xlsx')])
            self.assertEqual(code, EXIT_REFUSED, text)
            self.assertIn('Everything a run writes stays under', text)
            self.assertFalse((Path(tmp).parent / 'outside.xlsx').exists())
        code, text = run_cli(['profile', '--dry-run', '--config', str(FIXTURE), '--inventory', str(FIXTURE_INVENTORY)])
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('needs --out FILE', text)

    def test_profile_exits_4_when_the_window_does_not_fit(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp, lambda d: d['window'].update(end='2026-11-01T05:10:00Z'))
            out = Path(tmp) / 'short.xlsx'
            code, text = run_cli(['profile', '--dry-run', '--config', str(path), '--inventory', str(FIXTURE_INVENTORY), '--out', str(out)])
            self.assertEqual(code, EXIT_GATE, text)
            self.assertIn('DOES NOT FIT', text)
            self.assertTrue(out.is_file())
            self.assertNotIn('ERROR', text)
            record_passed(cfg, '20261101-0500')
            code, text = run_cli(['profile', '--config', str(path), '--inventory', str(FIXTURE_INVENTORY), '--run', '20261101-0500'])
            self.assertEqual(code, EXIT_GATE, text)
            code, text = run_cli(['status', '--config', str(path)])
            self.assertRegex(text, r'P3\s+Profile\s+passed', 'a non-fitting estimate is still a completed capture')
            code, text = run_cli(['profile', '--dry-run', '--config', str(path), '--inventory', str(FIXTURE_INVENTORY)])
            self.assertEqual(code, EXIT_GATE, text)
            reports = Path(tmp) / 'runs' / '20261101-0500' / 'example_orders_db' / 'reports'
            self.assertTrue((reports / 'profile-dry-run.xlsx').is_file(), 'a dry run never overwrites the live workbook')
            self.assertTrue((reports / 'profile.xlsx').is_file())

    def test_profile_plan_calibrate_cycle_on_a_run(self):
        from openpyxl import load_workbook
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp, lambda d: d.update(estimate={'baseline': str(Path(tmp) / 'customer' / 'baseline.json')}))
            base = ['--config', str(path)]
            code, text = run_cli(['plan'] + base)
            self.assertEqual(code, EXIT_REFUSED, 'plan needs a run with a capture')
            record_passed(cfg, '20261101-0500')
            code, text = run_cli(['profile', '--inventory', str(FIXTURE_INVENTORY), '--run', '20261101-0500'] + base)
            self.assertEqual(code, EXIT_DONE, text)
            run_dir = Path(tmp) / 'runs' / '20261101-0500' / 'example_orders_db'
            self.assertTrue((run_dir / 'inventory' / 'P3' / 'inventory.json').is_file())
            self.assertTrue((run_dir / 'inventory' / 'P3' / 'pipelines' / 'orders_stream.sql').is_file())
            self.assertTrue((run_dir / 'reports' / 'profile.xlsx').is_file())
            code, text = run_cli(['status'] + base)
            self.assertRegex(text, r'P3\s+Profile\s+passed')
            self.assertIn('profile.xlsx', text)
            code, text = run_cli(['plan'] + base)
            self.assertEqual(code, EXIT_DONE, text)
            self.assertIn('SingleStore Database Rebuild Kit    run 20261101-0500    database example_orders_db', text)
            self.assertIn('Deadline', text)
            self.assertIn('Pipelines  2 captured (1 KAFKA, 1 S3)', text)
            journal_path = run_dir / 'journal.sqlite'
            with contextlib.closing(Journal(journal_path)) as journal:
                journal.step_end('P2', 'failed', 'preflight run again and failed')
            code, text = run_cli(['step', 'P3'] + base)
            self.assertEqual(code, EXIT_REFUSED, 'P3 passed while P2 is not passed now, so it is refused, not skipped (F4)')
            self.assertIn('earlier steps are not passed: P2 (P3 passed earlier', text)
            record_passed(cfg, '20261101-0500', ('P2', 'O1', 'O2', 'O3'))
            code, text = run_cli(['profile', '--step', 'O4', '--inventory', str(FIXTURE_INVENTORY)] + base)
            self.assertEqual(code, EXIT_DONE, text)
            self.assertIn('drift: 0 structural difference(s)', text)
            self.assertTrue((run_dir / 'inventory' / 'O4' / 'plan.json').is_file())
            self.assertTrue((run_dir / 'inventory' / 'O4' / 'drift.json').is_file())
            self.assertTrue((run_dir / 'reports' / 'freeze-capture.xlsx').is_file())
            code, text = run_cli(['status'] + base)
            self.assertRegex(text, r'O4\s+Freeze capture\s+passed')
            code, text = run_cli(['calibrate', '--journal', str(REFERENCE_JOURNAL), '--until', '2026-10-24T08:00:00Z', '--size-gib', '938', '--dry-run'] + base)
            self.assertEqual(code, EXIT_DONE, text)
            self.assertIn('scan_s_per_row = 7.62', text)
            self.assertIn('about 488 GiB per hour', text)
            self.assertIn('DRY RUN: baseline not written', text)
            self.assertFalse((Path(tmp) / 'customer' / 'baseline.json').exists())
            code, text = run_cli(['calibrate', '--journal', str(REFERENCE_JOURNAL), '--until', '2026-10-24T08:00:00Z'] + base)
            self.assertEqual(code, EXIT_DONE, text)
            baseline = Path(tmp) / 'customer' / 'baseline.json'
            self.assertTrue(baseline.is_file())
            self.assertIn('baseline written to %s' % baseline, text)
            code, text = run_cli(['profile', '--inventory', str(FIXTURE_INVENTORY), '--out', str(Path(tmp) / 'customer' / 'again.xlsx')] + base)
            self.assertEqual(code, EXIT_REFUSED, 'P3 passed, so a capture again needs --redo (F5)')
            code, text = run_cli(['profile', '--redo', '--inventory', str(FIXTURE_INVENTORY), '--out', str(Path(tmp) / 'customer' / 'again.xlsx')] + base)
            self.assertEqual(code, EXIT_DONE, text)
            wb = load_workbook(str(Path(tmp) / 'customer' / 'again.xlsx'))
            summary = {str(r[0].value): r[1].value for r in wb['Summary'].iter_rows(min_col=1, max_col=2) if r[0].value}
            self.assertTrue(str(summary['Baseline']).startswith(str(baseline)), 'the written baseline is picked up by the next profile')
            code, text = run_cli(['calibrate', '--run', '20261101-0500'] + base)
            self.assertEqual(code, EXIT_REFUSED, 'the run journal has no chunks yet')
            self.assertIn('at least 4', text)


    def test_profile_refuses_a_load_target_the_replacement_will_not_hold_and_the_plan_sheet_counts_the_routines(self):
        """P3 and O4 refuse a pipeline INTO PROCEDURE whose procedure objects.routines.ignore lists and record the step failed; without
        the entry the capture passes and the plan sheet and the workbook count the routines by kind. Failure point: the journal write
        of the P3 refusal is killed once (the 'journal' point); P3 is not left passed and the rerun refuses again. A routine created
        during the window is drift, not a refusal."""
        import json
        import yaml
        from openpyxl import load_workbook
        from s2rebuild import inventory, steps
        from s2rebuild.common import KitError
        from s2rebuild.journal import Journal
        from tests.fakedb import SimulatedCrash
        from tests.helpers import crash_journals
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp, lambda d: d.update(objects={'routines': {'ignore': ['nightly_rollup']}}))
            run = '20261101-0500'
            base = ['--config', str(path), '--run', run]
            run_dir = Path(tmp) / 'runs' / run / 'example_orders_db'
            journal = Journal(run_dir / 'journal.sqlite')
            journal.record_gate('P1', True, 'test')
            journal.step_end('P2', 'passed', 'test')
            journal.close()
            into_procedure = Path(tmp) / 'into-procedure'
            into_procedure.mkdir()
            capture = json.loads((FIXTURE_INVENTORY / 'inventory.json').read_text(encoding='utf8'))
            capture['pipelines']['items'][0]['target'] = 'procedure nightly_rollup'
            (into_procedure / 'inventory.json').write_text(json.dumps(capture), encoding='utf8')

            def status(step_id):
                j = Journal(run_dir / 'journal.sqlite', read_only=True)
                try:
                    return j.step_status(step_id)
                finally:
                    j.close()

            def listing(names):
                data = yaml.safe_load(path.read_text(encoding='utf8'))
                data['objects'] = {'routines': {'ignore': names}}
                path.write_text(yaml.safe_dump(data, sort_keys=False), encoding='utf8')
            crash_journals(self, 'step_end', match=lambda step_id, state, *a, **k: step_id == 'P3' and state == 'failed')
            with self.assertRaises(SimulatedCrash):
                run_cli(['profile', '--inventory', str(into_procedure)] + base)
            self.assertEqual(status('P3'), 'running', 'killed before the refusal was journaled: P3 is not passed')
            code, text = run_cli(['profile', '--inventory', str(into_procedure)] + base)
            self.assertEqual(code, EXIT_REFUSED, text)
            self.assertIn('P3 refused for example_orders_db: orders_stream loads into procedure nightly_rollup, which objects.routines.ignore lists, '
                          'so O9 would find no target', text)
            self.assertEqual(status('P3'), 'failed')
            self.assertFalse((run_dir / 'reports' / 'profile.xlsx').exists(), 'no workbook for a refused capture')
            ctx = steps.StepContext(cfg, cfg.databases[0], None, run, None)
            with self.assertRaisesRegex(KitError, 'orders_stream loads into procedure nightly_rollup, which objects.routines.ignore lists'):
                inventory.handle_p3(steps.REGISTRY['P3'], ctx)
            code, text = run_cli(['plan'] + base)
            self.assertIn('Routines   2 routines (1 functions, 0 TVFs, 0 aggregates, 1 procedures), 1 ignored by configuration: procedure nightly_rollup; '
                          'REFUSED: orders_stream loads into procedure nightly_rollup, which objects.routines.ignore lists', text)

            listing([])
            code, text = run_cli(['profile', '--inventory', str(into_procedure)] + base)
            self.assertEqual(code, EXIT_DONE, text)
            self.assertEqual(status('P3'), 'passed')
            self.assertTrue((run_dir / 'inventory' / 'P3' / 'definitions' / 'routines' / 'nightly_rollup.procedure.sql').is_file())
            code, text = run_cli(['plan'] + base)
            self.assertEqual(code, EXIT_DONE, text)
            self.assertIn('Routines   2 routines (1 functions, 0 TVFs, 0 aggregates, 1 procedures), 0 ignored by configuration', text)
            wb = load_workbook(str(run_dir / 'reports' / 'profile.xlsx'))
            summary = {str(r[0].value): r[1].value for r in wb['Summary'].iter_rows(min_col=1, max_col=2) if r[0].value}
            self.assertEqual(summary['Routines ignored (objects.routines.ignore)'], 'none')
            self.assertEqual(summary['Routines (captured with the owner profile; created at O5 and O7)'],
                             '2 routines (1 functions, 0 TVFs, 0 aggregates, 1 procedures), 0 ignored by configuration')

            journal = Journal(run_dir / 'journal.sqlite')
            for step_id in ('O1', 'O2', 'O3'):
                journal.record_gate(step_id, True, 'test')
            journal.close()
            capture['routines'].append({'name': 'window_fix', 'kind': 'function', 'type': 'FUNCTION', 'definer': None,
                                        'definition': 'CREATE FUNCTION `window_fix`(a INT) RETURNS INT AS BEGIN RETURN a; END'})
            (Path(tmp) / 'o4').mkdir()
            (Path(tmp) / 'o4' / 'inventory.json').write_text(json.dumps(capture), encoding='utf8')
            code, text = run_cli(['profile', '--step', 'O4', '--inventory', str(Path(tmp) / 'o4')] + base)
            self.assertEqual(code, EXIT_DONE, 'a routine created during the window is drift, not a refusal: ' + text)
            self.assertIn('DRIFT: object added: function window_fix', text)
            self.assertEqual(status('O4'), 'passed')
            listing(['nightly_rollup'])
            code, text = run_cli(['profile', '--step', 'O4', '--redo', '--inventory', str(Path(tmp) / 'o4')] + base)
            self.assertEqual(code, EXIT_REFUSED, 'the freeze capture holds the same rule as P3')
            self.assertIn('O4 refused for example_orders_db: orders_stream loads into procedure nightly_rollup', text)
            self.assertEqual(status('O4'), 'failed')

    def test_o4_redo_refusal_is_journaled_failed_before_the_capture_is_rewritten(self):
        """profile --step O4 --redo on a capture with a load target the replacement will not hold wrote inventory/O4 before it journaled
        O4 running and failed, so a stop between the two left O4 passed on a rewritten capture, and a plain run then skipped O4 into
        the copy plan. Now O4 is recorded failed first. The failure points of that write (inventory.write): a kill at the journal write
        that starts the step and at the one that records it failed, a kill just before the write and one just after it. In no case is
        O4 passed on a capture that holds the refused target; the rerun refuses with O4 failed, step O4 refuses it, O5 waits for O4, and
        once the procedure is no longer ignored O4 passes."""
        import json
        import yaml
        from s2rebuild import inventory, steps
        from s2rebuild.journal import Journal
        from tests.fakedb import SimulatedCrash
        from tests.helpers import crash_journals
        real_write = inventory.write

        def crash_write(after):
            def write(*args, **kwargs):
                if after:
                    real_write(*args, **kwargs)
                raise SimulatedCrash('killed %s the capture write' % ('after' if after else 'before'))
            return write
        for point in ('journal start', 'journal end', 'before', 'after'):
            with self.subTest(point=point), tempfile.TemporaryDirectory() as tmp:
                path, cfg = write_config(tmp, lambda d: d.update(objects={'routines': {'ignore': ['nightly_rollup']}}))
                run = '20261101-0500'
                base = ['--config', str(path), '--run', run]
                run_dir = Path(tmp) / 'runs' / run / 'example_orders_db'
                o4_file = run_dir / 'inventory' / 'O4' / 'inventory.json'
                journal = Journal(run_dir / 'journal.sqlite')
                journal.record_gate('P1', True, 'test')
                journal.step_end('P2', 'passed', 'test')
                journal.close()

                def journal_state():
                    j = Journal(run_dir / 'journal.sqlite', read_only=True)
                    try:
                        return j.step_status('O4'), steps.unmet_preconditions(steps.REGISTRY['O5'], j)
                    finally:
                        j.close()

                def captured():
                    return json.loads(o4_file.read_text(encoding='utf8'))['pipelines']['items'][0]['target']
                code, text = run_cli(['profile', '--inventory', str(FIXTURE_INVENTORY)] + base)
                self.assertEqual(code, EXIT_DONE, text)
                journal = Journal(run_dir / 'journal.sqlite')
                for step_id in ('O1', 'O2', 'O3'):
                    journal.record_gate(step_id, True, 'test')
                journal.close()
                code, text = run_cli(['profile', '--step', 'O4', '--inventory', str(FIXTURE_INVENTORY)] + base)
                self.assertEqual((code, journal_state()[0], captured()), (EXIT_DONE, 'passed', 'table orders'), text)
                capture = json.loads((FIXTURE_INVENTORY / 'inventory.json').read_text(encoding='utf8'))
                capture['pipelines']['items'][0]['target'] = 'procedure nightly_rollup'
                (Path(tmp) / 'o4').mkdir()
                (Path(tmp) / 'o4' / 'inventory.json').write_text(json.dumps(capture), encoding='utf8')
                redo = ['profile', '--step', 'O4', '--redo', '--inventory', str(Path(tmp) / 'o4')] + base
                with contextlib.ExitStack() as stack:
                    if point == 'journal start':
                        crash_journals(self, 'step_start', match=lambda step_id, *a, **k: step_id == 'O4')
                    elif point == 'journal end':
                        crash_journals(self, 'step_end', match=lambda step_id, state, *a, **k: step_id == 'O4' and state == 'failed')
                    else:
                        stack.enter_context(mock.patch.object(inventory, 'write', crash_write(point == 'after')))
                    with self.assertRaises(SimulatedCrash):
                        run_cli(redo)
                state = journal_state()[0]
                expected = {'journal start': ('passed', 'table orders'), 'journal end': ('running', 'table orders'),
                            'before': ('failed', 'table orders'), 'after': ('failed', 'procedure nightly_rollup')}[point]
                self.assertEqual((state, captured()), expected, 'O4 is never passed on a capture that holds the refused target')
                code, text = run_cli(redo)
                self.assertEqual(code, EXIT_REFUSED, text)
                self.assertIn('O4 refused for example_orders_db: orders_stream loads into procedure nightly_rollup, which objects.routines.ignore lists', text)
                self.assertEqual((journal_state(), captured()), (('failed', ['O4']), 'procedure nightly_rollup'))
                code, text = run_cli(['step', 'O4'] + base)
                self.assertEqual(code, EXIT_REFUSED, text)
                self.assertIn('orders_stream loads into procedure nightly_rollup', text)
                data = yaml.safe_load(path.read_text(encoding='utf8'))
                data['objects'] = {'routines': {'ignore': []}}
                path.write_text(yaml.safe_dump(data, sort_keys=False), encoding='utf8')
                code, text = run_cli(['profile', '--step', 'O4', '--inventory', str(Path(tmp) / 'o4')] + base)
                self.assertEqual((code, journal_state()[0]), (EXIT_DONE, 'passed'), text)


class CopyCliTests(unittest.TestCase):
    def test_dry_run_prints_statements_from_a_capture_and_the_copy_commands_work(self):
        from tests.test_copy import Harness
        h = Harness(self)
        base = ['--config', str(h.config_path), '--run', h.run_id]
        h.journal.close()
        code, text = run_cli(['run', '--dry-run', '--from', 'O5', '--to', 'O8'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('DRY RUN: CREATE DATABASE `example_orders_db_rebuild_20261101` PARTITIONS 32 SUB_PARTITIONS 512', text)
        self.assertEqual(text.count('DRY RUN: INSERT INTO'), 7)
        self.assertIn('DRY RUN: SELECT CRC32', text)
        self.assertIn('No connection was opened', text)
        code, text = run_cli(['pause'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE)
        self.assertTrue((h.journal_path.parent / 'journal.sqlite.stop').is_file())
        code, text = run_cli(['resume'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE)
        self.assertFalse((h.journal_path.parent / 'journal.sqlite.stop').exists())
        code, text = run_cli(['mark-recopy', 't_single'] + base, env=h.env)
        self.assertEqual(code, EXIT_REFUSED, 'nothing planned yet in the journal')
        self.assertIn('not in the journal', text)
        code, text = run_cli(['report'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertTrue((h.tmp / 'runs' / h.run_id / 'example_orders_db' / 'reports' / 'final.xlsx').is_file())
        code, text = run_cli(['watch', '--dry-run'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('DRY RUN: SELECT ID, USER, HOST', text)
        code, text = run_cli(['quiet-proof', '--dry-run'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        code, text = run_cli(['step', 'O6', '--fresh-table', 't_single', '--dry-run'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)


class LiveJournalCliTests(unittest.TestCase):
    def test_run_gate_status_cycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp)
            base = ['--config', str(path)]
            code, text = run_cli(['status'] + base)
            self.assertEqual(code, EXIT_REFUSED)
            self.assertIn('No run exists', text)
            code, text = run_cli(['gate', 'P1', '--pass', '--note', 'account, Keychain items and config in place', '--run', '20261101-0500'] + base)
            self.assertEqual(code, EXIT_DONE, 'the P1 gate starts the first run')
            record_passed(cfg, '20261101-0500', ('P2',))
            code, text = run_cli(['run', '--run', '20261101-0500', '--no-rehearsal', 'first run of the kit'] + base)
            self.assertEqual(code, EXIT_GATE, text)
            self.assertIn('Stopped at O1', text)
            journal = Path(tmp) / 'runs' / '20261101-0500' / 'example_orders_db' / 'journal.sqlite'
            self.assertTrue(journal.exists())
            logs = list((Path(tmp) / 'runs' / '20261101-0500' / 'logs').glob('run-*.log'))
            self.assertEqual(len(logs), 1)
            self.assertIn('[O1] Start', logs[0].read_text())
            code, text = run_cli(['gate', 'O1', '--pass', '--note', 'would go', '--dry-run'] + base)
            self.assertEqual(code, EXIT_DONE)
            self.assertIn('DRY RUN: would record gate O1 as PASS for run 20261101-0500, database(s) example_orders_db: would go', text)
            self.assertIn('Nothing was written.', text)
            self.assertEqual(len(list((Path(tmp) / 'runs' / '20261101-0500' / 'logs').glob('gate-O1-*.log'))), 0)
            code, text = run_cli(['status'] + base)
            self.assertRegex(text, r'O1\s+Start\s+pending', 'the dry-run gate recorded nothing')
            code, text = run_cli(['gate', 'O1', '--pass', '--note', 'go recorded'] + base)
            self.assertEqual(code, EXIT_DONE)
            self.assertIn('gate O1 recorded as PASS for run 20261101-0500, database(s) example_orders_db: go recorded', text)
            code, text = run_cli(['gate', 'O6', '--pass', '--note', 'by hand'] + base)
            self.assertEqual(code, EXIT_REFUSED)
            self.assertIn('python step the kit runs', text)
            code, text = run_cli(['run', '--from', 'O2'] + base)
            self.assertEqual(code, EXIT_GATE, 'the most recent run is picked up without --run')
            self.assertIn('[O2] Stop pipelines', text)
            code, text = run_cli(['run', '--from', 'O4'] + base)
            self.assertEqual(code, EXIT_REFUSED)
            self.assertIn('Cannot run O4 for database example_orders_db: earlier steps are not passed: O2, O3', text)
            evidence = Path(tmp) / 'o2.txt'
            evidence.write_text('pipelines: 0 running')
            code, text = run_cli(['gate', 'O2', '--fail', '--note', 'one still running', '--evidence', str(evidence)] + base)
            self.assertEqual(code, EXIT_DONE)
            code, text = run_cli(['gate', 'O3', '--pass', '--note', 'x', '--evidence', str(Path(tmp) / 'missing.txt')] + base)
            self.assertEqual(code, EXIT_REFUSED)
            self.assertIn('Evidence file not found', text)
            code, text = run_cli(['status'] + base)
            self.assertEqual(code, EXIT_DONE)
            self.assertIn('run 20261101-0500', text)
            self.assertRegex(text, r'O1\s+Start\s+passed')
            self.assertRegex(text, r'O2\s+Stop pipelines\s+failed')
            self.assertIn('[evidence: %s]' % evidence.resolve(), text)
            self.assertRegex(text, r'O3\s+Freeze\s+not started')
            code, text = run_cli(['step', 'P1'] + base)
            self.assertEqual(code, EXIT_DONE)
            self.assertIn('[P1] Prerequisites (human + admin; owner) database example_orders_db: already passed, skipped', text)
            code, text = run_cli(['step', 'P4'] + base)
            self.assertEqual(code, EXIT_REFUSED)
            self.assertIn('Cannot run P4 for database example_orders_db: earlier steps are not passed: P3', text)
            code, text = run_cli(['run', '--dry-run', '--to', 'O2'] + base)
            self.assertEqual(code, EXIT_DONE)
            self.assertIn('already passed, skipped', text, 'a dry run reads the live journal')

    def test_run_without_run_id_starts_the_first_run_and_env_run_is_honored(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp)
            code, text = run_cli(['run', '--config', str(path)])
            self.assertEqual(code, EXIT_REFUSED, 'with no run at all run starts one; it holds no P2, so O1 refuses (F5)')
            self.assertIn('P2 is not passed in this run (not run)', text)
            runs = [p.name for p in (Path(tmp) / 'runs').iterdir()]
            self.assertEqual(len(runs), 1)
            self.assertRegex(runs[0], r'^\d{8}-\d{4}$')
            code, text = run_cli(['status', '--config', str(path)], env={'S2REBUILD_RUN': 'named-run'})
            self.assertEqual(code, EXIT_REFUSED)
            self.assertIn('Run named-run does not exist under', text)
            self.assertIn('existing runs: %s' % runs[0], text)
            code, text = run_cli(['run', '--config', str(path), '--run', 'bad id!'])
            self.assertEqual(code, EXIT_REFUSED)
            self.assertIn('Run id must be', text)
            code, text = run_cli(['run', '--config', str(path), '--run', 'dry-run'])
            self.assertEqual(code, EXIT_REFUSED)
            self.assertIn('reserved for --dry-run', text)
            code, text = run_cli(['run', '--config', str(path)])
            self.assertEqual(code, EXIT_REFUSED, 'a second run resumes the first run, which still holds no P2 (F6)')
            self.assertEqual(sorted(p.name for p in (Path(tmp) / 'runs').iterdir()), runs)
            code, text = run_cli(['run', '--new', '--config', str(path)])
            self.assertEqual(code, EXIT_REFUSED, 'run --new starts a second run, which holds no P2 either')
            runs_now = sorted(p.name for p in (Path(tmp) / 'runs').iterdir())
            self.assertEqual(len(runs_now), 2)
            self.assertTrue(any(name.count('-') == 2 for name in runs_now) or runs_now[0][:13] != runs_now[1][:13],
                            'the second id is unique even inside the same minute: %s' % runs_now)

    def test_most_recent_run_is_by_change_time_not_by_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp)
            base = ['--config', str(path), '--no-rehearsal', 'selection test']
            record_passed(cfg, 'zz-rehearsal')
            code, text = run_cli(['run', '--run', 'zz-rehearsal'] + base)
            self.assertEqual(code, EXIT_GATE, text)
            old = time.time() - 3600
            for p in (Path(tmp) / 'runs' / 'zz-rehearsal').rglob('*'):
                os.utime(p, (old, old))
            os.utime(Path(tmp) / 'runs' / 'zz-rehearsal', (old, old))
            record_passed(cfg, '20261101-0500')
            code, text = run_cli(['run', '--run', '20261101-0500'] + base)
            self.assertEqual(code, EXIT_GATE, text)
            base = base[:2]
            code, text = run_cli(['status'] + base)
            self.assertEqual(code, EXIT_DONE)
            self.assertIn('run 20261101-0500', text)
            self.assertNotIn('zz-rehearsal', text)
            code, text = run_cli(['gate', 'O1', '--pass', '--note', 'go'] + base)
            self.assertIn('for run 20261101-0500', text)

    def test_f12_lockout_with_two_databases_is_refused_at_check_run_and_step(self):
        """Path B is single database in this pass: run and step O3, O10 and O11 refuse before any step is recorded, --db included;
        check refuses before any connection and records nothing. The print and notebook routes refuse as well: owner-sql O3,
        O10 and O11 and owner-notebook write no block file, and gate O3, O10 and O11 --pass record nothing. A connection
        attempt fails the test instead of reaching a host."""
        from unittest import mock
        from s2rebuild.connect import SessionFactory
        from s2rebuild.journal import Journal

        def never(**kw):
            raise AssertionError('the refusal must come before any connection')

        def two_on_path_b(d):
            d['databases'].append({'source': 'second_db', 'layout': {'partitions': 8, 'sub_partitions': 64}})
            d['freeze'] = {'mode': 'lockout'}
        refusal = ('freeze.mode lockout (Path B) is single database in this pass, and the configuration has 2 databases '
                   '(example_orders_db, second_db)')
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp, two_on_path_b)
            base = ['--config', str(path), '--run', '20261101-0500']
            env = {'S2REBUILD_RUNNER_PASSWORD': 'fixture-password'}
            for argv in (['run', '--yes'], ['run', '--from', 'O1', '--db', 'example_orders_db'], ['step', 'O3', '--yes'], ['step', 'O10', '--yes'],
                         ['step', 'O11', '--db', 'second_db'], ['check', '--yes'], ['owner-sql', 'O11', '--yes'],
                         ['owner-sql', 'O3', '--db', 'example_orders_db'], ['owner-sql', 'O10', '--yes', '--dry-run'], ['owner-notebook', '--yes'],
                         ['gate', 'O3', '--pass', '--note', 'x'], ['gate', 'O10', '--pass', '--note', 'x', '--db', 'second_db'],
                         ['gate', 'O11', '--pass', '--note', 'x', '--yes', '--dry-run']):
                with mock.patch.object(cli, 'SessionFactory', lambda config, env, log, dry_run: SessionFactory(config, env, log, dry_run, connector=never)):
                    code, text = run_cli(argv + base, env=env)
                self.assertEqual(code, EXIT_REFUSED, (argv, text))
                self.assertIn(refusal, text, argv)
            run_dir = Path(tmp) / 'runs' / '20261101-0500'
            self.assertEqual(sorted(p.name for p in run_dir.rglob('*') if p.suffix in ('.sql', '.ipynb')), [], 'no block file is written')
            for source in ('example_orders_db', 'second_db'):
                # F5's entry rule: a refused command writes nothing, so no step is recorded for either database
                path_ = run_dir / source / 'journal.sqlite'
                if path_.exists():
                    journal = Journal(path_, read_only=True)
                    try:
                        self.assertEqual(journal.steps_board(), {}, source)
                    finally:
                        journal.close()

    def test_refused_arguments_create_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp)
            base = ['--config', str(path)]
            for argv in (['run', '--from', 'O12'], ['run', '--from', 'O7', '--to', 'O6'], ['run', '--from', 'P1'],
                         ['step', 'Q1'], ['gate', 'O99', '--pass', '--note', 'x'], ['gate', 'O6', '--pass', '--note', 'x'],
                         ['gate', 'O1', '--pass', '--note', 'x'], ['run', '--from', 'O2'], ['step', 'O2', '--run', 'nope'],
                         ['gate', 'O2', '--pass', '--note', 'x', '--run', 'nope'], ['status'], ['run', '--new', '--from', 'O4']):
                code, text = run_cli(argv + base)
                self.assertEqual(code, EXIT_REFUSED, argv)
                self.assertFalse((Path(tmp) / 'runs').exists(), 'refusal of %s must not create a run: %s' % (argv, text))
            code, text = run_cli(['step', 'P1'] + base)
            self.assertEqual(code, EXIT_GATE, 'step creates the first run when none exists')
            runs = [p.name for p in (Path(tmp) / 'runs').iterdir()]
            self.assertEqual(len(runs), 1)
            code, text = run_cli(['step', 'P1', '--run', 'other'] + base)
            self.assertEqual(code, EXIT_GATE, 'a preparation step may start an explicitly named run')
            self.assertTrue((Path(tmp) / 'runs' / 'other').is_dir())
            code, text = run_cli(['gate', 'P1', '--pass', '--note', 'x', '--run', 'typo'] + base)
            self.assertEqual(code, EXIT_REFUSED)
            self.assertIn('Run typo does not exist', text)
            self.assertFalse((Path(tmp) / 'runs' / 'typo').exists())
            code, text = run_cli(['run', '--run', 'named-by-hand'] + base)
            self.assertEqual(code, EXIT_REFUSED, 'once a run exists, run --run must name an existing run (F6)')
            self.assertIn('Run named-by-hand does not exist', text)
            self.assertFalse((Path(tmp) / 'runs' / 'named-by-hand').exists())
            code, text = run_cli(['run', '--new', '--run', 'named-by-hand'] + base)
            self.assertEqual(code, EXIT_REFUSED, 'run --new may start an explicitly named run; O1 refuses there without P2 (F5)')
            self.assertIn('O1 refused: P2 is not passed in this run (not run)', text)
            self.assertTrue((Path(tmp) / 'runs' / 'named-by-hand').is_dir())

    def test_f9_gate_accepts_c1_and_c2_only_with_override(self):
        """gate C1 and gate C2 refuse without --override NOTE and name the flag; with it the gate is recorded and a gate_override
        event, written before the gate, holds the note. Every other python step, and --override on any other step, stays refused."""
        with tempfile.TemporaryDirectory() as tmp:
            path, _ = write_config(tmp)
            base = ['--config', str(path), '--run', '20261101-0500']
            journal_path = Path(tmp) / 'runs' / '20261101-0500' / 'example_orders_db' / 'journal.sqlite'
            Journal(journal_path).close()
            for step_id in ('C1', 'C2'):
                code, text = run_cli(['gate', step_id, '--pass', '--note', 'checked by hand'] + base)
                self.assertEqual(code, EXIT_REFUSED, text)
                self.assertIn('a %s the kit cannot pass is recorded with --override NOTE' % step_id, text)
            code, text = run_cli(['gate', 'C1', '--pass', '--note', 'writes seen in the Portal', '--override', 'runner lacks PROCESS'] + base)
            self.assertEqual(code, EXIT_DONE, text)
            code, text = run_cli(['gate', 'C2', '--fail', '--note', 'workbook not written', '--override', 'reports disk full'] + base)
            self.assertEqual(code, EXIT_DONE, text)
            for step_id in ('O6', 'O3'):
                code, text = run_cli(['gate', step_id, '--pass', '--note', 'x', '--override', 'y'] + base)
                self.assertEqual(code, EXIT_REFUSED, step_id)
                self.assertIn('--override records a gate for C1 and C2 only, not for %s' % step_id, text)
            journal = Journal(journal_path, read_only=True)
            try:
                self.assertEqual([journal.step_status(s) for s in ('C1', 'C2', 'O6', 'O3')], ['passed', 'failed', None, None])
                self.assertEqual(journal.events('gate_override'), ['C1 pass: runner lacks PROCESS', 'C2 fail: reports disk full'])
                kinds = [e['kind'] for e in journal.events()]
                self.assertLess(kinds.index('gate_override'), kinds.index('gate'))
            finally:
                journal.close()

    def test_f9_gate_override_fault_points_then_the_rerun_records_the_gate(self):
        """gate C1 --override has no statement, so the three failure points sit on its journal writes: a kill just before the
        gate_override event, just after it, and after it but before the gate's journal update. No point leaves C1 passed
        without its override event; the same gate command again records the gate with the event."""
        for point in FAULT_POINTS:
            with self.subTest(point=point), tempfile.TemporaryDirectory() as tmp:
                path, _ = write_config(tmp)
                argv = ['gate', 'C1', '--pass', '--note', 'writes seen in the Portal', '--override', 'runner lacks PROCESS',
                        '--config', str(path), '--run', '20261101-0500']
                journal_path = Path(tmp) / 'runs' / '20261101-0500' / 'example_orders_db' / 'journal.sqlite'
                Journal(journal_path).close()
                if point == 'journal':
                    fired = crash_journals(self, 'step_end', match=lambda step_id, *rest, **kw: step_id == 'C1')
                else:
                    fired = crash_journals(self, 'event', match=lambda kind, detail: kind == 'gate_override', after=point == 'after')
                with self.assertRaises(SimulatedCrash):
                    run_cli(argv)
                self.assertEqual(len(fired), 1)
                journal = Journal(journal_path, read_only=True)
                try:
                    self.assertIsNone(journal.step_status('C1'))
                finally:
                    journal.close()
                code, text = run_cli(argv)
                self.assertEqual(code, EXIT_DONE, text)
                journal = Journal(journal_path, read_only=True)
                try:
                    self.assertEqual(journal.step_status('C1'), 'passed')
                    self.assertEqual(journal.events('gate_override'), ['C1 pass: runner lacks PROCESS'] * (1 if point == 'before' else 2))
                finally:
                    journal.close()


RUN = '20261101-0500'
LIVE_ENV = {'S2REBUILD_RUNNER_PASSWORD': 'fixture-password'}
NAME_REFUSAL = ('Journal for example_orders_db records replacement database example_orders_db_rebuild_20261101 but the configuration '
                'says example_orders_db_rebuild_20261102')
HASH_REFUSAL = 'it changed after O1. Put the configuration of this run back, or record the change with --accept-config-change NOTE'
CAPTURE_READ = r'^SELECT TABLE_NAME, TABLE_TYPE FROM information_schema\.TABLES'


def snapshot(folder):
    """sha256 of every file under folder by relative path; journal files are left out (every writable open records the schema version)."""
    folder = Path(folder)
    return {str(p.relative_to(folder)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(folder.rglob('*'))
            if p.is_file() and not p.name.startswith('journal.sqlite')}


class LiveRun:
    """A configuration in a temporary folder and a fake workspace behind the driver: live commands on the command line, offline."""

    def __init__(self, test, mutate=None):
        from tests.test_copy import fake_db
        self.tmp = Path(tempfile.mkdtemp())
        test.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.path, self.config = write_config(self.tmp, mutate)
        self.fake = fake_db()
        fake_driver(test, self.fake)
        original = inventory.capture
        patcher = mock.patch.object(inventory, 'capture', lambda *a, **k: original(*a, **dict(k, sleep=lambda s: None)))
        patcher.start()
        test.addCleanup(patcher.stop)
        self.run_dir = self.tmp / 'runs' / RUN / 'example_orders_db'

    def cli(self, *argv):
        """Run the command line on run RUN with this configuration."""
        return run_cli(list(argv) + ['--config', str(self.path), '--run', RUN], env=LIVE_ENV)

    def journal(self):
        """The run journal, closed when the with block ends."""
        return contextlib.closing(Journal(self.run_dir / 'journal.sqlite'))

    def rewrite(self, mutate):
        """Edit the configuration file as an operator would."""
        data = yaml.safe_load(self.path.read_text(encoding='utf8'))
        mutate(data)
        self.path.write_text(yaml.safe_dump(data, sort_keys=False), encoding='utf8')

    def started(self, test):
        """P1 gated, P2 by a live check, O1 run with the rehearsal waived: O1 recorded the names and the configuration hash."""
        code, text = self.cli('gate', 'P1', '--pass', '--note', 'account, Keychain items and config in place')
        test.assertEqual(code, EXIT_DONE, text)
        code, text = self.cli('check')
        test.assertEqual(code, EXIT_DONE, text)
        code, text = self.cli('run', '--no-rehearsal', 'no rehearsal for this test')
        test.assertEqual(code, EXIT_GATE, text)
        return self


class EntryPointRuleTests(unittest.TestCase):
    """F5: check, profile, gate, owner-sql, owner-notebook, run and step follow one rule set."""

    def test_f5_check_refused_without_p1_and_the_p1_gate_starts_the_first_run(self):
        """check needs P1 passed in the run and creates nothing when it refuses; the P1 gate starts the first run; a passed P2 runs again."""
        h = LiveRun(self)
        base = ['--config', str(h.path)]
        code, text = run_cli(['check'] + base, env=LIVE_ENV)
        self.assertEqual(code, EXIT_REFUSED, text)
        self.assertIn('Cannot run P2 for database example_orders_db: earlier steps are not passed: P1. Record P1 with s2rebuild gate P1 '
                      '--pass --note TEXT --db example_orders_db.', text)
        self.assertFalse((h.tmp / 'runs').exists(), 'a refused check creates no run')
        self.assertEqual(h.fake.statements, [], 'and opens no connection')
        code, text = run_cli(['check', '--dry-run'] + base, env=LIVE_ENV)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('[P2] Preflight (python; runner + owner) database example_orders_db: dry run; earlier steps without a passed gate: P1', text)
        code, text = run_cli(['gate', 'P1', '--pass', '--note', 'account, Keychain items and config in place'] + base, env=LIVE_ENV)
        self.assertEqual(code, EXIT_DONE, text)
        runs = [p.name for p in (h.tmp / 'runs').iterdir()]
        self.assertEqual(len(runs), 1, 'the P1 gate starts the first run')
        code, text = run_cli(['gate', 'P1', '--pass', '--note', 'x', '--run', 'typo'] + base, env=LIVE_ENV)
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('Run typo does not exist', text)
        for attempt in (1, 2):
            code, text = run_cli(['check'] + base, env=LIVE_ENV)
            self.assertEqual(code, EXIT_DONE, text)
            self.assertIn('check passed: P2 recorded in run %s; next, s2rebuild profile (P3)' % runs[0], text)
        with contextlib.closing(Journal(h.tmp / 'runs' / runs[0] / 'example_orders_db' / 'journal.sqlite')) as journal:
            self.assertEqual(journal.step_status('P2'), 'passed')
            self.assertEqual(journal.events('step_start'), ['P2', 'P2'], 'a passed P2 runs again without --redo')

    def test_w8_a_driver_error_prints_the_plain_line_and_exits_2(self):
        """A connection reset (2003 as the session opens) ends the command with one plain line and exit 2; the raw error goes
        to the log file only, redacted; the step is recorded failed, so a rerun re-enters."""
        h = LiveRun(self)
        self.assertEqual(h.cli('gate', 'P1', '--pass', '--note', 'in place')[0], EXIT_DONE)
        h.fake.fail_connect('s2rebuild_runner')
        code, text = h.cli('check')
        self.assertEqual(code, EXIT_INCOMPLETE, text)
        self.assertIn('INCOMPLETE: a node reset the connection; the workspace may be scaling or restarting; rerun the step '
                      '(driver error 2003; the raw error is in the log file)', text)
        self.assertNotIn("Can't connect", text)
        logs = list((h.tmp / 'runs' / RUN / 'logs').glob('check-*.log'))
        self.assertEqual(len(logs), 1)
        log_text = logs[0].read_text(encoding='utf8')
        self.assertIn("driver error 2003: FakeDisconnect: (2003, \"Can't connect to the server (injected)\")", log_text)
        self.assertIn('result: failed: interrupted by FakeDisconnect (error 2003)', log_text)
        with h.journal() as journal:
            self.assertEqual(journal.step_status('P2'), 'failed')
        code, text = h.cli('check')
        self.assertEqual(code, EXIT_DONE, text)

    def test_f5_check_fault_points_resume_with_a_plain_rerun(self):
        """The P2 preflight at the three points: a lost connection before and after its source attachment read, and a kill after
        every read but before the preflight record is journaled. P2 is left failed, never passed; a plain rerun of check passes
        and records the source STORAGE_ID. A kill at the passed write itself leaves P2 running (before) or passed (after); check
        reruns it either way."""
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = LiveRun(self)
                self.assertEqual(h.cli('gate', 'P1', '--pass', '--note', 'in place')[0], EXIT_DONE)
                if point == 'journal':
                    fired = crash_journals(self, 'set_meta', match=lambda key, value: key == 'preflight')
                    with self.assertRaises(SimulatedCrash):
                        h.cli('check')
                    self.assertEqual(len(fired), 1)
                else:
                    h.fake.fail_statement(r'^SELECT DATABASE_NAME, ATTACH_TYPE, STORAGE_ID', point)
                    code, text = h.cli('check')
                    # a lost connection (2013) is a driver reset: one plain line and exit 2 (W8); the step is still recorded failed
                    self.assertEqual(code, EXIT_INCOMPLETE, text)
                    self.assertIn('INCOMPLETE: a node reset the connection; the workspace may be scaling or restarting; rerun the step '
                                  '(driver error 2013; the raw error is in the log file)', text)
                    self.assertNotIn('ERROR: FakeDisconnect', text, 'the raw error goes to the log file')
                    self.assertEqual(h.fake.faults_fired[0][0], point)
                with h.journal() as journal:
                    row = journal.step('P2')
                    self.assertEqual(row['status'], 'failed')
                    self.assertTrue(row['note'].startswith('interrupted by'), row['note'])
                    if point != 'journal':
                        self.assertIsNone(journal.attachment('source'))
                code, text = h.cli('check')
                self.assertEqual(code, EXIT_DONE, text)
                with h.journal() as journal:
                    self.assertEqual(journal.step_status('P2'), 'passed')
                    self.assertEqual(journal.attachment('source')['storage_id'], '1028600672070281646:7367158985892520834')
        for after in (False, True):
            with self.subTest(result_write='after' if after else 'before'):
                h = LiveRun(self)
                self.assertEqual(h.cli('gate', 'P1', '--pass', '--note', 'in place')[0], EXIT_DONE)
                crash_journals(self, 'step_end', match=lambda step_id, status, *a, **k: (step_id, status) == ('P2', 'passed'), after=after)
                with self.assertRaises(SimulatedCrash):
                    h.cli('check')
                with h.journal() as journal:
                    self.assertEqual(journal.step_status('P2'), 'passed' if after else 'running', 'a kill at the result write')
                code, text = h.cli('check')
                self.assertEqual(code, EXIT_DONE, 'check reruns P2 either way: %s' % text)

    def test_f5_profile_fault_points_resume_with_a_plain_rerun(self):
        """P3 and O4 captures at the three points: a lost connection before and after the first capture read (nothing journaled,
        nothing written under inventory/), and a kill after the files but before the last journal writes (the step is left
        failed, never passed). A plain rerun captures again and passes; a further rerun needs --redo. A kill at the passed write
        itself leaves the step running (a plain rerun captures again) or passed (the rerun needs --redo)."""
        for step_id in ('P3', 'O4'):
            for point in FAULT_POINTS:
                with self.subTest(step=step_id, point=point):
                    h = LiveRun(self)
                    record_passed(h.config, RUN, ('P1', 'P2') + (('O1', 'O2', 'O3') if step_id == 'O4' else ()))
                    target = h.run_dir / 'inventory' / step_id
                    argv = ['profile', '--step', step_id]
                    if point == 'journal':
                        seen, original = [], Journal.set_meta

                        def killed(journal, key, value, step_id=step_id, seen=seen, original=original):
                            if key == 'inventory_dir_' + step_id and not seen:
                                seen.append(journal.step_status(step_id))
                                raise SimulatedCrash('killed before the journal write: set_meta %s' % key)
                            return original(journal, key, value)
                        with mock.patch.object(Journal, 'set_meta', killed):
                            with self.assertRaises(SimulatedCrash):
                                h.cli(*argv)
                        self.assertEqual(seen, ['running'], 'at the kill the step is running, never passed: passed is the last write')
                        with h.journal() as journal:
                            self.assertEqual(journal.step_status(step_id), 'failed')
                        self.assertTrue((target / 'inventory.json').is_file(), 'the files were written before the kill')
                    else:
                        h.fake.fail_statement(CAPTURE_READ, point)
                        code, text = h.cli(*argv)
                        self.assertEqual(code, EXIT_INCOMPLETE, text)
                        self.assertIn('INCOMPLETE: a node reset the connection', text)
                        with h.journal() as journal:
                            self.assertIsNone(journal.step_status(step_id), 'a capture that stops journals nothing')
                        self.assertFalse(target.exists(), 'and writes nothing under inventory/')
                        self.assertFalse((h.run_dir / 'reports').exists())
                    code, text = h.cli(*argv)
                    self.assertEqual(code, EXIT_DONE, text)
                    with h.journal() as journal:
                        self.assertEqual(journal.step_status(step_id), 'passed')
                        self.assertEqual(journal.get_meta('inventory_dir_' + step_id), str(target))
                        self.assertEqual(journal.attachment('source')['storage_id'], '1028600672070281646:7367158985892520834')
                    self.assertTrue((target / 'inventory.json').is_file())
                    self.assertEqual((target / 'plan.json').is_file(), step_id == 'O4')
                    code, text = h.cli(*argv)
                    self.assertEqual(code, EXIT_REFUSED, text)
                    self.assertIn('%s passed already for database example_orders_db; a rerun rewrites' % step_id, text)
            for after in (False, True):
                with self.subTest(step=step_id, result_write='after' if after else 'before'):
                    h = LiveRun(self)
                    record_passed(h.config, RUN, ('P1', 'P2') + (('O1', 'O2', 'O3') if step_id == 'O4' else ()))
                    argv = ['profile', '--step', step_id]
                    crash_journals(self, 'step_end', match=lambda sid, status, *a, **k: (sid, status) == (step_id, 'passed'), after=after)
                    with self.assertRaises(SimulatedCrash):
                        h.cli(*argv)
                    with h.journal() as journal:
                        self.assertEqual(journal.step_status(step_id), 'passed' if after else 'running')
                    code, text = h.cli(*argv)
                    if after:
                        self.assertEqual(code, EXIT_REFUSED, 'the passed capture is kept until --redo: %s' % text)
                        self.assertIn('Run it again with --redo', text)
                    else:
                        self.assertEqual(code, EXIT_DONE, 'a step left running is captured again by a plain rerun: %s' % text)

    def test_f5_profile_o4_refused_without_o3_and_nothing_written(self):
        """profile --step O4 runs O4 only after O1 to O3: refused before any connection or file, a dry run reports it, and a rerun of
        a passed O4 needs --redo, which names the copy plan it rewrites. A refusal without a statement of its own: the three
        failure points of the capture it guards are test_f5_profile_fault_points_resume_with_a_plain_rerun."""
        h = LiveRun(self)
        record_passed(h.config, RUN, ('P1', 'P2', 'O1', 'O2'))
        files = snapshot(h.tmp)
        with h.journal() as journal:
            board, events = journal.steps_board(), journal.events()
        for argv in (['profile', '--step', 'O4'], ['profile', '--step', 'O4', '--inventory', str(FIXTURE_INVENTORY)],
                     ['profile', '--step', 'O4', '--redo']):
            code, text = h.cli(*argv)
            self.assertEqual(code, EXIT_REFUSED, text)
            self.assertIn('REFUSED: Cannot run O4 for database example_orders_db: earlier steps are not passed: O3. Record O3 with '
                          's2rebuild gate O3 --pass --note TEXT --db example_orders_db', text)
        self.assertEqual(snapshot(h.tmp), files, 'nothing was written: no capture, no plan, no workbook, no log')
        self.assertEqual(h.fake.statements, [], 'no connection was opened')
        with h.journal() as journal:
            self.assertEqual((journal.steps_board(), journal.events()), (board, events))
        code, text = h.cli('profile', '--step', 'O4', '--dry-run', '--inventory', str(FIXTURE_INVENTORY))
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('DRY RUN: a real run refuses here: Cannot run O4 for database example_orders_db: earlier steps are not passed: O3', text)
        self.assertFalse((h.run_dir / 'inventory' / 'O4').exists())
        self.assertTrue((h.run_dir / 'reports' / 'freeze-capture-dry-run.xlsx').is_file())
        self.assertEqual(h.cli('gate', 'O3', '--pass', '--note', 'read only verified')[0], EXIT_DONE)
        code, text = h.cli('profile', '--step', 'O4')
        self.assertEqual(code, EXIT_DONE, text)
        plan = (h.run_dir / 'inventory' / 'O4' / 'plan.json').read_bytes()
        code, text = h.cli('profile', '--step', 'O4')
        self.assertEqual(code, EXIT_REFUSED, text)
        self.assertIn('O4 passed already for database example_orders_db; a rerun rewrites the O4 freeze capture under inventory/O4 with '
                      'plan.json and drift.json, the copy plan O5 to O8 read', text)
        self.assertIn('Run it again with --redo, or leave it as it is.', text)
        self.assertEqual((h.run_dir / 'inventory' / 'O4' / 'plan.json').read_bytes(), plan)
        code, text = h.cli('profile', '--step', 'O4', '--redo')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('warning: O4 passed already for example_orders_db; --redo captures again and rewrites the O4 freeze capture', text)
        logs = list((h.tmp / 'runs' / RUN / 'logs').glob('profile-O4-*.log'))
        self.assertTrue(any('--redo captures again and rewrites' in p.read_text() for p in logs), 'the warning is in the log')

    def test_f5_dry_run_profile_leaves_the_inventory_directory_byte_for_byte(self):
        """A dry-run profile (P3 and O4, with and without --redo) after the configuration changed the plan writes nothing under
        inventory/: every file hashes the same before and after; only the -dry-run workbooks are written. A dry run without a
        statement of its own: the three failure points of the capture write it guards are
        test_f5_profile_fault_points_resume_with_a_plain_rerun."""
        h = LiveRun(self)
        record_passed(h.config, RUN, ('P1', 'P2'))
        self.assertEqual(h.cli('profile')[0], EXIT_DONE)
        record_passed(h.config, RUN, ('O1', 'O2', 'O3'))
        self.assertEqual(h.cli('profile', '--step', 'O4')[0], EXIT_DONE)
        folder = h.run_dir / 'inventory'
        before = snapshot(folder)
        self.assertIn('O4/plan.json', before)
        self.assertIn('P3/tables.csv', before)
        with h.journal() as journal:
            board = journal.steps_board()
        h.rewrite(lambda d: d.setdefault('copy', {}).update(chunk_rows=100, min_rows_for_chunking=50))
        for argv in (['profile', '--dry-run'], ['profile', '--step', 'O4', '--dry-run'], ['profile', '--step', 'O4', '--dry-run', '--redo']):
            code, text = h.cli(*argv)
            self.assertEqual(code, EXIT_DONE, text)
            self.assertIn('nothing was written under inventory/', text)
            self.assertEqual(snapshot(folder), before, 'the dry run %s rewrote the capture' % ' '.join(argv))
        self.assertIn('warning: O4 passed already for example_orders_db; --redo captures again', text)
        self.assertTrue((h.run_dir / 'reports' / 'profile-dry-run.xlsx').is_file())
        self.assertTrue((h.run_dir / 'reports' / 'freeze-capture-dry-run.xlsx').is_file())
        with h.journal() as journal:
            self.assertEqual(journal.steps_board(), board, 'a dry run journals nothing')
        code, text = h.cli('profile', '--step', 'O4', '--redo')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertNotEqual(snapshot(folder)['O4/plan.json'], before['O4/plan.json'], 'the real rerun does rewrite the plan')

    def test_f5_changed_replacement_name_after_o1_refused_at_every_entry_point(self):
        """A replacement name changed after O1 is refused by every command that opens the journal, before it prints, writes or
        records anything; --accept-config-change does not override names; status only reports it. A refusal without a statement
        of its own: the three failure points of the O1 records it compares with are test_steps
        test_f5_o1_journal_fault_points_resume_on_rerun."""
        h = LiveRun(self).started(self)
        h.rewrite(lambda d: d['databases'][0].update(replacement='example_orders_db_rebuild_20261102'))
        with h.journal() as journal:
            board, events = journal.steps_board(), journal.events()
        statements = len(h.fake.statements)
        for argv in (['owner-sql', 'O3'], ['owner-sql', 'O10', '--rollback'], ['owner-notebook'], ['gate', 'O1', '--pass', '--note', 'go'],
                     ['run', '--from', 'O2'], ['run', '--no-rehearsal', 'x'], ['step', 'O2'], ['check'], ['profile', '--step', 'O4'],
                     ['mark-recopy', 't_single'], ['owner-sql', 'O3', '--accept-config-change', 'renamed on purpose'],
                     ['owner-sql', 'O3', '--dry-run']):
            code, text = h.cli(*argv)
            self.assertEqual(code, EXIT_REFUSED, argv)
            self.assertIn(NAME_REFUSAL, text, argv)
            self.assertNotIn('ATTACH DATABASE', text, argv)
        self.assertFalse((h.run_dir / 'owner-sql').exists(), 'no block was written')
        self.assertFalse((h.tmp / 'runs' / RUN / 'owner-steps.ipynb').exists(), 'no notebook was written')
        self.assertEqual(len(h.fake.statements), statements, 'no statement was issued')
        with h.journal() as journal:
            self.assertEqual((journal.steps_board(), journal.events()), (board, events), 'nothing was recorded')
        code, text = h.cli('status')
        self.assertEqual(code, EXIT_DONE)
        self.assertIn('warning: ' + NAME_REFUSAL, text)

    def test_f5_config_change_after_o1_refused_then_accepted_and_recorded(self):
        """Before O1 a configuration change counts for nothing (P4 decisions); after O1 every command refuses it until
        --accept-config-change NOTE records both hashes and the note (here through the read-only owner-sql); a dry run and status
        report it."""
        h = LiveRun(self)
        self.assertEqual(h.cli('gate', 'P1', '--pass', '--note', 'in place')[0], EXIT_DONE)
        self.assertEqual(h.cli('check')[0], EXIT_DONE)
        h.rewrite(lambda d: d['window'].update(buffer_minutes=20))
        self.assertEqual(h.cli('check')[0], EXIT_DONE, 'no hash is recorded before O1')
        self.assertEqual(h.cli('run', '--no-rehearsal', 'no rehearsal for this test')[0], EXIT_GATE)
        with h.journal() as journal:
            old = journal.get_meta('config_hash')
            board = journal.steps_board()
        h.rewrite(lambda d: d.setdefault('copy', {}).update(workers=8))
        for argv in (['gate', 'O1', '--pass', '--note', 'go'], ['owner-sql', 'O2'], ['run', '--from', 'O2'], ['step', 'O2'],
                     ['owner-notebook'], ['check']):
            code, text = h.cli(*argv)
            self.assertEqual(code, EXIT_REFUSED, argv)
            self.assertIn('Journal for example_orders_db records configuration hash %s from O1' % old[:12], text)
            self.assertIn(HASH_REFUSAL, text)
        self.assertFalse((h.run_dir / 'owner-sql').exists())
        code, text = h.cli('status')
        self.assertEqual(code, EXIT_DONE)
        self.assertIn('warning: Journal for example_orders_db records configuration hash', text)
        code, text = h.cli('run', '--from', 'O2', '--to', 'O2', '--dry-run')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('DRY RUN: Journal for example_orders_db records configuration hash', text)
        self.assertIn('a real run refuses here', text)
        with h.journal() as journal:
            self.assertEqual((journal.get_meta('config_hash'), journal.steps_board()), (old, board), 'refusals and dry runs record nothing')
        code, text = h.cli('owner-sql', 'O2', '--accept-config-change', 'workers raised to 8 for the copy')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('example_orders_db: configuration change after O1 recorded (%s to ' % old[:12], text)
        with h.journal() as journal:
            new = journal.get_meta('config_hash')
            self.assertNotEqual(new, old)
            self.assertEqual(journal.events('config_change'), ['configuration hash %s replaced by %s after O1: workers raised to 8 for the copy'
                                                               % (old, new)])
        code, text = h.cli('gate', 'O1', '--pass', '--note', 'go')
        self.assertEqual(code, EXIT_DONE, text)
        code, text = h.cli('owner-sql', 'O2', '--accept-config-change', 'again')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('the configuration hash matches the journal; nothing to record', text)
        with h.journal() as journal:
            self.assertEqual(len(journal.events('config_change')), 1)

    def test_f5_config_change_acceptance_fault_points(self):
        """The accepted change at the three points: a kill before its first write (the event), a kill just after it committed (at
        the gate that follows), and a kill between the event and the new hash (the transaction rolls back). The journal holds the
        old hash and no event, or the new hash and one event; a rerun of the same command records the gate with one event."""
        argv = ['gate', 'O1', '--pass', '--note', 'go', '--accept-config-change', 'workers raised']
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = LiveRun(self).started(self)
                with h.journal() as journal:
                    old = journal.get_meta('config_hash')
                h.rewrite(lambda d: d.setdefault('copy', {}).update(workers=8))
                method, match = {'before': ('event', lambda kind, detail: kind == 'config_change'),
                                 'after': ('record_gate', lambda step_id, *a, **k: step_id == 'O1'),
                                 'journal': ('set_meta', lambda key, value: key == 'config_hash')}[point]
                fired = crash_journals(self, method, match=match)
                with self.assertRaises(SimulatedCrash):
                    h.cli(*argv)
                self.assertEqual(len(fired), 1)
                with h.journal() as journal:
                    accepted = point == 'after'
                    self.assertEqual(journal.get_meta('config_hash') != old, accepted)
                    self.assertEqual(len(journal.events('config_change')), 1 if accepted else 0)
                    self.assertEqual(journal.step_status('O1'), 'pending', 'the gate was not recorded')
                code, text = h.cli('gate', 'O1', '--pass', '--note', 'go')
                if not accepted:
                    self.assertEqual(code, EXIT_REFUSED, 'without the note the change is still refused')
                    self.assertIn(HASH_REFUSAL, text)
                code, text = h.cli(*argv)
                self.assertEqual(code, EXIT_DONE, text)
                with h.journal() as journal:
                    self.assertEqual(journal.step_status('O1'), 'passed')
                    self.assertEqual(len(journal.events('config_change')), 1)
                    self.assertNotEqual(journal.get_meta('config_hash'), old)

    def test_f5_gate_warns_when_preconditions_are_unmet_and_records_anyway(self):
        """A gate records what a person observed: with earlier steps not passed it prints and logs a warning naming them and records
        the gate; a dry run prints the warning and records nothing. The O1 gate also warns where O1 never ran."""
        h = LiveRun(self)
        record_passed(h.config, RUN, ('P1', 'P2'))
        warning = ('warning: example_orders_db: earlier steps are not passed: O1, O2; the O3 gate is recorded anyway, since it records '
                   'what a person observed. Record O1 with s2rebuild gate O1 --pass --note TEXT --db example_orders_db')
        code, text = h.cli('gate', 'O3', '--pass', '--note', 'read only verified by hand', '--dry-run')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn(warning, text)
        with h.journal() as journal:
            self.assertIsNone(journal.step_status('O3'))
        code, text = h.cli('gate', 'O3', '--pass', '--note', 'read only verified by hand')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn(warning, text)
        logs = list((h.tmp / 'runs' / RUN / 'logs').glob('gate-O3-*.log'))
        self.assertEqual(len(logs), 1)
        self.assertIn(warning, logs[0].read_text())
        with h.journal() as journal:
            self.assertEqual(journal.step_status('O3'), 'passed')
        code, text = h.cli('gate', 'P1', '--pass', '--note', 'again')
        self.assertEqual(code, EXIT_DONE)
        self.assertNotIn('warning', text, 'P1 has no preconditions')
        code, text = h.cli('gate', 'O1', '--pass', '--note', 'go without running O1')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('warning: example_orders_db: O1 has not run in this journal, so its checks (P2, rehearsal evidence) and its records '
                      '(window, deadline, names, configuration hash) are missing; run s2rebuild run --from O1 --to O1 --redo --db '
                      'example_orders_db --run %s, then record the go again' % RUN, text)
        h.cli('run', '--from', 'O1', '--to', 'O1', '--redo', '--no-rehearsal', 'gate test')
        code, text = h.cli('gate', 'O1', '--pass', '--note', 'go after O1 ran')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertNotIn('warning', text)

    def test_f5_owner_notebook_dry_run_writes_nothing(self):
        """owner-notebook honors --dry-run: it prints the path and the blocks it would write and writes nothing."""
        from tests.test_copy import Harness
        h = Harness(self)
        h.journal.close()
        base = ['--config', str(h.config_path), '--run', h.run_id]
        path = h.tmp / 'runs' / h.run_id / 'owner-steps.ipynb'
        code, text = run_cli(['owner-notebook', '--dry-run'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('DRY RUN: example_orders_db: would write the notebook to %s (' % path, text)
        self.assertIn('O10-rollback', text)
        self.assertIn('nothing was written', text)
        self.assertFalse(path.exists())
        self.assertEqual(list((h.tmp / 'runs' / h.run_id).glob('logs/owner-notebook-*.log')), [], 'a dry run writes no log either')
        code, text = run_cli(['owner-notebook'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertTrue(path.is_file())

    def test_f5_two_databases_o1_records_every_journal_before_the_go(self):
        """With two databases run runs O1 for both before it stops, so the one gate O1 --pass is recorded where both journals hold
        the names and the configuration hash; owner-sql O3 --db second_db then refuses a changed configuration and a renamed
        second replacement and writes no block. A refusal without a statement of its own: the three failure points of the
        second database's O1 writes are in tests/test_steps.py test_f5_o1_runs_for_every_database_before_the_run_stops."""
        h = LiveRun(self, lambda d: d['databases'].append({'source': 'second_db', 'layout': {'partitions': 8, 'sub_partitions': 64}}))
        record_passed(h.config, RUN, ('P1', 'P2'))
        # W9: the second database leaves its names to the kit; --mode test applies the defaults without a prompt
        code, text = h.cli('run', '--no-rehearsal', 'no rehearsal for this test', '--yes', '--mode', 'test')
        self.assertEqual(code, EXIT_GATE, text)
        self.assertEqual(text.count('Stopped at O1'), 1, text)
        code, text = h.cli('gate', 'O1', '--pass', '--note', 'go', '--yes')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertNotIn('O1 has not run in this journal', text)
        for db in h.config.databases:
            with contextlib.closing(Journal(h.tmp / 'runs' / RUN / db.source / 'journal.sqlite')) as journal:
                self.assertEqual(journal.step_status('O1'), 'passed', db.source)
                self.assertEqual(journal.get_meta('config_hash'), h.config.config_hash(), db.source)
                self.assertEqual(tuple(journal.get_meta(k) for k in ('source_db', 'replacement_db', 'retained_db')), db.names)
                self.assertEqual(journal.events('no_rehearsal'), ['O1 without rehearsal evidence: no rehearsal for this test'])
        second = h.config.databases[1]
        blocks = h.tmp / 'runs' / RUN / 'second_db' / 'owner-sql'
        original = h.path.read_text(encoding='utf8')
        h.rewrite(lambda d: d.setdefault('copy', {}).update(workers=8))
        code, text = h.cli('owner-sql', 'O3', '--db', 'second_db')
        self.assertEqual(code, EXIT_REFUSED, text)
        self.assertIn('Journal for second_db records configuration hash %s from O1' % h.config.config_hash()[:12], text)
        self.assertIn(HASH_REFUSAL, text)
        h.path.write_text(original, encoding='utf8')
        h.rewrite(lambda d: d['databases'][1].update(replacement='second_db_renamed'))
        code, text = h.cli('owner-sql', 'O3', '--db', 'second_db')
        self.assertEqual(code, EXIT_REFUSED, text)
        self.assertIn('Journal for second_db records replacement database %s but the configuration says second_db_renamed'
                      % second.replacement, text)
        self.assertFalse(blocks.exists(), 'no block was written')
        h.path.write_text(original, encoding='utf8')
        code, text = h.cli('owner-sql', 'O3', '--db', 'second_db')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertTrue((blocks / 'O3.sql').is_file(), 'the configuration of the run writes the block')


def readme_sequence():
    """The commands of the README sequence block (the bash block that records the O1 gate), in order, as word lists."""
    text = (ROOT / 'README.md').read_text(encoding='utf8')
    blocks = [b for b in re.findall(r'```bash\n(.*?)```', text, re.S) if 's2rebuild gate O1' in b]
    if len(blocks) != 1:
        raise AssertionError('README.md must hold one bash block with the sequence to the go; found %d' % len(blocks))
    return [words for words in (shlex.split(line, comments=True) for line in blocks[0].splitlines()) if words]


def passed_rehearsal(ctx, prefix, rows=None, keep=False, kill_test=None, from_database=None, percent=1, start_pipelines=False):
    """What a passed rehearsal leaves in the run (rehearse.run on the fake is tested in test_rehearse): timings with P5 passed."""
    folder = ctx.config.runs_dir() / ctx.run_id / 'rehearsal'
    folder.mkdir(parents=True, exist_ok=True)
    (folder / 'timings.json').write_text(json.dumps({'prefix': prefix, 'p5_status': 'passed'}), encoding='utf8')
    ctx.journal.step_start('P5')
    ctx.journal.step_end('P5', 'passed', 'rehearsal %s passed' % prefix, str(folder / 'timings.json'))
    return steps.Outcome(steps.PASSED, 'rehearsal complete')


class RunSelectionTests(unittest.TestCase):
    """F6: run resumes the run the README sequence works on; every gate hint and log header names it with --run."""

    def bare(self, h, *argv, **env):
        """A command as the README writes it: no --run, so the run comes from S2REBUILD_RUN (env) or is the most recent one."""
        return run_cli(list(argv) + ['--config', str(h.path)], env=dict(LIVE_ENV, **env))

    def runs(self, h):
        return sorted(p.name for p in (h.tmp / 'runs').iterdir())

    def test_f6_readme_sequence_verbatim_ends_with_one_run_and_o1_passed(self):
        """The bash block of README.md, parsed out of the file and run line by line against the fake, P1 gate to the run after the go,
        with RUN_ID replaced by the id the P1 gate printed. The rehearsal is the one line whose work is stood in (passed_rehearsal):
        it forks and kills a copy process and needs the owner profile. The sequence ends with one run directory, O1 passed in it and
        the run stopped at O2. A sequence without a statement of its own: the three failure points of the journal writes that start
        and resume the run are test_f6_run_selection_fault_points_rerun_lands_in_the_same_run."""
        h = LiveRun(self)
        sequence = readme_sequence()
        self.assertEqual(sequence[-1], ['s2rebuild', 'run'], 'the sequence ends with the run that continues after the go')
        for needed in (['s2rebuild', 'gate', 'P1'], ['s2rebuild', 'check'], ['s2rebuild', 'run', '--from', 'O1'], ['s2rebuild', 'gate', 'O1']):
            self.assertTrue(any(words[:len(needed)] == needed for words in sequence), needed)
        env, run_id, outputs = {}, None, {}
        with mock.patch.object(rehearse, 'run', passed_rehearsal):
            for words in sequence:
                if words[0] == 'export':
                    self.assertEqual(words[1], 'S2REBUILD_RUN=RUN_ID')
                    self.assertIsNotNone(run_id, 'the run is exported after the command that starts it')
                    env['S2REBUILD_RUN'] = run_id
                    continue
                self.assertEqual(words[0], 's2rebuild')
                code, text = self.bare(h, *words[1:], **env)
                self.assertEqual(code, EXIT_GATE if words[1] == 'run' else EXIT_DONE, '%s: %s' % (' '.join(words), text))
                if words[1:3] == ['gate', 'P1']:
                    run_id = re.search(r'gate P1 recorded as PASS for run (\S+),', text).group(1)
                outputs[' '.join(words)] = text
        self.assertEqual(self.runs(h), [run_id], 'one run directory')
        start = outputs['s2rebuild run --from O1']
        self.assertIn('run, run %s (--run %s) under' % (run_id, run_id), start)
        self.assertIn('record with: s2rebuild gate O1 --pass --note TEXT --run %s [--evidence FILE]' % run_id, start)
        self.assertIn('rehearsal evidence: P5 passed in this run', start)
        self.assertIn('[O1] Start (python + decision; runner) database example_orders_db: already passed, skipped', outputs['s2rebuild run'])
        self.assertIn('Stopped at O2: a person must record the gate before the run continues; then s2rebuild run --run %s continues it'
                      % run_id, outputs['s2rebuild run'])
        with contextlib.closing(Journal(h.tmp / 'runs' / run_id / 'example_orders_db' / 'journal.sqlite')) as journal:
            self.assertEqual({s: journal.step_status(s) for s in ('P1', 'P2', 'P3', 'P4', 'P5', 'O1', 'O2')},
                             {'P1': 'passed', 'P2': 'passed', 'P3': 'passed', 'P4': 'passed', 'P5': 'passed', 'O1': 'passed', 'O2': 'pending'})
            self.assertEqual(journal.get_meta('run_id'), run_id)
            self.assertEqual(journal.get_meta('config_hash'), h.config.config_hash())

    def test_f6_run_resumes_the_most_recent_run_and_new_starts_another(self):
        """run without --run starts a run only when none exists and otherwise resumes the most recent one, however often it runs; --new
        starts another. A --run that names no run is refused unless --new, --new refuses an id that exists or a later --from, and a
        refusal creates nothing. The gate hint, the stop line, the log header and the O1 refusal name the run with its --run form. A
        selection without a statement of its own: the three failure points of the writes it resumes are
        test_f6_run_selection_fault_points_rerun_lands_in_the_same_run."""
        h = LiveRun(self)
        start = ('run', '--no-rehearsal', 'no rehearsal for this test')
        code, text = self.bare(h, *start)
        self.assertEqual(code, EXIT_REFUSED, 'with no run at all, run starts one; O1 refuses there without P2: %s' % text)
        (first,) = self.runs(h)
        self.assertIn('O1 refused: P2 is not passed in this run (not run); run s2rebuild check --run %s for this run first' % first, text)
        self.assertEqual(self.bare(h, 'gate', 'P1', '--pass', '--note', 'in place')[0], EXIT_DONE)
        self.assertEqual(self.bare(h, 'check')[0], EXIT_DONE)
        for attempt in (1, 2):
            code, text = self.bare(h, *start)
            self.assertEqual(code, EXIT_GATE, text)
            self.assertEqual(self.runs(h), [first], 'run %d resumed the run' % attempt)
        self.assertIn('run, run %s (--run %s) under' % (first, first), text)
        self.assertIn('record with: s2rebuild gate O1 --pass --note TEXT --run %s [--evidence FILE]' % first, text)
        self.assertIn('Stopped at O1: a person must record the gate before the run continues; then s2rebuild run --run %s continues it' % first, text)
        self.assertEqual(self.bare(h, 'gate', 'O1', '--pass', '--note', 'go')[0], EXIT_DONE)
        code, text = self.bare(h, 'run')
        self.assertEqual(code, EXIT_GATE, text)
        self.assertIn('[O1] Start (python + decision; runner) database example_orders_db: already passed, skipped', text)
        self.assertIn('Stopped at O2', text)
        for argv, env, refusal in ((('run', '--run', 'typo'), {}, 'Run typo does not exist under %s; existing runs: %s; s2rebuild run --new --run '
                                    'typo starts a run of that name' % (h.tmp / 'runs', first)),
                                   (('run', '--new', '--run', first), {}, 'Run %s exists already' % first),
                                   (('run', '--new'), {'S2REBUILD_RUN': first}, 'Run %s exists already under %s (named by S2REBUILD_RUN)'
                                    % (first, h.tmp / 'runs')),
                                   (('run', '--new', '--from', 'O4'), {}, '--new starts a run at O1; leave out --from O4')):
            code, text = self.bare(h, *argv, **env)
            self.assertEqual(code, EXIT_REFUSED, argv)
            self.assertIn(refusal, text, argv)
            self.assertEqual(self.runs(h), [first], 'the refusal of %s created nothing' % ' '.join(argv))
        code, text = self.bare(h, 'run', '--new', '--no-rehearsal', 'no rehearsal for this test')
        self.assertEqual(code, EXIT_REFUSED, 'the new run holds no P2: %s' % text)
        (second,) = [name for name in self.runs(h) if name != first]
        self.assertIn('run s2rebuild check --run %s for this run first' % second, text)
        code, text = self.bare(h, 'run', S2REBUILD_RUN=first)
        self.assertEqual(code, EXIT_GATE, text)
        self.assertIn('run, run %s (--run %s)' % (first, first), text)
        self.assertIn('Stopped at O2', text)
        code, text = self.bare(h, 'run', '--new', '--run', 'second-night', '--no-rehearsal', 'no rehearsal for this test')
        self.assertEqual(code, EXIT_REFUSED, text)
        self.assertEqual(self.runs(h), sorted([first, second, 'second-night']))
        with contextlib.closing(Journal(h.tmp / 'runs' / first / 'example_orders_db' / 'journal.sqlite')) as journal:
            self.assertEqual((journal.step_status('O1'), journal.step_status('O2')), ('passed', 'pending'))
            self.assertEqual(journal.events('step_start').count('O1'), 3, 'one O1 attempt per run command, all in the first run')

    def test_f6_run_selection_fault_points_rerun_lands_in_the_same_run(self):
        """The journal writes that start and resume the run, at the three points: before: a kill just before O1's first journal write
        (step_start); after: a kill just after O1 recorded the run id (set_meta run_id); journal: a kill in gate O1 before its journal
        update (record_gate). The operator reruns the same commands as the README writes them, without --run: every rerun lands in
        the run the P1 gate started, never a new one, the go is recorded once and the run continues at O2."""
        start = ('run', '--no-rehearsal', 'no rehearsal for this test')
        go = ('gate', 'O1', '--pass', '--note', 'go')
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = LiveRun(self)
                self.assertEqual(self.bare(h, 'gate', 'P1', '--pass', '--note', 'in place')[0], EXIT_DONE)
                self.assertEqual(self.bare(h, 'check')[0], EXIT_DONE)
                (run_id,) = self.runs(h)
                if point == 'journal':
                    self.assertEqual(self.bare(h, *start)[0], EXIT_GATE)
                    fired = crash_journals(self, 'record_gate', match=lambda step_id, *a, **k: step_id == 'O1')
                    with self.assertRaises(SimulatedCrash):
                        self.bare(h, *go)
                else:
                    method, match = {'before': ('step_start', lambda step_id, *a, **k: step_id == 'O1'),
                                     'after': ('set_meta', lambda key, value: key == 'run_id')}[point]
                    fired = crash_journals(self, method, match=match, after=point == 'after')
                    with self.assertRaises(SimulatedCrash):
                        self.bare(h, *start)
                self.assertEqual(len(fired), 1)
                journal_path = h.tmp / 'runs' / run_id / 'example_orders_db' / 'journal.sqlite'
                with contextlib.closing(Journal(journal_path)) as journal:
                    self.assertEqual(journal.step_status('O1'), {'before': None, 'after': 'failed', 'journal': 'pending'}[point])
                    self.assertEqual(journal.get_meta('run_id'), None if point == 'before' else run_id)
                if point != 'journal':
                    code, text = self.bare(h, *start)
                    self.assertEqual(code, EXIT_GATE, text)
                    self.assertIn('record with: s2rebuild gate O1 --pass --note TEXT --run %s' % run_id, text)
                code, text = self.bare(h, *go)
                self.assertEqual(code, EXIT_DONE, text)
                self.assertIn('gate O1 recorded as PASS for run %s' % run_id, text)
                code, text = self.bare(h, 'run')
                self.assertEqual(code, EXIT_GATE, text)
                self.assertIn('Stopped at O2', text)
                self.assertEqual(self.runs(h), [run_id], 'every rerun landed in the run the P1 gate started')
                with contextlib.closing(Journal(journal_path)) as journal:
                    self.assertEqual((journal.step_status('O1'), journal.step_status('O2')), ('passed', 'pending'))
                    self.assertEqual(journal.get_meta('run_id'), run_id)
                    self.assertEqual(len([e for e in journal.events('gate') if e.startswith('O1 pass')]), 1, 'the go is recorded once')


if __name__ == '__main__':
    unittest.main()
