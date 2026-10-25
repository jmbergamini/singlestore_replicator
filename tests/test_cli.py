"""s2rebuild tests test_cli: help lists every command, stubs refuse, dry-run walk, gate and status cycle.

Author: John Bergamini
"""
import os
import re
import tempfile
import time
import unittest
from pathlib import Path

from s2rebuild import cli
from s2rebuild.common import EXIT_DONE, EXIT_GATE, EXIT_REFUSED
from tests.helpers import FIXTURE, ROOT, run_cli, write_config

FIXTURE_INVENTORY = ROOT / 'tests' / 'fixtures' / 'inventory'
REFERENCE_JOURNAL = ROOT / 'tests' / 'fixtures' / 'reference-run' / 'copy-journal.sqlite'

STUB_ARGS = {'owner-sql': ['O10'], 'step': ['O6'], 'gate': ['O1', '--pass', '--note', 'x']}


class HelpTests(unittest.TestCase):
    def test_help_lists_every_command_of_section_8(self):
        code, text = run_cli(['--help'])
        self.assertEqual(code, 0)
        expected = ['check', 'account-sql', 'profile', 'plan', 'rehearse', 'run', 'step', 'gate', 'status', 'watch',
                    'quiet-proof', 'lockout', 'owner-sql', 'owner-notebook', 'mark-recopy', 'pause', 'resume', 'report',
                    'calibrate', 'bundle', 'docs-flags']
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
            code, text = run_cli(['step', 'P3'] + base)
            self.assertEqual(code, EXIT_DONE, 'P3 is already passed and skipped')
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
            self.assertEqual(code, EXIT_DONE, text)
            wb = load_workbook(str(Path(tmp) / 'customer' / 'again.xlsx'))
            summary = {str(r[0].value): r[1].value for r in wb['Summary'].iter_rows(min_col=1, max_col=2) if r[0].value}
            self.assertTrue(str(summary['Baseline']).startswith(str(baseline)), 'the written baseline is picked up by the next profile')
            code, text = run_cli(['calibrate', '--run', '20261101-0500'] + base)
            self.assertEqual(code, EXIT_REFUSED, 'the run journal has no chunks yet')
            self.assertIn('at least 4', text)


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
            code, text = run_cli(['run', '--run', '20261101-0500'] + base)
            self.assertEqual(code, EXIT_GATE)
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
            self.assertEqual(len(list((Path(tmp) / 'runs' / '20261101-0500' / 'logs').glob('gate-*.log'))), 0)
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
            self.assertEqual(code, EXIT_GATE)
            self.assertIn('[P1] Prerequisites (human + admin; owner)', text)
            code, text = run_cli(['step', 'P4'] + base)
            self.assertEqual(code, EXIT_REFUSED)
            self.assertIn('Cannot run P4 for database example_orders_db: earlier steps are not passed: P1, P2, P3', text)
            code, text = run_cli(['run', '--dry-run', '--to', 'O2'] + base)
            self.assertEqual(code, EXIT_DONE)
            self.assertIn('already passed, skipped', text, 'a dry run reads the live journal')

    def test_run_without_run_id_creates_a_new_run_and_env_run_is_honored(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp)
            code, text = run_cli(['run', '--config', str(path)])
            self.assertEqual(code, EXIT_GATE)
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
            self.assertEqual(code, EXIT_GATE, 'a second run from O1 starts a second run')
            runs_now = sorted(p.name for p in (Path(tmp) / 'runs').iterdir())
            self.assertEqual(len(runs_now), 2)
            self.assertTrue(any(name.count('-') == 2 for name in runs_now) or runs_now[0][:13] != runs_now[1][:13],
                            'the second id is unique even inside the same minute: %s' % runs_now)

    def test_most_recent_run_is_by_change_time_not_by_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp)
            base = ['--config', str(path)]
            code, text = run_cli(['run', '--run', 'zz-rehearsal'] + base)
            self.assertEqual(code, EXIT_GATE)
            old = time.time() - 3600
            for p in (Path(tmp) / 'runs' / 'zz-rehearsal').rglob('*'):
                os.utime(p, (old, old))
            os.utime(Path(tmp) / 'runs' / 'zz-rehearsal', (old, old))
            code, text = run_cli(['run', '--run', '20261101-0500'] + base)
            self.assertEqual(code, EXIT_GATE)
            code, text = run_cli(['status'] + base)
            self.assertEqual(code, EXIT_DONE)
            self.assertIn('run 20261101-0500', text)
            self.assertNotIn('zz-rehearsal', text)
            code, text = run_cli(['gate', 'O1', '--pass', '--note', 'go'] + base)
            self.assertIn('for run 20261101-0500', text)

    def test_refused_arguments_create_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp)
            base = ['--config', str(path)]
            for argv in (['run', '--from', 'O12'], ['run', '--from', 'O7', '--to', 'O6'], ['run', '--from', 'P1'],
                         ['step', 'Q1'], ['gate', 'O99', '--pass', '--note', 'x'], ['gate', 'O6', '--pass', '--note', 'x'],
                         ['gate', 'O1', '--pass', '--note', 'x'], ['run', '--from', 'O2'], ['step', 'O2', '--run', 'nope'],
                         ['gate', 'O2', '--pass', '--note', 'x', '--run', 'nope'], ['status']):
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
            self.assertEqual(code, EXIT_GATE, 'run from O1 may start an explicitly named run')
            self.assertTrue((Path(tmp) / 'runs' / 'named-by-hand').is_dir())


if __name__ == '__main__':
    unittest.main()
