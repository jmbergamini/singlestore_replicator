"""s2rebuild tests test_steps: registry order, --from refusal, gate recording, exit codes, dry-run walk.

Author: John Bergamini
"""
import datetime as dt
import io
import shutil
import tempfile
import unittest
from pathlib import Path

from s2rebuild import steps
from s2rebuild.common import EXIT_DONE, EXIT_GATE, EXIT_REFUSED, KitError
from s2rebuild.connect import Log
from s2rebuild.journal import MEMORY, Journal
from tests.helpers import config_from, write_config

EXPECTED_ORDER = ['P1', 'P2', 'P3', 'P4', 'P5', 'O1', 'O2', 'O3', 'O4', 'O5', 'O6', 'O7', 'O8', 'O9', 'O10', 'O11',
                  'C1', 'C2', 'C3', 'C4']


class RegistryTests(unittest.TestCase):
    def test_order_ids_and_metadata(self):
        self.assertEqual(steps.ORDER, EXPECTED_ORDER)
        self.assertEqual(steps.OUTAGE, EXPECTED_ORDER[5:16])
        self.assertEqual(steps.ADMIN_STEPS, ['P1', 'P5', 'O2', 'O3', 'O9', 'O10', 'O11', 'C3', 'C4'])
        self.assertEqual(steps.OWNER_SQL_STEPS, ['O2', 'O3', 'O9', 'O10', 'O11', 'C3', 'C4'])
        for step_id, step in steps.REGISTRY.items():
            self.assertEqual(step.id, step_id)
            self.assertTrue(step.gate and step.title and step.kinds and step.identities, step_id)
            self.assertEqual(step.phase, step_id[0])
        self.assertEqual(steps.REGISTRY['O1'].kinds, ('python', 'decision'))
        self.assertEqual(steps.REGISTRY['O11'].identities, ('owner', 'customer'))
        self.assertTrue(steps.REGISTRY['O3'].needs_person)
        self.assertFalse(steps.REGISTRY['O6'].needs_person)
        self.assertEqual(steps.REGISTRY['O6'].kind_label, 'python')
        self.assertEqual(steps.REGISTRY['O11'].identity_label, 'owner + customer')
        for step_id in ('P2', 'C2', 'P3', 'P5', 'O2', 'O3', 'O4', 'O5', 'O6', 'O7', 'O8', 'O9', 'O10', 'O11', 'C1', 'C3', 'C4'):
            self.assertIn(step_id, steps.HANDLERS)
        for admin_step in steps.ADMIN_STEPS:
            if admin_step != 'P1':
                self.assertIsNot(steps.HANDLERS[admin_step], steps.admin_pending, '%s has its own handler' % admin_step)

    def test_preconditions_chain(self):
        self.assertEqual(steps.all_preconditions(steps.REGISTRY['O1']), [])
        self.assertEqual(steps.all_preconditions(steps.REGISTRY['O4']), ['O1', 'O2', 'O3'])
        self.assertEqual(steps.all_preconditions(steps.REGISTRY['O11']), steps.OUTAGE[:-1])
        self.assertEqual(steps.all_preconditions(steps.REGISTRY['C2']), steps.OUTAGE + ['C1'])
        self.assertEqual(steps.all_preconditions(steps.REGISTRY['P3']), ['P1', 'P2'])

    def test_get_step_and_outage_range(self):
        self.assertEqual(steps.get_step('o6').id, 'O6')
        with self.assertRaisesRegex(KitError, 'Unknown step'):
            steps.get_step('O12')
        self.assertEqual(steps.outage_range(), steps.OUTAGE)
        self.assertEqual(steps.outage_range('O6', 'O7'), ['O6', 'O7'])
        with self.assertRaisesRegex(KitError, 'after'):
            steps.outage_range('O7', 'O6')
        with self.assertRaisesRegex(KitError, 'outage steps'):
            steps.outage_range('P1', 'O3')

    def test_plan_order_two_databases(self):
        cfg = config_from(lambda d: d['databases'].append({'source': 'second_db', 'layout': {'partitions': 8, 'sub_partitions': 64}}))
        a, b = cfg.databases
        plan = steps.plan_order([a, b], steps.OUTAGE)
        labels = ['%s:%s' % (db.source[:1], s) for db, s in plan]
        self.assertEqual(labels[:2], ['e:O1', 's:O1'])
        self.assertEqual(labels[2:10], ['e:O%d' % i for i in range(2, 10)])
        self.assertEqual(labels[10:18], ['s:O%d' % i for i in range(2, 10)])
        self.assertEqual(labels[18:], ['e:O10', 'e:O11', 's:O10', 's:O11'])
        self.assertEqual(len(steps.plan_order([a], ['O6', 'O7'])), 2)


class Harness:
    def __init__(self, test, mutate=None, on_disk=False):
        self.tmp = Path(tempfile.mkdtemp())
        test.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        test.addCleanup(self.close)
        self.path, self.config = write_config(self.tmp, mutate)
        self.databases = list(self.config.databases)
        self.out = io.StringIO()
        self.log = Log(stream=self.out)
        self.on_disk = on_disk
        self.journals = {}
        for db in self.databases:
            path = self.config.runs_dir() / '20261101-0500' / db.source / 'journal.sqlite'
            self.journals[db.source] = Journal(path) if on_disk else Journal(MEMORY)

    def run(self, **kw):
        kw.setdefault('run_id', '20261101-0500')
        return steps.run_steps(self.config, self.databases, self.journals, self.log, **kw)

    def text(self):
        return self.out.getvalue()

    def close(self):
        for journal in self.journals.values():
            try:
                journal.close()
            except Exception:  # noqa: BLE001 - already closed by the test
                pass

    @property
    def journal(self):
        return self.journals[self.databases[0].source]


class DryRunTests(unittest.TestCase):
    def test_dry_run_walks_o1_to_o11_and_prints_pending_gates(self):
        h = Harness(self)
        code = h.run(dry_run=True)
        self.assertEqual(code, EXIT_DONE)
        text = h.text()
        self.assertIn('names: source example_orders_db, replacement example_orders_db_rebuild_20261101, retained example_orders_db_retained_20261101', text)
        for step_id in steps.OUTAGE:
            self.assertIn('[%s]' % step_id, text)
        self.assertEqual(text.count('result: pending: gate pending:'), 6, 'six gates need a person')
        for gate_step in ('O1', 'O2', 'O3', 'O9', 'O10', 'O11'):
            self.assertIn('record with: s2rebuild gate %s --pass' % gate_step, text)
        self.assertIn('gates pending: O1 example_orders_db, O2 example_orders_db, O3 example_orders_db, O9 example_orders_db, '
                      'O10 example_orders_db, O11 example_orders_db', text)
        self.assertIn('python steps not run (no implementation or no input yet): O4 example_orders_db, O5 example_orders_db, O6 example_orders_db, '
                      'O7 example_orders_db, O8 example_orders_db', text)
        self.assertIn('No connection was opened and nothing was written.', text)
        self.assertIn('abort deadline: 2026-11-01T11:30:00Z', text)
        self.assertIn('freeze path: A read-only attachment', text)
        self.assertIn('-- O3: Freeze: read-only attachment (Path A)', text, 'admin steps print their block in a dry run')
        self.assertIn('DETACH DATABASE `example_orders_db`;', text)
        self.assertEqual(h.journal.steps_board(), {}, 'a dry run journals nothing')

    def test_dry_run_from_later_step_only_warns_about_preconditions(self):
        h = Harness(self)
        self.assertEqual(h.run(dry_run=True, from_id='O6', to_id='O7'), EXIT_DONE)
        self.assertIn('earlier steps without a passed gate: O1, O2, O3, O4, O5', h.text())
        self.assertIn('gates pending: none', h.text())

    def test_dry_run_reports_a_past_deadline_without_stopping(self):
        h = Harness(self, lambda d: d['window'].update(start='2000-01-01T00:00:00Z', end='2000-01-01T08:00:00Z'))
        self.assertEqual(h.run(dry_run=True, to_id='O2'), EXIT_DONE)
        self.assertIn('abort deadline is in the past', h.text())
        self.assertIn('[O2]', h.text())

    def test_dry_run_with_lockout_mode(self):
        h = Harness(self, lambda d: d.update(freeze={'mode': 'lockout'}))
        self.assertEqual(h.run(dry_run=True, to_id='O3'), EXIT_DONE)
        self.assertIn('freeze path: B lockout', h.text())
        self.assertIn('Path B lockout needs the owner profile', h.text())


class RealRunTests(unittest.TestCase):
    def test_run_stops_at_o1_gate_with_exit_4_and_records_window(self):
        h = Harness(self)
        self.assertEqual(h.run(), EXIT_GATE)
        self.assertEqual(h.journal.step_status('O1'), 'pending')
        meta = h.journal.meta()
        self.assertEqual(meta['window_start'], '2026-11-01T05:00:00Z')
        self.assertEqual(meta['abort_deadline'], '2026-11-01T11:30:00Z')
        self.assertEqual(meta['run_id'], '20261101-0500')
        self.assertEqual(meta['replacement_db'], 'example_orders_db_rebuild_20261101')
        self.assertEqual(meta['config_hash'], h.config.config_hash())
        self.assertEqual(meta['freeze_mode'], 'readonly')
        self.assertIn('Stopped at O1: a person must record the gate', h.text())
        self.assertIsNone(h.journal.step_status('O2'), 'nothing after the pending gate runs')

    def test_from_refused_when_earlier_steps_are_not_passed(self):
        h = Harness(self)
        with self.assertRaisesRegex(KitError, 'Cannot run O4 for database example_orders_db: earlier steps are not passed: O1, O2, O3'):
            h.run(from_id='O4')
        self.assertEqual(h.journal.steps_board(), {})
        steps.record_gate(h.journals, 'O1', True, 'go')
        with self.assertRaisesRegex(KitError, 'not passed: O2, O3'):
            h.run(from_id='O4')

    def test_gate_recording_lets_the_run_continue(self):
        h = Harness(self)
        self.assertEqual(h.run(), EXIT_GATE)
        step = steps.record_gate(h.journals, 'o1', True, 'go recorded', '/evidence/o1.txt')
        self.assertEqual(step.id, 'O1')
        row = h.journal.step('O1')
        self.assertEqual((row['status'], row['note'], row['evidence_path']), ('passed', 'go recorded', '/evidence/o1.txt'))
        self.assertEqual(h.run(from_id='O2'), EXIT_GATE)
        self.assertEqual(h.journal.step_status('O2'), 'pending')
        steps.record_gate(h.journals, 'O2', True, 'pipelines stopped')
        steps.record_gate(h.journals, 'O3', True, 'read only verified')
        code = h.run(from_id='O4')
        self.assertEqual(code, EXIT_REFUSED, 'a python step without its input refuses in a real run')
        self.assertIn('profile --step O4', h.text())
        self.assertEqual(h.journal.step_status('O4'), 'pending')
        with self.assertRaisesRegex(KitError, 'needs --note'):
            steps.record_gate(h.journals, 'O3', True, '  ')
        with self.assertRaisesRegex(KitError, 'Unknown step'):
            steps.record_gate(h.journals, 'O99', True, 'x')

    def test_failed_check_exits_4_and_stops(self):
        h = Harness(self, lambda d: d.pop('window'))
        self.assertEqual(h.run(), EXIT_GATE)
        self.assertEqual(h.journal.step_status('O1'), 'failed')
        self.assertIn('Stopped at O1: gate failed', h.text())
        self.assertIsNone(h.journal.step_status('O2'))

    def test_failed_gate_blocks_the_next_step(self):
        h = Harness(self)
        steps.record_gate(h.journals, 'O1', False, 'no go')
        self.assertEqual(h.journal.step_status('O1'), 'failed')
        with self.assertRaisesRegex(KitError, 'not passed: O1'):
            h.run(from_id='O2')

    def test_gate_refuses_python_steps(self):
        h = Harness(self)
        for step_id in ('P2', 'P3', 'O4', 'O5', 'O6', 'O7', 'O8', 'C1', 'C2'):
            with self.assertRaisesRegex(KitError, 'python step the kit runs', msg=step_id):
                steps.record_gate(h.journals, step_id, True, 'by hand')
        self.assertEqual(steps.gate_step('o11').id, 'O11')
        self.assertEqual(h.journal.steps_board(), {})

    def test_run_names_must_match_the_journal(self):
        h = Harness(self)
        self.assertEqual(h.run(), EXIT_GATE)
        self.assertEqual(h.journal.get_meta('replacement_db'), 'example_orders_db_rebuild_20261101')
        steps.check_run_names(h.journal, h.databases[0])
        other = config_from(lambda d: d['databases'][0].update(replacement='example_orders_db_rebuild_20261102')).databases[0]
        with self.assertRaisesRegex(KitError, 'records replacement database example_orders_db_rebuild_20261101 but the configuration says'):
            steps.check_run_names(h.journal, other)
        with self.assertRaisesRegex(KitError, 'records replacement database'):
            steps.run_steps(h.config, [other], {other.source: h.journal}, h.log, run_id='20261101-0500', from_id='O2')
        with self.assertRaisesRegex(KitError, 'records replacement database'):
            steps.run_one(h.config, [other], {other.source: h.journal}, h.log, 'P1')
        fresh = Journal(MEMORY)
        steps.check_run_names(fresh, other)
        fresh.close()

    def test_passed_step_is_skipped_unless_redo(self):
        h = Harness(self)
        steps.record_gate(h.journals, 'O1', True, 'go')
        self.assertEqual(h.run(), EXIT_GATE)
        self.assertIn('[O1] Start (python + decision; runner) database example_orders_db: already passed, skipped', h.text())
        self.assertEqual(h.journal.step_status('O1'), 'passed')
        self.assertEqual(h.journal.step_status('O2'), 'pending')
        h.out.truncate(0)
        h.out.seek(0)
        self.assertEqual(h.run(redo=True), EXIT_GATE)
        self.assertEqual(h.journal.step_status('O1'), 'pending', 'redo runs O1 again and waits for a new go')

    def test_o1_fails_without_a_window_or_with_a_past_deadline(self):
        h = Harness(self, lambda d: d.pop('window'))
        self.assertEqual(h.run(), EXIT_GATE)
        self.assertEqual(h.journal.step_status('O1'), 'failed')
        self.assertIn('window.start and window.end are not configured', h.text())
        h = Harness(self, lambda d: d['window'].update(start='2000-01-01T00:00:00Z', end='2000-01-01T08:00:00Z'))
        self.assertEqual(h.run(), EXIT_GATE)
        self.assertEqual(h.journal.step_status('O1'), 'failed')
        self.assertIn('already past', h.text())

    def test_two_databases_run_database_major(self):
        h = Harness(self, lambda d: d['databases'].append({'source': 'second_db', 'layout': {'partitions': 8, 'sub_partitions': 64}}))
        for journals in (h.journals,):
            steps.record_gate(journals, 'O1', True, 'go')
        self.assertEqual(h.run(from_id='O2'), EXIT_GATE)
        self.assertEqual(h.journals['example_orders_db'].step_status('O2'), 'pending')
        self.assertIsNone(h.journals['second_db'].step_status('O2'), 'the second database waits for the first')

    def test_run_one_any_phase(self):
        h = Harness(self)
        self.assertEqual(steps.run_one(h.config, h.databases, h.journals, h.log, 'P1'), EXIT_GATE)
        self.assertEqual(h.journal.step_status('P1'), 'pending')
        self.assertIn('done by: owner', h.text())
        with self.assertRaisesRegex(KitError, 'Cannot run P4 for database example_orders_db: earlier steps are not passed: P1, P2, P3'):
            steps.run_one(h.config, h.databases, h.journals, h.log, 'P4')
        steps.record_gate(h.journals, 'P1', True, 'done')
        for step_id in ('P2', 'P3'):
            h.journal.step_end(step_id, 'passed', 'as the kit records it after running the step')
        self.assertEqual(steps.run_one(h.config, h.databases, h.journals, h.log, 'P4'), EXIT_GATE)
        self.assertEqual(h.journal.step_status('P4'), 'pending')
        self.assertIn('decision recorded by: people', h.text())
        with self.assertRaisesRegex(KitError, 'Cannot run P5 for database example_orders_db: earlier steps are not passed: P4'):
            steps.run_one(h.config, h.databases, h.journals, h.log, 'P5')
        self.assertEqual(steps.run_one(h.config, h.databases, h.journals, h.log, 'C4', dry_run=True), EXIT_DONE)
        self.assertIsNone(h.journal.step_status('C4'))

    def test_board_rows(self):
        h = Harness(self)
        steps.record_gate(h.journals, 'O1', True, 'go', '/e.txt')
        rows = steps.board(h.journals)
        self.assertEqual(len(rows), len(steps.ORDER))
        by_step = {r['step']: r for r in rows}
        self.assertEqual(by_step['O1']['status'], 'passed')
        self.assertEqual(by_step['O1']['evidence_path'], '/e.txt')
        self.assertEqual(by_step['O2']['status'], 'not started')
        self.assertEqual(by_step['O10']['title'], 'Switch')

    def test_on_disk_journal_round_trip(self):
        h = Harness(self, on_disk=True)
        self.assertEqual(h.run(), EXIT_GATE)
        path = h.config.runs_dir() / '20261101-0500' / 'example_orders_db' / 'journal.sqlite'
        self.assertTrue(path.exists())
        for j in h.journals.values():
            j.close()
        again = Journal(path)
        self.assertEqual(again.step_status('O1'), 'pending')
        again.close()


if __name__ == '__main__':
    unittest.main()
