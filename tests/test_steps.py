"""s2rebuild tests test_steps: registry order, --from refusal, gate recording, exit codes, dry-run walk.

Author: John Bergamini
"""
import datetime as dt
import io
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

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
        self.options = None
        self.journals = {}
        for db in self.databases:
            path = self.config.runs_dir() / '20261101-0500' / db.source / 'journal.sqlite'
            self.journals[db.source] = Journal(path) if on_disk else Journal(MEMORY)

    def ready_for_o1(self):
        """Record P1 and P2 as the P1 gate and s2rebuild check do, and waive the rehearsal for run(), so O1 reaches its go gate."""
        for journal in self.journals.values():
            journal.record_gate('P1', True, 'account, Keychain items and config in place')
            journal.step_end('P2', 'passed', 'preflight passed')
        self.options = {'no_rehearsal': 'test run without a rehearsal'}
        return self

    def run(self, **kw):
        kw.setdefault('run_id', '20261101-0500')
        kw.setdefault('options', self.options)
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
        h = Harness(self).ready_for_o1()
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

    def test_f7_run_stopped_at_a_pending_python_step_gives_the_step_instruction(self):
        """run told the operator to record a gate when a python step (O7, O8) ended pending, but gate refuses python steps. Now it
        stops with the step's own instruction; an admin step that ends pending keeps the gate line. Issues nothing (no statement)."""
        h = Harness(self)
        for step_id in steps.OUTAGE[:7]:
            h.journal.step_end(step_id, 'passed', 'test')
        note = 'tier 1 and 2 pass; tier 3 was sampled beyond the budget on t_big; rerun step O8 --yes --run 20261101-0500 to accept'
        with mock.patch.dict(steps.HANDLERS, {'O8': lambda step, ctx: steps.Outcome(steps.PENDING, note, EXIT_GATE)}):
            self.assertEqual(h.run(from_id='O8', to_id='O8'), EXIT_GATE)
        self.assertIn('Stopped at O8: ' + note, h.text())
        self.assertNotIn('a person must record the gate', h.text())
        with self.assertRaisesRegex(KitError, 'O8 Verify is a python step the kit runs and records itself'):
            steps.gate_step('O8')
        h.journal.step_end('O8', 'passed', 'test')
        with mock.patch.dict(steps.HANDLERS, {'O9': lambda step, ctx: steps.Outcome(steps.PENDING, 'run the O9 block', EXIT_GATE)}):
            self.assertEqual(h.run(from_id='O9', to_id='O9'), EXIT_GATE)
        self.assertIn('Stopped at O9: a person must record the gate before the run continues; then s2rebuild run --run 20261101-0500 '
                      'continues it', h.text())

    def test_from_refused_when_earlier_steps_are_not_passed(self):
        h = Harness(self)
        with self.assertRaisesRegex(KitError, 'Cannot run O4 for database example_orders_db: earlier steps are not passed: O1, O2, O3'):
            h.run(from_id='O4')
        self.assertEqual(h.journal.steps_board(), {})
        steps.record_gate(h.journals, 'O1', True, 'go')
        with self.assertRaisesRegex(KitError, 'not passed: O2, O3'):
            h.run(from_id='O4')

    def test_gate_recording_lets_the_run_continue(self):
        h = Harness(self).ready_for_o1()
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
        # W9: run performs the O4 capture itself; this harness has no session factory, so the live capture refuses and O4 stays pending
        self.assertEqual(code, EXIT_REFUSED, 'a python step without its input refuses in a real run')
        self.assertIn('No connection factory is available for a live step', h.text())
        self.assertEqual(h.journal.step_status('O4'), 'pending')
        with self.assertRaisesRegex(KitError, 'needs --note'):
            steps.record_gate(h.journals, 'O3', True, '  ')
        with self.assertRaisesRegex(KitError, 'Unknown step'):
            steps.record_gate(h.journals, 'O99', True, 'x')

    def test_failed_check_exits_4_and_stops(self):
        h = Harness(self, lambda d: d['window'].update(start='2000-01-01T00:00:00Z', end='2000-01-01T08:00:00Z'))
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
        h = Harness(self).ready_for_o1()
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
        h = Harness(self).ready_for_o1()
        steps.record_gate(h.journals, 'O1', True, 'go')
        self.assertEqual(h.run(), EXIT_GATE)
        self.assertIn('[O1] Start (python + decision; runner) database example_orders_db: already passed, skipped', h.text())
        self.assertEqual(h.journal.step_status('O1'), 'passed')
        self.assertEqual(h.journal.step_status('O2'), 'pending')
        h.out.truncate(0)
        h.out.seek(0)
        self.assertEqual(h.run(redo=True), EXIT_GATE)
        self.assertEqual(h.journal.step_status('O1'), 'pending', 'redo runs O1 again and waits for a new go')

    def test_f4_a_passed_step_behind_a_reopened_step_is_refused_not_skipped(self):
        """run_step checks the preconditions before the already-passed skip; a dry run only reports them.

        A refusal without a statement of its own: it guards the step_start journal write (a refused step is not recorded). The
        three failure points of the re-copy that reopens O6 to O8 are in test_verify and test_switch (test_f4_*fault_points*).
        """
        h = Harness(self)
        for step_id in ('O1', 'O2', 'O3'):
            steps.record_gate(h.journals, step_id, True, 'recorded')
        h.journal.step_end('O2', 'pending', 'reopened for the test')
        before = h.journal.step('O3')
        with self.assertRaisesRegex(KitError, r'Cannot run O3 for database example_orders_db: earlier steps are not passed: O2 '
                                              r'\(O3 passed earlier, but a result counts only while the steps before it are passed\)'):
            h.run(from_id='O3', to_id='O3')
        self.assertEqual(h.journal.step('O3'), before, 'the refusal journals nothing')
        self.assertEqual(h.run(from_id='O3', to_id='O3', dry_run=True), EXIT_DONE)
        self.assertIn('[O3] Freeze (admin; owner) database example_orders_db: dry run; earlier steps without a passed gate: O2', h.text())
        self.assertIn('[O3] Freeze (admin; owner) database example_orders_db: already passed, skipped', h.text())
        self.assertEqual(h.journal.step('O3'), before)
        steps.record_gate(h.journals, 'O2', True, 'stopped again')
        self.assertEqual(h.run(from_id='O3', to_id='O3'), EXIT_DONE)
        self.assertEqual(h.text().count('[O3] Freeze (admin; owner) database example_orders_db: already passed, skipped'), 2,
                         'with its preconditions passed again, the passed step is skipped as before')

    def test_f4_refusal_hint_runs_python_steps_and_gates_the_steps_of_a_person(self):
        """The precondition refusal names how each missing step passes, earliest first: gate for a step a person records, then run
        --from for the reopened python steps (gate refuses those), since run --from stops at a gate a person has not recorded; step
        for each P or C python step. A refusal without a statement of its own: it guards the step_start journal write; the failure
        points of the re-copy that reopens O6 to O8 are in test_verify and test_switch (test_f4_*fault_points*)."""
        h = Harness(self)
        for step_id in ('O1', 'O2', 'O3'):
            steps.record_gate(h.journals, step_id, True, 'recorded')
        for step_id in ('O4', 'O5', 'O6', 'O7', 'O8'):
            h.journal.step_end(step_id, 'passed', 'recorded for the test')
        for step_id in ('O6', 'O7', 'O8'):
            h.journal.step_end(step_id, 'pending', 'reopened by a re-copy')
        with self.assertRaises(KitError) as caught:
            h.run(from_id='O9', to_id='O9')
        self.assertIn('earlier steps are not passed: O6, O7, O8. Run O6, O7, O8 with s2rebuild run --from O6 --db example_orders_db '
                      '--run 20261101-0500, or start from O1.', str(caught.exception))
        self.assertNotIn('gate ID', str(caught.exception), 'gate refuses python steps, so it is not offered for them')
        h.journal.step_end('O3', 'pending', 'reopened for the test')
        with self.assertRaises(KitError) as caught:
            h.run(from_id='O9', to_id='O9')
        self.assertIn('not passed: O3, O6, O7, O8. Record O3 with s2rebuild gate O3 --pass --note TEXT --db example_orders_db --run '
                      '20261101-0500; then run O6, O7, O8 with s2rebuild run --from O6 --db example_orders_db --run 20261101-0500, or start '
                      'from O1.', str(caught.exception))
        with self.assertRaises(KitError) as caught:
            steps.run_one(h.config, h.databases, h.journals, h.log, 'P4')
        self.assertIn('not passed: P1, P2, P3. Record P1 with s2rebuild gate P1 --pass --note TEXT --db example_orders_db; '
                      'then run P2 with s2rebuild step P2 --db example_orders_db; then run P3 with s2rebuild step P3 --db example_orders_db.',
                      str(caught.exception))
        self.assertNotIn('start from O1', str(caught.exception), 'no outage step is missing')
        self.assertEqual(steps.missing_hint(['O2', 'O4', 'O6'], restart=False),
                         'Record O2 with s2rebuild gate O2 --pass --note TEXT; then run O4, O6 with s2rebuild run --from O4.',
                         'python outage steps with only passed steps between them share one run --from')

    def test_o1_without_a_window_reaches_the_go_with_no_deadline(self):
        """The window is optional: O1 records no deadline, waits for the go, and the copy deadline resolves to None."""
        from s2rebuild import copy as copy_module
        h = Harness(self, lambda d: d.pop('window')).ready_for_o1()
        self.assertEqual(h.run(), EXIT_GATE)
        self.assertEqual(h.journal.step_status('O1'), 'pending')
        self.assertIn('window: none configured, so no abort deadline', h.text())
        self.assertIn('Stopped at O1', h.text())
        self.assertEqual(h.journal.get_meta('abort_deadline'), '')
        self.assertEqual(h.journal.get_meta('window_start'), '')
        self.assertIsNotNone(h.journal.get_meta('config_hash'), 'O1 ran, so the gate warning stays quiet')
        self.assertEqual(h.journal.events('window'), ['none configured; no abort deadline'])
        self.assertIsNone(copy_module.resolve_abort_at(h.journal, h.config, 600))

    def test_o1_without_a_window_clears_a_deadline_an_earlier_o1_recorded(self):
        from s2rebuild import copy as copy_module
        h = Harness(self, lambda d: d.pop('window')).ready_for_o1()
        h.journal.set_meta('abort_deadline', '2000-01-01T00:00:00Z')
        self.assertEqual(h.run(), EXIT_GATE)
        self.assertEqual(h.journal.get_meta('abort_deadline'), '')
        self.assertIsNone(copy_module.resolve_abort_at(h.journal, h.config))

    def test_o1_fails_with_a_past_deadline(self):
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

    def test_f1_a_non_kit_error_in_a_handler_leaves_the_step_failed_not_running(self):
        """A driver error or an interrupt escapes the handler: the step ends failed with the type and number only, and the exception propagates."""
        h = Harness(self)
        hidden = 'not-for-the-journal'

        def driver_error(step, ctx):
            raise RuntimeError(2013, "Lost connection near CREDENTIALS '{\"sasl.password\": \"%s\"}'" % hidden)

        def interrupted(step, ctx):
            raise KeyboardInterrupt()

        def refused(step, ctx):
            raise KitError('refused for the test')
        with mock.patch.dict(steps.HANDLERS, {'P1': driver_error}):
            with self.assertRaises(RuntimeError):
                steps.run_one(h.config, h.databases, h.journals, h.log, 'P1')
        row = h.journal.step('P1')
        self.assertEqual(row['status'], 'failed')
        self.assertIn('RuntimeError', row['note'])
        self.assertIn('2013', row['note'])
        stored = row['note'] + ' '.join(e['detail'] for e in h.journal.events()) + h.text()
        self.assertNotIn('not-for-the-journal', stored, 'the exception text never reaches the journal or the log')
        with mock.patch.dict(steps.HANDLERS, {'P1': interrupted}):
            with self.assertRaises(KeyboardInterrupt):
                steps.run_one(h.config, h.databases, h.journals, h.log, 'P1')
        row = h.journal.step('P1')
        self.assertEqual(row['status'], 'failed')
        self.assertIn('KeyboardInterrupt', row['note'])
        with mock.patch.dict(steps.HANDLERS, {'P1': refused}):
            self.assertEqual(steps.run_one(h.config, h.databases, h.journals, h.log, 'P1'), EXIT_REFUSED)
        self.assertEqual(h.journal.step_status('P1'), 'pending', 'a KitError keeps its handling')
        with mock.patch.dict(steps.HANDLERS, {'C4': driver_error}):
            with self.assertRaises(RuntimeError):
                steps.run_one(h.config, h.databases, h.journals, h.log, 'C4', dry_run=True)
        self.assertIsNone(h.journal.step_status('C4'), 'a dry run records nothing')

    def test_f5_o1_refused_without_p2_in_the_journal(self):
        """O1 refuses before any journal write when P2 is not passed in this run; a dry run reports it and walks on."""
        h = Harness(self)
        h.options = {'no_rehearsal': 'not the point of this test'}
        self.assertEqual(h.run(dry_run=True, to_id='O2'), EXIT_DONE)
        self.assertIn('P2 preflight: not passed in this run (not run)', h.text())
        self.assertIn('a real run refuses here: P2 is not passed in this run (not run); run s2rebuild check --run 20261101-0500 for this run first', h.text())
        self.assertIn('[O2]', h.text(), 'the dry run walks on')
        self.assertEqual(h.journal.steps_board(), {})
        self.assertEqual(h.run(), EXIT_REFUSED)
        row = h.journal.step('O1')
        self.assertEqual(row['status'], 'pending')
        self.assertEqual(row['note'], 'O1 refused: P2 is not passed in this run (not run); run s2rebuild check --run 20261101-0500 for this run first')
        meta = h.journal.meta()
        for key in ('config_hash', 'source_db', 'replacement_db', 'window_start', 'abort_deadline'):
            self.assertNotIn(key, meta, 'the refusal records nothing of O1: %s' % key)
        self.assertIsNone(h.journal.step_status('O2'))
        h.journal.step_end('P2', 'failed', 'preflight failed')
        self.assertEqual(h.run(), EXIT_REFUSED)
        self.assertIn('O1 refused: P2 is not passed in this run (failed)', h.journal.step('O1')['note'])
        h.journal.step_end('P2', 'passed', 'preflight passed')
        self.assertEqual(h.run(), EXIT_GATE)
        self.assertIn('P2 preflight: passed in this run at', h.text())
        self.assertEqual(h.journal.get_meta('config_hash'), h.config.config_hash())

    def test_f5_o1_refused_without_rehearsal_evidence_unless_no_rehearsal(self):
        """Rehearsal evidence is P5 passed in this run or a passed rehearsal of any run; without it only --no-rehearsal NOTE lets O1
        go on, recorded as an event and in the O1 note."""
        h = Harness(self, on_disk=True)
        for journal in h.journals.values():
            journal.record_gate('P1', True, 'in place')
            journal.step_end('P2', 'passed', 'preflight passed')
        self.assertEqual(h.run(), EXIT_REFUSED)
        self.assertIn('O1 refused: no rehearsal evidence (P5 passed in this run, or a passed rehearsal under', h.journal.step('O1')['note'])
        self.assertIn('run s2rebuild rehearse, or waive it with --no-rehearsal NOTE', h.journal.step('O1')['note'])
        self.assertIsNone(h.journal.get_meta('config_hash'))
        self.assertEqual(h.run(options={'no_rehearsal': '   '}), EXIT_REFUSED, 'an empty note waives nothing')
        self.assertEqual(h.run(options={'no_rehearsal': 'owner accepted the risk'}), EXIT_GATE)
        self.assertEqual(h.journal.events('no_rehearsal'), ['O1 without rehearsal evidence: owner accepted the risk'])
        self.assertIn('rehearsal waived with --no-rehearsal: owner accepted the risk', h.journal.step('O1')['note'])
        self.assertIn('rehearsal evidence: none found; waived with --no-rehearsal: owner accepted the risk', h.text())
        h.journal.step_end('P5', 'passed', 'rehearsal passed')
        self.assertEqual(h.run(redo=True, to_id='O1'), EXIT_GATE)
        self.assertIn('rehearsal evidence: P5 passed in this run', h.text())
        self.assertEqual(len(h.journal.events('no_rehearsal')), 1, 'no waiver is recorded while evidence exists')
        h.journal.step_end('P5', 'pending', 'reopened for the test')
        timings = h.config.runs_dir() / 'earlier-rehearsal' / 'rehearsal' / 'timings.json'
        timings.parent.mkdir(parents=True)
        timings.write_text('{"p5_status": "pending"}', encoding='utf8')
        self.assertEqual(h.run(redo=True, to_id='O1'), EXIT_REFUSED, 'a rehearsal that did not pass is no evidence')
        timings.write_text('{"p5_status": "passed"}', encoding='utf8')
        self.assertEqual(h.run(redo=True, to_id='O1'), EXIT_GATE)
        self.assertIn('rehearsal evidence: a passed rehearsal under %s' % h.config.runs_dir(), h.text())

    def test_w8_o1_refuses_while_autoscale_is_on_unless_accepted_and_records_the_workspace_answer(self):
        """O1 reads the workspace facts P2 recorded: autoscale on refuses before any journal write unless --accept-autoscale NOTE,
        which is recorded as an event and in the O1 note; without an API read O1 prints the three questions and records the
        operator's --workspace NOTE as evidence of the go."""
        import json
        h = Harness(self).ready_for_o1()
        api = {'source': 'api', 'workspace_id': 'ws-123', 'state': 'ACTIVE', 'autoscale': True, 'auto_suspend': False,
               'fields': {'state': 'ACTIVE', 'autoScale': {'enabled': True}, 'autoSuspend': {'suspendType': 'DISABLED'}}}
        h.journal.set_meta('preflight', json.dumps({'workspace': api}))
        self.assertEqual(h.run(), EXIT_REFUSED)
        note = h.journal.step('O1')['note']
        self.assertIn('autoscale is on (Management API at P2); every chunk in flight fails with error 1997', note)
        self.assertIn('override with --accept-autoscale NOTE', note)
        self.assertIsNone(h.journal.get_meta('config_hash'), 'the refusal records nothing of O1')
        self.assertIn('workspace: state ACTIVE, autoscale on (enabled=True), auto-suspend off (suspendType=DISABLED)', h.text())
        self.assertEqual(h.run(options=dict(h.options, accept_autoscale='owner turned it off by hand at 04:50')), EXIT_GATE)
        self.assertEqual(h.journal.events('accept_autoscale'), ['O1 with autoscale on: owner turned it off by hand at 04:50'])
        self.assertIn('O1 with autoscale on: owner turned it off by hand at 04:50', h.journal.step('O1')['note'])
        self.assertEqual(h.journal.get_meta('config_hash'), h.config.config_hash())
        api.update(autoscale=False, fields=dict(api['fields'], autoScale={'enabled': False}))
        h.journal.set_meta('preflight', json.dumps({'workspace': api}))
        self.assertEqual(h.run(redo=True, to_id='O1'), EXIT_GATE, 'autoscale off needs no override')
        self.assertEqual(len(h.journal.events('accept_autoscale')), 1)
        h.journal.set_meta('preflight', json.dumps({'workspace': {'source': 'questions'}}))
        self.assertEqual(h.run(redo=True, to_id='O1'), EXIT_GATE)
        self.assertIn('workspace: not read through the Management API; the three questions: is the workspace ACTIVE', h.text())
        self.assertIn('workspace answer: none recorded; record the answers at O1 with --workspace NOTE', h.text())
        self.assertEqual(h.journal.events('workspace'), [])
        self.assertEqual(h.run(redo=True, to_id='O1', options=dict(h.options, workspace='ACTIVE, autoscale off, auto-suspend off')), EXIT_GATE)
        self.assertEqual(h.journal.events('workspace'), ['O1 workspace answer: ACTIVE, autoscale off, auto-suspend off'])
        self.assertIn('workspace answer: ACTIVE, autoscale off, auto-suspend off', h.text())
        self.assertIn('O1 workspace answer: ACTIVE, autoscale off, auto-suspend off', h.journal.step('O1')['note'])

    def test_f5_o1_journal_fault_points_resume_on_rerun(self):
        """The O1 journal writes the readiness checks guard, at the three points: before its first write (run_id), after the
        names, hash and window are written (at the no_rehearsal event), and after every meta write but before the step result
        (step_end, which leaves O1 running as a kill -9 does). Each rerun runs O1 again (it is not passed), records every key once
        more and waits for the go; the names and hash rules then hold on the journal (a changed replacement name or configuration
        is seen)."""
        from tests.fakedb import SimulatedCrash
        from tests.helpers import FAULT_POINTS, crash_journal
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = Harness(self).ready_for_o1()
                method, match = {'before': ('set_meta', lambda key, value: key == 'run_id'),
                                 'after': ('event', lambda kind, detail: kind == 'no_rehearsal'),
                                 'journal': ('step_end', lambda step_id, status, *a, **k: step_id == 'O1')}[point]
                fired = crash_journal(self, h.journal, method, match=match)
                with self.assertRaises(SimulatedCrash):
                    h.run()
                self.assertEqual(len(fired), 1)
                self.assertEqual(h.journal.step_status('O1'), 'running' if point == 'journal' else 'failed',
                                 'run_step records an interrupted O1 failed; a kill at the result write leaves it running')
                self.assertEqual(h.run(), EXIT_GATE)
                self.assertEqual(h.journal.step_status('O1'), 'pending')
                meta = h.journal.meta()
                self.assertEqual(meta['config_hash'], h.config.config_hash())
                self.assertEqual((meta['source_db'], meta['replacement_db'], meta['retained_db']), h.databases[0].names)
                self.assertEqual(meta['abort_deadline'], '2026-11-01T11:30:00Z')
                other = config_from(lambda d: d['databases'][0].update(replacement='example_orders_db_rebuild_20261102'))
                with self.assertRaisesRegex(KitError, 'records replacement database example_orders_db_rebuild_20261101'):
                    steps.check_run_names(h.journal, other.databases[0])
                self.assertIsNotNone(steps.config_change(h.journal, config_from(lambda d: d.update(copy={'workers': 8}))))
                self.assertIsNone(steps.config_change(h.journal, h.config))

    def test_f5_o1_runs_for_every_database_before_the_run_stops(self):
        """With two databases a pending O1 stops run_steps (and run_one, step O1) only after O1 ran for the second one too, so both
        journals hold the names, the configuration hash and the window before the one go; a refused O1 still stops at once. The
        three failure points are the second database's O1 writes: before its first write (run_id), after its names, hash and
        window (at the no_rehearsal event), and before its result (step_end, left running as a kill -9 does). The first database
        is left pending; a rerun records both and waits for the go, and the names and hash rules then hold on the second journal."""
        import yaml
        from s2rebuild.config import build_config
        from tests.fakedb import SimulatedCrash
        from tests.helpers import FAULT_POINTS, RUN_DATE, crash_journal

        def two(d):
            # W9: the second database leaves its names to the kit; run.mode test applies the defaults without a prompt
            d['databases'].append({'source': 'second_db', 'layout': {'partitions': 8, 'sub_partitions': 64}})
            d['run'] = {'mode': 'test'}

        def edited(h, mutate):
            """The harness configuration after an operator's edit."""
            data = yaml.safe_load(h.path.read_text(encoding='utf8'))
            mutate(data)
            return build_config(data, path=h.path, run_date=RUN_DATE, root=h.tmp)

        def recorded(h):
            for db in h.databases:
                journal = h.journals[db.source]
                self.assertEqual(journal.step_status('O1'), 'pending', db.source)
                self.assertIsNone(journal.step_status('O2'), db.source)
                meta = journal.meta()
                self.assertEqual(meta['config_hash'], h.config.config_hash(), db.source)
                self.assertEqual((meta['source_db'], meta['replacement_db'], meta['retained_db']), db.names)
                self.assertEqual(meta['abort_deadline'], '2026-11-01T11:30:00Z')
        h = Harness(self, two).ready_for_o1()
        self.assertEqual(h.run(), EXIT_GATE)
        recorded(h)
        self.assertEqual(h.text().count('Stopped at O1'), 1)
        h = Harness(self, two).ready_for_o1()
        self.assertEqual(steps.run_one(h.config, h.databases, h.journals, h.log, 'O1', run_id='20261101-0500', options=h.options),
                         EXIT_GATE)
        recorded(h)
        h = Harness(self, two).ready_for_o1()
        h.journal.step_end('P2', 'failed', 'preflight failed')
        self.assertEqual(h.run(), EXIT_REFUSED)
        self.assertIsNone(h.journals['second_db'].step_status('O1'), 'a refused O1 stops the run at once')
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = Harness(self, two).ready_for_o1()
                second = h.journals['second_db']
                method, match = {'before': ('set_meta', lambda key, value: key == 'run_id'),
                                 'after': ('event', lambda kind, detail: kind == 'no_rehearsal'),
                                 'journal': ('step_end', lambda step_id, status, *a, **k: step_id == 'O1')}[point]
                fired = crash_journal(self, second, method, match=match)
                with self.assertRaises(SimulatedCrash):
                    h.run()
                self.assertEqual(len(fired), 1)
                self.assertEqual(h.journal.step_status('O1'), 'pending', 'the first database ran O1 and waits for the go')
                self.assertEqual(second.step_status('O1'), 'running' if point == 'journal' else 'failed')
                self.assertEqual(h.run(), EXIT_GATE)
                recorded(h)
                steps.record_gate(h.journals, 'O1', True, 'go')
                self.assertIsNone(steps.config_change(second, edited(h, lambda d: None)))
                steps.check_run_names(second, edited(h, lambda d: None).databases[1])
                renamed = edited(h, lambda d: d['databases'][1].update(replacement='second_db_renamed'))
                with self.assertRaisesRegex(KitError, 'Journal for second_db records replacement database'):
                    steps.check_run_names(second, renamed.databases[1])
                self.assertIsNotNone(steps.config_change(second, edited(h, lambda d: d.setdefault('copy', {}).update(workers=8))))

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
        h = Harness(self, on_disk=True).ready_for_o1()
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
