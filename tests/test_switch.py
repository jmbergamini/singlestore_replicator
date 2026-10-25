"""s2rebuild tests test_switch: the state machine on a fake attachment table, re-entry, rollback, S0, the owner block engine and ownerlib.

Author: John Bergamini
"""
import contextlib
import io
import unittest

from s2rebuild import ownersql, steps, switch
from s2rebuild.common import KitError
from s2rebuild.sqlutil import read_guard
from tests.fakedb import FakeDisconnect
from tests.test_copy import SRC, TGT, Harness

RET = 'example_orders_db_retained_20261101'


def OWNER(d):
    """Config mutation that adds the owner profile (user ops_owner, password from the environment)."""
    d['connections'].update(owner={'user': 'ops_owner', 'password_env': 'S2REBUILD_OWNER_PASSWORD'})


def switched_harness(test, mutate=None):
    """A harness after O5 and O6 with the source frozen READONLY and the source and replacement storage ids recorded."""
    def both(d):
        OWNER(d)
        if mutate:
            mutate(d)
    h = Harness(test, mutate=both)
    h.env['S2REBUILD_OWNER_PASSWORD'] = 'owner-fixture'
    assert h.o5_o6().status == steps.PASSED, h.text()
    h.fake.attachment(SRC)['ATTACH_TYPE'] = 'READONLY'
    h.journal.record_attachment('source', SRC, 'sid-source', 'cid-1', 'READONLY')
    assert h.journal.attachment('replacement')['storage_id'] == h.fake.attachment(TGT)['STORAGE_ID'], 'O5 records the replacement'
    return h


def machine_for(h, runner=True, **kw):
    """A SwitchMachine on the harness with the capture counts and the target layout."""
    names = ownersql.names_for(h.ctx())
    owner = h.sessions.owner(steps.guard_for('O10', h.db, steps.OWNER))
    reader = h.sessions.runner(read_guard()) if runner else None
    counts = {'tables': len(h.inventory['tables']), 'views': 0, 'pipelines': 0}
    return switch.SwitchMachine(owner, names, h.journal, h.log, reader, counts, h.db.layout, **kw), names


def ready_for_s0(h):
    """Mark O7 to O9 passed, record freeze_time and verified_at after it."""
    for step_id in ('O7', 'O8', 'O9'):
        h.journal.step_end(step_id, 'passed', 'test')
    h.journal.set_meta('freeze_time', '2026-11-01T05:10:00Z')
    for t in h.journal.tables():
        h.journal.set_table_attr(t['table_name'], 'verified_at_utc', '2026-11-01T06:00:00Z')


class ExpectAndBlockTests(unittest.TestCase):
    def test_expectations(self):
        rows = [{'ATTACH_TYPE': 'READONLY', 'n': 0, 'STORAGE_ID': 'sid-9', 'NUM_PARTITIONS': 32, 'NUM_SUB_PARTITIONS': 512}]
        self.assertEqual(ownersql.Expect('row_count', count=1).evaluate(rows), (True, '1 row(s)'))
        self.assertEqual(ownersql.Expect('no_rows').evaluate([]), (True, '0 row(s)'))
        self.assertFalse(ownersql.Expect('some_rows').evaluate([])[0])
        self.assertEqual(ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READONLY').evaluate(rows), (True, 'ATTACH_TYPE = READONLY'))
        self.assertEqual(ownersql.Expect('column_equals', 'n', 1).evaluate([])[1], 'no rows')
        self.assertTrue(ownersql.Expect('column_not_in', 'STORAGE_ID', ['sid-source', 'sid-101']).evaluate(rows)[0])
        self.assertFalse(ownersql.Expect('column_not_in', 'STORAGE_ID', ['sid-9']).evaluate(rows)[0])
        ok, observed = ownersql.Expect('columns_equal', value={'NUM_PARTITIONS': 32, 'NUM_SUB_PARTITIONS': 512}).evaluate(rows)
        self.assertEqual((ok, observed), (True, 'NUM_PARTITIONS = 32, NUM_SUB_PARTITIONS = 512'))
        self.assertFalse(ownersql.Expect('columns_equal', value={'NUM_PARTITIONS': 16}).evaluate(rows)[0])
        self.assertTrue(ownersql.Expect('column_at_least', 'n', 0).evaluate(rows)[0])
        self.assertFalse(ownersql.Expect('all_column_equals', 'STATE', 'Stopped').evaluate([{'STATE': 'Stopped'}, {'STATE': 'Running'}])[0])
        with self.assertRaises(KitError):
            ownersql.Expect('bogus').evaluate([{'a': 1}])

    def test_forward_block_checks_every_s3_precondition_before_s1(self):
        h = switched_harness(self)
        names = ownersql.names_for(h.ctx())
        block = switch.forward_block(names, {'tables': 4, 'views': 0, 'pipelines': 2})
        text = block.render()
        self.assertTrue(text.startswith('-- O10: Switch forward\n'))
        detach_at = text.index('DETACH DATABASE `%s`;' % SRC)
        self.assertLess(text.index('S1 before (for S3): replacement attached READWRITE with the recorded STORAGE_ID'), detach_at)
        self.assertLess(text.index('S1 before (for S2): no attached database named retained'), detach_at)
        self.assertIn("expect STORAGE_ID = %s, ATTACH_TYPE = READWRITE" % names['replacement_sid'], text)
        self.assertIn('S3 after: the live STORAGE_ID is a new branch; expect STORAGE_ID not in (sid-source, %s)' % names['replacement_sid'], text)
        self.assertIn('expect NUM_PARTITIONS = 32, NUM_SUB_PARTITIONS = 512', text)
        self.assertIn('S3 after: tables count equals the capture; expect n = 4', text)
        self.assertIn('S3 after: pipelines count equals the capture; expect n = 2', text)
        self.assertIn("(DB = 'example_orders_db' OR INFO LIKE '%example_orders_db%') AND INFO IS NOT NULL", text)
        self.assertIn("USER NOT IN ('s2rebuild_runner', 'ops_owner')", text, 'printed blocks keep the SQL user names')
        detach = [i for i in block.statements if i.sql.startswith('DETACH')][0]
        self.assertEqual(detach.detach, {'wait': 120, 'kill': True})
        no_counts = switch.forward_block(names).render()
        self.assertIn('no capture is available', no_counts)

    def test_rollback_block_stops_once_writers_were_released(self):
        h = switched_harness(self)
        names = ownersql.names_for(h.ctx())
        self.assertNotIn('-- STOP', switch.rollback_block(names).render())
        text = switch.rollback_block(names, released=True).render()
        self.assertIn('-- STOP: rollback refused: O11 released writers on the live name', text)
        self.assertLess(text.index('-- STOP'), text.index('DETACH DATABASE'))
        self.assertIn("expect STORAGE_ID = sid-source, ATTACH_TYPE = READWRITE", text, 'R2 after checks READWRITE too')
        self.assertIn('C1 saw application writes', switch.rollback_block(names, app_writes_seen='2026-11-01T07:00:00Z').render())

    def test_block_dispatch_and_account_block(self):
        h = switched_harness(self)
        names = ownersql.names_for(h.ctx())
        for step_id in ('P1', 'C4', 'C3', 'O2', 'O3', 'O9', 'O10', 'O11'):
            block = ownersql.block_for(step_id, names, h.config, pipelines=[], grants={}, capture_step='O2')
            self.assertTrue(block.render().startswith('-- %s:' % step_id), step_id)
        with self.assertRaises(KitError):
            ownersql.block_for('O6', names)
        text = ownersql.account_block(names).render()
        self.assertIn("IDENTIFIED BY '<password>'", text)
        self.assertTrue(ownersql.ownerlib_compiles())


class MachineTests(unittest.TestCase):
    def test_forward_sequence_on_the_fake_attachment_table(self):
        h = switched_harness(self)
        machine, names = machine_for(h)
        self.assertEqual(machine.position(), switch.NOT_STARTED)
        self.assertEqual(machine.forward(), switch.SWITCHED)
        live = h.fake.attachment(SRC)
        self.assertEqual(live['ATTACH_TYPE'], 'READWRITE')
        self.assertNotIn(live['STORAGE_ID'], ('sid-source', names['replacement_sid']), 'the live name is a new branch')
        self.assertEqual(h.fake.attachment(RET)['ATTACH_TYPE'], 'READONLY')
        self.assertEqual(h.journal.attachment('live')['storage_id'], live['STORAGE_ID'])
        self.assertEqual(h.journal.attachment('retained')['database_name'], RET)
        self.assertEqual(h.fake.databases[SRC]['layout']['NUM_SUB_PARTITIONS'], 512, 'the live name has the target layout')
        self.assertIsNotNone(h.journal.get_meta('switched_utc'))
        self.assertEqual(sum(1 for w in h.fake.writes if w.startswith(('DETACH', 'ATTACH'))), 3)
        self.assertEqual(machine.forward(), switch.SWITCHED, 'a rerun at switched reruns the after-checks and issues nothing')
        self.assertEqual(sum(1 for w in h.fake.writes if w.startswith(('DETACH', 'ATTACH'))), 3)

    def test_out_of_order_statements_are_refused(self):
        h = switched_harness(self)
        machine, _ = machine_for(h, runner=False)
        before = len(h.fake.writes)
        with self.assertRaisesRegex(KitError, 'S2 refused: the switch is at "not started", not "source detached"'):
            machine.s2()
        with self.assertRaisesRegex(KitError, 'S3 refused'):
            machine.s3()
        self.assertEqual(len(h.fake.writes), before, 'a refused step issues nothing')
        machine.s1()
        self.assertEqual(machine.position(), switch.DETACHED)
        with self.assertRaisesRegex(KitError, 'S1 refused: the switch is at "source detached"'):
            machine.s1()
        with self.assertRaisesRegex(KitError, 'S3 refused'):
            machine.s3()
        machine.s2()
        self.assertEqual(machine.position(), switch.RETAINED)
        with self.assertRaisesRegex(KitError, 'S2 refused'):
            machine.s2()
        machine.s3()
        self.assertEqual(machine.position(), switch.SWITCHED)

    def test_s3_preconditions_are_checked_before_the_source_is_detached(self):
        h = switched_harness(self)
        h.journal.conn.execute("DELETE FROM attachments WHERE role = 'replacement'")
        machine, _ = machine_for(h)
        with self.assertRaisesRegex(KitError, 'replacement_sid not recorded'):
            machine.forward()
        self.assertFalse(any(w.startswith('DETACH') for w in h.fake.writes), 'the source stays attached')
        h2 = switched_harness(self)
        h2.fake.attachments.append({'DATABASE_NAME': RET, 'ATTACH_TYPE': 'READONLY', 'STORAGE_ID': 'sid-old', 'COMPUTE_ID': 'c', 'ATTACH_SYNTAX': None})
        machine2, _ = machine_for(h2)
        with self.assertRaisesRegex(KitError, 'state "unknown"|switch is at "unknown"|no attached database named retained'):
            machine2.forward()
        self.assertFalse(any(w.startswith('DETACH') for w in h2.fake.writes))
        h3 = switched_harness(self)
        h3.fake.attachment(TGT)['ATTACH_TYPE'] = 'READONLY'
        machine3, _ = machine_for(h3)
        with self.assertRaisesRegex(KitError, 'replacement attached READWRITE'):
            machine3.forward()
        self.assertFalse(any(w.startswith('DETACH') for w in h3.fake.writes))

    def test_checks_before_s1(self):
        h = switched_harness(self)
        machine, _ = machine_for(h, runner=False)
        h.fake.processlist = [{'ID': 5, 'USER': 'app', 'HOST': 'h', 'DB': SRC, 'COMMAND': 'Query', 'TIME': 1, 'INFO': 'INSERT INTO t VALUES (1)'}]
        with self.assertRaisesRegex(KitError, 'no foreign non-idle session'):
            machine.s1()
        h.fake.processlist = [{'ID': 6, 'USER': 'app', 'HOST': 'h', 'DB': 'other', 'COMMAND': 'Query', 'TIME': 1, 'INFO': 'DELETE FROM `%s`.t' % SRC}]
        with self.assertRaisesRegex(KitError, 'no foreign non-idle session'):
            machine.s1()
        h.fake.processlist = [{'ID': 7, 'USER': 'app', 'HOST': 'h', 'DB': SRC, 'COMMAND': 'Sleep', 'TIME': 1, 'INFO': None}]
        h.fake.attachment(SRC)['STORAGE_ID'] = 'sid-other'
        with self.assertRaisesRegex(KitError, 'switch is at "unknown"|recorded STORAGE_ID'):
            machine.s1()
        self.assertFalse(any(w.startswith('DETACH') for w in h.fake.writes))

    def test_a_failed_s3_check_never_becomes_a_passed_switch_on_rerun(self):
        h = switched_harness(self)
        h.fake.rows(TGT, 't_comp').pop()
        machine, _ = machine_for(h)
        with self.assertRaisesRegex(KitError, 't_comp holds 299 rows, the journal says 300'):
            machine.forward()
        self.assertEqual(machine.position(), switch.SWITCHED)
        self.assertIsNone(h.journal.get_meta('switched_utc'))
        machine2, _ = machine_for(h)
        with self.assertRaisesRegex(KitError, 't_comp holds 299 rows'):
            machine2.forward()
        self.assertIsNone(h.journal.get_meta('switched_utc'))

    def test_a_failed_s2_check_is_rerun_before_s3(self):
        h = switched_harness(self)
        machine, _ = machine_for(h)
        machine.s1()
        h.fake.storage_content['sid-source']['tables']['t_comp']['rows'].pop()
        with self.assertRaisesRegex(KitError, 'retained_20261101.t_comp holds 299 rows'):
            machine.s2()
        machine2, _ = machine_for(h)
        self.assertEqual(machine2.position(), switch.RETAINED)
        with self.assertRaisesRegex(KitError, 'retained_20261101.t_comp holds 299 rows'):
            machine2.forward()
        self.assertFalse(any(w.startswith('ATTACH DATABASE `%s` AS `%s`' % (TGT, SRC)) for w in h.fake.writes), 'S3 never ran')

    def test_the_live_name_on_the_replacement_storage_is_not_switched(self):
        h = switched_harness(self)
        names = ownersql.names_for(h.ctx())
        position = switch.Position({'DATABASE_NAME': SRC, 'ATTACH_TYPE': 'READWRITE', 'STORAGE_ID': names['replacement_sid']},
                                   {'DATABASE_NAME': RET, 'ATTACH_TYPE': 'READONLY', 'STORAGE_ID': 'sid-9'}, None, None)
        self.assertEqual(position.state(names), switch.UNKNOWN)
        position = switch.Position({'DATABASE_NAME': SRC, 'ATTACH_TYPE': 'READONLY', 'STORAGE_ID': 'sid-new'},
                                   {'DATABASE_NAME': RET, 'ATTACH_TYPE': 'READONLY', 'STORAGE_ID': 'sid-9'}, None, None)
        self.assertEqual(position.state(names), switch.UNKNOWN, 'a READONLY live name is not a switch')
        names['live_sid'] = 'sid-live'
        position = switch.Position({'DATABASE_NAME': SRC, 'ATTACH_TYPE': 'READWRITE', 'STORAGE_ID': 'sid-new'},
                                   {'DATABASE_NAME': RET, 'ATTACH_TYPE': 'READONLY', 'STORAGE_ID': 'sid-9'}, None, None)
        self.assertEqual(position.state(names), switch.UNKNOWN, 'once recorded, the live id must match')

    def test_rollback_from_every_position_and_its_refusals(self):
        h = switched_harness(self)
        machine, _ = machine_for(h)
        machine.forward()
        self.assertEqual(machine.rollback(drop_retained=True), switch.NOT_STARTED)
        src = h.fake.attachment(SRC)
        self.assertEqual((src['STORAGE_ID'], src['ATTACH_TYPE']), ('sid-source', 'READWRITE'))
        self.assertIsNone(h.fake.attachment(RET))
        self.assertIsNotNone(h.journal.get_meta('rolled_back_utc'))
        h2 = switched_harness(self)
        machine2, _ = machine_for(h2)
        machine2.s1()
        self.assertEqual(machine2.rollback(), switch.NOT_STARTED, 'R2 alone after S1')
        self.assertEqual(h2.fake.attachment(SRC)['STORAGE_ID'], 'sid-source')
        h3 = switched_harness(self)
        machine3, _ = machine_for(h3)
        machine3.forward()
        h3.journal.set_meta('released_utc', '2026-11-01T07:00:00Z')
        with self.assertRaisesRegex(KitError, 'Rollback refused: O11 released writers'):
            machine3.rollback()
        h3.journal.set_meta('app_writes_seen', '2026-11-01T07:05:00Z')
        with self.assertRaisesRegex(KitError, 'Rollback refused: C1 recorded application writes'):
            machine3.rollback()

    def test_a_started_rollback_is_never_continued_forward(self):
        h = switched_harness(self)
        machine, names = machine_for(h)
        machine.forward()
        machine._run('DETACH DATABASE `%s`' % SRC, 'R1 by hand')
        machine2, _ = machine_for(h)
        self.assertEqual(machine2.position(), switch.LIVE_DETACHED)
        with self.assertRaisesRegex(KitError, 'a rollback is in progress'):
            machine2.forward()
        self.assertEqual(machine2.rollback(), switch.NOT_STARTED)

    def test_a_failed_statement_reports_the_position(self):
        h = switched_harness(self)
        machine, _ = machine_for(h)
        machine.s1()
        h.fake.storage.pop('sid-source')
        with self.assertRaisesRegex(KitError, 'Switch statement "S2 ATTACH retained READ ONLY" failed: .*the switch is now at "source detached"'):
            machine.s2()
        self.assertIn('switch_statement_failed', [e['kind'] for e in h.journal.events()])


class PipelinesAcrossTheSwitchTests(unittest.TestCase):
    def test_replayed_pipelines_follow_the_replacement_branch_and_the_retained_copy_keeps_the_originals(self):
        h = switched_harness(self)
        h.fake.pipelines = [{'PIPELINE_NAME': 'orig', 'STATE': 'Stopped', 'DATABASE_NAME': SRC},
                            {'PIPELINE_NAME': 'p1', 'STATE': 'Stopped', 'DATABASE_NAME': TGT}]
        machine, _ = machine_for(h)
        machine.counts['pipelines'] = 1
        self.assertEqual(machine.forward(), switch.SWITCHED)
        visible = sorted((p['DATABASE_NAME'], p['PIPELINE_NAME'], p['STATE']) for p in h.fake.pipelines if p.get('DATABASE_NAME'))
        self.assertIn((SRC, 'p1', 'Stopped'), visible, 'the live name carries the pipeline replayed into the replacement')
        self.assertIn((RET, 'orig', 'Stopped'), visible, 'the retained copy keeps the original pipelines, stopped')
        self.assertNotIn((SRC, 'orig', 'Stopped'), visible)


class CloseoutHandlerTests(unittest.TestCase):
    def test_c3_waits_for_retain_days_and_c4_runs_its_block(self):
        h = switched_harness(self)
        for step_id in ('O7', 'O8', 'O9', 'O10', 'O11', 'C1', 'C2'):
            h.journal.step_end(step_id, 'passed', 'test')
        out = h.step('C3', redo=True)
        self.assertEqual(out.status, steps.REFUSED)
        self.assertIn('needs the release time', out.note)
        h.journal.set_meta('released_utc', '2099-01-01T00:00:00Z')
        out = h.step('C3', redo=True)
        self.assertEqual(out.status, steps.REFUSED)
        self.assertIn('C3 is due after 2099-01-08', out.note)
        h.journal.step_end('C3', 'passed', 'test')
        out = h.step('C4', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertTrue(any(w.startswith("DROP USER 's2rebuild_runner'") for w in h.fake.writes))


class DetachEscalationTests(unittest.TestCase):
    def test_a_waiting_detach_is_cancelled_and_issued_again_with_kill(self):
        h = switched_harness(self, mutate=lambda d: d.setdefault('freeze', {}).update(detach_wait_seconds=1))
        h.fake.detach_blocks.add(SRC)
        guard = steps.guard_for('O10', h.db, steps.OWNER)
        machine, _ = machine_for(h, open_session=h.sessions.owner, guard=guard)
        machine.s1()
        self.assertTrue(any(w.startswith('KILL QUERY') for w in h.fake.writes))
        self.assertIn('DETACH DATABASE `%s` KILL' % SRC, h.fake.writes)
        self.assertEqual(machine.position(), switch.DETACHED)
        self.assertIn('detach_kill', [e['kind'] for e in h.journal.events()])

    def test_without_detach_kill_the_waiting_detach_is_cancelled_and_the_step_stops(self):
        h = switched_harness(self, mutate=lambda d: d.setdefault('freeze', {}).update(detach_wait_seconds=1, detach_kill=False))
        h.fake.detach_blocks.add(SRC)
        guard = steps.guard_for('O10', h.db, steps.OWNER)
        machine, _ = machine_for(h, open_session=h.sessions.owner, guard=guard)
        with self.assertRaisesRegex(KitError, 'detach_kill is off; the waiting DETACH was cancelled'):
            machine.s1()
        self.assertNotIn('DETACH DATABASE `%s` KILL' % SRC, h.fake.writes)
        self.assertEqual(machine.position(), switch.NOT_STARTED, 'the source is still attached')


class S0AndHandlerTests(unittest.TestCase):
    def test_s0_refusals(self):
        h = switched_harness(self)
        ctx = h.ctx(yes=True)
        names = ownersql.names_for(ctx)
        with self.assertRaisesRegex(KitError, 'O9 has not passed'):
            switch.s0_checks(ctx, names)
        ready_for_s0(h)
        h.journal.set_meta('freeze_time', None)
        h.journal.conn.execute("DELETE FROM meta WHERE key = 'freeze_time'")
        with self.assertRaisesRegex(KitError, 'no freeze_time is recorded .*step O3 --redo'):
            switch.s0_checks(ctx, names)
        ready_for_s0(h)
        for t in h.journal.tables():
            h.journal.set_table_attr(t['table_name'], 'verified_at_utc', '2026-11-01T05:00:00Z')
        with self.assertRaisesRegex(KitError, r'verified .* before the freeze'):
            switch.s0_checks(ctx, names)
        ready_for_s0(h)
        lines = switch.s0_checks(ctx, names)
        self.assertTrue(lines[0].startswith('S0: O8 and O9 passed'))
        self.assertIn('abort deadline', lines[1])

    def test_s0_uses_the_window_deadline_and_skips_the_time_check_after_s1(self):
        import dataclasses
        import datetime as dt
        h = switched_harness(self)
        ready_for_s0(h)
        past = dataclasses.replace(h.config.window, start=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc), end=dt.datetime(2020, 1, 1, 8, tzinfo=dt.timezone.utc))
        ctx = steps.StepContext(dataclasses.replace(h.config, window=past), h.db, h.journal, h.run_id, h.log, yes=True, sessions=h.sessions)
        names = ownersql.names_for(ctx)
        with self.assertRaisesRegex(KitError, 'switch allowance remains before the abort deadline 2020-01-01'):
            switch.s0_checks(ctx, names)
        self.assertTrue(switch.s0_checks(ctx, names, position=switch.RETAINED), 're-entry behind S1 is never blocked by the clock')

    def test_handler_typed_live_name(self):
        h = switched_harness(self)
        ready_for_s0(h)
        out = h.step('O10', yes=False)
        self.assertEqual(out.status, steps.PENDING)
        self.assertIn('type the live name', out.note)
        out = h.step('O10', yes=False, options={'confirm': lambda prompt: 'wrong_name'})
        self.assertEqual(out.status, steps.REFUSED)
        self.assertFalse(any(w.startswith('DETACH') for w in h.fake.writes))
        out = h.step('O10', yes=False, options={'confirm': lambda prompt: SRC})
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('switch complete', out.note)
        self.assertIn('switch_go', [e['kind'] for e in h.journal.events()])
        self.assertTrue((h.tmp / 'runs' / h.run_id / SRC / 'owner-sql' / 'O10.sql').is_file())
        self.assertTrue((h.tmp / 'runs' / h.run_id / SRC / 'owner-sql' / 'O10-rollback.sql').is_file())

    def test_handler_with_yes_and_a_replacement_recorded_late(self):
        h = switched_harness(self)
        h.journal.conn.execute("DELETE FROM attachments WHERE role = 'replacement'")
        ready_for_s0(h)
        out = h.step('O10', yes=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIsNotNone(h.journal.attachment('replacement'))
        out = h.step('O10', dry_run=True)
        self.assertEqual(out.status, steps.PENDING)


class ExecuteBlockTests(unittest.TestCase):
    def test_execute_block_stops_at_a_failed_check(self):
        h = switched_harness(self)
        names = ownersql.names_for(h.ctx())
        owner = h.sessions.owner(steps.guard_for('C4', h.db, steps.OWNER))
        block = ownersql.account_block(names, drop=True)
        h.fake.privileges = [p for p in h.fake.privileges if p['GRANTEE'] != "'s2rebuild_runner'@'%'"]
        with self.assertRaisesRegex(KitError, 'stopped at check "runner account still has privileges"'):
            ownersql.execute_block(block, owner, h.log, h.journal)
        self.assertFalse(any(w.startswith('DROP USER') for w in h.fake.writes))
        h.fake.privileges.append({'GRANTEE': "'s2rebuild_runner'@'%'", 'PRIVILEGE_TYPE': 'SELECT'})
        results = ownersql.execute_block(block, owner, h.log, h.journal)
        self.assertEqual([label for label, _ in results], ['runner account still has privileges', 'zero privileges remain'])
        self.assertTrue(any(w.startswith("DROP USER 's2rebuild_runner'") for w in h.fake.writes))

    def test_stop_items_skip_checks_and_withheld_secret_errors(self):
        h = switched_harness(self)
        owner = h.sessions.owner(steps.guard_for('O9', h.db, steps.OWNER))
        block = ownersql.Block('O9', 'test').refuse('not today')
        with self.assertRaisesRegex(KitError, 'O9 refused: not today'):
            ownersql.execute_block(block, owner, h.log, h.journal)
        block = ownersql.Block('O9', 'test')
        block.statement('USE `%s`' % TGT, 'use', skip_if=('SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s', (SRC,)))
        h.fake.pipelines = [{'PIPELINE_NAME': 'p', 'STATE': 'Stopped'}]
        ownersql.execute_block(block, owner, h.log, h.journal)
        self.assertIn('owner_skipped', [e['kind'] for e in h.journal.events()])
        block = ownersql.Block('O9', 'test')
        block.statement("CREATE PIPELINE `p9` AS LOAD DATA KAFKA 'b/t' CREDENTIALS '<<CREDENTIALS>>' INTO TABLE `t`", 'create pipeline p9')
        h.fake.pipelines = [{'PIPELINE_NAME': 'p9', 'STATE': 'Stopped', 'DATABASE_NAME': SRC}]
        with self.assertRaises(KitError) as caught:
            ownersql.execute_block(block, owner, h.log, h.journal, substitute=lambda sql: sql.replace("'<<CREDENTIALS>>'", "'super-secret-value'"))
        self.assertIn('the message is withheld because the statement carried an injected secret', str(caught.exception))
        self.assertNotIn('super-secret-value', str(caught.exception))

    def test_placeholders_refuse_execution(self):
        h = switched_harness(self)
        names = ownersql.names_for(h.ctx())
        names['live_sid'] = '<live_sid>'
        block = ownersql.cleanup_block(names)
        with self.assertRaisesRegex(KitError, 'C3 cannot run: live_sid not recorded'):
            ownersql.refuse_placeholders(block)


class _TupleCursor:
    """A DB-API cursor over the fake that returns tuples and a description, like drivers without dict rows."""

    def __init__(self, inner):
        self.inner = inner
        self.description = None

    def execute(self, sql, params=None):
        self.inner.execute(sql, params)
        rows = self.inner.fetchall() or []
        self.description = [(k,) for k in rows[0]] if rows else []
        self._rows = [tuple(r.values()) for r in rows]

    def fetchall(self):
        return self._rows


class OwnerlibTests(unittest.TestCase):
    def namespace(self):
        ns = {}
        exec(compile(ownersql.OWNERLIB_SOURCE, 'ownerlib.py', 'exec'), ns)
        return ns

    def test_ownerlib_runs_the_forward_block_records_ids_and_refuses_a_rerun(self):
        h = switched_harness(self)
        names = ownersql.names_for(h.ctx())
        ns = self.namespace()
        cursor = h.fake.connect().cursor()
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            result = ns['run_block'](cursor, switch.forward_block(names, {'tables': 4, 'views': 0, 'pipelines': 0}).to_dict(), {'run_id': h.run_id})
        self.assertEqual(result['status'], 'passed', out.getvalue())
        self.assertEqual(result['statements'], 3)
        roles = {a['role']: a['row']['STORAGE_ID'] for a in result['attachments']}
        self.assertEqual(set(roles), {'retained', 'live'})
        self.assertEqual(roles['live'], h.fake.attachment(SRC)['STORAGE_ID'])
        with contextlib.redirect_stdout(out):
            again = ns['run_block'](cursor, switch.forward_block(names).to_dict(), {'run_id': h.run_id})
        self.assertTrue(again['status'].startswith('FAILED at check S1 before'))

    def test_ownerlib_with_tuple_rows_and_stops(self):
        h = switched_harness(self)
        names = ownersql.names_for(h.ctx())
        ns = self.namespace()
        cursor = _TupleCursor(h.fake.connect().cursor())
        with contextlib.redirect_stdout(io.StringIO()):
            result = ns['run_block'](cursor, switch.forward_block(names).to_dict(), {})
        self.assertEqual(result['status'], 'passed', 'tuple rows are mapped through the cursor description')
        with contextlib.redirect_stdout(io.StringIO()):
            refused = ns['run_block'](cursor, switch.rollback_block(names, released=True).to_dict(), {})
        self.assertTrue(refused['status'].startswith('REFUSED: rollback refused'))
        self.assertEqual(refused['statements'], 0)

    def test_ownerlib_credentials_by_name_or_source_type_escaped_and_never_echoed(self):
        h = switched_harness(self)
        ns = self.namespace()
        value = '{"key": "a\\\\nb", "q": "it\'s"}'
        self.assertEqual(ns['sql_literal'](value), "'" + value.replace('\\', '\\\\').replace("'", "''") + "'")
        block = ownersql.Block('O9', 'replay')
        block.statement('USE `%s`' % TGT, 'use')
        block.statement("CREATE PIPELINE `p1` AS LOAD DATA KAFKA 'b/t' CREDENTIALS '<<CREDENTIALS>>' INTO TABLE `t`", 'create pipeline p1',
                        pipeline='p1', source_type='KAFKA')
        cursor = h.fake.connect().cursor()
        with contextlib.redirect_stdout(io.StringIO()):
            result = ns['run_block'](cursor, block.to_dict(), {}, credentials={'KAFKA': value})
        self.assertEqual(result['status'], 'passed')
        created = [p for p in h.fake.pipelines if p['PIPELINE_NAME'] == 'p1'][0]
        self.assertIn(ns['sql_literal'](value), created['definition'])
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            again = ns['run_block'](cursor, block.to_dict(), {}, credentials={'p1': value})
        self.assertTrue(again['status'].startswith('ERROR'), 'a duplicate CREATE fails on the fake')
        self.assertIn('message is withheld', again['error'])
        self.assertNotIn('it', again['error'].replace('withheld', '').replace('carried', '').replace('credentials', ''), again['error'])
        self.assertNotIn(value, out.getvalue())

    def test_ownerlib_capture_item(self):
        h = switched_harness(self)
        h.fake.pipelines = [{'PIPELINE_NAME': 'p1', 'STATE': 'Stopped', 'DATABASE_NAME': SRC}]
        h.fake.pipeline_offsets = [{'DATABASE_NAME': SRC, 'PIPELINE_NAME': 'p1', 'SOURCE_TYPE': 'KAFKA', 'SOURCE_PARTITION_ID': '0', 'EARLIEST_OFFSET': 1,
                                    'LATEST_OFFSET': 9, 'LATEST_LOADED_OFFSET': 9, 'LATEST_SUCCESSFUL_OFFSET': 9}]
        block = ownersql.Block('O2', 'stop').capture(SRC, 'capture now')
        with contextlib.redirect_stdout(io.StringIO()):
            result = self.namespace()['run_block'](h.fake.connect().cursor(), block.to_dict(), {})
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['capture']['database'], SRC)
        self.assertEqual(len(result['capture']['definitions']['p1']), 2, 'CREATE and ALTER rows')
        self.assertEqual(result['capture']['offsets'][0]['LATEST_LOADED_OFFSET'], '9')


if __name__ == '__main__':
    unittest.main()
