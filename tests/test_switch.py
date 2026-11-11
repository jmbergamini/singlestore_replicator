"""s2rebuild tests test_switch: the state machine on a fake attachment table, re-entry, rollback, S0, the owner block engine and ownerlib.

Author: John Bergamini
"""
import contextlib
import io
import json
import os
import unittest
from pathlib import Path
from unittest import mock

from s2rebuild import ownersql, steps, switch
from s2rebuild.common import EXIT_DONE, EXIT_INCOMPLETE, EXIT_REFUSED, KitError
from s2rebuild.journal import Journal
from s2rebuild.sqlutil import read_guard
from tests.fakedb import SimulatedCrash, multiset
from tests.helpers import FAULT_POINTS, Plan, crash_journal, crash_journals, run_cli
from tests.test_copy import SRC, TGT, Harness
from tests.test_verify import (LATE_ROW, arm_recopy, assert_counter_follows, assert_reinserted_once, auto_increment_on_t_comp, counter_writes,
                               run_from_o6)

RET = 'example_orders_db_retained_20261101'
SID = '1028600672070281646:7367158985892520834'
# the branch record of the fixture source as the fake seeds it (O3 records it with the attachment row)
SOURCE_RECORD = {'record_type': 'create', 'attach_syntax': 'ATTACH DATABASE `%s` STORAGEID %s COMPUTEID cid-1' % (SRC, SID), 'is_detached': 0}
S3_PATTERN = r'^ATTACH DATABASE `%s` AS `%s`' % (TGT, SRC)


def OWNER(d):
    """Config mutation that adds the owner profile (user ops_owner, password from the environment)."""
    d['connections'].update(owner={'user': 'ops_owner', 'password_env': 'S2REBUILD_OWNER_PASSWORD'})


def switched_harness(test, mutate=None, mutate_inventory=None, owner=True):
    """A harness after O5 and O6 with the source frozen READONLY and the source and replacement storage ids recorded.

    owner=False leaves the owner profile out, so O10 prints its blocks for the hand route (print mode).
    """
    def both(d):
        if owner:
            OWNER(d)
        if mutate:
            mutate(d)
    h = Harness(test, mutate=both, mutate_inventory=mutate_inventory)
    h.env['S2REBUILD_OWNER_PASSWORD'] = 'owner-fixture'
    assert h.o5_o6().status == steps.PASSED, h.text()
    h.fake.attachment(SRC)['ATTACH_TYPE'] = 'READONLY'
    h.journal.record_attachment('source', SRC, SID, 'cid-1', 'READONLY', **SOURCE_RECORD)
    assert h.journal.attachment('replacement')['storage_id'] == h.fake.attachment(TGT)['STORAGE_ID'], 'O5 records the replacement'
    return h


def machine_for(h, runner=True, **kw):
    """A SwitchMachine on the harness with the capture counts and the target layout."""
    names = ownersql.names_for(h.ctx())
    owner = h.sessions.owner(steps.guard_for('O10', h.db, steps.OWNER))
    reader = h.sessions.runner(read_guard()) if runner else None
    counts = {'tables': len(h.inventory['tables']), 'views': 0, 'pipelines': 0}
    return switch.SwitchMachine(owner, names, h.journal, h.log, reader, counts, h.db.layout, **kw), names


COPIED_BEFORE_THE_STAMPS = '2026-10-31T00:00:00Z'


def ready_for_s0(h):
    """Mark O7 to O9 passed, record freeze_time, a matching count row per table at its re-copy epoch (as O8 does) and verified_at after it.

    The harness copies at the wall clock, and the stamps below are fixed. Every finished chunk
    is given a fixed end before them, so the result does not depend on the day the suite runs.
    """
    for step_id in ('O7', 'O8', 'O9'):
        h.journal.step_end(step_id, 'passed', 'test')
    h.journal.conn.execute('UPDATE chunks SET ended_utc = ? WHERE ended_utc IS NOT NULL', (COPIED_BEFORE_THE_STAMPS,))
    h.journal.set_meta('freeze_time', '2026-11-01T05:10:00Z')
    for t in h.journal.tables():
        h.journal.record_verification(t['table_name'], 'count', 'match', t['target_count'], t['target_count'])
        h.journal.set_table_attr(t['table_name'], 'verified_at_utc', '2026-11-01T06:00:00Z')


FROZE_BEFORE_THE_COPY = '2026-01-01T00:00:00Z'


def counters_settable(h):
    """The ALTER path of the counter logic: Helios 10.8.0 refuses it on a sharded table (1749, covered by its own test)."""
    h.fake.refuse_sharded_counter = False


def verified_harness(test, mutate_inventory=None, owner=True, mutate=None, prepare=None):
    """A switched harness after real O7 and O8 runs, O9 passed and freeze_time before the copy, so S0 accepts.

    prepare(h), when given, runs before O7 (a test sets a fake flag the first O7 must see).
    """
    h = switched_harness(test, mutate=mutate, mutate_inventory=mutate_inventory, owner=owner)
    if prepare is not None:
        prepare(h)
    h.journal.set_meta('freeze_time', FROZE_BEFORE_THE_COPY)
    for step_id in ('O7', 'O8'):
        out = h.step(step_id, redo=False)
        assert out.status == steps.PASSED, h.text()
    h.journal.step_end('O9', 'passed', 'no pipeline in the fixture')
    return h


def s0(h):
    """The S0 lines for the harness, or the KitError S0 raises."""
    ctx = h.ctx(yes=True)
    return switch.s0_checks(ctx, ownersql.names_for(ctx))


def detaches(h):
    """Every S1 DETACH of the live name the fake applied."""
    return [w for w in h.fake.writes if w.startswith('DETACH DATABASE `%s`' % SRC)]


def copy_evidence(h):
    """What a re-copy would change: chunk states, the rollback proof's target counts, O6 to O9 and each table's stamp and epoch."""
    return (h.journal.chunk_states(), ownersql.table_counts(h.journal), {s: h.journal.step_status(s) for s in ('O6', 'O7', 'O8', 'O9')},
            [(t['table_name'], t['verified_at_utc'], t['recopy_epoch']) for t in h.journal.tables()])


def mark_recopy_cli(h, table='t_single', bucket='1'):
    """The operator's s2rebuild mark-recopy TABLE --bucket N on the harness run; returns (exit code, text)."""
    return run_cli(['mark-recopy', table, '--bucket', bucket, '--config', str(h.config_path), '--run', h.run_id], env=h.env)


FRESH_RENAME = r'^ALTER TABLE `%s`\.`t_comp__fresh` RENAME TO `t_comp`$' % TGT


def o10_files(h):
    """The forward and rollback block files that O10 and owner-sql O10 write for the harness database."""
    folder = h.tmp / 'runs' / h.run_id / SRC / 'owner-sql'
    return folder / 'O10.sql', folder / 'O10-rollback.sql'


def stale_notebook_switch(test, h):
    """The O10 forward cell of a notebook generated after O8 passed runs after mark-recopy reopened the copy.

    It holds no STOP, so the switch runs on rows that were not verified again. Returns the
    evidence file of its result, as the operator saves it for gate O10.
    """
    ctx = h.ctx()
    stale = ownersql.block_for('O10', ownersql.names_for(ctx), h.config, **ownersql.block_inputs(ctx))
    test.assertEqual(stale.refusals, [], 'generated while S0 passed')
    code, text = mark_recopy_cli(h)
    test.assertEqual(code, EXIT_DONE, text)
    with contextlib.redirect_stdout(io.StringIO()):
        result = ownerlib()['run_block'](h.fake.connect('ops_owner').cursor(), stale.to_dict(), {'run_id': h.run_id})
    test.assertEqual(result['status'], 'passed', result)
    evidence = h.tmp / 'o10-result.json'
    evidence.write_text(json.dumps(result))
    return evidence


SECOND = 'second_db'


def two_databases(d):
    """Config mutation: a second configured database, second_db."""
    d['databases'].append({'source': SECOND, 'layout': {'partitions': 8, 'sub_partitions': 64}})


def second_journal(test, h, switched=False):
    """The journal of second_db: two planned tables copied and verified after freeze_time, O1 to O9 passed.

    switched=True also records O10 passed, as after its switch. The fake holds no second_db, so
    a step of it that is not skipped would fail; the tests only run the steps it skips.
    """
    db = h.config.database(SECOND)
    j = Journal(h.tmp / 'runs' / h.run_id / SECOND / 'journal.sqlite')
    test.addCleanup(j.close)
    j.init_plan([Plan('t_single', 700, 3, key_expr='`id`', columns=('id', 'payload', 'n')), Plan('t_comp', 300, 1, columns=('a', 'b', 'c', 'note'))],
                db.source, db.replacement)
    j.set_meta('freeze_time', FROZE_BEFORE_THE_COPY)
    for t in j.tables():
        for b in range(t['n_buckets']):
            j.mark_started(t['table_name'], b)
            j.mark_done(t['table_name'], b, 10)
        j.record_table_counts(t['table_name'], 10 * t['n_buckets'], 10 * t['n_buckets'])
        j.record_verification(t['table_name'], 'count', 'match', 10 * t['n_buckets'], 10 * t['n_buckets'])
    for step_id in ('O1', 'O2', 'O3', 'O4', 'O5', 'O6', 'O7', 'O8', 'O9'):
        j.step_end(step_id, 'passed', 'second database, recorded for the test')
    if switched:
        j.record_gate('O10', True, 'second database switched')
    return j


def second_state(j):
    """What a run over second_db must leave alone: chunk states, step statuses, stamps, epochs and target counts."""
    return (j.chunk_states(), {k: v['status'] for k, v in j.steps_board().items()},
            [(t['table_name'], t['verified_at_utc'], t['recopy_epoch'], t['target_count']) for t in j.tables()])


def arm_s3(test, h, point, crash=False):
    """Arm a failure at one of FAULT_POINTS around the S3 ATTACH; returns the exception the switch then raises.

    before: the server never sees S3. after: S3 is applied and the reply is lost. journal: S3 is
    applied and the kit dies before its journal event (and so before the live branch is recorded).
    crash=True makes before and after a kill -9 stand-in too.
    """
    if point == 'journal':
        crash_journal(test, h.journal, 'event', match=lambda kind, detail: kind == 'switch_statement' and detail.startswith('S3'))
        return SimulatedCrash
    h.fake.fail_statement(S3_PATTERN, point, crash=crash)
    return SimulatedCrash if crash else KitError


def s3_writes(h):
    """Every S3 ATTACH the fake applied."""
    return [w for w in h.fake.writes if w.startswith('ATTACH DATABASE `%s` AS `%s`' % (TGT, SRC))]


def app_write(h):
    """The application writes one row to the live branch (the live name after S3)."""
    h.fake.rows(SRC, 't_comp').append({'a': 'a9', 'b': '2026-11-01 07:00:00.000000', 'c': 999, 'note': 'written after S3'})


def ownerlib():
    """The ownerlib namespace the notebook runs."""
    ns = {}
    exec(compile(ownersql.OWNERLIB_SOURCE, 'ownerlib.py', 'exec'), ns)
    return ns


def rollback_block_for(h, retained=False):
    """The printed rollback block: R1 and R2a, or with retained=True the companion through the retained copy (R1, R2b)."""
    ctx = h.ctx()
    names, inputs = ownersql.names_for(ctx), ownersql.block_inputs(ctx)
    if retained:
        return dict(ownersql.companion_blocks('O10', names, h.config, rollback=True, released=inputs['released'],
                                              app_writes_seen=inputs['app_writes_seen']))['retained']
    return ownersql.block_for('O10', names, h.config, rollback=True, **inputs)


def run_rollback_block(h, retained=False):
    """Run the rollback block as the notebook does (ownerlib on a plain cursor); returns the result dict."""
    block = rollback_block_for(h, retained)
    with contextlib.redirect_stdout(io.StringIO()):
        return ownerlib()['run_block'](h.fake.connect().cursor(), block.to_dict(), {'run_id': h.run_id})


def switch_without_s2b(h):
    """A switch that took S1, S2 and S3 but not S2b and S4: the machine's S1 and S2, then S3 by hand on the fake with the live record recorded.

    The workspace then holds four entries: the detached original and the live branch under the
    live name, the replacement and the retained copy. Returns (machine, names) read afterwards.
    """
    machine, names = machine_for(h)
    machine.s1()
    machine.s2()
    h.fake.connect('ops_owner').cursor().execute('ATTACH DATABASE `%s` AS `%s` STORAGEID %s' % (TGT, SRC, names['replacement_sid']))
    session = h.sessions.runner(read_guard())
    try:
        switch.journal_record(h.journal, 'live', switch.identify(session, SRC))
    finally:
        session.close()
    h.journal.set_meta('switched_utc', '2026-11-01T06:30:00Z')
    h.journal.event('switch_statement', 'S3 ATTACH replacement AS source')
    return machine_for(h)


class ExpectAndBlockTests(unittest.TestCase):
    def test_expectations(self):
        rows = [{'ATTACH_TYPE': 'READONLY', 'n': 0, 'STORAGE_ID': 'sid-9', 'NUM_PARTITIONS': 32, 'NUM_SUB_PARTITIONS': 512}]
        self.assertEqual(ownersql.Expect('row_count', count=1).evaluate(rows), (True, '1 row(s)'))
        self.assertEqual(ownersql.Expect('no_rows').evaluate([]), (True, '0 row(s)'))
        self.assertFalse(ownersql.Expect('some_rows').evaluate([])[0])
        self.assertEqual(ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READONLY').evaluate(rows), (True, 'ATTACH_TYPE = READONLY'))
        self.assertEqual(ownersql.Expect('column_equals', 'n', 1).evaluate([])[1], 'no rows')
        self.assertTrue(ownersql.Expect('column_not_in', 'STORAGE_ID', ['1028600672070281646:7367158985892520834', '1000000000000000101:7000000000000000101']).evaluate(rows)[0])
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
        self.assertIn('S3 after: the live STORAGE_ID is not the original storage; expect STORAGE_ID not in (1028600672070281646:7367158985892520834)', text)
        self.assertIn('S3 after: the live name is its own branch record of the replacement; expect n >= 1', text)
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
        self.assertIn("expect STORAGE_ID = 1028600672070281646:7367158985892520834, ATTACH_TYPE = READWRITE", text, 'R2 after checks READWRITE too')
        self.assertIn('C1 saw application writes', switch.rollback_block(names, app_writes_seen='2026-11-01T07:00:00Z').render())

    def test_block_dispatch_and_account_block(self):
        h = switched_harness(self)
        names = ownersql.names_for(h.ctx())
        for step_id in ('P1', 'C4', 'C3', 'O2', 'O3', 'O9', 'O10', 'O11'):
            block = ownersql.block_for(step_id, names, h.config, pipelines=[], capture_step='O2')
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
        self.assertEqual(machine.forward(), switch.COMPLETE)
        live = h.fake.attachment(SRC)
        self.assertEqual(live['ATTACH_TYPE'], 'READWRITE')
        self.assertEqual(live['STORAGE_ID'], names['replacement_sid'], 'Helios keeps the replacement STORAGE_ID for the live branch')
        self.assertNotEqual(live['COMPUTE_ID'], h.journal.attachment('replacement')['compute_id'], 'the live name is its own branch record')
        self.assertIsNone(h.fake.attachment(TGT), 'S4 dropped the replacement record')
        self.assertEqual(h.fake.attachment(RET)['ATTACH_TYPE'], 'READONLY')
        self.assertEqual(h.journal.attachment('live')['storage_id'], live['STORAGE_ID'])
        self.assertEqual(h.journal.attachment('retained')['database_name'], RET)
        self.assertEqual(h.fake.databases[SRC]['layout']['NUM_SUB_PARTITIONS'], 512, 'the live name has the target layout')
        self.assertIsNotNone(h.journal.get_meta('switched_utc'))
        moves = [w for w in h.fake.writes if w.startswith(('DETACH', 'ATTACH', 'DROP DATABASE'))]
        self.assertEqual(len(moves), 6, 'S1, S2, S2b attach and drop, S3, S4')
        self.assertEqual(machine.forward(), switch.COMPLETE, 'a rerun at complete reruns the after-checks and the gate and issues nothing')
        self.assertEqual([w for w in h.fake.writes if w.startswith(('DETACH', 'ATTACH', 'DROP DATABASE'))], moves)

    def test_forward_accepts_a_live_branch_on_the_replacement_storage_id(self):
        """Helios 10.8.0 (October 27, 2026): ATTACH replacement AS live keeps the replacement's STORAGE_ID; the live name is still its own branch."""
        h = switched_harness(self)
        h.fake.branch_keeps_storage_id = True
        machine, names = machine_for(h)
        self.assertEqual(machine.forward(), switch.COMPLETE, h.text())
        live, replacement = h.fake.attachment(SRC), h.journal.attachment('replacement')
        self.assertEqual(live['STORAGE_ID'], names['replacement_sid'], 'the fake keeps the storage id, as Helios does')
        self.assertNotEqual(live['COMPUTE_ID'], replacement['compute_id'])
        self.assertIn('the live name is its own branch of the replacement', h.text())
        self.assertEqual(h.journal.attachment('live')['storage_id'], names['replacement_sid'])
        self.assertEqual(machine.position(), switch.COMPLETE)
        self.assertEqual(machine.forward(), switch.COMPLETE, 'a rerun at complete reruns the after-checks and issues nothing')

    def test_the_replacement_storage_id_without_a_branch_row_is_refused(self):
        h = switched_harness(self)
        h.fake.branch_keeps_storage_id = True
        machine, names = machine_for(h)
        machine.s1()
        machine.s2()
        machine.s2b()
        h.fake.branches.append({'STORAGE_ID': 'x', 'DATABASE_NAME': 'x', 'IS_DETACHED': 0, 'IS_ATTACHED_TO_THIS_COMPUTE': 1, 'IS_ATTACHABLE': 1,
                                'IS_DROPPED': 0, 'PREV_DATABASE_NAME': None, 'LATEST_ATTACH_TIME': 'now'})
        real = switch.live_is_branch_of_replacement
        try:
            switch.live_is_branch_of_replacement = lambda *a: False
            with self.assertRaisesRegex(KitError, 'which is not a new branch'):
                machine.s3()
        finally:
            switch.live_is_branch_of_replacement = real

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
        with self.assertRaisesRegex(KitError, 'S3 refused: the switch is at "retained attached", not "original record dropped"'):
            machine.s3()
        with self.assertRaisesRegex(KitError, 'S4 refused'):
            machine.s4()
        machine.s2b()
        self.assertEqual(machine.position(), switch.SOURCE_DROPPED)
        with self.assertRaisesRegex(KitError, 'S2b refused'):
            machine.s2b()
        machine.s3()
        self.assertEqual(machine.position(), switch.SWITCHED)
        with self.assertRaisesRegex(KitError, 'S3 refused'):
            machine.s3()
        machine.s4()
        self.assertEqual(machine.position(), switch.COMPLETE)

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
        h.fake.storage_content['1028600672070281646:7367158985892520834']['tables']['t_comp']['rows'].pop()
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
        self.assertEqual(machine.rollback(), switch.ROLLED_BACK, 'after S2b the rollback goes through the retained copy')
        src = h.fake.attachment(SRC)
        self.assertEqual((src['STORAGE_ID'], src['ATTACH_TYPE']), ('1028600672070281646:7367158985892520834', 'READWRITE'))
        self.assertEqual(h.fake.attachment(RET)['ATTACH_TYPE'], 'READONLY', 'no rollback step drops the retained copy')
        self.assertIsNotNone(h.journal.get_meta('rolled_back_utc'))
        self.assertEqual(machine.rollback(), switch.ROLLED_BACK, 'a second rollback issues nothing')
        with self.assertRaisesRegex(KitError, 'a rollback ran through it'):
            machine.forward()
        h2 = switched_harness(self)
        machine2, _ = machine_for(h2)
        machine2.s1()
        self.assertEqual(machine2.rollback(), switch.NOT_STARTED, 'R2 alone after S1')
        self.assertEqual(h2.fake.attachment(SRC)['STORAGE_ID'], '1028600672070281646:7367158985892520834')
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
        self.assertEqual(machine2.rollback(), switch.ROLLED_BACK)

    def test_a_failed_statement_reports_the_position(self):
        h = switched_harness(self)
        machine, _ = machine_for(h)
        machine.s1()
        h.fake.storage.pop('1028600672070281646:7367158985892520834')
        with self.assertRaisesRegex(KitError, 'Switch statement "S2 ATTACH retained READ ONLY" failed: .*the switch is now at "source detached"'):
            machine.s2()
        self.assertIn('switch_statement_failed', [e['kind'] for e in h.journal.events()])


class RollbackBoundaryTests(unittest.TestCase):
    """F1: S3 attaches the live name READWRITE, so a rollback from switched first proves that nothing changed."""

    def test_f1_rollback_refused_after_rows_on_live_branch(self):
        """S3 applied and its reply lost, then a row lands on the live branch: the kit and the notebook both refuse before R1."""
        h = switched_harness(self)
        machine, _ = machine_for(h)
        h.fake.fail_statement(S3_PATTERN, 'after')
        with self.assertRaisesRegex(KitError, 'S3 ATTACH replacement AS source'):
            machine.forward()
        live = dict(h.fake.attachment(SRC))
        app_write(h)
        before = list(h.fake.writes)
        machine2, _ = machine_for(h)
        self.assertEqual(machine2.position(), switch.SWITCHED)
        with self.assertRaisesRegex(KitError, 'reconcile first') as caught:
            machine2.rollback()
        self.assertIn('t_comp holds 301 rows, the copy had 300', str(caught.exception))
        self.assertNotIn('t_single', str(caught.exception), 'only the tables that differ are listed')
        self.assertEqual(h.fake.writes, before, 'nothing was issued')
        self.assertEqual(h.fake.attachment(SRC), live, 'the live name still resolves to the live branch')
        self.assertIsNone(h.journal.get_meta('rollback_started_utc'))
        self.assertEqual(len(h.journal.events('rollback_refused')), 1)
        result = run_rollback_block(h)
        self.assertTrue(result['status'].startswith('FAILED at check R1 before (zero-change proof): t_comp'), result['status'])
        self.assertEqual(result['statements'], 0)
        self.assertEqual(h.fake.writes, before, 'the notebook route refuses at the same proof')

    def test_f1_s3_fault_points_then_rollback_refused_once_the_live_branch_moved(self):
        """Before, after and before-the-journal around S3: the rerun issues S3 once, then a row on the live branch refuses the rollback."""
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = switched_harness(self)
                machine, _ = machine_for(h)
                raised = arm_s3(self, h, point)
                with self.assertRaises(raised):
                    machine.forward()
                self.assertEqual(len(s3_writes(h)), 0 if point == 'before' else 1)
                rerun, _ = machine_for(h)
                self.assertEqual(rerun.forward(), switch.COMPLETE, 'the rerun continues from the live position')
                self.assertEqual(len(s3_writes(h)), 1, 'S3 is issued exactly once')
                live_sid = h.journal.attachment('live')['storage_id']
                app_write(h)
                before = list(h.fake.writes)
                with self.assertRaisesRegex(KitError, 'reconcile first.*t_comp holds 301 rows, the copy had 300'):
                    machine_for(h)[0].rollback()
                self.assertEqual(h.fake.writes, before, 'no rollback statement was issued')
                self.assertEqual(h.fake.attachment(SRC)['STORAGE_ID'], live_sid)
                self.assertIsNone(h.journal.get_meta('rollback_started_utc'))

    def test_f1_s3_fault_points_unchanged_live_branch_rolls_back(self):
        """Before, after and before-the-journal around S3 with no write: the proof passes (or is not needed before S3) and the original is back."""
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = switched_harness(self)
                machine, _ = machine_for(h)
                raised = arm_s3(self, h, point)
                with self.assertRaises(raised):
                    machine.forward()
                if point == 'before':
                    self.assertEqual(machine_for(h)[0].position(), switch.SOURCE_DROPPED, 'nothing is attached under the live name')
                else:
                    machine_for(h)[0].forward()
                self.assertEqual(machine_for(h)[0].rollback(), switch.ROLLED_BACK)
                src = h.fake.attachment(SRC)
                self.assertEqual((src['STORAGE_ID'], src['ATTACH_TYPE']), ('1028600672070281646:7367158985892520834', 'READWRITE'))
                proofs = h.journal.events('rollback_proof')
                if point == 'before':
                    self.assertEqual(proofs, [], 'no proof is needed when S3 never ran')
                else:
                    self.assertEqual(len(proofs), 1)
                    self.assertIn('passed on 4 tables', proofs[0])
                self.assertEqual(len([w for w in h.fake.writes if w == 'DETACH DATABASE `%s`' % SRC]), 1 if point == 'before' else 2,
                                 'R1 runs only when S3 ran')

    def test_f1_rollback_refused_while_a_foreign_session_is_active_on_the_live_name(self):
        h = switched_harness(self)
        machine, _ = machine_for(h)
        machine.forward()
        h.fake.processlist = [{'ID': 9, 'USER': 'app', 'HOST': 'h', 'DB': SRC, 'COMMAND': 'Query', 'TIME': 1, 'INFO': 'INSERT INTO t VALUES (1)'}]
        before = list(h.fake.writes)
        with self.assertRaisesRegex(KitError, 'reconcile first.*1 foreign non-idle session on the live name'):
            machine.rollback()
        self.assertEqual(h.fake.writes, before)
        h.fake.processlist = []
        self.assertEqual(machine.rollback(), switch.ROLLED_BACK, 'once the session is gone the proof passes')

    def test_f1_rollback_refused_when_a_planned_table_has_no_target_count(self):
        h = switched_harness(self)
        machine, _ = machine_for(h)
        machine.forward()
        h.journal.conn.execute("UPDATE tables SET target_count = NULL WHERE table_name = 't_empty'")
        before = list(h.fake.writes)
        with self.assertRaisesRegex(KitError, 'reconcile first.*t_empty has no recorded target count'):
            machine.rollback()
        self.assertEqual(h.fake.writes, before)
        text = switch.rollback_block(ownersql.names_for(h.ctx())).render()
        self.assertIn('-- STOP: rollback refused: no target count is recorded for t_empty', text)
        self.assertLess(text.index('-- STOP'), text.index('DETACH DATABASE'))

    def test_f1_rollback_block_carries_the_zero_change_proof_before_r1(self):
        h = switched_harness(self)
        machine, _ = machine_for(h)
        machine.forward()
        text = rollback_block_for(h, retained=True).render()
        detach_at = text.index('DETACH DATABASE `%s`;' % SRC)
        for table, count in (('t_comp', 300), ('t_empty', 0), ('t_nokey', 500), ('t_single', 700)):
            check = '-- CHECK R1 before (zero-change proof): %s holds the copied row count; expect n = %d' % (table, count)
            self.assertLess(text.index(check), detach_at, table)
            self.assertIn('SELECT COUNT(*) AS n FROM `%s`.`%s`;' % (SRC, table), text)
        self.assertLess(text.index('R1 before (zero-change proof): no non-idle session on the live name other than ours'), detach_at)
        self.assertLess(text.index("-- CHECK R1 before (zero-change proof): no pipeline Running on the live name; expect n = 0\n"
                                   "SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = '%s' AND STATE = 'Running';" % SRC),
                        detach_at)
        self.assertNotIn('holds none of the write privileges', text, 'no lockout snapshot, so no privilege check')
        self.assertNotIn('-- STOP', text)
        result = run_rollback_block(h, retained=True)
        self.assertEqual(result['status'], 'passed', result)
        self.assertEqual((result['statements'], len(result['skipped'])), (2, 0), 'R1 and R2b')
        src = h.fake.attachment(SRC)
        self.assertEqual((src['STORAGE_ID'], src['ATTACH_TYPE']), ('1028600672070281646:7367158985892520834', 'READWRITE'))

    def test_f1_r1_fault_points_resume_the_rollback(self):
        """Before, after and before-the-journal around R1 DETACH, the statement the zero-change proof guards: the rerun completes with one R1.

        before: the proof runs again (the live branch is still attached). after and journal: R1
        took effect, so the rerun starts at R2 without a second proof and never issues R1 again.
        """
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = switched_harness(self)
                machine, _ = machine_for(h)
                machine.forward()
                if point == 'journal':
                    crash_journal(self, h.journal, 'event', match=lambda kind, detail: kind == 'switch_statement' and detail.startswith('R1'))
                    raised = SimulatedCrash
                else:
                    h.fake.fail_statement(r'^DETACH DATABASE `%s`$' % SRC, point)
                    raised = KitError
                with self.assertRaises(raised):
                    machine.rollback()
                self.assertIsNotNone(h.journal.get_meta('rollback_started_utc'), 'the proof passed before R1')
                self.assertEqual(machine_for(h)[0].position(), switch.COMPLETE if point == 'before' else switch.LIVE_DETACHED)
                self.assertEqual(machine_for(h)[0].rollback(), switch.ROLLED_BACK)
                self.assertEqual(len(h.journal.events('rollback_proof')), 2 if point == 'before' else 1)
                self.assertEqual(len([w for w in h.fake.writes if w == 'DETACH DATABASE `%s`' % SRC]), 2, 'S1 and one R1, never R1 twice')
                src = h.fake.attachment(SRC)
                self.assertEqual((src['STORAGE_ID'], src['ATTACH_TYPE']), ('1028600672070281646:7367158985892520834', 'READWRITE'))
                self.assertEqual(h.fake.attachment(RET)['ATTACH_TYPE'], 'READONLY', 'no rollback step drops the retained copy')
                self.assertIsNotNone(h.journal.get_meta('rolled_back_utc'))

    def test_f1_behind_s3_the_notebook_block_stops_and_the_hand_route_finishes_from_r2(self):
        """Behind S3 nothing is attached under the live name: ownerlib stops at the first proof check with nothing issued; R2 onward finishes it.

        The states come from the failure points: before S3 (retained attached), and after R1 or
        after R1 before its journal event (live branch detached).
        """
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = switched_harness(self)
                machine, _ = machine_for(h)
                if point == 'before':
                    h.fake.fail_statement(S3_PATTERN, 'before')
                    with self.assertRaises(KitError):
                        machine.forward()
                    expected = switch.SOURCE_DROPPED
                else:
                    machine.forward()
                    if point == 'journal':
                        crash_journal(self, h.journal, 'event', match=lambda kind, detail: kind == 'switch_statement' and detail.startswith('R1'))
                    else:
                        h.fake.fail_statement(r'^DETACH DATABASE `%s`$' % SRC, 'after')
                    with self.assertRaises(SimulatedCrash if point == 'journal' else KitError):
                        machine.rollback()
                    expected = switch.LIVE_DETACHED
                self.assertEqual(machine_for(h)[0].position(), expected)
                block = rollback_block_for(h, retained=True)
                self.assertIn('The notebook runs every item in order, so there the first proof check stops the block before any statement',
                              block.render())
                before = list(h.fake.writes)
                result = run_rollback_block(h, retained=True)
                self.assertNotEqual(result['status'], 'passed')
                self.assertEqual(result['statements'], 0)
                self.assertEqual(h.fake.writes, before, 'the notebook route issued nothing')
                start = next(i for i, item in enumerate(block.items) if item.label.startswith('R2b before'))
                tail = ownersql.Block('O10', 'Rollback from R2b (by hand)')
                tail.items = block.items[start:]
                with contextlib.redirect_stdout(io.StringIO()):
                    finished = ownerlib()['run_block'](h.fake.connect().cursor(), tail.to_dict(), {'run_id': h.run_id})
                self.assertEqual((finished['status'], finished['statements'], len(finished['skipped'])), ('passed', 1, 0), 'R2b')
                src = h.fake.attachment(SRC)
                self.assertEqual((src['STORAGE_ID'], src['ATTACH_TYPE']), ('1028600672070281646:7367158985892520834', 'READWRITE'))


class PipelinesAcrossTheSwitchTests(unittest.TestCase):
    def test_replayed_pipelines_follow_the_replacement_branch_and_the_retained_copy_keeps_the_originals(self):
        h = switched_harness(self)
        h.fake.pipelines = [{'PIPELINE_NAME': 'orig', 'STATE': 'Stopped', 'DATABASE_NAME': SRC},
                            {'PIPELINE_NAME': 'p1', 'STATE': 'Stopped', 'DATABASE_NAME': TGT}]
        machine, _ = machine_for(h)
        machine.counts['pipelines'] = 1
        self.assertEqual(machine.forward(), switch.COMPLETE)
        visible = sorted((p['DATABASE_NAME'], p['PIPELINE_NAME'], p['STATE']) for p in h.fake.pipelines if p.get('DATABASE_NAME'))
        self.assertIn((SRC, 'p1', 'Stopped'), visible, 'the live name carries the pipeline replayed into the replacement')
        self.assertIn((RET, 'orig', 'Stopped'), visible, 'the retained copy keeps the original pipelines, stopped')
        self.assertNotIn((SRC, 'orig', 'Stopped'), visible)


class CloseoutHandlerTests(unittest.TestCase):
    def test_c3_passes_on_two_entries_and_c4_runs_its_block(self):
        """C3 End state: two entries after the switch, the write probe on the retained name refused, the listing recorded; then C4 drops the runner."""
        h = switched_harness(self)
        machine, _ = machine_for(h)
        self.assertEqual(machine.forward(), switch.COMPLETE, h.text())
        for step_id in ('O7', 'O8', 'O9', 'O10', 'O11', 'C1', 'C2'):
            h.journal.step_end(step_id, 'passed', 'test')
        before = list(h.fake.writes)
        out = h.step('C3', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual([w for w in h.fake.writes[len(before):] if not w.startswith('DELETE FROM `%s`' % RET)], [], 'C3 issues the write probe only')
        self.assertEqual(len(h.journal.events('endstate_probe')), 1)
        self.assertEqual([d.split(':')[0] for d in h.journal.events('endstate_listing')], ['before', 'after'], 'the listing before and after the gate')
        self.assertEqual(len(h.fake.portal_entries()), 2)
        out = h.step('C4', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertTrue(any(w.startswith("DROP USER 's2rebuild_runner'") for w in h.fake.writes))

    def test_f7_c4_refused_while_a_runner_defined_view_exists(self):
        """A view in the live database whose definer is the runner stops C4 before DROP USER, on the kit and the notebook route alike."""
        from tests.fakedb import FakeView
        h = switched_harness(self)
        for step_id in ('O7', 'O8', 'O9', 'O10', 'O11', 'C1', 'C2', 'C3'):
            h.journal.step_end(step_id, 'passed', 'test')
        live = h.fake.databases[SRC]['views']
        live['v_run'] = FakeView('CREATE VIEW `%s`.`v_run` AS SELECT 1' % SRC, 's2rebuild_runner@10.0.0.%')
        block = ownersql.account_block(ownersql.names_for(h.ctx()), drop=True)
        text = block.render()
        self.assertLess(text.index('no view in the live database has the runner as its definer'), text.index('DROP USER'))
        with contextlib.redirect_stdout(io.StringIO()):
            result = ownerlib()['run_block'](h.fake.connect('ops_owner').cursor(), block.to_dict(), {'run_id': h.run_id})
        self.assertEqual(result['status'], 'FAILED at check no view in the live database has the runner as its definer', result)
        out = h.step('C4', redo=True)
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('stopped at check "no view in the live database has the runner as its definer": expected n = 0, observed n = 1', out.note)
        self.assertFalse([w for w in h.fake.writes if w.startswith('DROP USER')], 'the runner is kept while a view needs it')
        live['v_run'] = FakeView(live['v_run'], 'ops_owner@%')
        self.assertEqual(h.step('C4', redo=True).status, steps.PASSED, h.text())
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

    def test_f1_o10_fault_points_never_leave_the_step_running(self):
        """kill -9 stand-in before S3, after S3, and after S3 before its journal event: O10 ends failed, not running; the rerun passes."""
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = switched_harness(self)
                ready_for_s0(h)
                arm_s3(self, h, point, crash=True)
                with self.assertRaises(SimulatedCrash):
                    h.step('O10', yes=True)
                row = h.journal.step('O10')
                self.assertEqual(row['status'], 'failed')
                self.assertIn('SimulatedCrash', row['note'])
                self.assertNotIn('killed', row['note'], 'the exception text is never stored')
                out = h.step('O10', yes=True)
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertEqual(len(s3_writes(h)), 1, 'S3 is issued exactly once')

    def test_handler_with_yes_and_a_replacement_recorded_late(self):
        h = switched_harness(self)
        h.journal.conn.execute("DELETE FROM attachments WHERE role = 'replacement'")
        ready_for_s0(h)
        out = h.step('O10', yes=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIsNotNone(h.journal.attachment('replacement'))
        out = h.step('O10', dry_run=True)
        self.assertEqual(out.status, steps.PENDING)


class RecopyEvidenceTests(unittest.TestCase):
    def test_f4_mark_recopy_after_o8_reruns_o7_and_o8_and_s0_refuses_until_they_pass(self):
        """The reproduction: after O8 passed, mark-recopy then run --from O6 skipped O7 and O8 and S0 accepted. Now both run and S0 waits,
        and the rerun of O7 sets the AUTO_INCREMENT counters again after the re-copy (t_comp carries one in this fixture)."""
        h = verified_harness(self, mutate_inventory=auto_increment_on_t_comp, prepare=counters_settable)
        self.assertTrue(s0(h)[0].startswith('S0: O8 and O9 passed'))
        self.assertEqual(len(counter_writes(h, 't_comp')), 2, 'the first O7 set the counter')
        code, text = mark_recopy_cli(h)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('rerun O6 to O8 (s2rebuild run --from O6)', text)
        self.assertEqual({s: h.journal.step_status(s) for s in ('O6', 'O7', 'O8', 'O9')},
                         {'O6': 'pending', 'O7': 'pending', 'O8': 'pending', 'O9': 'passed'})
        with self.assertRaisesRegex(KitError, r'S0 refused: O8 has not passed; not verified on the rows now copied: '
                                              r't_single \(no verified_at_utc\); rerun O6 to O8'):
            s0(h)
        self.assertEqual(switch.unproven_tables(ownersql.table_counts(h.journal)), ['t_single'],
                         'the cleared target_count stops the rollback proof too until the copy is counted again')
        with self.assertRaisesRegex(KitError, r'Cannot run O9 .*not passed: O6, O7, O8 \(O9 passed earlier'):
            steps.run_steps(h.config, [h.db], {h.db.source: h.journal}, h.log, run_id=h.run_id, from_id='O9', to_id='O9', sessions=h.sessions)
        mark, writes = len(h.text()), len(h.fake.writes)
        self.assertEqual(run_from_o6(h, to_id='O7'), EXIT_DONE, h.text())
        first = h.text()[mark:]
        self.assertEqual([w.split(' ')[0] for w in counter_writes(h, 't_comp', writes)], ['ALTER', 'AGGREGATOR'],
                         'the rerun of O7 sets the counter and syncs it again after the re-copy')
        assert_counter_follows(self, h, 't_comp', 'mark_recopy')
        for step_id, title in (('O6', 'Copy'), ('O7', 'Post-copy')):
            self.assertIn('[%s] %s (python; runner) database %s\n' % (step_id, title, SRC), first)
            self.assertNotIn('[%s] %s (python; runner) database %s: already passed' % (step_id, title, SRC), first)
        self.assertEqual(sum(1 for w in h.fake.writes if w.startswith('INSERT INTO `%s`.`t_single`' % TGT)), 4, 'three buckets, then bucket 1 again')
        with self.assertRaisesRegex(KitError, r'^S0 refused: O8 has not passed$'):
            s0(h)
        mark = len(h.text())
        self.assertEqual(run_from_o6(h, to_id='O9'), EXIT_DONE, h.text())
        second = h.text()[mark:]
        self.assertIn('[O8] Verify (python; runner) database %s\n' % SRC, second)
        self.assertIn('[O9] Pipelines (admin; owner) database %s: already passed, skipped' % SRC, second)
        self.assertEqual(h.journal.recopy_epoch('t_single'), 1)
        self.assertTrue(s0(h)[0].startswith('S0: O8 and O9 passed; every planned table verified'))
        self.assertEqual(switch.unproven_tables(ownersql.table_counts(h.journal)), [])

    def test_f4_recopy_fault_points_refuse_the_switch_until_the_rerun_verifies(self):
        """A failure before, after, or after and before the journal update, at the DELETE and at the INSERT of the bucket mark-recopy
        marked: the retry round of the same O6 pass copies the bucket again (B3), O6 to O8 pass and O10 detaches once. A kill at the
        journal write ends the pass instead; S0 refuses until the operator's rerun from O6 passes O6 to O8."""
        for statement in ('DELETE', 'INSERT'):
            for point in FAULT_POINTS:
                with self.subTest(statement=statement, point=point):
                    h = verified_harness(self)
                    h.journal.mark_recopy('t_single', 1)
                    start = len(h.fake.writes)
                    fired = arm_recopy(self, h, 't_single', statement, point)
                    code = run_from_o6(h, to_id='O9')
                    self.assertEqual(len(fired), 1, 'the fault fired')
                    if point == 'journal':
                        # a kill at a journal write ends the pass with no retry round: S0 refuses until the rerun verifies
                        self.assertEqual(code, EXIT_INCOMPLETE, h.text())
                        with self.assertRaisesRegex(KitError, r'S0 refused: O8 has not passed; not verified on the rows now copied: t_single \('):
                            s0(h)
                        self.assertEqual(detaches(h), [], 'nothing is switched on an unverified copy')
                        self.assertEqual(run_from_o6(h, to_id='O9'), EXIT_DONE, h.text())
                    else:
                        self.assertEqual(code, EXIT_DONE, h.text())
                        self.assertEqual(len(h.journal.events('retry_round')), 1, 'the retry round copied the bucket again')
                    assert_reinserted_once(self, h, 't_single', start)
                    self.assertTrue(s0(h))
                    out = h.step('O10', yes=True, redo=False)
                    self.assertEqual(out.status, steps.PASSED, h.text())
                    self.assertEqual(len(detaches(h)), 1, 'S1 is issued once')

    def test_f4_mark_recopy_crash_part_way_marks_nothing_and_the_rerun_holds_the_switch(self):
        """kill -9 stand-in at the journal writes of mark-recopy (clearing verified_at_utc, reopening O8): the journal is as before, so the
        unchanged copy still passes S0; the rerun of mark-recopy then refuses S0 until O6 to O8 pass again."""
        for method, match in (('set_table_attr', lambda table, column, value: column == 'verified_at_utc'),
                              ('step_end', lambda step_id, status, note=None, evidence_path=None: step_id == 'O8')):
            with self.subTest(method=method):
                h = verified_harness(self)
                states = h.journal.chunk_states()
                crash_journal(self, h.journal, method, match=match)
                with self.assertRaises(SimulatedCrash):
                    h.journal.mark_recopy('t_single', 1)
                self.assertEqual(h.journal.chunk_states(), states, 'no bucket is marked')
                self.assertEqual({s: h.journal.step_status(s) for s in ('O6', 'O7', 'O8')}, {'O6': 'passed', 'O7': 'passed', 'O8': 'passed'})
                self.assertEqual(h.journal.recopy_epoch('t_single'), 0)
                self.assertTrue(s0(h), 'the copy is unchanged and still verified')
                self.assertEqual(h.journal.mark_recopy('t_single', 1), 1)
                with self.assertRaisesRegex(KitError, 'S0 refused: O8 has not passed'):
                    s0(h)
                self.assertEqual(run_from_o6(h, to_id='O9'), EXIT_DONE, h.text())
                self.assertTrue(s0(h))

    def test_f4_s0_refuses_a_table_whose_verified_at_was_cleared(self):
        """The old S0 took the oldest stamp among the stamped tables and ignored a NULL one. Now it names the table and O10 issues
        no DETACH. A refusal without a statement of its own: the guarded statement is the S1 DETACH; the three failure points
        around the re-copy statements that clear a stamp are in test_f4_recopy_fault_points_refuse_the_switch_until_the_rerun_verifies."""
        h = switched_harness(self)
        ready_for_s0(h)
        self.assertTrue(s0(h))
        h.journal.set_table_attr('t_comp', 'verified_at_utc', None)
        with self.assertRaisesRegex(KitError, r'not verified on the rows now copied: t_comp \(no verified_at_utc\); rerun'):
            s0(h)
        out = h.step('O10', yes=True)
        self.assertEqual(out.status, steps.REFUSED)
        self.assertIn('t_comp (no verified_at_utc)', out.note)
        self.assertNotIn('t_single (', out.note)
        self.assertEqual(detaches(h), [])

    def test_f4_s0_refuses_a_stamp_before_the_last_chunk_an_earlier_epoch_or_an_open_chunk(self):
        """Each table is checked on its own: a stamp older than its newest chunk, a stamp without a match at the current re-copy
        epoch, and a chunk not done each refuse S0 and name the table."""
        h = switched_harness(self)
        ready_for_s0(h)
        h.journal.conn.execute("UPDATE chunks SET ended_utc = '2026-11-01T07:00:00Z' WHERE table_name = 't_single' AND bucket = 2")
        with self.assertRaisesRegex(KitError, r't_single \(verified 2026-11-01T06:00:00Z, before its last chunk ended 2026-11-01T07:00:00Z\)'):
            s0(h)
        h.journal.conn.execute("UPDATE chunks SET ended_utc = '2026-10-01T00:00:00Z' WHERE table_name = 't_single' AND bucket = 2")
        self.assertTrue(s0(h))
        h.journal.conn.execute("UPDATE tables SET recopy_epoch = 1 WHERE table_name = 't_nokey'")
        with self.assertRaisesRegex(KitError, r't_nokey \(no matching verification at re-copy epoch 1\)'):
            s0(h)
        h.journal.record_verification('t_nokey', 'count', 'match', 500, 500)
        h.journal.set_table_attr('t_nokey', 'verified_at_utc', '2026-11-01T06:00:00Z')
        self.assertTrue(s0(h), 'a match at the current epoch counts')
        h.journal.conn.execute("UPDATE chunks SET state = 'started' WHERE table_name = 't_empty'")
        with self.assertRaisesRegex(KitError, r't_empty \(1 chunk not done\)'):
            s0(h)


    def assert_the_switch_holds_the_evidence(self, h, before):
        """mark-recopy and a rerun of O8 refuse with nothing written; the evidence the switch stands on is as it was."""
        code, text = mark_recopy_cli(h)
        self.assertEqual(code, EXIT_REFUSED, text)
        self.assertIn('mark-recopy refused for t_single: the switch may have begun (', text)
        self.assertIn('nothing was marked', text)
        with self.assertRaisesRegex(KitError, 'Cannot run O8 for database %s: the switch may have begun' % SRC):
            h.step('O8', redo=True)
        with self.assertRaisesRegex(KitError, 'mark-recopy refused'):
            h.journal.mark_recopy('t_comp')
        self.assertEqual(copy_evidence(h), before, 'nothing marked, reopened or cleared')

    def test_f4_mark_recopy_and_o6_to_o8_refused_once_the_switch_began(self):
        """A failure before, after, or after and before the journal event, at the S1 DETACH, the statement from which the copy is what
        the switch stands on: mark-recopy and a rerun of O8 refuse with nothing written, there (mid-switch) and once O10 passed.
        target_count, the step statuses and the rollback proof are unchanged, and O11 is not stranded behind a reopened O8."""
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = verified_harness(self)
                before = copy_evidence(h)
                self.assertIsNone(h.journal.switch_began())
                if point == 'journal':
                    crash_journal(self, h.journal, 'event', match=lambda kind, detail: kind == 'switch_statement' and detail.startswith('S1'))
                    with self.assertRaises(SimulatedCrash):
                        h.step('O10', yes=True)
                else:
                    h.fake.fail_statement(r'^DETACH DATABASE `%s`$' % SRC, point)
                    self.assertEqual(h.step('O10', yes=True).status, steps.REFUSED, h.text())
                self.assertEqual(len(detaches(h)), 0 if point == 'before' else 1)
                self.assert_the_switch_holds_the_evidence(h, before)
                out = h.step('O10', yes=True)
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertEqual(len(detaches(h)), 1, 'S1 is applied once')
                self.assertEqual(h.journal.switch_began(), 'O10 is passed')
                self.assert_the_switch_holds_the_evidence(h, before)
                self.assertEqual(run_from_o6(h, to_id='O9'), EXIT_DONE, 'a run over the passed steps skips them')
                self.assertEqual(steps.unmet_preconditions(steps.REGISTRY['O11'], h.journal), [], 'O11 is not stranded')
                machine, _ = machine_for(h)
                machine.zero_change_proof()
                self.assertEqual(h.journal.events('rollback_proof')[-1].split(' ')[:5], ['zero-change', 'proof', 'passed', 'on', '4'])

    def test_f4_print_owner_sql_notebook_and_gate_refuse_a_table_whose_verified_at_was_cleared(self):
        """The journal part of S0 holds on every route: print mode refuses O10, owner-sql O10 and the notebook start the forward block
        with a STOP naming the table, the notebook run of that block issues nothing, and gate O10 --pass records nothing. Once O8
        verifies again, every route is open. A refusal without a statement of its own: the guarded statement is the S1 DETACH, never
        issued here; the three failure points at the S1 DETACH are in test_f4_mark_recopy_and_o6_to_o8_refused_once_the_switch_began."""
        stop = '-- STOP: S0 refused: not verified on the rows now copied: t_comp (no verified_at_utc); rerun O8 (s2rebuild step O8 --redo)'
        h = verified_harness(self, owner=False)
        base = ['--config', str(h.config_path), '--run', h.run_id]
        block_file = h.tmp / 'runs' / h.run_id / SRC / 'owner-sql' / 'O10.sql'
        self.assertEqual(h.step('O10', yes=True).status, steps.PENDING, h.text())
        self.assertNotIn('-- STOP', block_file.read_text())
        h.journal.set_table_attr('t_comp', 'verified_at_utc', None)
        out = h.step('O10', yes=True)
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('t_comp (no verified_at_utc)', out.note)
        self.assertEqual(h.journal.step_status('O10'), 'pending')
        text = block_file.read_text()
        self.assertIn(stop, text)
        self.assertLess(text.index('-- STOP'), text.index('DETACH DATABASE'))
        code, text = run_cli(['owner-sql', 'O10'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn(stop, text)
        self.assertLess(text.index('-- STOP'), text.index('DETACH DATABASE'))
        for extra in (['--dry-run'], []):
            code, text = run_cli(['gate', 'O10', '--pass', '--note', 'switched by hand'] + extra + base, env=h.env)
            self.assertEqual(code, EXIT_REFUSED, text)
            self.assertIn('gate O10 --pass refused for %s: S0 refused: not verified on the rows now copied: t_comp (no verified_at_utc)' % SRC, text)
        self.assertEqual(h.journal.step_status('O10'), 'pending', 'the refused gate records nothing')
        self.assertNotIn('O10 pass', ' '.join(h.journal.events('gate')))
        self.assertEqual(h.step('O8').status, steps.PASSED, h.text())
        self.assertEqual(h.step('O10', yes=True).status, steps.PENDING, h.text())
        self.assertNotIn('-- STOP', block_file.read_text())
        code, text = run_cli(['gate', 'O10', '--pass', '--note', 'switched by hand'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertEqual(h.journal.step_status('O10'), 'passed')

        h = verified_harness(self)
        h.journal.set_table_attr('t_comp', 'verified_at_utc', None)
        code, text = run_cli(['owner-notebook', '--config', str(h.config_path), '--run', h.run_id], env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('S0 refused: not verified on the rows now copied: t_comp (no verified_at_utc)',
                      (h.tmp / 'runs' / h.run_id / 'owner-steps.ipynb').read_text())
        ctx = h.ctx()
        block = ownersql.block_for('O10', ownersql.names_for(ctx), h.config, **ownersql.block_inputs(ctx))
        with contextlib.redirect_stdout(io.StringIO()):
            result = ownerlib()['run_block'](h.fake.connect('ops_owner').cursor(), block.to_dict(), {'run_id': h.run_id})
        self.assertTrue(result['status'].startswith('REFUSED: S0 refused: not verified on the rows now copied: t_comp'), result['status'])
        self.assertEqual((result['statements'], result['checks'], detaches(h)), (0, [], []), 'the notebook issues nothing')

    def test_f4_fresh_table_after_o8_reruns_o7_and_o8_and_sets_the_counter_again(self):
        """A fresh-table load after O8 replaces the whole table: it reopens O7 and O8 and clears the table's verification, S0 refuses
        with the run --from O6 hint, and the rerun sets the counter of the new table, which CREATE TABLE LIKE left at the default."""
        h = verified_harness(self, mutate_inventory=auto_increment_on_t_comp, prepare=counters_settable)
        self.assertIsNotNone(h.fake.databases[TGT]['tables']['t_comp']['auto_increment'], 'O7 set the counter')
        out = h.step('O6', options={'fresh_table': 't_comp'})
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIsNone(h.fake.databases[TGT]['tables']['t_comp']['auto_increment'], 'the new table starts at the default counter')
        self.assertEqual({s: h.journal.step_status(s) for s in ('O6', 'O7', 'O8', 'O9')},
                         {'O6': 'passed', 'O7': 'pending', 'O8': 'pending', 'O9': 'passed'})
        self.assertIn('t_comp', h.journal.step('O8')['note'])
        self.assertEqual(h.journal.recopy_epoch('t_comp'), 1)
        with self.assertRaisesRegex(KitError, r'S0 refused: O8 has not passed; not verified on the rows now copied: t_comp \(no verified_at_utc\); '
                                              r'rerun O6 to O8 \(s2rebuild run --from O6\)'):
            s0(h)
        writes = len(h.fake.writes)
        self.assertEqual(run_from_o6(h, to_id='O9'), EXIT_DONE, h.text())
        for step_id, title in (('O7', 'Post-copy'), ('O8', 'Verify')):
            self.assertIn('[%s] %s (python; runner) database %s\n' % (step_id, title, SRC), h.text())
        self.assertEqual(len(counter_writes(h, 't_comp', writes)), 2, 'ALTER TABLE AUTO_INCREMENT and AGGREGATOR SYNC on the new table')
        assert_counter_follows(self, h, 't_comp', 'evidence_reopened')
        self.assertTrue(s0(h))

    def test_f4_fresh_table_fault_points_hold_the_switch_until_the_rerun_verifies(self):
        """A failure before, after, or after and before the journal update, at the RENAME that swaps the fresh table in after O8, and a
        kill -9 stand-in inside the journal write that reopens the evidence: S0 or the precondition check refuses O10 with no DETACH;
        the operator's rerun of step O6 --fresh-table and run --from O6 load the rows once, set the counter and pass O6 to O8."""
        for point in FAULT_POINTS + ('reopen',):
            with self.subTest(point=point):
                h = verified_harness(self, mutate_inventory=auto_increment_on_t_comp)
                h.fake.refuse_sharded_counter = False  # the ALTER path of the counter logic: Helios 10.8.0 refuses it on a sharded table (1749, covered by its own test)
                if point in ('journal', 'reopen'):
                    if point == 'journal':
                        crash_journal(self, h.journal, 'mark_started', match=lambda table, bucket: table == 't_comp')
                    else:
                        crash_journal(self, h.journal, 'step_end', match=lambda step_id, status, note=None, evidence_path=None: step_id == 'O8')
                    with self.assertRaises(SimulatedCrash):
                        h.step('O6', options={'fresh_table': 't_comp'})
                else:
                    h.fake.fail_statement(FRESH_RENAME, point)
                    out = h.step('O6', options={'fresh_table': 't_comp'})
                    self.assertEqual(out.status, steps.INCOMPLETE, h.text())
                self.assertNotEqual(h.journal.step_status('O6'), 'passed')
                if point == 'reopen':
                    self.assertFalse([w for w in h.fake.writes if 't_comp__fresh' in w], 'the journal comes first: no statement ran')
                    self.assertEqual({s: h.journal.step_status(s) for s in ('O7', 'O8')}, {'O7': 'passed', 'O8': 'passed'},
                                     'the crashed transaction reopened nothing')
                else:
                    self.assertEqual({s: h.journal.step_status(s) for s in ('O7', 'O8')}, {'O7': 'pending', 'O8': 'pending'})
                    with self.assertRaisesRegex(KitError, r'S0 refused: O8 has not passed; not verified on the rows now copied: t_comp \('):
                        s0(h)
                with self.assertRaisesRegex(KitError, r'Cannot run O10 .*not passed: O6'):
                    h.step('O10', yes=True, redo=False)
                self.assertEqual(detaches(h), [], 'nothing is switched on an unverified copy')
                self.assertEqual(h.step('O6', options={'fresh_table': 't_comp'}).status, steps.PASSED, h.text())
                self.assertEqual(run_from_o6(h, to_id='O9'), EXIT_DONE, h.text())
                self.assertEqual({s: h.journal.step_status(s) for s in ('O6', 'O7', 'O8')}, {'O6': 'passed', 'O7': 'passed', 'O8': 'passed'})
                self.assertEqual(multiset(h.fake.rows(TGT, 't_comp')), multiset(h.fake.rows(SRC, 't_comp')), 'no row lost or doubled')
                self.assertNotIn('t_comp__fresh', h.fake.databases[TGT]['tables'])
                assert_counter_follows(self, h, 't_comp', 'evidence_reopened')
                self.assertTrue(s0(h))
                self.assertEqual(h.step('O10', yes=True, redo=False).status, steps.PASSED, h.text())
                self.assertEqual(len(detaches(h)), 1)

    def test_f4_s0_hint_names_the_steps_that_verify_again(self):
        """The S0 refusal points at what passes it: step O8 --redo only when O7 passed after the newest chunk, run --from O6 while O7 is
        reopened, and each step with --redo when O7 passed before the copy last changed. A computation without a statement of its own;
        the failure points of the re-copy statements behind each state are in the fault-point tests of this class."""
        h = verified_harness(self)
        h.journal.set_table_attr('t_comp', 'verified_at_utc', None)
        self.assertEqual(switch.reverify_hint(h.journal),
                         'rerun O8 (s2rebuild step O8 --redo): O7 passed after the last chunk, so only the verification is missing')
        h.journal.conn.execute("UPDATE chunks SET ended_utc = '2099-01-01T00:00:00Z' WHERE table_name = 't_comp'")
        self.assertIn('rerun O6, O7 and O8 each with --redo', switch.reverify_hint(h.journal))
        self.assertIn('O7 passed before the copy last changed', switch.reverify_hint(h.journal))
        h.journal.step_end('O7', 'pending', 'reopened for the test')
        self.assertEqual(switch.reverify_hint(h.journal), 'rerun O6 to O8 (s2rebuild run --from O6)')
        h.journal.conn.execute("UPDATE chunks SET state = 'failed' WHERE table_name = 't_comp'")
        self.assertIn('O6 passed with a chunk not done', switch.reverify_hint(h.journal))
        h.journal.step_end('O6', 'pending', 'reopened for the test')
        self.assertEqual(switch.reverify_hint(h.journal), 'rerun O6 to O8 (s2rebuild run --from O6)')

    def test_f4_recopy_marks_rewrite_the_o10_blocks_with_a_stop_and_name_the_stale_notebook(self):
        """A forward block written after O8 passed kept no STOP after a re-copy, so the hand and notebook routes could switch onto rows
        never verified again. Every way the copy evidence is reopened (mark-recopy, an O8 mismatch, the O7 stale check, a fresh-table
        load) now rewrites owner-sql/O10.sql and O10-rollback.sql with a STOP ahead of every statement, before its journal write, and
        says a notebook generated before must not run its O10 cells. After O8 passes again, owner-sql O10 writes them without a STOP.
        The failure points around that journal write are in test_f4_o10_block_rewrite_fault_points_never_leave_a_block_without_a_stop."""
        for site in ('mark-recopy', 'O8 mismatch', 'O7 stale check', 'fresh-table load'):
            with self.subTest(site=site):
                h = verified_harness(self, owner=False)
                base = ['--config', str(h.config_path), '--run', h.run_id]
                forward, rollback = o10_files(h)
                self.assertEqual(h.step('O10', yes=True).status, steps.PENDING, h.text())
                code, text = run_cli(['owner-notebook'] + base, env=h.env)
                self.assertEqual(code, EXIT_DONE, text)
                self.assertNotIn('-- STOP', forward.read_text() + rollback.read_text())
                mark = len(h.text())
                if site == 'mark-recopy':
                    code, text = mark_recopy_cli(h)
                    self.assertEqual(code, EXIT_DONE, text)
                    self.assertIn('generate the O10 blocks (s2rebuild owner-sql O10, and --rollback) or the notebook', text)
                    table, reason = 't_single', 'mark-recopy t_single bucket 1 reopens its copy evidence'
                elif site == 'O8 mismatch':
                    h.fake.rows(TGT, 't_single')[0]['payload'] = 'tampered'
                    self.assertEqual(h.step('O8').status, steps.MISMATCH, h.text())
                    table, reason = 't_single', 'O8 found a mismatch and marks t_single for re-copy'
                elif site == 'O7 stale check':
                    h.fake.rows(SRC, 't_comp').append(dict(LATE_ROW))
                    self.assertEqual(h.step('O7').status, steps.PASSED, h.text())
                    table, reason = 't_comp', 'the O7 stale check re-copies t_comp'
                else:
                    self.assertEqual(h.step('O6', options={'fresh_table': 't_comp'}).status, steps.PASSED, h.text())
                    table, reason = 't_comp', 'a fresh-table load replaces t_comp'
                if site != 'mark-recopy':
                    text = h.text()[mark:]
                body = forward.read_text()
                self.assertIn('-- STOP: S0 refused: ', body)
                self.assertIn(reason, body)
                self.assertLess(body.index('-- STOP'), body.index('DETACH DATABASE'))
                body = rollback.read_text()
                self.assertIn('-- STOP: rollback refused: no target count is recorded for %s' % table, body)
                self.assertLess(body.index('-- STOP'), body.index('DETACH DATABASE'))
                self.assertIn('O10 forward block %s rewritten with a STOP' % forward, text)
                self.assertIn('owner notebook: a notebook generated before now must not run its O10 cells, forward or rollback (%s)'
                              % (h.tmp / 'runs' / h.run_id / 'owner-steps.ipynb'), text)
                self.assertTrue(switch.s0_journal_problems(h.journal), 'the journal refuses the switch as well')
                self.assertEqual(run_from_o6(h, to_id='O9'), EXIT_DONE, h.text())
                self.assertEqual(switch.s0_journal_problems(h.journal), [])
                self.assertIn('-- STOP', forward.read_text(), 'the file stays stopped until it is generated again')
                for extra in ([], ['--rollback']):
                    code, text = run_cli(['owner-sql', 'O10'] + extra + base, env=h.env)
                    self.assertEqual(code, EXIT_DONE, text)
                    self.assertNotIn('-- STOP', text)
                self.assertNotIn('-- STOP', forward.read_text() + rollback.read_text())

    def test_f4_o10_block_rewrite_fault_points_never_leave_a_block_without_a_stop(self):
        """A kill -9 stand-in immediately before the mark-recopy journal write (at the rewrite of O10.sql), inside it (before the journal
        update that reopens O8), and immediately after it: the journal is either unchanged on a copy that is still verified, or reopened
        with both block files stopped; never reopened beside a block that can run. The operator's rerun of mark-recopy and run --from O6
        then pass O6 to O8, and owner-sql O10 writes both blocks again without a STOP."""
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = verified_harness(self, owner=False)
                base = ['--config', str(h.config_path), '--run', h.run_id]
                forward, rollback = o10_files(h)
                self.assertEqual(h.step('O10', yes=True).status, steps.PENDING, h.text())
                original, before = forward.read_text(), copy_evidence(h)
                write_block = ownersql.write_block

                def crash_at_the_forward_block(block, path):
                    if Path(path).name == 'O10.sql':
                        raise SimulatedCrash('killed before the journal write')
                    return write_block(block, path)
                with contextlib.ExitStack() as stack:
                    if point == 'before':
                        stack.enter_context(mock.patch.object(ownersql, 'write_block', crash_at_the_forward_block))
                    elif point == 'journal':
                        crash_journals(self, 'step_end', match=lambda step_id, status, note=None, evidence_path=None: (step_id, status) == ('O8', 'pending'))
                    else:
                        crash_journals(self, 'mark_recopy', after=True)
                    with self.assertRaises(SimulatedCrash):
                        mark_recopy_cli(h)
                if point == 'after':
                    self.assertEqual({s: h.journal.step_status(s) for s in ('O6', 'O7', 'O8')}, {'O6': 'pending', 'O7': 'pending', 'O8': 'pending'})
                    self.assertIn('-- STOP', forward.read_text())
                    self.assertIn('-- STOP: rollback refused: no target count is recorded for t_single', rollback.read_text())
                else:
                    self.assertEqual(copy_evidence(h), before, 'the journal is as it was')
                    self.assertTrue(s0(h), 'the copy is unchanged and still verified')
                    if point == 'before':
                        self.assertEqual(forward.read_text(), original, 'nothing was written')
                    else:
                        self.assertIn('-- STOP', forward.read_text(), 'a conservative STOP on an unchanged copy; owner-sql O10 lifts it')
                code, text = mark_recopy_cli(h)
                self.assertEqual(code, EXIT_DONE, text)
                self.assertIn('-- STOP', forward.read_text())
                self.assertIn('-- STOP', rollback.read_text())
                self.assertEqual(run_from_o6(h, to_id='O9'), EXIT_DONE, h.text())
                self.assertTrue(s0(h))
                for extra in ([], ['--rollback']):
                    code, text = run_cli(['owner-sql', 'O10'] + extra + base, env=h.env)
                    self.assertEqual(code, EXIT_DONE, text)
                self.assertNotIn('-- STOP', forward.read_text() + rollback.read_text())

    def test_f4_o10_block_that_cannot_be_built_again_is_replaced_by_a_stop_alone(self):
        """When the blocks cannot be built at the time of a re-copy mark (here the capture cannot be read), each O10 block file is
        replaced by a STOP alone, so no statement of it can run; a dry run rewrites nothing. A computation guarding the mark-recopy
        journal write; its failure points are in test_f4_o10_block_rewrite_fault_points_never_leave_a_block_without_a_stop."""
        h = verified_harness(self, owner=False)
        forward, rollback = o10_files(h)
        self.assertEqual(h.step('O10', yes=True).status, steps.PENDING, h.text())
        original = forward.read_text()
        self.assertEqual(ownersql.retire_o10_blocks(h.ctx(dry_run=True), ['t_single'], 'a re-copy'), [])
        self.assertEqual(forward.read_text(), original, 'a dry run writes nothing')
        with mock.patch.object(ownersql, 'block_inputs', side_effect=KitError('the capture cannot be read')):
            lines = ownersql.retire_o10_blocks(h.ctx(), ['t_single'], 'a re-copy of t_single')
        for path, flag in ((forward, ''), (rollback, ' --rollback')):
            body = path.read_text()
            self.assertIn('-- STOP: a re-copy of t_single; this block was written before that and cannot be built again now; generate '
                          'it again with s2rebuild owner-sql O10%s' % flag, body)
            self.assertNotIn('DETACH', body)
            self.assertNotIn('ATTACH', body)
        self.assertEqual(len(lines), 3, lines)
        self.assertTrue(lines[2].startswith('owner notebook: a notebook generated before now must not run its O10 cells'),
                        'S0 passed until now, so a notebook generated then holds no STOP')

    def test_f4_refused_gate_records_the_switch_a_stale_notebook_ran(self):
        """A notebook generated after O8 passed and run after a re-copy switches onto rows not verified again. gate O10 --pass with
        its result is refused and the pass is not recorded, but the attachments it observed and a switch_observed event are, so the
        journal knows the switch ran: mark-recopy and O6 are refused from then on, and the message names the rollback block. The
        failure points at the journal writes of that import are in test_f4_refused_gate_import_fault_points_record_the_switch_on_the_rerun."""
        h = verified_harness(self, owner=False)
        base = ['--config', str(h.config_path), '--run', h.run_id]
        evidence = stale_notebook_switch(self, h)
        self.assertIsNone(h.journal.switch_began(), 'until the evidence is recorded the journal cannot know')
        code, text = run_cli(['gate', 'O10', '--pass', '--note', 'notebook switch', '--evidence', str(evidence)] + base, env=h.env)
        self.assertEqual(code, EXIT_REFUSED, text)
        self.assertIn('gate O10 --pass refused for %s: S0 refused: O8 has not passed' % SRC, text)
        self.assertIn('The pass was not recorded. The evidence shows the switch ran on %s' % SRC, text)
        self.assertIn('s2rebuild owner-sql O10 --rollback', text)
        self.assertNotEqual(h.journal.step_status('O10'), 'passed')
        self.assertFalse([d for d in h.journal.events('gate') if d.startswith('O10')], 'no gate is recorded')
        self.assertEqual(h.journal.attachment('live')['storage_id'], h.fake.attachment(SRC)['STORAGE_ID'])
        self.assertEqual(h.journal.attachment('retained')['storage_id'], h.fake.attachment(RET)['STORAGE_ID'])
        self.assertTrue(h.journal.switch_began().startswith('a switch was observed: gate O10 --pass evidence (refused): '), h.journal.switch_began())
        code, text = mark_recopy_cli(h, 't_comp', '0')
        self.assertEqual(code, EXIT_REFUSED, text)
        self.assertIn('the switch may have begun (a switch was observed', text)
        with self.assertRaisesRegex(KitError, 'Cannot run O6 for database %s: the switch may have begun' % SRC):
            run_from_o6(h, to_id='O9')
        code, text = run_cli(['owner-sql', 'O10', '--rollback'] + base, env=h.env)
        self.assertIn('-- STOP: rollback refused: no target count is recorded for t_single', text,
                      'the re-copy cleared the count the zero-change proof needs: reconcile first')

    def test_f4_refused_gate_import_fault_points_record_the_switch_on_the_rerun(self):
        """A kill -9 stand-in at each journal write of the refused gate's import: before the first (the retained attachment), between
        them (the live attachment), and after both, before the switch_observed event. The pass is never recorded; the operator's rerun
        of the gate is refused again and records the switch, so mark-recopy is refused from then on."""
        writes = (('retained attachment', 'record_attachment', lambda role, *a, **k: role == 'retained', None),
                  ('live attachment', 'record_attachment', lambda role, *a, **k: role == 'live', 'the retained attachment is recorded'),
                  ('switch_observed event', 'event', lambda kind, detail: kind == 'switch_observed', 'the retained attachment is recorded'))
        for write, method, match, began in writes:
            with self.subTest(write=write):
                h = verified_harness(self, owner=False)
                gate = ['gate', 'O10', '--pass', '--note', 'notebook switch', '--evidence', str(stale_notebook_switch(self, h)),
                        '--config', str(h.config_path), '--run', h.run_id]
                fired = crash_journals(self, method, match=match)
                with self.assertRaises(SimulatedCrash):
                    run_cli(gate, env=h.env)
                self.assertEqual(len(fired), 1)
                self.assertNotEqual(h.journal.step_status('O10'), 'passed')
                self.assertEqual(h.journal.switch_began(), began)
                code, text = run_cli(gate, env=h.env)
                self.assertEqual(code, EXIT_REFUSED, text)
                self.assertIn('The evidence shows the switch ran on %s' % SRC, text)
                self.assertNotEqual(h.journal.step_status('O10'), 'passed')
                self.assertTrue(h.journal.switch_began().startswith('a switch was observed'))
                code, text = mark_recopy_cli(h, 't_comp', '0')
                self.assertEqual(code, EXIT_REFUSED, text)

    def test_f4_s0_journal_part_refuses_a_passed_o8_behind_an_o7_that_stopped(self):
        """The journal part of S0 checked that O8 passed but not the steps before it. A failure before, after, or after and before the
        journal event, at the counter ALTER of a rerun of O7 after O8 passed leaves O7 not passed and O8 passed: owner-sql O10 now starts
        with a STOP naming O7, gate O10 --pass refuses, and the print route refuses O10. The operator's rerun of O7 opens every route."""
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = verified_harness(self, mutate_inventory=auto_increment_on_t_comp, owner=False)
                h.fake.refuse_sharded_counter = False  # the ALTER path of the counter logic: Helios 10.8.0 refuses it on a sharded table (1749, covered by its own test)
                base = ['--config', str(h.config_path), '--run', h.run_id]
                if point == 'journal':
                    fired = crash_journal(self, h.journal, 'event', match=lambda kind, detail: kind == 'counter')
                else:
                    h.fake.fail_statement(r'^ALTER TABLE `%s`\.`t_comp` AUTO_INCREMENT' % TGT, point, crash=True)
                    fired = h.fake.faults_fired
                with self.assertRaises(SimulatedCrash):
                    h.step('O7')
                self.assertEqual(len(fired), 1, 'the fault fired')
                self.assertEqual((h.journal.step_status('O7'), h.journal.step_status('O8')), ('failed', 'passed'))
                problem = 'O8 passed, but steps before it are not passed: O7; run O7 with s2rebuild run --from O7'
                self.assertEqual(switch.s0_journal_problems(h.journal), [problem])
                code, text = run_cli(['owner-sql', 'O10'] + base, env=h.env)
                self.assertIn('-- STOP: S0 refused: ' + problem, text)
                self.assertLess(text.index('-- STOP'), text.index('DETACH DATABASE'))
                code, text = run_cli(['gate', 'O10', '--pass', '--note', 'switched by hand'] + base, env=h.env)
                self.assertEqual(code, EXIT_REFUSED, text)
                self.assertIn(problem, text)
                with self.assertRaisesRegex(KitError, r'Cannot run O10 .*not passed: O7'):
                    h.step('O10', yes=True)
                self.assertEqual(detaches(h), [])
                self.assertEqual(h.step('O7', redo=False).status, steps.PASSED, h.text())
                self.assertEqual(switch.s0_journal_problems(h.journal), [])
                code, text = run_cli(['owner-sql', 'O10'] + base, env=h.env)
                self.assertNotIn('-- STOP', text)
                code, text = run_cli(['gate', 'O10', '--pass', '--note', 'switched by hand'] + base, env=h.env)
                self.assertEqual(code, EXIT_DONE, text)

    def test_f4_o10_stopped_before_the_go_leaves_the_recopy_open(self):
        """The kit route records the go decision before its first statement. An O10 that stops before it (a Ctrl-C at the typed-name
        prompt, a kill -9 stand-in as the owner connection opens, or at the journal write of the go itself) issued nothing, so it
        no longer blocks a re-copy: mark-recopy and run --from O6 pass and O10 then detaches once. A failure before the S1 DETACH,
        after the go, still refuses the re-copy, and so does a Ctrl-C at the prompt once O10 read a switch under way (a hand S1)."""
        def interrupt(prompt):
            raise KeyboardInterrupt
        for case in ('prompt', 'owner connect', 'go journal', 'S1 before', 'hand S1'):
            with self.subTest(case=case):
                h = verified_harness(self)
                if case == 'hand S1':
                    h.fake.connect('admin').cursor().execute('DETACH DATABASE `%s`' % SRC)
                    with self.assertRaises(KeyboardInterrupt):
                        h.step('O10', yes=False, options={'confirm': interrupt})
                elif case == 'prompt':
                    with self.assertRaises(KeyboardInterrupt):
                        h.step('O10', yes=False, options={'confirm': interrupt})
                elif case == 'owner connect':
                    h.fake.fail_connect('ops_owner', crash=True)
                    with self.assertRaises(SimulatedCrash):
                        h.step('O10', yes=True)
                elif case == 'go journal':
                    crash_journal(self, h.journal, 'event', match=lambda kind, detail: kind == 'switch_go')
                    with self.assertRaises(SimulatedCrash):
                        h.step('O10', yes=True)
                else:
                    h.fake.fail_statement(r'^DETACH DATABASE `%s`$' % SRC, 'before')
                    self.assertEqual(h.step('O10', yes=True).status, steps.REFUSED, h.text())
                self.assertEqual(len(detaches(h)), 1 if case == 'hand S1' else 0, 'the kit issued nothing')
                if case in ('S1 before', 'hand S1'):
                    began = h.journal.switch_began()
                    self.assertTrue(began.startswith('the go decision was recorded' if case == 'S1 before' else
                                                     'a switch was observed: O10 read the position "source detached"'), began)
                    code, text = mark_recopy_cli(h)
                    self.assertEqual(code, EXIT_REFUSED, text)
                else:
                    self.assertEqual(h.journal.step_status('O10'), 'failed')
                    self.assertIsNone(h.journal.switch_began())
                    code, text = mark_recopy_cli(h)
                    self.assertEqual(code, EXIT_DONE, text)
                    self.assertEqual(run_from_o6(h, to_id='O9'), EXIT_DONE, h.text())
                    self.assertTrue(s0(h))
                self.assertEqual(h.step('O10', yes=True, redo=False).status, steps.PASSED, h.text())
                self.assertEqual(len(detaches(h)), 1, 'S1 is applied once')

    def test_f4_go_refused_after_a_recopy_while_o10_waits_at_the_prompt(self):
        """O10 ran S0 before the typed-name prompt and switched after it with no second look, so a re-copy started in another terminal
        while the operator typed was switched on. O10 now holds the copy lock from S0 through the forward machine and records the go in
        one journal transaction that reads S0's journal part again. Each case acts from inside the prompt, as a second terminal would:
        a mark that reaches the journal (as O8's mismatch mark does) and then run --from O6, whose copy is refused by the lock with no
        DELETE or INSERT, so the go is refused with no DETACH and the rerun after O6 to O8 pass detaches once; mark-recopy, refused by
        the lock with nothing written, so the switch runs on the unchanged copy; an O7 whose stale check finds a late row, refused by
        the lock before it marks anything, so the go is refused until O7 passes again; and O8 started again, which refuses the go
        until it passes. The failure points at the go write are in test_f4_go_write_fault_points_hold_the_recopy_rule_on_the_rerun."""
        for case in ('mark and run --from O6', 'mark-recopy', 'O7 re-copy round', 'O8 running'):
            with self.subTest(case=case):
                h = verified_harness(self)
                before = copy_evidence(h)
                seen = {}

                def confirm(prompt, h=h, case=case, seen=seen):
                    writes = len(h.fake.writes)
                    if case == 'mark and run --from O6':
                        seen['marked'] = h.journal.mark_recopy('t_single', 1)
                        seen['code'] = run_from_o6(h, to_id='O6')
                    elif case == 'mark-recopy':
                        seen['code'], seen['text'] = mark_recopy_cli(h)
                    elif case == 'O7 re-copy round':
                        h.fake.rows(SRC, 't_comp').append(dict(LATE_ROW))
                        seen['o7'] = h.step('O7', redo=True)
                        seen['after'] = copy_evidence(h)
                    else:
                        h.journal.step_start('O8')
                    seen['issued'] = h.fake.writes[writes:]
                    return SRC
                out = h.step('O10', yes=False, options={'confirm': confirm})
                self.assertEqual(seen['issued'], [], 'nothing was issued from the second terminal while O10 waited')
                lock = 'holds the copy lock for this journal (pid %d, running O10)' % os.getpid()
                if case == 'mark-recopy':
                    self.assertEqual(seen['code'], EXIT_REFUSED, seen['text'])
                    self.assertIn(lock + '; wait for it to finish, then run mark-recopy again', seen['text'])
                    self.assertEqual(out.status, steps.PASSED, h.text())
                    self.assertEqual(len(detaches(h)), 1)
                    self.assertEqual(copy_evidence(h), before, 'nothing was marked, so the switch stands on the copy O8 verified')
                    continue
                if case == 'mark and run --from O6':
                    self.assertEqual((seen['marked'], seen['code']), (1, EXIT_REFUSED))
                    self.assertIn(lock + '; wait for it to finish, then run O6 again', h.text())
                    self.assertIn('t_single (no verified_at_utc)', out.note)
                elif case == 'O7 re-copy round':
                    self.assertEqual(seen['o7'].status, steps.REFUSED, h.text())
                    self.assertIn(lock + '; wait for it to finish, then run O7 again', seen['o7'].note)
                    self.assertEqual(seen['after'][0], before[0], 'no bucket was marked')
                    self.assertEqual(([t[2] for t in seen['after'][3]], seen['after'][2]['O8']), ([t[2] for t in before[3]], 'passed'),
                                     'no epoch moved and no mark reopened O8; the mismatch itself cleared the stamp of t_comp')
                    self.assertIn('O8 passed, but steps before it are not passed: O7', out.note)
                else:
                    self.assertIn('O8 is running (another process started it after S0)', out.note)
                    h.journal.step_end('O8', 'passed', 'the O8 of the second terminal passed on the same copy')
                self.assertEqual(out.status, steps.REFUSED, h.text())
                self.assertTrue(out.note.startswith('S0 refused at the go decision: '), out.note)
                self.assertIn('nothing was issued', out.note)
                self.assertEqual(detaches(h), [], 'nothing is switched on a copy whose evidence was reopened')
                self.assertEqual(h.journal.events('switch_go'), [])
                self.assertIsNone(h.journal.switch_began(), 'the refused go leaves the re-copy open')
                if case in ('mark and run --from O6', 'O7 re-copy round'):
                    self.assertEqual(run_from_o6(h, to_id='O9'), EXIT_DONE, h.text())
                self.assertTrue(s0(h))
                self.assertEqual(h.step('O10', yes=True, redo=False).status, steps.PASSED, h.text())
                self.assertEqual(len(detaches(h)), 1, 'S1 is applied once')

    def test_f4_go_write_fault_points_hold_the_recopy_rule_on_the_rerun(self):
        """The go decision is one journal write (Journal.record_go). before: a kill -9 stand-in as the switch_go row is written; journal:
        just after the row is written, before the transaction commits; after: just after the commit, before S1. before and journal leave
        no go, since the transaction rolls back, so the re-copy is open: mark-recopy gets the copy lock O10 released, run --from O6
        passes O6 to O8 and O10 then detaches once. after leaves the go, so mark-recopy is refused with nothing written and the rerun
        of O10 detaches once. In every case O10 is failed, not running, and nothing was issued before the rerun."""
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = verified_harness(self)
                if point == 'before':
                    fired = crash_journal(self, h.journal, 'event', match=lambda kind, detail: kind == 'switch_go')
                elif point == 'journal':
                    fired = crash_journals(self, 'event', match=lambda kind, detail: kind == 'switch_go', after=True)
                else:
                    fired = crash_journals(self, 'record_go', after=True)
                with self.assertRaises(SimulatedCrash):
                    h.step('O10', yes=True)
                self.assertEqual(len(fired), 1, 'the fault fired')
                self.assertEqual(detaches(h), [])
                self.assertEqual(h.journal.step_status('O10'), 'failed')
                before = copy_evidence(h)
                code, text = mark_recopy_cli(h)
                if point == 'after':
                    self.assertEqual(len(h.journal.events('switch_go')), 1)
                    self.assertEqual(code, EXIT_REFUSED, text)
                    self.assertIn('mark-recopy refused for t_single: the switch may have begun (the go decision was recorded', text)
                    self.assertEqual(copy_evidence(h), before, 'nothing marked, reopened or cleared')
                else:
                    self.assertEqual(h.journal.events('switch_go'), [], 'the go transaction rolled back')
                    self.assertEqual(code, EXIT_DONE, text)
                    self.assertEqual(run_from_o6(h, to_id='O9'), EXIT_DONE, h.text())
                out = h.step('O10', yes=True, redo=False)
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertEqual(len(detaches(h)), 1, 'S1 is applied once')

    def test_f4_a_go_recorded_just_after_the_switch_check_still_refuses_o6_to_o8(self):
        """run_step read switch_began and then started the step in a second journal write, so a go recorded by another process between
        the two let O6 to O8 run behind the switch. The check and the start are now one transaction (Journal.step_start with unless). A
        go recorded just after the first check refuses the step with nothing run or journaled (O6 with a bucket marked, so it would
        issue the reconciliation DELETE; O7 and O8 with --redo). A refusal without a statement of its own: it guards the step_start
        journal write, and so the reconciliation DELETE of O6; the failure points at the S1 DETACH are in
        test_f4_mark_recopy_and_o6_to_o8_refused_once_the_switch_began."""
        for step_id in steps.AFTER_SWITCH_REFUSED:
            with self.subTest(step=step_id):
                h = verified_harness(self)
                if step_id == 'O6':
                    h.journal.mark_recopy('t_single', 1)
                real = h.journal.switch_began

                def go_just_after_the_check(h=h, real=real):
                    h.journal.switch_began = real
                    h.journal.event('switch_go', 'go recorded for live name %s by another process' % SRC)
                    return None
                h.journal.switch_began = go_just_after_the_check
                self.addCleanup(h.journal.__dict__.pop, 'switch_began', None)
                before, starts, writes = copy_evidence(h), h.journal.events('step_start'), len(h.fake.writes)
                with self.assertRaisesRegex(KitError, r'Cannot run %s for database %s: the switch may have begun \(the go decision was '
                                                      r'recorded' % (step_id, SRC)):
                    h.step(step_id, redo=True)
                self.assertEqual(h.fake.writes[writes:], [], 'nothing was run')
                self.assertEqual((copy_evidence(h), h.journal.events('step_start')), (before, starts), 'nothing was journaled')

    def test_f4_two_databases_mark_recopy_refuses_before_marking_any_and_reruns_only_the_marked_one(self):
        """mark-recopy over two journals marked the first and then raised on the second once its switch began. Now every journal is
        checked first, so the refusal marks none. Marking one database reruns O6 to O8 there only, and gate O10 --pass refused for one
        of two journals records nothing in either. A refusal without a statement of its own: it guards the mark_recopy journal write,
        whose failure points are in test_f4_o10_block_rewrite_fault_points_never_leave_a_block_without_a_stop."""
        h = verified_harness(self, mutate=two_databases)
        base = ['--config', str(h.config_path), '--run', h.run_id]
        second = second_journal(self, h, switched=True)
        before, held = copy_evidence(h), second_state(second)
        code, text = run_cli(['mark-recopy', 't_single', '--bucket', '1', '--yes'] + base, env=h.env)
        self.assertEqual(code, EXIT_REFUSED, text)
        self.assertIn('%s: mark-recopy refused for t_single: the switch may have begun (O10 is passed)' % SECOND, text)
        self.assertIn('no database was marked', text)
        self.assertEqual((copy_evidence(h), second_state(second)), (before, held), 'neither database is marked')

        h = verified_harness(self, mutate=two_databases)
        base = ['--config', str(h.config_path), '--run', h.run_id]
        second = second_journal(self, h)
        held = second_state(second)
        code, text = run_cli(['mark-recopy', 't_single', '--bucket', '1', '--db', SRC] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertEqual(second_state(second), held)
        journals = {SRC: h.journal, SECOND: second}
        self.assertEqual(steps.run_steps(h.config, h.config.databases, journals, h.log, run_id=h.run_id, from_id='O6', to_id='O9',
                                         sessions=h.sessions), EXIT_DONE, h.text())
        for step_id, title in (('O6', 'Copy'), ('O7', 'Post-copy'), ('O8', 'Verify')):
            self.assertIn('[%s] %s (python; runner) database %s\n' % (step_id, title, SRC), h.text())
            self.assertIn('[%s] %s (python; runner) database %s: already passed, skipped' % (step_id, title, SECOND), h.text())
        self.assertEqual(second_state(second), held, 'the database that was not marked is left alone')
        self.assertEqual((h.journal.recopy_epoch('t_single'), second.recopy_epoch('t_single')), (1, 0))
        self.assertTrue(s0(h))
        self.assertEqual(switch.s0_journal_problems(second), [])
        second.set_table_attr('t_comp', 'verified_at_utc', None)
        code, text = run_cli(['gate', 'O10', '--pass', '--note', 'both switched by hand', '--yes'] + base, env=h.env)
        self.assertEqual(code, EXIT_REFUSED, text)
        self.assertIn('gate O10 --pass refused for %s: S0 refused: not verified on the rows now copied: t_comp (no verified_at_utc)' % SECOND, text)
        self.assertNotIn('refused for %s' % SRC, text)
        for journal in (h.journal, second):
            self.assertIsNone(journal.step_status('O10'))
            self.assertFalse([d for d in journal.events('gate') if d.startswith('O10')], 'nothing is recorded in either journal')


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
        self.assertEqual([label for label, _ in results], ['runner account still has privileges', 'no view in the live database has the runner as its definer',
                                                          'no routine in the live database has the runner as its definer', 'zero privileges remain'])
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
        h.fake.attachment(SRC)['ATTACH_TYPE'] = 'READWRITE'  # the fake lists no pipeline on a READ ONLY attachment (fact 23)
        ownersql.execute_block(block, owner, h.log, h.journal)
        self.assertIn('owner_skipped', [e['kind'] for e in h.journal.events()])
        block = ownersql.Block('O9', 'test')
        block.statement("CREATE PIPELINE `p9` AS LOAD DATA KAFKA 'b/t' CREDENTIALS '<<CREDENTIALS>>' INTO TABLE `t`", 'create pipeline p9')
        h.fake.pipelines = [{'PIPELINE_NAME': 'p9', 'STATE': 'Stopped', 'DATABASE_NAME': SRC}]
        with self.assertRaises(KitError) as caught:
            ownersql.execute_block(block, owner, h.log, h.journal, substitute=lambda sql: sql.replace("'<<CREDENTIALS>>'", "'super-secret-value'"))
        self.assertIn('the message is withheld because the statement carried an injected secret', str(caught.exception))
        self.assertNotIn('super-secret-value', str(caught.exception))

    def test_f1_before_first_statement_runs_once_after_the_leading_checks(self):
        """The hook runs once, inside the session, after every check ahead of the first statement and before that statement.

        A failed leading check, or a block with no statement, never calls it. A first statement
        found already done by its skip_if count still calls it (the change it guards happened).
        """
        h = switched_harness(self)
        owner = h.sessions.owner(steps.guard_for('O11', h.db, steps.OWNER))
        running = "SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s AND STATE = 'Running'"
        present = 'SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s'
        calls = []

        def hook():
            calls.append((list(h.fake.writes), [e['detail'] for e in h.journal.events() if e['kind'] == 'owner_check']))

        def block(expect_present):
            b = ownersql.Block('O11', 'test')
            b.check('pipelines present', present, (SRC,), ownersql.Expect('column_equals', 'n', expect_present))
            for name in ('p1', 'p2'):
                b.statement('START PIPELINE `%s`.`%s`' % (SRC, name), 'start ' + name, skip_if=(running, (SRC, name)))
            return b
        h.fake.pipelines = [{'PIPELINE_NAME': name, 'STATE': 'Stopped', 'DATABASE_NAME': SRC} for name in ('p1', 'p2')]
        # an O11 block runs on the live name, which is read write; the fake lists no pipeline on a READ ONLY attachment (fact 23)
        h.fake.attachment(SRC)['ATTACH_TYPE'] = 'READWRITE'
        with self.assertRaisesRegex(KitError, 'stopped at check "pipelines present"'):
            ownersql.execute_block(block(3), owner, h.log, h.journal, before_first_statement=hook)
        self.assertEqual(calls, [], 'a failed leading check never calls the hook')
        ownersql.execute_block(ownersql.Block('O11', 'test').check('pipelines present', present, (SRC,), ownersql.Expect('some_rows')),
                               owner, h.log, h.journal, before_first_statement=hook)
        self.assertEqual(calls, [], 'a block without a statement never calls it')
        writes = list(h.fake.writes)
        ownersql.execute_block(block(2), owner, h.log, h.journal, before_first_statement=hook)
        self.assertEqual(len(calls), 1, 'once per block')
        self.assertEqual(calls[0][0], writes, 'before the first START')
        self.assertIn('O11 pipelines present: ok (n = 2)', calls[0][1], 'after the leading check passed')
        self.assertEqual(len(h.fake.writes), len(writes) + 2)
        calls.clear()
        writes = list(h.fake.writes)
        ownersql.execute_block(block(2), owner, h.log, h.journal, before_first_statement=hook)
        self.assertEqual(len(calls), 1, 'a first statement already done still calls it')
        self.assertEqual(h.fake.writes, writes, 'both STARTs were skipped')

    def test_placeholders_refuse_execution(self):
        h = switched_harness(self)
        names = ownersql.names_for(h.ctx())
        names['live_sid'] = '<live_sid>'
        block = ownersql.endstate_block(names)
        with self.assertRaisesRegex(KitError, 'C3 cannot run: live, retained not recorded'):
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
        self.assertEqual(result['statements'], 6, 'S1, S2, S2b attach and drop, S3, S4')
        self.assertEqual(len(h.fake.portal_entries()), 2, 'the notebook route ends with two entries too')
        roles = {a['role']: a['row']['STORAGE_ID'] for a in result['attachments']}
        self.assertEqual(set(roles), {'source', 'retained', 'live'}, 'S1 records the detached original, S2 and S3 the new records')
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
        h.fake.attachment(SRC)['ATTACH_TYPE'] = 'READWRITE'  # O2 runs before the freeze; PIPELINES is empty on a READ ONLY attachment (fact 23)
        block = ownersql.Block('O2', 'stop').capture(SRC, 'capture now')
        with contextlib.redirect_stdout(io.StringIO()):
            result = self.namespace()['run_block'](h.fake.connect().cursor(), block.to_dict(), {})
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['capture']['database'], SRC)
        self.assertEqual(len(result['capture']['definitions']['p1']), 2, 'CREATE and ALTER rows')
        self.assertEqual(result['capture']['offsets'][0]['LATEST_LOADED_OFFSET'], '9')


if __name__ == '__main__':
    unittest.main()
