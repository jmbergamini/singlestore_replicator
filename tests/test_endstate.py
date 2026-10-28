"""s2rebuild tests test_endstate: the two-entry switch (S2b, S4), the rollback through the retained copy, C3 End state and C5 cleanup.

Author: John Bergamini

Release 1.0 W3. The fake's branch records are on (its defaults); refuse_read_only_branch is
off until the live test I0b decides whether a branch of a READ ONLY attachment is refused, and
one test turns it on to exercise R2c.
"""
import datetime as dt
import re
import unittest
from unittest import mock

from s2rebuild import cleanup, ownersql, report_xlsx, steps, switch
from s2rebuild.common import EXIT_DONE, KitError, iso, parse_utc
from tests.helpers import run_cli
from tests.test_copy import SRC, TGT, Harness
from tests.test_identity import attach_writes
from tests.test_switch import RET, SID, machine_for, old_kit_switch, switched_harness, verified_harness

LONG_AGO, FAR_AHEAD = '2026-01-01T00:00:00Z', '2099-01-01T00:00:00Z'


def entries(h):
    """(name, ATTACH_TYPE) of every Portal entry of the fake, sorted."""
    return sorted((e['DATABASE_NAME'], e['ATTACH_TYPE']) for e in h.fake.portal_entries())


def complete_run(test, until=LONG_AGO):
    """A harness switched through S4 with O7 to C3 passed and retain_until_utc set: what cleanup sees after a run."""
    h = switched_harness(test)
    machine, _ = machine_for(h)
    assert machine.forward() == switch.COMPLETE, h.text()
    for step_id in ('O7', 'O8', 'O9', 'O10', 'O11', 'C1', 'C2', 'C3'):
        h.journal.step_end(step_id, 'passed', 'test')
    h.journal.set_meta('retain_until_utc', until)
    return h


class TwoEntrySwitchTests(unittest.TestCase):
    def test_o10_on_the_fake_ends_with_two_portal_entries(self):
        """S1, S2, S2b (attach by syntax, proof, drop), S3, S4 (drop by record), the gate: the live record and the retained record remain, the
        listing before and after is journaled, and the printed block carries the same statements in the same order."""
        h = switched_harness(self)
        machine, names = machine_for(h)
        machine.record_listing('before')
        self.assertEqual(machine.forward(), switch.COMPLETE, h.text())
        self.assertEqual(entries(h), [(SRC, 'pitr'), (RET, 'resume')])
        writes = attach_writes(h)
        self.assertEqual(writes[0], 'DETACH DATABASE `%s`' % SRC)
        self.assertTrue(writes[2].startswith("ATTACH DATABASE `%s` AT MILESTONE '" % SRC), 'S2b attaches the original by its own syntax')
        self.assertEqual(writes[3], 'DROP DATABASE `%s`' % SRC)
        self.assertEqual(writes[5], 'DROP DATABASE `%s`' % TGT)
        self.assertNotIn('DROP DATABASE `%s`' % RET, writes)
        statements = h.journal.events('switch_statement')
        self.assertIn('S2b DROP source', statements)
        self.assertIn('S4 DROP replacement', statements)
        listings = h.journal.events('switch_listing')
        self.assertEqual(len(listings), 2)
        self.assertTrue(listings[0].startswith('before: ') and listings[1].startswith('after: '))
        self.assertEqual(listings[1].count('name '), 2, 'the listing after holds the two entries')
        self.assertEqual(switch.original_state([switch._record(r) for r in h.fake.branches], names), 'dropped')
        block = switch.forward_block(names, {'tables': 4, 'views': 0, 'pipelines': 0})
        labels = [i.label.split(':')[0] for i in block.statements]
        self.assertEqual(labels, ['S1', 'S2', 'S2b', 'S2b', 'S3', 'S4'])
        self.assertEqual([i.sql for i in block.statements][3], 'DROP DATABASE `%s`' % SRC)
        self.assertLess(block.render().index('S2b proof'), block.render().index('DROP DATABASE `%s`;' % SRC))
        self.assertIn('gate: the original storage holds one entry', block.render())

    def test_a_branch_of_the_source_made_before_the_run_fails_p2_and_refuses_s1_with_nothing_issued(self):
        """A customer branch of the source shares its STORAGE_ID (fact 27) and would be a third entry found only after the DROP of S2b:
        P2 fails naming it, and the S1 before-checks refuse with the listing before any statement."""
        branch = 'ATTACH DATABASE `%s` AS `%s_dev` STORAGEID %s' % (SRC, SRC, SID)
        fresh = Harness(self)
        fresh.journal.record_gate('P1', True, 'test')
        self.assertEqual(fresh.step('P2').status, steps.PASSED, fresh.text())
        self.assertIn('source storage: STORAGE_ID %s holds the source record only (as the runner sees it' % SID, fresh.text())
        fresh.fake.connect('admin').cursor().execute(branch)
        out = fresh.step('P2')
        self.assertEqual((out.status, out.exit_code), (steps.FAILED, 4), fresh.text())
        self.assertIn('source storage: FAILED, STORAGE_ID %s holds 1 record(s) besides the source: name %s_dev, STORAGE_ID %s, ATTACH_TYPE pitr'
                      % (SID, SRC, SID), out.note)
        h = switched_harness(self)
        machine, _ = machine_for(h)
        h.fake.connect('admin').cursor().execute(branch)
        before = attach_writes(h)
        with self.assertRaisesRegex(KitError, r'Switch refused at "S1 before \(for S2b\): the original storage holds one entry, the original '
                                    r'record".*Extra: name %s_dev, STORAGE_ID %s, ATTACH_TYPE pitr.*nothing was issued' % (SRC, SID)):
            machine.forward()
        self.assertEqual(attach_writes(h), before, 'the source was not detached')
        self.assertEqual(machine.position(), switch.NOT_STARTED)
        block = switch.forward_block(ownersql.names_for(h.ctx()))
        labels = [i.label for i in block.items]
        self.assertLess(labels.index(switch.STORAGE_ONLY_LABELS['source']), labels.index(block.statements[0].label), 'the printed block checks it before S1')

    def test_s2b_passes_when_the_original_comes_back_with_a_new_compute_id(self):
        """Helios gives a record a new COMPUTE_ID on every attach, its own ATTACH_SYNTAX included (I0b, October 27, 2026): S2b matches the
        original by name, STORAGE_ID and ATTACH_TYPE, drops it, and the state check after the DROP finds it by name and STORAGE_ID."""
        h = switched_harness(self)
        machine, _ = machine_for(h)
        machine.s1()
        at_s1 = h.journal.attachment('source')['compute_id']
        machine.s2()
        seen = []
        h.fake.change_before(r'^DROP DATABASE `%s`$' % SRC, lambda db: seen.append(db.attachment(SRC)['COMPUTE_ID']))
        machine.s2b()
        self.assertEqual(len(seen), 1)
        self.assertNotEqual(seen[0], at_s1, 'the original record came back with a new COMPUTE_ID')
        self.assertEqual(machine.position(), switch.SOURCE_DROPPED)

    def test_s2b_refuses_the_drop_when_the_zero_change_proof_fails_and_leaves_the_original_attached(self):
        """A foreign session on the original attached under the live name: no DROP, the original stays attached read write (the rollback state),
        the rollback has nothing to do, and the forward continues at the proof once the session is gone."""
        h = switched_harness(self)
        machine, _ = machine_for(h)
        machine.s1()
        machine.s2()
        h.fake.processlist = [{'ID': 9, 'USER': 'app', 'HOST': 'h', 'DB': SRC, 'COMMAND': 'Query', 'TIME': 1, 'INFO': 'INSERT INTO t VALUES (1)'}]
        with self.assertRaisesRegex(KitError, 'the zero-change proof failed, so the original record was not dropped.*rollback state'):
            machine.s2b()
        self.assertNotIn('DROP DATABASE `%s`' % SRC, h.fake.writes)
        src = h.fake.attachment(SRC)
        self.assertEqual((src['STORAGE_ID'], src['ATTACH_TYPE']), (SID, 'READWRITE'))
        self.assertEqual(machine.position(), switch.ORIGINAL_ATTACHED)
        self.assertEqual(machine.rollback(), switch.ORIGINAL_ATTACHED, 'nothing to roll back')
        self.assertEqual(h.fake.attachment(RET)['ATTACH_TYPE'], 'READONLY')
        h.fake.processlist = []
        self.assertEqual(machine.forward(), switch.COMPLETE, h.text())
        self.assertEqual(entries(h), [(SRC, 'pitr'), (RET, 'resume')])

    def test_rollback_before_s2b_uses_r2a(self):
        """After S1 alone the original comes back by its syntax (not started); after S2 too, and the retained copy stays (original attached)."""
        h = switched_harness(self)
        machine, _ = machine_for(h)
        machine.s1()
        syntax = h.journal.attachment('source')['attach_syntax']
        self.assertEqual(machine.rollback(), switch.NOT_STARTED)
        self.assertEqual(attach_writes(h)[-1], syntax, 'R2a is the detached original record\'s own syntax')
        h2 = switched_harness(self)
        machine2, _ = machine_for(h2)
        machine2.s1()
        machine2.s2()
        self.assertEqual(machine2.rollback(), switch.ORIGINAL_ATTACHED)
        self.assertTrue(attach_writes(h2)[-1].startswith("ATTACH DATABASE `%s` AT MILESTONE '" % SRC), 'R2a is the original syntax')
        self.assertTrue(h2.journal.events('rollback')[-1].startswith('R2a:'))
        self.assertEqual(h2.fake.attachment(RET)['ATTACH_TYPE'], 'READONLY')
        self.assertNotIn('DROP DATABASE `%s`' % RET, h2.fake.writes)

    def test_rollback_after_s2b_uses_r2b_and_r2c_when_the_fake_refuses_r2b(self):
        """R2b branches the retained copy under the live name; when the fake refuses that with 2720, R2c detaches the retained name and
        re-attaches its record by syntax under the live name, and the journal says the retained name is gone."""
        h = switched_harness(self)
        machine, _ = machine_for(h)
        machine.forward()
        self.assertEqual(machine.rollback(), switch.ROLLED_BACK)
        self.assertEqual(attach_writes(h)[-1], 'ATTACH DATABASE `%s` AS `%s` STORAGEID %s' % (RET, SRC, SID))
        src = h.fake.attachment(SRC)
        self.assertEqual((src['STORAGE_ID'], src['ATTACH_TYPE']), (SID, 'READWRITE'))
        self.assertEqual(len(h.fake.rows(SRC, 't_comp')), 300, 'the branch carries the retained data')
        self.assertEqual(h.fake.attachment(RET)['ATTACH_TYPE'], 'READONLY')
        self.assertTrue(h.journal.events('rollback')[-1].startswith('R2b:'))
        h2 = switched_harness(self)
        h2.fake.refuse_read_only_branch = True  # the I0b question: a branch of a READ ONLY attachment refused
        machine2, _ = machine_for(h2)
        machine2.forward()
        self.assertEqual(machine2.rollback(), switch.ROLLED_BACK)
        writes = attach_writes(h2)
        self.assertEqual(writes[-2], 'DETACH DATABASE `%s`' % RET)
        self.assertTrue(writes[-1].startswith("ATTACH DATABASE `%s` AS `%s` AT MILESTONE '" % (RET, SRC)), writes[-1])
        self.assertEqual(len(h2.journal.events('switch_statement_refused')), 1)
        self.assertIsNone(h2.fake.attachment(RET), 'the retained name left the attached list')
        self.assertEqual(h2.fake.attachment(SRC)['ATTACH_TYPE'], 'READWRITE')
        self.assertEqual(len(h2.fake.rows(SRC, 't_comp')), 300)
        self.assertIn('the retained name %s is gone' % RET, h2.journal.events('rollback')[-1])
        self.assertNotIn('DROP DATABASE `%s`' % RET, h2.fake.writes)

    def test_a_second_record_listed_first_on_the_original_storage_strands_neither_s1_nor_r2a(self):
        """A dropped record of an earlier attempt listed first on the original storage (facts 35, 33): S1 reaches the detached position, and
        after S2 with the retained record listed first R2a still re-attaches the original; both read the original by its record."""
        h = switched_harness(self)
        machine, _ = machine_for(h)
        earlier = h.fake.record(SRC, SID, 'cid-0', 'pitr', SRC)
        earlier.update(IS_DETACHED=0, IS_DROPPED=1)
        h.fake.branches.insert(0, earlier)
        machine.s1()
        self.assertEqual(machine.position(), switch.DETACHED)
        machine.s2()
        h.fake.branches.reverse()
        self.assertEqual([b['DATABASE_NAME'] for b in h.fake.branches if b['STORAGE_ID'] == SID][0], RET, 'the retained record comes first')
        self.assertEqual(machine.rollback(), switch.ORIGINAL_ATTACHED)
        self.assertEqual(h.fake.attachment(SRC)['STORAGE_ID'], SID, 'the original record is back (with a new COMPUTE_ID, fact 44)')

    def test_a_default_run_issues_no_drop_of_the_retained_name_from_o1_through_c4(self):
        """O10 with the owner profile, then C3 and C4: the retained copy is attached read only at the end and no DROP of its name was issued."""
        h = verified_harness(self)
        self.assertEqual(h.step('O10', yes=True).status, steps.PASSED, h.text())
        for step_id in ('O11', 'C1', 'C2'):
            h.journal.step_end(step_id, 'passed', 'test')
        self.assertEqual(h.step('C3').status, steps.PASSED, h.text())
        self.assertEqual(h.step('C4').status, steps.PASSED, h.text())
        self.assertNotIn('DROP DATABASE `%s`' % RET, h.fake.writes)
        self.assertEqual(h.fake.attachment(RET)['ATTACH_TYPE'], 'READONLY')
        self.assertEqual(entries(h), [(SRC, 'pitr'), (RET, 'resume')])
        self.assertIn('two entries', h.journal.step('O10')['note'])


class EndStateTests(unittest.TestCase):
    def test_c3_refuses_on_three_entries_without_the_repair_block_when_the_journal_has_s2b(self):
        """A third entry after a two-entry switch: C3 refuses with the listing and issues no DETACH, ATTACH or DROP, since S2b and S4 ran."""
        h = switched_harness(self)
        machine, names = machine_for(h)
        machine.forward()
        for step_id in ('O7', 'O8', 'O9', 'O10', 'O11', 'C1', 'C2'):
            h.journal.step_end(step_id, 'passed', 'test')
        h.fake.connect('admin').cursor().execute('ATTACH DATABASE `%s` AS `%s_extra` STORAGEID %s' % (SRC, SRC, names['replacement_sid']))
        self.assertEqual(len(h.fake.portal_entries()), 3)
        before = attach_writes(h)
        out = h.step('C3', yes=True)
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('The journal shows S2b and S4, so no repair applies', out.note)
        self.assertEqual(attach_writes(h), before, 'nothing was issued')
        self.assertEqual(h.journal.events('endstate_repair'), [])
        self.assertEqual(len(h.fake.portal_entries()), 3)

    def test_the_repair_block_runs_only_when_the_journal_lacks_s2b(self):
        """A switch by an older kit left four entries: C3 prints the repair, refuses on any answer but proceed, and on proceed detaches the live
        name, attaches the original by syntax, drops it, re-attaches the live record by syntax, drops the replacement, and ends at two entries."""
        h = switched_harness(self)
        old_kit_switch(h)
        for step_id in ('O7', 'O8', 'O9', 'O10', 'O11', 'C1', 'C2'):
            h.journal.step_end(step_id, 'passed', 'test')
        self.assertEqual(len(h.fake.portal_entries()), 4)
        before = attach_writes(h)
        with mock.patch.object(ownersql, 'ask', lambda prompt: 'no'):
            out = h.step('C3')
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('the repair was not confirmed', out.note)
        self.assertEqual(attach_writes(h), before)
        self.assertEqual(len(h.journal.events('endstate_repair_declined')), 1)
        self.assertTrue((h.tmp / 'runs' / h.run_id / SRC / 'owner-sql' / 'C3-repair.sql').is_file())
        with mock.patch.object(ownersql, 'ask', lambda prompt: 'proceed'):
            out = h.step('C3')
        self.assertEqual(out.status, steps.PASSED, h.text())
        writes = attach_writes(h)[len(before):]
        self.assertEqual(writes[0], 'DETACH DATABASE `%s`' % SRC)
        self.assertTrue(writes[1].startswith("ATTACH DATABASE `%s` AT MILESTONE '" % SRC) and writes[1].endswith('COMPUTEID cid-1'))
        self.assertEqual(writes[2], 'DROP DATABASE `%s`' % SRC)
        self.assertTrue(writes[3].startswith("ATTACH DATABASE `%s` AT MILESTONE '" % SRC), 'the live record by its own syntax')
        self.assertEqual(writes[4], 'DROP DATABASE `%s`' % TGT)
        self.assertEqual(len(writes), 5)
        self.assertEqual(entries(h), [(SRC, 'pitr'), (RET, 'resume')])
        self.assertEqual(h.fake.attachment(SRC)['COMPUTE_ID'], h.journal.attachment('live')['compute_id'], 'the live record is back')
        self.assertEqual(len(h.journal.events('endstate_repair')), 1)
        self.assertEqual(len(h.journal.events('endstate_probe')), 1)
        self.assertEqual(h.step('C3').status, steps.PASSED, 'a rerun passes the gate without the repair')

    def test_a_repair_stopped_after_its_drop_of_the_original_resumes_on_rerun(self):
        """The connection drops before the live record is re-attached: the live name is left detached, and the rerun of C3 resumes the
        repair (its own DROP is not S2b) with the re-attach and the replacement DROP, issuing no statement twice."""
        h = repair_ready(self)
        live_cid = h.journal.attachment('live')['compute_id']
        h.fake.fail_statement(r"^ATTACH DATABASE `%s` AT MILESTONE .* COMPUTEID %s$" % (SRC, re.escape(live_cid)))
        before = attach_writes(h)
        self.assertEqual(h.step('C3', yes=True).status, steps.REFUSED, h.text())
        self.assertIsNone(h.fake.attachment(SRC), 'the live name is detached')
        self.assertIn('repair DROP original record', h.journal.events('switch_statement'))
        self.assertEqual(h.step('C3', yes=True).status, steps.PASSED, h.text())
        writes = attach_writes(h)[len(before):]
        self.assertEqual(len(writes), 5, writes)
        self.assertEqual((writes[0], writes[2], writes[4]), ('DETACH DATABASE `%s`' % SRC, 'DROP DATABASE `%s`' % SRC, 'DROP DATABASE `%s`' % TGT))
        self.assertTrue(writes[1].endswith('COMPUTEID cid-1') and writes[3].endswith('COMPUTEID ' + live_cid), writes)
        self.assertEqual(entries(h), [(SRC, 'pitr'), (RET, 'resume')])

    def test_a_repair_stopped_at_the_proof_continues_there_on_rerun(self):
        """A foreign session on the original attached under the live name stops the repair before its DROP; once it is gone the rerun of C3
        continues at the proof with the original attached, not at the live record check, and ends at two entries."""
        h = repair_ready(self)
        session = {'ID': 9, 'USER': 'app', 'HOST': 'h', 'DB': SRC, 'COMMAND': 'Query', 'TIME': 1, 'INFO': 'INSERT INTO t VALUES (1)'}
        h.fake.change_before(r"^ATTACH DATABASE `%s` AT MILESTONE .* STORAGEID %s COMPUTEID \S+$" % (SRC, SID), lambda db: db.processlist.append(session))
        before = attach_writes(h)
        out = h.step('C3', yes=True)
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('rerun s2rebuild step C3, which continues at the proof', out.note)
        self.assertEqual(h.fake.attachment(SRC)['STORAGE_ID'], SID, 'the original is attached under the live name')
        h.fake.processlist = []
        self.assertEqual(h.step('C3', yes=True).status, steps.PASSED, h.text())
        writes = attach_writes(h)[len(before):]
        self.assertEqual([w.split(' AT MILESTONE ')[0] for w in writes], ['DETACH DATABASE `%s`' % SRC, 'ATTACH DATABASE `%s`' % SRC,
                                                                           'DROP DATABASE `%s`' % SRC, 'ATTACH DATABASE `%s`' % SRC,
                                                                           'DROP DATABASE `%s`' % TGT])
        self.assertEqual(entries(h), [(SRC, 'pitr'), (RET, 'resume')])

    def test_the_repair_refuses_before_any_detach_when_nothing_is_left_to_repair(self):
        """A two-entry switch that left no switch_statement event (a hand or notebook O10) and a third entry: C3 offers the repair, which reads
        the original dropped and the replacement name free and refuses with the listing; the live name is never detached."""
        h = switched_harness(self)
        machine, names = machine_for(h)
        self.assertEqual(machine.forward(), switch.COMPLETE, h.text())
        h.journal.conn.execute("DELETE FROM events WHERE kind = 'switch_statement'")
        for step_id in ('O7', 'O8', 'O9', 'O10', 'O11', 'C1', 'C2'):
            h.journal.step_end(step_id, 'passed', 'test')
        h.fake.connect('admin').cursor().execute('ATTACH DATABASE `%s` AS `%s_extra` STORAGEID %s' % (SRC, SRC, names['replacement_sid']))
        before = attach_writes(h)
        out = h.step('C3', yes=True)
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('nothing to repair, so nothing was issued', out.note)
        self.assertEqual(attach_writes(h), before, 'the live name was not detached')
        self.assertEqual(h.fake.attachment(SRC)['COMPUTE_ID'], h.journal.attachment('live')['compute_id'])


def repair_ready(test):
    """A harness after a switch by an older kit (four entries) with O7 to C2 passed, ready for the C3 repair."""
    h = switched_harness(test)
    old_kit_switch(h)
    for step_id in ('O7', 'O8', 'O9', 'O10', 'O11', 'C1', 'C2'):
        h.journal.step_end(step_id, 'passed', 'test')
    return h


class CleanupTests(unittest.TestCase):
    def test_cleanup_before_the_date_refuses_and_the_listing_prints(self):
        h = complete_run(self, until=FAR_AHEAD)
        before = list(h.fake.writes)
        note = cleanup.delete(h.ctx(), RET)
        self.assertIn('the retention period ends at %s' % FAR_AHEAD, note)
        self.assertEqual(h.fake.writes, before, 'nothing was issued')
        self.assertEqual(len(h.journal.events('cleanup_refused')), 1)
        self.assertIn('deletes the retained copy of this run only', cleanup.delete(h.ctx(), 'other_name'))
        code, text = run_cli(['cleanup', '--config', str(h.config_path), '--run', h.run_id], env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('retain until: %s' % FAR_AHEAD, text)
        self.assertIn('cleanup --delete %s' % RET, text)
        self.assertEqual(h.fake.attachment(RET)['ATTACH_TYPE'], 'READONLY')

    def test_a_wrong_typed_name_issues_no_drop_and_records_the_event(self):
        h = complete_run(self)
        before = list(h.fake.writes)
        with mock.patch.object(cleanup, 'ask', lambda prompt: RET + '_typo'):
            note = cleanup.delete(h.ctx(), RET)
        self.assertTrue(note.startswith('kept by customer'), note)
        self.assertEqual(h.fake.writes, before, 'nothing was issued')
        self.assertEqual(len(h.journal.events('kept_by_customer')), 1)
        self.assertEqual(h.fake.attachment(RET)['ATTACH_TYPE'], 'READONLY')
        with mock.patch.object(cleanup, 'ask', lambda prompt: None):
            with self.assertRaisesRegex(KitError, 'needs a terminal'):
                cleanup.delete(h.ctx(), RET)
        self.assertEqual(h.fake.writes, before)

    def test_the_typed_name_runs_detach_reattach_by_syntax_and_drop_by_record(self):
        """After C4 the runner account is gone, so the delete opens owner sessions only."""
        h = complete_run(self)
        h.journal.step_end('C4', 'passed', 'test')
        h.fake.fail_connect('s2rebuild_runner', times=5)
        before = attach_writes(h)
        with mock.patch.object(cleanup, 'ask', lambda prompt: RET):
            note = cleanup.delete(h.ctx(), RET)
        self.assertIn('deleted', note)
        self.assertNotIn(('connect', 's2rebuild_runner'), h.fake.faults_fired, 'no runner connection was tried')
        writes = attach_writes(h)[len(before):]
        self.assertEqual(writes[0], 'DETACH DATABASE `%s`' % RET)
        self.assertTrue(writes[1].startswith("ATTACH DATABASE `%s` AT MILESTONE '" % RET) and writes[1].endswith('COMPUTEID ' + h.journal.attachment('retained')['compute_id']))
        self.assertEqual(writes[2], 'DROP DATABASE `%s`' % RET)
        self.assertEqual(len(writes), 3)
        self.assertIsNone(h.fake.attachment(RET))
        self.assertEqual(entries(h), [(SRC, 'pitr')])
        self.assertEqual(h.fake.attachment(SRC)['COMPUTE_ID'], h.journal.attachment('live')['compute_id'], 'the live name is untouched')
        self.assertEqual(len(h.journal.events('retained_deleted')), 1)
        self.assertIsNotNone(h.journal.get_meta('retained_deleted_utc'))

    def test_a_switch_recorded_by_hand_gets_its_retain_until_date_at_c2(self):
        """The hand route (no owner profile) has no kit S3 to record the switch time: gate O10 --pass records it, so C2 records
        retain_until_utc, the date C5 waits for, instead of leaving it unrecorded for the life of the run."""
        h = verified_harness(self, owner=False)
        code, text = run_cli(['gate', 'O10', '--pass', '--note', 'switched by hand', '--config', str(h.config_path), '--run', h.run_id], env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        switched = h.journal.get_meta('switched_utc')
        self.assertIsNotNone(switched, 'the gate time stands for the switch time')
        until = report_xlsx.record_retain_until(h.ctx())
        self.assertEqual(until, iso(parse_utc(switched, 'switched_utc') + dt.timedelta(days=90)))
        self.assertEqual(h.journal.get_meta('retain_until_utc'), until)
        self.assertIn('retained copy %s kept until %s (closeout.retain_days 90)' % (RET, until), h.text())


if __name__ == '__main__':
    unittest.main()
