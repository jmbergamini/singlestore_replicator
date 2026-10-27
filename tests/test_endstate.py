"""s2rebuild tests test_endstate: the two-entry switch (S2b, S4), the rollback through the retained copy, C3 End state and C5 cleanup.

Author: John Bergamini

Release 1.0 W3. The fake's branch records are on (its defaults); refuse_read_only_branch is
off until the live test I0b decides whether a branch of a READ ONLY attachment is refused, and
one test turns it on to exercise R2c.
"""
import unittest
from unittest import mock

from s2rebuild import cleanup, ownersql, steps, switch
from s2rebuild.common import EXIT_DONE, KitError
from tests.helpers import run_cli
from tests.test_copy import SRC, TGT
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
        self.assertEqual((src['STORAGE_ID'], src['ATTACH_TYPE'], src['COMPUTE_ID']), (SID, 'READWRITE', 'cid-1'))
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
        h = complete_run(self)
        before = attach_writes(h)
        with mock.patch.object(cleanup, 'ask', lambda prompt: RET):
            note = cleanup.delete(h.ctx(), RET)
        self.assertIn('deleted', note)
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


if __name__ == '__main__':
    unittest.main()
