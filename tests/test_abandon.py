"""s2rebuild tests test_abandon: back out a run stopped before the switch, its refusals, the guard and the closed run.

Author: John Bergamini
"""
import unittest

from s2rebuild import abandon, steps
from s2rebuild import copy as copy_module
from s2rebuild.common import EXIT_DONE, EXIT_REFUSED, KitError
from tests.helpers import run_cli
from tests.test_copy import SRC, TGT
from tests.test_pipelines import o3_harness, o3_writes

SID = '1028600672070281646:7367158985892520834'


def frozen_with_replacement(test):
    """A Path A harness after O3 (source READONLY) and O5 (replacement created and recorded)."""
    h = o3_harness(test)
    for step_id in ('O3', 'O5'):
        out = h.step(step_id)
        test.assertEqual(out.status, steps.PASSED, h.text())
    test.assertEqual(h.fake.attachment(SRC)['ATTACH_TYPE'], 'READONLY')
    test.assertIsNotNone(h.fake.attachment(TGT))
    return h


def drops(h):
    """Every DROP DATABASE the fake applied."""
    return [w for w in h.fake.writes if w.startswith('DROP DATABASE')]


class AbandonTests(unittest.TestCase):
    def assert_backed_out(self, h):
        att = h.fake.attachment(SRC)
        self.assertEqual((att['ATTACH_TYPE'], att['STORAGE_ID']), ('READWRITE', SID))
        self.assertIsNone(h.fake.attachment(TGT))
        self.assertEqual(o3_writes(h, 'DETACH'), ['DETACH DATABASE `%s`' % SRC, 'DETACH DATABASE `%s`' % SRC], 'one DETACH at O3, one here')
        self.assertEqual(o3_writes(h, 'ATTACH')[-1], 'ATTACH DATABASE `%s` STORAGEID %s' % (SRC, SID))
        self.assertEqual(drops(h), ['DROP DATABASE `%s`' % TGT])
        self.assertIsNotNone(h.journal.get_meta('abandoned_utc'))
        self.assertEqual(h.journal.attachment('source')['attach_type'], 'READWRITE')

    def test_abandon_puts_the_source_back_and_drops_the_replacement(self):
        h = frozen_with_replacement(self)
        note = abandon.run(h.ctx(), 'copy too slow on this size', execute=True)
        self.assertIn('account-sql --drop --execute', note)
        self.assert_backed_out(h)
        self.assertIn('copy too slow on this size', h.journal.events('abandoned'))

    def test_rerun_after_abandon_changes_nothing(self):
        h = frozen_with_replacement(self)
        abandon.run(h.ctx(), 'first', execute=True)
        writes = list(h.fake.writes)
        abandon.run(h.ctx(), 'second', execute=True)
        self.assertEqual(h.fake.writes, writes)
        self.assertIn('skipped: drop the partly copied replacement (already done)', h.text())

    def test_stop_between_detach_and_attach_resumes_at_the_attach(self):
        h = frozen_with_replacement(self)
        h.fake.fail_statement(r'^ATTACH DATABASE', 'before')
        with self.assertRaises(Exception):
            abandon.run(h.ctx(), 'lost connection', execute=True)
        self.assertIsNone(h.fake.attachment(SRC))
        self.assertIsNone(h.journal.get_meta('abandoned_utc'))
        abandon.run(h.ctx(), 'again', execute=True)
        self.assert_backed_out(h)

    def test_refused_once_the_switch_may_have_begun(self):
        h = frozen_with_replacement(self)
        h.journal.record_attachment('retained', SRC + '_retained', SID, 'cid-1', 'READONLY')
        writes = list(h.fake.writes)
        with self.assertRaisesRegex(KitError, 'owner-sql O10 --rollback'):
            abandon.run(h.ctx(), 'too late', execute=True)
        self.assertEqual(h.fake.writes, writes)

    def test_another_database_under_the_replacement_name_stops_before_any_statement(self):
        h = frozen_with_replacement(self)
        h.journal.record_attachment('replacement', TGT, '99:99', 'cid-9', 'READWRITE')
        writes = list(h.fake.writes)
        with self.assertRaisesRegex(KitError, 'replacement name holds the recorded replacement'):
            abandon.run(h.ctx(), 'wrong storage', execute=True)
        self.assertEqual(h.fake.writes, writes, 'nothing detached, attached or dropped')
        self.assertEqual(h.fake.attachment(SRC)['ATTACH_TYPE'], 'READONLY')

    def test_refused_while_the_copy_lock_is_held(self):
        h = frozen_with_replacement(self)
        with copy_module.copy_lock(h.journal_path, 'O6'):
            with self.assertRaisesRegex(KitError, 'copy lock'):
                abandon.run(h.ctx(), 'copy running', execute=True)
        self.assertEqual(h.fake.attachment(SRC)['ATTACH_TYPE'], 'READONLY')

    def test_dry_run_and_print_only_issue_nothing(self):
        h = frozen_with_replacement(self)
        writes = list(h.fake.writes)
        self.assertIn('DRY RUN', abandon.run(h.ctx(dry_run=True), 'dry', execute=True))
        self.assertIn('print only', abandon.run(h.ctx(), '', execute=False))
        self.assertEqual(h.fake.writes, writes)
        self.assertIn('-- abandon: Abandon the run before the switch', h.text())
        self.assertIn('ATTACH DATABASE `%s` STORAGEID %s;' % (SRC, SID), h.text())
        self.assertIsNone(h.journal.get_meta('abandoned_utc'))

    def test_guard_allows_drop_of_the_replacement_only(self):
        h = frozen_with_replacement(self)
        guard = steps.guard_for('abandon', h.db, steps.OWNER)
        guard.check('DROP DATABASE `%s`' % TGT)
        guard.check('ATTACH DATABASE `%s` STORAGEID %s' % (SRC, SID))
        for sql in ('DROP DATABASE `%s`' % SRC, 'DETACH DATABASE `%s`' % TGT, 'DROP DATABASE `other_db`'):
            with self.assertRaises(Exception, msg=sql):
                guard.check(sql)

    def test_cli_needs_a_note_and_an_abandoned_run_refuses_run_step_and_gate(self):
        h = frozen_with_replacement(self)
        base = ['--config', str(h.config_path), '--run', h.run_id]
        code, text = run_cli(['abandon', '--execute'] + base, env=h.env)
        self.assertEqual(code, EXIT_REFUSED, text)
        self.assertIn('needs --note TEXT', text)
        abandon.run(h.ctx(), 'done', execute=True)
        h.journal.close()
        for argv in (['run', '--from', 'O6'], ['step', 'O6'], ['gate', 'O9', '--pass', '--note', 'x']):
            code, text = run_cli(argv + base, env=h.env)
            self.assertEqual(code, EXIT_REFUSED, argv)
            self.assertIn('was abandoned at', text)
        code, text = run_cli(['account-sql', '--drop'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)


if __name__ == '__main__':
    unittest.main()
