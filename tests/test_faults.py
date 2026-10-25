"""s2rebuild tests test_faults: the three failure points every fix is tested at (before, after, before the journal update).

Author: John Bergamini
"""
import unittest

from s2rebuild.journal import MEMORY, Journal
from tests.fakedb import FakeDB, FakeDisconnect, SimulatedCrash
from tests.helpers import FAULT_POINTS, crash_journal

SRC = 'example_orders_db'
TABLE = {'t': {'create': 'CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`))', 'rows': [{'id': 1}]}}


class StatementFaultTests(unittest.TestCase):
    def test_before_the_statement_the_server_never_sees_it(self):
        db = FakeDB(SRC, TABLE)
        db.fail_statement(r'^DETACH DATABASE', 'before')
        cur = db.connect('ops_owner').cursor()
        with self.assertRaises(FakeDisconnect):
            cur.execute('DETACH DATABASE `%s`' % SRC)
        self.assertEqual(db.attachment(SRC)['ATTACH_TYPE'], 'READWRITE')
        self.assertEqual(db.writes, [])
        cur.execute('DETACH DATABASE `%s`' % SRC)
        self.assertIsNone(db.attachment(SRC), 'the fault fires once')

    def test_after_the_statement_the_server_applied_it(self):
        db = FakeDB(SRC, TABLE)
        db.fail_statement(r'^DETACH DATABASE', 'after')
        cur = db.connect('ops_owner').cursor()
        with self.assertRaises(FakeDisconnect):
            cur.execute('DETACH DATABASE `%s`' % SRC)
        self.assertIsNone(db.attachment(SRC))
        self.assertEqual(db.faults_fired, [('after', 'DETACH DATABASE `%s`' % SRC)])

    def test_crash_is_not_an_exception(self):
        db = FakeDB(SRC, TABLE)
        db.fail_statement(r'^SELECT', 'before', crash=True, times=2)
        cur = db.connect().cursor()
        for _ in range(2):
            with self.assertRaises(SimulatedCrash):
                cur.execute('SELECT CONNECTION_ID() AS id')
        cur.execute('SELECT CONNECTION_ID() AS id')
        self.assertFalse(issubclass(SimulatedCrash, Exception))
        with self.assertRaises(AssertionError):
            db.fail_statement('x', 'during')

    def test_f1_fail_connect_and_change_before_fire_once(self):
        """fail_connect fails one connection for that user only; change_before changes the state once, just before the statement."""
        db = FakeDB(SRC, TABLE)
        db.fail_connect('ops_owner')
        with self.assertRaises(FakeDisconnect) as caught:
            db.connect('ops_owner')
        self.assertEqual(caught.exception.args[0], 2003)
        db.connect('s2rebuild_runner')
        db.connect('ops_owner')
        self.assertEqual(db.faults_fired, [('connect', 'ops_owner')])
        db.fail_connect('ops_owner', crash=True)
        with self.assertRaises(SimulatedCrash):
            db.connect('ops_owner')
        db.change_before(r'^SELECT COUNT', lambda d: d.rows(SRC, 't').append({'id': 2}))
        cur = db.connect().cursor()
        cur.execute('SELECT COUNT(*) AS n FROM `%s`.`t`' % SRC)
        self.assertEqual(cur.fetchall(), [{'n': 2}], 'the change lands before the server answers')
        cur.execute('SELECT COUNT(*) AS n FROM `%s`.`t`' % SRC)
        self.assertEqual(cur.fetchall(), [{'n': 2}], 'the change fires once')


class JournalFaultTests(unittest.TestCase):
    def test_crash_before_the_journal_update(self):
        journal = Journal(MEMORY)
        self.addCleanup(journal.close)
        fired = crash_journal(self, journal, 'set_meta', match=lambda key, value: key == 'freeze_time')
        journal.set_meta('other', 'x')
        with self.assertRaises(SimulatedCrash):
            journal.set_meta('freeze_time', '2026-11-01T05:10:00Z')
        self.assertIsNone(journal.get_meta('freeze_time'))
        self.assertEqual(fired, [(('freeze_time', '2026-11-01T05:10:00Z'), {})])
        journal.set_meta('freeze_time', '2026-11-01T05:11:00Z')
        self.assertEqual(journal.get_meta('freeze_time'), '2026-11-01T05:11:00Z')
        self.assertEqual(FAULT_POINTS, ('before', 'after', 'journal'))


if __name__ == '__main__':
    unittest.main()
