"""s2rebuild tests test_watch: PROCESS refusal, writer lines, count deltas, quiet proof and freeze_time.

Author: John Bergamini
"""
import datetime as dt
import unittest

from s2rebuild import steps
from s2rebuild import watch as W
from s2rebuild.common import KitError
from s2rebuild.sqlutil import read_guard
from tests.test_copy import SRC, TGT, Harness


class WatchTests(unittest.TestCase):
    def test_refuses_without_process_privilege(self):
        h = Harness(self)
        h.fake.privileges = [p for p in h.fake.privileges if p['PRIVILEGE_TYPE'] != 'PROCESS']
        session = h.sessions.runner(read_guard())
        with self.assertRaisesRegex(KitError, 'PROCESS'):
            W.watch(session, SRC, h.plans(), {'s2rebuild_runner'}, h.log, max_checks=1, sleep=lambda s: None)

    def test_reports_foreign_writers_and_count_deltas(self):
        h = Harness(self)
        self.assertEqual(h.o5_o6().status, steps.PASSED)
        h.fake.processlist = [
            {'ID': 7, 'USER': 'app', 'HOST': 'app-host:1234', 'DB': SRC, 'COMMAND': 'Query', 'TIME': 3, 'INFO': 'INSERT INTO t_single VALUES (1)'},
            {'ID': 8, 'USER': 's2rebuild_runner', 'HOST': 'kit:1', 'DB': SRC, 'COMMAND': 'Query', 'TIME': 1, 'INFO': 'SELECT 1'},
            {'ID': 9, 'USER': 'other', 'HOST': 'h:2', 'DB': SRC, 'COMMAND': 'Sleep', 'TIME': 100, 'INFO': None},
        ]
        clock = {'t': 0.0}

        def sleep(s):
            clock['t'] += 61
            h.fake.rows(SRC, 't_single').append({'id': 'late-%d' % clock['t'], 'payload': 'x', 'n': 1})
        session = h.sessions.runner(read_guard())
        summary = W.watch(session, SRC, h.plans(), {'s2rebuild_runner'}, h.log, interval=5, tables=2, max_checks=2, sleep=sleep, clock=lambda: clock['t'])
        text = h.text()
        self.assertEqual(summary['checks'], 2)
        self.assertEqual(summary['writers_seen'], 2, 'one foreign writer per check; the idle session and the runner are ignored')
        self.assertIn('SESSION SEEN id=7 user=app host=app-host:1234 command=Query time=3s kind=INSERT', text)
        self.assertNotIn('user=other', text)
        self.assertEqual(summary['count_deltas']['t_single'], 1, 'one row arrived between the count samples')
        self.assertIn('counts: t_single +1', text)
        self.assertEqual(W.statement_kind('  update t set a=1'), 'UPDATE')
        self.assertEqual(W.statement_kind('START PIPELINE p'), 'START PIPELINE')
        self.assertEqual(W.statement_kind(None), 'other')

    def test_ctrl_c_ends_the_watch_with_a_summary(self):
        h = Harness(self)
        session = h.sessions.runner(read_guard())

        def interrupt(s):
            raise KeyboardInterrupt()
        summary = W.watch(session, SRC, h.plans(), set(), h.log, sleep=interrupt)
        self.assertEqual(summary['checks'], 1)
        self.assertIn('watch stopped by Ctrl-C', h.text())
        self.assertIn('writer watch finished after 1 checks', h.text())

    def test_until_stops_the_watch(self):
        h = Harness(self)
        session = h.sessions.runner(read_guard())
        summary = W.watch(session, SRC, h.plans(), set(), h.log, until=dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc), sleep=lambda s: None)
        self.assertEqual(summary['checks'], 0)


class QuietProofTests(unittest.TestCase):
    def test_passes_and_writes_freeze_time(self):
        h = Harness(self, mutate=lambda d: d['connections'].update(owner={'user': 'admin', 'password_env': 'S2REBUILD_OWNER_PASSWORD'}))
        h.env['S2REBUILD_OWNER_PASSWORD'] = 'owner-fixture'
        h.fake.pipelines = [{'PIPELINE_NAME': 'p1', 'STATE': 'Stopped'}]
        session = h.sessions.runner(read_guard())
        owner = h.sessions.owner(read_guard())
        ok, lines = W.quiet_proof(session, owner, SRC, h.plans(), {'s2rebuild_runner', 'admin'}, h.journal, h.log, sleep=lambda s: None)
        self.assertTrue(ok, lines)
        self.assertIsNotNone(h.journal.get_meta('freeze_time'))
        self.assertIn('quiet proof passed', lines[-1])
        self.assertEqual(h.journal.events('quiet_proof')[0][:6], 'passed')

    def test_fails_on_running_pipeline_foreign_session_or_moving_counts(self):
        h = Harness(self, mutate=lambda d: d['connections'].update(owner={'user': 'admin', 'password_env': 'S2REBUILD_OWNER_PASSWORD'}))
        h.env['S2REBUILD_OWNER_PASSWORD'] = 'owner-fixture'
        h.fake.pipelines = [{'PIPELINE_NAME': 'p1', 'STATE': 'Running'}]
        h.fake.processlist = [{'ID': 1, 'USER': 'app', 'HOST': 'h:1', 'DB': SRC, 'COMMAND': 'Query', 'TIME': 1, 'INFO': 'INSERT INTO t_single VALUES (1)'}]
        session = h.sessions.runner(read_guard())
        owner = h.sessions.owner(read_guard())

        def sleep(s):
            h.fake.rows(SRC, 't_single').append({'id': 'moving', 'payload': 'x', 'n': 1})
        ok, lines = W.quiet_proof(session, owner, SRC, h.plans(), {'s2rebuild_runner', 'admin'}, h.journal, h.log, sleep=sleep)
        self.assertFalse(ok)
        text = '\n'.join(lines)
        self.assertIn('pipelines still Running: p1', text)
        self.assertIn('non-idle sessions from other accounts: app@h', text)
        self.assertIn('counts or newest values changed on: t_single', text)
        self.assertIsNone(h.journal.get_meta('freeze_time'))
        self.assertTrue(h.journal.events('quiet_proof')[0].startswith('failed'))

    def test_without_owner_the_proof_cannot_pass(self):
        h = Harness(self)
        session = h.sessions.runner(read_guard())
        ok, lines = W.quiet_proof(session, None, SRC, h.plans(), {'s2rebuild_runner'}, h.journal, h.log, sleep=lambda s: None)
        self.assertFalse(ok)
        self.assertIn('not visible without the owner profile', lines[0])


if __name__ == '__main__':
    unittest.main()
