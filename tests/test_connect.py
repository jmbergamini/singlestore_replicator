"""s2rebuild tests test_connect: redaction, secrets, keychain reads, guarded sessions and the log.

Author: John Bergamini
"""
import io
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from s2rebuild import sqlutil
from s2rebuild.common import KitError
from s2rebuild.connect import DryRunSession, KeepAlive, Log, Redactor, Secrets, Session, is_driver_error, read_keychain
from tests.helpers import FakeConn, config_from

HOST, USER, PASSWORD, CA = 'svc-secret-host.example', 'runner_user', 'Sup3r-Secret-Pw', '/x/ca/bundle.pem'


class RedactorTests(unittest.TestCase):
    def test_masks_every_value_longest_first(self):
        r = Redactor([HOST, USER, PASSWORD, CA, '', None])
        text = r('Access denied for %s@%s using %s ca=%s and %s' % (USER, HOST, PASSWORD, CA, PASSWORD))
        for v in (HOST, USER, PASSWORD, CA):
            self.assertNotIn(v, text)
        self.assertEqual(text.count('<redacted>'), 5)
        r.extend(['{"sasl.password":"k"}'])
        self.assertNotIn('sasl', r('CREDENTIALS {"sasl.password":"k"}'))
        self.assertEqual(Redactor()('plain'), 'plain')

    def test_user_names_are_masked_only_where_they_stand_as_a_user(self):
        """W6: a soft value (a SQL user named admin) is masked as user@host, a quoted account and USER = 'admin', never in a label."""
        r = Redactor([HOST], soft=['admin', 's2rebuild_runner'])
        self.assertEqual(r('[O3] Freeze (admin; owner) database d'), '[O3] Freeze (admin; owner) database d', 'the step kind label stays')
        self.assertEqual(r("Access denied for user 'admin'@'%' on " + HOST), "Access denied for user '<redacted>'@'%' on <redacted>")
        self.assertEqual(r('owner: connected as admin@%; runner s2rebuild_runner@%'), 'owner: connected as <redacted>@%; runner <redacted>@%')
        self.assertEqual(r("DEFINER=`admin`@`%` WHERE USER = 'admin' AND user='s2rebuild_runner'"),
                         "DEFINER=`<redacted>`@`%` WHERE USER = '<redacted>' AND user='<redacted>'")
        self.assertEqual(r('sysadmin@% and admin_user@% and administrator'), 'sysadmin@% and admin_user@% and administrator',
                         'never inside a word')
        self.assertEqual(r("GRANT SELECT TO 'admin'@'%'", soft=False), "GRANT SELECT TO 'admin'@'%'", 'kept in the blocks an operator copies')


class SecretsTests(unittest.TestCase):
    def test_from_env_and_redacted_repr(self):
        s = Secrets('runner', USER, HOST, 3306, CA, password_env='PW', env={'PW': PASSWORD})
        self.assertEqual(repr(s), 'Secrets(<redacted>)')
        self.assertEqual(repr(s.redactor), 'Redactor(<4 values>)')
        self.assertNotIn(PASSWORD, repr(s) + repr(s.redactor) + str(s))
        kwargs = s.connect_kwargs()
        self.assertEqual(kwargs['password'], PASSWORD)
        self.assertTrue(kwargs['ssl_verify_cert'])
        self.assertTrue(kwargs['ssl_verify_identity'])
        self.assertEqual(kwargs['ssl_ca'], CA)
        self.assertTrue(kwargs['autocommit'])
        self.assertEqual(kwargs['read_timeout'], 8 * 3600)
        self.assertEqual(kwargs['charset'], 'utf8mb4')
        self.assertEqual(kwargs['cursorclass'].__name__, 'DictCursor')
        self.assertIn(PASSWORD, s.redactor.values)

    def test_missing_variable_is_named_without_a_value(self):
        with self.assertRaisesRegex(KitError, 'S2REBUILD_RUNNER_PASSWORD for the runner password is not set'):
            Secrets('runner', USER, HOST, 3306, CA, password_env='S2REBUILD_RUNNER_PASSWORD', env={})
        with self.assertRaisesRegex(KitError, 'no password source'):
            Secrets('runner', USER, HOST, 3306, CA, env={})
        with self.assertRaisesRegex(KitError, 'host'):
            Secrets('runner', USER, '', 3306, CA, password_env='PW', env={'PW': 'x'})

    def test_keychain_source_uses_the_injected_reader(self):
        calls = []

        def keychain(service, account):
            calls.append((service, account))
            return PASSWORD
        s = Secrets('owner', 'admin', HOST, 3306, CA, password_keychain='s2rebuild-owner', env={'USER': 'me'}, keychain=keychain)
        self.assertEqual(calls, [('s2rebuild-owner', 'me')])
        self.assertEqual(s.connect_kwargs()['password'], PASSWORD)

    def test_read_keychain_command_and_errors(self):
        seen = {}

        def ok(command, **kw):
            seen['command'] = command
            seen['kw'] = kw
            return subprocess.CompletedProcess(command, 0, stdout=PASSWORD + '\n', stderr='')
        self.assertEqual(read_keychain('svc', 'acct', run=ok), PASSWORD)
        self.assertEqual(seen['command'], ['security', 'find-generic-password', '-a', 'acct', '-s', 'svc', '-w'])
        self.assertTrue(seen['kw']['capture_output'])

        def missing(command, **kw):
            return subprocess.CompletedProcess(command, 44, stdout='', stderr='The specified item could not be found')
        with self.assertRaisesRegex(KitError, 'svc for account acct was not found') as ctx:
            read_keychain('svc', 'acct', run=missing)
        self.assertNotIn('could not be found', str(ctx.exception))

        def empty(command, **kw):
            return subprocess.CompletedProcess(command, 0, stdout='\n', stderr='')
        with self.assertRaisesRegex(KitError, 'is empty'):
            read_keychain('svc', 'acct', run=empty)

        def no_tool(command, **kw):
            raise FileNotFoundError(command[0])
        with self.assertRaisesRegex(KitError, 'not available'):
            read_keychain('svc', 'acct', run=no_tool)

        def slow(command, **kw):
            raise subprocess.TimeoutExpired(command, 60)
        with self.assertRaisesRegex(KitError, 'timed out'):
            read_keychain('svc', 'acct', run=slow)

    def test_from_config_and_connect(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'certs').mkdir()
            cfg = config_from(root=root)
            with self.assertRaisesRegex(KitError, 'owner is not configured'):
                Secrets.from_config(cfg, 'owner', env={})
            s = Secrets.from_config(cfg, 'runner', env={'S2REBUILD_RUNNER_PASSWORD': PASSWORD})
            self.assertEqual(s.ca, str(root / 'certs' / 'singlestore_bundle.pem'))
            with self.assertRaisesRegex(KitError, 'CA file not found'):
                s.connect(connector=lambda **kw: None)
            (root / 'certs' / 'singlestore_bundle.pem').write_text('not a real certificate')
            captured = {}

            def connector(**kw):
                captured.update(kw)
                return 'conn'
            self.assertEqual(s.connect(connector=connector), 'conn')
            self.assertEqual(captured['host'], cfg.workspace.host)
            self.assertEqual(captured['user'], 's2rebuild_runner')
            self.assertEqual(captured['password'], PASSWORD)


class ConnectionTests(unittest.TestCase):
    def secrets(self, tmp):
        root = Path(tmp)
        (root / 'certs').mkdir()
        (root / 'certs' / 'singlestore_bundle.pem').write_text('not a real certificate')
        return Secrets.from_config(config_from(root=root), 'runner', env={'S2REBUILD_RUNNER_PASSWORD': PASSWORD})

    def test_b1_every_connection_gets_tcp_keepalive_on_its_socket(self):
        import socket

        class Sock:
            def __init__(self):
                self.options = []

            def setsockopt(self, level, option, value):
                self.options.append((level, option, value))

        class Conn:
            _sock = Sock()
        with tempfile.TemporaryDirectory() as tmp:
            conn = self.secrets(tmp).connect(connector=lambda **kw: Conn())
        options = conn._sock.options
        self.assertIn((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1), options)
        idle = socket.TCP_KEEPALIVE if sys.platform == 'darwin' else socket.TCP_KEEPIDLE
        self.assertIn((socket.IPPROTO_TCP, idle, 60), options)
        self.assertIn((socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30), options)

    def test_b5_connect_retries_name_resolution_then_fails_with_the_host(self):
        lost = RuntimeError(2003, "Can't connect to MySQL server on 'svc' ([Errno 8] nodename nor servname provided, or not known)")
        calls, sleeps = [], []

        def connector(**kw):
            calls.append(1)
            if len(calls) < 3:
                raise lost
            return 'conn'
        with tempfile.TemporaryDirectory() as tmp:
            s = self.secrets(tmp)
            self.assertEqual(s.connect(connector=connector, sleep=sleeps.append), 'conn', 'the third attempt connects')
            self.assertEqual(sleeps, [20, 20])
            calls.clear()
            sleeps.clear()

            def never(**kw):
                calls.append(1)
                raise lost
            with self.assertRaisesRegex(KitError, 'Cannot connect to %s: name resolution failed after 4 attempts over 60 s' % s.host):
                s.connect(connector=never, sleep=sleeps.append)
            self.assertEqual((len(calls), sleeps), (4, [20, 20, 20]))
            denied = RuntimeError(1045, 'Access denied')
            with self.assertRaises(RuntimeError):
                s.connect(connector=lambda **kw: (_ for _ in ()).throw(denied), sleep=sleeps.append)
            self.assertEqual(sleeps, [20, 20, 20], 'any other error is raised at once')


class SessionTests(unittest.TestCase):
    def test_session_applies_the_guard(self):
        conn = FakeConn(responses={'SELECT COUNT(*) AS n FROM `src`.`t`': [{'n': 3}]},
                        rowcounts={'INSERT INTO `repl`.`t` (`a`) SELECT `a` FROM `src`.`t`': 7})
        s = Session(conn, sqlutil.runner_guard('repl'))
        self.assertEqual(s.query('SELECT COUNT(*) AS n FROM `src`.`t`'), [{'n': 3}])
        self.assertEqual(s.execute('INSERT INTO `repl`.`t` (`a`) SELECT `a` FROM `src`.`t`'), 7)
        with self.assertRaises(sqlutil.GuardError):
            s.execute('DELETE FROM `src`.`t` WHERE 1')
        self.assertEqual(len(conn.statements), 2, 'a refused statement never reaches the connection')
        self.assertEqual(len(s.statements), 2)
        s.close()
        self.assertTrue(conn.closed)

    def test_dry_run_session_records_and_issues_nothing(self):
        out = io.StringIO()
        log = Log(stream=out)
        s = DryRunSession(sqlutil.runner_guard('repl'), log)
        self.assertEqual(s.query('SELECT 1'), [])
        self.assertEqual(s.execute('CREATE TABLE `repl`.`t` LIKE `src`.`t`'), 0)
        with self.assertRaises(sqlutil.GuardError):
            s.execute('DROP DATABASE `repl`')
        self.assertEqual(s.statements, ['SELECT 1', 'CREATE TABLE `repl`.`t` LIKE `src`.`t`'])
        self.assertIn('DRY RUN: CREATE TABLE `repl`.`t` LIKE `src`.`t`', out.getvalue())
        s.close()


class LogTests(unittest.TestCase):
    def test_log_redacts_and_writes_a_0600_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'logs' / 'step.log'
            out = io.StringIO()
            log = Log(redactor=Redactor([PASSWORD, HOST]), stream=out, path=path, verbose=False)
            log('connecting to %s with %s' % (HOST, PASSWORD))
            log.plain('block line with %s' % PASSWORD)
            log.debug('hidden unless verbose')
            log.close()
            text = out.getvalue()
            self.assertNotIn(PASSWORD, text)
            self.assertNotIn(HOST, text)
            self.assertIn('<redacted>', text)
            self.assertNotIn('hidden', text)
            self.assertTrue(text.splitlines()[0].endswith('Z connecting to <redacted> with <redacted>'))
            self.assertEqual(text.splitlines()[1], 'block line with <redacted>')
            on_disk = path.read_text()
            self.assertEqual(on_disk, text)
            self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
            verbose = Log(stream=out, verbose=True)
            verbose.debug('now shown')
            self.assertIn('now shown', out.getvalue())



class KeepAliveTests(unittest.TestCase):
    def test_w8_keep_alive_sends_select_1_on_schedule_with_a_fake_clock(self):
        """The thread sends SELECT 1 every workspace.keepalive_minutes of the injected clock (a minute per reading), each on a
        connection of its own that it closes (B4); stop() ends it. A ping that fails is counted and logged with its error number
        and the redacted message, never raised; run_step starts no thread for a dry run or 0 minutes."""
        import threading
        from s2rebuild import steps
        ticks = [0.0]
        lock = threading.Lock()

        def clock():
            with lock:
                ticks[0] += 60.0
                return ticks[0]
        conn = FakeConn({'SELECT 1': [{'1': 1}]})
        opens = []

        def opener():
            opens.append(1)
            return conn
        out = io.StringIO()
        keepalive = KeepAlive(opener, 15, Log(stream=out), clock=clock, poll=0.001)
        done = threading.Event()
        original = keepalive.ping

        def ping():
            original()
            if len(keepalive.sent) >= 2:
                done.set()
        keepalive.ping = ping
        keepalive.start()
        self.assertTrue(done.wait(5), 'two pings on the fake clock')
        keepalive.stop()
        self.assertEqual([sql for sql, _ in conn.statements][:2], ['SELECT 1', 'SELECT 1'])
        self.assertEqual(len(opens), 2, 'a fresh connection per ping')
        self.assertGreaterEqual(keepalive.sent[1] - keepalive.sent[0], 15 * 60, 'the second ping waits the interval on the clock')
        self.assertTrue(conn.closed, 'each ping closes its connection')
        self.assertIn('keep-alive: SELECT 1 sent', out.getvalue())

        def refuse():
            raise RuntimeError(2003, 'cannot connect to svc-secret-host.example')
        failing = KeepAlive(refuse, 15, Log(Redactor([HOST]), stream=out), clock=clock, poll=0.001)
        failing.ping()
        self.assertEqual((failing.failures, failing.sent), (1, []))
        self.assertIn("keep-alive: SELECT 1 failed: RuntimeError error 2003: (2003, 'cannot connect to <redacted>')", out.getvalue())
        self.assertNotIn(HOST, out.getvalue())
        cfg = config_from(lambda d: d['workspace'].update(keepalive_minutes=0))
        ctx = steps.StepContext(cfg, cfg.databases[0], None, 'run', Log(stream=out), sessions=object())
        self.assertIsNone(steps.start_keepalive(ctx), '0 minutes disables the thread')
        ctx = steps.StepContext(config_from(), cfg.databases[0], None, 'run', Log(stream=out), dry_run=True, sessions=object())
        self.assertIsNone(steps.start_keepalive(ctx), 'a dry run starts none')
        self.assertTrue(is_driver_error(RuntimeError(2013, 'Lost connection to MySQL server during query')))
        self.assertFalse(is_driver_error(RuntimeError('lost connection before the statement ran')))
        self.assertFalse(is_driver_error(RuntimeError(1205, 'Lock wait timeout exceeded')))


if __name__ == '__main__':
    unittest.main()
