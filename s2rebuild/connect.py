"""s2rebuild connect: secrets, redaction, connections, guarded sessions and the log.

Author: John Bergamini

Secrets reads a password from the macOS Keychain or from an environment variable and keeps
it in memory only. Redactor masks host, user, password and CA path in every line that is
printed or written. Session applies the active step's Guard to every statement before it
reaches the server. DryRunSession records statements and issues nothing. Every connection gets
TCP keep-alive on its socket (enable_tcp_keepalive), and a connect that fails on name resolution
or a refused connection is tried again (CONNECT_RETRIES). KeepAlive sends SELECT 1 on a fresh
runner connection every workspace.keepalive_minutes while a step runs, and is_driver_error names
a connection reset (2003, 2004, 2006, 2013). Statements issued by this
module: SELECT 1 from the keep-alive thread; otherwise it forwards the statements the steps
hand to a Session.
"""
import getpass
import os
import re
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

from .common import KitError, iso, utc_now

KEYCHAIN_TOOL = 'security'
LOG_FILE_MODE = 0o600
# B1: an INSERT that runs for hours leaves its socket idle, and a NAT or firewall on the path closes an idle flow (I5: 9 chunks lost
# with [Errno 60] after 2 to 3 hours). Keep-alive probes after 60 s idle, then every 30 s.
TCP_KEEPALIVE_IDLE_SECONDS = 60
TCP_KEEPALIVE_INTERVAL_SECONDS = 30
# B5: a connect that fails on name resolution or a refused connection is tried this many more times, CONNECT_RETRY_SECONDS apart.
CONNECT_RETRIES = 3
CONNECT_RETRY_SECONDS = 20
_RETRY_CONNECT = re.compile(r'Errno (8|-2|-3|61|111)\b|nodename nor servname|Name or service not known|Temporary failure in name resolution|'
                            r'Connection refused', re.I)


USER_FORMS = (
    # user@host, 'user'@'host' and `user`@`host`: the name must stand alone before the @
    r"(?<![\w.])(['`\"]?)%s\1(?=@)",
    # USER = 'user', as in a PROCESSLIST condition or an account statement
    r"(?i)(\bUSER\s*=\s*['`\"])%s(['`\"])",
)


def mask_user(text, name):
    """Mask name where it stands as a SQL user: user@host, a quoted account ('user'@'host'), USER = 'user'.

    Never inside a word or a label, so a step kind such as admin stays readable when the owner
    user is admin.
    """
    escaped = re.escape(name)
    text = re.sub(USER_FORMS[0] % escaped, r'\1<redacted>\1', text)
    return re.sub(USER_FORMS[1] % escaped, r'\1<redacted>\2', text)


class Redactor:
    """Replaces every connection setting value in text before it is printed.

    soft values (the SQL user names) are masked in log lines but kept in the admin blocks the
    operator copies, since a block that names a user cannot run with the name masked. A soft
    value is masked only where it stands as a user (mask_user); a hard value everywhere.
    """

    def __init__(self, values=(), soft=()):
        self.soft = {str(v) for v in soft if v}
        self.values = sorted({str(v) for v in values if v} | self.soft, key=len, reverse=True)

    def __call__(self, text, soft=True):
        text = str(text)
        for v in self.values:
            if v in self.soft:
                if soft and v in text:
                    text = mask_user(text, v)
                continue
            if v in text:
                text = text.replace(v, '<redacted>')
        return text

    def extend(self, values, soft=()):
        """Add more values to mask (the credentials injected at O9, for example)."""
        self.soft |= {str(v) for v in soft if v}
        self.values = sorted(set(self.values) | {str(v) for v in values if v} | self.soft, key=len, reverse=True)

    def __repr__(self):
        return 'Redactor(<%d values>)' % len(self.values)


def read_keychain(service, account=None, run=None):
    """Read a generic password from the macOS Keychain with the security tool.

    Runs: security find-generic-password -a <account> -s <service> -w
    Returns the value and never logs it. Raises KitError naming the item, never the value.
    """
    run = run or subprocess.run
    account = account or os.environ.get('USER') or getpass.getuser()
    command = [KEYCHAIN_TOOL, 'find-generic-password', '-a', account, '-s', service, '-w']
    try:
        proc = run(command, capture_output=True, text=True, timeout=60, check=False)
    except FileNotFoundError:
        raise KitError('The %s command is not available on this machine; use password_env instead' % KEYCHAIN_TOOL)
    except subprocess.TimeoutExpired:
        raise KitError('Reading Keychain item %s timed out; unlock the Keychain and retry' % service)
    if proc.returncode != 0:
        raise KitError('Keychain item %s for account %s was not found or could not be read' % (service, account))
    value = (proc.stdout or '').rstrip('\r\n')
    if not value:
        raise KitError('Keychain item %s is empty' % service)
    return value


class Secrets:
    """Connection settings for one profile. Values live in memory only; repr is redacted."""

    def __init__(self, name, user, host, port, ca_file, password_keychain=None, password_env=None,
                 env=None, keychain=None):
        env = os.environ if env is None else env
        if not host:
            raise KitError('workspace.host is not set')
        if password_env:
            password = env.get(password_env)
            if not password:
                raise KitError('Environment variable %s for the %s password is not set' % (password_env, name))
        elif password_keychain:
            password = (keychain or read_keychain)(password_keychain, env.get('USER') or None)
        else:
            raise KitError('connections.%s has no password source' % name)
        self.name = name
        self.user = user
        self.host = host
        self.port = int(port or 3306)
        self.ca = str(ca_file)
        self._password = password
        self.redactor = Redactor([host, password, self.ca], soft=[user])

    @classmethod
    def from_config(cls, config, profile_name, env=None, keychain=None):
        """Build Secrets for 'runner' or 'owner' from a Config."""
        profile = config.runner if profile_name == 'runner' else config.owner
        if profile is None:
            raise KitError('connections.%s is not configured' % profile_name)
        ca = config.resolve(config.workspace.ca_file)
        return cls(profile_name, profile.user, config.workspace.host, config.workspace.port, ca,
                   profile.password_keychain, profile.password_env, env=env, keychain=keychain)

    def __repr__(self):
        return 'Secrets(<redacted>)'

    def connect_kwargs(self):
        """Keyword arguments for the PyMySQL driver (TLS with CA verification, autocommit, DictCursor)."""
        import pymysql.cursors
        return dict(host=self.host, port=self.port, user=self.user, password=self._password,
                    ssl_ca=self.ca, ssl_verify_cert=True, ssl_verify_identity=True,
                    autocommit=True, connect_timeout=15, read_timeout=8 * 3600, write_timeout=600,
                    charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    def connect(self, connector=None, sleep=time.sleep):
        """Open a connection with TCP keep-alive on its socket. connector defaults to pymysql.connect; tests pass a fake.

        A failure on name resolution or a refused connection (retryable_connect_error) is tried
        CONNECT_RETRIES more times, CONNECT_RETRY_SECONDS apart (B5), then raises KitError with the
        host name; any other error is raised at once.
        """
        if not Path(self.ca).is_file():
            raise KitError('CA file not found at the configured workspace.ca_file path')
        if connector is None:
            import pymysql
            connector = pymysql.connect
        for attempt in range(CONNECT_RETRIES + 1):
            try:
                conn = connector(**self.connect_kwargs())
                break
            except Exception as exc:
                if not retryable_connect_error(exc):
                    raise
                if attempt == CONNECT_RETRIES:
                    # the redactor masks the host in printed lines, so the text also names the key that holds it
                    raise KitError('Cannot connect to %s (workspace.host): %s after %d attempts over %d s; check name resolution (dig the '
                                   'workspace.host of the configuration, then flush the resolver cache) and that the workspace is ACTIVE' % (
                                       self.host, _connect_reason(exc), attempt + 1, CONNECT_RETRIES * CONNECT_RETRY_SECONDS)) from exc
                sleep(CONNECT_RETRY_SECONDS)
        enable_tcp_keepalive(conn)
        return conn


def retryable_connect_error(exc):
    """True for a connect failure worth another try: name resolution failed or the connection was refused (B5)."""
    return driver_errno(exc) == 2003 and bool(_RETRY_CONNECT.search(str(exc)))


def _connect_reason(exc):
    """'name resolution failed' or 'connection refused' for a retryable connect error."""
    return 'connection refused' if re.search(r'refused|Errno (61|111)\b', str(exc), re.I) else 'name resolution failed'


def enable_tcp_keepalive(conn):
    """B1: SO_KEEPALIVE on the connection's socket, first probe after TCP_KEEPALIVE_IDLE_SECONDS, then every TCP_KEEPALIVE_INTERVAL_SECONDS.

    The idle option is TCP_KEEPALIVE on macOS and TCP_KEEPIDLE on Linux. Returns True when the
    socket was set; a connection without a socket (a test fake) is left as it is.
    """
    sock = getattr(conn, '_sock', None)
    if sock is None:
        return False
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    idle = getattr(socket, 'TCP_KEEPALIVE', None) if sys.platform == 'darwin' else getattr(socket, 'TCP_KEEPIDLE', None)
    if idle is not None:
        sock.setsockopt(socket.IPPROTO_TCP, idle, TCP_KEEPALIVE_IDLE_SECONDS)
    if hasattr(socket, 'TCP_KEEPINTVL'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, TCP_KEEPALIVE_INTERVAL_SECONDS)
    return True


class Session:
    """One database connection with the step's write guard applied to every statement."""

    def __init__(self, conn, guard, log=None):
        self.conn = conn
        self.guard = guard
        self.log = log
        self.statements = []

    def query(self, sql, args=()):
        """Check the guard, execute, return every row (list of dicts with DictCursor)."""
        self.guard.check(sql)
        self.statements.append(sql)
        with self.conn.cursor() as cur:
            cur.execute(sql, args or None)
            return cur.fetchall()

    def execute(self, sql, args=()):
        """Check the guard, execute, return the affected row count."""
        self.guard.check(sql)
        self.statements.append(sql)
        with self.conn.cursor() as cur:
            cur.execute(sql, args or None)
            return cur.rowcount

    def close(self):
        """Close the connection; a failure while closing is ignored so it cannot mask the real error."""
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001 - closing a broken connection must not mask the real error
            pass


class DryRunSession:
    """Records every statement after the guard check and issues nothing."""

    def __init__(self, guard, log=None, label='DRY RUN'):
        self.guard = guard
        self.log = log
        self.label = label
        self.statements = []

    def _record(self, sql):
        self.guard.check(sql)
        self.statements.append(sql)
        if self.log is not None:
            self.log('%s: %s' % (self.label, ' '.join(str(sql).split())))

    def query(self, sql, args=()):
        """Guard-check and record the statement; return no rows."""
        self._record(sql)
        return []

    def execute(self, sql, args=()):
        """Guard-check and record the statement; return a row count of 0."""
        self._record(sql)
        return 0

    def close(self):
        """Nothing to close."""
        pass


class SessionFactory:
    """Opens guarded sessions for the runner and owner identities; a dry run gets recording sessions.

    Secrets are read once per identity and their values are added to the log's redactor. The
    copy workers open one connection each through runner_connector().
    """

    def __init__(self, config, env=None, log=None, dry_run=False, connector=None, keychain=None):
        self.config = config
        self.env = os.environ if env is None else env
        self.log = log
        self.dry_run = dry_run
        self.connector = connector
        self.keychain = keychain
        self._secrets = {}
        self.opened = []

    def secrets(self, profile):
        """Secrets for 'runner' or 'owner', read once; their values join the log redactor."""
        if profile not in self._secrets:
            s = Secrets.from_config(self.config, profile, env=self.env, keychain=self.keychain)
            if self.log is not None:
                self.log.redactor.extend(s.redactor.values, soft=s.redactor.soft)
            self._secrets[profile] = s
        return self._secrets[profile]

    def session(self, profile, guard):
        """A guarded Session for the profile, or a DryRunSession when dry_run is set."""
        if self.dry_run:
            return DryRunSession(guard, self.log)
        conn = self.secrets(profile).connect(self.connector)
        session = Session(conn, guard, self.log)
        self.opened.append(session)
        return session

    def runner(self, guard):
        """A runner session behind guard."""
        return self.session('runner', guard)

    def owner(self, guard):
        """An owner session behind guard; KitError when connections.owner is not configured."""
        return self.session('owner', guard)

    def runner_connector(self):
        """A zero-argument callable that opens a new raw runner connection (one per copy worker)."""
        if self.dry_run:
            raise KitError('A dry run opens no connection')
        secrets = self.secrets('runner')
        return lambda: secrets.connect(self.connector)

    def close(self):
        """Close every session this factory opened."""
        for session in self.opened:
            session.close()
        self.opened = []


class Log:
    """Timestamped, redacted lines to a stream and optionally to a 0600 file."""

    def __init__(self, redactor=None, stream=None, path=None, verbose=False):
        self.redactor = redactor or Redactor()
        self.stream = stream or sys.stdout
        self.verbose = verbose
        self.lock = threading.Lock()
        self.path = Path(path) if path else None
        self._file = None
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(str(self.path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, LOG_FILE_MODE)
            self._file = os.fdopen(fd, 'a', encoding='utf8')

    def __call__(self, msg):
        line = iso(utc_now()) + ' ' + self.redactor(msg)
        with self.lock:
            print(line, file=self.stream, flush=True)
            if self._file is not None:
                self._file.write(line + '\n')
                self._file.flush()

    def plain(self, msg):
        """A line without the timestamp."""
        self._plain(self.redactor(msg))

    def block(self, msg):
        """A line of an admin block the operator copies: secrets masked, SQL user names kept."""
        self._plain(self.redactor(msg, soft=False))

    def _plain(self, text):
        with self.lock:
            print(text, file=self.stream, flush=True)
            if self._file is not None:
                self._file.write(text + '\n')
                self._file.flush()

    def debug(self, msg):
        """Log the line only when verbose is on."""
        if self.verbose:
            self(msg)

    def file(self, msg):
        """A timestamped, redacted line to the log file only (the raw text of a driver error); nothing without a file path.

        Works after close() too, by appending to the path, since the command's last line may come
        after the log was closed.
        """
        if self.path is None:
            return
        line = iso(utc_now()) + ' ' + self.redactor(msg)
        with self.lock:
            if self._file is not None:
                self._file.write(line + '\n')
                self._file.flush()
                return
            fd = os.open(str(self.path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, LOG_FILE_MODE)
            with os.fdopen(fd, 'a', encoding='utf8') as handle:
                handle.write(line + '\n')

    def close(self):
        """Close the log file when one is open."""
        if self._file is not None:
            self._file.close()
            self._file = None


# ---------------------------------------------------------------------------
# Driver errors and the keep-alive thread
# ---------------------------------------------------------------------------
# The driver numbers of a connection reset: unreachable (2003), no socket (2004), gone away between
# statements (2006), lost during a statement (2013). A node replaced by autoscale, a restart or a
# resume shows as one of them.
DRIVER_RESET_ERRNOS = (2003, 2004, 2006, 2013)
DRIVER_RESET_LINE = 'a node reset the connection; the workspace may be scaling or restarting; rerun the step'
KEEPALIVE_SQL = 'SELECT 1'


def driver_errno(exc):
    """The driver error number of an exception, or None when it carries none."""
    args = getattr(exc, 'args', ())
    if args and isinstance(args[0], int) and not isinstance(args[0], bool):
        return args[0]
    return None


def is_driver_error(exc):
    """True for a connection reset the driver reports with one of DRIVER_RESET_ERRNOS."""
    return driver_errno(exc) in DRIVER_RESET_ERRNOS


class KeepAlive:
    """A daemon thread that sends SELECT 1 on a fresh runner connection every interval while a step runs.

    Auto-suspend counts queries from connections as activity, so a long step with no statement
    for a while (a wait, a slow block) must not leave the workspace idle. Each ping opens its own
    connection and closes it (B4), so an aggregator restart between pings costs nothing, and a
    short step opens none; a ping that fails is logged with the error number and the redacted
    message. run_step starts it and stops it at the end of the step; 0 minutes disables it. clock
    and poll are injected by the tests (a fake clock, a short poll).
    """

    def __init__(self, opener, minutes, log=None, clock=None, poll=1.0, guard=None):
        from .sqlutil import read_guard
        self.opener = opener
        self.seconds = float(minutes) * 60.0
        self.log = log
        self.clock = clock or time.monotonic
        self.poll = float(poll)
        self.guard = guard or read_guard('keep-alive')
        self.stopped = threading.Event()
        self.thread = None
        self.sent = []
        self.failures = 0

    def start(self):
        """Start the thread; returns self."""
        self.thread = threading.Thread(target=self._loop, name='keep-alive', daemon=True)
        self.thread.start()
        return self

    def stop(self):
        """Stop the thread and wait for it."""
        self.stopped.set()
        if self.thread is not None:
            self.thread.join(timeout=max(5.0, self.poll * 5))

    def ping(self):
        """Send SELECT 1 once on a new connection, then close it; a failure is logged with its number and message, never raised."""
        session = None
        try:
            session = Session(self.opener(), self.guard, self.log)
            session.query(KEEPALIVE_SQL)
            self.sent.append(self.clock())
            if self.log is not None:
                self.log('keep-alive: %s sent' % KEEPALIVE_SQL)
        except Exception as exc:  # noqa: BLE001 - the keep-alive never ends a step; the failure is logged
            self.failures += 1
            if self.log is not None:
                self.log('keep-alive: %s failed: %s error %s: %s' % (KEEPALIVE_SQL, type(exc).__name__, driver_errno(exc) or 'without a number', exc))
        finally:
            if session is not None:
                session.close()

    def _loop(self):
        due = self.clock() + self.seconds
        while not self.stopped.is_set():
            remaining = due - self.clock()
            if remaining > 0:
                if self.stopped.wait(min(remaining, self.poll)):
                    return
                continue
            self.ping()
            due = self.clock() + self.seconds
