"""s2rebuild connect: secrets, redaction, connections, guarded sessions and the log.

Author: John Bergamini

Secrets reads a password from the macOS Keychain or from an environment variable and keeps
it in memory only. Redactor masks host, user, password and CA path in every line that is
printed or written. Session applies the active step's Guard to every statement before it
reaches the server. DryRunSession records statements and issues nothing. Statements issued
by this module: none of its own; it forwards the statements the steps hand to a Session.
"""
import getpass
import os
import subprocess
import sys
import threading
from pathlib import Path

from .common import KitError, iso, utc_now

KEYCHAIN_TOOL = 'security'
LOG_FILE_MODE = 0o600


class Redactor:
    """Replaces every connection setting value in text before it is printed.

    soft values (the SQL user names) are masked in log lines but kept in the admin blocks the
    operator copies, since a block that names a user cannot run with the name masked.
    """

    def __init__(self, values=(), soft=()):
        self.soft = {str(v) for v in soft if v}
        self.values = sorted({str(v) for v in values if v} | self.soft, key=len, reverse=True)

    def __call__(self, text, soft=True):
        text = str(text)
        for v in self.values:
            if not soft and v in self.soft:
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

    def connect(self, connector=None):
        """Open a connection. connector defaults to pymysql.connect; tests pass a fake."""
        if not Path(self.ca).is_file():
            raise KitError('CA file not found at the configured workspace.ca_file path')
        if connector is None:
            import pymysql
            connector = pymysql.connect
        return connector(**self.connect_kwargs())


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

    def close(self):
        """Close the log file when one is open."""
        if self._file is not None:
            self._file.close()
            self._file = None
