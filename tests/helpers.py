"""s2rebuild tests helpers: fixture configuration builder, fake connection and plan objects.

Author: John Bergamini
"""
import contextlib
import copy
import datetime as dt
import io
from pathlib import Path

import yaml

from s2rebuild import cli
from s2rebuild.config import build_config, load_config

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
FIXTURE = HERE / 'fixtures' / 'config.yaml'
RUN_DATE = dt.date(2026, 11, 1)


def fixture_data():
    """The fixture YAML as a fresh mapping."""
    return yaml.safe_load(FIXTURE.read_text(encoding='utf8'))


def config_from(mutate=None, run_date=RUN_DATE, root=None):
    """A Config built from the fixture mapping after an optional mutation, without touching disk."""
    data = fixture_data()
    if mutate is not None:
        mutate(data)
    return build_config(data, path=FIXTURE, run_date=run_date, root=root or ROOT)


def write_config(tmp_dir, mutate=None, run_date=RUN_DATE):
    """Write a fixture-derived config into tmp_dir with an absolute runs_dir and return (path, Config)."""
    tmp_dir = Path(tmp_dir)
    data = fixture_data()
    data.setdefault('paths', {})['runs_dir'] = str(tmp_dir / 'runs')
    if mutate is not None:
        mutate(data)
    path = tmp_dir / 'config.yaml'
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding='utf8')
    return path, load_config(path, run_date=run_date, root=tmp_dir)


class Plan:
    """A table plan with the attributes the journal reads (copy.TablePlan arrives in M3)."""

    def __init__(self, name, est_rows, n_buckets, key_kind='key', key_expr='`id`', columns=('id', 'v'), **extra):
        self.name = name
        self.est_rows = est_rows
        self.n_buckets = n_buckets
        self.key_kind = key_kind
        self.key_expr = key_expr
        self.columns = list(columns)
        for k, v in extra.items():
            setattr(self, k, v)


def plans():
    return [Plan('t_big', 25_000_000, 3, size_gib=12.5, time_column='updated_at'),
            Plan('t_small', 100, 1, columns=('a', 'b', 'c')),
            Plan('t_nokey', 12_000_000, 2, key_kind='row', key_expr="CONCAT_WS('|', COALESCE(`x`, ''))", columns=('x',))]


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rows = []
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, args=None):
        self.conn.statements.append((sql, args))
        self.rows = copy.deepcopy(self.conn.responses.get(' '.join(str(sql).split()), []))
        self.rowcount = self.conn.rowcounts.get(' '.join(str(sql).split()), len(self.rows))

    def fetchall(self):
        return self.rows


class FakeConn:
    """Records statements; answers from a dict of normalized SQL to rows."""

    def __init__(self, responses=None, rowcounts=None):
        self.statements = []
        self.responses = responses or {}
        self.rowcounts = rowcounts or {}
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


FAULT_POINTS = ('before', 'after', 'journal')


def crash_journal(test, journal, method, match=None, times=1, exc=None):
    """Fail a Journal method before it writes: the third failure point (statement done, journal not updated).

    method names a Journal method such as set_meta, event, step_end or mark_done; match, when
    given, is called with the method's arguments and must return True for the call to fail. The
    default exception is SimulatedCrash (kill -9: not an Exception, so no except Exception handler can
    journal it; steps.run_step still records the step failed, where a real kill -9 leaves it running).
    Returns a list that holds the arguments of every call that failed. The patch is removed at cleanup.
    """
    from tests.fakedb import SimulatedCrash
    original = getattr(journal, method)
    fired = []
    left = [int(times)]

    def failing(*args, **kwargs):
        if left[0] > 0 and (match is None or match(*args, **kwargs)):
            left[0] -= 1
            fired.append((args, kwargs))
            raise (exc or SimulatedCrash)('killed before the journal update: %s%r' % (method, args))
        return original(*args, **kwargs)
    setattr(journal, method, failing)
    test.addCleanup(lambda: journal.__dict__.pop(method, None))
    return fired


def crash_journals(test, method, match=None, after=False):
    """crash_journal for every Journal object, those a command opens itself included (tests that drive the command line).

    The first call of the Journal method that match accepts raises SimulatedCrash; after=True
    lets that call write first, so the process dies just after the journal write. Returns the
    list of calls that failed. The patch is removed at cleanup.
    """
    from unittest import mock
    from s2rebuild.journal import Journal
    from tests.fakedb import SimulatedCrash
    original = getattr(Journal, method)
    fired = []

    def failing(self, *args, **kwargs):
        if not fired and (match is None or match(*args, **kwargs)):
            fired.append((args, kwargs))
            if after:
                original(self, *args, **kwargs)
            raise SimulatedCrash('killed %s the journal write: %s%r' % ('after' if after else 'before', method, args))
        return original(self, *args, **kwargs)
    patcher = mock.patch.object(Journal, method, failing)
    patcher.start()
    test.addCleanup(patcher.stop)
    return fired


def record_passed(config, run_id, step_ids=('P1', 'P2'), note='recorded for the test'):
    """Record steps passed in the on-disk journal of every configured database of run_id, as their gates and commands would.

    A step a person records gets a gate (P1, O1 to O3), a python step a passed result (P2 as
    s2rebuild check records it). Returns the journal paths.
    """
    from s2rebuild import steps
    from s2rebuild.journal import Journal
    paths = []
    for db in config.databases:
        path = config.runs_dir() / run_id / db.source / 'journal.sqlite'
        journal = Journal(path)
        try:
            for step_id in step_ids:
                if steps.REGISTRY[step_id].needs_person:
                    journal.record_gate(step_id, True, note)
                else:
                    journal.step_end(step_id, 'passed', note)
        finally:
            journal.close()
        paths.append(path)
    return paths


def fake_driver(test, fake):
    """Stand in for the PyMySQL module so a live command on the command line opens its connections on fake (a FakeDB).

    No network connection is opened: connect returns fake.connect(user=...). The real module is
    put back at cleanup.
    """
    import sys
    import types
    module = types.ModuleType('pymysql')
    cursors = types.ModuleType('pymysql.cursors')
    cursors.DictCursor = object
    module.cursors = cursors
    module.connect = lambda **kw: fake.connect(user=kw.get('user'))
    saved = {k: sys.modules.get(k) for k in ('pymysql', 'pymysql.cursors')}

    def restore():
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
    sys.modules['pymysql'] = module
    sys.modules['pymysql.cursors'] = cursors
    test.addCleanup(restore)
    return module


def run_cli(argv, env=None):
    """Run the command line in-process; return (exit code, captured text)."""
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        try:
            code = cli.main(list(argv), env=dict(env or {}), stream=out)
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else 1
    return code, out.getvalue()
