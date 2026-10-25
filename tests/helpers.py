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


def run_cli(argv, env=None):
    """Run the command line in-process; return (exit code, captured text)."""
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        try:
            code = cli.main(list(argv), env=dict(env or {}), stream=out)
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else 1
    return code, out.getvalue()
