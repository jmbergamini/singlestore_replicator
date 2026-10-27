"""s2rebuild tests test_copy: the 27 reference tests ported to the kit plus the section 10.9 changes.

Author: John Bergamini

Port map (reference test, new test): predicate shapes, bucket rule, identifiers, bucket coverage,
reference inventory shape (now the neutral reference journal), dry run opens no connection, shape
must match the capture, names must differ, write guard, no forbidden statements in the source,
every write names the replacement, full copy with count proof and verify, resume after a kill,
reconciliation after a server commit, retry before the statement, abort deadline and stop file,
durability refusal, missing source, replacement durability, non-empty unjournaled target, rows
not explained by the journal, AUTO_INCREMENT counter is not a difference, definition difference,
journal from another plan, redaction, missing settings named without values, connection errors
redacted. New: bucket cap, lock wait requeue and serialization, lock probe, concurrency limit,
fresh-table fallback, retry of parked chunks, count proof mismatch.
"""
import contextlib
import datetime as dt
import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path

from s2rebuild import copy as C
from s2rebuild import estimate as E
from s2rebuild import inventory as I
from s2rebuild import steps
from s2rebuild.common import EXIT_DONE, EXIT_GATE, EXIT_INCOMPLETE, EXIT_MISMATCH, EXIT_REFUSED, KitError, auto_table_limit
from s2rebuild.config import load_config
from s2rebuild.connect import Log, Redactor, Secrets, SessionFactory
from s2rebuild.journal import Journal
from s2rebuild.sqlutil import GuardError, classify, ident, runner_guard
from tests.fakedb import FakeDB, SimulatedCrash, evaluate_predicate, multiset
from tests.helpers import FAULT_POINTS, ROOT, RUN_DATE, crash_journal, fixture_data

SRC, TGT = 'example_orders_db', 'example_orders_db_rebuild_20261101'
REFERENCE_JOURNAL = ROOT / 'tests' / 'fixtures' / 'reference-run' / 'copy-journal.sqlite'

FIXTURE_TABLES = {
    # name: (columns with types, primary key columns, estimated rows, create text)
    't_single': ([('id', 'varchar(100)', False), ('payload', 'varchar(200)', True), ('n', 'int(11)', True)], ['id'], 25_000_000,
                 'CREATE TABLE `t_single` (\n  `id` varchar(100) NOT NULL,\n  `payload` varchar(200) DEFAULT NULL,\n'
                 '  `n` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n)'),
    't_comp': ([('a', 'varchar(64)', False), ('b', 'datetime(6)', False), ('c', 'int(11)', False), ('note', 'varchar(100)', True)], ['a', 'b', 'c'], 100,
               'CREATE TABLE `t_comp` (\n  `a` varchar(64) NOT NULL,\n  `b` datetime(6) NOT NULL,\n  `c` int(11) NOT NULL,\n'
               '  `note` varchar(100) DEFAULT NULL,\n  PRIMARY KEY (`a`,`b`,`c`)\n)'),
    't_nokey': ([('x', 'int(11)', True), ('y', 'varchar(50)', True), ('z', 'double', True)], [], 12_000_000,
                'CREATE TABLE `t_nokey` (\n  `x` int(11) DEFAULT NULL,\n  `y` varchar(50) DEFAULT NULL,\n  `z` double DEFAULT NULL\n)'),
    't_empty': ([('p', 'varchar(10)', True)], [], 0, 'CREATE TABLE `t_empty` (\n  `p` varchar(10) DEFAULT NULL\n)'),
}


def fixture_rows():
    rows = {'t_single': [], 't_comp': [], 't_nokey': [], 't_empty': []}
    for i in range(700):
        rows['t_single'].append({'id': 'id-%d' % i, 'payload': 'p%d' % i, 'n': i})
    for i in range(300):
        rows['t_comp'].append({'a': 'a%d' % (i % 7), 'b': '2026-01-01 00:00:%02d.%06d' % (i % 60, i), 'c': i, 'note': None})
    for i in range(250):
        row = {'x': i, 'y': None if i % 5 == 0 else 'y%d' % i, 'z': None if i % 3 == 0 else i / 7.0}
        rows['t_nokey'].append(row)
        rows['t_nokey'].append(dict(row))
    return rows


def fake_db(tables=FIXTURE_TABLES, rows=None):
    rows = fixture_rows() if rows is None else rows
    return FakeDB(SRC, {name: {'create': spec[3], 'rows': rows.get(name, [])} for name, spec in tables.items()})


def fixture_inventory(tables=FIXTURE_TABLES, database=SRC, step='P3'):
    """An inventory dict in the capture layout for the fixture tables."""
    out = []
    for name, (columns, pk, est, create) in tables.items():
        cols = [{'name': c, 'ordinal': i, 'data_type': t.split('(')[0], 'column_type': t, 'nullable': nullable, 'default': None, 'extra': '',
                 'generation_expression': ''} for i, (c, t, nullable) in enumerate(columns, 1)]
        keys = [{'name': 'PRIMARY', 'unique': True, 'type': 'HASH', 'columns': pk}] if pk else []
        out.append({'name': name, 'columns': cols, 'keys': keys, 'definition': create, 'storage_type': 'rowstore', 'auto_increment': None,
                    'est_rows': est, 'exact_rows': None, 'size_gib': est / 1e8, 'computed_columns': []})
    return {'kit': 's2rebuild', 'version': 1, 'database': database, 'captured_utc': '2026-10-20T12:00:00Z', 'step': step,
            'layout': {'NUM_PARTITIONS': 16, 'NUM_SUB_PARTITIONS': 0, 'IS_SYNC': 1, 'IS_SYNC_DURABILITY': 0, 'IS_SYNC_DURABILITY_ON_REF_DB_ONLY': 1},
            'attachment': None, 'tables': out, 'views': [], 'routines': [], 'accounts': [], 'sessions': [],
            'size_gib_total': 0.0, 'est_rows_total': sum(t[2] for t in tables.values()),
            'pipelines': {'captured': False, 'items': [], 'note': 'no owner profile'}}


@contextlib.contextmanager
def quiet_thread_exceptions():
    old = threading.excepthook
    threading.excepthook = lambda args: None
    try:
        yield
    finally:
        threading.excepthook = old


class Harness:
    """A run folder with a P3 capture of the fixture tables, a fake workspace and a session factory."""

    def __init__(self, test, tables=FIXTURE_TABLES, rows=None, mutate=None, run_id='20261101-0500', mutate_inventory=None):
        self.tmp = Path(tempfile.mkdtemp())
        test.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        data = fixture_data()
        data['paths'] = {'runs_dir': str(self.tmp / 'runs')}
        data['copy'] = {'workers': 2, 'progress_seconds': 1}
        if mutate:
            mutate(data)
        import yaml
        (self.tmp / 'certs').mkdir()
        (self.tmp / 'certs' / 'singlestore_bundle.pem').write_text('not a real certificate')
        self.config_path = self.tmp / 'config.yaml'
        self.config_path.write_text(yaml.safe_dump(data, sort_keys=False))
        self.config = load_config(self.config_path, run_date=RUN_DATE, root=self.tmp)
        self.db = self.config.databases[0]
        self.run_id = run_id
        self.inventory = fixture_inventory(tables)
        if mutate_inventory:
            mutate_inventory(self.inventory)
        plans = I.plan_tables(self.inventory, self.config, self.db)
        I.write(self.inventory, I.inventory_dir(self.config, run_id, self.db, 'P3'), plans)
        self.fake = fake_db(tables, rows)
        self.out = io.StringIO()
        self.log = Log(stream=self.out)
        self.env = {'S2REBUILD_RUNNER_PASSWORD': 'fixture-password'}
        self.journal_path = self.tmp / 'runs' / run_id / self.db.source / 'journal.sqlite'
        self.journal = Journal(self.journal_path)
        test.addCleanup(self.journal.close)
        for step_id in ('O1', 'O2', 'O3'):
            self.journal.record_gate(step_id, True, 'recorded for the test')
        self.journal.step_end('O4', 'passed', 'capture written by the test harness')
        self.sessions = SessionFactory(self.config, self.env, self.log, dry_run=False, connector=lambda **kw: self.fake.connect(user=kw.get('user')))

    def plans(self):
        return I.plan_tables(self.inventory, self.config, self.db)

    def ctx(self, dry_run=False, options=None, yes=False):
        def never(**kw):
            raise AssertionError('a dry run must not connect')
        return steps.StepContext(self.config, self.db, self.journal, self.run_id, self.log, dry_run=dry_run, yes=yes, options=options,
                                 sessions=self.sessions if not dry_run else SessionFactory(self.config, self.env, self.log, dry_run=True, connector=never))

    def step(self, step_id, redo=True, **kw):
        return steps.run_step(steps.REGISTRY[step_id], self.ctx(**kw), redo=redo)

    def o5_o6(self, **kw):
        out5 = self.step('O5', **kw)
        assert out5.status == steps.PASSED, out5.note
        return self.step('O6', **kw)

    def states(self):
        return {(t, b): (s, r) for t, b, s, r, _ in self.journal.chunk_states()}

    def text(self):
        return self.out.getvalue()


# ---------------------------------------------------------------------------
# Ported: predicates, buckets, identifiers, coverage
# ---------------------------------------------------------------------------
class PredicateTests(unittest.TestCase):
    def test_predicate_shapes(self):
        single = I.TablePlan('t', ['id', 'v'], 25_000_000, 'key', ['id'], '', 3)
        self.assertEqual(single.predicate(2), 'CRC32(`id`) MOD 3 = 2')
        comp = I.TablePlan('t', ['a', 'b', 'c', 'v'], 10, 'key', ['a', 'b', 'c'], '', 1)
        self.assertEqual(comp.predicate(0), "CRC32(CONCAT_WS('|', `a`, `b`, `c`)) MOD 1 = 0")
        nokey = I.TablePlan('t', ['x', 'y'], 12_000_000, 'row', [], '', 2)
        self.assertEqual(nokey.predicate(1), "CRC32(CONCAT_WS('|', COALESCE(`x`, ''), COALESCE(`y`, ''))) MOD 2 = 1")
        with self.assertRaises(KitError):
            single.predicate(3)
        with self.assertRaises(KitError):
            single.predicate(-1)

    def test_bucket_rule_with_and_without_the_cap(self):
        self.assertEqual(I.buckets_for(0, 10_000_000, 5_000_000, 1000), 1)
        self.assertEqual(I.buckets_for(4_999_999, 10_000_000, 5_000_000, 1000), 1)
        self.assertEqual(I.buckets_for(5_000_000, 10_000_000, 5_000_000, 1000), 1)
        self.assertEqual(I.buckets_for(25_000_000, 10_000_000, 5_000_000, 1000), 3)
        self.assertEqual(I.buckets_for(1_334_952_052, 10_000_000, 5_000_000, 1000), 134)
        self.assertEqual(I.buckets_for(1_334_952_052, 10_000_000, 5_000_000, 32), 32, 'the section 10.6 cap')
        with self.assertRaises(KitError):
            I.buckets_for(-1, 10_000_000, 5_000_000, 32)

    def test_identifiers(self):
        self.assertEqual(ident('2026_05_growth_1_c_trials_per'), '`2026_05_growth_1_c_trials_per`')
        for bad in ('a`b', 'a b', 'a;b', '', 'a-b'):
            with self.assertRaises(KitError):
                ident(bad)

    def test_complete_coverage_of_buckets(self):
        db = fake_db()
        cfg = load_config(ROOT / 'tests' / 'fixtures' / 'config.yaml', run_date=RUN_DATE, root=ROOT)
        for p in I.plan_tables(fixture_inventory(), cfg, cfg.databases[0]):
            for row in db.rows(SRC, p.name):
                hits = [b for b in range(p.n_buckets) if evaluate_predicate(p.predicate(b), row)]
                self.assertEqual(len(hits), 1, 'row must fall in exactly one bucket: %s %r' % (p.name, row))


class ReferencePlanTests(unittest.TestCase):
    def test_reference_run_plan_is_reproduced(self):
        """The reference journal's bucket counts come out of the kit's bucket rule without the cap."""
        conn = sqlite3.connect('file:%s?mode=ro' % REFERENCE_JOURNAL, uri=True)
        try:
            rows = conn.execute('SELECT table_name, est_rows, n_buckets, key_kind FROM tables ORDER BY est_rows DESC').fetchall()
        finally:
            conn.close()
        self.assertEqual(len(rows), 95)
        self.assertEqual(sum(r[2] for r in rows), 382)
        self.assertEqual(rows[0][2], 134, 'largest table')
        self.assertEqual(sum(1 for r in rows if r[1] > 100_000_000), 6)
        self.assertEqual(sum(1 for r in rows if r[3] == 'key'), 83)
        self.assertEqual(sum(1 for r in rows if r[3] == 'row'), 12)
        for name, est, buckets, _ in rows:
            self.assertEqual(I.buckets_for(est, 10_000_000, 5_000_000, 1000), buckets, name)
            if est >= 5_000_000:
                self.assertTrue(5_000_000 <= est / buckets <= 20_000_000, (name, est / buckets))
        capped = sum(I.buckets_for(est, 10_000_000, 5_000_000, 32) for _, est, _, _ in rows)
        self.assertLess(capped, 382, 'the cap reduces the chunk count of the reference plan')


# ---------------------------------------------------------------------------
# Ported: guard and source scan
# ---------------------------------------------------------------------------
class GuardTests(unittest.TestCase):
    def test_write_guard(self):
        g = runner_guard(TGT)
        g.check('INSERT INTO `%s`.`t` (`a`) SELECT `a` FROM `%s`.`t` WHERE 1' % (TGT, SRC))
        g.check('SELECT COUNT(*) AS n FROM `%s`.`t`' % SRC)
        g.check('SHOW CREATE TABLE `%s`.`t`' % TGT)
        for bad in ['INSERT INTO `%s`.`t` (`a`) SELECT `a` FROM `%s`.`t`' % (SRC, TGT), 'DELETE FROM `%s`.`t` WHERE 1' % SRC,
                    'CREATE TABLE `%s`.`t` LIKE `%s`.`t`' % (SRC, TGT), 'UPDATE `%s`.`t` SET a = 1' % TGT,
                    'ALTER TABLE `%s`.`t` ADD COLUMN b int' % TGT]:
            with self.assertRaises(GuardError, msg=bad):
                g.check(bad)

    def test_copy_source_has_no_forbidden_statements(self):
        text = (ROOT / 's2rebuild' / 'copy.py').read_text().lower()
        text = text.replace(C.HARD_STOP_HINT.lower(), '')  # the hint names the operating system kill; copy issues no KILL statement
        for word in ('detach', 'attach', 'truncate', 'pipeline', 'grant', 'revoke', 'kill'):
            self.assertNotIn(word, text, word)
        self.assertNotIn('drop database', text)
        self.assertNotIn('drop user', text)

    def test_all_writes_name_the_replacement_first(self):
        h = Harness(self)
        self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
        self.assertTrue(h.fake.writes)
        for w in h.fake.writes:
            st = classify(w)
            self.assertEqual(st.databases[0], TGT, w)


# ---------------------------------------------------------------------------
# Ported: the copy itself
# ---------------------------------------------------------------------------
class CopyTests(unittest.TestCase):
    def test_full_copy_count_proof_and_verify(self):
        from s2rebuild import verify as V
        h = Harness(self)
        outcome = h.o5_o6()
        self.assertEqual(outcome.status, steps.PASSED, h.text())
        for name in FIXTURE_TABLES:
            self.assertEqual(multiset(h.fake.rows(TGT, name)), multiset(h.fake.rows(SRC, name)), name)
        states = h.states()
        self.assertEqual({s for s, _ in states.values()}, {'done'})
        self.assertEqual(len(states), 3 + 1 + 2 + 1)
        self.assertEqual(sum(r for _, r in states.values()), sum(len(v) for v in fixture_rows().values()))
        proof = Path(str(h.journal_path) + '.counts.txt').read_text()
        self.assertIn('all_tables_match\tyes', proof)
        self.assertIn('t_nokey\t500\t500\tyes', proof)
        self.assertEqual(h.fake.databases[TGT]['layout']['NUM_PARTITIONS'], 32)
        self.assertEqual(h.fake.databases[TGT]['layout']['NUM_SUB_PARTITIONS'], 512)
        self.assertEqual(h.journal.step_status('O6'), 'passed')
        session = h.sessions.runner(runner_guard(TGT))
        results = V.Verifier(session, SRC, TGT, h.plans(), h.journal, h.config).run()
        self.assertTrue(all(r.ok for r in results), [r for r in results if not r.ok])
        self.assertTrue(all(t['verified_at_utc'] for t in h.journal.tables()))
        writes_before = len(h.fake.writes)
        self.assertEqual(h.step('O6').status, steps.PASSED, 'a rerun on a complete journal does nothing destructive')
        self.assertEqual(len(h.fake.writes), writes_before)

    def test_resume_after_kill_mid_chunk(self):
        h = Harness(self)
        self.assertEqual(h.step('O5').status, steps.PASSED, h.text())
        plans = h.plans()
        statements = C.statements_for(h.db)

        def crash(table, bucket):
            if (table, bucket) == ('t_single', 1):
                raise SimulatedCrash()
        runner = C.Runner(plans, statements, h.journal, h.fake.connect, h.log, h.config.copy, workers=1, after_insert_hook=crash,
                          sleep=lambda s: None)
        with quiet_thread_exceptions():
            self.assertFalse(runner.copy())
        states = h.states()
        self.assertEqual(states[('t_single', 1)][0], 'started', 'kill -9 leaves the chunk started')
        self.assertTrue(any(s == 'pending' for s, _ in states.values()))
        self.assertGreater(len(h.fake.rows(TGT, 't_single')), 0, 'the server committed the chunk before the client died')
        outcome = h.step('O6')
        self.assertEqual(outcome.status, steps.PASSED, h.text())
        expected_removed = sum(1 for r in fixture_rows()['t_single'] if evaluate_predicate('CRC32(`id`) MOD 3 = 1', r))
        self.assertEqual(h.journal.events('reconcile_delete'), ['t_single bucket 1 removed %d rows before re-insert' % expected_removed])
        for name in FIXTURE_TABLES:
            self.assertEqual(multiset(h.fake.rows(TGT, name)), multiset(h.fake.rows(SRC, name)), name)
        self.assertEqual({s for s, _ in h.states().values()}, {'done'})
        self.assertEqual([w for w in h.fake.writes if w.startswith('DELETE')], ['DELETE FROM `%s`.`t_single` WHERE CRC32(`id`) MOD 3 = 1' % TGT])

    def test_failed_chunk_after_server_commit_is_reconciled(self):
        h = Harness(self)
        pred = "CRC32(CONCAT_WS('|', COALESCE(`x`, ''), COALESCE(`y`, ''), COALESCE(`z`, ''))) MOD 2 = 0"
        h.fake.fail_insert[('t_nokey', pred)] = 'after'
        outcome = h.o5_o6()
        self.assertEqual((outcome.status, outcome.exit_code), (steps.INCOMPLETE, EXIT_INCOMPLETE), h.text())
        self.assertEqual(h.states()[('t_nokey', 0)][0], 'failed')
        self.assertEqual(h.step('O6').status, steps.PASSED, h.text())
        self.assertEqual(multiset(h.fake.rows(TGT, 't_nokey')), multiset(h.fake.rows(SRC, 't_nokey')))
        self.assertEqual(len(h.journal.events('reconcile_delete')), 1)

    def test_failed_chunk_before_statement_is_retried(self):
        h = Harness(self)
        h.fake.fail_insert[('t_single', 'CRC32(`id`) MOD 3 = 0')] = 'before'
        self.assertEqual(h.o5_o6().exit_code, EXIT_INCOMPLETE)
        self.assertEqual(h.step('O6').status, steps.PASSED)
        self.assertEqual(multiset(h.fake.rows(TGT, 't_single')), multiset(h.fake.rows(SRC, 't_single')))

    def test_abort_deadline_and_stop_file(self):
        h = Harness(self, mutate=lambda d: d['window'].update(start='2000-01-01T00:00:00Z', end='2000-01-01T08:00:00Z'))
        outcome = h.o5_o6()
        self.assertEqual(outcome.exit_code, EXIT_INCOMPLETE, h.text())
        self.assertIn('Abort deadline reached', h.text())
        self.assertEqual({s for s, _ in h.states().values()}, {'pending'})
        self.assertFalse(any(w.startswith('INSERT') for w in h.fake.writes))
        h2 = Harness(self)
        self.assertEqual(h2.step('O5').status, steps.PASSED)
        stop = C.stop_file_for(h2.journal_path)
        stop.write_text('stop')
        self.assertEqual(h2.step('O6').exit_code, EXIT_INCOMPLETE)
        self.assertIn('Stop file present', h2.text())
        self.assertFalse(any(w.startswith('INSERT') for w in h2.fake.writes))
        stop.unlink()
        self.assertEqual(h2.step('O6').status, steps.PASSED)
        h2.journal.set_meta('abort_deadline', '2000-01-01T00:00:00Z')
        self.assertEqual(C.resolve_abort_at(h2.journal, h2.config), dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc), 'the O1 record wins')


# ---------------------------------------------------------------------------
# Ported: refusals
# ---------------------------------------------------------------------------
class RefusalTests(unittest.TestCase):
    def test_refuses_source_layout_that_differs_from_the_capture(self):
        for key, bad in [('IS_SYNC_DURABILITY', 1), ('IS_SYNC', 0), ('IS_SYNC_DURABILITY_ON_REF_DB_ONLY', 0), ('NUM_PARTITIONS', 8)]:
            h = Harness(self)
            h.fake.databases[SRC]['layout'][key] = bad
            outcome = h.step('O5')
            self.assertEqual((outcome.status, outcome.exit_code), (steps.REFUSED, EXIT_REFUSED), key)
            self.assertIn('differs from the capture', h.text())
            self.assertEqual(h.fake.writes, [], 'no write may run after a layout refusal')

    def test_refuses_missing_source(self):
        h = Harness(self)
        h.fake.databases = {'some_other_db': h.fake.databases[SRC]}
        outcome = h.step('O5')
        self.assertEqual(outcome.status, steps.REFUSED)
        self.assertIn('not reported', h.text())

    def test_refuses_replacement_durability_mismatch(self):
        h = Harness(self)
        h.fake.target_layout_override = {'IS_SYNC_DURABILITY': 1}
        self.assertEqual(h.step('O5').status, steps.REFUSED)
        self.assertIn('durability flag', h.text())
        self.assertFalse(any(w.startswith('CREATE TABLE') for w in h.fake.writes))

    def test_refuses_nonempty_unjournaled_target(self):
        h = Harness(self)
        src = h.fake.databases[SRC]
        h.fake.databases[TGT] = {'layout': dict(src['layout'], NUM_PARTITIONS=32, NUM_SUB_PARTITIONS=512), 'views': {}, 'tables': {
            name: {'create': t['create'], 'rows': [dict(r) for r in t['rows'][:3]], 'auto_increment': None} for name, t in src['tables'].items()}}
        self.assertEqual(h.step('O5').status, steps.REFUSED)
        self.assertIn('does not account', h.text())
        self.assertFalse(any(w.startswith(('INSERT', 'DELETE')) for w in h.fake.writes))

    def test_refuses_target_rows_not_explained_by_journal(self):
        from s2rebuild import verify as V
        h = Harness(self)
        self.assertEqual(h.o5_o6().status, steps.PASSED)
        h.fake.rows(TGT, 't_comp').append({'a': 'zzz', 'b': '2026-02-02 00:00:00.000000', 'c': 99999, 'note': 'stray'})
        writes = len(h.fake.writes)
        session = h.sessions.runner(runner_guard(TGT))
        results = V.Verifier(session, SRC, TGT, h.plans(), h.journal, h.config).run()
        self.assertTrue(any(r.result == V.MISMATCH and r.table == 't_comp' for r in results), 'verification must flag the stray row')
        self.assertEqual(h.step('O6').status, steps.REFUSED, 'a rerun of the copy must refuse a row the journal cannot explain')
        self.assertIn('does not account', h.text())
        self.assertEqual(h.step('O5').status, steps.REFUSED, 'and so must O5')
        self.assertEqual(len(h.fake.writes), writes)

    def test_resume_without_redo_refuses_unexplained_rows_before_any_write(self):
        h = Harness(self)
        h.fake.fail_insert[('t_single', 'CRC32(`id`) MOD 3 = 0')] = 'before'
        self.assertEqual(h.o5_o6().exit_code, EXIT_INCOMPLETE)
        h.fake.rows(TGT, 't_comp').append({'a': 'stray', 'b': '2026-02-02 00:00:00.000000', 'c': 1, 'note': None})
        writes = len(h.fake.writes)
        outcome = h.step('O6', redo=False)
        self.assertEqual(outcome.status, steps.REFUSED, h.text())
        self.assertIn('does not account', outcome.note)
        self.assertEqual(len(h.fake.writes), writes, 'nothing is written after the refusal')
        h.fake.databases[SRC]['layout']['IS_SYNC_DURABILITY'] = 1
        h.fake.rows(TGT, 't_comp').pop()
        outcome = h.step('O6', redo=False)
        self.assertEqual(outcome.status, steps.REFUSED)
        self.assertIn('differs from the capture', outcome.note)

    def test_auto_increment_counter_value_is_not_a_difference(self):
        tables = dict(FIXTURE_TABLES)
        cols, pk, est, create = tables['t_comp']
        tables['t_comp'] = (cols, pk, est, create.replace('\n)', '\n) AUTO_INCREMENT=4503599627372497 AUTOSTATS_SAMPLING=ON'))
        h = Harness(self, tables=tables)
        h.fake.databases[SRC]['tables']['t_comp']['create'] = create.replace('\n)', '\n) AUTOSTATS_SAMPLING=ON')
        self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
        self.assertEqual(h.journal.events('auto_increment_counter'), ['t_comp saved AUTO_INCREMENT=4503599627372497; the counter is set at O7'])
        self.assertEqual(I.auto_increment_counter('CREATE TABLE `x` (`id` bigint NOT NULL AUTO_INCREMENT) AUTO_INCREMENT=12 X=1'), 12)
        self.assertIsNone(I.auto_increment_counter('CREATE TABLE `x` (`id` bigint NOT NULL AUTO_INCREMENT)'))

    def test_refuses_definition_difference(self):
        h = Harness(self)
        h.fake.databases[SRC]['tables']['t_comp']['create'] = h.fake.databases[SRC]['tables']['t_comp']['create'].replace(
            '`note` varchar(100)', '`note` varchar(120)')
        self.assertEqual(h.step('O5').status, steps.REFUSED)
        self.assertIn('differs from the captured definition', h.text())
        self.assertFalse(any(w.startswith('INSERT') for w in h.fake.writes))
        self.assertEqual(h.journal.events('definition_mismatch'), ['t_comp'])

    def test_refuses_journal_from_another_plan(self):
        h = Harness(self, mutate=lambda d: d['window'].update(start='2000-01-01T00:00:00Z', end='2000-01-01T08:00:00Z'))
        self.assertEqual(h.o5_o6().exit_code, EXIT_INCOMPLETE)
        other = dict(FIXTURE_TABLES)
        cols, pk, est, create = other['t_single']
        other['t_single'] = (cols, pk, 60_000_000, create)
        h.inventory = fixture_inventory(other)
        I.write(h.inventory, I.inventory_dir(h.config, h.run_id, h.db, 'O4'), h.plans())
        outcome = h.step('O6')
        self.assertEqual((outcome.status, outcome.exit_code), (steps.REFUSED, EXIT_REFUSED))
        self.assertIn('refusing to resume', h.text())
        with self.assertRaises(KitError):
            C.Statements(SRC, SRC, 32, 512)

    def test_shape_must_match_the_capture(self):
        h = Harness(self)
        self.assertEqual(h.step('O5').status, steps.PASSED)
        h.fake.databases[TGT]['tables']['stray_table'] = {'create': 'CREATE TABLE `stray_table` (a int)', 'rows': [], 'auto_increment': None}
        self.assertEqual(h.step('O5').status, steps.REFUSED)
        self.assertIn('not in the plan', h.text())


# ---------------------------------------------------------------------------
# Ported: secrets
# ---------------------------------------------------------------------------
class SecretTests(unittest.TestCase):
    ENV = {'HOST': 'svc-secret-host.example', 'USER': 'runner_user', 'PW': 'Sup3r-Secret-Pw', 'CA': '/x/ca/bundle.pem'}

    def test_redaction(self):
        s = Secrets('runner', self.ENV['USER'], self.ENV['HOST'], 3306, self.ENV['CA'], password_env='PW', env={'PW': self.ENV['PW']})
        out = io.StringIO()
        log = Log(redactor=s.redactor, stream=out)
        log('OperationalError: (1045, "Access denied for user %s@%s using %s ca=%s")' % (self.ENV['USER'], self.ENV['HOST'], self.ENV['PW'], self.ENV['CA']))
        text = out.getvalue()
        for v in self.ENV.values():
            self.assertNotIn(v, text)
        self.assertIn('<redacted>', text)
        self.assertNotIn('Sup3r', repr(s))

    def test_missing_settings_named_without_values(self):
        with self.assertRaisesRegex(KitError, 'S2REBUILD_RUNNER_PASSWORD'):
            Secrets('runner', 'u', 'h', 3306, 'c', password_env='S2REBUILD_RUNNER_PASSWORD', env={})

    def test_connection_errors_are_redacted_through_the_cli(self):
        from tests.helpers import run_cli
        h = Harness(self)
        fake_pymysql = types.ModuleType('pymysql')
        fake_cursors = types.ModuleType('pymysql.cursors')
        fake_cursors.DictCursor = object

        def connect(**kw):
            # the driver's own shape: the user stands as a quoted account, which the redactor masks (W6)
            raise RuntimeError("cannot reach %s for user '%s'@'%%' with %s" % (kw['host'], kw['user'], kw['password']))
        fake_pymysql.connect = connect
        fake_pymysql.cursors = fake_cursors
        saved = {k: sys.modules.get(k) for k in ('pymysql', 'pymysql.cursors')}
        sys.modules['pymysql'] = fake_pymysql
        sys.modules['pymysql.cursors'] = fake_cursors
        try:
            for step_id in ('O1', 'O2', 'O3', 'O4'):
                h.journal.record_gate(step_id, True, 'gated for the test') if steps.REGISTRY[step_id].needs_person else h.journal.step_end(step_id, 'passed', 'x')
            h.journal.close()
            code, text = run_cli(['step', 'O5', '--config', str(h.config_path), '--run', h.run_id], env=dict(h.env, S2REBUILD_RUNNER_PASSWORD='Sup3r-Secret-Pw'))
        finally:
            for k, v in saved.items():
                if v is None:
                    sys.modules.pop(k, None)
                else:
                    sys.modules[k] = v
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('cannot reach', text)
        for v in ('Sup3r-Secret-Pw', 'svc-example-dml.svc.singlestore.com', 's2rebuild_runner'):
            self.assertNotIn(v, text, v)


# ---------------------------------------------------------------------------
# New: section 10.9 changes
# ---------------------------------------------------------------------------
REF_CREATE = ('CREATE REFERENCE TABLE `t_ref` (\n  `k` int(11) DEFAULT NULL,\n  `label` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n'
              '  SORT KEY `k` (`k`)\n) AUTOSTATS_CARDINALITY_MODE=INCREMENTAL')
REF_TABLES = dict(FIXTURE_TABLES, t_ref=([('k', 'int(11)', True), ('label', 'varchar(20)', True)], [], 0, REF_CREATE))


def ref_harness(test):
    """The fixture tables plus one reference table with 7 rows, O5 passed and the source attached READ ONLY (after the freeze)."""
    rows = fixture_rows()
    rows['t_ref'] = [{'k': i, 'label': 'l%d' % i} for i in range(7)]
    h = Harness(test, tables=REF_TABLES, rows=rows)
    test.assertEqual(h.step('O5').status, steps.PASSED, h.text())
    h.fake.attachment(SRC)['ATTACH_TYPE'] = 'READONLY'
    return h


class ReferenceTableTests(unittest.TestCase):
    def ref_plan(self, h):
        return next(p for p in h.plans() if p.name == 't_ref')

    def test_a_plain_insert_into_a_reference_table_from_a_read_only_source_fails_with_1777(self):
        h = ref_harness(self)
        sql = C.statements_for(h.db).insert_chunk(self.ref_plan(h), 0)
        with self.assertRaises(Exception) as caught:
            h.fake.connect(user='s2rebuild_runner').cursor().execute(sql)
        self.assertEqual(caught.exception.args[0], 1777)
        self.assertIn('has no master instance', caught.exception.args[1])

    def test_reference_table_copies_through_a_staging_table(self):
        h = ref_harness(self)
        out = h.step('O6')
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual(multiset(h.fake.rows(TGT, 't_ref')), multiset(h.fake.rows(SRC, 't_ref')))
        self.assertNotIn('t_ref__stage', h.fake.databases[TGT]['tables'], 'the staging table is dropped')
        writes = [w for w in h.fake.writes if 't_ref' in w and ' LIKE ' not in w]
        self.assertTrue(writes[0].startswith('CREATE TABLE `%s`.`t_ref__stage` (`k` int(11) DEFAULT NULL, `label` varchar(20) CHARACTER SET' % TGT), writes)
        self.assertNotIn('KEY', writes[0])
        self.assertTrue(writes[1].startswith('INSERT INTO `%s`.`t_ref__stage`' % TGT))
        self.assertIn('FROM `%s`.`t_ref` WHERE' % SRC, writes[1])
        self.assertEqual(writes[2], 'INSERT INTO `%s`.`t_ref` (`k`, `label`) SELECT `k`, `label` FROM `%s`.`t_ref__stage`' % (TGT, TGT))
        self.assertEqual(writes[3], 'DROP TABLE `%s`.`t_ref__stage`' % TGT)
        self.assertEqual(h.journal.chunks('t_ref')[0]['rows_inserted'], 7)

    def test_a_staging_table_left_by_an_interrupted_attempt_is_dropped_first(self):
        h = ref_harness(self)
        h.fake.databases[TGT]['tables']['t_ref__stage'] = {'create': 'CREATE TABLE `t_ref__stage` (`k` int(11))', 'rows': [{'k': 99, 'label': 'x'}],
                                                           'auto_increment': None}
        self.assertEqual(h.step('O6').status, steps.PASSED, h.text())
        self.assertIn('Dropped the staging table t_ref__stage left by an interrupted attempt', h.text())
        self.assertEqual(multiset(h.fake.rows(TGT, 't_ref')), multiset(h.fake.rows(SRC, 't_ref')))

    def test_a_failed_staging_insert_drops_the_staging_table_and_the_rerun_copies(self):
        h = ref_harness(self)
        h.fake.fail_insert[('t_ref__stage', self.ref_plan(h).predicate(0))] = 'before'
        self.assertEqual(h.step('O6').exit_code, EXIT_INCOMPLETE, h.text())
        self.assertNotIn('t_ref__stage', h.fake.databases[TGT]['tables'])
        self.assertEqual(h.fake.rows(TGT, 't_ref'), [])
        self.assertEqual(h.step('O6').status, steps.PASSED, h.text())
        self.assertEqual(multiset(h.fake.rows(TGT, 't_ref')), multiset(h.fake.rows(SRC, 't_ref')))

    def test_detection_names_and_columns(self):
        h = ref_harness(self)
        plans = {p.name: p for p in h.plans()}
        self.assertTrue(plans['t_ref'].reference)
        self.assertFalse(plans['t_single'].reference)
        rowstore = I.TablePlan('r', ['a'], 0, 'row', [], 'CREATE ROWSTORE REFERENCE TABLE `r` (\n  `a` bigint(20) NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`a`)\n)', 1)
        self.assertTrue(rowstore.reference)
        self.assertEqual(C.stage_columns(rowstore), ['`a` bigint(20) NOT NULL'])
        long_plan = I.TablePlan('x' * 60, ['a'], 0, 'row', [], 'CREATE REFERENCE TABLE `x` (\n  `a` int(11)\n)', 1)
        with self.assertRaisesRegex(KitError, 'over the 64'):
            C.stage_name(long_plan)
        with self.assertRaisesRegex(KitError, 'planned table'):
            C.stage_name(plans['t_ref'], {'t_ref__stage'})
        broken = I.TablePlan('b', ['a', 'c'], 0, 'row', [], 'CREATE REFERENCE TABLE `b` (\n  `a` int(11)\n)', 1)
        with self.assertRaisesRegex(KitError, 'no definition line for c'):
            C.stage_columns(broken)

    def test_w4_the_capture_on_the_fake_counts_the_reference_table_exactly(self):
        """The fake's TABLE_STATISTICS has no row for t_ref (fact 36); the capture through the runner session takes COUNT(*) and the
        plan built from it is one bucket on the staging path, which O6 then copies."""
        from s2rebuild.sqlutil import read_guard
        h = ref_harness(self)
        session = h.ctx().runner_session(read_guard('runner P3'))
        try:
            inv = I.capture(session, h.db, h.config, None, log=lambda line: None, sleep=lambda s: None)
        finally:
            session.close()
        by_name = {t['name']: t for t in inv['tables']}
        self.assertEqual((by_name['t_ref']['reference'], by_name['t_ref']['est_rows'], by_name['t_ref']['exact_rows']), (True, 7, 7))
        self.assertEqual((by_name['t_single']['reference'], by_name['t_single']['est_rows'], by_name['t_single']['exact_rows']), (False, 700, None))
        statistics = [s for s, _ in h.fake.statements if 'TABLE_STATISTICS' in s and 'SUM(ROWS)' in s]
        self.assertEqual(len(statistics), 1, 'the estimate query ran once and answered nothing for t_ref')
        plans = {p.name: p for p in I.plan_tables(inv, h.config, h.db)}
        self.assertEqual((plans['t_ref'].n_buckets, plans['t_ref'].copy_path), (1, 'staging'))
        self.assertEqual(h.step('O6').status, steps.PASSED, h.text())
        self.assertEqual(multiset(h.fake.rows(TGT, 't_ref')), multiset(h.fake.rows(SRC, 't_ref')))


FORMATS = {
    # name: (create text, reference): every table format Helios writes in SHOW CREATE TABLE
    'f_col': ('CREATE TABLE `f_col` (\n  `k` int(11) DEFAULT NULL,\n  `label` varchar(20) DEFAULT NULL,\n  SORT KEY `k` (`k`),\n  SHARD KEY `k` (`k`)\n)', False),
    'f_row': ('CREATE ROWSTORE TABLE `f_row` (\n  `k` int(11) NOT NULL,\n  `label` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`k`)\n)', False),
    'f_ref': ('CREATE REFERENCE TABLE `f_ref` (\n  `k` int(11) DEFAULT NULL,\n  `label` varchar(20) DEFAULT NULL,\n  SORT KEY `k` (`k`)\n)', True),
    'f_rowref': ('CREATE ROWSTORE REFERENCE TABLE `f_rowref` (\n  `k` bigint(20) NOT NULL AUTO_INCREMENT,\n  `label` varchar(20) DEFAULT NULL,\n'
                 '  PRIMARY KEY (`k`)\n)', True),
}


class TableFormatTests(unittest.TestCase):
    def test_every_table_format_copies_from_a_read_only_source(self):
        tables = dict(FIXTURE_TABLES)
        rows = fixture_rows()
        for name, (create, _) in FORMATS.items():
            tables[name] = ([('k', 'int(11)', True), ('label', 'varchar(20)', True)], [], 0, create)
            rows[name] = [{'k': i, 'label': '%s-%d' % (name, i)} for i in range(5)]
        h = Harness(self, tables=tables, rows=rows)
        self.assertEqual(h.step('O5').status, steps.PASSED, h.text())
        h.fake.attachment(SRC)['ATTACH_TYPE'] = 'READONLY'
        plans = {p.name: p for p in h.plans()}
        self.assertEqual({n: plans[n].reference for n in FORMATS}, {n: ref for n, (_, ref) in FORMATS.items()})
        self.assertEqual(h.step('O6').status, steps.PASSED, h.text())
        for name, (_, reference) in FORMATS.items():
            self.assertEqual(multiset(h.fake.rows(TGT, name)), multiset(h.fake.rows(SRC, name)), name)
            staged = any(w.startswith('CREATE TABLE `%s`.`%s__stage`' % (TGT, name)) for w in h.fake.writes)
            self.assertEqual(staged, reference, '%s: only reference tables are staged' % name)
            self.assertNotIn(name + '__stage', h.fake.databases[TGT]['tables'])
            self.assertTrue(h.fake.databases[TGT]['tables'][name]['create'].startswith(FORMATS[name][0].split(' `')[0]), 'O5 keeps the format of ' + name)


class SchedulerTests(unittest.TestCase):
    def test_lock_wait_requeues_serializes_and_probes(self):
        h = Harness(self, mutate=lambda d: d['copy'].update(lock_retry_seconds=1, lock_probe_timeout_seconds=5))
        self.assertEqual(h.step('O5').status, steps.PASSED)
        h.fake.lock_tables.add((TGT, 't_single'))
        plans = h.plans()
        clock = {'t': 0.0}

        def sleep(s):
            clock['t'] += s
            if h.fake.lock_hits >= 1:
                h.fake.lock_tables.discard((TGT, 't_single'))
        runner = C.Runner(plans, C.statements_for(h.db), h.journal, h.fake.connect, h.log, h.config.copy, workers=2,
                          sleep=sleep, clock=lambda: clock['t'])
        with quiet_thread_exceptions():
            self.assertTrue(runner.copy(), h.text())
        self.assertGreaterEqual(h.fake.lock_hits, 1)
        self.assertEqual(runner.limits['t_single'], 1, 'serialized after the first lock wait')
        self.assertLessEqual(runner.peak_in_flight['t_single'], 2, 'two workers may have started before the first lock wait')
        self.assertIn('serialized', [e['kind'] for e in h.journal.events()])
        self.assertIn('park', [e['kind'] for e in h.journal.events()])
        self.assertEqual(next(t for t in h.journal.tables() if t['table_name'] == 't_single')['concurrency'], 1)
        self.assertIn(5, h.fake.lock_wait_settings, 'the probe lowers lock_wait_timeout')
        self.assertIn(60, h.fake.lock_wait_settings, 'and restores the server default afterwards')
        self.assertTrue(all(c['attempts'] == 1 for c in h.journal.chunks('t_single')), 'a lock wait does not consume an attempt')
        self.assertEqual(multiset(h.fake.rows(TGT, 't_single')), multiset(h.fake.rows(SRC, 't_single')))
        self.assertIn('LOCK WAIT', h.text())

    def test_concurrency_limits_from_config(self):
        h = Harness(self, mutate=lambda d: (d['copy'].update(table_concurrency=1), d['databases'][0]['tables'].update(t_nokey={'concurrency': 2})))
        self.assertEqual(h.step('O5').status, steps.PASSED)
        plans = h.plans()
        runner = C.Runner(plans, C.statements_for(h.db), h.journal, h.fake.connect, h.log, h.config.copy, workers=4, sleep=lambda s: None)
        self.assertEqual(runner.limits['t_single'], 1)
        self.assertEqual(runner.limits['t_nokey'], 2)
        self.assertTrue(runner.copy())
        self.assertLessEqual(runner.peak_in_flight['t_single'], 1)

    def test_auto_caps_one_table_at_half_the_workers_above_eight(self):
        self.assertEqual([auto_table_limit(w) for w in (1, 4, 8, 9, 12, 16)], [1, 4, 8, 4, 6, 8])
        h = Harness(self)
        self.assertEqual(h.step('O5').status, steps.PASSED)
        plans = h.plans()
        for workers, expected in ((12, 6), (8, 8)):
            runner = C.Runner(plans, C.statements_for(h.db), h.journal, h.fake.connect, h.log, h.config.copy, workers=workers, sleep=lambda s: None)
            for name in runner.queues:
                runner.queues[name].clear()
            first, second = plans[0].name, plans[1].name
            runner.queues[first].extend((b, 'pending') for b in range(20))
            runner.queues[second].extend((b, 'pending') for b in range(20))
            taken = [runner.take() for _ in range(workers)]
            self.assertEqual(sum(1 for t in taken if t and t[0] == first), expected, workers)
            self.assertEqual(sum(1 for t in taken if t and t[0] == second), workers - expected, 'the other workers go to the next table')

    def test_parked_chunks_need_retry_failed(self):
        h = Harness(self, mutate=lambda d: d['copy'].update(max_attempts=1))
        h.fake.fail_insert[('t_single', 'CRC32(`id`) MOD 3 = 0')] = 'before'
        self.assertEqual(h.o5_o6().exit_code, EXIT_INCOMPLETE)
        self.assertEqual(h.journal.chunks('t_single')[0]['attempts'], 1)
        self.assertEqual(h.step('O6').exit_code, EXIT_INCOMPLETE, 'the parked chunk is left alone without --retry-failed')
        self.assertIn('pending=0 started=0 failed=1', h.text())
        self.assertEqual(h.step('O6', options={'retry_failed': True}).status, steps.PASSED)
        self.assertEqual(multiset(h.fake.rows(TGT, 't_single')), multiset(h.fake.rows(SRC, 't_single')))

    def test_fresh_table_fallback(self):
        h = Harness(self)
        self.assertEqual(h.step('O5').status, steps.PASSED)
        h.fake.lock_tables.add((TGT, 't_single'))
        outcome = h.step('O6', options={'fresh_table': 't_single'})
        self.assertEqual((outcome.status, outcome.exit_code), (steps.INCOMPLETE, EXIT_INCOMPLETE), h.text())
        self.assertIn('4 chunks still pending on other tables', outcome.note)
        self.assertEqual(multiset(h.fake.rows(TGT, 't_single')), multiset(h.fake.rows(SRC, 't_single')))
        self.assertNotIn('t_single__fresh', h.fake.databases[TGT]['tables'])
        self.assertEqual([s for (t, _), (s, _) in h.states().items() if t == 't_single'], ['done'] * 3)
        self.assertEqual(len(h.journal.events('manual_load')), 1)
        h.fake.lock_tables.discard((TGT, 't_single'))
        self.assertEqual(h.step('O6').status, steps.PASSED, h.text())
        self.assertEqual([s for s, _ in h.states().values()], ['done'] * 7)
        self.assertEqual(h.step('O6', options={'fresh_table': 'nope'}).status, steps.REFUSED)
        writes = [w for w in h.fake.writes if 't_single__fresh' in w]
        self.assertEqual(writes[0], 'CREATE TABLE `%s`.`t_single__fresh` LIKE `%s`.`t_single`' % (TGT, TGT), 'the checked replacement definition')
        self.assertTrue(any(w.startswith('ALTER TABLE `%s`.`t_single__fresh` RENAME TO `t_single`' % TGT) for w in writes))

    def test_count_proof_mismatch_exits_3(self):
        h = Harness(self)
        self.assertEqual(h.step('O5').status, steps.PASSED)
        original = h.fake.rows(SRC, 't_comp')
        runner = C.Runner(h.plans(), C.statements_for(h.db), h.journal, h.fake.connect, h.log, h.config.copy, workers=1,
                          after_insert_hook=lambda t, b: original.append({'a': 'late', 'b': 'x', 'c': 1, 'note': None}) if t == 't_comp' else None,
                          sleep=lambda s: None)
        self.assertFalse(runner.copy())
        self.assertIn('MISMATCH in per-table counts', h.journal.events('count_proof'))
        outcome = h.step('O6')
        self.assertEqual((outcome.status, outcome.exit_code), (steps.MISMATCH, EXIT_MISMATCH))

    def test_two_runners_on_one_journal_are_refused(self):
        import fcntl
        h = Harness(self)
        self.assertEqual(h.step('O5').status, steps.PASSED)
        lock_path = Path(str(h.journal_path) + '.lock')
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.write(fd, b'4242')
        try:
            outcome = h.step('O6')
            self.assertEqual((outcome.status, outcome.exit_code), (steps.REFUSED, EXIT_REFUSED))
            self.assertIn('holds the copy lock', outcome.note)
            self.assertIn('4242', outcome.note)
            self.assertFalse(any(w.startswith('INSERT') for w in h.fake.writes))
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        self.assertEqual(h.step('O6').status, steps.PASSED, 'the lock is released afterwards')
        self.assertTrue(lock_path.exists())

    def test_mark_recopy_reopens_o6_and_the_rerun_copies_the_marked_buckets(self):
        h = Harness(self)
        self.assertEqual(h.o5_o6().status, steps.PASSED)
        self.assertEqual(h.journal.step_status('O6'), 'passed')
        h.journal.mark_recopy('t_single', 1)
        self.assertEqual(h.journal.step_status('O6'), 'pending', 'marked buckets reopen O6')
        writes = len(h.fake.writes)
        self.assertEqual(h.step('O6', redo=False).status, steps.PASSED, h.text())
        new = h.fake.writes[writes:]
        self.assertEqual([w for w in new if w.startswith('DELETE')], ['DELETE FROM `%s`.`t_single` WHERE CRC32(`id`) MOD 3 = 1' % TGT])
        self.assertEqual(sum(1 for w in new if w.startswith('INSERT')), 1, 'only the marked bucket is copied again')
        self.assertEqual(multiset(h.fake.rows(TGT, 't_single')), multiset(h.fake.rows(SRC, 't_single')))

    def test_abort_deadline_keeps_the_earlier_of_record_and_window(self):
        h = Harness(self)
        plans = h.plans()
        longest = C.longest_chunk_seconds(plans, h.config)
        self.assertGreater(longest, 0)
        window_deadline = h.config.window.abort_deadline(longest)
        self.assertEqual(C.resolve_abort_at(h.journal, h.config, longest), window_deadline)
        h.journal.set_meta('abort_deadline', '2026-11-01T11:30:00Z')
        self.assertEqual(C.resolve_abort_at(h.journal, h.config, longest), window_deadline, 'an O1 record without the allowance is tightened')
        h.journal.set_meta('abort_deadline', '2026-11-01T10:00:00Z')
        self.assertEqual(C.resolve_abort_at(h.journal, h.config, longest), dt.datetime(2026, 11, 1, 10, tzinfo=dt.timezone.utc), 'an earlier record wins')
        h.journal.record_gate('P1', True, 'account in place')
        h.journal.step_end('P2', 'passed', 'preflight passed')
        self.assertEqual(h.step('O1', redo=True, options={'no_rehearsal': 'copy test'}).status, steps.PENDING)
        self.assertIn('longest modeled chunk', h.text())

    def test_serialization_survives_a_resume_and_configured_limits_are_kept(self):
        h = Harness(self, mutate=lambda d: (d['copy'].update(lock_retry_seconds=1), d['databases'][0]['tables'].update(t_nokey={'concurrency': 2})))
        self.assertEqual(h.step('O5').status, steps.PASSED)
        h.journal.set_table_attr('t_single', 'concurrency', 1)
        runner = C.Runner(h.plans(), C.statements_for(h.db), h.journal, h.fake.connect, h.log, h.config.copy, workers=2, sleep=lambda s: None)
        self.assertEqual(runner.limits['t_single'], 1, 'seeded from the journal')
        self.assertEqual(runner.lock_waits['t_single'], 1, 'so the first chunk is probed')
        runner.requeue_after_lock('t_nokey', 0)
        self.assertEqual(runner.limits['t_nokey'], 2, 'a configured limit is not lowered by the auto rule')

    def test_fresh_table_restarts_and_reports_driver_errors(self):
        h = Harness(self)
        self.assertEqual(h.step('O5').status, steps.PASSED)
        h.fake.fail_drop.add('t_single')
        outcome = h.step('O6', options={'fresh_table': 't_single'})
        self.assertEqual((outcome.status, outcome.exit_code), (steps.INCOMPLETE, EXIT_INCOMPLETE), h.text())
        self.assertIn('fresh-table load of t_single failed', outcome.note)
        self.assertIn('t_single__fresh', h.fake.databases[TGT]['tables'], 'the loaded fresh table stays')
        self.assertEqual(len(h.journal.events('fresh_table_error')), 1)
        inserts = sum(1 for w in h.fake.writes if 't_single__fresh' in w and w.startswith('INSERT'))
        outcome = h.step('O6', options={'fresh_table': 't_single'})
        self.assertEqual(outcome.status, steps.INCOMPLETE, 'other tables are still pending')
        self.assertEqual(sum(1 for w in h.fake.writes if 't_single__fresh' in w and w.startswith('INSERT')), inserts, 'the complete fresh table is not reloaded')
        self.assertIn('reload skipped', h.text())
        self.assertNotIn('t_single__fresh', h.fake.databases[TGT]['tables'])
        self.assertEqual(multiset(h.fake.rows(TGT, 't_single')), multiset(h.fake.rows(SRC, 't_single')))
        h2 = Harness(self)
        self.assertEqual(h2.step('O5').status, steps.PASSED)
        h2.fake.fail_statement(r'^ALTER TABLE `%s`\.`t_single__fresh` RENAME' % TGT, 'before')
        self.assertEqual(h2.step('O6', options={'fresh_table': 't_single'}).status, steps.INCOMPLETE, 'the base table is dropped, the rename lost')
        self.assertNotIn('t_single', h2.fake.databases[TGT]['tables'])
        writes = len(h2.fake.writes)
        self.assertEqual(h2.step('O6', options={'fresh_table': 't_single'}).status, steps.INCOMPLETE)
        self.assertTrue(any(w.startswith('ALTER TABLE `%s`.`t_single__fresh` RENAME TO `t_single`' % TGT) for w in h2.fake.writes[writes:]))
        self.assertFalse(any(w.startswith(('DROP TABLE', 'CREATE TABLE', 'INSERT')) for w in h2.fake.writes[writes:]),
                         'rename only when the base table is already gone')

    def test_dry_run_with_fresh_table_prints_the_fallback_statements(self):
        h = Harness(self)
        outcome = h.step('O6', dry_run=True, options={'fresh_table': 't_single'})
        self.assertEqual(outcome.status, steps.PASSED)
        text = h.text()
        self.assertIn('DRY RUN: CREATE TABLE `%s`.`t_single__fresh` LIKE `%s`.`t_single`' % (TGT, TGT), text)
        self.assertIn('DRY RUN: ALTER TABLE `%s`.`t_single__fresh` RENAME TO `t_single`' % TGT, text)
        self.assertNotIn('MOD 3 =', text)

    def test_capture_for_another_database_is_refused(self):
        h = Harness(self)
        h.inventory['database'] = 'other_db'
        I.write(h.inventory, I.inventory_dir(h.config, h.run_id, h.db, 'O4'), h.plans())
        outcome = h.step('O5')
        self.assertEqual(outcome.status, steps.REFUSED)
        self.assertIn('describes database other_db', outcome.note)

    def test_runner_robustness(self):
        h = Harness(self)
        self.assertEqual(h.step('O5').status, steps.PASSED)
        plans = h.plans()
        runner = C.Runner(plans, C.statements_for(h.db), h.journal, h.fake.connect, h.log, h.config.copy, workers=1, sleep=lambda s: None)
        runner.started_at = -1e9
        runner.rows_since_start = 1
        self.assertIn('beyond a year', runner.progress_line())

        def refuse(**kw):
            raise RuntimeError('cannot connect to svc-example-dml.svc.singlestore.com')
        failing = C.Runner(plans, C.statements_for(h.db), h.journal, lambda: refuse(), h.log, h.config.copy, workers=2, sleep=lambda s: None)
        self.assertFalse(failing.copy())
        self.assertEqual({s for s, _ in h.states().values()}, {'pending'}, 'no chunk is charged for a connection failure')
        self.assertGreaterEqual(failing.connect_failures, 1)
        self.assertIn('cannot connect', h.text())
        calls = {'n': 0}

        def interrupt(s):
            calls['n'] += 1
            if calls['n'] == 1:
                raise KeyboardInterrupt()
        ok_runner = C.Runner(plans, C.statements_for(h.db), h.journal, h.fake.connect, h.log, h.config.copy, workers=1, sleep=interrupt)
        ok_runner.copy()
        self.assertTrue(ok_runner.interrupted)
        self.assertIn('Interrupted: no new chunk starts', h.text())
        self.assertEqual(h.step('O6').status, steps.PASSED, 'the resume completes the copy')

    def test_dry_run_prints_every_statement_and_opens_no_connection(self):
        h = Harness(self)
        out5 = h.step('O5', dry_run=True)
        self.assertEqual(out5.status, steps.PASSED)
        out6 = h.step('O6', dry_run=True)
        self.assertEqual(out6.status, steps.PASSED)
        text = h.text()
        self.assertIn('DRY RUN: CREATE DATABASE `%s` PARTITIONS 32 SUB_PARTITIONS 512' % TGT, text)
        self.assertEqual(text.count('DRY RUN: INSERT INTO'), 7)
        self.assertEqual(h.fake.connections, [])
        self.assertEqual(h.fake.writes, [])
        self.assertTrue(h.journal.is_empty())
        self.assertIn('7 INSERT statements over 4 tables printed', text)


# ---------------------------------------------------------------------------
# F13: copy stop safety
# ---------------------------------------------------------------------------
FRESH = {'fresh_table': 't_single'}


class StopSafetyTests(unittest.TestCase):
    def test_f13_lock_that_never_clears_ends_incomplete_with_the_fresh_table_hint(self):
        """A lock that never clears: the chunk fails at its copy.max_lock_waits-th lock wait with the fresh-table command, O6 ends
        INCOMPLETE on its own with that command in the note, and the fresh-table load it names completes the copy.

        Failure point: the INSERT, then the probe DELETE, raise error 1205 before the server applies them (the before point); the
        journal write the cap guards is the chunk's mark_failed, which keeps the attempt.
        """
        h = Harness(self, mutate=lambda d: d['copy'].update(max_lock_waits=2, lock_retry_seconds=1, min_rows_for_chunking=10 ** 9))
        self.assertEqual(h.step('O5').status, steps.PASSED)
        h.fake.lock_tables.add((TGT, 't_single'))
        outcome = h.step('O6')
        self.assertEqual((outcome.status, outcome.exit_code), (steps.INCOMPLETE, EXIT_INCOMPLETE), h.text())
        hint = 's2rebuild step O6 --fresh-table t_single'
        self.assertIn(hint, outcome.note)
        self.assertEqual(h.fake.lock_hits, 2, 'two lock waits, then the chunk fails')
        chunk = h.journal.chunks('t_single')[0]
        self.assertEqual((chunk['state'], chunk['attempts']), ('failed', 1), 'the last lock wait consumes the attempt')
        self.assertIn(hint, chunk['error'])
        self.assertIn('FAILED t_single bucket 0', h.text())
        self.assertEqual({s for (t, _), (s, _) in h.states().items() if t != 't_single'}, {'done'})
        self.assertEqual(h.step('O6', options=FRESH).status, steps.PASSED, h.text())
        self.assertEqual(multiset(h.fake.rows(TGT, 't_single')), multiset(h.fake.rows(SRC, 't_single')))

    def test_f13_second_interrupt_does_not_kill_the_workers(self):
        """Two Ctrl-C while the workers sit between their INSERT and its journal update (mark_done): the second prints the hard-stop
        hint and the wait goes on, so every chunk a worker started finishes and is journaled done; a rerun completes the copy."""
        h = Harness(self)
        self.assertEqual(h.step('O5').status, steps.PASSED)
        caller = threading.current_thread()
        in_chunk, release = threading.Event(), threading.Event()
        hooked, interrupts = [], {'n': 0}

        def hook(table, bucket):
            hooked.append((table, bucket))
            in_chunk.set()
            release.wait(10)

        def sleep(seconds):
            if threading.current_thread() is not caller:
                time.sleep(0.001)
                return
            in_chunk.wait(10)
            interrupts['n'] += 1
            if interrupts['n'] <= 2:
                raise KeyboardInterrupt()
            release.set()
            time.sleep(0.001)
        runner = C.Runner(h.plans(), C.statements_for(h.db), h.journal, h.fake.connect, h.log, h.config.copy, workers=2,
                          after_insert_hook=hook, sleep=sleep)
        self.assertFalse(runner.copy(), 'the interrupt stops new chunks, so the copy is incomplete')
        self.assertTrue(runner.interrupted)
        self.assertGreaterEqual(interrupts['n'], 3, 'the wait went on after the second interrupt')
        self.assertIn('Interrupted again', h.text())
        self.assertIn(C.HARD_STOP_HINT, h.text())
        states = h.states()
        self.assertNotIn('started', {s for s, _ in states.values()}, 'no chunk is left started by the interrupts')
        self.assertTrue(hooked)
        for key in hooked:
            self.assertEqual(states[key][0], 'done', key)
        self.assertEqual(h.step('O6').status, steps.PASSED, h.text())
        for name in FIXTURE_TABLES:
            self.assertEqual(multiset(h.fake.rows(TGT, name)), multiset(h.fake.rows(SRC, name)), name)

    def test_f13_fresh_table_refused_past_the_deadline(self):
        """Past the abort deadline the fresh-table load is refused before a connection opens: no statement, no journal write."""
        h = Harness(self, mutate=lambda d: d['window'].update(start='2000-01-01T00:00:00Z', end='2000-01-01T08:00:00Z'))
        self.assertEqual(h.step('O5').status, steps.PASSED)
        connections, writes = len(h.fake.connections), len(h.fake.writes)
        outcome = h.step('O6', options=FRESH)
        self.assertEqual((outcome.status, outcome.exit_code), (steps.REFUSED, EXIT_REFUSED), h.text())
        self.assertIn('Abort deadline', outcome.note)
        self.assertEqual((len(h.fake.connections), len(h.fake.writes)), (connections, writes))
        self.assertEqual(h.journal.events('evidence_reopened'), [])
        self.assertIsNone(h.journal.get_meta(C.FRESH_META + 't_single'))

    def test_f13_fresh_table_refused_while_the_stop_file_exists(self):
        """The stop file refuses the fresh-table load with nothing written; once it is removed the load runs."""
        h = Harness(self)
        self.assertEqual(h.step('O5').status, steps.PASSED)
        stop = C.stop_file_for(h.journal_path)
        stop.write_text('stop')
        writes = len(h.fake.writes)
        outcome = h.step('O6', options=FRESH)
        self.assertEqual((outcome.status, outcome.exit_code), (steps.REFUSED, EXIT_REFUSED), h.text())
        self.assertIn('s2rebuild resume', outcome.note)
        self.assertEqual(len(h.fake.writes), writes)
        self.assertEqual(h.journal.events('evidence_reopened'), [])
        stop.unlink()
        self.assertEqual(h.step('O6', options=FRESH).status, steps.INCOMPLETE, 'other tables are still pending')
        self.assertEqual(multiset(h.fake.rows(TGT, 't_single')), multiset(h.fake.rows(SRC, 't_single')))

    def test_f13_fresh_name_over_64_characters_or_in_the_plan_is_refused(self):
        """A fresh name over 64 characters, or one that is a planned table, is refused in a dry run and a real run, with nothing written."""
        cols, pk, est, create = FIXTURE_TABLES['t_comp']
        long_name = 't_' + 'x' * 56
        cases = {
            'over 64': ({long_name: (cols, pk, est, create.replace('`t_comp`', '`%s`' % long_name))}, long_name, '65 characters'),
            'planned': ({'t_comp': FIXTURE_TABLES['t_comp'], 't_comp__fresh': (cols, pk, est, create.replace('`t_comp`', '`t_comp__fresh`'))},
                        't_comp', 'is a planned table'),
        }
        for label, (tables, table, words) in cases.items():
            with self.subTest(label):
                h = Harness(self, tables=tables)
                self.assertEqual(h.step('O5').status, steps.PASSED, h.text())
                writes = len(h.fake.writes)
                for dry_run in (True, False):
                    outcome = h.step('O6', dry_run=dry_run, options={'fresh_table': table})
                    self.assertEqual(outcome.status, steps.REFUSED, (dry_run, h.text()))
                    self.assertIn(words, outcome.note)
                self.assertEqual(len(h.fake.writes), writes)
                self.assertEqual(h.journal.events('evidence_reopened'), [])
        self.assertEqual(C.fresh_name(types.SimpleNamespace(name='t_' + 'x' * 55)), 't_' + 'x' * 55 + '__fresh', '64 characters are allowed')

    def test_f13_unrecorded_fresh_table_or_missing_replacement_table_is_refused(self):
        """A t__fresh this journal did not record is neither reused nor dropped; a replacement table that is gone, with no complete
        recorded fresh table, is not recreated from the source definition. Both refuse before any write."""
        h = Harness(self)
        self.assertEqual(h.step('O5').status, steps.PASSED)
        stray = [dict(r) for r in h.fake.rows(SRC, 't_single')]
        h.fake.databases[TGT]['tables']['t_single__fresh'] = {'create': FIXTURE_TABLES['t_single'][3], 'rows': stray, 'auto_increment': None}
        writes = len(h.fake.writes)
        outcome = h.step('O6', options=FRESH)
        self.assertEqual((outcome.status, outcome.exit_code), (steps.REFUSED, EXIT_REFUSED), h.text())
        self.assertIn('did not record creating it', outcome.note)
        self.assertEqual(len(h.fake.writes), writes)
        self.assertEqual(len(h.fake.rows(TGT, 't_single__fresh')), len(stray), 'the stray table is left as it was')
        self.assertEqual(h.journal.events('evidence_reopened'), [])
        h2 = Harness(self)
        self.assertEqual(h2.step('O5').status, steps.PASSED)
        del h2.fake.databases[TGT]['tables']['t_single']
        writes = len(h2.fake.writes)
        outcome = h2.step('O6', options=FRESH)
        self.assertEqual(outcome.status, steps.REFUSED, h2.text())
        self.assertIn('does not recreate it from the source definition', outcome.note)
        self.assertEqual(len(h2.fake.writes), writes)
        self.assertNotIn('t_single', h2.fake.databases[TGT]['tables'])

    def test_f13_fresh_table_faults_rerun_without_a_duplicate_load(self):
        """Failure points around the fresh-table INSERT, DROP TABLE and RENAME: before the statement, after it, and a crash before
        the journal update that follows it (the loaded record after the INSERT, mark_done after the DROP and the RENAME). Each time
        a rerun of step O6 --fresh-table completes the load with one whole-table INSERT on the server and every bucket done."""
        patterns = {'INSERT': r'^INSERT INTO `%s`\.`t_single__fresh`' % TGT, 'DROP': r'^DROP TABLE `%s`\.`t_single`$' % TGT,
                    'RENAME': r'^ALTER TABLE `%s`\.`t_single__fresh` RENAME' % TGT}
        for statement, pattern in patterns.items():
            for point in FAULT_POINTS:
                with self.subTest(statement=statement, point=point):
                    h = Harness(self)
                    self.assertEqual(h.step('O5').status, steps.PASSED)
                    if point == 'journal':
                        if statement == 'INSERT':
                            crash_journal(self, h.journal, 'set_meta',
                                          match=lambda key, value: key == C.FRESH_META + 't_single' and value.split(' ')[1] == C.FRESH_LOADED)
                        else:
                            crash_journal(self, h.journal, 'mark_done')
                        with self.assertRaises(SimulatedCrash):
                            h.step('O6', options=FRESH)
                    else:
                        h.fake.fail_statement(pattern, point)
                        outcome = h.step('O6', options=FRESH)
                        self.assertEqual(outcome.status, steps.INCOMPLETE, h.text())
                        self.assertIn('fresh-table load of t_single failed', outcome.note)
                    outcome = h.step('O6', options=FRESH)
                    self.assertEqual(outcome.status, steps.INCOMPLETE, h.text())
                    self.assertIn('4 chunks still pending on other tables', outcome.note)
                    loads = [w for w in h.fake.writes if w.startswith('INSERT INTO `%s`.`t_single__fresh`' % TGT)]
                    self.assertEqual(len(loads), 1, 'one whole-table load reached the server')
                    self.assertEqual(multiset(h.fake.rows(TGT, 't_single')), multiset(h.fake.rows(SRC, 't_single')))
                    self.assertNotIn('t_single__fresh', h.fake.databases[TGT]['tables'])
                    chunks = h.journal.chunks('t_single')
                    self.assertEqual({(c['state'], c['error']) for c in chunks}, {('done', E.FRESH_LOAD_ERROR)})
                    self.assertEqual(sum(c['rows_inserted'] for c in chunks), len(h.fake.rows(SRC, 't_single')))
                    self.assertEqual(h.journal.get_meta(C.FRESH_META + 't_single'), 't_single__fresh done')
                    self.assertEqual(h.step('O6').status, steps.PASSED, h.text())

    def test_f13_stale_loaded_record_is_never_taken_for_a_load(self):
        """A crash on the done record, the journal write after the RENAME: Journal.mark_loaded writes it with the chunk rows, so
        they roll back together and the record still reads loaded.

        Failure point: journal, after the RENAME and before the done record. Rerun at once: a plain O6 refuses and the fresh-table
        rerun records the load with no second INSERT. A plain O6 that redid the table's failed bucket, or a re-copy mark, makes the
        loaded record stale: with one row of the replacement table changed (count unchanged) the rerun loads the table again, so
        it never reports a load it did not do.
        """
        done = lambda key, value: key == C.FRESH_META + 't_single' and value.endswith(' ' + C.FRESH_DONE)
        for route in ('rerun', 'plain O6', 'mark-recopy'):
            with self.subTest(route):
                if route == 'plain O6':
                    h = Harness(self, mutate=lambda d: d['copy'].update(max_lock_waits=1, lock_retry_seconds=1, min_rows_for_chunking=10 ** 9))
                    self.assertEqual(h.step('O5').status, steps.PASSED)
                    h.fake.lock_tables.add((TGT, 't_single'))
                    self.assertEqual(h.step('O6').status, steps.INCOMPLETE, h.text())
                    h.fake.lock_tables.clear()
                else:
                    h = Harness(self)
                    self.assertEqual(h.step('O5').status, steps.PASSED)
                before = {c['bucket']: (c['state'], c['attempts']) for c in h.journal.chunks('t_single')}
                crash_journal(self, h.journal, 'set_meta', match=done)
                with self.assertRaises(SimulatedCrash):
                    h.step('O6', options=FRESH)
                self.assertEqual({c['bucket']: (c['state'], c['attempts']) for c in h.journal.chunks('t_single')}, before,
                                 'the chunk rows rolled back with the done record')
                self.assertEqual(C.fresh_record(h.journal, 't_single')[1], C.FRESH_LOADED)
                self.assertNotIn('t_single__fresh', h.fake.databases[TGT]['tables'], 'the RENAME was applied')
                if route == 'rerun':
                    outcome = h.step('O6')
                    self.assertEqual(outcome.status, steps.REFUSED, 'a plain O6 does not pass over the load it cannot account for')
                    self.assertIn('does not account for', outcome.note)
                elif route == 'plain O6':
                    self.assertEqual(h.step('O6').status, steps.PASSED, h.text())
                else:
                    h.journal.mark_recopy('t_single')
                if route != 'rerun':
                    h.fake.rows(TGT, 't_single')[0]['payload'] = 'changed by hand'
                outcome = h.step('O6', options=FRESH)
                self.assertIn(outcome.status, (steps.PASSED, steps.INCOMPLETE), h.text())
                loads = [w for w in h.fake.writes if w.startswith('INSERT INTO `%s`.`t_single__fresh`' % TGT)]
                if route == 'rerun':
                    self.assertEqual(len(loads), 1, 'the load the crashed attempt did is recorded, not repeated')
                    self.assertIn('t_single__fresh was already renamed to t_single', h.text())
                else:
                    self.assertEqual(len(loads), 2, 'a stale loaded record is not taken for a load')
                    self.assertIn('t_single__fresh was recorded loaded, but t_single', h.text())
                self.assertEqual(multiset(h.fake.rows(TGT, 't_single')), multiset(h.fake.rows(SRC, 't_single')))
                self.assertNotIn('t_single__fresh', h.fake.databases[TGT]['tables'])
                self.assertEqual({(c['state'], c['error']) for c in h.journal.chunks('t_single')}, {('done', E.FRESH_LOAD_ERROR)})
                self.assertEqual(h.journal.get_meta(C.FRESH_META + 't_single'), 't_single__fresh done')
                self.assertEqual(h.step('O6').status, steps.PASSED, h.text())

    def test_f13_calibrate_skips_fresh_table_rows(self):
        """The chunk rows of a fresh-table load (bucket 0 holds every row, zero duration) carry the fresh-table error, and the
        chunk reader of calibrate skips them."""
        h = Harness(self)
        self.assertEqual(h.step('O5').status, steps.PASSED)
        self.assertEqual(h.step('O6', options=FRESH).status, steps.INCOMPLETE)
        self.assertEqual(h.step('O6').status, steps.PASSED, h.text())
        self.assertEqual({c['error'] for c in h.journal.chunks('t_single')}, {E.FRESH_LOAD_ERROR})
        for first_pass_only in (True, False):
            points = E.read_chunks(h.journal_path, first_pass_only=first_pass_only)
            self.assertEqual(sorted({p[-1] for p in points}), ['t_comp', 't_empty', 't_nokey'], first_pass_only)


if __name__ == '__main__':
    unittest.main()
