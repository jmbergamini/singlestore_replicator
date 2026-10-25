"""s2rebuild tests test_journal: schema creation, migrations, step and chunk transitions, plan check refusal.

Author: John Bergamini
"""
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from s2rebuild.common import KitError
from s2rebuild.journal import ADDED_COLUMNS, COLUMNS, MEMORY, SCHEMA_VERSION, TABLE_CONSTRAINTS, Journal
from tests.fakedb import SimulatedCrash
from tests.helpers import Plan, crash_journal, plans

# The reference runner's journal schema, with neutral names, as the migration fixture.
LEGACY_SCHEMA = [
    'CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)',
    'CREATE TABLE IF NOT EXISTS tables (table_name TEXT PRIMARY KEY, est_rows INTEGER NOT NULL, '
    'n_buckets INTEGER NOT NULL, key_kind TEXT NOT NULL, key_expr TEXT NOT NULL, columns_json TEXT NOT NULL, '
    'created_in_target INTEGER NOT NULL DEFAULT 0, definition_ok INTEGER NOT NULL DEFAULT 0, '
    'source_count INTEGER, target_count INTEGER, counted_utc TEXT)',
    'CREATE TABLE IF NOT EXISTS chunks (table_name TEXT NOT NULL, bucket INTEGER NOT NULL, state TEXT NOT NULL, '
    'rows_inserted INTEGER, started_utc TEXT, ended_utc TEXT, attempts INTEGER NOT NULL DEFAULT 0, error TEXT, '
    'source_count INTEGER, target_count INTEGER, counted_utc TEXT, PRIMARY KEY (table_name, bucket))',
    'CREATE TABLE IF NOT EXISTS events (ts TEXT NOT NULL, kind TEXT NOT NULL, detail TEXT NOT NULL)',
]


def legacy_journal(path):
    conn = sqlite3.connect(str(path))
    for sql in LEGACY_SCHEMA:
        conn.execute(sql)
    conn.execute("INSERT INTO meta VALUES ('source_db', 'src_db'), ('target_db', 'tgt_db')")
    conn.execute("INSERT INTO tables (table_name, est_rows, n_buckets, key_kind, key_expr, columns_json) "
                 "VALUES ('t001', 25000000, 3, 'key', '`id`', '[\"id\", \"v\"]')")
    for b, state in enumerate(('done', 'done', 'started')):
        conn.execute('INSERT INTO chunks (table_name, bucket, state, rows_inserted, attempts) VALUES (?, ?, ?, ?, 1)',
                     ('t001', b, state, 1000 if state == 'done' else None))
    conn.execute("INSERT INTO events VALUES ('2026-01-01T00:00:00Z', 'create_database', 'tgt_db')")
    conn.commit()
    conn.close()


def version_1_journal(path):
    """A journal as schema version 1 of the kit wrote it: every table, without the columns version 2 added, O8 passed on one verified table."""
    conn = sqlite3.connect(str(path))
    for name, cols in COLUMNS.items():
        later = {c for c, _ in ADDED_COLUMNS[2].get(name, [])}
        parts = ['%s %s' % col for col in cols if col[0] not in later]
        if name in TABLE_CONSTRAINTS:
            parts.append(TABLE_CONSTRAINTS[name])
        conn.execute('CREATE TABLE %s (%s)' % (name, ', '.join(parts)))
    conn.execute("INSERT INTO meta VALUES ('schema_version', '1'), ('source_db', 'src_db'), ('replacement_db', 'tgt_db')")
    conn.execute("INSERT INTO tables (table_name, est_rows, n_buckets, key_kind, key_expr, columns_json, target_count, verified_at_utc) "
                 "VALUES ('t1', 10, 1, 'key', '`id`', '[\"id\", \"v\"]', 10, '2026-11-01T06:00:00Z')")
    conn.execute("INSERT INTO chunks (table_name, bucket, state, rows_inserted, ended_utc, attempts) VALUES ('t1', 0, 'done', 10, '2026-11-01T05:30:00Z', 1)")
    conn.execute("INSERT INTO verification (table_name, tier, result, checked_utc) VALUES ('t1', 'count', 'match', '2026-11-01T06:00:00Z')")
    conn.execute("INSERT INTO steps (step_id, status, note) VALUES ('O8', 'passed', 'every tier passed')")
    conn.commit()
    conn.close()


class SchemaTests(unittest.TestCase):
    def test_schema_created_in_memory_and_on_disk(self):
        j = Journal(MEMORY)
        names = {r[0] for r in j.conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        self.assertTrue(set(COLUMNS) <= names, names)
        self.assertEqual(len(COLUMNS), 10)
        self.assertEqual(j.schema_version(), SCHEMA_VERSION)
        j.close()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'deep' / 'nested' / 'journal.sqlite'
            j = Journal(path)
            self.assertEqual(j.conn.execute('PRAGMA journal_mode').fetchone()[0], 'wal')
            self.assertEqual(j.conn.execute('PRAGMA synchronous').fetchone()[0], 2)
            for table, cols in COLUMNS.items():
                have = [r[1] for r in j.conn.execute('PRAGMA table_info(%s)' % table)]
                self.assertEqual(have, [c for c, _ in cols], table)
            j.close()
            self.assertTrue(path.exists())

    def test_migration_from_reference_runner_journal(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'legacy.sqlite'
            legacy_journal(path)
            j = Journal(path)
            self.assertEqual(j.schema_version(), SCHEMA_VERSION)
            self.assertEqual(j.get_meta('source_db'), 'src_db')
            cols = [r[1] for r in j.conn.execute('PRAGMA table_info(tables)')]
            for added in ('size_gib', 'time_column', 'unique_key_lock_risk', 'concurrency', 'verified_at_utc'):
                self.assertIn(added, cols)
            names = {r[0] for r in j.conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            self.assertTrue(set(COLUMNS) <= names)
            self.assertEqual([(b, s) for _, b, s, _, _ in j.chunk_states('t001')], [(0, 'done'), (1, 'done'), (2, 'started')])
            totals, tables_done = j.totals()
            self.assertEqual(totals['done'], (2, 2000))
            self.assertEqual(tables_done, 0)
            self.assertEqual(j.events('create_database'), ['tgt_db'])
            j.check_plan([Plan('t001', 25_000_000, 3)], 'src_db', 'tgt_db')
            j.step_start('O6')
            self.assertEqual(j.step_status('O6'), 'running')
            j.close()
            again = Journal(path)
            self.assertEqual(again.schema_version(), SCHEMA_VERSION)
            again.close()

    def test_refuses_a_newer_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'future.sqlite'
            j = Journal(path)
            j.set_meta('schema_version', 99)
            j.close()
            with self.assertRaisesRegex(KitError, 'newer than this kit'):
                Journal(path)

    def test_dry_run_copy_reads_without_writing(self):
        empty = Journal.dry_run_copy(None)
        self.assertTrue(empty.is_empty())
        empty.close()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'live.sqlite'
            live = Journal(path)
            live.step_end('O1', 'passed', 'go')
            live.close()
            before = path.read_bytes()
            copy_ = Journal.dry_run_copy(path)
            self.assertEqual(copy_.step_status('O1'), 'passed')
            copy_.step_end('O2', 'passed', 'in memory only')
            copy_.close()
            self.assertEqual(path.read_bytes(), before)
            reopened = Journal(path)
            self.assertIsNone(reopened.step_status('O2'))
            reopened.close()
            missing = Journal.dry_run_copy(Path(tmp) / 'none.sqlite')
            self.assertTrue(missing.is_empty())
            missing.close()

    def test_read_only_open(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'ro.sqlite'
            Journal(path).close()
            ro = Journal(path, read_only=True)
            self.assertEqual(ro.steps_board(), {})
            with self.assertRaises(sqlite3.OperationalError):
                ro.set_meta('x', 'y')
            ro.close()

    def test_f4_version_1_journal_upgrades_in_place_to_version_2(self):
        """A version 1 journal gains tables.recopy_epoch (0) and verification.epoch (unknown for the rows it holds) and keeps its data."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'v1.sqlite'
            version_1_journal(path)
            j = Journal(path)
            self.assertEqual((j.schema_version(), SCHEMA_VERSION), (2, 2))
            for table, cols in COLUMNS.items():
                self.assertEqual(set(j._existing_columns(table)), {c for c, _ in cols}, table)
            self.assertEqual(j.recopy_epoch('t1'), 0)
            self.assertIsNone(j.verifications('t1')[0]['epoch'], 'a version 1 row has no epoch: S0 asks for a new O8')
            self.assertEqual(j.step_status('O8'), 'passed')
            state = j.verification_state()[0]
            self.assertEqual((state['table_name'], state['verified_at_utc'], state['epoch_verified']), ('t1', '2026-11-01T06:00:00Z', 0))
            j.record_verification('t1', 'count', 'match', 10, 10)
            self.assertEqual(j.verifications('t1')[-1]['epoch'], 0)
            self.assertEqual(j.verification_state()[0]['epoch_verified'], 1)
            j.close()
            again = Journal(path)
            self.assertEqual(again.schema_version(), 2)
            again.close()

    def test_f4_read_only_open_reads_a_version_1_journal_without_changing_it(self):
        """status opens a version 1 journal read-only as it is: the steps read, the version stays 1, the file is not written."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'v1.sqlite'
            version_1_journal(path)
            before = path.read_bytes()
            ro = Journal(path, read_only=True)
            self.assertEqual(ro.schema_version(), 1)
            self.assertEqual(ro.steps_board()['O8']['status'], 'passed')
            with self.assertRaises(sqlite3.OperationalError):
                ro.set_meta('x', 'y')
            ro.close()
            self.assertEqual(path.read_bytes(), before)

    def test_f4_read_only_version_1_journal_gives_the_s0_state_without_changing_it(self):
        """owner-sql opens a version 1 journal read only: its S0 state reads as after the upgrade (epoch 0, no row at that epoch),
        so the O10 forward block carries a STOP naming the table, and the file is not written. Read only, so nothing to inject."""
        from s2rebuild import switch
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'v1.sqlite'
            version_1_journal(path)
            before = path.read_bytes()
            ro = Journal(path, read_only=True)
            state = ro.verification_state()
            self.assertEqual([(t['table_name'], t['recopy_epoch'], t['epoch_verified'], t['chunks_open']) for t in state], [('t1', 0, 0, 0)])
            self.assertEqual(switch.unverified_tables(ro), ['t1 (no matching verification at re-copy epoch 0)'])
            self.assertIn('not verified on the rows now copied: t1 (no matching verification at re-copy epoch 0); rerun O6 to O8',
                          '; '.join(switch.s0_journal_problems(ro)))
            ro.close()
            self.assertEqual(path.read_bytes(), before)

    def test_read_only_refuses_an_older_schema_with_a_remedy(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'legacy.sqlite'
            legacy_journal(path)
            with self.assertRaisesRegex(KitError, 'older schema .* writable command'):
                Journal(path, read_only=True)
            Journal(path).close()
            Journal(path, read_only=True).close()

    def test_corrupt_and_odd_paths_become_kit_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            corrupt = Path(tmp) / 'corrupt.sqlite'
            corrupt.write_bytes(b'this is not a database file' * 40)
            with self.assertRaisesRegex(KitError, 'could not be opened'):
                Journal(corrupt)
            with self.assertRaisesRegex(KitError, 'could not be opened'):
                Journal(corrupt, read_only=True)
            with self.assertRaisesRegex(KitError, 'could not be opened'):
                Journal.dry_run_copy(corrupt)
            odd_dir = Path(tmp) / 'run#1?x'
            odd = odd_dir / 'journal.sqlite'
            j = Journal(odd)
            j.step_end('O1', 'passed', 'go')
            j.close()
            ro = Journal(odd, read_only=True)
            self.assertEqual(ro.step_status('O1'), 'passed')
            ro.close()
            copy_ = Journal.dry_run_copy(odd)
            self.assertEqual(copy_.step_status('O1'), 'passed')
            copy_.close()
            self.assertEqual(sorted(p.name for p in odd_dir.iterdir() if not p.name.endswith(('-wal', '-shm'))), ['journal.sqlite'])
            self.assertEqual([p.name for p in Path(tmp).iterdir() if p.is_file() and p.name != 'corrupt.sqlite'], [],
                             'no stray database file outside the run folder')


class StepTests(unittest.TestCase):
    def setUp(self):
        self.j = Journal(MEMORY)

    def tearDown(self):
        self.j.close()

    def test_step_transitions_and_board(self):
        j = self.j
        self.assertIsNone(j.step_status('O1'))
        j.step_start('O1', 'first attempt')
        row = j.step('O1')
        self.assertEqual(row['status'], 'running')
        self.assertIsNotNone(row['started_utc'])
        self.assertIsNone(row['ended_utc'])
        j.step_end('O1', 'pending', 'gate pending: go')
        self.assertEqual(j.step_status('O1'), 'pending')
        j.record_gate('O1', True, 'go recorded', '/tmp/evidence.txt')
        row = j.step('O1')
        self.assertEqual((row['status'], row['note'], row['evidence_path']), ('passed', 'go recorded', '/tmp/evidence.txt'))
        j.step_end('O1', 'skipped')
        self.assertEqual(j.step('O1')['note'], 'go recorded', 'a missing note keeps the previous one')
        self.assertEqual(j.step('O1')['evidence_path'], '/tmp/evidence.txt', 'a step attempt keeps the gate evidence')
        j.record_gate('O1', True, 'second decision')
        self.assertIsNone(j.step('O1')['evidence_path'], 'a new gate without evidence clears the old file')
        j.record_gate('O2', False, 'pipelines still running')
        self.assertEqual(j.step_status('O2'), 'failed')
        self.assertEqual(set(j.steps_board()), {'O1', 'O2'})
        with self.assertRaises(KitError):
            j.step_end('O3', 'running')
        with self.assertRaises(KitError):
            j.step_end('O3', 'done')
        kinds = [e['kind'] for e in j.events()]
        self.assertEqual(kinds, ['step_start', 'step_pending', 'step_passed', 'gate', 'step_skipped', 'step_passed', 'gate',
                                 'step_failed', 'gate'])

    def test_meta(self):
        self.j.set_meta('run_id', '20261101-0500')
        self.j.set_meta('run_id', '20261101-0501')
        self.assertEqual(self.j.get_meta('run_id'), '20261101-0501')
        self.assertEqual(self.j.meta()['run_id'], '20261101-0501')
        self.assertIsNone(self.j.get_meta('missing'))


class PlanAndChunkTests(unittest.TestCase):
    def setUp(self):
        self.j = Journal(MEMORY)
        self.plans = plans()
        self.j.init_plan(self.plans, 'src_db', 'tgt_db', '/inventory')

    def tearDown(self):
        self.j.close()

    def test_init_plan_records_tables_and_chunks(self):
        j = self.j
        self.assertFalse(j.is_empty())
        self.assertEqual(j.get_meta('source_db'), 'src_db')
        self.assertEqual(j.get_meta('replacement_db'), 'tgt_db')
        self.assertIsNone(j.get_meta('target_db'), 'one name for the replacement in new journals')
        self.assertEqual(j.get_meta('inventory_dir'), '/inventory')
        rows = {r['table_name']: r for r in j.tables()}
        self.assertEqual(set(rows), {'t_big', 't_small', 't_nokey'})
        self.assertEqual(rows['t_big']['size_gib'], 12.5)
        self.assertEqual(rows['t_big']['time_column'], 'updated_at')
        self.assertEqual(rows['t_small']['columns_json'], '["a", "b", "c"]')
        self.assertEqual(rows['t_nokey']['key_kind'], 'row')
        self.assertEqual(len(j.chunk_states()), 6)
        self.assertEqual({s for _, _, s, _, _ in j.chunk_states()}, {'pending'})
        self.assertEqual([r['table_name'] for r in j.tables()], ['t_big', 't_nokey', 't_small'], 'largest first')

    def test_check_plan_refusals(self):
        j = self.j
        j.check_plan(self.plans, 'src_db', 'tgt_db')
        with self.assertRaisesRegex(KitError, 'different source/replacement pair'):
            j.check_plan(self.plans, 'src_db', 'other_db')
        fewer = self.plans[:2]
        with self.assertRaisesRegex(KitError, 'table set differs'):
            j.check_plan(fewer, 'src_db', 'tgt_db')
        changed = plans()
        changed[0].n_buckets = 4
        with self.assertRaisesRegex(KitError, 'chunk plan differs'):
            j.check_plan(changed, 'src_db', 'tgt_db')
        changed = plans()
        changed[1].columns = ['a', 'b']
        with self.assertRaisesRegex(KitError, 'chunk plan differs'):
            j.check_plan(changed, 'src_db', 'tgt_db')
        changed = plans()
        changed[2].key_expr = '`x`'
        with self.assertRaisesRegex(KitError, 'chunk plan differs'):
            j.check_plan(changed, 'src_db', 'tgt_db')

    def test_chunk_transitions(self):
        j = self.j
        j.mark_started('t_big', 0)
        row = j.chunks('t_big')[0]
        self.assertEqual((row['state'], row['attempts']), ('started', 1))
        self.assertIsNotNone(row['started_utc'])
        j.mark_done('t_big', 0, 5000)
        row = j.chunks('t_big')[0]
        self.assertEqual((row['state'], row['rows_inserted']), ('done', 5000))
        self.assertIsNotNone(row['ended_utc'])
        j.mark_started('t_big', 1)
        j.mark_failed('t_big', 1, 'OperationalError: 1205 Lock wait timeout ' + 'x' * 3000)
        row = j.chunks('t_big')[1]
        self.assertEqual(row['state'], 'failed')
        self.assertEqual(len(row['error']), 2000)
        j.undo_attempt('t_big', 1)
        self.assertEqual(j.chunks('t_big')[1]['attempts'], 0)
        j.undo_attempt('t_big', 1)
        self.assertEqual(j.chunks('t_big')[1]['attempts'], 0, 'never below zero')
        j.record_chunk_counts('t_big', 0, 5000, 5000)
        self.assertEqual(j.chunks('t_big')[0]['target_count'], 5000)
        j.record_table_counts('t_big', 15000, 15000)
        self.assertEqual(j.tables()[0]['source_count'], 15000)
        totals, tables_done = j.totals()
        self.assertEqual(totals['done'], (1, 5000))
        self.assertEqual(totals['failed'], (1, 0))
        self.assertEqual(totals['pending'], (4, 0))
        self.assertEqual(tables_done, 0)
        for b in range(3):
            j.mark_done('t_big', b, 1)
        j.mark_done('t_small', 0, 1)
        self.assertEqual(j.totals()[1], 2)

    def test_mark_recopy(self):
        j = self.j
        for b in range(3):
            j.mark_started('t_big', b)
            j.mark_done('t_big', b, 10)
        self.assertEqual(j.mark_recopy('t_big', 1), 1)
        rows = j.chunks('t_big')
        self.assertEqual([r['state'] for r in rows], ['done', 'failed', 'done'])
        self.assertEqual(rows[1]['attempts'], 0)
        self.assertEqual(rows[1]['error'], 'marked for re-copy')
        self.assertEqual(j.mark_recopy('t_big'), 3)
        self.assertEqual({r['state'] for r in j.chunks('t_big')}, {'failed'})
        self.assertEqual(j.events('mark_recopy'), ['t_big bucket 1', 't_big'])
        with self.assertRaisesRegex(KitError, 'not in this journal'):
            j.mark_recopy('nope')
        with self.assertRaisesRegex(KitError, 'No bucket matched'):
            j.mark_recopy('t_big', 7)

    def test_f4_mark_recopy_reopens_o6_to_o8_and_clears_the_evidence_of_that_table(self):
        """O6, O7 and O8 go back to pending with the table named; verified_at_utc and target_count are cleared; the epoch counts the marks."""
        j = self.j
        for p in self.plans:
            for b in range(p.n_buckets):
                j.mark_started(p.name, b)
                j.mark_done(p.name, b, 10)
            j.record_table_counts(p.name, 10 * p.n_buckets, 10 * p.n_buckets)
            j.record_verification(p.name, 'count', 'match', 10, 10)
        for step_id in ('O6', 'O7', 'O8'):
            j.step_end(step_id, 'passed', 'passed before the re-copy')
        j.step_end('O9', 'passed', 'pipelines')
        self.assertEqual(j.mark_recopy('t_big', 1), 1)
        board = j.steps_board()
        self.assertEqual({s: board[s]['status'] for s in ('O6', 'O7', 'O8', 'O9')},
                         {'O6': 'pending', 'O7': 'pending', 'O8': 'pending', 'O9': 'passed'}, 'O9 is held by run_step, not reopened')
        for step_id in ('O6', 'O7', 'O8'):
            self.assertIn('t_big', board[step_id]['note'])
        rows = {r['table_name']: r for r in j.tables()}
        self.assertEqual((rows['t_big']['verified_at_utc'], rows['t_big']['target_count'], rows['t_big']['recopy_epoch']), (None, None, 1))
        self.assertEqual((rows['t_small']['target_count'], rows['t_small']['recopy_epoch']), (10, 0), 'other tables keep their evidence')
        self.assertIsNotNone(rows['t_small']['verified_at_utc'])
        j.record_verification('t_big', 'count', 'match', 30, 30)
        self.assertEqual([r['epoch'] for r in j.verifications('t_big')], [0, 1], 'each row carries the epoch it verified')
        j.step_start('O8')
        j.step_end('O7', 'failed', 'stale check mismatch')
        self.assertEqual(j.mark_recopy('t_big'), 3)
        self.assertEqual((j.step_status('O8'), j.step_status('O7')), ('running', 'failed'), 'a step that is not passed keeps its status')
        self.assertEqual(j.recopy_epoch('t_big'), 2)
        with self.assertRaisesRegex(KitError, 'No bucket matched'):
            j.mark_recopy('t_big', 7)
        self.assertEqual(j.recopy_epoch('t_big'), 2, 'a refused mark changes nothing')

    def test_f4_mark_recopy_crash_part_way_leaves_the_journal_as_it_was(self):
        """kill -9 stand-in at each journal write of mark_recopy after the chunk update: nothing is marked, nothing reopened; the rerun marks it all."""
        for method, match in (('set_table_attr', lambda table, column, value: column == 'verified_at_utc'),
                              ('event', lambda kind, detail: kind == 'mark_recopy'),
                              ('step_end', lambda step_id, status, note=None, evidence_path=None: step_id == 'O8')):
            with self.subTest(method=method):
                j = Journal(MEMORY)
                self.addCleanup(j.close)
                j.init_plan(self.plans, 'src_db', 'tgt_db')
                for b in range(3):
                    j.mark_started('t_big', b)
                    j.mark_done('t_big', b, 10)
                j.record_table_counts('t_big', 30, 30)
                j.record_verification('t_big', 'count', 'match', 30, 30)
                for step_id in ('O6', 'O7', 'O8'):
                    j.step_end(step_id, 'passed', 'passed before the re-copy')
                before = (j.chunks('t_big'), j.tables(), j.steps_board(), j.events('mark_recopy'))
                fired = crash_journal(self, j, method, match=match)
                with self.assertRaises(SimulatedCrash):
                    j.mark_recopy('t_big', 1)
                self.assertEqual(len(fired), 1)
                self.assertEqual((j.chunks('t_big'), j.tables(), j.steps_board(), j.events('mark_recopy')), before)
                self.assertEqual(j.mark_recopy('t_big', 1), 1)
                self.assertEqual([c['state'] for c in j.chunks('t_big')], ['done', 'failed', 'done'])
                self.assertEqual({s: j.step_status(s) for s in ('O6', 'O7', 'O8')}, {'O6': 'pending', 'O7': 'pending', 'O8': 'pending'})
                self.assertEqual(j.recopy_epoch('t_big'), 1, 'the epoch counts one mark, not the crashed one')

    def test_f4_mark_recopy_refused_once_the_switch_began_with_nothing_written(self):
        """Every record that the switch may have begun refuses mark_recopy before any write; a pending O10 without a go does not, and
        neither does an O10 left running or failed before the go (test_f4_o10_stopped_before_the_go_leaves_the_recopy_open).
        A refusal with no statement of its own: it guards the mark_recopy journal write; the failure points at the S1 DETACH are in
        test_switch test_f4_mark_recopy_and_o6_to_o8_refused_once_the_switch_began."""
        marks = [('O10 failed by a gate', lambda j: j.record_gate('O10', False, 'hand switch stopped at S2'), 'gate O10 --fail was recorded'),
                 ('O10 passed', lambda j: j.record_gate('O10', True, 'switched by hand'), 'O10 is passed'),
                 ('go', lambda j: j.event('switch_go', 'go recorded for live name src_db'), 'the go decision was recorded'),
                 ('observed', lambda j: j.event('switch_observed', 'gate O10 --pass evidence (refused): live src_db on STORAGE_ID sid-9'),
                  'a switch was observed: gate O10 --pass evidence'),
                 ('switched', lambda j: j.set_meta('switched_utc', '2026-11-01T07:00:00Z'), 'switched_utc is 2026-11-01T07:00:00Z'),
                 ('rolled back', lambda j: j.set_meta('rolled_back_utc', '2026-11-01T07:30:00Z'), 'rolled_back_utc is'),
                 ('retained', lambda j: j.record_attachment('retained', 'src_db_retained', 'sid-1', 'cid-1', 'READONLY'),
                  'the retained attachment is recorded')]
        for label, record, reason in marks:
            with self.subTest(mark=label):
                j = Journal(MEMORY)
                self.addCleanup(j.close)
                j.init_plan(self.plans, 'src_db', 'tgt_db')
                for b in range(3):
                    j.mark_started('t_big', b)
                    j.mark_done('t_big', b, 10)
                j.record_table_counts('t_big', 30, 30)
                j.record_verification('t_big', 'count', 'match', 30, 30)
                for step_id in ('O6', 'O7', 'O8'):
                    j.step_end(step_id, 'passed', 'passed before the switch')
                j.step_end('O10', 'pending', 'printed for the hand route')
                self.assertIsNone(j.switch_began(), 'a printed, pending O10 is not a switch')
                record(j)
                before = (j.chunks('t_big'), j.tables(), j.steps_board(), j.events('mark_recopy'))
                with self.assertRaisesRegex(KitError, 'mark-recopy refused for t_big: the switch may have begun \\(%s' % reason):
                    j.mark_recopy('t_big', 1)
                with self.assertRaisesRegex(KitError, 'mark-recopy refused for t_big'):
                    j.check_recopy('t_big')
                self.assertEqual((j.chunks('t_big'), j.tables(), j.steps_board(), j.events('mark_recopy')), before)

    def test_f4_o10_stopped_before_the_go_does_not_count_as_a_switch(self):
        """An O10 left running (a killed process) or failed (a Ctrl-C, a lost connection) with no go decision issued nothing, so
        mark_recopy goes ahead; a refused mark (no such bucket) is refused by check_recopy before any write. Journal unit test: the
        failure points around the go are in test_switch test_f4_o10_stopped_before_the_go_leaves_the_recopy_open."""
        for label, record in (('running', lambda j: j.step_start('O10')),
                              ('failed', lambda j: j.step_end('O10', 'failed', 'interrupted by KeyboardInterrupt (error unknown)'))):
            with self.subTest(o10=label):
                j = Journal(MEMORY)
                self.addCleanup(j.close)
                j.init_plan(self.plans, 'src_db', 'tgt_db')
                for step_id in ('O6', 'O7', 'O8'):
                    j.step_end(step_id, 'passed', 'passed before O10')
                record(j)
                self.assertIsNone(j.switch_began())
                before = (j.chunks('t_big'), j.tables(), j.steps_board())
                with self.assertRaisesRegex(KitError, 'No bucket matched t_big bucket 9'):
                    j.check_recopy('t_big', 9)
                with self.assertRaisesRegex(KitError, 'not in this journal'):
                    j.check_recopy('t_none')
                self.assertEqual((j.chunks('t_big'), j.tables(), j.steps_board()), before)
                self.assertEqual(j.mark_recopy('t_big', 1), 1)
                self.assertEqual(j.step_status('O8'), 'pending')
                self.assertEqual(j.step_status('O10'), label, 'the mark leaves O10 as it was')

    def test_f4_mark_step_start_and_go_read_the_switch_state_in_their_own_transaction(self):
        """Of a re-copy mark, a start of O6 to O8 and the go decision, whichever commits second is refused: mark_recopy and step_start
        with unless read switch_began, and record_go reads its check, each inside its own BEGIN IMMEDIATE transaction. A refused go
        or a refused start writes nothing. Journal unit test: the failure points at the go write are in test_switch
        test_f4_go_write_fault_points_hold_the_recopy_rule_on_the_rerun."""
        j = self.j
        for step_id in ('O6', 'O7', 'O8'):
            j.step_end(step_id, 'passed', 'passed before O10')
        seen = []
        real = j.switch_began

        def spy():
            seen.append(j.conn.in_transaction)
            return real()
        j.switch_began = spy
        self.assertEqual(j.mark_recopy('t_big', 1), 1)
        self.assertIsNone(j.step_start('O6', unless=j.switch_began))
        self.assertEqual((j.step_status('O6'), seen), ('running', [True, True]), 'both read the switch state inside the transaction')
        checked = []

        def refuse(journal):
            checked.append(journal.conn.in_transaction)
            return ['O6 is running (another process started it after S0)', 'O8 has not passed']
        before = j.events()
        with self.assertRaisesRegex(KitError, r'^S0 refused at the go decision: O6 is running \(another process started it after S0\); '
                                              r'O8 has not passed; nothing was issued$'):
            j.record_go('go recorded for live name src_db', refuse)
        self.assertEqual((checked, j.events(), j.conn.in_transaction), ([True], before, False), 'nothing written, the transaction closed')
        j.record_go('go recorded for live name src_db', lambda journal: [])
        self.assertEqual(j.events('switch_go'), ['go recorded for live name src_db'])
        board = j.steps_board()
        self.assertEqual(j.step_start('O7', unless=j.switch_began), 'the go decision was recorded: go recorded for live name src_db')
        self.assertEqual(j.steps_board(), board, 'a refused start writes nothing')
        with self.assertRaisesRegex(KitError, 'mark-recopy refused for t_big: the switch may have begun'):
            j.mark_recopy('t_big', 0)
        self.assertEqual(seen[-2:], [True, True])

    def test_f4_reopen_evidence_clears_the_table_and_a_crash_part_way_leaves_the_journal_as_it_was(self):
        """reopen_evidence (the fresh-table load) reopens O7 and O8 and clears the table's evidence in one transaction; a kill -9
        stand-in at each of its journal writes rolls it all back, and the rerun reopens once."""
        for method, match in ((None, None), ('event', lambda kind, detail: kind == 'evidence_reopened'),
                              ('set_table_attr', lambda table, column, value: column == 'verified_at_utc'),
                              ('step_end', lambda step_id, status, note=None, evidence_path=None: step_id == 'O8')):
            with self.subTest(method=method):
                j = Journal(MEMORY)
                self.addCleanup(j.close)
                j.init_plan(self.plans, 'src_db', 'tgt_db')
                j.record_table_counts('t_big', 30, 30)
                j.record_verification('t_big', 'count', 'match', 30, 30)
                for step_id in ('O7', 'O8'):
                    j.step_end(step_id, 'passed', 'passed before the load')
                j.step_start('O6')
                if method is not None:
                    before = (j.tables(), j.steps_board(), j.events('evidence_reopened'))
                    crash_journal(self, j, method, match=match)
                    with self.assertRaises(SimulatedCrash):
                        j.reopen_evidence('t_big', 'fresh-table load')
                    self.assertEqual((j.tables(), j.steps_board(), j.events('evidence_reopened')), before)
                j.reopen_evidence('t_big', 'fresh-table load')
                row = next(t for t in j.tables() if t['table_name'] == 't_big')
                self.assertEqual((row['target_count'], row['verified_at_utc'], row['recopy_epoch']), (None, None, 1))
                self.assertEqual({s: j.step_status(s) for s in ('O6', 'O7', 'O8')}, {'O6': 'running', 'O7': 'pending', 'O8': 'pending'},
                                 'the running O6 that loads the table keeps its status')
                self.assertEqual(j.events('evidence_reopened'), ['t_big: fresh-table load'])

    def test_table_flags_and_attrs(self):
        j = self.j
        self.assertEqual(tuple(j.table_flags('t_big')), (0, 0))
        j.set_table_flag('t_big', 'created_in_target')
        j.set_table_flag('t_big', 'definition_ok')
        self.assertEqual(tuple(j.table_flags('t_big')), (1, 1))
        self.assertEqual(tuple(j.table_flags('unknown')), (0, 0))
        j.set_table_attr('t_big', 'concurrency', 1)
        self.assertEqual(j.tables()[0]['concurrency'], 1)
        with self.assertRaises(KitError):
            j.set_table_flag('t_big', 'est_rows', 0)
        with self.assertRaises(KitError):
            j.set_table_attr('t_big', 'table_name', 'x')


class OtherRowsTests(unittest.TestCase):
    def setUp(self):
        self.j = Journal(MEMORY)
        self.j.init_plan([Plan('t1', 10, 1)], 'src_db', 'tgt_db')

    def tearDown(self):
        self.j.close()

    def test_verification_rows_and_verified_at(self):
        j = self.j
        self.assertIsNone(j.tables()[0]['verified_at_utc'])
        j.record_verification('t1', 'count', 'mismatch', 10, 9, 'SELECT COUNT(*) a', 'SELECT COUNT(*) b', 'one row short')
        self.assertIsNone(j.tables()[0]['verified_at_utc'], 'a mismatch does not set verified_at')
        when = j.record_verification('t1', 'count', 'match', 10, 10)
        self.assertEqual(j.tables()[0]['verified_at_utc'], when)
        rows = j.verifications('t1')
        self.assertEqual([r['result'] for r in rows], ['mismatch', 'match'])
        self.assertEqual(rows[0]['source_value'], '10')
        self.assertEqual(len(j.verifications(tier='count')), 2)
        self.assertEqual(j.verifications(tier='newest'), [])

    def test_attachments(self):
        j = self.j
        self.assertIsNone(j.attachment('source'))
        j.record_attachment('source', 'src_db', 'sid-1', 'cid-1', 'READWRITE')
        j.record_attachment('source', 'src_db', 'sid-1', 'cid-1', 'READONLY')
        self.assertEqual(j.attachment('source')['attach_type'], 'READONLY')
        with self.assertRaises(KitError):
            j.record_attachment('other', 'x')

    def test_grants_snapshot(self):
        j = self.j
        j.snapshot_grant("'app'@'%'", ['UPDATE', 'INSERT'])
        snap = j.grants_snapshot()
        self.assertEqual(snap["'app'@'%'"]['privileges'], ['INSERT', 'UPDATE'])
        self.assertIsNone(snap["'app'@'%'"]['restored_utc'])
        j.mark_grant_restored("'app'@'%'")
        self.assertIsNotNone(j.grants_snapshot()["'app'@'%'"]['restored_utc'])
        self.assertFalse(j.snapshot_grant("'app'@'%'", ['DELETE']), 'a rerun never overwrites the first snapshot')
        row = j.grants_snapshot()["'app'@'%'"]
        self.assertEqual(row['privileges'], ['INSERT', 'UPDATE'])
        self.assertIsNotNone(row['restored_utc'])
        self.assertTrue(j.snapshot_grant("'other'@'%'", {'*.*': ['INSERT'], '`db1`.*': ['UPDATE', 'DELETE']}))
        self.assertEqual(j.grants_snapshot()["'other'@'%'"]['privileges'], {'*.*': ['INSERT'], '`db1`.*': ['DELETE', 'UPDATE']})

    def test_pipelines_and_offsets(self):
        j = self.j
        j.upsert_pipeline('p1', state_at_capture='Running', source_type='KAFKA', app_managed=0)
        j.upsert_pipeline('p1', state_at_capture='Stopped')
        rows = j.pipelines()
        self.assertEqual((rows[0]['name'], rows[0]['state_at_capture'], rows[0]['source_type']), ('p1', 'Stopped', 'KAFKA'))
        with self.assertRaisesRegex(KitError, 'Unknown pipeline fields'):
            j.upsert_pipeline('p1', credentials='never')
        j.record_offsets('p1', 0, 'capture', 10, 500, 480, 480)
        j.record_offsets('p1', 1, 'capture', 0, 300, 300, 300)
        j.record_offsets('p1', 0, 'before_start', 20, 900, 480, 480)
        self.assertEqual([r['source_partition'] for r in j.offsets('capture')], ['0', '1'])
        self.assertEqual(j.offsets('before_start', 'p1')[0]['earliest'], 20)
        with self.assertRaises(KitError):
            j.record_offsets('p1', 0, 'later')

    def test_concurrent_writers(self):
        j = self.j

        def work(n):
            for i in range(50):
                j.event('thread', '%d-%d' % (n, i))
        threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(j.events('thread')), 400)


if __name__ == '__main__':
    unittest.main()
