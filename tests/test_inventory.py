"""s2rebuild tests test_inventory: capture through a fake session, files, copy plan, drift, system profile.

Author: John Bergamini
"""
import json
import os
import re
import stat
import tempfile
import unittest
from pathlib import Path

from s2rebuild import inventory as I
from s2rebuild import steps
from s2rebuild.common import KitError
from s2rebuild.sqlutil import read_guard
from tests.helpers import ROOT, config_from

FIXTURE_INVENTORY = ROOT / 'tests' / 'fixtures' / 'inventory'
DB = 'example_orders_db'
PROC1_DEFINITION = 'CREATE DEFINER=`app`@`%` PROCEDURE `proc1`(a INT) AS\nBEGIN\n  ECHO SELECT a;\nEND'
PROC1_CAPTURED = {'name': 'proc1', 'kind': 'procedure', 'type': 'PROCEDURE', 'definer': '`app`@`%`', 'definition': PROC1_DEFINITION}


class FakeSession:
    """Answers the inventory queries from canned rows keyed by the statement's leading words."""

    def __init__(self, answers, owner=False):
        self.answers = answers
        self.owner = owner
        self.statements = []

    def query(self, sql, args=()):
        self.statements.append((sql, tuple(args)))
        norm = ' '.join(sql.split())
        for key, rows in self.answers.items():
            if norm.startswith(key) or key in norm:
                return [dict(r) for r in rows] if not callable(rows) else rows(norm, args)
        return []

    def close(self):
        pass


def canned_answers():
    def show_create(norm, args):
        if norm.startswith('SHOW CREATE TABLE'):
            name = norm.split('`.`')[1].rstrip('`')
            defs = {'t_keyed': 'CREATE TABLE `t_keyed` (\n  `id` bigint(20) NOT NULL,\n  `v` varchar(20) DEFAULT NULL,\n  `updated_at` datetime(6) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  SHARD KEY (`id`),\n  SORT KEY (`id`)\n) AUTO_INCREMENT=77',
                    't_nokey': 'CREATE TABLE `t_nokey` (\n  `x` int(11) DEFAULT NULL,\n  `y` varchar(50) DEFAULT NULL,\n  `z` as `x` * 2 PERSISTED int(11)\n)',
                    't_email': 'CREATE TABLE `t_email` (\n  `email` varchar(128) NOT NULL,\n  `n` int(11) DEFAULT NULL,\n  PRIMARY KEY (`email`),\n  SORT KEY (`email`)\n)'}
            return [{'Table': name, 'Create Table': defs[name]}]
        if norm.startswith('SHOW CREATE VIEW'):
            return [{'View': 'v1', 'Create View': 'CREATE DEFINER=`app`@`%` VIEW `v1` AS SELECT `id` FROM `t_keyed`'}]
        if norm.startswith('SHOW CREATE PROCEDURE'):
            return [{'Procedure': 'proc1', 'Create Procedure': PROC1_DEFINITION}]
        if norm.startswith('SHOW CREATE PIPELINE'):
            return [{'Pipeline': 'p1', 'Create Pipeline': "CREATE PIPELINE `p1` AS LOAD DATA KAFKA 'b:9092/topic' CREDENTIALS '{\"sasl.password\": \"SECRET-VALUE\"}' BATCH_INTERVAL 1000 REPLACE INTO TABLE `t_keyed` FORMAT JSON",
                     'Alter Pipeline 1': "ALTER PIPELINE `p1` SET OFFSETS '{\"0\": 10}'"}]
        return []
    return {
        'SELECT NUM_PARTITIONS': [{'NUM_PARTITIONS': 16, 'NUM_SUB_PARTITIONS': 0, 'IS_SYNC': 1, 'IS_SYNC_DURABILITY': 0, 'IS_SYNC_DURABILITY_ON_REF_DB_ONLY': 1}],
        'SELECT DATABASE_NAME, ATTACH_TYPE': [{'DATABASE_NAME': DB, 'ATTACH_TYPE': 'READWRITE', 'STORAGE_ID': 'sid-1', 'COMPUTE_ID': 'cid-1', 'ATTACH_SYNTAX': None}],
        'SELECT TABLE_NAME, TABLE_TYPE': [{'TABLE_NAME': 't_email', 'TABLE_TYPE': 'BASE TABLE'}, {'TABLE_NAME': 't_keyed', 'TABLE_TYPE': 'BASE TABLE'},
                                          {'TABLE_NAME': 't_nokey', 'TABLE_TYPE': 'BASE TABLE'}, {'TABLE_NAME': 'v1', 'TABLE_TYPE': 'VIEW'}],
        'SELECT ROUTINE_NAME': [{'ROUTINE_NAME': 'proc1', 'ROUTINE_TYPE': 'PROCEDURE'}],
        'SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION': [
            {'TABLE_NAME': 't_keyed', 'COLUMN_NAME': 'id', 'ORDINAL_POSITION': 1, 'COLUMN_DEFAULT': None, 'IS_NULLABLE': 'NO', 'DATA_TYPE': 'bigint', 'COLUMN_TYPE': 'bigint(20)', 'EXTRA': 'auto_increment', 'GENERATION_EXPRESSION': ''},
            {'TABLE_NAME': 't_keyed', 'COLUMN_NAME': 'v', 'ORDINAL_POSITION': 2, 'COLUMN_DEFAULT': None, 'IS_NULLABLE': 'YES', 'DATA_TYPE': 'varchar', 'COLUMN_TYPE': 'varchar(20)', 'EXTRA': '', 'GENERATION_EXPRESSION': ''},
            {'TABLE_NAME': 't_keyed', 'COLUMN_NAME': 'updated_at', 'ORDINAL_POSITION': 3, 'COLUMN_DEFAULT': None, 'IS_NULLABLE': 'YES', 'DATA_TYPE': 'datetime', 'COLUMN_TYPE': 'datetime(6)', 'EXTRA': '', 'GENERATION_EXPRESSION': ''},
            {'TABLE_NAME': 't_nokey', 'COLUMN_NAME': 'x', 'ORDINAL_POSITION': 1, 'COLUMN_DEFAULT': None, 'IS_NULLABLE': 'YES', 'DATA_TYPE': 'int', 'COLUMN_TYPE': 'int(11)', 'EXTRA': '', 'GENERATION_EXPRESSION': ''},
            {'TABLE_NAME': 't_nokey', 'COLUMN_NAME': 'y', 'ORDINAL_POSITION': 2, 'COLUMN_DEFAULT': None, 'IS_NULLABLE': 'YES', 'DATA_TYPE': 'varchar', 'COLUMN_TYPE': 'varchar(50)', 'EXTRA': '', 'GENERATION_EXPRESSION': ''},
            {'TABLE_NAME': 't_nokey', 'COLUMN_NAME': 'z', 'ORDINAL_POSITION': 3, 'COLUMN_DEFAULT': None, 'IS_NULLABLE': 'YES', 'DATA_TYPE': 'int', 'COLUMN_TYPE': 'int(11)', 'EXTRA': 'computed', 'GENERATION_EXPRESSION': '`x` * 2'},
            {'TABLE_NAME': 't_email', 'COLUMN_NAME': 'email', 'ORDINAL_POSITION': 1, 'COLUMN_DEFAULT': None, 'IS_NULLABLE': 'NO', 'DATA_TYPE': 'varchar', 'COLUMN_TYPE': 'varchar(128)', 'EXTRA': '', 'GENERATION_EXPRESSION': ''},
            {'TABLE_NAME': 't_email', 'COLUMN_NAME': 'n', 'ORDINAL_POSITION': 2, 'COLUMN_DEFAULT': None, 'IS_NULLABLE': 'YES', 'DATA_TYPE': 'int', 'COLUMN_TYPE': 'int(11)', 'EXTRA': '', 'GENERATION_EXPRESSION': ''}],
        'SELECT TABLE_NAME, NON_UNIQUE': [
            {'TABLE_NAME': 't_keyed', 'NON_UNIQUE': 0, 'INDEX_NAME': 'PRIMARY', 'SEQ_IN_INDEX': 1, 'COLUMN_NAME': 'id', 'INDEX_TYPE': 'COLUMNSTORE HASH'},
            {'TABLE_NAME': 't_keyed', 'NON_UNIQUE': 1, 'INDEX_NAME': '__SHARDKEY', 'SEQ_IN_INDEX': 1, 'COLUMN_NAME': 'id', 'INDEX_TYPE': 'SHARD'},
            {'TABLE_NAME': 't_email', 'NON_UNIQUE': 0, 'INDEX_NAME': 'PRIMARY', 'SEQ_IN_INDEX': 1, 'COLUMN_NAME': 'email', 'INDEX_TYPE': 'COLUMNSTORE HASH'}],
        'SELECT TABLE_NAME, SUM(ROWS)': [{'TABLE_NAME': 't_keyed', 'estimated_rows': 25000000}, {'TABLE_NAME': 't_nokey', 'estimated_rows': 12000000},
                                         {'TABLE_NAME': 't_email', 'estimated_rows': 100}],
        'SELECT f.TABLE_NAME': [{'TABLE_NAME': 't_keyed', 'primary_files_gib': 10.5}, {'TABLE_NAME': 't_nokey', 'primary_files_gib': 2.25}],
        'SELECT GRANTEE, PRIVILEGE_TYPE FROM information_schema.USER_PRIVILEGES': [
            {'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'INSERT'}, {'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'SELECT'},
            {'GRANTEE': "'s2rebuild_runner'@'%'", 'PRIVILEGE_TYPE': 'INSERT'}, {'GRANTEE': "'reader'@'%'", 'PRIVILEGE_TYPE': 'SELECT'}],
        'SELECT GRANTEE, TABLE_SCHEMA': [{'GRANTEE': "'retention'@'%'", 'TABLE_SCHEMA': DB, 'PRIVILEGE_TYPE': 'DELETE'}],
        'SELECT GRANTEE, TABLE_NAME': [],
        'SELECT ID, USER, HOST': [{'ID': 1, 'USER': 'app', 'HOST': 'app-host:4455', 'DB': DB, 'COMMAND': 'Query', 'TIME': 1, 'INFO': 'INSERT INTO t_keyed VALUES (1)'}],
        'SELECT PIPELINE_NAME, STATE': [{'PIPELINE_NAME': 'p1', 'STATE': 'Running'}],
        'SELECT PIPELINE_NAME, SOURCE_TYPE': [{'PIPELINE_NAME': 'p1', 'SOURCE_TYPE': 'KAFKA', 'SOURCE_PARTITION_ID': 0, 'EARLIEST_OFFSET': 1, 'LATEST_OFFSET': 12, 'LATEST_LOADED_OFFSET': 10, 'LATEST_SUCCESSFUL_OFFSET': 10}],
        'SELECT PIPELINE_NAME, BATCH_STATE': [{'PIPELINE_NAME': 'p1', 'BATCH_STATE': 'Succeeded', 'START_TIME': '2026-10-20 10:00:00',
                                               'ROWS_INSERTED': 400, 'ROWS_UPDATED': 100}],
        'SELECT PIPELINE_NAME, ERROR_UNIX_TIMESTAMP': [{'PIPELINE_NAME': 'p1', 'ERROR_UNIX_TIMESTAMP': 1792486800.25, 'ERROR_MESSAGE': 'example error'}],
        'SHOW CREATE': show_create,
        'SELECT COUNT(*) AS n FROM `example_orders_db`.`t_email`': [{'n': 100}],
        'SELECT COUNT(*) AS n': [{'n': 42}],
    }


REF_DEFINITION = 'CREATE REFERENCE TABLE `t_ref` (\n  `code` varchar(8) NOT NULL,\n  `label` varchar(40) DEFAULT NULL,\n  PRIMARY KEY (`code`),\n  SORT KEY (`code`)\n)'


def reference_answers(count_rows):
    """The canned answers plus the reference table t_ref: no TABLE_STATISTICS row (fact 36), COUNT(*) answering count_rows."""
    answers = canned_answers()
    answers['SELECT TABLE_NAME, TABLE_TYPE'] = answers['SELECT TABLE_NAME, TABLE_TYPE'] + [{'TABLE_NAME': 't_ref', 'TABLE_TYPE': 'BASE TABLE'}]
    answers['SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION'] = answers['SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION'] + [
        {'TABLE_NAME': 't_ref', 'COLUMN_NAME': 'code', 'ORDINAL_POSITION': 1, 'COLUMN_DEFAULT': None, 'IS_NULLABLE': 'NO', 'DATA_TYPE': 'varchar',
         'COLUMN_TYPE': 'varchar(8)', 'EXTRA': '', 'GENERATION_EXPRESSION': ''},
        {'TABLE_NAME': 't_ref', 'COLUMN_NAME': 'label', 'ORDINAL_POSITION': 2, 'COLUMN_DEFAULT': None, 'IS_NULLABLE': 'YES', 'DATA_TYPE': 'varchar',
         'COLUMN_TYPE': 'varchar(40)', 'EXTRA': '', 'GENERATION_EXPRESSION': ''}]
    answers['SELECT TABLE_NAME, NON_UNIQUE'] = answers['SELECT TABLE_NAME, NON_UNIQUE'] + [
        {'TABLE_NAME': 't_ref', 'NON_UNIQUE': 0, 'INDEX_NAME': 'PRIMARY', 'SEQ_IN_INDEX': 1, 'COLUMN_NAME': 'code', 'INDEX_TYPE': 'COLUMNSTORE HASH'}]
    show_create = answers['SHOW CREATE']
    answers['SHOW CREATE'] = lambda norm, args: ([{'Table': 't_ref', 'Create Table': REF_DEFINITION}] if norm.startswith('SHOW CREATE TABLE')
                                                 and '`t_ref`' in norm else show_create(norm, args))
    generic = answers.pop('SELECT COUNT(*) AS n')
    answers['SELECT COUNT(*) AS n FROM `example_orders_db`.`t_ref`'] = count_rows
    answers['SELECT COUNT(*) AS n'] = generic
    return answers


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.cfg = config_from(lambda d: d['connections'].update(owner={'user': 'admin', 'password_keychain': 's2rebuild-owner'}))
        self.db = self.cfg.databases[0]

    def test_capture_with_owner_builds_the_inventory(self):
        runner = FakeSession(canned_answers())
        owner = FakeSession(canned_answers())
        lines = []
        inv = I.capture(runner, self.db, self.cfg, owner, exact_counts=True, log=lines.append, step='P3', samples=3, sleep=lambda s: None)
        self.assertEqual(inv['database'], DB)
        self.assertEqual(inv['layout']['NUM_PARTITIONS'], 16)
        self.assertEqual(inv['attachment']['STORAGE_ID'], 'sid-1')
        names = [t['name'] for t in inv['tables']]
        self.assertEqual(names, ['t_email', 't_keyed', 't_nokey'])
        keyed = inv['tables'][1]
        self.assertEqual(keyed['auto_increment'], {'column': 'id', 'counter': 77})
        self.assertEqual(keyed['est_rows'], 25000000)
        self.assertEqual(keyed['exact_rows'], 42)
        self.assertEqual(keyed['size_gib'], 10.5)
        self.assertEqual(keyed['storage_type'], 'columnstore')
        self.assertEqual(inv['tables'][2]['computed_columns'], ['z'])
        self.assertEqual(inv['views'][0]['definer'], '`app`@`%`')
        self.assertEqual(inv['routines'], [PROC1_CAPTURED], 'the procedure with its definition, read through the owner session')
        self.assertEqual((inv['routines_captured'], inv['routines_note']), (True, '1 routines captured with the owner profile'))
        self.assertTrue(any(sql.startswith('SHOW CREATE PROCEDURE `%s`.`proc1`' % DB) for sql, _ in owner.statements))
        self.assertFalse(any('ROUTINES' in sql or 'SHOW CREATE PROCEDURE' in sql or 'USER_PRIVILEGES' in sql for sql, _ in runner.statements),
                         'the runner reads neither the routines nor the accounts (facts 21 and L12)')
        users = [a['user'] for a in inv['accounts']]
        self.assertEqual(users, ['app', 'retention'], 'runner excluded, read-only account excluded')
        self.assertEqual(inv['accounts'][0]['privileges'], ['INSERT'])
        self.assertEqual(inv['sessions'], [{'user': 'app', 'host': 'app-host', 'samples_seen': 3, 'statement_kinds': ['INSERT']}])
        self.assertEqual(inv['size_gib_total'], 12.75)
        pipes = inv['pipelines']
        self.assertTrue(pipes['captured'])
        p1 = pipes['items'][0]
        self.assertEqual((p1['source_type'], p1['target'], p1['batch_interval'], p1['duplicate_handling']), ('KAFKA', 'table t_keyed', 1000, 'REPLACE'))
        self.assertIn("CREDENTIALS '<<CREDENTIALS>>'", p1['definition'])
        self.assertNotIn('SECRET-VALUE', json.dumps(inv))
        self.assertEqual(p1['alter_statements'], ["ALTER PIPELINE `p1` SET OFFSETS '{\"0\": 10}'"])
        self.assertEqual(p1['offsets'][0]['loaded'], 10)
        self.assertEqual((p1['rows_24h'], p1['errors_24h'], p1['last_error']['message']), (500, 1, 'example error'))
        self.assertEqual((p1['last_batch_time'], p1['last_error']['time']), ('2026-10-20 10:00:00', '2026-10-20 09:00:00'))
        self.assertEqual(len(lines), 1)
        for sql, _ in runner.statements + owner.statements:
            read_guard().check(sql)
        self.assertEqual(sum(1 for sql, _ in runner.statements if sql.startswith('SELECT ID, USER')), 3, 'three process list samples')
        import datetime as dt
        captured = dt.datetime.strptime(inv['captured_utc'], '%Y-%m-%dT%H:%M:%SZ')
        for sql, args in owner.statements:
            if 'PIPELINES_BATCHES_SUMMARY' in sql or 'PIPELINES_ERRORS' in sql:
                since = dt.datetime.strptime(args[1], '%Y-%m-%d %H:%M:%S')
                self.assertAlmostEqual((captured - since).total_seconds(), 24 * 3600, delta=5, msg='the 24 hour window starts a day before the capture')

    def test_pipeline_queries_name_only_columns_helios_10_8_has(self):
        """Columns read live on engine 10.8.0 (October 26, 2026); P3 failed with error 1054 on BATCH_ROWS_WRITTEN before."""
        helios = {
            'PIPELINES_BATCHES_SUMMARY': {'DATABASE_NAME', 'PIPELINE_NAME', 'BATCH_ID', 'ROWS_INSERTED', 'ROWS_UPDATED', 'ROWS_DELETED',
                                          'BATCH_STATE', 'START_TIME', 'BATCH_TIME', 'ROWS_PER_SEC', 'ROWS_STREAMED', 'NUM_PARTITIONS',
                                          'NUM_PARTITIONS_FINISHED', 'MB_STREAMED', 'MB_PER_SEC', 'EXTRACTOR_WAIT_TIME', 'TRANSFORM_WAIT_TIME'},
            'PIPELINES_ERRORS': {'DATABASE_NAME', 'PIPELINE_NAME', 'ERROR_UNIX_TIMESTAMP', 'ERROR_TYPE', 'ERROR_CODE', 'ERROR_MESSAGE',
                                 'ERROR_KIND', 'STD_ERROR', 'LOAD_DATA_LINE', 'LOAD_DATA_LINE_NUMBER', 'BATCH_ID', 'ERROR_ID',
                                 'BATCH_SOURCE_PARTITION_ID', 'BATCH_EARLIEST_OFFSET', 'BATCH_LATEST_OFFSET', 'HOST', 'PORT', 'PARTITION'},
        }
        for sql in (I.BATCHES_SQL, I.ERRORS_SQL):
            view = next(name for name in helios if name in sql)
            named = set(re.findall(r'\b[A-Z][A-Z_]+[A-Z]\b', sql)) - {'SELECT', 'FROM', 'WHERE', 'AND', 'UNIX_TIMESTAMP', view}
            named.discard('information_schema'.upper())
            self.assertEqual(named - helios[view], set(), sql)

    def test_capture_without_owner_marks_pipelines_not_captured(self):
        inv = I.capture(FakeSession(canned_answers()), self.db, self.cfg, None, sleep=lambda s: None)
        self.assertFalse(inv['pipelines']['captured'])
        self.assertIn('no owner profile', inv['pipelines']['note'])

    def test_capture_refuses_a_missing_source(self):
        answers = canned_answers()
        answers['SELECT NUM_PARTITIONS'] = []
        with self.assertRaisesRegex(KitError, 'not reported'):
            I.capture(FakeSession(answers), self.db, self.cfg, None, sleep=lambda s: None)

    def test_w4_a_reference_table_is_counted_exactly_marked_and_planned_as_one_bucket(self):
        """TABLE_STATISTICS has no row for a reference table (fact 36): the capture takes COUNT(*) without --exact-counts, the row
        count is the exact one, the plan is one bucket on the staging path, and tables.csv carries the reference and copy_path columns."""
        runner = FakeSession(reference_answers([{'n': 30_000_000}]))
        lines = []
        inv = I.capture(runner, self.db, self.cfg, None, log=lines.append, sleep=lambda s: None)
        by_name = {t['name']: t for t in inv['tables']}
        ref = by_name['t_ref']
        self.assertEqual((ref['reference'], ref['storage_type'], ref['exact_rows'], ref['est_rows']), (True, 'columnstore', 30_000_000, 30_000_000))
        self.assertEqual((by_name['t_keyed']['reference'], by_name['t_keyed']['exact_rows'], by_name['t_keyed']['est_rows']), (False, None, 25_000_000),
                         'the other tables keep their estimates and are not counted')
        counted = [sql for sql, _ in runner.statements if sql.startswith('SELECT COUNT(*)')]
        self.assertEqual(counted, ['SELECT COUNT(*) AS n FROM `example_orders_db`.`t_ref`'])
        self.assertIn('4 tables (1 reference, counted exactly)', lines[0])
        plans = {p.name: p for p in I.plan_tables(inv, self.cfg, self.db)}
        self.assertEqual((plans['t_ref'].reference, plans['t_ref'].n_buckets, plans['t_ref'].copy_path), (True, 1, 'staging'),
                         'one bucket although 30 M rows would take 3 on the direct path')
        self.assertEqual((plans['t_keyed'].reference, plans['t_keyed'].n_buckets, plans['t_keyed'].copy_path), (False, 3, 'direct'))
        self.assertEqual((plans['t_ref'].describe()['reference'], plans['t_ref'].describe()['copy_path']), (True, 'staging'))
        summary = I.plan_summary(list(plans.values()))
        self.assertEqual((summary['reference_tables'], summary['statements']['staging_tables']), (1, 1))
        with tempfile.TemporaryDirectory() as tmp:
            out = I.write(inv, Path(tmp) / 'inv', list(plans.values()))
            csv_text = (out / 'tables.csv').read_text()
            self.assertIn('storage_type,reference,copy_path,key_kind', csv_text.splitlines()[0])
            self.assertIn('t_ref,30000000,30000000,,columnstore,1,staging,key,code,1,', csv_text)
            self.assertIn('t_keyed,25000000,,10.5,columnstore,0,direct,key,id,3,', csv_text)
        self.assertEqual(I.storage_type('CREATE ROWSTORE REFERENCE TABLE `r` (`a` int, PRIMARY KEY (`a`))'), 'rowstore')
        self.assertTrue(I.is_reference('CREATE ROWSTORE REFERENCE TABLE `r` (`a` int)'))
        self.assertFalse(I.is_reference('CREATE ROWSTORE TABLE `r` (`a` int)'))

    def test_w4_a_reference_table_whose_count_returns_no_row_refuses_the_capture(self):
        """Without a count the plan of a reference table would have no row count, so the capture refuses by name."""
        with self.assertRaisesRegex(KitError, 'Reference table t_ref: COUNT\\(\\*\\) returned no row'):
            I.capture(FakeSession(reference_answers([])), self.db, self.cfg, None, sleep=lambda s: None)

    def test_write_and_load_round_trip_with_private_files(self):
        inv = I.capture(FakeSession(canned_answers()), self.db, self.cfg, FakeSession(canned_answers()), sleep=lambda s: None)
        plans = I.plan_tables(inv, self.cfg, self.db)
        with tempfile.TemporaryDirectory() as tmp:
            out = I.write(inv, Path(tmp) / 'inv', plans)
            self.assertTrue((out / 'inventory.json').is_file())
            self.assertTrue((out / 'tables.csv').is_file())
            for rel in ('inventory.json', 'definitions/t_keyed.sql', 'definitions/v1.view.sql', 'pipelines/p1.sql', 'pipelines/recreate-with-placeholders.sql'):
                path = out / rel
                self.assertTrue(path.is_file(), rel)
                self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600, rel)
            text = (out / 'pipelines' / 'p1.sql').read_text()
            self.assertIn('<<CREDENTIALS>>', text)
            self.assertNotIn('SECRET-VALUE', text)
            self.assertIn('ALTER PIPELINE', text)
            again = I.load(out)
            self.assertEqual(again['tables'][1]['name'], 't_keyed')
            csv_text = (out / 'tables.csv').read_text()
            self.assertIn('t_keyed,25000000,,10.5,columnstore,0,direct,key,id,3,updated_at,0,id,', csv_text)
            self.assertIn('t_nokey,12000000,,2.25,rowstore,0,direct,row,,2,,0,,z', csv_text)
            with self.assertRaisesRegex(KitError, 'No inventory'):
                I.load(Path(tmp) / 'nowhere')
            (out / 'inventory.json').write_text('{"kit": "other"}')
            with self.assertRaisesRegex(KitError, 'not an s2rebuild inventory'):
                I.load(out)


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.cfg = config_from()
        self.db = self.cfg.databases[0]
        self.inv = I.load(FIXTURE_INVENTORY)

    def test_fixture_plan(self):
        plans = I.plan_tables(self.inv, self.cfg, self.db)
        by = {p.name: p for p in plans}
        self.assertEqual([p.name for p in plans], ['order_items', 'orders', 'events', 'customers', 'totals', 'empty_table'], 'largest first')
        self.assertEqual(by['order_items'].n_buckets, 32, 'capped at max_buckets_per_table')
        self.assertEqual(by['order_items'].key_columns, ['order_id', 'line_no'])
        self.assertEqual(by['order_items'].predicate(31), "CRC32(CONCAT_WS('|', `order_id`, `line_no`)) MOD 32 = 31")
        self.assertEqual(by['orders'].n_buckets, 3)
        self.assertEqual(by['orders'].time_column, 'updated_at')
        self.assertEqual(by['events'].key_kind, 'row')
        self.assertEqual(by['events'].predicate(0), "CRC32(CONCAT_WS('|', COALESCE(`event_date`, ''), COALESCE(`kind`, ''), COALESCE(`payload`, ''), COALESCE(`value`, ''))) MOD 2 = 0")
        self.assertTrue(by['customers'].unique_key_lock_risk, 'varchar(128) unique key')
        self.assertFalse(by['orders'].unique_key_lock_risk)
        self.assertEqual(by['totals'].columns, ['day', 'orders', 'cents'], 'computed column excluded')
        self.assertEqual(by['totals'].computed_columns, ['dollars'])
        self.assertEqual(by['empty_table'].n_buckets, 1)
        self.assertEqual(by['empty_table'].key_kind, 'row')
        self.assertIsNone(by['orders'].concurrency)
        summary = I.plan_summary(plans)
        self.assertEqual((summary['tables'], summary['buckets']), (6, 40))
        self.assertEqual(summary['statements']['insert_select'], 40)
        with self.assertRaises(KitError):
            by['orders'].predicate(3)

    def test_overrides_and_concurrency(self):
        cfg = config_from(lambda d: (d['databases'][0]['tables'].update(orders={'chunk_rows': 5000000, 'concurrency': 2}),
                                     d.update(copy={'table_concurrency': 1})))
        plans = {p.name: p for p in I.plan_tables(self.inv, cfg, cfg.databases[0])}
        self.assertEqual(plans['orders'].n_buckets, 5)
        self.assertEqual(plans['orders'].concurrency, 2)
        self.assertEqual(plans['events'].concurrency, 1, 'table_concurrency 1 serializes every table without an override')

    def test_buckets_rule(self):
        self.assertEqual(I.buckets_for(0, 10_000_000, 5_000_000, 32), 1)
        self.assertEqual(I.buckets_for(4_999_999, 10_000_000, 5_000_000, 32), 1)
        self.assertEqual(I.buckets_for(5_000_000, 10_000_000, 5_000_000, 32), 1)
        self.assertEqual(I.buckets_for(25_000_000, 10_000_000, 5_000_000, 32), 3)
        self.assertEqual(I.buckets_for(1_334_952_052, 10_000_000, 5_000_000, 32), 32)
        self.assertEqual(I.buckets_for(1_334_952_052, 10_000_000, 5_000_000, 1000), 134)
        with self.assertRaises(KitError):
            I.buckets_for(-1, 10_000_000, 5_000_000, 32)

    def test_key_choice_rules(self):
        cols = [{'name': 'a', 'nullable': True, 'column_type': 'int(11)', 'generation_expression': ''},
                {'name': 'b', 'nullable': False, 'column_type': 'varchar(32)', 'generation_expression': ''},
                {'name': 'c', 'nullable': False, 'column_type': 'int(11)', 'generation_expression': '`b` + 1'}]
        table = {'columns': cols, 'computed_columns': ['c'], 'keys': [
            {'name': 'u_a', 'unique': True, 'type': 'HASH', 'columns': ['a']},
            {'name': 'u_c', 'unique': True, 'type': 'HASH', 'columns': ['c']},
            {'name': 'u_b', 'unique': True, 'type': 'HASH', 'columns': ['b']},
            {'name': 'idx', 'unique': False, 'type': 'HASH', 'columns': ['a']}]}
        self.assertEqual(I.choose_key(table), ('key', ['b']), 'nullable and computed unique keys are skipped')
        table['keys'].insert(0, {'name': 'PRIMARY', 'unique': True, 'type': 'HASH', 'columns': ['b', 'a']})
        self.assertEqual(I.choose_key(table), ('key', ['b']), 'a PRIMARY with a nullable column is not usable')
        self.assertEqual(I.choose_key({'columns': cols, 'keys': []}), ('row', []))
        self.assertFalse(I.lock_risk({'columns': cols, 'keys': [{'name': 'u_b', 'unique': True, 'columns': ['b']}]}))
        self.assertTrue(I.lock_risk({'columns': cols, 'keys': [{'name': 'PRIMARY', 'unique': True, 'columns': ['b', 'a']}]}), 'composite with varchar')
        cols[1]['column_type'] = 'varchar(65)'
        self.assertTrue(I.lock_risk({'columns': cols, 'keys': [{'name': 'u_b', 'unique': True, 'columns': ['b']}]}))
        self.assertEqual(I.time_column_for({'columns': cols}, ('updated_at', 'b')), 'b')
        self.assertIsNone(I.time_column_for({'columns': cols}, ('updated_at',)))

    def test_helpers(self):
        self.assertEqual(I.auto_increment_counter('CREATE TABLE `x` (`id` bigint) AUTO_INCREMENT=12 X=1'), 12)
        self.assertIsNone(I.auto_increment_counter('CREATE TABLE `x` (`id` bigint)'))
        self.assertEqual(I.normalize_definition('CREATE TABLE `x`  (\n `a` int\n) AUTO_INCREMENT=5  Y=1'), 'CREATE TABLE `x` ( `a` int ) Y=1')
        self.assertEqual(I.storage_type('CREATE TABLE t (a int, SORT KEY (a))'), 'columnstore')
        self.assertEqual(I.storage_type('CREATE TABLE t (a int, PRIMARY KEY (a))'), 'rowstore')
        self.assertEqual(I.strip_definer('CREATE DEFINER=`u`@`%` VIEW `v` AS SELECT 1'), 'CREATE VIEW `v` AS SELECT 1')
        self.assertEqual(I.redact_credentials("LOAD DATA KAFKA 'x' CREDENTIALS '{\"p\": \"s\\'q\"}' INTO TABLE t"),
                         "LOAD DATA KAFKA 'x' CREDENTIALS '<<CREDENTIALS>>' INTO TABLE t")
        self.assertEqual(I.redact_credentials(r'LOAD DATA KAFKA "x" CREDENTIALS "{\"p\": \"dq-secret\"}" INTO TABLE t'),
                         "LOAD DATA KAFKA \"x\" CREDENTIALS '<<CREDENTIALS>>' INTO TABLE t")
        self.assertEqual(I.redact_credentials('LOAD DATA KAFKA "x" CREDENTIALS "it""s-dq-secret" INTO TABLE t'),
                         "LOAD DATA KAFKA \"x\" CREDENTIALS '<<CREDENTIALS>>' INTO TABLE t")
        self.assertEqual(I.redact_credentials("LOAD DATA KAFKA 'x' CREDENTIALS 'it''s-a-secret' INTO TABLE t"),
                         "LOAD DATA KAFKA 'x' CREDENTIALS '<<CREDENTIALS>>' INTO TABLE t")
        self.assertEqual(I.redact_credentials('LOAD DATA S3 "b" CONFIG "{}" INTO TABLE t'), 'LOAD DATA S3 "b" CONFIG "{}" INTO TABLE t')
        with self.assertRaisesRegex(KitError, 'cannot replace'):
            I.redact_credentials("LOAD DATA KAFKA 'x' CREDENTIALS <hidden> INTO TABLE t", 'p9')
        self.assertTrue(I.has_redaction_marker("CREDENTIALS '<redacted>'"))
        self.assertFalse(I.has_redaction_marker("CREDENTIALS '<<CREDENTIALS>>'"))
        create, alters = I.parse_pipeline_statements([{'Pipeline': 'p', 'Create Pipeline': 'CREATE PIPELINE `p` AS LOAD DATA S3 "b/k" INTO TABLE t',
                                                       'Alter 1': 'ALTER PIPELINE `p` SET OFFSETS 1', 'Alter 2': None}])
        self.assertTrue(create.startswith('CREATE'))
        self.assertEqual(alters, ['ALTER PIPELINE `p` SET OFFSETS 1'])
        d = I.describe_pipeline('CREATE PIPELINE `p` AS LOAD DATA S3 "bucket/prefix" CONFIG "{}" BATCH_INTERVAL 500 SKIP DUPLICATE KEY ERRORS INTO TABLE `t` FORMAT CSV')
        self.assertEqual(d, {'source_type': 'S3', 'source_location': 'bucket/prefix', 'target': 'table t', 'batch_interval': 500,
                             'duplicate_handling': 'SKIP DUPLICATE KEY ERRORS'})
        self.assertEqual(I.describe_pipeline('CREATE PIPELINE `p` AS LOAD DATA KAFKA "h/t" INTO PROCEDURE `pr`')['target'], 'procedure pr')


class RoutineTests(unittest.TestCase):
    def test_a_load_target_the_replacement_will_not_hold_is_refused_at_p3(self):
        """A pipeline INTO PROCEDURE is refused when objects.routines.ignore lists that procedure, or the capture lacks it (O9 would find
        no target); a captured, migrated procedure passes and the ignored ones are returned."""
        from s2rebuild import routines as R
        answers = canned_answers()
        show = answers['SHOW CREATE']

        def show_create(norm, args):
            rows = show(norm, args)
            if norm.startswith('SHOW CREATE PIPELINE'):
                rows[0]['Create Pipeline'] = rows[0]['Create Pipeline'].replace('INTO TABLE `t_keyed`', 'INTO PROCEDURE `proc1`')
            return rows
        answers['SHOW CREATE'] = show_create

        def config(names):
            return config_from(lambda d: (d['connections'].update(owner={'user': 'admin', 'password_keychain': 's2rebuild-owner'}),
                                          d.update(objects={'routines': {'ignore': names}})))
        cfg = config(['proc1'])
        inv = I.capture(FakeSession(answers), cfg.databases[0], cfg, FakeSession(answers), sleep=lambda s: None)
        self.assertEqual(inv['pipelines']['items'][0]['target'], 'procedure proc1')
        with self.assertRaisesRegex(KitError, 'P3 refused for example_orders_db: p1 loads into procedure proc1, which objects.routines.ignore lists, '
                                              'so O9 would find no target'):
            R.refuse_pipeline_targets(inv, cfg, cfg.databases[0], 'P3')
        cfg = config([])
        self.assertEqual(R.refuse_pipeline_targets(inv, cfg, cfg.databases[0], 'P3'), [])
        inv['routines'] = []
        self.assertEqual(R.pipeline_target_refusals(cfg, cfg.databases[0], inv), ['p1 loads into procedure proc1, which the capture does not hold'])
        inv['routines_captured'] = False
        self.assertEqual(R.pipeline_target_refusals(cfg, cfg.databases[0], inv), [], 'without a routine capture the target cannot be checked')

    def test_a_qualified_name_ignores_the_routine_for_its_database_only(self):
        """database.routine ignores the routine on that database only; a bare name ignores it on every configured database."""
        from s2rebuild import routines as R

        def config(names):
            def mutate(d):
                d['databases'].append({'source': 'second_db', 'layout': {'partitions': 8, 'sub_partitions': 64}})
                d['objects'] = {'routines': {'ignore': names}}
            return config_from(mutate)
        captured = [PROC1_CAPTURED]
        cfg = config([DB + '.proc1'])
        first, second = cfg.databases
        self.assertEqual(R.split(cfg, first, captured), ([], ['procedure proc1']))
        self.assertEqual(R.split(cfg, second, captured), ([PROC1_CAPTURED], []))
        cfg = config(['proc1'])
        self.assertEqual([R.split(cfg, db, captured) for db in cfg.databases], [([], ['procedure proc1'])] * 2)
        snapshot = {'routines': captured, 'routines_captured': True}
        self.assertEqual(R.summary(cfg, first, snapshot), '1 routines (0 functions, 0 TVFs, 0 aggregates, 1 procedures), 1 ignored by configuration: procedure proc1')
        self.assertEqual(R.summary(cfg, first, {'routines': [], 'routines_captured': False, 'routines_note': 'no owner profile'}), 'not captured (no owner profile)')


def answers_with_pipeline_rows(rows):
    """canned_answers() where SHOW CREATE PIPELINE returns the given rows."""
    answers = canned_answers()
    show = answers['SHOW CREATE']
    answers['SHOW CREATE'] = lambda norm, args: [dict(r) for r in rows] if norm.startswith('SHOW CREATE PIPELINE') else show(norm, args)
    return answers


class PipelineCaptureTests(unittest.TestCase):
    """F14: the CREDENTIALS check refuses only an unreplaced clause, and an empty definition refuses the capture."""

    def setUp(self):
        self.cfg = config_from(lambda d: d['connections'].update(owner={'user': 'admin', 'password_keychain': 's2rebuild-owner'}))
        self.db = self.cfg.databases[0]

    def capture(self, answers):
        return I.capture(FakeSession(answers), self.db, self.cfg, FakeSession(answers), sleep=lambda s: None)

    def test_capture_on_a_read_only_attachment_lists_no_pipeline(self):
        """W6: at O4 on Path A the source is READ ONLY, where PIPELINES is empty (fact 23): the capture reads no pipeline view and
        records the note O9 and the drift report rely on, instead of 0 pipelines captured."""
        answers = canned_answers()
        answers['SELECT DATABASE_NAME, ATTACH_TYPE'] = [dict(answers['SELECT DATABASE_NAME, ATTACH_TYPE'][0], ATTACH_TYPE='READONLY')]
        owner = FakeSession(answers)
        inv = I.capture(FakeSession(answers), self.db, self.cfg, owner, step='O4', sleep=lambda s: None)
        self.assertEqual(inv['pipelines'], {'captured': False, 'items': [], 'read_only': True, 'note': I.PIPELINES_READ_ONLY_NOTE})
        self.assertFalse([s for s, _ in owner.statements if 'PIPELINES' in s], 'no pipeline view is read on a READ ONLY attachment')
        self.assertTrue([s for s, _ in owner.statements if 'ROUTINES' in s], 'the routines are still read with the owner session')

    def test_f14_a_topic_table_or_column_named_credentials_is_captured(self):
        """The word credentials as a topic, a target table or a mapped column is no CREDENTIALS clause; an unreplaced clause still refuses."""
        definition = ("CREATE PIPELINE `p1` AS LOAD DATA KAFKA 'b:9092/credentials' BATCH_INTERVAL 1000 REPLACE INTO TABLE `credentials` "
                      "FORMAT JSON (`id` <- `id`, credentials <- data::credentials)")
        inv = self.capture(answers_with_pipeline_rows([{'Pipeline': 'p1', 'Create Pipeline': definition}]))
        p1 = inv['pipelines']['items'][0]
        self.assertEqual(p1['definition'], definition, 'kept as the server returned it')
        self.assertEqual((p1['source_location'], p1['target']), ('b:9092/credentials', 'table credentials'))
        for plain in ("CREATE PIPELINE `p2` AS LOAD DATA S3 'bucket/credentials/' INTO TABLE credentials (id, credentials)",
                      'CREATE PIPELINE `p3` AS LOAD DATA KAFKA "b/credentials" INTO TABLE `t`'):
            self.assertEqual(I.redact_credentials(plain, 'p2'), plain)
        kept = I.redact_credentials("CREATE PIPELINE `p4` AS LOAD DATA KAFKA 'b/credentials' CREDENTIALS <CREDENTIALS REDACTED> INTO TABLE credentials")
        self.assertEqual(kept, "CREATE PIPELINE `p4` AS LOAD DATA KAFKA 'b/credentials' CREDENTIALS '<<CREDENTIALS>>' INTO TABLE credentials")
        for unreplaced in ("CREDENTIALS <hidden>", "CREDENTIALS '{\"sasl.password\": \"unterminated"):
            with self.assertRaisesRegex(KitError, 'Pipeline p5: the CREDENTIALS clause has a form the kit cannot replace'):
                I.redact_credentials("CREATE PIPELINE `p5` AS LOAD DATA KAFKA 'b/credentials' %s INTO TABLE credentials" % unreplaced, 'p5')

    def test_f14_an_empty_definition_refuses_the_capture_naming_the_pipeline(self):
        """No CREATE row, an empty one, or no row at all from SHOW CREATE PIPELINE refuses the capture by name; nothing is returned to store."""
        shapes = {'empty CREATE value': [{'Pipeline': 'p1', 'Create Pipeline': ''}],
                  'ALTER rows only': [{'Pipeline': 'p1', 'Create Pipeline': "ALTER PIPELINE `p1` SET OFFSETS '{\"0\": 10}'"}],
                  'no row': []}
        for shape, rows in shapes.items():
            with self.subTest(shape=shape):
                with self.assertRaisesRegex(KitError, 'Pipeline p1: SHOW CREATE PIPELINE returned no CREATE statement; refusing to store'):
                    self.capture(answers_with_pipeline_rows(rows))


class DriftTests(unittest.TestCase):
    def test_drift_report(self):
        before = I.load(FIXTURE_INVENTORY)
        after = json.loads(json.dumps(before))
        after['tables'] = [t for t in after['tables'] if t['name'] != 'empty_table']
        after['tables'][0]['columns'].append({'name': 'new_col', 'ordinal': 99, 'data_type': 'int', 'column_type': 'int(11)', 'nullable': True,
                                              'default': None, 'extra': '', 'generation_expression': ''})
        after['tables'][0]['definition'] += '\n-- changed'
        after['tables'][1]['est_rows'] += 5
        after['views'][0]['definition'] = after['views'][0]['definition'].replace('SUM', 'MAX')
        after['tables'].append({'name': 'brand_new', 'columns': [], 'keys': [], 'definition': 'CREATE TABLE `brand_new` (a int)', 'est_rows': 0})
        after['routines'] = [{'name': 'new_proc', 'type': 'PROCEDURE'}, dict(before['routines'][1], definition='CREATE FUNCTION `tax_rate`(a INT) RETURNS INT AS BEGIN RETURN 2; END')]
        report = I.drift(before, after)
        self.assertEqual(report['objects_added'], ['brand_new', 'procedure new_proc'])
        self.assertEqual(report['objects_removed'], ['empty_table', 'procedure nightly_rollup'])
        self.assertEqual(report['columns_added'], ['orders.new_col'])
        self.assertEqual(report['columns_removed'], [])
        self.assertEqual(report['definitions_changed'], ['orders', 'order_totals', 'function tax_rate'], 'a routine whose normalized text changed')
        self.assertEqual(report['rows_changed'][0]['table'], 'order_items')
        self.assertEqual(report['count'], 8)
        lines = I.drift_lines(report)
        self.assertEqual(lines[0], 'DRIFT: object added: brand_new')
        self.assertIn('drift: 8 structural difference(s); row estimates changed on 1 table(s)', lines[-1])
        self.assertEqual(I.drift(before, before)['count'], 0)
        self.assertEqual(I.drift(before, json.loads(json.dumps(before)))['rows_changed'], [])

    def test_drift_with_pipelines(self):
        """W6: pipelines added, removed or changed are drift by name; a frozen source (no listing on a READ ONLY attachment) is a
        note, never a drop to zero; offsets and states alone are not drift."""
        before = I.load(FIXTURE_INVENTORY)
        after = json.loads(json.dumps(before))
        items = after['pipelines']['items']
        items[0]['definition'] = items[0]['definition'].replace('BATCH_INTERVAL', 'BATCH_INTERVAL 5 ')
        items[0]['state'] = 'Stopped'
        items[1]['alter_statements'] = ["ALTER PIPELINE `imports` SET OFFSETS '{\"0\": 99}'"]
        items.append(dict(items[1], name='late_feed'))
        report = I.drift(before, after)
        self.assertEqual(report['objects_added'], ['pipeline late_feed'])
        self.assertEqual(report['definitions_changed'], ['pipeline orders_stream'], 'a state or an offset alone is not drift')
        self.assertEqual((report['count'], report['pipelines_note']), (2, None))
        gone = json.loads(json.dumps(before))
        gone['pipelines']['items'] = gone['pipelines']['items'][:1]
        self.assertEqual(I.drift(before, gone)['objects_removed'], ['pipeline imports'])
        frozen = json.loads(json.dumps(before))
        frozen['pipelines'] = {'captured': False, 'items': [], 'read_only': True, 'note': I.PIPELINES_READ_ONLY_NOTE}
        report = I.drift(before, frozen)
        self.assertEqual((report['count'], report['objects_removed']), (0, []), 'two pipelines to none is not reported as removed')
        self.assertEqual(report['pipelines_note'], I.PIPELINES_READ_ONLY_NOTE)
        lines = I.drift_lines(report)
        self.assertEqual(lines[0], 'drift: pipelines not compared: ' + I.PIPELINES_READ_ONLY_NOTE)
        self.assertIn('drift: 0 structural difference(s)', lines[-1])


class SystemProfileTests(unittest.TestCase):
    def test_rows_per_database(self):
        from s2rebuild import estimate
        answers = {
            'SELECT DATABASE_NAME, NUM_PARTITIONS': [{'DATABASE_NAME': 'db_a', 'NUM_PARTITIONS': 32, 'NUM_SUB_PARTITIONS': 0},
                                                      {'DATABASE_NAME': 'db_b', 'NUM_PARTITIONS': 8, 'NUM_SUB_PARTITIONS': 64}],
            'SELECT DATABASE_NAME, TABLE_NAME, SUM(ROWS)': [{'DATABASE_NAME': 'db_a', 'TABLE_NAME': 't1', 'estimated_rows': 50000000},
                                                            {'DATABASE_NAME': 'db_a', 'TABLE_NAME': 't2', 'estimated_rows': 100},
                                                            {'DATABASE_NAME': 'db_b', 'TABLE_NAME': 't1', 'estimated_rows': 1000}],
            'SELECT f.DATABASE_NAME': [{'DATABASE_NAME': 'db_a', 'primary_files_gib': 40.0}],
            'SELECT DATABASE_NAME, COUNT(*) AS pipelines': [{'DATABASE_NAME': 'db_b', 'pipelines': 3}],
        }
        rows = I.system_profile(FakeSession(answers), config_from(), estimate.DEFAULT_BASELINE, estimate)
        self.assertEqual([r['database'] for r in rows], ['db_a', 'db_b'])
        self.assertEqual(rows[0]['tables'], 2)
        self.assertEqual(rows[0]['size_gib'], 40.0)
        self.assertEqual(rows[1]['pipelines'], 3)
        self.assertEqual(rows[1]['sub_partitions'], 64)
        self.assertGreater(rows[0]['copy_minutes_expected'], rows[1]['copy_minutes_expected'])


class HandlerTests(unittest.TestCase):
    def test_p3_and_o4_handlers_are_registered(self):
        from s2rebuild import cli
        self.assertIs(steps.HANDLERS['P3'], I.handle_p3)
        # W9: run takes the O4 capture itself (cli.handle_o4), which hands a capture already present to the inventory handler
        self.assertIs(steps.HANDLERS['O4'], cli.handle_o4)
        cfg = config_from()
        with tempfile.TemporaryDirectory() as tmp:
            cfg = config_from(lambda d: d.update(paths={'runs_dir': tmp}))
            ctx = steps.StepContext(cfg, cfg.databases[0], None, 'r1', None, dry_run=True)
            self.assertEqual(I.handle_p3(steps.REGISTRY['P3'], ctx).status, steps.STUB)
            ctx.dry_run = False
            self.assertEqual(I.handle_p3(steps.REGISTRY['P3'], ctx).status, steps.REFUSED)
            target = I.inventory_dir(cfg, 'r1', cfg.databases[0], 'P3')
            target.mkdir(parents=True)
            (target / I.INVENTORY_FILE).write_text('{"kit": "s2rebuild", "tables": []}')
            self.assertEqual(I.handle_p3(steps.REGISTRY['P3'], ctx).status, steps.PASSED)
            self.assertEqual(I.handle_o4(steps.REGISTRY['O4'], ctx).status, steps.REFUSED)


if __name__ == '__main__':
    unittest.main()
