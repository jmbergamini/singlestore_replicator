"""s2rebuild tests test_config: defaults, validation errors, unknown keys, per-table overrides.

Author: John Bergamini
"""
import datetime as dt
import tempfile
import unittest
from pathlib import Path

from s2rebuild.common import KitError
from s2rebuild.config import TIME_COLUMN_CANDIDATES, config_path, load_config
from tests.helpers import FIXTURE, ROOT, RUN_DATE, config_from, write_config


class DefaultsTests(unittest.TestCase):
    def test_fixture_loads_with_defaults(self):
        cfg = config_from()
        self.assertEqual(cfg.customer, 'Example Customer')
        self.assertEqual(cfg.workspace.port, 3306)
        self.assertEqual(cfg.workspace.ca_file, 'certs/singlestore_bundle.pem')
        self.assertEqual(cfg.runner.user, 's2rebuild_runner')
        self.assertEqual(cfg.runner.password_env, 'S2REBUILD_RUNNER_PASSWORD')
        self.assertIsNone(cfg.runner.password_keychain)
        self.assertIsNone(cfg.owner)
        self.assertEqual(cfg.copy.workers, 4)
        self.assertEqual(cfg.copy.chunk_rows, 10_000_000)
        self.assertEqual(cfg.copy.min_rows_for_chunking, 5_000_000)
        self.assertEqual(cfg.copy.max_buckets_per_table, 64)
        self.assertEqual(cfg.copy.slow_chunk_minutes, 30)
        self.assertEqual(cfg.copy.table_concurrency, 'auto')
        self.assertEqual(cfg.freeze.detach_wait_seconds, 120)
        self.assertTrue(cfg.freeze.detach_kill)
        self.assertEqual(cfg.verify.tiers, ['count', 'newest', 'fingerprint'])
        self.assertIsNone(cfg.verify.fingerprint_budget_minutes, 'no deadline unless set')
        self.assertIsNone(cfg.verify.sessions, 'copy.workers unless set')
        self.assertEqual((cfg.verify.full_fingerprint_max_rows, cfg.verify.sample_percent, cfg.verify.value_columns), (1_000_000_000, 5, 4))
        self.assertEqual((cfg.workspace.keepalive_minutes, cfg.copy.max_consecutive_failures), (15, 6))
        self.assertEqual(cfg.pipelines.route, 'kit')
        self.assertEqual(cfg.pipelines.credentials_env['KAFKA'], 'S2REBUILD_KAFKA_CREDENTIALS')
        self.assertEqual(cfg.estimate.rows_per_hour, 2_400_000_000)
        self.assertEqual(cfg.closeout.retain_days, 90)
        self.assertEqual(cfg.closeout.flexible_parallelism, 0.5)
        self.assertEqual(cfg.paths.runs_dir, 'customer/runs')
        db = cfg.databases[0]
        self.assertEqual(db.names, ('example_orders_db', 'example_orders_db_rebuild_20261101', 'example_orders_db_retained_20261101'))
        self.assertEqual((db.layout.partitions, db.layout.sub_partitions), (32, 512))
        self.assertEqual(db.time_column_candidates, TIME_COLUMN_CANDIDATES)
        self.assertEqual(db.app_managed_pipelines, ())
        self.assertEqual(cfg.window.start, dt.datetime(2026, 11, 1, 5, tzinfo=dt.timezone.utc))
        self.assertEqual(cfg.window.duration, dt.timedelta(hours=8))

    def test_default_names_come_from_the_run_date(self):
        def strip(d):
            del d['databases'][0]['replacement']
            del d['databases'][0]['retained']
        cfg = config_from(strip, run_date=dt.date(2026, 12, 24))
        self.assertEqual(cfg.databases[0].replacement, 'example_orders_db_rebuild_20261224')
        self.assertEqual(cfg.databases[0].retained, 'example_orders_db_retained_20261224')

    def test_sections_are_read_only(self):
        cfg = config_from()
        with self.assertRaises(AttributeError):
            cfg.copy.workers = 9
        self.assertEqual(cfg.copy.as_dict()['workers'], 4)
        self.assertEqual(cfg.copy.get('missing', 'x'), 'x')

    def test_optional_window_and_owner(self):
        cfg = config_from(lambda d: d.pop('window'))
        self.assertIsNone(cfg.window)
        cfg = config_from(lambda d: d['connections'].update(owner={'user': 'admin', 'password_keychain': 's2rebuild-owner'}))
        self.assertEqual(cfg.owner.name, 'owner')
        self.assertEqual(cfg.owner.password_keychain, 's2rebuild-owner')

    def test_resolve_paths_against_root(self):
        cfg = config_from(root=Path('/kit'))
        self.assertEqual(cfg.resolve('certs/x.pem'), Path('/kit/certs/x.pem'))
        self.assertEqual(cfg.resolve('/abs/x.pem'), Path('/abs/x.pem'))
        self.assertEqual(cfg.runs_dir(), Path('/kit/customer/runs'))

    def test_database_lookup(self):
        cfg = config_from()
        self.assertEqual(cfg.database('example_orders_db').source, 'example_orders_db')
        self.assertEqual(cfg.database_names(), ['example_orders_db'])
        self.assertEqual(len(cfg.run_names()), 3)
        with self.assertRaisesRegex(KitError, 'not configured'):
            cfg.database('other_db')


class ValidationTests(unittest.TestCase):
    def refuse(self, pattern, mutate):
        with self.assertRaisesRegex(KitError, pattern):
            config_from(mutate)

    def test_unknown_keys_refused_at_every_level(self):
        self.refuse('Unknown configuration key copy_workers', lambda d: d.update(copy_workers=4))
        self.refuse(r'Unknown configuration key copy\.worker \(did you mean workers\?\)', lambda d: d.update(copy={'worker': 4}))
        self.refuse(r'Unknown configuration key databases\.0\.layouts', lambda d: d['databases'][0].update(layouts={}))
        self.refuse(r'Unknown configuration key window\.stop', lambda d: d['window'].update(stop='x'))
        self.refuse(r'Unknown configuration key connections\.runner\.password',
                    lambda d: d['connections']['runner'].update(password='x'))
        self.refuse(r'Unknown configuration key databases\.0\.tables\.big_events\.chunk',
                    lambda d: d['databases'][0]['tables'].update(big_events={'chunk': 1}))
        self.refuse(r'Unknown configuration key freeze\.lockout',
                    lambda d: d.update(freeze={'lockout': {'connections': 1}}))

    def test_required_keys(self):
        self.refuse(r'workspace\.host is required', lambda d: d['workspace'].pop('host'))
        self.refuse(r'databases\.0\.source is required', lambda d: d['databases'][0].pop('source'))
        self.refuse(r'databases\.0\.layout\.partitions is required', lambda d: d['databases'][0]['layout'].pop('partitions'))
        self.refuse(r'databases needs at least 1', lambda d: d.update(databases=[]))
        self.refuse(r'kit must be one of', lambda d: d.update(kit='other'))
        self.refuse(r'version must be one of', lambda d: d.update(version=2))

    def test_identifier_rules(self):
        self.refuse('must be an identifier', lambda d: d['databases'][0].update(source='bad-name'))
        self.refuse('must be an identifier', lambda d: d['databases'][0].update(replacement='has space'))
        self.refuse('must be an identifier', lambda d: d['databases'][0].update(retained='x;drop'))
        self.refuse('invalid name', lambda d: d['databases'][0]['tables'].update({'bad-table': {}}))
        self.refuse('must be an identifier', lambda d: d['databases'][0].update(time_column_candidates=['a b']))

    def test_names_must_differ_and_be_unique(self):
        self.refuse('three different names', lambda d: d['databases'][0].update(replacement='example_orders_db'))
        self.refuse('three different names', lambda d: d['databases'][0].update(retained=d['databases'][0]['replacement']))

        def duplicate(d):
            d['databases'].append({'source': 'second_db', 'replacement': 'example_orders_db_rebuild_20261101',
                                   'layout': {'partitions': 8, 'sub_partitions': 64}})
        self.refuse('used twice', duplicate)

    def test_window_rules(self):
        self.refuse(r'window\.end must be after', lambda d: d['window'].update(end='2026-11-01T05:00:00Z'))
        self.refuse('timezone', lambda d: d['window'].update(end=dt.datetime(2026, 11, 1, 13)))
        self.refuse('ISO 8601', lambda d: d['window'].update(start='tomorrow'))
        cfg = config_from(lambda d: d['window'].update(start='2026-11-01T00:00:00-05:00'))
        self.assertEqual(cfg.window.start, dt.datetime(2026, 11, 1, 5, tzinfo=dt.timezone.utc))

    def test_workers(self):
        self.refuse(r'copy\.workers must be at least 1', lambda d: d.update(copy={'workers': 0}))
        self.refuse(r'copy\.workers must be at most 32', lambda d: d.update(copy={'workers': 33}))
        self.assertEqual(config_from(lambda d: d.update(copy={'workers': 16})).copy.workers, 16)
        self.refuse(r'copy\.workers must be of type int', lambda d: d.update(copy={'workers': True}))
        self.refuse(r'copy\.workers must be of type int', lambda d: d.update(copy={'workers': '4'}))

    def test_workers_up_to_32_and_an_integer_table_concurrency(self):
        """W11: copy.workers accepts 32; copy.table_concurrency accepts an integer up to copy.workers and refuses one above it."""
        cfg = config_from(lambda d: d.update(copy={'workers': 32, 'table_concurrency': 32}))
        self.assertEqual((cfg.copy.workers, cfg.copy.table_concurrency), (32, 32))
        self.assertEqual(config_from(lambda d: d.update(copy={'workers': 4, 'table_concurrency': 2})).copy.table_concurrency, 2)
        self.refuse(r'copy\.table_concurrency must be auto or an integer from 1 to copy\.workers \(4\)',
                    lambda d: d.update(copy={'workers': 4, 'table_concurrency': 5}))
        self.refuse(r'copy\.table_concurrency must be auto or an integer from 1 to copy\.workers \(8\)',
                    lambda d: d.update(copy={'workers': 8, 'table_concurrency': 9}))

    def test_password_source_exactly_one(self):
        self.refuse('exactly one of', lambda d: d['connections']['runner'].update(password_keychain='x'))
        self.refuse('exactly one of', lambda d: d['connections']['runner'].pop('password_env'))
        self.refuse('upper-case variable', lambda d: d['connections']['runner'].update(password_env='lower'))
        self.refuse('unsupported characters', lambda d: d['connections']['runner'].update(user="o'brien"))
        self.refuse(r'connections\.owner needs exactly one', lambda d: d['connections'].update(owner={'user': 'admin'}))

    def test_per_table_overrides(self):
        cfg = config_from()
        db = cfg.databases[0]
        self.assertEqual(db.table_override('big_events').concurrency, 1)
        self.assertEqual(db.table_override('big_events').chunk_rows, 50_000_000)
        self.assertIsNone(db.table_override('other').concurrency)
        self.assertIsNone(db.table_override('other').chunk_rows)
        self.refuse('chunk_rows must be at least 1', lambda d: d['databases'][0]['tables'].update(big_events={'chunk_rows': 0}))
        cfg = config_from(lambda d: d['databases'][0]['tables'].update(empty_override=None))
        self.assertEqual(cfg.databases[0].table_override('empty_override').concurrency, None)

    def test_table_concurrency_and_tiers(self):
        self.assertEqual(config_from(lambda d: d.update(copy={'table_concurrency': 1})).copy.table_concurrency, 1)
        for bad in (True, False, 1.0, 0, '1', 'AUTO'):
            self.refuse(r'table_concurrency must be auto or an integer from 1 to copy\.workers \(4\)',
                        lambda d, b=bad: d.update(copy={'table_concurrency': b}))
        self.refuse(r'verify\.tiers', lambda d: d.update(verify={'tiers': ['newest', 'count']}))
        self.refuse(r'verify\.tiers', lambda d: d.update(verify={'tiers': ['count', 'checksum']}))
        self.refuse(r'verify\.tiers', lambda d: d.update(verify={'tiers': []}))
        self.assertEqual(config_from(lambda d: d.update(verify={'tiers': ['count']})).verify.tiers, ['count'])

    def test_pipeline_maps_and_misc(self):
        self.refuse(r'pipelines\.credentials_env\.KAFKA must name', lambda d: d.update(pipelines={'credentials_env': {'KAFKA': 'lower'}}))
        self.refuse(r'pipelines\.retention_hours\.KAFKA must be a number', lambda d: d.update(pipelines={'retention_hours': {'KAFKA': 'week'}}))
        self.refuse(r'pipelines\.route must be one of', lambda d: d.update(pipelines={'route': 'manual'}))
        self.refuse(r'estimate\.rows_per_hour must be at least 1', lambda d: d.update(estimate={'rows_per_hour': 0}))
        self.assertIsNone(config_from(lambda d: d.update(closeout={'flexible_parallelism': None})).closeout.flexible_parallelism)
        with self.assertRaisesRegex(KitError, 'mapping at the top level'):
            from s2rebuild.config import build_config
            build_config(['not', 'a', 'mapping'])

    def test_f8_routine_ignore_entries_are_names_or_database_names(self):
        self.assertEqual(config_from(lambda d: d.pop('objects')).objects.routines.ignore, [], 'empty by default')
        cfg = config_from(lambda d: d.update(objects={'routines': {'ignore': ['p1', 'example_orders_db.f1']}}))
        self.assertEqual(cfg.objects.routines.ignore, ['p1', 'example_orders_db.f1'])
        for bad in ('a.b.c', 'bad-name', 'db.', '', 7):
            self.refuse(r'objects\.routines\.ignore entries must be', lambda d, bad=bad: d.update(objects={'routines': {'ignore': [bad]}}))
        self.refuse(r'Unknown configuration key objects\.routines\.ignored', lambda d: d.update(objects={'routines': {'ignored': []}}))

    def test_config_hash_is_stable_and_sensitive(self):
        a = config_from().config_hash()
        b = config_from().config_hash()
        c = config_from(lambda d: d.update(copy={'workers': 8})).config_hash()
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        self.assertEqual(len(a), 64)


class FileTests(unittest.TestCase):
    def test_config_path_resolution(self):
        self.assertEqual(config_path(None, {}), Path('customer/config.yaml'))
        self.assertEqual(config_path(None, {'S2REBUILD_CONFIG': 'x/y.yaml'}), Path('x/y.yaml'))
        self.assertEqual(config_path('z.yaml', {'S2REBUILD_CONFIG': 'x/y.yaml'}), Path('z.yaml'))

    def test_missing_empty_and_invalid_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(KitError, 'not found'):
                load_config(Path(tmp) / 'none.yaml')
            empty = Path(tmp) / 'empty.yaml'
            empty.write_text('')
            with self.assertRaisesRegex(KitError, 'is empty'):
                load_config(empty)
            bad = Path(tmp) / 'bad.yaml'
            bad.write_text('kit: s2rebuild\nversion: [1\n')
            with self.assertRaisesRegex(KitError, 'not valid YAML'):
                load_config(bad)
            with self.assertRaisesRegex(KitError, 'is not a file'):
                load_config(Path(tmp))

    def test_write_and_load_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, cfg = write_config(tmp)
            self.assertEqual(cfg.path, path)
            self.assertEqual(cfg.runs_dir(), Path(tmp) / 'runs')
            again = load_config(path, run_date=RUN_DATE, root=tmp)
            self.assertEqual(again.config_hash(), cfg.config_hash())

    def test_example_config_validates(self):
        cfg = load_config(ROOT / 'examples' / 'config.example.yaml', run_date=RUN_DATE, root=ROOT)
        self.assertEqual(cfg.databases[0].source, 'acme_orders_db')
        self.assertEqual(cfg.owner.user, 'admin')
        self.assertEqual(cfg.pipelines.retention_hours['KAFKA'], 168)
        fixture = load_config(FIXTURE, run_date=RUN_DATE, root=ROOT)
        self.assertEqual(fixture.databases[0].source, 'example_orders_db')


if __name__ == '__main__':
    unittest.main()
