"""s2rebuild tests test_report: the profile workbook opens, Summary first, one tab per table, names mapped, definition at the bottom.

Author: John Bergamini
"""
import tempfile
import unittest
from pathlib import Path

from openpyxl import load_workbook

from s2rebuild import estimate as E
from s2rebuild import inventory as I
from s2rebuild import report_xlsx as R
from tests.helpers import ROOT, config_from

FIXTURE_INVENTORY = ROOT / 'tests' / 'fixtures' / 'inventory'


class SheetNameTests(unittest.TestCase):
    def test_names_are_cut_replaced_and_unique(self):
        used = set()
        self.assertEqual(R.sheet_name('orders', used), 'orders')
        self.assertEqual(R.sheet_name('a' * 40, used), 'a' * 31)
        self.assertEqual(R.sheet_name('a' * 40, used), 'a' * 29 + '~2')
        self.assertEqual(R.sheet_name('a' * 40, used), 'a' * 29 + '~3')
        self.assertEqual(R.sheet_name('bad[name]:*?/\\x', used), 'bad_name______x')
        self.assertEqual(R.sheet_name('ORDERS', used), 'ORDERS~2', 'sheet names are case-insensitive in Excel')
        reserved = {'Summary', 'Pipelines', 'Pipeline offsets', 'System', 'History'}
        self.assertEqual(R.sheet_name('History', set(reserved)), 'History~2', 'Excel reserves the name History')


class WorkbookTests(unittest.TestCase):
    def test_profile_workbook_layout(self):
        cfg = config_from()
        db = cfg.databases[0]
        inv = I.load(FIXTURE_INVENTORY)
        plans = I.plan_tables(inv, cfg, db)
        est = E.estimate(plans, cfg, E.DEFAULT_BASELINE, pipeline_count=2)
        fit = E.window_fit(est, cfg.window)
        with tempfile.TemporaryDirectory() as tmp:
            path = R.write_profile_workbook(Path(tmp) / 'reports' / 'profile.xlsx', inv, plans, est, fit, cfg, db, '20261101-0500',
                                            E.DEFAULT_BASELINE, steps_board={'P3': {'status': 'passed', 'started_utc': 'a', 'ended_utc': 'b'}},
                                            env={'S2REBUILD_KAFKA_CREDENTIALS': 'x'})
            wb = load_workbook(str(path))
            self.assertEqual(wb.sheetnames[0], 'Summary')
            self.assertEqual(wb.sheetnames[1:7], ['order_items', 'orders', 'events', 'customers', 'totals', 'empty_table'])
            self.assertEqual(wb.sheetnames[7:], ['Pipelines', 'Pipeline offsets'])
            summary = {str(r[0].value): r[1].value for r in wb['Summary'].iter_rows(min_col=1, max_col=2) if r[0].value}
            self.assertEqual(summary['Database'], 'example_orders_db')
            self.assertEqual(summary['Tables'], 6)
            self.assertEqual(summary['Estimated rows'], 639003650)
            self.assertEqual(summary['Buckets'], 40)
            self.assertEqual(summary['Verdict'], 'FITS')
            self.assertEqual(summary['P3'], 'passed a b')
            self.assertEqual(summary['Captured'], '2 (1 KAFKA, 1 S3)')
            self.assertEqual(summary['Tables with AUTO_INCREMENT counters'], 'customers')
            self.assertEqual(summary['Tables with computed columns'], 'totals')
            self.assertEqual(summary['Tables with unique-key lock risk'], 'customers')
            self.assertEqual(summary['Tables hashed over all columns (no non-null key)'], 'events, empty_table')
            self.assertEqual(summary['Pipelines the application recreates'], 'imports')
            self.assertEqual(summary['Pipelines with errors in 24 h'], 'imports')
            self.assertEqual(summary['Write-capable'], 'app_user, retention_job')
            self.assertEqual(summary['Path B action'], 'none (Path A read-only attachment)')
            self.assertAlmostEqual(summary['Modeled GiB per hour'], round(est.size_gib / est.copy_seconds * 3600, 1))
            self.assertIsInstance(summary['Estimated rows'], int, 'numbers stored as numbers')
            self.assertEqual(summary['Abort deadline'], E.iso(fit.deadline))
            orders = wb['orders']
            labels = [orders.cell(row=r, column=1).value for r in range(1, orders.max_row + 1)]
            values = {orders.cell(row=r, column=1).value: orders.cell(row=r, column=2).value for r in range(1, orders.max_row + 1)}
            self.assertEqual(values['Table'], 'orders')
            self.assertEqual(values['Key kind'], 'key')
            self.assertEqual(values['Rows estimated'], 25000000)
            self.assertEqual(values['Buckets'], 3)
            self.assertEqual(values['Time column'], 'updated_at')
            self.assertEqual(values['Primary key'], 'id')
            self.assertEqual(values['Shard key'], 'id')
            self.assertEqual(values['Sort key'], 'created_at')
            self.assertIsInstance(values['Minutes expected'], float)
            definition_row = labels.index('Definition')
            self.assertEqual(labels[definition_row - 1], None, 'one blank row before the definition block')
            self.assertEqual(orders.cell(row=definition_row + 2, column=1).value, 'CREATE TABLE `orders` (')
            self.assertTrue(orders.cell(row=orders.max_row, column=1).value.startswith(') AUTOSTATS'))
            customers = {customers_row[0].value: customers_row[1].value for customers_row in wb['customers'].iter_rows(min_col=1, max_col=2) if customers_row[0].value}
            self.assertEqual(customers['AUTO_INCREMENT column'], 'id')
            self.assertEqual(customers['AUTO_INCREMENT counter'], 4503599627370497)
            self.assertIn('unique varchar key', ' '.join(str(c.value) for c in wb['customers']['B'] if c.value))
            pipes = list(wb['Pipelines'].iter_rows(values_only=True))
            self.assertEqual(pipes[0][0], 'name')
            self.assertEqual([r[0] for r in pipes[1:]], ['orders_stream', 'imports'])
            self.assertEqual(pipes[1][6], 'REPLACE')
            self.assertEqual(pipes[1][12], 95500, 'loaded offsets sum')
            self.assertEqual((pipes[1][15], pipes[2][15]), ('set', 'not set'), 'variable set column reads the environment')
            self.assertEqual(pipes[2][16], 'yes', 'application managed flag')
            offsets = list(wb['Pipeline offsets'].iter_rows(values_only=True))
            self.assertEqual(len(offsets), 4)
            self.assertEqual(offsets[1][:3], ('orders_stream', '0', 1000))
            text = ' '.join(str(c.value) for ws in wb.worksheets for row in ws.iter_rows() for c in row if c.value is not None)
            self.assertNotIn('<<CREDENTIALS>>', text, 'the workbook shows locations and targets, never definitions with credentials')
            rows = [{'database': 'db_a', 'partitions': 32, 'sub_partitions': 0, 'tables': 2, 'estimated_rows': 10, 'size_gib': 1.5, 'pipelines': 0,
                     'copy_minutes_expected': 0.1}]
            R.write_system_sheet(path, rows)
            wb = load_workbook(str(path))
            self.assertEqual(wb.sheetnames[1], 'System')
            self.assertEqual(wb['System'].cell(row=2, column=1).value, 'db_a')

    def test_long_and_reserved_table_names_are_mapped(self):
        cfg = config_from(lambda d: d.update(freeze={'mode': 'lockout'}))
        db = cfg.databases[0]
        inv = I.load(FIXTURE_INVENTORY)
        long_name = 'a_very_long_table_name_that_goes_past_31:chars'
        inv['tables'][0]['name'] = long_name
        inv['tables'][0]['definition'] = inv['tables'][0]['definition'].replace('`orders`', '`%s`' % long_name)
        inv['tables'][1]['name'] = 'System'
        inv['tables'][1]['definition'] = inv['tables'][1]['definition'].replace('`order_items`', '`System`')
        plans = I.plan_tables(inv, cfg, db)
        est = E.estimate(plans, cfg)
        with tempfile.TemporaryDirectory() as tmp:
            path = R.write_profile_workbook(Path(tmp) / 'p.xlsx', inv, plans, est, None, cfg, db, 'r', E.DEFAULT_BASELINE)
            wb = load_workbook(str(path))
            self.assertIn('a_very_long_table_name_that_goe', wb.sheetnames)
            self.assertIn('System~2', wb.sheetnames, 'System is reserved for the system sheet')
            summary = {str(r[0].value): r[1].value for r in wb['Summary'].iter_rows(min_col=1, max_col=2) if r[0].value}
            self.assertEqual(summary[long_name], 'a_very_long_table_name_that_goe')
            self.assertEqual(summary['System'], 'System~2')
            self.assertIn('REVOKE the write privileges', summary['Path B action'])
            R.write_system_sheet(path, [{'database': 'd', 'partitions': 1, 'sub_partitions': 0, 'tables': 0, 'estimated_rows': 0, 'size_gib': None,
                                         'pipelines': 0, 'copy_minutes_expected': 0.0}])
            wb = load_workbook(str(path))
            self.assertIn('System~2', wb.sheetnames, 'the table tab survives the system sheet')
            self.assertEqual(wb['System'].cell(row=1, column=1).value, 'database')

    def test_workbook_without_pipelines_or_window(self):
        cfg = config_from(lambda d: d.pop('window'))
        db = cfg.databases[0]
        inv = I.load(FIXTURE_INVENTORY)
        inv['pipelines'] = {'captured': False, 'note': 'pipelines not captured: no owner profile', 'items': []}
        plans = I.plan_tables(inv, cfg, db)
        est = E.estimate(plans, cfg)
        with tempfile.TemporaryDirectory() as tmp:
            path = R.write_profile_workbook(Path(tmp) / 'p.xlsx', inv, plans, est, None, cfg, db, 'r', E.DEFAULT_BASELINE)
            wb = load_workbook(str(path))
            summary = {str(r[0].value): r[1].value for r in wb['Summary'].iter_rows(min_col=1, max_col=2) if r[0].value}
            self.assertNotIn('Verdict', summary)
            self.assertIn('no owner profile', summary['Captured'])
            self.assertEqual(wb['Pipelines'].cell(row=2, column=1).value, 'pipelines not captured: no owner profile')
            self.assertEqual(wb['Pipeline offsets'].cell(row=2, column=1).value, 'none captured')


if __name__ == '__main__':
    unittest.main()
