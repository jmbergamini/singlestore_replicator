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

    def test_w4_the_table_tab_of_a_reference_table_says_staging_and_the_summary_lists_it(self):
        """A reference table in the capture: the table tab's Copy path row and callout say it is copied through a staging table, the
        profile Summary names it, its estimate is above zero, and a direct table says INSERT ... SELECT."""
        cfg = config_from()
        db = cfg.databases[0]
        inv = I.load(FIXTURE_INVENTORY)
        inv['tables'].append({
            'name': 'regions', 'columns': [{'name': 'code', 'ordinal': 1, 'data_type': 'varchar', 'column_type': 'varchar(8)', 'nullable': False,
                                           'default': None, 'extra': '', 'generation_expression': ''}],
            'keys': [{'name': 'PRIMARY', 'unique': True, 'type': 'COLUMNSTORE HASH', 'columns': ['code']}],
            'definition': 'CREATE REFERENCE TABLE `regions` (\n  `code` varchar(8) NOT NULL,\n  PRIMARY KEY (`code`),\n  SORT KEY (`code`)\n)',
            'storage_type': 'columnstore', 'reference': True, 'auto_increment': None, 'est_rows': 6_000_000, 'exact_rows': 6_000_000,
            'size_gib': 0.4, 'computed_columns': []})
        plans = I.plan_tables(inv, cfg, db)
        est = E.estimate(plans, cfg, E.DEFAULT_BASELINE)
        self.assertGreater(est.schedule.per_table['regions'][2], 0.0)
        with tempfile.TemporaryDirectory() as tmp:
            path = R.write_profile_workbook(Path(tmp) / 'p.xlsx', inv, plans, est, None, cfg, db, 'r', E.DEFAULT_BASELINE)
            wb = load_workbook(str(path))
            regions = {r[0].value: r[1].value for r in wb['regions'].iter_rows(min_col=1, max_col=2) if r[0].value}
            self.assertEqual(regions['Copy path'], 'reference table, copied through a staging table')
            self.assertEqual((regions['Buckets'], regions['Rows exact']), (1, 6_000_000), 'one bucket although 6 M rows would chunk')
            self.assertGreater(regions['Minutes expected'], 0.0)
            callouts = ' '.join(str(c.value) for c in wb['regions']['B'] if c.value)
            self.assertIn('reference table, copied through a staging table: the row count is an exact COUNT(*)', callouts)
            orders = {r[0].value: r[1].value for r in wb['orders'].iter_rows(min_col=1, max_col=2) if r[0].value}
            self.assertEqual(orders['Copy path'], 'INSERT ... SELECT by hash bucket')
            summary = {str(r[0].value): r[1].value for r in wb['Summary'].iter_rows(min_col=1, max_col=2) if r[0].value}
            self.assertEqual(summary['Reference tables (copied through a staging table, counted exactly)'], 'regions')
            self.assertEqual(summary['Tables'], 7)

    def test_final_summary_without_a_window(self):
        """A run whose O1 recorded no window shows 'none configured' and no deadline in the final workbook."""
        from s2rebuild.journal import MEMORY, Journal
        cfg = config_from(lambda d: d.pop('window'))
        db = cfg.databases[0]
        inv = I.load(FIXTURE_INVENTORY)
        plans = I.plan_tables(inv, cfg, db)
        journal = Journal(MEMORY)
        self.addCleanup(journal.close)
        journal.set_meta('window_start', '')
        journal.set_meta('window_end', '')
        journal.set_meta('abort_deadline', '')
        summary = {label: value for label, value in R.final_summary_rows(inv, plans, journal, cfg, db, 'r') if label}
        self.assertEqual(summary['Window'], 'none configured')
        self.assertEqual(summary['Abort deadline'], 'none')
        journal.set_meta('window_start', '2026-10-27T00:00:00Z')
        journal.set_meta('window_end', '2026-10-27T05:00:00Z')
        summary = {label: value for label, value in R.final_summary_rows(inv, plans, journal, cfg, db, 'r') if label}
        self.assertEqual(summary['Window'], '2026-10-27T00:00:00Z to 2026-10-27T05:00:00Z')


class FinalReportTests(unittest.TestCase):
    """Release 1.0 W13: the final workbook and run-report.md read what the journal recorded; a value absent says not recorded."""

    def setUp(self):
        from s2rebuild.journal import MEMORY, Journal
        self.cfg = config_from()
        self.db = self.cfg.databases[0]
        self.inv = I.load(FIXTURE_INVENTORY)
        self.plans = I.plan_tables(self.inv, self.cfg, self.db)
        self.journal = Journal(MEMORY)
        self.addCleanup(self.journal.close)
        self.journal.init_plan(self.plans, self.db.source, self.db.replacement)

    def summary(self):
        return {label: value for label, value in R.final_summary_rows(self.inv, self.plans, self.journal, self.cfg, self.db, 'r') if label}

    def test_w13_summary_shows_the_latest_result_after_a_mismatch_then_a_match_and_the_tab_keeps_both(self):
        """A fingerprint mismatch on orders followed by a match (the re-copy verified again): the Summary counts one match and no mismatch,
        the orders tab lists both rows in order under Verification history, and a sampled fingerprint shows its predicate in both places."""
        j = self.journal
        j.record_verification('orders', 'count', 'match', 10, 10)
        j.record_verification('orders', 'fingerprint', 'mismatch', 'a', 'b', detail='3 groups compared')
        j.record_verification('orders', 'fingerprint', 'match', 'a', 'a', detail='3 groups compared')
        j.record_verification('events', 'fingerprint', 'sampled', 'x', 'x', detail='2 groups compared', predicate='`ts` >= 1 AND `ts` < 5', sample_rows=15)
        summary = self.summary()
        self.assertEqual(summary['Tier fingerprint'], '1 match, 1 sampled', 'the latest row per tier and table, not the worst')
        self.assertEqual(summary['Tier count'], '1 match')
        self.assertEqual(summary['Sampled tables'], 'events')
        self.assertEqual(summary['Sampled fingerprints (latest, with the predicate)'], 'events (15 rows, WHERE `ts` >= 1 AND `ts` < 5)')
        table = next(t for t in self.inv['tables'] if t['name'] == 'orders')
        plan = next(p for p in self.plans if p.name == 'orders')
        rows = R.final_table_rows(table, plan, j, R.copy_stats(j)[0])
        labels = [label for label, _ in rows]
        start = labels.index('Verification history')
        history = [value for _, value in rows[start + 1:start + 4]]
        self.assertEqual(history, ['match (epoch 0): source 10, target 10', 'mismatch (epoch 0): source a, target b', 'match (epoch 0): source a, target a'])
        self.assertTrue(labels[start + 2].startswith('fingerprint 20'), 'each history row is labeled with its tier and time')
        self.assertEqual(rows[labels.index('Result', start - 20)], ('Result', 'match'), 'the tier 3 block shows the latest result')

    def test_w13_the_final_sheet_carries_the_retain_until_row_and_the_two_entries(self):
        """The final workbook and run-report.md carry the two entries with their records and ATTACH_SYNTAX, the O10 and C3 listings before and
        after, retain_until_utc with the C5 command, the workspace settings P2 observed and O1 recorded, the reference tables, the definer per
        view and routine on the Views and Routines tabs, and the pipelines skipped at the prompt."""
        import json
        from s2rebuild import report_md
        j = self.journal
        live_syntax = "ATTACH DATABASE `example_orders_db` AT MILESTONE 'm1' STORAGEID 200:1 COMPUTEID 7"
        j.record_attachment('live', self.db.source, '200:1', '7', 'READWRITE', 'pitr', live_syntax, 0)
        j.record_attachment('retained', self.db.retained, '100:1', '5', 'READONLY', 'resume', 'ATTACH DATABASE `x` READ ONLY STORAGEID 100:1', 0)
        j.set_meta('switched_utc', '2026-10-27T03:00:00Z')
        j.set_meta('retain_until_utc', '2027-01-25T03:00:00Z')
        j.event('switch_listing', 'before: name a (attached READONLY)')
        j.event('switch_listing', 'after: name live; name retained')
        j.event('endstate_listing', 'before: name live; name retained')
        j.event('endstate_listing', 'after: name live; name retained (C3)')
        j.set_meta('preflight', json.dumps({'leaf_cores': 16, 'workspace': {'source': 'api', 'state': 'ACTIVE', 'autoscale': True, 'auto_suspend': False,
                                                                           'fields': {'state': 'ACTIVE', 'autoScale': {'enabled': True}, 'autoSuspend': 'OFF'}}}))
        j.event('workspace', 'O1 workspace answer: autoscale off for the window')
        j.event('accept_autoscale', 'O1 with autoscale on: accepted for the lab')
        j.event('view', json.dumps({'view': 'order_totals', 'original_definer': '`app_user`@`%`', 'definer': 'owner', 'reason': 'the original definer no longer exists; the owner account is the definer'}))
        j.event('routine', json.dumps({'routine': 'tax_rate', 'kind': 'function', 'original_definer': '`app`@`%`', 'definer': 'kept', 'reason': 'original definer kept'}))
        j.set_meta('pipelines_skipped', json.dumps(['KAFKA']))
        self.inv['tables'][2]['definition'] = self.inv['tables'][2]['definition'].replace('CREATE TABLE', 'CREATE REFERENCE TABLE', 1)
        self.plans = I.plan_tables(self.inv, self.cfg, self.db)
        with tempfile.TemporaryDirectory() as tmp:
            path = R.write_final_workbook(Path(tmp) / 'final.xlsx', self.inv, self.plans, j, self.cfg, self.db, 'r')
            wb = load_workbook(str(path))
            self.assertEqual(wb.sheetnames[-4:], ['Views', 'Routines', 'Pipelines', 'Pipeline offsets'])
            summary = {str(r[0].value): r[1].value for r in wb['Summary'].iter_rows(min_col=1, max_col=2) if r[0].value}
            self.assertEqual(summary['Live entry'], 'name example_orders_db, STORAGE_ID 200:1, ATTACH_TYPE pitr, COMPUTE_ID 7 (attached READWRITE); ATTACH_SYNTAX ' + live_syntax)
            self.assertEqual(summary['Retained entry'], 'name %s, STORAGE_ID 100:1, ATTACH_TYPE resume, COMPUTE_ID 5 (attached READONLY); ATTACH_SYNTAX '
                             'ATTACH DATABASE `x` READ ONLY STORAGEID 100:1' % self.db.retained)
            self.assertEqual((summary['Entries before O10'], summary['Entries after O10']), ('name a (attached READONLY)', 'name live; name retained'))
            self.assertEqual((summary['Entries before C3'], summary['Entries after C3']), ('name live; name retained', 'name live; name retained (C3)'))
            self.assertEqual(summary['Retain until'], '2027-01-25T03:00:00Z')
            self.assertEqual(summary['Switched'], '2026-10-27T03:00:00Z')
            self.assertTrue(summary['Delete later with'].startswith('s2rebuild cleanup --delete %s --run r' % self.db.retained))
            self.assertEqual(summary['Observed at P2'], 'state ACTIVE; autoscale on (enabled=True); auto-suspend off (Management API at P2)')
            self.assertEqual(summary['Answered at O1'], 'O1 workspace answer: autoscale off for the window')
            self.assertEqual(summary['Autoscale accepted at O1'], 'O1 with autoscale on: accepted for the lab')
            self.assertEqual(summary['Reference tables (copied through a staging table)'], 'events')
            self.assertTrue(summary['Not recreated (no credentials given)'].startswith('orders_stream (KAFKA)'))
            views = {r[0].value: (r[2].value, r[3].value) for r in wb['Views'].iter_rows(min_row=2)}
            self.assertEqual(views['order_totals'], ('yes (the original definer no longer exists; the owner account is the definer)', 'the owner account'))
            routines = {r[0].value: tuple(c.value for c in r[1:]) for r in wb['Routines'].iter_rows(min_row=2)}
            self.assertEqual(routines['tax_rate'], ('function', '`app`@`%`', 'yes (original definer kept)', 'original definer kept', 'no'))
            self.assertEqual(routines['nightly_rollup'], ('procedure', 'none', 'no (ignored)', 'not recorded', 'yes'), 'the test config ignores it')
            md = report_md.write_run_report(Path(tmp) / 'run-report.md', self.inv, self.plans, j, self.cfg, self.db, 'r').read_text()
            self.assertIn('- Live entry: name example_orders_db, STORAGE_ID 200:1, ATTACH_TYPE pitr, COMPUTE_ID 7 (attached READWRITE); ATTACH_SYNTAX ' + live_syntax, md)
            self.assertIn('- Retain until: 2027-01-25T03:00:00Z', md)
            self.assertIn('- Entries after C3: name live; name retained (C3)', md)
            self.assertIn('## Routines\n\n- routine nightly_rollup; kind procedure; original definer none; created in replacement no (ignored); '
                          'definer in replacement not recorded; ignored by configuration yes', md)
            self.assertIn('- view order_totals; original definer `app_user`@`%`; created in replacement yes (', md)

    def test_w13_a_value_the_journal_lacks_reads_not_recorded(self):
        """A journal without a switch, a C3 listing, a preflight record or an O1 answer: every new row says not recorded, the Routines tab
        and the run report still list the captured routines, and the run report names the sections."""
        from s2rebuild import report_md
        summary = self.summary()
        for label in ('Live entry', 'Retained entry', 'Entries before O10', 'Entries after O10', 'Entries before C3', 'Entries after C3',
                      'Observed at P2', 'Answered at O1', 'Switched'):
            self.assertEqual(summary[label], 'not recorded', label)
        self.assertEqual(summary['Retain until'], 'not recorded (C2 records it)')
        self.assertEqual(summary['Autoscale accepted at O1'], 'none')
        self.assertEqual(summary['Sampled fingerprints (latest, with the predicate)'], 'none')
        self.assertNotIn('Entries before repair C3', summary, 'the repair moments print only when C3 ran a repair')
        self.assertEqual([r[0] for r in R.routine_rows(self.inv, self.journal, self.cfg, self.db)], ['nightly_rollup', 'tax_rate'])
        self.assertEqual(R.routine_rows({'routines': [], 'routines_captured': False}, self.journal, self.cfg, self.db)[0][0],
                         'none captured (the owner profile reads routines)')
        with tempfile.TemporaryDirectory() as tmp:
            md = report_md.write_run_report(Path(tmp) / 'run-report.md', self.inv, self.plans, self.journal, self.cfg, self.db, 'r').read_text()
        self.assertIn('## Views\n\n- view order_totals; original definer `app_user`@`%`; created in replacement no; definer in replacement not recorded', md)
        self.assertIn('- Live entry: not recorded', md)


if __name__ == '__main__':
    unittest.main()
