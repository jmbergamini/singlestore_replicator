"""s2rebuild tests test_postcopy: definer removal, counter statements, view creation through USE.

Author: John Bergamini
"""
import json
import unittest

from s2rebuild import postcopy as P
from s2rebuild import steps
from s2rebuild.common import KitError
from s2rebuild.sqlutil import runner_guard
from tests.test_copy import FIXTURE_TABLES, SRC, TGT, Harness, fixture_inventory


class ViewTests(unittest.TestCase):
    def test_view_statement_removes_the_definer_and_qualifies_the_name(self):
        view = {'name': 'v_orders', 'definer': '`app`@`%`',
                'definition': 'CREATE ALGORITHM=UNDEFINED DEFINER=`app`@`%` VIEW `v_orders` AS SELECT `id` FROM `t_single`'}
        sql = P.view_statement(view, TGT)
        self.assertEqual(sql, 'CREATE ALGORITHM=UNDEFINED VIEW `%s`.`v_orders` AS SELECT `id` FROM `t_single`' % TGT)
        runner_guard(TGT).check(sql)
        bound = {'name': 'v_bound', 'definer': '`u`@`h`', 'definition': 'CREATE DEFINER=`u`@`h` SCHEMA_BINDING=OFF VIEW `v_bound` AS SELECT 1 FROM `t_single`'}
        sql = P.view_statement(bound, TGT)
        self.assertEqual(sql, 'CREATE SCHEMA_BINDING=OFF VIEW `%s`.`v_bound` AS SELECT 1 FROM `t_single`' % TGT, 'the server text shape keeps its binding mode')
        runner_guard(TGT).check(sql)
        with self.assertRaises(KitError):
            P.view_statement({'name': 'other', 'definition': 'CREATE VIEW `v` AS SELECT 1'}, TGT)

    def test_create_views_uses_the_replacement_and_skips_existing(self):
        h = Harness(self)
        self.assertEqual(h.o5_o6().status, steps.PASSED)
        views = [{'name': 'v1', 'definer': '`app`@`%`', 'definition': 'CREATE DEFINER=`app`@`%` VIEW `v1` AS SELECT `id` FROM `t_single`'},
                 {'name': 'v2', 'definer': None, 'definition': 'CREATE VIEW `v2` AS SELECT `a` FROM `t_comp`'}]
        session = h.sessions.runner(runner_guard(TGT))
        created = P.create_views(session, TGT, views, h.journal, h.log)
        self.assertEqual(created, ['v1', 'v2'])
        self.assertEqual(set(h.fake.databases[TGT]['views']), {'v1', 'v2'})
        self.assertEqual(session.statements[0], 'USE `%s`' % TGT)
        events = [json.loads(d) for d in h.journal.events('view')]
        self.assertEqual(events[0], {'view': 'v1', 'definer_removed': '`app`@`%`'})
        self.assertEqual(P.create_views(session, TGT, views, h.journal, h.log), [], 'existing views are skipped')
        self.assertIn('already exists', h.text())


class CounterTests(unittest.TestCase):
    def test_counters_set_to_max_plus_one_and_synced(self):
        tables = dict(FIXTURE_TABLES)
        cols, pk, est, create = tables['t_comp']
        h = Harness(self, tables=tables)
        inv = fixture_inventory(tables)
        for t in inv['tables']:
            if t['name'] == 't_comp':
                t['auto_increment'] = {'column': 'c', 'counter': 500}
            if t['name'] == 't_empty':
                t['auto_increment'] = {'column': 'p', 'counter': 9}
        self.assertEqual(h.o5_o6().status, steps.PASSED)
        session = h.sessions.runner(runner_guard(TGT))
        actions = P.set_counters(session, TGT, inv['tables'], h.journal, h.log)
        by = {a['table']: a for a in actions}
        self.assertEqual(by['t_comp']['action'], 'counter set to MAX+1 and synced')
        self.assertEqual(by['t_comp']['max_value'], 299)
        self.assertEqual(by['t_comp']['target_after'], 300)
        self.assertEqual(by['t_comp']['source_counter'], 500)
        self.assertEqual(by['t_empty']['action'], 'table empty, counter left at default')
        self.assertIn('ALTER TABLE `%s`.`t_comp` AUTO_INCREMENT = 300' % TGT, h.fake.writes)
        self.assertEqual(h.fake.synced, ['t_comp'])
        recorded = [json.loads(d) for d in h.journal.events('counter')]
        self.assertEqual({r['table'] for r in recorded}, {'t_comp', 't_empty'})
        self.assertEqual(P.max_count_sql('d', 't', 'c'), 'SELECT MAX(`c`) AS m, COUNT(*) AS n FROM `d`.`t`')
        runner_guard(TGT).check(P.alter_counter_sql(TGT, 't', 5))
        runner_guard(TGT).check(P.sync_counter_sql(TGT, 't'))


if __name__ == '__main__':
    unittest.main()
