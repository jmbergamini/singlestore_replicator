"""s2rebuild tests test_postcopy: definer removal, counter statements, view creation through USE, the definer choice, rounds and the O7 block.

Author: John Bergamini
"""
import json
import unittest

from s2rebuild import ownersql
from s2rebuild import postcopy as P
from s2rebuild import steps
from s2rebuild.common import KitError
from s2rebuild.connect import Session
from s2rebuild.sqlutil import runner_guard
from tests.fakedb import FakeDisconnect, SimulatedCrash
from tests.helpers import FAULT_POINTS, crash_journal
from tests.test_copy import FIXTURE_TABLES, SRC, TGT, Harness, fixture_inventory
from tests.test_verify import auto_increment_on_t_comp, copied, counter_writes

# a view on a view captured in name order (the inventory reads views ORDER BY name), so a_top comes before the view it reads
V_BASE = {'name': 'v_base', 'definer': '`app`@`%`',
          'definition': 'CREATE DEFINER=`app`@`%` SCHEMA_BINDING=OFF VIEW `v_base` AS SELECT `id`, `n` FROM `t_single`'}
V_TOP = {'name': 'a_top', 'definer': '`gone`@`%`', 'definition': 'CREATE DEFINER=`gone`@`%` VIEW `a_top` AS SELECT `id` FROM `v_base`'}
V_RUN = {'name': 'm_run', 'definer': '`s2rebuild_runner`@`%`',
         'definition': 'CREATE ALGORITHM=UNDEFINED DEFINER=`s2rebuild_runner`@`%` VIEW `m_run` AS SELECT `a` FROM `t_comp`'}
CREATE_V_BASE = r'VIEW `%s`\.`v_base` AS' % TGT


def OWNER(d):
    """Config mutation that adds the owner profile (user ops_owner, password from the environment)."""
    d['connections'].update(owner={'user': 'ops_owner', 'password_env': 'S2REBUILD_OWNER_PASSWORD'})


def view_harness(test, views, owner=True):
    """A copied harness (O5 and O6 passed) whose capture holds views, sorted by name as the inventory reads them; the account app exists."""
    def add(inv):
        inv['views'] = sorted((dict(v) for v in views), key=lambda v: v['name'])
    h = Harness(test, mutate=OWNER if owner else None, mutate_inventory=add)
    h.env['S2REBUILD_OWNER_PASSWORD'] = 'owner-fixture'
    h.fake.privileges.append({'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'SELECT'})
    assert h.o5_o6().status == steps.PASSED, h.text()
    return h


def view_choices(h):
    """{view: (definer choice, observed definer or reason)} from the journal's view events."""
    return {e['view']: (e['definer'], e.get('observed_definer') or e['reason']) for e in (json.loads(d) for d in h.journal.events('view'))}


def created_views(h):
    """{view: the definer the fake recorded} of the replacement."""
    return {name: v.definer for name, v in h.fake.databases[TGT]['views'].items()}


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
        self.assertEqual(events[0], {'view': 'v1', 'original_definer': '`app`@`%`', 'definer': 'owner',
                                     'reason': 'the original definer no longer exists; the owner account is the definer'},
                         'the runner session sees no other account, so the definer is never kept through it')
        self.assertEqual(P.create_views(session, TGT, views, h.journal, h.log), [], 'existing views are skipped')
        self.assertIn('already exists', h.text())


class CounterTests(unittest.TestCase):
    def counter_harness(self, t_comp_counter, t_empty_counter):
        """(harness, inventory tables) after O5 and O6; t_comp (MAX(c) 299) and the empty t_empty carry the given source counters."""
        tables = dict(FIXTURE_TABLES)
        h = Harness(self, tables=tables)
        h.fake.refuse_sharded_counter = False  # the ALTER path of the counter logic: Helios 10.8.0 refuses it on a sharded table (1749, covered by its own test)
        inv = fixture_inventory(tables)
        for t in inv['tables']:
            if t['name'] == 't_comp':
                t['auto_increment'] = {'column': 'c', 'counter': t_comp_counter}
            if t['name'] == 't_empty':
                t['auto_increment'] = {'column': 'p', 'counter': t_empty_counter}
        self.assertEqual(h.o5_o6().status, steps.PASSED)
        return h, inv['tables']

    def test_counters_set_to_max_plus_one_and_synced(self):
        h, tables = self.counter_harness(100, None)
        session = h.sessions.runner(runner_guard(TGT))
        actions = P.set_counters(session, TGT, tables, h.journal, h.log)
        by = {a['table']: a for a in actions}
        self.assertEqual(by['t_comp']['action'], 'counter set to MAX+1 and synced on every aggregator')
        self.assertEqual((by['t_comp']['decision'], by['t_comp']['max_plus_one']), ('MAX+1', 300), 'MAX+1 is above the source counter 100')
        self.assertEqual(by['t_comp']['max_value'], 299)
        self.assertEqual(by['t_comp']['target_after'], 300)
        self.assertEqual(by['t_comp']['source_counter'], 100)
        self.assertEqual(by['t_empty']['action'], 'table empty, counter left at default')
        self.assertIn('ALTER TABLE `%s`.`t_comp` AUTO_INCREMENT = 300' % TGT, h.fake.writes)
        self.assertEqual(h.fake.synced, ['t_comp'])
        recorded = [json.loads(d) for d in h.journal.events('counter')]
        self.assertEqual({r['table'] for r in recorded}, {'t_comp', 't_empty'})
        self.assertEqual(P.max_count_sql('d', 't', 'c'), 'SELECT MAX(`c`) AS m, COUNT(*) AS n FROM `d`.`t`')
        runner_guard(TGT).check(P.alter_counter_sql(TGT, 't', 5))
        runner_guard(TGT).check(P.sync_counter_sql(TGT, 't'))

    def test_a_sharded_table_whose_counter_the_server_refuses_is_synced_only(self):
        """Helios refuses ALTER TABLE ... AUTO_INCREMENT on a sharded table (1749, October 27, 2026): the sync alone runs and the action says why."""
        h, tables = self.counter_harness(6755399441055745, None)
        h.fake.refuse_sharded_counter = True
        session = h.sessions.runner(runner_guard(TGT))
        actions = P.set_counters(session, TGT, tables, h.journal, h.log)
        by = {a['table']: a for a in actions}
        self.assertEqual(by['t_comp']['action'], P.COUNTER_REFUSED_ACTION)
        self.assertNotIn('ALTER TABLE `%s`.`t_comp` AUTO_INCREMENT = 6755399441055745' % TGT, h.fake.writes)
        self.assertEqual(h.fake.synced, ['t_comp'], 'the sync still runs on every aggregator')
        self.assertIn(P.COUNTER_REFUSED_ACTION, [json.loads(d)['action'] for d in h.journal.events('counter')])

    def test_f11_source_counter_above_max_plus_one_wins_and_the_sync_runs_on_every_aggregator(self):
        """t_comp: source counter 500 above MAX+1 300; t_empty: no rows, source counter 9. Each counter is set to the source counter and
        synced with ALL; the counter event and the table tab carry both values and the decision."""
        from s2rebuild import report_xlsx
        h, tables = self.counter_harness(500, 9)
        self.assertEqual(P.sync_counter_sql(TGT, 't_comp'), 'AGGREGATOR SYNC AUTO_INCREMENT ON `%s`.`t_comp` ALL' % TGT)
        session = h.sessions.runner(runner_guard(TGT))
        by = {a['table']: a for a in P.set_counters(session, TGT, tables, h.journal, h.log)}
        self.assertEqual({k: by['t_comp'][k] for k in ('source_counter', 'max_plus_one', 'decision', 'target_after', 'action')},
                         {'source_counter': 500, 'max_plus_one': 300, 'decision': 'source counter', 'target_after': 500,
                          'action': 'counter set to the source counter and synced on every aggregator'})
        self.assertEqual({k: by['t_empty'][k] for k in ('source_counter', 'max_plus_one', 'decision', 'target_after')},
                         {'source_counter': 9, 'max_plus_one': None, 'decision': 'source counter', 'target_after': 9},
                         'an empty table keeps the source counter, so no id the source handed out is used again')
        self.assertEqual(counter_writes(h, 't_comp'), ['ALTER TABLE `%s`.`t_comp` AUTO_INCREMENT = 500' % TGT,
                                                       'AGGREGATOR SYNC AUTO_INCREMENT ON `%s`.`t_comp` ALL' % TGT])
        for name, value in (('t_comp', 500), ('t_empty', 9)):
            t = h.fake.databases[TGT]['tables'][name]
            self.assertEqual((t['auto_increment'], t['synced_counter']), (value, value), 'every aggregator holds the counter of %s' % name)
        plan = next(p for p in h.plans() if p.name == 't_comp')
        rows = report_xlsx.final_table_rows(next(t for t in tables if t['name'] == 't_comp'), plan, h.journal, report_xlsx.copy_stats(h.journal)[0])
        self.assertIn(('', 'counter: counter set to the source counter and synced on every aggregator (source counter 500, MAX+1 300, '
                           'target before None, after 500)'), rows)

    def test_f11_counter_fault_points_rerun_of_o7_sets_the_source_counter_and_syncs_every_aggregator(self):
        """A failure before or after the ALTER TABLE AUTO_INCREMENT of t_comp and before or after its AGGREGATOR SYNC ... ALL, and a kill -9
        stand-in after both and before the counter event: O7 ends failed with no counter event. The operator's rerun (s2rebuild step O7)
        passes without error, sets the source counter 500 (above MAX+1 300) again, syncs it on every aggregator and records one event."""
        statements = {'ALTER': r'^ALTER TABLE `%s`\.`t_comp` AUTO_INCREMENT' % TGT, 'SYNC': r'^AGGREGATOR SYNC AUTO_INCREMENT ON `%s`\.`t_comp` ALL$' % TGT}
        for point in FAULT_POINTS:
            for statement in (('ALTER', 'SYNC') if point != 'journal' else ('counter event',)):
                with self.subTest(point=point, statement=statement):
                    h = copied(self, mutate_inventory=auto_increment_on_t_comp)
                    h.fake.refuse_sharded_counter = False  # the ALTER path of the counter logic: Helios 10.8.0 refuses it on a sharded table (1749, covered by its own test)
                    if point == 'journal':
                        fired = crash_journal(self, h.journal, 'event', match=lambda kind, detail: kind == 'counter')
                        with self.assertRaises(SimulatedCrash):
                            h.step('O7', redo=False)
                    else:
                        h.fake.fail_statement(statements[statement], point)
                        fired = h.fake.faults_fired
                        with self.assertRaises(FakeDisconnect):
                            h.step('O7', redo=False)
                    self.assertEqual(len(fired), 1, 'the fault fired')
                    self.assertEqual(h.journal.step_status('O7'), 'failed')
                    self.assertEqual(h.journal.events('counter'), [], 'no counter event before the rerun')
                    t = h.fake.databases[TGT]['tables']['t_comp']
                    synced_before = 500 if point == 'journal' or (statement, point) == ('SYNC', 'after') else None
                    self.assertEqual(t.get('synced_counter'), synced_before, 'the fault stopped O7 where it was armed')
                    writes = len(h.fake.writes)
                    out = h.step('O7', redo=False)
                    self.assertEqual(out.status, steps.PASSED, h.text())
                    self.assertEqual(counter_writes(h, 't_comp', writes), ['ALTER TABLE `%s`.`t_comp` AUTO_INCREMENT = 500' % TGT,
                                                                           'AGGREGATOR SYNC AUTO_INCREMENT ON `%s`.`t_comp` ALL' % TGT])
                    self.assertEqual((t['auto_increment'], t['synced_counter']), (500, 500), 'the counter is correct on every aggregator')
                    events = [json.loads(d) for d in h.journal.events('counter')]
                    self.assertEqual([(e['table'], e['decision'], e['source_counter'], e['max_plus_one'], e['target_after']) for e in events],
                                     [('t_comp', 'source counter', 500, 300, 500)])


class ViewDefinerTests(unittest.TestCase):
    """O7 views run under their definer: never the runner, the original kept while its account exists, in rounds, source names refused."""

    def test_f7_view_on_a_later_named_view_is_created_in_the_second_round_with_the_definer_choice(self):
        """The owner session creates v_base (definer kept), then a_top in round 2 (definer gone: owner); the runner definer is replaced too."""
        from openpyxl import load_workbook
        h = view_harness(self, [V_BASE, V_TOP, V_RUN])
        h.fake.refuse_other_definer = False  # the kept-definer choice: Helios 10.8.0 refuses another DEFINER (1227, covered by its own test)
        self.assertEqual(h.step('O7', dry_run=True).status, steps.PASSED)
        self.assertIn('DRY RUN: USE `%s`' % TGT, h.text(), 'the dry run prints what the owner session issues')
        self.assertIn('DRY RUN: SELECT COUNT(*) AS n FROM information_schema.USER_PRIVILEGES', h.text())
        out = h.step('O7')
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual(created_views(h), {'v_base': 'app@%', 'a_top': 'ops_owner@%', 'm_run': 'ops_owner@%'},
                         'the runner never creates a view and is never kept as a definer')
        self.assertEqual(view_choices(h), {
            'v_base': ('kept', 'original definer kept'),
            'a_top': ('owner', 'the original definer no longer exists; the owner account is the definer'),
            'm_run': ('owner', 'the original definer is the runner, which C4 removes; the owner account is the definer')})
        failed = [json.loads(d) for d in h.journal.events('view_failed')]
        self.assertEqual(failed, [{'view': 'a_top', 'round': 1, 'error': 'server error 1146 (FakeDisconnect)'}], 'caught per view, error number only')
        self.assertEqual(h.step('O8').status, steps.PASSED, h.text())
        wb = load_workbook(str(h.tmp / 'runs' / h.run_id / SRC / 'reports' / 'final.xlsx'))
        tab = {r[0].value: r[2].value for r in wb['Views'].iter_rows(min_row=2)}
        self.assertEqual(tab['v_base'], 'yes (original definer kept)')
        self.assertEqual(tab['a_top'], 'yes (the original definer no longer exists; the owner account is the definer)')

    def test_a_refused_original_definer_falls_back_to_the_owner(self):
        """Helios refuses another account as DEFINER (1227, October 27, 2026): the view is created again with the owner as definer."""
        h = view_harness(self, [V_BASE, V_TOP])
        h.fake.refuse_other_definer = True
        out = h.step('O7')
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual(created_views(h), {'v_base': 'ops_owner@%', 'a_top': 'ops_owner@%'})
        self.assertEqual(view_choices(h)['v_base'], ('owner', P.DEFINER_REFUSED_REASON))
        self.assertNotIn(1227, [json.loads(d).get('error') for d in h.journal.events('view_failed')])
        self.assertEqual(h.step('O8').status, steps.PASSED, h.text())

    def test_f7_create_view_is_followed_by_its_existence_check(self):
        """The admin CREATE VIEW sits between its checks: a CREATE the server accepts without listing the view stops O7, no choice recorded."""
        h = view_harness(self, [V_BASE])

        class Vanish(dict):
            """The replacement's views dict drops every view it is given, as a server that accepts CREATE VIEW and lists nothing."""

            def __setitem__(self, key, value):
                pass
        h.fake.databases[TGT]['views'] = Vanish()
        out = h.step('O7')
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('O7 stopped at check "view v_base exists"', out.note)
        self.assertEqual(view_choices(h), {}, 'no definer choice is journaled for a view the server does not list')
        self.assertEqual(h.journal.step_status('O7'), 'pending', 'never left running')

    def test_f7_a_round_that_creates_nothing_refuses_with_the_views_and_their_errors(self):
        """A view whose body reads a table that never exists stops O7 before the stale check, naming it with its error number."""
        broken = {'name': 'v_broken', 'definer': None, 'definition': 'CREATE VIEW `v_broken` AS SELECT 1 FROM `no_such_table`'}
        h = view_harness(self, [V_BASE, broken])
        out = h.step('O7')
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('round 2 created none of the views still missing: v_broken (server error 1146 (FakeDisconnect))', out.note)
        self.assertEqual(h.journal.step_status('O7'), 'pending', 'never left running')
        self.assertEqual(h.journal.events('stale_check'), [], 'refused before the stale check')
        self.assertEqual(set(created_views(h)), {'v_base'})

    def test_f7_view_naming_the_source_is_refused_before_any_view_statement(self):
        """`source`.x and source.x in a definition refuse O7 with the view names; a source name inside a literal does not."""
        quoted = {'name': 'v_q', 'definer': None, 'definition': 'CREATE VIEW `v_q` AS SELECT `id` FROM `%s`.`t_single`' % SRC}
        bare = {'name': 'v_b', 'definer': None, 'definition': 'CREATE VIEW `v_b` AS SELECT a FROM %s.t_comp' % SRC}
        literal = {'name': 'v_l', 'definer': None, 'definition': "CREATE VIEW `v_l` AS SELECT '%s.t' AS s FROM `t_single`" % SRC}
        h = view_harness(self, [quoted, bare, literal])
        before = len(h.fake.statements)
        out = h.step('O7')
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('view(s) v_b, v_q name the source database %s in their definition; no view statement was issued' % SRC, out.note)
        self.assertEqual(h.fake.statements[before:], [], 'nothing reached the server')
        self.assertFalse(P.names_database(literal['definition'], SRC))
        self.assertFalse(P.names_database('CREATE VIEW `v` AS SELECT 1 FROM `%s_rebuild`.`t`' % SRC, SRC))

    def test_f7_without_the_owner_profile_o7_prints_the_block_ends_pending_and_the_rerun_passes(self):
        """No CREATE VIEW as the runner: the O7 block in dependency order, the stale check and counters run, the rerun after the admin passes."""
        h = view_harness(self, [V_BASE, V_TOP], owner=False)
        dry = h.step('O7', dry_run=True)
        self.assertEqual(dry.status, steps.PASSED)
        self.assertIn('-- O7: Create the views in the replacement as the admin', h.text(), 'the dry run prints the block')
        out = h.step('O7')
        self.assertEqual(out.status, steps.PENDING, h.text())
        self.assertIn('view(s) a_top, v_base not created: no owner profile. Run the O7 block in', out.note)
        self.assertEqual(h.journal.step_status('O7'), 'pending')
        self.assertEqual(created_views(h), {}, 'the runner creates no view')
        self.assertFalse([w for w in h.fake.writes if 'VIEW' in w])
        self.assertEqual(len(h.journal.events('stale_check')), 1, 'the stale check ran')
        path = h.tmp / 'runs' / h.run_id / SRC / 'owner-sql' / 'O7.sql'
        text = path.read_text()
        self.assertLess(text.index('VIEW `%s`.`v_base`' % TGT), text.index('VIEW `%s`.`a_top`' % TGT), 'dependency order')
        self.assertNotIn('DEFINER', text.split('-- NOTE')[1].split('\n', 1)[1], 'the admin who runs it is the definer')
        block = P.view_block(h.inventory['views'], TGT)
        admin = Session(h.fake.connect('admin'), steps.guard_for('O7', h.db, steps.OWNER))
        ownersql.execute_block(block, admin, h.log)
        ownersql.execute_block(block, admin, h.log)  # run again: every CREATE is skipped, every check passes
        self.assertEqual(created_views(h), {'v_base': 'admin@%', 'a_top': 'admin@%'})
        out = h.step('O7', redo=False)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual(view_choices(h), {'v_base': ('found', 'admin@%'), 'a_top': ('found', 'admin@%')})

    def test_f7_create_view_failure_points_rerun_skips_the_existing_view(self):
        """A failure before the CREATE VIEW, after it with the reply lost, and after it before the journal write; then O7 is run again.

        The round creates nothing, so O7 refuses (or the crash records it failed), never running.
        The rerun creates the view (before) or finds it and records the definer the server shows
        (after, journal). Each ends with the CREATE applied once, the kept definer, and one journal
        choice for the view.
        """
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = view_harness(self, [V_BASE])
                h.fake.refuse_other_definer = False  # the kept-definer choice: Helios 10.8.0 refuses another DEFINER (1227, covered by its own test)
                if point == 'journal':
                    crash_journal(self, h.journal, 'event', match=lambda kind, detail: kind == 'view')
                    with self.assertRaises(SimulatedCrash):
                        h.step('O7')
                    self.assertEqual(h.journal.step_status('O7'), 'failed')
                else:
                    h.fake.fail_statement(CREATE_V_BASE, point)
                    out = h.step('O7')
                    self.assertEqual(out.status, steps.REFUSED, h.text())
                    self.assertIn('v_base (server error 2013 (FakeDisconnect))', out.note, 'the error number, never the message')
                    self.assertEqual(h.journal.step_status('O7'), 'pending')
                self.assertEqual(h.step('O7', redo=False).status, steps.PASSED, h.text())
                applied = [s for s, _ in h.fake.statements if s.startswith('CREATE DEFINER=`app`@`%`')]
                self.assertEqual(len(applied), 1, 'the server applied the CREATE once')
                self.assertEqual(created_views(h), {'v_base': 'app@%'})
                choice = view_choices(h)['v_base']
                self.assertEqual(choice, ('kept', 'original definer kept') if point == 'before' else ('found', 'app@%'))
                self.assertEqual(len(h.journal.events('view')), 1, 'one journal choice per view')


if __name__ == '__main__':
    unittest.main()
