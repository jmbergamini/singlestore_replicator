"""s2rebuild tests test_routines: routines captured with the owner session and recreated in order at O5 and O7 (W5).

Author: John Bergamini

Nine tests: the capture of each kind, the creation order (an aggregate before its functions
fails, after passes), a procedure that reads a view, the S3 routine count without a procedure,
a load target INTO PROCEDURE at O9, the ignore list, the source-name refusal, the definer
fallback and the runner's invisibility.
"""
import functools
import json
import os
import stat
import unittest
from unittest import mock

from s2rebuild import inventory, ownersql, routines as R, steps, switch
from s2rebuild.common import KitError
from s2rebuild.sqlutil import read_guard
from tests.helpers import fake_driver, run_cli
from tests.test_copy import SRC, TGT, Harness
from tests.test_pipelines import ITEM, owner_harness, pass_steps, write_o2
from tests.test_postcopy import V_BASE


def routine(name, kind, definition, definer=None):
    """A captured routine as inventory.json holds it."""
    types = {'function': 'FUNCTION', 'tvf': 'FUNCTION', 'aggregate': 'AGGREGATE', 'procedure': 'PROCEDURE'}
    return {'name': name, 'kind': kind, 'type': types[kind], 'definer': definer, 'definition': definition}


def scalar(name, body='RETURN a'):
    """A scalar function of one INT argument."""
    return routine(name, 'function', 'CREATE FUNCTION `%s`(a INT) RETURNS INT AS\nBEGIN\n  %s;\nEND' % (name, body))


F_ADD = scalar('f_add', 'RETURN a + 1')
TVF = routine('tvf_single', 'tvf', 'CREATE FUNCTION `tvf_single`(limit_n INT) RETURNS TABLE AS RETURN SELECT `id`, `n` FROM `t_single` WHERE `n` < limit_n')
AGG_FUNCTIONS = [scalar('f_init', 'RETURN 0'), scalar('f_iter', 'RETURN a + 1'), scalar('f_merge', 'RETURN a'), scalar('f_term', 'RETURN a')]
AGG_SUM = routine('agg_sum', 'aggregate', 'CREATE AGGREGATE `agg_sum`(INT) RETURNS INT WITH STATE INT INITIALIZE WITH `f_init` ITERATE WITH `f_iter` '
                                          'MERGE WITH `f_merge` TERMINATE WITH `f_term`')
AGG_RECORD = routine('agg_pair', 'aggregate', 'CREATE AGGREGATE `agg_pair`(INT) RETURNS INT WITH STATE RECORD(c INT, s INT) INITIALIZE WITH `f_init` '
                                              'ITERATE WITH `f_iter` MERGE WITH `f_merge` TERMINATE WITH `f_term`')
P_READ = routine('p_read', 'procedure', 'CREATE PROCEDURE `p_read`() AS\nBEGIN\n  ECHO SELECT `id` FROM `v_base`;\nEND')
P_LOAD = routine('p_load', 'procedure', 'CREATE PROCEDURE `p_load`(batch QUERY(id VARCHAR(100), payload VARCHAR(200), n INT)) AS\nBEGIN\n'
                                        '  INSERT INTO `t_single` SELECT * FROM batch;\nEND')
INTO_PROCEDURE = dict(ITEM, target='procedure p_load', definition=ITEM['definition'].replace('INTO TABLE `t_single`', 'INTO PROCEDURE `p_load`'))


def with_routines(items):
    """An inventory mutation that adds captured routines (and views in the capture order)."""
    def add(inv):
        inv['routines'] = [dict(r) for r in items if 'kind' in r]
        inv['routines_captured'] = True
        inv['routines_note'] = '%d routines captured with the owner profile' % len(inv['routines'])
        views = [dict(v) for v in items if 'kind' not in v]
        if views:
            inv['views'] = sorted(views, key=lambda v: v['name'])
    return add


def events(h, kind='routine'):
    """{routine: event} of the journal's routine events."""
    return {e['routine']: e for e in (json.loads(d) for d in h.journal.events(kind))}


def created_in(h, database=TGT):
    """[(name, type, definer)] of the routines the fake holds for a database, in creation order."""
    return [(r['ROUTINE_NAME'], r['ROUTINE_TYPE'], r['DEFINER']) for r in h.fake.routines_of(database)]


def owner_session(h, step='O5'):
    return h.sessions.owner(steps.guard_for(step, h.db, steps.OWNER))


def no_process_list_wait(test):
    """profile on the command line samples the process list three times a second apart; the test injects the sleep."""
    patch = mock.patch.object(inventory, 'capture', functools.partial(inventory.capture, sleep=lambda s: None))
    patch.start()
    test.addCleanup(patch.stop)


class RoutineTests(unittest.TestCase):
    def test_p3_captures_each_kind_with_the_owner_session_and_writes_the_definitions(self):
        """profile on the fake with the owner profile captures a function, a TVF, an aggregate and a procedure with their definitions
        and definers, writes definitions/routines/<name>.<kind>.sql with mode 0600, reads the write-capable accounts as the owner and
        prints the counts on the plan sheet; without the owner profile the capture says routines are not captured, never none."""
        h = owner_harness(self)
        h.fake.routines = [{'ROUTINE_NAME': 'f_add', 'ROUTINE_TYPE': 'FUNCTION', 'DEFINER': 'app@%', 'definition': F_ADD['definition']},
                           {'ROUTINE_NAME': 'tvf_single', 'ROUTINE_TYPE': 'FUNCTION', 'DEFINER': None, 'definition': TVF['definition']},
                           {'ROUTINE_NAME': 'agg_sum', 'ROUTINE_TYPE': 'AGGREGATE', 'DEFINER': None, 'definition': AGG_SUM['definition']},
                           {'ROUTINE_NAME': 'p_read', 'ROUTINE_TYPE': 'PROCEDURE', 'DEFINER': 'ops_owner@%', 'definition': P_READ['definition']}]
        h.fake.privileges.append({'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'INSERT'})
        h.journal.record_gate('P1', True, 'test')
        h.journal.step_end('P2', 'passed', 'test')
        h.journal.close()
        no_process_list_wait(self)
        fake_driver(self, h.fake)
        base = ['--config', str(h.config_path), '--run', h.run_id]
        env = dict(h.env, S2REBUILD_OWNER_PASSWORD='owner-fixture')
        code, text = run_cli(['profile'] + base, env=env)
        self.assertEqual(code, 0, text)
        self.assertIn('4 routines captured with the owner profile', text)
        p3 = h.tmp / 'runs' / h.run_id / SRC / 'inventory' / 'P3'
        inv = json.loads((p3 / 'inventory.json').read_text(encoding='utf8'))
        self.assertEqual([(r['name'], r['kind'], r['definer']) for r in inv['routines']],
                         [('agg_sum', 'aggregate', None), ('f_add', 'function', '`app`@`%`'), ('p_read', 'procedure', '`ops_owner`@`%`'),
                          ('tvf_single', 'tvf', None)], 'every kind, the TVF told apart by RETURNS TABLE, the definer from the DEFINER clause or column')
        self.assertEqual(inv['routines'][1]['definition'], 'CREATE DEFINER=`app`@`%` ' + F_ADD['definition'][len('CREATE '):])
        self.assertTrue(inv['routines_captured'])
        for rel in ('agg_sum.aggregate.sql', 'f_add.function.sql', 'p_read.procedure.sql', 'tvf_single.tvf.sql'):
            path = p3 / 'definitions' / 'routines' / rel
            self.assertTrue(path.is_file(), rel)
            self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600, rel)
        self.assertEqual([a['user'] for a in inv['accounts']], ['app'], 'the write-capable accounts are read with the owner session (L12)')
        code, text = run_cli(['plan'] + base, env=env)
        self.assertEqual(code, 0, text)
        self.assertIn('Routines   4 routines (1 functions, 1 TVFs, 1 aggregates, 1 procedures), 0 ignored by configuration', text)
        plain = Harness(self)
        plain.fake.routines = [dict(r) for r in h.fake.routines]
        plain.journal.record_gate('P1', True, 'test')
        plain.journal.step_end('P2', 'passed', 'test')
        plain.journal.close()
        fake_driver(self, plain.fake)
        base = ['--config', str(plain.config_path), '--run', plain.run_id]
        code, text = run_cli(['profile'] + base, env=plain.env)
        self.assertEqual(code, 0, text)
        self.assertIn('routines not captured: no owner profile; the runner sees none in ROUTINES', text)
        self.assertNotIn('0 routines', text)
        code, text = run_cli(['plan'] + base, env=plain.env)
        self.assertIn('Routines   not captured (routines not captured: no owner profile; the runner sees none in ROUTINES)', text)

    def test_o5_creates_the_functions_before_the_aggregate_that_names_them(self):
        """O5 creates the functions and TVFs after the tables and the aggregates after them, each behind its existence check, and the
        gate compares every definition with the capture; an aggregate whose functions are not in the replacement fails with 1305 and
        is tried again after the others, so O5 is refused when none can be created."""
        h = owner_harness(self, mutate_inventory=with_routines([AGG_SUM, AGG_RECORD] + AGG_FUNCTIONS + [TVF]))
        out = h.step('O5')
        self.assertEqual(out.status, steps.PASSED, h.text())
        created = created_in(h)
        self.assertEqual([n for n, _, _ in created], ['f_init', 'f_iter', 'f_merge', 'f_term', 'tvf_single', 'agg_sum', 'agg_pair'],
                         'functions and TVFs first, then the aggregates')
        self.assertTrue(all(d == 'ops_owner@%' for _, _, d in created), 'the owner session is the definer; never the runner')
        self.assertIn('routines gate: 4 functions, 1 TVFs, 2 aggregates equal the capture', h.text())
        self.assertEqual(set(events(h)), {'f_init', 'f_iter', 'f_merge', 'f_term', 'tvf_single', 'agg_sum', 'agg_pair'})
        self.assertEqual(h.step('O5').status, steps.PASSED, 'a rerun skips every routine that exists')
        self.assertIn('aggregate agg_sum already exists', h.text())
        self.assertEqual(R.normalize(h.fake.routines_of(TGT)[6]['definition']), R.normalize(AGG_RECORD['definition']), 'the record state passes through')
        alone = owner_harness(self, mutate_inventory=with_routines([AGG_SUM]))
        out = alone.step('O5')
        self.assertEqual((out.status, out.exit_code), (steps.REFUSED, 1), alone.text())
        self.assertIn('O5 stopped: round 1 created none of the aggregates still missing: agg_sum (server error 1305 (FakeDisconnect))', out.note)
        self.assertEqual(created_in(alone), [])
        self.assertEqual(events(alone, 'routine_failed')['agg_sum']['error'], 'server error 1305 (FakeDisconnect)')

    def test_o7_creates_a_procedure_after_the_view_it_reads_and_records_the_count_for_s3(self):
        """O7 creates the views, then the procedures, so a procedure that reads a view compiles; the gate records the ROUTINES count
        of the replacement, which the switch expects on the live name at S3. The same procedure before the view fails with 1146."""
        h = owner_harness(self, mutate_inventory=with_routines([V_BASE, P_READ, F_ADD]))
        h.fake.privileges.append({'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'SELECT'})
        self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
        self.assertEqual([n for n, _, _ in created_in(h)], ['f_add'], 'O5 creates the functions only')
        out = h.step('O7')
        self.assertEqual(out.status, steps.PASSED, h.text())
        writes = [w for w in h.fake.writes if 'VIEW `%s`.`v_base`' % TGT in w or w.startswith('CREATE PROCEDURE')]
        self.assertEqual(len(writes), 3, 'the view twice (its kept definer refused with 1227, then without the clause), then the procedure')
        self.assertTrue(all('VIEW' in w for w in writes[:2]))
        self.assertTrue(writes[2].startswith('CREATE PROCEDURE `%s`.`p_read`(' % TGT), writes[2])
        self.assertEqual([n for n, _, _ in created_in(h)], ['f_add', 'p_read'])
        self.assertEqual(h.journal.get_meta(R.ROUTINE_COUNT_META), '2', 'the ROUTINES rows of the replacement after the O7 gate')
        counts = ownersql.expected_counts(h.ctx())
        self.assertEqual(counts['routines'], 2)
        block = switch.forward_block(ownersql.names_for(h.ctx()), counts)
        self.assertIn('S3 after: routines count equals the capture', [i.label for i in block.items])
        early = owner_harness(self, mutate_inventory=with_routines([V_BASE, P_READ]))
        self.assertEqual(early.o5_o6().status, steps.PASSED, early.text())
        with self.assertRaisesRegex(KitError, r'O7 stopped: round 1 created none of the procedures still missing: p_read \(server error 1146'):
            R.create(owner_session(early, 'O7'), TGT, [dict(P_READ)], early.journal, early.log, step='O7')
        self.assertEqual(created_in(early), [])

    def test_o7_without_a_procedure_still_records_the_routine_count_for_s3(self):
        """Functions only: O7 has no procedure to create, yet it records the ROUTINES count of the replacement (the function O5
        created), so S3 still counts the routines on the live name."""
        h = owner_harness(self, mutate_inventory=with_routines([F_ADD]))
        self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
        out = h.step('O7')
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual(h.journal.get_meta(R.ROUTINE_COUNT_META), '1')
        self.assertIn('routines: no procedure to create at O7; 1 routine(s) in %s recorded for the S3 count' % TGT, h.text())
        counts = ownersql.expected_counts(h.ctx())
        self.assertEqual(counts['routines'], 1)
        block = switch.forward_block(ownersql.names_for(h.ctx()), counts)
        self.assertIn('S3 after: routines count equals the capture', [i.label for i in block.items])

    def test_o9_creates_a_load_target_into_a_procedure_once_o7_made_it(self):
        """A pipeline INTO PROCEDURE needs its procedure: O9 before O7 created it fails on the CREATE PIPELINE with 1305 and leaves no
        pipeline; after O7 the pipeline is created stopped at its offset."""
        h = owner_harness(self, mutate_inventory=with_routines([P_LOAD]))
        self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
        pass_steps(h, 'O7', 'O8')
        write_o2(h, [INTO_PROCEDURE])
        env = {'S2REBUILD_KAFKA_CREDENTIALS': 'x'}
        out = h.step('O9', options={'env': env})
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('O9 failed at "create pipeline p1": server error 1305', out.note, 'the message is withheld: the statement carried the credential')
        self.assertEqual(h.journal.step_status('O9'), 'pending')
        self.assertFalse(any(p.get('DATABASE_NAME') == TGT for p in h.fake.pipelines))
        out = h.step('O7')
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual([n for n, _, _ in created_in(h)], ['p_load'])
        out = h.step('O9', options={'env': env})
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual([(p['PIPELINE_NAME'], p['STATE']) for p in h.fake.pipelines if p.get('DATABASE_NAME') == TGT], [('p1', 'Stopped')])
        self.assertEqual(h.journal.step_status('O9'), 'passed')

    def test_an_ignored_routine_is_left_behind_and_named(self):
        """objects.routines.ignore leaves a routine out of O5 and the gate, names it in the log and on the summary; an aggregate whose
        function is ignored cannot be created, so O5 is refused with the 1305 error of the server."""
        f_old = scalar('f_old')
        h = owner_harness(self, mutate=lambda d: d.update(objects={'routines': {'ignore': ['f_old']}}),
                          mutate_inventory=with_routines([f_old, F_ADD]))
        out = h.step('O5')
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual([n for n, _, _ in created_in(h)], ['f_add'])
        self.assertIn('routines ignored by objects.routines.ignore at O5: function f_old', h.text())
        self.assertIn('routines gate: 1 functions equal the capture', h.text())
        self.assertEqual(R.summary(h.config, h.db, h.inventory), '2 routines (2 functions, 0 TVFs, 0 aggregates, 0 procedures), 1 ignored by configuration: function f_old')
        self.assertNotIn('f_old', set(events(h)))
        broken = owner_harness(self, mutate=lambda d: d.update(objects={'routines': {'ignore': ['f_iter']}}),
                               mutate_inventory=with_routines(AGG_FUNCTIONS + [AGG_SUM]))
        out = broken.step('O5')
        self.assertEqual(out.status, steps.REFUSED, broken.text())
        self.assertIn('created none of the aggregates still missing: agg_sum (server error 1305', out.note)
        self.assertEqual([n for n, _, _ in created_in(broken)], ['f_init', 'f_merge', 'f_term'], 'the functions before it were created; the rerun skips them')

    def test_a_body_naming_the_source_database_is_refused_before_any_statement(self):
        """A routine whose body names the source database refuses O5 (or O7 for a procedure) by name before any statement, the
        replacement included; the refusal says where to edit the definition."""
        bad = routine('f_bad', 'function', 'CREATE FUNCTION `f_bad`() RETURNS INT AS\nBEGIN\n  RETURN (SELECT COUNT(*) FROM `%s`.`t_single`);\nEND' % SRC)
        h = owner_harness(self, mutate_inventory=with_routines([bad, F_ADD]))
        out = h.step('O5')
        self.assertEqual((out.status, out.exit_code), (steps.REFUSED, 1), h.text())
        self.assertIn('function f_bad name the source database %s in their definition; no routine statement was issued' % SRC, out.note)
        self.assertIn('definitions/routines/', out.note)
        self.assertEqual(h.fake.writes, [], 'not even the replacement was created')
        p_bad = routine('p_bad', 'procedure', 'CREATE PROCEDURE `p_bad`() AS\nBEGIN\n  ECHO SELECT 1 FROM %s.`t_single`;\nEND' % SRC)
        h = owner_harness(self, mutate_inventory=with_routines([V_BASE, p_bad]))
        self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
        before = list(h.fake.writes)
        out = h.step('O7')
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('procedure p_bad name the source database', out.note)
        self.assertEqual(h.fake.writes, before, 'no view and no procedure statement')

    def test_the_definer_fallback_records_the_reason_per_routine(self):
        """A captured DEFINER is kept while its account exists and is not the runner; Helios refuses it with 1227 (fact 25), so the
        routine is created again without the clause and the owner is the definer, the reason recorded; a definer that no longer exists
        and the runner get the owner at once. On an engine without the refusal the definer is kept."""
        f_app = routine('f_app', 'function', 'CREATE DEFINER=`app`@`%` FUNCTION `f_app`(a INT) RETURNS INT AS\nBEGIN\n  RETURN a;\nEND', '`app`@`%`')
        f_gone = routine('f_gone', 'function', 'CREATE DEFINER=`gone`@`%` FUNCTION `f_gone`(a INT) RETURNS INT AS\nBEGIN\n  RETURN a;\nEND', '`gone`@`%`')
        f_run = routine('f_run', 'function', 'CREATE DEFINER=`s2rebuild_runner`@`%` FUNCTION `f_run`(a INT) RETURNS INT AS\nBEGIN\n  RETURN a;\nEND',
                        '`s2rebuild_runner`@`%`')
        h = owner_harness(self, mutate_inventory=with_routines([f_app, f_gone, f_run]))
        h.fake.privileges.append({'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'SELECT'})
        out = h.step('O5')
        self.assertEqual(out.status, steps.PASSED, h.text())
        recorded = events(h)
        self.assertEqual((recorded['f_app']['definer'], recorded['f_app']['reason']), ('owner', R.DEFINER_REFUSED_REASON))
        self.assertEqual((recorded['f_gone']['definer'], recorded['f_gone']['reason']), ('owner', 'the original definer no longer exists; the owner account is the definer'))
        self.assertEqual(recorded['f_run']['reason'], 'the original definer is the runner, which C4 removes; the owner account is the definer')
        creates = [w for w in h.fake.writes if 'FUNCTION `%s`.`f_app`' % TGT in w]
        self.assertEqual(len(creates), 2, 'once with the kept DEFINER (refused), once without')
        self.assertIn('DEFINER=`app`@`%`', creates[0])
        self.assertNotIn('DEFINER', creates[1])
        self.assertEqual(sum(1 for w in h.fake.writes if 'FUNCTION `%s`.`f_gone`' % TGT in w), 1)
        self.assertEqual(dict((n, d) for n, _, d in created_in(h)), {'f_app': 'ops_owner@%', 'f_gone': 'ops_owner@%', 'f_run': 'ops_owner@%'})
        self.assertIn('created function f_app (%s)' % R.DEFINER_REFUSED_REASON, h.text())
        kept = owner_harness(self, mutate_inventory=with_routines([f_app]))
        kept.fake.privileges.append({'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'SELECT'})
        kept.fake.refuse_other_definer = False  # the path of an engine that accepts another account as DEFINER; Helios 10.8.0 does not (fact 25)
        self.assertEqual(kept.step('O5').status, steps.PASSED, kept.text())
        self.assertEqual(created_in(kept), [('f_app', 'FUNCTION', 'app@%')])
        self.assertEqual(events(kept)['f_app']['definer'], 'kept')

    def test_the_runner_sees_no_routines_and_check_says_so(self):
        """information_schema.ROUTINES lists nothing to the runner (fact 21), so the capture through the runner session finds none
        where the owner session finds them all; check without the owner profile prints that the routines are not visible and never
        'none'."""
        h = owner_harness(self)
        h.fake.routines = [{'ROUTINE_NAME': 'f_add', 'ROUTINE_TYPE': 'FUNCTION', 'DEFINER': None, 'definition': F_ADD['definition']},
                           {'ROUTINE_NAME': 'p_read', 'ROUTINE_TYPE': 'PROCEDURE', 'DEFINER': None, 'definition': P_READ['definition']}]
        runner = h.sessions.runner(read_guard())
        self.assertEqual([r['name'] for r in R.capture(runner, SRC)], [], 'the runner sees no routine')
        owner = h.sessions.owner(read_guard())
        self.assertEqual([(r['name'], r['kind']) for r in R.capture(owner, SRC)], [('f_add', 'function'), ('p_read', 'procedure')])
        for session in (runner, owner):
            for sql in session.statements:
                read_guard().check(sql)
        plain = Harness(self)
        plain.fake.routines = [dict(r) for r in h.fake.routines]
        plain.journal.record_gate('P1', True, 'test')
        plain.journal.close()
        fake_driver(self, plain.fake)
        code, text = run_cli(['check', '--config', str(plain.config_path), '--run', plain.run_id], env=plain.env)
        self.assertEqual(code, 0, text)
        self.assertIn('routines: not visible to the runner; owner profile needed', text)
        self.assertNotIn('routines: none', text)


if __name__ == '__main__':
    unittest.main()
