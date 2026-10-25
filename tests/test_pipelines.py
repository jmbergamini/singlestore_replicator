"""s2rebuild tests test_pipelines: O2, O9, O11 and C1 blocks and handlers, credentials, the freeze, grants per scope and the lockout loop.

Author: John Bergamini
"""
import json
import os
import stat
import unittest

from s2rebuild import freeze, grants, inventory, lockout, ownersql, pipelines as P, steps
from s2rebuild.common import KitError
from s2rebuild.connect import Redactor
from s2rebuild.sqlutil import probe_guard, read_guard, secret_forms, sql_literal
from tests.fakedb import FakeDisconnect
from tests.helpers import config_from
from tests.test_copy import SRC, TGT, Harness
from tests.test_switch import OWNER

DEFINITION = "CREATE PIPELINE `p1` AS LOAD DATA KAFKA 'b:9092/t' CREDENTIALS '<<CREDENTIALS>>' INTO TABLE `t_single`"
ITEM = {'name': 'p1', 'state': 'Stopped', 'source_type': 'KAFKA', 'source_location': 'b:9092/t', 'target': 'table t_single', 'batch_interval': 1000,
        'duplicate_handling': 'REPLACE', 'definition': DEFINITION, 'alter_statements': ["ALTER PIPELINE `p1` SET OFFSETS '{\"0\": 5}'"],
        'redaction_marker': False, 'app_managed': False, 'offsets': [{'partition': '0', 'earliest': 1, 'latest': 5, 'loaded': 5, 'successful': 5}],
        'last_batch_time': None, 'rows_24h': 0, 'errors_24h': 0, 'last_error': None}
APP = dict(ITEM, name='app_owned', app_managed=True, definition=DEFINITION.replace('p1', 'app_owned'), alter_statements=[])
PLAIN = dict(ITEM, name='plain_fs', source_type='FS', definition="CREATE PIPELINE `plain_fs` AS LOAD DATA FS '/data' INTO TABLE `t_single`",
             alter_statements=[], offsets=[])
SERVER_DEFINITION = "CREATE PIPELINE `p1` AS LOAD DATA KAFKA 'b:9092/t' CREDENTIALS <CREDENTIALS REDACTED> INTO TABLE `t_single`"


def owner_harness(test, mutate=None, **kw):
    """A harness with the owner profile (user ops_owner) and its password in the environment."""
    def both(d):
        OWNER(d)
        if mutate:
            mutate(d)
    h = Harness(test, mutate=both, **kw)
    h.env['S2REBUILD_OWNER_PASSWORD'] = 'owner-fixture'
    return h


def names_of(h):
    """The block names of the harness run."""
    return ownersql.names_for(h.ctx())


def with_source_pipeline(h, state='Running', loaded=5, earliest=1, latest=5):
    """One Kafka pipeline on the source as the server returns it, with one partition."""
    h.fake.pipelines = [{'PIPELINE_NAME': 'p1', 'STATE': state, 'definition': SERVER_DEFINITION, 'alter': "ALTER PIPELINE `p1` SET OFFSETS '{\"0\": %d}'" % loaded}]
    h.fake.pipeline_offsets = [{'DATABASE_NAME': SRC, 'PIPELINE_NAME': 'p1', 'SOURCE_TYPE': 'KAFKA', 'SOURCE_PARTITION_ID': '0', 'EARLIEST_OFFSET': earliest,
                                'LATEST_OFFSET': latest, 'LATEST_LOADED_OFFSET': loaded, 'LATEST_SUCCESSFUL_OFFSET': loaded}]


def write_o2(h, items):
    """Write an O2 capture with the given pipeline items."""
    inv = dict(h.inventory, step='O2', pipelines={'captured': True, 'note': 'test', 'items': items})
    inventory.write(inv, inventory.inventory_dir(h.config, h.run_id, h.db, 'O2'), h.plans())


def pass_steps(h, *step_ids):
    """Mark steps passed in the journal."""
    for step_id in step_ids:
        h.journal.step_end(step_id, 'passed', 'test')


class BlockTests(unittest.TestCase):
    def test_stop_block_skips_stopped_pipelines_and_captures(self):
        names = {'source': SRC, 'replacement': TGT}
        b = P.stop_block(names, [ITEM, APP], extra_names=['late'])
        text = b.render()
        for name in ('p1', 'app_owned', 'late'):
            self.assertIn('STOP PIPELINE `%s`.`%s`;' % (SRC, name), text)
        self.assertIn("SKIP the next statement when this returns n > 0: SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = "
                      "'%s' AND PIPELINE_NAME = 'p1' AND STATE <> 'Running';" % SRC, text)
        self.assertIn('-- CAPTURE: every definition and offset', text)

    def test_replay_block_from_the_o2_capture(self):
        names = {'source': SRC, 'replacement': TGT}
        b = P.replay_block(names, [ITEM, APP], 'O2')
        text = b.render()
        self.assertNotIn('-- STOP', text)
        self.assertIn('USE `%s`;' % TGT, text)
        self.assertIn("CREDENTIALS '<<CREDENTIALS>>'", text)
        self.assertNotIn('app_owned', text, 'application-managed pipelines are not replayed')
        create = [i for i in b.statements if i.label == 'create pipeline p1'][0]
        self.assertEqual((create.pipeline, create.source_type), ('p1', 'KAFKA'))
        self.assertEqual(create.skip_if, (P.EXISTS_SQL, (TGT, 'p1')))
        self.assertIn("offsets of p1 equal the O2 capture (partitions and summed loaded offset); expect partitions = 1, loaded = 5", text)

    def test_replay_refuses_an_early_capture_a_missing_one_and_a_source_target(self):
        names = {'source': SRC, 'replacement': TGT}
        self.assertIn('-- STOP: the pipeline capture comes from P3, taken before the stop', P.replay_block(names, [ITEM], 'P3').render())
        self.assertIn('-- STOP: no pipeline capture exists yet', P.replay_block(names, [], None).render())
        self.assertNotIn('-- STOP', P.replay_block(names, [], 'P3').render(), 'a capture without pipelines needs no O2 offsets')
        bad = dict(ITEM, definition=DEFINITION.replace('INTO TABLE `t_single`', 'INTO TABLE `%s`.`t_single`' % SRC))
        self.assertIn('names the source database', P.replay_block(names, [bad], 'O2').render())
        literal = dict(ITEM, definition=DEFINITION.replace("'b:9092/t'", "'`%s`.x'" % SRC))
        self.assertNotIn('names the source database', P.replay_block(names, [literal], 'O2').render(), 'a literal is not a target')

    def test_verify_block_for_the_customer_route(self):
        b = P.verify_block({'source': SRC, 'replacement': TGT}, [ITEM], 'O2', 'customer/runs/x/pipelines/recreate-with-placeholders.sql')
        self.assertEqual(b.statements, [])
        self.assertIn('Route customer: the customer runs customer/runs/x/pipelines/recreate-with-placeholders.sql', b.render())
        self.assertIn('pipeline count equals the capture', b.render())

    def test_release_block_restores_grants_before_starting_pipelines(self):
        names = {'source': SRC, 'replacement': TGT, 'flexible_parallelism': 0.5}
        snap = {"'app'@'%'": {'*.*': ['INSERT', 'UPDATE'], '`%s`.*' % SRC: ['DELETE']}}
        b = P.release_block(names, [ITEM, APP], snap, config=object())
        text = b.render()
        self.assertLess(text.index("GRANT INSERT, UPDATE ON *.* TO 'app'@'%';"), text.index('START PIPELINE'))
        self.assertIn("GRANT DELETE ON `%s`.* TO 'app'@'%%';" % SRC, text)
        self.assertIn("holds every restored privilege on `%s`.*; expect n = 1" % SRC, text)
        self.assertNotIn('START PIPELINE `%s`.`app_owned`' % SRC, text)
        start = [i for i in b.statements if i.sql.startswith('START')][0]
        self.assertEqual(start.skip_if, (P.IS_RUNNING_SQL, (SRC, 'p1')))
        self.assertIn('no retention gap on p1', text)
        self.assertIn("SOURCE_PARTITION_ID = '0' AND EARLIEST_OFFSET > '5'", text)
        self.assertIn('every captured partition of p1 present on the live name', text)
        self.assertLess(text.index('current query_parallelism_per_leaf_core'), text.index('SET GLOBAL'))


class CredentialTests(unittest.TestCase):
    def test_resolution_order_and_refusals(self):
        cfg = config_from()
        self.assertEqual(P.credential_for(ITEM, cfg, {'S2REBUILD_KAFKA_CREDENTIALS': 'x'})[1], 'S2REBUILD_KAFKA_CREDENTIALS')
        with self.assertRaisesRegex(KitError, 'S2REBUILD_KAFKA_CREDENTIALS for pipeline p1 is not set'):
            P.credential_for(ITEM, cfg, {})
        with self.assertRaisesRegex(KitError, 'No credential variable is configured'):
            P.credential_for(dict(ITEM, source_type='AZURE'), cfg, {})
        with self.assertRaisesRegex(KitError, 'redaction marker'):
            P.credential_for(dict(ITEM, definition=DEFINITION.replace('<<CREDENTIALS>>', '<redacted>')), cfg, {'S2REBUILD_KAFKA_CREDENTIALS': 'x'})
        per = config_from(lambda d: d.update(pipelines={'per_pipeline_env': {'p1': 'P1_CREDS'}}))
        self.assertEqual(P.credential_for(ITEM, per, {'P1_CREDS': 'v', 'S2REBUILD_KAFKA_CREDENTIALS': 'w'})[1], 'P1_CREDS')
        P.check_credentials([PLAIN], cfg, {})
        with self.assertRaisesRegex(KitError, 'S2REBUILD_KAFKA_CREDENTIALS'):
            P.check_credentials([PLAIN, ITEM], cfg, {})

    def test_literal_escaping_and_every_secret_form_masked(self):
        cfg = config_from()
        value = '{"private_key": "line1\\nline2", "q": "it\'s"}'
        redactor = Redactor()
        sql = P.substitute_credentials([ITEM], cfg, {'S2REBUILD_KAFKA_CREDENTIALS': value}, redactor)(DEFINITION)
        self.assertIn('CREDENTIALS ' + sql_literal(value), sql)
        self.assertIn('\\\\n', sql_literal(value), 'the backslash reaches the server as one backslash')
        self.assertIn("it''s", sql_literal(value))
        for form in secret_forms(value):
            self.assertEqual(redactor(form), '<redacted>', form)
        self.assertNotIn('line1', redactor(repr((1064, "near %s" % sql))))
        self.assertEqual(P.substitute_credentials([ITEM], cfg, {}, redactor)('SELECT 1'), 'SELECT 1')

    def test_the_server_redaction_token_is_replaced(self):
        self.assertIn("CREDENTIALS '<<CREDENTIALS>>'", inventory.redact_credentials(SERVER_DEFINITION))
        self.assertIn("CREDENTIALS '<<CREDENTIALS>>'", inventory.redact_credentials(DEFINITION.replace("'<<CREDENTIALS>>'", "'{\"a\": \"b\"}'")))
        with self.assertRaises(KitError):
            inventory.redact_credentials("CREATE PIPELINE `p` AS LOAD DATA KAFKA 'b' CREDENTIALS @secret INTO TABLE t")


class HandlerTests(unittest.TestCase):
    def test_o2_o9_o11_c1_with_the_owner_profile(self):
        h = owner_harness(self)
        with_source_pipeline(h)
        h.journal.record_attachment('source', SRC, 'sid-source', 'cid-1', 'READWRITE')
        out2 = h.step('O2', redo=True)
        self.assertEqual(out2.status, steps.PASSED, h.text())
        self.assertEqual(h.fake.pipelines[0]['STATE'], 'Stopped')
        o2_dir = h.tmp / 'runs' / h.run_id / SRC / 'inventory' / 'O2'
        p1_sql = o2_dir / 'pipelines' / 'p1.sql'
        self.assertEqual(stat.S_IMODE(os.stat(p1_sql).st_mode), 0o600)
        self.assertIn("CREDENTIALS '<<CREDENTIALS>>'", p1_sql.read_text())
        self.assertEqual(P.capture_for(h.ctx())['step'], 'O2')
        self.assertEqual(h.step('O2', redo=True).status, steps.PASSED, 'a rerun skips the stopped pipeline')
        self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
        pass_steps(h, 'O7', 'O8')
        out9 = h.step('O9', redo=True)
        self.assertEqual(out9.status, steps.REFUSED, 'no credential variable set')
        self.assertFalse(any(w.startswith('CREATE PIPELINE') for w in h.fake.writes))
        value = '{"sasl.password": "s3cret\\\\value", "x": "it\'s"}'
        env = {'S2REBUILD_KAFKA_CREDENTIALS': value}
        out9 = h.step('O9', redo=True, options={'env': env})
        self.assertEqual(out9.status, steps.PASSED, h.text())
        created = [p for p in h.fake.pipelines if p['PIPELINE_NAME'] == 'p1' and p.get('DATABASE_NAME') == TGT]
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0]['STATE'], 'Stopped', 'never started at O9')
        self.assertIn(sql_literal(value), created[0]['definition'])
        self.assertNotIn('s3cret', h.text())
        self.assertNotIn('s3cret', (h.tmp / 'runs' / h.run_id / SRC / 'owner-sql' / 'O9.sql').read_text())
        self.assertEqual([o['phase'] for o in h.journal.offsets('before_start')][:1], ['before_start'])
        out9 = h.step('O9', redo=True, options={'env': env})
        self.assertEqual(out9.status, steps.PASSED, 'a rerun skips the CREATE of an existing pipeline: %s' % h.text()[-400:])
        self.assertEqual(len([p for p in h.fake.pipelines if p['PIPELINE_NAME'] == 'p1' and p.get('DATABASE_NAME') == TGT]), 1)
        pass_steps(h, 'O10')
        out11 = h.step('O11', redo=True)
        self.assertEqual(out11.status, steps.PASSED, h.text())
        self.assertEqual([p['STATE'] for p in h.fake.pipelines if p.get('DATABASE_NAME', SRC) == SRC and p['PIPELINE_NAME'] == 'p1'], ['Running'])
        self.assertIsNotNone(h.journal.get_meta('released_utc'))
        self.assertEqual(h.fake.globals.get('query_parallelism_per_leaf_core'), '0.5')
        h.fake.pipeline_offsets[0].update(LATEST_OFFSET=9, LATEST_LOADED_OFFSET=7)
        out_c1 = h.step('C1', redo=True, options={'sleep': lambda seconds: None})
        self.assertEqual((out_c1.status, out_c1.exit_code), (steps.FAILED, 4))
        self.assertIn('offsets not advancing although data waits: p1 partition 0', out_c1.note)

        def advance(seconds):
            h.fake.pipeline_offsets[0].update(LATEST_LOADED_OFFSET=9, LATEST_SUCCESSFUL_OFFSET=9)
        out_c1 = h.step('C1', redo=True, options={'sleep': advance})
        self.assertEqual(out_c1.status, steps.PASSED, h.text())
        self.assertIn('offsets advancing or caught up on every partition', out_c1.note)
        self.assertIsNone(h.journal.get_meta('app_writes_seen'), 'no session and no count change yet')

        def write(seconds):
            h.fake.rows(SRC, 't_single').append({'id': 'new', 'payload': 'p', 'n': 1})
            advance(seconds)
        out_c1 = h.step('C1', redo=True, options={'sleep': write})
        self.assertEqual(out_c1.status, steps.PASSED)
        self.assertIsNotNone(h.journal.get_meta('app_writes_seen'), 'a count change marks application writes')

    def test_o2_stops_pipelines_created_after_the_capture_and_refuses_a_running_capture(self):
        h = owner_harness(self)
        with_source_pipeline(h)
        write_o2(h, [])
        h.journal.record_attachment('source', SRC, 'sid-source', 'cid-1', 'READWRITE')
        out = h.step('O2', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('STOP PIPELINE `%s`.`p1`' % SRC, h.fake.writes)
        with self.assertRaisesRegex(KitError, 'p1 are not Stopped after the stop'):
            P._require_stopped([dict(ITEM, state='Running')])

    def test_o9_refuses_without_the_o2_capture_and_passes_with_zero_pipelines(self):
        h = owner_harness(self)
        self.assertEqual(h.o5_o6().status, steps.PASSED)
        pass_steps(h, 'O7', 'O8')
        out = h.step('O9', redo=True)
        self.assertEqual((out.status, out.exit_code), (steps.FAILED, 4))
        self.assertIn('no pipeline capture exists', out.note)
        early = dict(h.inventory, pipelines={'captured': True, 'note': 'x', 'items': [ITEM]})
        inventory.write(early, inventory.inventory_dir(h.config, h.run_id, h.db, 'P3'), h.plans())
        out = h.step('O9', redo=True, options={'env': {'S2REBUILD_KAFKA_CREDENTIALS': 'x'}})
        self.assertEqual((out.status, out.exit_code), (steps.FAILED, 4))
        self.assertIn('comes from P3, taken before the stop', out.note)
        self.assertFalse(any(w.startswith('CREATE PIPELINE') for w in h.fake.writes))
        write_o2(h, [])
        out = h.step('O9', redo=True)
        self.assertEqual(out.status, steps.PASSED, 'a capture with no pipelines passes O9: %s' % h.text()[-300:])

    def test_o9_customer_route_only_verifies(self):
        h = owner_harness(self, mutate=lambda d: d.update(pipelines={'route': 'customer'}))
        self.assertEqual(h.o5_o6().status, steps.PASSED)
        pass_steps(h, 'O7', 'O8')
        write_o2(h, [ITEM])
        out = h.step('O9', redo=True)
        self.assertEqual(out.status, steps.REFUSED, 'nothing created yet')
        self.assertIn('pipeline count equals the capture', out.note)
        self.assertFalse(any(w.startswith(('CREATE PIPELINE', 'USE')) for w in h.fake.writes))
        h.fake.pipelines.append({'PIPELINE_NAME': 'p1', 'STATE': 'Stopped', 'DATABASE_NAME': TGT})
        h.fake.pipeline_offsets.append({'DATABASE_NAME': TGT, 'PIPELINE_NAME': 'p1', 'SOURCE_TYPE': 'KAFKA', 'SOURCE_PARTITION_ID': '0', 'EARLIEST_OFFSET': 1,
                                        'LATEST_OFFSET': 5, 'LATEST_LOADED_OFFSET': 5, 'LATEST_SUCCESSFUL_OFFSET': 5})
        out = h.step('O9', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text()[-300:])
        self.assertIn('recreate-with-placeholders.sql', h.text())

    def test_o11_refuses_a_retention_gap_or_a_missing_partition(self):
        h = owner_harness(self)
        write_o2(h, [ITEM])
        with_source_pipeline(h, state='Stopped', earliest=40, latest=50)
        pass_steps(h, 'O5', 'O6', 'O7', 'O8', 'O9', 'O10')
        out = h.step('O11', redo=True)
        self.assertEqual((out.status, out.exit_code), (steps.FAILED, 4))
        self.assertIn('p1 partition 0 (recorded 5, earliest 40)', out.note)
        self.assertFalse(any(w.startswith(('START PIPELINE', 'GRANT')) for w in h.fake.writes))
        h.fake.pipeline_offsets = []
        out = h.step('O11', redo=True)
        self.assertIn('p1 partition 0 (recorded 5, earliest missing)', out.note)

    def test_o11_restores_an_unrestored_snapshot_whatever_the_freeze_mode(self):
        h = owner_harness(self)
        write_o2(h, [])
        grants.record_snapshot(h.journal, {"'app'@'%'": {'*.*': ['INSERT']}})
        pass_steps(h, 'O5', 'O6', 'O7', 'O8', 'O9', 'O10')
        out = h.step('O11', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn({'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'INSERT'}, h.fake.privileges)
        self.assertIsNotNone(h.journal.grants_snapshot()["'app'@'%'"]['restored_utc'])
        self.assertEqual(grants.unrestored_snapshot(h.journal), {})

    def test_c1_needs_the_release_and_the_owner_for_pipelines_and_grants(self):
        h = Harness(self)
        pass_steps(h, 'O5', 'O6', 'O7', 'O8', 'O9', 'O10', 'O11')
        out = h.step('C1', redo=True)
        self.assertEqual(out.status, steps.REFUSED)
        self.assertIn('needs the release time', out.note)
        h.journal.set_meta('released_utc', '2026-11-01T07:00:00Z')
        out = h.step('C1', redo=True, options={'sleep': lambda s: None})
        self.assertEqual((out.status, out.exit_code), (steps.PENDING, 4))
        self.assertIn('need the owner profile', out.note)
        ho = owner_harness(self)
        pass_steps(ho, 'O5', 'O6', 'O7', 'O8', 'O9', 'O10', 'O11')
        ho.journal.set_meta('released_utc', '2026-11-01T07:00:00Z')
        ho.fake.privileges += [{'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'INSERT'}]
        grants.record_reference(ho.journal, {"'app'@'%'": {'*.*': ['INSERT', 'UPDATE']}})
        out = ho.step('C1', redo=True, options={'sleep': lambda s: None})
        self.assertEqual(out.status, steps.FAILED)
        self.assertIn("grants differ from the snapshot: 'app'@'%'", out.note)

    def test_the_runner_cannot_see_other_accounts_grants_or_pipelines(self):
        h = owner_harness(self)
        with_source_pipeline(h)
        h.fake.privileges.append({'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'INSERT'})
        runner = h.sessions.runner(read_guard())
        self.assertNotIn("'app'@'%'", grants.snapshot(runner, SRC, set()), 'the runner sees only its own grants')
        self.assertEqual(runner.query(inventory.PIPELINES_SQL, (SRC,)), [])
        owner = h.sessions.owner(read_guard())
        self.assertIn("'app'@'%'", grants.snapshot(owner, SRC, set()))


class FreezeTests(unittest.TestCase):
    def test_probe_target_prefers_a_string_column_then_an_integer_key(self):
        h = owner_harness(self)
        self.assertEqual(freeze.probe_target(h.plans(), h.inventory['tables']), ('t_comp', 'a', 'string'))
        tables = [dict(t, columns=[dict(c, data_type='int') for c in t['columns']]) for t in h.inventory['tables']]
        self.assertEqual(freeze.probe_target(h.plans(), tables)[2], 'integer')
        with self.assertRaises(KitError):
            freeze.probe_target(h.plans(), [dict(t, columns=[dict(c, data_type='double') for c in t['columns']]) for t in h.inventory['tables']])

    def test_write_probe_never_deletes_and_passes_only_on_a_read_only_refusal(self):
        h = owner_harness(self)
        session = h.sessions.runner(probe_guard(SRC))
        before = len(h.fake.rows(SRC, 't_single'))
        with self.assertRaisesRegex(KitError, 'was accepted by the server'):
            freeze.write_probe(session, SRC, 't_single', 'id', h.log)
        self.assertEqual(len(h.fake.rows(SRC, 't_single')), before, 'the token matches no row')
        h.fake.rows(SRC, 't_nokey').append({'x': 0, 'y': None, 'z': None})
        with self.assertRaisesRegex(KitError, 'was accepted'):
            freeze.write_probe(session, SRC, 't_nokey', 'x', h.log, kind='integer')
        self.assertIn({'x': 0, 'y': None, 'z': None}, h.fake.rows(SRC, 't_nokey'), 'MIN minus one never matches the 0 row')
        self.assertTrue(any(w.endswith('`x` = -1') for w in h.fake.writes))
        h.fake.attachment(SRC)['ATTACH_TYPE'] = 'READONLY'
        self.assertTrue(freeze.write_probe(session, SRC, 't_single', 'id', h.log))
        self.assertTrue(freeze.is_read_only_refusal(FakeDisconnect(1290, 'x')))

    def test_o3_path_a_with_the_owner_profile(self):
        h = owner_harness(self)
        h.fake.privileges.append({'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'INSERT'})
        h.journal.record_attachment('source', SRC, 'sid-source', 'cid-1', 'READWRITE')
        out = h.step('O3', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual(h.fake.attachment(SRC)['ATTACH_TYPE'], 'READONLY')
        self.assertIsNotNone(h.journal.get_meta('freeze_time'))
        writes = [w for w in h.fake.writes if w.startswith(('DETACH', 'ATTACH', 'DELETE'))]
        self.assertEqual(writes[0], 'DETACH DATABASE `%s`' % SRC)
        self.assertTrue(writes[1].startswith("ATTACH DATABASE `%s` STORAGEID 'sid-source' READ ONLY" % SRC))
        self.assertTrue(writes[2].startswith("DELETE FROM `%s`.`t_comp` WHERE `a` = 's2rebuild-probe-" % SRC))
        self.assertEqual(grants.reference_from_journal(h.journal), {"'app'@'%'": {'*.*': ['INSERT']}}, 'Path A keeps a grants reference for C1')

    def test_o3_resumes_after_the_read_only_attachment_without_the_owner(self):
        h = Harness(self)
        h.fake.attachment(SRC)['ATTACH_TYPE'] = 'READONLY'
        h.journal.record_attachment('source', SRC, 'sid-source', 'cid-1', 'READWRITE')
        out = h.step('O3', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('O3 resumed', out.note)
        self.assertIsNotNone(h.journal.get_meta('freeze_time'))
        self.assertFalse(any(w.startswith(('DETACH', 'ATTACH')) for w in h.fake.writes))
        self.assertIn('refused as read only', h.text())

    def test_o3_refuses_without_a_recorded_source_storage_id(self):
        h = owner_harness(self)
        out = h.step('O3', redo=True)
        self.assertEqual(out.status, steps.REFUSED)
        self.assertIn('source_sid not recorded', out.note)
        self.assertFalse(any(w.startswith('DETACH') for w in h.fake.writes))
        no_owner = Harness(self)
        out = no_owner.step('O3', redo=True)
        self.assertEqual(out.status, steps.PENDING)
        self.assertIn('DETACH DATABASE `%s`;' % SRC, no_owner.text())

    def test_o3_detach_escalation_through_the_admin_block(self):
        h = owner_harness(self, mutate=lambda d: d.setdefault('freeze', {}).update(detach_wait_seconds=1))
        h.journal.record_attachment('source', SRC, 'sid-source', 'cid-1', 'READWRITE')
        h.fake.detach_blocks.add(SRC)
        out = h.step('O3', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('DETACH DATABASE `%s` KILL' % SRC, h.fake.writes)
        self.assertEqual(h.fake.attachment(SRC)['ATTACH_TYPE'], 'READONLY')


class GrantsTests(unittest.TestCase):
    def test_snapshot_per_scope_revoke_restore_and_compare(self):
        h = owner_harness(self)
        h.fake.privileges += [{'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': p} for p in ('SELECT', 'INSERT', 'UPDATE')]
        h.fake.privileges += [{'GRANTEE': "'admin_like'@'%'", 'PRIVILEGE_TYPE': 'ALL PRIVILEGES'}]
        h.fake.schema_privileges += [{'GRANTEE': "'dbwriter'@'%'", 'TABLE_SCHEMA': SRC, 'PRIVILEGE_TYPE': 'DELETE'}]
        h.fake.table_privileges += [{'GRANTEE': "'app'@'%'", 'TABLE_SCHEMA': SRC, 'TABLE_NAME': 't_single', 'PRIVILEGE_TYPE': 'UPDATE'}]
        owner = h.sessions.owner(steps.guard_for('lockout', h.db, steps.OWNER))
        snap = grants.snapshot(owner, SRC, {'s2rebuild_runner', 'ops_owner'})
        self.assertEqual(snap["'app'@'%'"], {'*.*': ['INSERT', 'UPDATE'], '`%s`.`t_single`' % SRC: ['UPDATE']})
        self.assertEqual(snap["'dbwriter'@'%'"], {'`%s`.*' % SRC: ['DELETE']}, 'a database-only writer is in the snapshot')
        self.assertEqual(len(snap["'admin_like'@'%'"]['*.*']), len(grants.REVOKE_PRIVILEGES), 'ALL PRIVILEGES expands to the write list')
        statements = grants.revoke_statements(snap)
        self.assertIn("REVOKE DELETE ON `%s`.* FROM 'dbwriter'@'%%'" % SRC, statements)
        self.assertIn("REVOKE UPDATE ON `%s`.`t_single` FROM 'app'@'%%'" % SRC, statements)
        for sql in statements:
            if 'admin_like' not in sql:
                owner.execute(sql)
        self.assertEqual(grants.restored_by_another(owner, SRC, {g: v for g, v in snap.items() if g != "'admin_like'@'%'"}), {})
        h.fake.schema_privileges.append({'GRANTEE': "'dbwriter'@'%'", 'TABLE_SCHEMA': SRC, 'PRIVILEGE_TYPE': 'DELETE'})
        self.assertEqual(grants.restored_by_another(owner, SRC, snap)["'dbwriter'@'%'"], {'`%s`.*' % SRC: ['DELETE']})
        part = {g: v for g, v in snap.items() if g != "'admin_like'@'%'"}
        grants.record_snapshot(h.journal, part)
        self.assertEqual(grants.snapshot_from_journal(h.journal), part)
        self.assertTrue(grants.restore(owner, h.journal, SRC, part, h.log))
        self.assertEqual(grants.compare(owner, SRC, part), {})
        path = h.tmp / 'grants-snapshot.json'
        grants.write_snapshot(path, part)
        self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
        grants.write_snapshot(path, {})
        self.assertEqual(grants.read_snapshot_file(path), part, 'an existing snapshot file is never overwritten')
        self.assertEqual(grants.normalize({"'old'@'%'": ['INSERT']}), {"'old'@'%'": {'*.*': ['INSERT']}}, 'an old list row reads as global')


class LockoutTests(unittest.TestCase):
    def harness(self):
        """A lockout-mode harness with two held connections and two quiet passes required."""
        return owner_harness(self, mutate=lambda d: d.update(freeze={'mode': 'lockout', 'lockout': {'connections': 2, 'quiet_passes_required': 2}}))

    def test_a_pass_kills_idle_and_busy_sessions_and_revokes_restored_grants(self):
        h = self.harness()
        h.fake.privileges += [{'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': p} for p in ('INSERT', 'UPDATE')]
        h.fake.schema_privileges += [{'GRANTEE': "'dbwriter'@'%'", 'TABLE_SCHEMA': SRC, 'PRIVILEGE_TYPE': 'INSERT'}]
        h.fake.processlist = [{'ID': 11, 'USER': 'app', 'HOST': 'h:1', 'DB': SRC, 'COMMAND': 'Query', 'TIME': 1, 'INFO': 'INSERT INTO t VALUES (1)'},
                              {'ID': 12, 'USER': 'app', 'HOST': 'h:2', 'DB': SRC, 'COMMAND': 'Sleep', 'TIME': 30, 'INFO': None},
                              {'ID': 13, 'USER': 'dbwriter', 'HOST': 'h:3', 'DB': SRC, 'COMMAND': 'Sleep', 'TIME': 5, 'INFO': None},
                              {'ID': 14, 'USER': 'other', 'HOST': 'h:4', 'DB': SRC, 'COMMAND': 'Query', 'TIME': 1, 'INFO': 'SELECT 1'}]
        admin, held, snap = lockout.prepare(h.sessions, h.db, h.config, h.journal, h.log, h.tmp / 'snap.json')
        self.assertEqual(set(snap), {"'app'@'%'", "'dbwriter'@'%'"})
        self.assertNotIn({'GRANTEE': "'dbwriter'@'%'", 'TABLE_SCHEMA': SRC, 'PRIVILEGE_TYPE': 'INSERT'}, h.fake.schema_privileges)
        h.fake.privileges.append({'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'UPDATE'})
        seen, killed, gone, restored = lockout.one_pass(held, SRC, snap, h.journal, h.log)
        self.assertEqual((seen, killed, gone), (3, 3, 0))
        self.assertEqual(sorted(h.fake.killed), [11, 12, 13], 'idle sessions of locked accounts are killed too')
        self.assertEqual(restored, {"'app'@'%'": {'*.*': ['UPDATE']}})
        self.assertIn('GRANT RESTORED BY ANOTHER SESSION', h.text())
        self.assertEqual([r['ID'] for r in h.fake.processlist], [14])

    def test_a_rerun_reuses_the_first_snapshot(self):
        h = self.harness()
        h.fake.privileges += [{'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': p} for p in ('INSERT', 'UPDATE')]
        path = h.tmp / 'snap.json'
        _, held, first = lockout.prepare(h.sessions, h.db, h.config, h.journal, h.log, path)
        h.fake.privileges.append({'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'INSERT'})
        _, _, second = lockout.prepare(h.sessions, h.db, h.config, h.journal, h.log, path)
        self.assertEqual(second, first)
        self.assertEqual(grants.snapshot_from_journal(h.journal)["'app'@'%'"], {'*.*': ['INSERT', 'UPDATE']})
        self.assertEqual(grants.read_snapshot_file(path)["'app'@'%'"], {'*.*': ['INSERT', 'UPDATE']})
        self.assertNotIn({'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'INSERT'}, h.fake.privileges, 'the reappeared grant was revoked again')
        self.assertIn('reusing the first grants snapshot', h.text())

    def test_the_quiet_proof_runs_again_after_a_failure_and_the_stop_file_is_seen(self):
        h = self.harness()
        h.fake.privileges += [{'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'INSERT'}]
        _, held, snap = lockout.prepare(h.sessions, h.db, h.config, h.journal, h.log)
        answers = iter([False, True])
        calls = []

        def quiet():
            calls.append(1)
            return next(answers)
        summary = lockout.run_lockout(held, SRC, snap, h.journal, h.log, h.config, sleep=lambda s: None, max_passes=20, quiet_hook=quiet)
        self.assertTrue(summary['quiet_proof'])
        self.assertEqual((len(calls), summary['passes']), (2, 4), 'proof after 2 quiet passes, failed, 2 more, passed')
        stop = h.tmp / 'stop'
        config = h.config

        def slow(seconds):
            stop.write_text('x')
        import dataclasses
        from s2rebuild.config import Section
        cfg = dataclasses.replace(config, freeze=Section(dict(config.freeze.as_dict(), lockout=dict(config.freeze.lockout.as_dict(), interval_seconds=60))))
        summary = lockout.run_lockout(held, SRC, snap, h.journal, h.log, cfg, stop_file=stop, sleep=slow, max_passes=50)
        self.assertEqual((summary['passes'], summary['stopped']), (1, 'the stop file is present'), 'the wait is cut short by the stop file')

    def test_o3_lockout_needs_the_owner_and_owner_sql_prints_the_plan(self):
        h = Harness(self, mutate=lambda d: d.update(freeze={'mode': 'lockout'}))
        out = h.step('O3', redo=True)
        self.assertEqual(out.status, steps.PENDING)
        self.assertIn('Path B lockout needs the owner profile', h.text())
        block = lockout.plan_block(names_of(h), {"'app'@'%'": {'*.*': ['INSERT']}})
        text = block.render()
        self.assertIn("REVOKE INSERT ON *.* FROM 'app'@'%';", text)
        self.assertLess(text.index("holds write privileges on *.*"), text.index('REVOKE'))
        self.assertIn('holds none of them on *.*; expect n = 0', text)


if __name__ == '__main__':
    unittest.main()
