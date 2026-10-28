"""s2rebuild tests test_pipelines: O2, O9, O11 and C1 blocks and handlers, credentials, and the freeze.

Author: John Bergamini
"""
import contextlib
import io
import json
import os
import stat
import threading
import unittest
from unittest import mock

from s2rebuild import freeze, grants, inventory, notebook, ownersql, pipelines as P, steps, switch
from s2rebuild.common import KitError
from s2rebuild.connect import Redactor
from s2rebuild.sqlutil import probe_guard, read_guard, secret_forms, sql_literal
from tests.fakedb import FakeDisconnect, SimulatedCrash
from tests.helpers import FAULT_POINTS, config_from, crash_journal, run_cli
from tests.test_copy import SRC, TGT, Harness
from tests.test_switch import OWNER, machine_for, ownerlib, run_rollback_block, switched_harness

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


def released_for_c1(test, owner=False):
    """A copied harness (target counts recorded), O7 to O11 passed and the release time recorded: ready for C1."""
    h = owner_harness(test) if owner else Harness(test)
    assert h.o5_o6().status == steps.PASSED, h.text()
    pass_steps(h, 'O7', 'O8', 'O9', 'O10', 'O11')
    h.journal.set_meta('released_utc', '2026-11-01T07:00:00Z')
    return h


def switched_for_release(test):
    """After S3: pipeline p1 replayed into the replacement (stopped at offset 5) followed the switch to the live name; O1 to O10 passed."""
    h = switched_harness(test)
    h.fake.pipelines = [{'PIPELINE_NAME': 'p1', 'STATE': 'Stopped', 'DATABASE_NAME': TGT}]
    h.fake.pipeline_offsets = [{'DATABASE_NAME': TGT, 'PIPELINE_NAME': 'p1', 'SOURCE_TYPE': 'KAFKA', 'SOURCE_PARTITION_ID': '0', 'EARLIEST_OFFSET': 1,
                                'LATEST_OFFSET': 5, 'LATEST_LOADED_OFFSET': 5, 'LATEST_SUCCESSFUL_OFFSET': 5}]
    machine, _ = machine_for(h)
    machine.counts['pipelines'] = 1
    assert machine.forward() == switch.COMPLETE, h.text()
    write_o2(h, [ITEM])
    pass_steps(h, 'O7', 'O8', 'O9', 'O10')
    return h


def starts(h):
    """Every START PIPELINE the fake applied."""
    return [w for w in h.fake.writes if w.startswith('START PIPELINE')]


def live_state(h):
    """The state of p1 on the live name."""
    return [p['STATE'] for p in h.fake.pipelines if p.get('DATABASE_NAME') == SRC and p['PIPELINE_NAME'] == 'p1']


def block_now(h, step_id, rollback=False):
    """The notebook form of a block built from the journal as it is now (the O10 handler writes the rollback files at O10).

    For the rollback, the variant the journal calls for: through the retained copy (R1, R2b)
    once S2b dropped the original record, else R1 and R2a.
    """
    ctx = h.ctx()
    names, inputs = ownersql.names_for(ctx), ownersql.block_inputs(ctx)
    if rollback and names['source_dropped']:
        return dict(ownersql.companion_blocks(step_id, names, h.config, rollback=True, released=inputs['released'],
                                              app_writes_seen=inputs['app_writes_seen']))['retained'].to_dict()
    return ownersql.block_for(step_id, names, h.config, rollback=rollback, **inputs).to_dict()


def run_dict(h, block):
    """Run a notebook block with ownerlib on a plain cursor; returns the result dict."""
    with contextlib.redirect_stdout(io.StringIO()):
        return ownerlib()['run_block'](h.fake.connect().cursor(), block, {'run_id': h.run_id})


PIPELINE_PROOF = 'FAILED at check R1 before (zero-change proof): no pipeline Running on the live name'
PARTITION_CHECK = r'PIPELINES_OFFSETS WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s AND SOURCE_PARTITION_ID IN'
RETENTION_CHECK = r'PIPELINES_OFFSETS WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s AND \(\(SOURCE_PARTITION_ID'
RETENTION_READ = r'LATEST_SUCCESSFUL_OFFSET FROM information_schema\.PIPELINES_OFFSETS WHERE DATABASE_NAME = %s$'
EARLY_STOPS = ('check before', 'check after', 'check journal', 'owner session', 'check expectation')


class BlockTests(unittest.TestCase):
    def test_stop_block_skips_stopped_pipelines_and_captures(self):
        names = {'source': SRC, 'replacement': TGT}
        b = P.stop_block(names, [ITEM, APP], extra_names=['late'])
        text = b.render()
        for name in ('p1', 'app_owned', 'late'):
            self.assertIn('STOP PIPELINE `%s`.`%s`;' % (SRC, name), text)
        self.assertIn("SKIP the next statement when this returns n > 0: SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = "
                      "'%s' AND PIPELINE_NAME = 'p1' AND STATE = 'Stopped';" % SRC, text)
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
        self.assertIn("offset of p1 partition 0 equals the O2 capture (LATEST_SUCCESSFUL_OFFSET); expect successful = 5", text)

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

    def test_release_block_leaves_pipelines_stopped_when_start_at_release_is_none(self):
        import types
        names = {'source': SRC, 'replacement': TGT}
        none = types.SimpleNamespace(pipelines=types.SimpleNamespace(start_at_release='none'))
        b = P.release_block(names, [ITEM, APP], config=none)
        text = b.render()
        self.assertNotIn('START PIPELINE', [i.sql[:14] for i in b.statements])
        self.assertEqual([i for i in b.statements if i.sql.startswith('START')], [])
        self.assertIn("no pipeline Running (pipelines.start_at_release: none); expect n = 0", text)
        self.assertIn('every captured partition of p1 present on the live name', text)
        self.assertNotIn('no retention gap', text, 'the retention check belongs to the start')
        self.assertIn('1 pipeline(s) left stopped by pipelines.start_at_release: none. Start one when you choose: START PIPELINE `%s`.`<name>`;' % SRC, text)
        self.assertIn('START PIPELINE `%s`.`p1`' % SRC, P.release_block(names, [ITEM], config=None).render(), 'all stays the default')
        with self.assertRaises(KitError):
            config_from(mutate=lambda d: d.setdefault('pipelines', {}).update(start_at_release='some'))
        self.assertEqual(config_from().pipelines.start_at_release, 'all')

    def test_c1_reports_pipelines_left_stopped_without_requiring_them_running(self):
        h = owner_harness(self, mutate=lambda d: d.setdefault('pipelines', {}).update(start_at_release='none'))
        self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
        pass_steps(h, 'O7', 'O8', 'O9', 'O10', 'O11')
        h.journal.set_meta('released_utc', '2026-11-01T07:00:00Z')
        write_o2(h, [ITEM])
        with_source_pipeline(h, state='Stopped')
        slept = []
        out = h.step('C1', redo=False, options={'sleep': slept.append, 'idle_workload': 'test database, no application'})
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('pipelines left stopped by pipelines.start_at_release: none: 1 of 1 not Running', h.text())
        self.assertIn('offsets not sampled: no pipeline was started', h.text())
        self.assertNotIn('pipelines not Running', h.text())

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
            inventory.redact_credentials("CREATE PIPELINE `p` AS LOAD DATA KAFKA 'b' CREDENTIALS <secret hidden> INTO TABLE t")


class CaptureRobustnessTests(unittest.TestCase):
    """F14: the O2 capture around SHOW CREATE PIPELINE resumes on a rerun; a masked CONFIG value never reaches CREATE PIPELINE."""

    def test_f14_capture_fault_points_around_show_create_pipeline_then_rerun(self):
        """Before and after SHOW CREATE PIPELINE, and before the journal update (upsert_pipeline): the rerun of O2 captures once, complete.

        The topic and the target are named credentials and the pipeline has no CREDENTIALS clause, so the
        rerun also proves the narrowed check: the word alone no longer aborts the capture.
        """
        definition = "CREATE PIPELINE `p1` AS LOAD DATA KAFKA 'b:9092/credentials' INTO TABLE `credentials`"
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = owner_harness(self)
                with_source_pipeline(h)
                h.fake.pipelines[0]['definition'] = definition
                h.journal.record_attachment('source', SRC, '1028600672070281646:7367158985892520834', 'cid-1', 'READWRITE')
                o2_file = inventory.inventory_dir(h.config, h.run_id, h.db, 'O2') / inventory.INVENTORY_FILE
                if point == 'journal':
                    fired = crash_journal(self, h.journal, 'upsert_pipeline')
                    with self.assertRaises(SimulatedCrash):
                        h.step('O2', redo=True)
                    self.assertEqual(len(fired), 1)
                else:
                    h.fake.fail_statement(r'^SHOW CREATE PIPELINE', point)
                    with self.assertRaises(FakeDisconnect):
                        h.step('O2', redo=True)
                    self.assertFalse(o2_file.exists(), 'nothing is stored from a capture that did not finish')
                self.assertEqual(h.journal.step_status('O2'), 'failed', 'O9 cannot use a capture whose step did not pass')
                self.assertEqual(h.journal.pipelines(), [])
                self.assertEqual(h.fake.pipelines[0]['STATE'], 'Stopped')
                out = h.step('O2', redo=True)
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertEqual(len([w for w in h.fake.writes if w.startswith('STOP PIPELINE')]), 1, 'the rerun skips the stopped pipeline')
                cap = P.capture_for(h.ctx())
                self.assertEqual(cap['step'], 'O2')
                self.assertEqual([(i['name'], i['state'], i['definition'], i['target']) for i in cap['items']],
                                 [('p1', 'Stopped', definition, 'table credentials')])
                self.assertEqual([(r['name'], r['state_at_capture']) for r in h.journal.pipelines()], [('p1', 'Stopped')])
                self.assertEqual(len(h.journal.offsets('capture')), 1, 'the offsets read after the stop are journaled once')

    def test_f14_a_masked_config_value_is_refused_at_o9_before_any_statement(self):
        """Helios returns a secret CONFIG value masked: O2 stores the mask, and O9 refuses before USE, CREATE or ALTER reaches the server.

        The capture flags the pipeline with redaction_marker, read on the stored definition as O9 reads it, so the workbook names the
        pipelines O9 will refuse: the server's CREDENTIALS token alone (replaced by the placeholder) does not flag one, on the kit
        capture and on the notebook capture (items_from_capture)."""
        masked = ("CREATE PIPELINE `p1` AS LOAD DATA KAFKA 'b:9092/t' CONFIG '{\"security.protocol\": \"SASL_SSL\", "
                  "\"sasl.password\": \"******\"}' CREDENTIALS <CREDENTIALS REDACTED> INTO TABLE `t_single`")
        plain = owner_harness(self)
        with_source_pipeline(plain)
        plain.journal.record_attachment('source', SRC, '1028600672070281646:7367158985892520834', 'cid-1', 'READWRITE')
        self.assertEqual(plain.step('O2', redo=True).status, steps.PASSED, plain.text())
        self.assertFalse(P.capture_for(plain.ctx())['items'][0]['redaction_marker'], 'the CREDENTIALS token alone is not a marker O9 refuses')
        notebook_capture = {'states': [{'PIPELINE_NAME': 'p1', 'STATE': 'Stopped'}, {'PIPELINE_NAME': 'p2', 'STATE': 'Stopped'}],
                            'definitions': {'p1': [{'Pipeline': 'p1', 'Create Pipeline': SERVER_DEFINITION}],
                                            'p2': [{'Pipeline': 'p2', 'Create Pipeline': masked.replace('`p1`', '`p2`')}]}}
        self.assertEqual([(i['name'], i['redaction_marker']) for i in P.items_from_capture(notebook_capture)], [('p1', False), ('p2', True)])
        h = owner_harness(self)
        with_source_pipeline(h)
        h.fake.pipelines[0]['definition'] = masked
        h.journal.record_attachment('source', SRC, '1028600672070281646:7367158985892520834', 'cid-1', 'READWRITE')
        self.assertEqual(h.step('O2', redo=True).status, steps.PASSED, h.text())
        item = P.capture_for(h.ctx())['items'][0]
        self.assertIn('"sasl.password": "******"', item['definition'])
        self.assertIn("CREDENTIALS '<<CREDENTIALS>>'", item['definition'])
        self.assertTrue(item['redaction_marker'], 'flagged at the capture, so the workbook names it before the window')
        self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
        pass_steps(h, 'O7', 'O8')
        before = list(h.fake.writes)
        out = h.step('O9', redo=True, options={'env': {'S2REBUILD_KAFKA_CREDENTIALS': 'x'}})
        self.assertEqual(out.status, steps.REFUSED)
        self.assertIn('Pipeline p1 still carries a redaction marker in its definition', out.note)
        self.assertEqual(h.fake.writes, before, 'no statement reached the server')
        self.assertFalse(any(p.get('DATABASE_NAME') == TGT for p in h.fake.pipelines))

    def test_f14_a_masked_config_value_is_refused_by_the_notebook_block_before_any_statement(self):
        """The O9 notebook block (ownerlib run_block, credentials supplied) stops at a STOP ahead of USE for a masked CONFIG value.

        The fault points apply to the CREATE PIPELINE the STOP guards: a failure armed before it, one
        armed after it, and a journal crash before upsert_pipeline (the O9 journal write after the
        offsets compare). None fires on the notebook block, its rerun or the kit step: each refuses
        the same way and nothing reaches the server.
        """
        masked = ("CREATE PIPELINE `p1` AS LOAD DATA KAFKA 'b:9092/t' CONFIG '{\"security.protocol\": \"SASL_SSL\", "
                  "\"sasl.password\": \"******\"}' CREDENTIALS <CREDENTIALS REDACTED> INTO TABLE `t_single`")
        stop = 'pipeline p1 still carries a redaction marker in its definition; capture it again'
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = owner_harness(self)
                with_source_pipeline(h)
                h.fake.pipelines[0]['definition'] = masked
                h.journal.record_attachment('source', SRC, '1028600672070281646:7367158985892520834', 'cid-1', 'READWRITE')
                self.assertEqual(h.step('O2', redo=True).status, steps.PASSED, h.text())
                self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
                pass_steps(h, 'O7', 'O8')
                built = ownersql.block_for('O9', names_of(h), h.config, **ownersql.block_inputs(h.ctx()))
                text, block = built.render(), built.to_dict()
                self.assertLess(text.index('-- STOP: ' + stop), text.index('USE `%s`;' % TGT), 'the printed block stops ahead of USE')
                if point == 'journal':
                    fired = crash_journal(self, h.journal, 'upsert_pipeline')
                else:
                    fired = []
                    h.fake.fail_statement(r'^CREATE PIPELINE', point)
                before = list(h.fake.writes)
                for _ in range(2):
                    with contextlib.redirect_stdout(io.StringIO()):
                        result = ownerlib()['run_block'](h.fake.connect().cursor(), block, {'run_id': h.run_id},
                                                         credentials={'KAFKA': '{"username": "u"}'})
                    self.assertEqual((result['status'], result['statements']), ('REFUSED: ' + stop, 0))
                    self.assertEqual(h.fake.writes, before, 'no USE and no CREATE PIPELINE issued')
                out = h.step('O9', redo=True, options={'env': {'S2REBUILD_KAFKA_CREDENTIALS': 'x'}})
                self.assertEqual(out.status, steps.REFUSED)
                self.assertEqual(h.fake.writes, before)
                self.assertEqual((h.fake.faults_fired, fired), ([], []), 'the guarded statement and journal write are never reached')
                self.assertFalse(any(p.get('DATABASE_NAME') == TGT for p in h.fake.pipelines))


class HandlerTests(unittest.TestCase):
    def test_o2_o9_o11_c1_with_the_owner_profile(self):
        h = owner_harness(self)
        with_source_pipeline(h)
        h.journal.record_attachment('source', SRC, '1028600672070281646:7367158985892520834', 'cid-1', 'READWRITE')
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
        out_c1 = h.step('C1', redo=True, options={'sleep': advance, 'idle_workload': 'no application traffic in the test'})
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
        h.journal.record_attachment('source', SRC, '1028600672070281646:7367158985892520834', 'cid-1', 'READWRITE')
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

    def test_c1_needs_the_release_and_the_owner_for_pipelines_and_grants(self):
        h = Harness(self)
        pass_steps(h, 'O5', 'O6', 'O7', 'O8', 'O9', 'O10', 'O11')
        out = h.step('C1', redo=True)
        self.assertEqual(out.status, steps.REFUSED)
        self.assertIn('needs the release time', out.note)
        h.journal.set_meta('released_utc', '2026-11-01T07:00:00Z')
        out = h.step('C1', redo=True, options={'sleep': lambda s: None})
        self.assertEqual((out.status, out.exit_code), (steps.FAILED, 4))
        self.assertIn('pipelines and grants: not checked, no owner profile', out.note)
        ho = owner_harness(self)
        pass_steps(ho, 'O5', 'O6', 'O7', 'O8', 'O9', 'O10', 'O11')
        ho.journal.set_meta('released_utc', '2026-11-01T07:00:00Z')
        ho.fake.privileges += [{'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'INSERT'}]
        grants.record_reference(ho.journal, {"'app'@'%'": {'*.*': ['INSERT', 'UPDATE']}})
        out = ho.step('C1', redo=True, options={'sleep': lambda s: None})
        self.assertEqual(out.status, steps.FAILED)
        self.assertIn("grants differ from the snapshot: 'app'@'%'", out.note)

    def test_f9_c1_without_writes_or_idle_workload_fails(self):
        """No application write and no --idle-workload: C1 is FAILED with exit 4 and the flag is named. With the flag C1 passes
        and records the note as an idle_workload event. Without the owner profile the pipeline and grants checks are recorded
        as not checked (c1_not_checked event and the step note) and no longer hold C1 pending. step C1 --idle-workload reaches
        the handler."""
        h = released_for_c1(self)
        out = h.step('C1', redo=False, options={'sleep': lambda s: None})
        self.assertEqual((out.status, out.exit_code), (steps.FAILED, 4), h.text())
        self.assertIn('s2rebuild step C1 --idle-workload NOTE', out.note)
        self.assertIn('pipelines and grants: not checked, no owner profile', out.note)
        self.assertEqual((h.journal.step_status('C1'), h.journal.events('idle_workload')), ('failed', []))
        note = 'batch application, no writes until Monday'
        out = h.step('C1', redo=False, options={'sleep': lambda s: None, 'idle_workload': note})
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('no application writes seen; idle workload accepted by the operator: ' + note, out.note)
        self.assertIn('pipelines and grants: not checked, no owner profile', out.note)
        self.assertEqual(h.journal.events('idle_workload'), [note])
        self.assertEqual(h.journal.events('c1_not_checked'), ['pipelines and grants: not checked, no owner profile'] * 2)
        self.assertIsNone(h.journal.get_meta('app_writes_seen'))
        self.assertEqual(h.journal.step_status('C1'), 'passed')
        with mock.patch.object(steps, 'run_one', return_value=0) as run_one:
            code, text = run_cli(['step', 'C1', '--idle-workload', note, '--config', str(h.config_path), '--run', h.run_id], env=h.env)
        self.assertEqual(code, 0, text)
        self.assertEqual(run_one.call_args.kwargs['options']['idle_workload'], note)

    def test_f9_c1_fails_when_an_application_managed_pipeline_is_missing(self):
        """With the owner profile an application-managed pipeline absent from the live database fails C1, writes seen or not;
        once the application recreated it, C1 passes."""
        h = released_for_c1(self, owner=True)
        write_o2(h, [ITEM, APP])
        with_source_pipeline(h)
        h.fake.rows(SRC, 't_single').append({'id': 'new', 'payload': 'p', 'n': 1})
        out = h.step('C1', redo=False, options={'sleep': lambda s: None})
        self.assertEqual((out.status, out.exit_code), (steps.FAILED, 4), h.text())
        self.assertIn('application-managed pipelines present again: missing app_owned', out.note)
        self.assertIn('counts differ from the copy on t_single', out.note)
        h.fake.pipelines.append({'PIPELINE_NAME': 'app_owned', 'STATE': 'Running'})
        out = h.step('C1', redo=False, options={'sleep': lambda s: None})
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('application-managed pipelines present again: yes', out.note)

    def test_f9_c1_rerun_counts_the_application_writes_an_earlier_attempt_recorded(self):
        """A first C1 sees an application session doing updates (the counts stay) and fails on another check (an application-managed
        pipeline not back yet); it records app_writes_seen. The rerun samples no session and no count change: it used to fail 'saw no
        application writes' and point to --idle-workload against the journal. Now it passes, naming when the writes were seen, and the
        time stays the first one. Issues nothing (reads only)."""
        h = released_for_c1(self, owner=True)
        write_o2(h, [ITEM, APP])
        with_source_pipeline(h)
        h.fake.processlist.append({'ID': 51, 'USER': 'app', 'HOST': 'h:51', 'DB': SRC, 'COMMAND': 'Query', 'TIME': 1,
                                   'INFO': 'UPDATE t_single SET n = 2 WHERE id = 1'})
        out = h.step('C1', redo=False, options={'sleep': lambda s: None})
        self.assertEqual((out.status, out.exit_code), (steps.FAILED, 4), h.text())
        self.assertIn('application-managed pipelines present again: missing app_owned', out.note)
        self.assertIn('application writes seen on the live database (sessions)', out.note)
        seen = h.journal.get_meta('app_writes_seen')
        self.assertIsNotNone(seen)
        h.fake.processlist = []
        h.fake.pipelines.append({'PIPELINE_NAME': 'app_owned', 'STATE': 'Running'})
        out = h.step('C1', redo=False, options={'sleep': lambda s: None})
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('no application write in this sample; an earlier C1 attempt of this run saw application writes on the live '
                      'database at %s (app_writes_seen)' % seen, out.note)
        self.assertNotIn('--idle-workload', out.note)
        self.assertEqual((h.journal.get_meta('app_writes_seen'), h.journal.events('idle_workload')), (seen, []))

    def test_f9_c1_fault_points_around_a_count_sample_then_the_rerun_passes(self):
        """The three failure points around C1's evidence of application writes: a lost connection just before and just after
        the first count query on the live database, and a kill after the samples but before the app_writes_seen journal write.
        No attempt passes C1. Rerunning step C1 sees the writes again (the live count differs from the copy) and passes with
        app_writes_seen recorded."""
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = released_for_c1(self)
                h.fake.rows(SRC, 't_single').append({'id': 'new', 'payload': 'p', 'n': 1})
                if point == 'journal':
                    fired = crash_journal(self, h.journal, 'set_meta', match=lambda key, value: key == 'app_writes_seen')
                    expected = SimulatedCrash
                else:
                    h.fake.fail_statement(r'^SELECT COUNT\(\*\) AS n FROM `%s`\.' % SRC, point)
                    fired = h.fake.faults_fired
                    expected = FakeDisconnect
                with self.assertRaises(expected):
                    h.step('C1', redo=False, options={'sleep': lambda s: None})
                self.assertEqual(len(fired), 1)
                self.assertEqual(h.journal.step_status('C1'), 'failed')
                self.assertIsNone(h.journal.get_meta('app_writes_seen'))
                out = h.step('C1', redo=False, options={'sleep': lambda s: None})
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertIn('application writes seen on the live database (counts differ from the copy on t_single)', out.note)
                self.assertIsNotNone(h.journal.get_meta('app_writes_seen'))
                self.assertEqual(h.journal.events('idle_workload'), [])

    def test_the_runner_cannot_see_other_accounts_grants_or_pipelines(self):
        h = owner_harness(self)
        with_source_pipeline(h)
        h.fake.privileges.append({'GRANTEE': "'app'@'%'", 'PRIVILEGE_TYPE': 'INSERT'})
        runner = h.sessions.runner(read_guard())
        self.assertNotIn("'app'@'%'", grants.snapshot(runner, SRC, set()), 'the runner sees only its own grants')
        self.assertEqual(runner.query(inventory.PIPELINES_SQL, (SRC,)), [])
        owner = h.sessions.owner(read_guard())
        self.assertIn("'app'@'%'", grants.snapshot(owner, SRC, set()))


def stops(h):
    """Every STOP PIPELINE the fake applied."""
    return [w for w in h.fake.writes if w.startswith('STOP PIPELINE')]


def ready_for_o9(test):
    """An owner harness with O5 to O8 passed: ready for O9 once an O2 capture is written."""
    h = owner_harness(test)
    pass_steps(h, 'O5', 'O6', 'O7', 'O8')
    return h


def offset_row(database, partition, loaded, successful, earliest=1, latest=9):
    """One PIPELINES_OFFSETS row of p1 as the server returns it."""
    return {'DATABASE_NAME': database, 'PIPELINE_NAME': 'p1', 'SOURCE_TYPE': 'KAFKA', 'SOURCE_PARTITION_ID': partition, 'EARLIEST_OFFSET': earliest,
            'LATEST_OFFSET': latest, 'LATEST_LOADED_OFFSET': loaded, 'LATEST_SUCCESSFUL_OFFSET': successful}


class CommittedOffsetTests(unittest.TestCase):
    """F10: the replay position is LATEST_SUCCESSFUL_OFFSET, compared partition by partition on the kit and notebook routes."""

    def test_f10_error_state_pipeline_is_stopped_and_captured(self):
        """A pipeline in state Error is stopped (the STOP is skipped only for a Stopped one) and captured Stopped, on the kit route
        and in the notebook O2 cell; a Stopped pipeline is still skipped."""
        h = owner_harness(self)
        with_source_pipeline(h, state='Error')
        out = h.step('O2', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual(stops(h), ['STOP PIPELINE `%s`.`p1`' % SRC])
        cap = P.capture_for(h.ctx())
        self.assertEqual((cap['step'], [i['state'] for i in cap['items']]), ('O2', ['Stopped']))
        h.fake.pipelines[0]['STATE'] = 'Error'
        result = run_dict(h, block_now(h, 'O2'))
        self.assertEqual((result['status'], result['statements'], result['skipped']), ('passed', 1, []))
        self.assertEqual([s['STATE'] for s in result['capture']['states']], ['Stopped'])
        result = run_dict(h, block_now(h, 'O2'))
        self.assertEqual((result['status'], result['statements'], result['skipped']), ('passed', 0, ['stop pipeline p1']))

    def test_f10_loaded_ahead_of_successful_after_the_stop_refuses_o2(self):
        """After the stop p1 partition 0 shows LATEST_LOADED_OFFSET 7 and LATEST_SUCCESSFUL_OFFSET 5 (an uncommitted batch). The kit
        route refuses O2 with the partition listed and writes no O2 capture; the notebook O2 result is refused at its import
        (gate O2 --evidence). Once the two agree, the rerun skips the stopped pipeline and passes."""
        h = owner_harness(self)
        with_source_pipeline(h, loaded=7, latest=7)
        h.fake.pipeline_offsets[0]['LATEST_SUCCESSFUL_OFFSET'] = 5
        out = h.step('O2', redo=True)
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('LATEST_LOADED_OFFSET differs from LATEST_SUCCESSFUL_OFFSET after the stop on p1 partition 0 (loaded 7, successful 5)', out.note)
        self.assertNotEqual(P.capture_for(h.ctx())['step'], 'O2')
        self.assertEqual(h.journal.offsets('capture'), [])
        result = run_dict(h, block_now(h, 'O2'))
        self.assertEqual(result['status'], 'passed')
        with self.assertRaisesRegex(KitError, r'after the stop on p1 partition 0 \(loaded 7, successful 5\)'):
            P.import_capture(h.ctx(), result['capture'])
        self.assertNotEqual(P.capture_for(h.ctx())['step'], 'O2')
        h.fake.pipeline_offsets[0]['LATEST_SUCCESSFUL_OFFSET'] = 7
        out = h.step('O2', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual(len(stops(h)), 1, 'the reruns skip the stopped pipeline')
        self.assertEqual([(o['loaded'], o['successful']) for o in h.journal.offsets('capture')], [(7, 7)])

    def test_f10_null_successful_beside_a_loaded_offset_refuses_o2(self):
        """After the stop p1 partition 0 shows LATEST_LOADED_OFFSET 7 and LATEST_SUCCESSFUL_OFFSET NULL (a first batch extracted and
        never committed). The kit route refuses O2 with the partition listed and writes no O2 capture; the notebook O2 result is
        refused at its import (gate O2 --evidence). With both NULL the kit route and the import pass. The three failure points
        around STOP PIPELINE, the statement this refusal follows, are covered by test_f10_stop_pipeline_fault_points_then_the_rerun_passes."""
        h = owner_harness(self)
        with_source_pipeline(h, loaded=7, latest=7)
        h.fake.pipeline_offsets[0]['LATEST_SUCCESSFUL_OFFSET'] = None
        out = h.step('O2', redo=True)
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('after the stop on p1 partition 0 (loaded 7, successful NULL)', out.note)
        self.assertNotEqual(P.capture_for(h.ctx())['step'], 'O2')
        self.assertEqual(h.journal.offsets('capture'), [])
        result = run_dict(h, block_now(h, 'O2'))
        self.assertEqual(result['status'], 'passed')
        with self.assertRaisesRegex(KitError, r'after the stop on p1 partition 0 \(loaded 7, successful NULL\)'):
            P.import_capture(h.ctx(), result['capture'])
        self.assertNotEqual(P.capture_for(h.ctx())['step'], 'O2')
        self.assertEqual(h.journal.offsets('capture'), [])
        h.fake.pipeline_offsets[0]['LATEST_LOADED_OFFSET'] = None
        out = h.step('O2', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual(len(stops(h)), 1, 'the reruns skip the stopped pipeline')
        self.assertEqual([(o['loaded'], o['successful']) for o in h.journal.offsets('capture')], [(None, None)])
        result = run_dict(h, block_now(h, 'O2'))
        P.import_capture(h.ctx(), result['capture'])
        self.assertEqual(P.capture_for(h.ctx())['step'], 'O2')

    def test_f10_swapped_partition_offsets_refused_by_the_block_and_the_after_hook(self):
        """The O2 capture records p1 partition 0 at 5 and partition 1 at 9; the replay sets them swapped, so the partition count and
        the summed offset still match. The kit route stops at the partition 0 check, the notebook O9 cell stops at the same check,
        and the after-hook compare (offsets_equal) lists both partitions."""
        h = ready_for_o9(self)
        swapped = dict(ITEM, alter_statements=["ALTER PIPELINE `p1` SET OFFSETS '{\"0\": 9, \"1\": 5}'"],
                       offsets=[{'partition': '0', 'earliest': 1, 'latest': 9, 'loaded': 5, 'successful': 5},
                                {'partition': '1', 'earliest': 1, 'latest': 9, 'loaded': 9, 'successful': 9}])
        write_o2(h, [swapped])
        out = h.step('O9', redo=True, options={'env': {'S2REBUILD_KAFKA_CREDENTIALS': 'x'}})
        self.assertEqual(out.status, steps.REFUSED, h.text())
        check = 'offset of p1 partition 0 equals the O2 capture (LATEST_SUCCESSFUL_OFFSET)'
        self.assertIn('stopped at check "%s": expected successful = 5, observed successful = 9' % check, out.note)
        self.assertEqual(h.journal.offsets('before_start'), [])
        result = run_dict(h, block_now(h, 'O9'))
        self.assertEqual(result['status'], 'FAILED at check ' + check)
        owner = h.sessions.owner(read_guard())
        self.assertEqual(P.offsets_equal(owner, TGT, [swapped]), [('p1', '0', 5, 9), ('p1', '1', 9, 5)])

    def test_f10_compare_and_retention_use_successful_falling_back_to_loaded_only_when_null(self):
        """A partition captured with loaded 9 and successful 5: O9 compares and O11 checks retention against 5, on the kit route
        (offsets_equal, retention_gaps) and in the block checks. A partition whose captured successful offset is NULL falls back to
        loaded, and its block check says so."""
        names = {'source': SRC, 'replacement': TGT}
        ahead = dict(ITEM, offsets=[{'partition': '0', 'earliest': 1, 'latest': 9, 'loaded': 9, 'successful': 5}])
        null = dict(ITEM, offsets=[{'partition': '0', 'earliest': 1, 'latest': 9, 'loaded': 9, 'successful': None}])
        self.assertIn('offset of p1 partition 0 equals the O2 capture (LATEST_SUCCESSFUL_OFFSET); expect successful = 5',
                      P.replay_block(names, [ahead]).render())
        self.assertIn('(LATEST_LOADED_OFFSET: the capture holds no successful offset); expect loaded = 9', P.verify_block(names, [null]).render())
        self.assertIn("SOURCE_PARTITION_ID = '0' AND EARLIEST_OFFSET > '5'", P.release_block(names, [ahead]).render())
        h = owner_harness(self)
        h.fake.pipeline_offsets = [offset_row(SRC, '0', loaded=9, successful=9, earliest=7), offset_row(TGT, '0', loaded=9, successful=5)]
        owner = h.sessions.owner(read_guard())
        self.assertEqual(P.offsets_equal(owner, TGT, [ahead]), [])
        self.assertEqual(P.offsets_equal(owner, SRC, [ahead]), [('p1', '0', 5, 9)])
        self.assertEqual(P.offsets_equal(owner, TGT, [null]), [], 'the fallback compares loaded with loaded')
        self.assertEqual(P.retention_gaps(owner, SRC, [ahead]), [('p1', '0', 5, 7)])
        self.assertEqual(P.retention_gaps(owner, SRC, [null]), [])

    def test_f10_stop_pipeline_fault_points_then_the_rerun_passes(self):
        """The three failure points around STOP PIPELINE at O2: a lost connection before the server sees it, the STOP applied with
        the reply lost, and a kill after the stop before the capture is journaled. No attempt passes O2. The rerun stops p1 only
        while it still runs (after the lost reply it skips the Stopped pipeline instead of failing on 'already Stopped'), then
        captures and passes."""
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = owner_harness(self)
                with_source_pipeline(h)
                if point == 'journal':
                    fired = crash_journal(self, h.journal, 'upsert_pipeline')
                    with self.assertRaises(SimulatedCrash):
                        h.step('O2', redo=True)
                else:
                    h.fake.fail_statement(r'^STOP PIPELINE', point)
                    fired = h.fake.faults_fired
                    self.assertEqual(h.step('O2', redo=True).status, steps.REFUSED)
                self.assertEqual(len(fired), 1)
                self.assertNotEqual(h.journal.step_status('O2'), 'passed')
                self.assertEqual(h.journal.offsets('capture'), [])
                out = h.step('O2', redo=True)
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertEqual(len(stops(h)), 1, 'STOP PIPELINE is applied exactly once')
                self.assertEqual(h.journal.events('owner_skipped'), [] if point == 'before' else ['O2 stop pipeline p1'])
                self.assertEqual([(o['pipeline_name'], o['loaded'], o['successful']) for o in h.journal.offsets('capture')], [('p1', 5, 5)])

    def test_f10_o9_create_and_set_offsets_fault_points_then_the_rerun_passes(self):
        """The three failure points around CREATE PIPELINE and around ALTER PIPELINE ... SET OFFSETS at O9: before the server sees
        the statement, applied with the reply lost, and a kill after it before its owner_statement journal event. No attempt passes
        O9 and no log line or journal event carries the credential. The rerun skips the existing pipeline, sets the offsets again
        and passes with one pipeline in the replacement at the captured committed offset."""
        env = {'S2REBUILD_KAFKA_CREDENTIALS': '{"sasl.password": "f10-secret"}'}
        for pattern, label in ((r'^CREATE PIPELINE', 'create pipeline p1'), (r'^ALTER PIPELINE `p1` SET OFFSETS', 'set offsets of p1')):
            for point in FAULT_POINTS:
                with self.subTest(statement=label, point=point):
                    h = ready_for_o9(self)
                    write_o2(h, [ITEM])
                    if point == 'journal':
                        fired = crash_journal(self, h.journal, 'event', match=lambda kind, detail, label=label: kind == 'owner_statement' and label in detail)
                        with self.assertRaises(SimulatedCrash):
                            h.step('O9', redo=True, options={'env': env})
                    else:
                        h.fake.fail_statement(pattern, point)
                        fired = h.fake.faults_fired
                        self.assertEqual(h.step('O9', redo=True, options={'env': env}).status, steps.REFUSED)
                    self.assertEqual(len(fired), 1)
                    self.assertNotEqual(h.journal.step_status('O9'), 'passed')
                    out = h.step('O9', redo=True, options={'env': env})
                    self.assertEqual(out.status, steps.PASSED, h.text())
                    self.assertEqual(len([w for w in h.fake.writes if w.startswith('CREATE PIPELINE')]), 1, 'one CREATE reached the server')
                    self.assertEqual([p['PIPELINE_NAME'] for p in h.fake.pipelines if p.get('DATABASE_NAME') == TGT], ['p1'])
                    self.assertEqual([(o['source_partition'], o['successful']) for o in h.journal.offsets('before_start')], [('0', 5)])
                    self.assertNotIn('f10-secret', h.text())
                    self.assertNotIn('f10-secret', json.dumps(h.journal.events()))


class ReleaseBoundaryTests(unittest.TestCase):
    """F1: O11 records release_started_utc before its first statement, so a failed release never leaves the rollback open."""

    def assert_rollback_refused(self, h, reason='O11 started releasing writers on the live name'):
        before = list(h.fake.writes)
        with self.assertRaisesRegex(KitError, 'Rollback refused: ' + reason):
            machine_for(h)[0].rollback()
        self.assertTrue(ownersql.block_inputs(h.ctx())['released'])
        result = run_rollback_block(h)
        self.assertTrue(result['status'].startswith('REFUSED: rollback refused'), result['status'])
        self.assertEqual(result['statements'], 0)
        self.assertEqual(h.fake.writes, before, 'neither route issued a statement')

    def test_f1_rollback_refused_after_the_first_start_pipeline_failed(self):
        """START PIPELINE applied, reply lost: O11 is not passed and released_utc is unset, yet the rollback is refused."""
        h = switched_for_release(self)
        h.fake.fail_statement(r'^START PIPELINE', 'after')
        out = h.step('O11', redo=True)
        self.assertEqual(out.status, steps.REFUSED)
        self.assertEqual(h.journal.step_status('O11'), 'pending')
        self.assertIsNone(h.journal.get_meta('released_utc'))
        self.assertIsNotNone(h.journal.get_meta('release_started_utc'))
        self.assertEqual(live_state(h), ['Running'], 'the pipeline writes to the live name')
        self.assert_rollback_refused(h)

    def test_f1_start_pipeline_fault_points_resume_and_keep_the_rollback_refused(self):
        """Before, after and before-the-journal around the first START PIPELINE: rollback refused; the rerun starts it exactly once."""
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = switched_for_release(self)
                if point == 'journal':
                    crash_journal(self, h.journal, 'event', match=lambda kind, detail: kind == 'owner_statement' and 'start pipeline p1' in detail)
                    with self.assertRaises(SimulatedCrash):
                        h.step('O11', redo=True)
                    self.assertEqual(h.journal.step_status('O11'), 'failed', 'never left running')
                else:
                    h.fake.fail_statement(r'^START PIPELINE', point)
                    self.assertEqual(h.step('O11', redo=True).status, steps.REFUSED)
                self.assertIsNotNone(h.journal.get_meta('release_started_utc'))
                self.assertIsNone(h.journal.get_meta('released_utc'))
                self.assertEqual(len(starts(h)), 0 if point == 'before' else 1)
                self.assert_rollback_refused(h)
                out = h.step('O11', redo=True)
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertEqual(len(starts(h)), 1, 'START PIPELINE is issued exactly once')
                self.assertEqual(live_state(h), ['Running'])
                self.assertIsNotNone(h.journal.get_meta('released_utc'))

    def test_f1_crash_before_released_utc_still_refuses_rollback(self):
        """The whole block ran and the kit died before released_utc: release_started_utc keeps the rollback refused."""
        h = switched_for_release(self)
        crash_journal(self, h.journal, 'set_meta', match=lambda key, value: key == 'released_utc')
        with self.assertRaises(SimulatedCrash):
            h.step('O11', redo=True)
        self.assertEqual(h.journal.step_status('O11'), 'failed')
        self.assertIsNone(h.journal.get_meta('released_utc'))
        self.assert_rollback_refused(h)

    def test_f1_release_started_utc_is_written_before_the_first_release_statement(self):
        """A crash on the release_started_utc write itself: no release statement ran, so the rollback runs; print and dry run never mark it."""
        h = switched_for_release(self)
        crash_journal(self, h.journal, 'set_meta', match=lambda key, value: key == 'release_started_utc')
        with self.assertRaises(SimulatedCrash):
            h.step('O11', redo=True)
        self.assertFalse(any(w.startswith(('START PIPELINE', 'GRANT', 'SET GLOBAL')) for w in h.fake.writes), 'no release statement ran')
        self.assertEqual(h.journal.step_status('O11'), 'failed')
        self.assertIsNone(h.journal.get_meta('release_started_utc'))
        self.assertFalse(ownersql.block_inputs(h.ctx())['released'])
        self.assertEqual(machine_for(h)[0].rollback(), switch.ROLLED_BACK, 'nothing was released, so the rollback runs')
        self.assertEqual(h.fake.attachment(SRC)['STORAGE_ID'], '1028600672070281646:7367158985892520834')
        hp = Harness(self)
        write_o2(hp, [ITEM])
        pass_steps(hp, 'O5', 'O6', 'O7', 'O8', 'O9', 'O10')
        self.assertEqual(hp.step('O11', redo=True).status, steps.PENDING, 'print mode: the block waits for the gate')
        self.assertIsNone(hp.journal.get_meta('release_started_utc'))
        ho = switched_for_release(self)
        self.assertEqual(ho.step('O11', redo=True, dry_run=True).status, steps.PENDING)
        self.assertIsNone(ho.journal.get_meta('release_started_utc'))
        self.assertEqual(starts(ho), [])

    def stop_o11_before_its_first_statement(self, case):
        """Run O11 on a fresh harness that stops in one of EARLY_STOPS, before any statement; assert it released nothing.

        check before, check after: the block's first check (every captured partition present)
        loses its connection before or after the server answered. check journal: the kill -9
        stand-in at the journal event of that check. owner session: the block's owner session
        cannot connect (armed by the kit's own retention read just ahead of it). check
        expectation: the retention moves on between the kit's retention read and the block's own
        retention check, so that check fails its expectation.
        """
        h = switched_for_release(self)
        expected = FakeDisconnect
        if case == 'check journal':
            crash_journal(self, h.journal, 'event', match=lambda kind, detail: kind == 'owner_check' and 'every captured partition' in detail)
            expected = SimulatedCrash
        elif case == 'owner session':
            h.fake.change_before(RETENTION_READ, lambda db: db.fail_connect('ops_owner'))
        elif case == 'check expectation':
            h.fake.change_before(RETENTION_CHECK, lambda db: [o.update(EARLIEST_OFFSET=9) for o in db.pipeline_offsets])
            expected = None
        else:
            h.fake.fail_statement(PARTITION_CHECK, case.split()[1])
        writes = list(h.fake.writes)
        if expected is None:
            out = h.step('O11', redo=True)
            self.assertEqual(out.status, steps.REFUSED)
            self.assertIn('no retention gap on p1', out.note)
        else:
            with self.assertRaises(expected):
                h.step('O11', redo=True)
        if case in ('check before', 'check after', 'owner session'):
            self.assertEqual(len(h.fake.faults_fired), 1, 'the failure fired')
        self.assertIn(h.journal.step_status('O11'), ('failed', 'pending'), 'O11 is neither passed nor left running')
        self.assertEqual(h.fake.writes, writes, 'O11 issued no statement')
        self.assertIsNone(h.journal.get_meta('release_started_utc'))
        self.assertIsNone(h.journal.get_meta('released_utc'))
        self.assertIsNone(switch.rollback_refusal(h.journal))
        self.assertFalse(ownersql.block_inputs(h.ctx())['released'])
        return h

    def test_f1_o11_stopped_before_its_first_statement_leaves_the_rollback_open(self):
        """An O11 that stopped before its first statement records no release_started_utc: both rollback routes run, and the rerun releases once.

        The release_started_utc write guards the first statement (START PIPELINE p1 here), so the
        three points sit on the block's first check ahead of it: before, after and its journal
        event; plus an owner session that cannot open and a check that fails its expectation. The
        machine's proof passes and the rollback block through the retained copy runs R1 and R2b. The rerun of O11 starts p1
        once and stamps both times; after a retention gap it fails at the kit's retention check
        instead, and the rollback stays open.
        """
        for case in EARLY_STOPS:
            with self.subTest(case=case):
                h = self.stop_o11_before_its_first_statement(case)
                before = list(h.fake.writes)
                machine_for(h)[0].zero_change_proof()
                self.assertEqual(h.fake.writes, before, 'the proof reads only')
                result = run_rollback_block(h, retained=True)
                self.assertEqual((result['status'], result['statements']), ('passed', 2), 'nothing was released, so the rollback runs')
                self.assertEqual((h.fake.attachment(SRC)['STORAGE_ID'], h.fake.attachment(SRC)['ATTACH_TYPE']), ('1028600672070281646:7367158985892520834', 'READWRITE'))
                h = self.stop_o11_before_its_first_statement(case)
                out = h.step('O11', redo=True)
                if case == 'check expectation':
                    self.assertEqual(out.status, steps.FAILED)
                    self.assertIn('retention gap or missing partition', out.note)
                    self.assertEqual(starts(h), [])
                    self.assertIsNone(h.journal.get_meta('release_started_utc'))
                    self.assertIsNone(switch.rollback_refusal(h.journal), 'a retention gap keeps the rollback open')
                    continue
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertEqual(len(starts(h)), 1, 'START PIPELINE is issued exactly once')
                self.assertEqual(live_state(h), ['Running'])
                self.assertIsNotNone(h.journal.get_meta('release_started_utc'))
                self.assertIsNotNone(h.journal.get_meta('released_utc'))

    def test_f1_stale_rollback_block_refused_once_a_notebook_o11_started_a_pipeline(self):
        """The rollback block written at O10, run after a notebook O11 the journal knows nothing about: the live pipeline check refuses it.

        before: the server never saw START, so nothing was released and the stale block rolls
        back. after: START applied, reply lost. journal: START applied and the notebook died
        before its result and the gate record (kill -9 stand-in). Both routes then refuse with
        nothing issued, without release_started_utc.
        """
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = switched_for_release(self)
                stale = block_now(h, 'O10', rollback=True)
                h.fake.fail_statement(r'^START PIPELINE', 'after' if point == 'journal' else point, crash=(point == 'journal'))
                if point == 'journal':
                    with self.assertRaises(SimulatedCrash):
                        run_dict(h, block_now(h, 'O11'))
                else:
                    self.assertTrue(run_dict(h, block_now(h, 'O11'))['status'].startswith('ERROR'))
                self.assertIsNone(h.journal.get_meta('release_started_utc'), 'the notebook route writes nothing to the journal')
                self.assertFalse(ownersql.block_inputs(h.ctx())['released'])
                if point == 'before':
                    self.assertEqual(live_state(h), ['Stopped'])
                    result = run_dict(h, stale)
                    self.assertEqual((result['status'], result['statements']), ('passed', 2), 'nothing was released, so the rollback runs (R1, R2b)')
                    self.assertEqual(h.fake.attachment(SRC)['STORAGE_ID'], '1028600672070281646:7367158985892520834')
                    continue
                self.assertEqual(live_state(h), ['Running'])
                before = list(h.fake.writes)
                result = run_dict(h, stale)
                self.assertEqual((result['status'], result['statements']), (PIPELINE_PROOF, 0))
                self.assertEqual(run_rollback_block(h)['status'], PIPELINE_PROOF, 'a block generated now refuses the same way')
                with self.assertRaisesRegex(KitError, 'reconcile first.*1 pipeline Running on the live name'):
                    machine_for(h)[0].rollback()
                self.assertEqual(h.fake.writes, before, 'neither route issued a statement')

    def test_f1_kit_o11_fault_points_refused_by_the_live_checks_without_release_started_utc(self):
        """Each failure point of the first START, then release_started_utc removed: the machine and the stale block refuse on the live state alone."""
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = switched_for_release(self)
                stale = block_now(h, 'O10', rollback=True)
                if point == 'journal':
                    crash_journal(self, h.journal, 'event', match=lambda kind, detail: kind == 'owner_statement' and 'start pipeline p1' in detail)
                    with self.assertRaises(SimulatedCrash):
                        h.step('O11', redo=True)
                else:
                    h.fake.fail_statement(r'^START PIPELINE', point)
                    self.assertEqual(h.step('O11', redo=True).status, steps.REFUSED)
                h.journal.conn.execute("DELETE FROM meta WHERE key = 'release_started_utc'")
                self.assertIsNone(switch.rollback_refusal(h.journal), 'the journal alone no longer refuses')
                if point == 'before':
                    self.assertEqual(machine_for(h)[0].rollback(), switch.ROLLED_BACK, 'START never reached the server')
                    continue
                before = list(h.fake.writes)
                with self.assertRaisesRegex(KitError, 'reconcile first.*1 pipeline Running on the live name'):
                    machine_for(h)[0].rollback()
                self.assertEqual(run_dict(h, stale)['status'], PIPELINE_PROOF)
                self.assertEqual(h.fake.writes, before)

    def test_f1_a_killed_o11_left_running_refuses_the_rollback_on_both_routes(self):
        """A real kill -9 leaves O11 running with no step_end: the kit and the notebook routes refuse; the rerun of O11 starts p1 once.

        Direct: step_start with no step_end and no release_started_utc. Fault points: a kill -9
        before or after the first START, or after it before its journal event, with the step_end
        write killed too, so run_step cannot record the step failed.
        """
        h = switched_for_release(self)
        h.journal.step_start('O11')
        self.assertIsNone(h.journal.get_meta('release_started_utc'))
        self.assert_rollback_refused(h, 'O11 released writers on the live name')
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = switched_for_release(self)
                crash_journal(self, h.journal, 'step_end', match=lambda step_id, status, *rest, **kw: step_id == 'O11' and status == 'failed')
                if point == 'journal':
                    crash_journal(self, h.journal, 'event', match=lambda kind, detail: kind == 'owner_statement' and 'start pipeline p1' in detail)
                else:
                    h.fake.fail_statement(r'^START PIPELINE', point, crash=True)
                with self.assertRaises(SimulatedCrash):
                    h.step('O11', redo=True)
                self.assertEqual(h.journal.step_status('O11'), 'running', 'a real kill -9 leaves the step running')
                self.assert_rollback_refused(h, 'O11 released writers on the live name')
                self.assertEqual(h.step('O11', redo=True).status, steps.PASSED, h.text())
                self.assertEqual(len(starts(h)), 1, 'START PIPELINE is issued exactly once')
                self.assertIsNotNone(h.journal.get_meta('released_utc'))


O3_EVENTS = {'DETACH': 'O3 detach the source', 'ATTACH': 'O3 attach the same storage read only'}


def o3_harness(test, owner=True):
    """A Path A harness with the source STORAGE_ID (1028600672070281646:7367158985892520834) recorded, with or without the owner profile."""
    h = owner_harness(test) if owner else Harness(test)
    h.journal.record_attachment('source', SRC, '1028600672070281646:7367158985892520834', 'cid-1', 'READWRITE', record_type='create',
                                attach_syntax='ATTACH DATABASE `%s` STORAGEID 1028600672070281646:7367158985892520834 COMPUTEID cid-1' % SRC, is_detached=0)
    return h


def fail_o3_at(test, h, point, word):
    """Run O3 once with a failure at point around its DETACH or ATTACH (word); returns the Outcome, or None after a crash.

    'before' kills the process just before the ATTACH reaches the server (the DETACH runs on the
    escalation worker thread, where a simulated kill would end only that thread, so a DETACH
    loses its connection instead); 'after' loses the reply once the server applied the statement;
    'journal' kills the process after the statement and before the owner_statement event.
    """
    if point == 'before':
        h.fake.fail_statement(r'^%s DATABASE' % word, 'before', crash=(word == 'ATTACH'))
    elif point == 'after':
        h.fake.fail_statement(r'^%s DATABASE' % word, 'after')
    else:
        crash_journal(test, h.journal, 'event', match=lambda kind, detail: kind == 'owner_statement' and detail.startswith(O3_EVENTS[word]))
    try:
        out = h.step('O3', redo=True)
    except SimulatedCrash:
        return None
    test.assertNotEqual(out.status, steps.PASSED, h.text())
    return out


BRANCH_READ = r'FROM information_schema\.MV_CLOUD_BRANCHES'
O3_STOP_WORD = {'before': 'ATTACH', 'after': 'DETACH', 'journal': 'DETACH'}


def foreign_source(storage_id='sid-other', attach_type='READWRITE'):
    """A database attached under the source name on another STORAGE_ID (another actor's database)."""
    return {'DATABASE_NAME': SRC, 'ATTACH_TYPE': attach_type, 'STORAGE_ID': storage_id, 'COMPUTE_ID': 'cid-1', 'ATTACH_SYNTAX': None}


def o3_writes(h, word):
    """Every DETACH or ATTACH (word) the fake applied."""
    return [w for w in h.fake.writes if w.startswith(word + ' DATABASE')]


def assert_between_detach_and_attach(test, h):
    """The source storage is detached, no database carries the source name, no ATTACH was applied and freeze_time is unset."""
    test.assertIsNone(h.fake.attachment(SRC))
    test.assertEqual([b['IS_DETACHED'] for b in h.fake.branches if b['STORAGE_ID'] == '1028600672070281646:7367158985892520834'], [1])
    test.assertEqual(o3_writes(h, 'ATTACH'), [])
    test.assertIsNone(h.journal.get_meta('freeze_time'))


def assert_frozen_once(test, h):
    """READONLY on 1028600672070281646:7367158985892520834 after exactly one DETACH and one ATTACH; the probe ran and was refused; freeze_time is recorded."""
    att = h.fake.attachment(SRC)
    test.assertEqual((att['ATTACH_TYPE'], att['STORAGE_ID']), ('READONLY', '1028600672070281646:7367158985892520834'))
    test.assertEqual(o3_writes(h, 'DETACH'), ['DETACH DATABASE `%s`' % SRC], 'DETACH is issued exactly once')
    test.assertEqual(o3_writes(h, 'ATTACH'), ["ATTACH DATABASE `%s` READ ONLY STORAGEID 1028600672070281646:7367158985892520834" % SRC], 'ATTACH is issued exactly once')
    test.assertTrue(any(w.startswith("DELETE FROM `%s`.`t_comp` WHERE `a` = 's2rebuild-probe-" % SRC) for w in h.fake.writes), 'the probe ran')
    test.assertIn('refused as read only', h.text())
    test.assertIsNotNone(h.journal.get_meta('freeze_time'))
    test.assertEqual(h.journal.attachment('source')['attach_type'], 'READONLY')


def run_o3_cell(h):
    """Generate the owner notebook from the journal as it is now, run cell 1 and the O3 cell on the fake; returns the O3 result."""
    names, blocks, inputs = notebook.blocks_for_run(h.ctx())
    path = notebook.generate(h.tmp / 'owner-steps.ipynb', names, h.config, blocks, items=inputs['pipelines'],
                             capture_step=inputs['capture_step'])
    cells = [''.join(c['source']) for c in json.loads(path.read_text())['cells'] if c['cell_type'] == 'code']
    namespace = {'cur': h.fake.connect('ops_owner').cursor(), 'connect': lambda: h.fake.connect('ops_owner')}
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile(cells[0], 'cell1', 'exec'), namespace)
        exec(compile(next(c for c in cells if c.startswith('# O3 freeze')), 'cell-O3', 'exec'), namespace)
    return namespace['result']


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
        h.journal.record_attachment('source', SRC, '1028600672070281646:7367158985892520834', 'cid-1', 'READWRITE')
        out = h.step('O3', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertEqual(h.fake.attachment(SRC)['ATTACH_TYPE'], 'READONLY')
        self.assertIsNotNone(h.journal.get_meta('freeze_time'))
        writes = [w for w in h.fake.writes if w.startswith(('DETACH', 'ATTACH', 'DELETE'))]
        self.assertEqual(writes[0], 'DETACH DATABASE `%s`' % SRC)
        self.assertTrue(writes[1].startswith("ATTACH DATABASE `%s` READ ONLY STORAGEID 1028600672070281646:7367158985892520834" % SRC))
        self.assertTrue(writes[2].startswith("DELETE FROM `%s`.`t_comp` WHERE `a` = 's2rebuild-probe-" % SRC))
        self.assertEqual(grants.reference_from_journal(h.journal), {"'app'@'%'": {'*.*': ['INSERT']}}, 'Path A keeps a grants reference for C1')

    def test_o3_resumes_after_the_read_only_attachment_without_the_owner(self):
        h = Harness(self)
        h.fake.attachment(SRC)['ATTACH_TYPE'] = 'READONLY'
        h.journal.record_attachment('source', SRC, '1028600672070281646:7367158985892520834', 'cid-1', 'READWRITE')
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
        self.assertIn('source not recorded', out.note)
        self.assertFalse(any(w.startswith('DETACH') for w in h.fake.writes))
        no_owner = Harness(self)
        out = no_owner.step('O3', redo=True)
        self.assertEqual(out.status, steps.PENDING)
        self.assertIn('DETACH DATABASE `%s`;' % SRC, no_owner.text())

    def test_o3_detach_escalation_through_the_admin_block(self):
        h = owner_harness(self, mutate=lambda d: d.setdefault('freeze', {}).update(detach_wait_seconds=1))
        h.journal.record_attachment('source', SRC, '1028600672070281646:7367158985892520834', 'cid-1', 'READWRITE')
        h.fake.detach_blocks.add(SRC)
        out = h.step('O3', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('DETACH DATABASE `%s` KILL' % SRC, h.fake.writes)
        self.assertEqual(h.fake.attachment(SRC)['ATTACH_TYPE'], 'READONLY')

    def test_f2_rerun_after_a_kill_between_detach_and_attach_issues_one_attach(self):
        """Stopped between DETACH and ATTACH (killed before the ATTACH, DETACH reply lost, or killed before the DETACH event): the rerun starts at the ATTACH.

        The rerun prints and runs the re-entry block, never DETACH again, and ends READONLY on
        the recorded STORAGE_ID with the probe refused and freeze_time recorded. A kill just
        before the DETACH leaves the source READWRITE, and the rerun runs the whole block.
        """
        word = {'before': 'ATTACH', 'after': 'DETACH', 'journal': 'DETACH'}
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = o3_harness(self)
                fail_o3_at(self, h, point, word[point])
                assert_between_detach_and_attach(self, h)
                out = h.step('O3', redo=True)
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertIn('O3 resumed at the ATTACH', out.note)
                assert_frozen_once(self, h)
                printed = ownersql.owner_sql_path(h.ctx(), 'O3', variant='reentry').read_text()
                self.assertIn('-- O3: Freeze: re-entry between DETACH and ATTACH (Path A)', printed)
                self.assertNotIn('DETACH DATABASE `%s`;' % SRC, printed, 'the printed block starts at the ATTACH')
                self.assertIn('-- O3: Freeze: read-only attachment (Path A)', ownersql.owner_sql_path(h.ctx(), 'O3').read_text(),
                              'O3.sql keeps the whole block; the re-entry block has its own file')
        h = o3_harness(self)
        fail_o3_at(self, h, 'before', 'DETACH')
        self.assertEqual(h.fake.attachment(SRC)['ATTACH_TYPE'], 'READWRITE', 'the server never saw the DETACH')
        self.assertEqual(h.step('O3', redo=True).status, steps.PASSED, h.text())
        assert_frozen_once(self, h)

    def test_f2_failure_at_the_reentry_attach_never_issues_a_second_attach(self):
        """The re-entry block's ATTACH fails before it, after it (reply lost) or before its journal event; the next rerun still ends with one ATTACH in total.

        Before: the source stays detached and the next rerun starts at the ATTACH again. After and
        journal: the source is READONLY and the next rerun resumes at the probe.
        """
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = o3_harness(self)
                fail_o3_at(self, h, 'after', 'DETACH')
                fail_o3_at(self, h, point, 'ATTACH')
                if point == 'before':
                    assert_between_detach_and_attach(self, h)
                else:
                    self.assertEqual(h.fake.attachment(SRC)['ATTACH_TYPE'], 'READONLY')
                    self.assertIsNone(h.journal.get_meta('freeze_time'))
                out = h.step('O3', redo=True)
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertIn('O3 resumed', out.note)
                assert_frozen_once(self, h)

    def test_f2_rerun_with_another_database_under_the_source_name_refuses_and_issues_nothing(self):
        """After the stop between DETACH and ATTACH, a database under the source name on another STORAGE_ID refuses O3 (READWRITE or READONLY); nothing is issued."""
        for attach_type in ('READWRITE', 'READONLY'):
            with self.subTest(attach_type=attach_type):
                h = o3_harness(self)
                fail_o3_at(self, h, 'after', 'DETACH')
                h.fake.attachments.append({'DATABASE_NAME': SRC, 'ATTACH_TYPE': attach_type, 'STORAGE_ID': 'sid-other', 'COMPUTE_ID': 'cid-1',
                                           'ATTACH_SYNTAX': None})
                writes = list(h.fake.writes)
                out = h.step('O3', redo=True)
                self.assertEqual(out.status, steps.REFUSED, h.text())
                self.assertRegex(out.note, r'O3 stopped at check "source (attached READWRITE|STORAGE_ID equals the recorded one)"')
                self.assertEqual(h.fake.writes, writes, 'nothing is issued')
                self.assertIsNone(h.journal.get_meta('freeze_time'))

    def test_f2_reentry_refused_once_the_replacement_exists(self):
        """Source storage detached, source name free and the replacement attached is the switch or a rollback, not O3: the re-entry block stops before the ATTACH."""
        h = o3_harness(self)
        h.fake.attachments.append({'DATABASE_NAME': TGT, 'ATTACH_TYPE': 'READWRITE', 'STORAGE_ID': 'sid-replacement', 'COMPUTE_ID': 'cid-1',
                                   'ATTACH_SYNTAX': None})
        h.fake.connect('ops_owner').cursor().execute('DETACH DATABASE `%s`' % SRC)
        out = h.step('O3', redo=True)
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('O3 stopped at check "no attached database named replacement', out.note)
        self.assertEqual(o3_writes(h, 'ATTACH'), [])
        self.assertIsNone(h.fake.attachment(SRC))

    def test_f2_hand_route_prints_the_block_that_starts_at_the_attach(self):
        """Without the owner profile the kit sees the stop between DETACH and ATTACH, prints the re-entry block and waits; after the hand ATTACH, step O3 --redo resumes at the probe."""
        h = o3_harness(self, owner=False)
        h.fake.connect('ops_owner').cursor().execute('DETACH DATABASE `%s`' % SRC)
        out = h.step('O3', redo=True)
        self.assertEqual(out.status, steps.PENDING, h.text())
        self.assertIn('-- O3: Freeze: re-entry between DETACH and ATTACH (Path A)', h.text())
        self.assertIn("ATTACH DATABASE `%s` READ ONLY STORAGEID 1028600672070281646:7367158985892520834;" % SRC, h.text())
        self.assertNotIn('DETACH DATABASE `%s`;' % SRC, h.text())
        self.assertEqual(o3_writes(h, 'ATTACH'), [], 'the runner issues no admin statement')
        h.fake.connect('ops_owner').cursor().execute("ATTACH DATABASE `%s` READ ONLY STORAGEID 1028600672070281646:7367158985892520834" % SRC)
        out = h.step('O3', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('O3 resumed', out.note)
        assert_frozen_once(self, h)

    def test_f2_notebook_o3_cell_runs_the_reentry_block_only_when_the_source_name_is_free(self):
        """The O3 cell runs O3-reentry only after the whole block stopped at its first check with no rows; one ATTACH in total, then step O3 --redo probes."""
        h = o3_harness(self)
        fail_o3_at(self, h, 'after', 'DETACH')
        result = run_o3_cell(h)
        self.assertEqual(result['status'], 'passed', result)
        self.assertEqual(result['statements'], 1)
        self.assertEqual(h.fake.attachment(SRC)['ATTACH_TYPE'], 'READONLY')
        out = h.step('O3', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text())
        assert_frozen_once(self, h)
        fresh = o3_harness(self)
        self.assertEqual(run_o3_cell(fresh)['status'], 'passed')
        self.assertEqual((len(o3_writes(fresh, 'DETACH')), len(o3_writes(fresh, 'ATTACH'))), (1, 1), 'READWRITE: the whole block runs once')
        writes = list(fresh.fake.writes)
        self.assertEqual(run_o3_cell(fresh)['status'], 'FAILED at check source attached READWRITE', 'READONLY: no re-entry')
        self.assertEqual(fresh.fake.writes, writes)

    def test_f2_whole_block_checks_the_storage_detached_between_detach_and_attach(self):
        """The whole O3 block (kit, printed and notebook forms) opens with the re-entry note, then runs DETACH, the check "source storage detached", then ATTACH; the re-entry block opens with that check."""
        h = o3_harness(self)
        names = names_of(h)
        detach = 'DETACH DATABASE `%s`' % SRC
        attach = "ATTACH DATABASE `%s` READ ONLY STORAGEID 1028600672070281646:7367158985892520834" % SRC
        gate = ('check', 'source storage detached', switch.RECORD_STATE_SQL, [SRC, '1028600672070281646:7367158985892520834'])
        for form, items in (('kit', [i.to_dict() for i in freeze.block(names).items]), ('notebook', block_now(h, 'O3')['items'])):
            with self.subTest(form=form):
                self.assertEqual((items[0]['kind'], items[0]['label']), ('note', freeze.REENTRY_NOTE), 'the re-entry note comes before the first check')
                self.assertEqual([i['label'] for i in items].count(freeze.REENTRY_NOTE), 1)
                shapes = [(i['kind'], i['label'], i['sql'], i['params']) for i in items if i['kind'] in ('check', 'statement')]
                at = [s[2] for s in shapes].index(detach)
                self.assertEqual(shapes[at + 1], gate, 'the gate follows the DETACH')
                self.assertEqual(shapes[at + 2][:1] + shapes[at + 2][2:3], ('statement', attach), 'the ATTACH follows the gate')
        printed = freeze.block(names).render()
        self.assertLess(printed.index('-- NOTE: Re-entry: when the first check finds no rows'), printed.index('-- CHECK '))
        self.assertIn('owner-sql/O3-reentry.sql', freeze.REENTRY_NOTE)
        self.assertLess(printed.index(detach + ';'), printed.index('-- CHECK source storage detached; expect IS_DETACHED = 1'))
        self.assertLess(printed.index('-- CHECK source storage detached'), printed.index(attach + ';'))
        reentry = [(i.kind, i.label) for i in freeze.reentry_block(names).items if i.kind in ('check', 'statement')]
        self.assertEqual(reentry[0], ('check', 'source storage detached'))
        self.assertEqual([i.sql for i in freeze.reentry_block(names).statements], [attach])

    def test_f2_detach_that_reports_done_with_the_source_attached_stops_before_the_attach(self):
        """The check "source storage detached" reads the real state when the DETACH worker ends without a result.

        A simulated kill on the DETACH worker thread ends only that thread, so the kit and the
        notebook both see a plain DETACH. Before (the server never saw the DETACH): both routes
        stop at the check and issue no ATTACH; the rerun runs the whole block once. After (the
        server applied it): the check passes and the same run attaches once. Journal (killed
        after the DETACH, before its owner_statement event): the rerun starts at the ATTACH.
        """
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = o3_harness(self)
                if point == 'journal':
                    self.assertIsNone(fail_o3_at(self, h, 'journal', 'DETACH'))
                    out = h.step('O3', redo=True)
                    self.assertIn('O3 resumed at the ATTACH', out.note)
                else:
                    h.fake.fail_statement(r'^DETACH DATABASE', point, crash=True)
                    with mock.patch.object(threading, 'excepthook', lambda args: None):
                        out = h.step('O3', redo=True)
                    self.assertEqual(h.fake.faults_fired, [(point, 'DETACH DATABASE `%s`' % SRC)])
                if point == 'before':
                    self.assertEqual(out.status, steps.REFUSED, h.text())
                    self.assertIn('O3 stopped at check "source storage detached"', out.note)
                    self.assertEqual(o3_writes(h, 'ATTACH'), [], 'no ATTACH while the source is still attached')
                    self.assertEqual(h.fake.attachment(SRC)['ATTACH_TYPE'], 'READWRITE')
                    self.assertIsNone(h.journal.get_meta('freeze_time'))
                    out = h.step('O3', redo=True)
                self.assertEqual(out.status, steps.PASSED, h.text())
                assert_frozen_once(self, h)
        h = o3_harness(self)
        h.fake.fail_statement(r'^DETACH DATABASE', 'before', crash=True)
        with mock.patch.object(threading, 'excepthook', lambda args: None):
            result = run_o3_cell(h)
        self.assertEqual(result['status'], 'FAILED at check source storage detached', result)
        self.assertEqual(o3_writes(h, 'ATTACH'), [], 'the notebook route stops at the same gate')
        self.assertEqual(h.fake.attachment(SRC)['ATTACH_TYPE'], 'READWRITE')

    def test_f2_branch_read_error_falls_back_to_the_whole_block_which_stops_at_its_first_check(self):
        """Between DETACH and ATTACH, a position read of MV_CLOUD_BRANCHES that fails gives the whole block, which stops at its first check and issues nothing.

        With the owner profile the owner session reads the position. The read fails with a lost
        connection before it, a lost reply after it, or a privilege error; a read has no journal
        write of its own, so the state comes from a DETACH whose reply was lost (the 'after'
        point). The log names the identity and keeps the error number, never the server message.
        The rerun, with the read working again, starts at the ATTACH. Without the owner profile
        the runner reads; a runner denied the view gets the whole block printed, whose first
        item names the re-entry block, and the gate stays pending.
        """
        denied = 'SELECT command denied to user for table MV_CLOUD_BRANCHES'

        def deny(db):
            raise FakeDisconnect(1142, denied)
        for failure in ('before', 'after', 'denied'):
            with self.subTest(failure=failure):
                h = o3_harness(self)
                fail_o3_at(self, h, 'after', 'DETACH')
                if failure == 'denied':
                    h.fake.change_before(r'FROM information_schema\.MV_CLOUD_BRANCHES', deny)
                else:
                    h.fake.fail_statement(r'FROM information_schema\.MV_CLOUD_BRANCHES', failure)
                writes = list(h.fake.writes)
                out = h.step('O3', redo=True)
                self.assertEqual(out.status, steps.REFUSED, h.text())
                self.assertIn('O3 stopped at check "source attached READWRITE"', out.note)
                self.assertIn('could not read MV_CLOUD_BRANCHES for STORAGE_ID 1028600672070281646:7367158985892520834 as the owner (FakeDisconnect, error %s)'
                              % ('1142' if failure == 'denied' else '2013'), h.text())
                self.assertNotIn(denied, h.text())
                self.assertEqual(h.fake.writes, writes, 'nothing is issued')
                assert_between_detach_and_attach(self, h)
                out = h.step('O3', redo=True)
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertIn('O3 resumed at the ATTACH', out.note)
                assert_frozen_once(self, h)
        with self.subTest(failure='runner denied, no owner profile'):
            h = o3_harness(self, owner=False)
            h.fake.connect('ops_owner').cursor().execute('DETACH DATABASE `%s`' % SRC)
            h.fake.deny('s2rebuild_runner', r'FROM information_schema\.MV_CLOUD_BRANCHES')
            writes = list(h.fake.writes)
            out = h.step('O3', redo=True)
            self.assertEqual(out.status, steps.PENDING, h.text())
            self.assertIn('could not read MV_CLOUD_BRANCHES for STORAGE_ID 1028600672070281646:7367158985892520834 as the runner (FakeDisconnect, error 1142)', h.text())
            self.assertIn('-- O3: Freeze: read-only attachment (Path A)', h.text())
            self.assertIn('-- NOTE: ' + freeze.REENTRY_NOTE, h.text())
            self.assertEqual(h.fake.writes, writes, 'nothing is issued')
            assert_between_detach_and_attach(self, h)

    def test_f2_reentry_block_refuses_a_database_attached_under_the_source_name_after_the_position_read(self):
        """A database attached under the source name between the kit's position read and the re-entry block's checks stops the block before the ATTACH.

        The stop between DETACH and ATTACH comes from each fault point: killed just before the
        ATTACH, the DETACH reply lost, or killed after the DETACH and before its owner_statement
        event. Another actor then attaches a database under the source name on another
        STORAGE_ID just after the kit read the position. The re-entry block stops at "no attached
        database named source": no ATTACH, freeze_time unset, the other database left alone. Once
        that database is gone, the rerun starts at the ATTACH and attaches once.
        """
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = o3_harness(self)
                fail_o3_at(self, h, point, O3_STOP_WORD[point])
                assert_between_detach_and_attach(self, h)
                foreign = foreign_source()
                h.fake.change_before(BRANCH_READ, lambda db: None)
                h.fake.change_before(BRANCH_READ, lambda db: db.attachments.append(foreign))
                writes = list(h.fake.writes)
                out = h.step('O3', redo=True)
                self.assertEqual(out.status, steps.REFUSED, h.text())
                self.assertIn('STORAGE_ID 1028600672070281646:7367158985892520834 is detached: an earlier run stopped between DETACH and ATTACH; resuming at the ATTACH', h.text(),
                              'the position read saw the source name free')
                self.assertIn('O3 stopped at check "no attached database named source"', out.note)
                self.assertEqual(h.fake.writes, writes, 'nothing is issued')
                self.assertIsNone(h.journal.get_meta('freeze_time'))
                self.assertEqual(h.fake.attachment(SRC)['STORAGE_ID'], 'sid-other', 'the other database is left alone')
                h.fake.attachments.remove(foreign)
                out = h.step('O3', redo=True)
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertIn('O3 resumed at the ATTACH', out.note)
                assert_frozen_once(self, h)

    def test_f2_hand_run_reentry_block_refuses_a_database_the_runner_cannot_see(self):
        """A database under the source name that only the admin sees never leads to an ATTACH, on any route.

        Without the owner profile the runner reads the position, sees the name free and prints
        the re-entry block to owner-sql/O3-reentry.sql. That block run by hand as the admin
        (ownerlib on an admin cursor) stops at "no attached database named source", and the
        notebook O3 cell stops at the whole block's STORAGE_ID check without falling through.
        With the owner profile the owner reads the position, sees the other database and gives
        the whole block, which refuses at its first checks. The stop between DETACH and ATTACH
        comes from a hand DETACH (no owner profile) or a DETACH whose reply was lost (the
        'after' point); nothing is issued after it.
        """
        h = o3_harness(self, owner=False)
        h.fake.connect('ops_owner').cursor().execute('DETACH DATABASE `%s`' % SRC)
        h.fake.attachments.append(foreign_source())
        h.fake.admin_only_storage.add('sid-other')
        writes = list(h.fake.writes)
        out = h.step('O3', redo=True)
        self.assertEqual(out.status, steps.PENDING, h.text())
        self.assertIn('resuming at the ATTACH', h.text(), 'the runner does not see the other database')
        printed = ownersql.owner_sql_path(h.ctx(), 'O3', variant='reentry').read_text()
        block = freeze.reentry_block(names_of(h))
        self.assertEqual(printed, block.render(), 'the hand run below runs the printed block')
        result = run_dict(h, block.to_dict())
        self.assertEqual(result['status'], 'FAILED at check no attached database named source', result)
        self.assertEqual(run_o3_cell(h)['status'], 'FAILED at check source STORAGE_ID equals the recorded one')
        self.assertEqual(h.fake.writes, writes, 'no ATTACH on the hand or notebook route')
        self.assertIsNone(h.journal.get_meta('freeze_time'))
        h = o3_harness(self)
        fail_o3_at(self, h, 'after', 'DETACH')
        h.fake.attachments.append(foreign_source())
        h.fake.admin_only_storage.add('sid-other')
        writes = list(h.fake.writes)
        out = h.step('O3', redo=True)
        self.assertEqual(out.status, steps.REFUSED, h.text())
        self.assertIn('O3 stopped at check "source STORAGE_ID equals the recorded one"', out.note)
        self.assertNotIn('resuming at the ATTACH', h.text(), 'the owner sees the other database')
        self.assertEqual(h.fake.writes, writes, 'nothing is issued')
        self.assertIsNone(h.journal.get_meta('freeze_time'))

    def test_f2_owner_profile_reads_the_position_when_the_runner_cannot_read_the_views(self):
        """With the owner profile the owner session reads the position, so a runner denied the attachment and branch views still resumes at the ATTACH.

        The stop between DETACH and ATTACH comes from each fault point: killed just before the
        ATTACH, the DETACH reply lost, or killed after the DETACH and before its owner_statement
        event. The runner is then denied MV_CLOUD_BRANCHES and MV_CLOUD_ATTACHED_DATABASES
        (error 1142). The rerun starts at the ATTACH, attaches once, and the runner's write probe
        and freeze_time follow.
        """
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = o3_harness(self)
                fail_o3_at(self, h, point, O3_STOP_WORD[point])
                h.fake.deny('s2rebuild_runner', r'FROM information_schema\.MV_CLOUD_(BRANCHES|ATTACHED_DATABASES)')
                out = h.step('O3', redo=True)
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertIn('O3 resumed at the ATTACH', out.note)
                self.assertNotIn('could not read', h.text())
                assert_frozen_once(self, h)

    def test_f2_dry_run_between_detach_and_attach_names_the_reentry_block(self):
        """A dry run opens no connection, so it cannot read the position: it prints the whole block and says a real rerun starts at the ATTACH; nothing is issued."""
        h = o3_harness(self)
        fail_o3_at(self, h, 'after', 'DETACH')
        writes = list(h.fake.writes)
        out = h.step('O3', redo=True, dry_run=True)
        self.assertEqual(out.status, steps.PENDING, h.text())
        self.assertIn('dry run: no connection is opened, so the attachment state is not read; after a stop between DETACH and ATTACH, '
                      's2rebuild step O3 --redo prints and runs the re-entry block instead (it starts at the ATTACH)', h.text())
        self.assertIn('-- O3: Freeze: read-only attachment (Path A)', h.text())
        self.assertEqual(h.fake.writes, writes)
        assert_between_detach_and_attach(self, h)


if __name__ == '__main__':
    unittest.main()
