"""s2rebuild tests test_prompts: credential prompts per source type, the skip and the O9 gate, the runner password, names at O1, O4 inside run.

Author: John Bergamini

Every prompt is answered by a scripted stand-in for prompts.ask; every Keychain read and write
goes to a dict; no test touches the security command or a terminal.
"""
import io
import json
import random
import unittest
from unittest import mock

from s2rebuild import connect, estimate, inventory, ownersql, pipelines as P, prompts, report_xlsx, steps
from s2rebuild.common import EXIT_DONE, EXIT_GATE, EXIT_REFUSED, KitError
from s2rebuild.connect import Log, Redactor
from s2rebuild.journal import MEMORY, Journal
from s2rebuild.sqlutil import sql_literal
from tests.helpers import config_from, run_cli
from tests.test_cli import LiveRun, RUN
from tests.test_pipelines import ITEM, PLAIN, owner_harness, pass_steps, write_o2
from tests.test_steps import Harness as StepHarness

KAFKA_ITEM = 'kc-kafka'
POLICY = {'PASSWORD_MIN_LENGTH': '14', 'PASSWORD_MIN_UPPERCASE_CHARS': '1', 'PASSWORD_MIN_LOWERCASE_CHARS': '1',
          'PASSWORD_MIN_NUMERIC_CHARS': '1', 'PASSWORD_MIN_SPECIAL_CHARS': '1', 'PASSWORD_MAX_REPEATED_CHARS': '3',
          'PASSWORD_MAX_SEQUENTIAL_CHARS': '2'}


def scripted(test, answers):
    """Replace prompts.ask with a stand-in that returns the answers in order (None stands for no terminal)."""
    it = iter(list(answers))
    patcher = mock.patch.object(prompts, 'ask', lambda label, hidden=False: next(it))
    patcher.start()
    test.addCleanup(patcher.stop)


def never_asks(test):
    """Replace prompts.ask with a stand-in that fails the test when called."""
    def _ask(label, hidden=False):
        raise AssertionError('no prompt expected, asked: %s' % label)
    patcher = mock.patch.object(prompts, 'ask', _ask)
    patcher.start()
    test.addCleanup(patcher.stop)


def fake_store(test, items=None, write_fails=False):
    """A dict standing in for the Keychain behind prompts.read_keychain, prompts.write_keychain and connect.read_keychain."""
    store = dict(items or {})

    def read(service, account=None, run=None):
        if service not in store:
            raise KitError('Keychain item %s for account test was not found or could not be read' % service)
        return store[service]

    def write(service, value, account=None, run=None):
        if write_fails:
            raise KitError('Keychain item %s for account test could not be written (security exit 36)' % service)
        store[service] = value
    for module, name, func in ((prompts, 'read_keychain', read), (prompts, 'write_keychain', write), (connect, 'read_keychain', read)):
        patcher = mock.patch.object(module, name, func)
        patcher.start()
        test.addCleanup(patcher.stop)
    return store


def kafka_keychain(d):
    d.setdefault('pipelines', {})['credentials_keychain'] = {'KAFKA': KAFKA_ITEM}


class CredentialPromptTests(unittest.TestCase):
    def test_json_shape_per_source_type_and_the_fake_store(self):
        """S3 and Kafka are asked by field (the token optional), another type by CONFIG keys and key-value pairs; the JSON goes to the
        Keychain item of the type, every value is masked, a type that resolves already is not asked again, and the resolution order is
        the per-pipeline item, the type item, then the variables."""
        said = []
        scripted(self, ['AKIA', 's3-secret', 'tok'])
        status, text, values = prompts.ask_credentials('S3', [dict(ITEM, source_type='S3')], say=said.append)
        self.assertEqual((status, json.loads(text), values), (prompts.GIVEN, {'aws_access_key_id': 'AKIA', 'aws_secret_access_key': 's3-secret',
                                                                             'aws_session_token': 'tok'}, ['AKIA', 's3-secret', 'tok']))
        scripted(self, ['AKIA', 's3-secret', ''])
        self.assertEqual(json.loads(prompts.ask_credentials('S3', [ITEM])[1]), {'aws_access_key_id': 'AKIA', 'aws_secret_access_key': 's3-secret'})
        scripted(self, ['kuser', 'kpass'])
        self.assertEqual(json.loads(prompts.ask_credentials('KAFKA', [ITEM])[1]), {'sasl.username': 'kuser', 'sasl.password': 'kpass'})
        gcs = dict(ITEM, source_type='GCS', definition="CREATE PIPELINE `g` AS LOAD DATA GCS 'b/p' CONFIG '{\"region\": \"us\", \"prefix\": \"x\"}' "
                                                       "CREDENTIALS '<<CREDENTIALS>>' INTO TABLE `t_single`")
        scripted(self, ['credentials_json', '{"type": "service_account"}', ''])
        status, text, values = prompts.ask_credentials('GCS', [gcs], say=said.append)
        self.assertEqual((status, json.loads(text)), (prompts.GIVEN, {'credentials_json': '{"type": "service_account"}'}))
        self.assertIn('CONFIG keys in use: prefix, region', said[-1])
        scripted(self, [''])
        self.assertEqual(prompts.ask_credentials('KAFKA', [ITEM])[0], prompts.SKIPPED)
        scripted(self, [None, None])
        self.assertEqual(prompts.ask_credentials('KAFKA', [ITEM])[0], prompts.NO_TERMINAL)
        self.assertEqual(prompts.ask_credentials('S3', [dict(ITEM, source_type='S3')])[0], prompts.NO_TERMINAL)
        # the store: one prompt fills the item; the next call finds it and asks nothing; the redactor masks every value
        cfg = config_from(kafka_keychain)
        store = fake_store(self)
        journal = Journal(MEMORY)
        self.addCleanup(journal.close)
        out = io.StringIO()
        redactor = Redactor()
        log = Log(redactor=redactor, stream=out)
        scripted(self, ['kuser', 'kpass'])
        self.assertEqual(prompts.pipeline_credentials([ITEM, PLAIN], cfg, journal, log, {}, redactor, step='P3'), {'KAFKA': prompts.GIVEN})
        self.assertEqual(json.loads(store[KAFKA_ITEM]), {'sasl.username': 'kuser', 'sasl.password': 'kpass'})
        self.assertEqual(journal.events('credentials_stored'), ['P3: KAFKA credentials stored in Keychain item %s' % KAFKA_ITEM])
        self.assertNotIn('kpass', out.getvalue())
        self.assertEqual(redactor('sasl.password kpass kuser'), 'sasl.password <redacted> <redacted>')
        never_asks(self)
        self.assertEqual(prompts.pipeline_credentials([ITEM, PLAIN], cfg, journal, log, {}, redactor), {})
        self.assertEqual(P.credential_for(ITEM, cfg, {}), (store[KAFKA_ITEM], KAFKA_ITEM))
        per = config_from(lambda d: d.update(pipelines={'credentials_keychain': {'KAFKA': KAFKA_ITEM}, 'per_pipeline_keychain': {'p1': 'p1-item'}}))
        store['p1-item'] = 'per-pipeline-value'
        self.assertEqual(P.credential_for(ITEM, per, {}), ('per-pipeline-value', 'p1-item'))
        del store['p1-item']
        del store[KAFKA_ITEM]
        self.assertEqual(P.credential_for(ITEM, per, {'S2REBUILD_KAFKA_CREDENTIALS': 'from-env'}), ('from-env', 'S2REBUILD_KAFKA_CREDENTIALS'))
        with self.assertRaisesRegex(KitError, 'Keychain item p1-item is missing too'):
            P.credential_for(ITEM, per, {})
        # a type without a Keychain item configured is not asked; the variable is named instead
        plain_cfg = config_from()
        out.truncate(0)
        self.assertEqual(prompts.pipeline_credentials([ITEM], plain_cfg, journal, log, {}, redactor), {'KAFKA': prompts.NO_TERMINAL})
        self.assertIn('no Keychain item is configured (pipelines.credentials_keychain), so nothing is asked; export S2REBUILD_KAFKA_CREDENTIALS', out.getvalue())

    def test_blank_answer_records_the_skip_and_the_o9_gate_excludes_it(self):
        """A blank answer at O9 skips the source type: the journal records it, the pipelines of that type are not created, the count
        gate expects the others only, the block, the plan sheet and the final report list them with the placeholder file."""
        h = owner_harness(self, mutate=kafka_keychain)
        fake_store(self)
        self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
        pass_steps(h, 'O7', 'O8')
        write_o2(h, [ITEM, PLAIN])
        scripted(self, [''])
        out = h.step('O9', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text()[-600:])
        created = [w for w in h.fake.writes if w.startswith('CREATE PIPELINE')]
        self.assertEqual(len(created), 1)
        self.assertIn('`plain_fs`', created[0])
        self.assertEqual(h.journal.events('pipeline_skip'), ['O9: no KAFKA credentials given; not recreated at O9: p1'])
        self.assertEqual(P.skipped_types(h.journal), {'KAFKA'})
        self.assertIn('these pipelines will not be recreated at O9: p1', h.text())
        block = (h.tmp / 'runs' / h.run_id / h.db.source / 'owner-sql' / 'O9.sql').read_text(encoding='utf8')
        self.assertIn('Not recreated, no credentials given at the prompt (the count above leaves them out): p1 (KAFKA)', block)
        self.assertIn('expect n = 1', block)
        self.assertEqual(ownersql.expected_counts(h.ctx())['pipelines'], 1, 'S3 expects the recreated count on the live name')
        never_asks(self)
        self.assertEqual(h.step('O9', redo=True).status, steps.PASSED, 'a skipped type is not asked again')
        inv = dict(h.inventory, pipelines={'captured': True, 'note': 'test', 'items': [ITEM, PLAIN]})
        rows = dict(report_xlsx.final_summary_rows(inv, h.plans(), h.journal, h.config, h.db, h.run_id))
        self.assertTrue(rows['Not recreated (no credentials given)'].startswith('p1 (KAFKA); recreate from '), rows['Not recreated (no credentials given)'])
        self.assertIn('recreate-with-placeholders.sql', rows['Not recreated (no credentials given)'])
        est = estimate.estimate(h.plans(), h.config, estimate.DEFAULT_BASELINE, 2)
        sheet = '\n'.join(estimate.plan_sheet(h.config, h.db, h.run_id, inv, est, None, skipped=P.skipped_types(h.journal)))
        self.assertIn('not recreated at O9 (no credentials given at the P3 prompt; the count gate leaves them out): p1 (KAFKA)', sheet)
        self.assertIn('KAFKA Keychain item %s, env set' % KAFKA_ITEM, '\n'.join(estimate.plan_sheet(
            h.config, h.db, h.run_id, inv, est, None, env={'S2REBUILD_KAFKA_CREDENTIALS': 'x'})))

    def test_o9_asks_stores_and_replays_and_a_failed_keychain_write_stops_before_any_statement(self):
        """Credentials typed at O9 are stored in the Keychain item and injected at CREATE PIPELINE, never printed; a Keychain write that
        fails refuses O9 before any statement."""
        h = owner_harness(self, mutate=kafka_keychain)
        store = fake_store(self)
        self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
        pass_steps(h, 'O7', 'O8')
        write_o2(h, [ITEM])
        scripted(self, ['kuser', 'kpass-fixture'])
        out = h.step('O9', redo=True)
        self.assertEqual(out.status, steps.PASSED, h.text()[-600:])
        created = [p for p in h.fake.pipelines if p['PIPELINE_NAME'] == 'p1' and p.get('DATABASE_NAME') == h.db.replacement]
        self.assertEqual(len(created), 1)
        self.assertIn(sql_literal(store[KAFKA_ITEM]), created[0]['definition'])
        self.assertEqual(json.loads(store[KAFKA_ITEM]), {'sasl.username': 'kuser', 'sasl.password': 'kpass-fixture'})
        self.assertNotIn('kpass', h.text())
        self.assertNotIn('kpass', (h.tmp / 'runs' / h.run_id / h.db.source / 'owner-sql' / 'O9.sql').read_text(encoding='utf8'))
        self.assertEqual(h.journal.events('credentials_stored'), ['O9: KAFKA credentials stored in Keychain item %s' % KAFKA_ITEM])
        self.assertEqual(h.journal.events('pipeline_skip'), [])
        failing = owner_harness(self, mutate=kafka_keychain)
        fake_store(self, write_fails=True)
        self.assertEqual(failing.o5_o6().status, steps.PASSED)
        pass_steps(failing, 'O7', 'O8')
        write_o2(failing, [ITEM])
        scripted(self, ['kuser', 'kpass-fixture'])
        out = failing.step('O9', redo=True)
        self.assertEqual(out.status, steps.REFUSED, failing.text()[-400:])
        self.assertIn('Keychain item %s for account test could not be written' % KAFKA_ITEM, out.note)
        self.assertFalse(any(w.startswith(('CREATE PIPELINE', 'USE')) for w in failing.fake.writes), 'nothing was issued')
        self.assertNotIn('kpass', failing.text())

    def test_a_missing_per_pipeline_item_is_filled_in_that_item_and_the_type_item_is_kept(self):
        """A pipeline with a per-pipeline Keychain item configured and absent is asked for alone and its answer goes to that item, the
        item credential_for reads; the type item that serves the other pipeline of the type stays as it was; the next call asks nothing."""
        cfg = config_from(lambda d: d.update(pipelines={'credentials_keychain': {'KAFKA': KAFKA_ITEM}, 'per_pipeline_keychain': {'p1': 'p1-item'}}))
        p2 = dict(ITEM, name='p2', definition=ITEM['definition'].replace('p1', 'p2'))
        store = fake_store(self, {KAFKA_ITEM: 'type-value'})
        journal = Journal(MEMORY)
        self.addCleanup(journal.close)
        out = io.StringIO()
        log = Log(redactor=Redactor(), stream=out)
        scripted(self, ['kuser', 'kpass'])
        self.assertEqual(prompts.pipeline_credentials([ITEM, p2], cfg, journal, log, {}, step='P3'), {'KAFKA': prompts.GIVEN})
        self.assertEqual(json.loads(store['p1-item']), {'sasl.username': 'kuser', 'sasl.password': 'kpass'})
        self.assertEqual(store[KAFKA_ITEM], 'type-value')
        self.assertIn('credentials for the KAFKA pipelines p1 (', out.getvalue())
        self.assertEqual(journal.events('credentials_stored'), ['P3: KAFKA credentials stored in Keychain item p1-item'])
        self.assertEqual(P.credential_for(ITEM, cfg, {}), (store['p1-item'], 'p1-item'))
        never_asks(self)
        self.assertEqual(prompts.pipeline_credentials([ITEM, p2], cfg, journal, log, {}, step='O9'), {})
        P.check_credentials([ITEM, p2], cfg, {})


class PasswordTests(unittest.TestCase):
    def test_generator_meets_a_fixture_policy_and_the_checker_names_each_miss(self):
        """The generated password meets the fixture policy (length, classes, repeat and sequence limits) with a seeded generator; the
        checker names what a weak password misses; a policy no 32 character password can meet is refused."""
        rng = random.Random(7)
        for _ in range(20):
            password = prompts.generate_password(POLICY, rng=rng)
            self.assertEqual(len(password), 32)
            self.assertEqual(prompts.password_problems(password, POLICY), [], password)
            self.assertFalse(any(password[i] == password[i + 1] for i in range(31)), 'no character twice in a row: %s' % password)
            self.assertNotIn("'", password)
            self.assertNotIn('\\', password)
        self.assertEqual(len(prompts.generate_password(dict(POLICY, PASSWORD_MIN_LENGTH='40'), rng=rng)), 40, 'the policy minimum wins over 32')
        self.assertEqual(len(prompts.generate_password({}, rng=rng)), 32, 'no policy reported: 32 characters with every class')
        problems = prompts.password_problems('aaaab1234', POLICY)
        self.assertEqual(len(problems), 5, problems)
        self.assertIn('length 9 is under the minimum 14', problems)
        self.assertIn('0 upper-case characters, under the minimum 1', problems)
        self.assertIn('0 special characters, under the minimum 1', problems)
        self.assertIn('a character repeats more than 3 times in a row', problems)
        self.assertIn('more than 2 sequential characters in a row', problems)
        self.assertEqual(prompts.password_problems('Ab1!Cd2@Ef3#Gh4$Ij', POLICY), [])
        self.assertEqual(prompts.policy_rules({'password_min_numeric_chars': '2', 'PASSWORD_MAX_REPEATED_CHARS': '3'})['min_digits'], 2)
        self.assertEqual(prompts.policy_lines(POLICY), ['password policy: ' + ', '.join('%s = %s' % (k, POLICY[k]) for k in sorted(POLICY))])
        with self.assertRaisesRegex(KitError, 'more characters per class than a 32 character password holds'):
            prompts.generate_password(dict(POLICY, PASSWORD_MIN_UPPERCASE_CHARS='20', PASSWORD_MIN_LOWERCASE_CHARS='20'), rng=rng)

    def test_account_sql_execute_asks_the_owner_password_generates_the_runner_password_and_stops_on_a_failed_write(self):
        """P1 with Keychain items: the missing owner password is asked (hidden) and stored; the policy is printed, a compliant runner
        password is generated and stored, then CREATE USER runs with it; neither value reaches the output or the log file; a rerun with
        the account present keeps the item; a Keychain write that fails stops before CREATE USER."""
        def keychain_profiles(d):
            d['connections'] = {'runner': {'user': 's2rebuild_runner', 'password_keychain': 's2rebuild-runner'},
                                'owner': {'user': 'admin', 'password_keychain': 's2rebuild-owner'}}
        h = LiveRun(self, keychain_profiles)
        store = fake_store(self)
        h.fake.privileges = []  # the runner account does not exist yet
        h.fake.globals.update(POLICY)
        scripted(self, ['owner-fixture-secret'])
        code, text = h.cli('account-sql', '--execute')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertEqual(store['s2rebuild-owner'], 'owner-fixture-secret')
        self.assertIn('owner password stored in Keychain item s2rebuild-owner', text)
        self.assertIn('password policy: PASSWORD_MAX_REPEATED_CHARS = 3, PASSWORD_MAX_SEQUENTIAL_CHARS = 2, PASSWORD_MIN_LENGTH = 14', text)
        runner = store['s2rebuild-runner']
        self.assertEqual(len(runner), 32)
        self.assertEqual(prompts.password_problems(runner, POLICY), [])
        self.assertIn('runner password: 32 characters generated to the policy and stored in Keychain item s2rebuild-runner', text)
        self.assertIn("CREATE USER 's2rebuild_runner'@'%%' IDENTIFIED BY %s" % sql_literal(runner), h.fake.writes)
        self.assertIn("'s2rebuild_runner'@'%'", h.fake.users)
        self.assertIn('P1 account creation executed with the owner profile', text)
        for secret in (runner, 'owner-fixture-secret'):
            self.assertNotIn(secret, text)
        logs = list((h.tmp / 'runs' / RUN / 'logs').glob('account-sql-*.log'))
        self.assertEqual(len(logs), 1)
        for secret in (runner, 'owner-fixture-secret'):
            self.assertNotIn(secret, logs[0].read_text(encoding='utf8'))
        never_asks(self)
        code, text = h.cli('account-sql', '--execute')
        self.assertEqual(code, EXIT_REFUSED, 'the first check stops: the account exists')
        self.assertIn('the account exists already, so Keychain item s2rebuild-runner is kept as it is', text)
        self.assertEqual(store['s2rebuild-runner'], runner)
        self.assertEqual(sum(1 for w in h.fake.writes if w.startswith('CREATE USER')), 1)
        failing = LiveRun(self, keychain_profiles)
        fake_store(self, write_fails=True)
        failing.fake.privileges = []
        scripted(self, ['owner-fixture-secret'])
        code, text = failing.cli('account-sql', '--execute')
        self.assertEqual(code, EXIT_REFUSED, text)
        self.assertIn('Keychain item s2rebuild-owner for account test could not be written', text)
        self.assertEqual(failing.fake.writes, [], 'nothing reached the server')


class NamesAndO4Tests(unittest.TestCase):
    def test_production_mode_asks_for_missing_names_and_test_mode_does_not(self):
        """In production mode O1 asks for a replacement or retained name the configuration leaves to the kit, records the answers and
        later commands read them back from the journal; test mode (run.mode or --mode) applies the defaults silently; without a terminal
        production mode refuses and records nothing."""
        def two(d):
            d['databases'].append({'source': 'second_db', 'layout': {'partitions': 8, 'sub_partitions': 64}})
        h = StepHarness(self, two, on_disk=True).ready_for_o1()
        scripted(self, ['second_db_new', ''])
        self.assertEqual(h.run(), EXIT_GATE, h.text())
        second = h.journals['second_db']
        self.assertEqual((second.get_meta('replacement_db'), second.get_meta('retained_db')), ('second_db_new', 'second_db_retained_20261101'))
        self.assertEqual(second.events('names'), ['O1 names typed in production mode: replacement second_db_new, retained second_db_retained_20261101'])
        self.assertIn('names: source second_db, replacement second_db_new, retained second_db_retained_20261101 (replacement and retained typed at O1)', h.text())
        self.assertIn('names typed at O1: replacement second_db_new', second.step('O1')['note'])
        self.assertEqual(h.journal.events('names'), [], 'the first database has its names in the configuration')
        for journal in h.journals.values():
            journal.close()
        code, text = run_cli(['status', '--config', str(h.path), '--run', '20261101-0500', '--yes'])
        self.assertEqual(code, EXIT_DONE, text)
        self.assertNotIn('warning', text, 'the typed name is read back from the journal, so the names check passes')
        code, text = run_cli(['run', '--dry-run', '--from', 'O2', '--to', 'O2', '--db', 'second_db', '--config', str(h.path), '--run', '20261101-0500'])
        self.assertEqual(code, EXIT_DONE, text)
        never_asks(self)
        quiet = StepHarness(self, lambda d: (two(d), d.update(run={'mode': 'test'}))).ready_for_o1()
        self.assertEqual(quiet.run(), EXIT_GATE, quiet.text())
        self.assertEqual(quiet.journals['second_db'].get_meta('replacement_db'), 'second_db_rebuild_20261101')
        self.assertIn('retained second_db_retained_20261101 (replacement and retained by the kit\'s date default: run.mode test)', quiet.text())
        flagged = StepHarness(self, two).ready_for_o1()
        flagged.options = dict(flagged.options, mode='test')
        self.assertEqual(flagged.run(), EXIT_GATE, flagged.text())
        self.assertEqual(flagged.journals['second_db'].get_meta('retained_db'), 'second_db_retained_20261101')
        scripted(self, [None])
        blind = StepHarness(self, two).ready_for_o1()
        self.assertEqual(blind.run(), EXIT_REFUSED)
        note = blind.journals['second_db'].step('O1')['note']
        self.assertIn('run.mode is production and databases[].replacement is not set for second_db', note)
        self.assertIn('run with --mode test to apply the default second_db_rebuild_20261101', note)
        self.assertIsNone(blind.journals['second_db'].get_meta('replacement_db'), 'a refused O1 records no name')
        self.assertEqual(blind.run(dry_run=True, to_id='O1'), EXIT_DONE)
        self.assertIn('(defaults; a real run in production mode asks for the replacement and retained name(s))', blind.text())

    def test_run_reaches_o4_and_o5_without_the_profile_command(self):
        """run performs the O4 capture itself after the O3 gate: the inventory, the plan and the workbook are written, O4 is passed, O5
        creates the replacement from that capture; a second run skips the passed O4 and takes no capture again."""
        h = LiveRun(self).started(self)
        for step_id in ('O1', 'O2', 'O3'):
            self.assertEqual(h.cli('gate', step_id, '--pass', '--note', 'recorded for the test')[0], EXIT_DONE)
        code, text = h.cli('run', '--to', 'O5')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('[O4] Freeze capture', text)
        self.assertIn('drift: no P3 inventory to compare against', text)
        o4 = h.run_dir / 'inventory' / 'O4'
        self.assertTrue((o4 / inventory.INVENTORY_FILE).is_file())
        self.assertTrue((o4 / 'plan.json').is_file())
        self.assertTrue((h.run_dir / 'reports' / 'freeze-capture.xlsx').is_file())
        with h.journal() as journal:
            self.assertEqual((journal.step_status('O4'), journal.step_status('O5')), ('passed', 'passed'))
            self.assertEqual(journal.get_meta('inventory_dir_O4'), str(o4))
            self.assertIn('O4 captured 4 tables', journal.step('O4')['note'])
        self.assertIn(h.config.databases[0].replacement, h.fake.databases, 'O5 created the replacement from the O4 capture')
        reads = len(h.fake.statements)
        code, text = h.cli('run', '--from', 'O4', '--to', 'O5')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('[O4] Freeze capture (python; runner) database example_orders_db: already passed, skipped', text)
        self.assertEqual(len(h.fake.statements), reads, 'no capture is taken again')


if __name__ == '__main__':
    unittest.main()
