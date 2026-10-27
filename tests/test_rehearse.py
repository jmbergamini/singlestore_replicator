"""s2rebuild tests test_rehearse: the owner notebook, the rehearsal (prefix rules, fill, the real steps, kill test, rollback leg, cleanup by
record), sample-database and the M4 commands.

Author: John Bergamini
"""
import contextlib
import io
import json
import os
import stat
import unittest

from s2rebuild import freeze, grants, inventory, notebook, ownersql, postcopy, rehearse, sample, steps
from s2rebuild.common import EXIT_DONE, EXIT_GATE, EXIT_REFUSED, KitError
from s2rebuild.journal import Journal
from s2rebuild.sqlutil import GuardError, qualified
from tests.fakedb import evaluate_predicate, multiset
from tests.helpers import FAULT_POINTS, config_from, fake_driver, run_cli
from tests.test_copy import REF_TABLES, SRC, TGT, Harness, fixture_inventory, fixture_rows
from tests.test_identity import attach_writes
from tests.test_pipelines import (ITEM, O3_STOP_WORD, assert_between_detach_and_attach, assert_frozen_once, fail_o3_at, names_of, o3_harness,
                                  owner_harness, run_dict, write_o2)

REHEARSAL_TABLES = {
    'keyed_single': ([('id', 'varchar(100)', False), ('payload', 'varchar(200)', True), ('n', 'int(11)', True), ('updated_at', 'datetime(6)', True)], ['id'], 300),
    'keyed_composite': ([('a', 'varchar(64)', False), ('b', 'datetime(6)', False), ('c', 'int(11)', False), ('note', 'varchar(100)', True)], ['a', 'b', 'c'], 50),
    'keyed_bigint': ([('id', 'bigint(20)', False), ('v', 'varchar(20)', True)], ['id'], 39),
    'unique_varchar': ([('email', 'varchar(128)', False), ('n', 'int(11)', True)], ['email'], 20),
    'nokey_dupes': ([('x', 'int(11)', True), ('y', 'varchar(50)', True), ('z', 'double', True)], [], 300),
    'nokey_empty': ([('p', 'varchar(10)', True), ('q', 'int(11)', True)], [], 0),
}
PREFIX = 's2rebuild_rehearsal_test'


def rehearsal_specs():
    """(columns, primary key, estimated rows, create text) per rehearsal table; the create text unqualified, as the fake stores it."""
    return {name: (columns, pk, est, rehearse.TABLES[name].format(db='__db__').replace('__db__.', ''))
            for name, (columns, pk, est) in REHEARSAL_TABLES.items()}


def rehearsal_rows():
    """Rows for the fake rehearsal source (the fake cannot run the INSERT ... SELECT fill)."""
    rows = {name: [] for name in REHEARSAL_TABLES}
    rows['keyed_single'] = [{'id': 'id-%d' % i, 'payload': 'p', 'n': i, 'updated_at': '2026-01-01 00:00:%02d.000000' % (i % 60)} for i in range(300)]
    rows['keyed_composite'] = [{'a': 'a%d' % (i % 5), 'b': '2026-01-01 00:00:%02d.%06d' % (i % 60, i), 'c': i, 'note': None} for i in range(50)]
    rows['keyed_bigint'] = [{'id': i, 'v': 'v%d' % i} for i in range(1, 40)]
    rows['unique_varchar'] = [{'email': 'user%d@example.test' % i, 'n': i} for i in range(20)]
    for i in range(150):
        row = {'x': i, 'y': None if i % 7 == 0 else 'y%d' % i, 'z': None if i % 5 == 0 else i / 3.0}
        rows['nokey_dupes'].append(row)
        rows['nokey_dupes'].append(dict(row))
    return rows


def rehearsal_dir(h):
    """The rehearsal folder of the harness run, where the rehearsal journal, its timings and the files of the rehearsed steps live."""
    return h.tmp / 'runs' / h.run_id / 'rehearsal'


def rehearsal_journal(test, h):
    """The rehearsal's own journal (the rehearsed steps O2 to C3 are recorded there), closed at cleanup."""
    j = Journal(rehearsal_dir(h) / 'journal.sqlite')
    test.addCleanup(j.close)
    return j


def prefixed_entries(h):
    """The Portal entries of the fake whose name carries the rehearsal prefix (the rows cleanup must remove)."""
    return [e['DATABASE_NAME'] for e in h.fake.portal_entries() if e['DATABASE_NAME'].startswith(PREFIX)]


def run_fake_rehearsal(test, h, kill_test='inprocess', rows=300, keep=False):
    """Run the rehearsal on the fake with the fill and the capture replaced by direct rows; returns the outcome.

    The view is created by the USE and CREATE VIEW statements of rehearse.fill_statements, and
    captured the way inventory.capture reads it (information_schema.TABLES, SHOW CREATE VIEW).
    The capture replacement serves the P3 capture of the rehearsal and the O4 capture inside run.
    """
    specs = rehearsal_specs()

    def filled_source(session, names_, layout, rows_, log):
        taken = [n for n in names_.values() if n in h.fake.databases]
        if taken:
            raise KitError('Rehearsal name(s) %s already exist' % ', '.join(taken))
        session.execute('CREATE DATABASE `%s` PARTITIONS %d SUB_PARTITIONS %d' % (names_['source'], layout.partitions, layout.sub_partitions))
        src = h.fake.databases[names_['source']]
        data_rows = rehearsal_rows()
        for name, (columns, pk, est, create) in specs.items():
            src['tables'][name] = {'create': create, 'rows': data_rows[name], 'auto_increment': None}
        for sql in rehearse.fill_statements(names_['source'], rows_):
            if sql.startswith(('USE ', 'CREATE VIEW ')):
                session.execute(sql)
        log('fake rehearsal source filled')

    def fake_capture(session, spec, config, *args, **kwargs):
        inv = fixture_inventory(specs, database=spec.source, step='O4')
        inv['layout'] = dict(h.fake.databases[spec.source]['layout'])
        for t in inv['tables']:
            t['exact_rows'] = t['est_rows'] = len(h.fake.databases[spec.source]['tables'][t['name']]['rows'])
        for o in session.query(inventory.OBJECTS_SQL, (spec.source,)):
            if o['TABLE_TYPE'] == 'VIEW':
                definition = session.query('SHOW CREATE VIEW ' + qualified(spec.source, o['TABLE_NAME']))[0]['Create View']
                inv['views'].append({'name': o['TABLE_NAME'], 'definition': definition, 'definer': inventory.view_definer(definition)})
        return inv
    original = (rehearse.create_source, rehearse.inventory.capture)
    rehearse.create_source, rehearse.inventory.capture = filled_source, fake_capture
    try:
        return rehearse.run(h.ctx(), PREFIX, rows=rows, keep=keep, kill_test=kill_test)
    finally:
        rehearse.create_source, rehearse.inventory.capture = original


class NotebookTests(unittest.TestCase):
    def test_generate_writes_a_private_notebook_whose_first_cell_runs(self):
        h = owner_harness(self, mutate=lambda d: d['workspace'].update(id='ws-example-1'))
        h.journal.record_attachment('source', SRC, '1028600672070281646:7367158985892520834', 'cid-1', 'READWRITE')
        write_o2(h, [ITEM])
        names, blocks, inputs = notebook.blocks_for_run(h.ctx())
        self.assertEqual(sorted(blocks), ['C3', 'C4', 'O10', 'O10-rollback', 'O10-rollback-retained', 'O11', 'O2', 'O3', 'O3-reentry', 'O9'])
        self.assertEqual(inputs['capture_step'], 'O2')
        path = notebook.generate(h.tmp / 'owner-steps.ipynb', names, h.config, blocks, inputs['grants'], items=inputs['pipelines'],
                                 capture_step=inputs['capture_step'])
        self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
        self.assertTrue(notebook.first_cell_compiles(path))
        nb = json.loads(path.read_text())
        self.assertEqual(len(nb['cells']), 12)
        text = path.read_text()
        for needle in ('from singlestoredb.notebook import portal', "results_type='dicts', autocommit=True", 'getpass.getpass(', 'del credentials',
                       "str(portal.workspace_id) == str(RUN['workspace_id'])", 'connect=connect', 'step O3 --redo'):
            self.assertIn(needle, text)
        self.assertIn('<<CREDENTIALS>>', text)
        self.assertNotIn('owner-fixture', text)
        self.assertNotIn('fixture-password', text)
        namespace = {}
        with contextlib.redirect_stdout(io.StringIO()):
            exec(compile(notebook.first_code_cell(path), 'cell1', 'exec'), namespace)
        run = namespace['RUN']
        self.assertEqual((run['source'], run['workspace_id'], run['credential_keys'], run['pipeline_capture_step']), (SRC, 'ws-example-1', ['KAFKA'], 'O2'))
        self.assertIn('s2rebuild_runner', run['protected_users'])
        lockout_cell = ''.join(nb['cells'][5]['source'])
        compile(lockout_cell, 'cell5', 'exec')
        self.assertIn('WHERE DB = %s OR INFO LIKE %s', lockout_cell, 'idle sessions are found too')
        self.assertIn('GRANT RESTORED BY ANOTHER SESSION', lockout_cell)
        with self.assertRaises(KitError):
            notebook.generate(h.tmp / 'x.ipynb', names, h.config, blocks, template=h.tmp / 'missing.ipynb')

    def test_the_o9_block_of_an_early_capture_stops_in_the_notebook_too(self):
        h = owner_harness(self)
        inv = dict(h.inventory, pipelines={'captured': True, 'note': 'x', 'items': [ITEM]})
        inventory.write(inv, inventory.inventory_dir(h.config, h.run_id, h.db, 'P3'), h.plans())
        _, blocks, inputs = notebook.blocks_for_run(h.ctx())
        self.assertEqual(inputs['capture_step'], 'P3')
        self.assertEqual(blocks['O9'].to_dict()['items'][0]['kind'], 'refuse')


class RehearsalRulesTests(unittest.TestCase):
    def test_prefix_marker_and_collisions(self):
        self.assertEqual(rehearse.default_prefix('20261101-0500'), 's2rebuild_rehearsal_20261101_0500')
        with self.assertRaisesRegex(KitError, 'must start with s2rebuild_rehearsal_'):
            rehearse.check_prefix('rehearsal_x')
        with self.assertRaises(KitError):
            rehearse.check_prefix('s2rebuild_rehearsal_x-y')
        cfg = config_from(lambda d: d['databases'][0].update(source='s2rebuild_rehearsal_x_src'))
        with self.assertRaisesRegex(KitError, 'collides with configured database name'):
            rehearse.check_prefix('s2rebuild_rehearsal_x', cfg)
        guard = rehearse.rehearsal_guard(PREFIX)
        guard.check('DROP DATABASE `%s_rebuild`' % PREFIX)
        with self.assertRaises(GuardError):
            guard.check('DROP DATABASE `%s`' % SRC)

    def test_fill_is_exact_and_chunking_scales(self):
        statements = rehearse.fill_statements(PREFIX + '_src', 1500)
        keyed = [s for s in statements if 'INTO `%s_src`.`keyed_single`' % PREFIX in s]
        self.assertEqual(len(keyed), 1)
        self.assertTrue(keyed[0].endswith('< 1500'))
        dupes = [s for s in statements if s.startswith('INSERT INTO') and '`nokey_dupes` (`x`' in s]
        self.assertEqual(len(dupes), 2)
        self.assertTrue(all(s.endswith('< 750') for s in dupes))
        big = [s for s in rehearse.fill_statements(PREFIX + '_src', 2500000) if s.startswith('INSERT INTO') and '`keyed_single` (`id`' in s]
        self.assertEqual([s.rsplit('< ', 1)[1] for s in big], ['1000000', '1000000', '500000'])
        guard = rehearse.rehearsal_guard(PREFIX)
        for s in statements:
            guard.check(s)
        cfg = rehearse.rehearsal_config(config_from(), 1000000)
        self.assertEqual((cfg.copy.min_rows_for_chunking, cfg.copy.chunk_rows), (100000, 250000))


class RehearsalRunTests(unittest.TestCase):
    def test_rehearsal_end_to_end_with_the_switch(self):
        """P5 walks the real steps on the fake (W12): O2, O3 (DETACH, ATTACH READ ONLY), O4 to O9, S1 and S2, the rollback by the original
        record (R2a), O10 from the rollback state (S2b, S3, S4, the gate), O11, C1, C3; then cleanup by record. Asserted on the statement log."""
        h = owner_harness(self)
        outcome = run_fake_rehearsal(self, h)
        self.assertEqual(outcome.status, steps.PASSED, h.text())
        names = rehearse.rehearsal_names(PREFIX)
        src, rebuild, retained = names['source'], names['replacement'], names['retained']
        self.assertIn('switch live %s' % src, outcome.note)
        self.assertIn('not rehearsed: ', outcome.note)
        self.assertEqual(h.journal.step_status('P5'), 'passed')
        timings = json.loads((rehearsal_dir(h) / 'timings.json').read_text())
        self.assertEqual(timings['p5_status'], 'passed')
        self.assertEqual((timings['reconciled_chunks'], len(timings['interrupted_chunks'])), (1, 1), 'exactly the interrupted chunk was reconciled')
        self.assertIn('keyed_single', timings['bucketed_tables'], 'hash-bucket predicates are rehearsed')
        self.assertEqual(list(timings['steps']), ['create and fill', 'P3 capture', 'O2', 'O3', 'O4', 'O5', 'O6 kill test', 'O6 resume', 'O7', 'O8', 'O9',
                                                  'O10 switch and rollback', 'O10 switch again', 'O11', 'C1', 'C3'])
        self.assertTrue(timings['end_state'].startswith('two entries: '), timings['end_state'])
        self.assertIn('refused as read only', h.text())
        j = rehearsal_journal(self, h)
        self.assertIsNotNone(j.get_meta('freeze_time'), 'O3 froze the throwaway source and recorded freeze_time')
        self.assertEqual(len(j.events('endstate_probe')), 1, 'C3 ran the write probe on the retained rehearsal copy')
        self.assertEqual({s: j.step_status(s) for s in ('O2', 'O3', 'O4', 'O5', 'O6', 'O7', 'O8', 'O9', 'O10', 'O11', 'C1', 'C2', 'C3')},
                         dict({s: 'passed' for s in ('O2', 'O3', 'O4', 'O5', 'O6', 'O7', 'O8', 'O9', 'O10', 'O11', 'C1', 'C3')}, C2='skipped'))
        self.assertTrue(j.events('rollback')[-1].startswith('R2a:'), 'the rollback leg re-attached the original record by its own syntax')
        self.assertEqual(len(j.events('switch_go')), 1, 'O10 through run_step recorded the go once, after the rollback')
        self.assertEqual(len(j.events('endstate_listing')), 1)
        self.assertTrue((rehearsal_dir(h) / h.run_id / src / 'owner-sql' / 'O10.sql').is_file(), 'the rehearsed steps write under rehearsal/')
        sid = j.attachment('retained')['storage_id']
        writes = attach_writes(h)
        self.assertEqual(writes[:4], ['DETACH DATABASE `%s`' % src, 'ATTACH DATABASE `%s` READ ONLY STORAGEID %s' % (src, sid),
                                      'DETACH DATABASE `%s`' % src, 'ATTACH DATABASE `%s` AS `%s` READ ONLY STORAGEID %s' % (src, retained, sid)],
                         'O3 freezes the throwaway source; S1 and S2 follow')
        self.assertTrue(writes[4].startswith("ATTACH DATABASE `%s` AT MILESTONE '" % src) and writes[4].endswith('STORAGEID %s COMPUTEID %s' % (
            sid, j.attachment('source')['compute_id'])), 'R2a: the original record by its own ATTACH_SYNTAX')
        self.assertEqual(writes[5], 'DROP DATABASE `%s`' % src, 'S2b from the rollback state')
        self.assertEqual(writes[6], 'ATTACH DATABASE `%s` AS `%s` STORAGEID %s' % (rebuild, src, j.attachment('live')['storage_id']), 'S3')
        self.assertEqual(writes[7], 'DROP DATABASE `%s`' % rebuild, 'S4')
        self.assertEqual(writes[8:10], ['DROP DATABASE `%s`' % src, 'DETACH DATABASE `%s`' % retained], 'cleanup: the live name, then the retained copy')
        self.assertTrue(writes[10].startswith("ATTACH DATABASE `%s` AT MILESTONE '" % retained), 'the retained record re-attached read write by its syntax')
        self.assertEqual(writes[11:], ['DROP DATABASE `%s`' % retained])
        for name in names.values():
            self.assertNotIn(name, h.fake.databases, 'cleanup dropped %s' % name)
        self.assertEqual(prefixed_entries(h), [], 'no entry of the prefix is left on the workspace')
        self.assertIn(SRC, h.fake.databases, 'the configured source is untouched')
        self.assertFalse(any(w.startswith(('DETACH', 'ATTACH', 'DROP DATABASE')) and '`%s`' % SRC in w for w in h.fake.writes))
        self.assertTrue(steps._rehearsal_evidence(h.ctx()))

    def test_f12_a_path_b_configuration_rehearses_with_path_a_and_the_note_lists_what_was_left_out(self):
        """The lockout is about the customer's accounts, which the throwaway has none of: the rehearsal freezes with Path A whatever the
        configured path, and the P5 note lists the lockout among what was not rehearsed, with the objects the rehearsal source lacks."""
        h = owner_harness(self, mutate=lambda d: d.update(freeze={'mode': 'lockout'}))
        outcome = run_fake_rehearsal(self, h)
        self.assertEqual(outcome.status, steps.PASSED, h.text())
        self.assertIn('[O3] Freeze', h.text())
        self.assertNotIn('the last lockout pass', h.text())
        self.assertNotIn('REVOKE', ' '.join(h.fake.writes))
        timings = json.loads((rehearsal_dir(h) / 'timings.json').read_text())
        self.assertEqual(timings['not_rehearsed'], [
            'the lockout freeze (Path B): the rehearsal freezes with Path A, the read-only attachment',
            'reference tables (none on the rehearsal source)', 'routines (none on the rehearsal source)', 'pipelines (none on the rehearsal source)',
            rehearse.R2B_NOTE, 'application writes at C1 (the throwaway has no application; C1 passed as an idle workload)',
            'C2, C4 and the deletion of the retained copy (C5)'])
        self.assertIn('not rehearsed: the lockout freeze (Path B)', h.journal.step('P5')['note'])
        self.assertEqual(timings['freeze'].split(':')[0], 'Path A')

    def test_without_the_owner_profile_p5_stays_pending(self):
        h = Harness(self)
        outcome = run_fake_rehearsal(self, h)
        self.assertEqual((outcome.status, outcome.exit_code), (steps.PENDING, EXIT_GATE), h.text())
        self.assertIn('P5 needs the owner profile for the freeze, the pipelines, the switch', outcome.note)
        self.assertEqual(h.journal.step_status('P5'), 'pending')
        timings = json.loads((rehearsal_dir(h) / 'timings.json').read_text())
        self.assertEqual(timings['p5_status'], 'pending')
        self.assertEqual(timings['not_rehearsed'][0], rehearse.NEEDS_OWNER)
        self.assertFalse(steps._rehearsal_evidence(h.ctx()), 'a rehearsal without the switch is not O1 evidence')
        self.assertEqual(len([w for w in h.fake.writes if w.startswith('DROP DATABASE')]), 2, 'source and replacement were created and dropped')
        self.assertEqual(prefixed_entries(h), [])

    def test_w12_p5_fails_when_the_end_state_is_not_two_entries(self):
        """A third entry on the original storage when C3 runs: the two-entry gate stops C3 (the journal shows S2b and S4, so no repair), P5 ends
        failed with the listing, the evidence says so, and cleanup still drops only the names of the prefix, by record."""
        h = owner_harness(self)
        names = rehearse.rehearsal_names(PREFIX)

        def stray_entry(db):
            sid = next(a['STORAGE_ID'] for a in db.attachments if a['DATABASE_NAME'] == names['retained'])
            db.branches.append(dict(db.record('stray_entry_of_another_run', sid, db.new_cid(), 'resume', names['retained']),
                                    IS_DETACHED=1, IS_ATTACHED_TO_THIS_COMPUTE=0))
        h.fake.change_before(r'AND LATEST_COMPUTE_ID <> %s$', stray_entry)  # the first C3 check that reads no other step
        outcome = run_fake_rehearsal(self, h)
        self.assertEqual((outcome.status, outcome.exit_code), (steps.FAILED, EXIT_GATE), h.text())
        self.assertIn('Rehearsal C3 refused', outcome.note)
        self.assertIn('the original storage holds one entry', outcome.note)
        self.assertIn('stray_entry_of_another_run', outcome.note, 'the listing is in the note')
        self.assertEqual(h.journal.step_status('P5'), 'failed')
        timings = json.loads((rehearsal_dir(h) / 'timings.json').read_text())
        self.assertEqual(timings['p5_status'], 'failed')
        self.assertNotIn('C3', timings['steps'], 'a failed step has no timing')
        self.assertFalse(steps._rehearsal_evidence(h.ctx()), 'a failed rehearsal is no evidence for O1')
        j = rehearsal_journal(self, h)
        self.assertEqual((j.step_status('C3'), j.events('endstate_repair')), ('pending', []), 'C3 refused without a repair')
        self.assertEqual(prefixed_entries(h), [], 'cleanup dropped every entry of the prefix')
        self.assertIn('stray_entry_of_another_run', [e['DATABASE_NAME'] for e in h.fake.portal_entries()], 'an entry outside the prefix is never touched')
        self.assertIn(SRC, h.fake.databases)

    def test_w12_rehearse_from_database_builds_the_source_as_a_sample(self):
        """rehearse --from-database NAME --percent P: the rehearsal source is a sample of NAME (sample.build, the configured thresholds), and
        the rehearsed steps run on it; the sampled database is read only, never changed."""
        h = owner_harness(self, mutate=lambda d: d['copy'].update(min_rows_for_chunking=100))
        outcome = rehearse.run(h.ctx(), PREFIX, kill_test='inprocess', from_database=SRC, percent=10)
        self.assertEqual(outcome.status, steps.PASSED, h.text())
        names = rehearse.rehearsal_names(PREFIX)
        inserts = [w for w in h.fake.writes if w.startswith('INSERT INTO `%s`.' % names['source'])]
        self.assertEqual(len(inserts), 4, 'one INSERT per table of the sampled database')
        self.assertTrue(inserts[0].endswith('FROM `%s`.`t_single` WHERE CRC32(`id`) MOD 100 < 10' % SRC), inserts[0])
        self.assertTrue(inserts[-1].endswith('FROM `%s`.`t_empty`' % SRC), 'a table under copy.min_rows_for_chunking is copied whole')
        timings = json.loads((rehearsal_dir(h) / 'timings.json').read_text())
        self.assertEqual((timings['from_database'], timings['percent'], list(timings['steps'])[0]), (SRC, 10, 'sample source'))
        self.assertIn('sample %s built from %s at 10 percent: 4 tables (3 sampled by CRC32 of the key, 1 whole)' % (names['source'], SRC), h.text())
        counts = {t['table_name']: t['source_count'] for t in rehearsal_journal(self, h).tables()}
        expected = sum(1 for r in fixture_rows()['t_single'] if evaluate_predicate('CRC32(`id`) MOD 100 < 10', r))
        self.assertEqual(counts['t_single'], expected)
        self.assertTrue(0 < expected < 700)
        self.assertEqual(len(h.fake.rows(SRC, 't_single')), 700, 'the sampled database keeps its rows')
        self.assertFalse(any(w.startswith(('DETACH', 'ATTACH', 'DROP DATABASE')) and '`%s`' % SRC in w for w in h.fake.writes))
        self.assertEqual(prefixed_entries(h), [])
        self.assertEqual(h.journal.step_status('P5'), 'passed')

    def test_f7_the_rehearsal_view_is_captured_and_o7_creates_it_through_the_owner_or_ends_pending_without_it(self):
        """The rehearsal view is created after USE of the source with an unqualified body, so its capture names no database.

        With the owner profile O7 creates it in the replacement through the owner session (the
        captured definer is the runner, so the owner becomes the definer), and the switch counts
        it in the live name. Without the owner profile the runner creates no view: O7 ends
        pending after the stale check and the counters, the rehearsal records O7 skipped in its
        own journal, O8 runs, and P5 stays pending naming the views. No statement of its own;
        the fault points of the CREATE VIEW are covered in test_postcopy.
        """
        names = rehearse.rehearsal_names(PREFIX)
        fill = rehearse.fill_statements(names['source'], 300)
        view_sql = next(s for s in fill if s.startswith('CREATE VIEW'))
        self.assertEqual(fill[fill.index(view_sql) - 1], 'USE `%s`' % names['source'], 'USE of the source comes right before the view')
        self.assertFalse(postcopy.names_database(view_sql.split(' AS ', 1)[1], names['source']), 'the body names no database')
        for mode in ('owner', 'no owner'):
            with self.subTest(mode=mode):
                h = owner_harness(self) if mode == 'owner' else Harness(self)
                outcome = run_fake_rehearsal(self, h, keep=True)
                run_dir = rehearsal_dir(h)
                timings = json.loads((run_dir / 'timings.json').read_text())
                # the rehearsed steps write under rehearsal/: the O4 capture inside run lands there, under the rehearsal's own runs_dir
                sub = rehearse.rehearsal_config(h.config, 300, run_dir)
                captured = inventory.load(inventory.inventory_dir(sub, h.run_id, rehearse.rehearsal_spec(h.db, PREFIX), 'O4'))['views']
                self.assertEqual([(v['name'], v['definer']) for v in captured], [('keyed_counts', '`s2rebuild_runner`@`%`')])
                self.assertFalse(postcopy.names_database(captured[0]['definition'], names['source']))
                j = rehearsal_journal(self, h)
                creates = [w for w in h.fake.writes if w.startswith('CREATE') and 'VIEW `%s`.' % names['replacement'] in w]
                if mode == 'owner':
                    self.assertEqual(outcome.status, steps.PASSED, h.text())
                    self.assertEqual(j.step_status('O7'), 'passed')
                    self.assertEqual(len(creates), 1, creates)
                    self.assertNotIn('DEFINER=', creates[0], 'the runner definer is removed')
                    choice = json.loads(j.events('view')[0])
                    self.assertEqual((choice['view'], choice['definer']), ('keyed_counts', postcopy.OWNER_DEFINER))
                    live = h.fake.databases[names['source']]['views']['keyed_counts']
                    self.assertEqual(live.definer, 'ops_owner@%', 'after the switch the live name holds the view the owner defined')
                    self.assertEqual(timings['views'], 'created through the owner session')
                else:
                    self.assertEqual((outcome.status, outcome.exit_code), (steps.PENDING, EXIT_GATE), h.text())
                    self.assertIn('needs the owner profile', outcome.note)
                    self.assertEqual(creates, [], 'the runner creates no view in the replacement')
                    self.assertEqual(j.step_status('O7'), 'skipped')
                    self.assertTrue(any(d.startswith('O7 ') for d in j.events('step_pending')), 'O7 ended pending before the rehearsal recorded it skipped')
                    self.assertEqual(j.step_status('O8'), 'passed', 'O8 runs after O7 is recorded skipped')
                    self.assertIn('O8', timings['steps'])
                    self.assertEqual(timings['views'], 'not rehearsed (no owner profile)')
                    self.assertIn('not rehearsed without the owner profile', h.text())
                    self.assertEqual(h.journal.step_status('P5'), 'pending')

    def test_an_existing_name_is_refused_and_nothing_is_dropped(self):
        h = owner_harness(self)
        h.fake.databases[PREFIX + '_rebuild'] = {'layout': dict(h.fake.databases[SRC]['layout']), 'tables': {}, 'views': {}}
        with self.assertRaisesRegex(KitError, 'already exist'):
            run_fake_rehearsal(self, h)
        self.assertIn(PREFIX + '_rebuild', h.fake.databases, 'a name the run did not create is never dropped')
        self.assertFalse(any(w.startswith('DROP DATABASE') for w in h.fake.writes))

    def test_a_dry_run_writes_nothing(self):
        h = Harness(self)
        outcome = rehearse.run(h.ctx(dry_run=True), PREFIX, rows=1000)
        self.assertEqual(outcome.status, steps.PASSED)
        self.assertIn('DRY RUN: CREATE DATABASE `%s_src`' % PREFIX, h.text())
        self.assertFalse((h.tmp / 'runs' / h.run_id / 'rehearsal').exists())

    def test_the_sigkill_strategy_kills_a_copy_process_with_a_chunk_started(self):
        h = Harness(self)
        self.assertEqual(h.step('O5').status, steps.PASSED)
        h.fake.insert_delay = 5
        started = rehearse.kill_sigkill(h.db, h.plans(), h.journal, h.ctx().runner_connector(), h.log, h.config.copy, wait_seconds=20)
        self.assertEqual(len(started), 1)
        self.assertEqual([s for _, _, s, _, _ in h.journal.chunk_states() if s == 'started'], ['started'])


class SampleDatabaseTests(unittest.TestCase):
    def test_w12_sample_database_issues_one_insert_per_table_with_the_predicate(self):
        """sample-database SOURCE NAME --percent 10 on the fake: the layout of the source, every table by LIKE, one INSERT ... SELECT per table
        with CRC32 of the key MOD 100 under 10 (the key columns, or every column of a keyless table), a table under
        copy.min_rows_for_chunking whole; --tables limits the rows to the named tables; a run name is refused."""
        h = Harness(self, mutate=lambda d: d['copy'].update(min_rows_for_chunking=100))
        fake_driver(self, h.fake)
        base = ['--config', str(h.config_path)]
        code, text = run_cli(['sample-database', SRC, 'sample_db', '--percent', '10'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('CREATE DATABASE `sample_db` PARTITIONS 16 SUB_PARTITIONS 0', h.fake.writes)
        self.assertEqual(len([w for w in h.fake.writes if w.startswith('CREATE TABLE `sample_db`.') and ' LIKE `%s`.' % SRC in w]), 4)
        inserts = [w for w in h.fake.writes if w.startswith('INSERT INTO `sample_db`.')]
        self.assertEqual(len(inserts), 4, 'one INSERT per table')
        self.assertEqual(inserts[0], 'INSERT INTO `sample_db`.`t_single` (`id`, `payload`, `n`) SELECT `id`, `payload`, `n` FROM `%s`.`t_single` '
                                     'WHERE CRC32(`id`) MOD 100 < 10' % SRC)
        self.assertTrue(inserts[1].endswith("FROM `%s`.`t_nokey` WHERE CRC32(CONCAT_WS('|', COALESCE(`x`, ''), COALESCE(`y`, ''), COALESCE(`z`, ''))) "
                                            'MOD 100 < 10' % SRC), 'a keyless table is sampled by its row hash (largest table first)')
        self.assertTrue(inserts[2].endswith("FROM `%s`.`t_comp` WHERE CRC32(CONCAT_WS('|', `a`, `b`, `c`)) MOD 100 < 10" % SRC), inserts[2])
        self.assertTrue(inserts[3].endswith('FROM `%s`.`t_empty`' % SRC), 'a table under copy.min_rows_for_chunking is copied whole')
        expected = [r for r in h.fake.rows(SRC, 't_single') if evaluate_predicate('CRC32(`id`) MOD 100 < 10', r)]
        self.assertEqual(multiset(h.fake.rows('sample_db', 't_single')), multiset(expected))
        self.assertTrue(0 < len(expected) < 700)
        self.assertIn('sample sample_db built from %s at 10 percent: 4 tables (3 sampled by CRC32 of the key, 1 whole), 0 of 0 views, 0 of 0 routines; '
                      'views and routines need the owner profile' % SRC, text)
        self.assertIn('    t_single: %d rows (sampled)' % len(expected), text)
        code, text = run_cli(['sample-database', SRC, 'sample_two', '--percent', '10', '--tables', 't_comp'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertEqual([w for w in h.fake.writes if w.startswith('INSERT INTO `sample_two`.')],
                         ['INSERT INTO `sample_two`.`t_comp` (`a`, `b`, `c`, `note`) SELECT `a`, `b`, `c`, `note` FROM `%s`.`t_comp` '
                          "WHERE CRC32(CONCAT_WS('|', `a`, `b`, `c`)) MOD 100 < 10" % SRC])
        self.assertEqual(sorted(h.fake.databases['sample_two']['tables']), ['t_comp', 't_empty', 't_nokey', 't_single'], 'every table is created')
        before = list(h.fake.writes)
        code, text = run_cli(['sample-database', SRC, TGT, '--percent', '10'] + base, env=h.env)
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('a database of the configured run', text)
        code, text = run_cli(['sample-database', SRC, 'sample_three', '--percent', '0'] + base, env=h.env)
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('--percent must be a whole number from 1 to 100', text)
        code, text = run_cli(['sample-database', SRC, 'sample_three', '--percent', '10', '--tables', 'nope'] + base, env=h.env)
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('--tables names nope', text)
        self.assertEqual(h.fake.writes, before, 'a refusal issues nothing')
        code, text = run_cli(['sample-database', SRC, 'sample_three', '--percent', '10', '--dry-run'] + base, env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('DRY RUN: sample-database would capture %s and build sample_three as a 10 percent sample' % SRC, text)
        self.assertEqual(h.fake.writes, before)

    def test_w12_a_reference_table_in_the_sample_goes_through_staging(self):
        """A reference table is copied whole: from a READ ONLY source through a sharded staging table in the sample (a plain INSERT would fail
        with 1777), from a read-write source by one direct INSERT."""
        rows = fixture_rows()
        rows['t_ref'] = [{'k': i, 'label': 'l%d' % i} for i in range(7)]
        h = Harness(self, tables=REF_TABLES, rows=rows)
        h.fake.attachment(SRC)['ATTACH_TYPE'] = 'READONLY'
        summary = sample.build(h.sessions.runner, None, SRC, 'sample_ref', 50, h.config, h.log)
        self.assertEqual((summary['read_only'], summary['rows']['t_ref']), (True, 7))
        self.assertIn('t_ref', summary['whole'])
        writes = [w for w in h.fake.writes if 't_ref' in w and ' LIKE ' not in w]
        self.assertTrue(writes[0].startswith('CREATE TABLE `sample_ref`.`t_ref__stage` (`k` int(11) DEFAULT NULL, `label` varchar(20)'), writes)
        self.assertEqual(writes[1], 'INSERT INTO `sample_ref`.`t_ref__stage` (`k`, `label`) SELECT `k`, `label` FROM `%s`.`t_ref` WHERE '
                                    "CRC32(CONCAT_WS('|', COALESCE(`k`, ''), COALESCE(`label`, ''))) MOD 1 = 0" % SRC)
        self.assertEqual(writes[2], 'INSERT INTO `sample_ref`.`t_ref` (`k`, `label`) SELECT `k`, `label` FROM `sample_ref`.`t_ref__stage`')
        self.assertEqual(writes[3], 'DROP TABLE `sample_ref`.`t_ref__stage`')
        self.assertEqual(len(writes), 4)
        self.assertEqual(multiset(h.fake.rows('sample_ref', 't_ref')), multiset(h.fake.rows(SRC, 't_ref')))
        self.assertNotIn('t_ref__stage', h.fake.databases['sample_ref']['tables'], 'the staging table is dropped')
        self.assertIn('t_ref: 7 rows, whole (reference table, through the staging table t_ref__stage)', h.text())
        h.fake.attachment(SRC)['ATTACH_TYPE'] = 'READWRITE'
        summary = sample.build(h.sessions.runner, None, SRC, 'sample_rw', 50, h.config, h.log)
        self.assertEqual((summary['read_only'], summary['rows']['t_ref']), (False, 7))
        direct = [w for w in h.fake.writes if 'sample_rw' in w and 't_ref' in w and ' LIKE ' not in w]
        self.assertEqual(direct, ['INSERT INTO `sample_rw`.`t_ref` (`k`, `label`) SELECT `k`, `label` FROM `%s`.`t_ref`' % SRC],
                         'a reference table of a read-write source is one direct INSERT, whole')


class M4CliTests(unittest.TestCase):
    def setUp(self):
        self.h = owner_harness(self)
        self.h.journal.record_attachment('source', SRC, '1028600672070281646:7367158985892520834', 'cid-1', 'READWRITE')
        write_o2(self.h, [ITEM])
        self.h.journal.close()
        self.base = ['--config', str(self.h.config_path), '--run', self.h.run_id]

    def cli(self, *args):
        return run_cli(list(args) + self.base, env=self.h.env)

    def test_owner_sql_prints_every_admin_block(self):
        h = self.h
        for step_id in steps.OWNER_SQL_STEPS:
            code, text = self.cli('owner-sql', step_id)
            self.assertEqual(code, EXIT_DONE, (step_id, text))
            self.assertIn('-- %s:' % step_id, text)
            self.assertIn('-- CHECK', text)
            path = h.tmp / 'runs' / h.run_id / SRC / 'owner-sql' / ('%s.sql' % step_id)
            self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600, step_id)
        code, text = self.cli('owner-sql', 'O10', '--rollback')
        self.assertEqual(code, EXIT_DONE)
        self.assertIn('Rollback R1 and R2a', text)
        self.assertIn('Rollback R1 and R2b, R2c', text)
        code, text = self.cli('owner-sql', 'O6')
        self.assertEqual(code, EXIT_REFUSED)
        code, text = self.cli('owner-sql', 'O3', '--rollback')
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('--rollback applies to O10 only', text)
        code, text = run_cli(['owner-sql', 'O2', '--config', str(h.config_path), '--run', '20991231-0000'], env=h.env)
        self.assertEqual(code, EXIT_REFUSED, 'a mistyped run is refused')
        self.assertFalse((h.tmp / 'runs' / '20991231-0000').exists())
        (h.tmp / 'runs' / h.run_id / SRC / 'owner-sql' / 'O11.sql').unlink()
        code, text = self.cli('owner-sql', 'O11', '--dry-run')
        self.assertEqual(code, EXIT_DONE)
        self.assertFalse((h.tmp / 'runs' / h.run_id / SRC / 'owner-sql' / 'O11.sql').exists(), 'a dry run writes no file')

    def test_f2_owner_sql_o3_writes_the_reentry_block_beside_the_whole_block(self):
        """owner-sql O3 (Path A) writes and prints O3.sql and O3-reentry.sql; after a stop between DETACH and ATTACH the hand route runs O3-reentry once.

        owner-sql opens no session, so it writes both blocks; the checks of each refuse the wrong
        state. The stop comes from each fault point: killed just before the ATTACH, the DETACH
        reply lost, or killed after the DETACH and before its owner_statement event. The printed
        re-entry block run by hand as the admin (ownerlib on an admin cursor) attaches once, and
        step O3 --redo then resumes at the probe. Path B writes no re-entry file.
        """
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                h = o3_harness(self)
                fail_o3_at(self, h, point, O3_STOP_WORD[point])
                assert_between_detach_and_attach(self, h)
                h.journal.close()
                code, text = run_cli(['owner-sql', 'O3', '--config', str(h.config_path), '--run', h.run_id], env=h.env)
                self.assertEqual(code, EXIT_DONE, text)
                folder = h.tmp / 'runs' / h.run_id / SRC / 'owner-sql'
                for name in ('O3.sql', 'O3-reentry.sql'):
                    self.assertEqual(stat.S_IMODE(os.stat(folder / name).st_mode), 0o600, name)
                whole, reentry = (folder / 'O3.sql').read_text(), (folder / 'O3-reentry.sql').read_text()
                self.assertIn('-- O3: Freeze: read-only attachment (Path A)', whole)
                self.assertIn('-- NOTE: ' + freeze.REENTRY_NOTE, whole)
                self.assertIn('-- O3: Freeze: re-entry between DETACH and ATTACH (Path A)', reentry)
                self.assertNotIn('DETACH DATABASE `%s`;' % SRC, reentry, 'the re-entry block starts at the ATTACH')
                self.assertLess(text.index(whole), text.index(reentry), 'both blocks are printed, the whole block first')
                h.journal = Journal(h.journal_path)
                self.addCleanup(h.journal.close)
                block = freeze.reentry_block(names_of(h))
                self.assertEqual(reentry, block.render(), 'the hand run below runs the written file')
                result = run_dict(h, block.to_dict())
                self.assertEqual((result['status'], result['statements']), ('passed', 1), result)
                out = h.step('O3', redo=True)
                self.assertEqual(out.status, steps.PASSED, h.text())
                self.assertIn('O3 resumed', out.note)
                assert_frozen_once(self, h)
        h = owner_harness(self, mutate=lambda d: d.update(freeze={'mode': 'lockout'}))
        h.journal.close()
        code, text = run_cli(['owner-sql', 'O3', '--config', str(h.config_path), '--run', h.run_id], env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertNotIn('re-entry', text)
        self.assertFalse((h.tmp / 'runs' / h.run_id / SRC / 'owner-sql' / 'O3-reentry.sql').exists(), 'Path B has no re-entry block')

    def test_owner_sql_o3_in_lockout_mode_prints_the_lockout_plan(self):
        h = owner_harness(self, mutate=lambda d: d.update(freeze={'mode': 'lockout'}))
        grants.record_snapshot(h.journal, {"'app'@'%'": {'*.*': ['INSERT'], '`%s`.*' % SRC: ['DELETE']}})
        h.journal.close()
        code, text = run_cli(['owner-sql', 'O3', '--config', str(h.config_path), '--run', h.run_id], env=h.env)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('-- O3: Freeze: lockout (Path B)', text)
        self.assertIn("REVOKE DELETE ON `%s`.* FROM 'app'@'%%';" % SRC, text)
        self.assertNotIn('DETACH DATABASE', text)

    def test_owner_notebook_and_account_sql(self):
        h = self.h
        code, text = self.cli('owner-notebook')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('mode 0600, first cell compiles; pipeline capture from O2', text)
        self.assertEqual(stat.S_IMODE(os.stat(h.tmp / 'runs' / h.run_id / 'owner-steps.ipynb').st_mode), 0o600)
        code, text = self.cli('account-sql')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn("CREATE USER 's2rebuild_runner'@'%' IDENTIFIED BY '<password>';", text)
        code, text = self.cli('account-sql', '--drop')
        self.assertIn("DROP USER 's2rebuild_runner'@'%';", text)
        code, text = self.cli('account-sql', '--drop', '--execute', '--dry-run')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('DRY RUN: --execute would run the block above with the owner profile; no connection was opened', text)

    def test_lockout_validates_until_before_anything(self):
        code, text = self.cli('lockout', '--until', '2020-01-01T00:00:00Z')
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('is not in the future', text)
        code, text = self.cli('lockout', '--dry-run')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('Path B lockout needs the owner profile', text)

    def test_rehearse_flags(self):
        code, text = self.cli('rehearse', '--dry-run')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('DRY RUN: CREATE DATABASE `s2rebuild_rehearsal_%s_src`' % self.h.run_id.replace('-', '_'), text)
        code, text = self.cli('rehearse', '--dry-run', '--cleanup', '--prefix', PREFIX)
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('DRY RUN: DROP DATABASE `%s_src`' % PREFIX, text)
        code, text = self.cli('rehearse', '--dry-run', '--prefix', 'rehearsal_x')
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('must start with s2rebuild_rehearsal_', text)
        code, text = self.cli('rehearse', '--dry-run', '--from-database', SRC, '--percent', '5')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('DRY RUN: the rehearsal would build s2rebuild_rehearsal_%s_src as a 5 percent sample of %s' % (self.h.run_id.replace('-', '_'), SRC), text)
        self.assertNotIn('CREATE DATABASE', text, 'the sample statements need the capture, which a dry run does not read')
        code, text = self.cli('rehearse', '--dry-run', '--from-database', SRC, '--percent', '101')
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('--percent must be a whole number from 1 to 100', text)

    def test_gate_imports_what_a_notebook_result_carries(self):
        h = self.h
        capture = {'database': SRC, 'states': [{'PIPELINE_NAME': 'p9', 'STATE': 'Stopped'}],
                   'definitions': {'p9': [{'Pipeline': 'p9', 'Create Pipeline': "CREATE PIPELINE `p9` AS LOAD DATA KAFKA 'b/t' CREDENTIALS <CREDENTIALS REDACTED> INTO TABLE `t_single`"},
                                          {'Pipeline': 'p9', 'Create Pipeline': "ALTER PIPELINE `p9` SET OFFSETS '{\"0\": 3}'"}]},
                   'offsets': [{'PIPELINE_NAME': 'p9', 'SOURCE_TYPE': 'KAFKA', 'SOURCE_PARTITION_ID': '0', 'EARLIEST_OFFSET': '1', 'LATEST_OFFSET': '3',
                                'LATEST_LOADED_OFFSET': '3', 'LATEST_SUCCESSFUL_OFFSET': '3'}]}
        evidence = h.tmp / 'o2-result.json'
        evidence.write_text(json.dumps({'step': 'O2', 'run_id': h.run_id, 'status': 'passed', 'capture': capture,
                                        'attachments': [{'role': 'retained', 'row': {'DATABASE_NAME': 'r', 'STORAGE_ID': 'sid-77', 'ATTACH_TYPE': 'READONLY'}}]}))
        code, text = self.cli('gate', 'O2', '--pass', '--note', 'notebook stop', '--evidence', str(evidence))
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('O2 pipeline capture imported', text)
        j = Journal(h.journal_path)
        self.addCleanup(j.close)
        self.assertEqual(j.attachment('retained')['storage_id'], 'sid-77')
        o2 = inventory.load(inventory.inventory_dir(h.config, h.run_id, h.db, 'O2'))
        self.assertEqual([i['name'] for i in o2['pipelines']['items']], ['p9'])
        self.assertIn("CREDENTIALS '<<CREDENTIALS>>'", o2['pipelines']['items'][0]['definition'])
        self.assertEqual(o2['pipelines']['items'][0]['offsets'][0]['loaded'], 3)
        wrong = h.tmp / 'wrong.json'
        wrong.write_text(json.dumps({'step': 'O9', 'status': 'passed'}))
        code, text = self.cli('gate', 'O2', '--pass', '--note', 'x', '--evidence', str(wrong))
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('the result of step O9, not O2', text)
        failed = h.tmp / 'failed.json'
        failed.write_text(json.dumps({'step': 'O10', 'status': 'FAILED at check S1 before'}))
        code, text = self.cli('gate', 'O10', '--pass', '--note', 'x', '--evidence', str(failed))
        self.assertEqual(code, EXIT_REFUSED)
        self.assertIn('a gate cannot pass on it', text)
        snap = h.tmp / 'o3.json'
        snap.write_text(json.dumps({'step': 'O3', 'run_id': h.run_id, 'grant_snapshot': {"'app'@'%'": {'*.*': ['INSERT']}}}))
        code, text = self.cli('gate', 'O3', '--pass', '--note', 'notebook lockout', '--evidence', str(snap))
        self.assertEqual(code, EXIT_DONE, text)
        self.assertEqual(grants.snapshot_from_journal(j), {"'app'@'%'": {'*.*': ['INSERT']}})
        code, text = self.cli('gate', 'O11', '--pass', '--note', 'released by hand')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIsNotNone(j.get_meta('released_utc'), 'the release recorded by hand starts the C1 window')

    def test_run_dry_run_prints_the_admin_blocks(self):
        code, text = self.cli('run', '--dry-run', '--redo', '--from', 'O2', '--to', 'O3')
        self.assertEqual(code, EXIT_DONE, text)
        self.assertIn('STOP PIPELINE `%s`.`p1`;' % SRC, text)
        self.assertIn('DETACH DATABASE `%s`;' % SRC, text)
        self.assertIn('gates pending: O2 example_orders_db, O3 example_orders_db', text)


if __name__ == '__main__':
    unittest.main()
