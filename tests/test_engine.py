"""s2rebuild tests test_engine: the column assertion over the kit's queries, the ATTACH forms and the branch records.

Author: John Bergamini
"""
import inspect
import unittest

from s2rebuild import abandon, copy as copy_module, freeze, grants, inventory, ownersql, pipelines, postcopy, routines, steps, switch, verify
from s2rebuild.sqlutil import GuardError, classify
from tests.fakedb import CAPTURE, FakeDisconnect, check_columns
from tests.helpers import config_from
from tests.test_copy import SRC, fake_db, fixture_rows

ENV = {'S2REBUILD_RUNNER_PASSWORD': 'fixture-password', 'S2REBUILD_OWNER_PASSWORD': 'owner-fixture'}
SID = '1028600672070281646:7367158985892520834'
T_COMP_ROWS = len(fixture_rows()['t_comp'])
MODULES = (inventory, pipelines, switch, freeze, ownersql, postcopy, grants, abandon, copy_module, verify, routines)


def kit_queries():
    """Every information_schema query the kit holds as a module constant or returns from a zero-argument function: (module, name, sql)."""
    out = []
    for module in MODULES:
        for name, value in sorted(vars(module).items()):
            if name.isupper() and isinstance(value, str) and 'information_schema.' in value and value.upper().startswith(('SELECT', 'SHOW')):
                out.append((module.__name__, name, value))
            elif callable(value) and name.endswith('_sql') and not inspect.signature(value).parameters:
                sql = value()
                if isinstance(sql, str) and 'information_schema.' in sql:
                    out.append((module.__name__, name, sql))
    return out


def modeled_switch(fake):
    """O10 on the fake as the owner: create the replacement, detach the source, retain it READ ONLY, attach the replacement under the live name."""
    cur = fake.connect('ops_owner').cursor()
    cur.execute('CREATE DATABASE `src_rebuild` PARTITIONS 16 SUB_PARTITIONS 0')
    cur.execute('CREATE TABLE `src_rebuild`.`t_comp` LIKE `%s`.`t_comp`' % SRC)
    rsid = fake.attachment('src_rebuild')['STORAGE_ID']
    cur.execute('DETACH DATABASE `%s`' % SRC)
    cur.execute('ATTACH DATABASE `%s` AS `src_retained` READ ONLY STORAGEID %s' % (SRC, SID))
    cur.execute('ATTACH DATABASE `src_rebuild` AS `%s` STORAGEID %s' % (SRC, rsid))
    return cur, rsid


class CaptureTests(unittest.TestCase):
    def test_every_kit_query_names_only_captured_columns(self):
        """The inventory, pipeline, switch, freeze, branch and owner-block queries name columns the capture lists; an absent column is 1054."""
        queries = kit_queries()
        self.assertGreaterEqual(len(queries), 40, [q[:2] for q in queries])
        names = {q[1] for q in queries}
        for wanted in ('ATTACHMENT_SQL', 'BATCHES_SQL', 'ERRORS_SQL', 'OFFSETS_SQL', 'BRANCH_SQL', 'BRANCH_OF_SQL', 'PIPELINES_RUNNING_SQL',
                       'attached_elsewhere_sql', 'inventory_attachment_sql', 'other_storage_sql'):
            self.assertIn(wanted, names)
        problems = []
        for module, name, sql in queries:
            try:
                check_columns(sql, CAPTURE)
            except FakeDisconnect as exc:
                problems.append('%s.%s: %s' % (module, name, exc.args[1]))
        self.assertEqual(problems, [])
        with self.assertRaises(FakeDisconnect) as refused:
            check_columns('SELECT PIPELINE_NAME, ERROR_TIME FROM information_schema.PIPELINES_ERRORS WHERE DATABASE_NAME = %s', CAPTURE)
        self.assertEqual(refused.exception.args[0], 1054)
        self.assertIn('ERROR_TIME', refused.exception.args[1])
        with self.assertRaises(FakeDisconnect) as refused:
            check_columns('SELECT f.TABLE_NAME FROM information_schema.MV_COLUMNSTORE_FILES f JOIN information_schema.TABLE_STATISTICS t '
                          'ON f.DATABASE_NAME = t.DATABASE_NAME WHERE t.ORDINAL = f.PARTITION AND t.FILE = 1', CAPTURE)
        self.assertEqual(refused.exception.args[1], "Unknown column 't.FILE' in 'field list'", 'an alias binds the column to its view')
        with self.assertRaises(FakeDisconnect) as refused:
            check_columns('SELECT ID FROM information_schema.MV_NO_SUCH_VIEW', CAPTURE)
        self.assertEqual(refused.exception.args[0], 1146)


class AttachFormTests(unittest.TestCase):
    def test_the_three_attach_syntax_forms_classify_as_attach_database(self):
        """The attached, detached (AT MILESTONE) and dropped forms, with READ ONLY before AT MILESTONE, pass the O10 owner guard; wrong forms do not."""
        db = config_from().databases[0]
        cid = '2000000000000000101:8000000000000000101'
        forms = {
            'attached': 'ATTACH DATABASE `%s` STORAGEID %s COMPUTEID %s' % (db.source, SID, cid),
            'detached': "ATTACH DATABASE `%s` AT MILESTONE '2026-10-27 06_44_00.000000' STORAGEID %s COMPUTEID %s" % (db.source, SID, cid),
            'read only detached': "ATTACH DATABASE `%s` READ ONLY AT MILESTONE '2026-10-27 06_44_00.000000' STORAGEID %s COMPUTEID %s" % (db.source, SID, cid),
            'dropped': 'ATTACH DROPPED DATABASE `%s` STORAGEID %s COMPUTEID %s' % (db.source, SID, cid),
            'branch': 'ATTACH DATABASE `%s` AS `%s` STORAGEID %s' % (db.replacement, db.source, SID),
        }
        guard = steps.guard_for('O10', db, steps.OWNER)
        for label, sql in forms.items():
            with self.subTest(form=label):
                st = classify(sql)
                self.assertEqual(st.kind, 'ATTACH DATABASE')
                self.assertEqual(st.databases, (db.replacement, db.source) if label == 'branch' else (db.source,))
                self.assertEqual(st.detail, 'READ ONLY' if label.startswith('read only') else '')
                guard.check(sql)
        for template in CAPTURE['attach']['syntax_forms'].values():
            sql = template.replace('<name>', db.source).replace('<storage_id>', SID).replace('<compute_id>', cid).replace(
                '<YYYY-MM-DD HH_MM_SS.ffffff>', '2026-10-27 06_44_00.000000')
            self.assertEqual(classify(sql).kind, 'ATTACH DATABASE', template)
        for bad in ("ATTACH DATABASE `%s` STORAGEID %s READ ONLY" % (db.source, SID),
                    "ATTACH DATABASE `%s` AT MILESTONE 'now; DROP DATABASE `x`' STORAGEID %s" % (db.source, SID),
                    "ATTACH DATABASE `%s` STORAGEID '%s'" % (db.source, SID),
                    'ATTACH DROPPED DATABASE `%s` STORAGEID %s COMPUTEID %s' % ('elsewhere', SID, cid)):
            with self.subTest(sql=bad), self.assertRaises(GuardError):
                guard.check(bad)


class BranchRecordTests(unittest.TestCase):
    def test_the_portal_lists_four_entries_after_a_modeled_switch(self):
        """Two storages carry four records: the detached original (create), the retained copy (resume), the replacement (create) and the live
        branch (pitr, same STORAGE_ID, own COMPUTE_ID); writes are not shared; ROW_NUMBER changes per listing; a DROP leaves the branch whole."""
        fake = fake_db()
        cur, rsid = modeled_switch(fake)
        entries = fake.portal_entries()
        self.assertEqual(len(entries), 4)
        by = {(e['DATABASE_NAME'], e['ATTACH_TYPE']): e for e in entries}
        self.assertEqual(set(by), {(SRC, 'create'), ('src_retained', 'resume'), ('src_rebuild', 'create'), (SRC, 'pitr')})
        self.assertEqual([by[k]['IS_DETACHED'] for k in ((SRC, 'create'), ('src_retained', 'resume'), ('src_rebuild', 'create'), (SRC, 'pitr'))],
                         [1, 0, 0, 0])
        self.assertEqual((by[(SRC, 'create')]['STORAGE_ID'], by[('src_retained', 'resume')]['STORAGE_ID']), (SID, SID), 'one storage, two records')
        self.assertEqual((by[('src_rebuild', 'create')]['STORAGE_ID'], by[(SRC, 'pitr')]['STORAGE_ID']), (rsid, rsid))
        live, replacement = fake.attachment(SRC), fake.attachment('src_rebuild')
        self.assertNotEqual(live['COMPUTE_ID'], replacement['COMPUTE_ID'])
        self.assertEqual(by[(SRC, 'pitr')]['LATEST_COMPUTE_ID'], live['COMPUTE_ID'])
        self.assertEqual(by[(SRC, 'pitr')]['PREV_DATABASE_NAME'], 'src_rebuild')
        self.assertTrue(by[(SRC, 'create')]['ATTACH_SYNTAX'].startswith("ATTACH DATABASE `%s` AT MILESTONE '" % SRC), 'a detached record')
        self.assertTrue(by[(SRC, 'create')]['ATTACH_SYNTAX'].endswith(' STORAGEID %s COMPUTEID cid-1' % SID))
        self.assertEqual(by[(SRC, 'pitr')]['ATTACH_SYNTAX'], 'ATTACH DATABASE `%s` STORAGEID %s COMPUTEID %s' % (SRC, rsid, live['COMPUTE_ID']))
        self.assertEqual(classify(by[(SRC, 'create')]['ATTACH_SYNTAX']).kind, 'ATTACH DATABASE')
        cur.execute(switch.BRANCH_OF_SQL, (SRC, rsid, 'src_rebuild'))
        self.assertEqual(cur.fetchall(), [{'n': 1}], 'the kit sees the live branch record')
        fake.rows(SRC, 't_comp').append(dict(fake.rows('src_retained', 't_comp')[0], c=999999))
        self.assertEqual((len(fake.rows(SRC, 't_comp')), len(fake.rows('src_rebuild', 't_comp'))), (1, 0), 'writes are not shared between records')
        numbers = []
        for _ in range(2):
            cur.execute('SELECT ROW_NUMBER, DATABASE_NAME FROM information_schema.MV_CLOUD_BRANCHES WHERE STORAGE_ID = %s', (rsid,))
            rows = cur.fetchall()
            self.assertEqual([r['DATABASE_NAME'] for r in rows], ['src_rebuild', SRC])
            numbers.append([r['ROW_NUMBER'] for r in rows])
        self.assertNotEqual(numbers[0], numbers[1], 'ROW_NUMBER is renumbered on every listing')
        cur.execute('DROP DATABASE `src_rebuild`')
        self.assertEqual(len(fake.portal_entries()), 3, 'a dropped record leaves the Portal list')
        dropped = next(b for b in fake.branches if b['DATABASE_NAME'] == 'src_rebuild')
        self.assertEqual((dropped['IS_DROPPED'], dropped['ATTACH_SYNTAX']), (1, 'ATTACH DROPPED DATABASE `src_rebuild` STORAGEID %s COMPUTEID %s' % (
            rsid, replacement['COMPUTE_ID'])))
        self.assertEqual(fake.attachment(SRC)['ATTACH_TYPE'], 'READWRITE', 'the live branch is whole after the parent DROP')
        self.assertEqual((len(fake.rows(SRC, 't_comp')), len(fake.rows('src_retained', 't_comp'))), (1, T_COMP_ROWS))
        cur.execute('DETACH DATABASE `src_retained`')
        with self.assertRaises(FakeDisconnect) as refused:
            cur.execute('DROP DATABASE `src_retained`')
        self.assertEqual(refused.exception.args[0], 1008, 'a detached record cannot be dropped')

    def test_drop_of_a_read_only_attachment_fails_with_2720_and_a_record_re_attaches_by_its_syntax(self):
        """DROP of the READ ONLY retained copy is error 2720; the detached original comes back by its own ATTACH_SYNTAX, read write or read only."""
        fake = fake_db()
        cur = fake.connect('ops_owner').cursor()
        rows_before = len(fake.rows(SRC, 't_comp'))
        cur.execute('DETACH DATABASE `%s`' % SRC)
        cur.execute('ATTACH DATABASE `%s` AS `src_retained` READ ONLY STORAGEID %s' % (SRC, SID))
        with self.assertRaises(FakeDisconnect) as refused:
            cur.execute('DROP DATABASE `src_retained`')
        self.assertEqual(refused.exception.args, (2720, CAPTURE['errors']['2720']['message']))
        self.assertEqual(fake.attachment('src_retained')['ATTACH_TYPE'], 'READONLY', 'nothing changed')
        self.assertEqual(len(fake.portal_entries()), 2)
        original = next(b for b in fake.branches if b['DATABASE_NAME'] == SRC and b['ATTACH_TYPE'] == 'create')
        syntax = original['ATTACH_SYNTAX']
        self.assertIn(' AT MILESTONE ', syntax)
        cur.execute(syntax)
        att = fake.attachment(SRC)
        self.assertEqual((att['ATTACH_TYPE'], att['STORAGE_ID']), ('READWRITE', SID), 'the record itself is back, read write')
        self.assertNotEqual(att['COMPUTE_ID'], 'cid-1', 'with a new COMPUTE_ID, as on Helios (fact 44)')
        self.assertEqual((original['IS_DETACHED'], original['LATEST_COMPUTE_ID'], original['ATTACH_SYNTAX']),
                         (0, att['COMPUTE_ID'], 'ATTACH DATABASE `%s` STORAGEID %s COMPUTEID %s' % (SRC, SID, att['COMPUTE_ID'])))
        self.assertEqual(len(fake.rows(SRC, 't_comp')), rows_before)
        self.assertEqual(len(fake.portal_entries()), 2, 're-attaching adds no record')
        cur.execute('DETACH DATABASE `%s`' % SRC)
        cur.execute(original['ATTACH_SYNTAX'].replace('` AT MILESTONE', '` READ ONLY AT MILESTONE', 1))
        self.assertEqual(fake.attachment(SRC)['ATTACH_TYPE'], 'READONLY', 'READ ONLY before AT MILESTONE')
        with self.assertRaises(FakeDisconnect) as refused:
            cur.execute('DROP DATABASE `%s`' % SRC)
        self.assertEqual(refused.exception.args[0], 2720)
        cur.execute('DETACH DATABASE `%s`' % SRC)
        cur.execute(original['ATTACH_SYNTAX'])
        cur.execute('DROP DATABASE `%s`' % SRC)
        self.assertEqual((original['IS_DROPPED'], original['ATTACH_SYNTAX']), (1, 'ATTACH DROPPED DATABASE `%s` STORAGEID %s COMPUTEID %s' % (SRC, SID, original['LATEST_COMPUTE_ID'])))
        self.assertEqual([e['DATABASE_NAME'] for e in fake.portal_entries()], ['src_retained'], 'the resume branch is whole')
        self.assertEqual(len(fake.rows('src_retained', 't_comp')), rows_before)


if __name__ == '__main__':
    unittest.main()
