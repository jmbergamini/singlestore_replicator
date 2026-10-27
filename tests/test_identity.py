"""s2rebuild tests test_identity: a database is its branch record (name, STORAGE_ID, ATTACH_TYPE, COMPUTE_ID), never a STORAGE_ID alone.

Author: John Bergamini

Helios 10.8.0, October 27, 2026: a branch keeps its parent's STORAGE_ID and every ATTACH ... AS
leaves the old record behind, so the switch, the rollback, C3 and abandon identify a database by
its record. The fake's branch records are on (its defaults).
"""
import sqlite3
import tempfile
import unittest
from pathlib import Path

from s2rebuild import abandon, ownersql, switch
from s2rebuild.common import KitError
from s2rebuild.journal import COLUMNS, Journal, create_table_sql
from s2rebuild.sqlutil import read_guard
from tests.test_abandon import frozen_with_replacement
from tests.test_copy import SRC, TGT
from tests.test_switch import RET, SID, machine_for, old_kit_switch, switched_harness


def attach_writes(h):
    """Every DETACH, ATTACH and DROP the fake applied, in order."""
    return [w for w in h.fake.writes if w.startswith(('DETACH', 'ATTACH', 'DROP'))]


def live_record(h, name):
    """identify(name) through a runner session on the harness."""
    session = h.sessions.runner(read_guard())
    try:
        return switch.identify(session, name)
    finally:
        session.close()


class RecordIdentityTests(unittest.TestCase):
    def test_rollback_detaches_the_live_branch_not_the_parent_and_re_attaches_the_original_by_its_syntax(self):
        """R1 detaches the live pitr record and leaves its parent, the replacement, attached; R2a runs the detached original's own ATTACH_SYNTAX.

        A switch made without S2b (old_kit_switch) still has the detached original record, so the rollback takes R2a.
        """
        h = switched_harness(self)
        machine, names = old_kit_switch(h)
        self.assertEqual(machine.position(), switch.SWITCHED, h.text())
        detached_original = h.journal.attachment('source')
        self.assertEqual((detached_original['record_type'], detached_original['is_detached'], detached_original['compute_id']), ('create', 1, 'cid-1'))
        self.assertTrue(detached_original['attach_syntax'].startswith("ATTACH DATABASE `%s` AT MILESTONE '" % SRC), 'S1 recorded the detached record')
        parent = dict(h.fake.attachment(TGT))
        self.assertEqual(h.fake.attachment(SRC)['STORAGE_ID'], parent['STORAGE_ID'], 'the live name is a branch on the parent storage')
        self.assertEqual(machine.rollback(), switch.ORIGINAL_ATTACHED, h.text())
        self.assertEqual(h.fake.attachment(TGT), parent, 'the parent is untouched')
        live = next(b for b in h.fake.branches if b['DATABASE_NAME'] == SRC and b['ATTACH_TYPE'] == 'pitr')
        self.assertEqual((live['IS_DETACHED'], live['IS_DROPPED']), (1, 0), 'the live branch is detached, not dropped')
        source = h.fake.attachment(SRC)
        self.assertEqual((source['ATTACH_TYPE'], source['STORAGE_ID'], source['COMPUTE_ID']), ('READWRITE', SID, 'cid-1'))
        writes = attach_writes(h)
        self.assertEqual(writes[-2], 'DETACH DATABASE `%s`' % SRC)
        self.assertEqual(writes[-1], detached_original['attach_syntax'], 'R2a is the recorded ATTACH_SYNTAX, not ATTACH ... STORAGEID')
        self.assertEqual(h.fake.attachment(RET)['ATTACH_TYPE'], 'READONLY', 'no rollback step drops the retained copy')
        self.assertEqual(h.journal.attachment('source')['is_detached'], 0, 'R2a recorded the original attached again')

    def test_a_statement_refuses_when_the_record_differs_and_issues_nothing(self):
        """Another actor replaced the live record by a new branch of the replacement: R1 refuses with both records printed and detaches nothing;
        a source record that differs from the journal refuses S1 before the DETACH."""
        h = switched_harness(self)
        machine, names = old_kit_switch(h)
        recorded = h.journal.attachment('live')
        cur = h.fake.connect('admin').cursor()
        cur.execute('DETACH DATABASE `%s`' % SRC)
        cur.execute('ATTACH DATABASE `%s` AS `%s` STORAGEID %s' % (TGT, SRC, names['replacement_sid']))
        other = h.fake.attachment(SRC)
        self.assertEqual(other['STORAGE_ID'], recorded['storage_id'], 'the same STORAGE_ID')
        self.assertNotEqual(other['COMPUTE_ID'], recorded['compute_id'], 'another record')
        before = attach_writes(h)
        with self.assertRaisesRegex(KitError, 'the record differs, so nothing was issued. Expected: name %s, STORAGE_ID %s, ATTACH_TYPE pitr, COMPUTE_ID %s '
                                    r'\(attached READWRITE\). Observed: name %s, STORAGE_ID %s, ATTACH_TYPE pitr, COMPUTE_ID %s' % (
                                        SRC, recorded['storage_id'], recorded['compute_id'], SRC, other['STORAGE_ID'], other['COMPUTE_ID'])):
            machine.rollback()
        self.assertEqual(attach_writes(h), before, 'nothing was detached or attached')
        self.assertEqual(h.fake.attachment(SRC), other)
        failed = [e for e in h.journal.events('switch_check') if e.startswith('R1 before') and 'FAILED' in e]
        self.assertEqual(len(failed), 1)
        h2 = switched_harness(self)
        h2.journal.record_attachment('source', SRC, SID, 'cid-other', 'READONLY', record_type='create', is_detached=0)
        machine2, _ = machine_for(h2)
        with self.assertRaisesRegex(KitError, 'S1 before: the source is the recorded record.*Expected: name %s, STORAGE_ID %s, ATTACH_TYPE create, '
                                    'COMPUTE_ID cid-other.*Observed: name %s, STORAGE_ID %s, ATTACH_TYPE create, COMPUTE_ID cid-1' % (SRC, SID, SRC, SID)):
            machine2.s1()
        self.assertEqual(attach_writes(h2), [], 'the source stays attached')

    def test_abandon_refuses_a_replacement_that_has_a_branch(self):
        """A branch made from the replacement means a switch attached the live name from it: abandon stops before any statement."""
        h = frozen_with_replacement(self)
        rsid = h.fake.attachment(TGT)['STORAGE_ID']
        h.fake.connect('admin').cursor().execute('ATTACH DATABASE `%s` AS `%s_branch` STORAGEID %s' % (TGT, TGT, rsid))
        before = list(h.fake.writes)
        with self.assertRaisesRegex(KitError, 'abandon stopped at check "the replacement has no branch": expected n = 0, observed n = 1'):
            abandon.run(h.ctx(), 'branched', execute=True)
        self.assertEqual(h.fake.writes, before, 'nothing detached, attached or dropped')
        self.assertEqual(h.fake.attachment(SRC)['ATTACH_TYPE'], 'READONLY')
        self.assertIsNotNone(h.fake.attachment(TGT))
        self.assertIsNone(h.journal.get_meta('abandoned_utc'))

    def test_a_second_record_under_the_live_name_never_matches_the_live_record(self):
        """After a switch without S2b the live name carries two records, the detached original (create) and the live branch (pitr); identify
        returns the attached one, even when the original is expected, and the two never match."""
        h = switched_harness(self)
        machine, names = old_kit_switch(h)
        session = h.sessions.runner(read_guard())
        try:
            found = switch.records(session, SRC)
            self.assertEqual([(r['ATTACH_TYPE'], r['IS_DETACHED'], r['ACCESS']) for r in found], [('pitr', 0, 'READWRITE'), ('create', 1, None)])
            original = h.journal.attachment('source')
            self.assertEqual(switch.record_key(original), (SRC, SID, 'create', 'cid-1'))
            live = switch.identify(session, SRC, expected=original)
        finally:
            session.close()
        self.assertEqual((live['ATTACH_TYPE'], live['STORAGE_ID'], live['COMPUTE_ID']), ('pitr', names['replacement_sid'], h.fake.attachment(SRC)['COMPUTE_ID']))
        self.assertFalse(switch.records_match(original, live))
        self.assertFalse(switch.records_match(live, original))
        self.assertTrue(switch.records_match(h.journal.attachment('live'), live))
        self.assertTrue(switch.records_match(original, found[1]), 'the detached original is the recorded source record')
        self.assertFalse(switch.records_match({'DATABASE_NAME': SRC, 'STORAGE_ID': SID, 'ATTACH_TYPE': 'create', 'COMPUTE_ID': None}, found[1]),
                         'an unknown part never matches')

    def test_a_row_number_change_between_two_listings_changes_nothing(self):
        """ROW_NUMBER is renumbered on every listing; identify reads none of it and returns the same record twice."""
        h = switched_harness(self)
        machine, _ = machine_for(h)
        machine.forward()
        session = h.sessions.runner(read_guard())
        try:
            numbers = []
            for _ in range(2):
                rows = session.query('SELECT ROW_NUMBER, DATABASE_NAME FROM information_schema.MV_CLOUD_BRANCHES WHERE DATABASE_NAME = %s', (SRC,))
                numbers.append(sorted(r['ROW_NUMBER'] for r in rows))
            self.assertNotEqual(numbers[0], numbers[1])
            first, second = switch.identify(session, SRC), switch.identify(session, SRC)
            retained = [switch.identify(session, RET) for _ in range(2)]
        finally:
            session.close()
        self.assertEqual(first, second)
        self.assertNotIn('ROW_NUMBER', first)
        self.assertEqual(retained[0], retained[1])
        self.assertEqual((retained[0]['ATTACH_TYPE'], retained[0]['ACCESS'], retained[0]['PREV_DATABASE_NAME']), ('resume', 'READONLY', SRC))
        self.assertTrue(switch.records_match(first, second))

    def test_the_journal_round_trips_a_record_and_upgrades_a_version_2_journal(self):
        """A version 2 journal keeps its attachment rows readable (record fields unknown); a record stored whole comes back whole, and a row of
        either view completes the latest row of the same record."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'v2.sqlite'
            conn = sqlite3.connect(str(path))
            for name in COLUMNS:
                if name != 'attachments':
                    conn.execute(create_table_sql(name))
            conn.execute('CREATE TABLE attachments (id INTEGER PRIMARY KEY AUTOINCREMENT, role TEXT NOT NULL, database_name TEXT NOT NULL, '
                         'storage_id TEXT, compute_id TEXT, attach_type TEXT, observed_utc TEXT NOT NULL)')
            conn.execute("INSERT INTO meta VALUES ('schema_version', '2')")
            conn.execute("INSERT INTO attachments (role, database_name, storage_id, compute_id, attach_type, observed_utc) "
                         "VALUES ('source', 'src_db', 'sid-1', 'cid-1', 'READWRITE', '2026-10-27T00:00:00Z')")
            conn.commit()
            conn.close()
            j = Journal(path)
            self.addCleanup(j.close)
            self.assertEqual(j.schema_version(), 3)
            old = j.attachment('source')
            self.assertEqual((old['storage_id'], old['compute_id'], old['attach_type']), ('sid-1', 'cid-1', 'READWRITE'))
            self.assertEqual((old['record_type'], old['attach_syntax'], old['is_detached']), (None, None, None), 'a version 2 row has no record')
            self.assertIsNone(switch.record_key(old), 'an unknown record type never matches')
            record = {'DATABASE_NAME': 'src_db', 'STORAGE_ID': 'sid-1', 'ATTACH_TYPE': 'create', 'COMPUTE_ID': 'cid-1', 'ACCESS': 'READONLY',
                      'PREV_DATABASE_NAME': None, 'IS_DETACHED': 0, 'IS_DROPPED': 0, 'END_VERSION': None,
                      'ATTACH_SYNTAX': 'ATTACH DATABASE `src_db` STORAGEID sid-1 COMPUTEID cid-1'}
            j.record_attachment_row('source', record)
            row = j.attachment('source')
            self.assertEqual((row['database_name'], row['storage_id'], row['compute_id'], row['attach_type'], row['record_type'], row['attach_syntax'],
                              row['is_detached']), ('src_db', 'sid-1', 'cid-1', 'READONLY', 'create', record['ATTACH_SYNTAX'], 0))
            self.assertTrue(switch.records_match(row, record))
            j.record_attachment_row('live', {'DATABASE_NAME': 'src_db', 'ATTACH_TYPE': 'READWRITE', 'STORAGE_ID': 'sid-2', 'COMPUTE_ID': 'cid-2',
                                             'ATTACH_SYNTAX': 'ATTACH DATABASE `src_db` STORAGEID sid-2 COMPUTEID cid-2'})
            j.record_attachment_row('live', {'DATABASE_NAME': 'src_db', 'STORAGE_ID': 'sid-2', 'ATTACH_TYPE': 'pitr', 'LATEST_COMPUTE_ID': 'cid-2',
                                             'IS_DETACHED': 0, 'IS_DROPPED': 0, 'ATTACH_SYNTAX': 'ATTACH DATABASE `src_db` STORAGEID sid-2 COMPUTEID cid-2'})
            live = j.attachment('live')
            self.assertEqual((live['attach_type'], live['record_type'], live['compute_id'], live['is_detached']), ('READWRITE', 'pitr', 'cid-2', 0),
                             'the branch row completes the attachment row of the same record')
            j.record_attachment_row('live', {'DATABASE_NAME': 'src_db', 'STORAGE_ID': 'sid-2', 'ATTACH_TYPE': 'pitr', 'LATEST_COMPUTE_ID': 'cid-3',
                                             'IS_DETACHED': 1, 'IS_DROPPED': 0, 'ATTACH_SYNTAX': 'x'})
            self.assertIsNone(j.attachment('live')['attach_type'], 'another record takes nothing over')
            with self.assertRaisesRegex(KitError, 'Unknown branch record type'):
                j.record_attachment('live', 'src_db', record_type='branch')

    def test_the_rollback_block_carries_the_recorded_syntax_after_s1_and_a_placeholder_before(self):
        """Before S1 the printed R2 statement holds a milestone placeholder and the note; after S1 it is the detached original's own ATTACH_SYNTAX."""
        h = switched_harness(self)
        ctx = h.ctx()

        def rollback_block():
            return ownersql.block_for('O10', ownersql.names_for(ctx), h.config, rollback=True, **ownersql.block_inputs(ctx))
        before = rollback_block()
        statements = [item.sql for item in before.statements]
        self.assertEqual(statements[1], "ATTACH DATABASE `%s` AT MILESTONE '<source_milestone>' STORAGEID %s COMPUTEID cid-1" % (SRC, SID))
        self.assertIn('Generate this block again after S1', before.render())
        self.assertEqual(ownersql.placeholders(before.render()), ['<live_cid>', '<live_sid>', '<source_milestone>'])
        ownersql.guard_check_block(before, ctx)
        machine, _ = machine_for(h)
        machine.s1()
        after = rollback_block()
        syntax = [item.sql for item in after.statements][1]
        self.assertEqual(syntax, h.journal.attachment('source')['attach_syntax'])
        self.assertTrue(syntax.startswith("ATTACH DATABASE `%s` AT MILESTONE '" % SRC) and syntax.endswith(' STORAGEID %s COMPUTEID cid-1' % SID))
        self.assertNotIn('Generate this block again after S1', after.render())
        self.assertIn('-- CHECK R2a before: the original record is detached with its ATTACH_SYNTAX; expect LATEST_COMPUTE_ID = cid-1, ATTACH_TYPE = create',
                      after.render())
        self.assertIn('never deleted in the Portal by STORAGE_ID', after.render())


if __name__ == '__main__':
    unittest.main()
