"""s2rebuild tests test_projections: projections captured with the owner session and created at O7 over the full tables (R6).

Author: John Bergamini

Two tests: the capture and the O7 creation with its gate and the S3 count; the refusal of a
definition that names a database, before any statement.
"""
import json
import unittest

from s2rebuild import ownersql, projections as P, steps, switch
from s2rebuild.sqlutil import read_guard
from tests.test_copy import SRC, TGT
from tests.test_pipelines import owner_harness

P_BY_N = {'name': 'p_by_n', 'table': 't_single', 'status': 'Sync',
          'definition': 'CREATE PROJECTION `p_by_n`( SORT KEY `n` (`n`) ) AS SELECT `id`, `n` FROM `t_single`'}


def with_projections(items):
    """An inventory mutation that adds captured projections."""
    def add(inv):
        inv['projections'] = [dict(p) for p in items]
        inv['projections_captured'] = True
    return add


class ProjectionTests(unittest.TestCase):
    def test_capture_then_o7_creates_the_projection_on_the_full_table_and_records_the_count_for_s3(self):
        """The owner capture reads PROJECTIONS and SHOW CREATE PROJECTION; O7 creates the projection after the copy, behind a USE of
        the replacement (1046 without one), the gate compares the definition with the capture, the count goes to the journal for S3
        and a rerun skips it."""
        h = owner_harness(self, mutate_inventory=with_projections([P_BY_N]))
        h.fake.projections_of(SRC).append({k: P_BY_N[k] for k in ('name', 'table', 'definition')})
        owner = h.sessions.owner(read_guard())
        self.assertEqual(P.capture(owner, SRC), [P_BY_N])
        self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
        self.assertEqual(h.fake.projections_of(TGT), [], 'nothing before O7: the copy fills a table without projections')
        out = h.step('O7')
        self.assertEqual(out.status, steps.PASSED, h.text())
        self.assertIn('1 projections (t_single.p_by_n)', out.note)
        self.assertEqual([p['name'] for p in h.fake.projections_of(TGT)], ['p_by_n'])
        self.assertIn(P_BY_N['definition'], h.fake.writes, 'the captured text as it is, after USE of the replacement')
        self.assertIn('projections gate: 1 equal the capture (t_single.p_by_n Sync)', h.text())
        self.assertEqual(h.journal.get_meta(P.PROJECTION_COUNT_META), '1')
        self.assertEqual({json.loads(d)['projection'] for d in h.journal.events('projection')}, {'p_by_n'})
        counts = ownersql.expected_counts(h.ctx())
        self.assertEqual(counts['projections'], 1)
        block = switch.forward_block(ownersql.names_for(h.ctx()), counts)
        self.assertIn('S3 after: projections count equals the capture', [i.label for i in block.items])
        self.assertEqual(h.step('O7').status, steps.PASSED, 'a rerun skips the projection that exists')
        self.assertIn('projection t_single.p_by_n already exists', h.text())

    def test_a_definition_naming_a_database_refuses_o7_before_any_statement(self):
        """A captured FROM `db`.`table` would read another database: O7 refuses it with the projection named before any statement,
        so no CREATE PROJECTION; the O7 guard refuses the same text naming the source."""
        qualified = dict(P_BY_N, definition=P_BY_N['definition'].replace('FROM `t_single`', 'FROM `%s`.`t_single`' % SRC))
        h = owner_harness(self, mutate_inventory=with_projections([qualified]))
        self.assertEqual(h.o5_o6().status, steps.PASSED, h.text())
        out = h.step('O7')
        self.assertEqual((out.status, out.exit_code), (steps.REFUSED, 1), h.text())
        self.assertIn('t_single.p_by_n: the captured definition does not end with FROM `t_single` (no database qualifier)', out.note)
        self.assertFalse([w for w in h.fake.writes if w.startswith(('CREATE PROJECTION', 'CREATE VIEW', 'ALTER TABLE'))])
        self.assertFalse(steps.guard_for('O7', h.db, steps.OWNER).allows(qualified['definition']))


if __name__ == '__main__':
    unittest.main()
