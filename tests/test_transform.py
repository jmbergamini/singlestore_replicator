"""s2rebuild tests test_transform: pass-through hook, target block refusal, report block.

Author: John Bergamini
"""
import unittest

from s2rebuild import inventory as I
from s2rebuild import transform as T
from s2rebuild.common import KitError
from tests.helpers import config_from


class TransformTests(unittest.TestCase):
    def test_apply_is_a_pass_through(self):
        plan = I.TablePlan('t', ['a'], 10, 'key', ['a'], 'CREATE TABLE `t` (`a` int, PRIMARY KEY (`a`))', 1)
        target = T.apply(plan, {'transforms': []})
        self.assertEqual((target.table, target.original, target.target, target.changes), ('t', plan.definition, plan.definition, ()))
        self.assertTrue(target.identical)
        self.assertTrue(T.apply(plan, None).identical)

    def test_transforms_are_refused_in_this_pass(self):
        with self.assertRaisesRegex(KitError, 'partition layout only'):
            T.check_target({'transforms': [{'table': 't', 'drop_column': 'x'}]}, 'db1')
        with self.assertRaisesRegex(KitError, 'partition layout only'):
            config_from(lambda d: d['databases'][0].update(target={'transforms': [{'table': 'orders'}]}))
        cfg = config_from(lambda d: d['databases'][0].update(target={'transforms': []}))
        self.assertEqual(cfg.databases[0].target, {'transforms': []})
        self.assertEqual(config_from().databases[0].target, {'transforms': []}, 'the block defaults to an empty list')
        with self.assertRaisesRegex(KitError, 'Unknown configuration key databases.0.target.keys'):
            config_from(lambda d: d['databases'][0].update(target={'keys': []}))

    def test_report_rows(self):
        plan = I.TablePlan('t', ['a'], 10, 'key', ['a'], 'CREATE TABLE `t` (`a` int)', 1)
        rows = T.report_rows([T.apply(plan)])
        self.assertEqual(rows[0][0], 'Replacement definition')
        self.assertIn('pass-through', rows[0][1])
        self.assertEqual(rows[1], ('t', 'identical'))
        later = (T.__doc__ or '')
        for item in ('keys', 'indexes', 'storage', 'renames', 'column type changes'):
            self.assertIn(item, later, 'the later-pass list names ' + item)


if __name__ == '__main__':
    unittest.main()
