"""s2rebuild tests test_guard: statement classification, allowances per step, source scan per module.

Author: John Bergamini
"""
import unittest
from pathlib import Path

from s2rebuild import sqlutil, steps
from s2rebuild.common import KitError
from s2rebuild.sqlutil import Guard, GuardError, classify, ident, qualified
from tests.helpers import ROOT, config_from

DB = config_from().databases[0]

SRC, TGT, RET = 'example_orders_db', 'example_orders_db_rebuild_20261101', 'example_orders_db_retained_20261101'


class IdentTests(unittest.TestCase):
    def test_ident_and_qualified(self):
        self.assertEqual(ident('a_1'), '`a_1`')
        self.assertEqual(qualified('db', 't'), '`db`.`t`')
        for bad in ('a`b', 'a b', 'a;b', '', 'a-b', None, 3):
            with self.assertRaises(KitError, msg=repr(bad)):
                ident(bad)


class ClassifyTests(unittest.TestCase):
    def test_kinds_and_databases(self):
        cases = [
            ('SELECT 1', 'SELECT', ()),
            ('  select COUNT(*) FROM `a`.`b`;', 'SELECT', ()),
            ('SHOW CREATE TABLE `a`.`b`', 'SHOW', ()),
            ('SHOW CREATE PIPELINE `a`.`p` EXTENDED', 'SHOW', ()),
            ('EXPLAIN SELECT 1', 'EXPLAIN', ()),
            ('SET lock_wait_timeout = 5', 'SET SESSION', ()),
            ('SET SESSION lock_wait_timeout = 5', 'SET SESSION', ()),
            ('SET GLOBAL query_parallelism_per_leaf_core = 0.5', 'SET GLOBAL', ()),
            ('USE `repl`', 'USE', ('repl',)),
            ('CREATE DATABASE `repl` PARTITIONS 32 SUB_PARTITIONS 512', 'CREATE DATABASE', ('repl',)),
            ('DROP DATABASE `ret`', 'DROP DATABASE', ('ret',)),
            ('DETACH DATABASE `src`', 'DETACH DATABASE', ('src',)),
            ('DETACH DATABASE `src` KILL', 'DETACH DATABASE', ('src',)),
            ("ATTACH DATABASE `src` READ ONLY STORAGEID abc-123", 'ATTACH DATABASE', ('src',)),
            ("ATTACH DATABASE `src` AS `ret` READ ONLY STORAGEID abc", 'ATTACH DATABASE', ('src', 'ret')),
            ("ATTACH DATABASE `repl` AS `src` STORAGEID def", 'ATTACH DATABASE', ('repl', 'src')),
            # a real Helios STORAGE_ID (engine 10.8.0); the colon was refused as an unsupported kind before October 26, 2026
            ("ATTACH DATABASE `src` READ ONLY STORAGEID 1028600672070281646:7367158985892520834", 'ATTACH DATABASE', ('src',)),
            ('CREATE TABLE `repl`.`t` LIKE `src`.`t`', 'CREATE TABLE', ('repl',)),
            ('CREATE TABLE `repl`.`t__fresh` LIKE `repl`.`t`', 'CREATE TABLE', ('repl',)),
            ('DROP TABLE `repl`.`t`', 'DROP TABLE', ('repl',)),
            ('ALTER TABLE `repl`.`t` AUTO_INCREMENT = 12', 'ALTER TABLE', ('repl',)),
            ('ALTER TABLE `repl`.`t__fresh` RENAME TO `t`', 'ALTER TABLE', ('repl',)),
            ('INSERT INTO `repl`.`t` (`a`) SELECT `a` FROM `src`.`t` WHERE CRC32(`a`) MOD 3 = 1', 'INSERT', ('repl',)),
            ('DELETE FROM `repl`.`t` WHERE CRC32(`a`) MOD 3 = 1', 'DELETE', ('repl',)),
            ('CREATE VIEW `repl`.`v` AS SELECT 1', 'CREATE VIEW', ('repl',)),
            ('AGGREGATOR SYNC AUTO_INCREMENT ON `repl`.`t`', 'AGGREGATOR SYNC AUTO_INCREMENT', ('repl',)),
            ('STOP PIPELINE `src`.`p`', 'STOP PIPELINE', ('src',)),
            ('START PIPELINE `src`.`p`', 'START PIPELINE', ('src',)),
            ('CREATE PIPELINE `p` AS LOAD DATA KAFKA "x/y"', 'CREATE PIPELINE', ()),
            ('CREATE OR REPLACE AGGREGATOR PIPELINE `p` AS LOAD DATA S3 "b"', 'CREATE PIPELINE', ()),
            ('ALTER PIPELINE `p` SET OFFSETS 1', 'ALTER PIPELINE', ()),
            ('DROP PIPELINE `repl`.`p`', 'DROP PIPELINE', ('repl',)),
            ("GRANT SELECT ON *.* TO 'u'@'%'", 'GRANT', ()),
            ("REVOKE INSERT ON *.* FROM 'u'@'%'", 'REVOKE', ()),
            ('KILL CONNECTION 42', 'KILL CONNECTION', ()),
            ('KILL 42', 'KILL CONNECTION', ()),
            ("CREATE USER 'u'@'%' IDENTIFIED BY '<password>'", 'CREATE USER', ()),
            ("DROP USER 'u'@'%'", 'DROP USER', ()),
            # W5: the three routine kinds name their database; a body's semicolons sit inside BEGIN ... END
            ('CREATE FUNCTION `repl`.`f`(a INT) RETURNS INT AS BEGIN RETURN a + 1; END', 'CREATE FUNCTION', ('repl',)),
            ('CREATE FUNCTION `repl`.`tvf`(n INT) RETURNS TABLE AS RETURN SELECT 1', 'CREATE FUNCTION', ('repl',)),
            ('CREATE DEFINER=`app`@`%` PROCEDURE `repl`.`p`() AS\nDECLARE x INT;\nBEGIN\n  ECHO SELECT 1;\nEND', 'CREATE PROCEDURE', ('repl',)),
            ('CREATE OR REPLACE AGGREGATE `repl`.`a`(INT) RETURNS INT WITH STATE INT INITIALIZE WITH f1 ITERATE WITH f2 MERGE WITH f3 TERMINATE WITH f4',
             'CREATE AGGREGATE', ('repl',)),
        ]
        for sql, kind, dbs in cases:
            st = classify(sql)
            self.assertEqual((st.kind, st.databases), (kind, dbs), sql)
        self.assertEqual(classify('CREATE PROCEDURE `repl`.`p`() AS BEGIN ECHO SELECT 1; END').detail, 'p')
        for sql in ('CREATE PROCEDURE `repl`.`p`() AS BEGIN ECHO SELECT 1; END; DROP DATABASE `src`',
                    'CREATE AGGREGATE `repl`.`a`(INT) RETURNS INT; DROP DATABASE `src`',
                    'CREATE FUNCTION `repl`.`f`(a INT) RETURNS INT AS RETURN a; DROP DATABASE `src`',
                    'CREATE FUNCTION `f`(a INT) RETURNS INT AS BEGIN RETURN a; END'):
            with self.assertRaises(GuardError, msg=sql):
                classify(sql)
        o5 = steps.guard_for('O5', DB, steps.OWNER)
        o5.check('USE `%s`' % TGT)
        o5.check('CREATE FUNCTION `%s`.`f`(a INT) RETURNS INT AS BEGIN RETURN a; END' % TGT)
        o5.check('CREATE AGGREGATE `%s`.`a`(INT) RETURNS INT WITH STATE INT INITIALIZE WITH f1 ITERATE WITH f2 MERGE WITH f3 TERMINATE WITH f4' % TGT)
        for sql in ('CREATE FUNCTION `%s`.`f`(a INT) RETURNS INT AS BEGIN RETURN a; END' % SRC,
                    'CREATE PROCEDURE `%s`.`p`() AS BEGIN ECHO SELECT 1; END' % TGT, 'CREATE VIEW `%s`.`v` AS SELECT 1' % TGT):
            with self.assertRaises(GuardError, msg=sql):
                o5.check(sql)
        o7 = steps.guard_for('O7', DB, steps.OWNER)
        o7.check('CREATE PROCEDURE `%s`.`p`() AS BEGIN ECHO SELECT 1; END' % TGT)
        with self.assertRaises(GuardError):
            o7.check('CREATE FUNCTION `%s`.`f`(a INT) RETURNS INT AS BEGIN RETURN a; END' % TGT)
        self.assertEqual(classify('ALTER TABLE `r`.`t` ADD COLUMN x int').detail, 'OTHER')
        self.assertEqual(classify('DETACH DATABASE `s` FORCE').detail, 'FORCE')
        self.assertEqual(classify("ATTACH DATABASE `s` READ ONLY STORAGEID x").detail, 'READ ONLY')
        self.assertEqual(classify('SET GLOBAL Query_Parallelism_Per_Leaf_Core = 1').detail, 'query_parallelism_per_leaf_core')

    def test_f7_create_view_with_a_definer_names_the_replacement_only(self):
        """CREATE VIEW keeps its kind and database with a DEFINER clause in either quote form; the O7 owner guard names the replacement only."""
        for sql in ('CREATE DEFINER=`app`@`%` SCHEMA_BINDING=OFF VIEW `repl`.`v` AS SELECT 1',
                    'CREATE ALGORITHM=UNDEFINED DEFINER=`app`@`%` SQL SECURITY DEFINER VIEW `repl`.`v` AS SELECT 1',
                    "CREATE DEFINER = 'app'@'%' VIEW `repl`.`v` AS SELECT 1"):
            st = classify(sql)
            self.assertEqual((st.kind, st.databases, st.detail), ('CREATE VIEW', ('repl',), 'v'), sql)
        with self.assertRaises(GuardError):
            classify("CREATE DEFINER=`app`@`%` VIEW `repl`.`v` AS SELECT 1; DROP DATABASE `src`")
        o7 = steps.guard_for('O7', DB, steps.OWNER)
        o7.check('USE `%s`' % TGT)
        o7.check('CREATE DEFINER=`app`@`%%` VIEW `%s`.`v` AS SELECT 1' % TGT)
        for sql in ('CREATE VIEW `%s`.`v` AS SELECT 1' % SRC, 'USE `%s`' % SRC, 'CREATE TABLE `%s`.`t` LIKE `%s`.`t`' % (TGT, SRC),
                    "DROP USER 's2rebuild_runner'@'%'"):
            with self.assertRaises(GuardError, msg=sql):
                o7.check(sql)

    def test_unsupported_kinds_refused(self):
        for sql in ('UPDATE `r`.`t` SET a = 1', 'TRUNCATE TABLE `r`.`t`', 'RENAME TABLE `r`.`t` TO `r`.`u`',
                    "LOAD DATA INFILE 'x' INTO TABLE t", 'INSERT INTO `t` VALUES (1)', 'CREATE TABLE `t` (a int)',
                    'DELETE FROM `t`', 'DROP TABLE `t`', 'CREATE DATABASE repl', 'SET @x', '', '   ;',
                    'BEGIN', 'COMMIT', 'CALL `r`.`proc`()', 'DETACH DATABASE `s` AT MILESTONE "m"',
                    'STOP PIPELINE `p`', 'START PIPELINE `p`', 'DROP PIPELINE `p`', 'KILL QUERY x', 'GRANT SELECT TO u',
                    "SET PASSWORD = 'x'", "SET PASSWORD FOR 'u'@'%' = 'x'", 'SET ROLE admin', 'SET a = 1, b = 2',
                    'SET SESSION a = 1, GLOBAL b = 2', "SELECT * FROM `a`.`b` INTO OUTFILE '/tmp/x'",
                    "SELECT 1 INTO S3 'b/k' CREDENTIALS '{}'", 'SELECT a INTO @v FROM `a`.`b`', 'SET PERSIST x = 1',
                    # Helios 10.8.0 rejects a quoted STORAGEID with error 1064, so the guard never lets one through
                    "ATTACH DATABASE `s` READ ONLY STORAGEID '1028600672070281646:7367158985892520834'",
                    'ATTACH DATABASE `s` STORAGEID 1:1 READ ONLY', 'ATTACH DATABASE `s` STORAGEID 1 2', 'ATTACH DATABASE `s` READ ONLY STORAGEID x;DROP'):
            with self.assertRaises(GuardError, msg=sql):
                classify(sql)

    def test_kill_query_only_through_the_narrow_cancel_guard(self):
        from s2rebuild.sqlutil import owner_guard
        self.assertEqual(classify('KILL QUERY 42').kind, 'KILL QUERY')
        for step_id in steps.OWNER_ALLOWANCES:
            with self.assertRaises(GuardError, msg=step_id):
                steps.guard_for(step_id, DB, steps.OWNER).check('KILL QUERY 42')
        cancel = owner_guard('owner detach cancel', ('KILL QUERY',), (), required_detail={'KILL QUERY': '42'})
        cancel.check('KILL QUERY 42')
        with self.assertRaises(GuardError):
            cancel.check('KILL QUERY 43')
        with self.assertRaises(GuardError):
            cancel.check('KILL CONNECTION 42')

    def test_grant_scope_and_pipeline_targets_name_their_database(self):
        self.assertEqual(classify("REVOKE INSERT ON `src`.* FROM 'app'@'%'").databases, ('src',))
        self.assertEqual(classify("GRANT INSERT ON `src`.`t` TO 'app'@'%'").databases, ('src',))
        self.assertEqual(classify("GRANT INSERT, UPDATE ON *.* TO 'app'@'%'").databases, ())
        self.assertEqual(classify("CREATE PIPELINE `p` AS LOAD DATA KAFKA 'b/t INTO TABLE `x`.y' INTO TABLE `src`.`t`").databases, ('src',))
        self.assertEqual(classify("CREATE PIPELINE `p` AS LOAD DATA KAFKA 'b/t' INTO TABLE `t`").databases, ())
        guard = steps.guard_for('O9', DB, steps.OWNER)
        with self.assertRaises(GuardError):
            guard.check("CREATE PIPELINE `p` AS LOAD DATA KAFKA 'b/t' INTO TABLE `%s`.`t`" % DB.source)

    def test_probe_accepts_a_string_token_or_an_integer(self):
        from s2rebuild.sqlutil import probe_guard
        guard = probe_guard('src')
        guard.check("DELETE FROM `src`.`t` WHERE `c` = 's2rebuild-probe-ab'")
        guard.check('DELETE FROM `src`.`t` WHERE `id` = -1')
        with self.assertRaises(GuardError):
            guard.check('DELETE FROM `src`.`t` WHERE `id` > 0')

    def test_compound_text_refused_but_semicolons_in_literals_allowed(self):
        for sql in ('SELECT 1; DROP DATABASE `src`', 'SELECT 1 ; DELETE FROM `src`.`t`',
                    "DELETE FROM `repl`.`t` WHERE a = 1; DELETE FROM `src`.`t`", 'SELECT 1;;', 'SELECT 1 -- x\n; DROP DATABASE `s`',
                    "SET lock_wait_timeout = 5; DETACH DATABASE `src`", "USE `repl`; DROP DATABASE `src`"):
            with self.assertRaises(GuardError, msg=sql):
                classify(sql)
        ok = classify('CREATE PIPELINE `p` AS LOAD DATA KAFKA "broker:9092/t;x" CONFIG \'{"a": "b;c"}\' INTO TABLE t')
        self.assertEqual(ok.kind, 'CREATE PIPELINE')
        self.assertEqual(classify("SELECT ';' AS semi FROM `a`.`b`").kind, 'SELECT')
        self.assertEqual(classify("INSERT INTO `repl`.`t` (`a`) SELECT 'x;y' FROM `src`.`t`").databases, ('repl',))
        self.assertEqual(classify("SELECT 'it''s; fine' FROM `a`.`b`").kind, 'SELECT')
        self.assertEqual(classify('SELECT 1;').kind, 'SELECT', 'one trailing semicolon is fine')

    def test_global_set_variants_are_set_global(self):
        for sql in ('SET GLOBAL x = 1', 'SET @@GLOBAL.x = 1', 'SET @@global.x = 1', 'set global X=1'):
            st = classify(sql)
            self.assertEqual((st.kind, st.detail), ('SET GLOBAL', 'x'), sql)
        for sql in ('SET x = 1', 'SET SESSION x = 1', 'SET @@session.x = 1', 'SET @@x = 1'):
            self.assertEqual(classify(sql).kind, 'SET SESSION', sql)
        g = sqlutil.runner_guard(TGT)
        for sql in ('SET @@GLOBAL.x = 1', 'SET GLOBAL x = 1'):
            with self.assertRaises(GuardError, msg=sql):
                g.check(sql)


class GuardTests(unittest.TestCase):
    def test_runner_guard_allows_the_eight_kinds_on_the_replacement_only(self):
        g = sqlutil.runner_guard(TGT)
        allowed = [
            'CREATE DATABASE `%s` PARTITIONS 32 SUB_PARTITIONS 512' % TGT,
            'CREATE TABLE `%s`.`t` LIKE `%s`.`t`' % (TGT, SRC),
            'CREATE TABLE `%s`.`t__fresh` LIKE `%s`.`t`' % (TGT, TGT),   # the fresh-table fallback copies the checked replacement definition
            'INSERT INTO `%s`.`t` (`a`) SELECT `a` FROM `%s`.`t` WHERE CRC32(`a`) MOD 2 = 0' % (TGT, SRC),
            'DELETE FROM `%s`.`t` WHERE CRC32(`a`) MOD 2 = 0' % TGT,
            'CREATE VIEW `%s`.`v` AS SELECT 1 FROM `%s`.`t`' % (TGT, TGT),
            'ALTER TABLE `%s`.`t` AUTO_INCREMENT = 100' % TGT,
            'ALTER TABLE `%s`.`t__fresh` RENAME TO `t`' % TGT,
            'AGGREGATOR SYNC AUTO_INCREMENT ON `%s`.`t` ALL' % TGT,
            'DROP TABLE `%s`.`t`' % TGT,
            'SELECT COUNT(*) AS n FROM `%s`.`t`' % SRC,
            'SHOW CREATE TABLE `%s`.`t`' % TGT,
            'SET lock_wait_timeout = 5',
            'USE `%s`' % TGT,
        ]
        for sql in allowed:
            g.check(sql)
        refused = [
            'INSERT INTO `%s`.`t` (`a`) SELECT `a` FROM `%s`.`t`' % (SRC, TGT),
            'DELETE FROM `%s`.`t` WHERE 1' % SRC,
            'CREATE TABLE `%s`.`t` LIKE `%s`.`t`' % (SRC, TGT),
            'CREATE TABLE `%s`.`t__fresh` LIKE `%s`.`t`' % (SRC, SRC),
            'CREATE DATABASE `%s`' % SRC,
            'DROP TABLE `%s`.`t`' % SRC,
            'ALTER TABLE `%s`.`t` ADD COLUMN b int' % TGT,
            'ALTER TABLE `%s`.`t` DROP COLUMN b' % TGT,
            'UPDATE `%s`.`t` SET a = 1' % TGT,
            'DROP DATABASE `%s`' % TGT,
            'DETACH DATABASE `%s`' % TGT,
            "ATTACH DATABASE `%s` STORAGEID x" % TGT,
            'STOP PIPELINE `%s`.`p`' % TGT,
            'CREATE PIPELINE `p` AS LOAD DATA KAFKA "x"',
            "GRANT SELECT ON *.* TO 'u'@'%'",
            'KILL CONNECTION 1',
            'SET GLOBAL query_parallelism_per_leaf_core = 1',
            'USE `%s`' % SRC,
        ]
        for sql in refused:
            with self.assertRaises(GuardError, msg=sql):
                g.check(sql)
            self.assertFalse(g.allows(sql))

    def test_f11_aggregator_sync_runs_on_every_aggregator_only(self):
        """AGGREGATOR SYNC AUTO_INCREMENT ... ALL is the one sync form the runner guard accepts: without ALL only the aggregator of
        the session syncs, so a counter set at O7 would not reach the others. The ALL form names the replacement only."""
        st = classify('AGGREGATOR SYNC AUTO_INCREMENT ON `repl`.`t` ALL')
        self.assertEqual((st.kind, st.databases, st.detail), ('AGGREGATOR SYNC AUTO_INCREMENT', ('repl',), 'ALL'))
        self.assertEqual(classify('AGGREGATOR SYNC AUTO_INCREMENT ON `repl`.`t`').detail, '')
        g = sqlutil.runner_guard(TGT)
        g.check('AGGREGATOR SYNC AUTO_INCREMENT ON `%s`.`t` ALL' % TGT)
        with self.assertRaisesRegex(GuardError, 'refuses AGGREGATOR SYNC AUTO_INCREMENT without ALL'):
            g.check('AGGREGATOR SYNC AUTO_INCREMENT ON `%s`.`t`' % TGT)
        for sql in ('AGGREGATOR SYNC AUTO_INCREMENT ON `%s`.`t` ALL' % SRC, 'AGGREGATOR SYNC AUTO_INCREMENT ON `%s`.`t` ALL x' % TGT):
            with self.assertRaises(GuardError, msg=sql):
                g.check(sql)

    def test_read_guard(self):
        g = sqlutil.read_guard()
        for sql in ('SELECT 1', 'SHOW GRANTS', 'SET lock_wait_timeout = 1', 'USE `anything`'):
            g.check(sql)
        for sql in ('INSERT INTO `a`.`b` VALUES (1)', 'CREATE DATABASE `x`', 'DELETE FROM `a`.`b`'):
            with self.assertRaises(GuardError, msg=sql):
                g.check(sql)

    def test_probe_guard(self):
        g = sqlutil.probe_guard(SRC)
        g.check("DELETE FROM `%s`.`smallest` WHERE `id` = 'probe-7f3a-cannot-exist'" % SRC)
        g.check('SELECT 1')
        for sql in ("DELETE FROM `%s`.`smallest` WHERE 1=0" % SRC,
                    "DELETE FROM `%s`.`smallest`" % SRC,
                    "DELETE FROM `%s`.`smallest` WHERE `id` = 'x' OR 1=1" % SRC,
                    "DELETE FROM `%s`.`t` WHERE `id` = 'x'" % TGT,
                    "INSERT INTO `%s`.`t` (`a`) SELECT 1" % SRC):
            with self.assertRaises(GuardError, msg=sql):
                g.check(sql)

    def test_owner_guard_per_kind_and_force(self):
        g = Guard('owner O10', ('DETACH DATABASE', 'ATTACH DATABASE'), (SRC, TGT, RET), per_kind={'DROP DATABASE': (RET,)})
        g.check('DETACH DATABASE `%s`' % SRC)
        g.check('DETACH DATABASE `%s` KILL' % SRC)
        g.check("ATTACH DATABASE `%s` AS `%s` READ ONLY STORAGEID sid" % (SRC, RET))
        g.check("ATTACH DATABASE `%s` AS `%s` STORAGEID sid2" % (TGT, SRC))
        g.check('DROP DATABASE `%s`' % RET)
        for sql in ('DROP DATABASE `%s`' % SRC, 'DROP DATABASE `%s`' % TGT, 'DETACH DATABASE `other_db`',
                    "ATTACH DATABASE `%s` AS `other_db` STORAGEID x" % SRC, 'DETACH DATABASE `%s` FORCE' % SRC,
                    'DROP TABLE `%s`.`t`' % RET, "GRANT ALL ON *.* TO 'u'@'%'"):
            with self.assertRaises(GuardError, msg=sql):
                g.check(sql)

    def test_guard_errors_never_echo_the_statement(self):
        secret = 'SuperSecretSaslPassword'
        sql = 'CREATE PIPELINE `p` AS LOAD DATA KAFKA "b/t" CREDENTIALS \'{"sasl.password": "%s"}\' INTO TABLE t' % secret
        with self.assertRaises(GuardError) as ctx:
            sqlutil.runner_guard(TGT).check(sql)
        self.assertNotIn(secret, str(ctx.exception))
        with self.assertRaises(GuardError) as ctx:
            classify("UPDATE `a`.`b` SET pw = '%s'" % secret)
        self.assertNotIn(secret, str(ctx.exception))

    def test_unknown_kind_names_refused_at_construction(self):
        with self.assertRaises(KitError):
            Guard('x', ('TRUNCATE',))
        with self.assertRaises(KitError):
            Guard('x', (), per_kind={'UPDATE': ()})


class StepGuardTests(unittest.TestCase):
    def setUp(self):
        self.db = config_from().databases[0]

    def test_runner_guards_per_step(self):
        for step_id in ('O5', 'O6', 'O7'):
            g = steps.guard_for(step_id, self.db)
            g.check('INSERT INTO `%s`.`t` (`a`) SELECT `a` FROM `%s`.`t`' % (TGT, SRC))
            with self.assertRaises(GuardError):
                g.check('INSERT INTO `%s`.`t` (`a`) SELECT `a` FROM `%s`.`t`' % (SRC, TGT))
        for step_id in ('P2', 'P3', 'O4', 'O8', 'C1', 'C2'):
            g = steps.guard_for(step_id, self.db)
            g.check('SELECT 1')
            with self.assertRaises(GuardError, msg=step_id):
                g.check('INSERT INTO `%s`.`t` (`a`) SELECT 1' % TGT)
        probe = steps.guard_for('O3', self.db)
        probe.check("DELETE FROM `%s`.`t` WHERE `id` = 'nope'" % SRC)
        with self.assertRaises(GuardError):
            probe.check("DELETE FROM `%s`.`t` WHERE `id` = 'nope'" % TGT)

    def test_owner_guards_per_step(self):
        o2 = steps.guard_for('O2', self.db, steps.OWNER)
        o2.check('STOP PIPELINE `%s`.`p`' % SRC)
        with self.assertRaises(GuardError):
            o2.check('STOP PIPELINE `%s`.`p`' % TGT)
        with self.assertRaises(GuardError):
            o2.check('STOP PIPELINE `p`')
        with self.assertRaises(GuardError):
            o2.check('START PIPELINE `%s`.`p`' % SRC)
        o3 = steps.guard_for('O3', self.db, steps.OWNER)
        o3.check('DETACH DATABASE `%s`' % SRC)
        o3.check('DETACH DATABASE `%s` KILL' % SRC)
        o3.check("ATTACH DATABASE `%s` READ ONLY STORAGEID sid" % SRC)
        with self.assertRaises(GuardError):
            o3.check('DETACH DATABASE `%s`' % TGT)
        with self.assertRaisesRegex(GuardError, 'requires ATTACH DATABASE with READ ONLY'):
            o3.check("ATTACH DATABASE `%s` STORAGEID sid" % SRC)
        with self.assertRaises(GuardError):
            o3.check("ATTACH DATABASE `%s` AS `%s` READ ONLY STORAGEID sid" % (SRC, RET))
        o9 = steps.guard_for('O9', self.db, steps.OWNER)
        o9.check('USE `%s`' % TGT)
        o9.check('CREATE PIPELINE `p` AS LOAD DATA KAFKA "x"')
        o9.check('ALTER PIPELINE `p` SET OFFSETS 1')
        with self.assertRaises(GuardError):
            o9.check('USE `%s`' % SRC)
        with self.assertRaises(GuardError):
            o9.check('START PIPELINE `%s`.`p`' % TGT)
        o10 = steps.guard_for('O10', self.db, steps.OWNER)
        o10.check("ATTACH DATABASE `%s` AS `%s` READ ONLY STORAGEID sid" % (SRC, RET))
        # S2b drops the original record and S4 the replacement; the retained copy is never dropped by O10
        o10.check('DROP DATABASE `%s`' % SRC)
        o10.check('DROP DATABASE `%s`' % TGT)
        with self.assertRaises(GuardError):
            o10.check('DROP DATABASE `%s`' % RET)
        o11 = steps.guard_for('O11', self.db, steps.OWNER)
        o11.check('START PIPELINE `%s`.`p`' % SRC)
        o11.check('SET GLOBAL query_parallelism_per_leaf_core = 0.5')
        for refused in ('START PIPELINE `%s`.`p`' % TGT, "GRANT INSERT ON *.* TO 'app'@'%'"):
            with self.assertRaises(GuardError):
                o11.check(refused)
        c3 = steps.guard_for('C3', self.db, steps.OWNER)  # C3 issues reads only
        for refused in ('DROP DATABASE `%s`' % TGT, 'DETACH DATABASE `%s`' % SRC, 'DROP DATABASE `%s`' % RET):
            with self.assertRaises(GuardError):
                c3.check(refused)
        c5 = steps.guard_for('cleanup', self.db, steps.OWNER)
        c5.check('DROP DATABASE `%s`' % RET)
        c5.check('DETACH DATABASE `%s`' % RET)
        with self.assertRaises(GuardError):
            c5.check('DROP DATABASE `%s`' % SRC)
        steps.guard_for('C4', self.db, steps.OWNER).check("DROP USER 's2rebuild_runner'@'%'")
        steps.guard_for('P1', self.db, steps.OWNER).check("CREATE USER 'u'@'%' IDENTIFIED BY '<password>'")
        with self.assertRaises(KitError):
            steps.guard_for('O6', self.db, steps.OWNER)


# Words that mark an admin or destructive statement. A module may contain one only when the
# allowance below names it, with the reason.
FORBIDDEN_WORDS = ('detach', 'attach', 'drop', 'truncate', 'pipeline', 'grant', 'revoke', 'kill')
ALLOWANCES = {
    'sqlutil.py': set(FORBIDDEN_WORDS),           # classifies every statement kind
    'steps.py': set(FORBIDDEN_WORDS),             # registry titles, gate texts and owner allowances
    'cli.py': {'pipeline', 'drop', 'grant', 'attach'},   # command help text, the --drop flag, record_attachment
    'inventory.py': {'pipeline', 'grant', 'attach', 'drop'},  # pipeline capture, grantees, attachment row, DROP in the write privilege list
    'estimate.py': {'pipeline', 'attach'},   # the plan sheet's pipeline lines and freeze wording
    'report_xlsx.py': {'pipeline', 'grant', 'attach', 'detach'},  # Pipelines tabs, grants scope row, the attachment rows and their is_detached column
    'copy.py': {'drop', 'kill'},                                # DROP TABLE in the fresh-table fallback (section 10.9); the hard-stop hint names the operating system kill, no KILL statement
    'transform.py': set(),
    'verify.py': set(),
    'postcopy.py': set(),
    'projections.py': {'attach'},  # the docstring: ATTACH ... AS carries the projections to the live name (fact 45); no ATTACH statement
    'routines.py': {'pipeline'},  # the load target refusal (a pipeline INTO PROCEDURE whose procedure the replacement will not hold)
    'prompts.py': {'pipeline'},  # the pipeline credential prompts (P3, O9); the only statement is the PASSWORD_ variables read
    'report_md.py': set(),
    'ownersql.py': {'drop', 'grant', 'attach', 'pipeline', 'detach', 'kill'},  # account and cleanup blocks, attachment checks, pipeline block dispatch
    'switch.py': {'detach', 'attach', 'drop', 'pipeline', 'kill'},     # the state machine's statements, DROP at S2b and S4, pipeline count, DETACH KILL
    'cleanup.py': {'detach', 'attach', 'drop'},  # C5: DETACH, ATTACH by syntax and DROP of the retained copy behind its record checks
    'freeze.py': {'detach', 'attach', 'kill', 'pipeline', 'grant', 'revoke'},  # the freeze block, DETACH KILL escalation, pipelines check, grants reference
    'grants.py': {'grant', 'revoke', 'drop', 'pipeline'},              # REVOKE and GRANT statements, the privilege names
    'pipelines.py': {'pipeline', 'grant', 'drop', 'truncate'},         # pipeline statements, the C1 grants compare, the C1 writer sample's write regex (DROP, TRUNCATE)
    'notebook.py': {'kill', 'pipeline', 'detach'},  # the run payload (detach_wait_seconds, detach_kill) and the pipeline keys
    'abandon.py': {'detach', 'attach', 'drop', 'kill'},  # the abandon block, detach_kill of the escalation
    'config.py': {'pipeline', 'detach', 'kill'},  # pipelines section, detach_wait_seconds, detach_kill
    'journal.py': {'pipeline', 'grant', 'attach', 'detach'},  # pipelines, the grants_snapshot table, the attachments table and its is_detached column
    'connect.py': set(),
    'common.py': set(),
    '__init__.py': set(),
    '__main__.py': set(),
}


class SourceScanTests(unittest.TestCase):
    def test_every_module_has_an_allowance_entry(self):
        modules = sorted(p.name for p in (ROOT / 's2rebuild').glob('*.py'))
        self.assertEqual(modules, sorted(ALLOWANCES), 'add a reviewed allowance for every module')

    def test_module_sources_stay_within_their_allowance(self):
        for name, allowed in ALLOWANCES.items():
            text = (ROOT / 's2rebuild' / name).read_text(encoding='utf8').lower()
            for word in FORBIDDEN_WORDS:
                if word in allowed:
                    continue
                self.assertNotIn(word, text, '%s contains %r outside its allowance' % (name, word))

    def test_allowances_are_no_wider_than_the_module_text(self):
        for name, allowed in ALLOWANCES.items():
            if allowed == set(FORBIDDEN_WORDS):
                continue
            text = (ROOT / 's2rebuild' / name).read_text(encoding='utf8').lower()
            unused = sorted(w for w in allowed if w not in text)
            self.assertEqual(unused, [], '%s allows words it does not contain; narrow its entry' % name)


if __name__ == '__main__':
    unittest.main()
