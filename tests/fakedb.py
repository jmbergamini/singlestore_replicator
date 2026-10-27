"""s2rebuild tests fakedb: an in-memory stand-in for the workspace that understands the kit's statement shapes.

Author: John Bergamini

Rows live in Python lists; hash-bucket predicates are evaluated with zlib.crc32 on the string
form of each value, which matches the server's CRC32 for the values the tests use. Fault
injection: fail_insert[(table, predicate)] = 'before' | 'after' | 'lock'; lock_tables holds
(database, table) pairs whose INSERT and DELETE raise error 1205 until removed; the crash hook
of the runner stands in for kill -9. fail_statement(pattern, when) fails any statement: 'before'
raises before the server sees it, 'after' raises once the server applied it (the client lost the
reply); crash=True raises SimulatedCrash instead of a lost connection. The third failure point,
after the statement and before the journal update, is tests.helpers.crash_journal.
fail_connect(user) fails the next connection opened for user (the server unreachable as a
session opens). change_before(pattern, action) changes the state just before a matching
statement reaches the server (another actor between two reads of the kit). deny(user, pattern)
refuses matching statements on that account's connections with error 1142 (a missing privilege).
admin_only_storage holds STORAGE_IDs whose attachment and branch rows only the admin sees (an
account without privileges on another database). DROP DATABASE of a name that is neither
attached nor created raises error 1008, as the server does, so a second DROP of the same name
shows up as a failure (it is still listed in writes). The capture reads of P3 and O4 (objects,
routines, columns, keys, row estimates, sizes) are answered from the tables, so a live profile
can run on the fake through the command line (tests.helpers.fake_driver). CREATE VIEW records
the definer (its DEFINER clause, else the account of the connection) and raises error 1146 when
its body reads a table or view that does not exist yet; information_schema.VIEWS answers DEFINER
as user@host text. information_schema.TABLES lists tables and views by name, and SHOW CREATE
VIEW answers in the server form: the view name unqualified, the recorded definer as a DEFINER
clause, and the body as it was created. AGGREGATOR SYNC AUTO_INCREMENT ... ALL records the
table's counter as the one every aggregator holds (synced_counter, and the name in synced); the
form without ALL syncs only the aggregator of the session, so it changes neither. aggregators =
N spreads the connections over N aggregators (connection id modulo N); a processlist row with an
AGGREGATOR value is listed and killed only from a connection on that aggregator, since thread
ids are per aggregator.
"""
import datetime as dt
import re
import threading
import time
import zlib
from collections import Counter

GOOD_LAYOUT = {'NUM_PARTITIONS': 16, 'NUM_SUB_PARTITIONS': 0, 'IS_SYNC': 1, 'IS_SYNC_DURABILITY': 0,
               'IS_SYNC_DURABILITY_ON_REF_DB_ONLY': 1}


REFERENCE_CREATE = re.compile(r'\s*CREATE\s+(?:ROWSTORE\s+)?REFERENCE\s+TABLE\b', re.I)


class FakeDisconnect(Exception):
    """The connection dropped."""


class LockWait(Exception):
    """Error 1205 as the driver raises it."""

    def __init__(self):
        super().__init__(1205, 'Lock wait timeout exceeded; try restarting transaction')


class SimulatedCrash(BaseException):
    """Stands in for kill -9: not an Exception, so the worker cannot journal it."""


class FakeView(str):
    """The CREATE VIEW text the fake stored, with the definer the server records (user@host)."""

    def __new__(cls, text, definer):
        obj = super().__new__(cls, text)
        obj.definer = definer
        return obj


def view_definer(view):
    """The user@host definer of a stored view: the one recorded at CREATE, else the DEFINER clause of its text, else None."""
    if getattr(view, 'definer', None):
        return view.definer
    m = re.search(r"DEFINER ?= ?[`']([^`']*)[`']@[`']([^`']*)[`']", str(view))
    return '%s@%s' % m.groups() if m else None


def show_create_view(name, view):
    """The SHOW CREATE VIEW text of a stored view: CREATE ALGORITHM=UNDEFINED DEFINER=`u`@`h` SQL SECURITY DEFINER VIEW `name` AS body."""
    m = re.search(r'VIEW (?:`[^`]+`\.)?`[^`]+` AS (.*)$', str(view))
    if not m:
        return str(view)
    definer = view_definer(view)
    clause = 'DEFINER=`%s`@`%s` ' % tuple(definer.split('@', 1)) if definer and '@' in definer else ''
    return 'CREATE ALGORITHM=UNDEFINED %sSQL SECURITY DEFINER VIEW `%s` AS %s' % (clause, name, m.group(1))


def mysql_str(v):
    """The string form the server hashes."""
    if isinstance(v, float):
        return repr(v)
    return str(v)


def crc32(text):
    return zlib.crc32(text.encode('utf8')) & 0xFFFFFFFF


def key_text(expr, row):
    """Evaluate a key expression (`col`, CONCAT_WS of columns, or COALESCE form) to the hashed text."""
    if expr.startswith("CONCAT_WS('|', "):
        inner = expr[len("CONCAT_WS('|', "):-1]
        parts = re.findall(r"COALESCE\(`([^`]+)`, ''\)|`([^`]+)`", inner)
        values = []
        for coalesced, plain in parts:
            if coalesced:
                v = row[coalesced]
                values.append('' if v is None else mysql_str(v))
            else:
                v = row[plain]
                assert v is not None, 'NULL in a non-null key'
                values.append(mysql_str(v))
        return '|'.join(values)
    m = re.fullmatch(r'`([^`]+)`', expr)
    assert m, expr
    return mysql_str(row[m.group(1)])


def evaluate_predicate(pred, row):
    """CRC32(expr) MOD n = k, or CRC32(expr) MOD 100 < p for a sample."""
    m = re.fullmatch(r'CRC32\((.*)\) MOD (\d+) = (\d+)', pred)
    if m:
        return crc32(key_text(m.group(1), row)) % int(m.group(2)) == int(m.group(3))
    m = re.fullmatch(r'CRC32\((.*)\) MOD 100 < (\d+)', pred)
    if m:
        return crc32(key_text(m.group(1), row)) % 100 < int(m.group(2))
    m = re.fullmatch(r"`([^`]+)` = '((?:[^']|'')*)'", pred)
    if m:
        value = row.get(m.group(1))
        return value is not None and mysql_str(value) == m.group(2).replace("''", "'")
    m = re.fullmatch(r'`([^`]+)` = (-?\d+)', pred)
    if m:
        value = row.get(m.group(1))
        return value is not None and str(value) == m.group(2)
    raise AssertionError('unexpected predicate: ' + pred)


def _split_top(text, sep=','):
    out, depth, buf, quote = [], 0, [], None
    for ch in text:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in ("'", '"', '`'):
            quote = ch
            buf.append(ch)
        elif ch == '(':
            depth += 1
            buf.append(ch)
        elif ch == ')':
            depth -= 1
            buf.append(ch)
        elif ch == sep and depth == 0:
            out.append(''.join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        out.append(''.join(buf).strip())
    return out


class FakeCursor:
    def __init__(self, db, conn):
        self.db = db
        self.conn = conn
        self.rows = []
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def fetchall(self):
        return self.rows

    def execute(self, sql, args=None):
        args = tuple(args) if args else ()
        s = ' '.join(sql.split())
        self.db.run_changes(s)
        self.db.fault(s, 'before')
        self.db.check_denied(s, self.conn.user)
        if self.db.insert_delay and s.startswith('INSERT INTO'):
            time.sleep(self.db.insert_delay)
        m = re.match(r'DETACH DATABASE `([^`]+)`$', s)
        if m and m.group(1) in self.db.detach_blocks:
            with self.db.lock:
                self.db.statements.append((s, args))
                self.db.writes.append(s)
            self.db.waiting_detach.set()
            if not self.conn.kill_event.wait(10):
                raise AssertionError('a blocked DETACH was never cancelled')
            self.conn.kill_event.clear()
            raise FakeDisconnect(1317, 'Query execution was interrupted')
        with self.db.lock:
            self.db.statements.append((s, args))
            self._execute(s, args)
        self.db.fault(s, 'after')

    def _sees_storage(self, row):
        """The attachment and branch views hide the rows of admin_only_storage from every account but the admin."""
        return self.conn.is_admin or row.get('STORAGE_ID') not in self.db.admin_only_storage

    def _sees_grantee(self, grantee):
        """The privilege views show other accounts only to the admin (section 5)."""
        return self.conn.is_admin or grantee == "'%s'@'%%'" % self.conn.user

    def _pipes(self, database=None):
        """Visible pipelines: none for an account without pipeline privileges (Helios fact 10)."""
        if not self.conn.is_admin:
            return []
        return [p for p in self.db.pipelines if p.get('DATABASE_NAME', self.db.source) is not None
                and (database is None or p.get('DATABASE_NAME', self.db.source) == database)]

    def _offsets(self, database, pipeline=None):
        if not self.conn.is_admin:
            return []
        return [o for o in self.db.pipeline_offsets if o.get('DATABASE_NAME', self.db.source) == database
                and (pipeline is None or o['PIPELINE_NAME'] == pipeline)]

    def _processlist(self, s, args):
        rows = [r for r in self.db.processlist if r.get('AGGREGATOR', self.conn.aggregator) == self.conn.aggregator]
        if 'WHERE' in s and args:
            needle = str(args[1]).strip('%') if len(args) > 1 else None
            rows = [r for r in rows if r.get('DB') == args[0] or (needle and needle in str(r.get('INFO') or ''))]
        if 'INFO IS NOT NULL' in s:
            rows = [r for r in rows if r.get('INFO') is not None]
        return rows

    def _execute(self, s, args):
        db = self.db
        m = re.match(r'SELECT NUM_PARTITIONS.* FROM information_schema\.DISTRIBUTED_DATABASES WHERE DATABASE_NAME = %s$', s)
        if m:
            d = db.databases.get(args[0])
            self.rows = [dict(d['layout'])] if d else []
            return
        m = re.match(r"SELECT TABLE_NAME FROM information_schema\.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'$", s)
        if m:
            d = db.databases.get(args[0])
            self.rows = [{'TABLE_NAME': t} for t in d['tables']] if d else []
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.VIEWS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s$', s)
        if m:
            d = db.databases.get(args[0])
            self.rows = [{'n': 1 if d and args[1] in d['views'] else 0}]
            return
        if s == 'SELECT @@lock_wait_timeout AS v':
            self.rows = [{'v': self.conn.lock_wait_timeout}]
            return
        m = re.match(r'SET lock_wait_timeout = (\d+)$', s)
        if m:
            self.conn.lock_wait_timeout = int(m.group(1))
            db.lock_wait_settings.append(int(m.group(1)))
            return
        if s.startswith('USE `'):
            self.conn.current_db = s[5:-1]
            return
        if s == 'SELECT CURRENT_USER() AS u, @@memsql_version AS v':
            self.rows = [{'u': '%s@%%' % (self.conn.user or db.current_user), 'v': db.engine_version}]
            return
        if s == 'SELECT PRIVILEGE_TYPE FROM information_schema.USER_PRIVILEGES WHERE GRANTEE = %s':
            self.rows = [{'PRIVILEGE_TYPE': r['PRIVILEGE_TYPE']} for r in db.privileges if r['GRANTEE'] == args[0] and self._sees_grantee(r['GRANTEE'])]
            return
        if s == "SELECT SUM(NUM_CPUS) AS cores FROM information_schema.MV_NODES WHERE TYPE = 'LEAF'":
            if db.leaf_cores is None:
                raise FakeDisconnect(1054, "Unknown column 'NUM_CPUS'")
            self.rows = [{'cores': db.leaf_cores}]
            return
        m = re.match(r'SELECT CURRENT_USER\(\) AS u', s)
        if m:
            self.rows = [{'u': db.current_user}]
            return
        if s == 'SELECT CONNECTION_ID() AS id':
            self.rows = [{'id': self.conn.id}]
            return
        m = re.match(r'KILL QUERY (\d+)$', s)
        if m:
            db.writes.append(s)
            target = next((c for c in db.connections if c.id == int(m.group(1))), None)
            if target is None:
                raise FakeDisconnect(1094, 'Unknown thread id: %s' % m.group(1))
            target.kill_event.set()
            return
        if s.startswith('SELECT GRANTEE, PRIVILEGE_TYPE FROM information_schema.USER_PRIVILEGES'):
            self.rows = [dict(r) for r in db.privileges if self._sees_grantee(r['GRANTEE'])]
            return
        if s.startswith('SELECT GRANTEE, TABLE_SCHEMA, PRIVILEGE_TYPE FROM information_schema.SCHEMA_PRIVILEGES'):
            self.rows = [dict(r) for r in db.schema_privileges if r['TABLE_SCHEMA'] == args[0] and self._sees_grantee(r['GRANTEE'])]
            return
        if s.startswith('SELECT GRANTEE, TABLE_NAME, PRIVILEGE_TYPE FROM information_schema.TABLE_PRIVILEGES'):
            self.rows = [dict(r) for r in db.table_privileges if r['TABLE_SCHEMA'] == args[0] and self._sees_grantee(r['GRANTEE'])]
            return
        if s.startswith('SELECT ID, USER, HOST'):
            self.rows = [dict(r) for r in self._processlist(s, args)]
            return
        if s.startswith('SELECT PIPELINE_NAME, STATE FROM information_schema.PIPELINES'):
            self.rows = [dict(r) for r in self._pipes(args[0] if args else None)]
            return
        m = re.match(r'CREATE DATABASE `([^`]+)` PARTITIONS (\d+) SUB_PARTITIONS (\d+)$', s)
        if m:
            db.writes.append(s)
            name = m.group(1)
            if name in db.databases:
                raise FakeDisconnect('database exists')
            layout = dict(db.databases[db.source]['layout'])
            layout['NUM_PARTITIONS'] = int(m.group(2))
            layout['NUM_SUB_PARTITIONS'] = int(m.group(3))
            layout.update(db.target_layout_override)
            db.databases[name] = {'layout': layout, 'tables': {}, 'views': {}}
            sid = db.new_sid()
            db.storage[sid] = name
            db.attachments.append({'DATABASE_NAME': name, 'ATTACH_TYPE': 'READWRITE', 'STORAGE_ID': sid, 'COMPUTE_ID': 'cid-1', 'ATTACH_SYNTAX': None})
            db.branches.append({'STORAGE_ID': sid, 'DATABASE_NAME': name, 'IS_DETACHED': 0, 'IS_ATTACHED_TO_THIS_COMPUTE': 1, 'IS_ATTACHABLE': 1,
                                'IS_DROPPED': 0, 'PREV_DATABASE_NAME': None, 'LATEST_ATTACH_TIME': 'now'})
            return
        m = re.match(r'CREATE TABLE `([^`]+)`\.`([^`]+)` LIKE `([^`]+)`\.`([^`]+)`$', s)
        if m:
            db.writes.append(s)
            tdb, tt, sdb, st = m.groups()
            src = db.databases[sdb]['tables'][st]
            create = src['create'].replace('`%s`' % st, '`%s`' % tt, 1) if tt != st else src['create']
            # Helios fact 14: LIKE copies the AUTO_INCREMENT attribute but not the counter
            db.databases[tdb]['tables'][tt] = {'create': create, 'rows': [], 'auto_increment': None}
            return
        m = re.match(r'CREATE TABLE `([^`]+)`\.`([^`]+)` \((.+)\)$', s)
        if m:
            db.writes.append(s)
            tdb, tt, body = m.groups()
            if tt in db.databases[tdb]['tables']:
                raise FakeDisconnect(1050, "Table '%s' already exists" % tt)
            db.databases[tdb]['tables'][tt] = {'create': 'CREATE TABLE `%s` (%s)' % (tt, body), 'rows': [], 'auto_increment': None}
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s$', s)
        if m:
            d = db.databases.get(args[0])
            self.rows = [{'n': int(bool(d) and args[1] in d['tables'])}]
            return
        m = re.match(r'SHOW CREATE TABLE `([^`]+)`\.`([^`]+)`$', s)
        if m:
            t = db.databases[m.group(1)]['tables'][m.group(2)]
            create = t['create']
            if t.get('auto_increment'):
                create = re.sub(r'\s*AUTO_INCREMENT=\d+', '', create) + ' AUTO_INCREMENT=%d' % t['auto_increment']
            self.rows = [{'Table': m.group(2), 'Create Table': create}]
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM `([^`]+)`\.`([^`]+)`(?: WHERE (.*))?$', s)
        if m:
            if m.group(1) not in db.databases:
                raise FakeDisconnect(1049, "Unknown database '%s'" % m.group(1))
            t = db.databases[m.group(1)]['tables'][m.group(2)]
            pred = m.group(3)
            self.rows = [{'n': sum(1 for r in t['rows'] if pred is None or evaluate_predicate(pred, r))}]
            return
        m = re.match(r'SELECT MAX\(`([^`]+)`\) AS m, COUNT\(\*\) AS n FROM `([^`]+)`\.`([^`]+)`$', s)
        if m:
            t = db.databases[m.group(2)]['tables'][m.group(3)]
            values = [r[m.group(1)] for r in t['rows'] if r.get(m.group(1)) is not None]
            self.rows = [{'m': max(values) if values else None, 'n': len(t['rows'])}]
            return
        m = re.match(r'SELECT MAX\(`([^`]+)`\) AS newest FROM `([^`]+)`\.`([^`]+)`$', s)
        if m:
            t = db.databases[m.group(2)]['tables'][m.group(3)]
            values = [r[m.group(1)] for r in t['rows'] if r.get(m.group(1)) is not None]
            self.rows = [{'newest': max(values) if values else None}]
            return
        m = re.match(r'SELECT MIN\(`([^`]+)`\) AS lo, MAX\(`([^`]+)`\) AS hi FROM `([^`]+)`\.`([^`]+)`$', s)
        if m:
            t = db.databases[m.group(3)]['tables'][m.group(4)]
            values = [r[m.group(1)] for r in t['rows'] if r.get(m.group(1)) is not None]
            self.rows = [{'lo': min(values) if values else None, 'hi': max(values) if values else None}]
            return
        m = re.match(r'SELECT CRC32\((.*)\) MOD (\d+) AS grp, COUNT\(\*\) AS n, SUM\(CRC32\((.*)\)\) AS key_hash, '
                     r'SUM\(CRC32\((.*)\)\) AS value_hash FROM `([^`]+)`\.`([^`]+)`(?: WHERE (.*))? GROUP BY grp$', s)
        if m:
            key_expr, groups, key_expr2, value_expr, dbname, table, pred = m.groups()
            assert key_expr == key_expr2
            t = db.databases[dbname]['tables'][table]
            out = {}
            for r in t['rows']:
                if pred and not evaluate_predicate(pred, r):
                    continue
                k = crc32(key_text(key_expr, r))
                grp = k % int(groups)
                entry = out.setdefault(grp, {'grp': grp, 'n': 0, 'key_hash': 0, 'value_hash': 0})
                entry['n'] += 1
                entry['key_hash'] += k
                entry['value_hash'] += crc32(key_text(value_expr, r))
            self.rows = [out[g] for g in sorted(out)]
            return
        m = re.match(r'INSERT INTO `([^`]+)`\.`([^`]+)` \((.*)\) SELECT (.*) FROM `([^`]+)`\.`([^`]+)`(?: WHERE (.*))?$', s)
        if m:
            db.writes.append(s)
            tdb, tt, cols1, cols2, sdb, st, pred = m.groups()
            assert cols1 == cols2
            if (tdb, tt) in db.lock_tables:
                db.lock_hits += 1
                raise LockWait()
            cols = [c.strip('`') for c in cols1.split(', ')]
            src = db.databases[sdb]['tables'][st]
            dst = db.databases[tdb]['tables'][tt]
            src_att = db.attachment(sdb)
            if REFERENCE_CREATE.match(dst['create']) and src_att is not None and src_att['ATTACH_TYPE'] == 'READONLY':
                # Helios 10.8.0, October 27, 2026: an INSERT into a reference table runs on the master aggregator,
                # where a READ ONLY source has no master instance to read from
                raise FakeDisconnect(1777, 'Forwarding Error (node-master-0:3306): Partitions of %s has no master instance. This is likely '
                                           'because the node or nodes that hold a copy of the partition are down.' % sdb)
            if src['rows']:
                assert list(src['rows'][0].keys()) == cols, 'the copy must name every saved column in ordinal order'
            moved = [{c: r[c] for c in cols} for r in src['rows'] if pred is None or evaluate_predicate(pred, r)]
            fail = db.fail_insert.pop((tt, pred), None)
            if fail == 'before':
                raise FakeDisconnect('lost connection before the statement ran')
            if fail == 'lock':
                db.lock_hits += 1
                raise LockWait()
            dst['rows'].extend(moved)
            self.rowcount = len(moved)
            if fail == 'after':
                raise FakeDisconnect('lost connection after the server committed')
            return
        m = re.match(r'DELETE FROM `([^`]+)`\.`([^`]+)` WHERE (.*)$', s)
        if m:
            db.writes.append(s)
            att = db.attachment(m.group(1))
            if att is not None and att['ATTACH_TYPE'] == 'READONLY':
                raise FakeDisconnect(1290, 'The database is attached read only')
            if (m.group(1), m.group(2)) in db.lock_tables:
                db.lock_hits += 1
                raise LockWait()
            t = db.databases[m.group(1)]['tables'][m.group(2)]
            keep = [r for r in t['rows'] if not evaluate_predicate(m.group(3), r)]
            self.rowcount = len(t['rows']) - len(keep)
            t['rows'] = keep
            return
        m = re.match(r'DROP TABLE `([^`]+)`\.`([^`]+)`$', s)
        if m:
            db.writes.append(s)
            if m.group(2) in db.fail_drop:
                db.fail_drop.discard(m.group(2))
                db.lock_hits += 1
                raise LockWait()
            del db.databases[m.group(1)]['tables'][m.group(2)]
            return
        m = re.match(r'ALTER TABLE `([^`]+)`\.`([^`]+)` RENAME TO `([^`]+)`$', s)
        if m:
            db.writes.append(s)
            tables = db.databases[m.group(1)]['tables']
            t = tables.pop(m.group(2))
            t['create'] = t['create'].replace('`%s`' % m.group(2), '`%s`' % m.group(3), 1)
            tables[m.group(3)] = t
            return
        m = re.match(r'ALTER TABLE `([^`]+)`\.`([^`]+)` AUTO_INCREMENT = (\d+)$', s)
        if m:
            if db.refuse_sharded_counter and not REFERENCE_CREATE.match(db.databases[m.group(1)]['tables'][m.group(2)]['create']):
                raise FakeDisconnect(1749, "Forwarding Error (node-master-0:3306): Feature 'ALTER TABLE...AUTO_INCREMENT=X for sharded tables' "
                                           "is not supported by SingleStore Distributed.")
            db.writes.append(s)
            db.databases[m.group(1)]['tables'][m.group(2)]['auto_increment'] = int(m.group(3))
            return
        m = re.match(r'AGGREGATOR SYNC AUTO_INCREMENT ON `([^`]+)`\.`([^`]+)`( ALL)?$', s)
        if m:
            db.writes.append(s)
            if m.group(3):  # every aggregator takes the counter; without ALL only the session's aggregator syncs
                t = db.databases[m.group(1)]['tables'][m.group(2)]
                t['synced_counter'] = t.get('auto_increment')
                db.synced.append(m.group(2))
            return
        m = re.match(r"CREATE (?:ALGORITHM=\w+ )?(?:DEFINER ?= ?(`[^`]*`|'[^']*')@(`[^`]*`|'[^']*') )?(?:SCHEMA_BINDING=\w+ )?"
                     r"(?:SQL SECURITY \w+ )?VIEW `([^`]+)`\.`([^`]+)` AS (.*)$", s)
        if m:
            db.writes.append(s)
            user, host, vdb, name, body = m.groups()
            if user and db.refuse_other_definer and user.strip("`'") != (self.conn.user or db.current_user):
                raise FakeDisconnect(1227, 'Access denied; you need (at least one of) the SUPER privilege(s) for this operation')
            if self.conn.current_db != vdb:
                raise FakeDisconnect('view body resolves against the current database; USE the replacement first')
            for qual, ref in re.findall(r'\b(?:FROM|JOIN)\s+(?:`([^`]+)`\.)?`([^`]+)`', body, re.I):
                target = db.databases.get(qual or vdb) or {'tables': {}, 'views': {}}
                if ref not in target['tables'] and ref not in target['views']:
                    raise FakeDisconnect(1146, "Table '%s.%s' doesn't exist" % (qual or vdb, ref))
            definer = '%s@%s' % (user.strip("`'"), host.strip("`'")) if user else '%s@%%' % (self.conn.user or db.current_user)
            db.databases[vdb]['views'][name] = FakeView(s, definer)
            return
        m = re.match(r'SELECT DEFINER FROM information_schema\.VIEWS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s$', s)
        if m:
            d = db.databases.get(args[0])
            self.rows = [{'DEFINER': view_definer(d['views'][args[1]])}] if d and args[1] in d['views'] else []
            return
        if s == 'SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s ORDER BY TABLE_NAME':
            d = db.databases.get(args[0]) or {'tables': {}, 'views': {}}
            objects = [(t, 'BASE TABLE') for t in d['tables']] + [(v, 'VIEW') for v in d['views']]
            self.rows = [{'TABLE_NAME': n, 'TABLE_TYPE': kind} for n, kind in sorted(objects)]
            return
        m = re.match(r'SHOW CREATE VIEW `([^`]+)`\.`([^`]+)`$', s)
        if m:
            view = db.databases[m.group(1)]['views'][m.group(2)]
            self.rows = [{'View': m.group(2), 'Create View': show_create_view(m.group(2), view)}]
            return
        m = re.match(r"SELECT COUNT\(\*\) AS n FROM information_schema\.VIEWS WHERE TABLE_SCHEMA = %s AND SUBSTRING_INDEX\(DEFINER, '@', 1\) = %s$", s)
        if m:
            d = db.databases.get(args[0])
            self.rows = [{'n': sum(1 for v in (d['views'].values() if d else ()) if str(view_definer(v)).split('@', 1)[0] == args[1])}]
            return
        m = re.match(r'SELECT DATABASE_NAME, ATTACH_TYPE, STORAGE_ID, COMPUTE_ID, ATTACH_SYNTAX FROM information_schema\.MV_CLOUD_ATTACHED_DATABASES WHERE DATABASE_NAME = %s$', s)
        if m:
            self.rows = [dict(a) for a in db.attachments if a['DATABASE_NAME'] == args[0] and self._sees_storage(a)]
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.MV_CLOUD_ATTACHED_DATABASES WHERE DATABASE_NAME = %s '
                     r'AND \(STORAGE_ID <> %s OR ATTACH_TYPE <> %s\)$', s)
        if m:
            self.rows = [{'n': sum(1 for a in db.attachments if a['DATABASE_NAME'] == args[0] and self._sees_storage(a)
                                   and (a['STORAGE_ID'] != args[1] or a['ATTACH_TYPE'] != args[2]))}]
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.MV_CLOUD_ATTACHED_DATABASES WHERE DATABASE_NAME = %s AND STORAGE_ID <> %s$', s)
        if m:
            self.rows = [{'n': sum(1 for a in db.attachments if a['DATABASE_NAME'] == args[0] and self._sees_storage(a) and a['STORAGE_ID'] != args[1])}]
            return
        m = re.match(r'SELECT COUNT\(\*\) = 0 AS n FROM information_schema\.MV_CLOUD_ATTACHED_DATABASES '
                     r'WHERE DATABASE_NAME = %s AND STORAGE_ID = %s AND ATTACH_TYPE = %s$', s)
        if m:
            self.rows = [{'n': int(not any(a['DATABASE_NAME'] == args[0] and a['STORAGE_ID'] == args[1] and a['ATTACH_TYPE'] == args[2]
                                           and self._sees_storage(a) for a in db.attachments))}]
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.MV_CLOUD_ATTACHED_DATABASES WHERE STORAGE_ID = %s( AND DATABASE_NAME <> %s)?$', s)
        if m:
            self.rows = [{'n': sum(1 for a in db.attachments if a['STORAGE_ID'] == args[0] and self._sees_storage(a)
                                   and (not m.group(1) or a['DATABASE_NAME'] != args[1]))}]
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.MV_CLOUD_BRANCHES WHERE DATABASE_NAME = %s AND STORAGE_ID = %s '
                     r'AND PREV_DATABASE_NAME = %s AND IS_DROPPED = 0 AND IS_DETACHED = 0$', s)
        if m:
            self.rows = [{'n': sum(1 for b in db.branches if b['DATABASE_NAME'] == args[0] and b['STORAGE_ID'] == args[1]
                                   and b.get('PREV_DATABASE_NAME') == args[2] and not b['IS_DROPPED'] and not b['IS_DETACHED'])}]
            return
        m = re.match(r'SELECT STORAGE_ID, DATABASE_NAME, IS_DETACHED, .* FROM information_schema\.MV_CLOUD_BRANCHES WHERE STORAGE_ID = %s$', s)
        if m:
            self.rows = [dict(b) for b in db.branches if b['STORAGE_ID'] == args[0] and self._sees_storage(b)]
            return
        m = re.match(r'DETACH DATABASE `([^`]+)`( KILL)?$', s)
        if m:
            db.writes.append(s)
            att = db.attachment(m.group(1))
            if att is None:
                raise FakeDisconnect('database %s is not attached' % m.group(1))
            if att['ATTACH_TYPE'] == 'READWRITE' and db.processlist and not m.group(2):
                raise FakeDisconnect('detach waits for writers')
            if m.group(2):
                db.processlist = [r for r in db.processlist if r.get('DB') != m.group(1)]
                db.detach_blocks.discard(m.group(1))
            for p in db.pipelines + db.pipeline_offsets:
                if p.get('DATABASE_NAME', db.source) == m.group(1):
                    p['_storage'] = att['STORAGE_ID']
                    p['DATABASE_NAME'] = None
            db.attachments.remove(att)
            for b in db.branches:
                if b['STORAGE_ID'] == att['STORAGE_ID'] and (not db.branch_keeps_storage_id or b['DATABASE_NAME'] == m.group(1)):
                    b['IS_DETACHED'] = 1
                    b['IS_ATTACHED_TO_THIS_COMPUTE'] = 0
            content = db.databases.pop(m.group(1), None)
            if content is not None:
                if not hasattr(db, 'storage_content'):
                    db.storage_content = {}
                db.storage_content[att['STORAGE_ID']] = content
            return
        if re.match(r"ATTACH DATABASE .* STORAGEID '", s):
            raise FakeDisconnect(1064, "You have an error in your SQL syntax; check the manual that corresponds to your MySQL "
                                         "server version for the right syntax to use near the quoted STORAGEID")
        if re.match(r'ATTACH DATABASE .* STORAGEID \S+ READ ONLY$', s):
            raise FakeDisconnect(1064, "You have an error in your SQL syntax; check the manual that corresponds to your MySQL "
                                         "server version for the right syntax to use near 'READ ONLY' at line 1")
        m = re.match(r"ATTACH DATABASE `([^`]+)`(?: AS `([^`]+)`)?( READ ONLY)?(?: STORAGEID ([A-Za-z0-9_:-]+))?$", s)
        if m:
            db.writes.append(s)
            parent, alias, ro, sid = m.group(1), m.group(2), bool(m.group(3)), m.group(4)
            name = alias or parent
            if db.attachment(name) is not None:
                raise FakeDisconnect('a database named %s is already attached' % name)
            source_sid = sid or next((b['STORAGE_ID'] for b in db.branches if b['DATABASE_NAME'] == parent), None)
            if source_sid is None or source_sid not in db.storage:
                raise FakeDisconnect('unknown storage for ' + parent)
            if alias:
                new_sid = source_sid if db.branch_keeps_storage_id else db.new_sid()
                db.storage.setdefault(new_sid, name)
                db.branches.append({'STORAGE_ID': new_sid, 'DATABASE_NAME': name, 'IS_DETACHED': 0, 'IS_ATTACHED_TO_THIS_COMPUTE': 1, 'IS_ATTACHABLE': 1,
                                    'IS_DROPPED': 0, 'PREV_DATABASE_NAME': parent, 'LATEST_ATTACH_TIME': 'now'})
                use_sid = new_sid
            else:
                for b in db.branches:
                    if b['STORAGE_ID'] == source_sid:
                        b['IS_DETACHED'] = 0
                        b['IS_ATTACHED_TO_THIS_COMPUTE'] = 1
                use_sid = source_sid
            parent_att = db.attachment(parent)
            if alias:
                carried = [p for p in db.pipelines + db.pipeline_offsets
                           if p.get('_storage') == source_sid and p.get('DATABASE_NAME') is None
                           or (parent_att is not None and parent_att['STORAGE_ID'] == source_sid and p.get('DATABASE_NAME', db.source) == parent)]
                for p in carried:
                    copy_ = dict(p, DATABASE_NAME=name, _storage=use_sid)
                    if 'STATE' in copy_:
                        copy_['STATE'] = 'Stopped'
                    (db.pipelines if 'STATE' in p else db.pipeline_offsets).append(copy_)
            else:
                for p in db.pipelines + db.pipeline_offsets:
                    if p.get('_storage') == source_sid and p.get('DATABASE_NAME') is None:
                        p['DATABASE_NAME'] = name
            compute = 'cid-%d' % (len(db.attachments) + len(db.branches)) if db.branch_keeps_storage_id and alias else 'cid-1'
            db.attachments.append({'DATABASE_NAME': name, 'ATTACH_TYPE': 'READONLY' if ro else 'READWRITE', 'STORAGE_ID': use_sid, 'COMPUTE_ID': compute, 'ATTACH_SYNTAX': s})
            content = db.storage_content.get(source_sid) if hasattr(db, 'storage_content') else None
            if content is None and alias and parent in db.databases:
                content = db.databases[parent]
            if content is not None:
                db.databases[name] = {'layout': dict(content['layout']), 'tables': {k: {'create': v['create'], 'rows': [dict(r) for r in v['rows']], 'auto_increment': v.get('auto_increment')} for k, v in content['tables'].items()},
                                      'views': dict(content['views']), 'readonly': ro}
            return
        m = re.match(r'DROP DATABASE `([^`]+)`$', s)
        if m:
            db.writes.append(s)
            att = db.attachment(m.group(1))
            if att is None and m.group(1) not in db.databases:
                raise FakeDisconnect(1008, "Can't drop database '%s'; database doesn't exist" % m.group(1))
            if att is not None:
                db.attachments.remove(att)
            db.databases.pop(m.group(1), None)
            db.pipelines = [p for p in db.pipelines if p.get('DATABASE_NAME', db.source) != m.group(1)]
            db.pipeline_offsets = [o for o in db.pipeline_offsets if o.get('DATABASE_NAME', db.source) != m.group(1)]
            return
        m = re.match(r'(STOP|START) PIPELINE `([^`]+)`\.`([^`]+)`$', s)
        if m:
            db.writes.append(s)
            for p in self._pipes(m.group(2)):
                if p['PIPELINE_NAME'] == m.group(3):
                    want = 'Stopped' if m.group(1) == 'STOP' else 'Running'
                    if p['STATE'] == want:
                        raise FakeDisconnect(1, 'pipeline %s is already %s' % (m.group(3), want))
                    p['STATE'] = want
                    break
            else:
                raise FakeDisconnect('no pipeline %s in %s' % (m.group(3), m.group(2)))
            return
        if s.startswith('SELECT PIPELINE_NAME, STATE FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s'):
            self.rows = [{'PIPELINE_NAME': p['PIPELINE_NAME'], 'STATE': p['STATE']} for p in self._pipes(args[0])]
            return
        if s == "SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s AND STATE = 'Stopped'":
            self.rows = [{'n': sum(1 for p in self._pipes(args[0]) if p['PIPELINE_NAME'] == args[1] and p['STATE'] == 'Stopped')}]
            return
        if s == "SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s AND STATE = 'Running'":
            self.rows = [{'n': sum(1 for p in self._pipes(args[0]) if p['PIPELINE_NAME'] == args[1] and p['STATE'] == 'Running')}]
            return
        if s == 'SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s':
            self.rows = [{'n': sum(1 for p in self._pipes(args[0]) if p['PIPELINE_NAME'] == args[1])}]
            return
        if s.startswith("SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s AND STATE = 'Running'"):
            self.rows = [{'n': sum(1 for p in self._pipes(args[0]) if p['STATE'] == 'Running')}]
            return
        if s.startswith('SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s'):
            self.rows = [{'n': len(self._pipes(args[0]))}]
            return
        if s.startswith('SELECT PIPELINE_NAME, SOURCE_TYPE, SOURCE_PARTITION_ID'):
            self.rows = [dict(o) for o in self._offsets(args[0])]
            return
        if s.startswith('SELECT LATEST_SUCCESSFUL_OFFSET AS successful, LATEST_LOADED_OFFSET AS loaded FROM information_schema.PIPELINES_OFFSETS'):
            self.rows = [{'successful': o.get('LATEST_SUCCESSFUL_OFFSET'), 'loaded': o.get('LATEST_LOADED_OFFSET')}
                         for o in self._offsets(args[0], args[1]) if str(o['SOURCE_PARTITION_ID']) == str(args[2])]
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.PIPELINES_OFFSETS WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s '
                     r'AND SOURCE_PARTITION_ID IN \(', s)
        if m:
            wanted = {str(a) for a in args[2:]}
            self.rows = [{'n': sum(1 for o in self._offsets(args[0], args[1]) if str(o['SOURCE_PARTITION_ID']) in wanted)}]
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.PIPELINES_OFFSETS WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s AND \(', s)
        if m:
            pairs = list(zip(args[2::2], args[3::2]))
            self.rows = [{'n': sum(1 for o in self._offsets(args[0], args[1]) for part, loaded in pairs
                                   if str(o['SOURCE_PARTITION_ID']) == str(part) and o.get('EARLIEST_OFFSET') is not None and o['EARLIEST_OFFSET'] > int(loaded))}]
            return
        if s == 'SELECT @@GLOBAL.query_parallelism_per_leaf_core AS v':
            self.rows = [{'v': db.globals.get('query_parallelism_per_leaf_core', '0')}]
            return
        m = re.match(r'SELECT MIN\(`([^`]+)`\) AS m FROM `([^`]+)`\.`([^`]+)`$', s)
        if m:
            t = db.databases[m.group(2)]['tables'][m.group(3)]
            values = [r[m.group(1)] for r in t['rows'] if r.get(m.group(1)) is not None]
            self.rows = [{'m': min(values) if values else None}]
            return
        if s.startswith('SELECT PIPELINE_NAME, BATCH_STATE'):
            self.rows = [dict(b) for b in db.pipeline_batches]
            return
        if s.startswith('SELECT PIPELINE_NAME, ERROR_UNIX_TIMESTAMP'):
            since = dt.datetime.strptime(str(args[1]), '%Y-%m-%d %H:%M:%S').replace(tzinfo=dt.timezone.utc).timestamp()
            self.rows = [dict(e) for e in db.pipeline_errors if self.conn.is_admin and e.get('DATABASE_NAME', db.source) == args[0]
                         and float(e.get('ERROR_UNIX_TIMESTAMP', 0)) >= since]
            return
        m = re.match(r"SHOW CREATE PIPELINE `([^`]+)`\.`([^`]+)` EXTENDED$", s)
        if m:
            p = next((p for p in self._pipes(m.group(1)) if p['PIPELINE_NAME'] == m.group(2)), None)
            if p is None:
                self.rows = []
                return
            create = p.get('definition', "CREATE PIPELINE `%s` AS LOAD DATA KAFKA 'b/t' CREDENTIALS <CREDENTIALS REDACTED> INTO TABLE `t`" % m.group(2))
            alters = p.get('alter', "ALTER PIPELINE `%s` SET OFFSETS '{\"0\": 5}'" % m.group(2))
            alters = [alters] if isinstance(alters, str) else list(alters)
            self.rows = [{'Pipeline': m.group(2), 'Create Pipeline': create}] + [{'Pipeline': m.group(2), 'Create Pipeline': a} for a in alters]
            return
        m = re.match(r'CREATE (?:OR REPLACE )?(?:AGGREGATOR )?PIPELINE `([^`]+)` AS (.*)$', s)
        if m:
            db.writes.append(s)
            if '<<CREDENTIALS>>' in s or '<redacted>' in s or 'REDACTED' in s:
                raise FakeDisconnect('credentials placeholder reached the server')
            target = self.conn.current_db or db.source
            if any(p['PIPELINE_NAME'] == m.group(1) for p in self._pipes(target)):
                raise FakeDisconnect(1, 'Pipeline %s already exists' % m.group(1))
            db.pipelines.append({'PIPELINE_NAME': m.group(1), 'STATE': 'Stopped', 'DATABASE_NAME': target, 'definition': s})
            return
        m = re.match(r"ALTER PIPELINE `([^`]+)` SET OFFSET CURSOR (.*)$", s)
        if m:
            db.writes.append(s)
            return
        m = re.match(r"ALTER PIPELINE `([^`]+)` SET OFFSETS '(.*)'$", s)
        if m:
            db.writes.append(s)
            import json as _json
            offsets = _json.loads(m.group(2))
            dbname = self.conn.current_db or db.source
            for partition, value in offsets.items():
                row = next((o for o in db.pipeline_offsets if o.get('DATABASE_NAME', db.source) == dbname and o['PIPELINE_NAME'] == m.group(1)
                            and str(o['SOURCE_PARTITION_ID']) == str(partition)), None)
                if row is None:
                    row = {'DATABASE_NAME': dbname, 'PIPELINE_NAME': m.group(1), 'SOURCE_TYPE': 'KAFKA', 'SOURCE_PARTITION_ID': partition, 'EARLIEST_OFFSET': 0}
                    db.pipeline_offsets.append(row)
                # assumption to confirm on the first live test: a stopped, never-run pipeline reports the offsets it was set to
                row.update(LATEST_OFFSET=value, LATEST_LOADED_OFFSET=value, LATEST_SUCCESSFUL_OFFSET=value)
            return
        m = re.match(r'(GRANT|REVOKE) (.*) ON (\*\.\*|`([^`]+)`\.\*|`([^`]+)`\.`([^`]+)`) (?:TO|FROM) (.*)$', s)
        if m:
            db.writes.append(s)
            verb, privs, _, sdb, tdb, table, grantee = m.groups()
            privs = [x.strip() for x in privs.split(',')]
            if sdb:
                store, extra = 'schema_privileges', {'TABLE_SCHEMA': sdb}
            elif tdb:
                store, extra = 'table_privileges', {'TABLE_SCHEMA': tdb, 'TABLE_NAME': table}
            else:
                store, extra = 'privileges', {}
            rows = getattr(db, store)

            def _match(r, p):
                return r['GRANTEE'] == grantee and r['PRIVILEGE_TYPE'] == p and all(r.get(k) == v for k, v in extra.items())
            if verb == 'GRANT':
                for p in privs:
                    if not any(_match(r, p) for r in rows):
                        rows.append(dict({'GRANTEE': grantee, 'PRIVILEGE_TYPE': p}, **extra))
            else:
                setattr(db, store, [r for r in rows if not any(_match(r, p) for p in privs)])
            return
        m = re.match(r'KILL (?:CONNECTION )?(\d+)$', s)
        if m:
            db.writes.append(s)
            tid = int(m.group(1))
            before = len(db.processlist)
            db.processlist = [r for r in db.processlist if int(r['ID']) != tid or r.get('AGGREGATOR', self.conn.aggregator) != self.conn.aggregator]
            if len(db.processlist) == before:
                raise FakeDisconnect(1094, 'Unknown thread id: %d' % tid)
            db.killed.append(tid)
            return
        m = re.match(r"CREATE USER ('[^']+'@'[^']+') IDENTIFIED BY " + r"'([^']*)'$", s)
        if m:
            db.writes.append(s)
            if m.group(2) == '<password>':
                raise FakeDisconnect('the password placeholder reached the server')
            db.users.add(m.group(1))
            return
        m = re.match(r"DROP USER ('[^']+'@'[^']+')$", s)
        if m:
            db.writes.append(s)
            db.users.discard(m.group(1))
            db.privileges = [r for r in db.privileges if r['GRANTEE'] != m.group(1)]
            return
        m = re.match(r'SET GLOBAL (\w+) = (.*)$', s)
        if m:
            db.writes.append(s)
            db.globals[m.group(1)] = m.group(2)
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.USER_PRIVILEGES WHERE GRANTEE = %s$', s)
        if m:
            self.rows = [{'n': sum(1 for r in db.privileges if r['GRANTEE'] == args[0] and self._sees_grantee(r['GRANTEE']))}]
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.USER_PRIVILEGES WHERE GRANTEE = %s AND PRIVILEGE_TYPE IN \(', s)
        if m:
            self.rows = [{'n': sum(1 for r in db.privileges if r['GRANTEE'] == args[0] and r['PRIVILEGE_TYPE'] in args[1:])}]
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.SCHEMA_PRIVILEGES WHERE GRANTEE = %s AND TABLE_SCHEMA = %s AND PRIVILEGE_TYPE IN \(', s)
        if m:
            self.rows = [{'n': sum(1 for r in db.schema_privileges if r['GRANTEE'] == args[0] and r['TABLE_SCHEMA'] == args[1] and r['PRIVILEGE_TYPE'] in args[2:])}]
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.TABLE_PRIVILEGES WHERE GRANTEE = %s AND TABLE_SCHEMA = %s AND TABLE_NAME = %s', s)
        if m:
            self.rows = [{'n': sum(1 for r in db.table_privileges if r['GRANTEE'] == args[0] and r['TABLE_SCHEMA'] == args[1]
                                    and r['TABLE_NAME'] == args[2] and r['PRIVILEGE_TYPE'] in args[3:])}]
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.PROCESSLIST WHERE \(DB = %s OR INFO LIKE %s\) AND INFO IS NOT NULL AND USER NOT IN \(%s, %s\)$', s)
        if m:
            needle = str(args[1]).strip('%')
            self.rows = [{'n': sum(1 for r in db.processlist if (r.get('DB') == args[0] or needle in str(r.get('INFO') or ''))
                                    and r.get('INFO') is not None and r.get('USER') not in (args[2], args[3]))}]
            return
        m = re.match(r"SELECT COUNT\(\*\) AS n FROM information_schema\.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'$", s)
        if m:
            d = db.databases.get(args[0])
            self.rows = [{'n': len(d['tables']) if d else 0}]
            return
        m = re.match(r'SELECT COUNT\(\*\) AS n FROM information_schema\.VIEWS WHERE TABLE_SCHEMA = %s$', s)
        if m:
            d = db.databases.get(args[0])
            self.rows = [{'n': len(d['views']) if d else 0}]
            return
        if s == 'SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = %s ORDER BY ROUTINE_NAME':
            self.rows = sorted(({'ROUTINE_NAME': r['ROUTINE_NAME'], 'ROUTINE_TYPE': r['ROUTINE_TYPE']} for r in db.routines
                                if r.get('ROUTINE_SCHEMA', db.source) == args[0]), key=lambda r: r['ROUTINE_NAME'])
            return
        if self._capture(s, args):
            return
        raise AssertionError('fake database does not understand: ' + s)

    def _capture(self, s, args):
        """The P3 and O4 capture reads (inventory.capture): objects, routines, columns and keys parsed from each CREATE TABLE
        text, row estimates (the row count), sizes (none), SHOW CREATE VIEW. Returns True when s is one of them."""
        db = self.db
        d = db.databases.get(args[0]) if args else None
        if s.startswith('SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s'):
            objects = [(t, 'BASE TABLE') for t in (d['tables'] if d else {})] + [(v, 'VIEW') for v in (d['views'] if d else {})]
            self.rows = [{'TABLE_NAME': name, 'TABLE_TYPE': kind} for name, kind in sorted(objects)]
            return True
        if s.startswith('SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = %s'):
            self.rows = [dict(r) for r in db.routines if r.get('ROUTINE_SCHEMA', db.source) == args[0]]
            return True
        if s.startswith('SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION') and 'information_schema.COLUMNS' in s:
            self.rows = []
            for table, spec in sorted((d['tables'] if d else {}).items()):
                lines = [m for m in (re.match(r'\s*`([^`]+)` (\S+)(.*)$', line) for line in spec['create'].splitlines()) if m]
                for i, m in enumerate(lines, 1):
                    rest = m.group(3).upper()
                    self.rows.append({'TABLE_NAME': table, 'COLUMN_NAME': m.group(1), 'ORDINAL_POSITION': i, 'COLUMN_DEFAULT': None,
                                      'IS_NULLABLE': 'NO' if 'NOT NULL' in rest else 'YES', 'DATA_TYPE': m.group(2).split('(')[0],
                                      'COLUMN_TYPE': m.group(2), 'EXTRA': 'auto_increment' if 'AUTO_INCREMENT' in rest else '',
                                      'GENERATION_EXPRESSION': ''})
            return True
        if s.startswith('SELECT TABLE_NAME, NON_UNIQUE, INDEX_NAME') and 'information_schema.STATISTICS' in s:
            self.rows = []
            for table, spec in sorted((d['tables'] if d else {}).items()):
                key = re.search(r'PRIMARY KEY \(([^)]*)\)', spec['create'])
                for i, column in enumerate(re.findall(r'`([^`]+)`', key.group(1)) if key else [], 1):
                    self.rows.append({'TABLE_NAME': table, 'NON_UNIQUE': 0, 'INDEX_NAME': 'PRIMARY', 'SEQ_IN_INDEX': i, 'COLUMN_NAME': column,
                                      'INDEX_TYPE': 'HASH'})
            return True
        if s.startswith('SELECT TABLE_NAME, SUM(ROWS) AS estimated_rows FROM information_schema.TABLE_STATISTICS'):
            self.rows = [{'TABLE_NAME': t, 'estimated_rows': len(spec['rows'])} for t, spec in sorted((d['tables'] if d else {}).items())]
            return True
        if s.startswith('SELECT f.TABLE_NAME, SUM(f.SIZE)'):
            self.rows = []
            return True
        m = re.match(r'SHOW CREATE VIEW `([^`]+)`\.`([^`]+)`$', s)
        if m:
            view = db.databases[m.group(1)]['views'][m.group(2)]
            self.rows = [{'View': m.group(2), 'Create View': view if isinstance(view, str) else str(view)}]
            return True
        return False


class FakeConn:
    def __init__(self, db, user=None):
        self.db = db
        self.user = user
        self.is_admin = user is None or user in db.admin_users
        self.id = db.next_connection_id()
        self.aggregator = self.id % db.aggregators
        self.kill_event = threading.Event()
        self.lock_wait_timeout = 60
        self.current_db = None
        self.closed = False

    def cursor(self):
        return FakeCursor(self.db, self)

    def close(self):
        self.closed = True


class FakeDB:
    """The fake workspace: one source database with tables and rows, plus whatever the kit creates."""

    def __init__(self, source, tables, layout=GOOD_LAYOUT, views=None, current_user='s2rebuild_runner'):
        self.source = source
        self.statements = []
        self.writes = []
        self.fail_insert = {}
        self.fail_drop = set()
        self.refuse_other_definer = False  # Helios: another account as DEFINER needs SUPER (error 1227)
        self.refuse_sharded_counter = False
        self.branch_keeps_storage_id = False  # Helios 10.8.0: ATTACH x AS y keeps x's STORAGE_ID; y gets its own COMPUTE_ID  # Helios: ALTER TABLE ... AUTO_INCREMENT on a sharded table is error 1749
        self.lock_tables = set()
        self.lock_hits = 0
        self.lock_wait_settings = []
        self.synced = []
        self.target_layout_override = {}
        self.lock = threading.RLock()
        self.current_user = current_user
        self.privileges = [{'GRANTEE': "'%s'@'%%'" % current_user, 'PRIVILEGE_TYPE': p}
                           for p in ('SELECT', 'INSERT', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'CREATE DATABASE', 'DROP DATABASE', 'PROCESS',
                                     'CREATE VIEW', 'SHOW VIEW')]
        self.processlist = []
        self.aggregators = 1
        self.schema_privileges = []
        self.table_privileges = []
        self.admin_users = {'ops_owner', 'admin'}
        self.detach_blocks = set()
        self.insert_delay = 0
        self.leaf_cores = 16
        self.engine_version = '8.9.0'
        self.waiting_detach = threading.Event()
        self.connection_counter = 1000
        self.pipelines = []
        self.pipeline_offsets = []
        self.pipeline_errors = []
        self.pipeline_batches = []
        self.routines = []  # {'ROUTINE_NAME', 'ROUTINE_TYPE', optional 'ROUTINE_SCHEMA' (default the source)}
        self.killed = []
        self.users = set()
        self.globals = {}
        self.sid_counter = 100
        self.attachments = [{'DATABASE_NAME': source, 'ATTACH_TYPE': 'READWRITE', 'STORAGE_ID': '1028600672070281646:7367158985892520834', 'COMPUTE_ID': 'cid-1', 'ATTACH_SYNTAX': None}]
        self.branches = [{'STORAGE_ID': '1028600672070281646:7367158985892520834', 'DATABASE_NAME': source, 'IS_DETACHED': 0, 'IS_ATTACHED_TO_THIS_COMPUTE': 1, 'IS_ATTACHABLE': 1,
                          'IS_DROPPED': 0, 'PREV_DATABASE_NAME': None, 'LATEST_ATTACH_TIME': '2026-01-01 00:00:00'}]
        self.storage = {'1028600672070281646:7367158985892520834': source}
        self.databases = {source: {'layout': dict(layout), 'tables': {
            name: {'create': spec['create'], 'rows': [dict(r) for r in spec.get('rows', [])], 'auto_increment': spec.get('auto_increment')}
            for name, spec in tables.items()}, 'views': dict(views or {})}}
        self.connections = []
        self.faults = []
        self.faults_fired = []
        self.connect_faults = []
        self.changes = []
        self.denials = []
        self.admin_only_storage = set()

    def fail_statement(self, pattern, when='before', crash=False, times=1):
        """Fail the next statement matching pattern (a regular expression searched in the normalized text).

        when 'before': the server never sees the statement. when 'after': the server applied it
        and the client lost the reply. crash=True raises SimulatedCrash (kill -9) instead of a
        lost connection. The fault fires at most times times; faults_fired lists what fired.
        """
        if when not in ('before', 'after'):
            raise AssertionError('when is before or after, not %r' % when)
        self.faults.append({'pattern': re.compile(pattern), 'when': when, 'crash': crash, 'left': int(times)})

    def fault(self, s, when):
        """Raise the first armed fault for statement text s at point when ('before' or 'after')."""
        with self.lock:
            hit = next((f for f in self.faults if f['when'] == when and f['left'] > 0 and f['pattern'].search(s)), None)
            if hit is None:
                return
            hit['left'] -= 1
            self.faults_fired.append((when, s))
        if hit['crash']:
            raise SimulatedCrash('killed %s the statement: %s' % (when, s[:80]))
        if when == 'before':
            raise FakeDisconnect(2013, 'Lost connection to server before the statement ran (injected)')
        raise FakeDisconnect(2013, 'Lost connection to server after the statement was applied (injected)')

    def fail_connect(self, user, crash=False, times=1):
        """Fail the next connection opened for user: error 2003 (server unreachable), or SimulatedCrash with crash=True."""
        self.connect_faults.append({'user': user, 'crash': crash, 'left': int(times)})

    def change_before(self, pattern, action, times=1):
        """Call action(db) just before the next statement matching pattern reaches the server (another actor changed the state)."""
        self.changes.append({'pattern': re.compile(pattern), 'action': action, 'left': int(times)})

    def deny(self, user, pattern):
        """Refuse every statement matching pattern on a connection of user with error 1142 (the account lacks the privilege)."""
        self.denials.append((user, re.compile(pattern)))

    def check_denied(self, s, user):
        """Raise error 1142 when a denial covers statement text s for user; the server never runs it."""
        if any(u == user and p.search(s) for u, p in self.denials):
            raise FakeDisconnect(1142, 'command denied to user %s (injected)' % user)

    def run_changes(self, s):
        """Apply the first armed change for statement text s."""
        with self.lock:
            hit = next((c for c in self.changes if c['left'] > 0 and c['pattern'].search(s)), None)
            if hit is None:
                return
            hit['left'] -= 1
            hit['action'](self)

    def connect(self, user=None):
        with self.lock:
            hit = next((f for f in self.connect_faults if f['user'] == user and f['left'] > 0), None)
            if hit is not None:
                hit['left'] -= 1
                self.faults_fired.append(('connect', user))
        if hit is not None:
            if hit['crash']:
                raise SimulatedCrash('killed while connecting as %s' % user)
            raise FakeDisconnect(2003, "Can't connect to the server (injected)")
        conn = FakeConn(self, user)
        self.connections.append(conn)
        return conn

    def next_connection_id(self):
        self.connection_counter += 1
        return self.connection_counter

    def attachment(self, name):
        return next((a for a in self.attachments if a['DATABASE_NAME'] == name), None)

    def new_sid(self):
        self.sid_counter += 1
        # Helios STORAGE_IDs are two numbers joined by a colon
        return '%d:%d' % (1000000000000000000 + self.sid_counter, 7000000000000000000 + self.sid_counter)

    def rows(self, db, table):
        return self.databases[db]['tables'][table]['rows']


def row_key(r):
    return tuple(sorted((k, repr(v)) for k, v in r.items()))


def multiset(rows):
    """Rows as a multiset for order-free comparison."""
    return Counter(row_key(r) for r in rows)
