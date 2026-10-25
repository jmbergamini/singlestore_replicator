"""s2rebuild tests fakedb: an in-memory stand-in for the workspace that understands the kit's statement shapes.

Author: John Bergamini

Rows live in Python lists; hash-bucket predicates are evaluated with zlib.crc32 on the string
form of each value, which matches the server's CRC32 for the values the tests use. Fault
injection: fail_insert[(table, predicate)] = 'before' | 'after' | 'lock'; lock_tables holds
(database, table) pairs whose INSERT and DELETE raise error 1205 until removed; the crash hook
of the runner stands in for kill -9.
"""
import re
import threading
import time
import zlib
from collections import Counter

GOOD_LAYOUT = {'NUM_PARTITIONS': 16, 'NUM_SUB_PARTITIONS': 0, 'IS_SYNC': 1, 'IS_SYNC_DURABILITY': 0,
               'IS_SYNC_DURABILITY_ON_REF_DB_ONLY': 1}


class FakeDisconnect(Exception):
    """The connection dropped."""


class LockWait(Exception):
    """Error 1205 as the driver raises it."""

    def __init__(self):
        super().__init__(1205, 'Lock wait timeout exceeded; try restarting transaction')


class SimulatedCrash(BaseException):
    """Stands in for kill -9: not an Exception, so the worker cannot journal it."""


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
            return self._execute(s, args)

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
        rows = list(self.db.processlist)
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
            db.databases[tdb]['tables'][tt] = {'create': create, 'rows': [], 'auto_increment': src.get('auto_increment')}
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
            db.writes.append(s)
            db.databases[m.group(1)]['tables'][m.group(2)]['auto_increment'] = int(m.group(3))
            return
        m = re.match(r'AGGREGATOR SYNC AUTO_INCREMENT ON `([^`]+)`\.`([^`]+)`$', s)
        if m:
            db.writes.append(s)
            db.synced.append(m.group(2))
            return
        m = re.match(r'CREATE VIEW `([^`]+)`\.`([^`]+)` AS (.*)$', s)
        if m:
            db.writes.append(s)
            if self.conn.current_db != m.group(1):
                raise FakeDisconnect('view body resolves against the current database; USE the replacement first')
            db.databases[m.group(1)]['views'][m.group(2)] = s
            return
        m = re.match(r'SELECT DATABASE_NAME, ATTACH_TYPE, STORAGE_ID, COMPUTE_ID, ATTACH_SYNTAX FROM information_schema\.MV_CLOUD_ATTACHED_DATABASES WHERE DATABASE_NAME = %s$', s)
        if m:
            self.rows = [dict(a) for a in db.attachments if a['DATABASE_NAME'] == args[0]]
            return
        m = re.match(r'SELECT STORAGE_ID, DATABASE_NAME, IS_DETACHED, .* FROM information_schema\.MV_CLOUD_BRANCHES WHERE STORAGE_ID = %s$', s)
        if m:
            self.rows = [dict(b) for b in db.branches if b['STORAGE_ID'] == args[0]]
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
                if b['STORAGE_ID'] == att['STORAGE_ID']:
                    b['IS_DETACHED'] = 1
                    b['IS_ATTACHED_TO_THIS_COMPUTE'] = 0
            content = db.databases.pop(m.group(1), None)
            if content is not None:
                if not hasattr(db, 'storage_content'):
                    db.storage_content = {}
                db.storage_content[att['STORAGE_ID']] = content
            return
        m = re.match(r"ATTACH DATABASE `([^`]+)`(?: AS `([^`]+)`)?(?: STORAGEID '([^']+)')?( READ ONLY)?$", s)
        if m:
            db.writes.append(s)
            parent, alias, sid, ro = m.group(1), m.group(2), m.group(3), bool(m.group(4))
            name = alias or parent
            if db.attachment(name) is not None:
                raise FakeDisconnect('a database named %s is already attached' % name)
            source_sid = sid or next((b['STORAGE_ID'] for b in db.branches if b['DATABASE_NAME'] == parent), None)
            if source_sid is None or source_sid not in db.storage:
                raise FakeDisconnect('unknown storage for ' + parent)
            if alias:
                new_sid = db.new_sid()
                db.storage[new_sid] = name
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
            db.attachments.append({'DATABASE_NAME': name, 'ATTACH_TYPE': 'READONLY' if ro else 'READWRITE', 'STORAGE_ID': use_sid, 'COMPUTE_ID': 'cid-1', 'ATTACH_SYNTAX': s})
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
        if s == "SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s AND STATE <> 'Running'":
            self.rows = [{'n': sum(1 for p in self._pipes(args[0]) if p['PIPELINE_NAME'] == args[1] and p['STATE'] != 'Running')}]
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
        if s.startswith('SELECT COUNT(*) AS partitions, SUM(LATEST_LOADED_OFFSET) AS loaded FROM information_schema.PIPELINES_OFFSETS'):
            rows = self._offsets(args[0], args[1])
            loaded = [o['LATEST_LOADED_OFFSET'] for o in rows if o.get('LATEST_LOADED_OFFSET') is not None]
            self.rows = [{'partitions': len(rows), 'loaded': sum(loaded) if loaded else None}]
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
        if s.startswith('SELECT PIPELINE_NAME, ERROR_TIME'):
            self.rows = [dict(e) for e in db.pipeline_errors if self.conn.is_admin and e.get('DATABASE_NAME', db.source) == args[0]
                         and str(e.get('ERROR_TIME', '')) >= str(args[1])]
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
            db.processlist = [r for r in db.processlist if int(r['ID']) != tid]
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
        raise AssertionError('fake database does not understand: ' + s)


class FakeConn:
    def __init__(self, db, user=None):
        self.db = db
        self.user = user
        self.is_admin = user is None or user in db.admin_users
        self.id = db.next_connection_id()
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
        self.killed = []
        self.users = set()
        self.globals = {}
        self.sid_counter = 100
        self.attachments = [{'DATABASE_NAME': source, 'ATTACH_TYPE': 'READWRITE', 'STORAGE_ID': 'sid-source', 'COMPUTE_ID': 'cid-1', 'ATTACH_SYNTAX': None}]
        self.branches = [{'STORAGE_ID': 'sid-source', 'DATABASE_NAME': source, 'IS_DETACHED': 0, 'IS_ATTACHED_TO_THIS_COMPUTE': 1, 'IS_ATTACHABLE': 1,
                          'IS_DROPPED': 0, 'PREV_DATABASE_NAME': None, 'LATEST_ATTACH_TIME': '2026-01-01 00:00:00'}]
        self.storage = {'sid-source': source}
        self.databases = {source: {'layout': dict(layout), 'tables': {
            name: {'create': spec['create'], 'rows': [dict(r) for r in spec.get('rows', [])], 'auto_increment': spec.get('auto_increment')}
            for name, spec in tables.items()}, 'views': dict(views or {})}}
        self.connections = []

    def connect(self, user=None):
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
        return 'sid-%d' % self.sid_counter

    def rows(self, db, table):
        return self.databases[db]['tables'][table]['rows']


def row_key(r):
    return tuple(sorted((k, repr(v)) for k, v in r.items()))


def multiset(rows):
    """Rows as a multiset for order-free comparison."""
    return Counter(row_key(r) for r in rows)
