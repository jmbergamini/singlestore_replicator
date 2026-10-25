"""s2rebuild grants: snapshot, revoke, restore and compare the write privileges of other accounts, per scope.

Author: John Bergamini

Section 10.14 of the architecture, used by the Path B lockout at O3, the release at O11 and the
C1 comparison. The snapshot enumerates write-capable grantees from the privilege views at run
time (never from a config guess), excluding the runner, the owner and protected_accounts. Each
privilege keeps its scope: '*.*' (USER_PRIVILEGES), '`db`.*' (SCHEMA_PRIVILEGES for the source)
and '`db`.`table`' (TABLE_PRIVILEGES), so a database-only writer is revoked on the database and
restored exactly where it was. A snapshot is {grantee: {scope: [privileges]}}; an older journal
row holding a plain list is read as global. Statements issued through the lockout and O11 owner
guards: REVOKE and GRANT, one per grantee and scope. Path A keeps a read-only reference snapshot
(journal meta grants_reference) for the C1 comparison; nothing is revoked from it.
"""
import json
import os

from . import inventory
from .common import KitError
from .sqlutil import ident, qualified

REVOKE_PRIVILEGES = ('INSERT', 'UPDATE', 'DELETE', 'ALTER', 'CREATE', 'DROP', 'INDEX', 'LOCK TABLES', 'EXECUTE', 'CREATE PIPELINE',
                     'ALTER PIPELINE', 'DROP PIPELINE', 'START PIPELINE', 'CREATE VIEW', 'DROP VIEW', 'ALTER VIEW')
GLOBAL = '*.*'
REFERENCE_META = 'grants_reference'


def _user(grantee):
    return grantee.split('@')[0].strip("'\"`")


def _expand(privilege):
    p = str(privilege).upper()
    if p == 'ALL PRIVILEGES':
        return list(REVOKE_PRIVILEGES)
    return [p] if p in REVOKE_PRIVILEGES else []


def database_scope(db_name):
    """The scope text of a database-level grant: `db`.*."""
    return ident(db_name) + '.*'


def table_scope(db_name, table):
    """The scope text of a table-level grant: `db`.`table`."""
    return qualified(db_name, table)


def normalize(snap):
    """{grantee: {scope: [privileges]}} from a snapshot that may hold plain lists (read as global)."""
    out = {}
    for grantee, value in (snap or {}).items():
        scopes = value if isinstance(value, dict) else {GLOBAL: list(value)}
        cleaned = {scope: sorted(set(privs)) for scope, privs in scopes.items() if privs}
        if cleaned:
            out[grantee] = cleaned
    return out


def current_privileges(session, db_name, grantees=None):
    """{grantee: {scope: [write privileges]}} from USER_PRIVILEGES, SCHEMA_PRIVILEGES and TABLE_PRIVILEGES for db_name.

    grantees limits the result to those accounts (each listed one appears, possibly empty).
    """
    out = {g: {} for g in grantees} if grantees is not None else {}

    def _add(grantee, scope, privilege):
        if grantees is not None and grantee not in out:
            return
        for p in _expand(privilege):
            out.setdefault(grantee, {}).setdefault(scope, set()).add(p)
    for r in session.query(inventory.USER_PRIVILEGES_SQL):
        _add(r['GRANTEE'], GLOBAL, r['PRIVILEGE_TYPE'])
    for r in session.query(inventory.SCHEMA_PRIVILEGES_SQL, (db_name,)):
        _add(r['GRANTEE'], database_scope(db_name), r['PRIVILEGE_TYPE'])
    for r in session.query(inventory.TABLE_PRIVILEGES_SQL, (db_name,)):
        _add(r['GRANTEE'], table_scope(db_name, str(r['TABLE_NAME'])), r['PRIVILEGE_TYPE'])
    return {g: {s: sorted(v) for s, v in scopes.items() if v} for g, scopes in out.items()}


def snapshot(session, db_name, protected):
    """{grantee: {scope: [write privileges]}} for every write-capable account outside protected (runner, owner, protected_accounts)."""
    found = current_privileges(session, db_name)
    return {g: scopes for g, scopes in sorted(found.items()) if scopes and _user(g) not in set(protected)}


def write_snapshot(path, snap):
    """Write grants-snapshot.json with mode 0600; an existing file is never overwritten (the first snapshot is the truth)."""
    path = str(path)
    if os.path.exists(path):
        return path
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w', encoding='utf8') as fh:
        json.dump(normalize(snap), fh, indent=1, sort_keys=True)
    return path


def read_snapshot_file(path):
    """The snapshot stored in grants-snapshot.json, or {} when the file does not exist."""
    if not os.path.exists(str(path)):
        return {}
    with open(str(path), encoding='utf8') as fh:
        return normalize(json.load(fh))


def revoke_statements(snap):
    """One REVOKE per grantee and scope with exactly the write privileges it holds there."""
    return ['REVOKE %s ON %s FROM %s' % (', '.join(privs), scope, grantee)
            for grantee, scopes in sorted(normalize(snap).items()) for scope, privs in sorted(scopes.items())]


def restore_statements(snap):
    """One GRANT per grantee and scope with exactly the privileges revoked there."""
    return ['GRANT %s ON %s TO %s' % (', '.join(privs), scope, grantee)
            for grantee, scopes in sorted(normalize(snap).items()) for scope, privs in sorted(scopes.items())]


def restored_by_another(session, db_name, snap):
    """{grantee: {scope: [privileges]}} of snapshot privileges that reappeared while the lockout holds."""
    snap = normalize(snap)
    now = current_privileges(session, db_name, snap)
    back = {}
    for grantee, scopes in now.items():
        for scope, privs in scopes.items():
            again = sorted(set(privs) & set(snap.get(grantee, {}).get(scope, privs)))
            if again:
                back.setdefault(grantee, {})[scope] = again
    return back


def compare(session, db_name, snap):
    """Differences between the snapshot and the current privileges: {grantee: (missing, extra)} as 'scope privilege' strings."""
    snap = normalize(snap)
    now = current_privileges(session, db_name, snap)
    diff = {}
    for grantee, want_scopes in snap.items():
        want = {(s, p) for s, privs in want_scopes.items() for p in privs}
        have = {(s, p) for s, privs in now.get(grantee, {}).items() for p in privs}
        missing = sorted('%s %s' % sp for sp in want - have)
        extra = sorted('%s %s' % sp for sp in have - want)
        if missing or extra:
            diff[grantee] = (missing, extra)
    return diff


def privilege_count_check(grantee, scope, privileges, db_name):
    """(sql, params) counting the given privileges a grantee holds at one scope, for block checks."""
    marks = ', '.join(['%s'] * len(privileges))
    if scope == GLOBAL:
        return ('SELECT COUNT(*) AS n FROM information_schema.USER_PRIVILEGES WHERE GRANTEE = %s AND PRIVILEGE_TYPE IN (' + marks + ')',
                (grantee,) + tuple(privileges))
    if scope == database_scope(db_name):
        return ('SELECT COUNT(*) AS n FROM information_schema.SCHEMA_PRIVILEGES WHERE GRANTEE = %s AND TABLE_SCHEMA = %s '
                'AND PRIVILEGE_TYPE IN (' + marks + ')', (grantee, db_name) + tuple(privileges))
    table = scope.split('.', 1)[1].strip('`')
    return ('SELECT COUNT(*) AS n FROM information_schema.TABLE_PRIVILEGES WHERE GRANTEE = %s AND TABLE_SCHEMA = %s AND TABLE_NAME = %s '
            'AND PRIVILEGE_TYPE IN (' + marks + ')', (grantee, db_name, table) + tuple(privileges))


def record_snapshot(journal, snap):
    """Store the snapshot rows in the journal; rows that already exist are kept (a rerun never overwrites them)."""
    written = 0
    for grantee, scopes in normalize(snap).items():
        written += 1 if journal.snapshot_grant(grantee, scopes) else 0
    return written


def snapshot_from_journal(journal):
    """The snapshot as {grantee: {scope: [privileges]}} from the journal."""
    return normalize({g: row['privileges'] for g, row in journal.grants_snapshot().items()})


def unrestored_snapshot(journal):
    """The journal snapshot rows not restored yet (whatever the freeze mode, for example a Path A bridge lockout)."""
    rows = journal.grants_snapshot()
    return normalize({g: row['privileges'] for g, row in rows.items() if not row.get('restored_utc')})


def record_reference(journal, snap):
    """Path A: keep a read-only reference snapshot for the C1 comparison (nothing is revoked from it)."""
    if journal.get_meta(REFERENCE_META) is None:
        journal.set_meta(REFERENCE_META, json.dumps(normalize(snap), sort_keys=True))


def reference_from_journal(journal):
    """The Path A reference snapshot, or {} when none was taken."""
    raw = journal.get_meta(REFERENCE_META)
    return normalize(json.loads(raw)) if raw else {}


def restore(session, journal, db_name, snap, log):
    """GRANT back exactly the revoked privileges, compare, then mark them restored; raises KitError on any difference."""
    for sql in restore_statements(snap):
        session.execute(sql)
    diff = compare(session, db_name, snap)
    if diff:
        raise KitError('Grants after the restore differ from the snapshot: ' + '; '.join(
            '%s missing %s extra %s' % (g, ','.join(m) or 'none', ','.join(e) or 'none') for g, (m, e) in sorted(diff.items())))
    for grantee in normalize(snap):
        journal.mark_grant_restored(grantee)
    log('grants restored for %d account(s); the privilege views equal the snapshot' % len(normalize(snap)))
    return True
