"""s2rebuild grants: the read-only grants reference of other accounts' write privileges, per scope.

Author: John Bergamini

Section 10.14 of the architecture. O3 records a reference snapshot after the freeze and C1
compares the live privileges with it; nothing is revoked or granted. The snapshot enumerates
write-capable grantees from the privilege views at run time (never from a config guess),
excluding the runner and the owner. Each privilege keeps its scope: '*.*' (USER_PRIVILEGES),
'`db`.*' (SCHEMA_PRIVILEGES for the source) and '`db`.`table`' (TABLE_PRIVILEGES). A snapshot is
{grantee: {scope: [privileges]}} (journal meta grants_reference). Statements issued: SELECT on
the three privilege views.
"""
import json

from . import inventory
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


def protected_users(config):
    """The runner and the owner: the accounts the reference leaves out."""
    return {config.runner.user} | ({config.owner.user} if config.owner is not None else set())


def snapshot(session, db_name, protected):
    """{grantee: {scope: [write privileges]}} for every write-capable account outside protected (protected_users)."""
    found = current_privileges(session, db_name)
    return {g: scopes for g, scopes in sorted(found.items()) if scopes and _user(g) not in set(protected)}


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


def record_reference(journal, snap):
    """O3: keep a read-only reference snapshot for the C1 comparison (nothing is revoked from it)."""
    if journal.get_meta(REFERENCE_META) is None:
        journal.set_meta(REFERENCE_META, json.dumps(normalize(snap), sort_keys=True))


def reference_from_journal(journal):
    """The O3 reference snapshot, or {} when none was taken."""
    raw = journal.get_meta(REFERENCE_META)
    return normalize(json.loads(raw)) if raw else {}
