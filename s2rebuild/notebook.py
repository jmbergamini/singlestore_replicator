"""s2rebuild notebook: fill the owner-steps notebook template with the run's names, ids and blocks.

Author: John Bergamini

Section 10.17 of the architecture. Cell 1 holds the ownerlib source and the run's names and ids
as JSON; one cell per admin step calls run_block; pipeline definitions are embedded with the
credentials placeholder, and the O9 cell reads each CREDENTIALS JSON with getpass (never saved
in the notebook) and deletes the variable after the call. The notebook connects with
singlestoredb through portal.connection_url and checks the workspace id (when workspace.id is
configured) and the host before running anything. The O2 result carries the pipeline capture
after the stop, which s2rebuild gate O2 --evidence imports; the lockout cell enumerates the
write-capable accounts at run time and prints its snapshot for s2rebuild gate O3 --evidence.
The file holds definitions, so it is written with mode 0600. Statements issued by this module:
none.
"""
import json
import os
from pathlib import Path

from . import grants, ownersql
from .common import KitError, iso, utc_now

TEMPLATE = Path(__file__).resolve().parent.parent / 'notebooks' / 'owner-steps.template.ipynb'

LOCKOUT_CELL = '''import json
import time
db = RUN["source"]
protected = set(RUN["protected_users"])
write = set(RUN["write_privileges"])


def scoped_privileges():
    """{grantee: {scope: [write privileges]}} from the three privilege views, protected accounts left out."""
    found = {}

    def add(grantee, scope, privilege):
        if grantee.split("@")[0].strip("'") in protected:
            return
        privs = list(write) if privilege.upper() == "ALL PRIVILEGES" else ([privilege.upper()] if privilege.upper() in write else [])
        for p in privs:
            found.setdefault(grantee, {}).setdefault(scope, set()).add(p)
    cur.execute("SELECT GRANTEE, PRIVILEGE_TYPE FROM information_schema.USER_PRIVILEGES")
    for r in _rows(cur):
        add(r["GRANTEE"], "*.*", r["PRIVILEGE_TYPE"])
    cur.execute("SELECT GRANTEE, TABLE_SCHEMA, PRIVILEGE_TYPE FROM information_schema.SCHEMA_PRIVILEGES WHERE TABLE_SCHEMA = %s", (db,))
    for r in _rows(cur):
        add(r["GRANTEE"], "`%s`.*" % db, r["PRIVILEGE_TYPE"])
    cur.execute("SELECT GRANTEE, TABLE_NAME, PRIVILEGE_TYPE FROM information_schema.TABLE_PRIVILEGES WHERE TABLE_SCHEMA = %s", (db,))
    for r in _rows(cur):
        add(r["GRANTEE"], "`%s`.`%s`" % (db, r["TABLE_NAME"]), r["PRIVILEGE_TYPE"])
    return {g: {s: sorted(p) for s, p in scopes.items()} for g, scopes in found.items()}


def revoke(snapshot):
    for grantee, scopes in sorted(snapshot.items()):
        for scope, privs in sorted(scopes.items()):
            cur.execute("REVOKE %s ON %s FROM %s" % (", ".join(privs), scope, grantee))


snapshot = RUN.get("grant_snapshot") or scoped_privileges()
if not snapshot:
    print("no write-capable account outside the protected set; nothing to lock out (check freeze.lockout.protected_accounts)")
else:
    print(json.dumps({"step": "O3", "run_id": RUN["run_id"], "grant_snapshot": snapshot}, indent=1))
    print("save the JSON above and record it: s2rebuild gate O3 --pass --note TEXT --evidence <file>; O11 restores from it")
    revoke(snapshot)
    users = {g.split("@")[0].strip("'") for g in snapshot}
    quiet = 0
    while quiet < RUN["quiet_passes_required"]:
        now = scoped_privileges()
        back = {g: {s: sorted(set(p) & set(snapshot[g].get(s, []))) for s, p in now.get(g, {}).items()} for g in snapshot}
        back = {g: {s: p for s, p in scopes.items() if p} for g, scopes in back.items()}
        back = {g: scopes for g, scopes in back.items() if scopes}
        if back:
            print("GRANT RESTORED BY ANOTHER SESSION:", json.dumps(back))
            revoke(back)
        cur.execute("SELECT ID, USER FROM information_schema.PROCESSLIST WHERE DB = %s OR INFO LIKE %s", (db, "%" + db + "%"))
        rows = [r for r in _rows(cur) if r["USER"] in users]
        for r in rows:
            try:
                cur.execute("KILL CONNECTION %d" % int(r["ID"]))
            except Exception as exc:
                print("kill", r["ID"], type(exc).__name__)
        quiet = quiet + 1 if not rows and not back else 0
        print(time.strftime("%H:%M:%S"), "threads", len(rows), "restored grants", len(back), "quiet passes", quiet)
        time.sleep(RUN["lockout_interval_seconds"])
    print("quiet; run s2rebuild quiet-proof from the terminal to record freeze_time")'''


def credential_keys(items, config):
    """The keys the O9 cell asks for: the per-pipeline entries of pipelines that need credentials, then their source types."""
    from . import pipelines
    needed = [i for i in pipelines.expected_items(items) if pipelines.needs_credentials(i)]
    per = sorted(i['name'] for i in needed if i['name'] in config.pipelines.per_pipeline_env)
    types = sorted({i.get('source_type') for i in needed if i['name'] not in per and i.get('source_type')})
    return per + types


def run_payload(names, config, grant_snapshot=None, items=(), capture_step=None):
    """The RUN dict embedded in cell 1: names, storage ids, host, workspace id, settings; never a password or credential."""
    from . import lockout
    return {'run_id': names.get('run_id'), 'source': names['source'], 'replacement': names['replacement'], 'retained': names['retained'],
            'source_sid': names['source_sid'], 'replacement_sid': names['replacement_sid'], 'retained_sid': names['retained_sid'],
            'live_sid': names['live_sid'], 'host': config.workspace.host, 'workspace_id': config.workspace.get('id'),
            'runner_user': names['runner_user'], 'freeze_mode': names['freeze_mode'],
            'detach_wait_seconds': names['detach_wait_seconds'], 'detach_kill': names['detach_kill'],
            'quiet_passes_required': config.freeze.lockout.quiet_passes_required, 'lockout_interval_seconds': max(1, config.freeze.lockout.interval_seconds),
            'protected_users': sorted(lockout.protected_users(config)), 'write_privileges': list(grants.REVOKE_PRIVILEGES),
            'grant_snapshot': grants.normalize(grant_snapshot or {}), 'credential_keys': credential_keys(items, config),
            'pipeline_capture_step': capture_step, 'generated_utc': iso(utc_now())}


def generate(path, names, config, blocks, grant_snapshot=None, template=TEMPLATE, items=(), capture_step=None):
    """Write the filled notebook to path with mode 0600; blocks is {step id: Block} including 'O10-rollback'. Returns the path."""
    template = Path(template)
    if not template.is_file():
        raise KitError('Notebook template not found at %s' % template)
    nb = json.loads(template.read_text(encoding='utf8'))
    payload = run_payload(names, config, grant_snapshot, items, capture_step)
    blocks_json = json.dumps({k: b.to_dict() for k, b in blocks.items()}, default=str)
    run_json = json.dumps(payload, default=str)
    replacements = {'{{OWNERLIB}}': ownersql.OWNERLIB_SOURCE, '{{RUN_JSON}}': repr(run_json),
                    '{{BLOCKS_JSON}}': repr(blocks_json), '{{LOCKOUT_CELL}}': LOCKOUT_CELL,
                    '{{RUN_ID}}': str(names.get('run_id')), '{{SOURCE}}': names['source'], '{{GENERATED_UTC}}': payload['generated_utc']}
    for cell in nb['cells']:
        source = ''.join(cell['source'])
        for key, value in replacements.items():
            source = source.replace(key, value)
        cell['source'] = source.splitlines(keepends=True)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w', encoding='utf8') as fh:
        fh.write(json.dumps(nb, indent=1))
    os.chmod(str(path), 0o600)
    return path


def first_code_cell(path):
    """The source of the first code cell of a notebook file."""
    nb = json.loads(Path(path).read_text(encoding='utf8'))
    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            return ''.join(cell['source'])
    raise KitError('Notebook %s has no code cell' % path)


def first_cell_compiles(path):
    """True when the first code cell compiles (the acceptance check)."""
    compile(first_code_cell(path), str(path), 'exec')
    return True


def blocks_for_run(ctx, items=None, grant_snapshot=None):
    """Every admin block of the run keyed by step id, plus the O10 rollback; returns (names, blocks, inputs)."""
    names = ownersql.names_for(ctx)
    inputs = ownersql.block_inputs(ctx)
    if items is not None:
        inputs['pipelines'] = items
        inputs['capture_step'] = inputs['capture_step'] or 'O2'
    if grant_snapshot is not None:
        inputs['grants'] = grant_snapshot
    blocks = {}
    for step_id in ('O2', 'O9', 'O11', 'C3', 'C4'):
        blocks[step_id] = ownersql.block_for(step_id, names, ctx.config, **inputs)
    blocks['O3'] = ownersql.block_for('O3', names, None, **inputs)
    blocks['O10'] = ownersql.block_for('O10', names, ctx.config, **inputs)
    blocks['O10-rollback'] = ownersql.block_for('O10', names, ctx.config, rollback=True, **inputs)
    return names, blocks, inputs
