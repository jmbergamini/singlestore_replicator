"""s2rebuild freeze: O3 Path A, the read-only attachment of the source behind precondition checks.

Author: John Bergamini

Section 10.7 of the architecture. DETACH then ATTACH READ ONLY stops every writer in minutes;
revoke-and-kill (lockout.py) is the fallback only when freeze.mode is lockout. The DETACH waits
at most freeze.detach_wait_seconds; then the kit cancels it on its own connection and issues
DETACH ... KILL when freeze.detach_kill is set (ownersql.detach_with_escalation). O3 is
resumable: when the source is already attached READONLY on the recorded STORAGE_ID (a rerun,
a notebook run or a block run by hand), the kit only checks the attachment, runs the write probe
and records freeze_time. When no database carries the source name and the recorded storage is
detached (a stop between DETACH and ATTACH), the kit prints and runs the re-entry block, which
starts at the ATTACH and never detaches again (owner-sql/O3-reentry.sql; owner-sql O3 writes it
too). The kit reads the position with the owner session when the owner profile is configured,
else as the runner. The write probe is the one runner statement that
names the source in a write, allowed by the probe guard in O3 only; it compares a string column
with a token that cannot exist (a string compared with a numeric column would convert to 0 and
could match), or, on a table without string columns, an integer key with MIN minus one.
Statements issued: DETACH DATABASE (and DETACH ... KILL, KILL QUERY on the kit's own waiting
connection), ATTACH DATABASE ... READ ONLY through the O3 owner guard; the probe DELETE and SET
lock_wait_timeout through the runner probe guard; SELECT on the attachment, branch and privilege
views.
"""
import secrets as random_secrets

from . import grants, ownersql, steps, switch
from .common import EXIT_GATE, KitError, iso, utc_now
from .sqlutil import ident, probe_guard, qualified, read_guard

PIPELINES_RUNNING_SQL = "SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s AND STATE = 'Running'"
READ_ONLY_ERRNOS = (1290, 1836)
STRING_TYPES = ('char', 'varchar', 'text', 'tinytext', 'mediumtext', 'longtext')
INTEGER_TYPES = ('tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint')
PROBE_NOTE = ('The kit then runs the write probe as the runner and records freeze_time. After a notebook or hand run, '
              's2rebuild step O3 --redo does both (it sees the READONLY attachment and resumes there).')
REENTRY_NOTE = ('Re-entry: when the first check finds no rows, an earlier run may have stopped between DETACH and ATTACH. '
                'Then run the re-entry block instead; it starts at the ATTACH: owner-sql/O3-reentry.sql (s2rebuild owner-sql O3 writes it), '
                'or s2rebuild step O3 --redo, which prints and runs it (O3-reentry in the notebook).')


def block(names):
    """The O3 Path A block: re-entry note, checks, DETACH (with the wait and KILL escalation), check, ATTACH READ ONLY, checks, probe note.

    The re-entry note comes first, so a reader who stops at the first check has already read it.
    """
    b = ownersql.Block('O3', 'Freeze: read-only attachment (Path A)')
    b.note(REENTRY_NOTE)
    b.check('source attached READWRITE', switch.ATTACHED_SQL, (names['source'],), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READWRITE'))
    b.check('source STORAGE_ID equals the recorded one', switch.ATTACHED_SQL, (names['source'],), ownersql.Expect('column_equals', 'STORAGE_ID', names['source_sid']))
    b.check('no pipeline Running (O2 passed)', PIPELINES_RUNNING_SQL, (names['source'],), ownersql.Expect('column_equals', 'n', 0))
    b.statement('DETACH DATABASE %s' % ident(names['source']),
                'detach the source; when it waits more than %s s%s' % (names['detach_wait_seconds'],
                                                                         ', cancel it and run DETACH DATABASE %s KILL instead' % ident(names['source']) if names['detach_kill']
                                                                         else ' cancel it and stop the writers first (detach_kill is off; s2rebuild lockout works as a bridge)'),
                detach={'wait': names['detach_wait_seconds'], 'kill': bool(names['detach_kill'])})
    _detached_check(b, names)
    _attach(b, names)
    _after_checks(b, names)
    b.note(PROBE_NOTE)
    return b


def reentry_block(names):
    """The O3 re-entry block between DETACH and ATTACH: checks (storage detached, names free), ATTACH READ ONLY, checks, probe note.

    An earlier run detached the source and stopped before the ATTACH. The replacement check
    tells this state from the switch (S1) or a rollback (R1), which detach the source too: the
    replacement exists only from O5 on, and P2 checked that its name was free.
    """
    b = ownersql.Block('O3', 'Freeze: re-entry between DETACH and ATTACH (Path A)')
    b.note('An earlier run detached the source and stopped before the ATTACH; the block starts at the ATTACH. No DETACH is issued again.')
    _detached_check(b, names)
    b.check('no attached database named source', switch.ATTACHED_SQL, (names['source'],), ownersql.Expect('no_rows'))
    b.check('no attached database named replacement (O5 and the switch have not run)', switch.ATTACHED_SQL, (names['replacement'],),
            ownersql.Expect('no_rows'))
    _attach(b, names)
    _after_checks(b, names)
    b.note(PROBE_NOTE)
    return b


def _detached_check(b, names):
    """The gate ahead of every ATTACH: MV_CLOUD_BRANCHES shows the recorded source storage IS_DETACHED = 1."""
    b.check('source storage detached', switch.BRANCH_SQL, (names['source_sid'],), ownersql.Expect('column_equals', 'IS_DETACHED', 1))


def _attach(b, names):
    b.statement("ATTACH DATABASE %s READ ONLY STORAGEID %s" % (ident(names['source']), names['source_sid']), 'attach the same storage read only')


def _after_checks(b, names):
    b.check('source attached READONLY', switch.ATTACHED_SQL, (names['source'],), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READONLY'), record='source')
    b.check('source STORAGE_ID unchanged', switch.ATTACHED_SQL, (names['source'],), ownersql.Expect('column_equals', 'STORAGE_ID', names['source_sid']))
    # the branch record of the source (name, STORAGE_ID, ATTACH_TYPE, COMPUTE_ID) is recorded whole: the switch and the rollback go by it
    b.check('source record attached on the recorded storage', switch.ATTACHED_RECORD_SQL, (names['source'], names['source_sid']),
            ownersql.Expect('columns_equal', value=ownersql.record_expect(names, 'source')), record='source')
    b.check('source reported by the workspace', switch.LAYOUT_SQL, (names['source'],), ownersql.Expect('some_rows'))


def resume_block(names):
    """The O3 resume block: the source is already READONLY on the recorded storage; checks only, then the probe."""
    b = ownersql.Block('O3', 'Freeze: resume after the read-only attachment (Path A)')
    b.note('The source is already attached READONLY on the recorded STORAGE_ID; no DETACH or ATTACH is issued again.')
    _after_checks(b, names)
    return b


def probe_statement(source, table, column, value=None):
    """The write probe: a DELETE for a value that cannot exist (a random string token, or an integer below MIN)."""
    if value is None:
        return "DELETE FROM %s WHERE %s = '%s'" % (qualified(source, table), ident(column), 's2rebuild-probe-' + random_secrets.token_hex(8))
    return 'DELETE FROM %s WHERE %s = %d' % (qualified(source, table), ident(column), int(value))


def _column_types(inventory_tables):
    out = {}
    for t in inventory_tables or []:
        out[t['name']] = [(c['name'], str(c.get('data_type') or c.get('column_type') or '').lower().split('(')[0]) for c in t.get('columns') or []]
    return out


def probe_target(plans, inventory_tables):
    """(table, column, kind) for the write probe: kind 'string' or 'integer'.

    The smallest non-empty table with a string column wins (its first string key column, else
    its first string column). Without any string column, the smallest non-empty table with an
    integer key column, probed with MIN minus one. Raises KitError when no column suits.
    """
    types = _column_types(inventory_tables)
    ordered = sorted(plans, key=lambda p: (p.est_rows <= 0, p.est_rows, p.name))
    for p in ordered:
        cols = types.get(p.name, [])
        strings = [c for c, t in cols if t in STRING_TYPES]
        keyed = [c for c in p.key_columns if c in strings]
        if keyed or strings:
            return p.name, (keyed or strings)[0], 'string'
    for p in ordered:
        cols = dict(types.get(p.name, []))
        ints = [c for c in p.key_columns if cols.get(c) in INTEGER_TYPES]
        if ints:
            return p.name, ints[0], 'integer'
    raise KitError('No table has a string column or an integer key for the write probe; check READONLY by hand and record the gate')


def smallest_nonempty(plans, inventory_tables):
    """(table, column) of the probe target; kept for callers that only need the names."""
    table, column, _ = probe_target(plans, inventory_tables)
    return table, column


def is_read_only_refusal(exc):
    """True when the server refused a write because the database is read only."""
    args = getattr(exc, 'args', ())
    if args and args[0] in READ_ONLY_ERRNOS:
        return True
    text = str(exc).lower()
    return 'read only' in text or 'read-only' in text or 'readonly' in text


def write_probe(session, source, table, column, log, lock_wait_timeout=5, kind='string'):
    """Issue the probe; pass only when the server refuses the write. A DELETE that succeeds (even 0 rows) fails the gate."""
    value = None
    if kind == 'integer':
        rows = session.query('SELECT MIN(%s) AS m FROM %s' % (ident(column), qualified(source, table)))
        low = rows[0]['m'] if rows and isinstance(rows[0], dict) else (rows[0][0] if rows else None)
        value = (int(low) - 1) if low is not None else -1
    sql = probe_statement(source, table, column, value)
    session.execute('SET lock_wait_timeout = %d' % int(lock_wait_timeout))
    try:
        session.execute(sql)
    except Exception as exc:  # noqa: BLE001 - the refusal is the expected outcome
        if is_read_only_refusal(exc):
            log('write probe on %s.%s refused as read only: freeze verified' % (source, table))
            return True
        raise KitError('Write probe failed for another reason: %s' % log.redactor('%s: %s' % (type(exc).__name__, exc)))
    raise KitError('Write probe on %s.%s was accepted by the server; the source is not read only. Do not continue.' % (source, table))


def source_attachment(session, source):
    """The MV_CLOUD_ATTACHED_DATABASES row of the source, or None."""
    rows = session.query(switch.ATTACHED_SQL, (source,))
    return dict(rows[0]) if rows else None


def storage_detached(session, storage_id, log=None, identity='runner'):
    """True when MV_CLOUD_BRANCHES shows the storage with IS_DETACHED = 1 (a SELECT; issues nothing else).

    A read error (a lost connection, or an account without access to the view) returns False
    and is logged with the identity, the error type and the error number only. The caller then
    gives the whole block, whose first check stops with the observed state.
    """
    try:
        rows = session.query(switch.BRANCH_SQL, (storage_id,))
    except Exception as exc:  # noqa: BLE001 - the whole block's first check reports the state instead
        args = getattr(exc, 'args', ())
        number = args[0] if args and isinstance(args[0], int) else 'unknown'
        if log is not None:
            log('could not read MV_CLOUD_BRANCHES for STORAGE_ID %s as the %s (%s, error %s); the whole block follows and its first '
                'check reports the state' % (storage_id, identity, type(exc).__name__, number))
        return False
    return bool(rows) and str(dict(rows[0]).get('IS_DETACHED')) == '1'


def finish_freeze(ctx):
    """The python postconditions of Path A: the write probe as the runner, freeze_time, and the grants reference for C1."""
    from . import copy as copy_module
    if ctx.config.freeze.write_probe:
        data, plans, _ = copy_module.load_plans(ctx)
        table, column, kind = probe_target(plans, data['tables'])
        runner = ctx.runner_session(probe_guard(ctx.db.source, 'runner O3'))
        try:
            write_probe(runner, ctx.db.source, table, column, ctx.log, ctx.config.copy.lock_probe_timeout_seconds, kind)
        finally:
            runner.close()
    when = iso(utc_now())
    ctx.journal.set_meta('freeze_time', when)
    ctx.journal.event('freeze', 'Path A read-only attachment verified; freeze_time ' + when)
    ctx.log('freeze_time recorded: ' + when)
    if ctx.owner_configured:
        from . import lockout
        owner = ctx.owner_session(read_guard('owner O3 grants reference'))
        try:
            grants.record_reference(ctx.journal, grants.snapshot(owner, ctx.db.source, lockout.protected_users(ctx.config)))
        finally:
            owner.close()
        ctx.log('grants reference recorded for the C1 comparison (nothing revoked)')
    else:
        ctx.log('no owner profile: no grants reference is taken, so C1 cannot compare grants for this run')


def handle_o3(step, ctx):
    """O3 handler: Path A prints and executes the block, or resumes (after READONLY, or between DETACH and ATTACH), then the probe and freeze_time.

    The position comes from the attachment views, read with the owner session when the owner
    profile is configured (as O10 does), else as the runner: the source READONLY on the
    recorded STORAGE_ID resumes at the probe; no database under the source name with the
    recorded storage detached resumes at the ATTACH through the normal admin flow (printed to
    owner-sql/O3-reentry.sql, executed with the owner profile or left for the gate). Anything
    else gets the whole block, whose first checks refuse a source name held by another storage.
    A read error on the branch view, and a dry run (it opens no connection), also get the whole
    block. Path B defers to lockout.
    """
    if ctx.config.freeze.mode == 'lockout':
        from . import lockout
        return lockout.handle_o3_lockout(step, ctx)
    names = ownersql.names_for(ctx)
    if not ctx.dry_run and not ownersql.placeholders(names['source_sid']):
        identity = 'owner' if ctx.owner_configured else 'runner'
        guard = read_guard('%s O3' % identity)
        reader = ctx.owner_session(guard) if ctx.owner_configured else ctx.runner_session(guard)
        results, detached = None, False
        try:
            att = source_attachment(reader, ctx.db.source)
            if att and str(att.get('ATTACH_TYPE')).upper() == 'READONLY' and str(att.get('STORAGE_ID')) == str(names['source_sid']):
                b = resume_block(names)
                ownersql.print_block(b, ctx, ownersql.owner_sql_path(ctx, 'O3'))
                ctx.log('source %s is already READONLY on STORAGE_ID %s; resuming at the probe' % (ctx.db.source, names['source_sid']))
                results = ownersql.execute_block(b, reader, ctx.log, ctx.journal)
            elif att is None:
                detached = storage_detached(reader, names['source_sid'], ctx.log, identity)
        finally:
            reader.close()
        if results is not None:
            finish_freeze(ctx)
            return steps.Outcome(steps.PASSED, 'O3 resumed: READONLY attachment verified, write probe refused, freeze_time %s'
                                 % ctx.journal.get_meta('freeze_time'))
        if detached:
            ctx.log('no database is attached as %s and STORAGE_ID %s is detached: an earlier run stopped between DETACH and ATTACH; '
                    'resuming at the ATTACH' % (ctx.db.source, names['source_sid']))
            out = ownersql.handle_admin(step, ctx, reentry_block(names), after=lambda results: finish_freeze(ctx),
                                        path=ownersql.owner_sql_path(ctx, 'O3', variant='reentry'))
            if out.status == steps.PASSED:
                return steps.Outcome(steps.PASSED, 'O3 resumed at the ATTACH: write probe refused, freeze_time %s; %s'
                                     % (ctx.journal.get_meta('freeze_time'), out.note))
            return out
    if ctx.dry_run:
        ctx.log('dry run: no connection is opened, so the attachment state is not read; after a stop between DETACH and ATTACH, '
                's2rebuild step O3 --redo prints and runs the re-entry block instead (it starts at the ATTACH)')
    b = block(names)
    return ownersql.handle_admin(step, ctx, b, after=lambda results: finish_freeze(ctx))


steps.register('O3', handle_o3)
