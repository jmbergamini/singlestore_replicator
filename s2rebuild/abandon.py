"""s2rebuild abandon: back out a run stopped before the switch; the source READWRITE again, the replacement dropped, the run closed.

Author: John Bergamini

A run that stops before O10 leaves the source READONLY (Path A) and a partly copied
replacement. abandon puts the workspace back the way P2 found it, through one owner block with
a check before and after every statement: the source is attached READWRITE again on the
branch record the run recorded (name, STORAGE_ID, ATTACH_TYPE, COMPUTE_ID), and the replacement
is dropped only while its name holds the record O5 recorded and no branch was made from it (the
identity checks of C3). Then the run is marked abandoned, so run,
step and gate refuse it; a new run starts with P1. The runner account goes afterwards with
account-sql --drop --execute, which a later run would otherwise refuse at P1.

abandon refuses once the switch may have begun (Journal.switch_began): from then on the way back
is owner-sql O10 --rollback. It takes the copy lock, so it refuses while a copy runs; pause the
copy first and let the running chunks finish. Path B gives the revoked privileges back at O11
only, so abandon refuses on Path B. The position is read first, as O3 does: the source READONLY
on the recorded storage gets the DETACH and the ATTACH; a stop between them resumes at the
ATTACH; a source already READWRITE on the recorded storage gets checks only. A rerun after a
partial abandon finishes it. Statements issued: DETACH DATABASE and ATTACH DATABASE on the
source, DROP DATABASE on the replacement, all through the abandon owner guard; SELECT on the
attachment and branch views.
"""
from . import copy as copy_module
from . import freeze, ownersql, steps, switch
from .common import KitError, iso, utc_now
from .sqlutil import ident, read_guard

STEP_ID = 'abandon'
RUNNER_NOTE = 'Then remove the runner account: s2rebuild account-sql --drop --execute (the C4 block). A new run starts with P1.'


def other_storage_sql():
    """COUNT of attachments under a name with another STORAGE_ID (parameters: name, STORAGE_ID). Issues nothing (reads only)."""
    return 'SELECT COUNT(*) AS n FROM information_schema.MV_CLOUD_ATTACHED_DATABASES WHERE DATABASE_NAME = %s AND STORAGE_ID <> %s'


def branches_of_sql():
    """COUNT of not dropped branch records made from a database (parameters: the parent name, its STORAGE_ID). Issues nothing (reads only)."""
    return 'SELECT COUNT(*) AS n FROM information_schema.MV_CLOUD_BRANCHES WHERE PREV_DATABASE_NAME = %s AND STORAGE_ID = %s AND IS_DROPPED = 0'


def _source_back(b, names, position):
    """The source part: DETACH and ATTACH READWRITE (frozen), the ATTACH only (detached), or checks only (already READWRITE).

    Every statement runs behind a record check: the source name holds the recorded record
    (name, STORAGE_ID, ATTACH_TYPE, COMPUTE_ID), attached before the DETACH and detached before
    the ATTACH.
    """
    source, sid = names['source'], names['source_sid']
    if position == 'frozen':
        b.check('source attached READONLY', switch.ATTACHED_SQL, (source,), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READONLY'))
        b.check('source STORAGE_ID equals the recorded one', switch.ATTACHED_SQL, (source,), ownersql.Expect('column_equals', 'STORAGE_ID', sid))
        b.check('source is the recorded record', switch.ATTACHED_RECORD_SQL, (source, sid), ownersql.Expect('columns_equal', value=ownersql.record_expect(names, 'source')))
        b.statement('DETACH DATABASE %s' % ident(source), 'detach the read-only source',
                    detach={'wait': names['detach_wait_seconds'], 'kill': bool(names['detach_kill'])})
    if position in ('frozen', 'detached'):
        b.check('source storage detached', switch.RECORD_STATE_SQL, (source, sid), ownersql.Expect('column_equals', 'IS_DETACHED', 1))
        b.check('source record detached', switch.DETACHED_RECORD_SQL, (source, sid), ownersql.Expect('columns_equal', value=ownersql.record_expect(names, 'source')))
        b.check('no attached database named source', switch.ATTACHED_SQL, (source,), ownersql.Expect('no_rows'))
        b.statement('ATTACH DATABASE %s STORAGEID %s' % (ident(source), sid), 'attach the same storage read write')
    b.check('source attached READWRITE', switch.ATTACHED_SQL, (source,), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READWRITE'), record='source')
    b.check('source STORAGE_ID unchanged', switch.ATTACHED_SQL, (source,), ownersql.Expect('column_equals', 'STORAGE_ID', sid))
    b.check('source record attached', switch.ATTACHED_RECORD_SQL, (source, sid), ownersql.Expect('columns_equal', value=ownersql.record_expect(names, 'source')),
            record='source')


def _replacement_identity(b, names):
    """The C3 identity checks of the replacement: its name holds the recorded storage or nothing, and that storage no other name."""
    replacement, rsid = names['replacement'], names['replacement_sid']
    if ownersql.placeholders(rsid):
        b.note('O5 recorded no replacement, so there is nothing to drop; the name must be free.')
        b.check('no attached database named replacement', switch.ATTACHED_SQL, (replacement,), ownersql.Expect('no_rows'))
        return
    recorded = (replacement, rsid, ownersql.CLEANUP_ATTACH_TYPES['replacement'])
    b.check('replacement name holds the recorded replacement %s, or nothing' % recorded[2], ownersql.attached_otherwise_sql(), recorded,
            ownersql.Expect('column_equals', 'n', 0))
    b.check('replacement name holds no other record', ownersql.other_record_sql(), (replacement, rsid), ownersql.Expect('column_equals', 'n', 0))
    # a branch of the replacement means the switch attached the live name from it (fact 27): abandon never drops its parent
    b.check('the replacement has no branch', branches_of_sql(), (replacement, rsid), ownersql.Expect('column_equals', 'n', 0))
    b.check('recorded replacement attached under no other name', ownersql.attached_elsewhere_sql(), (rsid, replacement),
            ownersql.Expect('column_equals', 'n', 0))


def _replacement_dropped(b, names):
    """The replacement part: the identity checks again, DROP DATABASE while the name holds the recorded storage, and the checks after it."""
    replacement, rsid = names['replacement'], names['replacement_sid']
    if ownersql.placeholders(rsid):
        return
    recorded = (replacement, rsid, ownersql.CLEANUP_ATTACH_TYPES['replacement'])
    _replacement_identity(b, names)
    b.statement('DROP DATABASE %s' % ident(replacement), 'drop the partly copied replacement', skip_if=(ownersql.recorded_absent_sql(), recorded))
    b.check('replacement is gone', switch.ATTACHED_SQL, (replacement,), ownersql.Expect('no_rows'))
    b.check('recorded replacement attached nowhere', ownersql.storage_attached_sql(), (rsid,), ownersql.Expect('column_equals', 'n', 0))
    b.check('recorded replacement record dropped', ownersql.record_state_sql(), (replacement, rsid),
            ownersql.Expect('column_equals', 'IS_DROPPED', 1))


def block(names, position='frozen'):
    """The abandon block for a source position: 'frozen' (READONLY), 'detached' (between DETACH and ATTACH) or 'writable' (READWRITE)."""
    b = ownersql.Block(STEP_ID, 'Abandon the run before the switch')
    b.note('Run %s stops here for good: %s goes back to READWRITE on STORAGE_ID %s and %s is dropped.' % (
        names['run_id'], names['source'], names['source_sid'], names['replacement']))
    b.check('source name held by no other storage', other_storage_sql(), (names['source'], names['source_sid']),
            ownersql.Expect('column_equals', 'n', 0))
    _replacement_identity(b, names)
    _source_back(b, names, position)
    _replacement_dropped(b, names)
    b.check('source still READWRITE', switch.ATTACHED_SQL, (names['source'],), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READWRITE'))
    b.check('source still on the recorded STORAGE_ID', switch.ATTACHED_SQL, (names['source'],),
            ownersql.Expect('column_equals', 'STORAGE_ID', names['source_sid']))
    b.check('source still the recorded record', switch.ATTACHED_RECORD_SQL, (names['source'], names['source_sid']),
            ownersql.Expect('columns_equal', value=ownersql.record_expect(names, 'source')))
    b.note(RUNNER_NOTE)
    return b


def refusal(ctx):
    """Why abandon cannot run for this database, or None: the switch may have begun, Path B, or no source STORAGE_ID recorded."""
    began = ctx.journal.switch_began()
    if began:
        return ('abandon refused for %s: the switch may have begun (%s); the way back is s2rebuild owner-sql O10 --rollback'
                % (ctx.db.source, began))
    if ctx.config.freeze.mode == 'lockout':
        return 'abandon refused for %s: Path B gives the revoked privileges back at O11 only; abandon covers Path A' % ctx.db.source
    if ownersql.placeholders(ownersql.names_for(ctx)['source_sid']):
        return 'abandon refused for %s: no source STORAGE_ID is recorded (P2 records it); nothing of this run has changed the workspace' % ctx.db.source
    return None


def position(ctx, names):
    """'frozen', 'detached' or 'writable' from the attachment views read with the owner session; 'frozen' in a dry run.

    Any other state (another storage under the source name, a READONLY attachment of another
    storage) gets the frozen block, whose first checks stop with the observed state.
    """
    if ctx.dry_run:
        return 'frozen'
    reader = ctx.owner_session(read_guard('owner abandon'))
    try:
        att = freeze.source_attachment(reader, ctx.db.source)
        if att is None:
            return 'detached' if freeze.storage_detached(reader, names, ctx.log, 'owner') else 'frozen'
        if str(att.get('STORAGE_ID')) == str(names['source_sid']) and str(att.get('ATTACH_TYPE')).upper() == 'READWRITE':
            return 'writable'
        return 'frozen'
    finally:
        reader.close()


def abandoned(journal):
    """The refusal text for a command on an abandoned run, or None."""
    when = journal.get_meta('abandoned_utc') if journal is not None else None
    if not when:
        return None
    return 'This run was abandoned at %s; start a new run with P1 (s2rebuild account-sql, then s2rebuild gate P1 --pass --note TEXT --run NEW_ID)' % when


def run(ctx, note, execute):
    """Print the block for the position; with execute run it with the owner profile under the copy lock, then mark the run abandoned.

    Returns the note line for the command. Raises KitError on a refusal or at the first failed check.
    """
    why = refusal(ctx)
    if why:
        raise KitError(why)
    if execute and not ctx.dry_run and not ctx.owner_configured:
        raise KitError('abandon --execute needs connections.owner in the configuration; without it, run the printed block as the admin')
    names = ownersql.names_for(ctx)
    where = position(ctx, names) if execute and not ctx.dry_run else 'frozen'
    b = block(names, where)
    ownersql.guard_check_block(b, ctx)
    path = ownersql.owner_sql_path(ctx, STEP_ID)
    ownersql.print_block(b, ctx, path if execute else None)
    if not execute:
        return 'print only; add --execute --note TEXT to run it with the owner profile'
    if ctx.dry_run:
        return 'DRY RUN: --execute would run the block above with the owner profile; no connection was opened'
    ownersql.refuse_placeholders(b)
    with copy_module.copy_lock(ctx.journal_path, STEP_ID):
        if refusal(ctx):
            raise KitError(refusal(ctx))
        ctx.journal.event('abandon_started', note)
        guard = steps.guard_for(STEP_ID, ctx.db, steps.OWNER)
        session = ctx.owner_session(guard)
        try:
            results = ownersql.execute_block(b, session, ctx.log, ctx.journal, open_session=ctx.owner_session, guard=guard)
        finally:
            session.close()
        when = iso(utc_now())
        ctx.journal.set_meta('abandoned_utc', when)
        ctx.journal.event('abandoned', note)
    ctx.log('run %s abandoned at %s: %s READWRITE on STORAGE_ID %s, %s dropped; %d checks passed' % (
        ctx.run_id, when, ctx.db.source, names['source_sid'], ctx.db.replacement, len(results)))
    return 'abandon executed with the owner profile; block at %s. %s' % (path, RUNNER_NOTE)
