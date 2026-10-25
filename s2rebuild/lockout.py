"""s2rebuild lockout: O3 Path B, revoke write privileges and kill writer sessions in a loop until quiet.

Author: John Bergamini

Section 10.8 of the architecture. Loaded only when freeze.mode is lockout or the lockout command
runs. Owner profile required. Each pass re-reads the grantees' privileges at every scope (a
grant restored by another session is logged and revoked again), lists every session from those
accounts touching the database on every held connection, idle ones included (a pooled idle
connection keeps its rights until killed), and kills each from the connection that saw it,
because KILL reaches only the aggregator the killer is connected to. Thread ids are per
aggregator, so the same id on two connections is two sessions, killed from each. Every pass
writes its time and whether it was quiet to journal meta (record_pass), and marks itself not
quiet before its first REVOKE or KILL; S1 of O10 on Path B refuses unless the last pass was
quiet within freeze.quiet_max_age_seconds (last_pass_problem). With s2rebuild lockout --hold
the loop keeps passing after the quiet proof until S1 of O10 has run. A rerun reuses the first
snapshot (journal first, grants-snapshot.json second) and never overwrites it. The loop never
restores grants; O11 does. Statements issued through the lockout owner guard: REVOKE, KILL
CONNECTION, and SELECT on the privilege views and PROCESSLIST.
"""
import time

from . import grants, ownersql, steps, watch
from .common import EXIT_GATE, KitError, iso, parse_utc, utc_now

UNKNOWN_THREAD_ERRNO = 1094
LAST_PASS_UTC, LAST_PASS_QUIET = 'lockout_last_pass_utc', 'lockout_last_pass_quiet'
# Every session of the locked accounts on the database, idle or not (10.8 step 4b).
SESSIONS_SQL = ('SELECT ID, USER, HOST, DB, COMMAND, TIME, LEFT(INFO, 160) AS INFO FROM information_schema.PROCESSLIST '
                'WHERE DB = %s OR INFO LIKE %s')


def protected_users(config):
    """Runner, owner and freeze.lockout.protected_accounts."""
    users = {config.runner.user}
    if config.owner is not None:
        users.add(config.owner.user)
    users.update(config.freeze.lockout.protected_accounts)
    return users


def sessions_of(session, db_name, snap):
    """PROCESSLIST ids on the database from the snapshot's accounts, idle or not, as seen by this connection."""
    users = {g.split('@')[0].strip("'") for g in snap}
    ids = []
    for r in session.query(SESSIONS_SQL, (db_name, '%' + db_name + '%')):
        if str(r.get('USER')) in users:
            ids.append(int(r['ID']))
    return ids


def record_pass(journal, quiet):
    """Write the last lockout pass to journal meta: lockout_last_pass_quiet first, then lockout_last_pass_utc.

    In that order a stop between the two writes leaves a pass that found work as not quiet, or a
    quiet pass under the older time, never a pass that found work as quiet at the newer time.
    """
    journal.set_meta(LAST_PASS_QUIET, 1 if quiet else 0)
    journal.set_meta(LAST_PASS_UTC, iso(utc_now()))


def last_pass_problem(journal, max_age_seconds, now=None):
    """Why the last lockout pass does not allow S1 of O10 on Path B, or None when it was quiet within max_age_seconds.

    Reads the journal meta that record_pass writes. Issues nothing.
    """
    when, quiet = journal.get_meta(LAST_PASS_UTC), journal.get_meta(LAST_PASS_QUIET)
    hold = 'keep s2rebuild lockout --hold running through O10'
    if not when:
        return 'no lockout pass is recorded; %s' % hold
    if str(quiet) != '1':
        return 'the last lockout pass at %s was not quiet (it found a session or a restored grant); %s' % (when, hold)
    age = ((now or utc_now()) - parse_utc(when, LAST_PASS_UTC)).total_seconds()
    if age > max_age_seconds:
        return 'the last lockout pass was at %s, %d s ago, more than freeze.quiet_max_age_seconds (%d s); %s' % (when, age, max_age_seconds, hold)
    return None


def one_pass(connections, db_name, snap, journal, log, skip_grants=False):
    """One lockout pass over the held connections; returns (threads_seen, killed, gone, restored_grantees).

    Each connection kills every id it lists, also an id another connection saw: thread ids are
    per aggregator. Before the first REVOKE or KILL of the pass, record_pass marks it not quiet.
    """
    admin = connections[0]
    restored = {}
    marked = []

    def _not_quiet():
        if not marked:
            record_pass(journal, False)
            marked.append(True)
    if not skip_grants:
        restored = grants.restored_by_another(admin, db_name, snap)
        for grantee, scopes in restored.items():
            text = '; '.join('%s on %s' % (', '.join(p), s) for s, p in sorted(scopes.items()))
            log('GRANT RESTORED BY ANOTHER SESSION: %s has %s again; revoking' % (grantee, text))
            journal.event('grant_restored', '%s %s' % (grantee, text))
        if restored:
            _not_quiet()
        for sql in grants.revoke_statements(restored):
            admin.execute(sql)
    seen, killed, gone = 0, 0, 0
    for conn in connections:
        mine = set()
        for tid in sessions_of(conn, db_name, snap):
            if tid in mine:
                continue
            mine.add(tid)
            seen += 1
            _not_quiet()
            try:
                conn.execute('KILL CONNECTION %d' % tid)
                killed += 1
            except Exception as exc:  # noqa: BLE001 - a thread that ended on its own is not an error
                if getattr(exc, 'args', (None,))[0] == UNKNOWN_THREAD_ERRNO or 'Unknown thread' in str(exc):
                    gone += 1
                else:
                    raise
    return seen, killed, gone, restored


def _stop_requested(until, stop_file):
    if until is not None and utc_now() >= until:
        return 'the stop time was reached'
    if stop_file is not None and stop_file.exists():
        return 'the stop file is present'
    return None


def run_lockout(connections, db_name, snap, journal, log, config, until=None, stop_file=None, sleep=time.sleep, max_passes=None,
                quiet_hook=None, skip_grants=False, hold_hook=None):
    """The loop of section 10.8: passes until until, the stop file, Ctrl-C or max_passes; the quiet proof after enough quiet passes.

    A failed quiet proof runs again after the next quiet_passes_required quiet passes. Without
    hold_hook the loop ends when the proof passes. With hold_hook (s2rebuild lockout --hold) it
    keeps passing: hold_hook is called before every pass and returns why S1 of O10 has run,
    which ends the hold. Every pass is written to journal meta with record_pass; a kill-only
    pass is never recorded as quiet, since it leaves the grants in place. Waits between passes
    are slept in steps of at most one second so the stop file is seen at once. An unexpected
    error still journals the summary before it is raised.
    Returns {'passes': n, 'killed': n, 'quiet_passes': n, 'quiet_proof': True, False or None, 'stopped': reason,
    's1_ran': the hold_hook reason or None}.
    """
    interval = config.freeze.lockout.interval_seconds
    needed = config.freeze.lockout.quiet_passes_required
    passes = killed_total = quiet = 0
    proof, stopped, s1_ran = None, None, None
    log('lockout started on %s: %d connection(s), %d account(s), interval %d s, quiet passes required %d%s%s' % (
        db_name, len(connections), len(snap), interval, needed, (', until %s' % iso(until)) if until else '',
        ', kill only (grants untouched)' if skip_grants else ''))
    try:
        while True:
            stopped = _stop_requested(until, stop_file)
            if stopped:
                log('lockout: %s' % stopped)
                break
            if max_passes is not None and passes >= max_passes:
                stopped = 'max passes reached'
                break
            s1_ran = hold_hook() if hold_hook is not None else None
            if s1_ran:
                stopped = 'S1 of O10 has run: %s' % s1_ran
                log('lockout --hold: %s; the hold ends, grants stay revoked until O11 restores them' % stopped)
                break
            passes += 1
            started = time.monotonic()
            seen, killed, gone, restored = one_pass(connections, db_name, snap, journal, log, skip_grants=skip_grants)
            killed_total += killed
            record_pass(journal, seen == 0 and not restored and not skip_grants)
            quiet = quiet + 1 if seen == 0 and not restored else 0
            log('pass %d threads=%d killed=%d gone=%d restored_grants=%d quiet_passes=%d took=%.1fs' % (
                passes, seen, killed, gone, len(restored), quiet, time.monotonic() - started))
            if quiet >= needed and proof is not True and quiet_hook is not None:
                proof = bool(quiet_hook())
                if proof and hold_hook is None:
                    log('lockout: quiet proof passed after %d quiet passes; the O3 gate can pass' % quiet)
                    stopped = 'quiet proof passed'
                    break
                if proof:
                    log('lockout: quiet proof passed after %d quiet passes; the O3 gate can pass; --hold keeps the loop running '
                        'until S1 of O10 has run' % quiet)
                else:
                    log('lockout: quiet proof failed; it runs again after %d more quiet passes' % needed)
                    quiet = 0
            remaining = interval
            while remaining > 0 and not _stop_requested(until, stop_file):
                step = min(1, remaining)
                sleep(step)
                remaining -= step
            if interval <= 0:
                sleep(0)
    except KeyboardInterrupt:
        stopped = 'Ctrl-C'
        log('lockout stopped by Ctrl-C; grants stay revoked until O11 restores them')
    except Exception as exc:
        journal.event('lockout', 'passes=%d killed=%d quiet_proof=%s stopped by error %s' % (passes, killed_total, proof, type(exc).__name__))
        log('lockout stopped by an error after %d passes and %d kills: %s; grants stay revoked until O11 restores them' % (
            passes, killed_total, type(exc).__name__))
        raise
    journal.event('lockout', 'passes=%d killed=%d quiet_proof=%s stopped=%s' % (passes, killed_total, proof, stopped))
    return {'passes': passes, 'killed': killed_total, 'quiet_passes': quiet, 'quiet_proof': proof, 'stopped': stopped, 's1_ran': s1_ran}


def prepare(sessions, db, config, journal, log, snapshot_path=None, kill_only=False):
    """Hold the connections, take or reuse the snapshot, revoke once; returns (admin_session, held_connections, snapshot).

    A snapshot already in the journal (or, failing that, in grants-snapshot.json) is reused and
    never overwritten, so a rerun cannot replace the original privileges with the revoked state;
    the stored grantees are revoked again for whatever they hold now. kill_only leaves every
    grant untouched and records no snapshot (nothing to restore). Before a REVOKE, record_pass
    marks the last pass not quiet, so an earlier quiet pass never stands for a state with grants back.
    """
    guard = steps.guard_for('lockout', db, steps.OWNER)
    held = [sessions.owner(guard) for _ in range(max(1, int(config.freeze.lockout.connections)))]
    admin = held[0]
    stored = grants.snapshot_from_journal(journal) or (grants.read_snapshot_file(snapshot_path) if snapshot_path is not None else {})
    if stored:
        snap = stored
        grants.record_snapshot(journal, snap)
        log('lockout: reusing the first grants snapshot (%d account(s)); it is never overwritten' % len(snap))
        if kill_only:
            return admin, held, snap
        again = grants.restored_by_another(admin, db.source, snap)
        if again:
            record_pass(journal, False)
        for sql in grants.revoke_statements(again):
            admin.execute(sql)
        journal.event('revoke', 'rerun: write privileges revoked again from %d account(s): %s' % (len(again), ', '.join(sorted(again)) or 'none'))
        log('revoked again from %d account(s) that held write privileges' % len(again))
        return admin, held, snap
    snap = grants.snapshot(admin, db.source, protected_users(config))
    if not snap:
        log('lockout: no write-capable account outside the protected set; nothing to revoke, check freeze.lockout.protected_accounts')
    if kill_only:
        log('lockout: kill only; %d account(s) found, no grant is revoked and no snapshot is recorded' % len(snap))
        return admin, held, snap
    grants.record_snapshot(journal, snap)
    if snapshot_path is not None:
        grants.write_snapshot(snapshot_path, snap)
        log('grants snapshot written to %s (mode 0600)' % snapshot_path)
    if snap:
        record_pass(journal, False)
    for sql in grants.revoke_statements(snap):
        admin.execute(sql)
    journal.event('revoke', 'write privileges revoked from %d account(s): %s' % (len(snap), ', '.join(sorted(snap)) or 'none'))
    log('revoked write privileges from %d account(s)' % len(snap))
    return admin, held, snap


def default_until(journal, config):
    """The abort deadline (journal record, else the window) as the lockout's default stop time; None without either."""
    from . import copy as copy_module
    return copy_module.resolve_abort_at(journal, config)


def plan_block(names, snap):
    """The printed Path B plan for owner-sql O3: per grantee and scope, a check, the REVOKE and the check after."""
    b = ownersql.Block('O3', 'Freeze: lockout (Path B)', guard_step='lockout')
    b.note('Path B runs as a loop, not a one-time block: s2rebuild lockout (owner profile) or the notebook lockout cell. '
           'The loop re-revokes grants restored by another session and kills every session of these accounts until quiet.')
    if not snap:
        b.note('No grants snapshot yet: the loop takes it at run time from USER_PRIVILEGES, SCHEMA_PRIVILEGES and TABLE_PRIVILEGES.')
    for grantee, scopes in sorted(grants.normalize(snap).items()):
        for scope, privs in sorted(scopes.items()):
            sql, params = grants.privilege_count_check(grantee, scope, privs, names['source'])
            b.check('%s holds write privileges on %s' % (grantee, scope), sql, params, ownersql.Expect('column_at_least', 'n', 1))
            b.statement('REVOKE %s ON %s FROM %s' % (', '.join(privs), scope, grantee), 'revoke the write privileges of %s on %s' % (grantee, scope))
            b.check('%s holds none of them on %s' % (grantee, scope), sql, params, ownersql.Expect('column_equals', 'n', 0))
    b.note('Then kill every session of these accounts on %s from each connection that sees it, repeat until %s quiet passes, '
           'and run s2rebuild quiet-proof, which records freeze_time.' % (names['source'], names.get('quiet_passes_required', 'the configured')))
    return b


def handle_o3_lockout(step, ctx):
    """O3 Path B handler: snapshot and revoke, loop with held connections until the abort deadline, quiet proof, freeze_time."""
    from . import copy as copy_module
    if ctx.dry_run or not ctx.owner_configured:
        lines = ['Path B lockout needs the owner profile: s2rebuild lockout runs the revoke and kill loop and the quiet proof',
                 'statements per pass: REVOKE <write privileges> ON <scope> FROM <grantee>; SELECT ... PROCESSLIST on %d connections; '
                 'KILL CONNECTION <id>' % ctx.config.freeze.lockout.connections,
                 'the gate passes when the quiet proof passed after %d quiet passes' % ctx.config.freeze.lockout.quiet_passes_required]
        return steps.gate_pending(step, ctx, lines)
    data, plans, _ = copy_module.load_plans(ctx)
    run_dir = ctx.config.runs_dir() / ctx.run_id / ctx.db.source
    run_dir.mkdir(parents=True, exist_ok=True)
    until = default_until(ctx.journal, ctx.config)
    if until is not None and utc_now() >= until:
        return steps.Outcome(steps.FAILED, 'the abort deadline %s is already past; the lockout does not start' % iso(until), EXIT_GATE)
    admin, held, snap = prepare(ctx.sessions, ctx.db, ctx.config, ctx.journal, ctx.log, run_dir / 'grants-snapshot.json')
    runner = ctx.runner_session(steps.guard_for('O4', ctx.db))

    def _quiet():
        ok, _ = watch.quiet_proof(runner, admin, ctx.db.source, plans, protected_users(ctx.config), ctx.journal, ctx.log)
        return ok
    try:
        summary = run_lockout(held, ctx.db.source, snap, ctx.journal, ctx.log, ctx.config, until=until,
                              stop_file=copy_module.stop_file_for(ctx.journal_path), quiet_hook=_quiet)
    finally:
        runner.close()
        for s in held:
            s.close()
    if summary['quiet_proof']:
        return steps.Outcome(steps.PASSED, 'Path B lockout: %d passes, %d sessions killed, quiet proof passed, freeze_time %s' % (
            summary['passes'], summary['killed'], ctx.journal.get_meta('freeze_time')))
    return steps.Outcome(steps.PENDING, 'lockout ended (%s) without a passed quiet proof (%d passes, %d killed); run s2rebuild lockout again or '
                         'record the gate after a manual proof' % (summary['stopped'], summary['passes'], summary['killed']), EXIT_GATE)
