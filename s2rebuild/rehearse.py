"""s2rebuild rehearse: P5, a throwaway copy and switch on synthetic databases with a kill test.

Author: John Bergamini

Section 10.18 of the architecture. Creates <prefix>_src with tables that exercise a single-column
key, a composite key, a bigint AUTO_INCREMENT key, a varchar unique key (lock risk), duplicate
rows without a key, an empty table, a table with a time column, and one view; fills them with
exactly the requested row count; then runs O4 to O10 against <prefix>_src and <prefix>_rebuild
with throwaway retained and live names. The chunking thresholds are scaled to the row count so
the hash-bucket predicates of large tables are rehearsed. The kill test sends SIGKILL to a copy
process while a chunk is started (the leaf lock it leaves is waited out, as on the night), then
the resume must reconcile exactly the interrupted chunk. Every rehearsal name starts with
s2rebuild_rehearsal_ and none may equal a configured database name; cleanup drops only what this
invocation created (rehearse --cleanup drops the prefixed names after a confirmation). P5 passes
only when the switch was rehearsed too (owner profile). Statements issued through a guard that
permits the runner write kinds plus CREATE DATABASE and DROP DATABASE on the prefixed names only;
the rehearsal steps themselves run through their own step guards.
"""
import dataclasses
import json
import os
import signal
import time

from . import copy as copy_module
from . import inventory, ownersql, steps, switch
from .common import EXIT_GATE, KitError, iso, utc_now
from .config import Section
from .journal import Journal
from .sqlutil import Guard, RUNNER_KINDS, ident, probe_guard, read_guard

MARKER = 's2rebuild_rehearsal_'
SEED_ROWS = 1000000
KILL_POLL_SECONDS = 0.05
KILL_WAIT_SECONDS = 600
RESUME_LIMIT_SECONDS = 30 * 60

TABLES = {
    'keyed_single': ('CREATE TABLE {db}.`keyed_single` (`id` varchar(100) NOT NULL, `payload` varchar(200) DEFAULT NULL, `n` int(11) DEFAULT NULL, '
                     '`updated_at` datetime(6) DEFAULT NULL, PRIMARY KEY (`id`), SHARD KEY (`id`), SORT KEY (`id`))'),
    'keyed_composite': ('CREATE TABLE {db}.`keyed_composite` (`a` varchar(64) NOT NULL, `b` datetime(6) NOT NULL, `c` int(11) NOT NULL, '
                        '`note` varchar(100) DEFAULT NULL, PRIMARY KEY (`a`, `b`, `c`), SHARD KEY (`a`), SORT KEY (`a`, `b`))'),
    'keyed_bigint': ('CREATE TABLE {db}.`keyed_bigint` (`id` bigint(20) NOT NULL AUTO_INCREMENT, `v` varchar(20) DEFAULT NULL, '
                     'PRIMARY KEY (`id`), SHARD KEY (`id`), SORT KEY (`id`))'),
    'unique_varchar': ('CREATE TABLE {db}.`unique_varchar` (`email` varchar(128) NOT NULL, `n` int(11) DEFAULT NULL, '
                       'PRIMARY KEY (`email`), SHARD KEY (`email`), SORT KEY (`email`))'),
    'nokey_dupes': ('CREATE TABLE {db}.`nokey_dupes` (`x` int(11) DEFAULT NULL, `y` varchar(50) DEFAULT NULL, `z` double DEFAULT NULL, '
                    'SHARD KEY (`x`), SORT KEY (`x`))'),
    'nokey_empty': 'CREATE TABLE {db}.`nokey_empty` (`p` varchar(10) DEFAULT NULL, `q` int(11) DEFAULT NULL)',
}
VIEW = 'CREATE VIEW {db}.`keyed_counts` AS SELECT `a`, COUNT(*) AS `n` FROM {db}.`keyed_composite` GROUP BY `a`'
SEED = 'CREATE TABLE {db}.`seed` (`k` int(11) NOT NULL, PRIMARY KEY (`k`))'


class _RehearsalKill(BaseException):
    """Raised inside a chunk by the in-process kill strategy so the chunk stays 'started' (no handler catches it)."""


def rehearsal_names(prefix):
    """The three throwaway names: source, replacement and retained (the live name is the source name after the switch)."""
    return {'source': prefix + '_src', 'replacement': prefix + '_rebuild', 'retained': prefix + '_retained'}


def rehearsal_guard(prefix):
    """Runner write kinds plus CREATE and DROP DATABASE, on the prefixed names only."""
    names = set(rehearsal_names(prefix).values())
    return Guard('rehearsal', RUNNER_KINDS | {'CREATE DATABASE', 'DROP DATABASE', 'USE'}, names)


def default_prefix(run_id):
    """s2rebuild_rehearsal_<run id with underscores>."""
    return MARKER + str(run_id or 'run').replace('-', '_')


def check_prefix(prefix, config=None):
    """Refuse a prefix that is not an identifier, lacks the s2rebuild_rehearsal_ marker, or names a configured database."""
    ident(prefix + '_src')
    if not prefix.startswith(MARKER):
        raise KitError('Rehearsal prefixes must start with %s so cleanup can never reach a real database; got %s' % (MARKER, prefix))
    if config is not None:
        configured = {n for db in config.databases for n in (db.source, db.replacement, db.retained)}
        clash = sorted(set(rehearsal_names(prefix).values()) & configured)
        if clash:
            raise KitError('Rehearsal prefix %s collides with configured database name(s): %s' % (prefix, ', '.join(clash)))
    return prefix


def rehearsal_config(config, rows):
    """The configuration with chunking scaled to the row count, so tables of rows rows split into four buckets."""
    copy = dict(config.copy.as_dict(), min_rows_for_chunking=max(1, rows // 10), chunk_rows=max(1, rows // 4))
    return dataclasses.replace(config, copy=Section(copy))


def _blocks(rows):
    """(block index, rows in the block) covering exactly rows rows in blocks of SEED_ROWS."""
    out, left, b = [], max(0, int(rows)), 0
    while left > 0:
        out.append((b, min(SEED_ROWS, left)))
        left -= SEED_ROWS
        b += 1
    return out


def fill_statements(db, rows):
    """The statements that create and fill the rehearsal source with exactly rows rows in keyed_single and nokey_dupes."""
    d = ident(db)
    out = [SEED.format(db=d), 'INSERT INTO %s.`seed` (`k`) VALUES %s' % (d, ', '.join('(%d)' % i for i in range(1000)))]
    for ddl in TABLES.values():
        out.append(ddl.format(db=d))
    for block, n in _blocks(rows):
        out.append("INSERT INTO %s.`keyed_single` (`id`, `payload`, `n`, `updated_at`) SELECT CONCAT('id-', %d, '-', a.k, '-', b.k), CONCAT('payload-', a.k), "
                   "a.k * 1000 + b.k, DATE_ADD('2026-01-01 00:00:00.000000', INTERVAL a.k * 1000 + b.k SECOND) FROM %s.`seed` a JOIN %s.`seed` b "
                   "WHERE a.k * 1000 + b.k < %d" % (d, block, d, d, n))
    out.append("INSERT INTO %s.`keyed_composite` (`a`, `b`, `c`, `note`) SELECT CONCAT('a', a.k %% 50), DATE_ADD('2026-01-01 00:00:00.000000', "
               "INTERVAL a.k * 1000 + b.k SECOND), b.k, CONCAT('n', b.k) FROM %s.`seed` a JOIN %s.`seed` b WHERE a.k * 1000 + b.k < %d"
               % (d, d, d, min(200000, max(1, rows))))
    out.append("INSERT INTO %s.`keyed_bigint` (`v`) SELECT CONCAT('v', a.k) FROM %s.`seed` a" % (d, d))
    out.append("INSERT INTO %s.`unique_varchar` (`email`, `n`) SELECT CONCAT('user', a.k, '@example.test'), a.k FROM %s.`seed` a" % (d, d))
    for block, n in _blocks(rows // 2):
        for _ in range(2):
            out.append("INSERT INTO %s.`nokey_dupes` (`x`, `y`, `z`) SELECT a.k * 1000 + b.k + %d, IF(b.k %% 7 = 0, NULL, CONCAT('y', b.k)), "
                       "IF(a.k %% 5 = 0, NULL, a.k / 3.0) FROM %s.`seed` a JOIN %s.`seed` b WHERE a.k * 1000 + b.k < %d" % (d, block * SEED_ROWS, d, d, n))
    out.append(VIEW.format(db=d))
    out.append('DROP TABLE %s.`seed`' % d)
    return out


def existing_names(session, prefix):
    """The rehearsal names that exist in the workspace now."""
    return [n for n in rehearsal_names(prefix).values() if session.query(inventory.LAYOUT_SQL, (n,))]


def create_source(session, names, layout, rows, log):
    """Create and fill the rehearsal source; refuses when any rehearsal name already exists."""
    taken = [n for n in names.values() if session.query(inventory.LAYOUT_SQL, (n,))]
    if taken:
        raise KitError('Rehearsal name(s) %s already exist; clean up first (s2rebuild rehearse --cleanup --prefix PREFIX --yes) or choose another prefix'
                       % ', '.join(taken))
    session.execute('CREATE DATABASE %s PARTITIONS %d SUB_PARTITIONS %d' % (ident(names['source']), layout.partitions, layout.sub_partitions))
    for sql in fill_statements(names['source'], rows):
        session.execute(sql)
    log('rehearsal source %s created and filled (%d rows in keyed_single and in nokey_dupes)' % (names['source'], rows))


def cleanup(session, prefix, log, only=None):
    """DROP DATABASE for the prefixed names that exist, limited to only when given (the names this run created)."""
    dropped = []
    for name in rehearsal_names(prefix).values():
        if only is not None and name not in only:
            continue
        if session.query(inventory.LAYOUT_SQL, (name,)):
            session.execute('DROP DATABASE ' + ident(name))
            dropped.append(name)
    log('rehearsal cleanup dropped: %s' % (', '.join(dropped) or 'nothing'))
    return dropped


def rehearsal_spec(db, prefix):
    """A DatabaseSpec for the throwaway names with the same layout and settings as the configured database."""
    n = rehearsal_names(prefix)
    return dataclasses.replace(db, source=n['source'], replacement=n['replacement'], retained=n['retained'],
                               time_column_candidates=tuple(db.time_column_candidates) + ('updated_at', 'b'))


def _started_chunks(journal):
    return [(t, b) for t, b, s, _, _ in journal.chunk_states() if s == 'started']


def kill_inprocess(spec, plans, journal, connector, log, copy_config):
    """Interrupt one chunk inside its INSERT window (after the rows are committed, before it is marked done); returns the started chunks."""
    def _kill(table, bucket):
        raise _RehearsalKill()
    runner = copy_module.Runner(plans, copy_module.statements_for(spec), journal, connector, log, copy_config, workers=1, after_insert_hook=_kill)
    runner.load_pending()
    item = runner.take()
    if item is None:
        raise KitError('Rehearsal kill test found no chunk to interrupt')
    session = runner.open_session()
    try:
        runner.run_chunk(session, item[0], item[1], item[2])
    except _RehearsalKill:
        log('kill test: %s bucket %d interrupted after its insert' % (item[0], item[1]))
    finally:
        session.close()
    return _started_chunks(journal)


def kill_sigkill(spec, plans, journal, connector, log, copy_config, wait_seconds=KILL_WAIT_SECONDS):
    """Fork a copy process, SIGKILL it while a chunk is started, and return the started chunks (section 10.18)."""
    path = journal.path
    pid = os.fork()
    if pid == 0:  # the child copies until it is killed
        try:
            child_journal = Journal(path)
            copy_module.Runner(plans, copy_module.statements_for(spec), child_journal, connector, log, copy_config, workers=1).copy()
        finally:
            os._exit(0)
    deadline = time.monotonic() + wait_seconds
    killed = False
    try:
        while time.monotonic() < deadline:
            if _started_chunks(journal):
                os.kill(pid, signal.SIGKILL)
                killed = True
                break
            done, _ = os.waitpid(pid, os.WNOHANG)
            if done:
                break
            time.sleep(KILL_POLL_SECONDS)
    finally:
        if not killed:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            os.waitpid(pid, 0)
        except ChildProcessError:
            pass
    if not killed:
        raise KitError('Rehearsal kill test inconclusive: the copy ended before a chunk could be interrupted; raise --rows')
    started = _started_chunks(journal)
    log('kill test: copy process %d killed with SIGKILL while %s was started' % (pid, ', '.join('%s bucket %d' % c for c in started)))
    return started


KILL_STRATEGIES = {'sigkill': kill_sigkill, 'inprocess': kill_inprocess}


def _dry_run(ctx, prefix, names, spec, rows):
    session = ctx.runner_session(rehearsal_guard(prefix))
    try:
        session.execute('CREATE DATABASE %s PARTITIONS %d SUB_PARTITIONS %d' % (ident(names['source']), spec.layout.partitions, spec.layout.sub_partitions))
        for sql in fill_statements(names['source'], rows):
            session.execute(sql)
    finally:
        session.close()
    ctx.log('DRY RUN: the rehearsal would fill %s, run O4 to O8 with a SIGKILL test on %s, freeze and switch with the owner profile, '
            'and drop the names it created; nothing was written' % (names['source'], names['replacement']))
    return steps.Outcome(steps.PASSED, 'dry run: rehearsal statements printed')


def run(ctx, prefix, rows=SEED_ROWS, keep=False, kill_test='sigkill'):
    """P5: build the source, capture, O5 to O8 with a SIGKILL and a resume, the freeze and switch with the owner profile, timings, cleanup."""
    check_prefix(prefix, ctx.config)
    names = rehearsal_names(prefix)
    spec = rehearsal_spec(ctx.db, prefix)
    if ctx.dry_run:
        return _dry_run(ctx, prefix, names, spec, rows)
    config = rehearsal_config(ctx.config, rows)
    guard = rehearsal_guard(prefix)
    timings = {'prefix': prefix, 'rows': rows, 'steps': {}, 'p5_status': 'running'}
    run_dir = ctx.config.runs_dir() / ctx.run_id / 'rehearsal'
    run_dir.mkdir(parents=True, exist_ok=True)
    journal = Journal(run_dir / 'journal.sqlite')
    sub = steps.StepContext(config, spec, journal, ctx.run_id, ctx.log, dry_run=False, yes=True, sessions=ctx.sessions)
    created = []

    def _timed(label, fn):
        started = time.monotonic()
        result = fn()
        timings['steps'][label] = round(time.monotonic() - started, 1)
        journal.event('rehearsal', '%s took %.1f s' % (label, timings['steps'][label]))
        return result
    try:
        session = ctx.runner_session(guard)
        try:
            _timed('create and fill', lambda: create_source(session, names, spec.layout, rows, ctx.log))
            created.append(names['source'])
        finally:
            session.close()
        reader = ctx.runner_session(read_guard('runner P5'))
        try:
            data = _timed('O4 capture', lambda: inventory.capture(reader, spec, config, None, exact_counts=True, log=ctx.log, step='O4', sleep=lambda s: None))
        finally:
            reader.close()
        plans = inventory.plan_tables(data, config, spec)
        timings['bucketed_tables'] = sorted(p.name for p in plans if p.n_buckets > 1)
        inventory.write(data, inventory.inventory_dir(config, ctx.run_id, spec, 'O4'), plans)
        for step_id in ('O1', 'O2', 'O3'):
            journal.record_gate(step_id, True, 'rehearsal')
        journal.step_end('O4', 'passed', 'rehearsal capture')
        out5 = _timed('O5', lambda: steps.run_step(steps.REGISTRY['O5'], sub))
        created.append(names['replacement'])
        if out5.status != steps.PASSED:
            raise KitError('Rehearsal O5 failed: ' + out5.note)
        killer = KILL_STRATEGIES[kill_test]
        interrupted = _timed('O6 kill test', lambda: killer(spec, plans, journal, ctx.runner_connector(), ctx.log, config.copy))
        if not interrupted:
            raise KitError('Rehearsal kill test left no started chunk; the resume would prove nothing')
        before = len(journal.events('reconcile_delete'))

        def _resume():
            limit = time.monotonic() + RESUME_LIMIT_SECONDS
            while True:
                outcome = steps.run_step(steps.REGISTRY['O6'], sub, redo=True)
                if outcome.status != steps.INCOMPLETE or time.monotonic() >= limit:
                    return outcome
                ctx.log('rehearsal: O6 incomplete (a killed chunk may still hold its leaf lock); waiting %d s and resuming' % config.copy.lock_retry_seconds)
                time.sleep(config.copy.lock_retry_seconds)
        out6 = _timed('O6 resume', _resume)
        if out6.status != steps.PASSED:
            raise KitError('Rehearsal O6 did not complete: ' + out6.note)
        reconciled = len(journal.events('reconcile_delete')) - before
        if reconciled != len(interrupted):
            raise KitError('Rehearsal kill test: %d chunk(s) were interrupted but %d were reconciled on resume' % (len(interrupted), reconciled))
        out7 = _timed('O7', lambda: steps.run_step(steps.REGISTRY['O7'], sub, redo=True))
        if out7.status != steps.PASSED:
            raise KitError('Rehearsal O7 failed: ' + out7.note)
        out8 = _timed('O8', lambda: steps.run_step(steps.REGISTRY['O8'], sub, redo=True))
        if out8.status != steps.PASSED:
            raise KitError('Rehearsal O8 failed: ' + out8.note)
        switched = None
        if ctx.owner_configured:
            switched = _rehearse_switch(ctx, sub, spec, names, plans, data, journal, _timed, timings, created)
        timings['reconciled_chunks'] = reconciled
        timings['interrupted_chunks'] = ['%s bucket %d' % c for c in interrupted]
        timings['switch'] = switched or 'not rehearsed (no owner profile)'
        timings['p5_status'] = 'passed' if switched else 'pending'
        timings['finished_utc'] = iso(utc_now())
        (run_dir / 'timings.json').write_text(json.dumps(timings, indent=1), encoding='utf8')
        summary = ', '.join('%s %.1f s' % kv for kv in timings['steps'].items())
        ctx.journal.step_start('P5')
        if not switched:
            ctx.journal.step_end('P5', 'pending', 'rehearsal %s: copy, kill test and verify passed (%s); the switch needs the owner profile' % (prefix, summary),
                                 str(run_dir / 'timings.json'))
            return steps.Outcome(steps.PENDING, 'rehearsal copy passed (%s); P5 needs the switch rehearsed with the owner profile' % summary, EXIT_GATE)
        ctx.journal.step_end('P5', 'passed', 'rehearsal %s: %s; switch %s' % (prefix, summary, switched), str(run_dir / 'timings.json'))
        ctx.log('rehearsal timings written to %s' % (run_dir / 'timings.json'))
        return steps.Outcome(steps.PASSED, 'rehearsal complete: %s; switch %s' % (summary, switched))
    finally:
        if not keep and created:
            session = ctx.runner_session(guard)
            try:
                cleanup(session, prefix, ctx.log, only=set(created))
            finally:
                session.close()
        journal.close()


def _rehearse_switch(ctx, sub, spec, names, plans, data, journal, timed, timings, created):
    """The Path A freeze with the write probe, then the switch state machine; returns the switch summary."""
    from . import freeze
    reader = ctx.runner_session(read_guard('runner P5'))
    try:
        for role, name in (('source', names['source']), ('replacement', names['replacement'])):
            rows_att = reader.query(switch.ATTACHED_SQL, (name,))
            if rows_att:
                r = rows_att[0]
                journal.record_attachment(role, name, r.get('STORAGE_ID'), r.get('COMPUTE_ID'), r.get('ATTACH_TYPE'))
    finally:
        reader.close()
    sw_names = ownersql.names_for(sub)
    if ctx.config.freeze.mode == 'readonly':
        guard = steps.guard_for('O3', spec, steps.OWNER)
        owner_freeze = ctx.owner_session(guard)
        try:
            timed('O3 freeze', lambda: ownersql.execute_block(freeze.block(sw_names), owner_freeze, ctx.log, journal,
                                                            open_session=ctx.owner_session, guard=guard))
        finally:
            owner_freeze.close()
        if ctx.config.freeze.write_probe:
            probe = ctx.runner_session(probe_guard(spec.source, 'runner P5'))
            try:
                table, column, kind = freeze.probe_target(plans, data['tables'])
                freeze.write_probe(probe, spec.source, table, column, ctx.log, ctx.config.copy.lock_probe_timeout_seconds, kind)
            finally:
                probe.close()
    else:
        ctx.log('rehearsal: the lockout freeze is not rehearsed; the rehearsal source stays READWRITE for the switch')
    journal.set_meta('freeze_time', iso(utc_now()))
    guard = steps.guard_for('O10', spec, steps.OWNER)
    owner = ctx.owner_session(guard)
    runner = ctx.runner_session(read_guard('runner P5'))
    try:
        counts = {'tables': len(data.get('tables') or []), 'views': len(data.get('views') or []), 'pipelines': 0}
        machine = switch.SwitchMachine(owner, sw_names, journal, ctx.log, runner, counts, spec.layout, open_session=ctx.owner_session, guard=guard)
        timed('O10 switch', machine.forward)
    finally:
        owner.close()
        runner.close()
    created.append(names['retained'])
    timings['portal_cleanup'] = ['detached original rehearsal storage %s: delete it in the Portal by STORAGE_ID' % sw_names['source_sid']]
    ctx.log('rehearsal: the detached original storage %s stays after cleanup; delete it in the Portal by STORAGE_ID' % sw_names['source_sid'])
    return 'live %s on STORAGE_ID %s' % (names['source'], sw_names.get('live_sid'))


def handle_p5(step, ctx):
    """P5 handler: run the rehearsal with the default prefix and row count from the step options."""
    prefix = ctx.options.get('prefix') or default_prefix(ctx.run_id)
    return run(ctx, prefix, rows=int(ctx.options.get('rows') or SEED_ROWS), keep=bool(ctx.options.get('keep')),
               kill_test=ctx.options.get('kill_test') or 'sigkill')


steps.register('P5', handle_p5)
