"""s2rebuild rehearse: P5, the real steps on throwaway databases, with a kill test and the rollback leg.

Author: John Bergamini

Section 10.18 of the architecture (release 1.0 W12). The rehearsal source <prefix>_src is either the
synthetic set (tables that exercise a single-column key, a composite key, a bigint AUTO_INCREMENT key, a
varchar unique key, duplicate rows without a key, an empty table, a table with a time column, and one
view, filled with exactly the requested row count, the chunking thresholds scaled to it) or, with
--from-database NAME --percent P, a sample of a real database built by sample.build (the thresholds of
the configuration then apply). With the owner profile the rehearsal then runs the real steps through
steps.run_step against the throwaway names, with the rehearsal's own journal, configuration and run
folder under <runs_dir>/<run>/rehearsal/: a P3 capture, O2 (stop and capture, nothing to stop on a
source without pipelines), O3 (DETACH and ATTACH READ ONLY of the throwaway source, the write probe,
freeze_time), O4 (the freeze capture inside run), O5, O6 with the kill test (SIGKILL of a copy process
mid-chunk, or the in-process interrupt in the tests) and the resume that reconciles exactly the
interrupted chunk, O7, O8, O9 (pipelines recreated stopped when the capture has any; O11 never starts
them unless --start-pipelines), then the switch state machine: S1 and S2, a rollback by the original
record's own ATTACH_SYNTAX (R2a), and O10 through run_step, which continues from the rollback state at
the S2b proof and ends with the two-entry gate; O11, C1 (the throwaway has no application: an idle
workload), C2 skipped, and C3 with its two-entry gate. P5 fails when the end state is not two entries.
The P5 note lists what was not rehearsed. Without the owner profile O2 to O11, C1 and C3 cannot run:
the rehearsal records O2 and O3 skipped, copies and verifies (O4 to O8) and P5 stays pending. Every
rehearsal name starts with s2rebuild_rehearsal_ and none may equal a configured database name;
cleanup drops only names with the prefix, by record (an attached read-write record is dropped behind a
record check; the READ ONLY retained copy is detached, re-attached read write by its own syntax and
dropped; a detached record is re-attached by its syntax first), limited to what this invocation
created. Statements issued through the rehearsal guard (the runner write kinds plus CREATE DATABASE,
DROP DATABASE and USE on the prefixed names only): the fill of the synthetic source and the runner's
cleanup; through the cleanup owner guard (DETACH, ATTACH and DROP DATABASE on the prefixed names
only): the cleanup by record; every rehearsed step runs through its own step guard.
"""
import dataclasses
import json
import os
import signal
import time

from . import copy as copy_module
from . import inventory, ownersql, sample, steps, switch
from .common import EXIT_GATE, KitError, iso, utc_now
from .config import Section
from .journal import Journal
from .sqlutil import Guard, RUNNER_KINDS, ident, owner_guard, read_guard

MARKER = 's2rebuild_rehearsal_'
SEED_ROWS = 1000000
KILL_POLL_SECONDS = 0.05
KILL_WAIT_SECONDS = 600
RESUME_LIMIT_SECONDS = 30 * 60
IDLE_WORKLOAD_NOTE = 'rehearsal: the throwaway database has no application, so C1 passes as an idle workload'
NEEDS_OWNER = 'the freeze, the pipelines, the switch, the rollback, the release, C1 and C3 (they need the owner profile)'
R2B_NOTE = ('the rollback through the retained copy (R2b, R2c): the rehearsal rolls back before S2b by the original record (R2a), '
            'since a switch cannot continue after a rollback through the retained copy')

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
# the body names no database: fill_statements issues USE of the source first, as an application view would be written
VIEW = 'CREATE VIEW {db}.`keyed_counts` AS SELECT `a`, COUNT(*) AS `n` FROM `keyed_composite` GROUP BY `a`'
SEED = 'CREATE TABLE {db}.`seed` (`k` int(11) NOT NULL, PRIMARY KEY (`k`))'


class _RehearsalKill(BaseException):
    """Raised inside a chunk by the in-process kill strategy so the chunk stays 'started' (no handler catches it)."""


class _RehearsalFailed(KitError):
    """A rehearsed step did not pass: P5 ends failed with the step's note (raised after the source exists, so cleanup runs)."""


def rehearsal_names(prefix):
    """The three throwaway names: source, replacement and retained (the live name is the source name after the switch)."""
    return {'source': prefix + '_src', 'replacement': prefix + '_rebuild', 'retained': prefix + '_retained'}


def rehearsal_guard(prefix):
    """Runner write kinds plus CREATE and DROP DATABASE, on the prefixed names only."""
    names = set(rehearsal_names(prefix).values())
    return Guard('rehearsal', RUNNER_KINDS | {'CREATE DATABASE', 'DROP DATABASE', 'USE'}, names)


def cleanup_guard(prefix):
    """The owner allowance of the cleanup by record: DETACH, ATTACH and DROP DATABASE on the prefixed names only."""
    return owner_guard('rehearsal cleanup', ('DETACH DATABASE', 'ATTACH DATABASE', 'DROP DATABASE'), set(rehearsal_names(prefix).values()))


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


def rehearsal_config(config, rows=None, run_dir=None, start_pipelines=False):
    """The configuration the rehearsed steps run with.

    The chunking thresholds are scaled to rows, so tables of rows rows split into four buckets
    (rows None, a sampled source, keeps the configured thresholds); no window, so nothing has a
    time limit; the read-only freeze whatever the configured path (the lockout is about the
    customer's accounts, which the throwaway has none of); no pipeline start at O11 unless
    start_pipelines; no parallelism change at O11; and every file of the rehearsed steps under
    run_dir (paths.runs_dir).
    """
    copy = config.copy.as_dict()
    if rows is not None:
        copy.update(min_rows_for_chunking=max(1, rows // 10), chunk_rows=max(1, rows // 4))
    freeze = dict(config.freeze.as_dict(), mode='readonly')
    pipelines = config.pipelines.as_dict()
    if not start_pipelines:
        pipelines['start_at_release'] = 'none'
    closeout = dict(config.closeout.as_dict(), flexible_parallelism=None)
    paths = config.paths.as_dict()
    if run_dir is not None:
        paths['runs_dir'] = str(run_dir)
    return dataclasses.replace(config, copy=Section(copy), freeze=Section(freeze), pipelines=Section(pipelines), closeout=Section(closeout),
                               paths=Section(paths), window=None)


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
    out.append('USE ' + d)
    out.append(VIEW.format(db=d))
    out.append('DROP TABLE %s.`seed`' % d)
    return out


def existing_names(session, prefix, records=False):
    """The rehearsal names that exist in the workspace now: attached databases, or with records (an owner session) any record not dropped."""
    names = rehearsal_names(prefix).values()
    if records:
        return [n for n in names if switch.identify(session, n) is not None]
    return [n for n in names if session.query(inventory.LAYOUT_SQL, (n,))]


def _refuse_taken(session, names):
    taken = [n for n in names.values() if session.query(inventory.LAYOUT_SQL, (n,))]
    if taken:
        raise KitError('Rehearsal name(s) %s already exist; clean up first (s2rebuild rehearse --cleanup --prefix PREFIX --yes) or choose another prefix'
                       % ', '.join(taken))


def create_source(session, names, layout, rows, log):
    """Create and fill the synthetic rehearsal source; refuses when any rehearsal name already exists."""
    _refuse_taken(session, names)
    session.execute('CREATE DATABASE %s PARTITIONS %d SUB_PARTITIONS %d' % (ident(names['source']), layout.partitions, layout.sub_partitions))
    for sql in fill_statements(names['source'], rows):
        session.execute(sql)
    log('rehearsal source %s created and filled (%d rows in keyed_single and in nokey_dupes)' % (names['source'], rows))


def sample_source(ctx, names, from_database, percent):
    """Build the rehearsal source as a percent sample of from_database (sample.build); refuses when any rehearsal name already exists."""
    reader = ctx.runner_session(read_guard('runner P5'))
    try:
        _refuse_taken(reader, names)
    finally:
        reader.close()
    summary = sample.build(ctx.runner_session, ctx.owner_session if ctx.owner_configured else None, from_database, names['source'], percent,
                           ctx.config, ctx.log)
    for line in sample.summary_lines(summary):
        ctx.log(line)
    return summary


def _syntax_of(record):
    """The own ATTACH_SYNTAX of a detached record (the AT MILESTONE form), or KitError."""
    syntax = str(record.get('ATTACH_SYNTAX') or '').strip().rstrip(';')
    if ' AT MILESTONE ' not in syntax:
        raise KitError('Rehearsal cleanup stopped: the detached record %s shows no AT MILESTONE syntax; remove it by hand from the Portal'
                       % switch.record_text(record))
    return syntax


def _expect_record(session, name, expected, detached=None, access=None):
    """identify(name) that must match expected (and its state); KitError with both records otherwise."""
    live = switch.identify(session, name, expected)
    if not switch.records_match(expected, live):
        raise KitError(switch.refusal_text('rehearsal cleanup of %s' % name, expected, live))
    if detached is not None and bool(live.get('IS_DETACHED')) != bool(detached):
        raise KitError('Rehearsal cleanup refused at %s: expected the record %s, observed %s' % (name, 'detached' if detached else 'attached',
                                                                                                  switch.record_text(live)))
    if access is not None and str(live.get('ACCESS') or '').upper() != access:
        raise KitError('Rehearsal cleanup refused at %s: expected the record attached %s, observed %s' % (name, access, switch.record_text(live)))
    return live


def cleanup(session, prefix, log, only=None, owner=False):
    """Remove the prefixed names that exist; returns the names dropped.

    With owner (an owner session behind cleanup_guard) every record under a prefixed name goes,
    by record: an attached read-write record is dropped behind a record check; the READ ONLY
    retained copy, which the server refuses to drop (fact 28), is detached, re-attached read write
    by its own ATTACH_SYNTAX and dropped; a detached record (a rehearsal that stopped after S1) is
    re-attached by its syntax and dropped. Without it (the runner behind rehearsal_guard) only
    attached read-write names are dropped, and the rest is logged for s2rebuild rehearse --cleanup
    with the owner profile. only limits the names to those this invocation created.
    """
    dropped, left = [], []
    for name in rehearsal_names(prefix).values():
        if only is not None and name not in only:
            continue
        if not owner:
            if not session.query(inventory.LAYOUT_SQL, (name,)):
                continue
            try:
                session.execute('DROP DATABASE ' + ident(name))
            except KitError:
                raise
            except Exception as exc:  # noqa: BLE001 - a READ ONLY attachment refuses the DROP (2720); the owner profile removes it
                if not switch.read_only_refusal(exc):
                    raise
                left.append('%s (READ ONLY)' % name)
                continue
            dropped.append(name)
            continue
        for _ in range(3):  # a name may hold an attached record and a detached one
            record = switch.identify(session, name)
            if record is None:
                break
            if record.get('ACCESS') == 'READONLY':
                session.execute('DETACH DATABASE ' + ident(name))
                record = _expect_record(session, name, record, detached=True)
            if record.get('ACCESS') is None:
                session.execute(_syntax_of(record))
                record = _expect_record(session, name, record, access='READWRITE')
            _expect_record(session, name, record, access='READWRITE')
            session.execute('DROP DATABASE ' + ident(name))
            dropped.append(name)
            log('rehearsal cleanup dropped %s (%s)' % (name, switch.record_text(record)))
    log('rehearsal cleanup dropped: %s' % (', '.join(dropped) or 'nothing'))
    if left:
        log('rehearsal cleanup left %s: a READ ONLY attachment is detached, re-attached by its record and dropped with the owner profile '
            '(s2rebuild rehearse --cleanup --prefix %s with connections.owner set)' % (', '.join(left), prefix))
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
        raise KitError('Rehearsal kill test inconclusive: the copy ended before a chunk could be interrupted; raise --rows (or --percent with --from-database)')
    started = _started_chunks(journal)
    log('kill test: copy process %d killed with SIGKILL while %s was started' % (pid, ', '.join('%s bucket %d' % c for c in started)))
    return started


KILL_STRATEGIES = {'sigkill': kill_sigkill, 'inprocess': kill_inprocess}


def _dry_run(ctx, prefix, names, spec, rows, from_database, percent):
    if from_database is None:
        session = ctx.runner_session(rehearsal_guard(prefix))
        try:
            session.execute('CREATE DATABASE %s PARTITIONS %d SUB_PARTITIONS %d' % (ident(names['source']), spec.layout.partitions, spec.layout.sub_partitions))
            for sql in fill_statements(names['source'], rows):
                session.execute(sql)
        finally:
            session.close()
    else:
        ctx.log('DRY RUN: the rehearsal would build %s as a %d percent sample of %s (s2rebuild sample-database %s %s --percent %d); the sample '
                'statements need the capture of %s, which a dry run does not read' % (names['source'], percent, from_database, from_database,
                                                                                      names['source'], percent, from_database))
    ctx.log('DRY RUN: the rehearsal would run O2 to O11, C1 and C3 on %s and %s through the real steps (the freeze, a SIGKILL test in the copy, '
            'the verify, the switch with a rollback by the original record and the switch again, the release, C1 and the two-entry gate of C3) '
            'with the owner profile, and drop the names it created by record; nothing was written' % (names['source'], names['replacement']))
    return steps.Outcome(steps.PASSED, 'dry run: rehearsal statements printed')


def unrehearsed(config, data, owner, start_pipelines):
    """What the rehearsal leaves out, as lines for the P5 note: from the configuration, the capture of the rehearsal source and the flags."""
    items = []
    if not owner:
        items.append(NEEDS_OWNER)
    if config.freeze.mode == 'lockout':
        items.append('the lockout freeze (Path B): the rehearsal freezes with Path A, the read-only attachment')
    tables = data.get('tables') or []
    if not any(t.get('reference') for t in tables):
        items.append('reference tables (none on the rehearsal source)')
    if not data.get('views'):
        items.append('views (none on the rehearsal source)')
    if not data.get('routines'):
        items.append('routines (none on the rehearsal source)')
    captured = (data.get('pipelines') or {}).get('items') or []
    if not captured:
        items.append('pipelines (none on the rehearsal source)')
    elif not start_pipelines:
        items.append('the pipeline start at O11 (the pipelines are created stopped; rehearse --start-pipelines starts them)')
    if owner:
        items.append(R2B_NOTE)
        items.append('application writes at C1 (the throwaway has no application; C1 passed as an idle workload)')
        items.append('C2, C4 and the deletion of the retained copy (C5)')
    return items


def _step(sub, step_id, redo=True):
    """Run one rehearsed step through steps.run_step; a result other than passed ends the rehearsal failed with the step's note."""
    try:
        out = steps.run_step(steps.REGISTRY[step_id], sub, redo=redo)
    except KitError as exc:
        raise _RehearsalFailed('Rehearsal %s refused: %s' % (step_id, exc))
    if out.status != steps.PASSED:
        raise _RehearsalFailed('Rehearsal %s %s: %s' % (step_id, out.status, out.note))
    return out


def run(ctx, prefix, rows=SEED_ROWS, keep=False, kill_test='sigkill', from_database=None, percent=1, start_pipelines=False):
    """P5: build the source, then the real steps on the throwaway names (see the module docstring); timings, the note, cleanup by record.

    Refusals before the source exists (a bad prefix, a taken name) raise KitError and leave
    nothing behind. A rehearsed step that does not pass, and a C3 gate that does not end at two
    entries, end P5 failed with the step's note; the names created are dropped unless keep.
    """
    check_prefix(prefix, ctx.config)
    names = rehearsal_names(prefix)
    spec = rehearsal_spec(ctx.db, prefix)
    if from_database is not None:
        ident(from_database)
        percent = sample.check_percent(percent)
        if from_database in names.values():
            raise KitError('--from-database %s is a rehearsal name; name the database to sample' % from_database)
    if ctx.dry_run:
        return _dry_run(ctx, prefix, names, spec, rows, from_database, percent)
    run_dir = ctx.config.runs_dir() / ctx.run_id / 'rehearsal'
    run_dir.mkdir(parents=True, exist_ok=True)
    config = rehearsal_config(ctx.config, None if from_database is not None else rows, run_dir, start_pipelines)
    guard = rehearsal_guard(prefix)
    owner = ctx.owner_configured
    timings = {'prefix': prefix, 'rows': rows, 'from_database': from_database, 'percent': percent if from_database is not None else None,
               'steps': {}, 'p5_status': 'running'}
    journal = Journal(run_dir / 'journal.sqlite')
    # the throwaway source has no sessions to sample, so the O4 capture inside run takes no pause between its samples
    options = {'env': ctx.options.get('env') or {}, 'idle_workload': IDLE_WORKLOAD_NOTE, 'capture_sleep': lambda seconds: None}
    if not start_pipelines:
        options['sleep'] = lambda seconds: None  # C1 samples the offsets only when a pipeline was started
    sub = steps.StepContext(config, spec, journal, ctx.run_id, ctx.log, dry_run=False, yes=True, sessions=ctx.sessions, options=options)
    created = []

    def _timed(label, fn):
        started = time.monotonic()
        result = fn()
        timings['steps'][label] = round(time.monotonic() - started, 1)
        journal.event('rehearsal', '%s took %.1f s' % (label, timings['steps'][label]))
        return result
    try:
        try:
            data = _build_and_capture(ctx, sub, names, spec, rows, from_database, percent, guard, created, _timed, timings)
            try:
                if owner:
                    summary = _rehearse_with_owner(ctx, sub, names, spec, data, kill_test, created, _timed, timings)
                else:
                    summary = _rehearse_copy_only(ctx, sub, names, spec, data, kill_test, created, _timed, timings)
            except KitError as exc:  # a rehearsed step that did not pass, or a check of the switch leg that refused
                raise _RehearsalFailed(str(exc))
        except _RehearsalFailed as exc:
            timings['p5_status'] = 'failed'
            timings['failure'] = str(exc)
            timings['finished_utc'] = iso(utc_now())
            (run_dir / 'timings.json').write_text(json.dumps(timings, indent=1), encoding='utf8')
            ctx.journal.step_start('P5')
            ctx.journal.step_end('P5', 'failed', 'rehearsal %s failed: %s' % (prefix, exc), str(run_dir / 'timings.json'))
            return steps.Outcome(steps.FAILED, 'rehearsal failed: %s' % exc, EXIT_GATE)
        timings['not_rehearsed'] = unrehearsed(ctx.config, data, owner, start_pipelines)
        timings['p5_status'] = 'passed' if owner else 'pending'
        timings['finished_utc'] = iso(utc_now())
        (run_dir / 'timings.json').write_text(json.dumps(timings, indent=1), encoding='utf8')
        times = ', '.join('%s %.1f s' % kv for kv in timings['steps'].items())
        left_out = '; not rehearsed: ' + '; '.join(timings['not_rehearsed'])
        ctx.journal.step_start('P5')
        if not owner:
            ctx.journal.step_end('P5', 'pending', 'rehearsal %s: copy, kill test and verify passed (%s); %s' % (prefix, times, NEEDS_OWNER),
                                 str(run_dir / 'timings.json'))
            return steps.Outcome(steps.PENDING, 'rehearsal copy passed (%s); P5 needs the owner profile for %s%s' % (
                times, NEEDS_OWNER.split(' (')[0], left_out), EXIT_GATE)
        ctx.journal.step_end('P5', 'passed', 'rehearsal %s: %s; %s%s' % (prefix, times, summary, left_out), str(run_dir / 'timings.json'))
        ctx.log('rehearsal timings written to %s' % (run_dir / 'timings.json'))
        return steps.Outcome(steps.PASSED, 'rehearsal complete: %s; %s%s' % (times, summary, left_out))
    finally:
        if not keep and created:
            _cleanup_created(ctx, prefix, created)
        journal.close()


def _cleanup_created(ctx, prefix, created):
    """Drop the names this invocation created: by record with the owner profile, by name as the runner without it."""
    if ctx.owner_configured:
        session = ctx.owner_session(cleanup_guard(prefix))
    else:
        session = ctx.runner_session(rehearsal_guard(prefix))
    try:
        cleanup(session, prefix, ctx.log, only=set(created), owner=ctx.owner_configured)
    finally:
        session.close()


def _build_and_capture(ctx, sub, names, spec, rows, from_database, percent, guard, created, timed, timings):
    """The rehearsal source, its P3 capture under the rehearsal folder, the source record and the steps before O2 recorded; returns the capture."""
    config, journal = sub.config, sub.journal
    if from_database is not None:
        timed('sample source', lambda: sample_source(ctx, names, from_database, percent))
    else:
        session = ctx.runner_session(guard)
        try:
            timed('create and fill', lambda: create_source(session, names, spec.layout, rows, ctx.log))
        finally:
            session.close()
    created.append(names['source'])
    reader = ctx.runner_session(read_guard('runner P5'))
    owner_reader = ctx.owner_session(read_guard('owner P5')) if ctx.owner_configured else None
    try:
        data = timed('P3 capture', lambda: inventory.capture(reader, spec, config, owner_reader, exact_counts=True, log=ctx.log, step='P3',
                                                             sleep=lambda s: None))
        record = switch.identify(reader, names['source'])
    finally:
        reader.close()
        if owner_reader is not None:
            owner_reader.close()
    if record is None or record.get('ACCESS') is None:
        raise _RehearsalFailed('the rehearsal source %s has no attached branch record; nothing to rehearse on' % names['source'])
    switch.journal_record(journal, 'source', record)  # as P2 records the source record of a run
    plans = inventory.plan_tables(data, config, spec)
    timings['bucketed_tables'] = sorted(p.name for p in plans if p.n_buckets > 1)
    inventory.write(data, inventory.inventory_dir(config, ctx.run_id, spec, 'P3'), plans)
    journal.record_gate('P1', True, 'rehearsal')
    journal.step_end('P2', 'passed', 'rehearsal: the source record recorded')
    journal.step_end('P3', 'passed', 'rehearsal capture')
    journal.record_gate('P4', True, 'rehearsal')
    journal.record_gate('O1', True, 'rehearsal go: the throwaway application is stopped')
    return data


def _copy_steps(ctx, sub, names, spec, kill_test, created, timed, timings):
    """O4 (the capture inside run), O5, the kill test, the O6 resume that reconciles exactly the interrupted chunk, O7 and O8."""
    from . import cli  # noqa: F401 - registers the O4 handler of run (the capture, drift and plan inside run)
    config, journal = sub.config, sub.journal
    timed('O4', lambda: _step(sub, 'O4'))
    data, plans, _ = copy_module.load_plans(sub)
    timed('O5', lambda: _step(sub, 'O5'))
    created.append(names['replacement'])
    killer = KILL_STRATEGIES[kill_test]
    interrupted = timed('O6 kill test', lambda: killer(spec, plans, journal, ctx.runner_connector(), ctx.log, config.copy))
    if not interrupted:
        raise _RehearsalFailed('the kill test left no started chunk; the resume would prove nothing')
    before = len(journal.events('reconcile_delete'))

    def _resume():
        limit = time.monotonic() + RESUME_LIMIT_SECONDS
        while True:
            outcome = steps.run_step(steps.REGISTRY['O6'], sub, redo=True)
            if outcome.status != steps.INCOMPLETE or time.monotonic() >= limit:
                return outcome
            ctx.log('rehearsal: O6 incomplete (a killed chunk may still hold its leaf lock); waiting %d s and resuming' % config.copy.lock_retry_seconds)
            time.sleep(config.copy.lock_retry_seconds)
    out6 = timed('O6 resume', _resume)
    if out6.status != steps.PASSED:
        raise _RehearsalFailed('O6 did not complete: ' + out6.note)
    reconciled = len(journal.events('reconcile_delete')) - before
    if reconciled != len(interrupted):
        raise _RehearsalFailed('kill test: %d chunk(s) were interrupted but %d were reconciled on resume' % (len(interrupted), reconciled))
    timings['reconciled_chunks'] = reconciled
    timings['interrupted_chunks'] = ['%s bucket %d' % c for c in interrupted]
    return data, plans


def _rehearse_copy_only(ctx, sub, names, spec, data, kill_test, created, timed, timings):
    """Without the owner profile: O2 and O3 skipped, the copy, the kill test, O7 (pending, recorded skipped) and O8; P5 stays pending."""
    journal = sub.journal
    for step_id in ('O2', 'O3'):
        journal.step_end(step_id, 'skipped', 'rehearsal: not rehearsed without the owner profile')
    _copy_steps(ctx, sub, names, spec, kill_test, created, timed, timings)
    out7 = timed('O7', lambda: steps.run_step(steps.REGISTRY['O7'], sub, redo=True))
    if out7.status == steps.PENDING:
        # the runner never creates views (section 10.11): O7 ran the stale check and the counters and waits for the admin
        journal.step_end('O7', 'skipped', 'rehearsal: the views need the owner profile and were not created; stale check and counters ran')
        ctx.log('rehearsal: the views are not rehearsed without the owner profile; the O7 block printed above is not run, '
                'and cleanup drops the rehearsal databases')
    elif out7.status != steps.PASSED:
        raise _RehearsalFailed('O7 %s: %s' % (out7.status, out7.note))
    timings['views'] = 'created' if out7.status == steps.PASSED else 'not rehearsed (no owner profile)'
    timed('O8', lambda: _step(sub, 'O8'))
    timings['switch'] = 'not rehearsed (no owner profile)'
    return 'switch not rehearsed'


def _rehearse_with_owner(ctx, sub, names, spec, data, kill_test, created, timed, timings):
    """With the owner profile: O2, O3, the copy steps, O9, the switch with the rollback leg, O11, C1, C3; returns the switch summary."""
    journal = sub.journal
    timed('O2', lambda: _step(sub, 'O2'))
    timed('O3', lambda: _step(sub, 'O3'))
    timings['freeze'] = 'Path A: DETACH and ATTACH READ ONLY of %s, write probe refused, freeze_time %s' % (names['source'], journal.get_meta('freeze_time'))
    data, plans = _copy_steps(ctx, sub, names, spec, kill_test, created, timed, timings)
    timed('O7', lambda: _step(sub, 'O7'))
    timings['views'] = 'created through the owner session' if data.get('views') else 'none on the rehearsal source'
    timed('O8', lambda: _step(sub, 'O8'))
    timed('O9', lambda: _step(sub, 'O9'))
    timed('O10 switch and rollback', lambda: _rollback_leg(ctx, sub, names, spec))
    created.append(names['retained'])
    timed('O10 switch again', lambda: _step(sub, 'O10'))
    timings['switch'] = 'live %s on STORAGE_ID %s (COMPUTE_ID %s); retained %s on STORAGE_ID %s' % (
        names['source'], (journal.attachment('live') or {}).get('storage_id'), (journal.attachment('live') or {}).get('compute_id'),
        names['retained'], (journal.attachment('retained') or {}).get('storage_id'))
    timed('O11', lambda: _step(sub, 'O11'))
    timed('C1', lambda: _step(sub, 'C1'))
    journal.step_end('C2', 'skipped', 'rehearsal: the report and the calibration are not rehearsed')
    timed('C3', lambda: _step(sub, 'C3'))
    listing = journal.events('endstate_listing')
    timings['end_state'] = 'two entries: ' + (listing[-1].split(': ', 1)[1] if listing else 'listing not recorded')
    ctx.log('rehearsal: two entries after C3 (the live rehearsal name and its retained copy); cleanup drops the names it created, by record')
    return 'switch %s; end state %s' % (timings['switch'], timings['end_state'])


def _rollback_leg(ctx, sub, names, spec):
    """S0 as reads, then S1 and S2 of the switch, then the rollback (R2a, the original record's own ATTACH_SYNTAX) through the state machine.

    After it the original is attached read write under the live name, the rollback state, from
    which O10 through run_step continues at the S2b proof. A rollback after S3 would go through
    the retained copy, after which a switch cannot continue (switch.ROLLED_BACK), so the leg rolls
    back before S2b. Runs through the O10 owner guard.
    """
    sw_names = ownersql.names_for(sub)
    counts = ownersql.expected_counts(sub)
    if counts is None:
        raise _RehearsalFailed('no capture for the switch counts under %s' % sub.config.runs_dir())
    for line in switch.s0_checks(sub, sw_names):
        ctx.log('    ' + line)
    guard = steps.guard_for('O10', spec, steps.OWNER)
    owner = ctx.owner_session(guard)
    runner = ctx.runner_session(read_guard('runner P5'))
    try:
        machine = switch.SwitchMachine(owner, sw_names, sub.journal, ctx.log, runner, counts, spec.layout, open_session=ctx.owner_session, guard=guard)
        machine.record_listing('before')
        machine.s1()
        machine.s2()
        reached = machine.rollback()
    finally:
        owner.close()
        runner.close()
    if reached != switch.ORIGINAL_ATTACHED:
        raise _RehearsalFailed('the rollback leg ended at "%s", not "%s"' % (reached, switch.ORIGINAL_ATTACHED))
    ctx.log('rehearsal: rolled back by the original record (R2a); the switch continues from the rollback state through O10')
    return reached


def handle_p5(step, ctx):
    """P5 handler: run the rehearsal with the default prefix and row count from the step options."""
    prefix = ctx.options.get('prefix') or default_prefix(ctx.run_id)
    return run(ctx, prefix, rows=int(ctx.options.get('rows') or SEED_ROWS), keep=bool(ctx.options.get('keep')),
               kill_test=ctx.options.get('kill_test') or 'sigkill', from_database=ctx.options.get('from_database'),
               percent=ctx.options.get('percent') or 1, start_pipelines=bool(ctx.options.get('start_pipelines')))


steps.register('P5', handle_p5)
