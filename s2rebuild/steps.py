"""s2rebuild steps: the step registry, per-step guards, gates and the run driver.

Author: John Bergamini

Section 3 of the architecture fixes the step ids P1 to P5, O1 to O11 and C1 to C4. This
module holds them with kind, identity, gate text and preconditions, builds the Guard each
step runs behind (section 10.4), and drives `run`, `step` and `gate`. Python steps whose
module lands in a later milestone are registered as stubs: a dry run names them and issues
nothing, a real run refuses them. Statements issued by this module: none.
"""
import dataclasses

from . import sqlutil
from .connect import DryRunSession
from .common import EXIT_DONE, EXIT_GATE, EXIT_INCOMPLETE, EXIT_MISMATCH, EXIT_REFUSED, KitError, format_duration, iso, utc_now

PYTHON, ADMIN, HUMAN, DECISION = 'python', 'admin', 'human', 'decision'
RUNNER, OWNER, PEOPLE, CUSTOMER = 'runner', 'owner', 'people', 'customer'
PASSED, PENDING, FAILED, REFUSED, STUB, SKIPPED = 'passed', 'pending', 'failed', 'refused', 'stub', 'skipped'
INCOMPLETE, MISMATCH = 'incomplete', 'mismatch'


@dataclasses.dataclass(frozen=True)
class Step:
    """One step of the P, O, C model."""

    id: str
    title: str
    kinds: tuple
    identities: tuple
    gate: str
    module: str
    milestone: str
    preconditions: tuple = ()

    @property
    def phase(self):
        """P, O or C."""
        return self.id[0]

    @property
    def needs_person(self):
        """True when a person records the gate (admin, human or decision kinds)."""
        return any(k in (ADMIN, HUMAN, DECISION) for k in self.kinds)

    @property
    def is_admin(self):
        """True when the step issues admin statements with the owner identity."""
        return ADMIN in self.kinds

    @property
    def kind_label(self):
        """The kinds joined for display, such as 'python + decision'."""
        return ' + '.join(self.kinds)

    @property
    def identity_label(self):
        """The identities joined for display, such as 'owner + customer'."""
        return ' + '.join(self.identities)


def _step(id_, title, kinds, identities, gate, module, milestone, pre=()):
    return Step(id_, title, tuple(kinds), tuple(identities), gate, module, milestone, tuple(pre))


REGISTRY = {s.id: s for s in [
    _step('P1', 'Prerequisites', (HUMAN, ADMIN), (OWNER,),
          'Kit installed, runner account created, Keychain items set, config written, s2rebuild check passes',
          'cli', 'M2'),
    _step('P2', 'Preflight', (PYTHON,), (RUNNER, OWNER),
          'Connection, privileges, source layout, STORAGE_ID recorded, target names free, compute size fits the partition count',
          'inventory', 'M5', ('P1',)),
    _step('P3', 'Profile', (PYTHON,), (RUNNER, OWNER),
          'Inventory JSON, profile Excel, estimate with bands, window fit printed', 'inventory', 'M2', ('P2',)),
    _step('P4', 'Plan', (DECISION,), (PEOPLE,),
          'Freeze path, window, abort deadline, named owner for application stopped, pipeline credential handover, '
          'retained name, retention days recorded in config', 'estimate', 'M2', ('P3',)),
    _step('P5', 'Rehearsal', (PYTHON, ADMIN), (RUNNER, OWNER),
          'Throwaway copy and switch completed, kill test resumed, timings recorded under customer/runs/',
          'rehearse', 'M4', ('P4',)),
    _step('O1', 'Start', (PYTHON, DECISION), (RUNNER,),
          'Window and deadline recorded, P2 re-run clean, rehearsal evidence present, go recorded', 'steps', 'M1'),
    _step('O2', 'Stop pipelines', (ADMIN,), (OWNER,),
          'No pipeline in state Running; every definition captured with a credentials placeholder; offsets captured after the stop',
          'pipelines', 'M4', ('O1',)),
    _step('O3', 'Freeze', (ADMIN,), (OWNER,),
          'Path A: write probe refused and ATTACH_TYPE = READONLY. Path B: quiet proof passed the configured number of times',
          'freeze', 'M4', ('O2',)),
    _step('O4', 'Freeze capture', (PYTHON,), (RUNNER,),
          'Inventory captured after the freeze; drift against P3 reviewed; copy plan written', 'inventory', 'M2', ('O3',)),
    _step('O5', 'Create replacement', (PYTHON,), (RUNNER,),
          'Replacement layout verified; every table definition matches the capture', 'copy', 'M3', ('O4',)),
    _step('O6', 'Copy', (PYTHON,), (RUNNER,),
          'Every chunk done; per-table count proof all match', 'copy', 'M3', ('O5',)),
    _step('O7', 'Post-copy', (PYTHON,), (RUNNER,),
          'Views created, counters set, stale check 0 mismatches after re-copy', 'postcopy', 'M3', ('O6',)),
    _step('O8', 'Verify', (PYTHON,), (RUNNER,),
          'Tier 1 and 2 pass for every table; tier 3 within budget or accepted by the operator', 'verify', 'M3', ('O7',)),
    _step('O9', 'Pipelines', (ADMIN,), (OWNER,),
          'Expected pipeline count in the replacement, all Stopped, offsets in the replacement equal the O2 capture',
          'pipelines', 'M4', ('O8',)),
    _step('O10', 'Switch', (ADMIN,), (OWNER,),
          'Live name resolves to the replacement branch with the target layout; retained copy attached read only',
          'switch', 'M4', ('O9',)),
    _step('O11', 'Release', (ADMIN, HUMAN), (OWNER, CUSTOMER),
          'Retention check passed per partition; pipelines started; grants match the snapshot (Path B); '
          'application restarted and writing', 'pipelines', 'M4', ('O10',)),
    _step('C1', 'Verify after release', (PYTHON,), (RUNNER,),
          'Writer watch shows the application on the live database; pipelines Running with offsets advancing; no pipeline errors',
          'watch', 'M4', ('O11',)),
    _step('C2', 'Report and calibrate', (PYTHON,), (RUNNER,),
          'Final Excel and Markdown written; baseline recalibrated from the journal', 'report_xlsx', 'M5', ('C1',)),
    _step('C3', 'Retention cleanup', (ADMIN,), (OWNER,),
          'After retain_days: retained branch and replacement dropped, detached original storage deleted, evidence kept',
          'ownersql', 'M4', ('C2',)),
    _step('C4', 'Remove migration account', (ADMIN,), (OWNER,),
          'DROP USER done, zero privileges remain, Keychain item deleted', 'ownersql', 'M4', ('C3',)),
]}

ORDER = list(REGISTRY)
OUTAGE = [s for s in ORDER if s.startswith('O')]
ADMIN_STEPS = [s for s in ORDER if REGISTRY[s].is_admin]
OWNER_SQL_STEPS = ['O2', 'O3', 'O9', 'O10', 'O11', 'C3', 'C4']


def get_step(step_id):
    """The Step for an id such as O6. Raises KitError for an unknown id."""
    key = str(step_id).upper()
    if key not in REGISTRY:
        raise KitError('Unknown step %s; steps are %s' % (step_id, ', '.join(ORDER)))
    return REGISTRY[key]


# ---------------------------------------------------------------------------
# Guards per step (section 10.4)
# ---------------------------------------------------------------------------
RUNNER_WRITE_STEPS = ('O5', 'O6', 'O7')

# step id: (kinds, database roles the writes may name, per-kind narrowing, required clause per kind)
OWNER_ALLOWANCES = {
    'P1': (('CREATE USER', 'GRANT'), (), {}, {}),
    'O2': (('STOP PIPELINE',), ('source',), {}, {}),
    'O3': (('DETACH DATABASE', 'ATTACH DATABASE'), ('source',), {}, {'ATTACH DATABASE': 'READ ONLY'}),
    'O9': (('USE', 'CREATE PIPELINE', 'ALTER PIPELINE'), ('replacement',), {}, {}),
    'O10': (('DETACH DATABASE', 'ATTACH DATABASE'), ('source', 'replacement', 'retained'), {'DROP DATABASE': ('retained',)}, {}),
    'O11': (('START PIPELINE', 'GRANT', 'SET GLOBAL'), ('source',), {}, {}),
    'C3': (('DROP DATABASE',), ('retained', 'replacement'), {}, {}),
    'C4': (('DROP USER',), (), {}, {}),
    'lockout': (('REVOKE', 'GRANT', 'KILL CONNECTION'), ('source',), {}, {}),
}


def guard_for(step_id, db, identity=RUNNER):
    """The Guard a step runs behind for one database and identity.

    Runner: writes naming the replacement at O5 to O7, the write probe at O3, reads elsewhere.
    Owner: the statement kinds of the step, naming only the run's database names.
    """
    key = str(step_id)
    if identity == RUNNER:
        if key in RUNNER_WRITE_STEPS:
            return sqlutil.runner_guard(db.replacement, 'runner ' + key)
        if key == 'O3':
            return sqlutil.probe_guard(db.source, 'runner O3')
        return sqlutil.read_guard('runner ' + key)
    if key not in OWNER_ALLOWANCES:
        raise KitError('Step %s has no owner statements' % key)
    kinds, roles, narrow, required = OWNER_ALLOWANCES[key]
    databases = {getattr(db, role) for role in roles}
    per_kind = {kind: {getattr(db, role) for role in kind_roles} for kind, kind_roles in narrow.items()}
    return sqlutil.owner_guard('owner ' + key, kinds, databases, per_kind=per_kind, required_detail=required)


# ---------------------------------------------------------------------------
# Context, outcomes and handlers
# ---------------------------------------------------------------------------
class StepContext:
    """What a handler receives: config, the database, its journal, the log and the run flags."""

    def __init__(self, config, db, journal, run_id, log, dry_run=False, yes=False, verbose=False, options=None, sessions=None):
        self.config = config
        self.db = db
        self.journal = journal
        self.run_id = run_id
        self.log = log
        self.dry_run = dry_run
        self.yes = yes
        self.verbose = verbose
        self.options = dict(options or {})
        self.sessions = sessions

    @property
    def journal_path(self):
        """The journal file path, or None for an in-memory journal."""
        path = getattr(self.journal, 'path', None)
        return None if path in (None, ':memory:') else path

    def runner_session(self, guard):
        """A guarded runner session from the factory; a recording session in a dry run without a factory."""
        if self.sessions is not None:
            return self.sessions.runner(guard)
        if self.dry_run:
            return DryRunSession(guard, self.log)
        raise KitError('No connection factory is available for a live step')

    def owner_session(self, guard):
        """A guarded owner session, with the same dry-run rule as runner_session."""
        if self.sessions is not None:
            return self.sessions.owner(guard)
        if self.dry_run:
            return DryRunSession(guard, self.log)
        raise KitError('No connection factory is available for a live step')

    def runner_connector(self):
        """A callable opening raw runner connections for the copy workers."""
        if self.sessions is None:
            raise KitError('No connection factory is available for a live step')
        return self.sessions.runner_connector()

    @property
    def owner_configured(self):
        """True when connections.owner is present, so admin steps can execute with gates (M4)."""
        return self.config.owner is not None


@dataclasses.dataclass
class Outcome:
    """The result of one step attempt."""

    status: str
    note: str = ''
    exit_code: int = EXIT_DONE


def gate_pending(step, ctx, lines=()):
    """Print what a person must do and record, then report the gate as pending."""
    for line in lines:
        ctx.log.plain('    ' + line)
    ctx.log.plain('    gate: ' + step.gate)
    ctx.log.plain('    record with: s2rebuild gate %s --pass --note TEXT [--evidence FILE]' % step.id)
    return Outcome(PENDING, 'gate pending: ' + step.gate, EXIT_GATE)


def not_yet(step, ctx):
    """Handler for a python step whose module lands in a later milestone."""
    note = '%s.py arrives in %s' % (step.module, step.milestone)
    if ctx.dry_run:
        return Outcome(STUB, 'no statements yet; ' + note)
    return Outcome(REFUSED, 'cannot run yet; ' + note, EXIT_REFUSED)


def admin_pending(step, ctx):
    """Fallback handler for an admin step without a module handler: print the pointer and wait for the gate."""
    lines = ['identity: %s' % step.identity_label]
    if step.id == 'O3':
        mode = ctx.config.freeze.mode
        lines.append('freeze path: %s' % ('A, read-only attachment (DETACH then ATTACH READ ONLY)' if mode == 'readonly'
                                           else 'B, lockout (revoke and kill loop with quiet proof)'))
    lines.append('SQL block: s2rebuild owner-sql %s' % step.id)
    return gate_pending(step, ctx, lines)


def decision_pending(step, ctx):
    """Handler for a decision step: the people named record the answer as a gate."""
    return gate_pending(step, ctx, ['decision recorded by: %s' % step.identity_label])


def human_pending(step, ctx):
    """Handler for a step a person performs by hand and records as a gate."""
    return gate_pending(step, ctx, ['done by: %s' % step.identity_label])


def o1_start(step, ctx):
    """O1: record window and deadline, report the readiness checks, then wait for the go decision."""
    config = ctx.config
    if config.window is None:
        return Outcome(FAILED, 'window.start and window.end are not configured; record them at P4 first', EXIT_GATE)
    window = config.window
    longest, longest_note = 0.0, 'no capture yet, so no longest-chunk allowance; O6 recomputes it from the plan'
    try:
        from . import copy as copy_module
        _, plans, _ = copy_module.load_plans(ctx)
        longest = copy_module.longest_chunk_seconds(plans, config)
        longest_note = 'longest modeled chunk %s' % format_duration(longest)
    except KitError:
        pass
    deadline = window.abort_deadline(longest)
    lines = [
        'names: source %s, replacement %s, retained %s' % ctx.db.names,
        'window: %s to %s (%s)' % (iso(window.start), iso(window.end), format_duration(window.duration.total_seconds())),
        'abort deadline: %s (end minus %d m restart allowance, %d m buffer; %s)' % (
            iso(deadline), window.restart_allowance_minutes, window.buffer_minutes, longest_note),
        'freeze path: %s' % ('A read-only attachment' if config.freeze.mode == 'readonly' else 'B lockout'),
        'P2 re-run: s2rebuild check (milestone M5)',
        'rehearsal evidence: %s' % ('found under %s' % ctx.config.runs_dir() if _rehearsal_evidence(ctx) else 'none found'),
    ]
    now = utc_now()
    if now >= deadline:
        if ctx.dry_run:
            lines.append('abort deadline is in the past relative to now (%s); a real run stops here' % iso(now))
        else:
            for line in lines:
                ctx.log.plain('    ' + line)
            return Outcome(FAILED, 'abort deadline %s is already past' % iso(deadline), EXIT_GATE)
    if not ctx.dry_run:
        journal = ctx.journal
        journal.set_meta('run_id', ctx.run_id or '')
        journal.set_meta('source_db', ctx.db.source)
        journal.set_meta('replacement_db', ctx.db.replacement)
        journal.set_meta('retained_db', ctx.db.retained)
        journal.set_meta('config_hash', config.config_hash())
        journal.set_meta('window_start', iso(window.start))
        journal.set_meta('window_end', iso(window.end))
        journal.set_meta('abort_deadline', iso(deadline))
        journal.set_meta('freeze_mode', config.freeze.mode)
        journal.event('window', 'start=%s end=%s deadline=%s' % (iso(window.start), iso(window.end), iso(deadline)))
    return gate_pending(step, ctx, lines + ['go decision recorded by the operator'])


def _rehearsal_evidence(ctx):
    """True when some run holds a rehearsal whose timings.json records a passed P5 (a dry run writes none)."""
    import json
    runs_dir = ctx.config.runs_dir()
    if not runs_dir.exists():
        return False
    for p in runs_dir.iterdir():
        timings = p / 'rehearsal' / 'timings.json'
        if timings.is_file():
            try:
                if json.loads(timings.read_text(encoding='utf8')).get('p5_status') == 'passed':
                    return True
            except ValueError:
                continue
    return False


def register(step_id, handler):
    """Register the python handler of a step; the step modules call this when they are imported."""
    HANDLERS[get_step(step_id).id] = handler


HANDLERS = {
    'P1': human_pending,
    'P4': decision_pending,
    'O1': o1_start,
    'O2': admin_pending,
    'O3': admin_pending,
    'O9': admin_pending,
    'O10': admin_pending,
    'O11': admin_pending,
    'C3': admin_pending,
    'C4': admin_pending,
}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def all_preconditions(step):
    """Every step that must be passed or skipped before this one, in order."""
    seen = []

    def _walk(s):
        for pre in s.preconditions:
            if pre not in seen:
                _walk(REGISTRY[pre])
                seen.append(pre)
    _walk(step)
    return [s for s in ORDER if s in seen]


def check_run_names(journal, db):
    """Refuse a journal whose recorded names differ from the configured database names.

    O1 writes source_db, replacement_db and retained_db to meta; a later invocation with
    another config, or with defaulted names computed on another day, must not continue on it.
    """
    recorded = (journal.get_meta('source_db'), journal.get_meta('replacement_db') or journal.get_meta('target_db'),
                journal.get_meta('retained_db'))
    for label, have, want in zip(('source', 'replacement', 'retained'), recorded, db.names):
        if have is not None and have != want:
            raise KitError('Journal for %s records %s database %s but the configuration says %s; use the run this '
                           'configuration belongs to, or set the names explicitly in the configuration' % (
                               db.source, label, have, want))


def unmet_preconditions(step, journal):
    """Precondition ids whose journal status is not passed or skipped."""
    board = journal.steps_board()
    return [pre for pre in all_preconditions(step) if board.get(pre, {}).get('status') not in ('passed', 'skipped')]


def outage_range(from_id='O1', to_id='O11'):
    """The ordered outage steps from from_id to to_id inclusive."""
    start, end = get_step(from_id).id, get_step(to_id).id
    if start not in OUTAGE or end not in OUTAGE:
        raise KitError('run covers the outage steps O1 to O11 only; use step ID for P and C steps')
    i, j = OUTAGE.index(start), OUTAGE.index(end)
    if i > j:
        raise KitError('--from %s is after --to %s' % (start, end))
    return OUTAGE[i:j + 1]


def run_step(step, ctx, redo=False):
    """Run one step for one database and return its Outcome. Journals the attempt unless dry run."""
    journal, log = ctx.journal, ctx.log
    header = '[%s] %s (%s; %s) database %s' % (step.id, step.title, step.kind_label, step.identity_label, ctx.db.source)
    if journal.step_status(step.id) == 'passed' and not redo:
        log(header + ': already passed, skipped (use --redo to run it again)')
        return Outcome(SKIPPED, 'already passed')
    missing = unmet_preconditions(step, journal)
    if missing:
        if ctx.dry_run:
            log(header + ': dry run; earlier steps without a passed gate: ' + ', '.join(missing))
        else:
            raise KitError('Cannot run %s for database %s: earlier steps are not passed: %s. Record them with '
                           's2rebuild gate ID --pass --db %s, or start from O1.' % (step.id, ctx.db.source, ', '.join(missing),
                                                                                   ctx.db.source))
    else:
        log(header)
    if not ctx.dry_run:
        journal.step_start(step.id)
    handler = HANDLERS.get(step.id, not_yet)
    try:
        outcome = handler(step, ctx)
    except KitError as exc:
        outcome = Outcome(REFUSED, str(exc), exc.exit_code)
    log('    result: %s%s' % (outcome.status, (': ' + outcome.note) if outcome.note else ''))
    if not ctx.dry_run:
        recorded = {PASSED: 'passed', PENDING: 'pending', FAILED: 'failed', REFUSED: 'pending', STUB: 'pending',
                    INCOMPLETE: 'pending', MISMATCH: 'failed'}[outcome.status]
        journal.step_end(step.id, recorded, outcome.note)
    return outcome


def plan_order(databases, step_ids):
    """(database, step) pairs: O1 per database, O2 to O9 per database in config order, then O10 and O11 per database."""
    plan = []
    if 'O1' in step_ids:
        plan.extend((db, 'O1') for db in databases)
    middle = [s for s in step_ids if s in OUTAGE[1:9]]
    for db in databases:
        plan.extend((db, s) for s in middle)
    tail = [s for s in step_ids if s in ('O10', 'O11')]
    for db in databases:
        plan.extend((db, s) for s in tail)
    return plan


def run_steps(config, databases, journals, log, run_id=None, from_id='O1', to_id='O11', redo=False, dry_run=False,
              yes=False, verbose=False, sessions=None, options=None):
    """Run the outage steps for the selected databases and return the exit code.

    A real run stops at the first pending or failed gate (exit 4) or refusal (exit 1). A dry
    run walks every step, prints the gates that need a person and returns 0.
    """
    step_ids = outage_range(from_id, to_id)
    for db in databases:
        check_run_names(journals[db.source], db)
    pending, failed, stubs = [], [], []
    for db, step_id in plan_order(databases, step_ids):
        step = REGISTRY[step_id]
        ctx = StepContext(config, db, journals[db.source], run_id, log, dry_run, yes, verbose, options, sessions)
        outcome = run_step(step, ctx, redo)
        label = '%s %s' % (step_id, db.source)
        if outcome.status == PENDING:
            pending.append(label)
            if not dry_run:
                log('Stopped at %s: a person must record the gate before the run continues' % step_id)
                return outcome.exit_code
        elif outcome.status == FAILED:
            failed.append(label)
            if not dry_run:
                log('Stopped at %s: gate failed' % step_id)
                return outcome.exit_code
        elif outcome.status == REFUSED:
            log('Stopped at %s: ' % step_id + outcome.note)
            return outcome.exit_code
        elif outcome.status in (INCOMPLETE, MISMATCH) and not dry_run:
            log('Stopped at %s: %s' % (step_id, outcome.note))
            return outcome.exit_code
        elif outcome.status == STUB:
            stubs.append(label)
    if dry_run:
        log('DRY RUN complete: %d step runs walked for %d database(s); gates pending: %s; failed checks: %s; '
            'python steps not run (no implementation or no input yet): %s' % (
                len(step_ids) * len(databases), len(databases), ', '.join(pending) or 'none',
                ', '.join(failed) or 'none', ', '.join(stubs) or 'none'))
        log('No connection was opened and nothing was written.')
        return EXIT_DONE
    log('Steps %s to %s passed for %s' % (step_ids[0], step_ids[-1], ', '.join(db.source for db in databases)))
    return EXIT_DONE


def run_one(config, databases, journals, log, step_id, run_id=None, redo=False, dry_run=False, yes=False, verbose=False,
            options=None, sessions=None):
    """Run a single step (any phase) for the selected databases and return the exit code."""
    step = get_step(step_id)
    for db in databases:
        check_run_names(journals[db.source], db)
    for db in databases:
        ctx = StepContext(config, db, journals[db.source], run_id, log, dry_run, yes, verbose, options, sessions)
        outcome = run_step(step, ctx, redo)
        if outcome.status in (PENDING, FAILED, REFUSED, INCOMPLETE, MISMATCH) and not dry_run:
            return outcome.exit_code
    if dry_run:
        log('No connection was opened and nothing was written.')
    return EXIT_DONE


def gate_step(step_id):
    """The Step a person may gate: admin, human and decision steps only. Raises KitError otherwise."""
    step = get_step(step_id)
    if not step.needs_person:
        raise KitError('%s %s is a python step the kit runs and records itself; gate records admin, human and decision '
                       'steps only (%s)' % (step.id, step.title, ', '.join(s for s in ORDER if REGISTRY[s].needs_person)))
    return step


def record_gate(journals, step_id, passed, note, evidence_path=None):
    """Record a gate decision for every selected database journal. Returns the Step."""
    step = gate_step(step_id)
    if not note or not str(note).strip():
        raise KitError('A gate record needs --note TEXT with what was observed')
    for journal in journals.values():
        journal.record_gate(step.id, passed, note, evidence_path)
        if passed and step.id == 'O11' and not journal.get_meta('released_utc'):
            # the release recorded by hand starts the C1 error window too
            journal.set_meta('released_utc', iso(utc_now()))
    return step


def board(journals):
    """Rows for the status board: one per database and step in registry order."""
    rows = []
    for source, journal in journals.items():
        steps = journal.steps_board()
        for step_id in ORDER:
            row = steps.get(step_id, {})
            rows.append({'database': source, 'step': step_id, 'title': REGISTRY[step_id].title,
                         'status': row.get('status') or 'not started', 'started_utc': row.get('started_utc') or '',
                         'ended_utc': row.get('ended_utc') or '', 'note': row.get('note') or '',
                         'evidence_path': row.get('evidence_path') or ''})
    return rows
