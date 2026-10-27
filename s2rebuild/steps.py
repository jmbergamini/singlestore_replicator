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
from .connect import DryRunSession, KeepAlive
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
          'Kit installed, runner account created, Keychain items set, config written',
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
          'Window and deadline recorded, P2 passed in this run, rehearsal evidence present or waived with --no-rehearsal NOTE, '
          'go recorded', 'steps', 'M1'),
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
          'Final Excel and Markdown written; baseline recalibrated from the journal, or kept with a note when the journal cannot be fitted',
          'report_xlsx', 'M5', ('C1',)),
    _step('C3', 'End state', (ADMIN,), (OWNER,),
          'Exactly two entries across both storages, the live record and the retained record; the live name resolves to the live record; '
          'a write on the retained name refused; the listing recorded', 'ownersql', 'M4', ('C2',)),
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
    # O5: the functions, TVFs and aggregates of the capture, created in the replacement after the tables (routines.py)
    'O5': (('USE', 'CREATE FUNCTION', 'CREATE AGGREGATE'), ('replacement',), {}, {}),
    # O7: the views, then the procedures (a procedure may read a view)
    'O7': (('USE', 'CREATE VIEW', 'CREATE PROCEDURE'), ('replacement',), {}, {}),
    'O9': (('USE', 'CREATE PIPELINE', 'ALTER PIPELINE'), ('replacement',), {}, {}),
    # O10: DROP DATABASE only on the original record (S2b) and the replacement (S4), never the retained copy
    'O10': (('DETACH DATABASE', 'ATTACH DATABASE'), ('source', 'replacement', 'retained'), {'DROP DATABASE': ('source', 'replacement')}, {}),
    'O11': (('START PIPELINE', 'GRANT', 'SET GLOBAL'), ('source',), {}, {}),
    # C3: the End state gate issues reads; the repair block for a switch made without S2b and S4 names the source and the replacement
    'C3': (('DETACH DATABASE', 'ATTACH DATABASE', 'DROP DATABASE'), ('source', 'replacement'), {'DETACH DATABASE': ('source',)}, {}),
    'C4': (('DROP USER',), (), {}, {}),
    # C5 (s2rebuild cleanup): the customer's delete of the retained copy, by its record: DETACH, ATTACH by syntax, DROP on that name only
    'cleanup': (('DETACH DATABASE', 'ATTACH DATABASE', 'DROP DATABASE'), ('retained',), {}, {}),
    'lockout': (('REVOKE', 'GRANT', 'KILL CONNECTION'), ('source',), {}, {}),
    'abandon': (('DETACH DATABASE', 'ATTACH DATABASE', 'DROP DATABASE'), ('source', 'replacement'),
                {'DETACH DATABASE': ('source',), 'ATTACH DATABASE': ('source',), 'DROP DATABASE': ('replacement',)}, {}),
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


def run_flag(run_id):
    """' --run ID' for a command the kit suggests, so the operator's next command lands in this run.

    '' without a run id, and for the id a dry run uses when no run exists (dry-run, reserved by
    common.check_run_id).
    """
    return ' --run %s' % run_id if run_id and run_id != 'dry-run' else ''


def gate_pending(step, ctx, lines=()):
    """Print what a person must do and record, with the --run form of this run, then report the gate as pending."""
    for line in lines:
        ctx.log.plain('    ' + line)
    ctx.log.plain('    gate: ' + step.gate)
    ctx.log.plain('    record with: s2rebuild gate %s --pass --note TEXT%s [--evidence FILE]' % (step.id, run_flag(ctx.run_id)))
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


def o1_readiness(ctx):
    """The O1 readiness lines and the problems a real run refuses on; returns (lines, problems, waiver).

    P2 must be passed in this run's journal (s2rebuild check records it). Rehearsal evidence is P5
    passed in this journal, or a rehearsal of any run whose timings record P5 passed
    (_rehearsal_evidence). Without either, the operator may waive it with --no-rehearsal NOTE
    (ctx.options['no_rehearsal']); waiver is that note, else None. Reads the journal and the
    runs folder only.
    """
    journal = ctx.journal
    lines, problems, waiver = [], [], None
    p2 = journal.step('P2') or {}
    if p2.get('status') == 'passed':
        lines.append('P2 preflight: passed in this run at %s' % (p2.get('ended_utc') or 'an unrecorded time'))
    else:
        state = p2.get('status') or 'not run'
        lines.append('P2 preflight: not passed in this run (%s)' % state)
        problems.append('P2 is not passed in this run (%s); run s2rebuild check%s for this run first' % (state, run_flag(ctx.run_id)))
    note = str(ctx.options.get('no_rehearsal') or '').strip()
    if journal.step_status('P5') == 'passed':
        lines.append('rehearsal evidence: P5 passed in this run')
    elif _rehearsal_evidence(ctx):
        lines.append('rehearsal evidence: a passed rehearsal under %s' % ctx.config.runs_dir())
    elif note:
        waiver = note
        lines.append('rehearsal evidence: none found; waived with --no-rehearsal: %s' % note)
    else:
        lines.append('rehearsal evidence: none found')
        problems.append('no rehearsal evidence (P5 passed in this run, or a passed rehearsal under %s); run s2rebuild rehearse, '
                        'or waive it with --no-rehearsal NOTE' % ctx.config.runs_dir())
    return lines, problems, waiver


def run_mode(ctx):
    """The run mode for O1: --mode (ctx.options['mode']) over run.mode of the configuration (production unless set)."""
    override = str(ctx.options.get('mode') or '').strip()
    if override:
        return override
    run = getattr(ctx.config, 'run', None)
    return (run.get('mode') if run is not None else None) or 'production'


def o1_names(ctx, mode, ask=True):
    """(names, typed, lines): the run's three names for O1, and the roles typed at the prompt.

    In production mode a replacement or retained name the configuration left to the kit's date
    default is asked for at O1 (prompts.ask_names, the default as the suggestion) unless the journal
    already records the names (a rerun, or an earlier O1); test mode applies the defaults silently
    and a dry run never asks. With ask False the defaults come back with a line saying the prompt
    follows (O1 asks only after its checks passed). The answers are the names the journal records,
    and every later command reads them back from the journal (cli.Invocation.apply_recorded_names).
    """
    db = ctx.db
    recorded = ctx.journal.get_meta('replacement_db') or ctx.journal.get_meta('target_db')
    if not db.defaulted or recorded or mode != 'production':
        note = ''
        if db.defaulted and not recorded:
            note = ' (%s by the kit\'s date default: run.mode %s)' % (' and '.join(db.defaulted), mode)
        return db.names, (), ['names: source %s, replacement %s, retained %s%s' % (db.names + (note,))]
    if ctx.dry_run or not ask:
        return db.names, (), ['names: source %s, replacement %s, retained %s (defaults; %s run in production mode asks for the %s name(s))'
                              % (db.names + ('a real' if ctx.dry_run else 'this', ' and '.join(db.defaulted)))]
    from . import prompts
    chosen = prompts.ask_names(db, ctx.config, say=ctx.log.plain)
    names = (db.source, chosen.get('replacement', db.replacement), chosen.get('retained', db.retained))
    return names, tuple(sorted(chosen)), ['names: source %s, replacement %s, retained %s (%s typed at O1)' % (names + (' and '.join(sorted(chosen)),))]


def o1_start(step, ctx):
    """O1: check the window, P2 and the rehearsal evidence, record window and deadline, then wait for the go decision.

    The window is optional. Without one there is no abort deadline: O1 records that, and the copy,
    the switch and the lockout run without a time limit. With one, a past deadline fails the gate.
    A real run then refuses, before any journal write, when P2 is not passed in this run or there
    is no rehearsal evidence and no --no-rehearsal NOTE (o1_readiness); a dry run reports both and
    goes on. A waiver is recorded as a no_rehearsal event and in the O1 note. The names come from
    o1_names: in production mode a name the configuration left to the kit is typed here.
    """
    config = ctx.config
    window = config.window
    deadline = None
    mode = run_mode(ctx)
    names, typed, lines = o1_names(ctx, mode, ask=False)
    if window is None:
        lines.append('window: none configured, so no abort deadline; the copy and the switch run without a time limit')
    else:
        longest, longest_note = 0.0, 'no capture yet, so no longest-chunk allowance; O6 recomputes it from the plan'
        try:
            from . import copy as copy_module
            _, plans, _ = copy_module.load_plans(ctx)
            longest = copy_module.longest_chunk_seconds(plans, config)
            longest_note = 'longest modeled chunk %s' % format_duration(longest)
        except KitError:
            pass
        deadline = window.abort_deadline(longest)
        lines.append('window: %s to %s (%s)' % (iso(window.start), iso(window.end), format_duration(window.duration.total_seconds())))
        lines.append('abort deadline: %s (end minus %d m restart allowance, %d m buffer; %s)' % (
            iso(deadline), window.restart_allowance_minutes, window.buffer_minutes, longest_note))
    lines.append('freeze path: %s' % ('A read-only attachment' if config.freeze.mode == 'readonly' else 'B lockout'))
    now = utc_now()
    if deadline is not None and now >= deadline:
        if ctx.dry_run:
            lines.append('abort deadline is in the past relative to now (%s); a real run stops here' % iso(now))
        else:
            for line in lines:
                ctx.log.plain('    ' + line)
            return Outcome(FAILED, 'abort deadline %s is already past' % iso(deadline), EXIT_GATE)
    readiness, problems, waiver = o1_readiness(ctx)
    lines.extend(readiness)
    from . import workspace
    workspace_lines, workspace_problems, workspace_events = workspace.o1_guard(ctx, workspace.recorded(ctx.journal))
    lines.extend(workspace_lines)
    problems.extend(workspace_problems)
    if problems:
        if ctx.dry_run:
            lines.append('a real run refuses here: ' + '; '.join(problems))
        else:
            for line in lines:
                ctx.log.plain('    ' + line)
            return Outcome(REFUSED, 'O1 refused: ' + '; '.join(problems), EXIT_REFUSED)
    if not ctx.dry_run:
        # the names are asked only now, after every check passed, so a refused O1 never asks
        names, typed, name_lines = o1_names(ctx, mode)
        lines[0] = name_lines[0]
        journal = ctx.journal
        journal.set_meta('run_id', ctx.run_id or '')
        journal.set_meta('source_db', names[0])
        journal.set_meta('replacement_db', names[1])
        journal.set_meta('retained_db', names[2])
        journal.set_meta('config_hash', config.config_hash())
        if typed:
            journal.event('names', 'O1 names typed in production mode: replacement %s, retained %s' % names[1:])
        # empty values clear a window an earlier O1 recorded; resolve_abort_at ignores an empty deadline
        journal.set_meta('window_start', iso(window.start) if window else '')
        journal.set_meta('window_end', iso(window.end) if window else '')
        journal.set_meta('abort_deadline', iso(deadline) if deadline else '')
        journal.set_meta('freeze_mode', config.freeze.mode)
        if window is None:
            journal.event('window', 'none configured; no abort deadline')
        else:
            journal.event('window', 'start=%s end=%s deadline=%s' % (iso(window.start), iso(window.end), iso(deadline)))
        if waiver:
            journal.event('no_rehearsal', 'O1 without rehearsal evidence: %s' % waiver)
        for kind, detail in workspace_events:
            journal.event(kind, detail)
    outcome = gate_pending(step, ctx, lines + ['go decision recorded by the operator'])
    if typed:
        outcome.note += '; names typed at O1: replacement %s, retained %s' % names[1:]
    if waiver:
        outcome.note += '; rehearsal waived with --no-rehearsal: %s' % waiver
    for kind, detail in workspace_events:
        outcome.note += '; ' + detail
    return outcome


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


def config_change(journal, config):
    """(recorded, current) configuration hashes when O1 recorded one and the configuration hashes differently now; else None.

    Before O1 nothing is recorded, so the P4 decisions written into the configuration never count.
    """
    recorded, current = journal.get_meta('config_hash'), config.config_hash()
    return (recorded, current) if recorded is not None and recorded != current else None


def config_change_text(db, recorded, current):
    """The refusal of a configuration that changed after O1, for one database."""
    return ('Journal for %s records configuration hash %s from O1, but the configuration now hashes to %s: it changed after O1. '
            'Put the configuration of this run back, or record the change with --accept-config-change NOTE, which keeps both '
            'hashes and the note in the journal' % (db.source, recorded[:12], current[:12]))


# The steps that refuse freeze.mode lockout with more than one configured database (path_b_single_database).
PATH_B_SINGLE_DATABASE_STEPS = ('O3', 'O10', 'O11')


def path_b_single_database(config):
    """Refuse freeze.mode lockout with more than one configured database: Path B is single database in this pass.

    Each database journal holds its own lockout snapshot and O11 gives it back per database, so
    a global grant restored by the first O11 releases writers on a database still frozen. check
    (P2), run, and step O3, O10 and O11 call this before anything is journaled; owner-sql O3, O10
    and O11, owner-notebook, and gate O3, O10 and O11 --pass call it before a database is chosen,
    a block is written or a gate is recorded; lockout (with or without --hold) calls it before a
    snapshot is taken or a REVOKE is issued. Issues nothing.
    """
    if config.freeze.mode == 'lockout' and len(config.databases) > 1:
        raise KitError('freeze.mode lockout (Path B) is single database in this pass, and the configuration has %d databases (%s); '
                       'configure one database, or use freeze.mode readonly (Path A) for several' % (
                           len(config.databases), ', '.join(db.source for db in config.databases)))


def unmet_preconditions(step, journal):
    """Precondition ids whose journal status is not passed or skipped; every precondition when journal is None (nothing recorded yet)."""
    if journal is None:
        return all_preconditions(step)
    board = journal.steps_board()
    return [pre for pre in all_preconditions(step) if board.get(pre, {}).get('status') not in ('passed', 'skipped')]


def precondition_refusal(step, journal, db, run_id=None):
    """The refusal of a step whose preconditions are not all passed or skipped, with how each passes; None when they are.

    run_step, check (P2) and profile (P3, O4) refuse with this text and gate warns with the same
    list, so every entry point follows one rule. journal None stands for a run with nothing recorded.
    run_id adds the --run form of the run to every command the hint names (missing_hint).
    """
    missing = unmet_preconditions(step, journal)
    if not missing:
        return None
    passed_before = journal is not None and journal.step_status(step.id) == 'passed'
    return 'Cannot run %s for database %s: earlier steps are not passed: %s%s. %s' % (
        step.id, db.source, ', '.join(missing),
        ' (%s passed earlier, but a result counts only while the steps before it are passed)' % step.id if passed_before else '',
        missing_hint(missing, db, run_id=run_id))


# The steps whose results the switch stands on (journal.RECOPY_REOPENS): refused once the switch may have begun.
AFTER_SWITCH_REFUSED = ('O6', 'O7', 'O8')


def outage_range(from_id='O1', to_id='O11'):
    """The ordered outage steps from from_id to to_id inclusive."""
    start, end = get_step(from_id).id, get_step(to_id).id
    if start not in OUTAGE or end not in OUTAGE:
        raise KitError('run covers the outage steps O1 to O11 only; use step ID for P and C steps')
    i, j = OUTAGE.index(start), OUTAGE.index(end)
    if i > j:
        raise KitError('--from %s is after --to %s' % (start, end))
    return OUTAGE[i:j + 1]


def interrupted_note(exc):
    """The step note for an exception that is not a KitError: its type and error number only.

    The exception text is never kept, since a driver message can echo a statement that carried
    an injected secret.
    """
    args = getattr(exc, 'args', ())
    number = args[0] if args and isinstance(args[0], int) else 'unknown'
    return ('interrupted by %s (error %s); the message is not stored; rerun the step, it re-enters from the journal and the live state'
            % (type(exc).__name__, number))


def missing_hint(missing, db=None, restart=True, run_id=None):
    """How to pass the missing steps, earliest first: a step that needs a person is recorded with gate, a python step is run
    (run --from for consecutive outage steps, step for each P or C step); then 'or start from O1' when an outage step is missing.

    The order matters: run --from stops at a step a person records, so that gate comes first.
    db adds the --db flag of its database, run_id the --run form of the run (run_flag);
    restart=False leaves out 'or start from O1'.
    """
    flag = (' --db %s' % db.source if db is not None else '') + run_flag(run_id)
    groups = []
    for step_id in missing:
        step = REGISTRY[step_id]
        kind = 'person' if step.needs_person else ('run' if step_id in OUTAGE else 'step')
        if kind == 'run' and groups and groups[-1][0] == 'run':
            groups[-1][1].append(step_id)
        else:
            groups.append((kind, [step_id]))
    parts = []
    for kind, ids in groups:
        if kind == 'person':
            parts.append('record %s with s2rebuild gate %s --pass --note TEXT%s' % (ids[0], ids[0], flag))
        elif kind == 'run':
            parts.append('run %s with s2rebuild run --from %s%s' % (', '.join(ids), ids[0], flag))
        else:
            parts.append('run %s with s2rebuild step %s%s' % (ids[0], ids[0], flag))
    text = '; then '.join(parts)
    return '%s%s%s.' % (text[:1].upper(), text[1:], ', or start from O1' if restart and any(m in OUTAGE for m in missing) else '')


def _after_switch_refusal(step, ctx, began):
    """The refusal of a step of AFTER_SWITCH_REFUSED once the switch may have begun (began is Journal.switch_began)."""
    return ('Cannot run %s for database %s: the switch may have begun (%s). %s copies or verifies the replacement for the switch; '
            'running it now would change the copy behind the live name or the evidence the rollback proof compares, so nothing was run '
            'or journaled. Finish the switch or roll it back; a new copy is a new run.' % (step.id, ctx.db.source, began, step.id))


def start_keepalive(ctx):
    """Start the keep-alive thread for a live step (workspace.keepalive_minutes above 0, a session factory), else None.

    The thread opens its own runner connection at its first SELECT 1, so a step shorter than the
    interval opens none; the opener is resolved then, and a failure is logged by the thread,
    never raised here. A dry run starts none.
    """
    minutes = ctx.config.workspace.get('keepalive_minutes', 0) or 0
    if ctx.dry_run or ctx.sessions is None or not minutes or getattr(ctx.sessions, 'dry_run', False):
        return None

    def _open():
        return ctx.sessions.runner_connector()()
    return KeepAlive(_open, minutes, ctx.log).start()


def run_step(step, ctx, redo=False):
    """Run one step for one database and return its Outcome. Journals the attempt unless dry run.

    The preconditions are checked before the "already passed" skip, so a passed step whose
    earlier steps were reopened (a re-copy reopens O6 to O8) is refused like any other step,
    never skipped on stale evidence; a dry run only reports them. A step of AFTER_SWITCH_REFUSED
    that would run is refused once the switch may have begun (Journal.switch_began), before the
    journal is touched, so its passed status and the copy behind the live name stay as they are.
    The check is read again in the transaction that starts the step, so a go decision another
    process commits in between refuses it, and a step started first refuses the go.
    A KitError becomes a refusal. Any other exception (a driver error, KeyboardInterrupt, a crash
    stand-in in the tests) records the step as failed with interrupted_note and is raised again,
    so a step is never left running by an exception the kit sees; the command line prints one
    plain line for a driver reset (connect.is_driver_error) and exits 2. The keep-alive thread
    runs from before the handler to the end of the step (start_keepalive).
    """
    journal, log = ctx.journal, ctx.log
    if step.id in PATH_B_SINGLE_DATABASE_STEPS:
        path_b_single_database(ctx.config)
    header = '[%s] %s (%s; %s) database %s' % (step.id, step.title, step.kind_label, step.identity_label, ctx.db.source)
    passed_before = journal.step_status(step.id) == 'passed'
    missing = unmet_preconditions(step, journal)
    if missing:
        if ctx.dry_run:
            log(header + ': dry run; earlier steps without a passed gate: ' + ', '.join(missing))
        else:
            raise KitError(precondition_refusal(step, journal, ctx.db, ctx.run_id))
    if passed_before and not redo:
        log(header + ': already passed, skipped (use --redo to run it again)')
        return Outcome(SKIPPED, 'already passed')
    after_switch = step.id in AFTER_SWITCH_REFUSED
    began = journal.switch_began() if after_switch else None
    if began:
        if ctx.dry_run:
            log(header + ': dry run; a real run refuses this step: the switch may have begun (%s)' % began)
        else:
            raise KitError(_after_switch_refusal(step, ctx, began))
    if not missing and not began:
        log(header)
    if not ctx.dry_run:
        # read again in the transaction that starts the step, so a go decision committed meanwhile refuses it (Journal.record_go)
        began = journal.step_start(step.id, unless=journal.switch_began if after_switch else None)
        if began:
            raise KitError(_after_switch_refusal(step, ctx, began))
    handler = HANDLERS.get(step.id, not_yet)
    keepalive = start_keepalive(ctx)
    try:
        outcome = handler(step, ctx)
    except KitError as exc:
        outcome = Outcome(REFUSED, str(exc), exc.exit_code)
    except BaseException as exc:
        note = interrupted_note(exc)
        log('    result: failed: ' + note)
        if not ctx.dry_run:
            try:
                journal.step_end(step.id, 'failed', note)
            except Exception:  # noqa: BLE001 - the handler's exception is the one the caller must see
                log('    the failed status could not be journaled; status may still show %s running' % step.id)
        raise
    finally:
        if keepalive is not None:
            keepalive.stop()
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

    A real run stops at the first pending or failed gate (exit 4) or refusal (exit 1). A pending
    O1 stops the run only after O1 ran for every selected database, so each journal holds its
    checks, names, configuration hash and window before the one go decision; a refused or failed
    O1 still stops at once. A python step that ends pending (O7 without the owner profile, O8 over
    its tier 3 budget) stops with its own instruction, since gate refuses python steps. A dry run
    walks every step, prints the gates that need a person and returns 0.
    """
    step_ids = outage_range(from_id, to_id)
    path_b_single_database(config)
    for db in databases:
        check_run_names(journals[db.source], db)
    pending, failed, stubs = [], [], []
    go_pending = None
    for db, step_id in plan_order(databases, step_ids):
        if go_pending is not None and step_id != 'O1':
            break
        step = REGISTRY[step_id]
        ctx = StepContext(config, db, journals[db.source], run_id, log, dry_run, yes, verbose, options, sessions)
        outcome = run_step(step, ctx, redo)
        label = '%s %s' % (step_id, db.source)
        if outcome.status == PENDING:
            pending.append(label)
            if not dry_run and step_id == 'O1':
                go_pending = outcome.exit_code  # O1 runs for every selected database before the run stops for the go
            elif not dry_run and not step.needs_person:
                # a python step (O7, O8) the kit records itself: gate refuses it, so the step's own instruction is the hint
                log('Stopped at %s: %s' % (step_id, outcome.note))
                return outcome.exit_code
            elif not dry_run:
                log('Stopped at %s: a person must record the gate before the run continues; then s2rebuild run%s continues it' % (
                    step_id, run_flag(run_id)))
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
    if go_pending is not None:
        log('Stopped at O1: a person must record the gate before the run continues; then s2rebuild run%s continues it' % run_flag(run_id))
        return go_pending
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
    """Run a single step (any phase) for the selected databases and return the exit code.

    A pending O1 does not stop it before O1 ran for every selected database, as in run_steps.
    """
    step = get_step(step_id)
    for db in databases:
        check_run_names(journals[db.source], db)
    go_pending = None
    for db in databases:
        ctx = StepContext(config, db, journals[db.source], run_id, log, dry_run, yes, verbose, options, sessions)
        outcome = run_step(step, ctx, redo)
        if step.id == 'O1' and outcome.status == PENDING and not dry_run:
            go_pending = outcome.exit_code
        elif outcome.status in (PENDING, FAILED, REFUSED, INCOMPLETE, MISMATCH) and not dry_run:
            return outcome.exit_code
    if go_pending is not None:
        return go_pending
    if dry_run:
        log('No connection was opened and nothing was written.')
    return EXIT_DONE


# The python steps a person may still gate, with gate --override NOTE only: the close-out steps C3 and C4 need them passed.
OVERRIDE_STEPS = ('C1', 'C2')


def gate_step(step_id, override=None):
    """The Step a person may gate: admin, human and decision steps, and C1 or C2 with an override note. Raises KitError otherwise."""
    step = get_step(step_id)
    overriding = bool(str(override or '').strip())
    if overriding and step.id in OVERRIDE_STEPS:
        return step
    if overriding:
        raise KitError('--override records a gate for %s only, not for %s' % (' and '.join(OVERRIDE_STEPS), step.id))
    if not step.needs_person:
        raise KitError('%s %s is a python step the kit runs and records itself; gate records admin, human and decision '
                       'steps only (%s)%s' % (step.id, step.title, ', '.join(s for s in ORDER if REGISTRY[s].needs_person),
                                             '; a %s the kit cannot pass is recorded with --override NOTE' % step.id
                                             if step.id in OVERRIDE_STEPS else ''))
    return step


def record_gate(journals, step_id, passed, note, evidence_path=None, override=None):
    """Record a gate decision for every selected database journal. Returns the Step.

    A gate for C1 or C2 needs override, the reason a person records it instead of the kit; it is
    journaled as a gate_override event just before the gate.
    """
    step = gate_step(step_id, override)
    if not note or not str(note).strip():
        raise KitError('A gate record needs --note TEXT with what was observed')
    for journal in journals.values():
        if step.id in OVERRIDE_STEPS:
            journal.event('gate_override', '%s %s: %s' % (step.id, 'pass' if passed else 'fail', str(override).strip()))
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
