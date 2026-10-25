"""s2rebuild cli: the command line, exit codes and dispatch to the step modules.

Author: John Bergamini

Section 8 of the architecture: every command and global flag is declared here so that
`s2rebuild --help` and `s2rebuild docs-flags` describe the whole kit. Commands whose module
lands in a later milestone refuse with exit 1 and name the milestone. Exit codes: 0 done,
1 refused or configuration error, 2 incomplete, 3 verification mismatch, 4 gate pending or
failed. Statements issued by this module: none.
"""
import argparse
import datetime as dt
import json
import os
import sqlite3
import sys
import traceback
from pathlib import Path

from . import __title__, __version__, bundle, copy as copy_module, estimate, freeze, grants, inventory, lockout, notebook, ownersql, pipelines, postcopy, rehearse, report_md, report_xlsx, steps, switch, watch
from .common import EXIT_DONE, EXIT_GATE, EXIT_MISMATCH, EXIT_REFUSED, KitError, check_run_id, parse_utc, run_date, unique_run_id, utc_now
from .sqlutil import secret_forms, sql_literal
from .config import load_config
from .connect import Log, Redactor, Secrets, Session, SessionFactory
from .journal import Journal, read_only_uri
from .sqlutil import read_guard

ENV_RUN = 'S2REBUILD_RUN'
DRY_RUN_ID = 'dry-run'

# command: (step label, help text, where it arrives; None when implemented here)
COMMANDS = [
    ('check', 'P1, P2', 'Connect as runner (and owner if configured), check privileges, engine version, source layout and '
                        'STORAGE_ID, free target names, compute cores times 8 at or above the partition count', None),
    ('account-sql', 'P1, C4', 'Print the CREATE USER and GRANT block for the runner account; --execute runs it with the '
                              'owner profile; --drop prints or executes the DROP USER block and the zero-privilege check', None),
    ('profile', 'P3', 'Inventory, sizes, estimate and the profile workbook for the configured databases', None),
    ('plan', 'P4', 'Print the plan sheet: window, abort deadline, freeze path, step owners, estimate with bands', None),
    ('rehearse', 'P5', 'Create throwaway databases, run O4 to O10 on them with a kill test, verify, switch, clean up', None),
    ('run', 'O1 to O11', 'Run the outage steps in order and stop at the first failed gate', None),
    ('step', 'any', 'Run one step by id', None),
    ('gate', 'admin, human', 'Record a gate result for a step the kit did not execute itself', None),
    ('status', 'any', 'Board of steps per database: status, start, end, evidence path', None),
    ('watch', 'O3, C1', 'Writer watch: non-runner sessions with statement kind every --interval seconds and count deltas '
                        'on the largest --tables tables every 60 seconds', None),
    ('quiet-proof', 'O3 Path B', 'Pipelines none Running, no non-idle session from another account, counts and newest '
                                 'timestamps stable across two samples; writes freeze_time to the journal', None),
    ('lockout', 'O3 Path B', 'Snapshot grants, revoke write privileges from every write-capable account, hold connections, '
                             'kill loop until --until, the stop file or Ctrl-C; owner profile required', None),
    ('owner-sql', 'O2, O3, O9, O10, O11, C3, C4', 'Print the ordered SQL block for an admin step with its precondition '
                                                   'queries and expected results; also written under owner-sql/', None),
    ('owner-notebook', 'any admin step', 'Generate owner-steps.ipynb for the run with one cell per admin step', None),
    ('mark-recopy', 'O6, O7', 'Mark a table, or one bucket of it, for re-copy; the next copy run deletes and re-inserts it', None),
    ('pause', 'O6', 'Create the stop file: running chunks finish, no new chunk starts', None),
    ('resume', 'O6', 'Remove the stop file', None),
    ('report', 'C2', 'Final Excel and Markdown for a run', None),
    ('calibrate', 'C2', 'Fit the throughput model from a journal and write customer/baseline.json', None),
    ('bundle', 'packaging', 'Write MANIFEST.json, check the denylist, zip the static tree to dist/; --verify ZIP checks a zip', None),
    ('docs-flags', 'docs', 'Print the flag table as Markdown for docs/CONFIG.md', None),
]
COMMAND_NAMES = [c[0] for c in COMMANDS]


class Parser(argparse.ArgumentParser):
    """argparse with usage errors raised as KitError so the exit code is 1, not 2."""

    def error(self, message):
        """Raise KitError instead of exiting with argparse's code 2."""
        raise KitError('%s (see %s --help)' % (message, self.prog))


def _global_flags(parser, suppress, collected):
    default = argparse.SUPPRESS if suppress else None
    flag_default = argparse.SUPPRESS if suppress else False

    class Collect(argparse.Action):
        """Append across the main parser and the subparser, so --db works before and after the command."""

        def __call__(self, parser_, namespace, values, option_string=None):
            collected.setdefault(self.dest, []).append(values)
            setattr(namespace, self.dest, list(collected[self.dest]))

    parser.add_argument('--config', metavar='PATH', default=default,
                        help='configuration file (default: $S2REBUILD_CONFIG or customer/config.yaml)')
    parser.add_argument('--run', metavar='ID', default=default,
                        help='run id (default: $S2REBUILD_RUN, else the most recent run, or a new one for run --from O1)')
    parser.add_argument('--db', metavar='NAME', action=Collect, default=default,
                        help='restrict to one configured source database; repeatable')
    parser.add_argument('--dry-run', action='store_true', default=flag_default,
                        help='print every statement, open no connection where the step allows it, write nothing')
    parser.add_argument('--yes', action='store_true', default=flag_default, help='no interactive confirmation')
    parser.add_argument('--verbose', action='store_true', default=flag_default, help='more detail in the log')


def build_parser():
    """The complete command line of section 8."""
    collected = {}
    parser = Parser(prog='s2rebuild', description='%s %s. Rebuild a SingleStore Helios database into a new partition '
                    'layout and switch it over inside one outage window. Steps P1 to P5 prepare, O1 to O11 run inside '
                    'the window, C1 to C4 close out. Exit codes: 0 done, 1 refused or configuration error, 2 incomplete '
                    '(rerun with the same run id to resume), 3 verification mismatch, 4 gate pending or failed (a person '
                    'is needed).' % (__title__, __version__))
    parser.add_argument('--version', action='version', version='%s %s' % (__title__, __version__))
    _global_flags(parser, suppress=False, collected=collected)
    shared = Parser(add_help=False)
    _global_flags(shared, suppress=True, collected=collected)
    sub = parser.add_subparsers(dest='command', metavar='COMMAND', required=True)
    p = {}
    for name, step_label, help_text, _ in COMMANDS:
        p[name] = sub.add_parser(name, help='%s: %s' % (step_label, help_text), description='%s. Step: %s.' % (help_text, step_label),
                                 parents=[shared])
    p['account-sql'].add_argument('--execute', action='store_true', help='run the block with the owner profile')
    p['account-sql'].add_argument('--drop', action='store_true', help='the DROP USER block and the zero-privilege check (C4)')
    p['profile'].add_argument('--exact-counts', action='store_true', help='run COUNT(*) per table instead of estimates')
    p['profile'].add_argument('--baseline', metavar='FILE', help='throughput baseline file to use instead of the configured one')
    p['profile'].add_argument('--all-databases', action='store_true',
                              help='add a system sheet with every database in the workspace for ranking and sequencing')
    p['profile'].add_argument('--inventory', metavar='DIR', help='read a saved capture instead of connecting (required for --dry-run without a prior capture)')
    p['profile'].add_argument('--out', metavar='FILE', help='workbook path inside the customer folder (default: the run reports folder)')
    p['profile'].add_argument('--step', choices=['P3', 'O4'], default='P3', help='P3 profile before the window (default) or O4 freeze capture with drift')
    p['plan'].add_argument('--inventory', metavar='DIR', help='read a saved capture instead of the run inventory')
    p['plan'].add_argument('--baseline', metavar='FILE', help='throughput baseline file to use instead of the configured one')
    p['rehearse'].add_argument('--keep', action='store_true', help='leave the throwaway databases for inspection')
    p['rehearse'].add_argument('--prefix', metavar='NAME', help='rehearsal database prefix, starting with s2rebuild_rehearsal_ (default s2rebuild_rehearsal_<run id>)')
    p['rehearse'].add_argument('--rows', metavar='N', type=int, default=1000000, help='rows in the keyed and keyless tables (default 1000000)')
    p['rehearse'].add_argument('--cleanup', action='store_true', help='drop the rehearsal databases of the prefix and exit (asks first unless --yes)')
    p['run'].add_argument('--from', dest='from_id', metavar='ID', default='O1', help='first step to run (default O1)')
    p['run'].add_argument('--to', dest='to_id', metavar='ID', default='O11', help='last step to run (default O11)')
    p['run'].add_argument('--redo', action='store_true', help='run steps that already passed')
    p['run'].add_argument('--retry-failed', action='store_true', help='O6: also rerun chunks parked after copy.max_attempts failures')
    p['step'].add_argument('step_id', metavar='ID', help='step id such as O6')
    p['step'].add_argument('--redo', action='store_true', help='run the step even when it already passed')
    p['step'].add_argument('--fresh-table', metavar='TABLE', help='O6 only: load one table with the fresh-table fallback')
    p['step'].add_argument('--retry-failed', action='store_true', help='O6: also rerun chunks parked after copy.max_attempts failures')
    p['gate'].add_argument('step_id', metavar='ID', help='step id such as O3')
    decision = p['gate'].add_mutually_exclusive_group(required=True)
    decision.add_argument('--pass', dest='passed', action='store_true', help='the gate passed')
    decision.add_argument('--fail', dest='failed', action='store_true', help='the gate failed')
    p['gate'].add_argument('--note', metavar='TEXT', required=True, help='what was observed')
    p['gate'].add_argument('--evidence', metavar='FILE', help='file with the evidence (notebook output, query result)')
    p['watch'].add_argument('--interval', metavar='N', type=int, default=5, help='seconds between process list samples (default 5)')
    p['watch'].add_argument('--tables', metavar='N', type=int, default=10, help='largest tables to count every 60 seconds (default 10)')
    p['watch'].add_argument('--until', metavar='TIME', help='stop time, ISO 8601 UTC (default: until Ctrl-C)')
    p['lockout'].add_argument('--until', metavar='TIME', help='stop time, ISO 8601 UTC (default: the abort deadline)')
    p['lockout'].add_argument('--kill-only', action='store_true', help='keep killing sessions without revoking or snapshotting grants')
    p['owner-sql'].add_argument('--rollback', action='store_true', help='O10 only: print the rollback block R1 to R3 instead of the forward block')
    p['owner-sql'].add_argument('step_id', metavar='ID', help='admin step id: %s' % ', '.join(steps.OWNER_SQL_STEPS))
    p['mark-recopy'].add_argument('table', metavar='TABLE', help='table name as recorded in the journal')
    p['mark-recopy'].add_argument('--bucket', metavar='N', type=int, help='one bucket instead of every bucket of the table')
    p['calibrate'].add_argument('--journal', metavar='FILE', help='journal to fit (default: the run journal)')
    p['calibrate'].add_argument('--until', metavar='TIME', help='use chunks that ended before this ISO 8601 UTC time')
    p['calibrate'].add_argument('--size-gib', metavar='N', type=float, help='total primary files GiB copied, for the GiB per hour figure')
    p['calibrate'].add_argument('--all-attempts', action='store_true', help='include re-copied chunks (default: first-pass chunks only)')
    p['bundle'].add_argument('--verify', metavar='ZIP', help='unzip to a temporary folder and compare hashes with the manifest')
    p['bundle'].add_argument('--include-customer', action='store_true', help='add customer/ to the zip')
    return parser


# ---------------------------------------------------------------------------
# Shared context
# ---------------------------------------------------------------------------
RUN_NEW, RUN_EXISTING, RUN_NEW_IF_NONE = 'new', 'existing', 'new_if_none'


def newest_first(run_dirs):
    """Run directories ordered by the newest change inside them (directory or journal file), newest first."""
    def _changed(path):
        times = [path.stat().st_mtime]
        times.extend(child.stat().st_mtime for child in path.rglob('journal.sqlite'))
        return max(times)
    return sorted(run_dirs, key=_changed, reverse=True)


class Invocation:
    """Config, databases, run id, journals and log for one command.

    run_mode says how the run id is chosen when none is given: RUN_NEW starts a run (run --from
    O1), RUN_EXISTING needs one (gate, status, run --from a later step), RUN_NEW_IF_NONE starts
    one only when no run exists yet (step and profile, for the preparation steps). A given id
    must name an existing run in RUN_EXISTING mode; the other modes start the named run. A dry
    run never creates a run directory.
    """

    def __init__(self, args, env, out, state, run_mode=RUN_EXISTING, dry_run=None):
        self.args = args
        self.env = env
        self.out = out
        self.state = state
        self.dry_run = args.dry_run if dry_run is None else dry_run
        self.config = load_config(args.config, env=env)
        self.run_id = self.resolve_run_id(run_mode)
        state['run_id'] = self.run_id
        date = run_date(self.run_id)
        if date is not None and date != utc_now().date():
            self.config = load_config(args.config, run_date=date, env=env)
        self.databases = self.select_databases()
        self.journals = {}
        self.log = None
        self.sessions = None

    def existing_runs(self):
        """Names of the runs under runs_dir, most recent first."""
        runs_dir = self.config.runs_dir()
        if not runs_dir.is_dir():
            return []
        return [p.name for p in newest_first([p for p in runs_dir.iterdir() if p.is_dir()])]

    def resolve_run_id(self, mode):
        """Apply the run id rules of section 8 (see the class docstring)."""
        runs_dir = self.config.runs_dir()
        names = self.existing_runs()
        given = self.args.run or self.env.get(ENV_RUN)
        if given:
            check_run_id(given)
            if given not in names and mode == RUN_EXISTING and not self.dry_run:
                raise KitError('Run %s does not exist under %s%s' % (
                    given, runs_dir, ('; existing runs: ' + ', '.join(names)) if names else '; no run exists yet'))
            return given
        if self.dry_run:
            return names[0] if names else DRY_RUN_ID
        if mode == RUN_NEW or (mode == RUN_NEW_IF_NONE and not names):
            return unique_run_id(names)
        if names:
            return names[0]
        raise KitError('No run exists under %s yet; start one with: s2rebuild run' % runs_dir)

    def select_databases(self):
        """The databases this command works on: --db, the only one, all with --yes or dry run, or a prompt."""
        config = self.config
        wanted = self.args.db
        if wanted:
            for name in wanted:
                config.database(name)
            return [db for db in config.databases if db.source in wanted]
        if len(config.databases) == 1 or self.args.yes or self.dry_run:
            return list(config.databases)
        names = config.database_names()
        if not sys.stdin.isatty():
            raise KitError('Several databases are configured (%s); pass --db NAME or --yes for all' % ', '.join(names))
        print('Databases configured: %s' % ', '.join(names), file=self.out)
        answer = input('all, or which? ').strip()
        if answer in ('', 'all'):
            return list(config.databases)
        chosen = [config.database(a.strip()).source for a in answer.replace(',', ' ').split()]
        return [db for db in config.databases if db.source in chosen]

    def journal_path(self, db):
        """customer/runs/<run>/<source>/journal.sqlite for a database."""
        return self.config.runs_dir() / self.run_id / db.source / 'journal.sqlite'

    def open_journals(self, read_only=False):
        """Open one journal per selected database: an in-memory copy for a dry run, read-only for status."""
        for db in self.databases:
            path = self.journal_path(db)
            if self.dry_run:
                self.journals[db.source] = Journal.dry_run_copy(path)
            elif read_only:
                if not path.exists():
                    raise KitError('No journal for run %s and database %s at %s' % (self.run_id, db.source, path))
                self.journals[db.source] = Journal(path, read_only=True)
            else:
                self.journals[db.source] = Journal(path)
        return self.journals

    def open_log(self, command):
        """The Log for this command: redacted (host, users, CA path) and written to logs/ unless dry run."""
        path = None
        if not self.dry_run:
            stamp = utc_now().strftime('%Y%m%dT%H%M%SZ')
            path = self.config.runs_dir() / self.run_id / 'logs' / ('%s-%s.log' % (command, stamp))
        values = [self.config.workspace.host, str(self.config.resolve(self.config.workspace.ca_file))]
        users = [self.config.runner.user] + ([self.config.owner.user] if self.config.owner is not None else [])
        redactor = Redactor(values, soft=users)
        self.state['redactor'] = redactor
        self.log = Log(redactor=redactor, stream=self.out, path=path, verbose=self.args.verbose)
        return self.log

    def open_sessions(self):
        """The SessionFactory for this command (recording sessions in a dry run)."""
        self.sessions = SessionFactory(self.config, self.env, self.log, self.dry_run)
        self.state['sessions'] = self.sessions
        return self.sessions

    def plans_for(self, db):
        """The capture (O4, else P3) and its plans for a database."""
        ctx = steps.StepContext(self.config, db, self.journals.get(db.source), self.run_id, self.log, self.dry_run)
        return copy_module.load_plans(ctx)

    def close(self):
        """Close every session, journal and the log."""
        if self.sessions is not None:
            self.sessions.close()
        for journal in self.journals.values():
            journal.close()
        if self.log is not None:
            self.log.close()


def _header(inv, command):
    return '%s %s: %s, run %s under %s, %s, %s' % (
        __title__, __version__, command, inv.run_id, inv.config.runs_dir(), 'DRY RUN' if inv.dry_run else 'live journal',
        ', '.join(db.source for db in inv.databases))


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def cmd_run(args, env, out, state):
    """run: O1 to O11 in order for the selected databases."""
    step_ids = steps.outage_range(args.from_id, args.to_id)
    inv = Invocation(args, env, out, state, run_mode=RUN_NEW if step_ids[0] == 'O1' else RUN_EXISTING)
    try:
        inv.open_journals()
        log = inv.open_log('run')
        log(_header(inv, 'run'))
        sessions = inv.open_sessions()
        return steps.run_steps(inv.config, inv.databases, inv.journals, log, run_id=inv.run_id, from_id=args.from_id,
                               to_id=args.to_id, redo=args.redo, dry_run=inv.dry_run, yes=args.yes, verbose=args.verbose,
                               sessions=sessions, options={'retry_failed': args.retry_failed, 'env': env})
    finally:
        inv.close()


def cmd_step(args, env, out, state):
    """step ID: one step of any phase for the selected databases."""
    step = steps.get_step(args.step_id)
    inv = Invocation(args, env, out, state, run_mode=RUN_NEW_IF_NONE if step.phase == 'P' else RUN_EXISTING)
    try:
        inv.open_journals()
        log = inv.open_log('step-%s' % step.id)
        log(_header(inv, 'step ' + step.id))
        options = {'retry_failed': args.retry_failed, 'env': env}
        if args.fresh_table:
            options['fresh_table'] = args.fresh_table
        sessions = inv.open_sessions()
        return steps.run_one(inv.config, inv.databases, inv.journals, log, step.id, run_id=inv.run_id, redo=args.redo,
                             dry_run=inv.dry_run, yes=args.yes, verbose=args.verbose, options=options, sessions=sessions)
    finally:
        inv.close()


def cmd_gate(args, env, out, state):
    """gate ID --pass or --fail: record a person's decision; with --dry-run only print what would be recorded."""
    step = steps.gate_step(args.step_id)
    evidence = None
    if args.evidence:
        evidence_path = Path(args.evidence)
        if not evidence_path.is_file():
            raise KitError('Evidence file not found: %s' % evidence_path)
        evidence = str(evidence_path.resolve())
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING)
    try:
        inv.open_journals()
        verdict = 'PASS' if args.passed else 'FAIL'
        targets = ', '.join(inv.journals)
        if inv.dry_run:
            print('DRY RUN: would record gate %s as %s for run %s, database(s) %s: %s%s' % (
                step.id, verdict, inv.run_id, targets, args.note, (' (evidence %s)' % evidence) if evidence else ''), file=out)
            print('Nothing was written.', file=out)
            return EXIT_DONE
        result = _evidence_result(evidence, step.id, inv.run_id, bool(args.passed))
        log = inv.open_log('gate-%s' % step.id)
        if result is not None:
            for db in inv.databases:
                _import_evidence(_step_context(inv, db, log), step.id, result, log)
        steps.record_gate(inv.journals, step.id, bool(args.passed), args.note, evidence)
        log('gate %s recorded as %s for run %s, database(s) %s: %s%s' % (
            step.id, verdict, inv.run_id, targets, args.note, (' (evidence %s)' % evidence) if evidence else ''))
        return EXIT_DONE
    finally:
        inv.close()


def _evidence_result(evidence, step_id, run_id, passed):
    """The notebook result JSON in an evidence file, checked against the gate; None for evidence that is not a result."""
    if not evidence:
        return None
    try:
        data = json.loads(Path(evidence).read_text(encoding='utf8'))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(data, dict) or 'step' not in data:
        return None
    if str(data['step']).upper() != step_id:
        raise KitError('The evidence is the result of step %s, not %s' % (data['step'], step_id))
    if data.get('run_id') and str(data['run_id']) != str(run_id):
        raise KitError('The evidence belongs to run %s, not %s' % (data['run_id'], run_id))
    status = str(data.get('status', 'passed'))
    if passed and 'grant_snapshot' not in data and status != 'passed':
        raise KitError('The evidence reports status "%s"; a gate cannot pass on it' % status)
    return data


def _import_evidence(ctx, step_id, result, log):
    """Bring what a notebook result carries into the journal: the O2 pipeline capture, the lockout snapshot, observed attachments."""
    if step_id == 'O2' and result.get('capture'):
        directory = pipelines.import_capture(ctx, result['capture'])
        log('O2 pipeline capture imported from the notebook result into %s' % directory)
    if step_id == 'O3' and result.get('grant_snapshot'):
        written = grants.record_snapshot(ctx.journal, result['grant_snapshot'])
        log('grants snapshot from the notebook lockout recorded: %d new account row(s); existing rows are kept' % written)
    for att in result.get('attachments') or []:
        row = att.get('row') or {}
        if att.get('role') in ('source', 'replacement', 'retained', 'live') and row.get('STORAGE_ID'):
            ctx.journal.record_attachment(att['role'], str(row.get('DATABASE_NAME')), row.get('STORAGE_ID'), row.get('COMPUTE_ID'), row.get('ATTACH_TYPE'))
            log('attachment %s recorded from the notebook result: %s on STORAGE_ID %s' % (att['role'], row.get('DATABASE_NAME'), row.get('STORAGE_ID')))


def cmd_status(args, env, out, state):
    """status: the board of steps per database for a run, read-only."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING, dry_run=False)
    try:
        inv.open_journals(read_only=True)
        rows = steps.board(inv.journals)
        print('%s %s: run %s under %s' % (__title__, __version__, inv.run_id, inv.config.runs_dir()), file=out)
        current = None
        for row in rows:
            if row['database'] != current:
                current = row['database']
                print('database %s' % current, file=out)
                print('  %-4s %-26s %-12s %-20s %-20s %s' % ('step', 'title', 'status', 'started', 'ended', 'note / evidence'), file=out)
            tail = row['note']
            if row['evidence_path']:
                tail = (tail + ' ' if tail else '') + '[evidence: %s]' % row['evidence_path']
            print('  %-4s %-26s %-12s %-20s %-20s %s' % (row['step'], row['title'][:26], row['status'], row['started_utc'],
                                                        row['ended_utc'], tail), file=out)
        return EXIT_DONE
    finally:
        inv.close()


def _baseline_for(config, override=None):
    """The baseline: --baseline FILE, else the configured file when present, else the shipped default."""
    if override:
        path = Path(override)
        if not path.is_file():
            raise KitError('Baseline file not found: %s' % path)
        return estimate.load_baseline(path)
    configured = config.estimate.baseline
    return estimate.load_baseline(config.resolve(configured) if configured else None)


def _rehearsal_note(config):
    runs_dir = config.runs_dir()
    if not runs_dir.is_dir():
        return None
    found = sorted(p.parent.name for p in runs_dir.glob('*/rehearsal') if p.is_dir())
    return ('run %s has rehearsal evidence' % found[-1]) if found else None


def _open_sessions(inv, env):
    """Runner (read guard) and owner sessions for a live capture. Returns (runner, owner or None)."""
    runner = Secrets.from_config(inv.config, 'runner', env=env)
    inv.state['redactor'].extend(runner.redactor.values, soft=runner.redactor.soft)
    inv.log.redactor.extend(runner.redactor.values, soft=runner.redactor.soft)
    sessions = [Session(runner.connect(), read_guard('runner P3'), inv.log)]
    if inv.config.owner is not None:
        owner = Secrets.from_config(inv.config, 'owner', env=env)
        inv.state['redactor'].extend(owner.redactor.values, soft=owner.redactor.soft)
        inv.log.redactor.extend(owner.redactor.values, soft=owner.redactor.soft)
        sessions.append(Session(owner.connect(), read_guard('owner P3'), inv.log))
    else:
        sessions.append(None)
    return sessions


def cmd_profile(args, env, out, state):
    """profile: capture (or load) the inventory, build the plan and estimate, write the workbook, record P3 or O4."""
    step_id = args.step
    inv = Invocation(args, env, out, state, run_mode=RUN_NEW_IF_NONE)
    try:
        inv.open_journals()
        log = inv.open_log('profile-' + step_id)
        log(_header(inv, 'profile ' + step_id))
        baseline = _baseline_for(inv.config, args.baseline)
        if inv.dry_run and inv.run_id == DRY_RUN_ID and not args.out:
            raise KitError('No run exists yet; a dry run needs --out FILE for the workbook')
        exit_code = EXIT_DONE
        for db in inv.databases:
            journal = inv.journals[db.source]
            steps.check_run_names(journal, db)
            target_dir = inventory.inventory_dir(inv.config, inv.run_id, db, step_id)
            runner_session = owner_session = None
            if args.inventory:
                data = inventory.load(args.inventory)
                log('%s: using the saved capture at %s' % (db.source, args.inventory))
            elif inv.dry_run:
                data = inventory.load(target_dir)
                log('%s: dry run, using the capture at %s' % (db.source, target_dir))
            else:
                runner_session, owner_session = _open_sessions(inv, env)
                data = inventory.capture(runner_session, db, inv.config, owner_session, exact_counts=args.exact_counts, log=log, step=step_id)
            try:
                plans = inventory.plan_tables(data, inv.config, db)
                if not (args.inventory and inv.dry_run):
                    inventory.write(data, target_dir, plans)
                    log('%s: inventory written to %s (%d tables, %d buckets)' % (db.source, target_dir, len(plans), sum(p.n_buckets for p in plans)))
                pipeline_count = len(data['pipelines']['items']) if data.get('pipelines', {}).get('captured') else 0
                est = estimate.estimate(plans, inv.config, baseline, pipeline_count)
                fit = estimate.window_fit(est, inv.config.window) if inv.config.window is not None else None
                for line in estimate.fit_lines(est, fit):
                    log.plain('    ' + line)
                if step_id == 'O4':
                    before_dir = inventory.inventory_dir(inv.config, inv.run_id, db, 'P3')
                    if (before_dir / inventory.INVENTORY_FILE).is_file():
                        report = inventory.drift(inventory.load(before_dir), data)
                        for line in inventory.drift_lines(report):
                            log('    ' + line)
                        inventory.write_private(target_dir / 'drift.json', json.dumps(report, indent=1))
                    else:
                        log('    drift: no P3 inventory to compare against')
                    inventory.write_private(target_dir / 'plan.json', json.dumps([p.describe() for p in plans], indent=1))
                if args.out:
                    out_path = _inside_customer(inv.config, args.out)
                else:
                    name = ('profile' if step_id == 'P3' else 'freeze-capture') + ('-dry-run' if inv.dry_run else '') + '.xlsx'
                    out_path = inv.config.runs_dir() / inv.run_id / db.source / 'reports' / name
                report_xlsx.write_profile_workbook(out_path, data, plans, est, fit, inv.config, db, inv.run_id, baseline,
                                                   steps_board=journal.steps_board(), env=env)
                if args.all_databases:
                    if runner_session is None:
                        log('    --all-databases needs a live connection; skipped in this mode')
                    else:
                        rows = inventory.system_profile(runner_session, inv.config, baseline, estimate)
                        report_xlsx.write_system_sheet(out_path, rows)
                        log('    system sheet: %d databases' % len(rows))
                log('%s: workbook written to %s (Summary plus %d table tabs)' % (db.source, out_path, len(plans)))
                if fit is not None and not fit.fits:
                    exit_code = EXIT_GATE
                if not inv.dry_run:
                    journal.step_start(step_id)
                    journal.step_end(step_id, 'passed', '%s captured %d tables; workbook %s' % (step_id, len(plans), out_path), str(out_path))
                    journal.set_meta('inventory_dir_' + step_id, str(target_dir))
                    if data.get('attachment'):
                        journal.record_attachment('source', db.source, data['attachment'].get('STORAGE_ID'),
                                                  data['attachment'].get('COMPUTE_ID'), data['attachment'].get('ATTACH_TYPE'))
            finally:
                for s in (runner_session, owner_session):
                    if s is not None:
                        s.close()
        if inv.dry_run:
            log('No connection was opened; the workbook was written from the saved capture.')
        return exit_code
    finally:
        inv.close()


def cmd_plan(args, env, out, state):
    """plan: print the P4 plan sheet from the run's P3 inventory (or --inventory DIR)."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING, dry_run=bool(args.inventory) or args.dry_run)
    try:
        baseline = _baseline_for(inv.config, args.baseline)
        for db in inv.databases:
            source = Path(args.inventory) if args.inventory else inventory.inventory_dir(inv.config, inv.run_id, db, 'P3')
            data = inventory.load(source)
            plans = inventory.plan_tables(data, inv.config, db)
            pipeline_count = len(data['pipelines']['items']) if data.get('pipelines', {}).get('captured') else 0
            est = estimate.estimate(plans, inv.config, baseline, pipeline_count)
            fit = estimate.window_fit(est, inv.config.window) if inv.config.window is not None else None
            for line in estimate.plan_sheet(inv.config, db, inv.run_id, data, est, fit, _rehearsal_note(inv.config), env):
                print(line, file=out)
            print('', file=out)
        return EXIT_DONE
    finally:
        inv.close()


def cmd_calibrate(args, env, out, state):
    """calibrate: fit the throughput constants from a journal and write the baseline file."""
    if args.journal:
        config = load_config(args.config, env=env)
        journal_path = Path(args.journal)
        run_id = journal_path.parent.parent.name if journal_path.parent.parent.name else None
    else:
        inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING, dry_run=False)
        try:
            if len(inv.databases) != 1:
                raise KitError('calibrate works on one journal; pass --db NAME or --journal FILE')
            config = inv.config
            journal_path = inv.journal_path(inv.databases[0])
            run_id = inv.run_id
        finally:
            inv.close()
    cal = estimate.calibrate(journal_path, until=args.until, first_pass_only=not args.all_attempts, workers=config.copy.workers,
                             compute_size=config.workspace.compute_size, run_id=run_id)
    size = args.size_gib
    if size is None:
        try:
            conn = sqlite3.connect(read_only_uri(journal_path), uri=True)
            try:
                total = conn.execute('SELECT SUM(size_gib) FROM tables').fetchone()[0]
            finally:
                conn.close()
            size = float(total) if total else None
        except sqlite3.DatabaseError:
            size = None
    for line in estimate.calibration_lines(cal, size):
        print(line, file=out)
    if args.dry_run:
        print('DRY RUN: baseline not written.', file=out)
        return EXIT_DONE
    if not config.estimate.baseline:
        print('estimate.baseline is null in the configuration; baseline not written.', file=out)
        return EXIT_DONE
    path = estimate.write_baseline(config.resolve(config.estimate.baseline), cal.baseline)
    print('baseline written to %s' % path, file=out)
    return EXIT_DONE


def cmd_mark_recopy(args, env, out, state):
    """mark-recopy TABLE [--bucket N]: set buckets to failed with attempts 0 so the next copy run redoes them."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING)
    try:
        inv.open_journals()
        matched = 0
        for db in inv.databases:
            journal = inv.journals[db.source]
            try:
                count = journal.mark_recopy(args.table, args.bucket)
            except KitError as exc:
                if 'not in this journal' in str(exc):
                    continue
                raise
            matched += 1
            print('%s%s: %d bucket(s) marked for re-copy in %s (run %s)' % (
                args.table, '' if args.bucket is None else ' bucket %d' % args.bucket, count, db.source, inv.run_id), file=out)
        if not matched:
            raise KitError('Table %s is not in the journal of any selected database for run %s' % (args.table, inv.run_id))
        if inv.dry_run:
            print('DRY RUN: nothing was written.', file=out)
        else:
            print('rerun O6 (s2rebuild step O6) to delete and re-insert the marked buckets', file=out)
        return EXIT_DONE
    finally:
        inv.close()


def _stop_files(inv):
    return [(db.source, copy_module.stop_file_for(inv.journal_path(db))) for db in inv.databases]


def cmd_pause(args, env, out, state):
    """pause: create the stop file; running chunks finish, no new chunk starts."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING, dry_run=False)
    try:
        for source, path in _stop_files(inv):
            if args.dry_run:
                print('DRY RUN: would create %s' % path, file=out)
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('paused by s2rebuild pause\n', encoding='utf8')
            print('%s: stop file created at %s; running chunks finish, no new chunk starts' % (source, path), file=out)
        return EXIT_DONE
    finally:
        inv.close()


def cmd_resume(args, env, out, state):
    """resume: remove the stop file."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING, dry_run=False)
    try:
        for source, path in _stop_files(inv):
            if args.dry_run:
                print('DRY RUN: would remove %s' % path, file=out)
                continue
            if path.exists():
                path.unlink()
                print('%s: stop file removed; rerun step O6 to continue the copy' % source, file=out)
            else:
                print('%s: no stop file at %s' % (source, path), file=out)
        return EXIT_DONE
    finally:
        inv.close()


def _protected_users(config):
    users = {config.runner.user}
    if config.owner is not None:
        users.add(config.owner.user)
    users.update(config.freeze.lockout.protected_accounts)
    return users


def cmd_watch(args, env, out, state):
    """watch: the writer watch on one database."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING)
    try:
        if len(inv.databases) != 1:
            raise KitError('watch works on one database; pass --db NAME')
        db = inv.databases[0]
        inv.open_journals()
        log = inv.open_log('watch')
        data, plans, _ = inv.plans_for(db)
        sessions = inv.open_sessions()
        session = sessions.runner(read_guard('runner watch'))
        if inv.dry_run:
            session.query(inventory.PROCESSLIST_SQL, (db.source, '%' + db.source + '%'))
            for p in sorted(plans, key=lambda p: -p.est_rows)[:args.tables]:
                session.query('SELECT COUNT(*) AS n FROM ' + '`%s`.`%s`' % (db.source, p.name))
            log('DRY RUN: the watch would repeat these statements every %d s (counts every 60 s); no connection was opened' % args.interval)
            return EXIT_DONE
        until = parse_utc(args.until, '--until') if args.until else None
        watch.watch(session, db.source, plans, _protected_users(inv.config), log, interval=args.interval, tables=args.tables, until=until)
        return EXIT_DONE
    finally:
        inv.close()


def cmd_quiet_proof(args, env, out, state):
    """quiet-proof: the Path B proof that the source is quiet; writes freeze_time on success."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING)
    try:
        if len(inv.databases) != 1:
            raise KitError('quiet-proof works on one database; pass --db NAME')
        db = inv.databases[0]
        inv.open_journals()
        log = inv.open_log('quiet-proof')
        data, plans, _ = inv.plans_for(db)
        sessions = inv.open_sessions()
        session = sessions.runner(read_guard('runner quiet-proof'))
        owner = sessions.owner(read_guard('owner quiet-proof')) if inv.config.owner is not None else None
        if inv.dry_run:
            session.query(inventory.PROCESSLIST_SQL, (db.source, '%' + db.source + '%'))
            if owner is not None:
                owner.query(inventory.PIPELINES_SQL, (db.source,))
            log('DRY RUN: the quiet proof would take these samples twice, 60 s apart; no connection was opened')
            return EXIT_DONE
        passed, _ = watch.quiet_proof(session, owner, db.source, plans, _protected_users(inv.config), inv.journals[db.source], log)
        return EXIT_DONE if passed else EXIT_GATE
    finally:
        inv.close()


def cmd_report(args, env, out, state):
    """report: write the final workbook and the Markdown run report for the run."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING)
    try:
        inv.open_journals(read_only=not inv.dry_run)
        baseline = _baseline_for(inv.config)
        for db in inv.databases:
            data, plans, _ = inv.plans_for(db)
            reports = inv.config.runs_dir() / inv.run_id / db.source / 'reports'
            suffix = '-dry-run' if inv.dry_run else ''
            xlsx = report_xlsx.write_final_workbook(reports / ('final%s.xlsx' % suffix), data, plans, inv.journals[db.source], inv.config, db,
                                                    inv.run_id, baseline, env)
            md = report_md.write_run_report(reports / ('run-report%s.md' % suffix), data, plans, inv.journals[db.source], inv.config, db, inv.run_id)
            print('%s: final workbook %s and run report %s written' % (db.source, xlsx, md), file=out)
        return EXIT_DONE
    finally:
        inv.close()


def _inside_customer(config, path):
    """A path the kit may write: inside the customer folder (the parent of paths.runs_dir); anything else is refused."""
    root = config.runs_dir().resolve().parent
    target = Path(path)
    target = target if target.is_absolute() else Path.cwd() / target
    try:
        target.resolve().relative_to(root)
    except ValueError:
        raise KitError('Everything a run writes stays under %s; %s is outside it' % (root, target))
    return target


def _confirm(prompt, state_yes):
    """True when --yes was given or the operator typed yes at a terminal; False without a terminal."""
    if state_yes:
        return True
    if not sys.stdin.isatty():
        return False
    return input(prompt).strip().lower() == 'yes'


def _step_context(inv, db, log):
    return steps.StepContext(inv.config, db, inv.journals[db.source], inv.run_id, log, dry_run=inv.dry_run, sessions=inv.sessions)


def cmd_owner_sql(args, env, out, state):
    """owner-sql ID: print the ordered SQL block for an admin step; also written under owner-sql/."""
    step_id = args.step_id.upper()
    if step_id not in steps.OWNER_SQL_STEPS:
        raise KitError('owner-sql covers %s; %s is not an admin step with a block' % (', '.join(steps.OWNER_SQL_STEPS), step_id))
    if args.rollback and step_id != 'O10':
        raise KitError('--rollback applies to O10 only')
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING)
    try:
        inv.open_journals(read_only=not inv.dry_run)
        log = inv.open_log('owner-sql')
        for db in inv.databases:
            ctx = _step_context(inv, db, log)
            block = ownersql.block_for(step_id, ownersql.names_for(ctx), inv.config, rollback=args.rollback, **ownersql.block_inputs(ctx))
            ownersql.guard_check_block(block, ctx)
            path = ownersql.owner_sql_path(ctx, step_id, rollback=args.rollback)
            if not inv.dry_run and inv.run_id != DRY_RUN_ID:
                ownersql.write_block(block, path)
                print('-- written to %s' % path, file=out)
            print(block.render(), file=out, end='')
        return EXIT_DONE
    finally:
        inv.close()


def cmd_owner_notebook(args, env, out, state):
    """owner-notebook: generate owner-steps.ipynb for the run with one cell per admin step."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING, dry_run=False)
    try:
        inv.open_journals()
        log = inv.open_log('owner-notebook')
        for db in inv.databases:
            ctx = _step_context(inv, db, log)
            names, blocks, inputs = notebook.blocks_for_run(ctx)
            path = notebook.generate(inv.config.runs_dir() / inv.run_id / ('owner-steps%s.ipynb' % ('' if len(inv.databases) == 1 else '-' + db.source)),
                                     names, inv.config, blocks, inputs['grants'], items=inputs['pipelines'], capture_step=inputs['capture_step'])
            notebook.first_cell_compiles(path)
            print('%s: notebook written to %s (%d blocks, mode 0600, first cell compiles; pipeline capture from %s)' % (
                db.source, path, len(blocks), inputs['capture_step'] or 'none yet'), file=out)
        return EXIT_DONE
    finally:
        inv.close()


def cmd_account_sql(args, env, out, state):
    """account-sql: print the runner account block; --execute runs it with the owner profile; --drop the removal block."""
    inv = Invocation(args, env, out, state, run_mode=RUN_NEW_IF_NONE, dry_run=args.dry_run or not args.execute)
    try:
        inv.open_journals()
        log = inv.open_log('account-sql')
        db = inv.databases[0]
        ctx = _step_context(inv, db, log)
        block = ownersql.account_block(ownersql.names_for(ctx), drop=args.drop)
        ownersql.guard_check_block(block, ctx)
        print(block.render(), file=out, end='')
        if not args.execute:
            print('-- print only; add --execute to run it with the owner profile (the password comes from the configured Keychain item or variable)', file=out)
            return EXIT_DONE
        if inv.dry_run:
            print('-- DRY RUN: --execute would run the block above with the owner profile; no connection was opened', file=out)
            return EXIT_DONE
        if inv.config.owner is None:
            raise KitError('--execute needs connections.owner in the configuration')
        sessions = inv.open_sessions()
        runner_secrets = sessions.secrets('runner')
        password = runner_secrets.connect_kwargs()['password']

        log.redactor.extend(secret_forms(password))
        state['redactor'].extend(secret_forms(password))

        def _substitute(sql):
            return sql.replace("'%s'" % ownersql.PASSWORD_PLACEHOLDER, sql_literal(password))
        owner = sessions.owner(steps.guard_for('C4' if args.drop else 'P1', db, steps.OWNER))
        try:
            ownersql.execute_block(block, owner, log, inv.journals[db.source], _substitute)
        finally:
            owner.close()
        print('%s executed with the owner profile' % ('C4 account removal' if args.drop else 'P1 account creation'), file=out)
        return EXIT_DONE
    finally:
        inv.close()


def cmd_lockout(args, env, out, state):
    """lockout: Path B revoke and kill loop with the quiet proof (owner profile required)."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING)
    try:
        if len(inv.databases) != 1:
            raise KitError('lockout works on one database; pass --db NAME')
        db = inv.databases[0]
        until = parse_utc(args.until, '--until') if args.until else None
        if until is not None and until <= utc_now():
            raise KitError('--until %s is not in the future' % args.until)
        inv.open_journals()
        log = inv.open_log('lockout')
        sessions = inv.open_sessions()
        ctx = _step_context(inv, db, log)
        if inv.dry_run or inv.config.owner is None:
            outcome = lockout.handle_o3_lockout(steps.REGISTRY['O3'], ctx)
            return outcome.exit_code if inv.config.owner is None and not inv.dry_run else EXIT_DONE
        if until is None:
            until = lockout.default_until(inv.journals[db.source], inv.config)
            if until is not None and until <= utc_now():
                raise KitError('the abort deadline %s is already past; pass --until to run the lockout anyway' % until.isoformat())
        data, plans, _ = copy_module.load_plans(ctx)
        run_dir = inv.config.runs_dir() / inv.run_id / db.source
        run_dir.mkdir(parents=True, exist_ok=True)
        admin, held, snap = lockout.prepare(sessions, db, inv.config, inv.journals[db.source], log, run_dir / 'grants-snapshot.json',
                                            kill_only=args.kill_only)
        runner = sessions.runner(read_guard('runner lockout'))

        def _quiet():
            ok, _ = watch.quiet_proof(runner, admin, db.source, plans, lockout.protected_users(inv.config), inv.journals[db.source], log)
            return ok
        summary = lockout.run_lockout(held, db.source, snap, inv.journals[db.source], log, inv.config, until=until,
                                      stop_file=copy_module.stop_file_for(inv.journal_path(db)), quiet_hook=_quiet, skip_grants=args.kill_only)
        return EXIT_DONE if summary['quiet_proof'] else EXIT_GATE
    finally:
        inv.close()


def cmd_rehearse(args, env, out, state):
    """rehearse: P5 throwaway copy and switch with a kill test."""
    inv = Invocation(args, env, out, state, run_mode=RUN_NEW_IF_NONE)
    try:
        if len(inv.databases) != 1:
            raise KitError('rehearse works on one configured database; pass --db NAME')
        db = inv.databases[0]
        inv.open_journals()
        log = inv.open_log('rehearse')
        sessions = inv.open_sessions()
        ctx = _step_context(inv, db, log)
        prefix = rehearse.check_prefix(args.prefix or rehearse.default_prefix(inv.run_id), inv.config)
        if args.cleanup:
            session = sessions.runner(rehearse.rehearsal_guard(prefix))
            try:
                if inv.dry_run:
                    for name in rehearse.rehearsal_names(prefix).values():
                        session.execute('DROP DATABASE ' + '`%s`' % name)
                    return EXIT_DONE
                found = rehearse.existing_names(session, prefix)
                if not found:
                    log('rehearsal cleanup: no database with the prefix %s exists' % prefix)
                    return EXIT_DONE
                if not _confirm('Drop %s? Type yes: ' % ', '.join(found), args.yes):
                    raise KitError('rehearse --cleanup would drop %s; pass --yes or confirm at a terminal' % ', '.join(found))
                rehearse.cleanup(session, prefix, log)
            finally:
                session.close()
            return EXIT_DONE
        outcome = rehearse.run(ctx, prefix, rows=args.rows, keep=args.keep)
        log('rehearsal: ' + outcome.note)
        return outcome.exit_code
    finally:
        inv.close()


def _flag_rows(parser, command):
    rows = []
    for action in parser._actions:  # noqa: SLF001 - argparse offers no public walk
        if isinstance(action, argparse._HelpAction) or isinstance(action, argparse._VersionAction):  # noqa: SLF001
            continue
        if isinstance(action, argparse._SubParsersAction):  # noqa: SLF001
            continue
        if action.option_strings:
            flag = ', '.join(action.option_strings)
            if action.metavar:
                flag += ' ' + action.metavar
        else:
            flag = action.metavar or action.dest
        default = action.default
        if default in (argparse.SUPPRESS, None, False):
            default = ''
        rows.append((command, flag, str(default), (action.help or '').replace('|', 'or')))
    return rows


def docs_flags_markdown(parser=None):
    """The Markdown flag table for docs/CONFIG.md: global flags, then every command."""
    parser = parser or build_parser()
    rows = _flag_rows(parser, '(global)')
    subparsers = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))  # noqa: SLF001
    for name in COMMAND_NAMES:
        sub_rows = [r for r in _flag_rows(subparsers.choices[name], name)
                    if r[1].split(',')[0].split(' ')[0] not in ('--config', '--run', '--db', '--dry-run', '--yes', '--verbose')]
        if not sub_rows:
            sub_rows = [(name, '', '', '(global flags only)')]
        rows.extend(sub_rows)
    lines = ['| Command | Flag | Default | Description |', '|---|---|---|---|']
    for command, flag, default, help_text in rows:
        lines.append('| %s | %s | %s | %s |' % (command, ('`%s`' % flag) if flag else '', default, help_text))
    return '\n'.join(lines) + '\n'


def cmd_docs_flags(args, env, out, state):
    """docs-flags: print the flag table."""
    print(docs_flags_markdown(), file=out, end='')
    return EXIT_DONE


def cmd_check(args, env, out, state):
    """check: the P2 preflight for every selected database (connection, privileges, layout, STORAGE_ID, names, compute size)."""
    inv = Invocation(args, env, out, state, run_mode=RUN_NEW_IF_NONE)
    try:
        inv.open_journals()
        log = inv.open_log('check')
        inv.open_sessions()
        worst = EXIT_DONE
        step = steps.REGISTRY['P2']
        for db in inv.databases:
            ctx = _step_context(inv, db, log)
            log('[P2] Preflight database %s' % db.source)
            if not inv.dry_run:
                ctx.journal.step_start('P2')
            try:
                outcome = inventory.handle_p2(step, ctx)
            except KitError as exc:
                outcome = steps.Outcome(steps.REFUSED, str(exc), exc.exit_code)
            log('    result: %s: %s' % (outcome.status, outcome.note))
            if not inv.dry_run:
                ctx.journal.step_end('P2', 'passed' if outcome.status == steps.PASSED else 'failed', outcome.note)
            worst = max(worst, outcome.exit_code)
        if worst == EXIT_DONE and not inv.dry_run:
            log('check passed: record P1 with s2rebuild gate P1 --pass --note TEXT once the account, Keychain items and config are in place')
        return worst
    finally:
        inv.close()


def cmd_bundle(args, env, out, state):
    """bundle: MANIFEST.json, the denylist and style scan, the zip under dist/; --verify ZIP checks a zip against its manifest."""
    root = bundle.kit_root()
    if args.verify:
        problems = bundle.verify(args.verify)
        for line in problems:
            print('MISMATCH: ' + line, file=out)
        if problems:
            return EXIT_MISMATCH
        print('verify passed: every file in %s matches its manifest hash' % args.verify, file=out)
        return EXIT_DONE
    result = bundle.build(root, include_customer=args.include_customer, dry_run=args.dry_run)
    for line in result['lines']:
        print(line, file=out)
    return EXIT_DONE


def not_implemented(name, milestone):
    """A handler that refuses a command whose module lands in a later milestone."""
    def _handler(args, env, out, state):
        print('REFUSED: %s is not available in this build; it arrives in %s. Nothing was run.' % (name, milestone), file=out)
        return EXIT_REFUSED
    return _handler


DISPATCH = {'run': cmd_run, 'step': cmd_step, 'gate': cmd_gate, 'status': cmd_status, 'docs-flags': cmd_docs_flags,
            'profile': cmd_profile, 'plan': cmd_plan, 'calibrate': cmd_calibrate, 'mark-recopy': cmd_mark_recopy, 'pause': cmd_pause,
            'resume': cmd_resume, 'watch': cmd_watch, 'quiet-proof': cmd_quiet_proof, 'report': cmd_report, 'owner-sql': cmd_owner_sql,
            'owner-notebook': cmd_owner_notebook, 'account-sql': cmd_account_sql, 'lockout': cmd_lockout, 'rehearse': cmd_rehearse,
            'check': cmd_check, 'bundle': cmd_bundle}
for _name, _label, _help, _milestone in COMMANDS:
    if _milestone is not None:
        DISPATCH[_name] = not_implemented(_name, _milestone)


def main(argv=None, env=None, stream=None):
    """Parse the command line, dispatch and return the exit code. Every message passes the redactor."""
    env = os.environ if env is None else env
    out = stream or sys.stdout
    state = {'redactor': Redactor(), 'verbose': False, 'run_id': None}
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        state['verbose'] = bool(args.verbose)
        return DISPATCH[args.command](args, env, out, state)
    except KitError as exc:
        print('REFUSED: %s' % state['redactor'](exc), file=out)
        return exc.exit_code
    except BrokenPipeError:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        return EXIT_DONE
    except KeyboardInterrupt:
        resume = (' Resume with: s2rebuild run --run %s --from <step shown above>.' % state['run_id']) if state['run_id'] else ''
        print('Interrupted; nothing further was issued.%s' % resume, file=out)
        return EXIT_REFUSED
    except Exception as exc:  # noqa: BLE001 - last resort, redacted, never a traceback with values unless --verbose
        redactor = state['redactor']
        print('ERROR: %s: %s' % (type(exc).__name__, redactor(exc)), file=out)
        if state['verbose']:
            print(redactor(traceback.format_exc()), file=out)
        return EXIT_REFUSED
