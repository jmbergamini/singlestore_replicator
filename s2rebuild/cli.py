"""s2rebuild cli: the command line, exit codes and dispatch to the step modules.

Author: John Bergamini

Section 8 of the architecture: every command and global flag is declared here so that
`s2rebuild --help` describes the whole kit. Commands whose module
lands in a later milestone refuse with exit 1 and name the milestone. Exit codes: 0 done,
1 refused or configuration error, 2 incomplete, 3 verification mismatch, 4 gate pending or
failed. Statements issued by this module: none.
"""
import argparse
import contextlib
import datetime as dt
import json
import os
import subprocess
import sys
import traceback
from pathlib import Path

from . import __title__, __version__, abandon, cleanup, copy as copy_module, estimate, freeze, inventory, notebook, ownersql, pipelines, postcopy, report_md, report_xlsx, routines, steps, switch
from .common import EXIT_DONE, EXIT_GATE, EXIT_INCOMPLETE, EXIT_REFUSED, KitError, check_run_id, run_date, unique_run_id, utc_now
from .sqlutil import secret_forms, sql_literal
from .config import load_config
from .connect import DRIVER_RESET_LINE, Log, Redactor, Secrets, Session, SessionFactory, driver_errno, is_driver_error
from .journal import Journal
from .sqlutil import read_guard

ENV_RUN = 'S2REBUILD_RUN'
DRY_RUN_ID = 'dry-run'
GLOBAL_FLAGS = ('--config', '--run', '--db', '--dry-run', '--yes', '--verbose', '--accept-config-change')

# command: (step label, help text, where it arrives; None when implemented here)
COMMANDS = [
    ('check', 'P2', 'Connect as runner (and owner if configured), check privileges, engine version, source layout and '
                    'STORAGE_ID, free target names, compute cores times 8 at or above the partition count; prints the three workspace '
                    'questions O1 records the answers to; needs P1 recorded', None),
    ('account-sql', 'P1, C4', 'Print the CREATE USER and GRANT block for the runner account; --execute runs it with the '
                              'owner profile; --drop prints or executes the DROP USER block and the zero-privilege check', None),
    ('profile', 'P3', 'Inventory, sizes, estimate and the profile workbook for the configured databases', None),
    ('plan', 'P4', 'Print the plan sheet: window, freeze, step owners, the estimate line', None),
    ('run', 'O1 to O11', 'Run the outage steps in order and stop at the first failed gate', None),
    ('step', 'any', 'Run one step by id', None),
    ('gate', 'admin, human', 'Record a gate result for a step the kit did not execute itself', None),
    ('status', 'any', 'Board of steps per database: status, start, end, evidence path', None),
    ('owner-sql', 'O2, O3, O9, O10, O11, C3, C4', 'Print the ordered SQL block for an admin step with its precondition '
                                                   'queries and expected results; also written under owner-sql/', None),
    ('owner-notebook', 'any admin step', 'Generate owner-steps.ipynb for the run with one cell per admin step', None),
    ('mark-recopy', 'O6 to O8', 'Mark a table, or one bucket of it, for re-copy; the next copy run deletes and re-inserts it, '
                                'and O7 and O8 run again', None),
    ('pause', 'O6', 'Create the stop file: running chunks finish, no new chunk starts', None),
    ('resume', 'O6', 'Remove the stop file', None),
    ('abandon', 'before O10', 'Back out a run stopped before the switch: the source READWRITE again on its recorded STORAGE_ID, '
                              'the replacement dropped, the run closed; --execute --note TEXT runs it with the owner profile', None),
    ('report', 'C2', 'Final Excel and Markdown for a run', None),
    ('cleanup', 'C5', 'List what the run left after C3 (the live record, the retained copy, its record, size and retain-until date); '
                      '--delete NAME deletes the retained copy after the date, by its record, after the name is typed again; --yes does not apply', None),
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
                        help='run id (default: $S2REBUILD_RUN, else the most recent run; a new run starts only with run --new, '
                             'or when no run exists yet)')
    parser.add_argument('--db', metavar='NAME', action=Collect, default=default,
                        help='restrict to one configured source database; repeatable')
    parser.add_argument('--dry-run', action='store_true', default=flag_default,
                        help='print every statement, open no connection where the step allows it, write nothing')
    parser.add_argument('--yes', action='store_true', default=flag_default, help='no interactive confirmation')
    parser.add_argument('--verbose', action='store_true', default=flag_default, help='more detail in the log')
    parser.add_argument('--accept-config-change', metavar='NOTE', default=default,
                        help='go on although the configuration changed after O1: the journal records both hashes and the note, '
                             'then keeps the new hash')


def build_parser():
    """The complete command line of section 8."""
    collected = {}
    parser = Parser(prog='s2rebuild', description='%s %s. Rebuild a SingleStore Helios database into a new partition '
                    'layout and switch it over inside one outage window. Steps P1 to P4 prepare, O1 to O11 run inside '
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
    p['profile'].add_argument('--all-databases', action='store_true',
                              help='add a system sheet with every database in the workspace for ranking and sequencing')
    p['profile'].add_argument('--inventory', metavar='DIR', help='read a saved capture instead of connecting (required for --dry-run without a prior capture)')
    p['profile'].add_argument('--out', metavar='FILE', help='workbook path inside the customer folder (default: the run reports folder)')
    p['profile'].add_argument('--step', choices=['P3', 'O4'], default='P3', help='P3 profile before the window (default) or O4 freeze capture with drift')
    p['profile'].add_argument('--redo', action='store_true', help='capture again although the step passed; logs what it rewrites (O4: the copy plan)')
    p['plan'].add_argument('--inventory', metavar='DIR', help='read a saved capture instead of the run inventory')
    p['run'].add_argument('--from', dest='from_id', metavar='ID', default='O1', help='first step to run (default O1)')
    p['run'].add_argument('--to', dest='to_id', metavar='ID', default='O11', help='last step to run (default O11)')
    p['run'].add_argument('--redo', action='store_true', help='run steps that already passed')
    p['run'].add_argument('--retry-failed', action='store_true', help='O6: also rerun chunks parked after copy.max_attempts failures')
    p['run'].add_argument('--workspace', metavar='NOTE', help='O1 only: the answers to the three workspace questions of P2 (ACTIVE, autoscale off, '
                                                             'auto-suspend off or a keep-alive job); recorded as evidence of the go')
    p['run'].add_argument('--mode', choices=['production', 'test'], help='O1 only: override run.mode; production asks for a replacement or '
                                                                         'retained name the configuration leaves to the kit, test applies the defaults')
    p['run'].add_argument('--new', action='store_true', help='start a new run at O1 instead of resuming the most recent one; with --run ID '
                                                             'the new run takes that id, which must not exist yet')
    p['step'].add_argument('step_id', metavar='ID', help='step id such as O6')
    p['step'].add_argument('--redo', action='store_true', help='run the step even when it already passed')
    p['step'].add_argument('--fresh-table', metavar='TABLE', help='O6 only: load one table with the fresh-table fallback')
    p['step'].add_argument('--retry-failed', action='store_true', help='O6: also rerun chunks parked after copy.max_attempts failures')
    p['step'].add_argument('--workspace', metavar='NOTE', help='O1 only: the answers to the three workspace questions of P2 (ACTIVE, autoscale off, '
                                                              'auto-suspend off or a keep-alive job); recorded as evidence of the go')
    p['step'].add_argument('--mode', choices=['production', 'test'], help='O1 only: override run.mode; production asks for a replacement or '
                                                                          'retained name the configuration leaves to the kit, test applies the defaults')
    p['step'].add_argument('--idle-workload', metavar='NOTE', help='C1 only: pass without application writes seen; why the live database is idle')
    p['step'].add_argument('--fingerprint', metavar='TABLE', action='append',
                           help='O8 only: run tier 3 for this table only, sampled (repeatable; the other tables keep their recorded result)')
    p['step'].add_argument('--percent', metavar='N', type=int, help='O8 with --fingerprint: the sample percent (default verify.sample_percent)')
    p['step'].add_argument('--full', metavar='TABLE', action='append', help='O8 only: run a full tier 3 fingerprint for this table only (repeatable)')
    p['gate'].add_argument('step_id', metavar='ID', help='step id such as O3')
    decision = p['gate'].add_mutually_exclusive_group(required=True)
    decision.add_argument('--pass', dest='passed', action='store_true', help='the gate passed')
    decision.add_argument('--fail', dest='failed', action='store_true', help='the gate failed')
    p['gate'].add_argument('--note', metavar='TEXT', required=True, help='what was observed')
    p['gate'].add_argument('--evidence', metavar='FILE', help='file with the evidence (notebook output, query result)')
    p['gate'].add_argument('--override', metavar='NOTE', help='C1 and C2 only: record the gate of a close-out step the kit could not pass, and why')
    p['owner-sql'].add_argument('--rollback', action='store_true', help='O10 only: print the rollback block R1 to R3 instead of the forward block')
    p['owner-sql'].add_argument('step_id', metavar='ID', help='admin step id: %s' % ', '.join(steps.OWNER_SQL_STEPS))
    p['abandon'].add_argument('--execute', action='store_true', help='run the block with the owner profile (needs --note)')
    p['abandon'].add_argument('--note', metavar='TEXT', help='why the run is abandoned; recorded in the journal')
    p['cleanup'].add_argument('--delete', metavar='NAME', help='the retained name to delete for good: refused before the retain-until date, '
                                                                 'then the name is typed again at the prompt (owner profile)')
    p['mark-recopy'].add_argument('table', metavar='TABLE', help='table name as recorded in the journal')
    p['mark-recopy'].add_argument('--bucket', metavar='N', type=int, help='one bucket instead of every bucket of the table')
    return parser


# ---------------------------------------------------------------------------
# Shared context
# ---------------------------------------------------------------------------
RUN_NEW, RUN_EXISTING, RUN_NEW_IF_NONE, RUN_FIRST = 'new', 'existing', 'new_if_none', 'first'


def newest_first(run_dirs):
    """Run directories ordered by the newest change inside them (directory or journal file), newest first."""

    def _changed(path):
        times = [path.stat().st_mtime]
        times.extend(child.stat().st_mtime for child in path.rglob('journal.sqlite'))
        return max(times)
    return sorted(run_dirs, key=_changed, reverse=True)


class Invocation:
    """Config, databases, run id, journals and log for one command.

    run_mode says how the run id is chosen when none is given: RUN_NEW starts a run (run --new),
    RUN_EXISTING needs one (gate, status, run --from a later step), RUN_NEW_IF_NONE starts
    one only when no run exists yet (step and profile, for the preparation steps). A given id
    must name an existing run in RUN_EXISTING mode; RUN_NEW_IF_NONE starts the named run, and
    RUN_NEW starts it only while no run has that id. RUN_FIRST (gate on a P step, run from O1)
    is RUN_NEW_IF_NONE while no run exists and RUN_EXISTING after, so the P1 gate starts the
    first run, run resumes the newest run, and a mistyped id never starts another. A dry run
    never creates a run directory; a dry run of run --new reads no run.

    Every journal a command opens passes check_journals (open_journals calls it), so every
    command follows one rule on the names and the configuration hash.
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
        if mode == RUN_FIRST:
            mode = RUN_EXISTING if names else RUN_NEW_IF_NONE
        given = self.args.run or self.env.get(ENV_RUN)
        if given:
            check_run_id(given)
            if mode == RUN_NEW and given in names:
                raise KitError('Run %s exists already under %s%s; --new starts a new run. Leave out --new to resume it, or name a new id '
                               'with --run' % (given, runs_dir, '' if self.args.run else ' (named by %s)' % ENV_RUN))
            if given not in names and mode == RUN_EXISTING and not self.dry_run:
                raise KitError('Run %s does not exist under %s%s%s' % (
                    given, runs_dir, ('; existing runs: ' + ', '.join(names)) if names else '; no run exists yet',
                    '; s2rebuild run --new --run %s starts a run of that name' % given if 'new' in vars(self.args) else ''))
            return given
        if self.dry_run:
            return names[0] if names and mode != RUN_NEW else DRY_RUN_ID
        if mode == RUN_NEW or (mode == RUN_NEW_IF_NONE and not names):
            return unique_run_id(names)
        if names:
            return names[0]
        raise KitError('No run exists under %s yet; the P1 gate starts the first run: s2rebuild gate P1 --pass --note TEXT' % runs_dir)

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

    def open_journals(self, read_only=False, create=True, report_only=False):
        """Open one journal per selected database, then check_journals: an in-memory copy for a dry run, read-only for status.

        create=False leaves out a journal that does not exist yet, so a command that refuses on
        its preconditions creates nothing; a later open_journals() opens the rest. A journal
        already open stays as it is. report_only (status, report) reports a check that fails
        instead of refusing.
        """
        opened = []
        for db in self.databases:
            if db.source in self.journals:
                continue
            path = self.journal_path(db)
            if self.dry_run:
                self.journals[db.source] = Journal.dry_run_copy(path)
            elif read_only:
                if not path.exists():
                    raise KitError('No journal for run %s and database %s at %s' % (self.run_id, db.source, path))
                self.journals[db.source] = Journal(path, read_only=True)
            elif not create and not path.exists():
                continue
            else:
                self.journals[db.source] = Journal(path)
            opened.append(self.apply_recorded_names(db, self.journals[db.source]))
        self.check_journals(opened, read_only=read_only, report_only=report_only)
        return self.journals

    def apply_recorded_names(self, db, journal):
        """The replacement and retained names O1 recorded replace a default the configuration left to the kit; returns the DatabaseSpec.

        run.mode production asks for such a name at O1 (steps.o1_names) and the journal is where
        the answer lives, so every later command, in this process or another, works on the typed
        name. A name set in the configuration is never replaced: check_journals refuses a journal
        that records another one.
        """
        if not db.defaulted:
            return db
        recorded = {'replacement': journal.get_meta('replacement_db'), 'retained': journal.get_meta('retained_db')}
        changes = {role: name for role, name in recorded.items() if role in db.defaulted and name and name != getattr(db, role)}
        if not changes:
            return db
        self.config = self.config.with_names(db.source, **changes)
        new = self.config.database(db.source)
        self.databases = [new if d.source == db.source else d for d in self.databases]
        return new

    def check_journals(self, databases, read_only=False, report_only=False):
        """Refuse a journal this configuration does not belong to; one rule for every command.

        The names the journal records (O1, O5) must equal the configured names
        (steps.check_run_names), and once O1 recorded the configuration hash the configuration
        must hash the same (steps.config_change). A changed hash is refused unless
        --accept-config-change NOTE is given: then Journal.accept_config_hash records an event with
        both hashes and the note and keeps the new hash (a read-only command opens the journal
        for that write alone). A dry run reports a changed hash, records nothing and goes on;
        report_only only reports both checks. Statements issued: none.
        """
        note = str(getattr(self.args, 'accept_config_change', None) or '').strip()
        for db in databases:
            journal = self.journals[db.source]
            try:
                steps.check_run_names(journal, db)
            except KitError as exc:
                if not report_only:
                    raise
                print('warning: %s' % exc, file=self.out)
            changed = steps.config_change(journal, self.config)
            if changed is None:
                if note:
                    print('%s: --accept-config-change: %s; nothing to record' % (db.source, 'no configuration hash is recorded before O1'
                          if journal.get_meta('config_hash') is None else 'the configuration hash matches the journal'), file=self.out)
                continue
            text = steps.config_change_text(db, *changed)
            if report_only:
                print('warning: %s' % text, file=self.out)
            elif not note and self.dry_run:
                print('DRY RUN: %s; a real run refuses here' % text, file=self.out)
            elif not note:
                raise KitError(text)
            elif self.dry_run:
                print('DRY RUN: would record the configuration change for %s (%s to %s): %s' % (
                    db.source, changed[0][:12], changed[1][:12], note), file=self.out)
            else:
                if read_only:
                    writable = Journal(self.journal_path(db))
                    try:
                        writable.accept_config_hash(changed[0], changed[1], note)
                    finally:
                        writable.close()
                else:
                    journal.accept_config_hash(changed[0], changed[1], note)
                print('%s: configuration change after O1 recorded (%s to %s): %s' % (db.source, changed[0][:12], changed[1][:12], note),
                      file=self.out)

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
        self.state['log'] = self.log
        return self.log

    def refuse_abandoned(self):
        """Raise KitError when s2rebuild abandon closed this run for any selected database."""
        for journal in self.journals.values():
            why = abandon.abandoned(journal)
            if why:
                raise KitError('Run %s: %s' % (self.run_id, why))

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
    """The first log line of a command: the run id with its --run form (steps.run_flag), the runs folder, the mode, the databases."""
    flag = steps.run_flag(inv.run_id).strip()
    return '%s %s: %s, run %s%s under %s, %s, %s' % (
        __title__, __version__, command, inv.run_id, ' (%s)' % flag if flag else '', inv.config.runs_dir(),
        'DRY RUN' if inv.dry_run else 'live journal', ', '.join(db.source for db in inv.databases))


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
CAFFEINATE = ('caffeinate', '-i', '-w')


def keep_awake(log, dry_run, platform=None, popen=subprocess.Popen):
    """K2: on macOS start caffeinate -i -w <pid>, so the Mac does not sleep while this command runs; it ends with the process.

    A sleeping Mac is the likeliest real interruption of a copy (LESSONS fact 12). Nothing in a
    dry run or on another platform; a missing caffeinate is logged, never fatal. Returns the
    process or None.
    """
    if dry_run or (platform or sys.platform) != 'darwin':
        return None
    try:
        proc = popen(list(CAFFEINATE) + [str(os.getpid())], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError as exc:
        log('caffeinate could not start (%s); keep the Mac awake and on power by hand' % type(exc).__name__)
        return None
    log('caffeinate -i -w %d started: the Mac stays awake while this command runs' % os.getpid())
    return proc


def cmd_run(args, env, out, state):
    """run: O1 to O11 in order for the selected databases.

    Without --run (or S2REBUILD_RUN) it resumes the most recent run and skips the steps that
    passed (steps.run_step); it starts a run only when none exists (RUN_FIRST, from O1) or with
    --new. Once a run exists, a --run ID that names no run is refused unless --new is given, so a
    mistyped id never starts another run; --new refuses an id that exists and starts at O1 only.
    """
    step_ids = steps.outage_range(args.from_id, args.to_id)
    if args.new and step_ids[0] != 'O1':
        raise KitError('--new starts a run at O1; leave out --from %s, or leave out --new to continue the most recent run' % step_ids[0])
    inv = Invocation(args, env, out, state, run_mode=RUN_NEW if args.new else (RUN_FIRST if step_ids[0] == 'O1' else RUN_EXISTING))
    try:
        inv.open_journals()
        inv.refuse_abandoned()
        log = inv.open_log('run')
        log(_header(inv, 'run'))
        keep_awake(log, inv.dry_run)
        sessions = inv.open_sessions()
        return steps.run_steps(inv.config, inv.databases, inv.journals, log, run_id=inv.run_id, from_id=args.from_id,
                               to_id=args.to_id, redo=args.redo, dry_run=inv.dry_run, yes=args.yes, verbose=args.verbose,
                               sessions=sessions, options={'retry_failed': args.retry_failed, 'env': env,
                                                           'workspace': args.workspace,
                                                           'mode': args.mode})
    finally:
        inv.close()


def cmd_step(args, env, out, state):
    """step ID: one step of any phase for the selected databases."""
    step = steps.get_step(args.step_id)
    inv = Invocation(args, env, out, state, run_mode=RUN_NEW_IF_NONE if step.phase == 'P' else RUN_EXISTING)
    try:
        inv.open_journals()
        inv.refuse_abandoned()
        log = inv.open_log('step-%s' % step.id)
        log(_header(inv, 'step ' + step.id))
        keep_awake(log, inv.dry_run)
        options = {'retry_failed': args.retry_failed, 'env': env, 'workspace': args.workspace,
                   'mode': args.mode}
        if args.fresh_table:
            options['fresh_table'] = args.fresh_table
        if args.idle_workload:
            options['idle_workload'] = args.idle_workload
        options.update(_o8_options(args, step))
        sessions = inv.open_sessions()
        return steps.run_one(inv.config, inv.databases, inv.journals, log, step.id, run_id=inv.run_id, redo=args.redo,
                             dry_run=inv.dry_run, yes=args.yes, verbose=args.verbose, options=options, sessions=sessions)
    finally:
        inv.close()


def _o8_options(args, step):
    """The O8 rerun options of step: --fingerprint TABLE (repeatable) with --percent N, and --full TABLE; refused on another step."""
    named = {'fingerprint': args.fingerprint, 'full': args.full, 'percent': args.percent}
    given = sorted(k for k, v in named.items() if v is not None)
    if not given:
        return {}
    if step.id != 'O8':
        raise KitError('--%s applies to step O8 only' % given[0])
    if args.percent is not None and not args.fingerprint:
        raise KitError('--percent needs --fingerprint TABLE')
    if args.percent is not None and not 1 <= args.percent <= 100:
        raise KitError('--percent must be between 1 and 100')
    return {k: v for k, v in named.items() if v is not None}


def _gate_warnings(inv, step):
    """The warning lines of a gate whose step has preconditions that are not passed, one per database (steps.unmet_preconditions).

    A gate records what a person observed, so it is recorded anyway; the warning says what to pass.
    The O1 gate also warns for a journal in which O1 never ran: its check (P2) and its records
    (window, names, configuration hash) are missing there.
    """
    lines = []
    for db in inv.databases:
        journal = inv.journals[db.source]
        missing = steps.unmet_preconditions(step, journal)
        if missing:
            lines.append('warning: %s: earlier steps are not passed: %s; the %s gate is recorded anyway, since it records what a person '
                         'observed. %s' % (db.source, ', '.join(missing), step.id, steps.missing_hint(missing, db, run_id=inv.run_id)))
        if step.id == 'O1' and journal.get_meta('config_hash') is None:
            lines.append('warning: %s: O1 has not run in this journal, so its check (P2) and its records (window, '
                         'names, configuration hash) are missing; run s2rebuild run --from O1 --to O1 --redo --db %s%s, then record the go '
                         'again' % (db.source, db.source, steps.run_flag(inv.run_id)))
    return lines


def cmd_gate(args, env, out, state):
    """gate ID --pass or --fail: record a person's decision; with --dry-run only print what would be recorded.

    A gate on a P step starts the first run when none exists (RUN_FIRST). When the step's
    preconditions are not passed, the gate is recorded with a warning that is printed and logged.
    C1 and C2, python steps, are gated only with --override NOTE.
    """
    step = steps.gate_step(args.step_id, args.override)
    evidence = None
    if args.evidence:
        evidence_path = Path(args.evidence)
        if not evidence_path.is_file():
            raise KitError('Evidence file not found: %s' % evidence_path)
        evidence = str(evidence_path.resolve())
    inv = Invocation(args, env, out, state, run_mode=RUN_FIRST if step.phase == 'P' else RUN_EXISTING)
    try:
        inv.open_journals()
        inv.refuse_abandoned()
        verdict = 'PASS' if args.passed else 'FAIL'
        targets = ', '.join(inv.journals)
        o10_pass = args.passed and step.id == 'O10'
        if inv.dry_run:
            if o10_pass:
                _refuse_unverified_switch(inv)
            for line in _gate_warnings(inv, step):
                print(line, file=out)
            print('DRY RUN: would record gate %s as %s for run %s, database(s) %s: %s%s' % (
                step.id, verdict, inv.run_id, targets, args.note, (' (evidence %s)' % evidence) if evidence else ''), file=out)
            print('Nothing was written.', file=out)
            return EXIT_DONE
        result = _evidence_result(evidence, step.id, inv.run_id, bool(args.passed))
        log = inv.open_log('gate-%s' % step.id)
        if o10_pass:
            _refuse_unverified_switch(inv, result, log)
        if result is not None:
            for db in inv.databases:
                _import_evidence(_step_context(inv, db, log), step.id, result, log)
        for line in _gate_warnings(inv, step):
            log(line)
        steps.record_gate(inv.journals, step.id, bool(args.passed), args.note, evidence, override=args.override)
        log('gate %s recorded as %s for run %s, database(s) %s: %s%s' % (
            step.id, verdict, inv.run_id, targets, args.note, (' (evidence %s)' % evidence) if evidence else ''))
        _refresh_reports(inv, step.id, log)
        return EXIT_DONE
    finally:
        inv.close()


def _refuse_unverified_switch(inv, result=None, log=None):
    """gate O10 --pass: refuse, before the gate is recorded, while S0 would refuse on the journal of any selected database.

    The printed and notebook forward blocks carry the same problems as a STOP (switch.s0_journal_problems),
    but a block generated before a re-copy carries none, so the switch may have run anyway. When
    the evidence shows it did (_record_observed_switch), the observed attachments and a
    switch_observed event are recorded, so Journal.switch_began holds and a re-copy is refused
    from then on; the pass is never recorded. A dry run records nothing.
    """
    refused = [(source, problems) for source, problems in ((s, switch.s0_journal_problems(j)) for s, j in inv.journals.items()) if problems]
    if not refused:
        return
    reasons = '; '.join('gate O10 --pass refused for %s: S0 refused: %s' % (source, '; '.join(problems)) for source, problems in refused)
    observed = [] if inv.dry_run or result is None else _record_observed_switch(inv, result, log)
    if observed:
        raise KitError('%s. The pass was not recorded. The evidence shows the switch ran on %s, so the observed attachments are now '
                       'recorded in its journal and mark-recopy and O6 to O8 are refused from now on. The switch cannot pass on this copy: '
                       'roll it back with the rollback block (s2rebuild owner-sql O10 --rollback), whose zero-change proof needs the copied '
                       'row counts, or record what happened with s2rebuild gate O10 --fail --note TEXT' % (reasons, ', '.join(observed)))
    raise KitError('%s; nothing was recorded. If the forward block ran, record it with s2rebuild gate O10 --fail --note TEXT '
                   '--evidence FILE, so the journal knows the switch ran' % reasons)


def _record_observed_switch(inv, result, log):
    """Record the switch an O10 result shows: its attachments and a switch_observed event, in the journal of each selected database
    whose retained or live attachment row the result carries (matched by name). Returns those databases."""
    observed = []
    for db in inv.databases:
        names = {'source': db.source, 'replacement': db.replacement, 'retained': db.retained, 'live': db.source}
        rows = [att for att in result.get('attachments') or [] if att.get('role') in names and (att.get('row') or {}).get('STORAGE_ID')
                and str(att['row'].get('DATABASE_NAME')) == names[att['role']]]
        if not any(att['role'] in ('retained', 'live') for att in rows):
            continue
        journal = inv.journals[db.source]
        for att in rows:
            journal.record_attachment_row(att['role'], att['row'])
        journal.event('switch_observed', 'gate O10 --pass evidence (refused): %s' % ', '.join(
            '%s %s on STORAGE_ID %s' % (att['role'], att['row'].get('DATABASE_NAME'), att['row'].get('STORAGE_ID')) for att in rows))
        if log is not None:
            log('%s: the refused O10 evidence shows the switch ran; recorded %s' % (db.source, ', '.join(att['role'] for att in rows)))
        observed.append(db.source)
    return observed


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
    if passed and status != 'passed':
        raise KitError('The evidence reports status "%s"; a gate cannot pass on it' % status)
    return data


def _import_evidence(ctx, step_id, result, log):
    """Bring what a notebook result carries into the journal: the O2 pipeline capture and observed attachments."""
    if step_id == 'O2' and result.get('capture'):
        directory = pipelines.import_capture(ctx, result['capture'])
        log('O2 pipeline capture imported from the notebook result into %s' % directory)
    for att in result.get('attachments') or []:
        row = att.get('row') or {}
        if att.get('role') in ('source', 'replacement', 'retained', 'live') and row.get('STORAGE_ID'):
            ctx.journal.record_attachment_row(att['role'], row)
            log('attachment %s recorded from the notebook result: %s on STORAGE_ID %s' % (att['role'], row.get('DATABASE_NAME'), row.get('STORAGE_ID')))


def cmd_status(args, env, out, state):
    """status: the board of steps per database for a run, read-only; a name or configuration change is reported, not refused."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING, dry_run=False)
    try:
        inv.open_journals(read_only=True, report_only=True)
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


# What a rerun of a passed step rewrites, named in the --redo warning and in the refusal without it.
PROFILE_REWRITES = {
    'P3': 'the P3 capture under inventory/P3 (inventory.json, tables.csv, definitions/, pipelines/), the profile workbook and '
          'the drift baseline O4 compares with',
    'O4': 'the O4 freeze capture under inventory/O4 with plan.json and drift.json, the copy plan O5 to O8 read (O5 and O6 refuse '
          'a plan that differs from the journal), and the freeze-capture workbook',
}


def _entry_checks(inv, step, redo=False, rewrites=None):
    """The rules of steps.run_step for a command that drives a step itself (check for P2, profile for P3 and O4).

    Checked for every selected database before anything is captured or written: the step's
    preconditions are passed (steps.precondition_refusal; a database without a journal has
    nothing passed), and when rewrites names what a rerun rewrites, a step that passed runs
    again only with redo. Returns the lines to log: the --redo warnings, and in a dry run what
    a real run refuses (a dry run goes on). A real run raises KitError with every refusal.
    """
    refusals, lines = [], []
    run_id = inv.run_id if (inv.config.runs_dir() / inv.run_id).is_dir() else None  # the hint names a run only when it exists
    for db in inv.databases:
        journal = inv.journals.get(db.source)
        refusal = steps.precondition_refusal(step, journal, db, run_id)
        if refusal:
            refusals.append(refusal)
        elif rewrites and journal is not None and journal.step_status(step.id) == 'passed':
            if redo:
                lines.append('warning: %s passed already for %s; --redo captures again and rewrites %s' % (step.id, db.source, rewrites))
            else:
                refusals.append('%s passed already for database %s; a rerun rewrites %s. Run it again with --redo, or leave it as it is.' % (
                    step.id, db.source, rewrites))
    if refusals and not inv.dry_run:
        raise KitError(' '.join(refusals))
    return lines + ['DRY RUN: a real run refuses here: %s' % r for r in refusals]


def cmd_profile(args, env, out, state):
    """profile: capture (or load) the inventory, build the plan and estimate, write the workbook, record P3 or O4.

    The rules of run_step apply (_entry_checks): P3 needs P1 and P2 passed and O4 needs O1 to O3,
    for every selected database before anything is captured or written, and a passed step is
    captured again only with --redo, which logs what it rewrites. The journal records the step
    running just before the first file of the capture is written and passed after the last
    journal write, so a stop part way leaves it not passed and a plain rerun captures again. A
    capture refused for a load target INTO PROCEDURE the replacement will not hold (a procedure
    objects.routines.ignore lists, or one the capture lacks) is recorded failed before its files
    are written for review, so a stop between the two never leaves an earlier pass (a --redo)
    standing on the rewritten capture. A dry run checks the same, writes nothing
    under inventory/, and writes only the workbook under its -dry-run name or --out.
    """
    step_id = args.step
    step = steps.REGISTRY[step_id]
    inv = Invocation(args, env, out, state, run_mode=RUN_NEW_IF_NONE)
    try:
        inv.open_journals(create=False)
        notes = _entry_checks(inv, step, redo=args.redo, rewrites=PROFILE_REWRITES[step_id])
        inv.open_journals()
        log = inv.open_log('profile-' + step_id)
        log(_header(inv, 'profile ' + step_id))
        for line in notes:
            log(line)
        if inv.dry_run and inv.run_id == DRY_RUN_ID and not args.out:
            raise KitError('No run exists yet; a dry run needs --out FILE for the workbook')
        exit_code = EXIT_DONE
        captured, sessions = [], []
        try:
            # every capture and its estimate first, then the files
            for db in inv.databases:
                journal = inv.journals[db.source]
                target_dir = inventory.inventory_dir(inv.config, inv.run_id, db, step_id)
                if args.out:
                    out_path = _inside_customer(inv.config, args.out)
                else:
                    name = ('profile' if step_id == 'P3' else 'freeze-capture') + ('-dry-run' if inv.dry_run else '') + '.xlsx'
                    out_path = inv.config.runs_dir() / inv.run_id / db.source / 'reports' / name
                runner_session = None
                if args.inventory:
                    data = inventory.load(args.inventory)
                    log('%s: using the saved capture at %s' % (db.source, args.inventory))
                elif inv.dry_run:
                    data = inventory.load(target_dir)
                    log('%s: dry run, using the capture at %s' % (db.source, target_dir))
                else:
                    runner_session, owner_session = _open_sessions(inv, env)
                    sessions.extend([runner_session, owner_session])
                    data = inventory.capture(runner_session, db, inv.config, owner_session, exact_counts=args.exact_counts, log=log, step=step_id)
                    if step_id == 'P3':
                        _ask_pipeline_credentials(inv, db, data, log, env)
                plans = inventory.plan_tables(data, inv.config, db)
                if routines.pipeline_target_refusals(inv.config, db, data):
                    if inv.dry_run:
                        routines.refuse_pipeline_targets(data, inv.config, db, step_id)
                    try:
                        # the step is recorded failed first (running, then failed), so an earlier pass never stands on the
                        # capture written next; a stop between the two leaves it failed or running on the old files
                        routines.refuse_pipeline_targets(data, inv.config, db, step_id, journal)
                    except KitError:
                        # kept for review and the plan sheet; O5 and later refuse the failed step
                        inventory.write(data, target_dir, plans)
                        raise
                pipeline_count = len(data['pipelines']['items']) if data.get('pipelines', {}).get('captured') else 0
                est = estimate.estimate(plans, inv.config, pipeline_count)
                captured.append((db, journal, target_dir, out_path, data, plans, est, runner_session))
            for db, journal, target_dir, out_path, data, plans, est, runner_session in captured:
                if not inv.dry_run:
                    journal.step_start(step_id)
                try:
                    _write_profile(inv.config, inv.run_id, journal, inv.dry_run, db, step_id, data, plans, est, target_dir, out_path,
                                   runner_session if args.all_databases else None, log, env, all_databases=args.all_databases)
                    if not inv.dry_run:
                        journal.set_meta('inventory_dir_' + step_id, str(target_dir))
                        if data.get('attachment'):
                            journal.record_attachment_row('source', dict(data['attachment'], DATABASE_NAME=db.source))
                except KitError as exc:
                    if not inv.dry_run:
                        journal.step_end(step_id, 'pending', str(exc))
                    raise
                except BaseException as exc:
                    if not inv.dry_run:
                        note = steps.interrupted_note(exc)
                        log('    result: failed: ' + note)
                        try:
                            journal.step_end(step_id, 'failed', note)
                        except Exception:  # noqa: BLE001 - the exception that stopped the capture is the one the caller must see
                            log('    the failed status could not be journaled; status may still show %s running' % step_id)
                    raise
                if not inv.dry_run:
                    # the last journal write, as in run_step: a stop before it leaves the step not passed
                    journal.step_end(step_id, 'passed', '%s captured %d tables; workbook %s' % (step_id, len(plans), out_path), str(out_path))
        finally:
            for s in sessions:
                if s is not None:
                    s.close()
        if inv.dry_run:
            log('No connection was opened and nothing was written under inventory/; the workbook was written from the saved capture.')
        return exit_code
    finally:
        inv.close()


def _ask_pipeline_credentials(inv, db, data, log, env):
    """P3 on the kit route: ask for the credentials of every captured source type that has none yet (prompts.pipeline_credentials).

    No prompt when the capture holds no pipeline, on route customer, or in a dry run. A skip is
    recorded in the run journal, so the plan sheet, O9 and the final report list the pipelines
    that will not be recreated.
    """
    from . import prompts
    pipes = data.get('pipelines') or {}
    if inv.dry_run or not pipes.get('captured') or not pipes.get('items') or inv.config.pipelines.route != 'kit':
        return
    prompts.pipeline_credentials(pipes['items'], inv.config, inv.journals[db.source], log, env, inv.log.redactor, step='P3')


def _write_profile(config, run_id, journal, dry_run, db, step_id, data, plans, est, target_dir, out_path, runner_session, log, env,
                   all_databases=False):
    """The files of a profile: the inventory, drift.json and plan.json (O4), the workbook; a dry run writes the workbook only.

    runner_session serves the --all-databases system sheet only (None without the flag or a live session).
    """
    if not dry_run:
        inventory.write(data, target_dir, plans)
        log('%s: inventory written to %s (%d tables, %d buckets)' % (db.source, target_dir, len(plans), sum(p.n_buckets for p in plans)))
    log.plain('    ' + estimate.estimate_line(est))
    if step_id == 'O4':
        before_dir = inventory.inventory_dir(config, run_id, db, 'P3')
        if (before_dir / inventory.INVENTORY_FILE).is_file():
            report = inventory.drift(inventory.load(before_dir), data)
            for line in inventory.drift_lines(report):
                log('    ' + line)
            if not dry_run:
                inventory.write_private(target_dir / 'drift.json', json.dumps(report, indent=1))
        else:
            log('    drift: no P3 inventory to compare against')
        if not dry_run:
            inventory.write_private(target_dir / 'plan.json', json.dumps([p.describe() for p in plans], indent=1))
    report_xlsx.write_profile_workbook(out_path, data, plans, est, config, db, run_id, steps_board=journal.steps_board(), env=env)
    if all_databases:
        if runner_session is None:
            log('    --all-databases needs a live connection; skipped in this mode')
        else:
            rows = inventory.system_profile(runner_session, config)
            report_xlsx.write_system_sheet(out_path, rows)
            log('    system sheet: %d databases' % len(rows))
    log('%s: workbook written to %s (Summary plus %d table tabs)' % (db.source, out_path, len(plans)))


def handle_o4(step, ctx):
    """O4 inside run: the freeze capture, the drift against P3, the copy plan and the workbook, as profile --step O4 writes them.

    profile --step O4 stays for a rerun (--redo) and for --exact-counts or --all-databases. The
    runner session reads behind the read guard; the owner session, when configured, reads the
    routines and the write-capable accounts (the source is READ ONLY, so no pipeline
    view is read; O9 replays the O2 capture). A load target the replacement will not hold
    (routines.pipeline_target_refusals) fails the step with its capture kept for review. A dry run
    reads nothing and writes nothing. Statements issued: the reads of inventory.capture.
    """
    config, db, journal, log = ctx.config, ctx.db, ctx.journal, ctx.log
    target_dir = inventory.inventory_dir(config, ctx.run_id or '', db, 'O4')
    if (target_dir / inventory.INVENTORY_FILE).is_file():
        # a capture taken already (profile --step O4, or an earlier run): checked, never taken again without --redo of profile
        return inventory.handle_o4(step, ctx)
    if ctx.dry_run:
        return steps.Outcome(steps.STUB, 'dry run: the freeze capture reads the source and writes inventory/O4; '
                                        's2rebuild profile --step O4 --dry-run shows the plan from a saved capture')
    out_path = config.runs_dir() / ctx.run_id / db.source / 'reports' / 'freeze-capture.xlsx'
    env = ctx.options.get('env') or {}
    runner = ctx.runner_session(read_guard('runner O4'))
    owner = ctx.owner_session(read_guard('owner O4')) if ctx.owner_configured else None
    try:
        data = inventory.capture(runner, db, config, owner, log=log, step='O4')
    finally:
        for s in (runner, owner):
            if s is not None:
                s.close()
    plans = inventory.plan_tables(data, config, db)
    if routines.pipeline_target_refusals(config, db, data):
        try:
            routines.refuse_pipeline_targets(data, config, db, 'O4')
        except KitError as exc:
            inventory.write(data, target_dir, plans)  # kept for review, as profile does
            return steps.Outcome(steps.FAILED, str(exc), EXIT_GATE)
    pipeline_count = len(data['pipelines']['items']) if data.get('pipelines', {}).get('captured') else 0
    est = estimate.estimate(plans, config, pipeline_count)
    _write_profile(config, ctx.run_id, journal, False, db, 'O4', data, plans, est, target_dir, out_path, None, log, env)
    journal.set_meta('inventory_dir_O4', str(target_dir))
    if data.get('attachment'):
        journal.record_attachment_row('source', dict(data['attachment'], DATABASE_NAME=db.source))
    return steps.Outcome(steps.PASSED, 'O4 captured %d tables; workbook %s' % (len(plans), out_path))


def _skipped_types(inv, db):
    """The pipeline source types the operator skipped at the P3 credential prompt, from the run's journal for db, read only; an empty set
    without a journal."""
    path = inv.journal_path(db)
    if inv.dry_run or not path.exists():
        return set()
    journal = Journal(path, read_only=True)
    try:
        return pipelines.skipped_types(journal)
    finally:
        journal.close()


def cmd_plan(args, env, out, state):
    """plan: print the P4 plan sheet from the run's P3 inventory (or --inventory DIR)."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING, dry_run=bool(args.inventory) or args.dry_run)
    try:
        for db in inv.databases:
            source = Path(args.inventory) if args.inventory else inventory.inventory_dir(inv.config, inv.run_id, db, 'P3')
            data = inventory.load(source)
            plans = inventory.plan_tables(data, inv.config, db)
            pipeline_count = len(data['pipelines']['items']) if data.get('pipelines', {}).get('captured') else 0
            est = estimate.estimate(plans, inv.config, pipeline_count)
            for line in estimate.plan_sheet(inv.config, db, inv.run_id, data, est, env, skipped=_skipped_types(inv, db)):
                print(line, file=out)
            print('', file=out)
        return EXIT_DONE
    finally:
        inv.close()


def cmd_mark_recopy(args, env, out, state):
    """mark-recopy TABLE [--bucket N]: set buckets to failed with attempts 0 so the next copy run redoes them.

    The same journal transaction reopens O6, O7 and O8 when they passed and clears the table's
    verified_at_utc and target_count, so O8 and the switch cannot pass on evidence from before.
    The copy lock of every journal that holds the table is taken first (copy.copy_lock), so no
    copy, O7 re-copy round or O10 of another process runs on it meanwhile; a held lock refuses
    with nothing marked. Then every such journal is checked (Journal.check_recopy: the bucket
    exists, the switch has not begun), so a refusal for one database marks none. Before each
    journal write the O10 block files of that database get a STOP (ownersql.retire_o10_blocks).
    """
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING)
    try:
        inv.open_journals()
        log = inv.open_log('mark-recopy')
        label = '%s%s' % (args.table, '' if args.bucket is None else ' bucket %d' % args.bucket)
        found = [db for db in inv.databases if inv.journals[db.source].has_table(args.table)]
        if not found:
            raise KitError('Table %s is not in the journal of any selected database for run %s' % (args.table, inv.run_id))
        with contextlib.ExitStack() as locks:
            for db in found:
                try:
                    locks.enter_context(copy_module.copy_lock(None if inv.dry_run else inv.journal_path(db), 'mark-recopy'))
                except KitError as exc:
                    raise KitError('%s: %s; no database was marked' % (db.source, exc))
            for db in found:
                try:
                    inv.journals[db.source].check_recopy(args.table, args.bucket)
                except KitError as exc:
                    raise KitError('%s: %s; no database was marked' % (db.source, exc))
            reason = 'mark-recopy %s reopens its copy evidence; rerun O6 to O8 (s2rebuild run --from O6)' % label
            for db in found:
                for line in ownersql.retire_o10_blocks(_step_context(inv, db, log), [args.table], reason):
                    log(line)
                count = inv.journals[db.source].mark_recopy(args.table, args.bucket)
                print('%s: %d bucket(s) marked for re-copy in %s (run %s)' % (label, count, db.source, inv.run_id), file=out)
        if inv.dry_run:
            print('DRY RUN: nothing was written.', file=out)
        else:
            print('rerun O6 to O8 (s2rebuild run --from O6): O6 deletes and re-inserts the marked buckets, O7 sets the counters '
                  'and runs the stale check again, O8 verifies again; O6, O7 and O8 are pending until then. After O8 passes again, '
                  'generate the O10 blocks (s2rebuild owner-sql O10, and --rollback) or the notebook (s2rebuild owner-notebook) again '
                  'before a hand or notebook switch', file=out)
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


def cmd_report(args, env, out, state):
    """report: write the final workbook and the Markdown run report for the run."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING)
    try:
        inv.open_journals(read_only=not inv.dry_run, report_only=True)
        for db in inv.databases:
            data, plans, _ = inv.plans_for(db)
            reports = inv.config.runs_dir() / inv.run_id / db.source / 'reports'
            suffix = '-dry-run' if inv.dry_run else ''
            xlsx = report_xlsx.write_final_workbook(reports / ('final%s.xlsx' % suffix), data, plans, inv.journals[db.source], inv.config, db,
                                                    inv.run_id, env)
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
    """owner-sql ID: print the ordered SQL block for an admin step, and its companion blocks (O3-reentry); also written under owner-sql/."""
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
            names, inputs = ownersql.names_for(ctx), ownersql.block_inputs(ctx)
            block = ownersql.block_for(step_id, names, inv.config, rollback=args.rollback, **inputs)
            for variant, b in [(None, block)] + ownersql.companion_blocks(step_id, names, inv.config, rollback=args.rollback, **inputs):
                ownersql.guard_check_block(b, ctx)
                path = ownersql.owner_sql_path(ctx, step_id, rollback=args.rollback, variant=variant)
                if not inv.dry_run and inv.run_id != DRY_RUN_ID:
                    ownersql.write_block(b, path)
                    print('-- written to %s' % path, file=out)
                print(b.render(), file=out, end='')
        return EXIT_DONE
    finally:
        inv.close()


def cmd_owner_notebook(args, env, out, state):
    """owner-notebook: generate owner-steps.ipynb for the run with one cell per admin step; a dry run prints what it would write and writes nothing."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING)
    try:
        inv.open_journals()
        log = inv.open_log('owner-notebook')
        for db in inv.databases:
            ctx = _step_context(inv, db, log)
            names, blocks, inputs = notebook.blocks_for_run(ctx)
            path = inv.config.runs_dir() / inv.run_id / ('owner-steps%s.ipynb' % ('' if len(inv.databases) == 1 else '-' + db.source))
            if inv.dry_run:
                print('DRY RUN: %s: would write the notebook to %s (%d blocks: %s; pipeline capture from %s); nothing was written' % (
                    db.source, path, len(blocks), ', '.join(blocks), inputs['capture_step'] or 'none yet'), file=out)
                continue
            path = notebook.generate(path, names, inv.config, blocks, items=inputs['pipelines'], capture_step=inputs['capture_step'])
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
        from . import prompts
        # P1: the owner password is asked for (hidden) and stored when its Keychain item is missing
        prompts.ensure_password(inv.config.owner, log)
        sessions = inv.open_sessions()
        owner = sessions.owner(steps.guard_for('C4' if args.drop else 'P1', db, steps.OWNER))
        try:
            if not args.drop:
                _runner_password(inv, db, owner, log)
            password = sessions.secrets('runner').connect_kwargs()['password']
            log.redactor.extend(secret_forms(password))
            state['redactor'].extend(secret_forms(password))

            def _substitute(sql):
                return sql.replace("'%s'" % ownersql.PASSWORD_PLACEHOLDER, sql_literal(password))
            results = ownersql.execute_block(block, owner, log, inv.journals[db.source], _substitute)
        finally:
            owner.close()
        print('%s executed with the owner profile' % ('C4 account removal' if args.drop else 'P1 account creation'), file=out)
        if args.drop:
            _record_c4(inv, results, log)
        return EXIT_DONE
    finally:
        inv.close()


def _record_c4(inv, results, log):
    """B6: the C4 gate after account-sql --drop --execute, in every selected journal: the privileges before the DROP USER and after it."""
    observed = dict(results)
    note = 'C4 by account-sql --drop --execute: DROP USER %s; privileges before: %s; after: %s' % (
        inv.config.runner.user, observed.get('runner account still has privileges', 'not read'), observed.get('zero privileges remain', 'not read'))
    steps.record_gate(inv.journals, 'C4', True, note)
    log('gate C4 recorded as PASS for run %s, database(s) %s: %s' % (inv.run_id, ', '.join(inv.journals), note))
    _refresh_reports(inv, 'C4', log)


def _refresh_reports(inv, step_id, log):
    """After a gate on C2, C3 or C4: write the final workbook and the run report again for every selected database (report_xlsx.refresh_reports)."""
    if step_id not in report_xlsx.REPORT_STEPS:
        return
    for db in inv.databases:
        report_xlsx.refresh_reports(_step_context(inv, db, log), step_id)


def _runner_password(inv, db, owner, log):
    """P1: print the workspace password policy, then generate the runner password and store it in its Keychain item.

    The policy (the PASSWORD_ variables) is read through the owner session and printed before
    CREATE USER. With connections.runner.password_keychain, and while the runner account does
    not exist yet (the block's first check), a compliant random password is generated
    (prompts.generate_password) and written to the item, which the block then reads; an account
    that exists keeps its item, and the block stops at its first check. With password_env the
    variable is used as it is. No prompt, and the value never reaches a log.
    """
    from . import prompts
    policy = prompts.read_policy(owner)
    for line in prompts.policy_lines(policy):
        log('    ' + line)
    item = inv.config.runner.password_keychain
    if not item:
        log('    runner password: from %s (connections.runner.password_env); no password is generated' % inv.config.runner.password_env)
        return
    exists = owner.query(ownersql.privileges_count_sql(), (ownersql.grantee(inv.config.runner.user),))
    if exists and int(list(exists[0].values())[0] or 0) > 0:
        log('    runner password: the account exists already, so Keychain item %s is kept as it is' % item)
        return
    password = prompts.generate_password(policy)
    prompts.write_keychain(item, password)
    log('    runner password: %d characters generated to the policy and stored in Keychain item %s' % (len(password), item))


def cmd_abandon(args, env, out, state):
    """abandon: print the block that backs out a run stopped before the switch; --execute --note TEXT runs it with the owner profile."""
    if args.execute and not args.note:
        raise KitError('abandon --execute needs --note TEXT: why the run is abandoned')
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING)
    try:
        inv.open_journals()
        log = inv.open_log('abandon')
        log(_header(inv, 'abandon'))
        sessions = inv.open_sessions() if args.execute and not inv.dry_run else None
        for db in inv.databases:
            ctx = steps.StepContext(inv.config, db, inv.journals[db.source], inv.run_id, log, dry_run=inv.dry_run, sessions=sessions)
            log(abandon.run(ctx, args.note or '', args.execute))
        return EXIT_DONE
    finally:
        inv.close()


def cmd_cleanup(args, env, out, state):
    """cleanup: list what the run left after C3; --delete NAME runs the customer's delete of the retained copy (C5) with the owner profile."""
    inv = Invocation(args, env, out, state, run_mode=RUN_EXISTING)
    try:
        inv.open_journals()
        log = inv.open_log('cleanup')
        log(_header(inv, 'cleanup'))
        sessions = inv.open_sessions() if args.delete and not inv.dry_run else None
        code = EXIT_DONE
        for db in inv.databases:
            ctx = steps.StepContext(inv.config, db, inv.journals[db.source], inv.run_id, log, dry_run=inv.dry_run, sessions=sessions)
            if args.delete and args.delete != db.retained:
                continue
            code = max(code, cleanup.run(ctx, args.delete))
        if args.delete and not any(db.retained == args.delete for db in inv.databases):
            raise KitError('cleanup deletes the retained copy of a configured database only; %s is none of %s'
                           % (args.delete, ', '.join(db.retained for db in inv.databases)))
        return code
    finally:
        inv.close()


def cmd_check(args, env, out, state):
    """check: the P2 preflight for every selected database (connection, privileges, layout, STORAGE_ID, names, compute size).

    P2 runs through steps.run_step, so it is journaled as every step is. P1 must be passed in
    the run for every selected database before any connection opens (_entry_checks; in a dry run
    run_step reports it and the dry run goes on). A passed P2 runs again without --redo: the preflight only reads, and
    it records its result again (O1 asks for P2 passed in the same run).
    """
    step = steps.REGISTRY['P2']
    inv = Invocation(args, env, out, state, run_mode=RUN_NEW_IF_NONE)
    try:
        inv.open_journals(create=False)
        if not inv.dry_run:
            _entry_checks(inv, step)
        inv.open_journals()
        log = inv.open_log('check')
        inv.open_sessions()
        worst = EXIT_DONE
        for db in inv.databases:
            outcome = steps.run_step(step, _step_context(inv, db, log), redo=True)
            worst = max(worst, outcome.exit_code)
        if worst == EXIT_DONE and not inv.dry_run:
            log('check passed: P2 recorded in run %s; next, s2rebuild profile (P3)' % inv.run_id)
        return worst
    finally:
        inv.close()


def not_implemented(name, milestone):
    """A handler that refuses a command whose module lands in a later milestone."""

    def _handler(args, env, out, state):
        print('REFUSED: %s is not available in this build; it arrives in %s. Nothing was run.' % (name, milestone), file=out)
        return EXIT_REFUSED
    return _handler


DISPATCH = {'run': cmd_run, 'step': cmd_step, 'gate': cmd_gate, 'status': cmd_status,
            'profile': cmd_profile, 'plan': cmd_plan, 'mark-recopy': cmd_mark_recopy, 'pause': cmd_pause,
            'resume': cmd_resume, 'report': cmd_report, 'owner-sql': cmd_owner_sql,
            'owner-notebook': cmd_owner_notebook, 'account-sql': cmd_account_sql,
            'check': cmd_check, 'abandon': cmd_abandon, 'cleanup': cmd_cleanup}
for _name, _label, _help, _milestone in COMMANDS:
    if _milestone is not None:
        DISPATCH[_name] = not_implemented(_name, _milestone)
steps.register('O4', handle_o4)


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
        if is_driver_error(exc):
            # a node reset the connection (2003, 2004, 2006, 2013): one plain line, the raw error to the log file, exit 2
            raw = 'driver error %s: %s: %s' % (driver_errno(exc), type(exc).__name__, redactor(exc))
            log = state.get('log')
            if log is not None:
                log.file(raw)
            print('INCOMPLETE: %s (driver error %s%s)' % (DRIVER_RESET_LINE, driver_errno(exc),
                                                          '; the raw error is in the log file' if log is not None and log.path else ''), file=out)
            if state['verbose']:
                print(redactor(raw), file=out)
            return EXIT_INCOMPLETE
        print('ERROR: %s: %s' % (type(exc).__name__, redactor(exc)), file=out)
        if state['verbose']:
            print(redactor(traceback.format_exc()), file=out)
        return EXIT_REFUSED
