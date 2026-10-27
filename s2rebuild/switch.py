"""s2rebuild switch: the O10 state machine over the attachment views, forward S0 to S3 and rollback R1 to R3.

Author: John Bergamini

Section 10.13 of the architecture. The position is computed from MV_CLOUD_ATTACHED_DATABASES
and MV_CLOUD_BRANCHES, never from memory, so a re-entered step continues from the live state;
re-entry reruns the after-checks of the step it lands behind (S2 before S3, S3 after a failed
S3 check), so a failed check can never turn into a passed O10. Every statement sits between a
check before and a check after; an out-of-order statement is refused, not executed. S0
accepts only when every planned table was verified after freeze_time and after its last chunk,
at its current re-copy epoch, so a re-copy after O8 refuses the switch until O6 to O8 pass
again. O10 holds the copy lock from S0 through S3, and the go decision is recorded in one
journal transaction that reads S0's journal part again (Journal.record_go), so a re-copy started
while the operator types the live name is refused or refuses the go. The journal part of S0
holds on every route: print mode refuses O10, and the forward block that owner-sql and the
notebook carry starts with a STOP while it would refuse (a re-copy rewrites the block files
already written with a STOP first). Every S3 precondition
(replacement attached READWRITE on its recorded STORAGE_ID, retained name free) is checked
before S1, so the source is never detached for a switch that cannot finish. On Path B, S1
refuses unless no account holds again a write privilege the lockout took and the last lockout
pass recorded in the journal was quiet within freeze.quiet_max_age_seconds (lockout --hold keeps
it fresh and ends once s1_ran sees S1 done); S0 and the go decision check that pass as well, and
the S1 before-checks run once before the go, so a refusal that issues nothing leaves the
re-copy open. The printed forward block checks the privileges live, one check per account and
scope, where the kit reads the journal pass.
S3 attaches the live name READWRITE, so the application can write from that moment: a rollback
from the switched position first runs the zero-change proof (every planned table on the live
name holds its journal target_count; no pipeline runs on it, no write privilege the lockout
took is back and no foreign non-idle session is on it) and refuses with what differs. Rollback
is refused once O11 started releasing writers or C1 saw application writes, and resumes from
any position behind S1. Statements issued through the O10 owner guard: DETACH DATABASE (with
the KILL escalation), ATTACH DATABASE and, in the optional rollback step R3, DROP DATABASE on
the retained copy only.
"""
import sys

from . import ownersql, steps
from .common import EXIT_GATE, KitError, iso, utc_now
from .sqlutil import ident, qualified, read_guard

ATTACHED_SQL = ownersql.inventory_attachment_sql()
BRANCH_SQL = ('SELECT STORAGE_ID, DATABASE_NAME, IS_DETACHED, IS_ATTACHED_TO_THIS_COMPUTE, IS_ATTACHABLE, IS_DROPPED, PREV_DATABASE_NAME, '
              'LATEST_ATTACH_TIME FROM information_schema.MV_CLOUD_BRANCHES WHERE STORAGE_ID = %s')
LAYOUT_SQL = ('SELECT NUM_PARTITIONS, NUM_SUB_PARTITIONS, IS_SYNC, IS_SYNC_DURABILITY, IS_SYNC_DURABILITY_ON_REF_DB_ONLY '
              'FROM information_schema.DISTRIBUTED_DATABASES WHERE DATABASE_NAME = %s')
SESSIONS_SQL = ('SELECT COUNT(*) AS n FROM information_schema.PROCESSLIST WHERE (DB = %s OR INFO LIKE %s) AND INFO IS NOT NULL '
                'AND USER NOT IN (%s, %s)')
TABLE_COUNT_SQL = "SELECT COUNT(*) AS n FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'"
VIEW_COUNT_SQL = 'SELECT COUNT(*) AS n FROM information_schema.VIEWS WHERE TABLE_SCHEMA = %s'
PIPELINE_COUNT_SQL = 'SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s'
RUNNING_PIPELINES_SQL = "SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s AND STATE = 'Running'"
SWITCH_ALLOWANCE_SECONDS = 10 * 60

NOT_STARTED, DETACHED, RETAINED, SWITCHED, LIVE_DETACHED, UNKNOWN = (
    'not started', 'source detached', 'retained attached', 'switched', 'live branch detached', 'unknown')
FORWARD = ('S1', 'S2', 'S3')
ROLLBACK = ('R1', 'R2', 'R3')


def _rows(session, sql, params):
    return [dict(r) if isinstance(r, dict) else r for r in session.query(sql, params)]


def _known(value):
    return bool(value) and not ownersql.placeholders(str(value))


BRANCH_OF_SQL = ('SELECT COUNT(*) AS n FROM information_schema.MV_CLOUD_BRANCHES WHERE DATABASE_NAME = %s AND STORAGE_ID = %s '
                 'AND PREV_DATABASE_NAME = %s AND IS_DROPPED = 0 AND IS_DETACHED = 0')


def live_is_branch_of_replacement(session, names, live_att, replacement_att):
    """True when the live name sits on the replacement's STORAGE_ID as its own branch of it; issues the MV_CLOUD_BRANCHES COUNT.

    Helios 10.8.0 (October 27, 2026): ATTACH replacement AS live keeps the parent's STORAGE_ID.
    The branch is still independent (writes are not shared, and a DROP of the parent leaves it
    attached); it has its own COMPUTE_ID and a branch row whose PREV_DATABASE_NAME is the
    replacement. Both must hold.
    """
    if live_att is None or str(live_att.get('STORAGE_ID')) != str(names['replacement_sid']):
        return False
    if replacement_att is not None and str(replacement_att.get('COMPUTE_ID')) == str(live_att.get('COMPUTE_ID')):
        return False
    rows = _rows(session, BRANCH_OF_SQL, (names['source'], str(names['replacement_sid']), names['replacement']))
    return bool(rows) and int(rows[0].get('n') or 0) > 0


class Position:
    """The live attachment facts the machine reasons about."""

    def __init__(self, source_att, retained_att, replacement_att, source_branch, live_branch=None, live_is_branch=False):
        self.source_att = source_att
        self.retained_att = retained_att
        self.replacement_att = replacement_att
        self.source_branch = source_branch
        self.live_branch = live_branch
        self.live_is_branch = live_is_branch

    def state(self, names):
        """NOT_STARTED, DETACHED, RETAINED, SWITCHED, LIVE_DETACHED or UNKNOWN from the facts and the recorded storage ids.

        SWITCHED needs the live name attached READWRITE on a STORAGE_ID that is none of the
        source, replacement or retained ids (and equals the recorded live id once one exists),
        with the retained copy attached READONLY. The replacement's id counts when the live name
        is its own branch of the replacement (live_is_branch_of_replacement), as on Helios 10.8.0.
        """
        src_sid = str(names['source_sid'])
        retained_ok = bool(self.retained_att) and str(self.retained_att.get('ATTACH_TYPE')).upper() == 'READONLY'
        if self.source_att:
            sid = str(self.source_att.get('STORAGE_ID'))
            if sid == src_sid:
                return NOT_STARTED if not self.retained_att else UNKNOWN
            if not retained_ok or str(self.source_att.get('ATTACH_TYPE')).upper() != 'READWRITE':
                return UNKNOWN
            taken = {src_sid} | (set() if self.live_is_branch else {str(names['replacement_sid'])}) | (
                {str(names['retained_sid'])} if _known(names.get('retained_sid')) else set())
            if sid in taken:
                return UNKNOWN
            if _known(names.get('live_sid')) and sid != str(names['live_sid']):
                return UNKNOWN
            return SWITCHED
        if self.retained_att:
            if not retained_ok:
                return UNKNOWN
            if self.live_branch and str(self.live_branch.get('IS_DETACHED')) == '1':
                return LIVE_DETACHED
            return RETAINED
        if self.source_branch and str(self.source_branch.get('IS_DETACHED')) == '1':
            return DETACHED
        return UNKNOWN


def read_position(session, names):
    """Query the attachment views for the three names, the source storage and (once recorded) the live branch."""
    def _first(sql, params):
        rows = _rows(session, sql, params)
        return rows[0] if rows else None
    live = None
    if _known(names.get('live_sid')):
        rows = _rows(session, BRANCH_SQL, (names['live_sid'],))
        live = next((r for r in rows if r.get('DATABASE_NAME') == names['source']), rows[0] if rows else None)
    source_att, replacement_att = _first(ATTACHED_SQL, (names['source'],)), _first(ATTACHED_SQL, (names['replacement'],))
    branch = (source_att is not None and _known(names.get('replacement_sid')) and str(source_att.get('STORAGE_ID')) == str(names['replacement_sid'])
              and live_is_branch_of_replacement(session, names, source_att, replacement_att))
    return Position(source_att, _first(ATTACHED_SQL, (names['retained'],)), replacement_att, _first(BRANCH_SQL, (names['source_sid'],)), live,
                    live_is_branch=branch)


def _sessions_params(names):
    return (names['source'], '%' + names['source'] + '%', names['runner_user'], names.get('owner_user') or names['runner_user'])


def withheld_label(account, scope):
    """The S1 before check on Path B that an account holds none of the write privileges the lockout took on a scope."""
    return 'S1 before (Path B): %s holds none of the write privileges the lockout took on %s' % (account, scope)


def forward_block(names, counts=None, s0_problems=None, withheld=None):
    """The printed forward block S1 to S3 with every check and expected result (S0 is a decision).

    counts is {'tables', 'views', 'pipelines'} from the capture; each known count becomes a
    check after S3. Every S3 precondition is also checked before S1. s0_problems is
    s0_journal_problems of the run (block_inputs): when any is left the block starts with a STOP
    naming them, so the hand route and the notebook refuse what the kit route refuses at S0.
    withheld is the lockout snapshot not given back yet (ownersql.withheld_privileges): on Path B each
    account and scope of it gets a check before the DETACH that it holds none of those write
    privileges (expect n = 0), the live check that stands in for the kit's journal check of the
    last lockout pass, so a privilege another job gave back after the quiet proof stops the hand and notebook
    routes before S1 as the machine (s1_before) stops the kit route.
    """
    path_a = names.get('freeze_mode', 'readonly') == 'readonly'
    block = ownersql.Block('O10', 'Switch forward')
    if s0_problems:
        block.refuse('S0 refused: %s; no statement of this block may run; generate the block again (s2rebuild owner-sql O10 or '
                     'owner-notebook) once S0 passes' % '; '.join(s0_problems))
    block.note('S0: O8 passed; every planned table verified after freeze_time and after its last chunk, at its current re-copy epoch; '
               'O9 passed; time remaining covers the switch allowance; operator typed the live name.')
    block.check('S1 before: source attached with the recorded STORAGE_ID', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('column_equals', 'STORAGE_ID', names['source_sid']))
    block.check('S1 before: source attached %s' % ('READONLY (Path A)' if path_a else 'READWRITE with a passed quiet proof (Path B)'),
                ATTACHED_SQL, (names['source'],), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READONLY' if path_a else 'READWRITE'))
    block.check('S1 before: no non-idle session on the source other than ours', SESSIONS_SQL, _sessions_params(names),
                ownersql.Expect('column_equals', 'n', 0))
    if not path_a:
        checks = ownersql.withheld_checks(withheld or {}, names['source'])
        block.note('S1 before (Path B): the last lockout pass was quiet within %s s (freeze.quiet_max_age_seconds). The kit checks this '
                   'in the journal. This block checks the live state instead: the session check above, and one check per account and scope '
                   'that the account holds none of the write privileges the lockout took. When one fails, nothing of this block has run: take the '
                   'privilege away again with the statement of the O3 lockout plan (s2rebuild owner-sql O3), kill the sessions of that '
                   'account, then run this block again from the top.' % names.get('quiet_max_age_seconds', 60))
        if not checks:
            block.note('S1 before (Path B): the journal holds no lockout snapshot, so this block has no privilege check. When the lockout '
                       'took any privilege, record its snapshot (s2rebuild gate O3 --pass --note TEXT --evidence FILE with the output of '
                       'the lockout cell) and generate this block again.')
        for account, scope, sql, params in checks:
            block.check(withheld_label(account, scope), sql, params, ownersql.Expect('column_equals', 'n', 0))
    block.check('S1 before (for S3): replacement attached READWRITE with the recorded STORAGE_ID', ATTACHED_SQL, (names['replacement'],),
                ownersql.Expect('columns_equal', value={'STORAGE_ID': names['replacement_sid'], 'ATTACH_TYPE': 'READWRITE'}))
    block.check('S1 before (for S2): no attached database named retained', ATTACHED_SQL, (names['retained'],), ownersql.Expect('no_rows'))
    block.statement('DETACH DATABASE %s' % ident(names['source']),
                    'S1: detach the source (after %s s of waiting the kit cancels it and issues DETACH ... KILL%s)' % (
                        names.get('detach_wait_seconds', 120), '' if names.get('detach_kill', True) else '; detach_kill is off, so it stops instead'),
                    detach={'wait': names.get('detach_wait_seconds', 120), 'kill': bool(names.get('detach_kill', True))})
    block.check('S1 after: source storage detached', BRANCH_SQL, (names['source_sid'],), ownersql.Expect('column_equals', 'IS_DETACHED', 1))
    block.check('S1 after: no attached database named source', ATTACHED_SQL, (names['source'],), ownersql.Expect('no_rows'))
    block.check('S2 before: no attached database named retained', ATTACHED_SQL, (names['retained'],), ownersql.Expect('no_rows'))
    block.statement("ATTACH DATABASE %s AS %s READ ONLY STORAGEID %s" % (ident(names['source']), ident(names['retained']), names['source_sid']),
                    'S2: attach the original storage read only under the retained name')
    block.check('S2 after: retained attached READONLY', ATTACHED_SQL, (names['retained'],), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READONLY'),
                record='retained')
    block.note('S2 after: one small table count in the retained copy equals the journal source count (the kit checks this).')
    block.check('S3 before: replacement attached READWRITE with the recorded STORAGE_ID', ATTACHED_SQL, (names['replacement'],),
                ownersql.Expect('columns_equal', value={'STORAGE_ID': names['replacement_sid'], 'ATTACH_TYPE': 'READWRITE'}))
    block.check('S3 before: no attached database named source', ATTACHED_SQL, (names['source'],), ownersql.Expect('no_rows'))
    block.statement("ATTACH DATABASE %s AS %s STORAGEID %s" % (ident(names['replacement']), ident(names['source']), names['replacement_sid']),
                    'S3: attach the replacement as a branch under the live name')
    block.check('S3 after: an attached READWRITE database named source', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READWRITE'), record='live')
    block.check('S3 after: the live STORAGE_ID is a new branch', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('column_not_in', 'STORAGE_ID', [names['source_sid'], names['replacement_sid']]))
    block.check('S3 after: the live database reports the target layout', LAYOUT_SQL, (names['source'],),
                ownersql.Expect('columns_equal', value={'NUM_PARTITIONS': names['partitions'], 'NUM_SUB_PARTITIONS': names['sub_partitions']}))
    for label, sql, key in (('tables', TABLE_COUNT_SQL, 'tables'), ('views', VIEW_COUNT_SQL, 'views'), ('pipelines', PIPELINE_COUNT_SQL, 'pipelines')):
        if counts and counts.get(key) is not None:
            block.check('S3 after: %s count equals the capture' % label, sql, (names['source'],), ownersql.Expect('column_equals', 'n', counts[key]))
    if not counts:
        block.note('S3 after: no capture is available, so the table, view and pipeline counts are not checked here; the kit refuses O10 without one.')
    block.note('S3 after: the three smallest non-empty tables match the journal counts (the kit checks these and records the live STORAGE_ID).')
    block.note('The replacement stays attached under its own name until C3.')
    return block


def count_sql(db_name, table):
    """COUNT(*) of one table (the zero-change proof and the S2 and S3 small-table checks)."""
    return 'SELECT COUNT(*) AS n FROM ' + qualified(db_name, table)


def unproven_tables(table_counts):
    """The planned tables the zero-change proof cannot compare: those without a recorded target_count."""
    return [table for table, count in table_counts if count is None]


def rollback_block(names, released=False, app_writes_seen=None, withheld=None):
    """The rollback block R1 to R3, valid only before O11 released writers; a STOP item when it is no longer valid.

    Before R1 the zero-change proof: one COUNT(*) check per planned table of the live name
    against its journal target_count (names['table_counts'] from names_for), no pipeline Running
    on the live name, none of the write privileges the lockout took (withheld, the snapshot not
    given back yet) held again, then the S1 session query on the live name. The pipeline and
    privilege checks read the live state, so a block written before O11 still refuses once a
    hand or notebook O11 started a pipeline or gave a privilege back. A planned table without a
    target_count, or no planned table at all, is a STOP, since the live branch cannot be proven
    unchanged.
    """
    block = ownersql.Block('O10', 'Rollback R1 to R3 (before O11 only)')
    if released or app_writes_seen:
        block.refuse('rollback refused: %s; a rollback is now a new freeze and a reconciliation' % (
            'C1 saw application writes on the live branch at %s' % app_writes_seen if app_writes_seen else 'O11 released writers on the live name'))
    table_counts = names.get('table_counts') or []
    if not table_counts:
        block.refuse('rollback refused: no planned table count is known, so the live branch cannot be proven unchanged; reconcile first')
    elif unproven_tables(table_counts):
        block.refuse('rollback refused: no target count is recorded for %s, so the live branch cannot be proven unchanged; reconcile first'
                     % ', '.join(unproven_tables(table_counts)))
    block.note('By hand: when S1 or S2 ran but S3 did not, or R1 already ran, no database is attached under the live name; skip the '
               'zero-change proof and R1 and start at the R2 check. The notebook runs every item in order, so there the first proof check '
               'stops the block before any statement; finish from the R2 check of this block by hand.')
    block.note('Zero-change proof: S3 attached the live name READWRITE, so the application could write from then on. Every planned table must '
               'hold the row count of the copy, no pipeline may run on the live name, no write privilege the lockout took may be back, and '
               'no foreign session may be active; a failed check means reconcile first, never R1.')
    for table, count in table_counts:
        if count is not None:
            block.check('R1 before (zero-change proof): %s holds the copied row count' % table, count_sql(names['source'], table), (),
                        ownersql.Expect('column_equals', 'n', count))
    block.check('R1 before (zero-change proof): no pipeline Running on the live name', RUNNING_PIPELINES_SQL, (names['source'],),
                ownersql.Expect('column_equals', 'n', 0))
    for account, scope, sql, params in ownersql.withheld_checks(withheld or {}, names['source']):
        block.check('R1 before (zero-change proof): %s holds none of the write privileges the lockout took on %s' % (account, scope), sql, params,
                    ownersql.Expect('column_equals', 'n', 0))
    block.check('R1 before (zero-change proof): no non-idle session on the live name other than ours', SESSIONS_SQL, _sessions_params(names),
                ownersql.Expect('column_equals', 'n', 0))
    block.check('R1 before: the attached database named source is the live branch', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('column_equals', 'STORAGE_ID', names['live_sid']))
    block.statement('DETACH DATABASE %s' % ident(names['source']), 'R1: detach the live branch')
    block.check('R1 after: no attached database named source', ATTACHED_SQL, (names['source'],), ownersql.Expect('no_rows'))
    block.check('R2 before: the original storage is detached', BRANCH_SQL, (names['source_sid'],), ownersql.Expect('column_equals', 'IS_DETACHED', 1))
    block.statement("ATTACH DATABASE %s STORAGEID %s" % (ident(names['source']), names['source_sid']), 'R2: re-attach the original read write')
    block.check('R2 after: source attached READWRITE with the original STORAGE_ID', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('columns_equal', value={'STORAGE_ID': names['source_sid'], 'ATTACH_TYPE': 'READWRITE'}))
    block.check('R3 before: retained attached', ATTACHED_SQL, (names['retained'],), ownersql.Expect('some_rows'))
    block.statement('DROP DATABASE %s' % ident(names['retained']), 'R3 (optional): drop the retained branch')
    block.check('R3 after: retained gone', ATTACHED_SQL, (names['retained'],), ownersql.Expect('no_rows'))
    block.note('The detached live branch left by R1 is deleted in the Portal by STORAGE_ID %s.' % names['live_sid'])
    return block


def s1_ran(session, journal, source):
    """Why S1 of O10 has run, or None; s2rebuild lockout --hold calls it before every pass and ends the hold on it.

    The journal records the S1 DETACH as a switch_statement event (O10 may run in another
    process on the same journal file), and the attachment views show the source name off the
    recorded source STORAGE_ID, which a hand or notebook S1 leaves without a journal event.
    Reads only.
    """
    done = [d for d in journal.events('switch_statement') if d.startswith('S1 DETACH')]
    if done:
        return 'the journal records %s' % done[-1]
    recorded = (journal.attachment('source') or {}).get('storage_id')
    if not _known(recorded):
        return None
    rows = _rows(session, ATTACHED_SQL, (source,))
    if not rows:
        return 'no database is attached under the name %s' % source
    if str(rows[0].get('STORAGE_ID')) != str(recorded):
        return 'the name %s is attached on STORAGE_ID %s, not the source STORAGE_ID %s' % (source, rows[0].get('STORAGE_ID'), recorded)
    return None


def writers_released(journal):
    """True once O11 started releasing writers: release_started_utc or released_utc recorded, or O11 running or passed."""
    return bool(journal.get_meta('release_started_utc') or journal.get_meta('released_utc')) or journal.step_status('O11') in ('running', 'passed')


def rollback_refusal(journal):
    """The reason a rollback is refused, or None: O11 started or finished releasing writers, or C1 saw application writes."""
    if journal.get_meta('app_writes_seen'):
        return 'C1 recorded application writes on the live branch at %s' % journal.get_meta('app_writes_seen')
    if journal.get_meta('released_utc') or journal.step_status('O11') in ('running', 'passed'):
        return 'O11 released writers on the live name'
    if journal.get_meta('release_started_utc'):
        return ('O11 started releasing writers on the live name at %s (a started pipeline, or an account whose write privileges came back, may write to it)'
                % journal.get_meta('release_started_utc'))
    return None


class SwitchMachine:
    """Executes the switch one statement at a time, each behind its checks, from the live position.

    counts is {'tables', 'views', 'pipelines'} from the capture, layout the target layout,
    open_session(guard) and guard enable the DETACH escalation of S1. check_lockout_pass=False
    leaves out the Path B last pass check of S1; only the rehearsal, which does not rehearse the
    lockout, passes it.
    """

    def __init__(self, session, names, journal, log, runner_session=None, counts=None, layout=None, open_session=None, guard=None,
                 check_lockout_pass=True):
        self.session = session
        self.names = names
        self.journal = journal
        self.log = log
        self.runner = runner_session
        self.counts = counts or {}
        self.layout = layout
        self.open_session = open_session
        self.guard = guard
        self.check_lockout_pass = check_lockout_pass

    def position(self):
        """The current state name from the attachment views."""
        return read_position(self.session, self.names).state(self.names)

    def _expect(self, label, sql, params, expect, record=None):
        rows = _rows(self.session, sql, params)
        ok, observed = expect.evaluate(rows)
        self.journal.event('switch_check', '%s: %s (%s)' % (label, 'ok' if ok else 'FAILED', observed))
        if not ok:
            raise KitError('Switch refused at "%s": expected %s, observed %s' % (label, expect.text(), observed))
        self.log('    check %s: ok (%s)' % (label, observed))
        if record and rows:
            row = rows[0]
            self.journal.record_attachment(record, str(row.get('DATABASE_NAME')), row.get('STORAGE_ID'), row.get('COMPUTE_ID'), row.get('ATTACH_TYPE'))
            self.names[record + '_sid'] = row.get('STORAGE_ID')
        return rows

    def _run(self, sql, label):
        try:
            self.session.execute(sql)
        except KitError:
            raise
        except Exception as exc:  # noqa: BLE001 - a failed statement is reported with the observed position
            self.journal.event('switch_statement_failed', '%s: %s %s' % (label, type(exc).__name__, getattr(exc, 'args', ('',))[0]))
            try:
                where = self.position()
            except Exception:  # noqa: BLE001 - the position read may fail on the same broken connection
                where = UNKNOWN
            raise KitError('Switch statement "%s" failed: %s: %s; the switch is now at "%s"' % (label, type(exc).__name__, exc, where))
        self.journal.event('switch_statement', label)
        self.log('    executed %s' % label)

    def require(self, step, expected_state):
        """Refuse a step unless the live position is the one it starts from."""
        state = self.position()
        if state != expected_state:
            raise KitError('%s refused: the switch is at "%s", not "%s"; re-enter from the live state, never rerun blindly' % (step, state, expected_state))

    def preflight(self):
        """Before S1: every storage id recorded, the replacement attached READWRITE on its id, the retained name free."""
        n = self.names
        missing = [k for k in ('source_sid', 'replacement_sid') if not _known(n.get(k))]
        if missing:
            raise KitError('Switch refused before S1: %s not recorded (the replacement STORAGE_ID is recorded at O5)' % ', '.join(missing))
        self._expect('S1 before (for S3): replacement attached READWRITE with the recorded STORAGE_ID', ATTACHED_SQL, (n['replacement'],),
                     ownersql.Expect('columns_equal', value={'STORAGE_ID': n['replacement_sid'], 'ATTACH_TYPE': 'READWRITE'}))
        self._expect('S1 before (for S2): no attached database named retained', ATTACHED_SQL, (n['retained'],), ownersql.Expect('no_rows'))

    def s1_before(self):
        """The S1 before-checks, reads only: source on its recorded STORAGE_ID with the attach type of the freeze path, no foreign
        non-idle session, the S2 and S3 preconditions (preflight); on Path B no write privilege the lockout took held again
        and, last, the last lockout pass check.

        handle_o10 runs these before it records the go decision, so a refusal here issues nothing and leaves the re-copy open
        (Journal.switch_began); s1 runs them again just before the DETACH.
        """
        n = self.names
        readonly = n.get('freeze_mode', 'readonly') == 'readonly'
        self._expect('S1 before: source attached with the recorded STORAGE_ID', ATTACHED_SQL, (n['source'],), ownersql.Expect('column_equals', 'STORAGE_ID', n['source_sid']))
        self._expect('S1 before: source attach type', ATTACHED_SQL, (n['source'],), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READONLY' if readonly else 'READWRITE'))
        self._expect('S1 before: no foreign non-idle session', SESSIONS_SQL, _sessions_params(n), ownersql.Expect('column_equals', 'n', 0))
        self.preflight()
        if not readonly and self.check_lockout_pass:
            for account, scope, sql, params in ownersql.withheld_checks(ownersql.withheld_privileges(self.journal), n['source']):
                self._expect(withheld_label(account, scope), sql, params, ownersql.Expect('column_equals', 'n', 0))
            self._last_pass_check()

    def s1(self, detach_kill=None):
        """S1: detach the source after its checks (and the S2 and S3 preconditions); on Path B the last lockout pass check comes last."""
        n = self.names
        self.require('S1', NOT_STARTED)
        self.s1_before()
        kill = n.get('detach_kill', True) if detach_kill is None else detach_kill
        if self.open_session is not None and self.guard is not None:
            form = ownersql.detach_with_escalation(self.open_session, self.guard, n['source'], int(n.get('detach_wait_seconds', 120)), bool(kill),
                                                   self.log, self.journal)
            self.journal.event('switch_statement', 'S1 DETACH source (%s)' % form)
        else:
            self._run('DETACH DATABASE %s' % ident(n['source']), 'S1 DETACH source')
        self._expect('S1 after: source storage detached', BRANCH_SQL, (n['source_sid'],), ownersql.Expect('column_equals', 'IS_DETACHED', 1))
        self._expect('S1 after: no attached database named source', ATTACHED_SQL, (n['source'],), ownersql.Expect('no_rows'))

    def _last_pass_check(self):
        """S1 before on Path B: the last lockout pass in the journal was quiet within freeze.quiet_max_age_seconds (lockout.last_pass_problem)."""
        from . import lockout
        max_age = int(self.names.get('quiet_max_age_seconds', 60))
        label = 'S1 before (Path B): the last lockout pass was quiet within %d s' % max_age
        problem = lockout.last_pass_problem(self.journal, max_age)
        self.journal.event('switch_check', '%s: %s' % (label, 'FAILED (%s)' % problem if problem else 'ok'))
        if problem:
            raise KitError('Switch refused at "%s": %s; nothing was issued' % (label, problem))
        self.log('    check %s: ok' % label)

    def s2(self):
        """S2: attach the original storage read only under the retained name, then its after-checks."""
        n = self.names
        self.require('S2', DETACHED)
        self._expect('S2 before: no attached database named retained', ATTACHED_SQL, (n['retained'],), ownersql.Expect('no_rows'))
        self._run("ATTACH DATABASE %s AS %s READ ONLY STORAGEID %s" % (ident(n['source']), ident(n['retained']), n['source_sid']), 'S2 ATTACH retained READ ONLY')
        self.s2_after()

    def s2_after(self):
        """The S2 after-checks: retained attached READONLY (recorded) and one small table count equal to the journal."""
        n = self.names
        self._expect('S2 after: retained attached READONLY', ATTACHED_SQL, (n['retained'],), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READONLY'), record='retained')
        self._small_table_check(n['retained'], 1)

    def s3(self):
        """S3: rerun the S2 after-checks, attach the replacement as a branch under the live name, then the S3 after-checks."""
        n = self.names
        self.require('S3', RETAINED)
        self.s2_after()
        self._expect('S3 before: replacement attached READWRITE with the recorded STORAGE_ID', ATTACHED_SQL, (n['replacement'],),
                     ownersql.Expect('columns_equal', value={'STORAGE_ID': n['replacement_sid'], 'ATTACH_TYPE': 'READWRITE'}))
        self._expect('S3 before: no attached database named source', ATTACHED_SQL, (n['source'],), ownersql.Expect('no_rows'))
        self._run("ATTACH DATABASE %s AS %s STORAGEID %s" % (ident(n['replacement']), ident(n['source']), n['replacement_sid']), 'S3 ATTACH replacement AS source')
        self.s3_after()

    def s3_after(self):
        """The S3 after-checks: live READWRITE on a new branch (recorded), target layout, capture counts, three smallest tables."""
        n = self.names
        rows = self._expect('S3 after: an attached READWRITE database named source', ATTACHED_SQL, (n['source'],),
                            ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READWRITE'), record='live')
        taken = [str(n['source_sid']), str(n['replacement_sid'])] + ([str(n['retained_sid'])] if _known(n.get('retained_sid')) else [])
        sid = str(rows[0].get('STORAGE_ID'))
        if sid in taken:
            replacement_rows = _rows(self.session, ATTACHED_SQL, (n['replacement'],))
            if not live_is_branch_of_replacement(self.session, n, rows[0], replacement_rows[0] if replacement_rows else None):
                raise KitError('Switch refused after S3: the live name resolves to STORAGE_ID %s, which is not a new branch' % sid)
            label = 'S3 after: the live name is its own branch of the replacement (same STORAGE_ID, own COMPUTE_ID, PREV_DATABASE_NAME %s)' % n['replacement']
            self.journal.event('switch_check', '%s: ok' % label)
            self.log('    check %s: ok' % label)
        if self.layout is not None:
            self._expect('S3 after: the live database reports the target layout', LAYOUT_SQL, (n['source'],),
                         ownersql.Expect('columns_equal', value={'NUM_PARTITIONS': self.layout.partitions, 'NUM_SUB_PARTITIONS': self.layout.sub_partitions}))
        for label, sql, key in (('tables', TABLE_COUNT_SQL, 'tables'), ('views', VIEW_COUNT_SQL, 'views'), ('pipelines', PIPELINE_COUNT_SQL, 'pipelines')):
            if self.counts.get(key) is not None:
                self._expect('S3 after: %s count equals the capture' % label, sql, (n['source'],), ownersql.Expect('column_equals', 'n', self.counts[key]))
        self._small_table_check(n['source'], 3)
        if not self.journal.get_meta('switched_utc'):
            self.journal.set_meta('switched_utc', iso(utc_now()))

    def _small_table_check(self, db_name, how_many):
        """COUNT(*) of the smallest non-empty tables against the journal's source counts (runner session)."""
        if self.runner is None:
            return
        rows = [t for t in self.journal.tables() if t.get('source_count')]
        rows.sort(key=lambda t: (t['source_count'], t['table_name']))
        for t in rows[:how_many]:
            got = _rows(self.runner, count_sql(db_name, t['table_name']), None)
            count = int(got[0]['n']) if got else -1
            if count != int(t['source_count']):
                raise KitError('Switch refused: %s.%s holds %d rows, the journal says %s' % (db_name, t['table_name'], count, t['source_count']))
            self.log('    check %s.%s count %d matches the journal' % (db_name, t['table_name'], count))

    def forward(self, detach_kill=None):
        """Run the remaining forward steps from the live position, rerunning the after-checks on re-entry; returns the final state."""
        state = self.position()
        if state == LIVE_DETACHED:
            raise KitError('Switch cannot continue: a rollback is in progress (R1 ran); finish it with the rollback, then start a new switch')
        if state == NOT_STARTED:
            self.s1(detach_kill)
            state = DETACHED
        if state == DETACHED:
            self.s2()
            state = RETAINED
        if state == RETAINED:
            self.s3()
            return SWITCHED
        if state == SWITCHED:
            self.log('    the live name already resolves to a new branch; rerunning the S3 after-checks')
            self.s3_after()
            return SWITCHED
        raise KitError('Switch cannot continue from state "%s"; inspect the attachments before any further statement' % state)

    def zero_change_proof(self):
        """Before R1: every planned table on the live name holds its journal target_count, and nothing can write to it.

        The counts run through the runner session (the owner session when the machine has no
        runner). Through the owner session: no pipeline Running on the live name, none of the
        write privileges the lockout took held again (the journal snapshot not given back yet),
        and the S1 session query on the live name. These live checks refuse even when the journal
        does not know that O11 began (a hand or notebook O11). A planned table without a
        target_count cannot be proven. Any difference refuses with the list, before any
        statement; the result is journaled as rollback_proof or rollback_refused. Issues reads only.
        """
        n = self.names
        reader = self.runner if self.runner is not None else self.session
        table_counts = ownersql.table_counts(self.journal)
        problems = [] if table_counts else ['the journal holds no planned table to compare']
        problems += ['%s has no recorded target count' % table for table in unproven_tables(table_counts)]
        for table, want in table_counts:
            if want is None:
                continue
            try:
                got = _rows(reader, count_sql(n['source'], table), None)
            except KitError:
                raise
            except Exception as exc:  # noqa: BLE001 - a table that cannot be counted is a difference, not a crash
                problems.append('%s could not be counted (%s)' % (table, type(exc).__name__))
                continue
            count = int(got[0]['n']) if got else None
            if count != int(want):
                problems.append('%s holds %s rows, the copy had %s' % (table, 'no' if count is None else count, want))
            else:
                self.log('    check %s.%s count %d equals the copy' % (n['source'], table, count))
        running = _rows(self.session, RUNNING_PIPELINES_SQL, (n['source'],))
        started = int(running[0]['n']) if running else 0
        if started:
            problems.append('%d pipeline%s Running on the live name' % (started, '' if started == 1 else 's'))
        for account, scope, sql, params in ownersql.withheld_checks(ownersql.withheld_privileges(self.journal), n['source']):
            held = _rows(self.session, sql, params)
            back = int(held[0]['n']) if held else 0
            if back:
                problems.append('%s holds %d of the write privileges the lockout took on %s' % (account, back, scope))
        sessions = _rows(self.session, SESSIONS_SQL, _sessions_params(n))
        busy = int(sessions[0]['n']) if sessions else 0
        if busy:
            problems.append('%d foreign non-idle session%s on the live name' % (busy, '' if busy == 1 else 's'))
        if problems:
            self.journal.event('rollback_refused', 'zero-change proof failed: ' + '; '.join(problems))
            raise KitError('Rollback refused before R1: the zero-change proof failed on the live name %s, so the application or O11 may have '
                           'written to it since S3; reconcile first (a new freeze and a reconciliation), then decide. Differences: %s'
                           % (n['source'], '; '.join(problems)))
        done = ('zero-change proof passed on %d tables; no pipeline Running, no lockout privilege back and no foreign non-idle session '
                'on the live name' % len(table_counts))
        self.journal.event('rollback_proof', done)
        self.log('    ' + done)

    def rollback(self, drop_retained=False):
        """R1 to R3 from the live position; refused once O11 started releasing writers or application writes were seen.

        From the switched position the zero-change proof runs first; behind S3 (source detached
        or retained attached) nothing is attached under the live name, so R2 runs without it.
        """
        n = self.names
        reason = rollback_refusal(self.journal)
        if reason:
            raise KitError('Rollback refused: %s; a rollback is now a new freeze and reconciliation' % reason)
        state = self.position()
        if state == NOT_STARTED:
            self.log('    nothing to roll back: the source is attached on its original storage')
            return NOT_STARTED
        if state == UNKNOWN:
            raise KitError('Rollback refused: the switch is at "unknown"; inspect the attachments before any further statement')
        if state == SWITCHED:
            self.zero_change_proof()
        self.journal.set_meta('rollback_started_utc', self.journal.get_meta('rollback_started_utc') or iso(utc_now()))
        if state == SWITCHED:
            self._expect('R1 before: the attached database named source is the live branch', ATTACHED_SQL, (n['source'],),
                         ownersql.Expect('column_equals', 'STORAGE_ID', n['live_sid']))
            self._run('DETACH DATABASE %s' % ident(n['source']), 'R1 DETACH live branch')
            self._expect('R1 after: no attached database named source', ATTACHED_SQL, (n['source'],), ownersql.Expect('no_rows'))
        self._expect('R2 before: the original storage is detached', BRANCH_SQL, (n['source_sid'],), ownersql.Expect('column_equals', 'IS_DETACHED', 1))
        self._expect('R2 before: no attached database named source', ATTACHED_SQL, (n['source'],), ownersql.Expect('no_rows'))
        self._run("ATTACH DATABASE %s STORAGEID %s" % (ident(n['source']), n['source_sid']), 'R2 ATTACH original read write')
        self._expect('R2 after: source attached READWRITE with the original STORAGE_ID', ATTACHED_SQL, (n['source'],),
                     ownersql.Expect('columns_equal', value={'STORAGE_ID': n['source_sid'], 'ATTACH_TYPE': 'READWRITE'}))
        if drop_retained and _rows(self.session, ATTACHED_SQL, (n['retained'],)):
            self._run('DROP DATABASE %s' % ident(n['retained']), 'R3 DROP retained')
            self._expect('R3 after: retained gone', ATTACHED_SQL, (n['retained'],), ownersql.Expect('no_rows'))
        self.journal.set_meta('rolled_back_utc', iso(utc_now()))
        self.journal.event('rollback', 'original %s re-attached READWRITE on STORAGE_ID %s' % (n['source'], n['source_sid']))
        return NOT_STARTED


def _unverified(journal):
    """[(table state, reason)] for the planned tables S0 cannot accept (unverified_tables gives the text)."""
    out = []
    for t in journal.verification_state():
        stamp = t.get('verified_at_utc')
        if not stamp:
            out.append((t, 'no verified_at_utc'))
        elif t.get('chunks_open'):
            out.append((t, '%d chunk%s not done' % (t['chunks_open'], '' if t['chunks_open'] == 1 else 's')))
        elif t.get('last_chunk_end') and stamp < t['last_chunk_end']:
            out.append((t, 'verified %s, before its last chunk ended %s' % (stamp, t['last_chunk_end'])))
        elif not t.get('epoch_verified'):
            out.append((t, 'no matching verification at re-copy epoch %d' % int(t.get('recopy_epoch') or 0)))
    return out


def unverified_tables(journal):
    """The planned tables S0 cannot accept, each with its reason; [] when every table is verified on the rows now copied.

    A table passes when it carries verified_at_utc, every chunk is done, the stamp is not earlier
    than the newest chunk ended_utc (times have second precision, as the freeze_time rule), and
    a match or sampled verification row carries the table's current recopy_epoch. A re-copy
    clears the stamp and increments the epoch, so only a verification after it counts.
    """
    return ['%s (%s)' % (t['table_name'], reason) for t, reason in _unverified(journal)]


def reverify_hint(journal, unverified=None):
    """How the operator verifies the copy again, from the step statuses and the chunk times of the unverified tables.

    run --from O6 runs every step a re-copy reopened (a passed step is skipped), so it is the hint
    while O7 is not passed. step O8 --redo is the hint only when O7 passed after the newest chunk
    of those tables, so the counters and the stale check already follow the rows. Otherwise
    (O7 passed before the copy last changed, or O6 passed with a chunk not done) the hint is
    each step again with --redo. Reads the journal only.
    """
    rows = [t for t, _ in (_unverified(journal) if unverified is None else unverified)]
    newest = max((t.get('last_chunk_end') or '' for t in rows), default='')
    open_chunks = any(t.get('chunks_open') for t in rows)
    o6, o7 = journal.step('O6') or {}, journal.step('O7') or {}
    if o7.get('status') != 'passed' and not (open_chunks and o6.get('status') == 'passed'):
        return 'rerun O6 to O8 (s2rebuild run --from O6)'
    if not open_chunks and o7.get('status') == 'passed' and (o7.get('ended_utc') or '') >= newest:
        return 'rerun O8 (s2rebuild step O8 --redo): O7 passed after the last chunk, so only the verification is missing'
    return ('rerun O6, O7 and O8 each with --redo (s2rebuild step O6 --redo, then step O7 --redo, then step O8 --redo): %s'
            % ('O6 passed with a chunk not done' if open_chunks else 'O7 passed before the copy last changed'))


def s0_journal_problems(journal):
    """The part of S0 the journal alone decides, the same on every route (kit, printed block, notebook, gate O10 --pass).

    O8 passed, with every step before it passed or skipped (a result counts only while the steps
    before it are passed, as run_step requires); freeze_time recorded; a planned table exists;
    every planned table verified on the rows now copied (unverified_tables), with the way to
    verify again; no stamp before freeze_time. O9 and the clock are left to the kit route and the
    operator, since a notebook is generated before its O9 cell runs. Returns the problems, []
    when this part passes.
    """
    problems = []
    if journal.step_status('O8') != 'passed':
        problems.append('O8 has not passed')
    else:
        earlier = steps.unmet_preconditions(steps.REGISTRY['O8'], journal)
        if earlier:
            hint = steps.missing_hint(earlier, restart=False)
            problems.append('O8 passed, but steps before it are not passed: %s; %s' % (', '.join(earlier), hint[:1].lower() + hint[1:-1]))
    freeze_time = journal.get_meta('freeze_time')
    planned = journal.tables()
    verified = [t['verified_at_utc'] for t in planned if t.get('verified_at_utc')]
    if not freeze_time:
        problems.append('no freeze_time is recorded (run s2rebuild step O3 --redo to run the write probe and record it)')
    if not planned:
        problems.append('the journal holds no planned table')
    unverified = _unverified(journal)
    if unverified:
        problems.append('not verified on the rows now copied: %s; %s' % (
            ', '.join('%s (%s)' % (t['table_name'], reason) for t, reason in unverified), reverify_hint(journal, unverified)))
    if freeze_time and verified and min(verified) < freeze_time:
        problems.append('a table was verified (%s) before the freeze (%s)' % (min(verified), freeze_time))
    return problems


def s0_checks(ctx, names, position=NOT_STARTED):
    """S0: the decision gate before the first statement; returns the lines, raises KitError when something is missing.

    O9 passed, then s0_journal_problems: O8 passed, freeze_time (step O3 --redo records it after
    a notebook or hand run), and every planned table verified after freeze_time and after its
    last chunk, at its current re-copy epoch; a table without a stamp refuses, it is never left
    out. Both storage ids recorded. The time check uses the abort deadline (O1 record, else the
    window) and applies only before S1, and so does the Path B check of the last lockout pass
    (lockout_pass_problems), which S1 checks again. Reads the journal only.
    """
    from . import copy as copy_module
    journal = ctx.journal
    problems = []
    if journal.step_status('O9') != 'passed':
        problems.append('O9 has not passed')
    problems.extend(s0_journal_problems(journal))
    problems.extend(lockout_pass_problems(journal, path_b_pass_age(names, position)))
    freeze_time = journal.get_meta('freeze_time')
    verified = [t['verified_at_utc'] for t in journal.tables() if t.get('verified_at_utc')]
    for key in ('source_sid', 'replacement_sid'):
        if not _known(names.get(key)):
            problems.append('%s is not recorded' % key)
    deadline = copy_module.resolve_abort_at(journal, ctx.config)
    remaining = None
    if deadline is not None:
        remaining = (deadline - utc_now()).total_seconds()
        if position == NOT_STARTED and remaining < SWITCH_ALLOWANCE_SECONDS:
            problems.append('less than the %d minute switch allowance remains before the abort deadline %s' % (SWITCH_ALLOWANCE_SECONDS // 60, iso(deadline)))
    if problems:
        raise KitError('S0 refused: ' + '; '.join(problems))
    return ['S0: O8 and O9 passed; every planned table verified after its last chunk at its re-copy epoch; oldest verified_at %s after '
            'freeze_time %s' % (min(verified), freeze_time),
            'S0: time remaining before the abort deadline: %s' % (('%d min' % (remaining / 60)) if remaining is not None else 'no deadline recorded'),
            'S0: position %s; live name %s, retained %s, replacement %s' % (position, names['source'], names['retained'], names['replacement'])]


def _typed_live_name(ctx, live):
    """The live name the operator typed at the terminal, or None when no terminal is attached."""
    confirm = ctx.options.get('confirm')
    if confirm is not None:
        return confirm('Type the live name %s to confirm the switch: ' % live)
    if sys.stdin is not None and sys.stdin.isatty():
        return input('Type the live name %s to confirm the switch: ' % live).strip()
    return None


def record_replacement(ctx, session):
    """Record the replacement attachment (STORAGE_ID) when the journal has none; returns the storage id or None."""
    current = ctx.journal.attachment('replacement')
    if current and current.get('storage_id'):
        return current['storage_id']
    rows = _rows(session, ATTACHED_SQL, (ctx.db.replacement,))
    if not rows:
        return None
    r = rows[0]
    ctx.journal.record_attachment('replacement', ctx.db.replacement, r.get('STORAGE_ID'), r.get('COMPUTE_ID'), r.get('ATTACH_TYPE'))
    return r.get('STORAGE_ID')


def path_b_pass_age(names, position):
    """freeze.quiet_max_age_seconds when S1 is still ahead on Path B (the last lockout pass decides it), else None."""
    if names.get('freeze_mode', 'readonly') == 'readonly' or position != NOT_STARTED:
        return None
    return int(names.get('quiet_max_age_seconds', 60))


def lockout_pass_problems(journal, max_age_seconds):
    """[the Path B last lockout pass problem] while S1 is ahead (max_age_seconds from path_b_pass_age), else []. Reads the journal only.

    The same check as SwitchMachine._last_pass_check, made in S0 and in the go transaction, so an
    O10 that would refuse at S1 for a missing, stale or not quiet pass refuses before the go is
    recorded and the re-copy stays open (Journal.switch_began). Behind S1 (a re-entry after the
    DETACH, when the hold has ended) it does not apply.
    """
    if max_age_seconds is None:
        return []
    from . import lockout
    problem = lockout.last_pass_problem(journal, max_age_seconds)
    return ['S1 before (Path B): the last lockout pass was not quiet within %d s: %s' % (max_age_seconds, problem)] if problem else []


def go_problems(journal, lockout_max_age_seconds=None):
    """What refuses the go decision as it is recorded; Journal.record_go reads it in the transaction that writes switch_go.

    O6, O7 or O8 running (another process started it after S0), O9 not passed, then
    s0_journal_problems, and on Path B before S1 the last lockout pass (lockout_pass_problems with
    lockout_max_age_seconds from path_b_pass_age). So a re-copy marked, a fresh-table load or a
    verification cleared while the operator typed the live name refuses the go with nothing
    issued, and so does a lockout pass that went stale or found work meanwhile. Reads the journal only.
    """
    problems = ['%s is running (another process started it after S0)' % step_id
                for step_id in steps.AFTER_SWITCH_REFUSED if journal.step_status(step_id) == 'running']
    if journal.step_status('O9') != 'passed':
        problems.append('O9 has not passed')
    return problems + s0_journal_problems(journal) + lockout_pass_problems(journal, lockout_max_age_seconds)


def handle_o10(step, ctx):
    """O10 handler: print both blocks; with the owner profile run S0, the typed go decision, then the machine.

    Without the owner profile (and not a dry run) it refuses while s0_journal_problems holds; the
    printed forward block then starts with the same STOP. A real run holds the copy lock of the
    journal (copy.copy_lock) from its first read through the forward machine, so no copy, O7
    re-copy round or mark-recopy of another process runs while O10 prints its blocks, decides S0,
    waits for the typed name and switches. The go decision is recorded by Journal.record_go,
    which reads go_problems in the same transaction, so a re-copy that reached the journal while
    the operator typed refuses the go with nothing issued. Every refusal that issues nothing comes
    before the go: S0 (on Path B with the last lockout pass), the S1 before-checks of the machine
    (s1_before, live reads) just before record_go, and go_problems inside it; so such a refusal
    leaves the re-copy open, and the machine checks S1 again before its DETACH.
    """
    from . import copy as copy_module
    with copy_module.copy_lock(None if ctx.dry_run else ctx.journal_path, 'O10'):
        return _o10(step, ctx)


def _o10(step, ctx):
    """The body of handle_o10, run while the copy lock is held."""
    if not ctx.dry_run:
        reader = ctx.runner_session(read_guard('runner O10'))
        try:
            record_replacement(ctx, reader)
        finally:
            reader.close()
    names = ownersql.names_for(ctx)
    counts = ownersql.expected_counts(ctx)
    inputs = ownersql.block_inputs(ctx)
    block = ownersql.block_for('O10', names, ctx.config, **inputs)  # the forward block, with the Path B privilege checks
    ownersql.guard_check_block(block, ctx)
    ownersql.print_block(block, ctx, ownersql.owner_sql_path(ctx, 'O10'))
    ownersql.print_block(ownersql.block_for('O10', names, ctx.config, rollback=True, **inputs), ctx, ownersql.owner_sql_path(ctx, 'O10', rollback=True))
    if ctx.dry_run or not ctx.owner_configured:
        if inputs['s0_problems'] and not ctx.dry_run:
            # the print route refuses what the kit route refuses at S0; the printed block starts with the same STOP
            return steps.Outcome(steps.REFUSED, 'S0 refused: %s; the printed forward block starts with a STOP, run none of it' % (
                '; '.join(inputs['s0_problems'])), 1)
        lines = ['identity: owner', 'dry run: the forward block above is what the owner profile would execute'
                 if ctx.dry_run else 'no owner profile: run the forward block as the admin, then record the gate']
        if block.refusals:
            lines.append('the block holds a STOP: ' + '; '.join(block.refusals))
        return steps.gate_pending(step, ctx, lines)
    if counts is None:
        return steps.Outcome(steps.REFUSED, 'O10 needs the capture (O4 or P3) for the S3 table, view and pipeline counts; none was found', 1)
    guard = steps.guard_for('O10', ctx.db, steps.OWNER)
    owner = ctx.owner_session(guard)
    runner = ctx.runner_session(steps.guard_for('O8', ctx.db))
    try:
        machine = SwitchMachine(owner, names, ctx.journal, ctx.log, runner, counts, ctx.db.layout, open_session=ctx.owner_session, guard=guard)
        position = machine.position()
        if position != NOT_STARTED and not ctx.journal.switch_began():
            # the live state shows a switch under way (a hand switch, or one the journal missed); recorded before anything can stop
            # this run, so a re-copy is refused even when the run stops before its go decision (Journal.switch_began)
            ctx.journal.event('switch_observed', 'O10 read the position "%s" from the attachment views before the go' % position)
        for line in s0_checks(ctx, names, position):
            ctx.log('    ' + line)
        if not ctx.yes:
            typed = _typed_live_name(ctx, names['source'])
            if typed is None:
                return steps.Outcome(steps.PENDING, 'S0: rerun step O10 --yes%s, or at a terminal type the live name %s, to confirm the go decision' % (
                    steps.run_flag(ctx.run_id), names['source']), EXIT_GATE)
            if typed != names['source']:
                return steps.Outcome(steps.REFUSED, 'S0: the typed name does not equal the live name %s; nothing was issued' % names['source'], 1)
        if position == NOT_STARTED:
            # the S1 before-checks issue nothing, so they run before the go: a refusal there leaves the re-copy open (Journal.switch_began),
            # and S1 runs them again before the DETACH. The position is read again first, so a hand S1 made while the operator typed
            # is recorded as observed, never refused unseen.
            position = machine.position()
            if position == NOT_STARTED:
                machine.s1_before()
            elif not ctx.journal.switch_began():
                ctx.journal.event('switch_observed', 'O10 read the position "%s" from the attachment views before the go' % position)
        # S0's journal part again, in the transaction that records the go: the operator may have typed for minutes; on Path B the last
        # lockout pass too while S1 is ahead
        age = path_b_pass_age(names, position)
        ctx.journal.record_go('go recorded for live name %s at position %s%s' % (names['source'], position, ' (--yes)' if ctx.yes else ' (typed)'),
                              lambda journal: go_problems(journal, age))
        machine.forward()
    finally:
        owner.close()
        runner.close()
    return steps.Outcome(steps.PASSED, 'switch complete: live name %s resolves to STORAGE_ID %s; retained %s' % (
        names['source'], names.get('live_sid'), names.get('retained_sid')))


steps.register('O10', handle_o10)
