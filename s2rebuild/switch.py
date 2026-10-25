"""s2rebuild switch: the O10 state machine over the attachment views, forward S0 to S3 and rollback R1 to R3.

Author: John Bergamini

Section 10.13 of the architecture. The position is computed from MV_CLOUD_ATTACHED_DATABASES
and MV_CLOUD_BRANCHES, never from memory, so a re-entered step continues from the live state;
re-entry reruns the after-checks of the step it lands behind (S2 before S3, S3 after a failed
S3 check), so a failed check can never turn into a passed O10. Every statement sits between a
check before and a check after; an out-of-order statement is refused, not executed. Every S3
precondition (replacement attached READWRITE on its recorded STORAGE_ID, retained name free) is
checked before S1, so the source is never detached for a switch that cannot finish. Rollback is
refused once O11 released writers or C1 saw application writes, and resumes from any position
behind S1. Statements issued through the O10 owner guard: DETACH DATABASE (with the KILL
escalation), ATTACH DATABASE and, in the optional rollback step R3, DROP DATABASE on the
retained copy only.
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
SWITCH_ALLOWANCE_SECONDS = 10 * 60

NOT_STARTED, DETACHED, RETAINED, SWITCHED, LIVE_DETACHED, UNKNOWN = (
    'not started', 'source detached', 'retained attached', 'switched', 'live branch detached', 'unknown')
FORWARD = ('S1', 'S2', 'S3')
ROLLBACK = ('R1', 'R2', 'R3')


def _rows(session, sql, params):
    return [dict(r) if isinstance(r, dict) else r for r in session.query(sql, params)]


def _known(value):
    return bool(value) and not ownersql.placeholders(str(value))


class Position:
    """The live attachment facts the machine reasons about."""

    def __init__(self, source_att, retained_att, replacement_att, source_branch, live_branch=None):
        self.source_att = source_att
        self.retained_att = retained_att
        self.replacement_att = replacement_att
        self.source_branch = source_branch
        self.live_branch = live_branch

    def state(self, names):
        """NOT_STARTED, DETACHED, RETAINED, SWITCHED, LIVE_DETACHED or UNKNOWN from the facts and the recorded storage ids.

        SWITCHED needs the live name attached READWRITE on a STORAGE_ID that is none of the
        source, replacement or retained ids (and equals the recorded live id once one exists),
        with the retained copy attached READONLY.
        """
        src_sid = str(names['source_sid'])
        retained_ok = bool(self.retained_att) and str(self.retained_att.get('ATTACH_TYPE')).upper() == 'READONLY'
        if self.source_att:
            sid = str(self.source_att.get('STORAGE_ID'))
            if sid == src_sid:
                return NOT_STARTED if not self.retained_att else UNKNOWN
            if not retained_ok or str(self.source_att.get('ATTACH_TYPE')).upper() != 'READWRITE':
                return UNKNOWN
            taken = {src_sid, str(names['replacement_sid'])} | ({str(names['retained_sid'])} if _known(names.get('retained_sid')) else set())
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
    live = _first(BRANCH_SQL, (names['live_sid'],)) if _known(names.get('live_sid')) else None
    return Position(_first(ATTACHED_SQL, (names['source'],)), _first(ATTACHED_SQL, (names['retained'],)),
                    _first(ATTACHED_SQL, (names['replacement'],)), _first(BRANCH_SQL, (names['source_sid'],)), live)


def _sessions_params(names):
    return (names['source'], '%' + names['source'] + '%', names['runner_user'], names.get('owner_user') or names['runner_user'])


def forward_block(names, counts=None):
    """The printed forward block S1 to S3 with every check and expected result (S0 is a decision).

    counts is {'tables', 'views', 'pipelines'} from the capture; each known count becomes a
    check after S3. Every S3 precondition is also checked before S1.
    """
    path_a = names.get('freeze_mode', 'readonly') == 'readonly'
    block = ownersql.Block('O10', 'Switch forward')
    block.note('S0: O8 passed with verified_at after freeze_time; O9 passed; time remaining covers the switch allowance; operator typed the live name.')
    block.check('S1 before: source attached with the recorded STORAGE_ID', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('column_equals', 'STORAGE_ID', names['source_sid']))
    block.check('S1 before: source attached %s' % ('READONLY (Path A)' if path_a else 'READWRITE with a passed quiet proof (Path B)'),
                ATTACHED_SQL, (names['source'],), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READONLY' if path_a else 'READWRITE'))
    block.check('S1 before: no non-idle session on the source other than ours', SESSIONS_SQL, _sessions_params(names),
                ownersql.Expect('column_equals', 'n', 0))
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
    block.statement("ATTACH DATABASE %s AS %s STORAGEID '%s' READ ONLY" % (ident(names['source']), ident(names['retained']), names['source_sid']),
                    'S2: attach the original storage read only under the retained name')
    block.check('S2 after: retained attached READONLY', ATTACHED_SQL, (names['retained'],), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READONLY'),
                record='retained')
    block.note('S2 after: one small table count in the retained copy equals the journal source count (the kit checks this).')
    block.check('S3 before: replacement attached READWRITE with the recorded STORAGE_ID', ATTACHED_SQL, (names['replacement'],),
                ownersql.Expect('columns_equal', value={'STORAGE_ID': names['replacement_sid'], 'ATTACH_TYPE': 'READWRITE'}))
    block.check('S3 before: no attached database named source', ATTACHED_SQL, (names['source'],), ownersql.Expect('no_rows'))
    block.statement("ATTACH DATABASE %s AS %s STORAGEID '%s'" % (ident(names['replacement']), ident(names['source']), names['replacement_sid']),
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


def rollback_block(names, released=False, app_writes_seen=None):
    """The rollback block R1 to R3, valid only before O11 released writers; a STOP item when it is no longer valid."""
    block = ownersql.Block('O10', 'Rollback R1 to R3 (before O11 only)')
    if released or app_writes_seen:
        block.refuse('rollback refused: %s; a rollback is now a new freeze and a reconciliation' % (
            'C1 saw application writes on the live branch at %s' % app_writes_seen if app_writes_seen else 'O11 released writers on the live name'))
    block.note('When S1 or S2 ran but S3 did not, no database is attached under the live name: skip R1 and start at R2.')
    block.check('R1 before: the attached database named source is the live branch', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('column_equals', 'STORAGE_ID', names['live_sid']))
    block.statement('DETACH DATABASE %s' % ident(names['source']), 'R1: detach the live branch')
    block.check('R1 after: no attached database named source', ATTACHED_SQL, (names['source'],), ownersql.Expect('no_rows'))
    block.check('R2 before: the original storage is detached', BRANCH_SQL, (names['source_sid'],), ownersql.Expect('column_equals', 'IS_DETACHED', 1))
    block.statement("ATTACH DATABASE %s STORAGEID '%s'" % (ident(names['source']), names['source_sid']), 'R2: re-attach the original read write')
    block.check('R2 after: source attached READWRITE with the original STORAGE_ID', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('columns_equal', value={'STORAGE_ID': names['source_sid'], 'ATTACH_TYPE': 'READWRITE'}))
    block.check('R3 before: retained attached', ATTACHED_SQL, (names['retained'],), ownersql.Expect('some_rows'))
    block.statement('DROP DATABASE %s' % ident(names['retained']), 'R3 (optional): drop the retained branch')
    block.check('R3 after: retained gone', ATTACHED_SQL, (names['retained'],), ownersql.Expect('no_rows'))
    block.note('The detached live branch left by R1 is deleted in the Portal by STORAGE_ID %s.' % names['live_sid'])
    return block


def rollback_refusal(journal):
    """The reason a rollback is refused, or None: O11 released writers, or C1 saw application writes."""
    if journal.get_meta('app_writes_seen'):
        return 'C1 recorded application writes on the live branch at %s' % journal.get_meta('app_writes_seen')
    if journal.get_meta('released_utc') or journal.step_status('O11') in ('running', 'passed'):
        return 'O11 released writers on the live name'
    return None


class SwitchMachine:
    """Executes the switch one statement at a time, each behind its checks, from the live position.

    counts is {'tables', 'views', 'pipelines'} from the capture, layout the target layout,
    open_session(guard) and guard enable the DETACH escalation of S1.
    """

    def __init__(self, session, names, journal, log, runner_session=None, counts=None, layout=None, open_session=None, guard=None):
        self.session = session
        self.names = names
        self.journal = journal
        self.log = log
        self.runner = runner_session
        self.counts = counts or {}
        self.layout = layout
        self.open_session = open_session
        self.guard = guard

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

    def s1(self, detach_kill=None):
        """S1: detach the source after its checks (and the S2 and S3 preconditions)."""
        n = self.names
        self.require('S1', NOT_STARTED)
        readonly = n.get('freeze_mode', 'readonly') == 'readonly'
        self._expect('S1 before: source attached with the recorded STORAGE_ID', ATTACHED_SQL, (n['source'],), ownersql.Expect('column_equals', 'STORAGE_ID', n['source_sid']))
        self._expect('S1 before: source attach type', ATTACHED_SQL, (n['source'],), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READONLY' if readonly else 'READWRITE'))
        self._expect('S1 before: no foreign non-idle session', SESSIONS_SQL, _sessions_params(n), ownersql.Expect('column_equals', 'n', 0))
        self.preflight()
        kill = n.get('detach_kill', True) if detach_kill is None else detach_kill
        if self.open_session is not None and self.guard is not None:
            form = ownersql.detach_with_escalation(self.open_session, self.guard, n['source'], int(n.get('detach_wait_seconds', 120)), bool(kill),
                                                   self.log, self.journal)
            self.journal.event('switch_statement', 'S1 DETACH source (%s)' % form)
        else:
            self._run('DETACH DATABASE %s' % ident(n['source']), 'S1 DETACH source')
        self._expect('S1 after: source storage detached', BRANCH_SQL, (n['source_sid'],), ownersql.Expect('column_equals', 'IS_DETACHED', 1))
        self._expect('S1 after: no attached database named source', ATTACHED_SQL, (n['source'],), ownersql.Expect('no_rows'))

    def s2(self):
        """S2: attach the original storage read only under the retained name, then its after-checks."""
        n = self.names
        self.require('S2', DETACHED)
        self._expect('S2 before: no attached database named retained', ATTACHED_SQL, (n['retained'],), ownersql.Expect('no_rows'))
        self._run("ATTACH DATABASE %s AS %s STORAGEID '%s' READ ONLY" % (ident(n['source']), ident(n['retained']), n['source_sid']), 'S2 ATTACH retained READ ONLY')
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
        self._run("ATTACH DATABASE %s AS %s STORAGEID '%s'" % (ident(n['replacement']), ident(n['source']), n['replacement_sid']), 'S3 ATTACH replacement AS source')
        self.s3_after()

    def s3_after(self):
        """The S3 after-checks: live READWRITE on a new branch (recorded), target layout, capture counts, three smallest tables."""
        n = self.names
        rows = self._expect('S3 after: an attached READWRITE database named source', ATTACHED_SQL, (n['source'],),
                            ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READWRITE'), record='live')
        taken = [str(n['source_sid']), str(n['replacement_sid'])] + ([str(n['retained_sid'])] if _known(n.get('retained_sid')) else [])
        if str(rows[0].get('STORAGE_ID')) in taken:
            raise KitError('Switch refused after S3: the live name resolves to STORAGE_ID %s, which is not a new branch' % rows[0].get('STORAGE_ID'))
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
            got = _rows(self.runner, 'SELECT COUNT(*) AS n FROM ' + qualified(db_name, t['table_name']), None)
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

    def rollback(self, drop_retained=False):
        """R1 to R3 from the live position; refused once O11 released writers or application writes were seen."""
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
        self.journal.set_meta('rollback_started_utc', self.journal.get_meta('rollback_started_utc') or iso(utc_now()))
        if state == SWITCHED:
            self._expect('R1 before: the attached database named source is the live branch', ATTACHED_SQL, (n['source'],),
                         ownersql.Expect('column_equals', 'STORAGE_ID', n['live_sid']))
            self._run('DETACH DATABASE %s' % ident(n['source']), 'R1 DETACH live branch')
            self._expect('R1 after: no attached database named source', ATTACHED_SQL, (n['source'],), ownersql.Expect('no_rows'))
        self._expect('R2 before: the original storage is detached', BRANCH_SQL, (n['source_sid'],), ownersql.Expect('column_equals', 'IS_DETACHED', 1))
        self._expect('R2 before: no attached database named source', ATTACHED_SQL, (n['source'],), ownersql.Expect('no_rows'))
        self._run("ATTACH DATABASE %s STORAGEID '%s'" % (ident(n['source']), n['source_sid']), 'R2 ATTACH original read write')
        self._expect('R2 after: source attached READWRITE with the original STORAGE_ID', ATTACHED_SQL, (n['source'],),
                     ownersql.Expect('columns_equal', value={'STORAGE_ID': n['source_sid'], 'ATTACH_TYPE': 'READWRITE'}))
        if drop_retained and _rows(self.session, ATTACHED_SQL, (n['retained'],)):
            self._run('DROP DATABASE %s' % ident(n['retained']), 'R3 DROP retained')
            self._expect('R3 after: retained gone', ATTACHED_SQL, (n['retained'],), ownersql.Expect('no_rows'))
        self.journal.set_meta('rolled_back_utc', iso(utc_now()))
        self.journal.event('rollback', 'original %s re-attached READWRITE on STORAGE_ID %s' % (n['source'], n['source_sid']))
        return NOT_STARTED


def s0_checks(ctx, names, position=NOT_STARTED):
    """S0: the decision gate before the first statement; returns the lines, raises KitError when something is missing.

    freeze_time must exist (step O3 --redo records it after a notebook or hand run) and every
    table must be verified after it; the time check uses the abort deadline (O1 record, else the
    window) and applies only before S1.
    """
    from . import copy as copy_module
    journal = ctx.journal
    problems = []
    if journal.step_status('O9') != 'passed':
        problems.append('O9 has not passed')
    if journal.step_status('O8') != 'passed':
        problems.append('O8 has not passed')
    freeze_time = journal.get_meta('freeze_time')
    verified = [t['verified_at_utc'] for t in journal.tables() if t.get('verified_at_utc')]
    if not freeze_time:
        problems.append('no freeze_time is recorded (run s2rebuild step O3 --redo to run the write probe and record it)')
    if not verified:
        problems.append('no table carries verified_at_utc')
    elif freeze_time and min(verified) < freeze_time:
        problems.append('a table was verified (%s) before the freeze (%s)' % (min(verified), freeze_time))
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
    return ['S0: O8 and O9 passed; oldest verified_at %s after freeze_time %s' % (min(verified), freeze_time),
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


def handle_o10(step, ctx):
    """O10 handler: print both blocks; with the owner profile run S0, the typed go decision, then the machine."""
    if not ctx.dry_run:
        reader = ctx.runner_session(read_guard('runner O10'))
        try:
            record_replacement(ctx, reader)
        finally:
            reader.close()
    names = ownersql.names_for(ctx)
    counts = ownersql.expected_counts(ctx)
    inputs = ownersql.block_inputs(ctx)
    block = forward_block(names, counts)
    ownersql.guard_check_block(block, ctx)
    ownersql.print_block(block, ctx, ownersql.owner_sql_path(ctx, 'O10'))
    ownersql.print_block(rollback_block(names, inputs['released'], inputs['app_writes_seen']), ctx, ownersql.owner_sql_path(ctx, 'O10', rollback=True))
    if ctx.dry_run or not ctx.owner_configured:
        return steps.gate_pending(step, ctx, ['identity: owner', 'dry run: the forward block above is what the owner profile would execute'
                                             if ctx.dry_run else 'no owner profile: run the forward block as the admin, then record the gate'])
    if counts is None:
        return steps.Outcome(steps.REFUSED, 'O10 needs the capture (O4 or P3) for the S3 table, view and pipeline counts; none was found', 1)
    guard = steps.guard_for('O10', ctx.db, steps.OWNER)
    owner = ctx.owner_session(guard)
    runner = ctx.runner_session(steps.guard_for('O8', ctx.db))
    try:
        machine = SwitchMachine(owner, names, ctx.journal, ctx.log, runner, counts, ctx.db.layout, open_session=ctx.owner_session, guard=guard)
        position = machine.position()
        for line in s0_checks(ctx, names, position):
            ctx.log('    ' + line)
        if not ctx.yes:
            typed = _typed_live_name(ctx, names['source'])
            if typed is None:
                return steps.Outcome(steps.PENDING, 'S0: rerun step O10 --yes, or at a terminal type the live name %s, to confirm the go decision' % names['source'], EXIT_GATE)
            if typed != names['source']:
                return steps.Outcome(steps.REFUSED, 'S0: the typed name does not equal the live name %s; nothing was issued' % names['source'], 1)
        ctx.journal.event('switch_go', 'go recorded for live name %s at position %s%s' % (names['source'], position, ' (--yes)' if ctx.yes else ' (typed)'))
        machine.forward()
    finally:
        owner.close()
        runner.close()
    return steps.Outcome(steps.PASSED, 'switch complete: live name %s resolves to STORAGE_ID %s; retained %s' % (
        names['source'], names.get('live_sid'), names.get('retained_sid')))


steps.register('O10', handle_o10)
