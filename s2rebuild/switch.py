"""s2rebuild switch: the O10 state machine over the attachment views, forward S0 to S4 and rollback R1 to R2c.

Author: John Bergamini

Section 10.13 of the architecture. The position is computed from MV_CLOUD_ATTACHED_DATABASES
and MV_CLOUD_BRANCHES, never from memory, so a re-entered step continues from the live state;
re-entry reruns the after-checks of the step it lands behind (S2 before S3, S3 after a failed
S3 check), so a failed check can never turn into a passed O10. Every statement sits between a
check before and a check after; an out-of-order statement is refused, not executed. S0
accepts only when every planned table was verified after freeze_time and after its last chunk,
at its current re-copy epoch, so a re-copy after O8 refuses the switch until O6 to O8 pass
again. O10 holds the copy lock from S0 through S4 (the gate included), and the go decision is
recorded in one journal transaction that reads S0's journal part again (Journal.record_go), so a re-copy started
while the operator types the live name is refused or refuses the go. The journal part of S0
holds on every route: print mode refuses O10, and the forward block that owner-sql and the
notebook carry starts with a STOP while it would refuse (a re-copy rewrites the block files
already written with a STOP first). Every S3 precondition
(replacement attached READWRITE on its recorded STORAGE_ID, retained name free) is checked
before S1, so the source is never detached for a switch that cannot finish; so is the two-entry
end state: each storage holds its recorded record only, since a branch of the source or of the
replacement shares its STORAGE_ID and would stop S2b or the gate after the DROP. On Path B, S1
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
any position behind S1.
Identity is the branch record, never a STORAGE_ID alone (Helios 10.8.0, October 27, 2026: a
STORAGE_ID names a storage, and a branch keeps its parent's). identify(session, name) joins
MV_CLOUD_ATTACHED_DATABASES with MV_CLOUD_BRANCHES into one record (name, STORAGE_ID,
ATTACH_TYPE create, resume or pitr, COMPUTE_ID, ACCESS, IS_DETACHED, IS_DROPPED, END_VERSION,
ATTACH_SYNTAX); the key is name, STORAGE_ID and ATTACH_TYPE (records_match; COMPUTE_ID changes on every attach, fact 44), and
ROW_NUMBER is never read. The journal stores the whole record per role; every DETACH, ATTACH and
DROP of the machine and of the rollback runs behind a comparison with the recorded record and
refuses with both records printed when they differ. S1 records the original's detached record,
whose own ATTACH_SYNTAX (the AT MILESTONE form) S2b and R2a run to re-attach it.
Two entries (release 1.0, proven on throwaway databases on October 27, 2026): S2b, while the
live name is free, attaches the original record by its own syntax read write under the live
name, proves it unchanged (the three smallest non-empty tables equal the journal's source
counts, no foreign non-idle session) and drops it, so the original storage keeps only the
retained record; S4, after S3's checks, drops the replacement record, whose branch (the live
name) is independent of it. The gate after S4: the records with IS_DROPPED = 0 on both storages
are exactly the live record and the retained record; the listing before and after goes into
the journal. A failed proof at S2b stops with the original attached read write under the live
name, the rollback state. Rollback before S2b ran: R1, then R2a (the original's own syntax).
After S2b the original record is gone, so the rollback goes through the retained copy: R2b,
ATTACH <retained> AS <source> STORAGEID <original>, a read-write pitr branch under the live
name; when the server refuses a branch of a READ ONLY attachment, R2c detaches the retained
name and re-attaches its record by syntax under the live name (a resume record; the retained
name then leaves the Portal, and the report says where the data is). No rollback step drops
the retained copy. Statements issued through the O10 owner guard: DETACH DATABASE (with the
KILL escalation), ATTACH DATABASE, DROP DATABASE on the source record at S2b and on the
replacement at S4.
"""
import re
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
ROUTINE_COUNT_SQL = 'SELECT COUNT(*) AS n FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = %s'
# the S3 after-checks of the capture counts: (label, query, counts key); the routine count is the one the O7 gate recorded
COUNT_CHECKS = (('tables', TABLE_COUNT_SQL, 'tables'), ('views', VIEW_COUNT_SQL, 'views'), ('pipelines', PIPELINE_COUNT_SQL, 'pipelines'),
                ('routines', ROUTINE_COUNT_SQL, 'routines'))
RUNNING_PIPELINES_SQL = "SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s AND STATE = 'Running'"
SWITCH_ALLOWANCE_SECONDS = 10 * 60

NOT_STARTED, DETACHED, RETAINED, SWITCHED, LIVE_DETACHED, UNKNOWN = (
    'not started', 'source detached', 'retained attached', 'switched', 'live branch detached', 'unknown')
# the positions of the two-entry switch: S2b attached the original read write under the live name (also the state after a
# rollback by R2a), S2b dropped it, S4 dropped the replacement, and a rollback through the retained copy (R2b or R2c)
ORIGINAL_ATTACHED, SOURCE_DROPPED, COMPLETE, ROLLED_BACK = (
    'original attached read write', 'original record dropped', 'switch complete', 'rolled back through the retained copy')
FORWARD = ('S1', 'S2', 'S2b', 'S3', 'S4')
ROLLBACK = ('R1', 'R2a', 'R2b', 'R2c')
READ_ONLY_REFUSAL = 2720


def _rows(session, sql, params):
    return [dict(r) if isinstance(r, dict) else r for r in session.query(sql, params)]


def _known(value):
    return bool(value) and not ownersql.placeholders(str(value))


BRANCH_OF_SQL = ('SELECT COUNT(*) AS n FROM information_schema.MV_CLOUD_BRANCHES WHERE DATABASE_NAME = %s AND STORAGE_ID = %s '
                 'AND PREV_DATABASE_NAME = %s AND IS_DROPPED = 0 AND IS_DETACHED = 0')
RECORD_COLUMNS = 'DATABASE_NAME, STORAGE_ID, ATTACH_TYPE, LATEST_COMPUTE_ID, PREV_DATABASE_NAME, IS_DETACHED, IS_DROPPED, END_VERSION, ATTACH_SYNTAX'
# every record of a name that is not dropped (parameter: the name); identify joins them with the attachment row
RECORDS_SQL = 'SELECT %s FROM information_schema.MV_CLOUD_BRANCHES WHERE DATABASE_NAME = %%s AND IS_DROPPED = 0' % RECORD_COLUMNS
# the attached record of a name on a storage (parameters: name, STORAGE_ID): the printed blocks' record checks
ATTACHED_RECORD_SQL = ('SELECT %s FROM information_schema.MV_CLOUD_BRANCHES WHERE DATABASE_NAME = %%s AND STORAGE_ID = %%s AND IS_DROPPED = 0 '
                       'AND IS_DETACHED = 0' % RECORD_COLUMNS)
# the attached record of a name (parameter: name): S2 and S3 after, R1 before
NAME_RECORD_SQL = 'SELECT %s FROM information_schema.MV_CLOUD_BRANCHES WHERE DATABASE_NAME = %%s AND IS_DROPPED = 0 AND IS_DETACHED = 0' % RECORD_COLUMNS
# the detached record of a name on a storage (parameters: name, STORAGE_ID): S1 after, S2b and R2a before
DETACHED_RECORD_SQL = ('SELECT %s FROM information_schema.MV_CLOUD_BRANCHES WHERE DATABASE_NAME = %%s AND STORAGE_ID = %%s AND IS_DROPPED = 0 '
                       'AND IS_DETACHED = 1' % RECORD_COLUMNS)
# every record of a name, dropped ones included (parameter: the name): the position reads whether the original record is dropped
ALL_RECORDS_SQL = 'SELECT %s FROM information_schema.MV_CLOUD_BRANCHES WHERE DATABASE_NAME = %%s' % RECORD_COLUMNS
# the Portal entries of a storage, the records with IS_DROPPED = 0 (parameter: STORAGE_ID): the S2b after-check and the gate
STORAGE_ENTRIES_SQL = 'SELECT %s FROM information_schema.MV_CLOUD_BRANCHES WHERE STORAGE_ID = %%s AND IS_DROPPED = 0' % RECORD_COLUMNS
STORAGE_ENTRY_COUNT_SQL = 'SELECT COUNT(*) AS n FROM information_schema.MV_CLOUD_BRANCHES WHERE STORAGE_ID = %s AND IS_DROPPED = 0'
# the S1 before-checks that each storage holds its recorded record only (SwitchMachine._storage_holds_only and the printed block)
STORAGE_ONLY_LABELS = {'source': 'S1 before (for S2b): the original storage holds one entry, the original record',
                       'replacement': 'S1 before (for S4): the replacement storage holds one entry, the replacement record'}
# COUNT of attached databases under a name (parameter: the name): the skip_if of the printed R2c statements
NAME_ATTACHED_COUNT_SQL = 'SELECT COUNT(*) AS n FROM information_schema.MV_CLOUD_ATTACHED_DATABASES WHERE DATABASE_NAME = %s'
# the state of one record (parameters: name, STORAGE_ID, COMPUTE_ID): IS_DROPPED = 1 after a DROP
# the records of a name on a STORAGE_ID (parameters: name, STORAGE_ID), one not dropped first and an attached one before a detached one, so
# the state checks read the record that matters; never by COMPUTE_ID, which every attach changes (fact 44)
RECORD_STATE_SQL = ('SELECT %s FROM information_schema.MV_CLOUD_BRANCHES WHERE DATABASE_NAME = %%s AND STORAGE_ID = %%s '
                    'ORDER BY IS_DROPPED, IS_DETACHED' % RECORD_COLUMNS)
# COMPUTE_ID is not part of the key: Helios gives a record a new one on every attach, a re-attach by its own syntax included (fact 44)
KEY_FIELDS = ('DATABASE_NAME', 'STORAGE_ID', 'ATTACH_TYPE')
JOURNAL_KEY_FIELDS = ('database_name', 'storage_id', 'record_type')
SYNTAX_COMPUTE = re.compile(r' COMPUTEID ([A-Za-z0-9_:-]+)\s*;?\s*$')


def compute_id_of(branch_row):
    """The COMPUTE_ID of a branch record: the COMPUTEID inside its ATTACH_SYNTAX, else LATEST_COMPUTE_ID."""
    m = SYNTAX_COMPUTE.search(str(branch_row.get('ATTACH_SYNTAX') or ''))
    if m:
        return m.group(1)
    value = branch_row.get('LATEST_COMPUTE_ID')
    return None if value is None else str(value)


def _record(branch_row, attachment=None):
    record = {'DATABASE_NAME': str(branch_row.get('DATABASE_NAME')), 'STORAGE_ID': str(branch_row.get('STORAGE_ID')),
              'ATTACH_TYPE': str(branch_row.get('ATTACH_TYPE') or '').lower() or None, 'COMPUTE_ID': compute_id_of(branch_row),
              'ACCESS': None if attachment is None else str(attachment.get('ATTACH_TYPE')).upper(),
              'PREV_DATABASE_NAME': branch_row.get('PREV_DATABASE_NAME'), 'IS_DETACHED': int(str(branch_row.get('IS_DETACHED') or 0)),
              'IS_DROPPED': int(str(branch_row.get('IS_DROPPED') or 0)), 'END_VERSION': branch_row.get('END_VERSION'),
              'ATTACH_SYNTAX': branch_row.get('ATTACH_SYNTAX')}
    return record


def record_key(record):
    """(name, STORAGE_ID, ATTACH_TYPE) as strings for a live record (identify) or a journal attachment row; ATTACH_TYPE None when unknown.

    None when the name or the STORAGE_ID is unknown. A journal row taken from MV_CLOUD_ATTACHED_DATABASES
    alone carries no ATTACH_TYPE; records_match then compares name and STORAGE_ID.
    """
    if record is None:
        return None
    fields = JOURNAL_KEY_FIELDS if 'record_type' in record or 'database_name' in record else KEY_FIELDS
    values = [record.get(f) for f in fields]
    if any(v is None or (isinstance(v, str) and ownersql.placeholders(v)) for v in values[:2]):
        return None
    kind = values[2]
    kind = None if kind is None or ownersql.placeholders(str(kind)) else str(kind).lower()
    return (str(values[0]), str(values[1]), kind)


def records_match(a, b):
    """True when two records (live or journal rows) carry the same name and STORAGE_ID, and the same ATTACH_TYPE where both know it.

    An unknown name or STORAGE_ID never matches. COMPUTE_ID is not compared: every attach changes it (fact 44).
    """
    ka, kb = record_key(a), record_key(b)
    if ka is None or kb is None:
        return False
    return ka[:2] == kb[:2] and (ka[2] is None or kb[2] is None or ka[2] == kb[2])


def record_text(record):
    """One line naming a record for a refusal or a log line: name, STORAGE_ID, ATTACH_TYPE, COMPUTE_ID and its state."""
    if record is None:
        return 'no record'
    if 'record_type' in record or 'database_name' in record:
        state = 'detached' if record.get('is_detached') else ('attached %s' % record['attach_type'] if record.get('attach_type') else 'recorded')
        return 'name %s, STORAGE_ID %s, ATTACH_TYPE %s, COMPUTE_ID %s (%s)' % (
            record.get('database_name'), record.get('storage_id'), record.get('record_type'), record.get('compute_id'), state)
    if record.get('IS_DROPPED'):
        state = 'dropped'
    elif record.get('IS_DETACHED'):
        state = 'detached'
    else:
        state = 'attached %s' % (record.get('ACCESS') or '')
    return 'name %s, STORAGE_ID %s, ATTACH_TYPE %s, COMPUTE_ID %s (%s)' % (
        record['DATABASE_NAME'], record['STORAGE_ID'], record['ATTACH_TYPE'], record['COMPUTE_ID'], state.strip())


def refusal_text(label, expected, observed):
    """The refusal for a record that differs, with both records printed."""
    return ('Refused at "%s": the record differs, so nothing was issued. Expected: %s. Observed: %s. Identify the database by its branch '
            'record (name, STORAGE_ID, ATTACH_TYPE, COMPUTE_ID), never by STORAGE_ID alone, before any further statement'
            % (label, record_text(expected), record_text(observed)))


def records(session, name):
    """Every record of a name that is not dropped, as identify builds them, the attached one (when any) first. Issues two reads."""
    attached = _rows(session, ATTACHED_SQL, (name,))
    att = attached[0] if attached else None
    out = []
    for row in _rows(session, RECORDS_SQL, (name,)):
        joined = att is not None and str(row.get('STORAGE_ID')) == str(att.get('STORAGE_ID')) \
            and str(compute_id_of(row)) == str(att.get('COMPUTE_ID')) and not int(str(row.get('IS_DETACHED') or 0))
        out.append(_record(row, att if joined else None))
    out.sort(key=lambda r: r['ACCESS'] is None)
    return out


def identify(session, name, expected=None):
    """The record of the attached database named name, else its detached record, else None; never by ROW_NUMBER.

    MV_CLOUD_BRANCHES rows of the name, not dropped, joined in the kit with the
    MV_CLOUD_ATTACHED_DATABASES row (same STORAGE_ID and COMPUTE_ID): the attached record carries
    ACCESS (READONLY or READWRITE). With no attached record, the detached one is returned; among
    several, the one matching expected (records_match), else the first. Issues reads only.
    """
    found = records(session, name)
    attached = [r for r in found if r['ACCESS'] is not None]
    if attached:
        return attached[0]
    detached = [r for r in found if r['IS_DETACHED']]
    if expected is not None:
        for r in detached:
            if records_match(expected, r):
                return r
    return detached[0] if detached else None


def journal_record(journal, role, record):
    """Store a live record (identify) in the journal for a role."""
    journal.record_attachment_row(role, record)


def all_records(session, name):
    """Every record of a name as identify builds them, dropped ones included, without the attachment join. Issues one read."""
    return [_record(row) for row in _rows(session, ALL_RECORDS_SQL, (name,))]


def original_state(source_records, names):
    """'attached', 'detached', 'dropped' or None: the state of the original record (the recorded source STORAGE_ID and ATTACH_TYPE) among records."""
    sid, kind = str(names.get('source_sid')), names.get('source_type')
    for r in source_records:
        if str(r['STORAGE_ID']) != sid or (_known(kind) and str(r['ATTACH_TYPE']).lower() != str(kind).lower()):
            continue
        if r['IS_DROPPED']:
            return 'dropped'
        return 'detached' if r['IS_DETACHED'] else 'attached'
    return None


def original_record(source_records, names):
    """The original record that is not dropped (the source name, the recorded source STORAGE_ID and ATTACH_TYPE) among records, or None.

    The position reads the source by this record, never by the first row of its storage: a
    branch of the source and a dropped record of an earlier attempt share the STORAGE_ID
    (facts 27 and 35), and the server lists them in no fixed order (fact 33).
    """
    sid, kind = str(names.get('source_sid')), names.get('source_type')
    return next((r for r in source_records if str(r['STORAGE_ID']) == sid and not r['IS_DROPPED']
                 and (not _known(kind) or str(r['ATTACH_TYPE']).lower() == str(kind).lower())), None)


def entry_key(record):
    """(name, STORAGE_ID, ATTACH_TYPE, COMPUTE_ID, detached) of a Portal entry, for the two-entry gate and the journal listing."""
    key = record_key(record)
    detached = int(record.get('is_detached') or 0) if 'record_type' in record else int(record.get('IS_DETACHED') or 0)
    return None if key is None else key + (detached,)


def listing(session, names):
    """The Portal entries (IS_DROPPED = 0) of the original storage and the replacement storage, as records. Issues one read per storage."""
    out = []
    for sid in dict.fromkeys(str(names.get(k)) for k in ('source_sid', 'replacement_sid') if _known(names.get(k))):
        out.extend(_record(row) for row in _rows(session, STORAGE_ENTRIES_SQL, (sid,)))
    return out


def source_storage_extras(session, attachment):
    """P2: the entries (IS_DROPPED = 0) on the source's STORAGE_ID other than the source's own attached record, one line each. Issues one read.

    A branch of the source (ATTACH ... AS keeps the parent's STORAGE_ID, fact 27) or a detached
    record left on that storage would be a third entry after the switch, which S2b and the
    two-entry gate find only after the original is dropped. attachment is the source's
    MV_CLOUD_ATTACHED_DATABASES row; its own record is the attached one under its name with the
    attachment's COMPUTE_ID.
    """
    name, cid = str(attachment.get('DATABASE_NAME')), attachment.get('COMPUTE_ID')
    extra = []
    for record in listing(session, {'source_sid': attachment.get('STORAGE_ID')}):
        own = record['DATABASE_NAME'] == name and not record['IS_DETACHED'] and (not _known(cid) or str(record['COMPUTE_ID']) == str(cid))
        if not own:
            extra.append(record_text(record))
    return extra


def listing_text(entries):
    """The entries as one line each: name, STORAGE_ID, ATTACH_TYPE, COMPUTE_ID and attached or detached."""
    return '; '.join(record_text(r) for r in entries) or 'no entry'


def read_only_refusal(exc):
    """True when the server refused a statement because the attachment is read only: error 2720, or a message that says so."""
    args = getattr(exc, 'args', ())
    if args and args[0] == READ_ONLY_REFUSAL:
        return True
    text = str(exc).lower()
    return 'read only' in text or 'read-only' in text or 'readonly' in text


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

    def __init__(self, source_att, retained_att, replacement_att, source_branch, live_branch=None, live_is_branch=False, source_records=None):
        self.source_att = source_att
        self.retained_att = retained_att
        self.replacement_att = replacement_att
        self.source_branch = source_branch
        self.live_branch = live_branch
        self.live_is_branch = live_is_branch
        self.source_records = list(source_records or [])

    def original(self, names):
        """The state of the original record among the records of the source name: 'attached', 'detached', 'dropped' or None (not read)."""
        return original_state(self.source_records, names)

    def state(self, names):
        """The position from the facts and the recorded ids: NOT_STARTED, DETACHED, RETAINED, ORIGINAL_ATTACHED, SOURCE_DROPPED, SWITCHED,
        COMPLETE, LIVE_DETACHED, ROLLED_BACK or UNKNOWN.

        SWITCHED needs the live name attached READWRITE on a STORAGE_ID that is none of the
        source, replacement or retained ids (and equals the recorded live id once one exists),
        with the retained copy attached READONLY. The replacement's id counts when the live name
        is its own branch of the replacement (live_is_branch_of_replacement), as on Helios 10.8.0;
        COMPLETE is SWITCHED with the replacement name gone (S4). The live name READWRITE on the
        original storage beside the retained copy is ORIGINAL_ATTACHED while it holds the original
        record (S2b attached it, or R2a put it back) and ROLLED_BACK when it holds a branch of the
        retained copy (R2b or R2c). SOURCE_DROPPED is the retained copy alone with the original
        record dropped (S2b done, S3 ahead).
        """
        src_sid = str(names['source_sid'])
        retained_ok = bool(self.retained_att) and str(self.retained_att.get('ATTACH_TYPE')).upper() == 'READONLY'
        if self.source_att:
            sid = str(self.source_att.get('STORAGE_ID'))
            if sid == src_sid:
                if not self.retained_att:
                    return NOT_STARTED
                if not retained_ok or str(self.source_att.get('ATTACH_TYPE')).upper() != 'READWRITE':
                    return UNKNOWN
                cid = str(self.source_att.get('COMPUTE_ID'))
                attached = next((r for r in self.source_records if str(r['STORAGE_ID']) == sid and str(r['COMPUTE_ID']) == cid and not r['IS_DROPPED']), None)
                # the original record attached again (S2b or R2a) carries a new COMPUTE_ID (fact 44); a branch of the retained copy names it as parent
                if self.original(names) == 'attached' and (attached is None or attached.get('PREV_DATABASE_NAME') != names['retained']):
                    return ORIGINAL_ATTACHED
                if attached is not None and attached.get('PREV_DATABASE_NAME') == names['retained']:
                    return ROLLED_BACK
                return UNKNOWN
            if not retained_ok or str(self.source_att.get('ATTACH_TYPE')).upper() != 'READWRITE':
                return UNKNOWN
            taken = {src_sid} | (set() if self.live_is_branch else {str(names['replacement_sid'])}) | (
                {str(names['retained_sid'])} if _known(names.get('retained_sid')) else set())
            if sid in taken:
                return UNKNOWN
            if _known(names.get('live_sid')) and sid != str(names['live_sid']):
                return UNKNOWN
            return SWITCHED if self.replacement_att else COMPLETE
        if self.retained_att:
            if not retained_ok:
                return UNKNOWN
            if self.live_branch and str(self.live_branch.get('IS_DETACHED')) == '1':
                return LIVE_DETACHED
            if self.original(names) == 'dropped':
                return SOURCE_DROPPED
            return RETAINED
        if self.source_branch and str(self.source_branch.get('IS_DETACHED')) == '1':
            return DETACHED
        return UNKNOWN


def read_position(session, names):
    """Query the attachment views for the three names, the records of the source name (the original record among them) and (once recorded) the live branch."""
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
    source_records = all_records(session, names['source'])
    return Position(source_att, _first(ATTACHED_SQL, (names['retained'],)), replacement_att, original_record(source_records, names), live,
                    live_is_branch=branch, source_records=source_records)


def _sessions_params(names):
    return (names['source'], '%' + names['source'] + '%', names['runner_user'], names.get('owner_user') or names['runner_user'])


def withheld_label(account, scope):
    """The S1 before check on Path B that an account holds none of the write privileges the lockout took on a scope."""
    return 'S1 before (Path B): %s holds none of the write privileges the lockout took on %s' % (account, scope)


def forward_block(names, counts=None, s0_problems=None, withheld=None):
    """The printed forward block S1 to S3 with every check and expected result (S0 is a decision).

    counts is {'tables', 'views', 'pipelines'} from the capture; each known count becomes a
    check after S3. Every S3 precondition is also checked before S1, and so is one entry on each
    storage (a branch of either database would stop the block after the DROP of S2b). s0_problems is
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
    block.check('S1 before: the source is the recorded record', ATTACHED_RECORD_SQL, (names['source'], names['source_sid']),
                ownersql.Expect('columns_equal', value=ownersql.record_expect(names, 'source')))
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
    block.check('S1 before (for S3): the replacement is the recorded record', ATTACHED_RECORD_SQL, (names['replacement'], names['replacement_sid']),
                ownersql.Expect('columns_equal', value=ownersql.record_expect(names, 'replacement')))
    block.check('S1 before (for S2): no attached database named retained', ATTACHED_SQL, (names['retained'],), ownersql.Expect('no_rows'))
    # a branch of either database shares its STORAGE_ID (fact 27): found here, before S1, not after the DROP of S2b
    block.check(STORAGE_ONLY_LABELS['source'], STORAGE_ENTRY_COUNT_SQL, (names['source_sid'],), ownersql.Expect('column_equals', 'n', 1))
    block.check(STORAGE_ONLY_LABELS['replacement'], STORAGE_ENTRY_COUNT_SQL, (names['replacement_sid'],), ownersql.Expect('column_equals', 'n', 1))
    block.statement('DETACH DATABASE %s' % ident(names['source']),
                    'S1: detach the source (after %s s of waiting the kit cancels it and issues DETACH ... KILL%s)' % (
                        names.get('detach_wait_seconds', 120), '' if names.get('detach_kill', True) else '; detach_kill is off, so it stops instead'),
                    detach={'wait': names.get('detach_wait_seconds', 120), 'kill': bool(names.get('detach_kill', True))})
    block.check('S1 after: no attached database named source', ATTACHED_SQL, (names['source'],), ownersql.Expect('no_rows'))
    block.check('S1 after: the original record is detached with its ATTACH_SYNTAX recorded', DETACHED_RECORD_SQL, (names['source'], names['source_sid']),
                ownersql.Expect('columns_equal', value=ownersql.record_expect(names, 'source')), record='source')
    block.check('S2 before: no attached database named retained', ATTACHED_SQL, (names['retained'],), ownersql.Expect('no_rows'))
    block.statement("ATTACH DATABASE %s AS %s READ ONLY STORAGEID %s" % (ident(names['source']), ident(names['retained']), names['source_sid']),
                    'S2: attach the original storage read only under the retained name')
    block.check('S2 after: retained attached READONLY', ATTACHED_SQL, (names['retained'],), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READONLY'),
                record='retained')
    block.check('S2 after: the retained name holds a resume record of the original', NAME_RECORD_SQL, (names['retained'],),
                ownersql.Expect('columns_equal', value={'ATTACH_TYPE': 'resume', 'PREV_DATABASE_NAME': names['source']}), record='retained')
    block.note('S2 after: one small table count in the retained copy equals the journal source count (the kit checks this).')
    s2b_block(block, names)
    block.check('S3 before: replacement attached READWRITE with the recorded STORAGE_ID', ATTACHED_SQL, (names['replacement'],),
                ownersql.Expect('columns_equal', value={'STORAGE_ID': names['replacement_sid'], 'ATTACH_TYPE': 'READWRITE'}))
    block.check('S3 before: no attached database named source', ATTACHED_SQL, (names['source'],), ownersql.Expect('no_rows'))
    block.statement("ATTACH DATABASE %s AS %s STORAGEID %s" % (ident(names['replacement']), ident(names['source']), names['replacement_sid']),
                    'S3: attach the replacement as a branch under the live name')
    block.check('S3 after: an attached READWRITE database named source', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READWRITE'), record='live')
    # Helios 10.8.0 keeps the replacement's STORAGE_ID for the live branch: the proof of a new branch is its own record, not a new id
    block.check('S3 after: the live STORAGE_ID is not the original storage', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('column_not_in', 'STORAGE_ID', [names['source_sid']]))
    block.check('S3 after: the live name is its own branch record of the replacement', BRANCH_OF_SQL,
                (names['source'], names['replacement_sid'], names['replacement']), ownersql.Expect('column_at_least', 'n', 1))
    block.check('S3 after: the live name holds a pitr record of the replacement', NAME_RECORD_SQL, (names['source'],),
                ownersql.Expect('columns_equal', value={'ATTACH_TYPE': 'pitr', 'PREV_DATABASE_NAME': names['replacement']}), record='live')
    block.check('S3 after: the live database reports the target layout', LAYOUT_SQL, (names['source'],),
                ownersql.Expect('columns_equal', value={'NUM_PARTITIONS': names['partitions'], 'NUM_SUB_PARTITIONS': names['sub_partitions']}))
    for label, sql, key in COUNT_CHECKS:
        if counts and counts.get(key) is not None:
            block.check('S3 after: %s count equals the capture' % label, sql, (names['source'],), ownersql.Expect('column_equals', 'n', counts[key]))
    if not counts:
        block.note('S3 after: no capture is available, so the table, view, pipeline and routine counts are not checked here; the kit refuses O10 without one.')
    block.note('S3 after: the three smallest non-empty tables match the journal counts (the kit checks these and records the live record).')
    s4_block(block, names)
    gate_block(block, names, 'gate')
    return block


def s2b_block(block, names):
    """S2b of the printed block: the original record attached by its own syntax under the live name, the zero-change proof, its DROP, the after-checks.

    The statement is original_syntax (a milestone placeholder before S1 ran, with a note). The
    proof is one COUNT(*) check per smallest non-empty table against the journal's source count
    (names['source_counts']) and the S1 session query. A failed proof stops the block with the
    original attached read write under the live name, the rollback state, and the DROP is never
    reached.
    """
    block.check('S2b before: no attached database named source', ATTACHED_SQL, (names['source'],), ownersql.Expect('no_rows'))
    block.check('S2b before: the original record is detached with its ATTACH_SYNTAX', DETACHED_RECORD_SQL, (names['source'], names['source_sid']),
                ownersql.Expect('columns_equal', value=ownersql.record_expect(names, 'source')))
    block.statement(original_syntax(names), 'S2b: attach the original record read write under the live name by its own ATTACH_SYNTAX')
    if ownersql.placeholders(original_syntax(names)):
        block.note('S2b: the AT MILESTONE value is the ATTACH_SYNTAX of the detached original record, recorded at S1. Generate this block again '
                   'after S1 (s2rebuild owner-sql O10) to fill it in, or copy the ATTACH_SYNTAX from the row of the S2b before check.')
    block.check('S2b after: the original record is attached READWRITE under the live name', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('columns_equal', value={'STORAGE_ID': names['source_sid'], 'ATTACH_TYPE': 'READWRITE'}))
    block.note('S2b zero-change proof: every check below must pass before the DROP. A failed check leaves the original attached read write '
               'under the live name, which is the rollback state: reconcile, or record the go again; never run the DROP.')
    for table, count in (names.get('source_counts') or [])[:3]:
        block.check('S2b proof: %s holds the source row count' % table, count_sql(names['source'], table), (), ownersql.Expect('column_equals', 'n', count))
    if not names.get('source_counts'):
        block.note('S2b proof: the journal holds no source count yet (O6 records them); the kit refuses S2b without them.')
    block.check('S2b proof: no non-idle session on the live name other than ours', SESSIONS_SQL, _sessions_params(names),
                ownersql.Expect('column_equals', 'n', 0))
    block.statement('DROP DATABASE %s' % ident(names['source']), 'S2b: drop the original record (its data stays in the retained copy)')
    block.check('S2b after: the original record is dropped', RECORD_STATE_SQL, (names['source'], names['source_sid']),
                ownersql.Expect('column_equals', 'IS_DROPPED', 1))
    block.check('S2b after: no attached database named source', ATTACHED_SQL, (names['source'],), ownersql.Expect('no_rows'))
    block.check('S2b after: the original storage holds one entry, the retained record', STORAGE_ENTRY_COUNT_SQL, (names['source_sid'],),
                ownersql.Expect('column_equals', 'n', 1))
    block.check('S2b after: the retained name still holds its resume record', NAME_RECORD_SQL, (names['retained'],),
                ownersql.Expect('columns_equal', value={'ATTACH_TYPE': 'resume', 'PREV_DATABASE_NAME': names['source']}))
    block.note('S2b after: one small table count in the retained copy equals the journal source count (the kit checks this).')


def s4_block(block, names):
    """S4 of the printed block: the replacement record dropped by its record after S3's checks, the live record unchanged."""
    block.check('S4 before: the replacement name holds the recorded replacement record', ATTACHED_RECORD_SQL, (names['replacement'], names['replacement_sid']),
                ownersql.Expect('columns_equal', value=ownersql.record_expect(names, 'replacement')))
    block.check('S4 before: the live name is its own branch record of the replacement', BRANCH_OF_SQL,
                (names['source'], names['replacement_sid'], names['replacement']), ownersql.Expect('column_at_least', 'n', 1))
    block.statement('DROP DATABASE %s' % ident(names['replacement']), 'S4: drop the replacement record (the live branch is independent of it)')
    block.check('S4 after: the replacement record is dropped', RECORD_STATE_SQL, (names['replacement'], names['replacement_sid']),
                ownersql.Expect('column_equals', 'IS_DROPPED', 1))
    block.check('S4 after: no attached database named replacement', ATTACHED_SQL, (names['replacement'],), ownersql.Expect('no_rows'))
    block.check('S4 after: the live name still holds its pitr record of the replacement', NAME_RECORD_SQL, (names['source'],),
                ownersql.Expect('columns_equal', value={'ATTACH_TYPE': 'pitr', 'PREV_DATABASE_NAME': names['replacement']}))
    block.note('S4 after: one small table count on the live name equals the journal source count (the kit checks this).')


def gate_block(block, names, label, recorded=False):
    """The two-entry gate as checks: one entry on the original storage (the retained record), one on the replacement storage (the live record).

    The records are checked by STORAGE_ID, type and parent, never by COMPUTE_ID (fact 44: a C3 repair
    re-attaches the live record); recorded is kept for the callers.
    """
    retained = {'ATTACH_TYPE': 'resume', 'PREV_DATABASE_NAME': names['source'], 'STORAGE_ID': names['source_sid']}
    live = {'ATTACH_TYPE': 'pitr', 'PREV_DATABASE_NAME': names['replacement'], 'STORAGE_ID': names['replacement_sid']}
    block.check('%s: the original storage holds one entry' % label, STORAGE_ENTRY_COUNT_SQL, (names['source_sid'],), ownersql.Expect('column_equals', 'n', 1))
    block.check('%s: that entry is the retained record' % label, NAME_RECORD_SQL, (names['retained'],), ownersql.Expect('columns_equal', value=retained))
    block.check('%s: the replacement storage holds one entry' % label, STORAGE_ENTRY_COUNT_SQL, (names['replacement_sid'],),
                ownersql.Expect('column_equals', 'n', 1))
    block.check('%s: that entry is the live record' % label, NAME_RECORD_SQL, (names['source'],), ownersql.Expect('columns_equal', value=live))
    block.note('%s: the workspace now holds two entries, the live database and the retained copy; the kit records this listing in the journal.' % label)


def count_sql(db_name, table):
    """COUNT(*) of one table (the zero-change proof and the S2 and S3 small-table checks)."""
    return 'SELECT COUNT(*) AS n FROM ' + qualified(db_name, table)


def unproven_tables(table_counts):
    """The planned tables the zero-change proof cannot compare: those without a recorded target_count."""
    return [table for table, count in table_counts if count is None]


def rollback_block(names, released=False, app_writes_seen=None, withheld=None, after_s2b=False):
    """The rollback block, valid only before O11 released writers; a STOP item when it is no longer valid.

    R1 detaches the live name only while it holds the recorded live record. Before S2b ran
    (after_s2b=False) R2a runs the detached original record's own ATTACH_SYNTAX
    (original_syntax). After S2b (after_s2b=True) the original record is dropped, so R2b
    attaches a read-write branch of the retained copy under the live name, and R2c, only when
    the server refused R2b on the READ ONLY attachment, detaches the retained name and
    re-attaches its record by syntax under the live name (a milestone placeholder with a note,
    since the syntax exists only after the DETACH); its statements are skipped while the live
    name is attached. No step drops the retained copy.

    Before R1 the zero-change proof: one COUNT(*) check per planned table of the live name
    against its journal target_count (names['table_counts'] from names_for), no pipeline Running
    on the live name, none of the write privileges the lockout took (withheld, the snapshot not
    given back yet) held again, then the S1 session query on the live name. The pipeline and
    privilege checks read the live state, so a block written before O11 still refuses once a
    hand or notebook O11 started a pipeline or gave a privilege back. A planned table without a
    target_count, or no planned table at all, is a STOP, since the live branch cannot be proven
    unchanged.
    """
    block = ownersql.Block('O10', 'Rollback R1 and R2b, R2c (after S2b ran; before O11 only)' if after_s2b else
                           'Rollback R1 and R2a (before S2b ran; before O11 only)')
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
    block.note('Two blocks exist: O10-rollback (R2a) while the original record is still detached, that is before S2b ran, and '
               'O10-rollback-retained (R2b, then R2c when R2b is refused) once S2b dropped it. Use the one the S2b state calls for; the '
               'R2 before check of the wrong one stops it with nothing issued.')
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
    block.check('R1 before: the attached database named source is the recorded live record', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('columns_equal', value={'STORAGE_ID': names['live_sid'], 'COMPUTE_ID': names['live_cid'], 'ATTACH_TYPE': 'READWRITE'}))
    block.check('R1 before: the live record under the live name', NAME_RECORD_SQL, (names['source'],),
                ownersql.Expect('columns_equal', value=ownersql.record_expect(names, 'live')))
    block.statement('DETACH DATABASE %s' % ident(names['source']), 'R1: detach the live branch')
    block.check('R1 after: no attached database named source', ATTACHED_SQL, (names['source'],), ownersql.Expect('no_rows'))
    if after_s2b:
        _r2b_items(block, names)
    else:
        _r2a_items(block, names)
    block.note('The retained copy %s stays attached read only; no rollback step drops it. The detached live record left by R1 (name %s, '
               'STORAGE_ID %s, COMPUTE_ID %s) stays recoverable by its own ATTACH_SYNTAX; it shares its STORAGE_ID with the replacement, so it '
               'is never deleted in the Portal by STORAGE_ID.' % (names['retained'], names['source'], names['live_sid'], names['live_cid']))
    return block


def _r2a_items(block, names):
    """R2a: the detached original record re-attached read write by its own ATTACH_SYNTAX.

    The check reads the original record by name and STORAGE_ID, never the first row of the
    storage: after S2 the retained record shares it (fact 27) in no fixed order (fact 33).
    """
    block.check('R2a before: the original record is detached with its ATTACH_SYNTAX', DETACHED_RECORD_SQL, (names['source'], names['source_sid']),
                ownersql.Expect('columns_equal', value=ownersql.record_expect(names, 'source')))
    block.statement(original_syntax(names), 'R2a: re-attach the original record read write by its own ATTACH_SYNTAX')
    if ownersql.placeholders(original_syntax(names)):
        block.note('R2a: the AT MILESTONE value is the ATTACH_SYNTAX of the detached original record, recorded at S1. Generate this block again '
                   'after S1 (s2rebuild owner-sql O10 --rollback) to fill it in, or copy the ATTACH_SYNTAX from the row of the R2a check.')
    block.check('R2a after: source attached READWRITE with the original STORAGE_ID', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('columns_equal', value={'STORAGE_ID': names['source_sid'], 'ATTACH_TYPE': 'READWRITE'}))


def retained_branch_sql(names):
    """R2b: a read-write pitr branch of the retained copy under the live name on the original storage."""
    return 'ATTACH DATABASE %s AS %s STORAGEID %s' % (ident(names['retained']), ident(names['source']), names['source_sid'])


def retained_resume_sql(names, syntax=None):
    """R2c: the detached retained record's own ATTACH_SYNTAX with AS <source>, so its record comes back read write under the live name.

    syntax is the ATTACH_SYNTAX read after the DETACH; without it the AT MILESTONE form carries a
    placeholder for the printed block.
    """
    text = str(syntax or '').strip().rstrip(';')
    m = re.match(r'ATTACH DATABASE `[A-Za-z0-9_]+`( .*)$', text)
    if m and ' AT MILESTONE ' in text:
        return 'ATTACH DATABASE %s AS %s%s' % (ident(names['retained']), ident(names['source']), m.group(1))
    return "ATTACH DATABASE %s AS %s AT MILESTONE '<retained_milestone>' STORAGEID %s COMPUTEID %s" % (
        ident(names['retained']), ident(names['source']), names['source_sid'], names['retained_cid'])


def _r2b_items(block, names):
    """R2b, then R2c as the fallback: the live name takes a branch of the retained copy, or the retained record itself when a branch is refused."""
    block.check('R2b before: the original record is dropped', RECORD_STATE_SQL, (names['source'], names['source_sid']),
                ownersql.Expect('column_equals', 'IS_DROPPED', 1))
    block.check('R2b before: the retained name holds the recorded record READONLY', ATTACHED_SQL, (names['retained'],),
                ownersql.Expect('columns_equal', value={'STORAGE_ID': names['retained_sid'], 'COMPUTE_ID': names['retained_cid'], 'ATTACH_TYPE': 'READONLY'}))
    block.check('R2b before: no attached database named source', ATTACHED_SQL, (names['source'],), ownersql.Expect('no_rows'))
    block.statement(retained_branch_sql(names), 'R2b: attach a read-write branch of the retained copy under the live name')
    block.note('R2c, only when R2b was refused on the READ ONLY attachment (error %d): the two statements below are skipped while the live '
               'name is attached. The AT MILESTONE value is the ATTACH_SYNTAX of the retained record after its DETACH; copy it from the '
               'R2c check row and add AS %s after the name. The retained name then leaves the Portal; its data is under the live name as a '
               'resume record of it.' % (READ_ONLY_REFUSAL, names['source']))
    attached_source = (NAME_ATTACHED_COUNT_SQL, (names['source'],))
    block.statement('DETACH DATABASE %s' % ident(names['retained']), 'R2c: detach the retained name (only after a refused R2b)', skip_if=attached_source)
    block.check('R2c: the retained record is detached with its ATTACH_SYNTAX, or attached (R2b done)', ALL_RECORDS_SQL, (names['retained'],),
                ownersql.Expect('some_rows'))
    block.statement(retained_resume_sql(names), 'R2c: re-attach the retained record read write under the live name', skip_if=attached_source)
    block.check('R2 after: source attached READWRITE on the original storage', ATTACHED_SQL, (names['source'],),
                ownersql.Expect('columns_equal', value={'STORAGE_ID': names['source_sid'], 'ATTACH_TYPE': 'READWRITE'}))
    block.check('R2 after: the live name holds a record made from the retained copy', NAME_RECORD_SQL, (names['source'],),
                ownersql.Expect('columns_equal', value={'PREV_DATABASE_NAME': names['retained']}))
    block.note('R2 after: the three smallest non-empty tables match the journal source counts (the kit checks these).')


def original_syntax(names):
    """The R2 statement: the detached original's own ATTACH_SYNTAX as S1 recorded it, else the AT MILESTONE form with a milestone placeholder."""
    syntax = str(names.get('source_syntax') or '')
    if names.get('source_detached') and ' AT MILESTONE ' in syntax and not ownersql.placeholders(syntax):
        return syntax.strip().rstrip(';')
    return "ATTACH DATABASE %s AT MILESTONE '<source_milestone>' STORAGEID %s COMPUTEID %s" % (ident(names['source']), names['source_sid'], names['source_cid'])


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
            self.journal.record_attachment_row(record, rows[0])
            self.names[record + '_sid'] = rows[0].get('STORAGE_ID')
        return rows

    def _recorded(self, role):
        """The journal's record for a role (the latest attachments row), or None."""
        return self.journal.attachment(role)

    def _identify(self, label, name, expected=None, record=None, **fields):
        """identify(name) behind a check: the record must match expected (when given) and every field named (ACCESS, ATTACH_TYPE, ...).

        A record that differs refuses with both records printed and issues nothing. With record,
        the live record is stored in the journal for that role and the names are refreshed.
        """
        live = identify(self.session, name, expected)
        if expected is not None and not records_match(expected, live):
            self.journal.event('switch_check', '%s: FAILED (expected %s; observed %s)' % (label, record_text(expected), record_text(live)))
            raise KitError(refusal_text(label, expected, live))
        wrong = [] if live is not None else ['no record under the name %s' % name]
        for key, want in fields.items():
            got = None if live is None else live.get(key)
            if str(got).lower() != str(want).lower():
                wrong.append('%s = %s, expected %s' % (key, got, want))
        if wrong:
            self.journal.event('switch_check', '%s: FAILED (%s; observed %s)' % (label, '; '.join(wrong), record_text(live)))
            raise KitError('Switch refused at "%s": %s; observed %s' % (label, '; '.join(wrong), record_text(live)))
        self.journal.event('switch_check', '%s: ok (%s)' % (label, record_text(live)))
        self.log('    check %s: ok (%s)' % (label, record_text(live)))
        if record:
            journal_record(self.journal, record, live)
            self.names.update(ownersql.record_names(record, self.journal.attachment(record)))
        return live

    def _run(self, sql, label, refusable=False):
        """Execute one statement and journal it; refusable=True returns False instead of raising when the server refused it as read only."""
        try:
            self.session.execute(sql)
        except KitError:
            raise
        except Exception as exc:  # noqa: BLE001 - a failed statement is reported with the observed position
            if refusable and read_only_refusal(exc):
                self.journal.event('switch_statement_refused', '%s: %s %s' % (label, type(exc).__name__, getattr(exc, 'args', ('',))[0]))
                self.log('    refused %s (error %s); the fallback follows' % (label, getattr(exc, 'args', ('',))[0]))
                return False
            self.journal.event('switch_statement_failed', '%s: %s %s' % (label, type(exc).__name__, getattr(exc, 'args', ('',))[0]))
            try:
                where = self.position()
            except Exception:  # noqa: BLE001 - the position read may fail on the same broken connection
                where = UNKNOWN
            raise KitError('Switch statement "%s" failed: %s: %s; the switch is now at "%s"' % (label, type(exc).__name__, exc, where))
        self.journal.event('switch_statement', label)
        self.log('    executed %s' % label)
        return True

    def require(self, step, expected_state):
        """Refuse a step unless the live position is the one it starts from."""
        state = self.position()
        if state != expected_state:
            raise KitError('%s refused: the switch is at "%s", not "%s"; re-enter from the live state, never rerun blindly' % (step, state, expected_state))

    def preflight(self):
        """Before S1: every storage id recorded, the replacement attached READWRITE on its id, the retained name free, and each of the two
        storages holding its recorded record only (_storage_holds_only)."""
        n = self.names
        missing = [k for k in ('source_sid', 'replacement_sid') if not _known(n.get(k))]
        if missing:
            raise KitError('Switch refused before S1: %s not recorded (the replacement STORAGE_ID is recorded at O5)' % ', '.join(missing))
        self._expect('S1 before (for S3): replacement attached READWRITE with the recorded STORAGE_ID', ATTACHED_SQL, (n['replacement'],),
                     ownersql.Expect('columns_equal', value={'STORAGE_ID': n['replacement_sid'], 'ATTACH_TYPE': 'READWRITE'}))
        self._identify('S1 before (for S3): the replacement is the recorded record', n['replacement'], self._recorded('replacement'), ACCESS='READWRITE')
        self._expect('S1 before (for S2): no attached database named retained', ATTACHED_SQL, (n['retained'],), ownersql.Expect('no_rows'))
        self._storage_holds_only(STORAGE_ONLY_LABELS['source'], n['source_sid'], 'source')
        self._storage_holds_only(STORAGE_ONLY_LABELS['replacement'], n['replacement_sid'], 'replacement')

    def _storage_holds_only(self, label, sid, role):
        """Before S1: the entries (IS_DROPPED = 0) of a storage are the recorded record of role and nothing else; reads only.

        A branch the customer made of the source or the replacement (ATTACH ... AS keeps the
        parent's STORAGE_ID, fact 27), or a detached record left on the storage, would be a third
        entry when the switch ends. S2b and the two-entry gate would find it only after the
        original is dropped, mid-outage, so it refuses here with nothing issued, naming each record.
        """
        recorded = self._recorded(role)
        entries = [_record(r) for r in _rows(self.session, STORAGE_ENTRIES_SQL, (sid,))]
        known = record_key(recorded) is not None
        extra = [e for e in entries if not records_match(recorded, e)] if known else entries[1:]
        if len(entries) == 1 and not extra:
            self.journal.event('switch_check', '%s: ok (%s)' % (label, listing_text(entries)))
            self.log('    check %s: ok' % label)
            return entries
        self.journal.event('switch_check', '%s: FAILED (%s)' % (label, listing_text(entries)))
        raise KitError('Switch refused at "%s": the storage %s should hold one entry, the recorded %s record (%s), and holds: %s. Extra: %s. '
                       'The run ends with two entries, so a branch of the database or a detached record on its storage would stop the switch '
                       'after the original is dropped; nothing was issued. Remove each extra record with the customer, or leave this database '
                       'out of the run' % (label, sid, role, record_text(recorded), listing_text(entries), listing_text(extra)))

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
        self._identify('S1 before: the source is the recorded record', n['source'], self._recorded('source'))
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
        # the original record by name and record, never the first row of its storage (a branch or a dropped record shares the STORAGE_ID)
        self._expect('S1 after: no attached database named source', ATTACHED_SQL, (n['source'],), ownersql.Expect('no_rows'))
        self._identify('S1 after: the original record is detached with its ATTACH_SYNTAX recorded', n['source'], self._recorded('source'),
                       record='source', IS_DETACHED=1)

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
        self._expect('S2 after: retained attached READONLY', ATTACHED_SQL, (n['retained'],), ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READONLY'))
        self._identify('S2 after: the retained name holds a resume record of the original', n['retained'], record='retained', ACCESS='READONLY',
                       ATTACH_TYPE='resume', PREV_DATABASE_NAME=n['source'])
        self._small_table_check(n['retained'], 1)

    def s2b(self):
        """S2b, first half: rerun the S2 after-checks, attach the original record read write under the live name by its own ATTACH_SYNTAX, then s2b_drop."""
        n = self.names
        self.require('S2b', RETAINED)
        self.s2_after()
        self._expect('S2b before: no attached database named source', ATTACHED_SQL, (n['source'],), ownersql.Expect('no_rows'))
        self._run(self._original_syntax('S2b before'), 'S2b ATTACH original by its ATTACH_SYNTAX')
        self._identify('S2b after: the original record is attached READWRITE under the live name', n['source'], self._recorded('source'),
                       ACCESS='READWRITE', IS_DETACHED=0)
        self.s2b_drop()

    def s2b_proof(self):
        """The S2b zero-change proof on the original attached under the live name: the three smallest non-empty tables hold the journal's source
        counts and no foreign non-idle session is on it. A failed proof refuses with the original left attached read write (the rollback state)."""
        n = self.names
        problems = []
        try:
            self._small_table_check(n['source'], 3)
        except KitError as exc:
            problems.append(str(exc).replace('Switch refused: ', ''))
        sessions = _rows(self.session, SESSIONS_SQL, _sessions_params(n))
        busy = int(sessions[0]['n']) if sessions else 0
        if busy:
            problems.append('%d foreign non-idle session%s on the live name' % (busy, '' if busy == 1 else 's'))
        if problems:
            self.journal.event('switch_check', 'S2b zero-change proof: FAILED (%s)' % '; '.join(problems))
            raise KitError('Switch stopped at S2b: the zero-change proof failed, so the original record was not dropped. The original %s stays '
                           'attached read write under the live name, which is the rollback state. Reconcile first, then record the go again '
                           '(s2rebuild step O10 continues at S2b) or stay on the original. Differences: %s' % (n['source'], '; '.join(problems)))
        self.journal.event('switch_check', 'S2b zero-change proof: ok (three smallest tables equal the journal source counts; no foreign non-idle session)')
        self.log('    check S2b zero-change proof: ok')

    def s2b_drop(self):
        """S2b, second half: the zero-change proof, DROP DATABASE of the original record, and the after-checks (the storage keeps the retained record only)."""
        n = self.names
        self.require('S2b', ORIGINAL_ATTACHED)
        self._identify('S2b before: the live name holds the original record READWRITE', n['source'], self._recorded('source'), ACCESS='READWRITE')
        self.s2b_proof()
        self._run('DROP DATABASE %s' % ident(n['source']), 'S2b DROP source')
        self._expect('S2b after: the original record is dropped', RECORD_STATE_SQL, (n['source'], n['source_sid']),
                     ownersql.Expect('column_equals', 'IS_DROPPED', 1))
        self._expect('S2b after: no attached database named source', ATTACHED_SQL, (n['source'],), ownersql.Expect('no_rows'))
        entries = [_record(r) for r in _rows(self.session, STORAGE_ENTRIES_SQL, (n['source_sid'],))]
        retained = self._recorded('retained')
        if len(entries) != 1 or not records_match(retained, entries[0]):
            self.journal.event('switch_check', 'S2b after: the original storage holds only the retained record: FAILED (%s)' % listing_text(entries))
            raise KitError('Switch refused after S2b: the original storage %s should hold one entry, the retained record (%s), and holds: %s'
                           % (n['source_sid'], record_text(retained), listing_text(entries)))
        self.journal.event('switch_check', 'S2b after: the original storage holds only the retained record: ok (%s)' % listing_text(entries))
        self.log('    check S2b after: the original storage holds only the retained record: ok')
        self._small_table_check(n['retained'], 1)

    def s3(self):
        """S3: rerun the S2 after-checks, attach the replacement as a branch under the live name, then the S3 after-checks."""
        n = self.names
        self.require('S3', SOURCE_DROPPED)
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
                            ownersql.Expect('column_equals', 'ATTACH_TYPE', 'READWRITE'))
        taken = [str(n['source_sid']), str(n['replacement_sid'])] + ([str(n['retained_sid'])] if _known(n.get('retained_sid')) else [])
        sid = str(rows[0].get('STORAGE_ID'))
        if sid in taken:
            replacement_rows = _rows(self.session, ATTACHED_SQL, (n['replacement'],))
            if not live_is_branch_of_replacement(self.session, n, rows[0], replacement_rows[0] if replacement_rows else None):
                raise KitError('Switch refused after S3: the live name resolves to STORAGE_ID %s, which is not a new branch' % sid)
            label = 'S3 after: the live name is its own branch of the replacement (same STORAGE_ID, own COMPUTE_ID, PREV_DATABASE_NAME %s)' % n['replacement']
            self.journal.event('switch_check', '%s: ok' % label)
            self.log('    check %s: ok' % label)
        self._identify('S3 after: the live name holds a pitr record of the replacement', n['source'], record='live', ACCESS='READWRITE',
                       ATTACH_TYPE='pitr', PREV_DATABASE_NAME=n['replacement'])
        if self.layout is not None:
            self._expect('S3 after: the live database reports the target layout', LAYOUT_SQL, (n['source'],),
                         ownersql.Expect('columns_equal', value={'NUM_PARTITIONS': self.layout.partitions, 'NUM_SUB_PARTITIONS': self.layout.sub_partitions}))
        for label, sql, key in COUNT_CHECKS:
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

    def s4(self):
        """S4: rerun the S3 after-checks, drop the replacement record (create, parent of the live record), then the after-checks and the gate."""
        n = self.names
        self.require('S4', SWITCHED)
        self.s3_after()
        self._identify('S4 before: the replacement name holds the recorded replacement record', n['replacement'], self._recorded('replacement'),
                       ACCESS='READWRITE', ATTACH_TYPE='create')
        self._expect('S4 before: the live name is its own branch record of the replacement', BRANCH_OF_SQL,
                     (n['source'], n['replacement_sid'], n['replacement']), ownersql.Expect('column_at_least', 'n', 1))
        self._run('DROP DATABASE %s' % ident(n['replacement']), 'S4 DROP replacement')
        self._expect('S4 after: the replacement record is dropped', RECORD_STATE_SQL, (n['replacement'], n['replacement_sid']),
                     ownersql.Expect('column_equals', 'IS_DROPPED', 1))
        self._expect('S4 after: no attached database named replacement', ATTACHED_SQL, (n['replacement'],), ownersql.Expect('no_rows'))
        self._identify('S4 after: the live record is unchanged', n['source'], self._recorded('live'), ACCESS='READWRITE')
        self._small_table_check(n['source'], 1)
        self.gate()

    def record_listing(self, when):
        """Record the Portal entries of both storages (listing) in the journal as a switch_listing event; returns the entries."""
        entries = listing(self.session, self.names)
        self.journal.event('switch_listing', '%s: %s' % (when, listing_text(entries)))
        self.log('    entries %s: %s' % (when, listing_text(entries)))
        return entries

    def gate(self):
        """The two-entry gate: the entries with IS_DROPPED = 0 across both storages are exactly the live record and the retained record."""
        entries = self.record_listing('after')
        expected = {entry_key(self._recorded('live')): 'the live record', entry_key(self._recorded('retained')): 'the retained record'}
        if None in expected:
            raise KitError('Switch gate refused: the live or the retained record is not recorded whole; inspect the journal attachments')
        found = {entry_key(e): e for e in entries}
        extra = [record_text(e) for key, e in found.items() if key not in expected]
        missing = [label for key, label in expected.items() if key not in found]
        if extra or missing:
            self.journal.event('switch_check', 'gate: FAILED (extra: %s; missing: %s)' % ('; '.join(extra) or 'none', ', '.join(missing) or 'none'))
            raise KitError('Switch gate refused: the workspace should hold two entries, the live record and the retained record. Extra: %s. '
                           'Missing: %s. Inspect the records before any further statement; C3 (End state) carries the repair block for a switch '
                           'made without S2b and S4' % ('; '.join(extra) or 'none', ', '.join(missing) or 'none'))
        self.journal.event('switch_check', 'gate: ok (two entries, the live record and the retained record)')
        self.log('    check gate: ok (two entries, the live record and the retained record)')

    def forward(self, detach_kill=None):
        """Run the remaining forward steps from the live position, rerunning the after-checks on re-entry; returns the final state."""
        state = self.position()
        if state == LIVE_DETACHED:
            raise KitError('Switch cannot continue: a rollback is in progress (R1 ran); finish it with the rollback, then start a new switch')
        if state == ROLLED_BACK:
            raise KitError('Switch cannot continue: the live name holds a branch of the retained copy (a rollback ran through it); a new switch '
                           'is a new freeze and copy')
        if state == NOT_STARTED:
            self.s1(detach_kill)
            state = DETACHED
        if state == DETACHED:
            self.s2()
            state = RETAINED
        if state == RETAINED:
            self.s2b()
            state = SOURCE_DROPPED
        elif state == ORIGINAL_ATTACHED:
            self.log('    the original is attached read write under the live name; continuing S2b at the zero-change proof')
            self.s2b_drop()
            state = SOURCE_DROPPED
        if state == SOURCE_DROPPED:
            self.s3()
            state = SWITCHED
        if state == SWITCHED:
            self.s4()
            return COMPLETE
        if state == COMPLETE:
            self.log('    the live name already resolves to a new branch and the replacement is gone; rerunning the after-checks and the gate')
            self.s3_after()
            self.gate()
            return COMPLETE
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

    def _original_syntax(self, label='R2a before'):
        """S2b and R2a before: the original record is detached and matches the journal; returns its own ATTACH_SYNTAX (the AT MILESTONE form).

        The syntax comes from the live record. When S1 recorded the detached record, its syntax
        must be the same text: a different one means the record was attached and detached again
        since, which refuses with both printed.
        """
        expected = self._recorded('source')
        live = self._identify('%s: the original record is detached' % label, self.names['source'], expected, IS_DETACHED=1)
        syntax = str(live.get('ATTACH_SYNTAX') or '').strip().rstrip(';')
        if not syntax:
            raise KitError('Refused at %s: the detached original record %s shows no ATTACH_SYNTAX; attach it by hand from the Portal'
                           % (label, record_text(live)))
        recorded = (expected or {}).get('attach_syntax')
        if expected and int(expected.get('is_detached') or 0) and recorded and str(recorded).strip().rstrip(';') != syntax:
            self.journal.event('switch_check', '%s: ATTACH_SYNTAX differs from the one S1 recorded (recorded %s; live %s)' % (label, recorded, syntax))
            raise KitError('Refused at %s: the ATTACH_SYNTAX of the original record differs from the one S1 recorded, so the record '
                           'was attached and detached again since. Recorded: %s. Live: %s. Inspect the record before any further statement'
                           % (label, recorded, syntax))
        return syntax

    def original_dropped(self):
        """True when the recorded original record shows IS_DROPPED = 1 (S2b ran); reads the records of the source name."""
        return original_state(all_records(self.session, self.names['source']), self.names) == 'dropped'

    def rollback(self):
        """R1, then R2a (before S2b ran) or R2b with R2c as the fallback (after S2b) from the live position; returns the position reached.

        Refused once O11 started releasing writers or application writes were seen. From the
        switched position the zero-change proof runs first; behind S3 (source detached, retained
        attached, original dropped) nothing is attached under the live name, so R1 is skipped.
        The retained copy is never dropped.
        """
        n = self.names
        reason = rollback_refusal(self.journal)
        if reason:
            raise KitError('Rollback refused: %s; a rollback is now a new freeze and reconciliation' % reason)
        state = self.position()
        if state == NOT_STARTED:
            self.log('    nothing to roll back: the source is attached on its original storage')
            return NOT_STARTED
        if state == ORIGINAL_ATTACHED:
            self.log('    nothing to roll back: the original is attached read write under the live name (the rollback state)')
            return ORIGINAL_ATTACHED
        if state == ROLLED_BACK:
            self.log('    nothing to roll back: the live name holds a branch of the retained copy')
            return ROLLED_BACK
        if state == UNKNOWN:
            raise KitError('Rollback refused: the switch is at "unknown"; inspect the attachments before any further statement')
        if state in (SWITCHED, COMPLETE):
            self.zero_change_proof()
        self.journal.set_meta('rollback_started_utc', self.journal.get_meta('rollback_started_utc') or iso(utc_now()))
        if state in (SWITCHED, COMPLETE):
            self._identify('R1 before: the attached database named source is the recorded live record', n['source'], self._recorded('live'),
                           ACCESS='READWRITE')
            self._run('DETACH DATABASE %s' % ident(n['source']), 'R1 DETACH live branch')
            self._expect('R1 after: no attached database named source', ATTACHED_SQL, (n['source'],), ownersql.Expect('no_rows'))
        self._expect('R2 before: no attached database named source', ATTACHED_SQL, (n['source'],), ownersql.Expect('no_rows'))
        if self.original_dropped():
            return self._rollback_through_retained()
        # _original_syntax identifies the detached original by its record; the storage also lists the retained record after S2
        self._run(self._original_syntax(), 'R2a ATTACH original by its ATTACH_SYNTAX')
        self._expect('R2a after: source attached READWRITE with the original STORAGE_ID', ATTACHED_SQL, (n['source'],),
                     ownersql.Expect('columns_equal', value={'STORAGE_ID': n['source_sid'], 'ATTACH_TYPE': 'READWRITE'}))
        self._identify('R2a after: the original record is attached READWRITE', n['source'], self._recorded('source'), record='source', ACCESS='READWRITE')
        self.journal.set_meta('rolled_back_utc', iso(utc_now()))
        self.journal.event('rollback', 'R2a: original %s re-attached READWRITE by its own ATTACH_SYNTAX (%s); the retained copy %s stays attached read only'
                           % (n['source'], record_text(self._recorded('source')), n['retained']))
        return self.position()

    def repair_end_state(self):
        """C3 repair for a switch made without S2b and S4 (an older kit): the post-switch sequence proven on October 27, 2026.

        Detach the live name (behind its record check), attach the original record by its own
        syntax, record and row check, DROP it, re-attach the live record by its own syntax, DROP
        the replacement record behind its record check, then the gate. The live name is detached
        for about 30 seconds on a 3 TB database (fact 34). Runs through the C3 owner guard.

        Every part is decided from the live state, so a repair that stopped part way resumes: the
        original attached under the live name continues at the proof, an original already dropped
        is not attached again, a live record already detached is re-attached, and a replacement
        already gone is not dropped again. The live name is detached only while the original
        record still needs its attach by syntax. With the live record attached, the original
        dropped and the replacement name free, there is nothing to repair: the repair refuses
        with the listing before any statement.
        """
        n = self.names
        live, original = self._recorded('live'), self._recorded('source')
        if live is None or original is None:
            raise KitError('C3 repair refused: the live record or the original record is not recorded in the journal; nothing was issued')
        held = identify(self.session, n['source']) if _rows(self.session, ATTACHED_SQL, (n['source'],)) else None
        dropped = self.original_dropped()
        if held is not None and records_match(live, held) and dropped and not _rows(self.session, ATTACHED_SQL, (n['replacement'],)):
            entries = listing(self.session, n)
            self.journal.event('switch_check', 'repair: nothing to repair (%s)' % listing_text(entries))
            raise KitError('C3 repair refused: nothing to repair, so nothing was issued. The live name holds the live record, the original record '
                           'is dropped and no database is attached under the replacement name, so the extra entry is not one the repair removes. '
                           'Find out who attached it. Entries: %s' % listing_text(entries))
        if not dropped:
            if held is not None and records_match(original, held):
                self._identify('repair: the live name holds the original record READWRITE', n['source'], original, ACCESS='READWRITE', IS_DETACHED=0)
                self.log('    the original record is attached read write under the live name; continuing the repair at the proof')
            else:
                if held is not None:
                    self._identify('repair: the live name holds the recorded live record', n['source'], live, ACCESS='READWRITE')
                    self._run('DETACH DATABASE %s' % ident(n['source']), 'repair DETACH live')
                    self._expect('repair: no attached database named source', ATTACHED_SQL, (n['source'],), ownersql.Expect('no_rows'))
                self._run(self._original_syntax('repair'), 'repair ATTACH original by its ATTACH_SYNTAX')
                self._identify('repair: the original record is attached READWRITE under the live name', n['source'], original,
                               ACCESS='READWRITE', IS_DETACHED=0)
            try:
                self.s2b_proof()
            except KitError as exc:
                raise KitError('C3 repair stopped at the proof, so the original record was not dropped: it stays attached read write under the live '
                               'name and the live record stays detached. Reconcile, then rerun s2rebuild step C3, which continues at the proof. '
                               'Differences: %s' % str(exc).split('Differences: ', 1)[-1]) from exc
            self._run('DROP DATABASE %s' % ident(n['source']), 'repair DROP original record')
            self._expect('repair: the original record is dropped', RECORD_STATE_SQL, (n['source'], n['source_sid']),
                         ownersql.Expect('column_equals', 'IS_DROPPED', 1))
            held = None
        if held is None:
            detached = self._identify('repair: the live record is detached with its ATTACH_SYNTAX', n['source'], live, IS_DETACHED=1)
            syntax = str(detached.get('ATTACH_SYNTAX') or '').strip().rstrip(';')
            if ' AT MILESTONE ' not in syntax:
                raise KitError('C3 repair stopped: the detached live record %s shows no AT MILESTONE syntax; attach it by hand from the Portal'
                               % record_text(detached))
            self._run(syntax, 'repair ATTACH live record by its ATTACH_SYNTAX')
            self._identify('repair: the live name holds the live record READWRITE again', n['source'], live, record='live', ACCESS='READWRITE', IS_DETACHED=0)
        else:
            self._identify('repair: the live name holds the recorded live record', n['source'], live, ACCESS='READWRITE')
        self._small_table_check(n['source'], 1)
        if _rows(self.session, ATTACHED_SQL, (n['replacement'],)):
            self._identify('repair: the replacement name holds the recorded replacement record', n['replacement'], self._recorded('replacement'),
                           ACCESS='READWRITE', ATTACH_TYPE='create')
            self._run('DROP DATABASE %s' % ident(n['replacement']), 'repair DROP replacement')
            self._expect('repair: the replacement record is dropped', RECORD_STATE_SQL, (n['replacement'], n['replacement_sid']),
                         ownersql.Expect('column_equals', 'IS_DROPPED', 1))
        self.gate()

    def _rollback_through_retained(self):
        """R2b: a read-write branch of the retained copy under the live name; R2c when the server refuses it on the READ ONLY attachment."""
        n = self.names
        self._identify('R2b before: the retained name holds the recorded record READONLY', n['retained'], self._recorded('retained'), ACCESS='READONLY')
        if self._run(retained_branch_sql(n), 'R2b ATTACH retained AS source (branch)', refusable=True):
            how = 'R2b: a read-write pitr branch of the retained copy %s is attached under the live name %s; the retained copy stays attached read only'
            how %= (n['retained'], n['source'])
        else:
            self._identify('R2c before: the retained name still holds the recorded record READONLY', n['retained'], self._recorded('retained'),
                           ACCESS='READONLY')
            self._run('DETACH DATABASE %s' % ident(n['retained']), 'R2c DETACH retained')
            detached = self._identify('R2c: the retained record is detached with its ATTACH_SYNTAX', n['retained'], self._recorded('retained'), IS_DETACHED=1)
            syntax = retained_resume_sql(n, detached.get('ATTACH_SYNTAX'))
            if ownersql.placeholders(syntax):
                raise KitError('Rollback stopped at R2c: the detached retained record %s shows no AT MILESTONE syntax; the live name is free and the '
                               'retained name is detached. Attach the record by hand from its ATTACH_SYNTAX with AS %s' % (record_text(detached), n['source']))
            self._run(syntax, 'R2c ATTACH retained record AS source by its ATTACH_SYNTAX')
            how = ('R2c: the server refused a branch of the READ ONLY retained copy, so its record is attached read write under the live name %s as '
                   'a resume record; the retained name %s is gone from the attached list (its record is detached) and the data is under the live '
                   'name' % (n['source'], n['retained']))
        self._expect('R2 after: source attached READWRITE on the original storage', ATTACHED_SQL, (n['source'],),
                     ownersql.Expect('columns_equal', value={'STORAGE_ID': n['source_sid'], 'ATTACH_TYPE': 'READWRITE'}))
        # the record under the live name is new (a pitr or resume record of the retained copy): the rollback event carries it, and the journal's
        # source row keeps the original record, which the position reads to tell this state from the original attached
        back = self._identify('R2 after: the live name holds a record made from the retained copy', n['source'], ACCESS='READWRITE',
                              PREV_DATABASE_NAME=n['retained'])
        self._small_table_check(n['source'], 3)
        self.journal.set_meta('rolled_back_utc', iso(utc_now()))
        self.journal.event('rollback', '%s (%s)' % (how, record_text(back)))
        self.log('    ' + how)
        return ROLLED_BACK


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
    """Record the replacement's attached record (identify) when the journal has none; returns the storage id or None."""
    current = ctx.journal.attachment('replacement')
    if current and current.get('storage_id'):
        return current['storage_id']
    live = identify(session, ctx.db.replacement)
    if live is None or live.get('ACCESS') is None:
        return None
    journal_record(ctx.journal, 'replacement', live)
    return live['STORAGE_ID']


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
    for variant, companion in ownersql.companion_blocks('O10', names, ctx.config, rollback=True, **inputs):
        ownersql.print_block(companion, ctx, ownersql.owner_sql_path(ctx, 'O10', rollback=True, variant=variant))
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
        return steps.Outcome(steps.REFUSED, 'O10 needs the capture (O4 or P3) for the S3 table, view, pipeline and routine counts; none was found', 1)
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
        machine.record_listing('before')
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
    return steps.Outcome(steps.PASSED, 'switch complete: two entries, the live name %s (STORAGE_ID %s, COMPUTE_ID %s) and the retained copy %s '
                         '(STORAGE_ID %s, COMPUTE_ID %s); the original record and the replacement are dropped' % (
                             names['source'], names.get('live_sid'), names.get('live_cid'), names['retained'], names.get('retained_sid'),
                             names.get('retained_cid')))


steps.register('O10', handle_o10)
