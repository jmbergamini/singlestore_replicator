"""s2rebuild ownersql: ordered admin blocks with precondition checks, executed with gates or printed.

Author: John Bergamini

Section 10.17 of the architecture. A Block is an ordered list of checks, statements, notes,
stop items and capture items for one admin step. With the owner profile the kit executes it
statement by statement and stops at the first failed check with the observed state; without it
the kit writes owner-sql/<step>.sql and prints the same block, and a person records the gate.
The same gate logic ships as OWNERLIB_SOURCE, a single-file, dependency-free module for the
owner notebook that works with any DB-API cursor. Statements issued here: only those of the
block, through the owner guard of the step (section 10.4); the P1 and C4 account blocks
(CREATE USER, GRANT, DROP USER; C4 first checks that no view in the live database has the
runner as its definer) and the C3 cleanup block (DROP DATABASE, by the recorded STORAGE_ID
and attach type) are built in this module. A DETACH that waits longer than
freeze.detach_wait_seconds is cancelled with KILL QUERY on the kit's own connection and issued
again WITH KILL when freeze.detach_kill is set. Before a re-copy reopens the copy evidence,
retire_o10_blocks rewrites the O10 block files already written so each starts with a STOP.
"""
import json
import os
import re
import threading
from pathlib import Path

from . import steps
from .common import EXIT_GATE, EXIT_REFUSED, KitError, iso, parse_utc, utc_now
from .sqlutil import ident, owner_guard, qualified

RUNNER_PRIVILEGES = ('SELECT', 'INSERT', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'CREATE DATABASE', 'DROP DATABASE', 'PROCESS',
                     'CREATE VIEW', 'SHOW VIEW')
PASSWORD_PLACEHOLDER = '<password>'
PLACEHOLDER_SID = re.compile(r'<[a-z_]+_(?:sid|cid|type|syntax|milestone)>')
CANCEL_JOIN_SECONDS = 30
CLEANUP_ATTACH_TYPES = {'retained': 'READONLY', 'replacement': 'READWRITE'}


class Expect:
    """What a check query must return.

    Kinds: row_count, no_rows, some_rows, column_equals, column_at_least, column_not_in (value is
    a list), columns_equal (value is {column: value}) and all_column_equals.
    """

    def __init__(self, kind, column=None, value=None, count=None):
        self.kind = kind
        self.column = column
        self.value = value
        self.count = count

    def evaluate(self, rows):
        """(ok, observed text) for the rows a check returned."""
        rows = list(rows)

        def _cell(row, column):
            return row[column] if isinstance(row, dict) else row[0]
        if self.kind == 'row_count':
            return len(rows) == self.count, '%d row(s)' % len(rows)
        if self.kind == 'no_rows':
            return not rows, '%d row(s)' % len(rows)
        if self.kind == 'some_rows':
            return bool(rows), '%d row(s)' % len(rows)
        if not rows and self.kind in ('column_equals', 'column_at_least', 'column_not_in', 'columns_equal', 'all_column_equals'):
            return False, 'no rows'
        if self.kind == 'column_equals':
            observed = _cell(rows[0], self.column)
            return str(observed) == str(self.value), '%s = %s' % (self.column, observed)
        if self.kind == 'column_at_least':
            observed = _cell(rows[0], self.column)
            try:
                ok = int(observed) >= int(self.value)
            except (TypeError, ValueError):
                ok = False
            return ok, '%s = %s' % (self.column, observed)
        if self.kind == 'column_not_in':
            observed = _cell(rows[0], self.column)
            return str(observed) not in [str(v) for v in self.value], '%s = %s' % (self.column, observed)
        if self.kind == 'columns_equal':
            observed = {c: rows[0][c] for c in self.value} if isinstance(rows[0], dict) else {}
            ok = bool(observed) and all(str(observed[c]) == str(v) for c, v in self.value.items())
            return ok, ', '.join('%s = %s' % (c, observed.get(c)) for c in self.value)
        if self.kind == 'all_column_equals':
            bad = [str(_cell(r, self.column)) for r in rows if str(_cell(r, self.column)) != str(self.value)]
            return not bad, ('all %d rows %s = %s' % (len(rows), self.column, self.value)) if not bad else ('%s in (%s)' % (self.column, ', '.join(bad)))
        raise KitError('Unknown expectation ' + str(self.kind))

    def text(self):
        """The expectation in words, for the printed block."""
        if self.kind == 'row_count':
            return 'exactly %d row(s)' % self.count
        if self.kind == 'no_rows':
            return 'no rows'
        if self.kind == 'some_rows':
            return 'at least one row'
        if self.kind == 'column_equals':
            return '%s = %s' % (self.column, self.value)
        if self.kind == 'column_at_least':
            return '%s >= %s' % (self.column, self.value)
        if self.kind == 'column_not_in':
            return '%s not in (%s)' % (self.column, ', '.join(str(v) for v in self.value))
        if self.kind == 'columns_equal':
            return ', '.join('%s = %s' % kv for kv in self.value.items())
        if self.kind == 'all_column_equals':
            return 'every row %s = %s' % (self.column, self.value)
        return self.kind

    def to_dict(self):
        """JSON form for the notebook."""
        return {'kind': self.kind, 'column': self.column, 'value': self.value, 'count': self.count}


class Item:
    """One line of a block: a check, a statement, a note, a stop (refuse) or a pipeline capture.

    A statement may carry skip_if (a COUNT query and its parameters; the statement is skipped
    when the count is above zero), detach ({'wait': seconds, 'kill': bool} for the DETACH
    escalation), and the pipeline name and source type used to look up its credentials.
    """

    def __init__(self, kind, sql='', label='', params=(), expect=None, record=None, skip_if=None, detach=None,
                 pipeline=None, source_type=None, database=None):
        self.kind = kind
        self.sql = sql
        self.label = label
        self.params = tuple(params)
        self.expect = expect
        self.record = record
        self.skip_if = (skip_if[0], tuple(skip_if[1])) if skip_if else None
        self.detach = dict(detach) if detach else None
        self.pipeline = pipeline
        self.source_type = source_type
        self.database = database

    def to_dict(self):
        """JSON form for the notebook."""
        return {'kind': self.kind, 'sql': self.sql, 'label': self.label, 'params': list(self.params),
                'expect': self.expect.to_dict() if self.expect else None, 'record': self.record,
                'skip_if': [self.skip_if[0], list(self.skip_if[1])] if self.skip_if else None, 'detach': self.detach,
                'pipeline': self.pipeline, 'source_type': self.source_type, 'database': self.database}


def _render_sql(sql, params):
    for p in params:
        sql = sql.replace('%s', "'%s'" % str(p).replace("'", "''"), 1)
    return sql


class Block:
    """The ordered checks, statements and notes of one admin step."""

    def __init__(self, step_id, title, guard_step=None):
        self.step_id = step_id
        self.title = title
        self.guard_step = guard_step or step_id
        self.items = []

    def check(self, label, sql, params=(), expect=None, record=None):
        """Add a precondition or postcondition query; record names a journal attachment role to store the first row under."""
        self.items.append(Item('check', sql, label, params, expect or Expect('some_rows'), record))
        return self

    def statement(self, sql, label='', skip_if=None, detach=None, pipeline=None, source_type=None):
        """Add a statement to execute (see Item for skip_if, detach, pipeline and source_type)."""
        self.items.append(Item('statement', sql, label, skip_if=skip_if, detach=detach, pipeline=pipeline, source_type=source_type))
        return self

    def note(self, text):
        """Add a line for the reader (printed as a comment, never executed)."""
        self.items.append(Item('note', '', text))
        return self

    def refuse(self, text):
        """Add a stop: printed as a STOP comment; execution (kit or notebook) refuses at this point."""
        self.items.append(Item('refuse', '', text))
        return self

    def capture(self, database, text):
        """Add a pipeline capture point: the notebook reads every definition and offset of database into its result."""
        self.items.append(Item('capture', '', text, database=database))
        return self

    @property
    def statements(self):
        """The statement items of the block, in order."""
        return [i for i in self.items if i.kind == 'statement']

    @property
    def refusals(self):
        """The text of every stop item."""
        return [i.label for i in self.items if i.kind == 'refuse']

    def render(self):
        """The block as SQL text: notes, stops and checks as comments with their expected results, statements with semicolons."""
        lines = ['-- %s: %s' % (self.step_id, self.title), '-- Run in order. Stop at the first check that does not return what is expected.']
        for item in self.items:
            if item.kind == 'note':
                lines.append('-- NOTE: ' + item.label)
            elif item.kind == 'refuse':
                lines.append('-- STOP: ' + item.label)
            elif item.kind == 'capture':
                lines.append('-- CAPTURE: ' + item.label)
            elif item.kind == 'check':
                lines.append('-- CHECK %s; expect %s' % (item.label, item.expect.text()))
                lines.append(_render_sql(item.sql, item.params) + ';')
            else:
                if item.label:
                    lines.append('-- ' + item.label)
                if item.skip_if:
                    lines.append('-- SKIP the next statement when this returns n > 0: ' + _render_sql(*item.skip_if) + ';')
                lines.append(item.sql + ';')
        return '\n'.join(lines) + '\n'

    def to_dict(self):
        """JSON form of the whole block for the notebook."""
        return {'step_id': self.step_id, 'title': self.title, 'items': [i.to_dict() for i in self.items]}


def write_block(block, path):
    """Write the rendered block to path with mode 0600 (the O9 block holds pipeline definitions)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w', encoding='utf8') as fh:
        fh.write(block.render())
    os.chmod(str(path), 0o600)
    return path


def _first_value(rows):
    if not rows:
        return None
    row = rows[0]
    return list(row.values())[0] if isinstance(row, dict) else row[0]


def _errno(exc):
    args = getattr(exc, 'args', ())
    return args[0] if args and isinstance(args[0], int) else 'unknown'


def _statement_label(block, item):
    return item.label or ' '.join(item.sql.split(' ', 2)[:2])


def _run_statement(session, sql, block, item, secret, journal):
    """Execute one statement; a driver error becomes a KitError naming the step and statement.

    When the statement carried an injected secret, only the error number is reported, since a
    server message can echo the statement text near the secret.
    """
    try:
        session.execute(sql)
    except KitError:
        raise
    except Exception as exc:  # noqa: BLE001 - every driver error becomes a refusal with the observed state
        label = _statement_label(block, item)
        if journal is not None:
            journal.event('owner_statement_failed', '%s %s: %s %s' % (block.step_id, label, type(exc).__name__, _errno(exc)))
        if secret:
            raise KitError('%s failed at "%s": server error %s; the message is withheld because the statement carried an injected secret'
                           % (block.step_id, label, _errno(exc)))
        raise KitError('%s failed at "%s": %s: %s' % (block.step_id, label, type(exc).__name__, exc))


def detach_with_escalation(open_session, guard, database, wait_seconds, kill, log, journal=None):
    """DETACH DATABASE on a second owner connection; after wait_seconds cancel it and issue it again WITH KILL.

    Section 10.7 step 2 and 10.13 S1. The waiting DETACH is cancelled with KILL QUERY on the
    kit's own connection id only (a guard built for that one id). Without detach_kill the waiting
    DETACH is cancelled and the step refuses with the bridge guidance. Returns 'plain' or 'kill'.
    """
    sql = 'DETACH DATABASE %s' % ident(database)
    worker = open_session(guard)
    outcome = {}
    try:
        cid = int(_first_value(worker.query('SELECT CONNECTION_ID() AS id')))

        def _run():
            try:
                worker.execute(sql)
                outcome['done'] = True
            except Exception as exc:  # noqa: BLE001 - reported by the caller thread
                outcome['error'] = exc
        thread = threading.Thread(target=_run, name='s2rebuild-detach', daemon=True)
        thread.start()
        thread.join(wait_seconds)
        if not thread.is_alive():
            if 'error' in outcome:
                exc = outcome['error']
                raise KitError('DETACH DATABASE %s failed: %s: %s' % (database, type(exc).__name__, exc))
            log('    executed: DETACH DATABASE %s (finished within %d s)' % (database, wait_seconds))
            return 'plain'
        log('    DETACH DATABASE %s still waits after %d s (writers block it); cancelling it on connection %d' % (database, wait_seconds, cid))
        canceller = open_session(owner_guard('owner detach cancel', ('KILL QUERY',), (), required_detail={'KILL QUERY': str(cid)}))
        try:
            canceller.execute('KILL QUERY %d' % cid)
        finally:
            canceller.close()
        thread.join(CANCEL_JOIN_SECONDS)
        if thread.is_alive():
            raise KitError('The waiting DETACH DATABASE %s did not stop after KILL QUERY %d; inspect the attachments before any further statement'
                           % (database, cid))
        if outcome.get('done'):
            log('    executed: DETACH DATABASE %s (finished as it was being cancelled)' % database)
            return 'plain'
        if journal is not None:
            journal.event('detach_wait', '%s waited %d s; cancelled with KILL QUERY on connection %d' % (database, wait_seconds, cid))
        if not kill:
            raise KitError('DETACH DATABASE %s waited more than %d s and freeze.detach_kill is off; the waiting DETACH was cancelled. '
                           'Stop the writers (s2rebuild lockout works as a bridge), then rerun the step' % (database, wait_seconds))
    finally:
        worker.close()
    killer = open_session(guard)
    try:
        killer.execute(sql + ' KILL')
    finally:
        killer.close()
    if journal is not None:
        journal.event('detach_kill', 'DETACH DATABASE %s KILL issued after %d s' % (database, wait_seconds))
    log('    executed: DETACH DATABASE %s KILL (the blocking workload was terminated)' % database)
    return 'kill'


def execute_block(block, session, log, journal=None, substitute=None, open_session=None, guard=None, before_first_statement=None):
    """Run the block through a guarded session: every check must pass before the next statement runs.

    substitute(sql) may rewrite a statement just before execution (credential or password
    injection); the rewritten text is never logged and a failure of such a statement reports
    the error number only. Checks with a record role store their first row as an attachment in
    the journal. A stop item refuses; a capture item is done by the caller afterwards. A
    statement with skip_if is skipped when its count is above zero. A DETACH with detach
    settings escalates through detach_with_escalation when open_session is given. Raises
    KitError at the first failed check with the observed state. Returns the list of
    (label, observed) check results.

    before_first_statement() runs once, when the first statement is about to execute or is
    found already done by its skip_if count: every earlier check passed and the session is
    open. A block that stops at a check, or fails before that point, never calls it.
    """
    results = []
    hook = before_first_statement

    def _first_statement():
        nonlocal hook
        if hook is not None:
            due, hook = hook, None
            due()

    for item in block.items:
        if item.kind == 'note':
            log('    note: ' + item.label)
            continue
        if item.kind == 'refuse':
            if journal is not None:
                journal.event('owner_refused', '%s: %s' % (block.step_id, item.label))
            raise KitError('%s refused: %s' % (block.step_id, item.label))
        if item.kind == 'capture':
            continue
        if item.kind == 'check':
            rows = session.query(item.sql, item.params or None)
            ok, observed = item.expect.evaluate(rows)
            results.append((item.label, observed))
            if journal is not None:
                journal.event('owner_check', '%s %s: %s (%s)' % (block.step_id, item.label, 'ok' if ok else 'FAILED', observed))
            if not ok:
                raise KitError('%s stopped at check "%s": expected %s, observed %s' % (block.step_id, item.label, item.expect.text(), observed))
            log('    check %s: ok (%s)' % (item.label, observed))
            if item.record and rows and journal is not None:
                journal.record_attachment_row(item.record, rows[0])
            continue
        if item.skip_if:
            count = _first_value(session.query(item.skip_if[0], item.skip_if[1] or None))
            _first_statement()
            if count is not None and int(count) > 0:
                log('    skipped: %s (already done)' % _statement_label(block, item))
                if journal is not None:
                    journal.event('owner_skipped', '%s %s' % (block.step_id, _statement_label(block, item)))
                continue
        if item.detach and open_session is not None and guard is not None:
            m = re.match(r'DETACH DATABASE `([A-Za-z0-9_]+)`$', item.sql)
            if m:
                _first_statement()
                form = detach_with_escalation(open_session, guard, m.group(1), int(item.detach.get('wait', 120)),
                                              bool(item.detach.get('kill')), log, journal)
                if journal is not None:
                    journal.event('owner_statement', '%s %s (%s)' % (block.step_id, _statement_label(block, item), form))
                continue
        sql = substitute(item.sql) if substitute else item.sql
        _first_statement()
        _run_statement(session, sql, block, item, sql != item.sql, journal)
        if journal is not None:
            journal.event('owner_statement', '%s %s' % (block.step_id, _statement_label(block, item)))
        log('    executed: %s' % (item.label or item.sql[:80]))
    return results


# ---------------------------------------------------------------------------
# Names and blocks
# ---------------------------------------------------------------------------
def names_for(ctx):
    """Everything a block needs about the run: names, storage ids, users, layout, settings and the planned table counts (rollback proof)."""
    journal = ctx.journal
    names = {
        'source': ctx.db.source, 'replacement': ctx.db.replacement, 'retained': ctx.db.retained,
        'partitions': ctx.db.layout.partitions, 'sub_partitions': ctx.db.layout.sub_partitions,
        'runner_user': ctx.config.runner.user, 'owner_user': ctx.config.owner.user if ctx.config.owner else None,
        'freeze_mode': ctx.config.freeze.mode, 'detach_wait_seconds': ctx.config.freeze.detach_wait_seconds,
        'detach_kill': ctx.config.freeze.detach_kill, 'quiet_max_age_seconds': ctx.config.freeze.quiet_max_age_seconds,
        'flexible_parallelism': ctx.config.closeout.flexible_parallelism,
        'retain_days': ctx.config.closeout.retain_days, 'run_id': ctx.run_id, 'freeze_time': journal.get_meta('freeze_time'),
        'workspace_id': ctx.config.workspace.get('id'), 'table_counts': table_counts(journal), 'source_counts': source_counts(journal),
        'source_dropped': any(d.startswith('S2b DROP') or d.startswith('repair DROP original') for d in journal.events('switch_statement')),
        'retain_until_utc': journal.get_meta('retain_until_utc'),
    }
    for role in ('source', 'replacement', 'retained', 'live'):
        names.update(record_names(role, journal.attachment(role)))
    return names


def record_names(role, att):
    """The block names of one recorded role: <role>_sid, _cid, _type, _syntax and _detached, with placeholders where nothing is recorded."""
    att = att or {}
    return {role + '_sid': att.get('storage_id') or '<%s_sid>' % role, role + '_cid': att.get('compute_id') or '<%s_cid>' % role,
            role + '_type': att.get('record_type') or '<%s_type>' % role, role + '_syntax': att.get('attach_syntax') or '<%s_syntax>' % role,
            role + '_detached': att.get('is_detached')}


def record_expect(names, role, **more):
    """The columns_equal values of a MV_CLOUD_BRANCHES record check for a role: LATEST_COMPUTE_ID, ATTACH_TYPE when recorded, and more."""
    value = {'LATEST_COMPUTE_ID': names[role + '_cid']}
    if not placeholders(str(names.get(role + '_type') or '')):
        value['ATTACH_TYPE'] = names[role + '_type']
    value.update(more)
    return value


def table_counts(journal):
    """[(table, target_count)] for every planned table of the journal, by name; target_count is None until the O6 count proof."""
    return [(t['table_name'], t.get('target_count')) for t in sorted(journal.tables(), key=lambda t: t['table_name'])]


def source_counts(journal):
    """[(table, source_count)] of the planned tables with a source count above zero, smallest first: the S2b zero-change proof and the repair."""
    rows = [t for t in journal.tables() if t.get('source_count')]
    rows.sort(key=lambda t: (int(t['source_count']), t['table_name']))
    return [(t['table_name'], int(t['source_count'])) for t in rows]


def withheld_privileges(journal):
    """The lockout snapshot rows O11 has not given back yet, as {grantee: {scope: [privileges]}} (grants.unrestored_snapshot)."""
    from . import grants
    return grants.unrestored_snapshot(journal)


def withheld_checks(snapshot, db_name):
    """[(grantee, scope, sql, params)]: COUNT of the snapshot's write privileges a grantee holds now, per scope (the rollback proof).

    Every count must be 0 until O11 gives the privileges back; a privilege back on the live
    name means the release began, so the rollback is refused. Issues nothing (reads only).
    """
    from . import grants
    checks = []
    for user, scopes in sorted(grants.normalize(snapshot).items()):
        for scope, privs in sorted(scopes.items()):
            sql, params = grants.privilege_count_check(user, scope, privs, db_name)
            checks.append((user, scope, sql, params))
    return checks


def placeholders(text):
    """The <role_sid>, <role_cid>, <role_type>, <role_syntax> and <role_milestone> placeholders still present in a text."""
    return sorted(set(PLACEHOLDER_SID.findall(text)))


def expected_counts(ctx):
    """{'tables', 'views', 'pipelines', 'routines'} the live name must show after S3, from the newest capture; None without one.

    The pipeline count is None when no pipeline capture exists (no owner profile yet); the
    routine count is the ROUTINES row count the O7 gate recorded for the replacement
    (routines.expected_count), None when no gate recorded one.
    """
    from . import copy as copy_module, pipelines, routines
    try:
        data = copy_module.load_plans(ctx)[0]
    except KitError:
        return None
    cap = pipelines.capture_for(ctx)
    return {'tables': len(data.get('tables') or []), 'views': len(data.get('views') or []),
            'pipelines': len(pipelines.expected_items(cap['items'])) if cap['exists'] else None,
            'routines': routines.expected_count(ctx.journal)}


def block_inputs(ctx):
    """The journal and capture state every block builder needs: pipelines, grants, counts, capture step, route, release.

    released is True once O11 started releasing writers (switch.writers_released): the
    rollback block then carries a STOP. grants (the lockout snapshot not given back yet) also
    feeds the rollback proof's privilege checks and, on Path B, the forward block's S1 privilege
    checks. s0_problems (switch.s0_journal_problems) puts
    a STOP at the head of the O10 forward block while S0 would refuse on the journal.
    """
    from . import grants, pipelines, switch
    cap = pipelines.capture_for(ctx)
    return {'pipelines': cap['items'], 'capture_step': cap['step'], 'grants': grants.unrestored_snapshot(ctx.journal),
            'counts': expected_counts(ctx), 'route': ctx.config.pipelines.route,
            'released': switch.writers_released(ctx.journal),
            'app_writes_seen': ctx.journal.get_meta('app_writes_seen'),
            's0_problems': switch.s0_journal_problems(ctx.journal)}


def grantee(user):
    """'user'@'%' for a user name."""
    return "'%s'@'%%'" % user


def privileges_count_sql():
    """COUNT of global privileges for a grantee (parameter: the grantee string)."""
    return 'SELECT COUNT(*) AS n FROM information_schema.USER_PRIVILEGES WHERE GRANTEE = %s'


def definer_views_sql():
    """COUNT of the views of a database whose DEFINER is an account name, on any host (parameters: database, user name).

    information_schema.VIEWS gives DEFINER as user@host text, so the user part before the @ is
    compared. A view whose definer is gone can no longer be executed, so C4 expects 0 for the
    runner before DROP USER. Issues nothing (reads only).
    """
    return "SELECT COUNT(*) AS n FROM information_schema.VIEWS WHERE TABLE_SCHEMA = %s AND SUBSTRING_INDEX(DEFINER, '@', 1) = %s"


def account_block(names, drop=False):
    """P1: CREATE USER and GRANT for the runner; C4 (drop=True): no live view defined by the runner, DROP USER and the zero-privilege check."""
    user = names['runner_user']
    if drop:
        block = Block('C4', 'Remove the migration account')
        block.check('runner account still has privileges', privileges_count_sql(), (grantee(user),), Expect('column_at_least', 'n', 1))
        runner_definer_checks(block, names)
        block.statement("DROP USER %s" % grantee(user), 'drop the runner account')
        block.check('zero privileges remain', privileges_count_sql(), (grantee(user),), Expect('column_equals', 'n', 0))
        block.note('Delete the Keychain item that held the runner password.')
        return block
    block = Block('P1', 'Create the runner account')
    block.check('runner account does not exist yet', privileges_count_sql(), (grantee(user),), Expect('column_equals', 'n', 0))
    block.statement('CREATE USER %s IDENTIFIED BY %s' % (grantee(user), "'" + PASSWORD_PLACEHOLDER + "'"), 'create the runner (the password is never printed)')
    block.statement('GRANT %s ON *.* TO %s' % (', '.join(RUNNER_PRIVILEGES), grantee(user)), 'grant the runner privileges of section 5')
    block.check('every privilege is present', privileges_count_sql(), (grantee(user),), Expect('column_equals', 'n', len(RUNNER_PRIVILEGES)))
    return block


def attached_otherwise_sql():
    """COUNT of databases attached under a name with another STORAGE_ID or ATTACH_TYPE (parameters: name, STORAGE_ID, ATTACH_TYPE).

    0 when the name holds the recorded database or nothing, so the C3 identity check also
    passes on a rerun after that name was dropped. Issues nothing (reads only).
    """
    return ('SELECT COUNT(*) AS n FROM information_schema.MV_CLOUD_ATTACHED_DATABASES '
            'WHERE DATABASE_NAME = %s AND (STORAGE_ID <> %s OR ATTACH_TYPE <> %s)')


def attached_elsewhere_sql():
    """COUNT of attachments of a STORAGE_ID under any other name (parameters: STORAGE_ID, name).

    0 when the recorded database is attached under its own name or nowhere. A positive count
    means the database was moved or the name in the block is not the recorded one, so C3
    refuses before any DROP. Issues nothing (reads only).
    """
    return 'SELECT COUNT(*) AS n FROM information_schema.MV_CLOUD_ATTACHED_DATABASES WHERE STORAGE_ID = %s AND DATABASE_NAME <> %s'


def storage_attached_sql():
    """COUNT of attachments of a STORAGE_ID under any name (parameter: STORAGE_ID): the C3 gate that the database is gone.

    Issues nothing (reads only).
    """
    return 'SELECT COUNT(*) AS n FROM information_schema.MV_CLOUD_ATTACHED_DATABASES WHERE STORAGE_ID = %s'


def recorded_absent_sql():
    """1 unless a name holds the recorded database, else 0 (parameters: name, STORAGE_ID, ATTACH_TYPE): the skip_if of a C3 DROP DATABASE.

    The DROP is skipped when its name holds nothing or another database, so a database that
    appears under the name after the identity check is never dropped; the checks after the
    DROP then refuse. Issues nothing (reads only).
    """
    return ('SELECT COUNT(*) = 0 AS n FROM information_schema.MV_CLOUD_ATTACHED_DATABASES '
            'WHERE DATABASE_NAME = %s AND STORAGE_ID = %s AND ATTACH_TYPE = %s')


def other_record_sql():
    """COUNT of attached, not dropped branch records under a name with another COMPUTE_ID (parameters: name, COMPUTE_ID).

    0 when the name holds the recorded record or nothing. A COMPUTE_ID belongs to one record,
    so another one means another database under the name. Issues nothing (reads only).
    """
    return ('SELECT COUNT(*) AS n FROM information_schema.MV_CLOUD_BRANCHES WHERE DATABASE_NAME = %s AND IS_DROPPED = 0 AND IS_DETACHED = 0 '
            'AND LATEST_COMPUTE_ID <> %s')


def record_elsewhere_sql():
    """COUNT of attached records with a STORAGE_ID and COMPUTE_ID under any other name (parameters: STORAGE_ID, COMPUTE_ID, name).

    0 when the recorded record is attached under its own name, detached or dropped. A positive
    count means the name in the block is not the recorded one. Issues nothing (reads only).
    """
    return ('SELECT COUNT(*) AS n FROM information_schema.MV_CLOUD_BRANCHES WHERE STORAGE_ID = %s AND LATEST_COMPUTE_ID = %s AND IS_DROPPED = 0 '
            'AND IS_DETACHED = 0 AND DATABASE_NAME <> %s')


def record_detached_sql():
    """COUNT of detached, not dropped records with a STORAGE_ID and COMPUTE_ID (parameters: STORAGE_ID, COMPUTE_ID).

    0 unless the recorded record was detached, which means someone moved it: the DROP would be
    skipped and the record left behind, so the block stops first. Issues nothing (reads only).
    """
    return ('SELECT COUNT(*) AS n FROM information_schema.MV_CLOUD_BRANCHES WHERE STORAGE_ID = %s AND LATEST_COMPUTE_ID = %s AND IS_DROPPED = 0 '
            'AND IS_DETACHED = 1')


def record_absent_sql():
    """1 unless a name holds the recorded record attached, else 0 (parameters: name, STORAGE_ID, COMPUTE_ID): the skip_if of a C3 DROP DATABASE.

    The DROP is skipped when its name holds nothing or another record, so a database that
    appears under the name after the identity check is never dropped; the checks after the
    DROP then refuse. Issues nothing (reads only).
    """
    return ('SELECT COUNT(*) = 0 AS n FROM information_schema.MV_CLOUD_BRANCHES WHERE DATABASE_NAME = %s AND STORAGE_ID = %s '
            'AND LATEST_COMPUTE_ID = %s AND IS_DROPPED = 0 AND IS_DETACHED = 0')


def record_state_sql():
    """The branch record of a name, STORAGE_ID and COMPUTE_ID with its IS_DETACHED, IS_DROPPED and ATTACH_SYNTAX (parameters: name, STORAGE_ID, COMPUTE_ID)."""
    return ('SELECT DATABASE_NAME, STORAGE_ID, ATTACH_TYPE, LATEST_COMPUTE_ID, IS_DETACHED, IS_DROPPED, ATTACH_SYNTAX '
            'FROM information_schema.MV_CLOUD_BRANCHES WHERE DATABASE_NAME = %s AND STORAGE_ID = %s AND LATEST_COMPUTE_ID = %s')


def record_identity_checks(block, names, role, label, again=False):
    """The identity checks of one recorded role before its DROP: the name holds the recorded record or nothing, no other record under the
    name, the recorded record attached under no other name and not detached (the last two once; again=True leaves them out)."""
    name, sid, cid, kind = names[role], names[role + '_sid'], names[role + '_cid'], CLEANUP_ATTACH_TYPES[role]
    still = 'still ' if again else ''
    block.check('%s name %sholds the recorded %s %s, or nothing' % (role, still, label, kind), attached_otherwise_sql(), (name, sid, kind),
                Expect('column_equals', 'n', 0))
    block.check('%s name %sholds no other record' % (role, still), other_record_sql(), (name, cid), Expect('column_equals', 'n', 0))
    if not again:
        block.check('recorded %s record attached under no other name' % role, record_elsewhere_sql(), (sid, cid, name), Expect('column_equals', 'n', 0))
        block.check('recorded %s record not detached' % role, record_detached_sql(), (sid, cid), Expect('column_equals', 'n', 0))


REPAIR_OUTAGE_NOTE = ('The repair detaches the live name for about 30 seconds on a 3 TB database (Helios fact 34): the application cannot '
                      'reach it meanwhile. Stop the application or accept the outage before you proceed.')


def endstate_block(names):
    """C3 End state: reads only. The live name resolves to the live record, the retained name to the retained record READONLY, and the
    entries with IS_DROPPED = 0 across both storages are exactly those two (switch.gate_block with the recorded ids).

    The write probe on the retained name runs through the runner (the kit route); the printed
    block carries it as a note. Issues no statement.
    """
    from . import switch
    live = {'STORAGE_ID': names['live_sid'], 'COMPUTE_ID': names['live_cid']}
    block = Block('C3', 'End state')
    block.note('After C2. Expected: exactly two entries on the workspace for this run, the live database %s (a pitr record of the replacement) and '
               'the retained copy %s (a resume record of the original storage, read only). The kit never deletes the retained copy; the customer '
               'does, after %s (s2rebuild cleanup --delete %s).' % (names['source'], names['retained'], names.get('retain_until_utc') or 'the retention date',
                                                                   names['retained']))
    block.check('live name resolves to the live record', inventory_attachment_sql(), (names['source'],), Expect('columns_equal', value=live))
    block.check('live record attached under the live name', switch.ATTACHED_RECORD_SQL, (names['source'], names['live_sid']),
                Expect('columns_equal', value=record_expect(names, 'live')))
    block.check('live name holds no other record', other_record_sql(), (names['source'], names['live_cid']), Expect('column_equals', 'n', 0))
    block.check('retained name holds the recorded retained record READONLY', inventory_attachment_sql(), (names['retained'],),
                Expect('columns_equal', value={'STORAGE_ID': names['retained_sid'], 'COMPUTE_ID': names['retained_cid'], 'ATTACH_TYPE': 'READONLY'}))
    block.check('retained name holds no other record', other_record_sql(), (names['retained'], names['retained_cid']), Expect('column_equals', 'n', 0))
    switch.gate_block(block, names, 'end state', recorded=True)
    block.note('The kit then runs the write probe on the retained name as the runner: a DELETE of a value that cannot exist must be refused as '
               'read only. By hand: run the O3 write probe statement against %s and expect the read-only refusal.' % names['retained'])
    block.note('When a check fails on a third entry (the detached original under the live name, or the replacement name) and the journal shows '
               'no S2b or S4, the switch was made by an older kit: s2rebuild step C3 prints and, with the owner profile, runs the repair block '
               '(owner-sql/C3-repair.sql).')
    return block


def repair_block(names):
    """C3 repair for a switch made without S2b and S4: the post-switch sequence proven on October 27, 2026, each statement behind a record check.

    The original record detached (checked before anything else, so an original already
    dropped stops the block before the live name is detached); detach the live name; the
    original record's own ATTACH_SYNTAX; record and row check; DROP
    the original record; the live record's own ATTACH_SYNTAX (a milestone placeholder, since
    the AT MILESTONE form exists only after the DETACH; the note says where to copy it from);
    DROP the replacement record by its record; the gate. The kit route runs the same sequence
    through switch.SwitchMachine.repair_end_state, reading each syntax live.
    """
    from . import switch
    block = Block('C3', 'End state repair (a switch made without S2b and S4)')
    block.refuse('run this block only when the end state gate failed and the journal shows no S2b or S4; remove this line by hand once decided')
    block.note(REPAIR_OUTAGE_NOTE)
    block.note('repair: the first check reads the original record before the live name is detached. When it finds no row, the original is '
               'attached or already dropped, so this block stops with nothing issued: s2rebuild step C3 with the owner profile resumes a '
               'repair that stopped part way from the live state, and refuses when nothing is left to repair.')
    block.check('repair: the original record is detached with its ATTACH_SYNTAX', switch.DETACHED_RECORD_SQL, (names['source'], names['source_sid']),
                Expect('columns_equal', value=record_expect(names, 'source')))
    block.check('repair: the live name holds the recorded live record', switch.ATTACHED_RECORD_SQL, (names['source'], names['live_sid']),
                Expect('columns_equal', value=record_expect(names, 'live')))
    block.statement('DETACH DATABASE %s' % ident(names['source']), 'repair: detach the live name',
                    detach={'wait': names['detach_wait_seconds'], 'kill': bool(names['detach_kill'])})
    block.check('repair: no attached database named source', inventory_attachment_sql(), (names['source'],), Expect('no_rows'))
    block.statement(switch.original_syntax(names), 'repair: attach the original record read write by its own ATTACH_SYNTAX')
    block.check('repair: the original record is attached READWRITE under the live name', inventory_attachment_sql(), (names['source'],),
                Expect('columns_equal', value={'STORAGE_ID': names['source_sid'], 'ATTACH_TYPE': 'READWRITE', 'COMPUTE_ID': names['source_cid']}))
    for table, count in (names.get('source_counts') or [])[:3]:
        block.check('repair proof: %s holds the source row count' % table, switch.count_sql(names['source'], table), (), Expect('column_equals', 'n', count))
    block.statement('DROP DATABASE %s' % ident(names['source']), 'repair: drop the original record (its data stays in the retained copy)')
    block.check('repair: the original record is dropped', switch.RECORD_STATE_SQL, (names['source'], names['source_sid'], names['source_cid']),
                Expect('column_equals', 'IS_DROPPED', 1))
    block.check('repair: the live record is detached with its ATTACH_SYNTAX', switch.DETACHED_RECORD_SQL, (names['source'], names['live_sid']),
                Expect('columns_equal', value=record_expect(names, 'live')))
    block.statement("ATTACH DATABASE %s AT MILESTONE '<live_milestone>' STORAGEID %s COMPUTEID %s" % (ident(names['source']), names['live_sid'], names['live_cid']),
                    'repair: re-attach the live record read write by its own ATTACH_SYNTAX')
    block.note('repair: the AT MILESTONE value is the ATTACH_SYNTAX of the detached live record; copy it from the row of the check above.')
    block.check('repair: the live name holds the live record READWRITE again', inventory_attachment_sql(), (names['source'],),
                Expect('columns_equal', value={'STORAGE_ID': names['live_sid'], 'ATTACH_TYPE': 'READWRITE', 'COMPUTE_ID': names['live_cid']}))
    record_identity_checks(block, names, 'replacement', 'replacement')
    block.statement('DROP DATABASE %s' % ident(names['replacement']), 'repair: drop the replacement record (the live branch is independent of it)',
                    skip_if=(record_absent_sql(), (names['replacement'], names['replacement_sid'], names['replacement_cid'])))
    block.check('repair: the replacement record is dropped', switch.RECORD_STATE_SQL, (names['replacement'], names['replacement_sid'], names['replacement_cid']),
                Expect('column_equals', 'IS_DROPPED', 1))
    switch.gate_block(block, names, 'end state', recorded=True)
    return block


def ask(prompt):
    """Read one line from the operator at a terminal; None without a terminal. Tests replace this function."""
    import sys
    if sys.stdin is None or not sys.stdin.isatty():
        return None
    return input(prompt).strip()


def inventory_attachment_sql():
    """MV_CLOUD_ATTACHED_DATABASES row for a name (parameter: the database name)."""
    return ('SELECT DATABASE_NAME, ATTACH_TYPE, STORAGE_ID, COMPUTE_ID, ATTACH_SYNTAX FROM information_schema.MV_CLOUD_ATTACHED_DATABASES '
            'WHERE DATABASE_NAME = %s')


def block_for(step_id, names, config=None, rollback=False, pipelines=None, grants=None, counts=None, capture_step=None, route='kit',
              released=False, app_writes_seen=None, s0_problems=None):
    """The Block for an admin step id (O2, O3, O9, O10, O11, C3, C4, P1); O10 with rollback=True gives R1 and R2a (the retained variant is a companion).

    The keyword inputs come from block_inputs(ctx); O3 gives the Path B lockout plan when
    freeze.mode is lockout; the O10 forward block starts with a STOP while s0_problems is not empty,
    and on Path B checks before S1 that no account holds again the write privileges the lockout took (the snapshot not given back).
    """
    from . import freeze, lockout, pipelines as pipeline_module, switch
    key = str(step_id).upper()
    if key == 'P1':
        return account_block(names)
    if key == 'C4':
        return account_block(names, drop=True)
    if key == 'C3':
        return endstate_block(names)
    if key == 'O2':
        return pipeline_module.stop_block(names, pipelines or [])
    if key == 'O3':
        if config is not None and config.freeze.mode == 'lockout':
            return lockout.plan_block(names, grants or {})
        return freeze.block(names)
    if key == 'O9':
        if route == 'customer':
            return pipeline_module.verify_block(names, pipelines or [], capture_step)
        return pipeline_module.replay_block(names, pipelines or [], capture_step)
    if key == 'O10':
        if rollback:
            return switch.rollback_block(names, released=released, app_writes_seen=app_writes_seen, withheld=grants)
        return switch.forward_block(names, counts, s0_problems, withheld=grants)
    if key == 'O11':
        return pipeline_module.release_block(names, pipelines or [], grants or {}, config)
    raise KitError('%s is not an admin step with an owner SQL block (owner-sql covers %s)' % (step_id, ', '.join(steps.OWNER_SQL_STEPS)))


def companion_blocks(step_id, names, config=None, rollback=False, **inputs):
    """The blocks printed beside a step's block, as (variant, Block): O3 Path A has the re-entry block (O3-reentry), the O10 rollback the
    variant through the retained copy (O10-rollback-retained, for after S2b ran), and C3 the repair block (C3-repair).

    inputs are the keyword inputs of block_for (block_inputs): the rollback companion takes
    released, app_writes_seen and grants from them. owner-sql opens no session, so it cannot
    see whether an earlier run stopped between DETACH and ATTACH, or whether S2b ran; it writes
    both blocks, and the checks of each refuse the wrong state.
    """
    from . import freeze, switch
    key = str(step_id).upper()
    if key == 'O3' and not rollback and (config is None or config.freeze.mode != 'lockout'):
        return [('reentry', freeze.reentry_block(names))]
    if key == 'O10' and rollback:
        return [('retained', switch.rollback_block(names, released=inputs.get('released', False), app_writes_seen=inputs.get('app_writes_seen'),
                                                   withheld=inputs.get('grants'), after_s2b=True))]
    if key == 'C3':
        return [('repair', repair_block(names))]
    return []


def owner_sql_path(ctx, step_id, rollback=False, variant=None):
    """customer/runs/<run>/<db>/owner-sql/<step>.sql (<step>-rollback.sql, or <step>-<variant>.sql for a companion block)."""
    suffix = '-rollback' if rollback else ('-%s' % variant if variant else '')
    return ctx.config.runs_dir() / (ctx.run_id or 'dry-run') / ctx.db.source / 'owner-sql' / ('%s%s.sql' % (step_id, suffix))


def notebook_paths(ctx):
    """The owner notebooks owner-notebook wrote for the run that can hold this database: owner-steps.ipynb and owner-steps-<db>.ipynb."""
    run_dir = ctx.config.runs_dir() / (ctx.run_id or 'dry-run')
    return [p for p in (run_dir / 'owner-steps.ipynb', run_dir / ('owner-steps-%s.ipynb' % ctx.db.source)) if p.is_file()]


def retire_o10_blocks(ctx, tables, reason):
    """Rewrite the O10 block files already written for this database so that neither can run; returns the lines to log.

    Called just before the journal reopens the copy evidence of tables (mark-recopy, an O7 or O8
    re-copy mark, a fresh-table load). The forward block gets a STOP naming reason, since the
    reopen is not in the journal yet; the rollback block is built with the target counts of
    tables cleared, as the reopen clears them, so it stops too. Written before the journal write,
    a crash after it never leaves a block without a STOP; a crash before the journal write leaves
    a STOP on an unchanged copy, and owner-sql O10 writes a valid block again. A block that cannot
    be built is replaced by a STOP alone. A notebook holds its blocks from when it was generated
    and an uploaded copy is out of reach, so the last line says not to run its O10 cells, when a
    notebook of the run exists or S0's journal part passed until now (a notebook generated then has
    no STOP). A dry run writes nothing. Issues nothing (files only).
    """
    from . import switch
    if ctx.dry_run or not ctx.journal_path:
        return []
    open_until_now = not switch.s0_journal_problems(ctx.journal)
    lines = []
    for rollback in (False, True):
        path = owner_sql_path(ctx, 'O10', rollback=rollback)
        if not path.is_file():
            continue
        try:
            names, inputs = names_for(ctx), block_inputs(ctx)
            if rollback:
                names['table_counts'] = [(t, None if t in tables else n) for t, n in names['table_counts']]
            else:
                inputs['s0_problems'] = list(inputs['s0_problems']) + [reason]
            block = block_for('O10', names, ctx.config, rollback=rollback, **inputs)
            guard_check_block(block, ctx)
        except KitError:
            block = Block('O10', 'Rollback R1 to R3 (before O11 only)' if rollback else 'Switch forward')
            block.refuse('%s; this block was written before that and cannot be built again now; generate it again with s2rebuild '
                         'owner-sql O10%s' % (reason, ' --rollback' if rollback else ''))
        write_block(block, path)
        lines.append('O10 %s block %s rewritten with a STOP: %s' % ('rollback' if rollback else 'forward', path,
                                                                  block.refusals[0] if block.refusals else reason))
    notebooks = notebook_paths(ctx)
    if notebooks or open_until_now:
        lines.append('owner notebook: a notebook generated before now must not run its O10 cells, forward or rollback%s; after O8 passes '
                     'again, generate it again with s2rebuild owner-notebook and use only the new one' % (
                         ' (%s)' % ', '.join(str(p) for p in notebooks) if notebooks else ''))
    return lines


def print_block(block, ctx, path=None):
    """Write the block file (unless dry run or the journal is in memory) and print it line by line.

    The printed lines keep the SQL user names (a block that names a user must run as printed);
    passwords, the host and the CA path stay masked.
    """
    text = block.render()
    if path is not None and ctx.journal_path and not ctx.dry_run:
        write_block(block, path)
        ctx.log('owner SQL block written to %s' % path)
    for line in text.rstrip('\n').splitlines():
        ctx.log.block('    ' + line)


def guard_check_block(block, ctx, identity=steps.OWNER):
    """Pass every statement of the block through the step's owner guard without executing (dry run and print mode)."""
    key = block.guard_step if block.guard_step in steps.OWNER_ALLOWANCES else 'P1'
    guard = steps.guard_for(key, ctx.db, identity)
    for item in block.statements:
        # a milestone placeholder stands where the AT MILESTONE value of the detached original goes (the rollback block before S1 ran)
        sql = re.sub(r"AT MILESTONE '<[a-z_]+_milestone>'", "AT MILESTONE '2000-01-01 00_00_00.000000'", item.sql)
        guard.check(re.sub(r'<[a-z_]+>', 'placeholder', sql))


def refuse_placeholders(block):
    """Refuse to execute a block that still holds a placeholder: the record of that role was never recorded."""
    missing = placeholders(block.render())
    if missing:
        roles = sorted({m.strip('<>').rsplit('_', 1)[0] for m in missing})
        raise KitError('%s cannot run: %s not recorded yet (the source record is recorded at P2, P3 or O3, the replacement at O5, '
                       'retained and live at O10, the detached original at O10 S1)' % (block.step_id, ', '.join(roles)))


def handle_admin(step, ctx, block, after=None, substitute=None, before_first_statement=None, path=None):
    """Shared admin step flow: print the block; execute it with the owner profile when configured; else wait for the gate.

    before_first_statement() goes to execute_block: it runs inside the open owner session once
    every check ahead of the first statement passed, just before that statement (O11 records
    release_started_utc there). after(results) runs after a successful execution (python
    postconditions such as the write probe or recording freeze_time). path overrides the block
    file (the O3 re-entry block goes to owner-sql/O3-reentry.sql). Returns the step Outcome.
    """
    guard_check_block(block, ctx)
    if path is None:
        path = owner_sql_path(ctx, step.id, rollback=(block.title.lower().startswith('rollback')))
    print_block(block, ctx, path)
    if ctx.dry_run or not ctx.owner_configured:
        lines = ['identity: %s' % step.identity_label,
                 ('dry run: the block above is what the owner profile would execute' if ctx.dry_run
                  else 'no owner profile: run the block above as the admin, then record the gate')]
        if block.refusals:
            lines.append('the block holds a STOP: ' + '; '.join(block.refusals))
        return steps.gate_pending(step, ctx, lines)
    refuse_placeholders(block)
    guard = steps.guard_for(step.id, ctx.db, steps.OWNER)
    session = ctx.owner_session(guard)
    try:
        results = execute_block(block, session, ctx.log, ctx.journal, substitute, open_session=ctx.owner_session, guard=guard,
                                before_first_statement=before_first_statement)
    finally:
        session.close()
    if after is not None:
        after(results)
    return steps.Outcome(steps.PASSED, '%s executed with the owner profile; %d checks passed; block at %s' % (step.id, len(results), path))


def retained_write_probe(ctx):
    """The write probe on the retained name through the runner: a DELETE of a value that cannot exist must be refused as read only."""
    from . import copy as copy_module, freeze
    from .sqlutil import probe_guard
    data, plans, _ = copy_module.load_plans(ctx)
    table, column, kind = freeze.probe_target(plans, data['tables'])
    runner = ctx.runner_session(probe_guard(ctx.db.retained, 'runner C3'))
    try:
        freeze.write_probe(runner, ctx.db.retained, table, column, ctx.log, ctx.config.copy.lock_probe_timeout_seconds, kind)
    finally:
        runner.close()
    ctx.journal.event('endstate_probe', 'write on the retained name %s refused as read only' % ctx.db.retained)


def record_endstate_listing(ctx, when):
    """Record the Portal entries of both storages in the journal (endstate_listing) through a read-only owner session; returns the entries."""
    from . import switch
    from .sqlutil import read_guard
    reader = ctx.owner_session(read_guard('owner C3'))
    try:
        entries = switch.listing(reader, names_for(ctx))
    finally:
        reader.close()
    ctx.journal.event('endstate_listing', '%s: %s' % (when, switch.listing_text(entries)))
    ctx.log('    entries %s: %s' % (when, switch.listing_text(entries)))
    return entries


def handle_c3(step, ctx):
    """C3 handler: the End state block (two entries, reads only), the write probe on the retained name, the listing recorded.

    With the owner profile, when the gate fails and the journal holds no S2b or S4 (a switch by
    an older kit), the repair block is printed with its outage note and runs after the operator
    types proceed (ownersql.ask), or at once with --yes; the gate then runs again. A failed gate
    with S2b and S4 in the journal refuses with the listing and no repair. The repair's own
    statements do not count as S2b or S4, so a repair that stopped part way is offered again and
    resumes from the live state (switch.SwitchMachine.repair_end_state), which refuses before any
    statement when nothing is left to repair. Without the owner profile the block is printed and
    the gate waits for a person.
    """
    from . import switch
    names = names_for(ctx)
    block = endstate_block(names)
    if ctx.dry_run or not ctx.owner_configured:
        out = handle_admin(step, ctx, block)
        for variant, companion in companion_blocks('C3', names):
            print_block(companion, ctx, owner_sql_path(ctx, 'C3', variant=variant))
        return out
    record_endstate_listing(ctx, 'before')
    try:
        out = handle_admin(step, ctx, block, after=lambda results: retained_write_probe(ctx))
    except KitError as exc:
        out = steps.Outcome(steps.REFUSED, str(exc), EXIT_REFUSED)
    if out.status != steps.REFUSED or 'stopped at check' not in out.note:
        if out.status == steps.PASSED:
            record_endstate_listing(ctx, 'after')
        return out
    entries = record_endstate_listing(ctx, 'before repair')
    # S2b and S4 of the kit's switch only: the repair's own DROP of the original marks a repair to resume from the live state, not a switch made with S2b
    switched_here = any(d.startswith('S2b DROP') or d.startswith('S4 DROP') for d in ctx.journal.events('switch_statement'))
    if switched_here:
        return steps.Outcome(steps.REFUSED, '%s. The journal shows S2b and S4, so no repair applies; entries: %s' % (out.note, switch.listing_text(entries)),
                             EXIT_REFUSED)
    repair = repair_block(names)
    repair.items = [i for i in repair.items if i.kind != 'refuse']
    guard_check_block(repair, ctx)
    print_block(repair, ctx, owner_sql_path(ctx, 'C3', variant='repair'))
    ctx.log('    ' + REPAIR_OUTAGE_NOTE)
    if not ctx.yes:
        answer = ask('The journal shows no S2b or S4 (a switch by an older kit). Run the repair now? Type proceed: ')
        if answer is None:
            return steps.Outcome(steps.PENDING, 'C3: the end state gate failed (%s); rerun step C3 --yes%s, or at a terminal type proceed, to run the '
                                 'repair block above' % (out.note, steps.run_flag(ctx.run_id)), EXIT_GATE)
        if answer.strip().lower() != 'proceed':
            ctx.journal.event('endstate_repair_declined', answer.strip()[:80] or 'no answer')
            return steps.Outcome(steps.REFUSED, 'C3: the repair was not confirmed; nothing was issued. Entries: %s' % switch.listing_text(entries),
                                 EXIT_REFUSED)
    guard = steps.guard_for('C3', ctx.db, steps.OWNER)
    owner = ctx.owner_session(guard)
    runner = ctx.runner_session(steps.guard_for('O8', ctx.db))
    try:
        machine = switch.SwitchMachine(owner, names, ctx.journal, ctx.log, runner, open_session=ctx.owner_session, guard=guard)
        ctx.journal.event('endstate_repair', 'started: the live name is detached for the repair')
        machine.repair_end_state()
    finally:
        owner.close()
        runner.close()
    retained_write_probe(ctx)
    record_endstate_listing(ctx, 'after repair')
    return steps.Outcome(steps.PASSED, 'C3 repaired the end state with the owner profile: two entries, the live record and the retained record')


def definer_routines_sql():
    """COUNT of the routines of a database whose DEFINER user is the given account (parameters: schema, user)."""
    return "SELECT COUNT(*) AS n FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = %s AND SUBSTRING_INDEX(DEFINER, '@', 1) = %s"


def runner_definer_checks(block, names):
    """C4 preconditions: no view and no routine in the live database has the runner as its definer (the admin sees every routine)."""
    block.check('no view in the live database has the runner as its definer', definer_views_sql(), (names['source'], names['runner_user']),
                Expect('column_equals', 'n', 0))
    block.check('no routine in the live database has the runner as its definer', definer_routines_sql(), (names['source'], names['runner_user']),
                Expect('column_equals', 'n', 0))


def handle_c4(step, ctx):
    """C4 handler: the check that no view in the live database has the runner as its definer, DROP USER and the zero-privilege check."""
    return handle_admin(step, ctx, account_block(names_for(ctx), drop=True))


steps.register('C3', handle_c3)
steps.register('C4', handle_c4)


# ---------------------------------------------------------------------------
# ownerlib: the single-file gate logic for the notebook
# ---------------------------------------------------------------------------
OWNERLIB_SOURCE = r'''"""ownerlib: run an s2rebuild owner block with any DB-API cursor; stop at the first failed check.

Author: John Bergamini
Dependency free. Usage inside the notebook:
    result = run_block(cursor, BLOCKS["O3"], RUN, credentials=None, connect=None)
connect, when given, opens a second connection for the DETACH escalation. The result is a dict
the operator saves to a file and records with: s2rebuild gate <ID> --pass --evidence <file>.
"""
import json
import threading
import time


def _rows(cursor):
    rows = cursor.fetchall() or []
    if rows and not isinstance(rows[0], dict):
        names = [d[0] for d in (cursor.description or [])]
        rows = [dict(zip(names, r)) for r in rows]
    return list(rows)


def evaluate(expect, rows):
    """(ok, observed) for a check's expectation dict."""
    kind = expect.get("kind")
    if kind == "row_count":
        return len(rows) == expect.get("count"), "%d row(s)" % len(rows)
    if kind == "no_rows":
        return not rows, "%d row(s)" % len(rows)
    if kind == "some_rows":
        return bool(rows), "%d row(s)" % len(rows)
    if not rows:
        return False, "no rows"
    column = expect.get("column")
    if kind == "column_equals":
        observed = rows[0].get(column)
        return str(observed) == str(expect["value"]), "%s = %s" % (column, observed)
    if kind == "column_at_least":
        observed = rows[0].get(column)
        try:
            ok = int(observed) >= int(expect["value"])
        except (TypeError, ValueError):
            ok = False
        return ok, "%s = %s" % (column, observed)
    if kind == "column_not_in":
        observed = rows[0].get(column)
        return str(observed) not in [str(v) for v in expect["value"]], "%s = %s" % (column, observed)
    if kind == "columns_equal":
        observed = {c: rows[0].get(c) for c in expect["value"]}
        ok = all(str(observed[c]) == str(v) for c, v in expect["value"].items())
        return ok, ", ".join("%s = %s" % (c, observed[c]) for c in expect["value"])
    if kind == "all_column_equals":
        bad = [str(r.get(column)) for r in rows if str(r.get(column)) != str(expect["value"])]
        return not bad, "ok" if not bad else "%s in (%s)" % (column, ", ".join(bad))
    raise ValueError("unknown expectation " + str(kind))


def sql_literal(value):
    """The SQL literal of a secret: backslash escaped first, then single quotes doubled."""
    text = str(value)
    return "'" + text.replace("\\", "\\\\").replace("'", "''") + "'"


def _mask(text, secrets):
    text = str(text)
    for value in secrets:
        for form in (value, sql_literal(value)[1:-1], repr(value)[1:-1]):
            if form:
                text = text.replace(form, "<redacted>")
    return text


def _errno(exc):
    args = getattr(exc, "args", ())
    return args[0] if args and isinstance(args[0], int) else "unknown"


def _capture(cursor, database):
    cursor.execute("SELECT PIPELINE_NAME, STATE FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s ORDER BY PIPELINE_NAME", (database,))
    states = _rows(cursor)
    definitions = {}
    for r in states:
        name = r["PIPELINE_NAME"]
        cursor.execute("SHOW CREATE PIPELINE `%s`.`%s` EXTENDED" % (database, name))
        definitions[name] = [{k: str(v) for k, v in row.items()} for row in _rows(cursor)]
    cursor.execute("SELECT PIPELINE_NAME, SOURCE_TYPE, SOURCE_PARTITION_ID, EARLIEST_OFFSET, LATEST_OFFSET, LATEST_LOADED_OFFSET, "
                   "LATEST_SUCCESSFUL_OFFSET FROM information_schema.PIPELINES_OFFSETS WHERE DATABASE_NAME = %s", (database,))
    offsets = [{k: (None if v is None else str(v)) for k, v in r.items()} for r in _rows(cursor)]
    return {"database": database, "states": [{k: str(v) for k, v in r.items()} for r in states], "definitions": definitions, "offsets": offsets}


def _detach(cursor, connect, sql, wait, kill):
    worker = connect()
    wcur = worker.cursor()
    wcur.execute("SELECT CONNECTION_ID() AS id")
    cid = int(list(_rows(wcur)[0].values())[0])
    outcome = {}

    def _run():
        try:
            wcur.execute(sql)
            outcome["done"] = True
        except Exception as exc:
            outcome["error"] = exc
    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(wait)
    if not thread.is_alive():
        if "error" in outcome:
            raise outcome["error"]
        return "plain"
    cursor.execute("KILL QUERY %d" % cid)
    thread.join(30)
    if thread.is_alive():
        raise RuntimeError("the waiting DETACH did not stop after KILL QUERY %d" % cid)
    if outcome.get("done"):
        return "plain"
    if not kill:
        raise RuntimeError("DETACH waited more than %d s and detach_kill is off; stop the writers (lockout cell) and rerun" % wait)
    cursor.execute(sql + " KILL")
    return "kill"


def run_block(cursor, block, run, credentials=None, connect=None):
    """Execute checks and statements in order and print the JSON result.

    credentials maps a pipeline name or a source type (name first) to a CREDENTIALS JSON text.
    """
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    result = {"step": block["step_id"], "run_id": run.get("run_id"), "started_utc": started, "checks": [], "statements": 0,
              "skipped": [], "attachments": [], "status": "running"}
    secrets = [v for v in (credentials or {}).values() if v]
    current = None
    try:
        for item in block["items"]:
            current = item
            kind = item["kind"]
            if kind == "note":
                continue
            if kind == "refuse":
                result["status"] = "REFUSED: " + item["label"]
                break
            if kind == "capture":
                result["capture"] = _capture(cursor, item["database"])
                continue
            if kind == "check":
                cursor.execute(item["sql"], tuple(item["params"]) or None)
                rows = _rows(cursor)
                ok, observed = evaluate(item["expect"], rows)
                result["checks"].append({"label": item["label"], "ok": ok, "observed": observed})
                if not ok:
                    result["status"] = "FAILED at check " + item["label"]
                    break
                if item.get("record") and rows:
                    result["attachments"].append({"role": item["record"], "row": {k: (None if v is None else str(v)) for k, v in rows[0].items()}})
                continue
            if item.get("skip_if"):
                cursor.execute(item["skip_if"][0], tuple(item["skip_if"][1]) or None)
                rows = _rows(cursor)
                if rows and int(list(rows[0].values())[0]) > 0:
                    result["skipped"].append(item["label"])
                    continue
            sql = item["sql"]
            if "<<CREDENTIALS>>" in sql:
                key = item.get("pipeline") if item.get("pipeline") in (credentials or {}) else item.get("source_type")
                value = (credentials or {}).get(key)
                if not value:
                    result["status"] = "FAILED: no credentials supplied for pipeline %s (source type %s)" % (item.get("pipeline"), item.get("source_type"))
                    break
                sql = sql.replace("'<<CREDENTIALS>>'", sql_literal(value))
                try:
                    cursor.execute(sql)
                except Exception as exc:
                    result["status"] = "ERROR " + type(exc).__name__
                    result["error"] = "server error %s at %s; the message is withheld because the statement carried credentials" % (_errno(exc), item["label"])
                    break
                result["statements"] += 1
                continue
            if item.get("detach") and connect is not None:
                result["detach"] = _detach(cursor, connect, sql, int(item["detach"].get("wait", 120)), bool(item["detach"].get("kill")))
                result["statements"] += 1
                continue
            cursor.execute(sql)
            result["statements"] += 1
        else:
            result["status"] = "passed"
    except Exception as exc:
        result["status"] = "ERROR " + type(exc).__name__
        result["error"] = _mask("%s at %s" % (exc, (current or {}).get("label")), secrets)[:300]
    result["ended_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(json.dumps(result, indent=1, default=str))
    return result
'''


def ownerlib_compiles():
    """True when OWNERLIB_SOURCE compiles (the notebook acceptance check)."""
    compile(OWNERLIB_SOURCE, 'ownerlib.py', 'exec')
    return True
