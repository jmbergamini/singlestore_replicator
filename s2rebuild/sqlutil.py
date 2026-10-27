"""s2rebuild sqlutil: identifier quoting, statement classification and the write guard.

Author: John Bergamini

This module issues no statements. It decides which statements the other modules may issue.
Every statement the kit sends to the server passes through a Guard first (section 10.4 of
the architecture). A refusal names the statement kind and the database, never the text,
so a refused CREATE PIPELINE cannot leak its credentials into a log line.
"""
import re

from .common import KitError

IDENT = re.compile(r'^[A-Za-z0-9_]+$')
_BT = r'`([A-Za-z0-9_]+)`'
_QUAL = _BT + r'\.' + _BT
# Helios STORAGE_IDs are two numbers joined by a colon, for example 1028600672070281646:7367158985892520834,
# and ATTACH takes them unquoted (MV_CLOUD_BRANCHES.ATTACH_SYNTAX, engine 10.8.0); a quoted id is error 1064
_ID_VALUE = r'[A-Za-z0-9_:-]+'
_DEFINER_CLAUSE = r"(?:DEFINER ?= ?(?:`[^`]*`|'[^']*')@(?:`[^`]*`|'[^']*') )?"

READ_KINDS = frozenset({'SELECT', 'SHOW', 'EXPLAIN', 'DESCRIBE', 'SET SESSION'})
RUNNER_KINDS = frozenset({'CREATE DATABASE', 'CREATE TABLE', 'INSERT', 'DELETE', 'CREATE VIEW',
                          'ALTER TABLE', 'AGGREGATOR SYNC AUTO_INCREMENT', 'DROP TABLE'})
OWNER_KINDS = frozenset({'STOP PIPELINE', 'START PIPELINE', 'CREATE PIPELINE', 'ALTER PIPELINE',
                         'DROP PIPELINE', 'DETACH DATABASE', 'ATTACH DATABASE', 'GRANT', 'REVOKE',
                         'KILL CONNECTION', 'KILL QUERY', 'CREATE USER', 'DROP USER', 'DROP DATABASE', 'SET GLOBAL',
                         'USE', 'CREATE FUNCTION', 'CREATE AGGREGATE', 'CREATE PROCEDURE'})
ALL_KINDS = READ_KINDS | RUNNER_KINDS | OWNER_KINDS
# a routine text: CREATE [OR REPLACE] [DEFINER=...] FUNCTION | AGGREGATE | PROCEDURE `db`.`name`( ...; the body of a function or
# procedure holds semicolons inside BEGIN ... END, the one place the kit accepts them (classify)
ROUTINE = re.compile(r'CREATE (?:OR REPLACE )?' + _DEFINER_CLAUSE + r'(FUNCTION|AGGREGATE|PROCEDURE) ' + _QUAL + r'(?:[ (]|$)', re.I)

# The one runner write that may name the source: the O3 write probe (architecture 10.7).
PROBE = re.compile(r"DELETE FROM " + _QUAL + r" WHERE " + _BT + r" = (?:'[^']*'|-?[0-9]+)$")


class GuardError(KitError):
    """A statement outside the allowance of the active step."""


def ident(name):
    """Quote one identifier with backticks. Letters, digits and underscores only."""
    if not isinstance(name, str) or not IDENT.fullmatch(name):
        raise KitError('Unsupported identifier; only letters, digits and underscores are allowed')
    return '`' + name + '`'


def qualified(db, table):
    """Quote `db`.`table`."""
    return ident(db) + '.' + ident(table)


def sql_literal(value):
    """A SQL string literal for a value: backslash escaped first, then single quotes doubled.

    Used for every secret the kit injects at execution time (pipeline CREDENTIALS, the runner
    password), so a value holding a backslash, a quote or a newline reaches the server unchanged.
    """
    text = str(value)
    return "'" + text.replace('\\', '\\\\').replace("'", "''") + "'"


def secret_forms(value):
    """Every text form a secret can take in a statement, a driver error or a repr; for the redactor."""
    raw = str(value)
    body = sql_literal(raw)[1:-1]
    forms = {raw, body}
    for f in list(forms):
        r = repr(f)[1:-1]
        forms.update({r, f.replace('\\', '\\\\'), f.replace("'", "\\'"), r.replace("'", "\\'")})
    return sorted((f for f in forms if f), key=len, reverse=True)


def normalize(sql):
    """Collapse whitespace and drop one trailing semicolon."""
    text = ' '.join(str(sql).split()).strip()
    if text.endswith(';'):
        text = text[:-1].rstrip()
    return text


class Statement:
    """The classification of one statement: kind, databases it names as a target, detail."""

    __slots__ = ('kind', 'databases', 'detail', 'sql')

    def __init__(self, kind, databases, detail, sql):
        self.kind = kind
        self.databases = tuple(databases)
        self.detail = detail
        self.sql = sql

    @property
    def is_read(self):
        """True for SELECT, SHOW, EXPLAIN, DESCRIBE and session variable settings."""
        return self.kind in READ_KINDS

    def __repr__(self):
        return 'Statement(%s, databases=%r, detail=%r)' % (self.kind, self.databases, self.detail)


def _match(pattern, text):
    return re.match(pattern, text, re.I)


def strip_literals(text):
    """Return text with the inside of every string literal removed; backtick identifiers stay. No statements are issued."""
    return re.sub(r"'(?:[^'\\]|\\.|'')*'|\"(?:[^\"\\]|\\.|\"\")*\"", "''", text)


def strip_quoted(text):
    """Return text with the inside of every quoted run removed, for checks that must ignore literals.

    Handles single quotes, double quotes and backticks, doubled quotes inside a run and a
    backslash escape. No statements are issued.
    """
    out = []
    quote = None
    i = 0
    while i < len(text):
        ch = text[i]
        if quote is None:
            if ch in ("'", '"', '`'):
                quote = ch
                out.append(ch)
            else:
                out.append(ch)
        else:
            if ch == '\\' and quote != '`' and i + 1 < len(text):
                i += 1
            elif ch == quote:
                if i + 1 < len(text) and text[i + 1] == quote:
                    i += 1
                else:
                    quote = None
                    out.append(ch)
        i += 1
    return ''.join(out)


def classify(sql):
    """Classify a statement into one of the kinds the kit knows.

    Input: the statement text. Output: a Statement whose databases tuple holds the names the
    statement writes to or switches to; reads and account statements carry none. Raises
    GuardError for compound text (a second statement after a semicolon), for an unsupported
    kind (UPDATE, TRUNCATE, RENAME, LOAD DATA, SELECT INTO, SET PASSWORD, unqualified table
    writes and so on). No statements are issued.
    """
    text = normalize(sql)
    if not text:
        raise GuardError('Refusing an empty statement')
    bare = strip_quoted(text)
    routine = ROUTINE.match(text)
    if routine is not None:
        return _classify_routine(routine, bare, text)
    if ';' in bare:
        raise GuardError('Refusing compound text; the kit issues one statement per call')
    first = text.split(' ', 1)[0].upper()
    if first in ('SELECT', 'SHOW', 'EXPLAIN', 'DESCRIBE', 'DESC'):
        kind = 'DESCRIBE' if first == 'DESC' else first
        if kind == 'SELECT' and re.search(r'\bINTO\b', bare, re.I):
            raise GuardError('Refusing SELECT INTO; the kit never writes query results to a file or a table this way')
        return Statement(kind, (), '', text)
    if first == 'SET':
        if ',' in bare:
            raise GuardError('Refusing SET with several assignments; the kit sets one variable per statement')
        if re.search(r'\bGLOBAL\b', bare, re.I) or re.search(r'@@GLOBAL\.', bare, re.I):
            m = _match(r'SET (?:GLOBAL |@@GLOBAL\.)([A-Za-z_][A-Za-z0-9_]*)\s*=', text)
            if not m:
                raise GuardError('Refusing statement of an unsupported kind: SET')
            return Statement('SET GLOBAL', (), m.group(1).lower(), text)
        m = _match(r'SET (?:SESSION |@@SESSION\.|@@)?([A-Za-z_][A-Za-z0-9_]*)\s*=', text)
        if not m or m.group(1).upper() in ('PASSWORD', 'ROLE', 'DEFAULT_ROLE', 'PERSIST', 'PERSIST_ONLY'):
            raise GuardError('Refusing statement of an unsupported kind: SET')
        return Statement('SET SESSION', (), m.group(1).lower(), text)
    m = _match(r'USE ' + _BT + r'$', text)
    if m:
        return Statement('USE', (m.group(1),), '', text)
    m = _match(r'CREATE DATABASE ' + _BT + r'(?: |$)', text)
    if m:
        return Statement('CREATE DATABASE', (m.group(1),), '', text)
    m = _match(r'DROP DATABASE ' + _BT + r'$', text)
    if m:
        return Statement('DROP DATABASE', (m.group(1),), '', text)
    m = _match(r'DETACH DATABASE ' + _BT + r'(?: (KILL|FORCE))?$', text)
    if m:
        return Statement('DETACH DATABASE', (m.group(1),), (m.group(2) or '').upper(), text)
    # the three ATTACH_SYNTAX forms of MV_CLOUD_BRANCHES (engine 10.8.0) are all ATTACH DATABASE: the attached record's form
    # (STORAGEID and COMPUTEID), the detached record's form (AT MILESTONE 'YYYY-MM-DD HH_MM_SS.ffffff' before the ids) and the
    # dropped record's form (ATTACH DROPPED DATABASE); READ ONLY comes right after the names
    m = _match(r'ATTACH (?:DROPPED )?DATABASE ' + _BT + r'(?: AS ' + _BT + r')?( READ ONLY)?(?: AT MILESTONE \'[0-9 _.:-]+\')?'
               r'(?: STORAGEID ' + _ID_VALUE + r')?(?: COMPUTEID ' + _ID_VALUE + r')?$', text)
    if m:
        names = (m.group(1),) if m.group(2) is None else (m.group(1), m.group(2))
        return Statement('ATTACH DATABASE', names, 'READ ONLY' if m.group(3) else '', text)
    m = _match(r'CREATE TABLE ' + _QUAL + r'(?: |$)', text)
    if m:
        return Statement('CREATE TABLE', (m.group(1),), m.group(2), text)
    m = _match(r'DROP TABLE ' + _QUAL + r'$', text)
    if m:
        return Statement('DROP TABLE', (m.group(1),), m.group(2), text)
    m = _match(r'ALTER TABLE ' + _QUAL + r' (.+)$', text)
    if m:
        rest = m.group(3)
        if _match(r'AUTO_INCREMENT\s*=\s*\d+$', rest):
            detail = 'AUTO_INCREMENT'
        elif _match(r'RENAME TO ' + _BT + r'$', rest):
            detail = 'RENAME'
        else:
            detail = 'OTHER'
        return Statement('ALTER TABLE', (m.group(1),), detail, text)
    m = _match(r'INSERT INTO ' + _QUAL + r'(?: |$)', text)
    if m:
        return Statement('INSERT', (m.group(1),), m.group(2), text)
    m = _match(r'DELETE FROM ' + _QUAL + r'(?: |$)', text)
    if m:
        return Statement('DELETE', (m.group(1),), m.group(2), text)
    # a DEFINER clause names an account, never a database: the captured `user`@`host` form or 'user'@'host'
    m = _match(r'CREATE (?:OR REPLACE )?(?:ALGORITHM=[A-Za-z]+ )?' + _DEFINER_CLAUSE + r'(?:SCHEMA_BINDING=(?:ON|OFF) )?(?:SQL SECURITY [A-Za-z]+ )?VIEW '
               + _QUAL + r'(?: |$)', text)
    if m:
        return Statement('CREATE VIEW', (m.group(1),), m.group(2), text)
    # ALL runs the sync on every aggregator; without it only the aggregator of the session syncs
    m = _match(r'AGGREGATOR SYNC AUTO_INCREMENT ON ' + _QUAL + r'( ALL)?$', text)
    if m:
        return Statement('AGGREGATOR SYNC AUTO_INCREMENT', (m.group(1),), 'ALL' if m.group(3) else '', text)
    m = _match(r'(CREATE(?: OR REPLACE)?(?: AGGREGATOR)?|ALTER|STOP|START|DROP) PIPELINE (?:' + _QUAL + '|' + _BT + r')(?: |$)', text)
    if m:
        verb = m.group(1).upper().split(' ')[0]
        targets = ()
        if verb == 'CREATE':
            # a load target qualified with a database is a write to that database
            targets = tuple(t.group(1) for t in re.finditer(r'\bINTO\s+(?:TABLE|PROCEDURE)\s+' + _BT + r'\.', strip_literals(text), re.I))
        if m.group(2) is not None:
            return Statement(verb + ' PIPELINE', tuple(dict.fromkeys((m.group(2),) + targets)), m.group(3), text)
        if verb in ('STOP', 'START', 'DROP'):
            raise GuardError('Refusing %s PIPELINE without a qualified `database`.`pipeline` name' % verb)
        return Statement(verb + ' PIPELINE', targets, m.group(4), text)
    if first in ('GRANT', 'REVOKE'):
        m = _match(r'(?:GRANT|REVOKE) .+? ON (?:\*\.\*|' + _BT + r'\.(?:\*|' + _BT + r')) (?:TO|FROM) .+$', text)
        if not m:
            raise GuardError('Refusing %s without an ON *.*, ON `database`.* or ON `database`.`table` clause' % first)
        return Statement(first, (m.group(1),) if m.group(1) else (), m.group(2) or '', text)
    m = _match(r'KILL QUERY (\d+)$', text)
    if m:
        return Statement('KILL QUERY', (), m.group(1), text)
    m = _match(r'KILL (?:CONNECTION )?(\d+)$', text)
    if m:
        return Statement('KILL CONNECTION', (), m.group(1), text)
    if _match(r'CREATE USER ', text):
        return Statement('CREATE USER', (), '', text)
    if _match(r'DROP USER ', text):
        return Statement('DROP USER', (), '', text)
    raise GuardError('Refusing statement of an unsupported kind: ' + first)


def _classify_routine(m, bare, text):
    """CREATE FUNCTION, CREATE AGGREGATE or CREATE PROCEDURE naming `db`.`name`: the one kind whose text may hold semicolons.

    A function or procedure body is a BEGIN ... END block whose statements end with semicolons,
    so the compound rule is: semicolons are accepted only when the text holds BEGIN and ends
    with END (the outer block); any other semicolon is compound text and refused. An aggregate
    and a table-valued function carry no block and no semicolon.
    """
    if ';' in bare:
        words = bare.upper().split()
        if 'BEGIN' not in words or words[-1] != 'END':
            raise GuardError('Refusing compound text; a routine body holds semicolons only inside its BEGIN ... END block')
    return Statement('CREATE ' + m.group(1).upper(), (m.group(2),), m.group(3), text)


class Guard:
    """The statement allowance of one step and identity.

    kinds: write kinds the guard permits (reads are always permitted).
    databases: the database names a write may name; None means any name (reads and USE only
    make sense with None). An empty set permits only writes that name no database.
    probe_database: when set, one DELETE in the write probe form may name this database.
    per_kind: {kind: databases} narrows the database set for single kinds (DROP DATABASE on
    the retained copy only at O10, for example).
    required_detail: {kind: detail} insists on a clause (ATTACH DATABASE must carry READ ONLY
    at O3, for example).
    """

    def __init__(self, name, kinds=(), databases=None, probe_database=None, per_kind=None, required_detail=None):
        self.name = name
        self.kinds = frozenset(kinds)
        self.per_kind = {k: frozenset(v) for k, v in (per_kind or {}).items()}
        self.required_detail = dict(required_detail or {})
        unknown = (self.kinds | set(self.per_kind)) - ALL_KINDS
        if unknown:
            raise KitError('Guard %s names unknown statement kinds: %s' % (name, ', '.join(sorted(unknown))))
        self.kinds = self.kinds | set(self.per_kind)
        self.databases = None if databases is None else frozenset(databases)
        self.probe_database = probe_database

    def check(self, sql):
        """Classify the statement and raise GuardError unless this guard permits it."""
        st = classify(sql)
        if st.is_read:
            return st
        if st.kind == 'USE':
            if self.databases is None or st.databases[0] in self.databases:
                return st
            raise GuardError('%s guard refuses USE of a database outside the run' % self.name)
        if st.kind == 'DELETE' and self.probe_database is not None and st.databases[0] == self.probe_database:
            if PROBE.fullmatch(st.sql):
                return st
            raise GuardError('%s guard refuses a DELETE on the source outside the write probe form' % self.name)
        if st.kind not in self.kinds:
            raise GuardError('%s guard refuses %s' % (self.name, st.kind))
        if st.kind == 'ALTER TABLE' and st.detail not in ('AUTO_INCREMENT', 'RENAME'):
            raise GuardError('%s guard refuses ALTER TABLE other than AUTO_INCREMENT or RENAME TO' % self.name)
        if st.kind == 'AGGREGATOR SYNC AUTO_INCREMENT' and st.detail != 'ALL':
            raise GuardError('%s guard refuses AGGREGATOR SYNC AUTO_INCREMENT without ALL' % self.name)
        if st.kind == 'DETACH DATABASE' and st.detail == 'FORCE':
            raise GuardError('DETACH DATABASE FORCE is never issued by the kit')
        want = self.required_detail.get(st.kind)
        if want is not None and st.detail != want:
            raise GuardError('%s guard requires %s with %s' % (self.name, st.kind, want))
        allowed = self.per_kind.get(st.kind, self.databases)
        if allowed is not None:
            for db in st.databases:
                if db not in allowed:
                    raise GuardError('%s guard refuses %s naming database %s' % (self.name, st.kind, db))
        return st

    def allows(self, sql):
        """True when check() would accept the statement."""
        try:
            self.check(sql)
        except GuardError:
            return False
        return True

    def __repr__(self):
        return 'Guard(%s, kinds=%d, databases=%r)' % (self.name, len(self.kinds), None if self.databases is None else sorted(self.databases))


def read_guard(name='read'):
    """Reads, session settings and USE only. The guard for inventory, verification and watch."""
    return Guard(name, (), databases=None)


def runner_guard(replacement, name='runner'):
    """The runner allowance: the eight write kinds of section 10.4, naming the replacement only."""
    return Guard(name, RUNNER_KINDS, (replacement,))


def probe_guard(source, name='probe'):
    """Reads plus the O3 write probe: one DELETE on the source in the probe form."""
    return Guard(name, ('DELETE',), databases=(), probe_database=source)


def owner_guard(name, kinds, databases, per_kind=None, required_detail=None):
    """An owner allowance for one admin step: the listed kinds, naming only run databases."""
    return Guard(name, kinds, databases, per_kind=per_kind, required_detail=required_detail)
