"""s2rebuild watch: the writer watch and the quiet proof.

Author: John Bergamini

Section 10.15 of the architecture. watch reads PROCESSLIST every interval and prints non-runner,
non-idle sessions on the database with user, host, statement kind and the first 100 characters
of the statement; every 60 seconds it counts the largest tables and prints the deltas. It
refuses to start without the PROCESS privilege instead of running blind. quiet-proof checks
pipelines (none Running), foreign sessions (none non-idle) and counts and newest values on the
largest tables across two samples 60 seconds apart, then writes freeze_time to the journal.
Statements issued through the read guard: SELECT on PROCESSLIST, USER_PRIVILEGES, PIPELINES,
COUNT(*) and MAX(time column). Nothing else.
"""
import re
import time

from . import inventory, verify
from .common import KitError, iso, utc_now
from .sqlutil import qualified

WRITE = re.compile(r'^\s*(INSERT|UPDATE|DELETE|REPLACE|LOAD|TRUNCATE|ALTER|DROP|CREATE|START\s+PIPELINE)\b', re.I)
IDLE_COMMANDS = ('Sleep', 'Binlog Dump', 'Connect')
COUNT_EVERY_SECONDS = 60


def current_grantee(session):
    """The CURRENT_USER() in the 'user'@'host' form used by USER_PRIVILEGES."""
    rows = session.query('SELECT CURRENT_USER() AS u')
    user = str(rows[0]['u'] if rows and isinstance(rows[0], dict) else (rows[0][0] if rows else ''))
    if '@' in user:
        name, host = user.split('@', 1)
        return "'%s'@'%s'" % (name.strip("'"), host.strip("'"))
    return "'%s'@'%%'" % user


def has_process_privilege(session):
    """True when the connected account holds PROCESS globally."""
    grantee = current_grantee(session)
    for r in session.query(inventory.USER_PRIVILEGES_SQL):
        if str(r['GRANTEE']) == grantee and str(r['PRIVILEGE_TYPE']).upper() == 'PROCESS':
            return True
    return False


def foreign_sessions(session, db_name, protected_users):
    """PROCESSLIST rows on the database from accounts other than the protected ones, non-idle only."""
    rows = session.query(inventory.PROCESSLIST_SQL, (db_name, '%' + db_name + '%'))
    out = []
    for r in rows:
        if str(r.get('USER')) in protected_users:
            continue
        if str(r.get('COMMAND') or '') in IDLE_COMMANDS and not (r.get('INFO') or '').strip():
            continue
        out.append(r)
    return out


def statement_kind(info):
    """The leading keyword of a statement, or 'other'."""
    text = (info or '').strip()
    m = WRITE.match(text)
    if m:
        return ' '.join(m.group(1).upper().split())
    return text.split(' ', 1)[0].upper() if text else 'other'


def count_largest(session, db_name, plans, limit):
    """{table: count} for the largest tables of the plan."""
    out = {}
    for p in sorted(plans, key=lambda p: -p.est_rows)[:limit]:
        rows = session.query(verify.count_sql(db_name, p.name))
        out[p.name] = int(rows[0]['n'] if rows and isinstance(rows[0], dict) else (rows[0][0] if rows else 0))
    return out


def newest_largest(session, db_name, plans, limit):
    """{table: newest time value} for the largest tables that have a time column."""
    out = {}
    for p in sorted(plans, key=lambda p: -p.est_rows)[:limit]:
        if p.time_column:
            rows = session.query(verify.newest_sql(db_name, p.name, p.time_column))
            out[p.name] = str(rows[0]['newest'] if rows and isinstance(rows[0], dict) else (rows[0][0] if rows else None))
    return out


def watch(session, db_name, plans, protected_users, log, interval=5, tables=10, until=None, sleep=time.sleep, clock=time.monotonic,
          max_checks=None):
    """The writer watch loop. Stops at until (UTC datetime), after max_checks samples, or on Ctrl-C.

    Returns {'checks': n, 'writers_seen': n, 'count_deltas': {table: delta}}.
    """
    if not has_process_privilege(session):
        raise KitError('The connected account lacks the PROCESS privilege; the watch would see only its own sessions. Grant PROCESS first.')
    log('writer watch started on %s: every %d s non-runner sessions, every %d s count deltas on the %d largest tables' % (
        db_name, interval, COUNT_EVERY_SECONDS, tables))
    checks = writers = 0
    last_counts = count_largest(session, db_name, plans, tables)
    last_count_at = clock()
    deltas = {name: 0 for name in last_counts}
    while True:
        if until is not None and utc_now() >= until:
            break
        if max_checks is not None and checks >= max_checks:
            break
        try:
            tick = _watch_tick(session, db_name, protected_users, log)
        except KeyboardInterrupt:
            log('watch stopped by Ctrl-C')
            break
        writers += tick
        checks += 1
        if clock() - last_count_at >= COUNT_EVERY_SECONDS:
            counts = count_largest(session, db_name, plans, tables)
            changed = {name: counts[name] - last_counts.get(name, counts[name]) for name in counts if counts[name] != last_counts.get(name)}
            for name, delta in changed.items():
                deltas[name] = deltas.get(name, 0) + delta
            log('counts: %s' % (', '.join('%s %+d' % kv for kv in sorted(changed.items())) or 'no change on the largest tables'))
            last_counts, last_count_at = counts, clock()
        try:
            sleep(interval)
        except KeyboardInterrupt:
            log('watch stopped by Ctrl-C')
            break
    log('writer watch finished after %d checks; sessions seen %d; net count deltas %s' % (
        checks, writers, ', '.join('%s %+d' % kv for kv in sorted(deltas.items()) if kv[1]) or 'none'))
    return {'checks': checks, 'writers_seen': writers, 'count_deltas': deltas}


def _watch_tick(session, db_name, protected_users, log):
    writers = 0
    for r in foreign_sessions(session, db_name, protected_users):
        writers += 1
        log('SESSION SEEN id=%s user=%s host=%s command=%s time=%ss kind=%s stmt=%s' % (
            r.get('ID'), r.get('USER'), r.get('HOST'), r.get('COMMAND'), r.get('TIME'), statement_kind(r.get('INFO')),
            (r.get('INFO') or '')[:100].replace('\n', ' ')))
    return writers


def quiet_proof(session, owner_session, db_name, plans, protected_users, journal, log, tables=10, sleep=time.sleep):
    """The Path B quiet proof; returns (passed, lines) and writes freeze_time to the journal when it passes."""
    lines = []
    ok = True
    if owner_session is not None:
        running = [r['PIPELINE_NAME'] for r in owner_session.query(inventory.PIPELINES_SQL, (db_name,)) if str(r['STATE']) == 'Running']
        if running:
            ok = False
            lines.append('pipelines still Running: ' + ', '.join(running))
        else:
            lines.append('pipelines: none Running')
    else:
        ok = False
        lines.append('pipelines: not visible without the owner profile; the quiet proof cannot pass')
    foreign = foreign_sessions(session, db_name, protected_users)
    if foreign:
        ok = False
        lines.append('non-idle sessions from other accounts: ' + ', '.join('%s@%s' % (r.get('USER'), str(r.get('HOST', '')).split(':')[0]) for r in foreign))
    else:
        lines.append('sessions: none from other accounts')
    counts1, newest1 = count_largest(session, db_name, plans, tables), newest_largest(session, db_name, plans, tables)
    sleep(COUNT_EVERY_SECONDS)
    counts2, newest2 = count_largest(session, db_name, plans, tables), newest_largest(session, db_name, plans, tables)
    moved = sorted(set(n for n in counts1 if counts1[n] != counts2.get(n)) | set(n for n in newest1 if newest1[n] != newest2.get(n)))
    if moved:
        ok = False
        lines.append('counts or newest values changed on: ' + ', '.join(moved))
    else:
        lines.append('counts and newest values stable on the %d largest tables across two samples' % len(counts1))
    if ok:
        when = iso(utc_now())
        journal.set_meta('freeze_time', when)
        journal.event('quiet_proof', 'passed; freeze_time ' + when)
        lines.append('quiet proof passed; freeze_time ' + when)
    else:
        journal.event('quiet_proof', 'failed: ' + '; '.join(lines))
        lines.append('quiet proof FAILED')
    for line in lines:
        log(line)
    return ok, lines
