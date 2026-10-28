"""s2rebuild projections: captured at P3 and O4, created in the replacement at O7 once its tables are full.

Author: John Bergamini

Section 10.24 of the architecture (roadmap R6). information_schema.PROJECTIONS lists the
projections of a database, a READ ONLY attachment included (fact 45), and SHOW CREATE
PROJECTION <name> FOR TABLE `db`.`table` gives the definition, whose FROM names the table
without its database. CREATE PROJECTION resolves its table against the current database and
fails with 1046 without one, so the kit issues USE of the replacement, then the captured text
as it is. P3 and O4 read them with the owner session and store each definition under
inventory/<step>/definitions/projections/<table>.<name>.sql (mode 0600). O7 creates them after
the stale check and the counters, so each projection is built once over the full table and
never slows the copy; ATTACH ... AS carries them to the live name at S3 (fact 45). The gate:
every captured projection exists in the replacement and its normalized definition equals the
capture; the PROJECTIONS row count is recorded for the S3 check. Statements issued through the
O7 owner guard: USE of the replacement, CREATE PROJECTION; reads: SELECT on
information_schema.PROJECTIONS, SHOW CREATE PROJECTION.
"""
import json
import re

from . import steps
from .common import EXIT_REFUSED, KitError
from .sqlutil import ident, qualified

PROJECTIONS_SQL = ('SELECT PROJECTION_NAME, BASE_TABLE_NAME, STATUS FROM information_schema.PROJECTIONS WHERE PROJECTION_SCHEMA = %s '
                   'ORDER BY BASE_TABLE_NAME, PROJECTION_NAME')
PROJECTION_COUNT_SQL = 'SELECT COUNT(*) AS n FROM information_schema.PROJECTIONS WHERE PROJECTION_SCHEMA = %s'
DEFINITIONS_SUBDIR = 'projections'
PROJECTION_COUNT_META = 'projection_count'  # the PROJECTIONS row count of the replacement after O7, the number S3 expects on the live name
_HEAD = re.compile(r'^\s*CREATE PROJECTION (?:IF NOT EXISTS )?`([A-Za-z0-9_]+)`', re.I)
_FROM = re.compile(r'\bFROM\s+(?:`([A-Za-z0-9_]+)`\.)?`([A-Za-z0-9_]+)`\s*;?\s*$', re.I)


def show_create_sql(name, db_name, table):
    """SHOW CREATE PROJECTION `name` FOR TABLE `db`.`table` (the form engine 10.8.0 accepts; ON TABLE and a bare name are 1064)."""
    return 'SHOW CREATE PROJECTION %s FOR TABLE %s' % (ident(name), qualified(db_name, table))


def normalize(definition):
    """Whitespace-insensitive projection text without a trailing semicolon; the gate compares these. Issues nothing."""
    text = ' '.join(str(definition or '').split())
    return text[:-1].rstrip() if text.endswith(';') else text


def file_name(projection):
    """<table>.<name>.sql under definitions/projections/."""
    return '%s.%s.sql' % (projection['table'], projection['name'])


def label(projection):
    """'<table>.<name>' for log lines, the plan sheet and refusals."""
    return '%s.%s' % (projection['table'], projection['name'])


def refusal(projection):
    """Why a captured definition cannot be replayed in the replacement as it is, or None. Issues nothing.

    The text must name the projection where the server writes it and read one table of the
    current database: a FROM that names a database would read the source, not the replacement.
    """
    text = str(projection.get('definition') or '')
    head = _HEAD.match(text)
    if not head or head.group(1) != projection['name']:
        return '%s: the captured definition does not start with CREATE PROJECTION `%s`' % (label(projection), projection['name'])
    tail = _FROM.search(text)
    if not tail or tail.group(1) or tail.group(2) != projection['table']:
        return '%s: the captured definition does not end with FROM `%s` (no database qualifier)' % (label(projection), projection['table'])
    return None


def refuse(inventory, step='O7'):
    """The refusal text when a captured definition cannot be replayed as it is (refusal); None otherwise. Issues nothing."""
    refused = [r for r in (refusal(p) for p in inventory.get('projections') or []) if r] if inventory.get('projections_captured') else []
    if not refused:
        return None
    return ('%s; no projection statement was issued. Edit the definition under definitions/projections/ of the capture, then rerun %s'
            % ('; '.join(refused), step))


def summary(inventory):
    """'N projections (table.name, ...)', or why none were captured, for the plan sheet and the capture log."""
    if not inventory.get('projections_captured'):
        return 'projections not captured (no owner profile)'
    items = inventory.get('projections') or []
    return '%d projections%s' % (len(items), ' (%s)' % ', '.join(label(p) for p in items) if items else '')


def _count(rows):
    if not rows:
        return 0
    row = rows[0]
    return int((row['n'] if isinstance(row, dict) else row[0]) or 0)


def _text(rows, what):
    if not rows:
        raise KitError('%s returned nothing; capture again with the owner profile' % what)
    row = rows[0]
    if isinstance(row, dict):
        return str(row.get('Create Projection') or list(row.values())[-1])
    return str(row[-1])


def listed(session, db_name):
    """{(table, name): status} of the projections information_schema.PROJECTIONS lists for a database. Issues that SELECT."""
    return {(str(r['BASE_TABLE_NAME']), str(r['PROJECTION_NAME'])): str(r.get('STATUS') or '') for r in session.query(PROJECTIONS_SQL, (db_name,))}


def capture(session, db_name):
    """Every projection of a database with its definition, through the owner session.

    Issues the PROJECTIONS listing, then SHOW CREATE PROJECTION per projection. Returns the list
    of {'name', 'table', 'status', 'definition'} in table and name order.
    """
    out = []
    for (table, name), status in sorted(listed(session, db_name).items()):
        text = _text(session.query(show_create_sql(name, db_name, table)), 'SHOW CREATE PROJECTION of %s.%s' % (table, name))
        out.append({'name': name, 'table': table, 'status': status, 'definition': text})
    return out


def create(session, target_db, projections, journal, log, step='O7'):
    """Create every captured projection missing from the replacement; returns the labels created.

    Issues USE of the replacement, the PROJECTIONS listing, then per missing projection the
    captured CREATE PROJECTION and the listing again. A projection that exists is skipped. A
    server error stops the step with the projection and its error number; the projections
    already created are skipped on the rerun. journal None (dry run) records nothing.
    """
    created = []
    if not projections:
        return created
    session.execute('USE ' + ident(target_db))
    present = listed(session, target_db)
    for p in projections:
        key = (p['table'], p['name'])
        if key in present:
            log('projection %s already exists in %s; skipped' % (label(p), target_db))
            continue
        try:
            session.execute(p['definition'])
        except KitError:
            raise
        except Exception as exc:  # noqa: BLE001 - the step stops with the projection and its error number
            args = getattr(exc, 'args', ())
            raise KitError('%s stopped at CREATE PROJECTION %s: server error %s (%s). Fix the cause, then rerun %s; projections already created '
                           'are skipped' % (step, label(p), args[0] if args else 'unknown', type(exc).__name__, step)) from exc
        present = listed(session, target_db)
        if journal is not None and key not in present:
            raise KitError('%s stopped at check "projection %s exists": the CREATE returned without an error, but the server does not list it in %s; '
                           'inspect the replacement before any further statement' % (step, label(p), target_db))
        if journal is not None:
            journal.event('projection', json.dumps({'projection': p['name'], 'table': p['table'], 'status': present.get(key)}))
        log('created projection %s (status %s)' % (label(p), present.get(key, 'not read in a dry run')))
        created.append(label(p))
    return created


def verify(session, target_db, projections, log, step='O7'):
    """The gate: every captured projection exists in the replacement and its normalized definition equals the capture.

    Issues the PROJECTIONS listing and one SHOW CREATE PROJECTION per expected projection.
    Raises KitError naming every projection missing or differing. Returns the PROJECTIONS row
    count of the replacement.
    """
    present = listed(session, target_db)
    problems = []
    for p in projections:
        if (p['table'], p['name']) not in present:
            problems.append('%s missing' % label(p))
            continue
        live = _text(session.query(show_create_sql(p['name'], target_db, p['table'])), 'SHOW CREATE PROJECTION of ' + label(p))
        if normalize(live) != normalize(p['definition']):
            problems.append('%s differs from the capture' % label(p))
    if problems:
        raise KitError('%s gate failed on the projections of %s: %s. Compare definitions/projections/ of the capture with SHOW CREATE PROJECTION '
                       'in the replacement, fix the cause, then rerun %s' % (step, target_db, '; '.join(problems), step))
    log('projections gate: %d equal the capture (%s)' % (len(projections), ', '.join('%s %s' % (label(p), present[(p['table'], p['name'])])
                                                                                     for p in projections)))
    return len(present)


def place(ctx, inventory, step='O7'):
    """Create the captured projections in the replacement with the owner profile, then run the gate; returns an Outcome or None.

    None means the step goes on: nothing captured, or every projection created and verified,
    with the PROJECTIONS count of the replacement recorded for S3 (projection_count). A
    definition that cannot be replayed as it is refuses the step before any statement; a
    missing owner profile with projections to create ends it pending. A dry run prints the
    statements and records nothing.
    """
    if not inventory.get('projections_captured'):
        ctx.log('projections: not captured; none created at %s' % step)
        return None
    items = inventory.get('projections') or []
    text = refuse(inventory, step)
    if text:
        return steps.Outcome(steps.REFUSED, text, EXIT_REFUSED)
    if not ctx.owner_configured and not ctx.dry_run:
        if not items:
            return None
        return steps.Outcome(steps.PENDING, '%d projection(s) to create at %s (%s) need the owner profile; configure connections.owner, then rerun %s'
                             % (len(items), step, ', '.join(label(p) for p in items), step), steps.EXIT_GATE)
    journal = None if ctx.dry_run else ctx.journal
    owner = ctx.owner_session(steps.guard_for(step, ctx.db, steps.OWNER))
    try:
        create(owner, ctx.db.replacement, items, journal, ctx.log, step)
        if not ctx.dry_run:
            ctx.journal.set_meta(PROJECTION_COUNT_META, str(verify(owner, ctx.db.replacement, items, ctx.log, step)))
    finally:
        owner.close()
    return None


def expected_count(journal):
    """The PROJECTIONS row count S3 expects on the live name (recorded by the O7 gate), or None when no gate recorded one."""
    value = journal.get_meta(PROJECTION_COUNT_META)
    try:
        return int(value) if value not in (None, '') else None
    except ValueError:
        return None
