"""s2rebuild pipelines: stop at O2, capture after the stop, replay at O9 with credentials from the environment, release at O11, C1 checks.

Author: John Bergamini

Section 10.12 of the architecture. The copy is not complete until every pipeline exists in the
replacement, stopped, at the offsets recorded when the source stopped. O9 therefore replays only
the O2 capture taken after the stop (taken by the kit with the owner profile, or imported from
the notebook O2 result with s2rebuild gate O2 --evidence); an earlier capture is refused because
its offsets were still moving. The recorded position is the committed one: LATEST_SUCCESSFUL_OFFSET
per partition, LATEST_LOADED_OFFSET only where successful is NULL; O2 refuses a capture in which
the two differ on any partition after the stop (a NULL successful offset beside a loaded one
included), and O9 and O11 compare and check retention partition by partition against it. Credentials are redacted by the server for every identity, so the
kit injects them at replay time from the environment variables named in the configuration, as a
SQL literal with backslashes and quotes escaped; the value lives only in the process environment
and in the statement sent to the server, and every form of it is masked in logs. Route customer
leaves the CREATE statements to the customer and keeps the same verification. Statements issued
through the owner guards: STOP PIPELINE (O2), USE, CREATE PIPELINE, ALTER PIPELINE (O9), GRANT,
START PIPELINE, SET GLOBAL (O11); SELECT on the pipeline and privilege views.
"""
import re
import time

from . import grants, inventory, ownersql, steps
from .common import EXIT_GATE, EXIT_REFUSED, KitError, iso, parse_utc, utc_now
from .sqlutil import ident, qualified, read_guard, secret_forms, sql_literal, strip_literals

RUNNING_SQL = "SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s AND STATE = 'Running'"
COUNT_SQL = 'SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s'
EXISTS_SQL = 'SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s'
STOPPED_SQL = "SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s AND STATE = 'Stopped'"
IS_RUNNING_SQL = "SELECT COUNT(*) AS n FROM information_schema.PIPELINES WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s AND STATE = 'Running'"
PARTITION_OFFSET_SQL = ('SELECT LATEST_SUCCESSFUL_OFFSET AS successful, LATEST_LOADED_OFFSET AS loaded FROM information_schema.PIPELINES_OFFSETS '
                        'WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s AND SOURCE_PARTITION_ID = %s')
PARALLELISM_SQL = 'SELECT @@GLOBAL.query_parallelism_per_leaf_core AS v'
NOT_CHECKED = 'pipelines and grants: not checked, no owner profile'
_PIPELINE_NAME = re.compile(r'PIPELINE\s+(?:`[^`]+`\.)?`([^`]+)`', re.I)


def starts_at_release(config):
    """True when O11 starts the recreated pipelines (pipelines.start_at_release: all, the default); False for none."""
    return getattr(getattr(config, 'pipelines', None), 'start_at_release', 'all') == 'all'


def expected_items(items):
    """Captured pipelines the kit recreates (application-managed ones are stopped but not replayed)."""
    return [i for i in items if not i.get('app_managed')]


def needs_credentials(item):
    """True when the definition or an ALTER statement of a pipeline carries the credentials placeholder."""
    return any(inventory.CREDENTIALS_PLACEHOLDER in (s or '') for s in [item.get('definition')] + list(item.get('alter_statements') or []))


def committed_offset(o):
    """(column, value) of a captured partition's replay position: ('successful', LATEST_SUCCESSFUL_OFFSET), the safe restart
    position; ('loaded', LATEST_LOADED_OFFSET) only when successful is NULL. O2 refuses a capture with a NULL successful
    offset beside a loaded one, so from an O2 capture the fallback carries NULL only."""
    if o.get('successful') is not None:
        return 'successful', o['successful']
    return 'loaded', o.get('loaded')


def _offsets_known(item):
    return [o for o in item.get('offsets') or [] if committed_offset(o)[1] is not None]


def names_source(text, source):
    """True when a statement names the source database outside its string literals."""
    return bool(re.search(r'`%s`\s*\.' % re.escape(source), strip_literals(text or '')))


# ---------------------------------------------------------------------------
# Capture provenance
# ---------------------------------------------------------------------------
def capture_for(ctx):
    """{'items', 'directory', 'step', 'exists'} of the newest pipeline capture: O2 (after the stop), else O4, else P3."""
    for step_id in ('O2', 'O4', 'P3'):
        directory = inventory.inventory_dir(ctx.config, ctx.run_id or '', ctx.db, step_id)
        if (directory / inventory.INVENTORY_FILE).is_file():
            data = inventory.load(directory)
            pipes = data.get('pipelines') or {}
            if pipes.get('captured'):
                return {'items': pipes['items'], 'directory': directory, 'step': step_id, 'exists': True}
    return {'items': [], 'directory': None, 'step': None, 'exists': False}


def captured_items(ctx):
    """(items, directory) of the newest pipeline capture."""
    cap = capture_for(ctx)
    return cap['items'], cap['directory']


# ---------------------------------------------------------------------------
# Blocks
# ---------------------------------------------------------------------------
def stop_block(names, items, extra_names=()):
    """O2: STOP PIPELINE for every captured pipeline (and every pipeline seen live since), then verify none Running, then capture.

    A STOP is skipped only when the pipeline is Stopped already, so a pipeline in state Error is stopped too.
    """
    b = ownersql.Block('O2', 'Stop pipelines')
    b.check('pipelines on the source (count, informational)', COUNT_SQL, (names['source'],), ownersql.Expect('some_rows'))
    if not items and not extra_names:
        b.note('No pipeline was captured yet; with the owner profile the kit reads the live PIPELINES view for this list.')
    listed = [(i['name'], bool(i.get('app_managed'))) for i in items] + [(n, False) for n in extra_names if n not in {i['name'] for i in items}]
    for name, app_managed in listed:
        b.statement('STOP PIPELINE %s' % qualified(names['source'], name), 'stop pipeline %s%s' % (name, ' (application managed)' if app_managed else ''),
                    skip_if=(STOPPED_SQL, (names['source'], name)))
    b.check('no pipeline Running', RUNNING_SQL, (names['source'],), ownersql.Expect('column_equals', 'n', 0))
    b.capture(names['source'], 'every definition and offset of %s is read again now; the offsets captured after the stop are the ones '
                               'replayed at O9 (in the notebook the result carries them for s2rebuild gate O2 --evidence)' % names['source'])
    return b


def _capture_refusal(capture_step, items):
    if capture_step is None:
        return 'no pipeline capture exists yet; capture with the owner profile (O2), or import the notebook O2 result with s2rebuild gate O2 --evidence'
    if capture_step != 'O2' and items:
        return ('the pipeline capture comes from %s, taken before the stop, so its offsets were still moving; O9 replays only the O2 capture '
                'taken after the stop (run O2, or import the notebook O2 result with s2rebuild gate O2 --evidence)' % capture_step)
    return None


def _verify_checks(b, names, expected):
    """The O9 gate checks: count, none Running, then one check per pipeline and partition that its committed offset equals the capture."""
    b.check('pipeline count equals the capture minus application-managed ones', COUNT_SQL, (names['replacement'],),
            ownersql.Expect('column_equals', 'n', len(expected)))
    b.check('no pipeline Running in the replacement', RUNNING_SQL, (names['replacement'],), ownersql.Expect('column_equals', 'n', 0))
    for item in expected:
        for o in _offsets_known(item):
            column, value = committed_offset(o)
            b.check('offset of %s partition %s equals the O2 capture (%s)' % (
                item['name'], o['partition'], 'LATEST_SUCCESSFUL_OFFSET' if column == 'successful'
                else 'LATEST_LOADED_OFFSET: the capture holds no successful offset'), PARTITION_OFFSET_SQL,
                (names['replacement'], item['name'], str(o['partition'])), ownersql.Expect('column_equals', column, value))


def replay_block(names, items, capture_step='O2'):
    """O9: USE the replacement, CREATE each pipeline (skipped when it exists; never started), its ALTER statements, then the checks.

    A STOP comes first for a capture that cannot be replayed, a pipeline that names the source,
    and a definition that still carries a redaction marker (as check_credentials refuses on the
    kit route), so the notebook never sends a masked value to CREATE PIPELINE.
    """
    b = ownersql.Block('O9', 'Recreate pipelines in the replacement, stopped, at the recorded offsets')
    expected = expected_items(items)
    refusal = _capture_refusal(capture_step, items)
    if refusal:
        b.refuse(refusal)
    for item in expected:
        for text in [item.get('definition')] + list(item.get('alter_statements') or []):
            if names_source(text, names['source']):
                b.refuse('pipeline %s names the source database %s in its statements; edit the capture so it loads into the replacement'
                         % (item['name'], names['source']))
                break
    for item in expected:
        if inventory.has_redaction_marker(item.get('definition')):
            b.refuse('pipeline %s still carries a redaction marker in its definition; capture it again' % item['name'])
    b.statement('USE %s' % ident(names['replacement']), 'work inside the replacement')
    for item in expected:
        b.statement(item['definition'], 'create pipeline %s' % item['name'], skip_if=(EXISTS_SQL, (names['replacement'], item['name'])),
                    pipeline=item['name'], source_type=item.get('source_type'))
        for alter in item.get('alter_statements') or []:
            b.statement(alter, 'set offsets of %s' % item['name'], pipeline=item['name'], source_type=item.get('source_type'))
    _verify_checks(b, names, expected)
    b.note('The kit then compares PIPELINES_OFFSETS in the replacement with the O2 capture, partition by partition. Never START here.')
    return b


def verify_block(names, items, capture_step='O2', placeholder_file=None):
    """O9 route customer: the customer runs the placeholder file in the replacement; the kit runs only the checks."""
    b = ownersql.Block('O9', 'Verify the pipelines the customer recreated in the replacement')
    refusal = _capture_refusal(capture_step, items)
    if refusal:
        b.refuse(refusal)
    b.note('Route customer: the customer runs %s inside %s, filling in CREDENTIALS, and never starts a pipeline. These checks are the same gate.'
           % (placeholder_file or 'pipelines/recreate-with-placeholders.sql of the O2 capture', names['replacement']))
    _verify_checks(b, names, expected_items(items))
    return b


def release_block(names, items, grant_snapshot, config=None):
    """O11: retention and partition checks, GRANT restores first (each checked), START PIPELINE per expected pipeline, parallelism.

    With pipelines.start_at_release: none no pipeline is started: the block checks that every
    captured partition is present, that none is Running, and notes how to start them later; the
    retention check belongs to the start and is left out.
    """
    start = starts_at_release(config)
    b = ownersql.Block('O11', 'Release: restore grants, start pipelines, application restart' if start else
                       'Release: restore grants, pipelines left stopped, application restart')
    for item in expected_items(items):
        known = _offsets_known(item)
        if not known:
            continue
        parts = [str(o['partition']) for o in known]
        b.check('every captured partition of %s present on the live name' % item['name'],
                'SELECT COUNT(*) AS n FROM information_schema.PIPELINES_OFFSETS WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s '
                'AND SOURCE_PARTITION_ID IN (' + ', '.join(['%s'] * len(parts)) + ')',
                (names['source'], item['name']) + tuple(parts), ownersql.Expect('column_equals', 'n', len(parts)))
        if not start:
            continue
        b.check('no retention gap on %s (recorded committed offset not below EARLIEST_OFFSET)' % item['name'],
                'SELECT COUNT(*) AS n FROM information_schema.PIPELINES_OFFSETS WHERE DATABASE_NAME = %s AND PIPELINE_NAME = %s AND ('
                + ' OR '.join(['(SOURCE_PARTITION_ID = %s AND EARLIEST_OFFSET > %s)'] * len(known)) + ')',
                (names['source'], item['name']) + tuple(v for o in known for v in (str(o['partition']), committed_offset(o)[1])),
                ownersql.Expect('column_equals', 'n', 0))
    for grantee, scopes in sorted(grants.normalize(grant_snapshot).items()):
        for scope, privs in sorted(scopes.items()):
            sql, params = grants.privilege_count_check(grantee, scope, privs, names['source'])
            b.check('%s write privileges on %s before the restore (informational)' % (grantee, scope), sql, params, ownersql.Expect('some_rows'))
            b.statement('GRANT %s ON %s TO %s' % (', '.join(privs), scope, grantee), 'restore the grants revoked by the lockout')
            b.check('%s holds every restored privilege on %s' % (grantee, scope), sql, params, ownersql.Expect('column_equals', 'n', len(privs)))
    if start:
        for item in expected_items(items):
            b.statement('START PIPELINE %s' % qualified(names['source'], item['name']), 'start pipeline %s on the live database' % item['name'],
                        skip_if=(IS_RUNNING_SQL, (names['source'], item['name'])))
        b.check('every expected pipeline Running', RUNNING_SQL, (names['source'],), ownersql.Expect('column_equals', 'n', len(expected_items(items))))
    else:
        b.check('no pipeline Running (pipelines.start_at_release: none)', RUNNING_SQL, (names['source'],), ownersql.Expect('column_equals', 'n', 0))
        b.note('%d pipeline(s) left stopped by pipelines.start_at_release: none. Start one when you choose: START PIPELINE `%s`.`<name>`;'
               % (len(expected_items(items)), names['source']))
    if names.get('flexible_parallelism') is not None and config is not None:
        b.check('current query_parallelism_per_leaf_core (informational)', PARALLELISM_SQL, (), ownersql.Expect('some_rows'))
        b.statement('SET GLOBAL query_parallelism_per_leaf_core = %s' % names['flexible_parallelism'], 'flexible parallelism for databases with sub-partitions (new connections)')
        b.check('query_parallelism_per_leaf_core after the change (informational)', PARALLELISM_SQL, (), ownersql.Expect('some_rows'))
    b.note('Pipelines in the retained copy stay stopped. The customer restarts the application; C1 verifies it writes to the live name.')
    return b


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------
def credential_for(item, config, env):
    """(value, variable) for a pipeline: the per-pipeline variable first, then the source-type variable; KitError when unset."""
    if inventory.has_redaction_marker(item['definition']):
        raise KitError('Pipeline %s still carries a redaction marker in its definition; capture it again' % item['name'])
    var = config.pipelines.per_pipeline_env.get(item['name']) or config.pipelines.credentials_env.get(item['source_type'])
    if not var:
        raise KitError('No credential variable is configured for pipeline %s (source type %s); set pipelines.credentials_env or per_pipeline_env' % (
            item['name'], item['source_type']))
    value = env.get(var)
    if not value:
        raise KitError('Environment variable %s for pipeline %s is not set; export it for O9 only, then unset it' % (var, item['name']))
    return value, var


def check_credentials(items, config, env):
    """Before any statement: every expected pipeline free of redaction markers, and a value set for each one that needs credentials."""
    for item in expected_items(items):
        if inventory.has_redaction_marker(item['definition']):
            raise KitError('Pipeline %s still carries a redaction marker in its definition; capture it again' % item['name'])
        if needs_credentials(item):
            credential_for(item, config, env)


def substitute_credentials(items, config, env, redactor=None):
    """A substitute(sql) callable for execute_block: replaces the placeholder with the pipeline's credential as a SQL literal."""
    by_name = {i['name']: i for i in items}

    def _substitute(sql):
        if inventory.CREDENTIALS_PLACEHOLDER not in sql:
            return sql
        m = _PIPELINE_NAME.search(sql)
        name = m.group(1) if m else None
        item = by_name.get(name)
        if item is None:
            raise KitError('Cannot tell which pipeline a statement belongs to; refusing to inject credentials')
        value, _ = credential_for(item, config, env)
        if redactor is not None:
            redactor.extend(secret_forms(value))
        return sql.replace("'%s'" % inventory.CREDENTIALS_PLACEHOLDER, sql_literal(value))
    return _substitute


# ---------------------------------------------------------------------------
# Offsets
# ---------------------------------------------------------------------------
def offsets_in(session, db_name):
    """{(pipeline, partition): (earliest, latest, loaded, successful)} from PIPELINES_OFFSETS."""
    out = {}
    for r in session.query(inventory.OFFSETS_SQL, (db_name,)):
        out[(str(r['PIPELINE_NAME']), str(r['SOURCE_PARTITION_ID']))] = (
            inventory._int(r.get('EARLIEST_OFFSET'), None), inventory._int(r.get('LATEST_OFFSET'), None),
            inventory._int(r.get('LATEST_LOADED_OFFSET'), None), inventory._int(r.get('LATEST_SUCCESSFUL_OFFSET'), None))
    return out


def offsets_equal(session, replacement, items):
    """Differences between the replacement's committed offsets and the capture, partition by partition: [(pipeline, partition, recorded, replacement)].

    The compared column is LATEST_SUCCESSFUL_OFFSET; LATEST_LOADED_OFFSET only for a partition whose captured successful offset is NULL.
    """
    current = offsets_in(session, replacement)
    diffs = []
    for item in expected_items(items):
        for o in item.get('offsets') or []:
            column, recorded = committed_offset(o)
            got = current.get((item['name'], str(o['partition'])))
            observed = (got[3] if column == 'successful' else got[2]) if got else None
            if observed != recorded:
                diffs.append((item['name'], str(o['partition']), recorded, observed))
    return diffs


def retention_gaps(session, db_name, items):
    """[(pipeline, partition, recorded committed offset, current earliest)] where the recorded offset is below the current EARLIEST.

    The recorded offset is LATEST_SUCCESSFUL_OFFSET, or LATEST_LOADED_OFFSET when successful is
    NULL. A captured partition with no PIPELINES_OFFSETS row on db_name is reported with earliest
    'missing', so the release refuses before any START.
    """
    current = offsets_in(session, db_name)
    gaps = []
    for item in expected_items(items):
        for o in _offsets_known(item):
            recorded = committed_offset(o)[1]
            got = current.get((item['name'], str(o['partition'])))
            if got is None:
                gaps.append((item['name'], str(o['partition']), recorded, 'missing'))
                continue
            earliest = got[0]
            if earliest is not None and recorded < earliest:
                gaps.append((item['name'], str(o['partition']), recorded, earliest))
    return gaps


def record_capture(journal, items, phase='capture', definition_dir=None):
    """Journal rows for captured pipelines and their offsets."""
    for item in items:
        journal.upsert_pipeline(item['name'], state_at_capture=item['state'], source_type=item['source_type'], source_location=item['source_location'],
                               target=item['target'], batch_interval=item['batch_interval'], duplicate_handling=item['duplicate_handling'],
                               definition_file=str(definition_dir / (item['name'] + '.sql')) if definition_dir else None,
                               app_managed=1 if item.get('app_managed') else 0)
        for o in item.get('offsets') or []:
            journal.record_offsets(item['name'], o['partition'], phase, o.get('earliest'), o.get('latest'), o.get('loaded'), o.get('successful'))


def write_capture(ctx, items, origin):
    """Write the O2 capture (inventory/O2 with 0600 definition files) from items and journal it; returns the directory."""
    data = inventory.load(inventory.inventory_dir(ctx.config, ctx.run_id or '', ctx.db, 'P3'))
    data['pipelines'] = {'captured': True, 'note': '%d pipelines captured after the stop (%s)' % (len(items), origin), 'items': items}
    data['step'] = 'O2'
    data['captured_utc'] = iso(utc_now())
    directory = inventory.inventory_dir(ctx.config, ctx.run_id or '', ctx.db, 'O2')
    inventory.write(data, directory, inventory.plan_tables(data, ctx.config, ctx.db))
    record_capture(ctx.journal, items, 'capture', directory / inventory.PIPELINES_DIR)
    ctx.log('pipelines captured after the stop: %d (%s), written to %s' % (len(items), origin, directory))
    return directory


def _require_stopped(items):
    running = [i['name'] for i in items if str(i.get('state')) != 'Stopped']
    if running:
        raise KitError('O2 capture refused: pipeline(s) %s are not Stopped after the stop (an application or another admin restarted them); '
                       'stop them and rerun O2' % ', '.join(running))


def _require_committed(items):
    """Refuse the capture when LATEST_LOADED_OFFSET differs from LATEST_SUCCESSFUL_OFFSET on any partition after the stop.

    The replay position must be the committed one. A NULL successful offset beside a loaded one
    counts as a difference (a batch extracted and never committed); only both NULL passes.
    """
    def shown(v):
        """An offset as the refusal prints it: NULL for None."""
        return 'NULL' if v is None else v
    pending = ['%s partition %s (loaded %s, successful %s)' % (i['name'], o['partition'], shown(o.get('loaded')), shown(o.get('successful')))
               for i in items for o in i.get('offsets') or [] if o.get('loaded') != o.get('successful')]
    if pending:
        raise KitError('O2 capture refused: LATEST_LOADED_OFFSET differs from LATEST_SUCCESSFUL_OFFSET after the stop on %s; the replay position '
                       'must be the committed offset, so rerun O2 once they agree (the rerun skips the stopped pipelines and reads the offsets again)'
                       % '; '.join(pending[:10]))


def capture_after_stop(ctx, owner_session):
    """O2 python part: capture definitions and offsets again after the stop, refuse when any pipeline runs again or a partition holds
    an uncommitted batch, write and journal."""
    items = inventory._pipelines(owner_session, ctx.db.source, set(ctx.db.app_managed_pipelines), utc_now())['items']
    _require_stopped(items)
    _require_committed(items)
    write_capture(ctx, items, 'owner profile')
    return items


def items_from_capture(capture, app_managed=()):
    """Pipeline items from the capture block of a notebook O2 result (states, SHOW CREATE PIPELINE EXTENDED rows, offsets)."""
    offsets = {}
    for o in capture.get('offsets') or []:
        offsets.setdefault(str(o['PIPELINE_NAME']), []).append({
            'partition': str(o['SOURCE_PARTITION_ID']), 'earliest': inventory._int(o.get('EARLIEST_OFFSET'), None),
            'latest': inventory._int(o.get('LATEST_OFFSET'), None), 'loaded': inventory._int(o.get('LATEST_LOADED_OFFSET'), None),
            'successful': inventory._int(o.get('LATEST_SUCCESSFUL_OFFSET'), None)})
    items = []
    for s in capture.get('states') or []:
        name = str(s['PIPELINE_NAME'])
        create, alters = inventory.parse_pipeline_statements(capture.get('definitions', {}).get(name) or [])
        if not create:
            raise KitError('The notebook capture holds no CREATE statement for pipeline %s' % name)
        definition = inventory.redact_credentials(create, name)
        # the marker is read on the stored definition, the text O9 checks (check_credentials), as in the kit capture
        item = {'name': name, 'state': str(s.get('STATE')), 'definition': definition,
                'alter_statements': [inventory.redact_credentials(a, name) for a in alters], 'redaction_marker': inventory.has_redaction_marker(definition),
                'app_managed': name in set(app_managed), 'offsets': offsets.get(name, []), 'last_batch_time': None, 'rows_24h': 0,
                'errors_24h': 0, 'last_error': None}
        item.update(inventory.describe_pipeline(create))
        items.append(item)
    return sorted(items, key=lambda i: i['name'])


def import_capture(ctx, capture):
    """Import the notebook O2 capture (gate O2 --evidence) as the O2 capture; refuses when a pipeline is not Stopped or loaded differs
    from successful on a partition."""
    if str(capture.get('database')) != ctx.db.source:
        raise KitError('The notebook capture is for database %s, not %s' % (capture.get('database'), ctx.db.source))
    items = items_from_capture(capture, ctx.db.app_managed_pipelines)
    _require_stopped(items)
    _require_committed(items)
    return write_capture(ctx, items, 'notebook O2 result')


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------
def handle_o2(step, ctx):
    """O2 handler: the stop block (capture plus every pipeline the live view shows), then the capture after the stop."""
    cap = capture_for(ctx)
    items = cap['items']
    extra = []
    if ctx.owner_configured and not ctx.dry_run:
        owner = ctx.owner_session(read_guard('owner O2'))
        try:
            live = [str(r['PIPELINE_NAME']) for r in owner.query(inventory.PIPELINES_SQL, (ctx.db.source,))]
        finally:
            owner.close()
        known = {i['name'] for i in items}
        extra = [n for n in live if n not in known]
        if extra:
            ctx.log('pipelines not in the %s capture, stopped too: %s' % (cap['step'] or 'earlier', ', '.join(extra)))
    names = ownersql.names_for(ctx)
    b = stop_block(names, items, extra)

    def _after(results):
        owner = ctx.owner_session(read_guard('owner O2 capture'))
        try:
            capture_after_stop(ctx, owner)
        finally:
            owner.close()
    return ownersql.handle_admin(step, ctx, b, after=_after)


def handle_o9(step, ctx):
    """O9 handler: replay the O2 capture with credentials from the environment (or verify the customer's replay), then the offset checks."""
    cap = capture_for(ctx)
    items = cap['items']
    expected = expected_items(items)
    names = ownersql.names_for(ctx)
    route = ctx.config.pipelines.route
    if route == 'customer':
        placeholder = (cap['directory'] / inventory.PIPELINES_DIR / inventory.RECREATE_FILE) if cap['directory'] else None
        b = verify_block(names, items, cap['step'], placeholder)
    else:
        b = replay_block(names, items, cap['step'])
    live = not ctx.dry_run and ctx.owner_configured
    if live:
        refusal = _capture_refusal(cap['step'], items)
        if refusal:
            ownersql.print_block(b, ctx, ownersql.owner_sql_path(ctx, 'O9'))
            return steps.Outcome(steps.FAILED, 'O9 gate cannot pass: ' + refusal, EXIT_GATE)

    def _after(results):
        owner = ctx.owner_session(read_guard('owner O9'))
        try:
            diffs = offsets_equal(owner, ctx.db.replacement, items)
            observed = offsets_in(owner, ctx.db.replacement)
        finally:
            owner.close()
        if diffs:
            raise KitError('Offsets in the replacement differ from the O2 capture: ' + '; '.join(
                '%s partition %s recorded %s replacement %s' % d for d in diffs[:10]))
        now = iso(utc_now())
        for item in expected:
            ctx.journal.upsert_pipeline(item['name'], created_in_replacement_utc=now, app_managed=0)
        for (name, partition), values in observed.items():
            ctx.journal.record_offsets(name, partition, 'before_start', *values)
        ctx.log('offsets in the replacement equal the O2 capture for %d pipeline(s)' % len(expected))
    if not live:
        return ownersql.handle_admin(step, ctx, b)
    if route == 'customer':
        return ownersql.handle_admin(step, ctx, b, after=_after)
    env = ctx.options.get('env') or {}
    check_credentials(items, ctx.config, env)
    return ownersql.handle_admin(step, ctx, b, after=_after, substitute=substitute_credentials(items, ctx.config, env, ctx.log.redactor))


def handle_o11(step, ctx):
    """O11 handler: retention and partition checks, grants restored first (from any unrestored snapshot), pipelines started, release time.

    With the owner profile, release_started_utc is written inside the open owner session once
    the retention check and every check ahead of the first statement passed, just before that
    statement (ownersql.execute_block before_first_statement). From that statement on a started
    pipeline or a restored grant may write to the live name, so an O11 that fails or is
    interrupted after it never leaves the rollback open. An O11 that stopped before it (a failed
    check, a lost connection) issued nothing and leaves the rollback open. released_utc follows
    once the whole block and the grants compare passed.
    """
    cap = capture_for(ctx)
    items = cap['items']
    names = ownersql.names_for(ctx)
    snap = grants.unrestored_snapshot(ctx.journal)
    b = release_block(names, items, snap, ctx.config)
    if ctx.dry_run or not ctx.owner_configured:
        return ownersql.handle_admin(step, ctx, b)
    owner = ctx.owner_session(read_guard('owner O11'))
    try:
        gaps = retention_gaps(owner, ctx.db.source, items) if starts_at_release(ctx.config) else []
    finally:
        owner.close()
    if gaps:
        for g in gaps:
            ctx.journal.event('retention_gap', '%s partition %s recorded %s earliest now %s' % g)
        return steps.Outcome(steps.FAILED, 'retention gap or missing partition on %d partition(s): %s; decide EARLIEST or LATEST per pipeline and record it '
                             'in the gate note' % (len(gaps), '; '.join('%s partition %s (recorded %s, earliest %s)' % g for g in gaps[:10])), EXIT_GATE)

    def _after(results):
        now = iso(utc_now())
        for item in expected_items(items) if starts_at_release(ctx.config) else ():
            ctx.journal.upsert_pipeline(item['name'], started_utc=now)
        if snap:
            owner2 = ctx.owner_session(read_guard('owner O11 compare'))
            try:
                diff = grants.compare(owner2, ctx.db.source, snap)
            finally:
                owner2.close()
            if diff:
                raise KitError('Grants differ from the snapshot after the restore: ' + ', '.join(sorted(diff)))
            for grantee in snap:
                ctx.journal.mark_grant_restored(grantee)
        ctx.journal.set_meta('released_utc', now)

    def _before_first_statement():
        ctx.journal.set_meta('release_started_utc', ctx.journal.get_meta('release_started_utc') or iso(utc_now()))
        ctx.log('    release_started_utc recorded: from the next statement on a rollback is refused')
    return ownersql.handle_admin(step, ctx, b, after=_after, before_first_statement=_before_first_statement)


def _server_time(value):
    return parse_utc(value, 'released_utc').strftime('%Y-%m-%d %H:%M:%S')


def after_release_checks(session, db_name, items, journal, log, sleep=time.sleep, started_since=None, start=True):
    """C1: every expected pipeline Running, application-managed ones present, offsets advancing where new data exists, no errors since
    the start; returns (ok, lines). With start False (pipelines.start_at_release: none) the pipelines are reported, not required
    Running, and the offset sample is skipped."""
    lines, ok = [], True
    expected = expected_items(items)
    states = {str(r['PIPELINE_NAME']): str(r['STATE']) for r in session.query(inventory.PIPELINES_SQL, (db_name,))}
    not_running = [i['name'] for i in expected if states.get(i['name']) != 'Running']
    missing = [i['name'] for i in expected if i['name'] not in states]
    if not start:
        if missing:
            ok = False
            lines.append('pipelines missing on the live database: ' + ', '.join(missing))
        lines.append('pipelines left stopped by pipelines.start_at_release: none: %d of %d not Running' % (len(not_running), len(expected)))
    elif not_running:
        ok = False
        lines.append('pipelines not Running: ' + ', '.join(not_running))
    else:
        lines.append('pipelines Running: %d' % len(expected))
    missing_app = [i['name'] for i in items if i.get('app_managed') and i['name'] not in states]
    if missing_app:
        ok = False
    lines.append('application-managed pipelines present again: %s' % ('yes' if not missing_app else 'missing ' + ', '.join(missing_app)))
    first = offsets_in(session, db_name)
    if start:
        sleep(60)
    second = offsets_in(session, db_name) if start else first
    stuck = []
    for key, (e1, l1, loaded1, s1) in first.items():
        e2, l2, loaded2, s2 = second.get(key, (None, None, None, None))
        if l1 is not None and loaded1 is not None and l1 > loaded1 and loaded2 is not None and loaded2 <= loaded1:
            stuck.append('%s partition %s' % key)
    if not start:
        lines.append('offsets not sampled: no pipeline was started')
    elif stuck:
        ok = False
        lines.append('offsets not advancing although data waits: ' + ', '.join(stuck))
    else:
        lines.append('offsets advancing or caught up on every partition')
    since = _server_time(started_since) if started_since else utc_now().replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
    errors = session.query(inventory.ERRORS_SQL, (db_name, since))
    if errors:
        ok = False
        by = {}
        for r in errors:
            by.setdefault(str(r['PIPELINE_NAME']), str(r.get('ERROR_MESSAGE') or '')[:200])
        lines.append('pipeline errors since the release: ' + '; '.join('%s: %s' % kv for kv in sorted(by.items())))
    else:
        lines.append('no pipeline errors since the release')
    for key, values in second.items():
        journal.record_offsets(key[0], key[1], 'after_release', *values)
    for item in expected:
        journal.upsert_pipeline(item['name'], state_after_start=states.get(item['name']))
    for line in lines:
        log(line)
    return ok, lines


def handle_c1(step, ctx):
    """C1 handler: writer watch and count changes on the live database, pipeline and grants checks with the owner, app_writes_seen.

    Pipelines and other accounts' grants are invisible to the runner (section 5), so those two
    checks need the owner profile; without it they are recorded as not checked (a c1_not_checked
    event and the step note) and do not hold the step. With the owner, an application-managed
    pipeline missing from the live database fails the step. The count change compares two samples
    taken around the 60 s offset sample and the journal's verified target counts. C1 passes only
    when this run saw application writes (a foreign session, counts moving between the samples or
    differing from the copy, or app_writes_seen recorded by an earlier C1 attempt of the run) or the
    operator passed step C1 --idle-workload NOTE, which is recorded as an idle_workload event;
    otherwise it fails with exit 4.
    """
    from . import copy as copy_module
    from . import watch
    cap = capture_for(ctx)
    items = cap['items']
    if ctx.dry_run:
        session = ctx.runner_session(steps.guard_for('C1', ctx.db))
        session.query(inventory.PROCESSLIST_SQL, (ctx.db.source, '%' + ctx.db.source + '%'))
        session.query(inventory.PIPELINES_SQL, (ctx.db.source,))
        session.query(inventory.OFFSETS_SQL, (ctx.db.source,))
        return steps.Outcome(steps.PASSED, 'dry run: C1 statements printed')
    released = ctx.journal.get_meta('released_utc')
    if not released:
        return steps.Outcome(steps.REFUSED, 'C1 needs the release time in the journal; run O11 or record its gate first', EXIT_REFUSED)
    data, plans, _ = copy_module.load_plans(ctx)
    sleep = ctx.options.get('sleep') or time.sleep
    runner = ctx.runner_session(steps.guard_for('C1', ctx.db))
    protected = {ctx.config.runner.user} | ({ctx.config.owner.user} if ctx.config.owner else set())
    ok, lines = True, []
    try:
        summary = watch.watch(runner, ctx.db.source, plans, protected, ctx.log, interval=1, tables=10, max_checks=3, sleep=lambda s: None)
        counts1 = watch.count_largest(runner, ctx.db.source, plans, 10)
        if ctx.owner_configured:
            owner = ctx.owner_session(read_guard('owner C1'))
            try:
                ok, lines = after_release_checks(owner, ctx.db.source, items, ctx.journal, ctx.log, sleep=sleep, started_since=released,
                                                 start=starts_at_release(ctx.config))
                snap = grants.snapshot_from_journal(ctx.journal) or grants.reference_from_journal(ctx.journal)
                if snap:
                    diff = grants.compare(owner, ctx.db.source, snap)
                    if diff:
                        ok = False
                        lines.append('grants differ from the snapshot: ' + ', '.join(sorted(diff)))
                    else:
                        lines.append('grants equal the snapshot for %d account(s)' % len(snap))
                else:
                    lines.append('no grants snapshot or reference was taken, so grants are not compared')
            finally:
                owner.close()
        else:
            sleep(60)
        counts2 = watch.count_largest(runner, ctx.db.source, plans, 10)
    finally:
        runner.close()
    journal_counts = {t['table_name']: t.get('target_count') for t in ctx.journal.tables()}
    moved = sorted(n for n in counts2 if counts1.get(n) != counts2[n])
    differs = sorted(n for n in counts2 if journal_counts.get(n) is not None and int(journal_counts[n]) != counts2[n])
    if not ctx.owner_configured:
        lines.append(NOT_CHECKED)
        ctx.journal.event('c1_not_checked', NOT_CHECKED)
        ctx.log(lines[-1])
    seen_now = bool(summary['writers_seen'] or moved or differs or any(summary['count_deltas'].values()))
    seen_before = ctx.journal.get_meta('app_writes_seen')
    writes = seen_now or bool(seen_before)
    if seen_now:
        ctx.journal.set_meta('app_writes_seen', seen_before or iso(utc_now()))
        lines.append('application writes seen on the live database (%s); a rollback is now a new freeze and reconciliation' % (
            ', '.join(filter(None, ['sessions' if summary['writers_seen'] else '', 'counts moved on ' + ', '.join(moved) if moved else '',
                                     'counts differ from the copy on ' + ', '.join(differs) if differs else '']))))
    elif seen_before:
        # an earlier C1 attempt of this run saw them and recorded app_writes_seen; this sample saw none, which does not undo that
        lines.append('no application write in this sample; an earlier C1 attempt of this run saw application writes on the live '
                     'database at %s (app_writes_seen)' % seen_before)
    else:
        lines.append('no application session or count change seen yet on the live database')
    ctx.log(lines[-1])
    if not ok:
        return steps.Outcome(steps.FAILED, 'C1 checks failed: ' + '; '.join(lines), EXIT_GATE)
    idle = str(ctx.options.get('idle_workload') or '').strip()
    if not writes:
        if not idle:
            return steps.Outcome(steps.FAILED, 'C1 saw no application writes on the live database; rerun s2rebuild step C1 once the application '
                                 'writes, or pass s2rebuild step C1 --idle-workload NOTE when the live database is idle on purpose; '
                                 + '; '.join(lines), EXIT_GATE)
        ctx.journal.event('idle_workload', idle)
        lines.append('no application writes seen; idle workload accepted by the operator: ' + idle)
        ctx.log(lines[-1])
    return steps.Outcome(steps.PASSED, 'C1 checks passed: ' + '; '.join(lines))


steps.register('O2', handle_o2)
steps.register('O9', handle_o9)
steps.register('O11', handle_o11)
steps.register('C1', handle_c1)
