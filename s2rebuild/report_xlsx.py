"""s2rebuild report_xlsx: the profile workbook (and the final workbook frame) with openpyxl.

Author: John Bergamini

Section 10.16 and Appendix C of the architecture. Summary tab first, one tab per table, a
Pipelines tab and a Pipeline offsets tab. Sheet names are cut to 31 characters, invalid
characters replaced and made unique with a numeric suffix; the name map sits on the Summary
tab. Numbers are stored as numbers, times as ISO 8601 UTC strings. Statements issued: none.
"""
import datetime as dt
import json
import re
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

from . import estimate, routines, steps, transform, verify
from .common import format_duration, iso

SHEET_NAME_LIMIT = 31
_INVALID = re.compile(r'[\[\]:*?/\\]')
_HEADER = Font(bold=True)
_TITLE = Font(bold=True, size=13)


def sheet_name(name, used):
    """A valid, unique sheet name for a table: invalid characters replaced, cut to 31, numeric suffix on collision."""
    base = _INVALID.sub('_', str(name)).strip("'") or 'sheet'
    base = base[:SHEET_NAME_LIMIT]
    candidate = base
    n = 1
    while candidate.lower() in {u.lower() for u in used}:
        n += 1
        suffix = '~%d' % n
        candidate = base[:SHEET_NAME_LIMIT - len(suffix)] + suffix
    used.add(candidate)
    return candidate


def _rows(ws, rows, start=1, bold_labels=True):
    """Write (label, value) pairs down columns A and B from start; return the next free row."""
    r = start
    for label, value in rows:
        if label is None and value is None:
            r += 1
            continue
        ws.cell(row=r, column=1, value=label)
        if bold_labels and label and value is None:
            ws.cell(row=r, column=1).font = _HEADER
        if value is not None:
            ws.cell(row=r, column=2, value=value)
        r += 1
    return r


def _table(ws, header, rows, start=1):
    """Write a header row (bold) and rows below it; return the next free row."""
    for c, h in enumerate(header, 1):
        cell = ws.cell(row=start, column=c, value=h)
        cell.font = _HEADER
    r = start + 1
    for row in rows:
        for c, value in enumerate(row, 1):
            ws.cell(row=r, column=c, value=value)
        r += 1
    return r


def _widths(ws, widths):
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w


def _key_lists(table):
    primary, unique, shard, sort, other = [], [], [], [], []
    for k in table.get('keys') or []:
        label = '%s (%s)' % (','.join(k['columns']), k.get('type') or 'index')
        kind = str(k.get('type') or '').upper()
        name = str(k.get('name') or '')
        if name.upper() == 'PRIMARY':
            primary.append(','.join(k['columns']))
        elif kind == 'SHARD' or name.upper() in ('__SHARDKEY', 'SHARD'):
            shard.append(','.join(k['columns']))
        elif kind in ('COLUMNSTORE', 'SORT') or name.upper() in ('__UNORDERED', 'SORT'):
            sort.append(','.join(k['columns']))
        elif k.get('unique'):
            unique.append('%s %s' % (name, ','.join(k['columns'])))
        else:
            other.append('%s %s' % (name, label))
    return primary, unique, shard, sort, other


def summary_rows(inventory, plans, est, config, db, run_id, steps_board=None, name_map=None):
    """The (label, value) rows of the Summary tab for the profile workbook."""
    window = config.window
    rows = [('SingleStore Database Rebuild Kit profile', None), (None, None),
            ('Run', run_id), ('Customer', config.customer), ('Database', db.source), ('Replacement', db.replacement),
            ('Retained', db.retained), ('Target layout', '%d partitions, %d sub-partitions' % (db.layout.partitions, db.layout.sub_partitions)),
            ('Captured', inventory.get('captured_utc')), ('Capture step', inventory.get('step'))]
    layout = inventory.get('layout') or {}
    if layout:
        rows.append(('Source layout', '%s partitions, %s sub-partitions, IS_SYNC %s, IS_SYNC_DURABILITY %s' % (
            layout.get('NUM_PARTITIONS'), layout.get('NUM_SUB_PARTITIONS'), layout.get('IS_SYNC'), layout.get('IS_SYNC_DURABILITY'))))
    attachment = inventory.get('attachment') or {}
    if attachment:
        rows.append(('Source attachment', '%s, STORAGE_ID %s' % (attachment.get('ATTACH_TYPE'), attachment.get('STORAGE_ID'))))
    if window is not None:
        rows.append(('Window', '%s to %s (%s)' % (iso(window.start), iso(window.end), format_duration(window.duration.total_seconds()))))
    rows.append(('Freeze', 'read-only attachment'))
    rows.append((None, None))
    rows.append(('Steps', None))
    for step_id, row in (steps_board or {}).items():
        rows.append((step_id, '%s %s %s' % (row.get('status', ''), row.get('started_utc') or '', row.get('ended_utc') or '')))
    rows.append((None, None))
    rows.append(('Totals', None))
    rows.extend([('Tables', est.tables), ('Views', len(inventory.get('views') or [])),
                 ('Routines', len(inventory.get('routines') or [])), ('Estimated rows', est.rows),
                 ('Primary files GiB', round(est.size_gib, 3)), ('Buckets', sum(p.n_buckets for p in plans)),
                 ('Workers', config.copy.workers)])
    rows.append((None, None))
    rows.append(('Estimate', None))
    rows.extend([('Rows per hour (estimate.rows_per_hour)', est.rows_per_hour), ('Copy expected', format_duration(est.copy_seconds))])
    pipelines = inventory.get('pipelines') or {}
    rows.append((None, None))
    rows.append(('Pipelines', None))
    if pipelines.get('captured'):
        items = pipelines['items']
        by_type = {}
        for item in items:
            by_type[item['source_type']] = by_type.get(item['source_type'], 0) + 1
        rows.append(('Captured', '%d (%s)' % (len(items), ', '.join('%d %s' % (n, t) for t, n in sorted(by_type.items())))))
        rows.append(('States', ', '.join('%d %s' % (n, s) for s, n in sorted(_count(i['state'] for i in items).items()))))
        rows.append(('Credential route', '1 (kit replays at O9)' if config.pipelines.route == 'kit' else '2 (customer runs the placeholder file)'))
        for source_type in sorted(by_type):
            rows.append(('Credential variable ' + source_type, config.pipelines.credentials_env.get(source_type) or 'not configured'))
            rows.append(('Credential Keychain item ' + source_type, config.pipelines.credentials_keychain.get(source_type)
                         or 'not configured (the P3 prompt needs one)'))
    else:
        rows.append(('Captured', pipelines.get('note') or 'not captured'))
    rows.append((None, None))
    rows.append(('Accounts', None))
    accounts = inventory.get('accounts') or []
    rows.append(('Write-capable', ', '.join(a['user'] for a in accounts) or 'none found'))
    for a in accounts:
        rows.append((a['user'], '%s on %s' % (', '.join(a['privileges']), ', '.join(a['scopes']))))
    rows.append((None, None))
    rows.append(('Findings', None))
    counters = [p.name for p in plans if _auto(inventory, p.name)]
    computed = [p.name for p in plans if p.computed_columns]
    rows.extend([('Tables with AUTO_INCREMENT counters', ', '.join(counters) or 'none'),
                 ('Reference tables (copied through a staging table, counted exactly)', ', '.join(p.name for p in plans if getattr(p, 'reference', False)) or 'none'),
                 ('Tables with computed columns', ', '.join(computed) or 'none'),
                 ('Tables without a time column (count only at tier 2)', ', '.join(p.name for p in plans if not p.time_column) or 'none'),
                 ('Tables with unique-key lock risk', ', '.join(p.name for p in plans if p.unique_key_lock_risk) or 'none'),
                 ('Tables hashed over all columns (no non-null key)', ', '.join(p.name for p in plans if p.key_kind == 'row') or 'none'),
                 ('Views with a DEFINER', ', '.join(v['name'] for v in inventory.get('views') or [] if v.get('definer')) or 'none'),
                 ('Routines (captured with the owner profile; created at O5 and O7)', routines.summary(config, db, inventory)),
                 ('Routines ignored (objects.routines.ignore)', ', '.join(routines.split(config, db, inventory.get('routines') or [])[1]) or 'none')])
    if pipelines.get('captured'):
        items = pipelines['items']
        rows.extend([('Pipelines the application recreates', ', '.join(i['name'] for i in items if i['app_managed']) or 'none'),
                     ('Pipelines with errors in 24 h', ', '.join(i['name'] for i in items if i['errors_24h']) or 'none'),
                     ('Pipelines with redaction markers', ', '.join(i['name'] for i in items if i['redaction_marker']) or 'none')])
    rows.append(('Grants scope', ', '.join(sorted({s for a in accounts for s in a['scopes']})) or 'none'))
    if name_map:
        rows.append((None, None))
        rows.append(('Sheet names', None))
        for table, sheet in name_map.items():
            if table != sheet:
                rows.append((table, sheet))
    return rows


def _count(values):
    out = {}
    for v in values:
        out[v] = out.get(v, 0) + 1
    return out


def _auto(inventory, name):
    for t in inventory['tables']:
        if t['name'] == name:
            return t.get('auto_increment')
    return None


REFERENCE_TEXT = 'reference table, copied through a staging table'


def copy_path_text(plan):
    """The Copy path row of a table tab: the staging path for a reference table, else INSERT ... SELECT by bucket."""
    return REFERENCE_TEXT if getattr(plan, 'reference', False) else 'INSERT ... SELECT by hash bucket'


def table_rows(table, plan, est):
    """The (label, value) rows of one table tab's top block."""
    minutes = round(plan.est_rows / float(est.rows_per_hour) * 60.0, 1) if est is not None and est.rows_per_hour else None
    primary, unique, shard, sort, other = _key_lists(table)
    auto = table.get('auto_increment') or {}
    rows = [('Table', plan.name), ('Storage type', table.get('storage_type') or 'unknown'), ('Copy path', copy_path_text(plan)),
            ('Key kind', plan.key_kind),
            ('Key columns', ','.join(plan.key_columns) or '(all columns, COALESCE)'), ('Column count', len(plan.columns)),
            ('Rows estimated', plan.est_rows), ('Rows exact', table.get('exact_rows')), ('Size GiB', None if plan.size_gib is None else round(plan.size_gib, 3)),
            (None, None), ('Estimate', None),
            ('Minutes at the run rate', minutes),
            ('Buckets', plan.n_buckets), ('Chunk rows', plan.chunk_rows), ('Rows per chunk', round(plan.est_rows / plan.n_buckets)),
            ('Concurrency limit', plan.concurrency if plan.concurrency else 'auto'), ('Time column', plan.time_column or 'none'),
            (None, None), ('Attributes', None),
            ('Primary key', ', '.join(primary) or 'none'), ('Unique keys', '; '.join(unique) or 'none'),
            ('Shard key', ', '.join(shard) or 'none'), ('Sort key', ', '.join(sort) or 'none'), ('Other indexes', '; '.join(other) or 'none'),
            ('AUTO_INCREMENT column', auto.get('column') or 'none'), ('AUTO_INCREMENT counter', auto.get('counter')),
            ('Computed columns', ', '.join(plan.computed_columns) or 'none'),
            (None, None), ('Callouts', None)]
    callouts = []
    if getattr(plan, 'reference', False):
        callouts.append(REFERENCE_TEXT + ': the row count is an exact COUNT(*), TABLE_STATISTICS reports none')
    if plan.unique_key_lock_risk:
        callouts.append('unique varchar key: chunks on this table may collide on a leaf lock; serialized after the first lock wait')
    if plan.key_kind == 'row':
        callouts.append('no non-null key: hashed over every column with COALESCE')
    if plan.computed_columns:
        callouts.append('computed columns are excluded from the copy column list')
    if auto.get('column'):
        callouts.append('counter set at O7 to the larger of the source counter and MAX plus 1, synced on every aggregator')
    rows.extend(('', c) for c in callouts) if callouts else rows.append(('', 'none'))
    return rows


def write_profile_workbook(path, inventory, plans, est, config, db, run_id, steps_board=None, env=None):
    """Write the profile workbook: Summary, one tab per table, Pipelines, Pipeline offsets. Returns the path.

    env is the operator's environment, read only to say whether a credential variable is set.
    """
    env = {} if env is None else env
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    summary = wb.active
    summary.title = 'Summary'
    used = {'Summary', 'Pipelines', 'Pipeline offsets', 'System', 'History'}
    name_map = {}
    for p in plans:
        name_map[p.name] = sheet_name(p.name, used)
    _rows(summary, summary_rows(inventory, plans, est, config, db, run_id, steps_board, name_map))
    summary.cell(row=1, column=1).font = _TITLE
    _widths(summary, [46, 90])
    by_name = {t['name']: t for t in inventory['tables']}
    for p in plans:
        ws = wb.create_sheet(name_map[p.name])
        nxt = _rows(ws, table_rows(by_name[p.name], p, est))
        nxt += 1
        ws.cell(row=nxt, column=1, value='Definition').font = _HEADER
        for i, line in enumerate(str(p.definition or '').splitlines(), start=nxt + 1):
            ws.cell(row=i, column=1, value=line)
        _widths(ws, [34, 60])
        ws.column_dimensions['B'].alignment = Alignment(wrap_text=True)
    pipelines = inventory.get('pipelines') or {}
    ws = wb.create_sheet('Pipelines')
    header = ['name', 'state at capture', 'source type', 'source location', 'target', 'batch interval', 'duplicate handling',
              'last batch time', 'rows loaded 24 h', 'errors 24 h', 'last error', 'source partitions', 'loaded offsets sum',
              'credential route', 'credential variable', 'variable set', 'application managed', 'redaction marker']
    rows = []
    for item in pipelines.get('items') or []:
        var = config.pipelines.per_pipeline_env.get(item['name']) or config.pipelines.credentials_env.get(item['source_type'])
        rows.append([item['name'], item['state'], item['source_type'], item['source_location'], item['target'], item['batch_interval'],
                     item['duplicate_handling'], item['last_batch_time'], item['rows_24h'], item['errors_24h'],
                     (item.get('last_error') or {}).get('message'), len(item['offsets']),
                     sum(o['loaded'] or 0 for o in item['offsets']), config.pipelines.route, var or 'not configured',
                     ('set' if env.get(var) else 'not set') if var else 'not configured',
                     'yes' if item['app_managed'] else 'no', 'yes' if item['redaction_marker'] else 'no'])
    if not rows:
        rows.append([pipelines.get('note') or 'not captured'] + [None] * (len(header) - 1))
    _table(ws, header, rows)
    _widths(ws, [40, 12, 10, 40, 30, 10, 12, 20, 12, 10, 40, 10, 14, 10, 30, 12, 12, 10])
    ws = wb.create_sheet('Pipeline offsets')
    header = ['pipeline', 'source partition', 'earliest offset', 'latest offset', 'latest loaded offset', 'latest successful offset']
    rows = [[item['name'], o['partition'], o['earliest'], o['latest'], o['loaded'], o['successful']]
            for item in pipelines.get('items') or [] for o in item['offsets']]
    if not rows:
        rows.append(['none captured', None, None, None, None, None])
    _table(ws, header, rows)
    _widths(ws, [40, 14, 14, 14, 18, 20])
    wb.save(str(path))
    return path


def write_system_sheet(path, rows):
    """Add or replace a System tab with one row per database (profile --all-databases). Returns the path."""
    from openpyxl import load_workbook
    path = Path(path)
    wb = load_workbook(str(path))
    if 'System' in wb.sheetnames and wb['System'].cell(row=1, column=1).value == 'database':
        del wb['System']
    ws = wb.create_sheet('System', 1)
    header = ['database', 'partitions', 'sub-partitions', 'tables', 'estimated rows', 'primary files GiB', 'pipelines', 'copy minutes expected']
    _table(ws, header, [[r['database'], r['partitions'], r['sub_partitions'], r['tables'], r['estimated_rows'],
                         None if r['size_gib'] is None else round(r['size_gib'], 3), r['pipelines'], r['copy_minutes_expected']] for r in rows])
    _widths(ws, [40, 10, 12, 8, 16, 16, 10, 20])
    wb.save(str(path))
    return path


# ---------------------------------------------------------------------------
# Final workbook (O8, C1, C2)
# ---------------------------------------------------------------------------
def _parse_ts(text):
    return dt.datetime.strptime(text, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=dt.timezone.utc)


def copy_stats(journal):
    """Per-table and total copy statistics from the journal's chunk rows and events."""
    per_table = {}
    starts, ends = [], []
    for c in journal.chunks():
        entry = per_table.setdefault(c['table_name'], {'buckets': 0, 'done': 0, 'rows': 0, 'worker_seconds': 0.0, 'start': None, 'end': None,
                                                       'attempts': 0, 'lock_waits': 0, 'recopies': 0})
        entry['buckets'] += 1
        entry['attempts'] += int(c['attempts'] or 0)
        if c['state'] == 'done':
            entry['done'] += 1
            entry['rows'] += int(c['rows_inserted'] or 0)
        if c['started_utc'] and c['ended_utc']:
            s, e = _parse_ts(c['started_utc']), _parse_ts(c['ended_utc'])
            entry['worker_seconds'] += (e - s).total_seconds()
            entry['start'] = s if entry['start'] is None or s < entry['start'] else entry['start']
            entry['end'] = e if entry['end'] is None or e > entry['end'] else entry['end']
            starts.append(s)
            ends.append(e)
    for event in journal.events():
        kind, detail = event['kind'], str(event['detail'])
        table = detail.split(' ', 1)[0]
        if table in per_table:
            if kind == 'park':
                per_table[table]['lock_waits'] += 1
            elif kind == 'mark_recopy':
                per_table[table]['recopies'] += 1
    total = {'rows': sum(e['rows'] for e in per_table.values()), 'worker_seconds': sum(e['worker_seconds'] for e in per_table.values()),
             'wall_seconds': (max(ends) - min(starts)).total_seconds() if starts else 0.0,
             'start': iso(min(starts)) if starts else None, 'end': iso(max(ends)) if ends else None}
    return per_table, total


def _latest(results, tier):
    for r in reversed(results):
        if r['tier'] == tier:
            return r
    return None


def latest_results(verifications):
    """{(tier, table): the latest verification row} in journal order: the Summary shows the latest result; the table tab keeps the history."""
    out = {}
    for r in verifications:
        out[(r['tier'], r['table_name'])] = r
    return out


NOT_RECORDED = 'not recorded'


def entry_text(att):
    """One line for a recorded entry: name, STORAGE_ID, ATTACH_TYPE, COMPUTE_ID, how it is attached and its ATTACH_SYNTAX; not recorded where absent."""
    if not att:
        return NOT_RECORDED
    if att.get('is_detached'):
        state = 'detached'
    elif att.get('attach_type'):
        state = 'attached %s' % att['attach_type']
    else:
        state = 'attachment ' + NOT_RECORDED
    return 'name %s, STORAGE_ID %s, ATTACH_TYPE %s, COMPUTE_ID %s (%s); ATTACH_SYNTAX %s' % (
        att.get('database_name'), att.get('storage_id') or NOT_RECORDED, att.get('record_type') or NOT_RECORDED,
        att.get('compute_id') or NOT_RECORDED, state, att.get('attach_syntax') or NOT_RECORDED)


LISTING_EVENTS = (('switch_listing', 'O10', ('before', 'after')), ('endstate_listing', 'C3', ('before', 'after', 'at the refusal')))


def listing_rows(journal):
    """The entries of both storages as O10 and C3 recorded them (switch_listing, endstate_listing), the latest of each moment; not recorded where absent."""
    rows = []
    for kind, step_id, moments in LISTING_EVENTS:
        found = {}
        for detail in journal.events(kind):
            when, _, text = detail.partition(': ')
            found[when] = text
        for when in moments:
            if when in found or ' ' not in when:  # before and after always print; the refusal moment only when C3 refused
                rows.append(('Entries %s %s' % (when, step_id), found.get(when) or NOT_RECORDED))
    return rows


def workspace_rows(journal):
    """The Workspace rows of the final Summary: the answers O1 recorded (--workspace NOTE)."""
    return [('Workspace', None), ('Answered at O1', '; '.join(journal.events('workspace')) or NOT_RECORDED)]


def definer_text(event):
    """The definer an object got in the replacement, from the view or routine event O7 or O5 recorded; not recorded without one."""
    if not event:
        return NOT_RECORDED
    choice = event.get('definer')
    if choice == 'kept':  # postcopy.KEPT, OWNER_DEFINER and FOUND; postcopy imports this module
        return 'original definer kept'
    if choice == 'owner':
        return 'the owner account'
    if choice == 'found':
        return 'found in the replacement: %s' % (event.get('observed_definer') or 'definer ' + NOT_RECORDED)
    return str(choice)


VIEW_HEADER = ['view', 'original definer', 'created in replacement', 'definer in replacement']
ROUTINE_HEADER = ['routine', 'kind', 'original definer', 'created in replacement', 'definer in replacement', 'ignored by configuration']


def _events_by(journal, kind, key):
    out = {}
    for detail in journal.events(kind):
        data = json.loads(detail)
        out[data[key]] = data
    return out


def view_rows(inventory, journal):
    """One row per captured view for the Views tab: the original definer, whether O7 created it and the definer it got."""
    events = _events_by(journal, 'view', 'view')
    rows = []
    for v in inventory.get('views') or []:
        e = events.get(v['name'])
        rows.append([v['name'], v.get('definer') or 'none', ('yes (%s)' % e.get('reason', 'definer removed')) if e else 'no', definer_text(e)])
    return rows or [['none', None, None, None]]


def routine_rows(inventory, journal, config, db):
    """One row per captured routine for the Routines tab: kind, original definer, whether O5 or O7 created it, its definer, ignored or not."""
    events = _events_by(journal, 'routine', 'routine')
    rows = []
    for r in inventory.get('routines') or []:
        kind = r.get('kind') or routines.kind_of(r.get('type'), r.get('definition'))
        left_out = routines.ignored(config, db, r['name'])
        e = events.get(r['name'])
        rows.append([r['name'], kind, r.get('definer') or 'none', ('yes (%s)' % e.get('reason', NOT_RECORDED)) if e else ('no (ignored)' if left_out else 'no'),
                     definer_text(e), 'yes' if left_out else 'no'])
    if not rows:
        note = 'none' if inventory.get('routines_captured') else 'none captured (the owner profile reads routines)'
        rows.append([note, None, None, None, None, None])
    return rows


def final_summary_rows(inventory, plans, journal, config, db, run_id, name_map=None):
    """The (label, value) rows of the final workbook's Summary tab."""
    board = journal.steps_board()
    per_table, total = copy_stats(journal)
    meta = journal.meta()
    window = '%s to %s' % (meta['window_start'], meta.get('window_end', '?')) if meta.get('window_start') else 'none configured'
    rows = [('SingleStore Database Rebuild Kit final report', None), (None, None),
            ('Run', run_id), ('Customer', config.customer), ('Database', db.source), ('Replacement', db.replacement), ('Retained', db.retained),
            ('Freeze', 'read-only attachment'),
            ('Freeze time', meta.get('freeze_time') or 'not recorded'), ('Window', window), (None, None), ('Steps', None)]
    for step_id in steps.ORDER:
        row = board.get(step_id)
        if row:
            rows.append((step_id, '%s %s %s %s' % (row.get('status'), row.get('started_utc') or '', row.get('ended_utc') or '', row.get('note') or '')))
    rows.append((None, None))
    rows.append(('Totals', None))
    size_gib = sum(p.size_gib or 0.0 for p in plans)
    hours = total['wall_seconds'] / 3600.0 if total['wall_seconds'] else 0.0
    rows.extend([('Tables', len(plans)),
                 ('Reference tables (copied through a staging table)', ', '.join(p.name for p in plans if getattr(p, 'reference', False)) or 'none'),
                 ('Rows copied', total['rows']), ('Primary files GiB', round(size_gib, 3)),
                 ('Copy start', total['start'] or 'not run'), ('Copy end', total['end'] or 'not run'),
                 ('Copy wall time', format_duration(total['wall_seconds'])), ('Copy worker seconds', round(total['worker_seconds'])),
                 ('Rows per hour', round(total['rows'] / hours) if hours else 0), ('GiB per hour', round(size_gib / hours, 1) if hours else 0),
                 ('Lock waits', sum(e['lock_waits'] for e in per_table.values())), ('Re-copies', sum(e['recopies'] for e in per_table.values()))])
    est = estimate.estimate(plans, config)
    rows.append((None, None))
    rows.append(('Estimate against actual', None))
    rows.extend([('Copy wall expected', format_duration(est.copy_seconds)), ('Copy wall actual', format_duration(total['wall_seconds'])),
                 ('Rows per hour expected (estimate.rows_per_hour)', est.rows_per_hour),
                 ('Rows per hour actual', round(total['rows'] / hours) if hours else 0)])
    rows.append((None, None))
    rows.append(('Accounts', None))
    accounts = inventory.get('accounts') or []
    rows.append(('Write-capable', ', '.join(a['user'] for a in accounts) or 'none found'))
    for a in accounts:
        rows.append((a['user'], '%s on %s' % (', '.join(a['privileges']), ', '.join(a['scopes']))))
    rows.append((None, None))
    rows.append(('Verification', None))
    # the latest row per tier and table: a match after a mismatch counts as a match; the table tab keeps every row
    latest_rows = latest_results(journal.verifications())
    tier_results = {key: r['result'] for key, r in latest_rows.items()}
    for tier in ('count', 'newest', 'fingerprint'):
        latest = {table: result for (t, table), result in tier_results.items() if t == tier}
        if latest:
            counts = {}
            for v in latest.values():
                counts[v] = counts.get(v, 0) + 1
            rows.append(('Tier %s' % tier, ', '.join('%d %s' % (n, k) for k, n in sorted(counts.items()))))
    rows.append(('Tables without a time column', ', '.join(p.name for p in plans if not p.time_column) or 'none'))
    sampled = sorted({table for (tier, table), r in latest_rows.items() if r['result'] == verify.SAMPLED})
    rows.append(('Sampled tables', ', '.join(sampled) or 'none'))
    rows.append(('Sampled fingerprints (latest, with the predicate)', '; '.join(
        '%s (%s rows, WHERE %s)' % (table, r.get('sample_rows') if r.get('sample_rows') is not None else 'rows ' + NOT_RECORDED,
                                     r.get('predicate') or 'predicate ' + NOT_RECORDED)
        for (tier, table), r in sorted(latest_rows.items()) if tier == 'fingerprint' and r['result'] == verify.SAMPLED) or 'none'))
    skipped = sorted({table for (tier, table), result in tier_results.items() if tier == 'fingerprint' and result == verify.NOT_FINGERPRINTED})
    rows.append(('Not fingerprinted (budget)', ', '.join(skipped) or 'none'))
    verified = [t['verified_at_utc'] for t in journal.tables() if t.get('verified_at_utc')]
    rows.append(('Oldest verified_at', min(verified) if verified else 'none'))
    rows.append((None, None))
    rows.append(('Pipelines', None))
    pipelines = journal.pipelines()
    if pipelines:
        rows.append(('In journal', '%d; started %d' % (len(pipelines), sum(1 for p in pipelines if p.get('started_utc')))))
    captured = (inventory.get('pipelines') or {})
    rows.append(('Captured at P3', '%d' % len(captured.get('items') or []) if captured.get('captured') else (captured.get('note') or 'not captured')))
    from . import inventory as inventory_module, pipelines as pipelines_module
    left_out = pipelines_module.skipped_names(pipelines_module.mark_skipped([dict(i) for i in captured.get('items') or []],
                                                                            pipelines_module.skipped_types(journal)))
    placeholder = None
    for step_id in ('O2', 'O4', 'P3'):
        candidate = inventory_module.inventory_dir(config, run_id, db, step_id) / inventory_module.PIPELINES_DIR / inventory_module.RECREATE_FILE
        if candidate.is_file():
            placeholder = candidate
            break
    rows.append(('Not recreated (no credentials given)', ('%s; recreate from %s' % (', '.join('%s (%s)' % s for s in left_out),
                                                                                    placeholder or 'pipelines/%s of the O2 capture' % inventory_module.RECREATE_FILE))
                 if left_out else 'none'))
    rows.append((None, None))
    rows.append(('Switch', None))
    for role in ('source', 'replacement', 'retained', 'live'):
        att = journal.attachment(role)
        rows.append((role, ('%s STORAGE_ID %s %s; record %s COMPUTE_ID %s%s' % (
            att['database_name'], att.get('storage_id'), att.get('attach_type') or '', att.get('record_type') or 'unknown', att.get('compute_id'),
            ' (not attached)' if att.get('is_detached') else '')) if att else NOT_RECORDED))
    rows.append((None, None))
    # the two entries a run leaves (the live record and the retained record, whole), then the listings O10 and C3 recorded
    rows.append(('End state', None))
    retained = journal.attachment('retained')
    rows.extend([('Live entry', entry_text(journal.attachment('live'))), ('Retained entry', entry_text(retained))])
    rows.extend(listing_rows(journal))
    rows.append((None, None))
    rows.append(('Retention', None))
    sizes = [float(t.get('size_gib') or 0) for t in journal.tables()]
    rows.extend([('Retained copy', db.retained),
                 ('Retained record', ('STORAGE_ID %s, ATTACH_TYPE %s, COMPUTE_ID %s' % (retained.get('storage_id'), retained.get('record_type') or 'unknown',
                                                                                        retained.get('compute_id'))) if retained else NOT_RECORDED),
                 ('Retained size GiB', round(sum(sizes), 3)),
                 ('Switched', meta.get('switched_utc') or NOT_RECORDED),
                 ('Retain until', meta.get('retain_until_utc') or NOT_RECORDED + ' (C2 records it)'),
                 ('Delete later with', 's2rebuild cleanup --delete %s --run %s (the customer, after the date; the kit never deletes it)' % (db.retained, run_id))])
    rows.append((None, None))
    rows.extend(workspace_rows(journal))
    rows.append((None, None))
    rows.append(('Open items', None))
    pending = [s for s, r in board.items() if r.get('status') in ('pending', 'failed', 'running')]
    rows.append(('Steps not passed', ', '.join(pending) or 'none'))
    rows.append(('Counters', ', '.join(sorted({json.loads(e['detail'])['table'] for e in journal.events() if e['kind'] == 'counter'})) or 'none'))
    rows.append((None, None))
    rows.extend(transform.report_rows([transform.apply(p, db.target) for p in plans]))
    if name_map:
        rows.append((None, None))
        rows.append(('Sheet names', None))
        for table, sheet in name_map.items():
            if table != sheet:
                rows.append((table, sheet))
    return rows


def _number(text):
    """An integer when the stored text is one, else the text."""
    try:
        return int(text)
    except (TypeError, ValueError):
        return text


def final_table_rows(table, plan, journal, per_table):
    """The (label, value) rows of one table tab in the final workbook."""
    stats = per_table.get(plan.name, {})
    primary, unique, shard, sort, other = _key_lists(table)
    jt = next((t for t in journal.tables() if t['table_name'] == plan.name), {})
    results = journal.verifications(plan.name)
    t1, t3 = _latest(results, 'count'), _latest(results, 'fingerprint')
    newest = [r for r in results if r['tier'] == 'newest']
    t2 = []
    for r in reversed(newest):
        if any(x['detail'] == r['detail'] for x in t2):
            break
        t2.insert(0, r)
    rows = [('Table', plan.name), ('Storage type', table.get('storage_type') or 'unknown'), ('Copy path', copy_path_text(plan)),
            ('Key kind', plan.key_kind),
            ('Key columns', ','.join(plan.key_columns) or '(all columns, COALESCE)'), ('Column count', len(plan.columns)),
            ('Rows source', jt.get('source_count')), ('Rows target', jt.get('target_count')),
            ('Rows match', 'yes' if jt.get('source_count') is not None and jt.get('source_count') == jt.get('target_count') else ('no' if jt.get('source_count') is not None else 'not counted')),
            ('Size GiB', None if plan.size_gib is None else round(plan.size_gib, 3)),
            (None, None), ('Copy', None),
            ('Buckets', plan.n_buckets), ('Buckets done', stats.get('done', 0)), ('Chunk rows', plan.chunk_rows),
            ('Worker seconds', round(stats.get('worker_seconds', 0.0))), ('Wall start', iso(stats['start']) if stats.get('start') else None),
            ('Wall end', iso(stats['end']) if stats.get('end') else None),
            ('Rows per second', round(stats.get('rows', 0) / stats['worker_seconds']) if stats.get('worker_seconds') else None),
            ('Lock waits', stats.get('lock_waits', 0)), ('Re-copies', stats.get('recopies', 0)), ('Concurrency limit', jt.get('concurrency') or plan.concurrency or 'auto'),
            (None, None), ('Tier 1 count', None),
            ('Result', t1['result'] if t1 else 'not run'), ('Source count', _number(t1['source_value']) if t1 else None), ('Target count', _number(t1['target_value']) if t1 else None),
            (None, None), ('Tier 2 newest', None), ('Time column', plan.time_column or 'none (count only)')]
    for r in t2:
        rows.append((r['detail'] or 'newest', '%s: source %s, target %s' % (r['result'], r['source_value'], r['target_value'])))
    rows.extend([(None, None), ('Tier 3 fingerprint', None),
                 ('Result', t3['result'] if t3 else 'not run'), ('Groups compared', t3['detail'] if t3 else None),
                 ('Sampled or full', ('sampled' if t3['result'] == 'sampled' or 'sampled' in (t3['detail'] or '') else 'full') if t3 else None),
                 ('Sample predicate', (t3.get('predicate') or 'none (full fingerprint)') if t3 else None),
                 ('Rows in sample', t3.get('sample_rows') if t3 else None),
                 ('SQL source side', t3['sql_source'] if t3 else None), ('SQL target side', t3['sql_target'] if t3 else None),
                 ('Verified at', jt.get('verified_at_utc')),
                 (None, None), ('Verification history', None)])
    # every row in journal order (the Summary shows the latest per tier); a sampled row carries its predicate
    for r in results:
        rows.append(('%s %s' % (r['tier'], r['checked_utc']), '%s (epoch %s): source %s, target %s%s' % (
            r['result'], r.get('epoch') if r.get('epoch') is not None else NOT_RECORDED, r['source_value'], r['target_value'],
            ('; WHERE ' + r['predicate']) if r.get('predicate') else '')))
    if not results:
        rows.append(('', 'none'))
    rows.extend([(None, None), ('Attributes', None),
                 ('Primary key', ', '.join(primary) or 'none'), ('Unique keys', '; '.join(unique) or 'none'), ('Shard key', ', '.join(shard) or 'none'),
                 ('Sort key', ', '.join(sort) or 'none'), ('Other indexes', '; '.join(other) or 'none'),
                 ('AUTO_INCREMENT column', (table.get('auto_increment') or {}).get('column') or 'none'),
                 ('Computed columns', ', '.join(plan.computed_columns) or 'none'),
                 (None, None), ('Callouts', None)])
    callouts = []
    for e in journal.events():
        detail = str(e['detail'])
        if e['kind'] == 'manual_load' and detail.startswith(plan.name + ' '):
            callouts.append('fresh-table load: ' + detail)
        elif e['kind'] == 'counter':
            data = json.loads(detail)
            if data.get('table') == plan.name:
                callouts.append('counter: %s (source counter %s, MAX+1 %s, target before %s, after %s)' % (
                    data.get('action'), data.get('source_counter'), data.get('max_plus_one'), data.get('target_before'), data.get('target_after')))
        elif e['kind'] == 'serialized' and detail.startswith(plan.name + ' '):
            callouts.append('serialized after a lock wait')
    if plan.unique_key_lock_risk:
        callouts.append('unique varchar key: lock risk flagged at P3')
    rows.extend(('', c) for c in callouts) if callouts else rows.append(('', 'none'))
    return rows


def write_final_workbook(path, inventory, plans, journal, config, db, run_id, env=None):
    """Write the final workbook: Summary, one tab per table with copy and verification results, Pipelines tabs."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    summary = wb.active
    summary.title = 'Summary'
    used = {'Summary', 'Pipelines', 'Pipeline offsets', 'System', 'Views', 'Routines', 'History'}
    name_map = {p.name: sheet_name(p.name, used) for p in plans}
    _rows(summary, final_summary_rows(inventory, plans, journal, config, db, run_id, name_map))
    summary.cell(row=1, column=1).font = _TITLE
    _widths(summary, [46, 90])
    per_table, _ = copy_stats(journal)
    by_name = {t['name']: t for t in inventory['tables']}
    for p in plans:
        ws = wb.create_sheet(name_map[p.name])
        nxt = _rows(ws, final_table_rows(by_name.get(p.name, {'keys': [], 'columns': []}), p, journal, per_table))
        for r in range(1, nxt):
            if ws.cell(row=r, column=1).value in ('SQL source side', 'SQL target side'):
                ws.cell(row=r, column=2).alignment = Alignment(wrap_text=True, vertical='top')
        nxt += 1
        ws.cell(row=nxt, column=1, value='Definition').font = _HEADER
        for i, line in enumerate(str(p.definition or '').splitlines(), start=nxt + 1):
            ws.cell(row=i, column=1, value=line)
        _widths(ws, [34, 60])
    # the definer choice O7 recorded per view (postcopy.create_views) and O5 or O7 per routine (routines.place)
    ws = wb.create_sheet('Views')
    _table(ws, VIEW_HEADER, view_rows(inventory, journal))
    _widths(ws, [40, 30, 60, 40])
    ws = wb.create_sheet('Routines')
    _table(ws, ROUTINE_HEADER, routine_rows(inventory, journal, config, db))
    _widths(ws, [40, 12, 30, 60, 40, 12])
    pipelines = inventory.get('pipelines') or {}
    journal_pipes = {p['name']: p for p in journal.pipelines()}
    ws = wb.create_sheet('Pipelines')
    env = {} if env is None else env
    header = ['name', 'state at capture', 'source type', 'source location', 'target', 'batch interval', 'duplicate handling',
              'last batch time', 'rows loaded 24 h', 'errors 24 h', 'last error', 'source partitions', 'loaded offsets sum',
              'credential route', 'credential variable', 'variable set', 'application managed', 'redaction marker',
              'created in replacement', 'offsets verified equal', 'started', 'state after start']
    rows = []
    for item in pipelines.get('items') or []:
        jp = journal_pipes.get(item['name'], {})
        var = config.pipelines.per_pipeline_env.get(item['name']) or config.pipelines.credentials_env.get(item['source_type'])
        rows.append([item['name'], item['state'], item['source_type'], item['source_location'], item['target'], item['batch_interval'],
                     item['duplicate_handling'], item['last_batch_time'], item['rows_24h'], item['errors_24h'],
                     (item.get('last_error') or {}).get('message'), len(item['offsets']), sum(o['loaded'] or 0 for o in item['offsets']),
                     config.pipelines.route, var or 'not configured', ('set' if env.get(var) else 'not set') if var else 'not configured',
                     'yes' if item['app_managed'] else 'no', 'yes' if item['redaction_marker'] else 'no',
                     jp.get('created_in_replacement_utc'), 'recorded at O9' if jp.get('created_in_replacement_utc') else None,
                     jp.get('started_utc'), jp.get('state_after_start')])
    if not rows:
        rows.append([pipelines.get('note') or 'not captured'] + [None] * (len(header) - 1))
    _table(ws, header, rows)
    _widths(ws, [40, 12, 10, 40, 30, 10, 12, 20, 12, 10, 40, 10, 14, 10, 30, 12, 12, 10, 20, 14, 20, 16])
    ws = wb.create_sheet('Pipeline offsets')
    header = ['pipeline', 'source partition', 'phase', 'earliest', 'latest', 'loaded', 'successful', 'observed']
    rows = [[item['name'], o['partition'], 'capture', o['earliest'], o['latest'], o['loaded'], o['successful'], inventory.get('captured_utc')]
            for item in pipelines.get('items') or [] for o in item['offsets']]
    for phase in ('before_start', 'after_release'):
        rows.extend([[o['pipeline_name'], o['source_partition'], phase, o['earliest'], o['latest'], o['loaded'], o['successful'], o['observed_utc']]
                     for o in journal.offsets(phase)])
    if not rows:
        rows.append(['none captured'] + [None] * 7)
    _table(ws, header, rows)
    _widths(ws, [40, 14, 12, 12, 12, 12, 12, 20])
    wb.save(str(path))
    return path


def record_retain_until(ctx):
    """C2: retain_until_utc, the switch time plus closeout.retain_days, in the journal meta; returns it, or None before a switch time exists."""
    import datetime as dt
    from .common import iso, parse_utc
    switched = ctx.journal.get_meta('switched_utc')
    if not switched:
        ctx.log('    retain_until_utc not recorded: no switch time in the journal')
        return None
    until = iso(parse_utc(switched, 'switched_utc') + dt.timedelta(days=ctx.config.closeout.retain_days))
    ctx.journal.set_meta('retain_until_utc', until)
    ctx.log('    retained copy %s kept until %s (closeout.retain_days %d); the customer deletes it later with s2rebuild cleanup --delete %s'
            % (ctx.db.retained, until, ctx.config.closeout.retain_days, ctx.db.retained))
    return until


def handle_c2(step, ctx):
    """C2 handler: retain_until_utc, then the final workbook and the Markdown run report."""
    from . import copy as copy_module, report_md
    data, plans, _ = copy_module.load_plans(ctx)
    reports = ctx.config.runs_dir() / ctx.run_id / ctx.db.source / 'reports'
    if ctx.dry_run:
        return steps.Outcome(steps.PASSED, 'dry run: final.xlsx and run-report.md would be written to %s' % reports)
    record_retain_until(ctx)
    xlsx = write_final_workbook(reports / 'final.xlsx', data, plans, ctx.journal, ctx.config, ctx.db, ctx.run_id, ctx.options.get('env') or {})
    md = report_md.write_run_report(reports / 'run-report.md', data, plans, ctx.journal, ctx.config, ctx.db, ctx.run_id)
    ctx.log('final workbook %s and run report %s written' % (xlsx, md))
    return steps.Outcome(steps.PASSED, 'reports written')


steps.register('C2', handle_c2)
