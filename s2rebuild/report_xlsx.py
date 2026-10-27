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

from . import estimate, steps, transform, verify
from .common import KitError, format_duration, iso

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


def summary_rows(inventory, plans, est, fit, config, db, run_id, baseline, steps_board=None, name_map=None):
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
    rows.append(('Freeze path', 'A read-only attachment' if config.freeze.mode == 'readonly' else 'B lockout'))
    rows.append((None, None))
    rows.append(('Steps', None))
    for step_id, row in (steps_board or {}).items():
        rows.append((step_id, '%s %s %s' % (row.get('status', ''), row.get('started_utc') or '', row.get('ended_utc') or '')))
    rows.append((None, None))
    rows.append(('Totals', None))
    rows.extend([('Tables', est.tables), ('Views', len(inventory.get('views') or [])),
                 ('Routines', len(inventory.get('routines') or [])), ('Estimated rows', est.rows),
                 ('Primary files GiB', round(est.size_gib, 3)), ('Buckets', sum(p.n_buckets for p in plans)),
                 ('Workers', est.workers)])
    low, expected, high = est.copy_band
    tl, te, th = est.total_band
    rows.append((None, None))
    rows.append(('Estimate', None))
    rows.extend([('Baseline', '%s (scan %.3e s/row, insert %.3e s/row, overhead %.1f s, %d workers, %s)' % (
                  baseline.source, baseline.scan_s_per_row, baseline.insert_s_per_row, baseline.chunk_overhead_s, baseline.workers, baseline.compute_size)),
                 ('Copy wall expected', format_duration(expected)), ('Copy wall low', format_duration(low)), ('Copy wall high', format_duration(high)),
                 ('Copy worker seconds', round(est.schedule.worker_seconds)), ('Longest chunk', format_duration(est.schedule.longest_chunk_seconds)),
                 ('Modeled rows per hour', round(est.rows_per_hour)),
                 ('Modeled GiB per hour', round(est.size_gib / est.copy_seconds * 3600, 1) if est.copy_seconds else 0),
                 ('Total O2 to O11 expected', format_duration(te)),
                 ('Total O2 to O11 low', format_duration(tl)), ('Total O2 to O11 high', format_duration(th))])
    for label, seconds in est.allowances:
        rows.append(('Allowance ' + label, format_duration(seconds)))
    if fit is not None:
        rows.append((None, None))
        rows.append(('Window fit', None))
        rows.extend([('Abort deadline', iso(fit.deadline)), ('Copy end expected', iso(fit.copy_end[1])), ('Copy end high', iso(fit.copy_end[2])),
                     ('O11 end expected', iso(fit.total_end[1])), ('O11 end high', iso(fit.total_end[2])),
                     ('Verdict', 'FITS' if fit.fits else 'DOES NOT FIT'),
                     ('Spare at high', ('%s %s' % (format_duration(abs(fit.spare_seconds)), 'spare' if fit.spare_seconds >= 0 else 'short')))])
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
    else:
        rows.append(('Captured', pipelines.get('note') or 'not captured'))
    rows.append((None, None))
    rows.append(('Accounts', None))
    accounts = inventory.get('accounts') or []
    rows.append(('Write-capable', ', '.join(a['user'] for a in accounts) or 'none found'))
    for a in accounts:
        rows.append((a['user'], '%s on %s' % (', '.join(a['privileges']), ', '.join(a['scopes']))))
    rows.append(('Path B action', ('REVOKE the write privileges above from every listed account on every database, restore at O11'
                                   if config.freeze.mode == 'lockout' else 'none (Path A read-only attachment)')))
    rows.append((None, None))
    rows.append(('Findings', None))
    counters = [p.name for p in plans if _auto(inventory, p.name)]
    computed = [p.name for p in plans if p.computed_columns]
    rows.extend([('Tables with AUTO_INCREMENT counters', ', '.join(counters) or 'none'),
                 ('Tables with computed columns', ', '.join(computed) or 'none'),
                 ('Tables without a time column (count only at tier 2)', ', '.join(p.name for p in plans if not p.time_column) or 'none'),
                 ('Tables with unique-key lock risk', ', '.join(p.name for p in plans if p.unique_key_lock_risk) or 'none'),
                 ('Tables hashed over all columns (no non-null key)', ', '.join(p.name for p in plans if p.key_kind == 'row') or 'none'),
                 ('Views with a DEFINER', ', '.join(v['name'] for v in inventory.get('views') or [] if v.get('definer')) or 'none'),
                 ('Routines not migrated (objects.routines.ignore)',
                  ', '.join('%s %s' % (str(r['type']).lower(), r['name']) for r in inventory.get('routines') or []) or 'none')])
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


def table_rows(table, plan, est):
    """The (label, value) rows of one table tab's top block."""
    low, expected, high = est.table_minutes(plan.name) if est is not None else (None, None, None)
    primary, unique, shard, sort, other = _key_lists(table)
    auto = table.get('auto_increment') or {}
    rows = [('Table', plan.name), ('Storage type', table.get('storage_type') or 'unknown'), ('Key kind', plan.key_kind),
            ('Key columns', ','.join(plan.key_columns) or '(all columns, COALESCE)'), ('Column count', len(plan.columns)),
            ('Rows estimated', plan.est_rows), ('Rows exact', table.get('exact_rows')), ('Size GiB', None if plan.size_gib is None else round(plan.size_gib, 3)),
            (None, None), ('Estimate', None),
            ('Minutes low', low), ('Minutes expected', expected), ('Minutes high', high),
            ('Buckets', plan.n_buckets), ('Chunk rows', plan.chunk_rows), ('Rows per chunk', round(plan.est_rows / plan.n_buckets)),
            ('Worker seconds', round(est.schedule.per_table.get(plan.name, (0, 0, 0))[2]) if est is not None else None),
            ('Concurrency limit', plan.concurrency if plan.concurrency else 'auto'), ('Time column', plan.time_column or 'none'),
            (None, None), ('Attributes', None),
            ('Primary key', ', '.join(primary) or 'none'), ('Unique keys', '; '.join(unique) or 'none'),
            ('Shard key', ', '.join(shard) or 'none'), ('Sort key', ', '.join(sort) or 'none'), ('Other indexes', '; '.join(other) or 'none'),
            ('AUTO_INCREMENT column', auto.get('column') or 'none'), ('AUTO_INCREMENT counter', auto.get('counter')),
            ('Computed columns', ', '.join(plan.computed_columns) or 'none'),
            (None, None), ('Callouts', None)]
    callouts = []
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


def write_profile_workbook(path, inventory, plans, est, fit, config, db, run_id, baseline, steps_board=None, env=None):
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
    _rows(summary, summary_rows(inventory, plans, est, fit, config, db, run_id, baseline, steps_board, name_map))
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


def final_summary_rows(inventory, plans, journal, config, db, run_id, name_map=None, baseline=None, calibration=None):
    """The (label, value) rows of the final workbook's Summary tab; calibration, when given, is the C2 Calibration line."""
    board = journal.steps_board()
    per_table, total = copy_stats(journal)
    meta = journal.meta()
    window = '%s to %s' % (meta['window_start'], meta.get('window_end', '?')) if meta.get('window_start') else 'none configured'
    rows = [('SingleStore Database Rebuild Kit final report', None), (None, None),
            ('Run', run_id), ('Customer', config.customer), ('Database', db.source), ('Replacement', db.replacement), ('Retained', db.retained),
            ('Freeze path', 'A read-only attachment' if config.freeze.mode == 'readonly' else 'B lockout'),
            ('Freeze time', meta.get('freeze_time') or 'not recorded'), ('Window', window),
            ('Abort deadline', meta.get('abort_deadline') or 'none'), (None, None), ('Steps', None)]
    for step_id in steps.ORDER:
        row = board.get(step_id)
        if row:
            rows.append((step_id, '%s %s %s %s' % (row.get('status'), row.get('started_utc') or '', row.get('ended_utc') or '', row.get('note') or '')))
    rows.append((None, None))
    rows.append(('Totals', None))
    size_gib = sum(p.size_gib or 0.0 for p in plans)
    hours = total['wall_seconds'] / 3600.0 if total['wall_seconds'] else 0.0
    rows.extend([('Tables', len(plans)), ('Rows copied', total['rows']), ('Primary files GiB', round(size_gib, 3)),
                 ('Copy start', total['start'] or 'not run'), ('Copy end', total['end'] or 'not run'),
                 ('Copy wall time', format_duration(total['wall_seconds'])), ('Copy worker seconds', round(total['worker_seconds'])),
                 ('Rows per hour', round(total['rows'] / hours) if hours else 0), ('GiB per hour', round(size_gib / hours, 1) if hours else 0),
                 ('Lock waits', sum(e['lock_waits'] for e in per_table.values())), ('Re-copies', sum(e['recopies'] for e in per_table.values()))])
    est = estimate.estimate(plans, config, baseline or estimate.DEFAULT_BASELINE)
    rows.append((None, None))
    rows.append(('Estimate against actual', None))
    rows.extend([('Copy wall expected', format_duration(est.copy_seconds)), ('Copy wall actual', format_duration(total['wall_seconds'])),
                 ('Rows per hour expected', round(est.rows_per_hour)), ('Rows per hour actual', round(total['rows'] / hours) if hours else 0),
                 ('GiB per hour expected', round(size_gib / est.copy_seconds * 3600, 1) if est.copy_seconds else 0),
                 ('GiB per hour actual', round(size_gib / hours, 1) if hours else 0),
                 ('Worker seconds expected', round(est.schedule.worker_seconds)), ('Worker seconds actual', round(total['worker_seconds']))])
    if calibration:
        rows.append(('Calibration', calibration))
    rows.append((None, None))
    rows.append(('Accounts', None))
    accounts = inventory.get('accounts') or []
    rows.append(('Write-capable', ', '.join(a['user'] for a in accounts) or 'none found'))
    for a in accounts:
        rows.append((a['user'], '%s on %s' % (', '.join(a['privileges']), ', '.join(a['scopes']))))
    rows.append(('Path B action', ('REVOKE the write privileges above from every listed account on every database, restore at O11'
                                   if config.freeze.mode == 'lockout' else 'none (Path A read-only attachment)')))
    rows.append((None, None))
    rows.append(('Verification', None))
    verifications = journal.verifications()
    tier_results = {}
    for r in verifications:
        key = (r['tier'], r['table_name'])
        current = tier_results.get(key)
        if current is None or verify.RESULT_PRECEDENCE.index(r['result']) < verify.RESULT_PRECEDENCE.index(current) \
                or r['result'] == verify.MATCH and current == verify.MATCH:
            tier_results[key] = r['result']
    for tier in ('count', 'newest', 'fingerprint'):
        latest = {table: result for (t, table), result in tier_results.items() if t == tier}
        if latest:
            counts = {}
            for v in latest.values():
                counts[v] = counts.get(v, 0) + 1
            rows.append(('Tier %s' % tier, ', '.join('%d %s' % (n, k) for k, n in sorted(counts.items()))))
    rows.append(('Tables without a time column', ', '.join(p.name for p in plans if not p.time_column) or 'none'))
    sampled = sorted({r['table_name'] for r in verifications if r['result'] == 'sampled'})
    rows.append(('Sampled tables', ', '.join(sampled) or 'none'))
    verified = [t['verified_at_utc'] for t in journal.tables() if t.get('verified_at_utc')]
    rows.append(('Oldest verified_at', min(verified) if verified else 'none'))
    rows.append((None, None))
    rows.append(('Pipelines', None))
    pipelines = journal.pipelines()
    if pipelines:
        rows.append(('In journal', '%d; started %d' % (len(pipelines), sum(1 for p in pipelines if p.get('started_utc')))))
    captured = (inventory.get('pipelines') or {})
    rows.append(('Captured at P3', '%d' % len(captured.get('items') or []) if captured.get('captured') else (captured.get('note') or 'not captured')))
    rows.append((None, None))
    rows.append(('Switch', None))
    for role in ('source', 'replacement', 'retained', 'live'):
        att = journal.attachment(role)
        rows.append((role, ('%s STORAGE_ID %s %s' % (att['database_name'], att.get('storage_id'), att.get('attach_type') or '')) if att else 'not recorded'))
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
    rows = [('Table', plan.name), ('Storage type', table.get('storage_type') or 'unknown'), ('Key kind', plan.key_kind),
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
                 ('SQL source side', t3['sql_source'] if t3 else None), ('SQL target side', t3['sql_target'] if t3 else None),
                 ('Verified at', jt.get('verified_at_utc')),
                 (None, None), ('Attributes', None),
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


def write_final_workbook(path, inventory, plans, journal, config, db, run_id, baseline=None, env=None, calibration=None):
    """Write the final workbook: Summary, one tab per table with copy and verification results, Pipelines tabs."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    summary = wb.active
    summary.title = 'Summary'
    used = {'Summary', 'Pipelines', 'Pipeline offsets', 'System', 'Views', 'History'}
    name_map = {p.name: sheet_name(p.name, used) for p in plans}
    _rows(summary, final_summary_rows(inventory, plans, journal, config, db, run_id, name_map, baseline, calibration))
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
    ws = wb.create_sheet('Views')
    view_events = {}
    for e in journal.events():
        if e['kind'] == 'view':
            data = json.loads(e['detail'])
            view_events[data['view']] = data
    # the definer choice O7 recorded per view (postcopy.create_views); an older journal recorded only the removal
    _table(ws, ['view', 'original definer', 'created in replacement'],
           [[v['name'], v.get('definer') or 'none', ('yes (%s)' % view_events[v['name']].get('reason', 'definer removed'))
             if v['name'] in view_events else 'no'] for v in inventory.get('views') or []] or [['none', None, None]])
    _widths(ws, [40, 30, 60])
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


def handle_c2(step, ctx):
    """C2 handler: the baseline fitted from the run journal, the final workbook and the Markdown run report, then the fitted baseline written.

    Calibration is advisory. When the journal cannot be fitted (too few first-pass chunks, or any
    other calibrate refusal), the baseline in use stays, the Summary of both reports carries a
    Calibration line that says so, and the step passes with that note. calibrate only reads the
    journal, so it runs before the reports; the baseline file is the last write.
    """
    from . import copy as copy_module, estimate, report_md
    data, plans, _ = copy_module.load_plans(ctx)
    reports = ctx.config.runs_dir() / ctx.run_id / ctx.db.source / 'reports'
    if ctx.dry_run:
        return steps.Outcome(steps.PASSED, 'dry run: final.xlsx and run-report.md would be written to %s and the baseline recalibrated' % reports)
    baseline = copy_module.baseline_for(ctx.config)
    try:
        cal, kept = estimate.calibrate(ctx.journal_path, first_pass_only=True, workers=ctx.config.copy.workers,
                                       compute_size=ctx.config.workspace.compute_size, run_id=ctx.run_id), None
    except KitError as exc:
        cal, kept = None, 'baseline not recalibrated (%s); the baseline in use stays: %s' % (
            exc, 'the shipped default' if baseline.source == 'default' else baseline.source)
    xlsx = write_final_workbook(reports / 'final.xlsx', data, plans, ctx.journal, ctx.config, ctx.db, ctx.run_id, baseline, ctx.options.get('env') or {},
                                calibration=kept)
    md = report_md.write_run_report(reports / 'run-report.md', data, plans, ctx.journal, ctx.config, ctx.db, ctx.run_id, calibration=kept)
    ctx.log('final workbook %s and run report %s written' % (xlsx, md))
    if cal is None:
        ctx.log('    ' + kept)
        return steps.Outcome(steps.PASSED, 'reports written; ' + kept)
    size = sum(float(t.get('size_gib') or 0) for t in ctx.journal.tables()) or None
    for line in estimate.calibration_lines(cal, size):
        ctx.log('    ' + line)
    if not ctx.config.estimate.baseline:
        return steps.Outcome(steps.PASSED, 'reports written; estimate.baseline is null, so the fitted baseline was printed, not written')
    path = estimate.write_baseline(ctx.config.resolve(ctx.config.estimate.baseline), cal.baseline)
    return steps.Outcome(steps.PASSED, 'reports written; baseline recalibrated and written to %s' % path)


steps.register('C2', handle_c2)
