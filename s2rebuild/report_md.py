"""s2rebuild report_md: the Markdown run report.

Author: John Bergamini

Section 10.16 of the architecture: the Summary as text, the timeline from the journal events
and the evidence index (file paths). Statements issued: none.
"""
from pathlib import Path

from . import report_xlsx, steps


def write_run_report(path, inventory, plans, journal, config, db, run_id):
    """Write run-report.md next to the final workbook; returns the path."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = report_xlsx.final_summary_rows(inventory, plans, journal, config, db, run_id)
    lines = ['# SingleStore Database Rebuild Kit: run report', '', 'Run %s, database %s, customer %s.' % (run_id, db.source, config.customer), '',
             '## Summary', '']
    for label, value in rows:
        if label is None:
            lines.append('')
        elif value is None:
            lines.append('### ' + str(label))
        else:
            lines.append('- %s: %s' % (label, value))
    # the definer per view and routine, as the Views and Routines tabs of the final workbook show them
    for title, header, table_rows in (('Views', report_xlsx.VIEW_HEADER, report_xlsx.view_rows(inventory, journal)),
                                      ('Routines', report_xlsx.ROUTINE_HEADER, report_xlsx.routine_rows(inventory, journal, config, db))):
        lines.extend(['', '## ' + title, ''])
        for row in table_rows:
            lines.append('- ' + '; '.join('%s %s' % (label, value) for label, value in zip(header, row) if value is not None))
    lines.extend(['', '## Timeline', ''])
    for event in journal.events():
        lines.append('- %s %s %s' % (event['ts'], event['kind'], str(event['detail'])[:200]))
    lines.extend(['', '## Evidence', ''])
    board = journal.steps_board()
    for step_id in steps.ORDER:
        row = board.get(step_id)
        if row and row.get('evidence_path'):
            lines.append('- %s: %s' % (step_id, row['evidence_path']))
    run_dir = config.runs_dir() / run_id / db.source
    for rel in ('journal.sqlite', 'inventory', 'reports/profile.xlsx', 'reports/final.xlsx'):
        candidate = run_dir / rel
        if candidate.exists():
            lines.append('- %s' % candidate)
    path.write_text('\n'.join(lines) + '\n', encoding='utf8')
    return path
