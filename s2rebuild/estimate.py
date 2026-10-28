"""s2rebuild estimate: the copy estimate and the plan sheet.

Author: John Bergamini

Section 10.6 of the architecture. The estimate is one line: the rows of the plan divided by the
rows per hour measured over a whole O6 on the workspace of the run (estimate.rows_per_hour).
I5 measured 3.0 to 3.5 B rows per hour while every worker ran and 2.4 B over the whole O6 on an
S-8 at 4X (LESSONS lesson 13); the default is the whole-step figure. No step stops on the
estimate. Statements issued: none.
"""
import dataclasses

from .common import format_duration
from . import projections, routines


@dataclasses.dataclass
class Estimate:
    """The copy estimate of a plan list: rows over the configured rows per hour."""

    tables: int
    rows: int
    size_gib: float
    rows_per_hour: int
    pipelines: int = 0

    @property
    def copy_seconds(self):
        """Expected O6 seconds: rows / rows_per_hour."""
        return self.rows / float(self.rows_per_hour) * 3600.0 if self.rows_per_hour else 0.0


def estimate(plans, config, pipeline_count=0):
    """The Estimate for a plan list under estimate.rows_per_hour."""
    return Estimate(tables=len(plans), rows=sum(p.est_rows for p in plans), size_gib=sum(p.size_gib or 0.0 for p in plans),
                    rows_per_hour=int(config.estimate.rows_per_hour), pipelines=pipeline_count)


def combine(estimates):
    """One Estimate for the databases of a run in sequence: the counts summed, the rate of the first."""
    estimates = list(estimates)
    first = estimates[0]
    return Estimate(tables=sum(e.tables for e in estimates), rows=sum(e.rows for e in estimates), size_gib=sum(e.size_gib for e in estimates),
                    rows_per_hour=first.rows_per_hour, pipelines=sum(e.pipelines for e in estimates))


def estimate_line(est):
    """The one estimate line of P3 and P4."""
    return 'Estimate   copy about %s: %s rows in %d tables, %.0f GiB, at %s rows per hour (estimate.rows_per_hour)' % (
        format_duration(est.copy_seconds), _human_rows(est.rows), est.tables, est.size_gib, _human_rows(est.rows_per_hour))


def _human_rows(rows):
    if rows >= 1e9:
        return '%.2f B' % (rows / 1e9)
    if rows >= 1e6:
        return '%.1f M' % (rows / 1e6)
    return str(rows)


# ---------------------------------------------------------------------------
# Plan sheet (P4, Appendix B)
# ---------------------------------------------------------------------------
def plan_sheet(config, db, run_id, inventory, est, env=None, skipped=None):
    """The plan sheet lines for one database; skipped is the pipeline source types the operator gave no credentials for at P3
    (the journal's pipelines_skipped)."""
    from . import steps
    from .common import iso
    env = {} if env is None else env
    lines = ['%s    run %s    database %s' % ('SingleStore Database Rebuild Kit', run_id, db.source)]
    window = config.window
    if window is not None:
        lines.append('Window     %s to %s (%s); no step stops on it' % (iso(window.start), iso(window.end),
                                                                      format_duration(window.duration.total_seconds())))
    else:
        lines.append('Window     none configured; the copy and the switch run without a time limit')
    lines.append('Freeze     read-only attachment; application stop owner: <name recorded in the P4 gate note>')
    lines.append(estimate_line(est))
    pipelines = (inventory or {}).get('pipelines') or {}
    if pipelines.get('captured'):
        items = pipelines['items']
        by_type = {}
        for item in items:
            by_type[item['source_type']] = by_type.get(item['source_type'], 0) + 1
        routes = {'kit': '1 (kit replays)', 'customer': '2 (customer runs the placeholder file)'}
        creds = []
        for source_type in sorted(by_type):
            var = config.pipelines.credentials_env.get(source_type)
            item = config.pipelines.credentials_keychain.get(source_type)
            creds.append('%s %senv %s' % (source_type, ('Keychain item %s, ' % item) if item else '',
                                           ('set' if var and env.get(var) else 'not set') if var else 'not configured'))
        lines.append('Pipelines  %d captured (%s); route %s; credentials: %s' % (
            len(items), ', '.join('%d %s' % (n, t) for t, n in sorted(by_type.items())), routes[config.pipelines.route], ', '.join(creds)))
        from . import inventory as inventory_module, pipelines as pipelines_module
        left_out = pipelines_module.skipped_names(pipelines_module.mark_skipped([dict(i) for i in items], set(skipped or ())))
        if left_out:
            lines.append('           not recreated at O9 (no credentials given at the P3 prompt; the count gate leaves them out): %s; '
                         'recreate them later from pipelines/%s' % (', '.join('%s (%s)' % s for s in left_out), inventory_module.RECREATE_FILE))
        exposure = est.copy_seconds
        verdicts = []
        for source_type in sorted(by_type):
            hours = config.pipelines.retention_hours.get(source_type)
            if hours is None:
                verdicts.append('%s not stated' % source_type)
            else:
                verdicts.append('%s %s h, margin %s' % (source_type, hours, 'OK' if hours * 3600.0 > exposure else 'SHORT'))
        lines.append('           retention stated by customer: %s; stop to start (the copy estimate): %s; app-managed: %d' % (
            '; '.join(verdicts), format_duration(exposure), sum(1 for i in items if i['app_managed'])))
    else:
        lines.append('Pipelines  not captured (%s)' % pipelines.get('note', 'no inventory'))
    accounts = (inventory or {}).get('accounts') or []
    lines.append('Accounts   write-capable: %s' % (', '.join(a['user'] for a in accounts) or 'none found'))
    refusals = routines.pipeline_target_refusals(config, db, inventory or {})
    lines.append('Routines   %s' % '; '.join([routines.summary(config, db, inventory or {})] + ['REFUSED: ' + r for r in refusals]))
    lines.append('Projections %s (created at O7)' % projections.summary(inventory or {}))
    lines.append('Workspace  answered at O1 with --workspace NOTE: ' + '; '.join(steps.WORKSPACE_QUESTIONS))
    if config.closeout.retain_days < 90:
        lines.append('Retention  closeout.retain_days is %d, under the 90 day default: the retained copy %s may be deleted by the customer '
                     '(s2rebuild cleanup --delete) that soon after the switch' % (config.closeout.retain_days, db.retained))
    return lines
