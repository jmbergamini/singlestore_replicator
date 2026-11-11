"""s2rebuild common: shared errors, exit codes and UTC time helpers.

Author: John Bergamini

No statements are issued by this module.
"""
import datetime as dt
import re

EXIT_DONE = 0
EXIT_REFUSED = 1
EXIT_INCOMPLETE = 2
EXIT_MISMATCH = 3
EXIT_GATE = 4

RUN_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$')


class KitError(Exception):
    """A refusal, a configuration error or a stop condition. The message is safe to print."""

    exit_code = EXIT_REFUSED


class GatePending(KitError):
    """A step needs a recorded gate before the run can continue."""

    exit_code = EXIT_GATE


def utc_now():
    """Return the current time as an aware UTC datetime."""
    return dt.datetime.now(dt.timezone.utc)


def iso(ts):
    """Format an aware datetime as ISO 8601 UTC with a Z suffix and second precision."""
    return ts.astimezone(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def parse_utc(value, label='time'):
    """Parse an ISO 8601 time with an explicit offset or Z into an aware UTC datetime.

    Accepts a datetime as well. A naive value is refused because a deadline without a
    timezone is ambiguous. Raises KitError with the label in the message.
    """
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            raise KitError('%s must carry a timezone; write it as 2026-11-01T05:00:00Z' % label)
        return value.astimezone(dt.timezone.utc)
    if not isinstance(value, str):
        raise KitError('%s must be an ISO 8601 UTC time such as 2026-11-01T05:00:00Z' % label)
    text = value.strip()
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        raise KitError('%s must be an ISO 8601 UTC time such as 2026-11-01T05:00:00Z' % label)
    if parsed.tzinfo is None:
        raise KitError('%s must carry an explicit UTC offset or Z' % label)
    return parsed.astimezone(dt.timezone.utc)


def new_run_id(now=None):
    """Build a run id from the current UTC time in the form YYYYMMDD-HHMM."""
    now = now or utc_now()
    return now.astimezone(dt.timezone.utc).strftime('%Y%m%d-%H%M')


def check_run_id(run_id):
    """Validate a run id: letters, digits, underscore and hyphen, 1 to 64 characters; never dry-run."""
    if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
        raise KitError('Run id must be 1 to 64 letters, digits, underscores or hyphens')
    if run_id == 'dry-run':
        raise KitError('dry-run is reserved for --dry-run and cannot name a live run')
    return run_id


def unique_run_id(existing, now=None):
    """A new run id of the form YYYYMMDD-HHMM that is not in existing; adds -2, -3 when it is."""
    base = new_run_id(now)
    candidate = base
    n = 1
    while candidate in set(existing):
        n += 1
        candidate = '%s-%d' % (base, n)
    return candidate


def run_date(run_id):
    """Return the date encoded in a run id (YYYYMMDD prefix), or None when it has none."""
    try:
        return dt.datetime.strptime(run_id[:8], '%Y%m%d').date()
    except (ValueError, TypeError):
        return None


def format_duration(seconds):
    """Format a number of seconds as 'H h MM m' for the plan sheet and progress lines."""
    seconds = max(0, int(round(seconds)))
    hours, rest = divmod(seconds, 3600)
    minutes = rest // 60
    if hours:
        return '%d h %02d m' % (hours, minutes)
    return '%d m' % minutes


AUTO_TABLE_SHARE_ABOVE = 8


def auto_table_limit(workers):
    """The most chunks of one table in flight under copy.table_concurrency auto: half the workers above 8 workers, else all.

    On S-8 at 4X (October 27, 2026) 12 workers on the last store_sales chunks copied no faster
    than 4, so above 8 workers no table takes more than half of them and the rest copy the next
    tables. A table serialized after a lock wait, or with its own concurrency, keeps that limit.
    """
    workers = max(1, int(workers))
    return workers // 2 if workers > AUTO_TABLE_SHARE_ABOVE else workers
