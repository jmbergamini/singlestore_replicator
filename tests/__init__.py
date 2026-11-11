"""s2rebuild tests: offline test package; no network and no database connection.

Author: John Bergamini
"""

# The O6 retry round waits RETRY_DELAY_SECONDS of real time after a failure (Helios fact 12); the tests run it at once.
# The retry test sets its own delay on a fake clock.
from s2rebuild import copy as _copy  # noqa: E402

_copy.RETRY_DELAY_SECONDS = 0

# K2 starts caffeinate for run and step on macOS; the tests start no process.
from s2rebuild import cli as _cli  # noqa: E402

_cli.keep_awake = lambda log, dry_run, **kw: None
