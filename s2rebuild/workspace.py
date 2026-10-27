"""s2rebuild workspace: the Management API read of the workspace state and settings (P2) and the O1 guard.

Author: John Bergamini

Run 20261026-1301 lost 28 chunks to autoscale replacing nodes, and on October 25 the workspace
suspended while loads ran with no connection open. P2 reads the workspace through the
Management API when workspace.management_api_key_keychain names a Keychain item: the state
(refused unless ACTIVE), the auto-suspend setting and the autoscale setting go into the
preflight record and onto the plan sheet. Without a key P2 prints three questions and O1
records the operator's answer (--workspace NOTE) as evidence of the go. O1 refuses while
autoscale is reported on unless --accept-autoscale NOTE records the override. The API field
names (GET /v1/workspaces/<id>: state, autoSuspend, autoScale) are a best guess the live
ladder confirms (I4); the raw fields are recorded so a wrong guess shows in the journal. The
client is urllib, no new dependency; tests pass a fake transport and never reach the network.
The key lives in memory only and joins the log redactor. Statements issued: none.
"""
import json
import urllib.error
import urllib.request

from .common import KitError

API_BASE = 'https://api.singlestore.com'
WORKSPACE_PATH = '/v1/workspaces/%s'
TIMEOUT_SECONDS = 30
ACTIVE = 'ACTIVE'
# The field names of the workspace object, a best guess to confirm in I4 (LESSONS fact 41).
STATE_FIELD, AUTO_SUSPEND_FIELD, AUTOSCALE_FIELD = 'state', 'autoSuspend', 'autoScale'
OFF_WORDS = ('OFF', 'DISABLED', 'NONE', 'FALSE', 'NO', '0')
QUESTIONS = (
    'is the workspace ACTIVE (not suspended, resuming or scaling)?',
    'is autoscale off for the window?',
    'is auto-suspend off for the window, or a keep-alive job in place between the commands?',
)
ANSWER_HINT = 'record the answers at O1 with --workspace NOTE'


def api_configured(config):
    """True when workspace.management_api_key_keychain names a Keychain item."""
    return bool(config.workspace.get('management_api_key_keychain'))


def default_transport(request):
    """Send a urllib Request and return (status, body text). Replaced by a fake in the tests."""
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return response.status, response.read().decode('utf8', 'replace')


class Client:
    """A Management API reader: GET only, the key as a bearer token, errors without the key."""

    def __init__(self, key, base=API_BASE, transport=None):
        if not key:
            raise KitError('The Management API key is empty')
        self._key = key
        self.base = base.rstrip('/')
        self.transport = transport or default_transport

    def __repr__(self):
        return 'Client(<redacted>)'

    def get(self, path):
        """GET base + path and return the JSON object; KitError names the status, never the key."""
        request = urllib.request.Request(self.base + path, method='GET',
                                         headers={'Authorization': 'Bearer ' + self._key, 'Accept': 'application/json'})
        try:
            status, body = self.transport(request)
        except urllib.error.HTTPError as exc:
            raise KitError('Management API GET %s answered %s; check the key, the workspace id and the key\'s organization' % (path, exc.code))
        except (urllib.error.URLError, OSError) as exc:
            raise KitError('Management API GET %s failed: %s; check the network, or leave workspace.management_api_key_keychain unset '
                           'and answer the three workspace questions at O1' % (path, getattr(exc, 'reason', None) or type(exc).__name__))
        if int(status) != 200:
            raise KitError('Management API GET %s answered %s; check the key, the workspace id and the key\'s organization' % (path, status))
        try:
            data = json.loads(body)
        except ValueError:
            raise KitError('Management API GET %s did not answer JSON' % path)
        if not isinstance(data, dict):
            raise KitError('Management API GET %s answered a %s, not an object' % (path, type(data).__name__))
        return data


def setting_on(value):
    """True, False or None (not reported) for an API setting in any of the shapes the guess allows.

    A bool stands as it is; a number is on above 1 (a scale factor); a string is off when it is
    one of OFF_WORDS; a mapping is off when enabled is false, suspendType is DISABLED, or
    maxScaleFactor is at most 1, else on; an empty mapping is off.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 1
    if isinstance(value, str):
        return value.strip().upper() not in OFF_WORDS
    if isinstance(value, dict):
        if 'enabled' in value:
            return bool(value['enabled'])
        if 'suspendType' in value:
            return str(value['suspendType']).upper() not in OFF_WORDS
        if 'maxScaleFactor' in value:
            try:
                return float(value['maxScaleFactor']) > 1
            except (TypeError, ValueError):
                return True
        return bool(value)
    return bool(value)


def describe(value):
    """'on', 'off' or 'not reported', with the raw field in brackets when it is a mapping."""
    on = setting_on(value)
    if on is None:
        return 'not reported'
    text = 'on' if on else 'off'
    if isinstance(value, dict) and value:
        text += ' (%s)' % ', '.join('%s=%s' % (k, v) for k, v in sorted(value.items()))
    return text


def read_workspace(config, keychain=None, transport=None, redactor=None):
    """The workspace facts through the Management API: state, autoscale, auto_suspend, the raw fields; the key joins redactor.

    Needs workspace.id and the Keychain item workspace.management_api_key_keychain. keychain is
    connect.read_keychain unless a test injects one; transport replaces the network in tests.
    """
    from .connect import read_keychain
    item = config.workspace.get('management_api_key_keychain')
    workspace_id = config.workspace.get('id')
    if not item:
        raise KitError('workspace.management_api_key_keychain is not set')
    if not workspace_id:
        raise KitError('workspace.id is not set; the Management API read needs the workspace id (Portal, workspace settings), or leave '
                       'workspace.management_api_key_keychain unset and answer the three workspace questions at O1')
    key = (keychain or read_keychain)(item, None)
    if redactor is not None:
        redactor.extend([key])
    data = Client(key, transport=transport).get(WORKSPACE_PATH % workspace_id)
    fields = {name: data.get(name) for name in (STATE_FIELD, AUTO_SUSPEND_FIELD, AUTOSCALE_FIELD)}
    state = str(fields[STATE_FIELD]).strip().upper() if fields[STATE_FIELD] is not None else None
    return {'source': 'api', 'workspace_id': workspace_id, 'state': state, 'autoscale': setting_on(fields[AUTOSCALE_FIELD]),
            'auto_suspend': setting_on(fields[AUTO_SUSPEND_FIELD]), 'fields': fields}


def preflight_lines(facts):
    """The P2 lines for API facts: the state (FAILED unless ACTIVE), autoscale and auto-suspend as read."""
    fields = facts.get('fields') or {}
    state = facts.get('state')
    lines = []
    if state == ACTIVE:
        lines.append('workspace state: ACTIVE (Management API)')
    else:
        lines.append('workspace state: FAILED, %s is not ACTIVE (Management API); resume the workspace or wait for it to settle, then run '
                     's2rebuild check again' % (state or 'not reported'))
    lines.append('workspace autoscale: %s; O1 refuses while it is on unless --accept-autoscale NOTE records the override' % (
        describe(fields.get(AUTOSCALE_FIELD))))
    lines.append('workspace auto-suspend: %s; the keep-alive thread covers a running step, not the waits between commands (STEPS P4)' % (
        describe(fields.get(AUTO_SUSPEND_FIELD))))
    return lines


def question_lines():
    """The P2 lines without an API key: the three questions and where the answer is recorded."""
    lines = ['workspace: no Management API key configured (workspace.management_api_key_keychain); answer before the window:']
    lines.extend('  %d. %s' % (i, q) for i, q in enumerate(QUESTIONS, 1))
    lines.append('  ' + ANSWER_HINT)
    return lines


def recorded(journal):
    """The workspace facts P2 recorded in the journal's preflight record, or None."""
    text = journal.get_meta('preflight') if journal is not None else None
    if not text:
        return None
    try:
        facts = json.loads(text).get('workspace')
    except (ValueError, AttributeError):
        return None
    return facts if isinstance(facts, dict) else None


def plan_lines(facts):
    """The plan sheet line(s) for the workspace: the API facts, or the questions when P2 asked them, or nothing recorded."""
    if not facts:
        return ['Workspace  state, autoscale and auto-suspend not recorded; run s2rebuild check (P2)']
    if facts.get('source') == 'api':
        fields = facts.get('fields') or {}
        return ['Workspace  state %s; autoscale %s; auto-suspend %s (Management API at P2)' % (
            facts.get('state') or 'not reported', describe(fields.get(AUTOSCALE_FIELD)), describe(fields.get(AUTO_SUSPEND_FIELD)))]
    return ['Workspace  not read (no Management API key); the three questions are answered at O1 with --workspace NOTE: ' + '; '.join(QUESTIONS)]


def o1_guard(ctx, facts):
    """The O1 lines, problems and events for the workspace: autoscale on refuses unless --accept-autoscale NOTE; --workspace NOTE is evidence.

    Returns (lines, problems, events) where events are (kind, detail) pairs recorded with the go.
    """
    lines, problems, events = [], [], []
    answer = str(ctx.options.get('workspace') or '').strip()
    accept = str(ctx.options.get('accept_autoscale') or '').strip()
    if facts and facts.get('source') == 'api':
        fields = facts.get('fields') or {}
        lines.append('workspace: state %s, autoscale %s, auto-suspend %s (Management API at P2)' % (
            facts.get('state') or 'not reported', describe(fields.get(AUTOSCALE_FIELD)), describe(fields.get(AUTO_SUSPEND_FIELD))))
        if facts.get('autoscale') is True:
            if accept:
                lines.append('autoscale is on; accepted with --accept-autoscale: ' + accept)
                events.append(('accept_autoscale', 'O1 with autoscale on: ' + accept))
            else:
                problems.append('autoscale is on (Management API at P2); every chunk in flight fails with error 1997 when a node is replaced. '
                                'Turn it off for the window and run s2rebuild check again, or override with --accept-autoscale NOTE')
    else:
        lines.append('workspace: not read through the Management API; the three questions: ' + '; '.join(QUESTIONS))
        if answer:
            lines.append('workspace answer: ' + answer)
        else:
            lines.append('workspace answer: none recorded; ' + ANSWER_HINT)
    if answer:
        events.append(('workspace', 'O1 workspace answer: ' + answer))
    return lines, problems, events
