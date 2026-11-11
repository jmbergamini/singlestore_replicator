"""s2rebuild prompts: operator prompts, the Keychain writer, pipeline credentials and the runner password.

Author: John Bergamini

Every question the kit asks goes through ask(label, hidden) (tests replace it); a hidden
answer is read with getpass and is never echoed, logged or written to a file. Every Keychain
write goes through write_keychain (security add-generic-password -U; tests replace it with a
fake store) and every read of an item the kit may fill through read_item. The pipeline
credentials are asked by field per source type (S3, Kafka; other types by the CONFIG keys
the pipeline uses), assembled as the CREDENTIALS JSON and stored in the Keychain item each
pipeline is read from (pipelines.per_pipeline_keychain, else credentials_keychain by source
type; keychain_item_for); a blank answer skips the source type and the journal
records it (pipelines.record_skip). The runner password is generated from the workspace
policy (the PASSWORD_ variables read through the owner session at P1) and stored the same
way. Statements issued by this module: the SELECT of the PASSWORD_ variables (read_policy).
"""
import getpass
import json
import os
import re
import secrets
import string
import subprocess
import sys

from .common import KitError
from .connect import KEYCHAIN_TOOL, read_keychain

GIVEN, SKIPPED, NO_TERMINAL = 'given', 'skipped', 'no_terminal'
POLICY_SQL = "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME LIKE 'PASSWORD_%'"
PASSWORD_LENGTH = 32
SPECIAL = '!#$%&*+,-.:;<=>?@^_~'
_CONFIG = re.compile(r"\bCONFIG\s+'((?:[^'\\]|\\.|'')*)'", re.I)

# source type: (JSON key, prompt label, hidden, required) in the order asked; the first blank answer of a required field skips the type
CREDENTIAL_FIELDS = {
    'S3': (('aws_access_key_id', 'S3 access key ID', False, True),
           ('aws_secret_access_key', 'S3 secret access key', True, True),
           ('aws_session_token', 'S3 session token (Return to skip)', True, False)),
    'KAFKA': (('sasl.username', 'Kafka SASL user', False, True),
              ('sasl.password', 'Kafka SASL password', True, True)),
}


# ---------------------------------------------------------------------------
# The one prompt and the one writer
# ---------------------------------------------------------------------------
def ask(label, hidden=False):
    """One answer from the operator at a terminal; None without a terminal. hidden reads with getpass, never echoed. Tests replace this."""
    if sys.stdin is None or not sys.stdin.isatty():
        return None
    if hidden:
        return getpass.getpass(label + ': ').rstrip('\r\n')
    return input(label + ': ').strip()


def write_keychain(service, value, account=None, run=None):
    """Store value in the macOS Keychain item service for account (security add-generic-password -U replaces an existing item).

    The value never appears in a message or a log line; a failure names the item only. Tests
    replace this function with a fake store.
    """
    run = run or subprocess.run
    account = account or os.environ.get('USER') or getpass.getuser()
    command = [KEYCHAIN_TOOL, 'add-generic-password', '-a', account, '-s', service, '-U', '-w', value]
    try:
        proc = run(command, capture_output=True, text=True, timeout=60, check=False)
    except FileNotFoundError:
        raise KitError('The %s command is not available on this machine; store the value in the environment variable named in the '
                       'configuration instead' % KEYCHAIN_TOOL)
    except subprocess.TimeoutExpired:
        raise KitError('Writing Keychain item %s timed out; unlock the Keychain and retry' % service)
    if proc.returncode != 0:
        raise KitError('Keychain item %s for account %s could not be written (%s exit %d)' % (service, account, KEYCHAIN_TOOL, proc.returncode))


def read_item(service):
    """The value of a Keychain item the kit may have filled, or None when it is missing, empty or unreadable (the caller asks or falls back)."""
    try:
        return read_keychain(service)
    except KitError:
        return None


# ---------------------------------------------------------------------------
# Passwords
# ---------------------------------------------------------------------------
def ensure_password(profile, log):
    """Ask for the password of a connection profile (hidden) and store it when its Keychain item is missing; nothing for a variable.

    Returns True when the item was filled now. KitError without a terminal or on a blank answer, naming the item, never a value.
    """
    item = profile.password_keychain
    if not item or read_item(item) is not None:
        return False
    answer = ask('%s password (Keychain item %s is missing; the answer is stored there)' % (profile.name, item), hidden=True)
    if answer is None:
        raise KitError('Keychain item %s for the %s password is missing and there is no terminal to type it at; add it with '
                       'security add-generic-password -a "$USER" -s %s -w, then run again' % (item, profile.name, item))
    if not answer:
        raise KitError('No %s password was typed; Keychain item %s stays missing' % (profile.name, item))
    write_keychain(item, answer)
    log('%s password stored in Keychain item %s' % (profile.name, item))
    return True


def read_policy(session):
    """The workspace password policy: {VARIABLE_NAME: VARIABLE_VALUE} of the PASSWORD_ variables, read through the owner session."""
    return {str(r.get('VARIABLE_NAME')): str(r.get('VARIABLE_VALUE')) for r in session.query(POLICY_SQL)}


def _int_value(value, default):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def policy_rules(policy):
    """The rules a policy mapping states, by meaning: min_length, min_upper, min_lower, min_digits, min_special, max_repeat, max_sequential.

    The variable names are matched by their words (MIN_LENGTH, UPPER, LOWER, NUMERIC or DIGIT,
    SPECIAL or SYMBOL, REPEAT, SEQUENTIAL), so a workspace that names them differently from
    the capture still counts; a rule the policy does not state is None.
    """
    rules = {'min_length': None, 'min_upper': None, 'min_lower': None, 'min_digits': None, 'min_special': None,
             'max_repeat': None, 'max_sequential': None}
    for name, value in (policy or {}).items():
        key = str(name).upper()
        if 'MIN' in key and 'LENGTH' in key:
            rules['min_length'] = _int_value(value, None)
        elif 'MIN' in key and 'UPPER' in key:
            rules['min_upper'] = _int_value(value, None)
        elif 'MIN' in key and 'LOWER' in key:
            rules['min_lower'] = _int_value(value, None)
        elif 'MIN' in key and ('NUMERIC' in key or 'DIGIT' in key):
            rules['min_digits'] = _int_value(value, None)
        elif 'MIN' in key and ('SPECIAL' in key or 'SYMBOL' in key):
            rules['min_special'] = _int_value(value, None)
        elif 'MAX' in key and 'REPEAT' in key:
            rules['max_repeat'] = _int_value(value, None)
        elif 'MAX' in key and 'SEQUENTIAL' in key:
            rules['max_sequential'] = _int_value(value, None)
    return rules


def policy_lines(policy):
    """The lines P1 prints before CREATE USER: every PASSWORD_ variable with its value, or that none is reported."""
    if not policy:
        return ['password policy: no PASSWORD_ variable reported by the workspace; the kit generates %d characters with every '
                'character class, no repeated and no sequential characters' % PASSWORD_LENGTH]
    return ['password policy: ' + ', '.join('%s = %s' % (k, policy[k]) for k in sorted(policy))]


def _longest_repeat(text):
    longest, run = 0, 0
    for i, c in enumerate(text):
        run = run + 1 if i and c == text[i - 1] else 1
        longest = max(longest, run)
    return longest


def _longest_sequence(text):
    """The longest run of consecutive letters or digits, up or down (abc, 321), in text."""
    longest, run, direction = (1 if text else 0), 1, 0
    for i in range(1, len(text)):
        a, b = text[i - 1], text[i]
        step = ord(b) - ord(a)
        if a.isalnum() and b.isalnum() and step in (1, -1):
            run = run + 1 if direction == step else 2
            direction = step
        else:
            run, direction = 1, 0
        longest = max(longest, run)
    return longest


def password_problems(password, policy):
    """The rules of policy a password misses, as text lines; empty when it meets them all (the same check the generator runs)."""
    return _rule_problems(password, policy_rules(policy))


def _rule_problems(password, rules):
    counts = {'min_upper': sum(c.isupper() for c in password), 'min_lower': sum(c.islower() for c in password),
              'min_digits': sum(c.isdigit() for c in password), 'min_special': sum(not c.isalnum() for c in password)}
    problems = []
    if rules['min_length'] is not None and len(password) < rules['min_length']:
        problems.append('length %d is under the minimum %d' % (len(password), rules['min_length']))
    for key, label in (('min_upper', 'upper-case'), ('min_lower', 'lower-case'), ('min_digits', 'numeric'), ('min_special', 'special')):
        if rules[key] is not None and counts[key] < rules[key]:
            problems.append('%d %s characters, under the minimum %d' % (counts[key], label, rules[key]))
    if rules['max_repeat'] is not None and _longest_repeat(password) > rules['max_repeat']:
        problems.append('a character repeats more than %d times in a row' % rules['max_repeat'])
    if rules['max_sequential'] is not None and _longest_sequence(password) > rules['max_sequential']:
        problems.append('more than %d sequential characters in a row' % rules['max_sequential'])
    return problems


def generate_password(policy, length=PASSWORD_LENGTH, rng=None):
    """A random password that meets policy: length (at least the policy minimum), at least two of each class or the policy
    minimum when higher, no character twice in a row and no three sequential characters, whatever the policy allows.

    rng is secrets.SystemRandom unless a test injects one. KitError when no password met the
    policy after many tries (a policy no 32 character password can satisfy).
    """
    rng = rng or secrets.SystemRandom()
    rules = policy_rules(policy)
    length = max(int(length), rules['min_length'] or 0)
    classes = [(string.ascii_uppercase, max(2, rules['min_upper'] or 0)), (string.ascii_lowercase, max(2, rules['min_lower'] or 0)),
               (string.digits, max(2, rules['min_digits'] or 0)), (SPECIAL, max(2, rules['min_special'] or 0))]
    if sum(n for _, n in classes) > length:
        raise KitError('The password policy asks for more characters per class than a %d character password holds' % length)
    # stricter than any policy: no character twice in a row, no three sequential characters
    rules['max_repeat'] = min(rules['max_repeat'] or 1, 1)
    rules['max_sequential'] = min(rules['max_sequential'] or 2, 2)
    everything = ''.join(a for a, _ in classes)
    for _ in range(1000):
        chars = [rng.choice(alphabet) for alphabet, n in classes for _ in range(n)]
        chars.extend(rng.choice(everything) for _ in range(length - len(chars)))
        rng.shuffle(chars)
        candidate = ''.join(chars)
        if not _rule_problems(candidate, rules):
            return candidate
    raise KitError('No password met the workspace password policy after 1000 tries; the policy is printed above')


# ---------------------------------------------------------------------------
# Pipeline credentials
# ---------------------------------------------------------------------------
def config_keys(definition):
    """The keys of the CONFIG JSON of a CREATE PIPELINE text, sorted; empty when it has none or the text is not JSON."""
    m = _CONFIG.search(str(definition or ''))
    if not m:
        return []
    text = m.group(1).replace("''", "'").replace("\\'", "'").replace('\\\\', '\\')
    try:
        data = json.loads(text)
    except ValueError:
        return []
    return sorted(str(k) for k in data) if isinstance(data, dict) else []


def ask_credentials(source_type, items, say=None):
    """Ask the credential fields of one source type and return (status, JSON text, values).

    S3 and Kafka have fixed fields (CREDENTIAL_FIELDS); for another type the CONFIG keys the
    pipelines use are printed through say and the kit asks for credential keys and values until
    a blank key. status is GIVEN with the assembled JSON, SKIPPED on a blank first answer (the
    operator skips the type), or NO_TERMINAL when nothing can be asked. values are every answer,
    for the redactor; nothing is logged here.
    """
    say = say or (lambda text: None)
    names = ', '.join(sorted(i['name'] for i in items))
    fields = CREDENTIAL_FIELDS.get(str(source_type).upper())
    data = {}
    if fields:
        say('credentials for the %s pipelines %s (a blank first answer skips the source type)' % (source_type, names))
        for key, label, hidden, required in fields:
            answer = ask(label, hidden=hidden)
            if answer is None:
                return NO_TERMINAL, None, []
            if not answer:
                if required:
                    return SKIPPED, None, []
                continue
            data[key] = answer
        return GIVEN, json.dumps(data), list(data.values())
    keys = sorted({k for i in items for k in config_keys(i.get('definition'))})
    say('credentials for the %s pipelines %s: no fixed fields for this source type; CONFIG keys in use: %s. Type each credential '
        'key and its value; a blank key ends (blank first skips the source type)' % (source_type, names, ', '.join(keys) or 'none'))
    while True:
        key = ask('credential key for %s (Return to end)' % source_type)
        if key is None:
            return NO_TERMINAL, None, []
        if not key:
            break
        value = ask('value of %s' % key, hidden=True)
        if value is None:
            return NO_TERMINAL, None, []
        data[key] = value
    if not data:
        return SKIPPED, None, []
    return GIVEN, json.dumps(data), list(data.values())


def keychain_item_for(item, config):
    """The Keychain item configured for a pipeline's credentials: pipelines.per_pipeline_keychain, then credentials_keychain by source type."""
    return (config.pipelines.per_pipeline_keychain.get(item['name'])
            or config.pipelines.credentials_keychain.get(str(item.get('source_type') or '').upper()))


def pipeline_credentials(items, config, journal, log, env, redactor=None, step='P3'):
    """Ask for the credentials of every source type that still has none, store them in the Keychain, record each skip.

    Only expected pipelines whose definition carries the credentials placeholder count. A type
    whose every pipeline already resolves (pipelines.credential_for: the Keychain item, else the
    variable) is not asked; the missing pipelines are asked once per Keychain item they are read
    from (keychain_item_for: the per-pipeline item, else the type item) and each answer is
    stored in that item, so credential_for finds it; a pipeline with no Keychain item configured
    cannot be stored and the type is named with the variable to export instead; a blank first
    answer skips the whole type and asks no further item of it; a skipped type is recorded with
    pipelines.record_skip, so the O9 count gate, the plan sheet and the final report list its
    pipelines. Returns {source_type: status}, the status SKIPPED, else NO_TERMINAL when any item
    of the type got it, else GIVEN. Nothing of a value reaches the log.
    """
    from . import pipelines
    from .sqlutil import secret_forms
    outcome = {}
    skipped = pipelines.skipped_types(journal)
    by_type = {}
    for item in pipelines.expected_items(items):
        if pipelines.needs_credentials(item) and item.get('source_type') not in skipped:
            by_type.setdefault(str(item.get('source_type') or 'UNKNOWN').upper(), []).append(item)
    for source_type in sorted(by_type):
        group = by_type[source_type]
        missing = []
        for item in group:
            try:
                pipelines.credential_for(item, config, env)
            except KitError:
                missing.append(item)
        if not missing:
            continue
        # each missing pipeline is filled in the item credential_for reads for it: its per-pipeline item, else the type item
        by_service = {}
        for item in missing:
            by_service.setdefault(keychain_item_for(item, config), []).append(item)
        statuses = []
        if None in by_service:
            var = config.pipelines.credentials_env.get(source_type)
            log('pipeline credentials for %s: no Keychain item is configured (pipelines.credentials_keychain), so nothing is asked; '
                '%s for O9 only' % (source_type, ('export %s' % var) if var else 'set pipelines.credentials_env or credentials_keychain'))
            statuses.append(NO_TERMINAL)
        for service in sorted(s for s in by_service if s):
            members = by_service[service]
            status, text, values = ask_credentials(source_type, members, say=log.plain)
            statuses.append(status)
            if status == GIVEN:
                if redactor is not None:
                    for value in values:
                        redactor.extend(secret_forms(value))
                write_keychain(service, text)
                log('pipeline credentials for %s stored in Keychain item %s (%d pipelines)' % (source_type, service, len(members)))
                journal.event('credentials_stored', '%s: %s credentials stored in Keychain item %s' % (step, source_type, service))
            elif status == SKIPPED:
                names = pipelines.record_skip(journal, source_type, [i['name'] for i in group], step)
                log('pipeline credentials for %s: none given, so these pipelines will not be recreated at O9: %s' % (source_type, ', '.join(names)))
                break
            else:
                log('pipeline credentials for %s: no terminal to ask at; O9 asks again, or reads Keychain item %s or the variable' % (source_type, service))
        outcome[source_type] = next((s for s in (SKIPPED, NO_TERMINAL) if s in statuses), GIVEN)
    return outcome


# ---------------------------------------------------------------------------
# Names at O1
# ---------------------------------------------------------------------------
def ask_names(db, config, say=None):
    """Ask for the replacement and retained names the configuration left to the kit (run.mode production), the default as the suggestion.

    Returns {role: name} for the roles asked. A blank answer keeps the default; a name must
    be an identifier and differ from every other run name. KitError without a terminal.
    """
    from .sqlutil import IDENT
    say = say or (lambda text: None)
    chosen = {}
    taken = {n for other in config.databases for n in other.names if other.source != db.source}
    say('run.mode is production and the configuration leaves the %s name(s) of %s to the kit; type each name, or Return for the default'
        % (' and '.join(db.defaulted), db.source))
    for role in db.defaulted:
        default = getattr(db, role)
        answer = ask('%s name for %s [%s]' % (role, db.source, default))
        if answer is None:
            raise KitError('run.mode is production and databases[].%s is not set for %s; type the name at a terminal, set it in the '
                           'configuration, or run with --mode test to apply the default %s' % (role, db.source, default))
        name = answer or default
        if not IDENT.fullmatch(name):
            raise KitError('%s name %r is not an identifier of letters, digits and underscores' % (role, name))
        if name == db.source or name in chosen.values() or name in taken:
            raise KitError('%s name %s is already used by another role or database of this run' % (role, name))
        chosen[role] = name
    return chosen
