"""s2rebuild config: load, validate and default the YAML configuration.

Author: John Bergamini

Exposes Config, DatabaseSpec, Window, ConnectionProfile and load_config (section 7 of the
architecture). Unknown keys are refused so a typo cannot pass silently. Relative paths in
the file resolve against the kit root, which is the current working directory unless the
caller passes another root. No statements are issued by this module.
"""
import dataclasses
import datetime as dt
import difflib
import hashlib
import json
import os
import re
from pathlib import Path

import yaml

from .common import KitError, iso, parse_utc, utc_now
from .sqlutil import IDENT

DEFAULT_CONFIG_PATH = 'customer/config.yaml'
ENV_CONFIG = 'S2REBUILD_CONFIG'
TIME_COLUMN_CANDIDATES = ('_modified_time', 'updated_at', 'modified_at', '_created_time', 'created_at', 'event_date')
TIERS = ('count', 'newest', 'fingerprint')
_ENV_NAME = re.compile(r'^[A-Z][A-Z0-9_]*$')
_USER = re.compile(r'^[A-Za-z0-9_.@-]+$')

REQUIRED = object()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
class Field:
    """One scalar or free-form key: type, default, allowed values and bounds."""

    def __init__(self, kind, default=REQUIRED, choices=None, minimum=None, maximum=None, nullable=False):
        self.kind = kind
        self.default = default
        self.choices = choices
        self.minimum = minimum
        self.maximum = maximum
        self.nullable = nullable


class Opt:
    """An optional section: None when absent, validated when present."""

    def __init__(self, schema):
        self.schema = schema


class ListOf:
    """A list of sections, at least min_items long."""

    def __init__(self, schema, min_items=0):
        self.schema = schema
        self.min_items = min_items


class MapOf:
    """A mapping from user-chosen names to sections (per-table overrides)."""

    def __init__(self, schema):
        self.schema = schema


PROFILE_SCHEMA = {
    'user': Field('str'),
    'password_keychain': Field('str', None, nullable=True),
    'password_env': Field('str', None, nullable=True),
}

TABLE_OVERRIDE_SCHEMA = {
    'concurrency': Field('int', None, minimum=1, nullable=True),
    'chunk_rows': Field('int', None, minimum=1, nullable=True),
}

DATABASE_SCHEMA = {
    'source': Field('str'),
    'replacement': Field('str', None, nullable=True),
    'retained': Field('str', None, nullable=True),
    'layout': {
        'partitions': Field('int', minimum=1),
        'sub_partitions': Field('int', minimum=0),
    },
    'time_column_candidates': Field('list', list(TIME_COLUMN_CANDIDATES)),
    'app_managed_pipelines': Field('list', []),
    'tables': MapOf(TABLE_OVERRIDE_SCHEMA),
    'target': {
        'transforms': Field('list', []),
    },
}

SCHEMA = {
    'kit': Field('str', choices=('s2rebuild',)),
    'version': Field('int', choices=(1,)),
    'customer': Field('str', 'Customer'),
    'workspace': {
        'host': Field('str'),
        'port': Field('int', 3306, minimum=1, maximum=65535),
        'ca_file': Field('str', 'certs/singlestore_bundle.pem'),
        'compute_size': Field('str', None, nullable=True),
        'id': Field('str', None, nullable=True),
    },
    'connections': {
        'runner': PROFILE_SCHEMA,
        'owner': Opt(PROFILE_SCHEMA),
    },
    'databases': ListOf(DATABASE_SCHEMA, min_items=1),
    'window': Opt({
        'start': Field('datetime'),
        'end': Field('datetime'),
        'restart_allowance_minutes': Field('int', 60, minimum=0),
        'buffer_minutes': Field('int', 30, minimum=0),
    }),
    'freeze': {
        'mode': Field('str', 'readonly', choices=('readonly', 'lockout')),
        'detach_wait_seconds': Field('int', 120, minimum=0),
        'detach_kill': Field('bool', True),
        'write_probe': Field('bool', True),
        'lockout': {
            'connections': Field('int', 6, minimum=1),
            'interval_seconds': Field('int', 0, minimum=0),
            'protected_accounts': Field('list', []),
            'quiet_passes_required': Field('int', 3, minimum=1),
        },
    },
    'copy': {
        'workers': Field('int', 4, minimum=1, maximum=16),
        'chunk_rows': Field('int', 10_000_000, minimum=1),
        'min_rows_for_chunking': Field('int', 5_000_000, minimum=0),
        'max_buckets_per_table': Field('int', 32, minimum=1),
        'table_concurrency': Field('any', 'auto'),
        'lock_probe_timeout_seconds': Field('int', 5, minimum=1),
        'lock_retry_seconds': Field('int', 120, minimum=1),
        'max_attempts': Field('int', 3, minimum=1),
        'progress_seconds': Field('int', 60, minimum=1),
    },
    'verify': {
        'tiers': Field('list', list(TIERS)),
        'fingerprint_budget_minutes': Field('int', 30, minimum=0),
        'fingerprint_groups': Field('int', 64, minimum=1),
        'sample_percent_over_budget': Field('int', 5, minimum=0, maximum=100),
    },
    'pipelines': {
        'route': Field('str', 'kit', choices=('kit', 'customer')),
        'credentials_env': Field('dict', {'KAFKA': 'S2REBUILD_KAFKA_CREDENTIALS', 'S3': 'S2REBUILD_S3_CREDENTIALS'}),
        'per_pipeline_env': Field('dict', {}),
        'retention_hours': Field('dict', {}),
    },
    'estimate': {
        'baseline': Field('str', 'customer/baseline.json', nullable=True),
        'band_percent': Field('int', 15, minimum=0, maximum=100),
    },
    'closeout': {
        'retain_days': Field('int', 7, minimum=0),
        'flexible_parallelism': Field('number', 0.5, nullable=True),
    },
    'paths': {
        'runs_dir': Field('str', 'customer/runs'),
    },
}


def _joined(path):
    return '.'.join(str(p) for p in path) or '(root)'


def _type_ok(field, value):
    if field.kind == 'str':
        return isinstance(value, str)
    if field.kind == 'int':
        return isinstance(value, int) and not isinstance(value, bool)
    if field.kind == 'number':
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if field.kind == 'bool':
        return isinstance(value, bool)
    if field.kind == 'list':
        return isinstance(value, list)
    if field.kind == 'dict':
        return isinstance(value, dict)
    if field.kind == 'datetime':
        return isinstance(value, (str, dt.datetime))
    return True


def _validate_field(field, value, path):
    if value is None:
        if field.nullable:
            return None
        raise KitError('Configuration key %s must not be empty' % _joined(path))
    if not _type_ok(field, value):
        raise KitError('Configuration key %s must be of type %s' % (_joined(path), field.kind))
    if field.kind == 'datetime':
        return iso(parse_utc(value, 'Configuration key ' + _joined(path)))
    if field.kind == 'str' and not value.strip():
        raise KitError('Configuration key %s must not be blank' % _joined(path))
    if field.choices is not None and value not in field.choices:
        raise KitError('Configuration key %s must be one of: %s' % (
            _joined(path), ', '.join(str(c) for c in field.choices)))
    if field.minimum is not None and value < field.minimum:
        raise KitError('Configuration key %s must be at least %s' % (_joined(path), field.minimum))
    if field.maximum is not None and value > field.maximum:
        raise KitError('Configuration key %s must be at most %s' % (_joined(path), field.maximum))
    if field.kind == 'list':
        return list(value)
    if field.kind == 'dict':
        return dict(value)
    return value


def _validate(node, value, path):
    """Validate value against the schema node and return it with defaults applied."""
    if isinstance(node, Field):
        if value is REQUIRED:
            if node.default is REQUIRED:
                raise KitError('Configuration key %s is required' % _joined(path))
            default = node.default
            return list(default) if isinstance(default, list) else dict(default) if isinstance(default, dict) else default
        return _validate_field(node, value, path)
    if isinstance(node, Opt):
        if value is REQUIRED or value is None:
            return None
        return _validate(node.schema, value, path)
    if isinstance(node, ListOf):
        if value is REQUIRED or value is None:
            value = []
        if not isinstance(value, list):
            raise KitError('Configuration key %s must be a list' % _joined(path))
        if len(value) < node.min_items:
            raise KitError('Configuration key %s needs at least %d entry' % (_joined(path), node.min_items))
        return [_validate(node.schema, item, path + (i,)) for i, item in enumerate(value)]
    if isinstance(node, MapOf):
        if value is REQUIRED or value is None:
            value = {}
        if not isinstance(value, dict):
            raise KitError('Configuration key %s must be a mapping' % _joined(path))
        out = {}
        for name, item in value.items():
            if not isinstance(name, str) or not IDENT.fullmatch(name):
                raise KitError('Configuration key %s has an invalid name %r; letters, digits and underscores only' % (
                    _joined(path), name))
            out[name] = _validate(node.schema, item if item is not None else {}, path + (name,))
        return out
    if isinstance(node, dict):
        if value is REQUIRED or value is None:
            value = {}
        if not isinstance(value, dict):
            raise KitError('Configuration key %s must be a mapping' % _joined(path))
        unknown = [k for k in value if k not in node]
        if unknown:
            key = str(unknown[0])
            hint = difflib.get_close_matches(key, [str(k) for k in node], n=1, cutoff=0.6)
            message = 'Unknown configuration key %s' % _joined(path + (key,))
            if hint:
                message += ' (did you mean %s?)' % hint[0]
            raise KitError(message)
        return {k: _validate(sub, value.get(k, REQUIRED), path + (k,)) for k, sub in node.items()}
    raise KitError('Invalid schema node at %s' % _joined(path))


# ---------------------------------------------------------------------------
# Typed views
# ---------------------------------------------------------------------------
class Section:
    """Read-only attribute view over one validated configuration section."""

    def __init__(self, data):
        object.__setattr__(self, '_data', {k: (Section(v) if isinstance(v, dict) and k not in _FREE_FORM else v)
                                           for k, v in data.items()})

    def __getattr__(self, name):
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        raise AttributeError('configuration is read only')

    def get(self, name, default=None):
        """The value of one key, or default when the section has no such key."""
        return self._data.get(name, default)

    def as_dict(self):
        """The section as a plain nested dict."""
        return {k: (v.as_dict() if isinstance(v, Section) else v) for k, v in self._data.items()}

    def __repr__(self):
        return 'Section(%r)' % sorted(self._data)


_FREE_FORM = frozenset({'credentials_env', 'per_pipeline_env', 'retention_hours'})


@dataclasses.dataclass(frozen=True)
class ConnectionProfile:
    """One SQL identity: user plus exactly one password source (Keychain item or variable)."""

    name: str
    user: str
    password_keychain: str = None
    password_env: str = None


@dataclasses.dataclass(frozen=True)
class Layout:
    """Target partition layout of the replacement database."""

    partitions: int
    sub_partitions: int


@dataclasses.dataclass(frozen=True)
class TableOverride:
    """Per-table copy overrides."""

    concurrency: int = None
    chunk_rows: int = None


@dataclasses.dataclass(frozen=True)
class DatabaseSpec:
    """One database to rebuild: its three names, layout and per-table settings."""

    source: str
    replacement: str
    retained: str
    layout: Layout
    time_column_candidates: tuple
    app_managed_pipelines: tuple
    tables: dict
    target: dict = dataclasses.field(default_factory=dict)

    @property
    def names(self):
        """(source, replacement, retained)."""
        return (self.source, self.replacement, self.retained)

    def table_override(self, table):
        """The TableOverride for a table, empty when none is configured."""
        return self.tables.get(table, TableOverride())


@dataclasses.dataclass(frozen=True)
class Window:
    """The outage window and the allowances that produce the abort deadline."""

    start: dt.datetime
    end: dt.datetime
    restart_allowance_minutes: int
    buffer_minutes: int

    @property
    def duration(self):
        """Length of the window as a timedelta."""
        return self.end - self.start

    def abort_deadline(self, longest_chunk_seconds=0):
        """end minus restart allowance minus buffer minus the longest chunk (section 10.6)."""
        return self.end - dt.timedelta(minutes=self.restart_allowance_minutes + self.buffer_minutes,
                                       seconds=longest_chunk_seconds)


@dataclasses.dataclass(frozen=True)
class Config:
    """The validated configuration with defaults applied."""

    path: Path
    root: Path
    raw: dict
    customer: str
    workspace: Section
    runner: ConnectionProfile
    owner: ConnectionProfile
    databases: tuple
    window: Window
    freeze: Section
    copy: Section
    verify: Section
    pipelines: Section
    estimate: Section
    closeout: Section
    paths: Section

    def database(self, name):
        """The DatabaseSpec whose source is name. Raises KitError when it is not configured."""
        for db in self.databases:
            if db.source == name:
                return db
        raise KitError('Database %s is not configured; configured: %s' % (name, ', '.join(self.database_names())))

    def database_names(self):
        """The configured source names in config order."""
        return [db.source for db in self.databases]

    def run_names(self):
        """Every database name the run may touch: source, replacement and retained of each database."""
        names = []
        for db in self.databases:
            names.extend(db.names)
        return names

    def resolve(self, relative):
        """Resolve a path from the file against the kit root."""
        p = Path(relative).expanduser()
        return p if p.is_absolute() else self.root / p

    def runs_dir(self):
        """The resolved runs directory (paths.runs_dir against the kit root)."""
        return self.resolve(self.paths.runs_dir)

    def config_hash(self):
        """sha256 of the normalized configuration, for the journal."""
        return hashlib.sha256(json.dumps(self.raw, sort_keys=True, default=str).encode('utf8')).hexdigest()


# ---------------------------------------------------------------------------
# Cross-field rules and construction
# ---------------------------------------------------------------------------
def _profile(name, data):
    sources = [k for k in ('password_keychain', 'password_env') if data.get(k)]
    if len(sources) != 1:
        raise KitError('connections.%s needs exactly one of password_keychain or password_env' % name)
    if not _USER.fullmatch(data['user']):
        raise KitError('connections.%s.user holds unsupported characters' % name)
    if data.get('password_env') and not _ENV_NAME.fullmatch(data['password_env']):
        raise KitError('connections.%s.password_env must be an upper-case variable name' % name)
    return ConnectionProfile(name, data['user'], data.get('password_keychain'), data.get('password_env'))


def _identifier(value, path):
    if not isinstance(value, str) or not IDENT.fullmatch(value):
        raise KitError('Configuration key %s must be an identifier of letters, digits and underscores' % path)
    return value


def _database(data, index, date_tag):
    source = _identifier(data['source'], 'databases.%d.source' % index)
    replacement = data.get('replacement') or '%s_rebuild_%s' % (source, date_tag)
    retained = data.get('retained') or '%s_retained_%s' % (source, date_tag)
    _identifier(replacement, 'databases.%d.replacement' % index)
    _identifier(retained, 'databases.%d.retained' % index)
    if len({source, replacement, retained}) != 3:
        raise KitError('databases.%d: source, replacement and retained must be three different names' % index)
    for key in ('time_column_candidates', 'app_managed_pipelines'):
        for item in data[key]:
            _identifier(item, 'databases.%d.%s' % (index, key))
    tables = {name: TableOverride(**fields) for name, fields in data['tables'].items()}
    if data['target']['transforms']:
        raise KitError('databases.%d.target.transforms: table transforms are not available in this pass; the replacement '
                       'differs from the source in partition layout only. Remove the entry or wait for the transform pass.' % index)
    data['replacement'] = replacement
    data['retained'] = retained
    return DatabaseSpec(source, replacement, retained, Layout(**data['layout']), tuple(data['time_column_candidates']),
                        tuple(data['app_managed_pipelines']), tables, dict(data['target']))


def _check_env_map(mapping, path):
    for key, value in mapping.items():
        if not isinstance(key, str) or not key:
            raise KitError('Configuration key %s has an empty name' % path)
        if not isinstance(value, str) or not _ENV_NAME.fullmatch(value):
            raise KitError('Configuration key %s.%s must name an upper-case environment variable' % (path, key))


def build_config(data, path=None, run_date=None, root=None):
    """Validate a mapping already loaded from YAML and return a Config.

    run_date supplies the YYYYMMDD tag for default replacement and retained names; it
    defaults to today in UTC. root is the kit root for relative paths (default: cwd).
    """
    if not isinstance(data, dict):
        raise KitError('The configuration file must hold a mapping at the top level')
    raw = _validate(SCHEMA, data, ())
    date_tag = (run_date or utc_now().date()).strftime('%Y%m%d')
    runner = _profile('runner', raw['connections']['runner'])
    owner = _profile('owner', raw['connections']['owner']) if raw['connections']['owner'] else None
    databases = tuple(_database(d, i, date_tag) for i, d in enumerate(raw['databases']))
    seen = {}
    for db in databases:
        for name in db.names:
            if name in seen:
                raise KitError('Database name %s is used twice (%s and %s); every run name must be unique' % (
                    name, seen[name], db.source))
            seen[name] = db.source
    window = None
    if raw['window']:
        start = parse_utc(raw['window']['start'], 'window.start')
        end = parse_utc(raw['window']['end'], 'window.end')
        if end <= start:
            raise KitError('window.end must be after window.start')
        window = Window(start, end, raw['window']['restart_allowance_minutes'], raw['window']['buffer_minutes'])
    copy = raw['copy']
    concurrency = copy['table_concurrency']
    if isinstance(concurrency, bool) or not (concurrency == 'auto' or (isinstance(concurrency, int) and concurrency == 1)):
        raise KitError('copy.table_concurrency must be auto or 1')
    tiers = raw['verify']['tiers']
    if not tiers or len(set(tiers)) != len(tiers) or any(t not in TIERS for t in tiers) \
            or [t for t in TIERS if t in tiers] != tiers:
        raise KitError('verify.tiers must list some of count, newest, fingerprint in that order')
    for account in raw['freeze']['lockout']['protected_accounts']:
        if not isinstance(account, str) or not account:
            raise KitError('freeze.lockout.protected_accounts must hold account names')
    _check_env_map(raw['pipelines']['credentials_env'], 'pipelines.credentials_env')
    _check_env_map(raw['pipelines']['per_pipeline_env'], 'pipelines.per_pipeline_env')
    for key, value in raw['pipelines']['retention_hours'].items():
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
            raise KitError('pipelines.retention_hours.%s must be a number of hours' % key)
    root = Path(root) if root is not None else Path.cwd()
    return Config(path=Path(path) if path is not None else None, root=root, raw=raw, customer=raw['customer'],
                  workspace=Section(raw['workspace']), runner=runner, owner=owner, databases=databases, window=window,
                  freeze=Section(raw['freeze']), copy=Section(copy), verify=Section(raw['verify']),
                  pipelines=Section(raw['pipelines']), estimate=Section(raw['estimate']),
                  closeout=Section(raw['closeout']), paths=Section(raw['paths']))


def config_path(explicit=None, env=None):
    """The config path: --config, then S2REBUILD_CONFIG, then customer/config.yaml."""
    env = os.environ if env is None else env
    return Path(explicit or env.get(ENV_CONFIG) or DEFAULT_CONFIG_PATH)


def load_config(path=None, run_date=None, env=None, root=None):
    """Read, validate and default the YAML file at path (see config_path for the default)."""
    path = config_path(path, env)
    if not path.exists():
        raise KitError('Configuration file not found: %s (copy examples/config.example.yaml to customer/config.yaml)' % path)
    if not path.is_file():
        raise KitError('Configuration path %s is not a file' % path)
    try:
        data = yaml.safe_load(path.read_text(encoding='utf8'))
    except yaml.YAMLError as exc:
        mark = getattr(exc, 'problem_mark', None)
        where = ' at line %d' % (mark.line + 1) if mark is not None else ''
        raise KitError('Configuration file %s is not valid YAML%s' % (path, where))
    if data is None:
        raise KitError('Configuration file %s is empty' % path)
    return build_config(data, path=path, run_date=run_date, root=root)
