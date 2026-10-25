"""s2rebuild bundle: MANIFEST.json, the denylist and style scan, and the zip of the static tree.

Author: John Bergamini

Section 10.20 of the architecture. The static tree is everything under the kit root except
customer/, dist/, .venv/, __pycache__/, .DS_Store, the built-in local patterns (.git/,
.pytest_cache/, *.egg-info/, editor folders, *.pyc) and the patterns in customer/bundle-exclude.txt
(the local assistant instruction and settings files by default). Before zipping, every text file of the static tree is scanned
for the words in customer/bundle-denylist.txt (case-insensitive; binary files are searched byte
by byte, which catches tool names in image metadata), for the em dash and en dash
characters, and for a quoted literal after IDENTIFIED BY other than the <password> placeholder;
any hit refuses the bundle with file and line. MANIFEST.json lists path, bytes and sha256 of
every file; dist/s2rebuild-<version>.zip holds the files under s2rebuild-<version>/ next to the
manifest, with fixed timestamps so the same tree gives the same zip. The build verifies its own
zip, and --verify ZIP unzips any bundle to a temporary folder and compares every hash with the
manifest inside it. Statements issued: none.
"""
import fnmatch
import hashlib
import json
import re
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

from . import __title__, __version__
from .common import KitError

MANIFEST = 'MANIFEST.json'
DENYLIST = 'customer/bundle-denylist.txt'
EXCLUDE_FILE = 'customer/bundle-exclude.txt'
BUILTIN_EXCLUDES = ('customer/', 'dist/', '.venv/', '__pycache__/', '.DS_Store', '.git/', '.pytest_cache/', '*.egg-info/', '.idea/',
                    '.vscode/', '*.pyc', MANIFEST)
DASHES = (chr(0x2014), chr(0x2013))
IDENTIFIED = re.compile('IDENTIFIED' + r"\s+BY\s+'([^']*)'", re.I)
ZIP_TIME = (1980, 1, 1, 0, 0, 0)


def kit_root():
    """The kit root: the folder that holds the s2rebuild package."""
    return Path(__file__).resolve().parent.parent


def read_lines(path):
    """The non-empty lines of a text file that are not comments; [] when the file does not exist."""
    path = Path(path)
    if not path.is_file():
        return []
    return [line.strip() for line in path.read_text(encoding='utf8').splitlines() if line.strip() and not line.strip().startswith('#')]


def excluded(rel, patterns):
    """True when a relative posix path matches an exclude pattern (a trailing slash names a folder at any depth)."""
    parts = PurePosixPath(rel).parts
    for pattern in patterns:
        if pattern.endswith('/'):
            name = pattern.rstrip('/')
            if any(fnmatch.fnmatch(part, name) for part in parts[:-1]):
                return True
        elif fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(parts[-1], pattern):
            return True
    return False


def tree_files(root, patterns, under=None):
    """Sorted relative posix paths of the files under root (or root/under) that no pattern excludes."""
    root = Path(root)
    base = root / under if under else root
    out = []
    for path in base.rglob('*'):
        if path.is_file() and not path.is_symlink():
            rel = path.relative_to(root).as_posix()
            if not excluded(rel, patterns):
                out.append(rel)
    return sorted(out)


def text_of(data):
    """The text of a file's bytes, or None for a binary file."""
    if b'\0' in data[:8192]:
        return None
    try:
        return data.decode('utf8')
    except UnicodeDecodeError:
        return None


def scan(root, files, denylist):
    """Hits as 'path:line: reason' for denylisted words, dash characters and password literals in the text files."""
    words = [w.lower() for w in denylist]
    hits = []
    for rel in files:
        data = (Path(root) / rel).read_bytes()
        text = text_of(data)
        if text is None:
            low = data.lower()
            for word in words:
                if word.encode('utf8') in low:
                    hits.append('%s: denylisted word %r in the bytes of a binary file (metadata?)' % (rel, word))
            continue
        for number, line in enumerate(text.splitlines(), 1):
            low = line.lower()
            for word in words:
                if word in low:
                    hits.append('%s:%d: denylisted word %r' % (rel, number, word))
            if any(d in line for d in DASHES):
                hits.append('%s:%d: em or en dash' % (rel, number))
            if any(m.group(1) != '<password>' for m in IDENTIFIED.finditer(line)):
                hits.append('%s:%d: password literal after IDENTIFIED BY' % (rel, number))
    return hits


def sha256(path):
    """The sha256 hex digest of a file."""
    digest = hashlib.sha256()
    with open(path, 'rb') as fh:
        for block in iter(lambda: fh.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def manifest_for(root, files, version=__version__):
    """The manifest dict: kit, title, version and path, bytes, sha256 of every file."""
    root = Path(root)
    return {'kit': 's2rebuild', 'title': __title__, 'version': version,
            'files': [{'path': rel, 'bytes': (root / rel).stat().st_size, 'sha256': sha256(root / rel)} for rel in files]}


def build(root=None, include_customer=False, dry_run=False, version=__version__):
    """Scan, write MANIFEST.json and dist/s2rebuild-<version>.zip, then verify the zip. Returns {'lines', 'zip', 'manifest'}.

    Raises KitError when the denylist file is missing or the scan finds a hit (file and line).
    """
    root = Path(root or kit_root())
    denylist_path = root / DENYLIST
    if not denylist_path.is_file():
        raise KitError('No denylist at %s; create it with the words that must not appear in the static tree' % denylist_path)
    patterns = list(BUILTIN_EXCLUDES) + read_lines(root / EXCLUDE_FILE)
    files = tree_files(root, patterns)
    hits = scan(root, files, read_lines(denylist_path))
    if hits:
        raise KitError('Bundle refused, %d hit(s) in the static tree: %s' % (len(hits), '; '.join(hits[:25])))
    extra = []
    if include_customer:
        customer_patterns = [p for p in patterns if p != 'customer/']
        extra = tree_files(root, customer_patterns, under='customer')
    manifest = manifest_for(root, files + extra, version)
    total = sum(f['bytes'] for f in manifest['files'])
    lines = ['static tree: %d files, %d bytes; scan clean (denylist %d words, dashes, password literals)' % (len(files), total, len(read_lines(denylist_path)))]
    if extra:
        lines.append('customer/ added: %d files (not scanned; it holds the customer content by design)' % len(extra))
    zip_path = root / 'dist' / ('s2rebuild-%s.zip' % version)
    if dry_run:
        lines.append('DRY RUN: would write %s and %s' % (root / MANIFEST, zip_path))
        return {'lines': lines, 'zip': None, 'manifest': manifest}
    (root / MANIFEST).write_text(json.dumps(manifest, indent=1, sort_keys=True) + '\n', encoding='utf8')
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    top = 's2rebuild-%s/' % version
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for rel in [MANIFEST] + [f['path'] for f in manifest['files']]:
            info = zipfile.ZipInfo(top + rel, date_time=ZIP_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = (0o755 if rel.endswith('.sh') else 0o644) << 16
            zf.writestr(info, (root / rel).read_bytes())
    problems = verify(zip_path)
    if problems:
        raise KitError('The new bundle failed its own verify: ' + '; '.join(problems[:10]))
    lines.append('MANIFEST.json written; bundle %s (%d files) verified against its manifest' % (zip_path, len(manifest['files'])))
    return {'lines': lines, 'zip': zip_path, 'manifest': manifest}


def verify(zip_path):
    """Unzip to a temporary folder and compare every file with the manifest inside; returns the list of problems (empty when it passes)."""
    zip_path = Path(zip_path)
    if not zip_path.is_file():
        return ['%s does not exist' % zip_path]
    problems = []
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            for name in names:
                pure = PurePosixPath(name)
                if pure.is_absolute() or '..' in pure.parts:
                    return ['unsafe path in the zip: %s' % name]
            zf.extractall(tmp)
        manifests = [n for n in names if PurePosixPath(n).name == MANIFEST and len(PurePosixPath(n).parts) <= 2]
        if len(manifests) != 1:
            return ['the zip holds %d manifests at its top, expected 1' % len(manifests)]
        base = Path(tmp) / PurePosixPath(manifests[0]).parent
        manifest = json.loads((base / MANIFEST).read_text(encoding='utf8'))
        listed = {f['path'] for f in manifest.get('files', [])}
        for f in manifest.get('files', []):
            path = base / f['path']
            if not path.is_file():
                problems.append('%s is listed in the manifest but missing' % f['path'])
                continue
            if path.stat().st_size != f['bytes'] or sha256(path) != f['sha256']:
                problems.append('%s differs from its manifest hash' % f['path'])
        present = {p.relative_to(base).as_posix() for p in base.rglob('*') if p.is_file()} - {MANIFEST}
        for rel in sorted(present - listed):
            problems.append('%s is in the zip but not in the manifest' % rel)
    return problems
