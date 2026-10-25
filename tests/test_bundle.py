"""s2rebuild tests test_bundle: manifest hashes, exclusions, the denylist, dash and password scan, verify, and the real kit root.

Author: John Bergamini
"""
import json
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path

from s2rebuild import __version__, bundle
from s2rebuild.common import EXIT_DONE, EXIT_MISMATCH, KitError
from tests.helpers import ROOT, run_cli

DASH = chr(0x2014)


def make_tree(root):
    """A small kit tree with every kind of file the bundle must keep, exclude or scan."""
    files = {
        's2rebuild/__init__.py': '"""pkg."""\n',
        'README.md': '# Kit\n\nAll clean.\n',
        'docs/notes.md': 'Steps.\n',
        'docs/flow.png': b'\x89PNG\x00binary',
        'examples/env.example.sh': 'export X=1\n',
        'customer/bundle-denylist.txt': '# words\nsecretcorp\n',
        'customer/bundle-exclude.txt': 'LOCAL.md\n.assistant/\n',
        'customer/config.yaml': 'customer: secretcorp\n',
        'LOCAL.md': 'secretcorp notes\n',
        '.assistant/settings.json': '{}\n',
        '.venv/lib/x.py': 'x = 1\n',
        's2rebuild/__pycache__/a.cpython-313.pyc': b'\x00\x01',
        '.DS_Store': b'\x00\x00',
        'kit.egg-info/PKG-INFO': 'secretcorp\n',
        'dist/old.zip': b'PK',
    }
    for rel, content in files.items():
        path = Path(root) / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content, encoding='utf8')
    return root


class BundleTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        make_tree(self.root)

    def test_manifest_hashes_exclusions_and_its_own_verify(self):
        result = bundle.build(self.root)
        manifest = json.loads((self.root / 'MANIFEST.json').read_text())
        paths = [f['path'] for f in manifest['files']]
        self.assertEqual(paths, ['README.md', 'docs/flow.png', 'docs/notes.md', 'examples/env.example.sh', 's2rebuild/__init__.py'])
        for f in manifest['files']:
            self.assertEqual(f['sha256'], bundle.sha256(self.root / f['path']))
            self.assertEqual(f['bytes'], (self.root / f['path']).stat().st_size)
        self.assertEqual(result['zip'], self.root / 'dist' / ('s2rebuild-%s.zip' % __version__))
        names = zipfile.ZipFile(result['zip']).namelist()
        self.assertIn('s2rebuild-%s/MANIFEST.json' % __version__, names)
        self.assertFalse(any(x in n for n in names for x in ('customer/', 'LOCAL.md', '.assistant', '.venv', '__pycache__', '.DS_Store', 'egg-info', 'dist/')))
        self.assertEqual(bundle.verify(result['zip']), [])
        first = result['zip'].read_bytes()
        bundle.build(self.root)
        self.assertEqual(result['zip'].read_bytes(), first, 'the same tree gives the same zip')

    def test_scan_hits_refuse_the_bundle_with_file_and_line(self):
        (self.root / 'README.md').write_text('# Kit\nbuilt for SecretCorp\n')
        with self.assertRaisesRegex(KitError, r"README.md:2: denylisted word 'secretcorp'"):
            bundle.build(self.root)
        (self.root / 'README.md').write_text('# Kit\n')
        (self.root / 'docs/flow.png').write_bytes(b'\x89PNG\x00meta by SecretCorp renderer')
        with self.assertRaisesRegex(KitError, "docs/flow.png: denylisted word 'secretcorp' in the bytes of a binary file"):
            bundle.build(self.root)
        (self.root / 'docs/flow.png').write_bytes(b'\x89PNG\x00binary')
        (self.root / 'docs/notes.md').write_text('a %s b\n' % DASH)
        with self.assertRaisesRegex(KitError, 'docs/notes.md:1: em or en dash'):
            bundle.build(self.root)
        (self.root / 'docs/notes.md').write_text("CREATE USER x IDENTIFIED BY '<password>';\n")
        bundle.build(self.root, dry_run=True)
        (self.root / 'docs/notes.md').write_text('CREATE USER x IDENTIFIED' + " BY 'hunter2';\n")
        with self.assertRaisesRegex(KitError, 'password literal after IDENTIFIED BY'):
            bundle.build(self.root)
        self.assertFalse((self.root / 'MANIFEST.json').exists(), 'a refused bundle writes nothing')

    def test_missing_denylist_dry_run_and_customer_option(self):
        bundle.build(self.root, dry_run=True)
        self.assertFalse((self.root / 'MANIFEST.json').exists())
        self.assertFalse((self.root / 'dist' / ('s2rebuild-%s.zip' % __version__)).exists())
        result = bundle.build(self.root, include_customer=True)
        names = zipfile.ZipFile(result['zip']).namelist()
        self.assertIn('s2rebuild-%s/customer/config.yaml' % __version__, names)
        self.assertEqual(bundle.verify(result['zip']), [])
        (self.root / 'customer' / 'bundle-denylist.txt').unlink()
        with self.assertRaisesRegex(KitError, 'No denylist'):
            bundle.build(self.root)

    def test_verify_catches_changed_missing_extra_and_unsafe_files(self):
        result = bundle.build(self.root)
        top = 's2rebuild-%s/' % __version__
        tampered = self.root / 'tampered.zip'
        with zipfile.ZipFile(result['zip']) as src, zipfile.ZipFile(tampered, 'w') as dst:
            for name in src.namelist():
                data = src.read(name)
                if name == top + 'README.md':
                    data = b'changed'
                if name == top + 'docs/notes.md':
                    continue
                dst.writestr(name, data)
            dst.writestr(top + 'extra.txt', 'x')
        problems = bundle.verify(tampered)
        self.assertIn('README.md differs from its manifest hash', problems)
        self.assertIn('docs/notes.md is listed in the manifest but missing', problems)
        self.assertIn('extra.txt is in the zip but not in the manifest', problems)
        unsafe = self.root / 'unsafe.zip'
        with zipfile.ZipFile(unsafe, 'w') as zf:
            zf.writestr('../evil.txt', 'x')
        self.assertEqual(bundle.verify(unsafe), ['unsafe path in the zip: ../evil.txt'])
        code, text = run_cli(['bundle', '--verify', str(tampered)])
        self.assertEqual(code, EXIT_MISMATCH)
        self.assertIn('MISMATCH: README.md differs from its manifest hash', text)
        code, text = run_cli(['bundle', '--verify', str(result['zip'])])
        self.assertEqual(code, EXIT_DONE, text)


class KitRootTests(unittest.TestCase):
    def test_the_kit_root_bundles_clean(self):
        if not (ROOT / bundle.DENYLIST).is_file():
            self.skipTest('customer/bundle-denylist.txt is not present (customer/ is never committed); the kit root scan needs it')
        result = bundle.build(ROOT, dry_run=True)
        self.assertIn('scan clean', result['lines'][0])
        paths = {f['path'] for f in result['manifest']['files']}
        for required in ('README.md', 'requirements.txt', 'pyproject.toml', 'docs/ARCHITECTURE.md', 'docs/STEPS.md', 'docs/CONFIG.md',
                         'docs/CODE-GUIDE.md', 'docs/LESSONS.md', 'docs/flow.mmd', 'docs/flow.svg', 'docs/flow.png', 'docs/build_pdf.py',
                         'docs/s2rebuild-guide.pdf', 'examples/config.example.yaml', 'notebooks/owner-steps.template.ipynb',
                         'certs/singlestore_bundle.pem', 's2rebuild/cli.py'):
            self.assertIn(required, paths)
        self.assertFalse(any(p.startswith('customer/') for p in paths))
        local = bundle.read_lines(ROOT / 'customer' / 'bundle-exclude.txt')
        self.assertTrue(local, 'customer/bundle-exclude.txt names the local assistant files')
        self.assertFalse([p for p in paths if bundle.excluded(p, local)])


if __name__ == '__main__':
    unittest.main()
