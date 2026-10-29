"""s2rebuild tests test_style: no dashes, no denylisted words, author line in every module, docstring first lines.

Author: John Bergamini
"""
import re
import unittest
from pathlib import Path

import s2rebuild
from tests.helpers import ROOT

# The kit's static tree as of this milestone. M5 widens the scan to the whole folder minus
# customer/ and the patterns in customer/bundle-exclude.txt, once the history folders have moved.
STATIC_PATHS = ['s2rebuild', 'tests', 'examples', 'docs', 'certs', 'notebooks', 'pyproject.toml', 'README.md', 'AGENTS.md', 'requirements.txt']
TEXT_SUFFIXES = {'.py', '.md', '.yaml', '.yml', '.toml', '.txt', '.sh', '.sql', '.json', '.mmd', '.svg', '.ipynb',
                 '.cfg', '.ini', '.pem', '.csv', ''}
SKIP_DIRS = {'__pycache__', '.pytest_cache', 's2rebuild.egg-info'}
DENYLIST_FILE = ROOT / 'customer' / 'bundle-denylist.txt'
DASHES = (chr(0x2014), chr(0x2013))
IDENTIFIED = re.compile('IDENTIFIED' + ' BY ' + r"'([^']*)'")
AUTHOR_LINE = 'Author: John Bergamini'


def static_files():
    for entry in STATIC_PATHS:
        path = ROOT / entry
        if path.is_file():
            yield path
        elif path.is_dir():
            for p in sorted(path.rglob('*')):
                if p.is_file() and not (set(p.parts) & SKIP_DIRS) and p.suffix.lower() in TEXT_SUFFIXES:
                    yield p


def read(path):
    return path.read_text(encoding='utf8', errors='replace')


class StyleTests(unittest.TestCase):
    def test_static_tree_has_files(self):
        files = list(static_files())
        self.assertGreater(len(files), 15)
        self.assertTrue(any(p.name == 'cli.py' for p in files))

    def test_no_em_or_en_dashes(self):
        hits = []
        for path in static_files():
            for i, line in enumerate(read(path).splitlines(), 1):
                if any(d in line for d in DASHES):
                    hits.append('%s:%d' % (path.relative_to(ROOT), i))
        self.assertEqual(hits, [])

    def test_no_denylisted_words(self):
        if not DENYLIST_FILE.exists():
            self.skipTest('customer/bundle-denylist.txt is not present; the word scan needs it')
        words = [w.strip().lower() for w in read(DENYLIST_FILE).splitlines() if w.strip() and not w.startswith('#')]
        self.assertGreater(len(words), 5)
        hits = []
        for path in static_files():
            low = read(path).lower()
            for word in words:
                if word in low:
                    hits.append('%s contains a denylisted word' % path.relative_to(ROOT))
        self.assertEqual(hits, [])

    def test_no_password_literal_after_identified_by(self):
        hits = []
        for path in static_files():
            for i, line in enumerate(read(path).splitlines(), 1):
                for m in IDENTIFIED.finditer(line):
                    if m.group(1) != '<password>':
                        hits.append('%s:%d' % (path.relative_to(ROOT), i))
        self.assertEqual(hits, [])

    def test_author_line_in_every_python_file(self):
        missing = []
        for path in static_files():
            if path.suffix == '.py':
                head = read(path)[:600]
                if AUTHOR_LINE not in head:
                    missing.append(str(path.relative_to(ROOT)))
        self.assertEqual(missing, [])
        self.assertEqual(s2rebuild.__author__, 'John Bergamini')
        self.assertEqual(s2rebuild.__title__, 'SingleStore Database Rebuild Kit')

    def test_module_docstring_first_lines(self):
        pattern = re.compile(r'^s2rebuild (\w+): .+\.$')
        for path in sorted((ROOT / 's2rebuild').glob('*.py')):
            text = read(path)
            self.assertTrue(text.startswith('"""'), '%s must start with a docstring' % path.name)
            first = text.splitlines()[0].strip('"').strip()
            m = pattern.match(first)
            self.assertIsNotNone(m, '%s: first docstring line is %r' % (path.name, first))
            self.assertEqual(m.group(1), path.stem, path.name)
        for path in sorted((ROOT / 'tests').glob('*.py')):
            text = read(path)
            self.assertTrue(text.startswith('"""s2rebuild tests'), path.name)

    def test_every_public_function_has_a_docstring(self):
        import ast
        missing = []
        for path in sorted((ROOT / 's2rebuild').glob('*.py')):
            tree = ast.parse(read(path))
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and not node.name.startswith('_'):
                    if ast.get_docstring(node) is None:
                        missing.append('%s:%s' % (path.name, node.name))
        self.assertEqual(missing, [])

    def test_example_config_holds_placeholders_only(self):
        text = read(ROOT / 'examples' / 'config.example.yaml')
        self.assertIn('svc-xxxx-dml.svc.singlestore.com', text)
        self.assertNotIn('password:', text)
        self.assertIn('password_keychain', text)


if __name__ == '__main__':
    unittest.main()
