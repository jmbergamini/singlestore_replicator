"""s2rebuild tests test_docs: the documents agree with the code (flags, keys, commands, steps, modules) and the PDF builds.

Author: John Bergamini
"""
import importlib.util
import re
import tempfile
import unittest
from pathlib import Path

from s2rebuild import cli, config, steps
from tests.helpers import ROOT

DOCS = ROOT / 'docs'


def read(name):
    """The text of a kit document."""
    return (ROOT / name).read_text(encoding='utf8')


def schema_keys(node=config.SCHEMA, prefix=''):
    """Every documented key path of the configuration schema."""
    for key, value in node.items():
        path = prefix + key
        if key == 'databases':
            inner = getattr(value, 'schema', None) or config.DATABASE_SCHEMA
            yield from schema_keys(inner, 'databases[].')
        elif key == 'tables' and prefix == 'databases[].':
            yield 'databases[].tables.<table>.concurrency'
            yield 'databases[].tables.<table>.chunk_rows'
        elif isinstance(value, dict):
            yield from schema_keys(value, path + '.')
        elif type(value).__name__ == 'Opt' and isinstance(getattr(value, 'schema', None), dict) and key != 'owner':
            yield from schema_keys(value.schema, path + '.')
        else:
            yield path


def flag_table():
    """{command: set of flags} from the parser; '(global)' holds the global flags."""
    import argparse
    parser = cli.build_parser()
    table = {'(global)': {s for a in parser._actions for s in a.option_strings}}  # noqa: SLF001
    subparsers = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))  # noqa: SLF001
    for name, sub in subparsers.choices.items():
        table[name] = {s for a in sub._actions for s in a.option_strings}  # noqa: SLF001
    return table


class DocsTests(unittest.TestCase):
    def test_every_configuration_key_is_documented(self):
        documented = set(re.findall(r'^\| `([^`]+)` \|', read('docs/CONFIG.md'), re.M))
        missing = sorted(k for k in schema_keys() if k not in documented)
        self.assertEqual(missing, [])

    def test_every_command_and_flag_in_the_documents_exists(self):
        flags = flag_table()
        known = set(cli.COMMAND_NAMES)
        for name in ('README.md', 'AGENTS.md', 'docs/STEPS.md', 'docs/CONFIG.md', 'docs/CODE-GUIDE.md'):
            for m in re.finditer(r'`s2rebuild ([a-z-]+)([^`]*)`', read(name)):
                command, rest = m.group(1), m.group(2)
                self.assertIn(command, known, '%s names an unknown command %s' % (name, command))
                for flag in re.findall(r'(--[a-z-]+)', rest):
                    self.assertTrue(flag in flags.get(command, set()) or flag in flags.get('(global)', set()),
                                    '%s uses %s with %s, which the parser does not define' % (name, flag, command))

    def test_every_command_is_in_the_readme_or_the_step_catalog(self):
        text = read('README.md') + read('docs/STEPS.md') + read('docs/CONFIG.md')
        for command in cli.COMMAND_NAMES:
            self.assertIn(command, text)

    def test_every_step_has_a_section_and_every_module_a_paragraph(self):
        catalog = read('docs/STEPS.md')
        for step_id in steps.ORDER:
            self.assertRegex(catalog, r'(?m)^## %s ' % step_id)
        guide = read('docs/CODE-GUIDE.md')
        for module in sorted(p.name for p in (ROOT / 's2rebuild').glob('*.py') if not p.name.startswith('__')):
            self.assertIn('`%s`' % module, guide)

    def test_readme_starts_with_the_flow_diagram_and_its_links_resolve(self):
        readme = read('README.md')
        self.assertTrue(readme.startswith('# SingleStore Database Rebuild Kit\n'))
        self.assertLess(readme.index('docs/flow.svg'), readme.index('## Who runs what'))
        for target in re.findall(r'\]\(([^)#]+)\)', readme):
            self.assertTrue((ROOT / target).exists(), target)
        self.assertEqual(readme.count('Stop it only with'), 1, 'the hard-stop warning appears once, in O6')

    def test_lessons_cover_every_helios_fact_of_the_architecture(self):
        architecture = read('docs/ARCHITECTURE.md').split('## 11.', 1)[1].split('\n## ', 1)[0]
        facts = re.findall(r'(?m)^\d+\. ', architecture)
        section = read('docs/LESSONS.md').split('## Helios facts', 1)[1].split('## Run lessons', 1)[0]
        self.assertEqual(len(re.findall(r'(?m)^\d+\. ', section)), len(facts))

    def test_the_guide_builds_and_the_committed_pdf_is_current(self):
        spec = importlib.util.spec_from_file_location('build_pdf', DOCS / 'build_pdf.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as tmp:
            out = module.build(Path(tmp) / 'guide.pdf')
            data = out.read_bytes()
        self.assertTrue(data.startswith(b'%PDF-1.4'))
        self.assertTrue(data.rstrip().endswith(b'%%EOF'))
        self.assertGreaterEqual(data.count(b'/Type /Page '), 4)
        self.assertIn(b'/Subtype /Image', data)
        self.assertIn(b'(SingleStore Database Rebuild Kit: step guide)', data)
        self.assertEqual(data, (DOCS / 's2rebuild-guide.pdf').read_bytes(), 'rebuild the guide: python docs/build_pdf.py')


if __name__ == '__main__':
    unittest.main()
