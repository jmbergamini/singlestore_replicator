"""s2rebuild build_pdf: render README.md and docs/STEPS.md with the flow diagram into docs/s2rebuild-guide.pdf.

Author: John Bergamini

A dependency-free PDF writer, so the guide builds anywhere the kit runs: the standard Type 1
fonts Helvetica, Helvetica-Bold and Courier, and the flow diagram embedded from docs/flow.png
(8-bit RGB or gray, not interlaced) with the PNG predictor, without decoding it. Page 1 is the
cover (title, author, version), page 2 the flow diagram, then the README and the step catalog,
each from a new page. The Markdown subset covers headings, paragraphs, bullet and numbered
lists, tables, fenced code, inline code, bold and links (rendered as their text).

Run from the kit root: python docs/build_pdf.py [--out FILE]. Statements issued: none.
"""
import argparse
import re
import struct
import sys
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TITLE = 'SingleStore Database Rebuild Kit'
AUTHOR = 'John Bergamini'
PAGE_W, PAGE_H = 612.0, 792.0
MARGIN = 60.0
WIDTH = PAGE_W - 2 * MARGIN

_HELV = [278, 278, 355, 556, 556, 889, 667, 222, 333, 333, 389, 584, 278, 333, 278, 278, 556, 556, 556, 556, 556, 556, 556, 556, 556, 556,
         278, 278, 584, 584, 584, 556, 1015, 667, 667, 722, 722, 667, 611, 778, 722, 278, 500, 667, 556, 833, 722, 778, 667, 778, 722, 667,
         611, 722, 667, 944, 667, 667, 611, 278, 278, 278, 469, 556, 222, 556, 556, 500, 556, 556, 278, 556, 556, 222, 222, 500, 222, 833,
         556, 556, 556, 556, 333, 500, 278, 556, 500, 722, 500, 500, 500, 334, 260, 334, 584]
_BOLD = [278, 333, 474, 556, 556, 889, 722, 278, 333, 333, 389, 584, 278, 333, 278, 278, 556, 556, 556, 556, 556, 556, 556, 556, 556, 556,
         333, 333, 584, 584, 584, 611, 975, 722, 722, 722, 722, 667, 611, 778, 722, 278, 556, 722, 611, 833, 722, 778, 667, 778, 722, 667,
         611, 722, 667, 944, 667, 667, 611, 333, 278, 333, 584, 556, 278, 556, 611, 556, 611, 556, 333, 611, 611, 278, 278, 556, 278, 889,
         611, 611, 611, 611, 389, 556, 333, 611, 556, 778, 556, 556, 500, 389, 280, 389, 584]
FONTS = {'F1': ('Helvetica', _HELV), 'F2': ('Helvetica-Bold', _BOLD), 'F3': ('Courier', None)}


def text_width(text, font, size):
    """Width in points of text set in font (F1, F2 or F3) at size."""
    table = FONTS[font][1]
    if table is None:
        return len(text) * 600 * size / 1000.0
    return sum(table[ord(c) - 32] if 32 <= ord(c) <= 126 else 556 for c in text) * size / 1000.0


def pdf_string(text):
    """A PDF literal string in WinAnsi with backslash, parentheses escaped."""
    data = text.encode('cp1252', 'replace').decode('latin-1')
    return '(' + data.replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)') + ')'


def inline_runs(text):
    """[(text, font)] for a line of Markdown: **bold** in Helvetica-Bold, `code` in Courier, links as their text."""
    text = re.sub(r'!\[[^\]]*\]\([^)]*\)', '', text)
    text = re.sub(r'\[([^\]]+)\]\([^)]*\)', r'\1', text)
    runs = []
    for part in re.split(r'(\*\*[^*]+\*\*|`[^`]+`)', text):
        if not part:
            continue
        if part.startswith('**') and part.endswith('**'):
            runs.append((part[2:-2], 'F2'))
        elif part.startswith('`') and part.endswith('`'):
            runs.append((part[1:-1], 'F3'))
        else:
            runs.append((part, 'F1'))
    return runs


def wrap_runs(runs, width, size, bold=False):
    """Lines of [(text, font)] no wider than width, broken at spaces (a long word is split by characters)."""
    tokens = []
    for text, font in runs:
        font = 'F2' if bold and font == 'F1' else font
        for piece in re.split(r'(\s+)', text):
            if piece:
                tokens.append((' ' if piece.isspace() else piece, font))
    lines, line, used = [], [], 0.0
    for text, font in tokens:
        w = text_width(text, font, size)
        if text == ' ':
            if line:
                line.append((text, font))
                used += w
            continue
        if used + w > width and line:
            while line and line[-1][0] == ' ':
                used -= text_width(' ', line[-1][1], size)
                line.pop()
            lines.append(line)
            line, used = [], 0.0
        while w > width:
            cut = len(text)
            while cut > 1 and text_width(text[:cut], font, size) > width:
                cut -= 1
            lines.append([(text[:cut], font)])
            text = text[cut:]
            w = text_width(text, font, size)
        line.append((text, font))
        used += w
    if line:
        lines.append(line)
    return lines or [[]]


class Document:
    """Pages of drawing operators with a cursor; written out by save()."""

    def __init__(self):
        self.pages = []
        self.y = 0.0
        self.image = None

    def new_page(self):
        """Start a page; the cursor goes to the top margin."""
        self.pages.append([])
        self.y = PAGE_H - MARGIN

    def ensure(self, height):
        """Start a new page when height does not fit above the bottom margin."""
        if not self.pages or self.y - height < MARGIN + 14:
            self.new_page()

    def draw(self, op):
        """Append a raw operator string to the current page."""
        self.pages[-1].append(op)

    def text_line(self, runs, x, size, leading):
        """Draw one line of runs at x and move the cursor down by leading."""
        self.y -= leading
        for text, font in runs:
            if text.strip() or text == ' ':
                self.draw('BT /%s %.1f Tf %.2f %.2f Td %s Tj ET' % (font, size, x, self.y, pdf_string(text)))
            x += text_width(text, font, size)

    def paragraph(self, text, size=10.0, leading=13.5, indent=0.0, bold=False, before=0.0, after=6.0, marker=None):
        """A wrapped paragraph; marker (a bullet or number) hangs left of the indent."""
        lines = wrap_runs(inline_runs(text), WIDTH - indent, size, bold)
        self.ensure(before + leading * min(len(lines), 2))
        self.y -= before
        for i, line in enumerate(lines):
            self.ensure(leading)
            if i == 0 and marker:
                mx = MARGIN + indent - 5 - text_width(marker, 'F1', size)
                self.draw('BT /F1 %.1f Tf %.2f %.2f Td %s Tj ET' % (size, mx, self.y - leading, pdf_string(marker)))
            self.text_line(line, MARGIN + indent, size, leading)
        self.y -= after

    def heading(self, text, level):
        """A heading in Helvetica-Bold; level 1 starts no new page by itself."""
        size, before = {1: (18.0, 6.0), 2: (14.0, 10.0), 3: (11.5, 8.0)}.get(level, (10.5, 6.0))
        self.ensure(before + size * 1.3 + 4.0 + 13.5 * 3)
        self.paragraph(text, size=size, leading=size * 1.3, bold=True, before=before, after=4.0)

    def code(self, lines):
        """A fenced code block in Courier on a light gray band, long lines wrapped by characters."""
        size, leading = 8.5, 10.5
        per_line = int((WIDTH - 12) / (600 * size / 1000.0))
        wrapped = []
        for line in lines:
            while len(line) > per_line:
                wrapped.append(line[:per_line])
                line = '    ' + line[per_line:]
            wrapped.append(line)
        for chunk_start in range(0, len(wrapped), 40):
            chunk = wrapped[chunk_start:chunk_start + 40]
            height = leading * len(chunk) + 8
            self.ensure(height + 4)
            self.draw('0.94 g %.2f %.2f %.2f %.2f re f 0 g' % (MARGIN, self.y - height, WIDTH, height))
            self.y -= 4
            for line in chunk:
                self.text_line([(line, 'F3')], MARGIN + 6, size, leading)
            self.y -= 8

    def table(self, rows):
        """A table with wrapped cells, a bold header row and rules between rows."""
        size, leading, pad = 8.5, 11.0, 3.0
        cols = max(len(r) for r in rows)
        rows = [r + [''] * (cols - len(r)) for r in rows]
        natural = [max(min(text_width(re.sub(r'[`*]', '', r[c]), 'F1', size), 260.0) for r in rows) + 2 * pad for c in range(cols)]
        floor = [max(text_width(word, 'F2', size) for r in rows for word in (re.sub(r'[`*]', '', r[c]).split() or [''])) + 2 * pad
                 for c in range(cols)]
        floor = [min(f, n) for f, n in zip(floor, natural)]
        widths = list(natural)
        excess = sum(widths) - WIDTH
        if excess > 0:
            room = [w - f for w, f in zip(widths, floor)]
            total_room = sum(room) or 1.0
            widths = [w - excess * r / total_room for w, r in zip(widths, room)]
        for index, row in enumerate(rows):
            cells = [wrap_runs(inline_runs(cell), widths[c] - 2 * pad, size, bold=(index == 0)) for c, cell in enumerate(row)]
            height = max(len(c) for c in cells) * leading + 4
            self.ensure(height)
            top = self.y
            x = MARGIN
            for c, lines in enumerate(cells):
                self.y = top
                for line in lines:
                    self.text_line(line, x + pad, size, leading)
                x += widths[c]
            self.y = top - height
            self.draw('0.7 G 0.4 w %.2f %.2f m %.2f %.2f l S 0 G' % (MARGIN, self.y + 1, MARGIN + WIDTH, self.y + 1))
        self.y -= 6

    def markdown(self, text):
        """Render the Markdown subset of a document, starting on a new page."""
        self.new_page()
        lines = text.splitlines()
        i = 0
        para = []

        def _flush():
            if para:
                self.paragraph(' '.join(s.strip() for s in para))
                para.clear()
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            if stripped.startswith('```'):
                _flush()
                block = []
                i += 1
                while i < len(lines) and not lines[i].strip().startswith('```'):
                    block.append(lines[i])
                    i += 1
                self.code(block)
            elif stripped.startswith('|'):
                _flush()
                rows = []
                while i < len(lines) and lines[i].strip().startswith('|'):
                    cells = [c.strip() for c in lines[i].strip().strip('|').split('|')]
                    if not all(re.fullmatch(r':?-{2,}:?', c) for c in cells if c):
                        rows.append(cells)
                    i += 1
                self.table(rows)
                continue
            elif stripped.startswith('#'):
                _flush()
                level = len(stripped) - len(stripped.lstrip('#'))
                self.heading(stripped[level:].strip(), level)
            elif re.match(r'^(\s*)([-*]|\d+\.)\s+', line):
                _flush()
                m = re.match(r'^(\s*)([-*]|\d+\.)\s+(.*)$', line)
                item = [m.group(3)]
                while i + 1 < len(lines) and lines[i + 1].strip() and not re.match(r'^\s*([-*]|\d+\.)\s+', lines[i + 1]) \
                        and not lines[i + 1].strip().startswith(('#', '|', '```')):
                    i += 1
                    item.append(lines[i].strip())
                depth = len(m.group(1)) // 2
                marker = '-' if m.group(2) in ('-', '*') else m.group(2)
                self.paragraph(' '.join(item), indent=22.0 + 14.0 * depth, marker=marker, after=3.0)
            elif not stripped or stripped.startswith('!['):
                _flush()
            else:
                para.append(line)
            i += 1
        _flush()

    def cover(self, version, subtitle):
        """Page 1: title, subtitle, author and version."""
        self.new_page()
        self.y = PAGE_H - 250
        self.paragraph(TITLE, size=26.0, leading=32.0, bold=True)
        self.paragraph(subtitle, size=14.0, leading=18.0, after=30.0)
        self.paragraph('Author: ' + AUTHOR, size=11.0)
        self.paragraph('Version ' + version, size=11.0)

    def flow_page(self, png_path):
        """Page 2: the flow diagram scaled to the page."""
        self.image = read_png(png_path)
        self.new_page()
        self.heading('Flow of the steps', 2)
        w, h = self.image['width'], self.image['height']
        avail_h = self.y - MARGIN - 20
        scale = min(WIDTH / w, avail_h / h)
        dw, dh = w * scale, h * scale
        x = MARGIN + (WIDTH - dw) / 2
        self.draw('q %.2f 0 0 %.2f %.2f %.2f cm /Im1 Do Q' % (dw, dh, x, self.y - dh - 6))

    def save(self, path, version):
        """Write the PDF objects, a page number footer on every page, the xref table and the trailer."""
        objects = []

        def add(body):
            objects.append(body)
            return len(objects)
        catalog = add(None)
        pages_id = add(None)
        font_ids = {}
        for key, (name, _) in FONTS.items():
            font_ids[key] = add(b'<< /Type /Font /Subtype /Type1 /BaseFont /' + name.encode() + b' /Encoding /WinAnsiEncoding >>')
        image_id = None
        if self.image:
            img = self.image
            colors = 3 if img['color'] == 2 else 1
            space = b'/DeviceRGB' if colors == 3 else b'/DeviceGray'
            head = (b'<< /Type /XObject /Subtype /Image /Width %d /Height %d /ColorSpace ' % (img['width'], img['height']) + space +
                    b' /BitsPerComponent 8 /Filter /FlateDecode /DecodeParms << /Predictor 15 /Colors %d /BitsPerComponent 8 /Columns %d >>'
                    b' /Length %d >>' % (colors, img['width'], len(img['data'])))
            image_id = add(head + b'\nstream\n' + img['data'] + b'\nendstream')
        resources = b'<< /Font << ' + b' '.join(b'/%s %d 0 R' % (k.encode(), v) for k, v in font_ids.items()) + b' >>'
        if image_id:
            resources += b' /XObject << /Im1 %d 0 R >>' % image_id
        resources += b' >>'
        page_ids = []
        total = len(self.pages)
        for number, ops in enumerate(self.pages, 1):
            footer = 'BT /F1 8 Tf %.2f %.2f Td %s Tj ET' % (MARGIN, MARGIN - 24, pdf_string('%s, version %s, page %d of %d' % (TITLE, version, number, total)))
            content = zlib.compress(('\n'.join(ops + [footer]) + '\n').encode('latin-1'))
            stream_id = add(b'<< /Length %d /Filter /FlateDecode >>\nstream\n' % len(content) + content + b'\nendstream')
            page_ids.append(add(b'<< /Type /Page /Parent %d 0 R /MediaBox [0 0 %d %d] /Resources ' % (pages_id, PAGE_W, PAGE_H) + resources +
                                b' /Contents %d 0 R >>' % stream_id))
        objects[catalog - 1] = b'<< /Type /Catalog /Pages %d 0 R >>' % pages_id
        objects[pages_id - 1] = b'<< /Type /Pages /Kids [' + b' '.join(b'%d 0 R' % p for p in page_ids) + b'] /Count %d >>' % len(page_ids)
        info = add(b'<< /Title ' + pdf_string(TITLE + ': step guide').encode('latin-1') + b' /Author ' + pdf_string(AUTHOR).encode('latin-1') +
                   b' /Subject ' + pdf_string('Version ' + version).encode('latin-1') + b' >>')
        out = bytearray(b'%PDF-1.4\n%\xe2\xe3\xcf\xd3\n')
        offsets = []
        for number, body in enumerate(objects, 1):
            offsets.append(len(out))
            out += b'%d 0 obj\n' % number + body + b'\nendobj\n'
        xref = len(out)
        out += b'xref\n0 %d\n0000000000 65535 f \n' % (len(objects) + 1)
        for offset in offsets:
            out += b'%010d 00000 n \n' % offset
        out += b'trailer\n<< /Size %d /Root %d 0 R /Info %d 0 R >>\nstartxref\n%d\n%%%%EOF\n' % (len(objects) + 1, catalog, info, xref)
        Path(path).write_bytes(bytes(out))
        return Path(path)


def read_png(path):
    """{'width', 'height', 'color', 'data'} of an 8-bit, non-interlaced RGB (2) or gray (0) PNG; data is the joined IDAT stream."""
    raw = Path(path).read_bytes()
    if raw[:8] != b'\x89PNG\r\n\x1a\n':
        raise SystemExit('%s is not a PNG file' % path)
    pos, idat, header = 8, [], None
    while pos < len(raw):
        length, kind = struct.unpack('>I4s', raw[pos:pos + 8])
        body = raw[pos + 8:pos + 8 + length]
        if kind == b'IHDR':
            header = struct.unpack('>IIBBBBB', body)
        elif kind == b'IDAT':
            idat.append(body)
        pos += 12 + length
    width, height, depth, color, _, _, interlace = header
    if depth != 8 or color not in (0, 2) or interlace:
        raise SystemExit('%s must be an 8-bit RGB or gray PNG without interlacing (found depth %d, color type %d)' % (path, depth, color))
    return {'width': width, 'height': height, 'color': color, 'data': b''.join(idat)}


def build(out=None, root=ROOT):
    """Build the guide and return its path."""
    sys.path.insert(0, str(root))
    from s2rebuild import __version__
    doc = Document()
    doc.cover(__version__, 'Step guide: the README and the step catalog')
    doc.flow_page(root / 'docs' / 'flow.png')
    doc.markdown((root / 'README.md').read_text(encoding='utf8'))
    doc.markdown((root / 'docs' / 'STEPS.md').read_text(encoding='utf8'))
    return doc.save(out or root / 'docs' / 's2rebuild-guide.pdf', __version__)


def main(argv=None):
    """Command line entry: python docs/build_pdf.py [--out FILE]."""
    parser = argparse.ArgumentParser(description='Build docs/s2rebuild-guide.pdf from README.md and docs/STEPS.md')
    parser.add_argument('--out', help='output file (default docs/s2rebuild-guide.pdf)')
    args = parser.parse_args(argv)
    path = build(args.out)
    print('guide written to %s (%d bytes)' % (path, path.stat().st_size))
    return 0


if __name__ == '__main__':
    sys.exit(main())
