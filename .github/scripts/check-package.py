"""Check the actual archive, then extract it for independent consumers."""

import html
import json
import os
import pathlib
import re
import sys
import tarfile
import tomllib
from html.parser import HTMLParser
from urllib.parse import unquote, urlsplit


class HtmlLinks(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        self.links.extend(
            value for name, value in attrs if name in {"href", "src"} and value
        )


def relative_links(text):
    # Examples inside code fences are source examples rather than rendered links.
    text = re.sub(r"(?m)^(`{3,}|~{3,}).*\n[\s\S]*?^\1\s*$", "", text)
    text = re.sub(r"<!--[\s\S]*?-->", "", text)
    parser = HtmlLinks()
    parser.feed(text)
    links = parser.links
    links.extend(re.findall(r"!?\[[^\]]*\]\((<[^>]+>|[^\s)]+)(?:\s+[^)]*)?\)", text))
    links.extend(re.findall(r"(?m)^\s*\[[^\]]+\]:\s*(<[^>]+>|\S+)", text))
    for link in links:
        link = html.unescape(link.strip("<>"))
        parts = urlsplit(link)
        if not parts.scheme and not parts.netloc and parts.path:
            yield unquote(parts.path)

destination = pathlib.Path(sys.argv[1])
manifest = tomllib.loads(pathlib.Path('Cargo.toml').read_text())
package = manifest['package']
target = pathlib.Path(os.environ.get('CARGO_TARGET_DIR', 'target'))
archive = target / 'package' / f"{package['name']}-{package['version']}.crate"
prefix = f"{package['name']}-{package['version']}"
forbidden = {'AGENTS.md', 'CLAUDE.md', 'GEMINI.md', '.agents', '.codex', '.github'}
with tarfile.open(archive) as contents:
    names = contents.getnames()
    for member in contents.getmembers():
        name = member.name
        path = pathlib.PurePosixPath(name)
        if path.is_absolute() or '..' in path.parts or path.parts[0] != prefix:
            sys.exit(f'Unexpected archive path: {name}')
        if not (member.isfile() or member.isdir()):
            sys.exit(f'Unsupported archive member: {name}')
        if forbidden.intersection(path.parts):
            sys.exit(f'Private/process file in package: {name}')
        if name.endswith(('docs/spec-fix-all-audit.md', 'docs/audit-repair-matrix.json', 'docs/IMPLEMENTATION_HISTORY.md', 'docs/PERFORMANCE_HISTORY_TC0.md')) or '/docs/engineering/' in name or '/docs/benchmarks/' in name:
            sys.exit(f'Internal process document in package: {name}')
    required = {f'{prefix}/{name}' for name in ('Cargo.toml', 'src/lib.rs', 'README.md', 'LICENSE')}
    missing = required.difference(names)
    if missing:
        sys.exit('Missing package files: ' + ', '.join(sorted(missing)))
    contents.extractall(destination)

source = destination / prefix
root = source.resolve()
documents = sorted(source.rglob('*.md'))
for document in documents:
    for target_link in relative_links(document.read_text()):
        target_file = (document.parent / target_link).resolve()
        if not target_file.is_relative_to(root) or not target_file.exists():
            sys.exit(f'Broken relative link in {document.relative_to(source)}: {target_link}')

(destination / 'package.json').write_text(json.dumps({'version': package['version'], 'source': str(source.resolve())}))
print(f"Verified {len(names)} archive files and {len(documents)} Markdown documents; consumer source: {source}")
