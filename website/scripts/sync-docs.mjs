// Generates Starlight content from the canonical Markdown in /docs.
import { readdirSync, readFileSync, writeFileSync, rmSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const DOCS_SRC = join(here, '..', '..', 'docs');
const OUT = join(here, '..', 'src', 'content', 'docs');
export const BASE = '/databricks-emulator/';
const REPO = 'https://github.com/calvinchengx/databricks-emulator';

const DOC_RE = /^(\d{2}-[a-z0-9-]+|parity|index)\.md$/;
const LINK_RE = /\]\((?:\.\/|docs\/)?(\d{2}-[a-z0-9-]+|parity|index)\.md(#[^)]*)?\)/g;

function rewriteLinks(md) {
  return md
    .replace(/\]\(witnesses\.json\)/g, `](${REPO}/blob/main/docs/witnesses.json)`)
    .replace(LINK_RE, (_m, slug, anchor) =>
      `](${BASE}${slug === 'index' ? '' : slug + '/'}${anchor ?? ''})`);
}

function cleanTitle(h1) {
  return h1.replace(/^\d+[a-z]?\s*[—:-]\s*/i, '').trim();
}

function yamlEscape(s) {
  return '"' + s.replace(/\\/g, '\\\\').replace(/"/g, '\\"') + '"';
}

function convert(srcPath, name) {
  const raw = readFileSync(srcPath, 'utf8');
  const lines = raw.split('\n');
  const h1Index = lines.findIndex((l) => /^#\s+/.test(l));
  const title = h1Index >= 0
    ? cleanTitle(lines[h1Index].replace(/^#\s+/, ''))
    : name.replace(/\.md$/, '');
  if (h1Index >= 0) {
    lines.splice(h1Index, lines[h1Index + 1]?.trim() === '' ? 2 : 1);
  }
  const body = rewriteLinks(lines.join('\n').replace(/^\n+/, ''));
  const editUrl = `${REPO}/edit/main/docs/${name}`;
  return `---\ntitle: ${yamlEscape(title)}\neditUrl: ${yamlEscape(editUrl)}\n---\n\n` + body;
}

rmSync(OUT, { recursive: true, force: true });
mkdirSync(OUT, { recursive: true });

const names = readdirSync(DOCS_SRC).filter((n) => DOC_RE.test(n)).sort();
for (const name of names) {
  writeFileSync(join(OUT, name), convert(join(DOCS_SRC, name), name));
}

console.log(`sync-docs: wrote ${names.length} docs to src/content/docs/`);
