// Generates Starlight content from the canonical Markdown in /docs.
import { readdirSync, readFileSync, writeFileSync, rmSync, mkdirSync, existsSync, statSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(here, '..', '..');
const DOCS_SRC = join(REPO_ROOT, 'docs');
const OUT = join(here, '..', 'src', 'content', 'docs');
export const BASE = '/databricks-emulator/';
const REPO = 'https://github.com/calvinchengx/databricks-emulator';

const DOC_RE = /^(\d{2}-[a-z0-9-]+|parity)\.md$/;
const LINK_RE = /\]\((?:\.\/|docs\/)?(\d{2}-[a-z0-9-]+|parity)\.md(#[^)]*)?\)/g;
const REPO_LINK_RE = /\]\(\.\.\/([^)#]+)(#[^)]*)?\)/g;

function rewriteRepoLinks(md, where) {
  return md.replace(REPO_LINK_RE, (_m, path, anchor) => {
    const clean = path.replace(/\/+$/, '');
    const target = join(REPO_ROOT, clean);
    const exists = existsSync(target);
    if (!exists) {
      console.warn(`sync-docs: WARNING ${where}: ../${path} matches nothing in the repo`);
    }
    const kind = exists && statSync(target).isDirectory() ? 'tree' : 'blob';
    return `](${REPO}/${kind}/main/${clean}${anchor ?? ''})`;
  });
}

function rewriteLinks(md, where = 'docs') {
  return rewriteRepoLinks(
    md
      .replace(/\]\(witnesses\.json\)/g, `](${REPO}/blob/main/docs/witnesses.json)`)
      .replace(LINK_RE, (_m, slug, anchor) => `](${BASE}${slug}/${anchor ?? ''})`),
    where,
  );
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
  const body = rewriteLinks(lines.join('\n').replace(/^\n+/, ''), name);
  const editUrl = `${REPO}/edit/main/docs/${name}`;
  return `---\ntitle: ${yamlEscape(title)}\neditUrl: ${yamlEscape(editUrl)}\n---\n\n` + body;
}

function writeIndex() {
  const body = rewriteLinks(
    `Local emulator of a **Databricks workspace** in a single Go binary — ` +
      `PAT and this process's own OIDC, workspace files, Jobs, SQL warehouses, ` +
      `and an attached Spark engine. The bet is the family's: **terminate the ` +
      `public REST, attach a real engine, refuse what you cannot compute.** ` +
      `Entra is an optional federated issuer, not a required STS.\n\n` +
      `:::caution\nLocal development tool only — self-signed TLS, a seeded admin PAT. ` +
      `It emulates the workspace **contract**, not a security boundary. Run it on ` +
      `\`localhost\` only. \`token=dev\` is 401.\n:::\n\n` +
      `## Start here\n\n` +
      `- [Doctrine](00-doctrine.md) — the founding constraint\n` +
      `- [Quickstart](01-quickstart.md) — seeded PAT, official SDK \`Me\`, \`token=dev\` is 401\n` +
      `- [Installation](02-installation.md) — source, GHCR, family compose\n` +
      `- [Architecture](03-architecture.md) — this process vs Sail vs UC vs vault\n` +
      `- [Configuration](04-configuration.md) — every \`DATABRICKS_*\` variable\n` +
      `- [TLS and hosts](05-tls-and-hosts.md) — self-signed cert, HTTP opt-out\n` +
      `- [Identity](06-identity.md) — PAT, emulator OIDC, federated JWT\n` +
      `- [Workspace and files](07-workspace-and-files.md) — SOURCE/PYTHON, workspace-files, DBFS\n` +
      `- [Jobs and the Spark attach](08-jobs-and-spark.md) — Sail; no engine means fail, never SUCCESS\n` +
      `- [Secrets](09-secrets.md) — persist, injection, AKV read-through\n` +
      `- [SQL warehouses and MCP](10-sql-and-mcp.md) — dialect spark-sql, not Photon\n` +
      `- [Clusters and Connect](11-clusters-and-connect.md) — session handle; gRPC URL is not the HTTP agent\n` +
      `- [Unity Catalog](12-unity-catalog.md) — UC OSS proxy; grants stay 501\n` +
      `- [Testing](13-testing.md) — what \`e2e-sdk\` / \`e2e-terraform\` / \`e2e-engine\` each prove\n` +
      `- [Family integration](14-family-integration.md) — entra, keyvault, fabric activities, chain test\n` +
      `- [Roadmap](15-roadmap.md) — next honest attaches; permanently red\n` +
      `- [Parity ledger](parity.md) — catalog is the workspace REST API reference\n`,
    'index',
  );
  const frontmatter =
    `---\ntitle: Databricks Emulator\ndescription: A local emulator of a Databricks workspace — PAT and OIDC identity, workspace files, Jobs, and an attached Spark engine — refuse what you cannot compute.\neditUrl: false\n---\n\n`;
  writeFileSync(join(OUT, 'index.md'), frontmatter + body);
}

rmSync(OUT, { recursive: true, force: true });
mkdirSync(OUT, { recursive: true });

const names = readdirSync(DOCS_SRC).filter((n) => DOC_RE.test(n)).sort();
for (const name of names) {
  writeFileSync(join(OUT, name), convert(join(DOCS_SRC, name), name));
}
writeIndex();

console.log(`sync-docs: wrote ${names.length} docs + index to src/content/docs/`);
