// Parity-map versioning, driven entirely by git tags (no frozen copies to
// maintain). The canonical parity map lives in /docs; each `v*` release tag is
// a snapshot git already holds. This module, called from sync-docs.mjs:
//
//   - resolves the version string from `git describe --tags`;
//   - for every `v*` tag that contains a parity file, emits a read-only
//     snapshot page under parity-history/<version>/;
//   - generates a changelog by diffing the parity tables between consecutive
//     versions (which rows changed status, were added, or removed);
//   - writes a parity-history index linking the live map + every snapshot.
//
// v0.1.0 already carries docs/parity.md. Tags that predate the map can still
// be reconstructed via docs/parity-snapshots/<tag>.md if one is ever needed.
import { execSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const PARITY_RE = /(^|\/)parity\.md$/;

function git(repo, args) {
  return execSync(`git ${args}`, { cwd: repo, stdio: ['ignore', 'pipe', 'ignore'] }).toString();
}

export function gitVersion(repo) {
  try {
    const exact = git(repo, 'describe --tags --exact-match --match v*').trim();
    if (exact) return exact;
  } catch {
    /* not on a release tag */
  }
  try {
    const sha = git(repo, 'rev-parse --short HEAD').trim();
    if (sha) return `latest-${sha}`;
  } catch {
    /* not a git checkout */
  }
  return 'latest';
}

const isRelease = (v) => /^v\d+\.\d+\.\d+$/.test(v);

function parityPathAt(repo, ref) {
  try {
    return git(repo, `ls-tree --name-only ${ref} docs/`).split('\n').find((n) => PARITY_RE.test(n)) || null;
  } catch {
    return null;
  }
}

function releaseTags(repo) {
  try {
    return git(repo, 'tag --list v* --sort=v:refname').split('\n').filter(Boolean);
  } catch {
    return [];
  }
}

const isSeparatorRow = (l) => /^\s*\|[\s:|-]+\|\s*$/.test(l || '') && l.includes('-');

function parseParity(md) {
  const map = new Map();
  const lines = md.split('\n');
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!/^\s*\|/.test(line)) continue;
    if (isSeparatorRow(line)) continue;
    if (isSeparatorRow(lines[i + 1])) continue;
    const cells = line.split('|').slice(1, -1).map((c) => c.trim());
    if (cells.length < 3) continue;
    const feature = cells[0].replace(/[*`]/g, '').trim();
    if (!feature) continue;
    const status = cells[cells.length - 1];
    const emoji = (status.match(/🟢|🟡|🟠|🔴/) || [''])[0];
    map.set(feature, { emoji, status });
  }
  return map;
}

function statusTally(map) {
  const t = { '🟢': 0, '🟡': 0, '🟠': 0, '🔴': 0 };
  for (const { emoji } of map.values()) if (t[emoji] !== undefined) t[emoji]++;
  return t;
}

function tallyLine(t) {
  const parts = [];
  if (t['🟢']) parts.push(`${t['🟢']} 🟢 Real`);
  if (t['🟡']) parts.push(`${t['🟡']} 🟡 Emulated`);
  if (t['🟠']) parts.push(`${t['🟠']} 🟠 BYO-engine`);
  if (t['🔴']) parts.push(`${t['🔴']} 🔴 Not implemented`);
  return parts.join(' · ');
}

function diffParity(prev, cur) {
  const added = [];
  const removed = [];
  const changed = [];
  for (const [f, v] of cur) {
    if (!prev.has(f)) added.push({ f, to: v.emoji });
    else if (prev.get(f).emoji !== v.emoji) changed.push({ f, from: prev.get(f).emoji, to: v.emoji });
  }
  for (const f of prev.keys()) if (!cur.has(f)) removed.push({ f });
  return { added, removed, changed };
}

const versionSlug = (v) => v.replace(/[.+]/g, '-');
const BASE = '/databricks-emulator/';

export function collectParity(repo) {
  const version = gitVersion(repo);
  const tags = releaseTags(repo);
  let headSha = '';
  try {
    headSha = git(repo, 'rev-parse HEAD').trim();
  } catch {
    /* not a git checkout */
  }
  const points = [];
  for (const tag of tags) {
    try {
      if (git(repo, `rev-parse ${tag}^{commit}`).trim() === headSha) continue;
    } catch {
      /* ignore an unreadable tag */
    }
    const p = parityPathAt(repo, tag);
    if (p) {
      points.push({ label: tag, released: true, md: git(repo, `show ${tag}:${p}`) });
      continue;
    }
    const backfill = join(repo, 'docs', 'parity-snapshots', `${tag}.md`);
    if (existsSync(backfill)) {
      points.push({ label: tag, released: true, reconstructed: true, md: readFileSync(backfill, 'utf8') });
    }
  }
  const docsDir = join(repo, 'docs');
  let liveName = null;
  try {
    liveName = readdirSync(docsDir).find((n) => PARITY_RE.test(n)) ?? null;
  } catch {
    /* no docs dir */
  }
  const liveMd = liveName ? readFileSync(join(docsDir, liveName), 'utf8') : '';
  const liveSlug = liveName ? liveName.replace(/\.md$/, '') : null;
  points.push({ label: version, released: isRelease(version), latest: true, md: liveMd });
  return { version, liveSlug, points, firstTag: tags[0] ?? null };
}

export function pointUrl(parity, pt) {
  return pt.latest ? `${BASE}${parity.liveSlug}/` : `${BASE}parity-history/${versionSlug(pt.label)}/`;
}

export function parityManifest(parity) {
  return {
    liveSlug: parity.liveSlug,
    points: parity.points
      .slice()
      .reverse()
      .map((pt) => ({
        label: pt.label,
        url: pointUrl(parity, pt),
        latest: !!pt.latest,
        reconstructed: !!pt.reconstructed,
      })),
  };
}

export function writeParityHistory(OUT, parity, helpers) {
  const { version, liveSlug, points } = parity;
  const outDir = join(OUT, 'parity-history');
  mkdirSync(outDir, { recursive: true });

  for (const pt of points) {
    if (pt.latest) continue;
    const slug = versionSlug(pt.label);
    const body = helpers.convertBody(pt.md);
    const fm = `---\ntitle: ${JSON.stringify(`Parity — ${pt.label}`)}\neditUrl: false\nprev: false\nnext: false\n---\n\n`;
    const banner = pt.reconstructed
      ? ''
      : `:::note[Historical snapshot]\nThe feature-parity map as of release **${pt.label}**. The current map is on the [Parity page](${BASE}${liveSlug}/).\n:::\n\n`;
    writeFileSync(join(outDir, `${slug}.md`), fm + banner + body);
  }

  const liveMd = points.find((p) => p.latest)?.md ?? '';

  const cl = [];
  for (let i = 1; i < points.length; i++) {
    const a = parseParity(points[i - 1].md);
    const b = parseParity(points[i].md);
    const { added, removed, changed } = diffParity(a, b);
    const to = points[i].label;
    cl.push(`## ${points[i - 1].label} → ${to}\n`);
    if (!added.length && !removed.length && !changed.length) {
      cl.push('_No parity changes._\n');
      continue;
    }
    for (const c of changed) cl.push(`- **${c.f}**: ${c.from || '—'} → ${c.to || '—'}`);
    for (const a2 of added) cl.push(`- **${a2.f}**: added ${a2.to || ''}`.trim());
    for (const r of removed) cl.push(`- **${r.f}**: removed`);
    cl.push('');
  }

  const liveTally = liveMd ? tallyLine(statusTally(parseParity(liveMd))) : '';
  const releasedPoints = points.filter((p) => p.released && !p.latest);

  const clFm = `---\ntitle: Parity changelog\neditUrl: false\n---\n\n`;
  const clBody =
    `How the [feature-parity map](${BASE}${liveSlug}/) changed across releases — ` +
    `generated by diffing the parity tables between consecutive \`v*\` tags.\n\n` +
    (liveTally ? `**Current (${version}):** ${liveTally}.\n\n` : '') +
    (cl.length
      ? cl.join('\n')
      : `_No tagged release includes the parity map yet — the map was introduced after ${parity.firstTag ?? 'the first tag'}. ` +
        `The first entry here appears when a release ships that carries it._\n`);
  writeFileSync(join(outDir, 'changelog.md'), clFm + clBody);

  const idxFm = `---\ntitle: Parity history\neditUrl: false\n---\n\n`;
  const rows = [
    `- **[${version}](${BASE}${liveSlug}/)** — the live map on \`main\``,
    ...releasedPoints
      .slice()
      .reverse()
      .map(
        (p) =>
          `- [${p.label}](${BASE}parity-history/${versionSlug(p.label)}/) — ` +
          (p.reconstructed ? 'written retrospectively (predates the map)' : 'snapshot at release'),
      ),
  ];
  const idxBody =
    `Versions of the [feature-parity map](${BASE}${liveSlug}/), tracked by git release tags. ` +
    `See the [parity changelog](${BASE}parity-history/changelog/) for what changed between them.\n\n` +
    rows.join('\n') +
    '\n\n' +
    (releasedPoints.length
      ? ''
      : `:::note\nOnly the unreleased tip carries a parity map so far (it was added after \`${parity.firstTag ?? 'v0.1.0'}\`). ` +
        `Each future \`vX.Y.Z\` release will appear above automatically.\n:::\n`);
  writeFileSync(join(outDir, 'index.md'), idxFm + idxBody);

  return { version, snapshots: releasedPoints.map((p) => p.label), liveSlug };
}
