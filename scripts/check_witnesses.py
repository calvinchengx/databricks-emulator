#!/usr/bin/env python3
"""Every 🟢 parity claim must name the test that witnesses it.

A row is graded 🟢 only when an unmodified client drove the call and the
attached engine or store did the work. This checker makes that mapping
explicit and verifiable.

Witness kinds, deliberately distinguished because they are not equal evidence:

  ci:<job>      a CI job driving a real external client (strongest)
  go:<Test>     a Go test: real HTTP, real store, but our own client
  boundary:...  the claim is scoped by a documented limitation
  TODO          not yet identified — the point of --strict

Usage:
    check_witnesses.py            report the mapping; fail on missing/dangling
    check_witnesses.py --strict   also fail on TODO
"""
from __future__ import annotations

import json
import pathlib
import re
import subprocess
import sys

# Windows stdout is cp1252. This script prints the ledger glyphs; without
# UTF-8 on stdout, `make-targets` dies on UnicodeEncodeError for 🟢.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = pathlib.Path(__file__).resolve().parent.parent
PARITY = ROOT / "docs" / "parity.md"
MANIFEST = ROOT / "docs" / "witnesses.json"
CI = ROOT / ".github" / "workflows" / "ci.yml"

# Sections that do not make capability claims. Names must match THIS repo's
# headings — family_parity.py parses this set out of the source.
SKIP_SECTIONS = {
    "Legend",
    "Scope boundary: the workspace REST, not Databricks Runtime",
}


def key_for(feature: str) -> str:
    """A stable-ish key from the row's feature cell: markdown and punctuation
    stripped, lowercased. Rewording a claim changes its key and trips the
    checker — that is intended, since a reworded claim deserves a fresh look
    at whether its witness still covers it."""
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", feature)
    text = re.sub(r"[*`_]", "", text)
    text = re.sub(r"[^a-z0-9]+", "-", text.lower())
    return text.strip("-")


def green_claims():
    """Yield (section, feature, key) for every row claiming 🟢."""
    section = None
    for line in PARITY.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            section = line[3:].strip()
            continue
        if not line.startswith("| ") or section is None or section in SKIP_SECTIONS:
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3:
            continue
        if cells[0] in ("Feature", "Emulator", "Type", "Surface", "What would make it real", "Status") or set(cells[0]) <= set("-"):
            continue
        if "🟢" in cells[-1]:
            yield section, cells[0], key_for(cells[0])


def grade_counts() -> dict[str, int]:
    """Capability-row grades, using the same skip list as green_claims."""
    counts = {"🟢": 0, "🟡": 0, "🟠": 0, "🔴": 0}
    section = None
    for line in PARITY.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            section = line[3:].strip()
            continue
        if not line.startswith("| ") or section is None or section in SKIP_SECTIONS:
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3:
            continue
        if cells[0] in ("Feature", "Emulator", "Type", "Surface", "What would make it real", "Status") or set(cells[0]) <= set("-"):
            continue
        last = cells[-1]
        for g in counts:
            if g in last:
                counts[g] += 1
                break
    return counts


def ci_job_ids() -> set[str]:
    return set(re.findall(r"^  ([a-z0-9-]+):$", CI.read_text(encoding="utf-8"), re.M))


def go_test_names() -> set[str]:
    out = subprocess.run(
        [
            "grep",
            "-rhoE",
            r"^func (Test[A-Za-z0-9_]+)",
            "--include=*_test.go",
            str(ROOT / "internal"),
            str(ROOT / "cmd"),
        ],
        capture_output=True,
        text=True,
    )
    return {line.split()[1] for line in out.stdout.splitlines() if line.startswith("func ")}


def main() -> int:
    strict = "--strict" in sys.argv
    raw = json.loads(MANIFEST.read_text(encoding="utf-8")) if MANIFEST.exists() else {}
    # Family shape is a flat map of claim keys. A "claims" wrapper is accepted
    # so an older checkout of this file still parses.
    manifest = raw.get("claims", raw) if isinstance(raw, dict) else {}
    jobs, tests = ci_job_ids(), go_test_names()

    missing, dangling, todo, malformed = [], [], [], []
    kinds = {"ci": 0, "go": 0, "boundary": 0}
    shared: dict[str, list[str]] = {}

    claims = list(green_claims())
    for section, feature, key in claims:
        entry = manifest.get(key)
        if entry is None:
            missing.append((section, feature, key))
            continue
        witnesses = entry.get("witnesses") if isinstance(entry, dict) else None
        if not isinstance(witnesses, list):
            malformed.append(f"{key} (need a witnesses list, not {type(entry).__name__})")
            continue
        for witness in witnesses:
            if witness == "TODO":
                todo.append((section, feature))
                continue
            kind, _, name = witness.partition(":")
            kinds[kind] = kinds.get(kind, 0) + 1
            shared.setdefault(witness, []).append(feature)
            if kind == "ci" and name not in jobs:
                dangling.append(f"{key} → {witness} (no such CI job)")
            elif kind == "go" and name not in tests:
                dangling.append(f"{key} → {witness} (no such Go test)")

    print(f"🟢 capability claims: {len(claims)}")
    print(f"  witnessed by a real external client (ci:) : {kinds.get('ci', 0)}")
    print(f"  witnessed by our own Go tests (go:)       : {kinds.get('go', 0)}")
    print(f"  scoped by a documented boundary           : {kinds.get('boundary', 0)}")
    print(f"  not yet identified (TODO)                 : {len(todo)}")
    print(f"  absent from the manifest                  : {len(missing)}")

    grades = grade_counts()
    print(
        f"ledger grades: 🟢 {grades['🟢']}  🟡 {grades['🟡']}  "
        f"🟠 {grades['🟠']}  🔴 {grades['🔴']}"
    )

    extra = [k for k in manifest if not str(k).startswith(("_", "$")) and k not in {c[2] for c in claims}]
    if extra:
        print("\nManifest keys with no 🟢 row:")
        for k in extra:
            print(f"  {k}")

    heavy = sorted(((w, c) for w, c in shared.items() if len(c) > 3), key=lambda x: -len(x[1]))
    if heavy:
        print("\nWitnesses carrying many claims (check none is over-credited):")
        for witness, covered in heavy[:5]:
            print(f"  {witness}: {len(covered)} claims")

    if missing:
        print("\nClaims with no manifest entry:")
        for section, feature, key in missing:
            print(f"  [{section}] {feature[:70]}\n      key: {key}")
    if malformed:
        print("\nMalformed manifest entries:")
        for m in malformed:
            print(f"  {m}")
    if dangling:
        print("\nDangling witness references:")
        for d in dangling:
            print(f"  {d}")

    if missing or dangling or malformed or (strict and todo):
        print("\nFAIL: every 🟢 claim needs an identified, existing witness.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
