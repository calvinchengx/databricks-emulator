#!/usr/bin/env python3
"""The landing page tells the truth about where it points and what it ships.

site/index.html is hand-written and copied over the built site's root. Astro
never sees it, so none of the checks that protect the docs protect it: a
renamed doc, a moved anchor, a missing image or a manifest that stopped being
published would all publish silently as a broken front page.

Four checks, each from a way this can go wrong:

  links      every relative href and src resolves inside the assembled site
  anchors    every same-page fragment names an id that exists
  version    the release pill names the newest v* tag, so the front page cannot
             advertise a version that was superseded
  counts     the evidence numbers in the hero are the ones witnesses.json and
             parity.md actually hold

The counts check is the one this repo needed most. The page advertises "31
parity claims, every one witnessed" and "31 of those proved by an outside
client", and those are exactly the kind of number that is true the day it is
typed and quietly wrong a month later -- the front page would go on
advertising a coverage figure the ledger had stopped supporting. They are read
back from the same files check_witnesses.py reads.

The link check FAILS IF IT FINDS NOTHING TO CHECK. A regex that quietly stops
matching reports a clean run over zero links, which is indistinguishable from
success and is how this class of checker dies.

Usage:
    check_landing_page.py --site DIR      DIR is the assembled site root
    check_landing_page.py --site DIR --skip-version
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SOURCE = REPO / "site" / "index.html"

# href="..." / src="..." with a double-quoted value.
REF_RE = re.compile(r'(?:href|src)="([^"]+)"')
# This page carries its numbers statically and has them checked below, rather
# than fetching a manifest at runtime. One less thing to serve, and a number
# that is wrong fails the build instead of rendering as a wrong number.
FETCH_RE = re.compile(r"fetch\('([^']+)'\)")
ID_RE = re.compile(r'\sid="([^"]+)"')
# <div class="stat"><b>31</b><span>parity claims, every one witnessed</span>
STAT_RE = re.compile(r'<div class="stat"><b>([0-9]+)</b><span>([^<]+)</span>')
PILL_RE = re.compile(r'class="release-pill"[^>]*>\s*<span>(v[0-9][^<]*)</span>')

EXTERNAL = ("http://", "https://", "mailto:", "data:", "//")


def newest_tag() -> str | None:
    out = subprocess.run(
        ["git", "-C", str(REPO), "tag", "--list", "v*", "--sort=-v:refname"],
        capture_output=True, text=True,
    )
    tags = [t for t in out.stdout.split() if t]
    return tags[0] if tags else None


def ledger_counts() -> dict[str, int]:
    """The numbers the ledger actually holds.

    IMPORTS check_witnesses rather than re-deriving them. The first version of
    this counted red rows with its own regex over parity.md and got 27 where
    check_witnesses reports 26 -- it was matching the LEGEND table, which
    explains the glyphs and claims nothing. The gate passed, because the page
    and the checker were wrong in the same way: a second parser is a second
    opinion, and a checker that holds a page to its own opinion is checking
    nothing. One parser, and it is the one CI already trusts.
    """
    sys.path.insert(0, str(REPO / "scripts"))
    import check_witnesses

    witnesses = json.loads(check_witnesses.MANIFEST.read_text(encoding="utf-8"))
    return {
        "claims": len(witnesses),
        "ci": sum(1 for v in witnesses.values()
                  if any(w.startswith("ci:") for w in v["witnesses"])),
        "red": check_witnesses.grade_counts()["\U0001F534"],
    }


# Hero stat -> which ledger number it must equal. Keyed by the wording on the
# page, so rewording a stat without revisiting this fails loudly rather than
# silently stopping being checked.
STAT_CLAIMS = {
    "parity claims, every one witnessed": "claims",
    "of those proved by an outside client": "ci",
    "surfaces it openly does not implement": "red",
}


def resolve(site: Path, target: str) -> Path:
    """Where a relative reference from the site root lands on disk."""
    clean = target.split("#", 1)[0].split("?", 1)[0]
    clean = clean[2:] if clean.startswith("./") else clean
    if clean in ("", "/"):
        return site / "index.html"
    path = site / clean.lstrip("/")
    return path / "index.html" if target.rstrip("#").endswith("/") else path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--site", required=True, help="the assembled site root")
    ap.add_argument("--skip-version", action="store_true")
    args = ap.parse_args()
    site = Path(args.site).resolve()

    served = site / "index.html"
    if not served.is_file():
        print(f"FAIL: {served} does not exist — the landing page was never copied in.")
        return 1
    html = served.read_text(encoding="utf-8")

    if html != SOURCE.read_text(encoding="utf-8"):
        print(f"FAIL: {served} differs from {SOURCE} — something rewrote the page after assembly.")
        return 1

    ids = set(ID_RE.findall(html))
    failures: list[str] = []
    checked = anchors = 0

    for ref in REF_RE.findall(html) + FETCH_RE.findall(html):
        if ref.startswith(EXTERNAL):
            continue
        if ref.startswith("#"):
            anchors += 1
            if ref[1:] not in ids:
                failures.append(f"anchor {ref} names no id on the page")
            continue
        checked += 1
        landing = resolve(site, ref)
        if not landing.exists():
            failures.append(f"{ref} → {landing.relative_to(site)} does not exist")
        # "./#evidence" is both a link to the root and a same-page fragment.
        if "#" in ref and landing == served:
            anchors += 1
            frag = ref.split("#", 1)[1]
            if frag and frag not in ids:
                failures.append(f"anchor #{frag} names no id on the page")

    # A checker that matched nothing is not a passing checker.
    if checked == 0:
        failures.append("no relative links were found at all — REF_RE has stopped matching")
    if anchors == 0:
        failures.append("no same-page anchors were found at all — the nav should have several")

    if not args.skip_version:
        tag = newest_tag()
        pill = PILL_RE.search(html)
        if tag is None:
            failures.append("no v* tag is visible — checkout needs fetch-tags, or the check "
                            "would pass by seeing nothing")
        elif pill is None:
            failures.append("the release pill's version could not be read from the page")
        elif pill.group(1) != tag:
            failures.append(f"the release pill says {pill.group(1)}, the newest tag is {tag}")

    ledger = ledger_counts()
    seen_stats = 0
    for value, wording in STAT_RE.findall(html):
        key = STAT_CLAIMS.get(wording.strip())
        if key is None:
            continue
        seen_stats += 1
        if int(value) != ledger[key]:
            failures.append(
                f"the hero says {value} for {wording.strip()!r}; the ledger holds "
                f"{ledger[key]}"
            )
    if seen_stats != len(STAT_CLAIMS):
        failures.append(
            f"only {seen_stats} of {len(STAT_CLAIMS)} checkable hero stats were found -- a stat "
            f"was reworded or removed, so it has stopped being checked rather than started "
            f"being wrong"
        )

    if failures:
        print("FAIL: the landing page does not hold up:")
        for f in failures:
            print(f"  {f}")
        return 1

    print(f"landing page: {checked} relative link(s) and {anchors} anchor(s) resolve in {site}; "
          f"{seen_stats} hero stat(s) match the ledger "
          f"({ledger['claims']} claims, {ledger['ci']} with a ci: witness, {ledger['red']} red)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
