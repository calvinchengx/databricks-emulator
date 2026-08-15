#!/usr/bin/env python3
"""Every witnesses.json claim must name a test or e2e script that exists.

A 🟢 parity row is not support without a witness. This checker does not
invent keys from the table — it verifies the explicit map the repo already
keeps, and that `supported` matches the number of claims.

A claim is either `file.go:TestName` (a Go test) or a path to an existing
file (an unmodified-client e2e script).
"""

from __future__ import annotations

import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "docs" / "witnesses.json"


def main() -> int:
    manifest = json.loads(MANIFEST.read_text())
    claims = manifest.get("claims") or {}
    supported = manifest.get("supported")
    if supported != len(claims):
        print(f"FAIL: supported={supported} but claims={len(claims)}")
        return 1

    dangling = []
    for key, ref in claims.items():
        if ":" not in ref:
            if (ROOT / ref).is_file():
                continue
            dangling.append(f"{key} → {ref} (no such file)")
            continue
        path, name = ref.rsplit(":", 1)
        src = ROOT / path
        if not src.is_file():
            dangling.append(f"{key} → {ref} (no such file)")
            continue
        if not re.search(rf"^func {re.escape(name)}\(", src.read_text(), re.M):
            dangling.append(f"{key} → {ref} (no such test)")

    print(f"witnessed claims: {len(claims)}")
    if dangling:
        print("dangling witnesses:")
        for d in dangling:
            print(f"  {d}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
