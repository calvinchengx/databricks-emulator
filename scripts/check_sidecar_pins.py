#!/usr/bin/env python3
"""Every sidecar reference agrees with e2e/sidecars.env, and is pinned.

WHY THIS EXISTS. Ten compose files each carried their own copy of the Sail and
spark-agent pins, and nothing compared them. By September they had split: six
ran the emulator-sail build from fabric-emulator v0.27.0, four the v0.30.0
build, the agent came from v0.32.0 -- all under tags that still read 0.7.0 and
4.2.0, because those tags are rebuilt by every fabric release. Every stack
started, every suite was green, and no two of them were witnessed against the
same bits. That is the failure this guards: not a broken pin, a quiet split.

Checks, offline:
  * every emulator-sail / emulator-spark-agent reference in a file that can
    pull (.yml, .yaml, .env) pins @sha256
  * its tag and digest equal sidecars.env
  * it is spelled `${<PREFIX>_VERSION:-..}@${<PREFIX>_DIGEST:-..}`, so an
    override exported from sidecars.env reaches it -- a literal pin or a
    misspelt variable would silently ignore the override

With --registry, also asks ghcr.io (anonymously) that `<image>:<_RELEASE>`
resolves to `_DIGEST`, i.e. that the stated release really published it.
Release tags are never overwritten, so this does not go red on its own when
fabric-emulator releases again.

Stdlib only.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
PINS = ROOT / "e2e" / "sidecars.env"
REGISTRY = "ghcr.io"
OWNER = "calvinchengx"

# image name -> the variable prefix sidecars.env and the compose files use
PREFIX = {
    "emulator-sail": "SAIL_ENGINE",
    "emulator-spark-agent": "SPARK_CLIENT",
}
PULLING_SUFFIXES = {".yml", ".yaml", ".env"}
SKIP_DIRS = {".git", "node_modules", ".venv", "_site", "__pycache__"}

# The lookbehind keeps `fabric-emulator-sail` from matching as `emulator-sail`.
NAME = r"(?<![A-Za-z0-9._-])(?P<name>" + "|".join(map(re.escape, PREFIX)) + r")"


def _part(g: str) -> str:
    """`${VAR:-default}` or a literal, captured as <g>_var/<g>_def or <g>_lit."""
    return rf"(?:\$\{{(?P<{g}_var>[A-Z0-9_]+):-(?P<{g}_def>[^}}]*)\}}|(?P<{g}_lit>[^@\s\"'}}]+))"


REF = re.compile(NAME + ":" + _part("tag") + "(?:@" + _part("dig") + ")?")
ACCEPT = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])


def load_pins(text: str) -> dict[str, str]:
    pins = {}
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            pins[key.strip()] = value.strip()
    missing = [
        f"{p}_{k}" for p in PREFIX.values() for k in ("VERSION", "DIGEST", "RELEASE")
        if not pins.get(f"{p}_{k}")
    ]
    if missing:
        raise ValueError(f"sidecars.env is missing {', '.join(missing)}")
    return pins


def check_line(line: str, pins: dict[str, str]) -> list[str]:
    """Problems with every sidecar reference on one line."""
    problems = []
    for m in REF.finditer(line):
        name, prefix = m["name"], PREFIX[m["name"]]
        tag = m["tag_def"] if m["tag_var"] else m["tag_lit"]
        digest = m["dig_def"] if m["dig_var"] else m["dig_lit"]
        if digest is None:
            problems.append(f"{name}:{tag} is not pinned by digest")
            continue
        if (m["tag_var"], m["dig_var"]) != (f"{prefix}_VERSION", f"{prefix}_DIGEST"):
            problems.append(
                f"{name} must read ${{{prefix}_VERSION:-..}}@${{{prefix}_DIGEST:-..}}, "
                f"found tag {m['tag_var'] or 'literal'} / digest {m['dig_var'] or 'literal'}"
            )
        if tag != pins[f"{prefix}_VERSION"]:
            problems.append(f"{name} tag {tag} != sidecars.env {pins[prefix + '_VERSION']}")
        if digest != pins[f"{prefix}_DIGEST"]:
            problems.append(f"{name} digest {digest} != sidecars.env {pins[prefix + '_DIGEST']}")
    return problems


def pulling_files(root: pathlib.Path):
    for path in sorted(root.rglob("*")):
        if (
            path.is_file()
            and path.suffix in PULLING_SUFFIXES
            and not SKIP_DIRS.intersection(path.relative_to(root).parts)
            and path.name != PINS.name
        ):
            yield path


def offenders(root: pathlib.Path, pins: dict[str, str]):
    """Yield (relative path, line number, problem) and count references seen."""
    for path in pulling_files(root):
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for n, line in enumerate(text.splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            for problem in check_line(line, pins):
                yield path.relative_to(root).as_posix(), n, problem


def count_refs(root: pathlib.Path) -> int:
    total = 0
    for path in pulling_files(root):
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        total += sum(
            len(REF.findall(line)) for line in text.splitlines()
            if not line.lstrip().startswith("#")
        )
    return total


def registry_digest(image: str, tag: str, opener=urllib.request.urlopen) -> str:
    repo = f"{OWNER}/{image}"
    with opener(f"https://{REGISTRY}/token?scope=repository:{repo}:pull&service={REGISTRY}") as r:
        token = json.load(r)["token"]
    req = urllib.request.Request(
        f"https://{REGISTRY}/v2/{repo}/manifests/{tag}",
        method="HEAD",
        headers={"Authorization": f"Bearer {token}", "Accept": ACCEPT},
    )
    with opener(req) as r:
        return r.headers["Docker-Content-Digest"]


def registry_problems(pins: dict[str, str], lookup=registry_digest) -> list[str]:
    problems = []
    for image, prefix in PREFIX.items():
        release = pins[f"{prefix}_RELEASE"]
        want = pins[f"{prefix}_DIGEST"]
        got = lookup(image, release)
        if got != want:
            problems.append(
                f"{image}:{release} is {got}, but sidecars.env says that release published {want}"
            )
    return problems


def main(argv=None, root=ROOT, lookup=registry_digest) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--registry", action="store_true", help="also verify _RELEASE against ghcr.io")
    args = ap.parse_args(argv)

    pins_path = root / PINS.relative_to(ROOT)
    try:
        pins = load_pins(pins_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        print(f"check_sidecar_pins: {e}", file=sys.stderr)
        return 1

    bad = list(offenders(root, pins))
    for rel, n, problem in bad:
        print(f"{rel}:{n}: {problem}", file=sys.stderr)
    refs = count_refs(root)
    if refs == 0:
        # A regex that stopped matching would otherwise pass everything.
        print("check_sidecar_pins: found no sidecar references at all", file=sys.stderr)
        return 1

    reg = registry_problems(pins, lookup) if args.registry else []
    for problem in reg:
        print(problem, file=sys.stderr)
    if bad or reg:
        return 1

    print(
        f"{refs} sidecar references agree with e2e/sidecars.env "
        f"(sail {pins['SAIL_ENGINE_VERSION']}, spark-agent {pins['SPARK_CLIENT_VERSION']})"
    )
    if args.registry:
        print(
            f"ghcr.io: sail digest is fabric-emulator v{pins['SAIL_ENGINE_RELEASE']}, "
            f"spark-agent digest is v{pins['SPARK_CLIENT_RELEASE']}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
