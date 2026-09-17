"""Tests for check_sidecar_pins.py. Stdlib unittest: python3 -m unittest discover -s scripts."""
from __future__ import annotations

import contextlib
import io
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import check_sidecar_pins as csp  # noqa: E402

SAIL_D = "sha256:" + "a" * 64
AGENT_D = "sha256:" + "b" * 64
OTHER_D = "sha256:" + "c" * 64
PINS_TEXT = f"""# comment
SAIL_ENGINE_VERSION=0.7.1
SAIL_ENGINE_DIGEST={SAIL_D}
SAIL_ENGINE_RELEASE=0.36.0
SPARK_CLIENT_VERSION=4.2.0
SPARK_CLIENT_DIGEST={AGENT_D}
SPARK_CLIENT_RELEASE=0.36.0
"""
PINS = csp.load_pins(PINS_TEXT)
SAIL = f"image: ghcr.io/calvinchengx/emulator-sail:${{SAIL_ENGINE_VERSION:-0.7.1}}@${{SAIL_ENGINE_DIGEST:-{SAIL_D}}}"
AGENT = f"image: ghcr.io/calvinchengx/emulator-spark-agent:${{SPARK_CLIENT_VERSION:-4.2.0}}@${{SPARK_CLIENT_DIGEST:-{AGENT_D}}}"


class LoadPins(unittest.TestCase):
    def test_reads_values_and_skips_comments(self):
        self.assertEqual(PINS["SAIL_ENGINE_RELEASE"], "0.36.0")
        self.assertEqual(len(PINS), 6)

    def test_missing_key_is_named(self):
        with self.assertRaisesRegex(ValueError, "SPARK_CLIENT_RELEASE"):
            csp.load_pins(PINS_TEXT.replace("SPARK_CLIENT_RELEASE=0.36.0", ""))

    def test_empty_value_counts_as_missing(self):
        with self.assertRaisesRegex(ValueError, "SAIL_ENGINE_DIGEST"):
            csp.load_pins(PINS_TEXT.replace(f"SAIL_ENGINE_DIGEST={SAIL_D}", "SAIL_ENGINE_DIGEST="))


class CheckLine(unittest.TestCase):
    def test_agreeing_references_pass(self):
        self.assertEqual(csp.check_line(SAIL, PINS), [])
        self.assertEqual(csp.check_line(AGENT, PINS), [])

    def test_unrelated_line_passes(self):
        self.assertEqual(csp.check_line("image: unitycatalog/unitycatalog:v0.5.0", PINS), [])

    def test_unpinned_tag(self):
        self.assertEqual(
            csp.check_line("image: ghcr.io/calvinchengx/emulator-sail:0.7.1", PINS),
            ["emulator-sail:0.7.1 is not pinned by digest"],
        )

    def test_unpinned_variable_tag(self):
        [p] = csp.check_line("image: x/emulator-sail:${SAIL_ENGINE_VERSION:-0.7.1}", PINS)
        self.assertIn("not pinned by digest", p)

    def test_literal_pin_ignores_overrides(self):
        [p] = csp.check_line(f"image: x/emulator-sail:0.7.1@{SAIL_D}", PINS)
        self.assertIn("found tag literal / digest literal", p)

    def test_misspelt_variable(self):
        line = SAIL.replace("SAIL_ENGINE_DIGEST", "SAIL_DIGEST")
        [p] = csp.check_line(line, PINS)
        self.assertIn("digest SAIL_DIGEST", p)

    def test_tag_drift(self):
        self.assertEqual(
            csp.check_line(SAIL.replace(":-0.7.1", ":-0.7.0"), PINS),
            ["emulator-sail tag 0.7.0 != sidecars.env 0.7.1"],
        )

    def test_digest_drift(self):
        self.assertEqual(
            csp.check_line(AGENT.replace(AGENT_D, OTHER_D), PINS),
            [f"emulator-spark-agent digest {OTHER_D} != sidecars.env {AGENT_D}"],
        )

    def test_prefixed_image_is_not_a_sidecar_reference(self):
        line = "image: ghcr.io/calvinchengx/fabric-emulator-sail:0.7.1"
        self.assertEqual(csp.check_line(line, PINS), [])

    def test_quoted_literal(self):
        [p] = csp.check_line('image: "x/emulator-spark-agent:4.2.0"', PINS)
        self.assertEqual(p, "emulator-spark-agent:4.2.0 is not pinned by digest")


class Tree(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self._tmp.name)
        self.write("e2e/sidecars.env", PINS_TEXT)
        self.write("e2e/a/docker-compose.yml", f"services:\n  sail:\n    {SAIL}\n  agent:\n    {AGENT}\n")

    def tearDown(self):
        self._tmp.cleanup()

    def write(self, rel, text):
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")

    def run_main(self, *argv, lookup=None):
        out, err = io.StringIO(), io.StringIO()
        kwargs = {"root": self.root}
        if lookup is not None:
            kwargs["lookup"] = lookup
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = csp.main(list(argv), **kwargs)
        return code, out.getvalue(), err.getvalue()

    def test_clean_tree_passes(self):
        code, out, err = self.run_main()
        self.assertEqual((code, err), (0, ""))
        self.assertIn("2 sidecar references agree", out)

    def test_drift_in_any_pulling_file_fails_with_location(self):
        self.write("e2e/b/compose.yaml", "    " + SAIL.replace(SAIL_D, OTHER_D) + "\n")
        self.write("e2e/c/override.env", "IMAGE=x/emulator-sail:0.7.0\n")
        code, _, err = self.run_main()
        self.assertEqual(code, 1)
        self.assertIn("e2e/b/compose.yaml:1: emulator-sail digest", err)
        self.assertIn("e2e/c/override.env:1: emulator-sail:0.7.0 is not pinned", err)

    def test_prose_comments_and_skipped_dirs_are_ignored(self):
        self.write("docs/x.md", "emulator-sail:0.1.0\n")
        self.write("e2e/a/notes.yml", "# was emulator-sail:0.7.0 until September\n")
        self.write("node_modules/p/x.yml", "image: x/emulator-sail:0.1.0\n")
        self.write("_site/x.yaml", "image: x/emulator-sail:0.1.0\n")
        self.write("e2e/a/bin.yml", "")
        (self.root / "e2e/a/bin.yml").write_bytes(b"\xff\xfe emulator-sail:0.1.0")
        code, out, _ = self.run_main()
        self.assertEqual(code, 0)
        self.assertIn("2 sidecar references", out)

    def test_no_references_at_all_fails(self):
        (self.root / "e2e/a/docker-compose.yml").unlink()
        code, _, err = self.run_main()
        self.assertEqual(code, 1)
        self.assertIn("found no sidecar references", err)

    def test_missing_pins_file_fails(self):
        (self.root / "e2e/sidecars.env").unlink()
        code, _, err = self.run_main()
        self.assertEqual(code, 1)
        self.assertIn("check_sidecar_pins:", err)

    def test_registry_confirms_release(self):
        seen = []

        def lookup(image, tag):
            seen.append((image, tag))
            return {"emulator-sail": SAIL_D, "emulator-spark-agent": AGENT_D}[image]

        code, out, _ = self.run_main("--registry", lookup=lookup)
        self.assertEqual(code, 0)
        self.assertEqual(seen, [("emulator-sail", "0.36.0"), ("emulator-spark-agent", "0.36.0")])
        self.assertIn("sail digest is fabric-emulator v0.36.0", out)

    def test_registry_disagreement_fails(self):
        code, _, err = self.run_main("--registry", lookup=lambda image, tag: OTHER_D)
        self.assertEqual(code, 1)
        self.assertIn(f"emulator-sail:0.36.0 is {OTHER_D}, but sidecars.env says", err)

    def test_registry_not_consulted_by_default(self):
        def lookup(image, tag):
            raise AssertionError("offline check reached the network")

        self.assertEqual(self.run_main(lookup=lookup)[0], 0)


class RegistryDigest(unittest.TestCase):
    def test_token_then_head_manifest(self):
        calls = []

        class Resp(io.BytesIO):
            headers = {"Docker-Content-Digest": SAIL_D}

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        def opener(req):
            calls.append(req)
            return Resp(b'{"token": "t0k"}')

        self.assertEqual(csp.registry_digest("emulator-sail", "0.36.0", opener=opener), SAIL_D)
        token_url, head = calls
        self.assertIn("scope=repository:calvinchengx/emulator-sail:pull", token_url)
        self.assertEqual(head.get_method(), "HEAD")
        self.assertEqual(head.full_url, "https://ghcr.io/v2/calvinchengx/emulator-sail/manifests/0.36.0")
        self.assertEqual(head.get_header("Authorization"), "Bearer t0k")
        self.assertIn("image.index", head.get_header("Accept"))


if __name__ == "__main__":
    unittest.main()
