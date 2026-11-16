import unittest

import helpers  # noqa: F401
import summarize
from test_rig_and_complete import prov, section

LAYERS = ["sha256:l1", "sha256:l2"]
DIGEST = summarize.layers_digest(LAYERS)


def with_content(rec, digest=DIGEST, imgs=("gateway", "controlplane", "demo")):
    rec = dict(rec)
    rec["content"] = {i: {"layers_sha256": digest} for i in imgs}
    return rec


def secs(bench_demo="sha256:A", chaos_demo="sha256:B", content=None):
    b, c = prov("bench", demo=bench_demo), prov("chaos", demo=chaos_demo)
    if content:
        b, c = with_content(b, content[0]), with_content(c, content[1])
    return {"bench": section("bench", provenance=[b]), "chaos": section("chaos", provenance=[c])}


def fake_inspect(layers_by_id):
    return lambda i: ({"RootFS": {"Layers": layers_by_id[i]}, "Created": "2026-11-16T11:43:31Z", "Architecture": "arm64", "Os": "linux"}
                      if i in layers_by_id else None)


class ContentIdentity(unittest.TestCase):
    def test_same_content_different_ids_is_no_blocker(self):
        summary, blockers = summarize.completeness(secs(content=(DIGEST, DIGEST)))
        self.assertEqual(blockers, [])
        self.assertIsNone(summary["images"]["demo"])                       # IDs still reported as differing
        self.assertEqual(summary["image_ids"]["demo"], ["sha256:A", "sha256:B"])
        self.assertEqual(summary["image_content_sha256"]["demo"], DIGEST)

    def test_different_content_is_a_blocker(self):
        _, blockers = summarize.completeness(secs(content=(DIGEST, "ff" * 32)))
        self.assertTrue(any("demo image differs" in b for b in blockers), blockers)

    def test_same_id_without_content_is_fine(self):
        self.assertEqual(summarize.completeness(secs(bench_demo="sha256:A", chaos_demo="sha256:A"))[1], [])


class LegacyFallback(unittest.TestCase):
    def records(self):
        return [prov("bench", demo="sha256:A"), prov("chaos", demo="sha256:B")]

    def test_identical_layers_verified_locally_clears_the_blocker_and_leaves_a_note(self):
        v = summarize.verify_image_identity(self.records(), fake_inspect({"sha256:A": LAYERS, "sha256:B": LAYERS}))
        self.assertEqual(set(v), {"demo"})
        self.assertEqual(v["demo"]["layers_sha256"], DIGEST)
        self.assertIn("verified at merge time by inspecting local images", v["demo"]["method"])
        summary, blockers = summarize.completeness(secs(), v)
        self.assertEqual(blockers, [])
        note = summary["image_identity_notes"][0]
        self.assertEqual((note["image"], note["ids"]), ("demo", ["sha256:A", "sha256:B"]))
        self.assertEqual(note["layers_sha256"], DIGEST)
        self.assertTrue(note["time_utc"])

    def test_missing_image_keeps_the_blocker(self):
        v = summarize.verify_image_identity(self.records(), fake_inspect({"sha256:A": LAYERS}))
        self.assertEqual(v, {})
        _, blockers = summarize.completeness(secs(), v)
        self.assertTrue(any("demo image differs" in b for b in blockers), blockers)

    def test_different_layers_keep_the_blocker(self):
        v = summarize.verify_image_identity(self.records(), fake_inspect({"sha256:A": LAYERS, "sha256:B": LAYERS + ["sha256:l3"]}))
        self.assertEqual(v, {})
        self.assertTrue(summarize.completeness(secs(), v)[1])

    def test_no_verification_supplied_keeps_the_blocker(self):
        self.assertTrue(any("demo image differs" in b for b in summarize.completeness(secs())[1]))

    def test_verification_is_skipped_when_content_was_recorded(self):
        recs = [with_content(prov("bench", demo="sha256:A")), with_content(prov("chaos", demo="sha256:B"))]
        calls = []
        summarize.verify_image_identity(recs, lambda i: calls.append(i))
        self.assertEqual(calls, [])

    def test_verification_does_not_excuse_other_ids(self):
        v = summarize.verify_image_identity(self.records(), fake_inspect({"sha256:A": LAYERS, "sha256:B": LAYERS}))
        s = secs(chaos_demo="sha256:C")                                    # an ID the inspection never saw
        self.assertTrue(summarize.completeness(s, v)[1])


class DroppedIterations(unittest.TestCase):
    def test_per_repetition_dropped_iterations_are_printed(self):
        runs = [{"run": 1, "dropped_iterations_total": 6323}, {"run": 2, "dropped_iterations_total": 0}]
        line = summarize.dropped_line(runs)
        self.assertIn("rep 1: 6323", line)
        self.assertIn("rep 2: 0", line)

    def test_report_contains_the_dropped_line_and_identity_note(self):
        import os, tempfile
        sc = secs()
        v = summarize.verify_image_identity([r for s in sc.values() for r in s["provenance"]],
                                            fake_inspect({"sha256:A": LAYERS, "sha256:B": LAYERS}))
        prov_summary, blockers = summarize.completeness(sc, v)
        step = {"target_rps": 2000, "status": "pass", "achieved_rps": 2000, "p99_ms": 1.0, "error_rate": 0.0, "labels": []}
        res = {"smoke": False, "complete": not blockers, "complete_blockers": blockers, "provenance": prov_summary,
               "sections": {}, "maxrps": {"gateway": [{"run": 1, "dropped_iterations_total": 6323, "steps": [dict(step)]}]}}
        with tempfile.TemporaryDirectory() as d:
            src, out = os.path.join(d, "l.json"), os.path.join(d, "r.md")
            import json
            with open(src, "w") as fh:
                json.dump(res, fh)
            summarize.cmd_report(src, out)
            with open(out) as fh:
                text = fh.read()
        self.assertIn("rep 1: 6323", text)
        self.assertIn("image identity note (demo)", text)
        self.assertIn("verified at merge time by inspecting local images", text)
        self.assertIn("**complete:** yes", text)


if __name__ == "__main__":
    unittest.main()
