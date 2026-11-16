import contextlib
import datetime
import io
import json
import os
import tempfile
import unittest
from unittest import mock

import helpers  # noqa: F401
import rigcheck
import summarize

RUN_START = int(datetime.datetime(2026, 11, 16, 10, 0, 0, tzinfo=datetime.timezone.utc).timestamp())
RUN_END = RUN_START + 2400


def pmset_line(epoch, event):
    # pmset prints local time with a numeric offset; use a non-UTC offset to exercise the parser
    tz = datetime.timezone(datetime.timedelta(hours=-5))
    return f"{datetime.datetime.fromtimestamp(epoch, tz).strftime('%Y-%m-%d %H:%M:%S %z')} {event}   Entering state due to 'test'"


def post(text):
    """rigcheck.post with a fake `pmset -g log` output; returns the rig.json it wrote."""
    with tempfile.TemporaryDirectory() as d:
        pre, out = os.path.join(d, "pre.json"), os.path.join(d, "rig.json")
        with open(pre, "w") as fh:
            json.dump({"warnings": [], "power_source": "AC Power"}, fh)
        with mock.patch.object(rigcheck, "sh", return_value=text), contextlib.redirect_stderr(io.StringIO()):
            rigcheck.post(pre, RUN_START, RUN_END, out)
        with open(out) as fh:
            return json.load(fh)


class SleepEvents(unittest.TestCase):
    def events(self, *lines):
        return rigcheck.parse_sleep_events("\n".join(lines) + "\nnoise line\n", RUN_START, RUN_END)

    def test_each_event_type_inside_the_window_is_found(self):
        for ev in ("Sleep", "Wake", "DarkWake"):
            got = self.events(pmset_line(RUN_START + 600, ev))
            self.assertEqual([e["event"] for e in got], [ev])

    def test_events_outside_the_window_are_ignored(self):
        got = self.events(pmset_line(RUN_START - 3600, "Sleep"), pmset_line(RUN_END + 3600, "Wake"))
        self.assertEqual(got, [])

    def test_other_log_lines_are_ignored(self):
        self.assertEqual(self.events(pmset_line(RUN_START + 5, "Assertions"), pmset_line(RUN_START + 5, "Clamshell")), [])

    def test_event_inside_window_makes_the_rig_invalid(self):
        rig = post(pmset_line(RUN_START + 900, "DarkWake"))
        self.assertFalse(rig["valid"])
        self.assertEqual(len(rig["sleep_wake_events"]), 1)

    def test_event_outside_window_keeps_it_valid(self):
        rig = post(pmset_line(RUN_START - 7200, "Sleep"))
        self.assertTrue(rig["valid"])
        self.assertEqual(rig["sleep_wake_events"], [])


def prov(label="bench", head="a" * 40, dirty=False, gw="sha256:gw", cp="sha256:cp", demo="sha256:demo"):
    return {"section": label, "label": label, "time_utc": "2026-11-16T10:00:00Z", "git_head": head, "git_dirty": dirty,
            "images": {"gateway": gw, "controlplane": cp, "demo": demo}}


def section(name, **kw):
    sec = {"smoke": False, "complete": True, "failed": [], "rig": {"valid": True}, "provenance": [prov(name)]}
    sec.update(kw)
    return sec


class Completeness(unittest.TestCase):
    def good(self):
        return {"bench": section("bench"), "chaos": section("chaos")}

    def assertBlocker(self, secs, text):
        _, blockers = summarize.completeness(secs)
        self.assertTrue(any(text in b for b in blockers), blockers)

    def test_all_good_is_complete(self):
        summary, blockers = summarize.completeness(self.good())
        self.assertEqual(blockers, [])
        self.assertEqual(summary["git_head"], "a" * 40)
        self.assertEqual(summary["images"]["gateway"], "sha256:gw")
        self.assertFalse(summary["git_dirty"])

    def test_different_commits(self):
        s = self.good()
        s["chaos"]["provenance"] = [prov("chaos", head="b" * 40)]
        self.assertBlocker(s, "different commits")

    def test_different_gateway_image(self):
        s = self.good()
        s["chaos"]["provenance"] = [prov("chaos", gw="sha256:other")]
        self.assertBlocker(s, "gateway image differs")

    def test_different_demo_image_within_one_section(self):
        s = self.good()
        s["bench"]["provenance"] = [prov("bench"), prov("accesslog", demo="sha256:other")]
        self.assertBlocker(s, "demo image differs")

    def test_smoke_section(self):
        s = self.good()
        s["bench"]["smoke"] = True
        self.assertBlocker(s, "section 'bench' is a smoke run")

    def test_missing_section(self):
        s = self.good()
        del s["chaos"]
        self.assertBlocker(s, "section 'chaos' is missing")

    def test_dirty_tree(self):
        s = self.good()
        s["bench"]["provenance"] = [prov("bench", dirty=True)]
        self.assertBlocker(s, "modified tracked files")
        self.assertTrue(summarize.completeness(s)[0]["git_dirty"])

    def test_invalid_rig(self):
        s = self.good()
        s["chaos"]["rig"] = {"valid": False}
        self.assertBlocker(s, "section 'chaos' is invalid")

    def test_missing_rig_record_counts_as_invalid(self):
        s = self.good()
        s["bench"]["rig"] = None
        self.assertBlocker(s, "section 'bench' is invalid")

    def test_failed_measurements(self):
        s = self.good()
        s["bench"].update(complete=False, failed=["limiter"])
        self.assertBlocker(s, "failed measurements: limiter")

    def test_no_provenance(self):
        s = self.good()
        s["bench"]["provenance"] = []
        self.assertBlocker(s, "no provenance record")

    def test_pmset_event_flows_into_a_complete_false_blocker(self):
        s = self.good()
        s["bench"]["rig"] = post(pmset_line(RUN_START + 100, "Sleep"))
        self.assertBlocker(s, "section 'bench' is invalid")
        s["bench"]["rig"] = post(pmset_line(RUN_START - 99999, "Sleep"))  # outside the window: fine again
        self.assertEqual(summarize.completeness(s)[1], [])


if __name__ == "__main__":
    unittest.main()
