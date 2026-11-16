import json
import os
import tempfile
import unittest

import helpers
import analyze_limiter
import analyze_hotreload

T = 1_000_000_000_000


def lines(key, start, stop, step_ms, allowed, retry=1, skip=None):
    """Request log lines for one key; the first `allowed` are 200, the rest 429. skip=(a, b) removes a time range."""
    out, n, t = [], 0, start
    while t < stop:
        if not (skip and skip[0] <= t < skip[1]):
            out.append((t, helpers.k6_line(f"L {key} {t} {200 if n < allowed else 429} {retry if n >= allowed else 0}")))
            n += 1
        t += step_ms
    return out


class Limiter(unittest.TestCase):
    CFG = {"rate_limit_rps": 100, "burst": 100, "duration_s": 15, "offered_multiple": 2, "noisy_keys": 2,
           "polite_rps": 50, "offered_per_key_rps": 200}

    def analyse(self, entries, cfg=None):
        with tempfile.TemporaryDirectory() as d:
            log, raw, out = (os.path.join(d, n) for n in ("l.log", "raw.json", "out.json"))
            with open(log, "w") as fh:
                for _, line in sorted(entries, key=lambda e: e[0]):
                    fh.write(line)
            with open(raw, "w") as fh:
                json.dump({"config": cfg or self.CFG, "k6_dropped_iterations": {"single": 0, "noisy": 0, "polite": 0}}, fh)
            self.assertEqual(analyze_limiter.main(log, raw, out), 0)
            with open(out) as fh:
                return json.load(fh)

    def entries(self, polite_rejects=0, gap_for_key1=None):
        e = lines(0, T, T + 2000, 5, 300)                                  # single-key phase, 200 rps for 2 s
        mt = T + 20_000                                                    # multi-key phase starts 20 s later
        e += lines(0, mt, mt + 3000, 5, 400)
        e += lines(1, mt, mt + 3000, 5, 400, skip=gap_for_key1)
        e += lines(9, mt, mt + 3000, 20, 10_000)                           # polite: all 200
        for k in range(polite_rejects):
            e.append((mt + 5 * k, helpers.k6_line(f"L 9 {mt + 5 * k} 429 1")))
        return e

    def test_healthy_generator_gives_error_and_polite_isolation(self):
        r = self.analyse(self.entries())
        self.assertTrue(r["single_key"]["offered_ok"])
        self.assertEqual(r["multi_key"]["noisy_keys_generator_ok"], 2)
        # 400 requests 5 ms apart: window = 1.995 s + 5 ms = 2.0 s -> expected 100 + 100*2.0 = 300; allowed 300
        self.assertAlmostEqual(r["single_key"]["expected_allowed"], 300.0)
        self.assertAlmostEqual(r["single_key"]["error_pct"], 0.0)
        self.assertEqual(r["single_key"]["allowed"], 300)
        self.assertEqual(set(r["multi_key"]["noisy_error_pct"]), {"min", "median", "max"})
        self.assertTrue(r["multi_key"]["polite_isolated"])
        self.assertEqual(r["multi_key"]["polite_key"]["rejected"], 0)
        self.assertEqual(r["bad_rejects"], 0)

    def test_key_with_a_gap_of_burst_over_rate_is_excluded(self):
        mt = T + 20_000
        r = self.analyse(self.entries(gap_for_key1=(mt + 1000, mt + 2000)))   # exactly 1 s hole in key 1
        k = {n["key"]: n for n in r["multi_key"]["noisy_keys"]}
        self.assertFalse(k[1]["offered_ok"])
        self.assertTrue(k[1]["max_gap_ms"] >= 1000)
        self.assertTrue(k[0]["offered_ok"])
        self.assertEqual(r["multi_key"]["noisy_keys_generator_ok"], 1)
        e = r["multi_key"]["noisy_error_pct"]
        self.assertEqual(e["min"], e["max"])                                # only key 0 counts
        self.assertAlmostEqual(e["min"], k[0]["error_pct"])

    def test_gap_just_under_the_limit_is_fine(self):
        mt = T + 20_000
        r = self.analyse(self.entries(gap_for_key1=(mt + 1000, mt + 1900)))
        self.assertEqual(r["multi_key"]["noisy_keys_generator_ok"], 2)

    def test_polite_key_reports_only_rejections(self):
        r = self.analyse(self.entries(polite_rejects=1))
        p = r["multi_key"]["polite_key"]
        self.assertEqual(p["rejected"], 1)
        self.assertFalse(r["multi_key"]["polite_isolated"])

    def test_bad_rejects_counted(self):
        e = self.entries()
        e.append((T + 500, helpers.k6_line(f"L 0 {T + 500} 429 0")))      # 429 without Retry-After
        e.append((T + 501, helpers.k6_line(f"L 0 {T + 501} 503 0")))      # other status
        r = self.analyse(e)
        self.assertEqual(r["missing_retry_after"] >= 1, True)
        self.assertEqual(r["other_status"], 1)
        self.assertGreaterEqual(r["bad_rejects"], 2)


class HotReload(unittest.TestCase):
    def analyse(self, reqs, changes):
        with tempfile.TemporaryDirectory() as d:
            log, raw, out = (os.path.join(d, n) for n in ("h.log", "raw.json", "out.json"))
            with open(log, "w") as fh:
                for kind, s, e, st in reqs:
                    fh.write(helpers.k6_line(f"R {kind} {s} {e} {st}"))
                for c in changes:
                    fh.write(helpers.k6_line("C " + " ".join(str(x) for x in c)))
            with open(raw, "w") as fh:
                json.dump({"config": {}}, fh)
            self.assertEqual(analyze_hotreload.main(log, raw, out), 0)
            with open(out) as fh:
                return json.load(fh)

    # change 1 (flip): sent 1000, returned 1005, observed 1010
    def test_only_requests_covering_the_whole_change_window_count(self):
        reqs = [
            ("S", 500, 2000, 200),    # spans
            ("F", 900, 1011, 200),    # spans (ends after observed)
            ("F", 900, 1004, 200),    # ends before the change was observed: not counted
            ("S", 1001, 2000, 200),   # started after the call was sent: not counted
            ("F", 0, 500, 200),       # before
            ("F", 2000, 2100, 200),   # after
            ("S", 100, 3000, 500),    # spans and failed
        ]
        r = self.analyse(reqs, [(1, "flip", 0, 1000, 1005, 1010)])
        self.assertEqual(r["spanning_requests"], 3)
        self.assertEqual(r["spanning_requests_slow_route"], 2)
        self.assertEqual(r["spanning_requests_fast_route"], 1)
        self.assertEqual(r["spanning_requests_failed"], 1)
        self.assertEqual(r["spanning_per_change"]["median"], 3)

    def test_unobserved_change_uses_admin_return_plus_100ms(self):
        changes = [(1, "slow", 0, 5000, 5005, 0)]                          # window ends at 5105
        r = self.analyse([("S", 4900, 5200, 200), ("S", 4900, 5100, 200)], changes)
        self.assertEqual(r["spanning_requests"], 1)

    def test_a_request_spanning_two_changes_is_counted_once_but_per_change_twice(self):
        changes = [(1, "flip", 0, 1000, 1005, 1010), (2, "flip", 0, 2000, 2005, 2010)]
        r = self.analyse([("S", 500, 3000, 200)], changes)
        self.assertEqual(r["spanning_requests"], 1)
        self.assertEqual(r["spanning_per_change"], {"min": 1, "median": 1, "max": 1})

    def test_no_changes_means_no_spanning_requests(self):
        r = self.analyse([("S", 500, 3000, 200)], [])
        self.assertEqual(r["spanning_requests"], 0)


if __name__ == "__main__":
    unittest.main()
