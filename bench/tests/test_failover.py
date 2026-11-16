import json
import os
import tempfile
import unittest

import helpers
import analyze_failover as af

T0 = 1_000_000.0
T1 = T0 + 25_000


def base(extra, victim_back_at=None):
    """200 ok requests every 5 ms around the fault plus the given failures [(start, status, lat)]."""
    reqs = [(T0 - 1000 + 5 * k, 200, "demo-2", 0.4) for k in range(0, 600)]
    reqs += [(t, s, "-", lat) for t, s, lat in extra]
    if victim_back_at is not None:
        reqs.append((victim_back_at, 200, "demo-1", 0.4))
    return sorted(reqs)


def run(extra, **kw):
    reqs = base(extra, kw.pop("victim_back_at", T1 + 8000))
    return af.analyze(reqs, "x", "/fo-passive", "kill", 200, T0, T1, **kw)


class Failover(unittest.TestCase):
    def test_offsets_use_the_real_t0_not_the_slack(self):
        r = run([(T0 + 40, 502, 1.0)])
        self.assertAlmostEqual(r["first_failure_offset_s"], 0.040, places=6)   # not 0.140
        self.assertAlmostEqual(r["failover_time_s"], 0.041, places=6)
        self.assertAlmostEqual(r["failure_window"]["first_offset_s"], 0.040, places=6)

    def test_failure_just_before_t0_is_classified_but_offset_is_negative(self):
        r = run([(T0 - 50, 502, 1.0), (T0 + 10, 502, 1.0)])
        self.assertEqual(r["failed_before_fault"], 0)
        self.assertAlmostEqual(r["first_failure_offset_s"], -0.050, places=6)
        self.assertEqual(r["failover_failed_requests"], 2)

    def test_failure_well_before_the_fault_is_not_part_of_it(self):
        r = run([(T0 - 500, 502, 1.0), (T0 + 10, 502, 1.0)])
        self.assertEqual(r["failed_before_fault"], 1)
        self.assertEqual(r["failover_failed_requests"], 1)

    def test_failover_time_is_null_when_the_first_cluster_starts_late(self):
        r = run([(T0 + 3000, 502, 1.0)])
        self.assertIsNone(r["failover_time_s"])
        self.assertIsNone(r["first_failure_offset_s"])
        self.assertEqual(r["failover_failed_requests"], 0)

    def test_failover_time_is_null_when_nothing_failed(self):
        r = run([])
        self.assertIsNone(r["failover_time_s"])
        self.assertEqual(r["failed"], 0)

    def test_client_visible_end_is_start_plus_latency(self):
        r = run([(T0 + 10, 502, 1000.0), (T0 + 30, 502, 1.0)])
        self.assertAlmostEqual(r["failover_time_s"], 1.010, places=6)  # 10 ms + 1000 ms, not 30 ms
        self.assertEqual(r["failover_max_latency_ms"], 1000.0)
        self.assertAlmostEqual(r["failure_clusters"][0]["end_offset_s"], 0.030, places=6)  # start-based end kept

    def test_reprobe_failures_are_counted_and_per_cooloff_is_scaled_by_fault_length(self):
        r = run([(T0 + 10, 502, 1.0), (T0 + 10_040, 500, 1.0), (T0 + 20_040, 500, 1.0)])
        self.assertEqual(r["reprobe_failures"], 2)
        self.assertEqual(r["reprobe_clusters"], 2)
        self.assertAlmostEqual(r["reprobe_failures_per_cooloff"], 2 / (25.0 / 10.0))  # 25 s fault, 10 s cool-off
        self.assertEqual(r["failed_after_first_cluster"], 2)

    def test_failure_after_restore_is_not_a_reprobe(self):
        r = run([(T0 + 10, 502, 1.0), (T1 + 500, 502, 1.0)])
        self.assertEqual(r["reprobe_failures"], 0)
        self.assertEqual(r["failed_after_restore"], 1)

    def test_fault_s_argument_is_used_for_the_rate(self):
        r = run([(T0 + 10, 502, 1.0), (T0 + 10_040, 500, 1.0)], fault_s=20.0)
        self.assertAlmostEqual(r["reprobe_failures_per_cooloff"], 0.5)

    def test_recovery_is_time_from_restore_to_first_victim_200(self):
        r = run([(T0 + 10, 502, 1.0)], victim_back_at=T1 + 7863)
        self.assertAlmostEqual(r["recovery_s"], 7.863, places=6)

    def test_recovery_null_when_victim_never_returns(self):
        r = run([(T0 + 10, 502, 1.0)], victim_back_at=None)
        self.assertIsNone(r["recovery_s"])

    def test_log_parsing_and_cli(self):
        with tempfile.TemporaryDirectory() as d:
            log = os.path.join(d, "x.log")
            with open(log, "w") as fh:
                for t, s, i, lat in base([(T0 + 40, 502, 1.0)], T1 + 100):
                    fh.write(helpers.k6_line(f"R {t:.0f} {s} {i} {lat}"))
                fh.write(helpers.k6_line("E fault_on 1 200"))   # non-request lines are skipped
            reqs = af.parse_log(log)
            self.assertEqual(len(reqs), len(base([(T0 + 40, 502, 1.0)], T1 + 100)))


if __name__ == "__main__":
    unittest.main()
