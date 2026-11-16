import unittest

import helpers  # noqa: F401
import summarize


def phase(kind, rnd, side, p50, p90=None, p99=None):
    return {"name": f"{kind}{rnd}{side}", "kind": kind, "round": rnd, "side": side, "target": "x", "requests": 1000,
            "error_rate": 0.0, "dropped_iterations": 0,
            "latency_ms": {"p50": p50, "p90": p90 if p90 is not None else p50 * 2, "p99": p99 if p99 is not None else p50 * 4}}


def rnd(kind, r, a, b):
    """One ABBA round: A phases (a1, a2) and B phases (b1, b2)."""
    return [phase(kind, r, "A", a[0]), phase(kind, r, "B", b[0]), phase(kind, r, "B", b[1]), phase(kind, r, "A", a[1])]


class Overhead(unittest.TestCase):
    def test_per_round_median_min_max_and_side_means(self):
        phases = []
        phases += rnd("AB", 0, (1.0, 1.2), (1.6, 1.6))   # A mean 1.1, B 1.6 -> +0.5
        phases += rnd("AB", 1, (1.0, 1.0), (1.7, 1.7))   # +0.7
        phases += rnd("AB", 2, (1.0, 1.0), (1.6, 1.6))   # +0.6
        o = summarize.overhead_stats({"phases": phases})
        st = o["stats"]["p50"]
        self.assertEqual(o["rounds_ab"], 3)
        self.assertAlmostEqual(st["median"], 0.6)
        self.assertAlmostEqual(st["min"], 0.5)
        self.assertAlmostEqual(st["max"], 0.7)
        self.assertIsNone(st["noise_floor"])                 # no A/A rounds: no floor, never "below"
        self.assertFalse(st["below_noise_floor"])
        self.assertAlmostEqual(o["direct_ms"]["p50"], 1.0)   # median over rounds of (1.1, 1.0, 1.0)

    def test_delta_above_the_aa_floor(self):
        phases = rnd("AB", 0, (1.0, 1.0), (1.5, 1.5)) + rnd("AA", 0, (1.0, 1.0), (1.05, 1.05)) + \
            rnd("AB", 1, (1.0, 1.0), (1.5, 1.5)) + rnd("AA", 1, (1.0, 1.0), (0.97, 0.97))
        st = summarize.overhead_stats({"phases": phases})["stats"]["p50"]
        self.assertAlmostEqual(st["noise_floor"], 0.05)      # largest |A/A delta|
        self.assertFalse(st["below_noise_floor"])

    def test_delta_below_the_aa_floor_is_labelled(self):
        phases = rnd("AB", 0, (1.0, 1.0), (1.03, 1.03)) + rnd("AA", 0, (1.0, 1.0), (1.2, 1.2))
        st = summarize.overhead_stats({"phases": phases})["stats"]["p50"]
        self.assertTrue(st["below_noise_floor"])

    def test_negative_delta_within_floor_is_below(self):
        phases = rnd("AB", 0, (1.0, 1.0), (0.9, 0.9)) + rnd("AA", 0, (1.0, 1.0), (1.2, 1.2))
        self.assertTrue(summarize.overhead_stats({"phases": phases})["stats"]["p50"]["below_noise_floor"])

    def test_delta_equal_to_floor_counts_as_below(self):
        phases = rnd("AB", 0, (1.0, 1.0), (1.2, 1.2)) + rnd("AA", 0, (1.0, 1.0), (1.2, 1.2))
        self.assertTrue(summarize.overhead_stats({"phases": phases})["stats"]["p50"]["below_noise_floor"])

    def test_each_percentile_has_its_own_floor(self):
        ab = rnd("AB", 0, (1.0, 1.0), (1.5, 1.5))
        aa = [phase("AA", 0, "A", 1.0, 2.0, 4.0), phase("AA", 0, "B", 1.0, 2.0, 5.0),
              phase("AA", 0, "B", 1.0, 2.0, 5.0), phase("AA", 0, "A", 1.0, 2.0, 4.0)]
        o = summarize.overhead_stats({"phases": ab + aa})["stats"]
        self.assertAlmostEqual(o["p50"]["noise_floor"], 0.0)
        self.assertAlmostEqual(o["p99"]["noise_floor"], 1.0)
        self.assertFalse(o["p99"]["below_noise_floor"])   # +2.0 p99 delta vs floor 1.0
        self.assertTrue(o["p50"]["below_noise_floor"] is False)  # p50 delta 0.5 vs floor 0.0

    def test_phases_without_data_are_skipped_and_drops_counted(self):
        phases = rnd("AB", 0, (1.0, 1.0), (1.5, 1.5))
        phases.append({**phase("AB", 1, "A", 1.0), "latency_ms": None, "requests": 0})
        phases[0]["dropped_iterations"] = 3
        o = summarize.overhead_stats({"phases": phases})
        self.assertEqual(o["rounds_ab"], 1)
        self.assertEqual(o["dropped_phases"], 1)


if __name__ == "__main__":
    unittest.main()
