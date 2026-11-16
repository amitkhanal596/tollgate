import datetime
import json
import os
import tempfile
import unittest

import helpers  # noqa: F401
import summarize

START = 1_000_000_000_000  # VM-clock epoch ms at which the synthetic k6 run began
PINNING = {"vm_cpus": 8, "k6_cpuset": "0-2", "gateway_cpuset": "3-5", "rest_cpuset": "6-7"}
NAME = "maxrps-gateway-r1"


def step(i, passes=True):
    return {"step": f"s{i}", "target_rps": 2000 * (i + 1), "achieved_rps": 2000 * (i + 1), "requests": 1000,
            "error_rate": 0.0, "p50_ms": 1.0, "p90_ms": 2.0, "p99_ms": 5.0, "max_ms": 9.0,
            "hold_start_offset_s": 5 + i * 9 + 1, "hold_end_offset_s": 5 + i * 9 + 9, "passes_k6": passes}


def window(i):
    s = step(i)
    return START + s["hold_start_offset_s"] * 1000, START + s["hold_end_offset_s"] * 1000


class Enrich(unittest.TestCase):
    """Labelling of one run's step, from synthetic docker stats / watchdog / k6 log files."""

    def label(self, passes=False, cpu=None, stall=None, insufficient=False):
        a, b = window(0)
        with tempfile.TemporaryDirectory() as raw:
            samples = [{"clock": {"host_ms": 5000.0, "vm_ms": 5000, "rtt_ms": 3}}]
            for k in range(4):  # four samples inside the hold, host clock == VM clock
                t = a + 1000 + k * 1500 + 500
                samples.append({"host_start_ms": t - 1500, "host_end_ms": t, "cpu": cpu or {}})
            with open(os.path.join(raw, f"{NAME}.stats.jsonl"), "w") as fh:
                for r in samples:
                    fh.write(json.dumps(r) + "\n")
            with open(os.path.join(raw, "stall.log"), "w") as fh:
                for sec in range(a // 1000 - 2, b // 1000 + 3):
                    gap = stall if (stall and sec == a // 1000 + 3) else 2.0
                    fh.write(f"{sec} {gap}\n")
            with open(os.path.join(raw, f"{NAME}.k6.log"), "w") as fh:
                if insufficient:
                    t = datetime.datetime.fromtimestamp((a + 2000) / 1000, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    fh.write(f'time="{t}" level=warning msg="Insufficient VUs, reached 3000 active VUs and cannot initialize more" executor=ramping-arrival-rate\n')
            d = {"start_epoch_ms": START, "steps": [step(0, passes)]}
            summarize.enrich_maxrps(raw, NAME, d, PINNING, 8)
        return d["steps"][0]

    def test_failed_with_containers_over_75_percent_of_vcpus_is_rig_limited(self):
        s = self.label(cpu={"tollgate-bench-gateway-1": 300, "tollgate-bench-k6-9-1": 250, "tollgate-bench-demo-1-1": 100})
        self.assertEqual(s["status"], "rig-limited")
        self.assertTrue(any("6.5 of 8 vCPUs" in l for l in s["labels"]), s["labels"])

    def test_failed_with_k6_cpuset_saturated_is_rig_limited(self):
        s = self.label(cpu={"tollgate-bench-gateway-1": 100, "tollgate-bench-k6-9-1": 285})
        self.assertEqual(s["status"], "rig-limited")
        self.assertTrue(any("k6 cpuset saturated" in l for l in s["labels"]))

    def test_failed_with_backend_cpuset_saturated_is_rig_limited(self):
        s = self.label(cpu={"tollgate-bench-demo-1-1": 120, "tollgate-bench-demo-2-1": 70})
        self.assertEqual(s["status"], "rig-limited")

    def test_failed_with_insufficient_vus_is_rig_limited(self):
        s = self.label(cpu={"tollgate-bench-gateway-1": 100}, insufficient=True)
        self.assertEqual(s["status"], "rig-limited")
        self.assertIn("k6 insufficient VUs", s["labels"])

    def test_failed_with_stall_over_100ms_is_rig_perturbed(self):
        s = self.label(cpu={"tollgate-bench-gateway-1": 100}, stall=250.0)
        self.assertEqual(s["status"], "rig-perturbed")
        self.assertEqual(s["stall_max_ms"], 250.0)

    def test_stall_wins_over_cpu_label(self):
        s = self.label(cpu={"tollgate-bench-gateway-1": 400, "tollgate-bench-k6-9-1": 280, "tollgate-bench-demo-1-1": 100}, stall=150.0)
        self.assertEqual(s["status"], "rig-perturbed")

    def test_small_stall_is_not_a_label(self):
        s = self.label(cpu={"tollgate-bench-gateway-1": 100}, stall=99.0)
        self.assertEqual(s["status"], "fail")

    def test_failed_on_healthy_rig_is_fail(self):
        s = self.label(cpu={"tollgate-bench-gateway-1": 290, "tollgate-bench-k6-9-1": 100, "tollgate-bench-demo-1-1": 50})
        self.assertEqual(s["status"], "fail")
        self.assertEqual(s["labels"], [])
        self.assertTrue(s["gateway_cpuset_saturated"])  # gateway at its cap: informational, still a gateway result

    def test_passing_step_stays_pass_even_when_perturbed(self):
        s = self.label(passes=True, cpu={"tollgate-bench-gateway-1": 100}, stall=500.0)
        self.assertEqual(s["status"], "pass")
        self.assertTrue(s["labels"])

    def test_cpu_is_the_median_of_the_samples_in_the_step(self):
        s = self.label(cpu={"tollgate-bench-gateway-1": 150, "tollgate-bench-demo-1-1": 50})
        self.assertAlmostEqual(s["cpu_cores"]["gateway"], 1.5)
        self.assertAlmostEqual(s["cpu_cores"]["total"], 2.0)

    def test_dials_and_time_wait_from_netwatch(self):
        a, b = window(0)
        with tempfile.TemporaryDirectory() as raw:
            with open(os.path.join(raw, "netwatch-maxrps.jsonl"), "w") as fh:
                for k in range(0, 17):
                    t = a + k * 500
                    fh.write(json.dumps({"t_ms": t, "active_opens": 1000 + 10 * k, "time_wait": 50 + k}) + "\n")
            d = {"start_epoch_ms": START, "steps": [step(0, True)]}
            summarize.enrich_maxrps(raw, NAME, d, PINNING, 8)
        s = d["steps"][0]
        self.assertAlmostEqual(s["upstream_dials_per_s"], 20.0)
        self.assertEqual(s["time_wait_peak"], 66)


def run(statuses, p99s, achieved=None):
    """A run whose single step has the given status (enough fields for aggregate_maxrps)."""
    steps = []
    for i, st in enumerate(statuses):
        steps.append({"target_rps": 2000 * (i + 1), "status": st, "achieved_rps": (achieved or {}).get(i, 2000 * (i + 1)),
                      "p99_ms": p99s[i] if p99s else 5.0, "error_rate": 0.0, "labels": []})
    return {"steps": steps}


class Aggregate(unittest.TestCase):
    def test_three_reps_ok_perturbed_fail_is_unstable_with_median_and_worst(self):
        runs = []
        for st, p99, ach in (("pass", 10.0, 1990), ("rig-perturbed", 80.0, 1500), ("fail", 30.0, 1900)):
            runs.append({"steps": [{"target_rps": 2000, "status": st, "achieved_rps": ach, "p99_ms": p99, "error_rate": 0.0, "labels": []}]})
        ag = summarize.aggregate_maxrps(runs)
        s = ag["steps"][0]
        self.assertEqual(s["verdict"], "unstable")          # one real fail: counts against the gateway
        self.assertEqual(s["p99_median"], 30.0)
        self.assertEqual(s["p99_worst"], 80.0)
        self.assertEqual(s["achieved_median"], 1900)
        self.assertIsNone(ag["max_step_rps"])

    def test_one_rig_perturbed_rep_does_not_block_sustained(self):
        runs = [run(["pass"], [10]), run(["rig-perturbed"], [90]), run(["pass"], [12])]
        s = summarize.aggregate_maxrps(runs)["steps"][0]
        self.assertEqual(s["verdict"], "sustained")  # 2 valid reps, none failed
        self.assertEqual(s["p99_worst"], 90)         # the worst is still shown

    def test_mostly_rig_affected_is_inconclusive_not_failed(self):
        runs = [run(["rig-perturbed"], [90]), run(["rig-limited"], [70]), run(["pass"], [12])]
        s = summarize.aggregate_maxrps(runs)["steps"][0]
        self.assertEqual(s["verdict"], "inconclusive (rig)")

    def test_all_fail_is_failed(self):
        s = summarize.aggregate_maxrps([run(["fail"], [90])] * 3)["steps"][0]
        self.assertEqual(s["verdict"], "failed")

    def test_top_step_passing_is_a_censored_max(self):
        ag = summarize.aggregate_maxrps([run(["pass"] * 8, None)] * 3)
        self.assertTrue(ag["ceiling_not_found"])
        self.assertEqual(ag["max_step_rps"], 16000)
        self.assertTrue(ag["text"].startswith(">= 16000 rps"), ag["text"])
        self.assertIn("ceiling not found", ag["text"])

    def test_gateway_failure_above_passes_is_a_real_max(self):
        ag = summarize.aggregate_maxrps([run(["pass", "pass", "fail"], None)] * 3)
        self.assertFalse(ag["ceiling_not_found"])
        self.assertEqual(ag["max_step_rps"], 4000)
        self.assertIn("failed at 6000", ag["text"])

    def test_non_monotonic_is_flagged_and_max_is_capped(self):
        ag = summarize.aggregate_maxrps([run(["pass", "pass", "fail", "pass"], None)] * 3)
        self.assertEqual(ag["non_monotonic"], {"failing_step": 6000, "sustained_above": [8000]})
        self.assertEqual(ag["max_step_rps"], 4000)           # capped below the failure
        self.assertFalse(ag["ceiling_not_found"])

    def test_rig_labelled_failure_is_not_a_gateway_failure(self):
        ag = summarize.aggregate_maxrps([run(["pass", "rig-limited", "pass"], None)] * 3)
        self.assertIsNone(ag["non_monotonic"])
        self.assertEqual(ag["max_step_rps"], 6000)
        self.assertEqual(ag["steps"][1]["verdict"], "inconclusive (rig)")

    def test_rig_limited_tail_does_not_claim_a_ceiling(self):
        ag = summarize.aggregate_maxrps([run(["pass", "pass", "rig-limited"], None)] * 3)
        self.assertEqual(ag["max_step_rps"], 4000)
        self.assertTrue(ag["ceiling_not_found"])
        self.assertTrue(ag["text"].startswith(">= 4000"), ag["text"])

    def test_nothing_sustained(self):
        ag = summarize.aggregate_maxrps([run(["fail", "fail"], None)] * 3)
        self.assertIsNone(ag["max_step_rps"])
        self.assertIn("no step sustained", ag["text"])


if __name__ == "__main__":
    unittest.main()
