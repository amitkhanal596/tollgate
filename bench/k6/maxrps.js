// Max sustainable throughput, one connection-stable load shape per target.
//
// A single ramping-arrival-rate scenario climbs through STEPS (hold each for
// STEP seconds after a short RAMP). One scenario means the same VUs and
// connections serve every step, so the gateway does not accumulate hundreds of
// extra never-closed client connections per step (the old one-scenario-per-step
// shape did). Requests are tagged with their step, so latency, errors and
// achieved rate are reported per step. Dropped iterations cannot be split by
// step in k6, so "the generator could not keep the rate" is detected as
// achieved < 98% of target, and from k6's "Insufficient VUs" warning (the
// wrapper script parses it out of k6's log).
//
// This script only measures. Whether a failed step counts against the gateway
// or against the test rig (CPU saturation, VM stalls, generator limits) is
// decided afterwards by bench/summarize.py from docker stats and the watchdog.
//
// Env: TARGET = direct | gateway | gateway3   (default gateway)
//        direct   -> http://demo-1:8080/bare   (the backend's own ceiling)
//        gateway  -> gateway /bare  -> single backend demo-1
//        gateway3 -> gateway /bare3 -> round robin over demo-1..3
//      STEPS (2000,4000,6000,8000,10000,12000,14000,16000)  STEP_DURATION (8, seconds)
//      RAMP (1)  WARMUP (5, unmeasured, at the first step's rate)  P99_MS (50)
//      PRE_VUS (600)  MAX_VUS (3000)  OUT_NAME (maxrps-<TARGET>.json)
import http from 'k6/http';
import { Gauge } from 'k6/metrics';
import { BACKEND, GATEWAY, TREND_STATS, jsonSummary, lat, reportSubmetric, val } from './common.js';

const TARGET = __ENV.TARGET || 'gateway';
const URLS = { direct: `${BACKEND}/bare`, gateway: `${GATEWAY}/bare`, gateway3: `${GATEWAY}/bare3` };
const URL = URLS[TARGET];
const STEPS = (__ENV.STEPS || '2000,4000,6000,8000,10000,12000,14000,16000').split(',').map((s) => parseInt(s, 10));
const STEP = parseFloat(__ENV.STEP_DURATION || '8');
const RAMP = parseFloat(__ENV.RAMP || '1');
const WARMUP = parseFloat(__ENV.WARMUP || '5');
const P99_MS = parseFloat(__ENV.P99_MS || '50');

// Seconds since the start of the run at which each step's hold begins and ends.
const holdStart = STEPS.map((_, i) => WARMUP + i * (RAMP + STEP) + RAMP);
const holdEnd = holdStart.map((s) => s + STEP);

const stages = [{ target: STEPS[0], duration: `${WARMUP}s` }];
STEPS.forEach((rate) => {
  stages.push({ target: rate, duration: `${RAMP}s` });
  stages.push({ target: rate, duration: `${STEP}s` });
});

const thresholds = {};
STEPS.forEach((_, i) => {
  for (const m of ['http_req_duration', 'http_req_failed', 'http_reqs']) reportSubmetric(thresholds, `${m}{step:s${i}}`);
});
reportSubmetric(thresholds, 'dropped_iterations');

const startGauge = new Gauge('start_epoch_ms');

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate', startRate: STEPS[0], timeUnit: '1s', stages,
      preAllocatedVUs: parseInt(__ENV.PRE_VUS || '600', 10), maxVUs: parseInt(__ENV.MAX_VUS || '3000', 10), exec: 'run',
    },
  },
  thresholds, discardResponseBodies: true,
  summaryTrendStats: TREND_STATS,
};

export function setup() {
  const now = Date.now(); // VM clock: the same clock as the stall watchdog and the TCP counters
  startGauge.add(now);
  console.log(`START ${now}`);
  return { start: now };
}

export function run(data) {
  const el = (Date.now() - data.start) / 1000;
  let step = 'other'; // warm-up and ramps are not measured
  for (let i = 0; i < holdStart.length; i++) {
    if (el >= holdStart[i] && el < holdEnd[i]) { step = `s${i}`; break; }
  }
  http.get(URL, { tags: { step } });
}

export function handleSummary(data) {
  const m = data.metrics;
  const steps = STEPS.map((target, i) => {
    const s = `{step:s${i}}`;
    const reqs = val(m[`http_reqs${s}`], 'count');
    const errRate = val(m[`http_req_failed${s}`], 'rate');
    const l = lat(m[`http_req_duration${s}`]);
    const achieved = reqs / STEP;
    return {
      step: `s${i}`, target_rps: target, achieved_rps: achieved, requests: reqs, error_rate: errRate,
      p50_ms: l && l.p50, p90_ms: l && l.p90, p99_ms: l && l.p99, max_ms: l && l.max,
      hold_start_offset_s: holdStart[i], hold_end_offset_s: holdEnd[i],
      // Raw k6-level verdict; summarize.py adds the rig labels.
      passes_k6: errRate < 0.001 && l !== null && l.p99 < P99_MS && achieved >= 0.98 * target,
    };
  });
  const result = {
    test: 'maxrps', target: TARGET, url: URL,
    criteria: { max_error_rate: 0.001, p99_ms_below: P99_MS, min_achieved_fraction: 0.98, step_duration_s: STEP, ramp_s: RAMP, warmup_s: WARMUP },
    shape: 'ramping-arrival-rate, one scenario, connection-stable',
    start_epoch_ms: val(m.start_epoch_ms, 'value'),
    summary_epoch_ms: Date.now(),
    test_run_duration_ms: data.state ? data.state.testRunDurationMs : null,
    dropped_iterations_total: val(m.dropped_iterations, 'count'),
    max_vus: val(m.vus_max, 'max'),
    steps,
  };
  return jsonSummary(__ENV.OUT_NAME || `maxrps-${TARGET}.json`, result);
}
