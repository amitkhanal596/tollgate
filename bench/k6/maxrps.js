// Max sustainable throughput. Each step is its own constant-arrival-rate
// scenario (so k6 reports dropped iterations per step) separated by a short
// gap. "Max RPS" = highest step with error rate < 0.1%, p99 < P99_MS and
// >= 98% of the target rate achieved (dropped iterations < 2%).
//
// Env: TARGET = direct | gateway | gateway3   (default gateway)
//        direct   -> http://demo-1:8080/bare   (the backend's own ceiling)
//        gateway  -> gateway /bare  -> single backend demo-1
//        gateway3 -> gateway /bare3 -> round robin over demo-1..3
//      STEPS (1000,2000,4000,8000,12000)  STEP_DURATION (20s)  P99_MS (50)
//      GAP (3s)  WARMUP (5s, unmeasured)  LABEL (output file suffix; default = TARGET)
import http from 'k6/http';
import { BACKEND, GATEWAY, TREND_STATS, jsonSummary, lat, reportSubmetric, val } from './common.js';

const TARGET = __ENV.TARGET || 'gateway';
const URLS = { direct: `${BACKEND}/bare`, gateway: `${GATEWAY}/bare`, gateway3: `${GATEWAY}/bare3` };
const URL = URLS[TARGET];
const STEPS = (__ENV.STEPS || '1000,2000,4000,8000,12000').split(',').map((s) => parseInt(s, 10));
const STEP = parseFloat(__ENV.STEP_DURATION || '20');
const GAP = parseFloat(__ENV.GAP || '3');
const P99_MS = parseFloat(__ENV.P99_MS || '50');
const LABEL = __ENV.LABEL || TARGET;

const WARMUP = parseFloat(__ENV.WARMUP || '5');

const scenarios = {};
const thresholds = {};
// Unmeasured warm-up at the first step's rate: opens the connection pools on
// both hops, which otherwise shows up as a p99 spike in step 1 (seen as ~490 ms).
scenarios.warm = {
  executor: 'constant-arrival-rate', rate: STEPS[0], timeUnit: '1s', duration: `${WARMUP}s`,
  preAllocatedVUs: 400, maxVUs: 2000, exec: 'run',
};
STEPS.forEach((rate, i) => {
  const name = `s${i}`;
  scenarios[name] = {
    executor: 'constant-arrival-rate', rate, timeUnit: '1s', duration: `${STEP}s`,
    startTime: `${WARMUP + GAP + i * (STEP + GAP)}s`, preAllocatedVUs: 400, maxVUs: 2000, exec: 'run',
  };
  for (const m of ['http_req_duration', 'http_req_failed', 'http_reqs', 'dropped_iterations']) {
    reportSubmetric(thresholds, `${m}{scenario:${name}}`);
  }
});

export const options = {
  scenarios, thresholds, discardResponseBodies: true,
  summaryTrendStats: TREND_STATS,
};

export function run() {
  http.get(URL);
}

export function handleSummary(data) {
  const m = data.metrics;
  const steps = STEPS.map((target, i) => {
    const s = `{scenario:s${i}}`;
    const reqs = val(m[`http_reqs${s}`], 'count');
    const dropped = val(m[`dropped_iterations${s}`], 'count');
    const errRate = val(m[`http_req_failed${s}`], 'rate');
    const l = lat(m[`http_req_duration${s}`]);
    const achieved = reqs / STEP;
    const ok = errRate < 0.001 && l !== null && l.p99 < P99_MS && dropped < 0.02 * target * STEP;
    return {
      target_rps: target, achieved_rps: achieved, requests: reqs, error_rate: errRate,
      dropped_iterations: dropped, p50_ms: l && l.p50, p99_ms: l && l.p99, max_ms: l && l.max,
      sustainable: ok,
    };
  });
  const good = steps.filter((s) => s.sustainable);
  // Highest passing step; its measured (not requested) rate is the number to quote.
  const best = good.length ? good[good.length - 1] : null;
  const result = {
    test: 'maxrps', target: TARGET, url: URL,
    criteria: { max_error_rate: 0.001, p99_ms_below: P99_MS, max_dropped_fraction: 0.02, step_duration_s: STEP },
    steps,
    max_sustainable_rps: best ? best.achieved_rps : null,
    max_sustainable_step_target_rps: best ? best.target_rps : null,
  };
  return jsonSummary(`maxrps-${LABEL}.json`, result);
}
