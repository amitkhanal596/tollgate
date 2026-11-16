// Gateway overhead with a noise floor.
//
// Compares identical constant-arrival-rate load sent (a) straight to one demo
// backend and (b) through the gateway to that same backend (route /bare ->
// upstream "single" -> demo-1). Phases run one after another, never together.
//
// ROUNDS rounds. Each A/B round is four phases in ABBA order (direct, gateway,
// gateway, direct) or BAAB, alternating between rounds, so linear drift cancels.
// With AA=1 every A/B round is followed by an A/A round: the SAME direct path
// measured in the same ABBA pattern. Its "delta" is pure noise (rig jitter,
// scheduling, thermal drift), so it is the noise floor an A/B delta must beat.
//
// Every phase is its own scenario, tagged ph=<name>, and reports its own
// percentiles; summarize.py turns them into per-round deltas. Pooling percentiles
// across phases (the old approach) hid how much the rig moves between phases.
//
// Env: RATE (1000)  DURATION (per phase, 8s)  WARMUP (per target, 5s)
//      ROUNDS (5)  AA (1)  PHASE_GAP (1s)  OUT_DIR (/out)  OUT_NAME (overhead.json)
import http from 'k6/http';
import { check } from 'k6';
import { BACKEND, GATEWAY, TREND_STATS, jsonSummary, lat, reportSubmetric, val } from './common.js';

const RATE = parseInt(__ENV.RATE || '1000', 10);
const DURATION = __ENV.DURATION || '8s';
const WARMUP = __ENV.WARMUP || '5s';
const ROUNDS = parseInt(__ENV.ROUNDS || '5', 10);
const AA = (__ENV.AA || '1') === '1';
const PHASE_GAP = parseFloat(__ENV.PHASE_GAP || '1');

const secs = (s) => parseFloat(s) * (s.endsWith('m') ? 60 : 1);
const dur = secs(DURATION);
const warm = secs(WARMUP);

const URLS = { direct: `${BACKEND}/bare`, gateway: `${GATEWAY}/bare` };

const scenarios = {};
const thresholds = {};
const phases = []; // {name, kind: AB|AA, round, side: A|B, target}
let t = 0;
for (const target of ['direct', 'gateway']) { // warm both paths before measuring either
  scenarios[`warm_${target}`] = {
    executor: 'constant-arrival-rate', rate: Math.min(RATE, 500), timeUnit: '1s',
    duration: `${warm}s`, startTime: `${t}s`, preAllocatedVUs: 100, maxVUs: 400,
    exec: 'run', env: { TARGET: target }, tags: { ph: `warm_${target}` },
  };
  t += warm;
}
t += 1;

function addPhase(kind, round, side, target, idx) {
  const name = `${kind.toLowerCase()}${round}_${idx}${side}`;
  scenarios[name] = {
    executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: `${dur}s`,
    startTime: `${t}s`, preAllocatedVUs: 200, maxVUs: 1000, exec: 'run',
    env: { TARGET: target }, tags: { ph: name },
  };
  for (const m of ['http_req_duration', 'http_req_failed', 'http_reqs']) reportSubmetric(thresholds, `${m}{ph:${name}}`);
  reportSubmetric(thresholds, `dropped_iterations{scenario:${name}}`);
  phases.push({ name, kind, round, side, target });
  t += dur + PHASE_GAP;
}

for (let r = 0; r < ROUNDS; r++) {
  // A = direct. B = gateway for A/B rounds, direct again for A/A rounds.
  const seq = r % 2 === 0 ? ['A', 'B', 'B', 'A'] : ['B', 'A', 'A', 'B'];
  seq.forEach((side, i) => addPhase('AB', r, side, side === 'A' ? 'direct' : 'gateway', i));
  if (AA) seq.forEach((side, i) => addPhase('AA', r, side, 'direct', i));
}

export const options = {
  scenarios,
  thresholds,
  discardResponseBodies: true,
  summaryTrendStats: TREND_STATS,
};

export function run() {
  const res = http.get(URLS[__ENV.TARGET]);
  check(res, { 'status 200': (r) => r.status === 200 });
}

export function handleSummary(data) {
  const m = data.metrics;
  const out = phases.map((p) => ({
    ...p,
    requests: val(m[`http_reqs{ph:${p.name}}`], 'count'),
    error_rate: val(m[`http_req_failed{ph:${p.name}}`], 'rate'),
    dropped_iterations: val(m[`dropped_iterations{scenario:${p.name}}`], 'count'),
    latency_ms: lat(m[`http_req_duration{ph:${p.name}}`]),
  }));
  const result = {
    test: 'overhead',
    config: {
      rate_rps: RATE, phase_duration_s: dur, warmup_s: warm, rounds: ROUNDS, aa: AA,
      direct_url: URLS.direct, gateway_url: URLS.gateway,
    },
    phases: out,
  };
  return jsonSummary(__ENV.OUT_NAME || 'overhead.json', result);
}
