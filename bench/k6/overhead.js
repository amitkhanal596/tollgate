// Gateway overhead: identical constant-arrival-rate load sent (a) straight to
// one demo backend and (b) through the gateway to that same single backend
// (route /bare -> upstream "single" -> demo-1). Overhead = gateway - direct.
//
// Phases run one after another (never together) so they do not compete for
// CPU. With ROUNDS>=2 the order is direct, gateway, gateway, direct (ABBA) so
// a warm-up or thermal drift does not favour one side.
//
// Env: RATE (rps, 1000)  DURATION (per phase, 30s)  WARMUP (per target, 5s)
//      ROUNDS (2)  OUT_DIR (/out)  OUT_NAME (overhead.json)
import http from 'k6/http';
import { check } from 'k6';
import { BACKEND, GATEWAY, TREND_STATS, jsonSummary, lat, reportSubmetric, val } from './common.js';

const RATE = parseInt(__ENV.RATE || '1000', 10);
const DURATION = __ENV.DURATION || '30s';
const WARMUP = __ENV.WARMUP || '5s';
const ROUNDS = parseInt(__ENV.ROUNDS || '2', 10);

const secs = (s) => parseFloat(s) * (s.endsWith('m') ? 60 : 1);
const dur = secs(DURATION);
const warm = secs(WARMUP);

const URLS = { direct: `${BACKEND}/bare`, gateway: `${GATEWAY}/bare` };
const order = ROUNDS >= 2 ? ['direct', 'gateway', 'gateway', 'direct'] : ['direct', 'gateway'];

const scenarios = {};
const thresholds = {};
let t = 0;
for (const target of ['direct', 'gateway']) { // warm both paths before measuring either
  scenarios[`warm_${target}`] = {
    executor: 'constant-arrival-rate', rate: Math.min(RATE, 500), timeUnit: '1s',
    duration: `${warm}s`, startTime: `${t}s`, preAllocatedVUs: 100, maxVUs: 400,
    exec: 'run', env: { TARGET: target }, tags: { phase: 'warmup', target: `warm_${target}` },
  };
  t += warm;
}
t += 1;
order.forEach((target, i) => {
  const name = `${target}_${i}`;
  scenarios[name] = {
    executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: `${dur}s`,
    startTime: `${t}s`, preAllocatedVUs: 200, maxVUs: 1000, exec: 'run',
    env: { TARGET: target }, tags: { phase: 'measure', target },
  };
  reportSubmetric(thresholds, `dropped_iterations{scenario:${name}}`);
  t += dur + 2;
});
for (const target of ['direct', 'gateway']) {
  reportSubmetric(thresholds, `http_req_duration{target:${target}}`);
  reportSubmetric(thresholds, `http_req_failed{target:${target}}`);
  reportSubmetric(thresholds, `http_reqs{target:${target}}`);
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
  const per = {};
  for (const target of ['direct', 'gateway']) {
    const reqs = val(m[`http_reqs{target:${target}}`], 'count');
    const dropped = order.reduce((n, tgt, i) => n + (tgt === target ? val(m[`dropped_iterations{scenario:${tgt}_${i}}`], 'count') : 0), 0);
    per[target] = {
      requests: reqs,
      error_rate: val(m[`http_req_failed{target:${target}}`], 'rate'),
      dropped_iterations: dropped,
      latency_ms: lat(m[`http_req_duration{target:${target}}`]),
    };
  }
  const d = per.direct.latency_ms, g = per.gateway.latency_ms;
  const delta = {};
  if (d && g) for (const k of Object.keys(d)) delta[k] = g[k] - d[k];
  const result = {
    test: 'overhead',
    config: { rate_rps: RATE, duration_s: dur, warmup_s: warm, order, direct_url: URLS.direct, gateway_url: URLS.gateway },
    direct: per.direct, gateway: per.gateway,
    overhead_ms: delta, // gateway minus direct, per statistic
  };
  return jsonSummary(__ENV.OUT_NAME || 'overhead.json', result);
}
