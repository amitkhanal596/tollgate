// Limiter accuracy and per-key isolation against route /limited
// (100 req/s sustained, burst 100, per API key).
//
// single: key 00 at 5x the limit for DURATION.
//   expected allowed = burst + rate * window, where window is the measured
//   time between the first and last request of the scenario.
// multi:  keys 00..08 (9 keys) at 5x the limit (the "noisy" clients) while key 09 sends
//   a polite 50 req/s at the same time. Every noisy key should be limited to
//   the same expected count, and the polite key should never see a 429.
//
// Every non-200 must be 429 with a Retry-After header ("bad_rejects" counts
// the ones that are not).
//
// Env: DURATION (30s)  OVER (5, offered load as a multiple of the limit)
//      RATE_LIMIT (100)  BURST (100)  POLITE_RPS (50)
import http from 'k6/http';
import { Counter, Trend } from 'k6/metrics';
import exec from 'k6/execution';
import { GATEWAY, apiKey, jsonSummary, val } from './common.js';

const DURATION = parseFloat(__ENV.DURATION || '30');
const OVER = parseFloat(__ENV.OVER || '5');
const LIMIT = parseFloat(__ENV.RATE_LIMIT || '100');
const BURST = parseFloat(__ENV.BURST || '100');
const POLITE_RPS = parseFloat(__ENV.POLITE_RPS || '50');
const NOISY = 9; // keys 00..08; key 09 is the polite one
const URL = `${GATEWAY}/limited/x`;

const allowed = [], rejected = [], keyTs = [];
for (let k = 0; k < 10; k++) {
  allowed.push(new Counter(`allowed_k${k}`));
  rejected.push(new Counter(`rejected_k${k}`));
  keyTs.push(new Trend(`ts_k${k}`)); // epoch ms of each request; min/max give the window
}
// Single-key phase (key 00) has its own metrics so the phases never mix.
const sAllowed = new Counter('single_allowed');
const sRejected = new Counter('single_rejected');
const sTs = new Trend('single_ts');
const badRejects = new Counter('bad_rejects');
const otherStatus = new Counter('other_status');
const noRetryAfter = new Counter('missing_retry_after');

const perKey = OVER * LIMIT;
export const options = {
  scenarios: {
    single: {
      executor: 'constant-arrival-rate', rate: perKey, timeUnit: '1s', duration: `${DURATION}s`,
      preAllocatedVUs: 100, maxVUs: 400, exec: 'single',
    },
    // Unmeasured: opens connection pools for the 5000 rps phase on the unlimited
    // /bare route (a cold start otherwise drops iterations in the load generator).
    warm: {
      executor: 'constant-arrival-rate', rate: perKey * (NOISY + 1), timeUnit: '1s', duration: '3s',
      startTime: `${DURATION + 1}s`, preAllocatedVUs: 400, maxVUs: 1500, exec: 'warm',
    },
    noisy: {
      executor: 'constant-arrival-rate', rate: perKey * NOISY, timeUnit: '1s', duration: `${DURATION}s`,
      startTime: `${DURATION + 5}s`, preAllocatedVUs: 400, maxVUs: 1500, exec: 'noisy',
    },
    polite: {
      executor: 'constant-arrival-rate', rate: POLITE_RPS, timeUnit: '1s', duration: `${DURATION}s`,
      startTime: `${DURATION + 5}s`, preAllocatedVUs: 20, maxVUs: 100, exec: 'polite',
    },
  },
  thresholds: {
    bad_rejects: ['count==0'],
    other_status: ['count==0'],
    // otherwise the offered load was not what we claim (warm-up drops do not matter)
    'dropped_iterations{scenario:single}': ['count==0'],
    'dropped_iterations{scenario:noisy}': ['count==0'],
    'dropped_iterations{scenario:polite}': ['count==0'],
  },
  discardResponseBodies: true,
};

function record(res, ok, rej) {
  if (res.status === 200) {
    ok.add(1);
  } else if (res.status === 429) {
    rej.add(1);
    if (!res.headers['Retry-After']) { noRetryAfter.add(1); badRejects.add(1); }
  } else {
    otherStatus.add(1);
    badRejects.add(1);
  }
}

function hit(key, ok, rej, ts) {
  const t = Date.now();
  const res = http.get(URL, { headers: { 'X-API-Key': apiKey(key) } });
  ts.add(t);
  record(res, ok, rej);
}

export function warm() { http.get(`${GATEWAY}/bare`); }
export function single() { hit(0, sAllowed, sRejected, sTs); }
// Round-robin over the noisy keys: each gets exactly perKey req/s.
export function noisy() {
  const k = exec.scenario.iterationInTest % NOISY;
  hit(k, allowed[k], rejected[k], keyTs[k]);
}
export function polite() { hit(9, allowed[9], rejected[9], keyTs[9]); }

export function handleSummary(data) {
  const m = data.metrics;
  const analyse = (key, a, r, tt, offeredRps) => {
    // Window: first request start to last request start, plus one inter-arrival
    // gap (the last request still claims its own token).
    const window = tt ? (tt.max - tt.min) / 1000 + 1 / offeredRps : 0;
    const expected = BURST + LIMIT * window;
    return {
      key, offered_rps: offeredRps, window_s: window, requests: a + r, allowed: a, rejected: r,
      expected_allowed: expected, error_pct: expected ? (100 * (a - expected)) / expected : null,
      allowed_rps_over_window: window ? a / window : null,
    };
  };
  const vals = (name) => (m[name] && m[name].values) || null;
  const single = analyse(0, val(m.single_allowed, 'count'), val(m.single_rejected, 'count'), vals('single_ts'), perKey);
  const keys = [];
  for (let k = 0; k < 10; k++) {
    keys.push(analyse(k, val(m[`allowed_k${k}`], 'count'), val(m[`rejected_k${k}`], 'count'), vals(`ts_k${k}`),
      k < NOISY ? perKey : POLITE_RPS));
  }
  const noisyRes = keys.slice(0, NOISY);
  const polite = keys[9];
  const result = {
    test: 'limiter',
    config: { rate_limit_rps: LIMIT, burst: BURST, duration_s: DURATION, offered_multiple: OVER },
    single_key: single,
    multi_key: {
      noisy_keys: noisyRes,
      noisy_max_abs_error_pct: Math.max(...noisyRes.map((r) => Math.abs(r.error_pct))),
      noisy_min_allowed: Math.min(...noisyRes.map((r) => r.allowed)),
      noisy_max_allowed: Math.max(...noisyRes.map((r) => r.allowed)),
      polite_key: polite,
      polite_rejected: polite.rejected,
    },
    rejected_total: single.rejected + keys.reduce((n, k) => n + k.rejected, 0),
    bad_rejects: val(m.bad_rejects, 'count'),
    missing_retry_after: val(m.missing_retry_after, 'count'),
    other_status: val(m.other_status, 'count'),
    dropped_iterations: ['single', 'noisy', 'polite'].reduce((n, sc) => n + val(m[`dropped_iterations{scenario:${sc}}`], 'count'), 0),
  };
  return jsonSummary('limiter.json', result);
}
