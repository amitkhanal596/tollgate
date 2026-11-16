// Limiter accuracy and per-key isolation against route /limited
// (100 req/s sustained, burst 100, per API key).
//
// single: key 00 at OVER x the limit for DURATION.
// multi:  keys 00..NOISY-1 at OVER x the limit in parallel (the "noisy" clients)
//         while key 09 sends a polite 50 req/s. Noisy keys should each be
//         limited to the same expected count; the polite key must never see a 429.
//
// This script only generates load and logs every request, one console line each
// (captured with --console-output into limiter.log):
//     L <key> <start_epoch_ms> <http_status> <retry_after_present 0|1>
// bench/analyze_limiter.py does the accounting. Expected allowed =
// burst + rate x window is only valid while the key's offered rate never drops
// below the limit for longer than burst/rate (a longer pause wastes tokens), so
// the analysis checks the largest gap between each key's requests rather than
// trusting k6's dropped-iteration counter.
//
// Defaults are deliberately modest (2x over the limit, 4 noisy keys, about
// 1000 rps in total) so that the load generator is not the bottleneck.
//
// Env: DURATION (15)  OVER (2)  NOISY (4)  RATE_LIMIT (100)  BURST (100)  POLITE_RPS (50)
import http from 'k6/http';
import exec from 'k6/execution';
import { GATEWAY, apiKey, jsonSummary, reportSubmetric, val } from './common.js';

const DURATION = parseFloat(__ENV.DURATION || '15');
const OVER = parseFloat(__ENV.OVER || '2');
const LIMIT = parseFloat(__ENV.RATE_LIMIT || '100');
const BURST = parseFloat(__ENV.BURST || '100');
const POLITE_RPS = parseFloat(__ENV.POLITE_RPS || '50');
const NOISY = parseInt(__ENV.NOISY || '4', 10); // keys 00..NOISY-1; key 09 is the polite one
const URL = `${GATEWAY}/limited/x`;

const perKey = OVER * LIMIT;
const thresholds = {};
for (const sc of ['single', 'noisy', 'polite']) reportSubmetric(thresholds, `dropped_iterations{scenario:${sc}}`);
export const options = {
  scenarios: {
    single: {
      executor: 'constant-arrival-rate', rate: perKey, timeUnit: '1s', duration: `${DURATION}s`,
      preAllocatedVUs: 50, maxVUs: 200, exec: 'single',
    },
    // Unmeasured: opens connection pools for the multi-key phase on the unlimited /bare route.
    warm: {
      executor: 'constant-arrival-rate', rate: perKey * NOISY, timeUnit: '1s', duration: '3s',
      startTime: `${DURATION + 1}s`, preAllocatedVUs: 100, maxVUs: 400, exec: 'warm',
    },
    noisy: {
      executor: 'constant-arrival-rate', rate: perKey * NOISY, timeUnit: '1s', duration: `${DURATION}s`,
      startTime: `${DURATION + 5}s`, preAllocatedVUs: 100, maxVUs: 400, exec: 'noisy',
    },
    polite: {
      executor: 'constant-arrival-rate', rate: POLITE_RPS, timeUnit: '1s', duration: `${DURATION}s`,
      startTime: `${DURATION + 5}s`, preAllocatedVUs: 20, maxVUs: 100, exec: 'polite',
    },
  },
  thresholds,
  discardResponseBodies: true,
};

function hit(key) {
  const t = Date.now();
  const res = http.get(URL, { headers: { 'X-API-Key': apiKey(key) } });
  console.log(`L ${key} ${t} ${res.status} ${res.headers['Retry-After'] ? 1 : 0}`);
}

export function warm() { http.get(`${GATEWAY}/bare`); }
export function single() { hit(0); }
// Round-robin over the noisy keys: each gets exactly perKey req/s on average.
export function noisy() { hit(exec.scenario.iterationInTest % NOISY); }
export function polite() { hit(9); }

export function handleSummary(data) {
  const m = data.metrics;
  const scen = (sc) => val(m[`dropped_iterations{scenario:${sc}}`], 'count');
  const result = {
    test: 'limiter-raw',
    config: {
      rate_limit_rps: LIMIT, burst: BURST, duration_s: DURATION, offered_multiple: OVER,
      noisy_keys: NOISY, polite_rps: POLITE_RPS, offered_per_key_rps: perKey,
    },
    // Informational only: pass/fail comes from the per-key gap check.
    k6_dropped_iterations: { single: scen('single'), noisy: scen('noisy'), polite: scen('polite') },
  };
  return jsonSummary('limiter-raw.json', result);
}
