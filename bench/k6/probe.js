// Steady load with a per-request log, for the failover chaos script.
// Every request writes one line to the k6 console (the script captures it with
// --console-output):
//     R <start_epoch_ms> <http_status|0> <instance|-> <latency_ms>
// status 0 = connection-level failure. instance is the "instance" field of the
// demo backend's JSON body ("-" when there is none, e.g. on a 5xx).
// k6 stamps these with the container clock, which is the Docker VM clock, the
// same clock `docker inspect` uses for State.FinishedAt/StartedAt.
//
// For the "process alive but failing" variant (MODE=fail500) this script
// injects the fault itself (POST /admin/fail on the victim, over the compose
// network) so the injection time is on the same clock as the request log:
//     E fault_on <ms>   E fault_off <ms>
//
// Env: PATH_PREFIX (/fo-passive)  RATE (200)  TOTAL (total seconds)
//      MODE (kill | fail500)  INJECT_AT / RESTORE_AT (seconds, fail500 only)
//      VICTIM_URL (http://demo-1:8080)
import http from 'k6/http';
import { sleep } from 'k6';
import { GATEWAY } from './common.js';

const RATE = parseInt(__ENV.RATE || '200', 10);
const TOTAL = parseFloat(__ENV.TOTAL || '50');
const MODE = __ENV.MODE || 'kill';
const INJECT_AT = parseFloat(__ENV.INJECT_AT || '5');
const RESTORE_AT = parseFloat(__ENV.RESTORE_AT || '25');
const VICTIM = __ENV.VICTIM_URL || 'http://demo-1:8080';
const URL = `${GATEWAY}${__ENV.PATH_PREFIX || '/fo-passive'}/x`;

const scenarios = {
  load: {
    executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: `${TOTAL}s`,
    preAllocatedVUs: 100, maxVUs: 500, exec: 'load',
  },
};
if (MODE === 'fail500') {
  scenarios.inject = {
    executor: 'per-vu-iterations', vus: 1, iterations: 1, maxDuration: `${TOTAL}s`, exec: 'inject',
  };
}
export const options = { scenarios };

export function load() {
  const t = Date.now();
  const res = http.get(URL, { timeout: '3s' });
  let inst = '-';
  if (res.status === 200) {
    try { inst = res.json('instance'); } catch (e) { inst = '?'; }
  }
  console.log(`R ${t} ${res.status} ${inst} ${res.timings.duration.toFixed(1)}`);
}

export function inject() {
  sleep(INJECT_AT);
  let t = Date.now();
  const on = http.post(`${VICTIM}/admin/fail?mode=500`, null, { timeout: '3s' });
  console.log(`E fault_on ${t} ${on.status}`);
  sleep(RESTORE_AT - INJECT_AT);
  t = Date.now();
  const off = http.post(`${VICTIM}/admin/fail?mode=off`, null, { timeout: '3s' });
  console.log(`E fault_off ${t} ${off.status}`);
}

// The per-request log is the output; suppress k6's end-of-test summary.
export function handleSummary() { return {}; }
