// Hot reload under load. Two kinds of steady traffic go through the gateway
// while one "reloader" VU makes CHANGES config changes through the control
// plane's admin API, spread evenly over the whole load window (one every PERIOD
// seconds):
//
//   fast load  GET /flip     at RATE rps. Route "flip" is what the reloader
//              flips between flip-a (demo-1) and flip-b (demo-2); after each
//              flip it polls /flip until the new instance answers, which
//              measures propagation.
//   slow load  GET /hrslow   at SLOW_RATE rps against demo-3, which the wrapper
//              puts in "slow" mode (every response takes 2 s). Each such request
//              is in flight for 2 s, so about 2 x SLOW_RATE requests span every
//              single config swap. Odd changes flip route "flip"; even changes
//              flip route "hrslow" between upstreams slow-a and slow-b (both
//              demo-3), so some in-flight requests span a swap of their own route.
//   Every ROLLBACK_EVERY-th change is POST /v1/config/rollback instead, which
//   undoes the change before it.
//
// Dropped = any request through the gateway (load and polls) that was not 2xx
// or failed at connection level (status 0).
//
// Every load request and every change is logged (console, captured with
// --console-output into hot-reload.log) so bench/chaos/analyze_hotreload.py can
// count the requests whose [start, end] interval contains a change:
//     R <F|S> <start_ms> <end_ms> <status>
//     C <index> <flip|slow> <rollback 0|1> <sent_ms> <returned_ms> <seen_ms|0>
//
// Env: CHANGES (100)  RATE (300)  SLOW_RATE (100)  PERIOD (0.5, seconds between change starts)
//      ROLLBACK_EVERY (10)  CP_URL (http://controlplane:9000)  ADMIN_TOKEN (required)
import http from 'k6/http';
import { Counter, Gauge, Trend } from 'k6/metrics';
import { sleep } from 'k6';
import { GATEWAY, TREND_STATS, jsonSummary, val } from './common.js';

const CHANGES = parseInt(__ENV.CHANGES || '100', 10);
const RATE = parseInt(__ENV.RATE || '300', 10);
const SLOW_RATE = parseInt(__ENV.SLOW_RATE || '100', 10);
const PERIOD = parseFloat(__ENV.PERIOD || '0.5');
const ROLLBACK_EVERY = parseInt(__ENV.ROLLBACK_EVERY || '10', 10);
const START_AT = 3; // seconds of load before the first change
const LOAD_SECONDS = START_AT + CHANGES * PERIOD + 5; // load outlasts the last change
const CP = __ENV.CP_URL || 'http://controlplane:9000';
const TOKEN = __ENV.ADMIN_TOKEN;
const FLIP_URL = `${GATEWAY}/flip/x`;
const SLOW_URL = `${GATEWAY}/hrslow/x`;

const total = new Counter('gw_requests');
const non2xx = new Counter('gw_non2xx');
const connErr = new Counter('gw_conn_errors');
const propagation = new Trend('reload_propagation_ms', true); // from the admin call RETURNING: a lower bound
const propFromSend = new Trend('reload_propagation_from_send_ms', true); // from the admin call being SENT: an upper bound
const adminCall = new Trend('reload_admin_call_ms', true);
const changesDone = new Counter('changes_done');
const putsDone = new Counter('puts_done');
const rollbacksDone = new Counter('rollbacks_done');
const adminFailed = new Counter('admin_failed');
const reloaderSeconds = new Gauge('reloader_seconds');
const neverSeen = new Counter('propagation_timeouts');

export const options = {
  scenarios: {
    load: {
      executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: `${LOAD_SECONDS}s`,
      preAllocatedVUs: 100, maxVUs: 500, exec: 'load',
    },
    slow: {
      executor: 'constant-arrival-rate', rate: SLOW_RATE, timeUnit: '1s', duration: `${LOAD_SECONDS}s`,
      preAllocatedVUs: SLOW_RATE * 3, maxVUs: SLOW_RATE * 8, exec: 'slow',
    },
    reloader: {
      executor: 'per-vu-iterations', vus: 1, iterations: 1, startTime: `${START_AT}s`,
      maxDuration: `${LOAD_SECONDS}s`, exec: 'reloader',
    },
  },
  thresholds: {
    gw_requests: ['count>0'],
    reload_propagation_ms: ['max>=0'],
    reload_propagation_from_send_ms: ['max>=0'],
    reload_admin_call_ms: ['max>=0'],
    dropped_iterations: ['count>=0'],
  },
  summaryTrendStats: TREND_STATS,
};

function count(res) {
  total.add(1);
  if (res.status === 0) connErr.add(1);
  else if (res.status < 200 || res.status > 299) non2xx.add(1);
}

function loadReq(kind, url, timeout) {
  const t = Date.now();
  const res = http.get(url, { responseType: 'none', timeout });
  count(res);
  console.log(`R ${kind} ${t} ${Date.now()} ${res.status}`);
}

export function load() { loadReq('F', FLIP_URL, '5s'); }
export function slow() { loadReq('S', SLOW_URL, '10s'); }

const authHeaders = { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' };

export function reloader() {
  const began = Date.now();
  const cur = { flip: 'flip-a', slow: 'slow-a' };   // current upstream per route
  const other = (u) => ({ 'flip-a': 'flip-b', 'flip-b': 'flip-a', 'slow-a': 'slow-b', 'slow-b': 'slow-a' }[u]);
  const instanceOf = { 'flip-a': 'demo-1', 'flip-b': 'demo-2' };
  let last = null; // the previous change: {kind, before}, what a rollback undoes
  for (let i = 1; i <= CHANGES; i++) {
    const wait = began + (i - 1) * PERIOD * 1000 - Date.now(); // spread changes over the whole window
    if (wait > 0) sleep(wait / 1000);

    const rollback = ROLLBACK_EVERY > 0 && i % ROLLBACK_EVERY === 0 && last !== null;
    const kind = rollback ? last.kind : (i % 2 === 1 ? 'flip' : 'slow');
    const target = rollback ? last.before : other(cur[kind]);
    let res;
    const sent = Date.now();
    if (rollback) {
      res = http.post(`${CP}/v1/config/rollback`, '{}', { headers: authHeaders, timeout: '10s' });
    } else {
      const body = JSON.stringify({
        name: kind === 'flip' ? 'flip' : 'hrslow', path_prefix: kind === 'flip' ? '/flip' : '/hrslow', upstream: target,
      });
      res = http.put(`${CP}/v1/routes/${kind === 'flip' ? 'flip' : 'hrslow'}`, body, { headers: authHeaders, timeout: '10s' });
    }
    const returned = Date.now(); // admin call returned
    adminCall.add(returned - sent);
    if (res.status !== 200 && res.status !== 201) {
      adminFailed.add(1);
      console.error(`change ${i}: admin call failed: ${res.status} ${res.body}`);
      console.log(`C ${i} ${kind} ${rollback ? 1 : 0} ${sent} ${returned} 0`);
      continue; // state unknown; the next change re-syncs
    }
    changesDone.add(1);
    (rollback ? rollbacksDone : putsDone).add(1);

    // Only the flip route is observable (instances differ); poll until the gateway serves the new target (10 s cap).
    let seenAt = 0;
    if (kind === 'flip') {
      while (Date.now() - returned < 10000) {
        const r = http.get(FLIP_URL, { timeout: '5s' });
        count(r);
        if (r.status === 200 && r.json('instance') === instanceOf[target]) {
          seenAt = Date.now();
          propagation.add(seenAt - returned);
          propFromSend.add(seenAt - sent);
          break;
        }
        sleep(0.002);
      }
      if (!seenAt) neverSeen.add(1);
    }
    console.log(`C ${i} ${kind} ${rollback ? 1 : 0} ${sent} ${returned} ${seenAt}`);
    if (!rollback) last = { kind, before: cur[kind] };
    else last = null; // a rollback is not itself rolled back
    cur[kind] = target;
  }
  reloaderSeconds.add((Date.now() - began) / 1000);
}

export function handleSummary(data) {
  const m = data.metrics;
  const tr = (name) => {
    const v = m[name] && m[name].values;
    return v && { min: v.min, p50: v.med, p90: v['p(90)'], p95: v['p(95)'], p99: v['p(99)'], max: v.max, avg: v.avg };
  };
  const result = {
    test: 'hot-reload',
    config: {
      changes: CHANGES, load_rps: RATE, slow_load_rps: SLOW_RATE, period_s: PERIOD, rollback_every: ROLLBACK_EVERY,
      load_seconds: LOAD_SECONDS, slow_backend_delay_s: 2,
    },
    changes_done: val(m.changes_done, 'count'),
    puts: val(m.puts_done, 'count'),
    rollbacks: val(m.rollbacks_done, 'count'),
    reloader_seconds: val(m.reloader_seconds, 'value'),
    admin_failed: val(m.admin_failed, 'count'),
    propagation_timeouts: val(m.propagation_timeouts, 'count'),
    requests_total: val(m.gw_requests, 'count'),
    non_2xx: val(m.gw_non2xx, 'count'),
    connection_errors: val(m.gw_conn_errors, 'count'),
    dropped_iterations: val(m.dropped_iterations, 'count'),
    // Lower bound: clock starts when the admin call returned. Upper bound: clock starts when it was sent.
    propagation_ms: tr('reload_propagation_ms'),
    propagation_from_send_ms: tr('reload_propagation_from_send_ms'),
    admin_call_ms: tr('reload_admin_call_ms'),
  };
  return jsonSummary('hot-reload-raw.json', result);
}
