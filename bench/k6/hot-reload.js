// Hot reload under load. Steady load goes through the gateway to /flip while a
// single "reloader" VU makes N config changes through the control plane's
// admin API, one after another:
//   - PUT /v1/routes/flip, pointing the route at the other upstream
//     (flip-a = demo-1 <-> flip-b = demo-2), or
//   - every ROLLBACK_EVERY-th change, POST /v1/config/rollback (head-1 undoes
//     the previous flip, so the route moves back to the other upstream).
// After each change returns, the reloader polls /flip until the response
// comes from the expected instance. Propagation latency = the time from the
// admin call returning to that first response (poll granularity ~2 ms + one
// request round trip; the SSE push and the snapshot build are inside it).
//
// Dropped = any request through the gateway (load and polls) that was not 2xx
// or failed at connection level (status 0). Everything runs on one clock.
//
// Env: CHANGES (100)  RATE (500)  GAP (0.25, seconds between changes)
//      ROLLBACK_EVERY (10)  LOAD_SECONDS (CHANGES*(GAP+0.3)+10)
//      CP_URL (http://controlplane:9000)  ADMIN_TOKEN (required)
import http from 'k6/http';
import { Counter, Gauge, Trend } from 'k6/metrics';
import { sleep } from 'k6';
import { GATEWAY, TREND_STATS, jsonSummary, val } from './common.js';

const CHANGES = parseInt(__ENV.CHANGES || '100', 10);
const RATE = parseInt(__ENV.RATE || '500', 10);
const GAP = parseFloat(__ENV.GAP || '0.25');
const ROLLBACK_EVERY = parseInt(__ENV.ROLLBACK_EVERY || '10', 10);
const LOAD_SECONDS = parseFloat(__ENV.LOAD_SECONDS || String(CHANGES * (GAP + 0.3) + 10));
const CP = __ENV.CP_URL || 'http://controlplane:9000';
const TOKEN = __ENV.ADMIN_TOKEN;
const FLIP_URL = `${GATEWAY}/flip/x`;
const START_AT = 3; // seconds of load before the first change

const total = new Counter('gw_requests');
const non2xx = new Counter('gw_non2xx');
const connErr = new Counter('gw_conn_errors');
const propagation = new Trend('reload_propagation_ms', true);
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
    reloader: {
      executor: 'per-vu-iterations', vus: 1, iterations: 1, startTime: `${START_AT}s`,
      maxDuration: `${LOAD_SECONDS}s`, exec: 'reloader',
    },
  },
  thresholds: {
    gw_requests: ['count>0'],
    reload_propagation_ms: ['max>=0'],
  },
  summaryTrendStats: TREND_STATS,
};

function count(res) {
  total.add(1);
  if (res.status === 0) connErr.add(1);
  else if (res.status < 200 || res.status > 299) non2xx.add(1);
}

export function load() {
  count(http.get(FLIP_URL, { responseType: 'none', timeout: '5s' }));
}

const authHeaders = { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' };

export function reloader() {
  const began = Date.now();
  let cur = 'demo-1'; // flip-a at startup
  for (let i = 1; i <= CHANGES; i++) {
    const other = cur === 'demo-1' ? 'demo-2' : 'demo-1';
    const rollback = ROLLBACK_EVERY > 0 && i % ROLLBACK_EVERY === 0;
    let res;
    const t0 = Date.now();
    if (rollback) {
      res = http.post(`${CP}/v1/config/rollback`, '{}', { headers: authHeaders, timeout: '10s' });
    } else {
      const body = JSON.stringify({
        name: 'flip', path_prefix: '/flip', upstream: other === 'demo-1' ? 'flip-a' : 'flip-b',
      });
      res = http.put(`${CP}/v1/routes/flip`, body, { headers: authHeaders, timeout: '10s' });
    }
    const t1 = Date.now(); // admin call returned
    adminCall.add(t1 - t0);
    if (res.status !== 200 && res.status !== 201) {
      adminFailed.add(1);
      console.error(`change ${i}: admin call failed: ${res.status} ${res.body}`);
      sleep(GAP);
      continue; // state unknown; leave `cur` unchanged and let the next change re-sync
    }
    changesDone.add(1);
    (rollback ? rollbacksDone : putsDone).add(1);

    // Poll until the gateway serves the new target (10 s cap).
    let seen = false;
    while (Date.now() - t1 < 10000) {
      const r = http.get(FLIP_URL, { timeout: '5s' });
      count(r);
      if (r.status === 200 && r.json('instance') === other) {
        propagation.add(Date.now() - t1);
        seen = true;
        break;
      }
      sleep(0.002);
    }
    if (!seen) neverSeen.add(1);
    cur = other;
    sleep(GAP);
  }
  reloaderSeconds.add((Date.now() - began) / 1000);
}

export function handleSummary(data) {
  const m = data.metrics;
  const p = m.reload_propagation_ms && m.reload_propagation_ms.values;
  const result = {
    test: 'hot-reload',
    config: { changes: CHANGES, load_rps: RATE, gap_s: GAP, rollback_every: ROLLBACK_EVERY, load_seconds: LOAD_SECONDS },
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
    propagation_ms: p && { min: p.min, p50: p.med, p90: p['p(90)'], p95: p['p(95)'], p99: p['p(99)'], max: p.max, avg: p.avg },
    admin_call_ms: m.reload_admin_call_ms && { p50: m.reload_admin_call_ms.values.med, max: m.reload_admin_call_ms.values.max },
  };
  return jsonSummary('hot-reload.json', result);
}
