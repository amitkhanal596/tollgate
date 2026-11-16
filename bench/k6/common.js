// Helpers shared by the k6 scripts. Everything runs inside the k6 container,
// attached to the compose network, so hostnames are compose service names.

export const GATEWAY = __ENV.GATEWAY_URL || 'http://gateway:8080';
export const BACKEND = __ENV.BACKEND_URL || 'http://demo-1:8080';
export const OUT = __ENV.OUT_DIR || '/out';

// API key for bench-client-NN; its sha256 is in deploy/compose/bench.yaml.
export function apiKey(i) {
  return `bench-key-${String(i).padStart(2, '0')}-tollgate-load-test`;
}

// k6 only reports a sub-metric (e.g. http_req_duration{target:direct}) in the
// end-of-test data if some threshold mentions it, so register a harmless one.
// The aggregation must suit the metric type: counters take count, rates take rate, trends take max.
export function reportSubmetric(thresholds, name) {
  const base = name.split('{')[0];
  let expr = 'max>=0';
  if (['http_reqs', 'dropped_iterations'].includes(base)) expr = 'count>=0';
  else if (base === 'http_req_failed') expr = 'rate>=0';
  thresholds[name] = (thresholds[name] || []).concat([expr]);
}

export const TREND_STATS = ['min', 'med', 'p(90)', 'p(95)', 'p(99)', 'p(99.9)', 'max', 'avg'];

// Latency summary (ms) from a k6 trend metric, or null when it has no data.
export function lat(m) {
  if (!m || !m.values || m.values.max === undefined) return null;
  const v = m.values;
  return {
    min: v.min, p50: v.med, p90: v['p(90)'], p95: v['p(95)'], p99: v['p(99)'],
    p999: v['p(99.9)'], max: v.max, avg: v.avg,
  };
}

export function val(m, key, dflt = 0) {
  return m && m.values && m.values[key] !== undefined ? m.values[key] : dflt;
}

export function jsonSummary(file, obj) {
  const out = {};
  out[`${OUT}/${file}`] = JSON.stringify(obj, null, 2);
  // Keep k6's own end-of-test text on stdout.
  return out;
}
