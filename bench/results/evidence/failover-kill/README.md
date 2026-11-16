# Evidence: why a killed backend cost 69 requests with Consul and 4 without

These are the raw request logs and analyses of the six `kill` runs behind the failover table in
[`../../RESULTS.md`](../../RESULTS.md) (run `20261116T162040Z`, commit `b80a383`). The rest of the raw
data is not committed (`bench/results/raw/` is git-ignored); these files are kept because the
explanation in the README and in `docs/` rests on them.

`python3 split.py` prints the table below from the logs.

| run | failed | fast (< 100 ms) | slow (>= 900 ms) | slow latency range (ms) | last slow start after fault (ms) | first slow end after fault (ms) |
|---|---|---|---|---|---|---|
| consul-kill r1 | 69 | 2 | 67 | 1000.4 .. 1002.2 | 1025 | 1035.8 |
| consul-kill r2 | 69 | 2 | 67 | 1000.3 .. 1016.5 | 1018 | 1028.3 |
| consul-kill r3 | 69 | 2 | 67 | 1000.3 .. 1002.5 | 1019 | 1029.5 |
| passive-kill r1 | 3 | 3 | 0 | n/a | n/a | n/a |
| passive-kill r2 | 4 | 3 | 1 | 1000.8 .. 1000.8 | 21 | 1021.8 |
| passive-kill r3 | 4 | 3 | 1 | 1000.6 .. 1000.6 | 26 | 1026.6 |

How to read it:

- **Consul-discovered backends** are dialled by IP (the demo registers its container IP). After the
  kill, 2 requests failed at once and every later request to that backend waited the full 1 s dial
  timeout set in `deploy/compose/bench.yaml`. Passive health ejects after 3 consecutive failures, and
  it counts a failure when the request *completes*. The 2 fast failures plus the first dial timeout
  make 3, so the backend was ejected about 1.03 s after the kill: no slow request started after the
  first one ended (compare the last two columns). The 67 requests routed to it in that second each
  failed after waiting 1 s.
- **Static backends** are dialled by name. Docker's DNS stops resolving a killed container's name, so
  the dial fails at once (`lookup demo-1 ...: no such host`; seen in the gateway log on the demo
  stack, not captured in these files), three failures arrive within tens of milliseconds and the
  backend is ejected. The single slow failure in r2 and r3 is one request that was already mid-dial.

The fast failure in the static case is a property of Docker's DNS. On a real network, a dead host
reached by IP behaves like the Consul case: about one dial timeout of failed requests.
