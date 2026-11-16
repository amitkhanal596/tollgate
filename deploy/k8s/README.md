# Tollgate on Kubernetes (kind)

Demo manifests: Postgres (emptyDir, so data is lost when the pod is
rescheduled), a single-node Consul dev agent, the control plane, a 2-replica
gateway, and three demo upstreams. Credentials in `secret.yaml` are DEMO-ONLY.

The gateway Service is a NodePort (30080) that `kind-config.yaml` maps to
`127.0.0.1:8080` on the host.

```sh
# From the repo root.
docker build -f deploy/Dockerfile --target gateway      -t tollgate/gateway:dev .
docker build -f deploy/Dockerfile --target controlplane -t tollgate/controlplane:dev .
docker build -f deploy/Dockerfile --target demo         -t tollgate/demo:dev .

kind create cluster --name tollgate --config deploy/k8s/kind-config.yaml
kind load docker-image --name tollgate tollgate/gateway:dev tollgate/controlplane:dev tollgate/demo:dev

kubectl apply -k deploy/k8s
kubectl -n tollgate rollout status deploy/postgres deploy/consul deploy/controlplane deploy/gateway deploy/demo-1 deploy/demo-2 deploy/demo-3 --timeout=180s

# Smoke test
curl -s 127.0.0.1:8080/                                   # JSON from one of the demo pods (Consul discovery)
curl -s -o /dev/null -w '%{http_code}\n' 127.0.0.1:8080/api/x          # 401
curl -s -o /dev/null -w '%{http_code}\n' -H 'X-API-Key: demo-api-key-tollgate-0001' 127.0.0.1:8080/api/x   # 200

# Admin API (control plane is cluster-internal)
kubectl -n tollgate port-forward svc/controlplane 9000:9000 &
curl -s -H 'Authorization: Bearer DEMO-ONLY-admin-token-0123456789abcdef-change-me' 127.0.0.1:9000/v1/dataplanes

# Clean up
kind delete cluster --name tollgate
```

Shutdown: `-shutdown-delay 5s` + `-drain-timeout 15s` fit inside
`terminationGracePeriodSeconds: 30`. During the delay `/readyz` returns 503 while the
gateway keeps serving, so endpoint removal can finish before the listener closes.
`kubectl -n tollgate rollout restart deploy/gateway` drops no requests
(`maxUnavailable: 0`).

## Demo caveats

- **Rate limit is per gateway instance.** There are 2 gateway replicas and no
  shared limiter state, so the seed's "5 req/s per client" is effectively up
  to 10 req/s (ADR 0004).
- **Consul is a `-dev` agent with no ACLs and no persistence.** Demo only: any
  pod in the cluster can register a "demo" instance pointing anywhere. The
  readiness probe runs `consul info | grep 'leader = true'` because
  `/v1/status/leader` returns 200 with an empty body before a leader exists.
  After a Consul restart the demo pods re-register within about 15 s.
- **Port clash with Compose.** kind maps `127.0.0.1:8080` on the host, which
  is also the Compose gateway's port. Run `make down` before creating the
  kind cluster (or the reverse).
- **No fault injection.** The demo pods run without `-fault-injection`, so
  `/admin/fail` returns 404 through the gateway. Only the bench stack enables it.
