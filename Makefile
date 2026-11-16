# Tollgate developer tasks. `make help` lists them.
COMPOSE   := docker compose -f deploy/compose/docker-compose.yml
COVER_MIN := 80
# Keep identical to the golangci-lint version pinned in .github/workflows/ci.yml.
GOLANGCI_LINT_VERSION := v2.15.0
BENCH_COMPOSE := docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/docker-compose.bench.yml
DEMO_KEY  := demo-api-key-tollgate-0001
ADMIN_TOK := DEMO-ONLY-admin-token-0123456789abcdef-change-me

.PHONY: help test cover lint build images up down demo logs bench bench-all chaos bench-up bench-down

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/'

test: ## go vet + go test -race
	go vet ./...
	go test -race ./...

cover: ## internal/ coverage; fails under $(COVER_MIN)%
	go test -race -coverprofile=coverage.out -coverpkg=./internal/... ./...
	@total=$$(go tool cover -func=coverage.out | awk '/^total:/ {gsub("%","",$$3); print $$3}'); \
	  echo "internal/ coverage: $$total% (min $(COVER_MIN)%)"; \
	  awk -v t="$$total" -v m="$(COVER_MIN)" 'BEGIN { exit (t+0 < m+0) }' || { echo "coverage below $(COVER_MIN)%"; exit 1; }

lint: ## golangci-lint (local binary, else the Docker image)
	@if command -v golangci-lint >/dev/null; then golangci-lint run ./...; \
	else docker run --rm -v "$$PWD":/src -w /src golangci/golangci-lint:$(GOLANGCI_LINT_VERSION) golangci-lint run ./...; fi

build: ## build both binaries into bin/
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/gateway ./cmd/gateway
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/controlplane ./cmd/controlplane

images: ## build gateway, controlplane and demo images
	$(COMPOSE) build

up: ## start the demo stack and wait until it works
	$(COMPOSE) up -d --build --wait

down: ## stop the demo stack and delete its volumes
	$(COMPOSE) down -v

logs: ## follow all logs
	$(COMPOSE) logs -f --tail=50

demo: ## a few requests showing routing, auth and rate limiting
	@echo "--- / (any instance, via Consul discovery)"; \
	for i in 1 2 3 4 5 6; do curl -s 127.0.0.1:8080/; done
	@echo "--- /static (round robin, static backends)"; \
	for i in 1 2 3; do curl -s 127.0.0.1:8080/static/x; done
	@echo "--- /api without key -> 401"; curl -s -o /dev/null -w '%{http_code}\n' 127.0.0.1:8080/api/x
	@echo "--- /api with key -> 200"; curl -s -o /dev/null -w '%{http_code}\n' -H 'X-API-Key: $(DEMO_KEY)' 127.0.0.1:8080/api/x
	@echo "--- 30 rapid /api requests (burst 10, 5/s) -> 200s then 429s"; \
	for i in $$(seq 30); do curl -s -o /dev/null -w '%{http_code}\n' -H 'X-API-Key: $(DEMO_KEY)' 127.0.0.1:8080/api/x; done | sort | uniq -c
	@echo "--- 429 headers"; curl -si -H 'X-API-Key: $(DEMO_KEY)' 127.0.0.1:8080/api/x | grep -i -E '^(HTTP|retry-after)'
	@echo "--- control plane: connected gateways"; \
	curl -s -H 'Authorization: Bearer $(ADMIN_TOK)' 127.0.0.1:9000/v1/dataplanes; echo
	@echo "Grafana http://127.0.0.1:3000  Prometheus http://127.0.0.1:9091  Consul http://127.0.0.1:8500"

bench-up: ## start the benchmark stack (access log off) and wait until it works
	$(BENCH_COMPOSE) up -d --build --wait

bench-down: ## stop the benchmark stack and delete its volumes
	$(BENCH_COMPOSE) down -v --remove-orphans

bench: ## overhead, max RPS and limiter accuracy (~15 min); results in bench/results/
	bench/run-all.sh bench

chaos: ## failover and hot-reload chaos tests (~9 min); results in bench/results/
	bench/run-all.sh chaos

bench-all: ## bench + chaos (~25 min)
	bench/run-all.sh all
