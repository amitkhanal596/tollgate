# syntax=docker/dockerfile:1.7
#
# One Dockerfile, three runnable targets: gateway, controlplane, demo (plus the
# shared build stage). Build context is the repository root:
#
#   docker build -f deploy/Dockerfile --target gateway -t tollgate/gateway .
#
# Final images are distroless/static (no shell, no package manager) and run as
# the built-in nonroot user (uid 65532).

# golang:1.26 satisfies go.mod's "go 1.26.9". GOTOOLCHAIN=local makes a version
# mismatch a build error instead of a silent toolchain download.
FROM golang:1.26-alpine AS build
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
WORKDIR /src

# Dependencies first: this layer is reused until go.mod/go.sum change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
# -trimpath: reproducible paths; -s -w: strip symbol and DWARF tables.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    for p in gateway:./cmd/gateway controlplane:./cmd/controlplane demo:./deploy/demo; do \
      go build -trimpath -ldflags "-s -w" -o "/out/${p%%:*}" "${p#*:}"; \
    done; \
    # Empty dir, copied below with the nonroot owner, so a mounted volume
    # for the last-known-good config is writable by uid 65532.
    mkdir -p /out/var-lib-tollgate

FROM gcr.io/distroless/static-debian12:nonroot AS gateway
COPY --from=build /out/gateway /gateway
COPY --from=build --chown=65532:65532 /out/var-lib-tollgate /var/lib/tollgate
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/gateway"]

FROM gcr.io/distroless/static-debian12:nonroot AS controlplane
COPY --from=build /out/controlplane /controlplane
USER 65532:65532
EXPOSE 9000
ENTRYPOINT ["/controlplane"]

# Demo-only upstream service. Not part of the product.
FROM gcr.io/distroless/static-debian12:nonroot AS demo
COPY --from=build /out/demo /demo
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/demo"]
