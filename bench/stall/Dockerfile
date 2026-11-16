# syntax=docker/dockerfile:1.7
# Benchmark helper (stall watchdog, TCP counters). Build context: bench/stall.
#   docker build -t tollgate-bench/helper:dev bench/stall
# Same pattern as deploy/Dockerfile: multi-stage, distroless final image. The
# program is standard library only, so no go.mod is needed in this context.
FROM golang:1.26-alpine AS build
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
WORKDIR /src
COPY main.go .
RUN go build -trimpath -ldflags "-s -w" -o /out/helper ./main.go

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/helper /helper
USER 65532:65532
ENTRYPOINT ["/helper"]
