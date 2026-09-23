# replicare — multi-stage build producing a small, static image.
#
# The binary is pure-Go (pgx has no cgo), so it links statically. The runtime
# stage is Chainguard's wolfi-base: a minimal, low-CVE image that — unlike
# `scratch` — ships a shell (/bin/sh), busybox tooling, and apk, so an operator
# can `kubectl exec -it <pod> -- /bin/sh` for interactive debugging AND run the
# binary directly (`kubectl exec <pod> -- replicare status /config.yml`). It
# already carries a CA bundle for TLS to source/target databases. Build with
# version metadata:
#
#   docker build \
#     --build-arg VERSION=$(git describe --tags --always) \
#     --build-arg COMMIT=$(git rev-parse --short HEAD) \
#     --build-arg DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
#     -t replicare:latest .

# ---- build stage ----
FROM golang:1.23-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src

# Cache module downloads across source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w \
      -X github.com/rudimk/replicare/internal/buildinfo.Version=${VERSION} \
      -X github.com/rudimk/replicare/internal/buildinfo.Commit=${COMMIT} \
      -X github.com/rudimk/replicare/internal/buildinfo.Date=${DATE}" \
    -o /out/replicare ./cmd/replicare

# ---- runtime stage ----
FROM cgr.dev/chainguard/wolfi-base:latest
# wolfi-base ships a CA bundle, but copy the build stage's too so TLS works even
# if the base's bundle path ever moves — belt and suspenders, cheap and offline.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/replicare /usr/local/bin/replicare

# Run unprivileged. Use a numeric UID (nobody) so it holds regardless of the
# base image's /etc/passwd contents.
USER 65534:65534

# The status/metrics HTTP surface (configurable; these are the sample defaults).
EXPOSE 8080 9090

ENTRYPOINT ["/usr/local/bin/replicare"]
CMD ["version"]
