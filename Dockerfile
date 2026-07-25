# replicare — multi-stage build producing a tiny static image.
#
# The binary is pure-Go (pgx has no cgo), so it links statically and runs from
# `scratch` with nothing but CA certificates (needed for TLS to source/target
# databases). Build with version metadata:
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
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/replicare /usr/local/bin/replicare

# Run unprivileged (nobody). scratch has no /etc/passwd, so use a numeric UID.
USER 65534:65534

# The status/metrics HTTP surface (configurable; these are the sample defaults).
EXPOSE 8080 9090

ENTRYPOINT ["/usr/local/bin/replicare"]
CMD ["version"]
