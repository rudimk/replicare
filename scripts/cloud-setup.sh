#!/usr/bin/env bash
#
# Cloud sandbox setup for Claude Code on the web (claude.ai/code).
#
# Paste the CONTENTS of this file into the cloud environment's "Setup script"
# field (cloud icon -> environment settings). It runs ONCE as root when the
# environment is created and is reused via snapshot, so it must be
# self-contained and NOT depend on the repo being present yet.
#
# It provisions everything a fresh sandbox lacks:
#   1. Go 1.23.x toolchain (the go.mod floor)   — only if Go is missing
#   2. go-task (the Taskfile is the entrypoint for build/test/lint)
#   3. golangci-lint v2.12.2 (tag-pinned; master's installer checksums are stale)
#   4. Oh-My-Claude-Sisyphus into ~/.claude (user-level config does NOT carry
#      over from a laptop, so we reinstall it here)
#   5. Warms the Postgres harness images so `task test:integration` is fast
#
# Idempotent: safe to re-run. Trusted egress covers go.dev, the module proxy,
# Docker Hub, and raw.githubusercontent.com.
set -euo pipefail

GO_VERSION=1.23.4
GOLANGCI_VERSION=v2.12.2

log() { printf '\n=== %s ===\n' "$1"; }

arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo amd64 ;;
    aarch64|arm64) echo arm64 ;;
    *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
  esac
}

log "Go toolchain"
if command -v go >/dev/null 2>&1; then
  echo "go already present: $(go version)"
  echo "go.mod's toolchain directive will fetch ${GO_VERSION} on demand if newer is needed"
else
  tarball="go${GO_VERSION}.linux.$(arch).tar.gz"
  curl -fsSL "https://go.dev/dl/${tarball}" -o "/tmp/${tarball}"
  rm -rf /usr/local/go
  tar -C /usr/local -xzf "/tmp/${tarball}"
  echo 'export PATH=$PATH:/usr/local/go/bin:$(go env GOPATH)/bin' >> "$HOME/.bashrc"
  export PATH="$PATH:/usr/local/go/bin"
  echo "installed $(go version)"
fi
export PATH="$PATH:$(go env GOPATH)/bin"

log "go-task"
if ! command -v task >/dev/null 2>&1; then
  sh -c "$(curl -fsSL https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin
fi
task --version

log "golangci-lint ${GOLANGCI_VERSION}"
if ! command -v golangci-lint >/dev/null 2>&1; then
  curl -fsSL "https://raw.githubusercontent.com/golangci/golangci-lint/${GOLANGCI_VERSION}/install.sh" \
    | sh -s -- -b "$(go env GOPATH)/bin" "${GOLANGCI_VERSION}"
fi
golangci-lint version

log "Oh-My-Claude-Sisyphus (into ~/.claude)"
curl -fsSL https://raw.githubusercontent.com/Yeachan-Heo/oh-my-claude-sisyphus/main/scripts/install.sh | bash

log "Warm the Postgres test-harness images"
# Repo-independent so this works at env-create time before the clone.
docker pull --platform linux/amd64 postgres:9.6 || true
docker pull postgres:17 || true

log "Done — toolchain + Sisyphus provisioned"
