#!/usr/bin/env bash
#
# install-build-deps.sh — install everything needed to build Valkey (C) and the
# bigobj module (Rust) on a fresh machine.
#
#   Valkey:  a C toolchain (gcc/make), plus optional openssl-devel (TLS) and
#            gtest (unit tests).
#   bigobj:  a current Rust toolchain via rustup.
#
# Usage:  ./install-build-deps.sh          (installs C toolchain + Rust)
#         ./install-build-deps.sh --tls    (also install openssl dev headers)

set -euo pipefail
log() { echo "[deps] $*"; }
die() { echo "[deps] ERROR: $*" >&2; exit 1; }

WANT_TLS=0
[ "${1:-}" = "--tls" ] && WANT_TLS=1

SUDO=""
[ "$(id -u)" -ne 0 ] && command -v sudo >/dev/null && SUDO="sudo"

# ── C toolchain (package-manager-aware) ───────────────────────────────────────
# clang/libclang is required at build time by the valkey-module crate (it uses
# bindgen to generate FFI bindings against Valkey's C headers).
if command -v dnf >/dev/null 2>&1; then
    log "installing C build tools via dnf..."
    $SUDO dnf groupinstall -y "Development Tools"
    $SUDO dnf install -y gcc make pkgconfig gtest-devel clang clang-libs
    [ "$WANT_TLS" = "1" ] && $SUDO dnf install -y openssl-devel
elif command -v apt-get >/dev/null 2>&1; then
    log "installing C build tools via apt-get..."
    $SUDO apt-get update
    $SUDO apt-get install -y build-essential pkg-config libgtest-dev clang libclang-dev curl
    [ "$WANT_TLS" = "1" ] && $SUDO apt-get install -y libssl-dev
elif command -v yum >/dev/null 2>&1; then
    log "installing C build tools via yum..."
    $SUDO yum groupinstall -y "Development Tools"
    $SUDO yum install -y gcc make pkgconfig gtest-devel clang clang-libs
    [ "$WANT_TLS" = "1" ] && $SUDO yum install -y openssl-devel
else
    die "unsupported package manager — install a C toolchain (gcc, make, clang) manually"
fi

# ── Rust toolchain (rustup — current stable; distro rust is often too old) ─────
if command -v cargo >/dev/null 2>&1; then
    log "cargo already present: $(cargo --version)"
else
    log "installing Rust via rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    # shellcheck disable=SC1091
    source "$HOME/.cargo/env"
fi

log "done. Toolchain versions:"
command -v gcc   >/dev/null && gcc --version | head -1
command -v make  >/dev/null && make --version | head -1
command -v cargo >/dev/null && cargo --version
echo
log "Rust env: run 'source \$HOME/.cargo/env' (or open a new shell) before 'cargo build'."
