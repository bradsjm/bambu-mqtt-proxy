#!/bin/sh
# Installs the Go toolchain version pinned by go.mod for building this repository.
#
# Downloads the official tarball for the host architecture, verifies its SHA-256
# checksum against go.dev, and installs it under /usr/local/go with go and gofmt
# symlinked into /usr/local/bin. Safe to re-run: an existing install of the
# pinned version is kept as is.
set -eu

cd "$(dirname "$0")/.."

# Pick the exact Go release that go.mod requires.
GO_VERSION=$(awk '$1 == "go" { print $2; exit }' go.mod)
if [ -z "$GO_VERSION" ]; then
	echo "setup-toolchain: cannot find the go directive in go.mod" >&2
	exit 1
fi

# Map the host platform to the Go tarball naming scheme; only Linux is supported.
case "$(uname -s)/$(uname -m)" in
Linux/arm64 | Linux/aarch64) GOARCH=arm64 ;;
Linux/x86_64) GOARCH=amd64 ;;
*)
	echo "setup-toolchain: unsupported platform: $(uname -s) $(uname -m)" >&2
	exit 1
esac

# Keep the existing install when it already matches the pinned version.
if [ -x /usr/local/go/bin/go ] && /usr/local/go/bin/go version | grep -qF "go$GO_VERSION "; then
	echo "setup-toolchain: go$GO_VERSION already installed"
	/usr/local/go/bin/go version
	exit 0
fi

TARBALL="go$GO_VERSION.linux-$GOARCH.tar.gz"
URL="https://dl.google.com/go/$TARBALL"

# Download the tarball and its published checksum into a private scratch dir.
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT
echo "setup-toolchain: downloading $URL"
curl -fsSL -o "$TMPDIR/$TARBALL" "$URL"
curl -fsSL -o "$TMPDIR/$TARBALL.sha256" "$URL.sha256"

# Reject a corrupted or tampered download before it reaches the system.
echo "$(cat "$TMPDIR/$TARBALL.sha256")  $TMPDIR/$TARBALL" | sha256sum -c - >/dev/null

# Elevate only when /usr/local is not directly writable.
if [ "$(id -u)" -eq 0 ]; then
	SUDO=
elif command -v sudo >/dev/null 2>&1; then
	SUDO=sudo
else
	echo "setup-toolchain: need root or sudo to write /usr/local/go" >&2
	exit 1
fi

$SUDO rm -rf /usr/local/go
$SUDO tar -C /usr/local -xzf "$TMPDIR/$TARBALL"
$SUDO ln -sf /usr/local/go/bin/go /usr/local/bin/go
$SUDO ln -sf /usr/local/go/bin/gofmt /usr/local/bin/gofmt

echo "setup-toolchain: installed $("/usr/local/go/bin/go" version)"
