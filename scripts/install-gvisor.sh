#!/usr/bin/env bash
# Installs a pinned gVisor runsc and registers it as the Docker "runsc"
# runtime, for the Execution Sandbox's gVisor runner and its integration
# tests. Requires root; restarts dockerd.
set -euo pipefail

VERSION="${GVISOR_VERSION:-20250106}"
ARCH="$(uname -m)"
URL="https://storage.googleapis.com/gvisor/releases/release/${VERSION}/${ARCH}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

curl -fsSL -o "$TMP/runsc" "$URL/runsc"
curl -fsSL -o "$TMP/runsc.sha512" "$URL/runsc.sha512"
(cd "$TMP" && sha512sum -c runsc.sha512)
install -m 0755 "$TMP/runsc" /usr/local/bin/runsc

# Merge the runtime into daemon.json rather than overwriting it.
CONFIG=/etc/docker/daemon.json
mkdir -p "$(dirname "$CONFIG")"
[ -s "$CONFIG" ] || echo '{}' > "$CONFIG"
python3 - "$CONFIG" <<'PY'
import json, sys
path = sys.argv[1]
config = json.load(open(path))
config.setdefault("runtimes", {})["runsc"] = {"path": "/usr/local/bin/runsc"}
json.dump(config, open(path, "w"), indent=2)
PY

if command -v systemctl >/dev/null && systemctl is-active --quiet docker; then
  systemctl restart docker
else
  echo "restart dockerd to pick up the runsc runtime"
fi
runsc --version
