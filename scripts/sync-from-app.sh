#!/usr/bin/env bash
# One-way copy while the app holds the source of truth. Usage:
#   scripts/sync-from-app.sh <path to the app repo>
# Copies library sources from its functions/src/durable and lists the app's test
# files that changed since the commit in SYNCED_FROM, to be ported by hand.
set -euo pipefail
app="${1:?usage: scripts/sync-from-app.sh <path to the app repo>}"
here="$(cd "$(dirname "$0")/.." && pwd)"
cp "$app"/functions/src/durable/*.ts "$here/src/"
new="$(git -C "$app" rev-parse HEAD)"
old="$(cat "$here/SYNCED_FROM")"
echo "Test changes to port (${old:0:9}..${new:0:9}):"
git -C "$app" diff --stat "$old" "$new" -- 'functions/src/__tests__/durable_*' || true
echo "$new" > "$here/SYNCED_FROM"
git -C "$here" status --short
