#!/usr/bin/env bash
set -euo pipefail

cat >&2 <<'MESSAGE'
The legacy Java client launcher is retired; interactive clients are unsupported.
Use the supported non-interactive Go client instead:
  make go-build
  ./golang/kvcli/kv
MESSAGE

exit 1
