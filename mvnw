#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec docker compose --project-directory "$SCRIPT_DIR" -f "$SCRIPT_DIR/compose.yaml" run --rm builder mvn "$@"
