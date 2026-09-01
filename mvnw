#!/usr/bin/env sh
set -eu
exec docker compose run --rm builder "$@"
