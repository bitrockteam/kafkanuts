#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose() {
  docker compose --project-directory "$ROOT" -f "$ROOT/compose.yaml" "$@"
}

command_name=${1:-doctor}
case "$command_name" in
  doctor)
    docker version
    compose config >/dev/null
    echo "doctor: OK"
    ;;
  config)
    compose config
    ;;
  up)
    compose --profile bootstrap up --build --abort-on-container-exit
    ;;
  down)
    compose down
    ;;
  status)
    compose ps --all
    ;;
  reset)
    compose down --volumes --remove-orphans
    ;;
  *)
    echo "usage: $0 {doctor|config|up|down|status|reset}" >&2
    exit 2
    ;;
esac
