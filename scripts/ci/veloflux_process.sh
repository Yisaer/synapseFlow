#!/usr/bin/env bash

is_numeric_pid() {
  case "${1:-}" in
    '' | *[!0-9]*)
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

pid_is_live() {
  local pid
  pid="${1:-}"
  is_numeric_pid "$pid" || return 1
  kill -0 "$pid" 2>/dev/null
}

veloflux_pid_matches() {
  local pid cmd
  pid="${1:-}"
  pid_is_live "$pid" || return 1
  cmd="$(ps -p "$pid" -o args= 2>/dev/null)" || return 1
  [ -n "$cmd" ] || return 1
  case "$cmd" in
    *target/release/veloflux* | *target-*/release/veloflux* | *"/veloflux --config "*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

wait_for_process_exit() {
  local pid
  pid="${1:-}"
  for _ in $(seq 1 30); do
    if ! pid_is_live "$pid"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_for_port_release() {
  local port listeners
  port="${1:-}"
  for _ in $(seq 1 30); do
    listeners="$(ss -ltnH "( sport = :${port} )" 2>/dev/null)" || {
      sleep 1
      continue
    }
    if [ -z "$listeners" ]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

report_port_release_failure() {
  echo "ports still listening after veloflux shutdown wait" >&2
  ss -ltnp 2>/dev/null >&2 || true
  ps -ef | grep '[v]eloflux' >&2 || true
}

report_pidfile_mismatch() {
  local pidfile pid
  pidfile="${1:-}"
  pid="${2:-}"
  echo "stale or mismatched veloflux pidfile: ${pidfile} (pid=${pid:-<empty>})" >&2
  if pid_is_live "$pid"; then
    ps -p "$pid" -o pid=,ppid=,comm=,args= >&2 || true
  fi
  ps -ef | grep '[v]eloflux' >&2 || true
}

stop_veloflux_from_pidfile() {
  local pidfile pid
  pidfile="${1:-}"
  [ -f "$pidfile" ] || return 0

  pid="$(cat "$pidfile" 2>/dev/null)"
  if ! veloflux_pid_matches "$pid"; then
    report_pidfile_mismatch "$pidfile" "$pid"
    rm -f "$pidfile"
    return 0
  fi

  kill "$pid" 2>/dev/null || true
  if ! wait_for_process_exit "$pid"; then
    kill -9 "$pid" 2>/dev/null || true
    wait_for_process_exit "$pid" || true
  fi
  rm -f "$pidfile"
}
