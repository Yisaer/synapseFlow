#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Convert jemalloc heap dump (heap_v2) to flamegraph SVG.

Prereqs:
  - jeprof (from libjemalloc-dev)
  - flamegraph.pl (from FlameGraph)
  - the Linux binary used to generate the heap profile (ideally not stripped)

Usage:
  scripts/heap_to_svg.sh --binary /path/to/synapse-flow --heap /path/to/heap.1 --out heap.svg

Options:
  --metric bytes|objects   Default: bytes
EOF
}

binary_path=""
heap_path=""
out_path=""
metric="bytes"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary)
      binary_path="${2:-}"
      shift 2
      ;;
    --heap)
      heap_path="${2:-}"
      shift 2
      ;;
    --out)
      out_path="${2:-}"
      shift 2
      ;;
    --metric)
      metric="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown arg: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${binary_path}" || -z "${heap_path}" || -z "${out_path}" ]]; then
  echo "error: --binary, --heap and --out are required" >&2
  usage >&2
  exit 2
fi

if ! command -v jeprof >/dev/null 2>&1; then
  echo "error: jeprof not found; run scripts/setup_heap_tools_ubuntu.sh first" >&2
  exit 1
fi
if ! command -v flamegraph.pl >/dev/null 2>&1; then
  echo "error: flamegraph.pl not found; run scripts/setup_heap_tools_ubuntu.sh first" >&2
  exit 1
fi

if [[ ! -f "${binary_path}" ]]; then
  echo "error: binary not found: ${binary_path}" >&2
  exit 1
fi
if [[ ! -f "${heap_path}" ]]; then
  echo "error: heap not found: ${heap_path}" >&2
  exit 1
fi

jeprof_metric_flag="--inuse_space"
case "${metric}" in
  bytes)
    jeprof_metric_flag="--inuse_space"
    ;;
  objects)
    jeprof_metric_flag="--inuse_objects"
    ;;
  *)
    echo "error: invalid --metric: ${metric} (expected bytes|objects)" >&2
    exit 2
    ;;
esac

tmp_folded="$(mktemp -t heap.folded.XXXXXX)"
cleanup() {
  rm -f "${tmp_folded}"
}
trap cleanup EXIT

echo "[heap_to_svg] generating folded stacks (${metric})"
jeprof --collapsed "${jeprof_metric_flag}" "${binary_path}" "${heap_path}" >"${tmp_folded}"

echo "[heap_to_svg] generating svg: ${out_path}"
flamegraph.pl "${tmp_folded}" >"${out_path}"

echo "[heap_to_svg] done"
