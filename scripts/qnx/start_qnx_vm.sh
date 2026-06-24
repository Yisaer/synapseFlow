#!/usr/bin/env bash
set -euo pipefail

log_dir="tmp/qnx-qemu"
work_dir="tmp/qnx-qemu-work"
timeout_secs=300
hold_secs=10
hostname="veloflux-qnx"
arch="aarch64le"
qnx_mirror_baseline="${QNX_MIRROR_BASELINE:-qnx800}"
qemuvirt_package="com.qnx.qnx800.target.qemuvirt"
vm_ip=""
run_pid=""

usage() {
  cat <<'USAGE'
Usage: start_qnx_vm.sh [options]

Options:
  --log-dir PATH        Directory for startup and console logs.
  --work-dir PATH       Working directory used by mkqnximage.
  --timeout-secs SECS   Maximum time to wait for mkqnximage --getip.
  --hold-secs SECS      Time to keep the VM running after the IP is found.
  --hostname NAME       QNX VM hostname.
  --arch ARCH           QNX target architecture.
  -h, --help            Show this help.
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --log-dir)
      log_dir="$2"
      shift 2
      ;;
    --work-dir)
      work_dir="$2"
      shift 2
      ;;
    --timeout-secs)
      timeout_secs="$2"
      shift 2
      ;;
    --hold-secs)
      hold_secs="$2"
      shift 2
      ;;
    --hostname)
      hostname="$2"
      shift 2
      ;;
    --arch)
      arch="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

mkdir -p "$log_dir" "$work_dir"
log_dir="$(cd "$log_dir" && pwd -P)"
work_dir="$(cd "$work_dir" && pwd -P)"
startup_log="$log_dir/start_qnx_vm.log"
console_log="$log_dir/qnx_console.log"
getip_log="$log_dir/mkqnximage_getip.log"

exec > >(tee -a "$startup_log") 2>&1

cleanup() {
  local status=$?

  set +e
  if command -v mkqnximage >/dev/null 2>&1; then
    mkqnximage --stop >>"$startup_log" 2>&1
  fi
  if [ -n "$run_pid" ] && kill -0 "$run_pid" >/dev/null 2>&1; then
    kill "$run_pid" >/dev/null 2>&1
    wait "$run_pid" >/dev/null 2>&1
  fi

  exit "$status"
}
trap cleanup EXIT

echo "QNX QEMU startup"
echo "log_dir=$log_dir"
echo "work_dir=$work_dir"
echo "timeout_secs=$timeout_secs"
echo "hold_secs=$hold_secs"
echo "hostname=$hostname"
echo "arch=$arch"
echo "qnx_mirror_baseline=$qnx_mirror_baseline"

if [ -f "${QNX_INSTALL_DIR:-/opt/qnx800}/qnxsdp-env.sh" ]; then
  # shellcheck disable=SC1091
  source "${QNX_INSTALL_DIR:-/opt/qnx800}/qnxsdp-env.sh"
else
  echo "QNX SDP env file was not found under ${QNX_INSTALL_DIR:-/opt/qnx800}" >&2
  exit 1
fi

if [ -z "${QNX_LICENSE_KEY:-}" ]; then
  echo "QNX_LICENSE_KEY is required to activate the QNX runtime license." >&2
  exit 1
fi

qsc_bin="${QNX_QSC_DIR:-/opt/qnxsoftwarecenter}/qnxsoftwarecenter/qnxsoftwarecenter_clt"
if [ ! -x "$qsc_bin" ]; then
  qsc_bin="$(find "${QNX_QSC_DIR:-/opt/qnxsoftwarecenter}" -type f -name qnxsoftwarecenter_clt -perm -111 | head -n 1)"
fi
if [ -z "$qsc_bin" ] || [ ! -x "$qsc_bin" ]; then
  echo "qnxsoftwarecenter_clt was not found under ${QNX_QSC_DIR:-/opt/qnxsoftwarecenter}" >&2
  exit 1
fi

"$qsc_bin" -addLicenseKey "$QNX_LICENSE_KEY" -listLicenseKeys

if ! find "${QNX_TARGET:-${QNX_INSTALL_DIR:-/opt/qnx800}/target/qnx}" -type f -name startup-qemu-virt | grep -q .; then
  echo "startup-qemu-virt was not found; installing ${qemuvirt_package}."

  if [ -z "${QNX_MYQNX_USER:-}" ] || [ -z "${QNX_MYQNX_PASSWORD:-}" ]; then
    echo "QNX_MYQNX_USER and QNX_MYQNX_PASSWORD are required to install ${qemuvirt_package}." >&2
    exit 1
  fi

  "$qsc_bin" \
    -setDebugSymbolsEnabled=false \
    -mirrorBaseline "$qnx_mirror_baseline" \
    -myqnx.user "$QNX_MYQNX_USER" \
    -myqnx.password "$QNX_MYQNX_PASSWORD"

  "$qsc_bin" \
    -setDebugSymbolsEnabled=false \
    -destination "${QNX_INSTALL_DIR:-/opt/qnx800}" \
    -installPackage "$qemuvirt_package" \
    -listLicenseKeys \
    -myqnx.user "$QNX_MYQNX_USER" \
    -myqnx.password "$QNX_MYQNX_PASSWORD"
fi

find "${QNX_TARGET:-${QNX_INSTALL_DIR:-/opt/qnx800}/target/qnx}" -type f -name startup-qemu-virt -print -quit

if ! command -v mkqnximage >/dev/null 2>&1; then
  mkqnximage_bin="$(find "${QNX_INSTALL_DIR:-/opt/qnx800}" -type f -name mkqnximage -perm -111 | head -n 1)"
  if [ -n "$mkqnximage_bin" ]; then
    export PATH="$(dirname "$mkqnximage_bin"):$PATH"
  fi
fi
if ! command -v mkqnximage >/dev/null 2>&1; then
  echo "mkqnximage was not found after sourcing the QNX SDP environment." >&2
  exit 1
fi

if [ -e /usr/lib/qemu/qemu-bridge-helper ]; then
  mkdir -p /etc/qemu
  printf 'allow all\n' >/etc/qemu/bridge.conf
  chmod u+s /usr/lib/qemu/qemu-bridge-helper
fi

qemu-system-aarch64 --version | head -n 1
mkqnximage --help | sed -n '1,80p'

cd "$work_dir"

echo "Building QNX QEMU image..."
mkqnximage \
  --type=qemu \
  --arch="$arch" \
  --hostname="$hostname" \
  --build

echo "Starting QNX QEMU VM..."
mkqnximage --run >"$console_log" 2>&1 &
run_pid=$!
echo "mkqnximage --run pid=$run_pid"

deadline=$((SECONDS + timeout_secs))
while [ "$SECONDS" -lt "$deadline" ]; do
  raw_getip="$(mkqnximage --getip 2>>"$getip_log" || true)"
  if [ -n "$raw_getip" ]; then
    echo "$raw_getip" >>"$getip_log"
    vm_ip="$(
      printf '%s\n' "$raw_getip" \
        | awk '{ for (i = 1; i <= NF; i++) if ($i ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/) { print $i; exit } }'
    )"
  fi

  if [ -n "$vm_ip" ]; then
    echo "QNX VM IP: $vm_ip"
    printf 'QNX_VM_IP=%s\n' "$vm_ip" >"$log_dir/qnx-vm.env"
    break
  fi

  echo "Waiting for QNX VM IP..."
  sleep 10
done

if [ -z "$vm_ip" ]; then
  echo "QNX VM did not report an IP within ${timeout_secs}s." >&2
  echo "Last console log lines:" >&2
  tail -n 200 "$console_log" >&2 || true
  exit 1
fi

echo "Keeping QNX VM alive for ${hold_secs}s..."
sleep "$hold_secs"
echo "QNX VM startup check completed."
