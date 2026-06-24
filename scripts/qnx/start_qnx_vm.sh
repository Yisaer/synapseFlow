#!/usr/bin/env bash
set -euo pipefail

log_dir="tmp/qnx-qemu"
work_dir="tmp/qnx-qemu-work"
timeout_secs=300
hold_secs=10
hostname="veloflux-qnx"
arch="aarch64le"
host_ssh_port=2222
host_qconn_port=8000
qnx_cpus=2
qnx_ram="1024M"
qnx_mirror_baseline="${QNX_MIRROR_BASELINE:-qnx800}"
qemuvirt_package="com.qnx.qnx800.target.qemuvirt"
virtio_driver_package="com.qnx.qnx800.target.driver.virtio"
run_pid=""
key_dir=""
ssh_key=""
delete_key_dir=0
skip_build=0

usage() {
  cat <<'USAGE'
Usage: start_qnx_vm.sh [options]

Options:
  --log-dir PATH        Directory for startup and console logs.
  --work-dir PATH       Working directory used by mkqnximage.
  --timeout-secs SECS   Maximum time to wait for the forwarded SSH port.
  --hold-secs SECS      Time to keep the VM running after SSH is reachable.
  --hostname NAME       QNX VM hostname.
  --arch ARCH           QNX target architecture.
  --host-ssh-port PORT  Host TCP port forwarded to QNX port 22.
  --host-qconn-port PORT Host TCP port forwarded to QNX port 8000.
  --cpus COUNT          QEMU vCPU count.
  --ram SIZE            QEMU RAM size.
  --ssh-key PATH        SSH private key used for QNX root login.
  --skip-build          Start an existing VM image without QNX SDP setup.
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
    --host-ssh-port)
      host_ssh_port="$2"
      shift 2
      ;;
    --host-qconn-port)
      host_qconn_port="$2"
      shift 2
      ;;
    --cpus)
      qnx_cpus="$2"
      shift 2
      ;;
    --ram)
      qnx_ram="$2"
      shift 2
      ;;
    --ssh-key)
      ssh_key="$2"
      shift 2
      ;;
    --skip-build)
      skip_build=1
      shift
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
qemu_pidfile="$log_dir/qemu.pid"
if [ -z "$ssh_key" ]; then
  key_dir="$(mktemp -d)"
  ssh_key="$key_dir/qnx_vm_ed25519"
  delete_key_dir=1
else
  mkdir -p "$(dirname "$ssh_key")"
  ssh_key="$(cd "$(dirname "$ssh_key")" && pwd -P)/$(basename "$ssh_key")"
fi

exec > >(tee -a "$startup_log") 2>&1

cleanup() {
  local status=$?

  set +e
  if [ -n "${QNX_LICENSE_KEY:-}" ] && [ -f "$startup_log" ]; then
    local sanitized_log
    sanitized_log="$(mktemp)"
    while IFS= read -r line; do
      printf '%s\n' "${line//${QNX_LICENSE_KEY}/[REDACTED]}"
    done <"$startup_log" >"$sanitized_log"
    mv "$sanitized_log" "$startup_log"
    chmod 0644 "$startup_log"
  fi
  if [ -n "$run_pid" ] && kill -0 "$run_pid" >/dev/null 2>&1; then
    kill "$run_pid" >/dev/null 2>&1
    wait "$run_pid" >/dev/null 2>&1
  fi
  if [ "$delete_key_dir" -eq 1 ] && [ -n "$key_dir" ]; then
    rm -rf "$key_dir"
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
echo "host_ssh_port=$host_ssh_port"
echo "host_qconn_port=$host_qconn_port"
echo "qnx_cpus=$qnx_cpus"
echo "qnx_ram=$qnx_ram"
echo "qnx_mirror_baseline=$qnx_mirror_baseline"
echo "skip_build=$skip_build"
echo "ssh_key=$ssh_key"

if [ "$skip_build" -eq 0 ]; then
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

  if ! find "${QNX_TARGET:-${QNX_INSTALL_DIR:-/opt/qnx800}/target/qnx}" -type f -path '*/sbin/devb-virtio' | grep -q .; then
    echo "devb-virtio was not found; installing ${virtio_driver_package}."

    if [ -z "${QNX_MYQNX_USER:-}" ] || [ -z "${QNX_MYQNX_PASSWORD:-}" ]; then
      echo "QNX_MYQNX_USER and QNX_MYQNX_PASSWORD are required to install ${virtio_driver_package}." >&2
      exit 1
    fi

    "$qsc_bin" \
      -setDebugSymbolsEnabled=false \
      -destination "${QNX_INSTALL_DIR:-/opt/qnx800}" \
      -installPackage "$virtio_driver_package" \
      -listLicenseKeys \
      -myqnx.user "$QNX_MYQNX_USER" \
      -myqnx.password "$QNX_MYQNX_PASSWORD"
  fi

  find "${QNX_TARGET:-${QNX_INSTALL_DIR:-/opt/qnx800}/target/qnx}" -type f -path '*/sbin/devb-virtio' -print -quit

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

  qemu-system-aarch64 --version | head -n 1
  mkqnximage --help | sed -n '1,80p'

  cd "$work_dir"

  if [ ! -f "$ssh_key" ]; then
    ssh-keygen -q -t ed25519 -N '' -f "$ssh_key"
  fi

  echo "Building QNX QEMU image..."
  mkqnximage \
    --type=qemu \
    --arch="$arch" \
    --hostname="$hostname" \
    --ssh-ident="${ssh_key}.pub" \
    --sshd-pregen=yes \
    --force \
    --build
else
  if [ ! -f "$ssh_key" ]; then
    echo "QNX SSH key was not found for the existing VM image: $ssh_key" >&2
    exit 1
  fi
  qemu-system-aarch64 --version | head -n 1
  cd "$work_dir"
fi

ifs_image="$work_dir/output/ifs.bin"
disk_image="$work_dir/output/disk-qemu"
if [ ! -f "$ifs_image" ]; then
  echo "QNX IFS image was not generated: $ifs_image" >&2
  exit 1
fi
if [ ! -f "$disk_image" ]; then
  echo "QNX disk image was not generated: $disk_image" >&2
  exit 1
fi

echo "Starting QNX QEMU VM with user networking..."
qemu-system-aarch64 \
  -machine virt \
  -cpu cortex-a57 \
  -smp "$qnx_cpus" \
  -m "$qnx_ram" \
  -drive "file=${disk_image},format=raw,if=none,id=drv0" \
  -device "virtio-blk-device,drive=drv0" \
  -netdev "user,id=net0,hostfwd=tcp:127.0.0.1:${host_ssh_port}-:22,hostfwd=tcp:127.0.0.1:${host_qconn_port}-:8000" \
  -device "virtio-net-device,netdev=net0,mac=52:54:00:0f:f0:9d" \
  -object rng-random,filename=/dev/urandom,id=rng0 \
  -device "virtio-rng-device,rng=rng0" \
  -pidfile "$qemu_pidfile" \
  -kernel "$ifs_image" \
  -nographic \
  >"$console_log" 2>&1 &
run_pid=$!
echo "qemu-system-aarch64 pid=$run_pid"

ssh_args=(
  -i "$ssh_key"
  -p "$host_ssh_port"
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o ConnectTimeout=5
  -o ConnectionAttempts=1
)

deadline=$((SECONDS + timeout_secs))
while [ "$SECONDS" -lt "$deadline" ]; do
  if ! kill -0 "$run_pid" >/dev/null 2>&1; then
    echo "QEMU exited before SSH became reachable." >&2
    echo "Last console log lines:" >&2
    tail -n 200 "$console_log" >&2 || true
    exit 1
  fi

  if ssh "${ssh_args[@]}" root@127.0.0.1 uname -a >/dev/null 2>&1; then
    echo "QNX VM SSH is reachable on 127.0.0.1:${host_ssh_port}"
    {
      printf 'QNX_VM_HOST=127.0.0.1\n'
      printf 'QNX_VM_SSH_PORT=%s\n' "$host_ssh_port"
      printf 'QNX_VM_QCONN_PORT=%s\n' "$host_qconn_port"
      printf 'QNX_VM_SSH_KEY=%s\n' "$ssh_key"
    } >"$log_dir/qnx-vm.env"
    break
  fi

  echo "Waiting for QNX VM SSH..."
  sleep 5
done

if [ ! -f "$log_dir/qnx-vm.env" ]; then
  echo "QNX VM SSH port was not reachable within ${timeout_secs}s." >&2
  echo "Last console log lines:" >&2
  tail -n 200 "$console_log" >&2 || true
  exit 1
fi

echo "Keeping QNX VM alive for ${hold_secs}s..."
sleep "$hold_secs"
echo "QNX VM startup check completed."
