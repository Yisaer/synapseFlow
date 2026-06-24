ARG QNX_SDP_IMAGE=ghcr.io/emqx/veloflux-qnx-sdp:8.0-try
FROM ${QNX_SDP_IMAGE}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      binutils \
      bridge-utils \
      iproute2 \
      iputils-ping \
      netcat-openbsd \
      net-tools \
      openssh-client \
      procps \
      qemu-system-arm \
      qemu-utils \
      rsync \
    && mkdir -p /etc/qemu \
    && printf 'allow all\n' > /etc/qemu/bridge.conf \
    && if [ -e /usr/lib/qemu/qemu-bridge-helper ]; then chmod u+s /usr/lib/qemu/qemu-bridge-helper; fi \
    && rm -rf /var/lib/apt/lists/*

COPY scripts/qnx/start_qnx_vm.sh /usr/local/bin/start_qnx_vm.sh
RUN chmod +x /usr/local/bin/start_qnx_vm.sh
