FROM ubuntu:24.04 AS qnx-sdp

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG QNX_BASELINE=com.qnx.qnx800
ARG QNX_MIRROR_BASELINE=qnx800
ARG QNX_INSTALLER_SHA256=5695a0ec8a1fd91840753b2cba86a9325f6052d4f69c757166cc61b09bcbe507
ARG QNX_INSTALLER_ASSET=qnx-setup-2.0.4-202501021438-linux.run
ARG QNX_INSTALLER_RELEASE=qnx-sdp-8-installer
ARG QNX_INSTALLER_REPO=Yisaer/qnx-private-assets

ENV DEBIAN_FRONTEND=noninteractive \
    QNX_INSTALL_DIR=/opt/qnx800 \
    QNX_QSC_DIR=/opt/qnxsoftwarecenter

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      file \
      git \
      jq \
      unzip \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://github.com/cli/cli/releases/download/v2.76.2/gh_2.76.2_linux_amd64.tar.gz \
      | tar -xz -C /tmp \
    && mv /tmp/gh_2.76.2_linux_amd64/bin/gh /usr/local/bin/gh \
    && rm -rf /tmp/gh_2.76.2_linux_amd64

RUN --mount=type=secret,id=qnx_installer_token \
    set -eux; \
    export GH_TOKEN="$(cat /run/secrets/qnx_installer_token)"; \
    installer="/tmp/qnx-setup-linux.run"; \
    gh release download "$QNX_INSTALLER_RELEASE" \
      --repo "$QNX_INSTALLER_REPO" \
      --pattern "$QNX_INSTALLER_ASSET" \
      --dir /tmp; \
    mv "/tmp/$QNX_INSTALLER_ASSET" "$installer"; \
    echo "$QNX_INSTALLER_SHA256  $installer" | sha256sum --check; \
    if file "$installer" | grep -qiE 'html|text'; then \
      echo "Downloaded QNX Software Center installer does not look executable." >&2; \
      file "$installer" >&2; \
      exit 1; \
    fi; \
    chmod +x "$installer"; \
    "$installer" force-override disable-auto-start agree-to-license-terms "$QNX_QSC_DIR"; \
    rm -f "$installer"

RUN --mount=type=secret,id=qnx_license_key \
    --mount=type=secret,id=qnx_myqnx_user \
    --mount=type=secret,id=qnx_myqnx_password \
    set -eux; \
    qsc_bin="$QNX_QSC_DIR/qnxsoftwarecenter/qnxsoftwarecenter_clt"; \
    if [ ! -x "$qsc_bin" ]; then \
      qsc_bin="$(find "$QNX_QSC_DIR" -type f -name qnxsoftwarecenter_clt -perm -111 | head -n 1)"; \
    fi; \
    test -x "$qsc_bin"; \
    qnx_license_key="$(cat /run/secrets/qnx_license_key)"; \
    qnx_myqnx_user="$(cat /run/secrets/qnx_myqnx_user)"; \
    qnx_myqnx_password="$(cat /run/secrets/qnx_myqnx_password)"; \
    "$qsc_bin" -addLicenseKey "$qnx_license_key" -listLicenseKeys; \
    "$qsc_bin" \
      -setDebugSymbolsEnabled=false \
      -mirrorBaseline "$QNX_MIRROR_BASELINE" \
      -myqnx.user "$qnx_myqnx_user" \
      -myqnx.password "$qnx_myqnx_password"; \
    echo "=== Searching for available QNX Rust packages ==="; \
    RUST_PKGS="$("$qsc_bin" \
      -listAvailable \
      -myqnx.user "$qnx_myqnx_user" \
      -myqnx.password "$qnx_myqnx_password" \
      2>/dev/null | grep -i 'rust' | awk '{print $1}' || true)"; \
    echo "Rust packages discovered: ${RUST_PKGS:-none}"; \
    INSTALL_ARGS=( \
      -setDebugSymbolsEnabled=false \
      -cleanInstall \
      -destination "$QNX_INSTALL_DIR" \
      -installBaseline "$QNX_BASELINE" \
      -listLicenseKeys \
      -myqnx.user "$qnx_myqnx_user" \
      -myqnx.password "$qnx_myqnx_password" \
    ); \
    for pkg in $RUST_PKGS; do \
      echo "Adding Rust add-on package: $pkg"; \
      INSTALL_ARGS+=(-installAddOn "$pkg"); \
    done; \
    for attempt in 1 2 3; do \
      "$qsc_bin" "${INSTALL_ARGS[@]}" && break; \
      if [ "$attempt" = 3 ]; then \
        exit 1; \
      fi; \
      sleep $((attempt * 30)); \
    done; \
    if [ -z "${RUST_PKGS:-}" ]; then \
      echo "No Rust packages discovered via -listAvailable; trying heuristics."; \
      for candidate in \
        com.qnx.qnx800.host.linux.x86_64.rust \
        com.qnx.qnx800.target.rust.host \
        com.qnx.qnx800.target.rust \
      ; do \
        echo "Trying add-on: $candidate"; \
        "$qsc_bin" \
          -installAddOn "$candidate" \
          -destination "$QNX_INSTALL_DIR" \
          -myqnx.user "$qnx_myqnx_user" \
          -myqnx.password "$qnx_myqnx_password" || true; \
      done; \
    fi; \
    rm -rf /root/.qnx/license /root/.qnx/swupdate/session /root/.qnx/swupdate/server-cache

FROM ubuntu:24.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND=noninteractive \
    QNX_INSTALL_DIR=/opt/qnx800 \
    QNX_QSC_DIR=/opt/qnxsoftwarecenter \
    QNX_RUST_TOOLCHAIN=qnx \
    CARGO_TERM_COLOR=always \
    CARGO_BUILD_JOBS=2 \
    CARGO_HOME=/root/.cargo \
    CARGO_INCREMENTAL=0

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      curl \
      file \
      git \
      m4 \
      pkg-config \
      protobuf-compiler \
      unzip \
    && rm -rf /var/lib/apt/lists/*

COPY --from=qnx-sdp /opt/qnx800 /opt/qnx800
COPY --from=qnx-sdp /opt/qnxsoftwarecenter /opt/qnxsoftwarecenter

RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable

ENV PATH="/root/.cargo/bin:${PATH}"

RUN set -eux; \
    . "$QNX_INSTALL_DIR/qnxsdp-env.sh"; \
    rust_root=""; \
    while IFS= read -r rustc_path; do \
      candidate="$(dirname "$(dirname "$rustc_path")")"; \
      if [ -x "$candidate/bin/rustc" ] && [ -d "$candidate/lib/rustlib" ]; then \
        rust_root="$candidate"; \
        break; \
      fi; \
    done < <(find "$QNX_HOST" "$QNX_INSTALL_DIR" -type f -name rustc -perm -111 | sort); \
    if [ -z "$rust_root" ]; then \
      echo "QNX Rust toolchain was not found." >&2; \
      find "$QNX_HOST" "$QNX_INSTALL_DIR" -type f \( -name rustc -o -name cargo \) | sort >&2; \
      exit 1; \
    fi; \
    rustup toolchain link "$QNX_RUST_TOOLCHAIN" "$rust_root"; \
    cargo +"$QNX_RUST_TOOLCHAIN" -V; \
    rustc +"$QNX_RUST_TOOLCHAIN" -Vv

WORKDIR /work
