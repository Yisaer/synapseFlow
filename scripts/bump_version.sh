#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <version>" >&2
  exit 1
fi

VERSION="$1"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CRATES=(
  "$ROOT/Cargo.toml"
  "$ROOT/src/flow/Cargo.toml"
  "$ROOT/src/datatypes/Cargo.toml"
  "$ROOT/src/parser/Cargo.toml"
  "$ROOT/src/manager/Cargo.toml"
  "$ROOT/src/telemetry/Cargo.toml"
  "$ROOT/src/storage/Cargo.toml"
)

for file in "${CRATES[@]}"; do
  if [[ ! -f "$file" ]]; then
    echo "Skip missing $file"
    continue
  fi
  # Replace the first occurrence of version = "...".
  perl -pi -e "s/^version = \".*\"/version = \"$VERSION\"/ if $. <= 10" "$file"
done

echo "Bumped versions to $VERSION. Remember to update Cargo.lock:"
echo "  cargo update -p veloFlux -p flow -p parser -p datatypes -p manager -p storage -p telemetry"
