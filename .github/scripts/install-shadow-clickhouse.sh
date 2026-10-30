#!/usr/bin/env bash
# Verified official release; no package repository, root access, or installer.
set -euo pipefail
install_dir="${1:?usage: install-clickhouse.sh INSTALL_DIR}"
version=26.9.21.9
case "$(uname -m)" in
  x86_64)
    arch=amd64
    checksum=a1be1c2e0f1932c9e447c0f5a11ab75cdf4ee934772bf4814ca1d04510daa153
    ;;
  aarch64)
    arch=arm64
    checksum=3a5ad02a43766da1d6374f9981db9f874cfc8bab92aaebcac18c80210d6b9aa5
    ;;
  *) echo 'This installer requires Linux x86_64 or aarch64.' >&2; exit 1 ;;
esac
archive="clickhouse-common-static-${version}-${arch}.tgz"
mkdir -p "$install_dir"
curl --fail --location --retry 3 \
  "https://github.com/ClickHouse/ClickHouse/releases/download/v${version}-stable/${archive}" \
  --output "$install_dir/$archive"
echo "$checksum  $install_dir/$archive" | sha256sum --check --strict
tar -xzf "$install_dir/$archive" -C "$install_dir" \
  --strip-components=3 "clickhouse-common-static-${version}/usr/bin/clickhouse"
rm "$install_dir/$archive"
"$install_dir/clickhouse" local --version
