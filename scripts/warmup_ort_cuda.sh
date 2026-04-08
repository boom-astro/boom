#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${ORT_DYLIB_PATH:-}" ]]; then
  echo "ORT_DYLIB_PATH must be set before running the ORT CUDA warmup" >&2
  exit 1
fi

if [[ ! -f "${ORT_DYLIB_PATH}" ]]; then
  echo "ORT_DYLIB_PATH does not point to a file: ${ORT_DYLIB_PATH}" >&2
  exit 1
fi

export RUST_LOG="${RUST_LOG:-info,ort=warn}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
app_root="$(cd "${script_dir}/.." && pwd)"

if [[ -n "${LD_LIBRARY_PATH:-}" ]]; then
  export LD_LIBRARY_PATH="$(dirname "${ORT_DYLIB_PATH}"):${LD_LIBRARY_PATH}"
else
  export LD_LIBRARY_PATH="$(dirname "${ORT_DYLIB_PATH}")"
fi

exec "${app_root}/target/release/warmup_ort_cuda"