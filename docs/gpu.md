# GPU Acceleration

This page covers practical GPU setup for BOOM and is aligned with the current Dockerfile and compose files.

**If you want to use GPU hardware acceleration on MacOS, simply set `gpu.enabled` to true in `config.yaml` or `BOOM_GPU__ENABLED=true` in the environment, no additional setup is needed!**. It's only on Linux that you need to worry about the ONNX Runtime shared library and CUDA dependencies.

## Quick summary

- Native Linux GPU runs: you must set `ORT_DYLIB_PATH` in the shell before starting BOOM.
- Docker GPU runs: `ORT_DYLIB_PATH` is already set inside the GPU image.
- BOOM GPU behavior is controlled by `gpu.enabled`/`gpu.device_ids` in `config.yaml` or `BOOM_GPU__*` env vars.

## Native Linux GPU setup

Use this when running BOOM directly on your host (not via container image).

Requirements:

1. NVIDIA driver installed and working.
2. CUDA major version compatible with your driver. We recommend 12.8 (which is what we tested at the time of writing), but check ONNX Runtime GPU wheel requirements for your version if you run into issues.
3. cuDNN 9 for that CUDA major version.
4. A CUDA-enabled ONNX Runtime shared library, pointed to by `ORT_DYLIB_PATH`. See below for how to get this via the `onnxruntime-gpu` Python wheel.

Recommended setup (matches project defaults), using a Python virtual environment to manage the ONNX Runtime GPU installation:

```bash
uv venv --python 3.13 .venv
source .venv/bin/activate
uv pip install "onnxruntime-gpu>=1.24,<1.25"
export ORT_DYLIB_PATH="$PWD/.venv/lib/python3.13/site-packages/onnxruntime/capi/libonnxruntime.so.<version>"
```

where `<version>` is the actual version number of the ONNX Runtime wheel you installed (e.g. `libonnxruntime.so.1.24.1`).
Note that `ORT_DYLIB_PATH` must point to the actual `libonnxruntime.so.*` file, not just the directory.
Set it in every shell/session where you run BOOM natively, or simply add the export line to your shell profile (e.g. `~/.bashrc`).

Then, you can run BOOM's scheduler or API as usual, with the addition of GPU config as described in [BOOM GPU config](#boom-gpu-config) below.

## Docker GPU setup

Use this when running BOOM with Docker Compose.

Requirements on host:

1. NVIDIA driver.
2. NVIDIA Container Toolkit.

Run with GPU override:

```bash
BOOM_GPU__ENABLED=true docker compose -f docker-compose.yaml -f docker-compose.cuda.yaml up
```

What the GPU image already does for you:

1. Uses CUDA + cuDNN runtime base image.
2. Copies ONNX Runtime GPU shared libraries into `/opt/ort`.
3. Sets `ORT_DYLIB_PATH=/opt/ort/libonnxruntime.so`.
4. Sets `LD_LIBRARY_PATH` to include `/opt/ort` and CUDA library locations.

So for containerized GPU runs, you do not set `ORT_DYLIB_PATH` on the host.

## BOOM GPU config

In `config.yaml`:

```yaml
gpu:
  enabled: true
  device_ids: [0]
```

Environment overrides:

- `BOOM_GPU__ENABLED` (e.g. `true` or `false`)
- `BOOM_GPU__DEVICE_IDS` (e.g. `0`, or `0,1` for multiple GPUs)

## Troubleshooting

- `cudaErrorNoKernelImageForDevice`:
  The ONNX Runtime CUDA binary does not include kernels for your GPU architecture. Use a compatible `onnxruntime-gpu` build and update `ORT_DYLIB_PATH`.
- Missing provider libraries (`libonnxruntime_providers_*.so`):
  Ensure you are pointing to a valid wheel install and that the sibling provider `.so` files are present in the same `onnxruntime/capi` directory.
