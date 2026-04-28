# Linux ONNX Runtime Setup & GPU Acceleration

This page covers ONNX Runtime setup on Linux and GPU acceleration for BOOM.

**On macOS, no ONNX Runtime setup is needed** — simply set `gpu.enabled: true` in `config.yaml` (or `BOOM_GPU__ENABLED=true`) when you want GPU inference, and BOOM handles the rest.

**On Linux**, BOOM links to the ONNX Runtime shared library at process start via `ORT_DYLIB_PATH`. This is required regardless of whether you use a GPU or not.

**Note:** On Linux, If `ORT_DYLIB_PATH` is not set, BOOM will fail to start with a clear error. This is a hard requirement due to ONNX Runtime's dynamic loading behavior.

## Quick summary

- Native Linux (CPU or GPU): you **must** set `ORT_DYLIB_PATH` before starting BOOM.
- Docker GPU runs: `ORT_DYLIB_PATH` is already set inside the GPU image.
- BOOM GPU behavior is controlled by `gpu.enabled`/`gpu.device_ids` in `config.yaml` or `BOOM_GPU__*` env vars.

## Native Linux setup

### ONNX Runtime (required on all Linux installs)

Whether or not you use GPU inference, you need to install the ONNX Runtime shared library and tell BOOM where to find it via `ORT_DYLIB_PATH`.

The easiest way is to install the Python wheel and point to the bundled `.so` file. We recommend using [uv](https://docs.astral.sh/uv/getting-started/installation/) to manage a small virtual environment for this:

**CPU-only (no GPU):**

```bash
uv venv --python 3.13 .venv
source .venv/bin/activate
uv pip install "onnxruntime>=1.24,<1.25"
export ORT_DYLIB_PATH="$PWD/.venv/lib/python3.13/site-packages/onnxruntime/capi/libonnxruntime.so.1.24.4"
```

**GPU (CUDA):** — see [GPU inference](#gpu-inference) below for additional system requirements, then use:

```bash
uv venv --python 3.13 .venv
source .venv/bin/activate
uv pip install "onnxruntime-gpu>=1.24,<1.25"
export ORT_DYLIB_PATH="$PWD/.venv/lib/python3.13/site-packages/onnxruntime/capi/libonnxruntime.so.1.24.4"
```

In both cases, `ORT_DYLIB_PATH` must point to the versioned `libonnxruntime.so.*` file (e.g. `libonnxruntime.so.1.24.4`), not just the directory. Adjust the version number to match the actual file present after installation:

```bash
ls .venv/lib/python3.13/site-packages/onnxruntime/capi/libonnxruntime.so.*
```

Set `ORT_DYLIB_PATH` in every shell or session where you run BOOM natively, or add the export line to your shell profile (e.g. `~/.bashrc`) so it is picked up automatically.

### GPU inference

GPU inference requires additional system software beyond the ONNX Runtime wheel. You also need:

1. NVIDIA driver installed and working.
2. CUDA major version compatible with your driver. We recommend 12.8 (which is what we tested at the time of writing), but check ONNX Runtime GPU wheel requirements for your version if you run into issues.
3. cuDNN 9 for that CUDA major version.
4. At least 10 GiB of free VRAM on each configured CUDA device for ZTF enrichment.

On Linux, BOOM validates this at scheduler startup for ZTF with GPU enabled and fails fast if any configured device has less than 10240 MiB free. You can check free VRAM with:

```bash
nvidia-smi --query-gpu=index,memory.free --format=csv,noheader,nounits
```

Then install the GPU wheel as shown above, enable GPU inference in BOOM config, and run as usual:

```yaml
# config.yaml
gpu:
  enabled: true
  device_ids: [0]
```

See [BOOM GPU config](#boom-gpu-config) for all available settings.

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
