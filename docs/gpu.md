# GPU Acceleration

BOOM supports GPU acceleration for two workloads: ONNX model inference (ACAI, BTSBot) and lightcurve fitting. GPU support is optional and configured at runtime.

When you run BOOM natively on Linux and want to use GPU acceleration (with BOOM_GPU__ENABLED=true), make sure the host has a working NVIDIA driver, the CUDA major version that matches that driver, and cuDNN 9 for that CUDA version. For ORT's CUDA execution provider, `libcudnn.so.9` must be available to the system linker. The matching cuDNN package is `cudnn9-cuda-12` for CUDA 12 and `cudnn9-cuda-13` for CUDA 13. 

If you see `cudaErrorNoKernelImageForDevice`, the ORT CUDA binary you are using likely does not include kernels for your GPU architecture. In that case, use the Python `onnxruntime-gpu` wheel as the runtime library source and point `ORT_DYLIB_PATH` at its `libonnxruntime.so.*`. `ORT_CUDA_VERSION` only selects the CUDA runtime variant; it does not change which GPU architectures the binary was compiled for.

Working setup for local Linux development:

1. Create a Python 3.13 virtual environment with `uv`.
2. Install `onnxruntime-gpu>=1.24,<1.25` into that venv.
3. Use the shared libraries from `.venv/lib/python3.13/site-packages/onnxruntime/capi/`.
4. Set `ORT_DYLIB_PATH` to the matching `libonnxruntime.so.*` file and add the same directory to `LD_LIBRARY_PATH`.

Example:

```bash
uv venv --python 3.13 .venv
source .venv/bin/activate
uv pip install "onnxruntime-gpu>=1.24,<1.25"
export ORT_DYLIB_PATH="$PWD/.venv/lib/python3.13/site-packages/onnxruntime/capi/libonnxruntime.so.1.24.4"
export LD_LIBRARY_PATH="$PWD/.venv/lib/python3.13/site-packages/onnxruntime/capi:$LD_LIBRARY_PATH"
```

If you run BOOM through the provided Docker images, those CUDA and cuDNN dependencies are already included in the image, so you do not need to install them on the host just to use the containerized workflow.

*If `ort` does not ship with all pre-built binaries for your system, you may run into a really long built time when the code first tries to run any ML-related GPU operations. This is a one-time cost, and subsequent runs will be fast. Unfortunately, the build time can be as long as 20 minutes on some platforms, so be patient. We are working on improving this in future releases by providing more pre-built binaries and better build caching. But, we are somewhat dependent on `ort`'s release process for this, so we appreciate your patience and understanding.*

## Configuration

In `config.yaml`:

```yaml
gpu:
  enabled: true
  device_ids: [0]  # CUDA device IDs (Linux) or ignored (macOS)
```

Environment variable overrides: `BOOM_GPU__ENABLED`, `BOOM_GPU__DEVICE_IDS`.

## Docker deployment

The Dockerfile uses NVIDIA CUDA base images so the container can run on both GPU and CPU-only hosts. GPU device passthrough is controlled by a separate compose override file.

**CPU-only** (works on any host):

```bash
docker compose up
```

**With GPUs** (requires [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)):

```bash
docker compose -f docker-compose.yaml -f docker-compose.cuda.yaml up
```

The GPU override adds NVIDIA device reservations to all scheduler services. Set `gpu.enabled: true` in `config.yaml` to have the application use them.

The GPU image installs `onnxruntime-gpu` only in the builder stage, runs the warmup once, and then copies the four ORT shared objects into `/opt/ort` in the final runtime image. The runtime stage has no Python dependency; it just points `ORT_DYLIB_PATH` at `/opt/ort/libonnxruntime.so.1.24.4` and loads the three provider `.so` files from the same directory.

During the GPU image build, BOOM also runs `scripts/warmup_ort_cuda.sh` once to load BTSBot on a fake batch and populate `~/.nv/ComputeCache`. That cache is copied into the final GPU image, so the first real enrichment run does not pay the kernel compilation cost.

If you want to warm the cache manually in a local Linux dev setup, run the same helper script after exporting `ORT_DYLIB_PATH`.

For native installs outside Docker, the container image is not available to provide the CUDA runtime libraries, so the host must have a compatible NVIDIA driver, CUDA toolkit, and cuDNN 9 installation.

If your GPU is newer than the prebuilt ORT CUDA packages support, use a matching `onnxruntime-gpu` wheel or a custom ONNX Runtime build and set `ORT_DYLIB_PATH` before starting BOOM.

## Platform behavior

### Linux (CUDA)

ONNX models run via the CUDA execution provider. Multiple GPUs are supported through the GPU pool, which distributes work to the least-contended device. Lightcurve fitting (when integrated) uses CUDA via the `cuda` feature flag:

```bash
cargo build --features cuda
```

### macOS (CoreML / Metal)

ONNX models run via the CoreML execution provider automatically on macOS builds. Lightcurve fitting uses Metal compute shaders via the `metal` feature flag:

```bash
cargo build --features metal
```

Metal computes in float32 with f64-to-f32 conversion at boundaries. Models with steep exponentials (Arnett, Magnetar, Afterglow) may show up to ~5% relative error vs. CPU double-precision in near-zero flux regions; this does not affect fitting quality since those regions are noise-dominated.

## GPU pool

The `GpuPool` (in `src/gpu/pool.rs`) manages multiple GPU devices with least-contended device selection. Each device has a mutex and an atomic contention counter. Workers call `pool.acquire()` to get a `DeviceGuard` that releases the device on drop.

With N devices and M workers, contention is reduced by a factor of N compared to a single shared mutex.

## Fallback

When `gpu.enabled` is `false` or no GPU is available, all inference runs on CPU. The ORT session builder attempts the platform-specific execution provider first and falls back to CPU transparently.

## Quick BTSBot Benchmark

For a small synthetic benchmark of BTSBot with fake inputs, use:

```bash
cargo run --release --bin bench_btsbot_onnx -- --provider cpu --batch-size 1024
cargo run --release --bin bench_btsbot_onnx -- --provider cuda --batch-size 1024 --device-id 0
```

You can also run both providers in one invocation with `--provider both`. The script uses deterministic fake metadata and triplet tensors with the same shapes as the app: `(N, 25)` and `(N, 63, 63, 3)`.
