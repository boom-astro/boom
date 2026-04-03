# GPU Acceleration

BOOM supports GPU acceleration for two workloads: ONNX model inference (ACAI, BTSBot) and lightcurve fitting. GPU support is optional and configured at runtime.

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
