// The entire module body is gated on `target_os = "linux"` (nvidia-smi / CUDA
// validation only runs there), so these imports are only used on Linux. Gate
// them too, otherwise non-Linux dev builds warn about unused imports.
#[cfg(target_os = "linux")]
use crate::{
    conf::AppConfig,
    enrichment::models::{BtsBotModel, Model},
    utils::enums::Survey,
};
#[cfg(target_os = "linux")]
use tracing::info;

#[cfg(target_os = "linux")]
const ZTF_MIN_FREE_VRAM_MIB: u64 = 10 * 1024;

#[cfg(target_os = "linux")]
fn validate_linux_gpu_runtime_preconditions() -> Result<(), &'static str> {
    // fail fast if the runtime library path is not explicitly configured.
    if std::env::var("ORT_DYLIB_PATH").map_or(true, |v| v.trim().is_empty()) {
        return Err("GPU is enabled but ORT_DYLIB_PATH is not set. \
Set ORT_DYLIB_PATH to a valid libonnxruntime.so path before starting this process.");
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn validate_gpu_inference(device_ids: &[i32]) -> Result<(), Box<dyn std::error::Error>> {
    info!("Validating GPU inference: running one inference per configured CUDA device");
    for &device_id in device_ids {
        info!(device_id, "Running BTSBotModel inference on device");
        // Standalone validation; no shared stream needed.
        let mut model = BtsBotModel::new_on_device(
            "data/models/btsbot-v1.0.1.onnx",
            device_id,
            std::ptr::null_mut(),
        )?;
        let metadata = ndarray::Array::from_shape_vec((1, 25), vec![0.5; 25])?;
        let triplet = ndarray::Array::from_shape_vec((1, 63, 63, 3), vec![0.5; 63 * 63 * 3])?;
        let _ = model.predict(&metadata, &triplet)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn parse_nvidia_smi_memory_free_output(
    output: &str,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let mut values = Vec::new();
    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value = trimmed.parse::<u64>().map_err(|e| {
            std::io::Error::other(format!(
                "failed to parse nvidia-smi free memory value '{trimmed}': {e}"
            ))
        })?;
        values.push(value);
    }

    if values.is_empty() {
        return Err(std::io::Error::other("nvidia-smi returned no GPU free-memory values").into());
    }

    Ok(values)
}

#[cfg(target_os = "linux")]
fn query_nvidia_smi_free_memory_mib() -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let output = std::process::Command::new("nvidia-smi")
        .args(["--query-gpu=memory.free", "--format=csv,noheader,nounits"])
        .output()
        .map_err(|e| {
            std::io::Error::other(format!(
                "failed to execute nvidia-smi for GPU memory validation: {e}"
            ))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(std::io::Error::other(format!(
            "nvidia-smi failed while validating free GPU memory: {}",
            stderr.trim()
        ))
        .into());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_nvidia_smi_memory_free_output(&stdout)
}

#[cfg(target_os = "linux")]
fn validate_gpu_free_vram(
    device_ids: &[i32],
    min_free_vram_mib: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let free_by_gpu = query_nvidia_smi_free_memory_mib()?;

    for &device_id in device_ids {
        if device_id < 0 {
            return Err(std::io::Error::other(format!(
                "invalid CUDA device id {device_id}; device ids must be >= 0"
            ))
            .into());
        }

        let index = device_id as usize;
        let Some(&free_mib) = free_by_gpu.get(index) else {
            return Err(std::io::Error::other(format!(
                "configured CUDA device id {device_id} is out of range; nvidia-smi reported {} device(s)",
                free_by_gpu.len()
            ))
            .into());
        };

        if free_mib < min_free_vram_mib {
            return Err(std::io::Error::other(format!(
                "configured CUDA device {device_id} has only {free_mib} MiB free VRAM; ZTF enrichment requires at least {min_free_vram_mib} MiB ({:.1} GiB) free per device",
                min_free_vram_mib as f64 / 1024.0
            ))
            .into());
        }

        info!(
            device_id,
            free_vram_mib = free_mib,
            min_required_mib = min_free_vram_mib,
            "validated free GPU VRAM for ZTF enrichment"
        );
    }

    Ok(())
}

#[cfg(target_os = "linux")]
pub fn validate_gpu_configuration_for_survey(
    survey: &Survey,
    config: &AppConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    if matches!(survey, &Survey::Ztf) && config.gpu.enabled {
        validate_linux_gpu_runtime_preconditions().map_err(|e| {
            std::io::Error::other(format!(
                "GPU configuration validation failed for survey {}: {e}",
                survey
            ))
        })?;
        validate_gpu_free_vram(&config.gpu.device_ids, ZTF_MIN_FREE_VRAM_MIB).map_err(|e| {
            std::io::Error::other(format!(
                "GPU free VRAM guardrail check failed for survey {}: {e}",
                survey
            ))
        })?;
        validate_gpu_inference(&config.gpu.device_ids).map_err(|e| {
            std::io::Error::other(format!(
                "GPU inference validation failed for survey {}: {e}",
                survey
            ))
        })?;
        info!("Confirmed GPU runtime preconditions, free VRAM guardrail, and GPU inference");
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::parse_nvidia_smi_memory_free_output;

    #[test]
    /// Verifies that the `nvidia-smi` parsing helper accepts the exact
    /// newline-separated MiB output format we rely on at startup.
    fn parses_memory_free_output_lines() {
        let parsed = parse_nvidia_smi_memory_free_output("12288\n8192\n").unwrap();
        assert_eq!(parsed, vec![12288, 8192]);
    }

    #[test]
    /// Verifies that malformed `nvidia-smi` output fails fast with a parse
    /// error instead of silently accepting bad VRAM data.
    fn rejects_invalid_memory_free_output() {
        let err = parse_nvidia_smi_memory_free_output("12288\nabc\n").unwrap_err();
        assert!(err
            .to_string()
            .contains("failed to parse nvidia-smi free memory value"));
    }
}
