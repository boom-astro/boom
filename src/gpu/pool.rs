//! GPU resource pool for distributing work across multiple CUDA devices.
//!
//! # Architecture
//!
//! `GpuPool` holds one GPU context per configured CUDA device. Workers
//! call `acquire()` to get a reference to the least-contended device,
//! submit their batch, and release. This maximizes GPU utilization across
//! multi-GPU nodes without dedicated GPU worker threads.
//!
//! # Usage
//!
//! ```ignore
//! let pool = GpuPool::new(&[0, 1, 2, 3])?;
//!
//! // From any enrichment worker thread:
//! let device = pool.acquire();
//! let results = device.context.batch_pso_multi_bazin(...)?;
//! drop(device); // releases the device back to the pool
//! ```
//!
//! # Contention model
//!
//! Each device has a `Mutex` that serializes access. When all devices are
//! busy, `acquire()` blocks on the device with the fewest queued waiters.
//! For N GPUs and M workers, contention is reduced by a factor of N vs
//! a single shared mutex.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use tracing::info;

/// A single GPU device with its context and contention tracking.
struct GpuDevice {
    context: Mutex<DeviceContext>,
    /// Approximate number of threads waiting or holding this device.
    /// Used by `acquire()` to pick the least-contended device.
    active_count: AtomicUsize,
    device_id: i32,
}

/// The actual GPU context held behind the mutex.
pub struct DeviceContext {
    pub device_id: i32,
    /// Lightcurve-fitting GPU context for batch GP/PSO/SVI operations.
    #[cfg(feature = "cuda")]
    pub lc_gpu: lightcurve_fitting::gpu::GpuContext,
}

impl std::fmt::Debug for DeviceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceContext")
            .field("device_id", &self.device_id)
            .finish()
    }
}

/// Guard returned by `GpuPool::acquire()`. Holds the mutex lock and
/// decrements the active count on drop.
pub struct DeviceGuard<'a> {
    pub context: MutexGuard<'a, DeviceContext>,
    active_count: &'a AtomicUsize,
}

impl<'a> Drop for DeviceGuard<'a> {
    fn drop(&mut self) {
        self.active_count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Pool of GPU devices for distributing work across multiple CUDA devices.
pub struct GpuPool {
    devices: Vec<GpuDevice>,
}

impl std::fmt::Debug for GpuPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuPool")
            .field("n_devices", &self.devices.len())
            .finish()
    }
}

impl GpuPool {
    /// Create a new pool with one context per device ID.
    pub fn new(device_ids: &[i32]) -> Result<Arc<Self>, String> {
        if device_ids.is_empty() {
            return Err("GpuPool requires at least one device ID".to_string());
        }

        let mut devices = Vec::with_capacity(device_ids.len());
        for &id in device_ids {
            info!(device_id = id, "initializing GPU device context");
            #[cfg(feature = "cuda")]
            let lc_gpu = lightcurve_fitting::gpu::GpuContext::new(id)?;
            devices.push(GpuDevice {
                context: Mutex::new(DeviceContext {
                    device_id: id,
                    #[cfg(feature = "cuda")]
                    lc_gpu,
                }),
                active_count: AtomicUsize::new(0),
                device_id: id,
            });
        }

        info!(
            n_devices = devices.len(),
            "GPU pool initialized with {} device(s)",
            devices.len()
        );
        Ok(Arc::new(Self { devices }))
    }

    /// Acquire the least-contended GPU device. Blocks until available.
    ///
    /// The returned `DeviceGuard` holds the device lock. Drop it to release
    /// the device back to the pool.
    pub fn acquire(&self) -> DeviceGuard<'_> {
        // Find the device with the lowest active count
        let best = self
            .devices
            .iter()
            .min_by_key(|d| d.active_count.load(Ordering::Relaxed))
            .expect("GpuPool is non-empty by construction");

        // Increment before locking so other threads see the contention
        best.active_count.fetch_add(1, Ordering::Relaxed);
        let guard = match best.context.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                info!(
                    "GpuPool: device {} mutex poisoned, recovering lock",
                    best.device_id
                );
                poisoned.into_inner()
            }
        };

        DeviceGuard {
            context: guard,
            active_count: &best.active_count,
        }
    }

    /// Number of devices in the pool.
    pub fn len(&self) -> usize {
        self.devices.len()
    }

    /// Returns true if the pool has no devices (should never happen after construction).
    pub fn is_empty(&self) -> bool {
        self.devices.is_empty()
    }

    /// Get the device IDs in the pool.
    pub fn device_ids(&self) -> Vec<i32> {
        self.devices.iter().map(|d| d.device_id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_pool_creation() {
        let pool = GpuPool::new(&[0]).unwrap();
        assert_eq!(pool.len(), 1);
        assert!(!pool.is_empty());
        assert_eq!(pool.device_ids(), vec![0]);
    }

    /// With cuda feature, multi-device tests require multiple real GPUs.
    /// Without cuda, any device IDs work (no real context is created).
    #[test]
    #[cfg(not(feature = "cuda"))]
    fn test_pool_multi_device() {
        let pool = GpuPool::new(&[0, 1, 2, 3]).unwrap();
        assert_eq!(pool.len(), 4);
        assert_eq!(pool.device_ids(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_pool_empty_fails() {
        assert!(GpuPool::new(&[]).is_err());
    }

    #[test]
    #[cfg(not(feature = "cuda"))]
    fn test_acquire_returns_device() {
        let pool = GpuPool::new(&[5]).unwrap();
        let guard = pool.acquire();
        assert_eq!(guard.context.device_id, 5);
    }

    #[test]
    fn test_acquire_returns_device_0() {
        let pool = GpuPool::new(&[0]).unwrap();
        let guard = pool.acquire();
        assert_eq!(guard.context.device_id, 0);
    }

    #[test]
    #[cfg(not(feature = "cuda"))]
    fn test_acquire_least_contended() {
        let pool = GpuPool::new(&[0, 1]).unwrap();

        // Hold device — the next acquire should pick the other one
        let guard1 = pool.acquire();
        let id1 = guard1.context.device_id;

        let guard2 = pool.acquire();
        let id2 = guard2.context.device_id;

        // With 2 devices, they should be different
        assert_ne!(id1, id2);
    }

    #[test]
    #[cfg(not(feature = "cuda"))]
    fn test_acquire_from_multiple_threads() {
        let pool = Arc::new(GpuPool::new(&[0, 1, 2, 3]).unwrap());
        let mut handles = Vec::new();

        for _ in 0..8 {
            let pool = Arc::clone(&pool);
            handles.push(std::thread::spawn(move || {
                let guard = pool.acquire();
                let id = guard.context.device_id;
                // Simulate some work
                std::thread::sleep(std::time::Duration::from_millis(10));
                drop(guard);
                id
            }));
        }

        let ids: Vec<i32> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // All IDs should be valid device IDs
        for id in &ids {
            assert!([0, 1, 2, 3].contains(id));
        }
    }

    #[test]
    fn test_active_count_decrements_on_drop() {
        let pool = GpuPool::new(&[0]).unwrap();

        {
            let _guard = pool.acquire();
            assert_eq!(pool.devices[0].active_count.load(Ordering::Relaxed), 1);
        }
        // Guard dropped
        assert_eq!(pool.devices[0].active_count.load(Ordering::Relaxed), 0);
    }
}
