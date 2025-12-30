//! SIMD-optimized implementations with graceful fallback
//!
//! Provides optimized versions of hot-path functions with runtime CPU feature detection:
//! - AVX-512 (x86_64)
//! - AVX2 (x86_64)
//! - NEON (aarch64)
//! - Scalar fallback (all platforms)

pub mod checksum;

pub use checksum::rolling_checksum_simd;
