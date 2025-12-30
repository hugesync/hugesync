//! SIMD-optimized rolling checksum implementations
//!
//! The rolling checksum is Adler-32 style:
//! - a = sum of all bytes
//! - b = sum of running sums = n*byte[0] + (n-1)*byte[1] + ... + 1*byte[n-1]
//! - result = (b << 16) | (a & 0xffff)
//!
//! SIMD acceleration processes multiple bytes in parallel using vectorized
//! weighted sums. The key insight is that for a chunk of N bytes:
//!   a_chunk = sum(bytes)
//!   b_chunk = N*byte[0] + (N-1)*byte[1] + ... + 1*byte[N-1]
//!
//! When combining chunks:
//!   a_total = a_prev + a_chunk
//!   b_total = b_prev + len_remaining * a_prev + b_chunk

use std::sync::atomic::{AtomicU8, Ordering};

/// CPU feature detection result (cached after first check)
/// 0 = not checked, 1 = scalar, 2 = NEON, 3 = AVX2, 4 = AVX-512
static CPU_FEATURE: AtomicU8 = AtomicU8::new(0);

const FEATURE_UNCHECKED: u8 = 0;
const FEATURE_SCALAR: u8 = 1;
#[cfg(target_arch = "aarch64")]
const FEATURE_NEON: u8 = 2;
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
const FEATURE_AVX2: u8 = 3;
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
const FEATURE_AVX512: u8 = 4;

/// Detect best available SIMD feature at runtime
fn detect_cpu_feature() -> u8 {
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    {
        if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw") {
            return FEATURE_AVX512;
        }
        if is_x86_feature_detected!("avx2") {
            return FEATURE_AVX2;
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        // NEON is always available on aarch64
        return FEATURE_NEON;
    }

    #[allow(unreachable_code)]
    FEATURE_SCALAR
}

/// Get cached CPU feature level
#[inline]
fn get_cpu_feature() -> u8 {
    let cached = CPU_FEATURE.load(Ordering::Relaxed);
    if cached != FEATURE_UNCHECKED {
        return cached;
    }

    let detected = detect_cpu_feature();
    CPU_FEATURE.store(detected, Ordering::Relaxed);
    detected
}

/// Compute rolling checksum using best available SIMD instructions
///
/// Automatically selects the fastest implementation based on CPU features:
/// - AVX2 on x86_64 (Haswell+, Zen 1+)
/// - NEON on aarch64 (all ARM64)
/// - Scalar fallback on other platforms
///
/// All implementations produce identical results to the scalar version.
#[inline]
pub fn rolling_checksum_simd(data: &[u8]) -> u32 {
    let feature = get_cpu_feature();

    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    {
        if feature == FEATURE_AVX512 || feature == FEATURE_AVX2 {
            return unsafe { rolling_checksum_avx2(data) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if feature == FEATURE_NEON {
            return unsafe { rolling_checksum_neon(data) };
        }
    }

    rolling_checksum_scalar(data)
}

/// Scalar implementation (fallback for all platforms)
/// This is the reference implementation that all SIMD versions must match.
#[inline]
pub fn rolling_checksum_scalar(data: &[u8]) -> u32 {
    let mut a: u32 = 0;
    let mut b: u32 = 0;

    for &byte in data {
        a = a.wrapping_add(byte as u32);
        b = b.wrapping_add(a);
    }

    (b << 16) | (a & 0xffff)
}

// ============================================================================
// AVX2 Implementation (x86_64)
//
// Processes 32 bytes at a time with exact weighted sum calculation.
// Uses precomputed weights [32, 31, 30, ..., 1] for the b component.
// ============================================================================

#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[target_feature(enable = "avx2")]
unsafe fn rolling_checksum_avx2(data: &[u8]) -> u32 {
    use std::arch::x86_64::*;

    const CHUNK_SIZE: usize = 32;
    let len = data.len();

    if len < CHUNK_SIZE {
        return rolling_checksum_scalar(data);
    }

    let mut a: u32 = 0;
    let mut b: u32 = 0;
    let mut offset = 0;
    let mut remaining = len;

    // Weights for 32 bytes: first 16 bytes get [32, 31, ..., 17], last 16 get [16, 15, ..., 1]
    // These weights compute: 32*b0 + 31*b1 + ... + 1*b31
    let weights_first: [i16; 16] = [32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17];
    let weights_second: [i16; 16] = [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1];

    let weights_first_vec = _mm256_loadu_si256(weights_first.as_ptr() as *const __m256i);
    let weights_second_vec = _mm256_loadu_si256(weights_second.as_ptr() as *const __m256i);

    let ptr = data.as_ptr();

    while remaining >= CHUNK_SIZE {
        let chunk = _mm256_loadu_si256(ptr.add(offset) as *const __m256i);

        // Split into low and high 16 bytes, extend to 16-bit
        let zeros = _mm256_setzero_si256();

        // Extract low 128 bits (bytes 0-15) and high 128 bits (bytes 16-31)
        let lo_128 = _mm256_castsi256_si128(chunk);
        let hi_128 = _mm256_extracti128_si256(chunk, 1);

        // Extend bytes to 16-bit (for weighted multiplication)
        let lo_16 = _mm256_cvtepu8_epi16(lo_128);  // bytes 0-15
        let hi_16 = _mm256_cvtepu8_epi16(hi_128);  // bytes 16-31

        // Sum all bytes for 'a' using SAD (fast horizontal sum)
        let sad = _mm256_sad_epu8(chunk, zeros);
        let sum0 = _mm256_extract_epi64(sad, 0) as u32;
        let sum1 = _mm256_extract_epi64(sad, 1) as u32;
        let sum2 = _mm256_extract_epi64(sad, 2) as u32;
        let sum3 = _mm256_extract_epi64(sad, 3) as u32;
        let chunk_sum = sum0.wrapping_add(sum1).wrapping_add(sum2).wrapping_add(sum3);

        // Compute weighted sum for 'b': sum(weight[i] * byte[i])
        // bytes 0-15 get weights [32, 31, ..., 17]
        // bytes 16-31 get weights [16, 15, ..., 1]
        let weighted_lo = _mm256_madd_epi16(lo_16, weights_first_vec);
        let weighted_hi = _mm256_madd_epi16(hi_16, weights_second_vec);

        // Horizontal sum of weighted values
        let weighted_sum = _mm256_add_epi32(weighted_lo, weighted_hi);

        // Reduce to single value
        let weighted_sum_128 = _mm_add_epi32(
            _mm256_castsi256_si128(weighted_sum),
            _mm256_extracti128_si256(weighted_sum, 1),
        );
        let weighted_sum_64 = _mm_add_epi32(weighted_sum_128, _mm_srli_si128(weighted_sum_128, 8));
        let weighted_sum_32 = _mm_add_epi32(weighted_sum_64, _mm_srli_si128(weighted_sum_64, 4));
        let b_chunk = _mm_cvtsi128_si32(weighted_sum_32) as u32;

        // Update running totals
        // b_new = b_old + CHUNK_SIZE * a_old + b_chunk
        b = b.wrapping_add((CHUNK_SIZE as u32).wrapping_mul(a));
        b = b.wrapping_add(b_chunk);
        a = a.wrapping_add(chunk_sum);

        offset += CHUNK_SIZE;
        remaining -= CHUNK_SIZE;
    }

    // Handle remaining bytes with scalar
    for &byte in &data[offset..] {
        a = a.wrapping_add(byte as u32);
        b = b.wrapping_add(a);
    }

    (b << 16) | (a & 0xffff)
}

// ============================================================================
// NEON Implementation (aarch64)
//
// Processes 16 bytes at a time with exact weighted sum calculation.
// ============================================================================

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn rolling_checksum_neon(data: &[u8]) -> u32 {
    use std::arch::aarch64::*;

    const CHUNK_SIZE: usize = 16;
    let len = data.len();

    if len < CHUNK_SIZE {
        return rolling_checksum_scalar(data);
    }

    let mut a: u32 = 0;
    let mut b: u32 = 0;
    let mut offset = 0;
    let mut remaining = len;

    // Weights for 16 bytes: first 8 get [16, 15, ..., 9], last 8 get [8, 7, ..., 1]
    // These weights compute: 16*b0 + 15*b1 + ... + 1*b15
    let weights_first: [u16; 8] = [16, 15, 14, 13, 12, 11, 10, 9];
    let weights_second: [u16; 8] = [8, 7, 6, 5, 4, 3, 2, 1];

    let weights_first_vec = vld1q_u16(weights_first.as_ptr());
    let weights_second_vec = vld1q_u16(weights_second.as_ptr());

    let ptr = data.as_ptr();

    while remaining >= CHUNK_SIZE {
        let chunk = vld1q_u8(ptr.add(offset));

        // Sum all bytes for 'a'
        let sum16 = vpaddlq_u8(chunk);
        let sum32 = vpaddlq_u16(sum16);
        let sum64 = vpaddlq_u32(sum32);
        let chunk_sum = (vgetq_lane_u64(sum64, 0) + vgetq_lane_u64(sum64, 1)) as u32;

        // Split into low and high 8 bytes, extend to 16-bit
        // low = bytes 0-7, high = bytes 8-15
        let lo_8 = vget_low_u8(chunk);   // bytes 0-7
        let hi_8 = vget_high_u8(chunk);  // bytes 8-15
        let lo_16 = vmovl_u8(lo_8);
        let hi_16 = vmovl_u8(hi_8);

        // Weighted multiplication
        // bytes 0-7 get weights [16, 15, ..., 9]
        // bytes 8-15 get weights [8, 7, ..., 1]
        let weighted_lo = vmull_u16(vget_low_u16(lo_16), vget_low_u16(weights_first_vec));
        let weighted_lo2 = vmull_u16(vget_high_u16(lo_16), vget_high_u16(weights_first_vec));
        let weighted_hi = vmull_u16(vget_low_u16(hi_16), vget_low_u16(weights_second_vec));
        let weighted_hi2 = vmull_u16(vget_high_u16(hi_16), vget_high_u16(weights_second_vec));

        // Sum all weighted values
        let sum1 = vaddq_u32(weighted_lo, weighted_lo2);
        let sum2 = vaddq_u32(weighted_hi, weighted_hi2);
        let sum3 = vaddq_u32(sum1, sum2);

        // Horizontal sum
        let b_chunk = vaddvq_u32(sum3);

        // Update running totals
        // b_new = b_old + CHUNK_SIZE * a_old + b_chunk
        b = b.wrapping_add((CHUNK_SIZE as u32).wrapping_mul(a));
        b = b.wrapping_add(b_chunk);
        a = a.wrapping_add(chunk_sum);

        offset += CHUNK_SIZE;
        remaining -= CHUNK_SIZE;
    }

    // Handle remaining bytes with scalar
    for &byte in &data[offset..] {
        a = a.wrapping_add(byte as u32);
        b = b.wrapping_add(a);
    }

    (b << 16) | (a & 0xffff)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_basic() {
        let data = b"hello world";
        let sum = rolling_checksum_scalar(data);
        assert_ne!(sum, 0);
    }

    #[test]
    fn test_scalar_empty() {
        let data = b"";
        let sum = rolling_checksum_scalar(data);
        assert_eq!(sum, 0);
    }

    #[test]
    fn test_scalar_single_byte() {
        let data = b"a";
        let sum = rolling_checksum_scalar(data);
        // a = 97, b = 97
        assert_eq!(sum, (97 << 16) | 97);
    }

    #[test]
    fn test_simd_matches_scalar_small() {
        let data = b"test data for checksum";
        let scalar = rolling_checksum_scalar(data);
        let simd = rolling_checksum_simd(data);

        // SIMD must produce exact same result as scalar
        assert_eq!(scalar, simd, "SIMD must match scalar exactly");
    }

    #[test]
    fn test_simd_matches_scalar_large() {
        // Test with data large enough to use SIMD paths
        let data: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
        let scalar = rolling_checksum_scalar(&data);
        let simd = rolling_checksum_simd(&data);

        // SIMD must produce exact same result as scalar
        assert_eq!(scalar, simd, "SIMD must match scalar exactly for large data");
    }

    #[test]
    fn test_simd_matches_scalar_various_sizes() {
        // Test various sizes to cover edge cases
        for size in [15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 256, 1000, 5000] {
            let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            let scalar = rolling_checksum_scalar(&data);
            let simd = rolling_checksum_simd(&data);
            assert_eq!(
                scalar, simd,
                "SIMD must match scalar for size {}",
                size
            );
        }
    }

    #[test]
    fn test_simd_matches_scalar_random_data() {
        // Test with pseudo-random data
        let mut seed: u64 = 0xDEADBEEF;
        let data: Vec<u8> = (0..4096)
            .map(|_| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                (seed >> 32) as u8
            })
            .collect();

        let scalar = rolling_checksum_scalar(&data);
        let simd = rolling_checksum_simd(&data);
        assert_eq!(scalar, simd, "SIMD must match scalar for random data");
    }

    #[test]
    fn test_simd_deterministic() {
        let data: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();
        let sum1 = rolling_checksum_simd(&data);
        let sum2 = rolling_checksum_simd(&data);
        assert_eq!(sum1, sum2);
    }

    #[test]
    fn test_simd_different_data() {
        let data1: Vec<u8> = vec![1; 1000];
        let data2: Vec<u8> = vec![2; 1000];
        let sum1 = rolling_checksum_simd(&data1);
        let sum2 = rolling_checksum_simd(&data2);
        assert_ne!(sum1, sum2);
    }

    #[test]
    fn test_cpu_feature_detection() {
        let feature = get_cpu_feature();
        assert!(feature >= FEATURE_SCALAR);

        #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
        {
            // On x86_64, should be at least scalar
            println!("Detected feature level: {}", feature);
            assert!(feature >= FEATURE_SCALAR);
        }

        #[cfg(target_arch = "aarch64")]
        {
            // On aarch64, should be NEON
            assert_eq!(feature, FEATURE_NEON);
        }
    }

    #[test]
    fn test_simd_never_panics() {
        // This test verifies that the SIMD dispatch never panics,
        // regardless of what CPU features are available.
        // The binary should work on ANY machine due to runtime detection.

        // Test various edge cases
        let empty: &[u8] = &[];
        let _ = rolling_checksum_simd(empty);

        let one = &[42u8];
        let _ = rolling_checksum_simd(one);

        // Sizes around SIMD chunk boundaries
        for size in [15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129] {
            let data: Vec<u8> = (0..size).map(|i| i as u8).collect();
            let result = rolling_checksum_simd(&data);
            assert_ne!(result, 0, "Non-zero data should produce non-zero checksum");
        }

        // Large data
        let large: Vec<u8> = vec![0xAB; 1024 * 1024];
        let _ = rolling_checksum_simd(&large);
    }

    #[test]
    fn test_scalar_fallback_always_works() {
        // Verify scalar fallback produces correct results
        // This ensures the binary works even on CPUs without SIMD
        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();

        let scalar_result = rolling_checksum_scalar(&data);
        assert_ne!(scalar_result, 0);

        // Verify it's deterministic
        let scalar_result2 = rolling_checksum_scalar(&data);
        assert_eq!(scalar_result, scalar_result2);
    }
}
