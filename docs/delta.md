# Delta Sync Algorithm

This document explains how HugeSync's delta synchronization works, including the hash layers and why each design decision was made.

## The Problem: Why Not Just Hash Fixed Blocks?

Consider a 100MB file divided into 20 fixed 5MB blocks:

```
Original file:
[Block 0][Block 1][Block 2][Block 3]...[Block 19]
   5MB      5MB      5MB      5MB          5MB
```

If we hash each block and store in `.hssig`, we get 20 hashes. This works for appends and in-place modifications.

**But what if someone inserts 1 byte at the beginning?**

```
Modified file:
[X][───────── All blocks shifted by 1 byte ─────────]
1B
```

Now EVERY block is misaligned. Comparing hashes:
- New bytes `0..5MB` ≠ Old Block 0 (off by 1 byte)
- New bytes `5MB..10MB` ≠ Old Block 1 (off by 1 byte)
- etc.

**Result: 0% match. Must upload entire 100MB even though 99.999999% is identical!**

## The Solution: Rolling Checksum

Instead of checking only at fixed positions, we scan byte-by-byte using a **rolling checksum** that can be updated in O(1) time:

```
Position 0: checksum(bytes[0..5MB]) → no match
Position 1: checksum(bytes[1..5MB+1]) → MATCH with Block 0!
Position 5MB+1: checksum(bytes[5MB+1..10MB+1]) → MATCH with Block 1!
...
```

**Result: 99.99% match. Only upload the 1 new byte!**

This is exactly how rsync works.

## The Three-Layer Hash Architecture

Scanning byte-by-byte means checking up to N positions for an N-byte file. We need to make each check as fast as possible while maintaining correctness.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         HASH LAYERS                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Layer 1: Rolling Checksum (Adler-32 style)                         │
│  ├── Purpose: Find CANDIDATE matches at any byte position           │
│  ├── Output: 32-bit hash                                            │
│  ├── Speed: O(1) to slide window by 1 byte                          │
│  ├── Collision rate: ~1/65,536 (2^16)                               │
│  └── Note: High false positive rate, but extremely fast             │
│                         │                                            │
│                         ▼ (on rolling hash match)                    │
│                                                                      │
│  Layer 2: Quick Hash (xxHash3)                                      │
│  ├── Purpose: Fast rejection of false positives                      │
│  ├── Output: 64-bit hash                                            │
│  ├── Speed: ~30 GB/s (for 5MB block: ~0.17ms)                       │
│  ├── Collision rate: ~1/2^64 ≈ 0                                    │
│  └── Note: Hashes FULL block, not sampled                           │
│                         │                                            │
│                         ▼ (on xxHash3 match)                         │
│                                                                      │
│  Layer 3: Strong Hash (BLAKE3)                                      │
│  ├── Purpose: Cryptographic verification                            │
│  ├── Output: 256-bit hash                                           │
│  ├── Speed: ~3 GB/s (for 5MB block: ~1.7ms)                         │
│  ├── Security: Cryptographically secure                             │
│  └── Note: Final verification, prevents malicious collisions        │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Why Each Layer?

| Layer | Without It | With It | Savings |
|-------|-----------|---------|---------|
| Rolling | Can't find shifted blocks | O(1) sliding window | Enables delta sync |
| xxHash3 | ~1500 BLAKE3 calls for 100MB | ~1500 xxHash3 calls | ~7 seconds saved |
| BLAKE3 | Could match wrong block | Cryptographic certainty | Correctness |

### Performance Example (100MB file with repetitive data)

Without quick hash (xxHash3):
- Rolling hash matches: ~1500 false positives
- BLAKE3 per match: ~1.7ms
- Total wasted time: 1500 × 1.7ms = **2.55 seconds**

With quick hash (xxHash3):
- Rolling hash matches: ~1500 false positives
- xxHash3 per match: ~0.17ms
- xxHash3 rejects: ~1500 (all false positives)
- BLAKE3 calls: ~0 (only true matches)
- Total wasted time: 1500 × 0.17ms = **0.26 seconds**

**Speedup: ~10x for repetitive data patterns**

## Algorithm Choices

### Layer 1: Why Adler-32 (Not Gear/Rabin)?

| Algorithm | Speed | Use Case | Our Choice |
|-----------|-------|----------|------------|
| Adler-32 | Fast | Fixed-block rsync-style | ✅ Yes |
| Gear | 10x faster | Content-Defined Chunking (CDC) | ❌ No |
| Rabin | Good distribution | CDC, dedup | ❌ No |
| Buzhash | No multiplication | CDC | ❌ No |

**Decision:** Adler-32 is optimal for fixed-block rsync-style matching. Gear hash is faster but designed for CDC (variable-size chunks), which we don't use.

#### SIMD Acceleration

The rolling checksum is SIMD-accelerated with runtime CPU detection:

| Platform | Instruction Set | Chunk Size | Speedup |
|----------|-----------------|------------|---------|
| x86_64 | AVX2 | 32 bytes | ~8x |
| x86_64 | AVX-512 | 64 bytes | ~16x |
| aarch64 | NEON | 16 bytes | ~4x |
| All | Scalar (fallback) | 1 byte | 1x |

The SIMD implementation uses vectorized weighted sums to compute exact results matching the scalar version:

```
For a 32-byte chunk with AVX2:
┌────────────────────────────────────────────────────────────────┐
│ Load 32 bytes into YMM register                                │
│                                                                 │
│ Sum 'a': Use VPSADBW (SAD against zero) for fast byte sum     │
│   → 4 horizontal additions = total byte sum                    │
│                                                                 │
│ Weighted 'b': VPMADDWD with weights [32,31,...,1]              │
│   → Multiply bytes by decreasing weights                       │
│   → Horizontal sum for chunk contribution                      │
│                                                                 │
│ Combine: b += CHUNK_SIZE * a_old + b_chunk                     │
└────────────────────────────────────────────────────────────────┘
```

The weights ensure identical results to scalar:
- First 16 bytes: weights `[32, 31, 30, ..., 17]`
- Last 16 bytes: weights `[16, 15, 14, ..., 1]`

#### Portability

The binary is **fully portable** - a single build works on all machines:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Binary built on AVX-512 machine              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Running on AVX-512 CPU:  Uses AVX-512 path (~16x speedup)      │
│  Running on AVX2 CPU:     Uses AVX2 path (~8x speedup)          │
│  Running on old x86:      Uses scalar path (1x, still works!)  │
│  Running on ARM64:        Uses NEON path (~4x speedup)          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**How it works:**
1. SIMD functions use `#[target_feature(enable = "avx2")]` attributes
2. Compiler generates SIMD code for those specific functions only
3. Runtime detection with `is_x86_feature_detected!()` before calling
4. Fallback to scalar if no SIMD available

**DO NOT use `target-cpu=native`** - it makes the binary non-portable!
The `#[target_feature]` approach gives us both performance AND portability

### Layer 2: Why xxHash3 (Not BLAKE3 Sampling)?

| Approach | Speed (5MB) | Coverage | Security |
|----------|-------------|----------|----------|
| BLAKE3 sample 1.5KB | ~0.5μs | Gaps possible | Crypto |
| xxHash3 full block | ~0.17ms | 100% | Non-crypto |
| BLAKE3 full block | ~1.7ms | 100% | Crypto |

**Decision:** xxHash3 on full block. It's fast enough (~30 GB/s) to hash the entire block while providing 64-bit collision resistance. No sampling means no gaps where changes could be missed.

Why not crypto for Layer 2? Layer 3 (BLAKE3) provides cryptographic verification. Layer 2 only needs to filter false positives efficiently.

### Layer 3: Why BLAKE3?

| Algorithm | Speed | Security | Output |
|-----------|-------|----------|--------|
| BLAKE3 | ~3 GB/s | Crypto | 256-bit |
| SHA-256 | ~0.5 GB/s | Crypto | 256-bit |
| SHA-3 | ~0.3 GB/s | Crypto | 256-bit |

**Decision:** BLAKE3 is the fastest cryptographic hash, 6-10x faster than SHA-256/SHA-3.

## The Complete Delta Sync Process

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PHASE 1: SIGNATURE GENERATION                     │
│                    (done once per file, stored in .hssig)            │
└─────────────────────────────────────────────────────────────────────┘

Remote file on S3 (100MB):
┌────────┬────────┬────────┬────────┬─────────────┬────────┐
│Block 0 │Block 1 │Block 2 │Block 3 │     ...     │Block 19│
│  5MB   │  5MB   │  5MB   │  5MB   │             │  5MB   │
└────────┴────────┴────────┴────────┴─────────────┴────────┘
    │         │         │         │                   │
    ▼         ▼         ▼         ▼                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│ .hssig file (~1KB per 5MB of data):                                 │
│                                                                      │
│ Header: version=2, block_size=5MB, file_size=100MB                  │
│                                                                      │
│ Block 0: rolling=0x1A2B3C4D, xxhash3=0x..., blake3=0x...            │
│ Block 1: rolling=0x5E6F7A8B, xxhash3=0x..., blake3=0x...            │
│ Block 2: rolling=0x9C0D1E2F, xxhash3=0x..., blake3=0x...            │
│ ...                                                                  │
│ Block 19: rolling=0x3A4B5C6D, xxhash3=0x..., blake3=0x...           │
└─────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────┐
│                    PHASE 2: DELTA COMPUTATION                        │
│                    (on local machine before upload)                  │
└─────────────────────────────────────────────────────────────────────┘

Step 1: Download .hssig from S3 (~2KB for 100MB file - tiny!)

Step 2: Build hash index from signature
        HashMap<rolling_checksum, Vec<block_indices>>

Step 3: Scan local file with ROLLING window

Local file (with 100 bytes inserted at position 50MB):
┌────────────────────────┬──────┬────────────────────────┐
│      First 50MB        │ NEW  │      Last 50MB         │
│      (unchanged)       │100B  │   (shifted by 100B)    │
└────────────────────────┴──────┴────────────────────────┘

Scanning process:
┌─────────────────────────────────────────────────────────────────────┐
│ Position 0:                                                          │
│   window = bytes[0..5MB]                                            │
│   rolling = adler32(window) = 0x1A2B3C4D                            │
│   Lookup in index → FOUND candidates: [Block 0]                      │
│                                                                      │
│   For Block 0:                                                       │
│     xxhash3(window) == Block0.xxhash3? YES                          │
│     blake3(window) == Block0.blake3? YES                            │
│     → MATCH! Copy Block 0 from remote                               │
│                                                                      │
│ Position 5MB, 10MB, ..., 45MB: Same process, all match              │
│                                                                      │
│ Position 50MB:                                                       │
│   rolling = adler32(bytes[50MB..55MB]) = 0xNEW                      │
│   Lookup in index → NOT FOUND                                        │
│   → No match, this is new data                                       │
│                                                                      │
│ Position 50MB+1, 50MB+2, ...: Rolling forward byte by byte          │
│   rolling = roll(old_rolling, old_byte, new_byte) ← O(1)!           │
│                                                                      │
│ Position 50MB+100:                                                   │
│   rolling matches Block 10!                                          │
│   xxhash3 matches! blake3 matches!                                   │
│   → MATCH! Copy Block 10 from remote                                │
│                                                                      │
│ ... continue until end of file                                       │
└─────────────────────────────────────────────────────────────────────┘

Step 4: Generate delta operations
┌─────────────────────────────────────────────────────────────────────┐
│ Delta:                                                               │
│   COPY Block 0 from remote (offset 0, length 5MB)                   │
│   COPY Block 1 from remote (offset 5MB, length 5MB)                 │
│   ...                                                                │
│   COPY Block 9 from remote (offset 45MB, length 5MB)                │
│   INSERT bytes[50MB..50MB+100] (the 100 new bytes)                  │
│   COPY Block 10 from remote (offset 50MB, length 5MB)               │
│   ...                                                                │
│   COPY Block 19 from remote (offset 95MB, length 5MB)               │
└─────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────┐
│                    PHASE 3: DELTA UPLOAD TO S3                       │
│                    (using multipart with UploadPartCopy)             │
└─────────────────────────────────────────────────────────────────────┘

S3 Multipart Upload:
┌─────────────────────────────────────────────────────────────────────┐
│ Part 1: UploadPartCopy (Block 0-9, 50MB, server-side copy)          │
│         → No data transfer, S3 copies internally                    │
│                                                                      │
│ Part 2: UploadPart (100 new bytes)                                  │
│         → Only 100 bytes transferred!                                │
│                                                                      │
│ Part 3: UploadPartCopy (Block 10-19, 50MB, server-side copy)        │
│         → No data transfer, S3 copies internally                    │
│                                                                      │
│ CompleteMultipartUpload                                             │
│         → S3 assembles the parts into final object                  │
└─────────────────────────────────────────────────────────────────────┘

Result: 100 bytes transferred instead of 100MB = 99.9999% savings!
```

## Signature File Format (.hssig)

```
┌─────────────────────────────────────────────────────────────────────┐
│ Header                                                               │
├──────────────────┬──────────────────────────────────────────────────┤
│ Magic bytes      │ "HSSIG\x01" (6 bytes)                            │
│ Version          │ 2 (supports quick_hash)                          │
│ Block size       │ 5,242,880 (5MB default)                          │
│ File size        │ Total bytes of source file                       │
│ Source ETag      │ For staleness detection                          │
├──────────────────┴──────────────────────────────────────────────────┤
│ Blocks (one per block_size chunk)                                   │
├──────────────────┬──────────────────────────────────────────────────┤
│ Block 0          │ index, offset, size, rolling, quick_hash, strong │
│ Block 1          │ index, offset, size, rolling, quick_hash, strong │
│ ...              │ ...                                              │
└──────────────────┴──────────────────────────────────────────────────┘

Compressed with zstd for storage efficiency.
Typical size: ~100 bytes per block = ~2KB for 100MB file.
```

## Why This Design?

### vs. Fixed Block Hashing
- Fixed blocks fail on insertions/deletions (0% reuse)
- Rolling checksum finds matches at any offset (99%+ reuse)

### vs. Full File Diff (like git)
- Git diff is O(N*M) for N and M sized files
- Rolling checksum is O(N) with O(1) per-position check

### vs. Content-Defined Chunking (CDC)
- CDC (used by restic, borg) creates variable-size chunks
- Good for deduplication across files
- We use fixed blocks for simpler S3 multipart alignment
- CDC could be a future enhancement

### vs. rsync
- rsync uses same algorithm but requires server-side rsync daemon
- HugeSync works with any S3-compatible storage
- Uses cloud-native operations (UploadPartCopy) for server-side assembly

## Performance Characteristics

| File Size | Signature Size | Delta Compute | Upload (1% change) |
|-----------|---------------|---------------|-------------------|
| 100MB | ~2KB | ~50ms | ~1MB transferred |
| 1GB | ~20KB | ~500ms | ~10MB transferred |
| 10GB | ~200KB | ~5s | ~100MB transferred |

## References

- [rsync algorithm](https://rsync.samba.org/tech_report/)
- [xxHash](https://xxhash.com/) - Extremely fast non-cryptographic hash
- [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) - Fast cryptographic hash
- [FastCDC](https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia) - Content-defined chunking
- [S3 Multipart Upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
