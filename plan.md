# HugeSync: A Cloud-First Rust-Powered Delta Synchronization Tool

HugeSync is a fast, secure, cross-platform CLI tool built in Rust that revolutionizes file synchronization. It targets massive binaries (VMs, databases, ML models) stored on immutable object storage (S3), filling the gap between rsync (efficient deltas, but no cloud support) and rclone (cloud support, but inefficient full-file re-uploads).

## The Core Innovation: "Sidecar Stitching"

Standard tools re-upload 100GB files even if only 1MB changed. HugeSync solves this without requiring a server-side daemon:

- **Sidecar Signatures**: For files >50MB, HugeSync generates a tiny `.hssig` file (BLAKE3 rolling hashes).
- **Local Diffing**: The client downloads the remote `.hssig`, compares it to the local file, and identifies changed blocks.
- **S3-Aware Coalescing**: It ensures changed chunks align with S3's 5MB minimum multipart limit.
- **Server-Side Assembly**: It uploads only the new chunks and uses `UploadPartCopy` to stitch the rest from the existing remote object.

Result: 99% bandwidth savings and zero egress fees for the sync.

## Tech Stack & Crates (Expanded & Production-Ready)

We are moving from a prototype list to a production-grade manifest.

### Core Runtime & Async

- **tokio**: (features = ["full"]) The industry-standard async runtime.
- **futures**: For stream combinators and sinking.
- **num_cpus**: To auto-configure thread pools and parallelism.

### Filesystem & Walking (Performance Upgrade)

- **jwalk**: (Replaces `walkdir`). Performs parallelism-based directory walking. Crucial for "millions of small files" performance, offering significant speedups over single-threaded walkers on SSDs/NVMe.
- **tokio-util**: For Codec framing when reading files.
- **memmap2**: For memory-mapping huge files during signature computation (avoids excessive copying).
- **ignore**: To respect `.gitignore` and `.dockerignore` files (standard dev expectation).

### Cloud & Network

- **object_store**: The unified API for S3, GCS, Azure, and HTTP.
- **aws-sdk-s3**: Required alongside `object_store` for low-level access to `UploadPartCopy` and presigned URLs, which abstract layers often hide.
- **aws-config**: Standard chain credential loading.
- **reqwest**: Robust HTTP client for non-S3 interactions.
- **url**: Strict URI parsing (`s3://`, `ssh://`).

### Delta & Cryptography

- **fast_rsync**: SIMD-accelerated rolling checksums.
- **blake3**: Much faster than SHA256 for generating file IDs and signature hashes.
- **hex** / **base64**: Encoding helpers.

### CLI & UX

- **clap**: (features = ["derive", "env"]) Argument parsing.
- **indicatif**: Multi-bar progress rendering (overall progress + current file + delta savings).
- **console**: Terminal manipulation.
- **dialoguer**: For interactive conflict resolution prompts (optional).
- **human-bytes**: For formatting sizes (e.g., "100 GiB").

### Observability & Error Handling

- **tracing** & **tracing-subscriber**: Structured logging (JSON for machines, pretty for humans). Replaces standard `log`.
- **anyhow**: Application-level error handling.
- **thiserror**: Library-level typed errors (crucial for distinguishing "Network Timeout" vs "Access Denied").

### Configuration & Serialization

- **serde**, **serde_json**, **toml**: Config file parsing.
- **dirs**: Cross-platform path resolution (`~/.config/hugesync`).

## Detailed Feature Specification

### 1. Performance & Scalability

- **Parallel Scanning (jwalk)**: Unlike rsync's serial scanning, HugeSync uses `jwalk` to index file trees in parallel, utilizing all CPU cores. This prevents the "hang" before transfer starts on directories with millions of files.
- **Concurrency Control**: `--jobs=N` controls active transfers.
- **Small File Optimization**: Files <50MB bypass the delta engine entirely. They are batched for standard `PutObject` to reduce API request overhead.

### 2. The "Huge" File Engine (Delta Sync)

- **Thresholding**: Configurable via `--delta-threshold` (Default: 50MB).
- **Safety**: Explicitly handles the S3 5MB Limit. If a change is 1KB, HugeSync reads adjacent data to create a valid 5MB S3 part, ensuring the API call doesn't fail.
- **Conflict Resolution**: Uses `If-Match` with ETags. If the remote object changes during diff calculation, the sync aborts or retries based on policy.

### 3. Rsync-Compatible Usability

- **Recursive (-a)**: Preserves metadata. On S3, this maps POSIX permissions to User Metadata headers (`x-amz-meta-mode`).
- **Filtering**: Full support for include/exclude patterns.
- **Delete Safeguards**:
    - `--delete`: Removes extraneous remote files.
    - `--delete-missing-args`: Prevents wiping the bucket if the source path is typo'd.
    - `--skip-vanished`: If a file disappears during the `jwalk` scan (common with temp files), log it and move on rather than crashing.
- **Dry Run (-n)**: Performs the scan and delta calculation but skips the upload. Critical Feature: It reports "Estimated Bandwidth Savings" (e.g., "Would transfer 50MB instead of 100GB").

### 4. Security & Reliability

- **Memory Safety**: Rust prevents the buffer overflow vulnerabilities recently found in rsync (CVE-202X).
- **No "Server" Required**: Unlike rsync daemon, this works with standard cloud credentials.
- **Resumability**: Tracks multipart upload IDs. If the network drops at 90% of a 1TB upload, HugeSync resumes from the last successful 5MB part.

## Roadmap (Revised for Feasibility)

### Phase 1: The Cloud Delta MVP (4 Weeks)

- **Focus**: Local Filesystem ↔ AWS S3.
- **Core**: Implement `jwalk` scanning and the `UploadPartCopy` logic.
- **Goal**: Demonstrate the bandwidth savings on a 100GB file.
- **Constraint**: No SSH, no GCS yet.
- **CLI**:
  ```bash
  hugesync sync ./local_dir s3://my-bucket/ --delta-threshold 50MB
  ```

### Phase 2: The Production Sync (4-6 Weeks)

- **Expansion**: Add GCS and Azure (using Block Lists).
- **Compression**: Add `-z` (Gzip). Note: Requires metadata to mark object as `Content-Encoding: gzip`.
- **Versioning**: Integration with S3 Object Versioning.
- **Delete**: Implement the safe delete logic.

### Phase 3: SSH & Advanced (Future)

- **SSH Mode**: Shelling out to system SSH to pipe binary data (rsync replacement mode).
- **Mount**: (Low priority) FUSE interface.

## CLI Design

```text
hugesync 0.1.0
A Cloud-First Rust-Powered Delta Synchronization Tool

USAGE:
    hugesync sync [OPTIONS] <SOURCE> <DESTINATION>

ARGS:
    <SOURCE>         Local path or URI
    <DESTINATION>    Local path or URI (s3://, gs://, ssh://)

OPTIONS:
    -a, --archive            Archive mode (recursive, preserve attrs)
    -n, --dry-run            Perform a trial run with no changes made
    -v, --verbose            Increase logging verbosity
    -P, --progress           Show progress bars
    -j, --jobs <N>           Number of parallel transfers [default: auto]
    
    --delete                 Delete extraneous files from dest dirs
    --delete-excluded        Delete files on dest that are excluded from sync
    --ignore-missing-args    Ignore missing source args without error
    
    --delta-threshold <MB>   File size to trigger delta sync [default: 50]
    --block-size <KB>        Block size for delta signatures (64-5120) [default: 5120]
    --no-sidecar             Disable .hssig generation (always full upload)
    --fail-on-conflict       Abort if remote file was modified externally
```
## Challenges & Solutions

- **Challenge**: S3 API costs for sidecars.
  - **Solution**: Strict 50MB threshold. 99% of files are usually small and don't need deltas.

- **Challenge**: Sidecar desynchronization.
  - **Solution**: Sidecars contain the ETag of the master file. If they don't match, the sidecar is considered stale and regenerated.

This plan restores the "HugeSync" vision to its full scope while upgrading the technical architecture to be performant and production-ready.