# HugeSync

A cloud-era delta synchronization tool built in Rust. HugeSync efficiently syncs large files between local filesystems and cloud object storage using delta transfers.

## Features

- **Delta Sync**: Only transfer changed blocks, not entire files
- **Multi-Cloud Storage**: S3, GCS, Azure Blob Storage, SSH/SFTP
- **rsync-Compatible**: Familiar options like `-a`, `-n`, `-c`, `--delete`, `--bwlimit`
- **Parallel Scanning**: Uses `jwalk` for fast parallel directory traversal
- **Memory Efficient**: Streaming transfers avoid OOM for large files
- **Progress Tracking**: Rich progress bars with transfer rates and savings
- **Resume Support**: Interrupted uploads can be resumed
- **Configurable Block Size**: 64KB to 5MB blocks (default: 5MB for S3 alignment)

## Supported Backends

| Backend | URI Format | Delta Sync | Server-Side Copy |
|---------|------------|------------|------------------|
| Local   | `/path/to/dir` | ✅ | N/A |
| AWS S3  | `s3://bucket/prefix` | ✅ | ✅ UploadPartCopy |
| GCS     | `gs://bucket/prefix` | ✅ | ✅ Compose |
| Azure   | `az://container/prefix` | ✅ | ✅ Block List |
| SSH     | `user@host:/path` | ✅ | ❌ (full transfer) |

## Installation

```bash
cargo install hugesync
```

Or build from source:

```bash
git clone https://github.com/hugesync/hugesync
cd hugesync
cargo build --release
```

The binary will be at `target/release/hsync`.

## Quick Start

### Sync local directory to S3

```bash
hsync sync ./local-folder s3://my-bucket/backup/
```

### Sync to Google Cloud Storage

```bash
hsync sync ./data gs://my-bucket/data/
```

### Sync to Azure Blob Storage

```bash
hsync sync ./data az://my-container/data/
```

### Sync via SSH

```bash
hsync sync ./data user@remote-host:/backup/data/
```

### Dry run (see what would be done)

```bash
hsync sync -n ./data s3://bucket/data/
```

### Delete extraneous files from destination

```bash
hsync sync --delete ./source s3://bucket/
```

### Bandwidth limiting

```bash
hsync sync --bwlimit=1000 ./data s3://bucket/  # 1000 KiB/s
```

## CLI Reference

```
hsync 0.0.1
A Cloud-Era Delta Synchronization Tool

USAGE:
    hsync [OPTIONS] <COMMAND>

COMMANDS:
    sync    Synchronize files between source and destination
    sign    Generate signature file for a large file
    config  Show configuration
    help    Print this message

OPTIONS:
    -v, --verbose...  Increase logging verbosity
        --json        Output logs as JSON
    -h, --help        Print help
    -V, --version     Print version
```

### Sync Options

```
hsync sync [OPTIONS] <SOURCE> <DESTINATION>

BASIC:
    -a, --archive              Archive mode (recursive, preserve attrs)
    -n, --dry-run              Perform trial run with no changes
    -P, --progress             Show progress bars
    -j, --jobs <N>             Parallel transfers [default: auto]
        --delete               Delete extraneous files from dest

COMPARISON (rsync-compatible):
    -c, --checksum             Skip based on checksum, not mtime+size
    -u, --update               Skip files newer on receiver
        --size-only            Compare by size only
        --ignore-existing      Skip files that exist on receiver
        --existing             Only update existing files
        --modify-window=NUM    Compare mod times with tolerance

SIZE FILTERING:
        --max-size=SIZE        Don't transfer files larger than SIZE
        --min-size=SIZE        Don't transfer files smaller than SIZE

BACKUP:
    -b, --backup               Make backups of changed files
        --backup-dir=DIR       Store backups in directory
        --suffix=SUFFIX        Backup suffix [default: ~]

PERFORMANCE:
        --bwlimit=RATE         Limit bandwidth in KiB/s
    -W, --whole-file           Disable delta-transfer

OUTPUT:
    -i, --itemize-changes      Output change summary for updates
        --stats                Show transfer statistics at end
        --log-file=FILE        Log transfers to file

DELTA SYNC:
        --delta-threshold=MB   File size for delta sync [default: 50]
        --block-size=KB        Block size (64-5120) [default: 5120]
        --no-sidecar           Disable .hssig generation
        --fail-on-conflict     Abort if remote was modified

FILTERING:
        --include=PATTERN      Include files matching pattern
        --exclude=PATTERN      Exclude files matching pattern
```

## Configuration

HugeSync loads configuration from `~/.config/hugesync/config.toml`:

```toml
delta_threshold = 52428800  # 50MB
jobs = 0                    # 0 = auto-detect CPUs
dry_run = false
delete = false
progress = true
block_size = 5242880        # 5MB
no_sidecar = false
max_retries = 3
bwlimit = 0                 # 0 = unlimited
```

### Initialize config

```bash
hsync config --init
```

## How It Works

### The Sidecar Stitching Approach

1. **Signature Generation**: For large files (>50MB by default), HugeSync generates a `.hssig` sidecar file containing block checksums (rolling + BLAKE3).

2. **Local Diffing**: When syncing, downloads the remote `.hssig`, compares blocks locally, and identifies changed blocks.

3. **Delta Upload (Local → Cloud)**: Uses cloud-native operations to stitch unchanged blocks from existing remote object with newly uploaded changed blocks:
   - S3: `UploadPartCopy`
   - GCS: `Compose`
   - Azure: Block List

4. **Delta Download (Cloud → Local)**:
   - Generates signature of local file
   - Compares with remote `.hssig`
   - Fetches only changed blocks via HTTP Range requests
   - Reconstructs file atomically from local blocks + remote ranges

**Result**: 99%+ bandwidth savings for incremental updates (upload AND download) to large files.

## Memory Efficiency

HugeSync uses streaming transfers to handle files of any size:

- **S3**: `AsyncRead` + `ReaderStream` for on-demand chunks
- **Local**: Memory-mapped files with lazy chunk copying
- **GCS**: Channel bridge for async stream conversion
- **SSH**: Incremental SFTP reads with seek/read

Files are never fully loaded into memory, preventing OOM for large transfers.

## Performance Tips

1. **Block Size**: Use larger blocks (5MB) for fewer API calls, smaller (64KB) for finer deltas
2. **Parallel Jobs**: Default auto-detection usually optimal, tune with `-j`
3. **Delta Threshold**: Lower for more delta opportunities, raise to reduce overhead
4. **Bandwidth Limit**: Use `--bwlimit` when sharing network resources

## License

Dual-licensed under MIT or Apache-2.0, at your option.

## Contributing

Contributions welcome! Please open an issue first to discuss major changes.
