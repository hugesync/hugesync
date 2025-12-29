# HugeSync

A cloud-era delta synchronization tool built in Rust. HugeSync efficiently syncs large files between local filesystems and cloud object storage using delta transfers.

## Features

- **Delta Sync**: Only transfer changed blocks, not entire files
- **Cloud Storage**: Native S3 support with UploadPartCopy for server-side stitching
- **Parallel Scanning**: Uses `jwalk` for fast parallel directory traversal
- **Progress Tracking**: Rich progress bars with transfer rates and savings
- **Resume Support**: Interrupted uploads can be resumed
- **Configurable Block Size**: 64KB to 5MB blocks (default: 5MB for S3 alignment)

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

### Sync with progress bars

```bash
hsync sync -P ./data s3://bucket/data/
```

### Dry run (see what would be done)

```bash
hsync sync -n ./data s3://bucket/data/
```

### Delete extraneous files from destination

```bash
hsync sync --delete ./source s3://bucket/
```

### Generate signature for a file

```bash
hsync sign large-file.bin
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
    help    Print this message or the help of the given subcommand(s)

OPTIONS:
    -v, --verbose...  Increase logging verbosity (-v, -vv, -vvv)
        --json        Output logs as JSON
    -h, --help        Print help
    -V, --version     Print version
```

### Sync Options

```
hsync sync [OPTIONS] <SOURCE> <DESTINATION>

ARGS:
    <SOURCE>         Local path or URI (s3://, gs://, az://)
    <DESTINATION>    Local path or URI

OPTIONS:
    -a, --archive              Archive mode (recursive, preserve attrs)
    -n, --dry-run              Perform a trial run with no changes made
    -P, --progress             Show progress bars
    -j, --jobs <N>             Number of parallel transfers [default: auto]
        --delete               Delete extraneous files from dest
        --delta-threshold <MB> File size to trigger delta sync [default: 50]
        --block-size <KB>      Block size for signatures (64-5120) [default: 5120]
        --no-sidecar           Disable .hssig generation
        --fail-on-conflict     Abort if remote file was modified
```

## Configuration

HugeSync loads configuration from `~/.config/hugesync/config.toml`:

```toml
# Default settings
delta_threshold = 52428800  # 50MB
jobs = 0  # 0 = auto-detect CPUs
dry_run = false
delete = false
progress = true
block_size = 5242880  # 5MB
no_sidecar = false
max_retries = 3
```

### Initialize config

```bash
hsync config --init
```

### Show config path

```bash
hsync config --path
```

## How It Works

### The Sidecar Stitching Approach

1. **Signature Generation**: For large files (>50MB by default), HugeSync generates a `.hssig` sidecar file containing block checksums (rolling + BLAKE3).

2. **Local Diffing**: When syncing, HSyncs downloads the remote `.hssig`, compares blocks locally, and identifies only the changed blocks.

3. **Delta Upload**: Uses S3's `UploadPartCopy` to stitch unchanged blocks from the existing remote object with newly uploaded changed blocks.

**Result**: 99%+ bandwidth savings for incremental updates to large files.

## Performance Tips

1. **Block Size**: Use larger blocks (5MB) for fewer API calls, smaller blocks (64KB) for finer granularity
2. **Parallel Jobs**: Default auto-detection usually works best, but you can tune with `-j`
3. **Delta Threshold**: Lower it for more delta sync opportunities, raise it to reduce overhead

## License

Dual-licensed under MIT or Apache-2.0, at your option.

## Contributing

Contributions welcome! Please open an issue first to discuss major changes.
