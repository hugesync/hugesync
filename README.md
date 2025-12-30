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

## Cloud Authentication

HugeSync automatically detects credentials from your environment.

### AWS S3
Set standard AWS environment variables:
```bash
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_REGION="us-east-1"
```

### Google Cloud Storage (GCP)
Use Application Default Credentials (ADC) via JSON key file:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

### Azure Blob Storage
Set storage account name and access key:
```bash
export AZURE_STORAGE_ACCOUNT="your_storage_account"
export AZURE_STORAGE_KEY="your_access_key"
```

### SSH/SFTP
Uses your standard SSH configuration:
- Parses `~/.ssh/config`
- Uses running `ssh-agent`
- Tries default keys (id_rsa, id_ed25519)

## S3-Compatible Providers

HugeSync supports many S3-compatible storage providers with easy shorthand syntax.

### Using Provider Shorthand

Instead of typing full endpoint URLs, use `--s3-provider` and `--s3-region`:

```bash
# Hetzner Object Storage (Helsinki)
hsync sync ./data s3://bucket/path --s3-provider hetzner --s3-region hel1

# DigitalOcean Spaces (NYC)
hsync sync ./data s3://bucket/path --s3-provider do --s3-region nyc3

# Backblaze B2 (EU)
hsync sync ./data s3://bucket/path --s3-provider b2 --s3-region eu-central-003

# Wasabi (Frankfurt)
hsync sync ./data s3://bucket/path --s3-provider wasabi --s3-region eu-central-1
```

### Supported Providers and Regions

Run `hsync providers` to see all supported providers:

| Provider | Alias | Regions |
|----------|-------|---------|
| Hetzner | `hetzner`, `htz` | fsn1, nbg1, hel1 |
| DigitalOcean | `digitalocean`, `do`, `spaces` | nyc3, sfo3, ams3, sgp1, fra1, syd1 |
| Backblaze B2 | `backblaze`, `b2` | us-west-000, us-west-001, us-west-002, us-west-004, eu-central-003 |
| Wasabi | `wasabi` | us-east-1, us-east-2, us-central-1, us-west-1, eu-central-1, eu-central-2, eu-west-1, eu-west-2, ap-northeast-1, ap-northeast-2, ap-southeast-1, ap-southeast-2, ca-central-1 |
| Vultr | `vultr` | ewr1, ord1, dfw1, sea1, lax1, atl1, ams1, lhr1, fra1, sjc1, syd1, jnb1, sgp1, nrt1, blr1, del1 |
| Linode | `linode`, `akamai` | us-east-1, us-southeast-1, us-ord-1, eu-central-1, ap-south-1, us-iad-1, fr-par-1, se-sto-1, in-maa-1, jp-osa-1, it-mil-1, us-lax-1, us-mia-1, id-cgk-1, br-gru-1 |
| Scaleway | `scaleway`, `scw` | fr-par, nl-ams, pl-waw |
| OVHcloud | `ovh`, `ovhcloud` | gra, sbg, bhs, de, uk, waw, rbx-hdd, rbx-ssd |
| Exoscale | `exoscale`, `exo` | ch-gva-2, ch-dk-2, de-fra-1, de-muc-1, at-vie-1, at-vie-2, bg-sof-1 |
| IDrive e2 | `idrive`, `e2` | us-or, us-va, us-la, us-phx, us-dal, eu-fra, eu-lon, eu-par, ap-sin |
| Contabo | `contabo` | eu, us-central, sin, aus, uk, jpn |

### Custom Endpoints

For providers not in the list or self-hosted solutions:

```bash
# Cloudflare R2 (requires account ID)
hsync sync ./data s3://bucket/path --s3-endpoint https://ACCOUNT_ID.r2.cloudflarestorage.com

# MinIO self-hosted
hsync sync ./data s3://bucket/path --s3-endpoint http://localhost:9000

# Any S3-compatible endpoint
hsync sync ./data s3://bucket/path --s3-endpoint https://s3.custom-provider.com
```

## AWS S3 Storage Classes

For AWS S3 destinations, you can specify storage classes to reduce costs:

```bash
# Standard (default)
hsync sync ./data s3://bucket/backup/

# Infrequent Access (40% cheaper, retrieval fees apply)
hsync sync ./data s3://bucket/backup/ --storage-class STANDARD_IA

# Glacier Instant Retrieval (68% cheaper)
hsync sync ./data s3://bucket/backup/ --storage-class GLACIER_IR

# Glacier Flexible Retrieval (for archives, minutes-hours retrieval)
hsync sync ./data s3://bucket/backup/ --storage-class GLACIER

# Deep Archive (cheapest, 12-48 hour retrieval)
hsync sync ./data s3://bucket/backup/ --storage-class DEEP_ARCHIVE
```

**Note**: Storage classes only work with AWS S3, not S3-compatible providers. If you specify `--storage-class` with `--s3-provider` or `--s3-endpoint`, it will be ignored with a warning.

| Storage Class | Cost Savings | Retrieval Time | Best For |
|---------------|--------------|----------------|----------|
| STANDARD | - | Instant | Frequently accessed |
| STANDARD_IA | ~40% | Instant | Monthly access |
| ONEZONE_IA | ~50% | Instant | Reproducible data |
| GLACIER_IR | ~68% | Instant | Quarterly access |
| GLACIER | ~78% | 1-12 hours | Archives |
| DEEP_ARCHIVE | ~95% | 12-48 hours | Long-term archives |

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
    sync       Synchronize files between source and destination
    sign       Generate signature file for a large file
    config     Show configuration
    providers  List supported S3-compatible providers and regions
    help       Print this message

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

CLOUD STORAGE:
        --storage-class=CLASS  AWS S3 storage class (STANDARD, STANDARD_IA,
                               GLACIER, GLACIER_IR, DEEP_ARCHIVE)
        --s3-endpoint=URL      Custom S3-compatible endpoint URL
        --s3-provider=NAME     S3 provider shorthand (hetzner, do, b2, wasabi...)
        --s3-region=REGION     Region for the S3 provider
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
