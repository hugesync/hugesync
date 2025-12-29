# HugeSync

A cloud-first delta synchronization tool built in Rust. Efficiently syncs massive files (VMs, databases, ML models) to S3/GCS/Azure by uploading only changed blocks—not the entire file.

**Key feature:** "Sidecar Stitching" uses rolling checksums and S3's `UploadPartCopy` to achieve 99% bandwidth savings on large file updates.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
