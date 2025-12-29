//! .hssig file format reading and writing

use super::{Signature, SIGNATURE_MAGIC, SIGNATURE_VERSION};
use crate::error::{Error, Result};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// Write a signature to a file
pub fn write_signature(sig: &Signature, path: &Path) -> Result<()> {
    let file = File::create(path).map_err(|e| Error::io("creating signature file", e))?;
    let mut writer = BufWriter::new(file);

    // Write magic and version
    writer
        .write_all(SIGNATURE_MAGIC)
        .map_err(|e| Error::io("writing magic", e))?;
    writer
        .write_all(&[SIGNATURE_VERSION])
        .map_err(|e| Error::io("writing version", e))?;

    // Serialize to JSON (simple format, can optimize later)
    let json = serde_json::to_vec(sig).map_err(|e| Error::Delta {
        message: format!("serializing signature: {}", e),
    })?;

    // Write length then data
    let len = json.len() as u64;
    writer
        .write_all(&len.to_le_bytes())
        .map_err(|e| Error::io("writing length", e))?;
    writer
        .write_all(&json)
        .map_err(|e| Error::io("writing data", e))?;

    writer.flush().map_err(|e| Error::io("flushing", e))?;

    Ok(())
}

/// Read a signature from a file
pub fn read_signature(path: &Path) -> Result<Signature> {
    let file = File::open(path).map_err(|e| Error::io("opening signature file", e))?;
    let mut reader = BufReader::new(file);

    // Read and verify magic
    let mut magic = [0u8; 6];
    reader
        .read_exact(&mut magic)
        .map_err(|e| Error::io("reading magic", e))?;

    if &magic != SIGNATURE_MAGIC {
        return Err(Error::Delta {
            message: "invalid signature file (bad magic)".to_string(),
        });
    }

    // Read and verify version
    let mut version = [0u8; 1];
    reader
        .read_exact(&mut version)
        .map_err(|e| Error::io("reading version", e))?;

    if version[0] != SIGNATURE_VERSION {
        return Err(Error::Delta {
            message: format!(
                "unsupported signature version {} (expected {})",
                version[0], SIGNATURE_VERSION
            ),
        });
    }

    // Read length
    let mut len_bytes = [0u8; 8];
    reader
        .read_exact(&mut len_bytes)
        .map_err(|e| Error::io("reading length", e))?;
    let len = u64::from_le_bytes(len_bytes) as usize;

    // Read data
    let mut data = vec![0u8; len];
    reader
        .read_exact(&mut data)
        .map_err(|e| Error::io("reading data", e))?;

    // Deserialize
    let sig: Signature = serde_json::from_slice(&data).map_err(|e| Error::Delta {
        message: format!("deserializing signature: {}", e),
    })?;

    Ok(sig)
}

/// Read a signature from bytes
pub fn read_signature_from_bytes(data: &[u8]) -> Result<Signature> {
    if data.len() < 15 {
        return Err(Error::Delta {
            message: "signature data too short".to_string(),
        });
    }

    // Verify magic
    if &data[0..6] != SIGNATURE_MAGIC {
        return Err(Error::Delta {
            message: "invalid signature (bad magic)".to_string(),
        });
    }

    // Verify version
    if data[6] != SIGNATURE_VERSION {
        return Err(Error::Delta {
            message: format!(
                "unsupported signature version {} (expected {})",
                data[6], SIGNATURE_VERSION
            ),
        });
    }

    // Read length
    let len = u64::from_le_bytes(data[7..15].try_into().unwrap()) as usize;

    if data.len() < 15 + len {
        return Err(Error::Delta {
            message: "signature data truncated".to_string(),
        });
    }

    // Deserialize
    let sig: Signature = serde_json::from_slice(&data[15..15 + len]).map_err(|e| Error::Delta {
        message: format!("deserializing signature: {}", e),
    })?;

    Ok(sig)
}

/// Write a signature to bytes
pub fn write_signature_to_bytes(sig: &Signature) -> Result<Vec<u8>> {
    let json = serde_json::to_vec(sig).map_err(|e| Error::Delta {
        message: format!("serializing signature: {}", e),
    })?;

    let len = json.len() as u64;
    let mut data = Vec::with_capacity(7 + 8 + json.len());
    data.extend_from_slice(SIGNATURE_MAGIC);
    data.push(SIGNATURE_VERSION);
    data.extend_from_slice(&len.to_le_bytes());
    data.extend_from_slice(&json);

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signature::generate::generate_signature_from_bytes;
    use tempfile::NamedTempFile;

    #[test]
    fn test_roundtrip_file() {
        let sig = generate_signature_from_bytes(b"hello world test data", 8);

        let file = NamedTempFile::new().unwrap();
        write_signature(&sig, file.path()).unwrap();

        let loaded = read_signature(file.path()).unwrap();
        assert_eq!(loaded.file_size, sig.file_size);
        assert_eq!(loaded.blocks.len(), sig.blocks.len());
    }

    #[test]
    fn test_roundtrip_bytes() {
        let sig = generate_signature_from_bytes(b"test content", 4);

        let bytes = write_signature_to_bytes(&sig).unwrap();
        let loaded = read_signature_from_bytes(&bytes).unwrap();

        assert_eq!(loaded.file_size, sig.file_size);
        assert_eq!(loaded.block_size, sig.block_size);
        assert_eq!(loaded.blocks.len(), sig.blocks.len());
    }

    #[test]
    fn test_invalid_magic() {
        let data = b"BADMAG\x01";
        let result = read_signature_from_bytes(data);
        assert!(result.is_err());
    }
}
