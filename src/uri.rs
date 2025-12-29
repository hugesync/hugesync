//! URI parsing for sync locations

use crate::error::{Error, Result};
use std::path::PathBuf;
use url::Url;

/// A sync location - either local filesystem or cloud storage
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Location {
    /// Local filesystem path
    Local(PathBuf),

    /// AWS S3 bucket and prefix
    S3 { bucket: String, prefix: String },

    /// Google Cloud Storage bucket and prefix
    Gcs { bucket: String, prefix: String },

    /// Azure Blob Storage container and prefix
    Azure { container: String, prefix: String },

    /// SSH remote (user@host:path)
    Ssh {
        user: Option<String>,
        host: String,
        path: String,
    },
}

impl Location {
    /// Parse a location string into a Location enum
    pub fn parse(s: &str) -> Result<Self> {
        // Check for SSH format first (user@host:path or host:path)
        if !s.contains("://") && s.contains(':') && !s.starts_with('/') {
            return Self::parse_ssh(s);
        }

        // Check for URI schemes
        if let Some(scheme) = s.split("://").next() {
            match scheme.to_lowercase().as_str() {
                "s3" => return Self::parse_s3(s),
                "gs" => return Self::parse_gcs(s),
                "az" | "azure" => return Self::parse_azure(s),
                "file" => {
                    let path = s.strip_prefix("file://").unwrap_or(s);
                    return Ok(Location::Local(PathBuf::from(path)));
                }
                _ => {}
            }
        }

        // Default to local path
        Ok(Location::Local(PathBuf::from(s)))
    }

    fn parse_s3(s: &str) -> Result<Self> {
        let url = Url::parse(s).map_err(|e| Error::InvalidUri {
            uri: s.to_string(),
            reason: e.to_string(),
        })?;

        let bucket = url
            .host_str()
            .ok_or_else(|| Error::InvalidUri {
                uri: s.to_string(),
                reason: "missing bucket name".to_string(),
            })?
            .to_string();

        let prefix = url.path().trim_start_matches('/').to_string();

        Ok(Location::S3 { bucket, prefix })
    }

    fn parse_gcs(s: &str) -> Result<Self> {
        let url = Url::parse(s).map_err(|e| Error::InvalidUri {
            uri: s.to_string(),
            reason: e.to_string(),
        })?;

        let bucket = url
            .host_str()
            .ok_or_else(|| Error::InvalidUri {
                uri: s.to_string(),
                reason: "missing bucket name".to_string(),
            })?
            .to_string();

        let prefix = url.path().trim_start_matches('/').to_string();

        Ok(Location::Gcs { bucket, prefix })
    }

    fn parse_azure(s: &str) -> Result<Self> {
        let url = Url::parse(s).map_err(|e| Error::InvalidUri {
            uri: s.to_string(),
            reason: e.to_string(),
        })?;

        let container = url
            .host_str()
            .ok_or_else(|| Error::InvalidUri {
                uri: s.to_string(),
                reason: "missing container name".to_string(),
            })?
            .to_string();

        let prefix = url.path().trim_start_matches('/').to_string();

        Ok(Location::Azure { container, prefix })
    }

    fn parse_ssh(s: &str) -> Result<Self> {
        // Format: [user@]host:path
        let (user_host, path) = s.split_once(':').ok_or_else(|| Error::InvalidUri {
            uri: s.to_string(),
            reason: "invalid SSH format, expected [user@]host:path".to_string(),
        })?;

        let (user, host) = if user_host.contains('@') {
            let parts: Vec<&str> = user_host.splitn(2, '@').collect();
            (Some(parts[0].to_string()), parts[1].to_string())
        } else {
            (None, user_host.to_string())
        };

        Ok(Location::Ssh {
            user,
            host,
            path: path.to_string(),
        })
    }

    /// Check if this location is local
    pub fn is_local(&self) -> bool {
        matches!(self, Location::Local(_))
    }

    /// Check if this location is remote (cloud or SSH)
    pub fn is_remote(&self) -> bool {
        !self.is_local()
    }

    /// Get the scheme/protocol name
    pub fn scheme(&self) -> &'static str {
        match self {
            Location::Local(_) => "file",
            Location::S3 { .. } => "s3",
            Location::Gcs { .. } => "gs",
            Location::Azure { .. } => "az",
            Location::Ssh { .. } => "ssh",
        }
    }

    /// Convert back to a URI string
    pub fn to_uri(&self) -> String {
        match self {
            Location::Local(path) => path.display().to_string(),
            Location::S3 { bucket, prefix } => {
                if prefix.is_empty() {
                    format!("s3://{}", bucket)
                } else {
                    format!("s3://{}/{}", bucket, prefix)
                }
            }
            Location::Gcs { bucket, prefix } => {
                if prefix.is_empty() {
                    format!("gs://{}", bucket)
                } else {
                    format!("gs://{}/{}", bucket, prefix)
                }
            }
            Location::Azure { container, prefix } => {
                if prefix.is_empty() {
                    format!("az://{}", container)
                } else {
                    format!("az://{}/{}", container, prefix)
                }
            }
            Location::Ssh { user, host, path } => {
                if let Some(u) = user {
                    format!("{}@{}:{}", u, host, path)
                } else {
                    format!("{}:{}", host, path)
                }
            }
        }
    }
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_uri())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_local() {
        let loc = Location::parse("/path/to/dir").unwrap();
        assert_eq!(loc, Location::Local(PathBuf::from("/path/to/dir")));

        let loc = Location::parse("./relative/path").unwrap();
        assert_eq!(loc, Location::Local(PathBuf::from("./relative/path")));
    }

    #[test]
    fn test_parse_s3() {
        let loc = Location::parse("s3://my-bucket/path/to/data").unwrap();
        assert_eq!(
            loc,
            Location::S3 {
                bucket: "my-bucket".to_string(),
                prefix: "path/to/data".to_string(),
            }
        );

        let loc = Location::parse("s3://bucket-only").unwrap();
        assert_eq!(
            loc,
            Location::S3 {
                bucket: "bucket-only".to_string(),
                prefix: "".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_gcs() {
        let loc = Location::parse("gs://my-bucket/prefix").unwrap();
        assert_eq!(
            loc,
            Location::Gcs {
                bucket: "my-bucket".to_string(),
                prefix: "prefix".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_azure() {
        let loc = Location::parse("az://container/blob").unwrap();
        assert_eq!(
            loc,
            Location::Azure {
                container: "container".to_string(),
                prefix: "blob".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_ssh() {
        let loc = Location::parse("user@host:/path/to/data").unwrap();
        assert_eq!(
            loc,
            Location::Ssh {
                user: Some("user".to_string()),
                host: "host".to_string(),
                path: "/path/to/data".to_string(),
            }
        );

        let loc = Location::parse("server:/data").unwrap();
        assert_eq!(
            loc,
            Location::Ssh {
                user: None,
                host: "server".to_string(),
                path: "/data".to_string(),
            }
        );
    }

    #[test]
    fn test_to_uri() {
        let loc = Location::S3 {
            bucket: "bucket".to_string(),
            prefix: "path".to_string(),
        };
        assert_eq!(loc.to_uri(), "s3://bucket/path");
    }
}
