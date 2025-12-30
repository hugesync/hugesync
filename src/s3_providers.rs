//! S3-compatible provider endpoint resolution
//!
//! Supports shorthand provider/region syntax instead of full endpoint URLs.

use crate::error::{Error, Result};

/// Known S3-compatible providers
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3Provider {
    /// Hetzner Object Storage
    Hetzner,
    /// DigitalOcean Spaces
    DigitalOcean,
    /// Backblaze B2
    Backblaze,
    /// Wasabi Hot Cloud Storage
    Wasabi,
    /// Cloudflare R2
    Cloudflare,
    /// Vultr Object Storage
    Vultr,
    /// Linode Object Storage
    Linode,
    /// Scaleway Object Storage
    Scaleway,
    /// OVHcloud Object Storage
    Ovh,
    /// Exoscale Object Storage
    Exoscale,
    /// MinIO (self-hosted, requires custom endpoint)
    Minio,
    /// IDrive e2
    Idrive,
    /// Contabo Object Storage
    Contabo,
}

impl S3Provider {
    /// Parse provider name from string
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "hetzner" | "htz" => Ok(S3Provider::Hetzner),
            "digitalocean" | "do" | "spaces" => Ok(S3Provider::DigitalOcean),
            "backblaze" | "b2" => Ok(S3Provider::Backblaze),
            "wasabi" => Ok(S3Provider::Wasabi),
            "cloudflare" | "r2" => Ok(S3Provider::Cloudflare),
            "vultr" => Ok(S3Provider::Vultr),
            "linode" | "akamai" => Ok(S3Provider::Linode),
            "scaleway" | "scw" => Ok(S3Provider::Scaleway),
            "ovh" | "ovhcloud" => Ok(S3Provider::Ovh),
            "exoscale" | "exo" => Ok(S3Provider::Exoscale),
            "minio" => Ok(S3Provider::Minio),
            "idrive" | "e2" => Ok(S3Provider::Idrive),
            "contabo" => Ok(S3Provider::Contabo),
            _ => Err(Error::config(format!(
                "Unknown S3 provider: '{}'. Supported: hetzner, digitalocean, backblaze, wasabi, \
                 cloudflare, vultr, linode, scaleway, ovh, exoscale, idrive, contabo",
                s
            ))),
        }
    }

    /// Get the display name
    pub fn name(&self) -> &'static str {
        match self {
            S3Provider::Hetzner => "Hetzner",
            S3Provider::DigitalOcean => "DigitalOcean",
            S3Provider::Backblaze => "Backblaze B2",
            S3Provider::Wasabi => "Wasabi",
            S3Provider::Cloudflare => "Cloudflare R2",
            S3Provider::Vultr => "Vultr",
            S3Provider::Linode => "Linode",
            S3Provider::Scaleway => "Scaleway",
            S3Provider::Ovh => "OVHcloud",
            S3Provider::Exoscale => "Exoscale",
            S3Provider::Minio => "MinIO",
            S3Provider::Idrive => "IDrive e2",
            S3Provider::Contabo => "Contabo",
        }
    }

    /// Get available regions/datacenters for this provider
    pub fn available_regions(&self) -> &'static [&'static str] {
        match self {
            S3Provider::Hetzner => &["fsn1", "nbg1", "hel1"],
            S3Provider::DigitalOcean => &["nyc3", "sfo3", "ams3", "sgp1", "fra1", "syd1"],
            S3Provider::Backblaze => &[
                "us-west-000",
                "us-west-001",
                "us-west-002",
                "us-west-004",
                "eu-central-003",
            ],
            S3Provider::Wasabi => &[
                "us-east-1",
                "us-east-2",
                "us-central-1",
                "us-west-1",
                "eu-central-1",
                "eu-central-2",
                "eu-west-1",
                "eu-west-2",
                "ap-northeast-1",
                "ap-northeast-2",
                "ap-southeast-1",
                "ap-southeast-2",
                "ca-central-1",
            ],
            S3Provider::Cloudflare => &["auto"], // R2 uses account ID, region is auto
            S3Provider::Vultr => &[
                "ewr1", "ord1", "dfw1", "sea1", "lax1", "atl1", "ams1", "lhr1", "fra1", "sjc1",
                "syd1", "jnb1", "sgp1", "nrt1", "blr1", "del1",
            ],
            S3Provider::Linode => &[
                "us-east-1",      // Newark
                "us-southeast-1", // Atlanta
                "us-ord-1",       // Chicago
                "eu-central-1",   // Frankfurt
                "ap-south-1",     // Singapore
                "us-iad-1",       // Washington DC
                "fr-par-1",       // Paris
                "se-sto-1",       // Stockholm
                "in-maa-1",       // Chennai
                "jp-osa-1",       // Osaka
                "it-mil-1",       // Milan
                "us-lax-1",       // Los Angeles
                "us-mia-1",       // Miami
                "id-cgk-1",       // Jakarta
                "br-gru-1",       // Sao Paulo
            ],
            S3Provider::Scaleway => &["fr-par", "nl-ams", "pl-waw"],
            S3Provider::Ovh => &[
                "gra",       // Gravelines, France
                "sbg",       // Strasbourg, France
                "bhs",       // Beauharnois, Canada
                "de",        // Germany
                "uk",        // United Kingdom
                "waw",       // Warsaw, Poland
                "rbx-hdd",   // Roubaix HDD
                "rbx-ssd",   // Roubaix SSD
            ],
            S3Provider::Exoscale => &[
                "ch-gva-2",  // Geneva
                "ch-dk-2",   // Zurich
                "de-fra-1",  // Frankfurt
                "de-muc-1",  // Munich
                "at-vie-1",  // Vienna
                "at-vie-2",  // Vienna 2
                "bg-sof-1",  // Sofia
            ],
            S3Provider::Minio => &[], // Self-hosted, requires custom endpoint
            S3Provider::Idrive => &[
                "us-or",    // Oregon
                "us-va",    // Virginia
                "us-la",    // Los Angeles
                "us-phx",   // Phoenix
                "us-dal",   // Dallas
                "eu-fra",   // Frankfurt
                "eu-lon",   // London
                "eu-par",   // Paris
                "ap-sin",   // Singapore
            ],
            S3Provider::Contabo => &[
                "eu",        // European Union
                "us-central", // US Central
                "sin",       // Singapore
                "aus",       // Australia
                "uk",        // United Kingdom
                "jpn",       // Japan
            ],
        }
    }

    /// Build the endpoint URL for the given region
    pub fn endpoint_url(&self, region: &str) -> Result<String> {
        // Validate region
        let valid_regions = self.available_regions();
        if !valid_regions.is_empty() && !valid_regions.contains(&region) {
            return Err(Error::config(format!(
                "Invalid region '{}' for {}. Available: {}",
                region,
                self.name(),
                valid_regions.join(", ")
            )));
        }

        let url = match self {
            S3Provider::Hetzner => {
                format!("https://{}.your-objectstorage.com", region)
            }
            S3Provider::DigitalOcean => {
                format!("https://{}.digitaloceanspaces.com", region)
            }
            S3Provider::Backblaze => {
                format!("https://s3.{}.backblazeb2.com", region)
            }
            S3Provider::Wasabi => {
                format!("https://s3.{}.wasabisys.com", region)
            }
            S3Provider::Cloudflare => {
                // R2 requires account ID in the URL
                // Format: https://<ACCOUNT_ID>.r2.cloudflarestorage.com
                return Err(Error::config(
                    "Cloudflare R2 requires account ID. Use --s3-endpoint https://<ACCOUNT_ID>.r2.cloudflarestorage.com"
                ));
            }
            S3Provider::Vultr => {
                format!("https://{}.vultrobjects.com", region)
            }
            S3Provider::Linode => {
                format!("https://{}.linodeobjects.com", region)
            }
            S3Provider::Scaleway => {
                format!("https://s3.{}.scw.cloud", region)
            }
            S3Provider::Ovh => {
                format!("https://s3.{}.cloud.ovh.net", region)
            }
            S3Provider::Exoscale => {
                format!("https://sos-{}.exo.io", region)
            }
            S3Provider::Minio => {
                return Err(Error::config(
                    "MinIO is self-hosted. Use --s3-endpoint http://your-minio-server:9000"
                ));
            }
            S3Provider::Idrive => {
                // IDrive e2 format: https://<region>.e2.idrivesync.com
                format!("https://{}.e2.idrivesync.com", region)
            }
            S3Provider::Contabo => {
                format!("https://{}.contabostorage.com", region)
            }
        };

        Ok(url)
    }
}

/// Resolve provider and region into an endpoint URL
pub fn resolve_endpoint(provider: &str, region: &str) -> Result<String> {
    let provider = S3Provider::parse(provider)?;
    provider.endpoint_url(region)
}

/// Print help about available providers and regions
pub fn print_providers_help() {
    println!("S3-Compatible Providers and Regions:\n");

    let providers = [
        S3Provider::Hetzner,
        S3Provider::DigitalOcean,
        S3Provider::Backblaze,
        S3Provider::Wasabi,
        S3Provider::Vultr,
        S3Provider::Linode,
        S3Provider::Scaleway,
        S3Provider::Ovh,
        S3Provider::Exoscale,
        S3Provider::Idrive,
        S3Provider::Contabo,
    ];

    for provider in providers {
        let regions = provider.available_regions();
        println!("  {} ({})", provider.name(), format!("{:?}", provider).to_lowercase());
        if regions.is_empty() {
            println!("    Requires --s3-endpoint URL\n");
        } else {
            println!("    Regions: {}\n", regions.join(", "));
        }
    }

    println!("Special cases:");
    println!("  Cloudflare R2: Use --s3-endpoint https://<ACCOUNT_ID>.r2.cloudflarestorage.com");
    println!("  MinIO:         Use --s3-endpoint http://your-server:9000");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_provider() {
        assert_eq!(S3Provider::parse("hetzner").unwrap(), S3Provider::Hetzner);
        assert_eq!(S3Provider::parse("htz").unwrap(), S3Provider::Hetzner);
        assert_eq!(S3Provider::parse("DO").unwrap(), S3Provider::DigitalOcean);
        assert_eq!(S3Provider::parse("b2").unwrap(), S3Provider::Backblaze);
    }

    #[test]
    fn test_endpoint_url() {
        assert_eq!(
            S3Provider::Hetzner.endpoint_url("hel1").unwrap(),
            "https://hel1.your-objectstorage.com"
        );
        assert_eq!(
            S3Provider::DigitalOcean.endpoint_url("nyc3").unwrap(),
            "https://nyc3.digitaloceanspaces.com"
        );
        assert_eq!(
            S3Provider::Backblaze.endpoint_url("eu-central-003").unwrap(),
            "https://s3.eu-central-003.backblazeb2.com"
        );
    }

    #[test]
    fn test_invalid_region() {
        assert!(S3Provider::Hetzner.endpoint_url("invalid").is_err());
    }

    #[test]
    fn test_resolve_endpoint() {
        assert_eq!(
            resolve_endpoint("wasabi", "eu-central-1").unwrap(),
            "https://s3.eu-central-1.wasabisys.com"
        );
    }
}
