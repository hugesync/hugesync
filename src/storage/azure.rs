//! Azure Blob Storage backend using native SDK for true delta support

use crate::error::{Error, Result};
use crate::storage::{ByteStream, CompletedPart};
use crate::types::FileEntry;

use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use futures::StreamExt;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

/// Azure Blob Storage backend
#[derive(Clone)]
pub struct AzureBackend {
    client: Arc<ContainerClient>,
    container: String,
    prefix: String,
    account: String,
}

impl AzureBackend {
    /// Create a new Azure backend
    pub async fn new(container: String, prefix: String) -> Result<Self> {
        let account = env::var("AZURE_STORAGE_ACCOUNT")
            .map_err(|_| Error::config("AZURE_STORAGE_ACCOUNT environment variable not set"))?;
        let access_key = env::var("AZURE_STORAGE_ACCESS_KEY")
            .map_err(|_| Error::config("AZURE_STORAGE_ACCESS_KEY environment variable not set"))?;

        let storage_creds = StorageCredentials::access_key(account.clone(), access_key);
        let service_client = BlobServiceClient::new(account.clone(), storage_creds);
        let container_client = service_client.container_client(&container);

        // Verify container exists
        if !container_client.exists().await.map_err(|e| Error::Azure {
            message: format!("Failed to check container existence: {}", e),
        })? {
            return Err(Error::Azure {
                message: format!("Container '{}' does not exist", container),
            });
        }

        Ok(Self {
            client: Arc::new(container_client),
            container,
            prefix,
            account,
        })
    }

    /// Resolve relative path to full blob name
    fn resolve_key(&self, path: &str) -> String {
        if self.prefix.is_empty() {
            path.to_string()
        } else if path.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), path)
        }
    }

    /// Strip prefix from blob name
    fn strip_prefix(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            let prefix_with_slash = format!("{}/", self.prefix.trim_end_matches('/'));
            key.strip_prefix(&prefix_with_slash)
                .or_else(|| key.strip_prefix(&self.prefix))
                .unwrap_or(key)
                .to_string()
        }
    }

    /// Helper to Generate Block ID
    fn generate_block_id(part_number: i32) -> BlockId {
        let id_str = format!("block-{:06}", part_number);
        BlockId::new(BASE64.encode(id_str))
    }

    /// List blobs
    pub async fn list(&self, prefix: &str) -> Result<Vec<FileEntry>> {
        let full_prefix = self.resolve_key(prefix);
        let mut entries = Vec::new();

        let mut stream = self.client.list_blobs().prefix(full_prefix.clone()).into_stream();

        while let Some(value) = stream.next().await {
            let page = value.map_err(|e| Error::Azure {
                message: format!("Failed to list blobs: {}", e),
            })?;

            for blob in page.blobs.blobs() {
                let name = blob.name.clone();
                let relative = self.strip_prefix(&name);
                if relative.is_empty() {
                    continue;
                }

                entries.push(FileEntry {
                    path: PathBuf::from(relative),
                    size: blob.properties.content_length,
                    mtime: Some(blob.properties.last_modified.into()),
                    is_dir: false, // Azure is flat namespace
                    mode: None,
                    etag: Some(blob.properties.etag.to_string()),
                });
            }
        }

        Ok(entries)
    }

    /// Get object metadata
    pub async fn head(&self, path: &str) -> Result<Option<FileEntry>> {
        let key = self.resolve_key(path);
        let blob_client = self.client.blob_client(&key);

        match blob_client.get_properties().await {
            Ok(props) => Ok(Some(FileEntry {
                path: PathBuf::from(path),
                size: props.blob.properties.content_length,
                mtime: Some(props.blob.properties.last_modified.into()),
                is_dir: false,
                mode: None,
                etag: Some(props.blob.properties.etag.to_string()),
            })),
            Err(e) => {
                // Check if 404
                 if e.to_string().contains("404") || e.to_string().contains("NotFound") {
                     Ok(None)
                 } else {
                     Err(Error::Azure {
                         message: format!("Failed to get blob properties: {}", e),
                     })
                 }
            }
        }
    }

    /// Get object content
    pub async fn get(&self, path: &str) -> Result<Bytes> {
        let key = self.resolve_key(path);
        let blob_client = self.client.blob_client(&key);

        let data = blob_client.get_content().await.map_err(|e| Error::Azure {
            message: format!("Failed to get blob content: {}", e),
        })?;

        Ok(Bytes::from(data))
    }

    /// Get range of object content
    pub async fn get_range(&self, path: &str, start: u64, end: u64) -> Result<Bytes> {
        let key = self.resolve_key(path);
        let blob_client = self.client.blob_client(&key);
        
        // Azure range is inclusive-inclusive? 
        // SDK Range expects u64
        // Check documentation: Standard HTTP Range is inclusive-inclusive.
        // azure_core::request_options::Range uses start..end
        
        // Explicitly request range using builder
        let builder = blob_client.get();
        // The builder API in 0.21 might differ. 
        // Let's try basic get_content first, but we need range.
        
        // Correct usage for 0.21:
        // builder.range(start..end+1)
        
        let data = builder
            .range(start..end + 1)
            .into_stream()
            .next()
            .await
            .ok_or(Error::Azure { message: "Empty stream".into() })?
            .map_err(|e| Error::Azure { message: format!("Failed to download range: {}", e) })?;

        let mut content = Vec::new();
        let mut stream = data.data;
        while let Some(chunk) = stream.next().await {
            content.extend_from_slice(&chunk.map_err(|e| Error::Azure { message: e.to_string() })?);
        }
        
        Ok(Bytes::from(content))
    }

    /// Upload object
    pub async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        let key = self.resolve_key(path);
        let blob_client = self.client.blob_client(&key);

        blob_client.put_block_blob(data).await.map_err(|e| Error::Azure {
            message: format!("Failed to upload blob: {}", e),
        })?;

        Ok(())
    }

    /// Write a file from a stream of chunks using block upload
    pub async fn put_stream(
        &self,
        path: &str,
        mut stream: ByteStream,
        _size_hint: Option<u64>,
    ) -> Result<()> {
        let upload_id = self.create_multipart_upload(path).await?;
        let mut parts = Vec::new();
        let mut part_number = 1;

        while let Some(chunk) = stream.next().await {
            let data = chunk?;
            let part = self.upload_part(path, &upload_id, part_number, data).await?;
            parts.push(part);
            part_number += 1;
        }

        self.complete_multipart_upload(path, &upload_id, parts).await
    }

    /// Delete object
    pub async fn delete(&self, path: &str) -> Result<()> {
        let key = self.resolve_key(path);
        let blob_client = self.client.blob_client(&key);

        blob_client.delete().await.map_err(|e| Error::Azure {
            message: format!("Failed to delete blob: {}", e),
        })?;

        Ok(())
    }

    /// Initialize multipart upload (Block Blob)
    pub async fn create_multipart_upload(&self, path: &str) -> Result<String> {
        // Return the blob name as the upload ID. 
        // Block blobs don't need explicit initialization on the server.
        Ok(self.resolve_key(path))
    }

    /// Upload a part (Block)
    pub async fn upload_part(
        &self,
        path: &str,
        _upload_id: &str, // This IS the path/key in our abstraction
        part_number: i32,
        data: Bytes,
    ) -> Result<CompletedPart> {
        let _key = self.resolve_key(path); // Path is already key if resolved? 
        // Wait, caller passes `remote_path` as `path` and `upload_id`.
        // In our case upload_id == key.
        
        let blob_client = self.client.blob_client(_upload_id);
        let block_id = Self::generate_block_id(part_number);

        blob_client.put_block(block_id.clone(), data).await.map_err(|e| Error::Azure {
            message: format!("Failed to put block: {}", e),
        })?;

        // Azure PUT Block doesn't return an ETag for the block usually.
        // We just need the block ID for the commit list.
        // CompletedPart stores ETag. We can store BlockID as ETag?
        
        Ok(CompletedPart {
            part_number,
            etag: String::from_utf8(block_id.as_ref().to_vec()).unwrap(), // Store Base64 ID as ETag
        })
    }

    /// Copy a part from an existing blob (delta key!)
    pub async fn upload_part_copy(
        &self,
        _path: &str,
        upload_id: &str,
        part_number: i32,
        source_path: &str,
        source_start: u64,
        source_end: u64,
    ) -> Result<CompletedPart> {
        let blob_client = self.client.blob_client(upload_id);
        let block_id = Self::generate_block_id(part_number);

        // Resolve source key and get its blob client
        let source_key = self.resolve_key(source_path);
        let source_blob_client = self.client.blob_client(&source_key);

        // Generate a SAS token for the source blob with read permission.
        // This is required because put_block_url performs a server-side GET
        // which won't inherit our client credentials.
        let sas_expiry = time::OffsetDateTime::now_utc() + time::Duration::hours(1);
        let sas_permissions = BlobSasPermissions {
            read: true,
            ..Default::default()
        };

        let sas_token = source_blob_client
            .shared_access_signature(sas_permissions, sas_expiry)
            .await
            .map_err(|e| Error::Azure {
                message: format!("Failed to generate SAS token: {}", e),
            })?
            .token()
            .map_err(|e| Error::Azure {
                message: format!("Failed to get SAS token string: {}", e),
            })?;

        // Construct source URL with SAS token
        let url = format!(
            "https://{}.blob.core.windows.net/{}/{}?{}",
            self.account,
            self.container,
            source_key,
            sas_token
        );
        let url = url::Url::parse(&url).map_err(|e| Error::config(format!("Invalid source URL: {}", e)))?;

        // Stage block from URL using put_block_url
        blob_client
            .put_block_url(block_id.clone(), url)
            .range(source_start..source_end + 1)
            .await
            .map_err(|e| Error::Azure {
                message: format!("Failed to stage block from URL: {}", e),
            })?;

        Ok(CompletedPart {
            part_number,
            etag: String::from_utf8(block_id.as_ref().to_vec()).unwrap(),
        })
    }

    /// Complete multipart upload (Commit Block List)
    pub async fn complete_multipart_upload(
        &self,
        _path: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> Result<()> {
        let blob_client = self.client.blob_client(upload_id);

        let block_list: Vec<BlockId> = parts
            .into_iter()
            .map(|p| BlockId::new(p.etag)) // We stored encoded ID in ETag
            .collect();
            
        // We need to provide BlockListType? Usually Latest.
        let block_list = BlockList {
            blocks: block_list.into_iter().map(|id| BlobBlockType::Uncommitted(id)).collect()
        };

        blob_client.put_block_list(block_list).await.map_err(|e| Error::Azure {
            message: format!("Failed to commit block list: {}", e),
        })?;

        Ok(())
    }

    /// Abort multipart upload
    pub async fn abort_multipart_upload(&self, _path: &str, _upload_id: &str) -> Result<()> {
        // Block blobs automatically garbage collect uncommitted blocks after a week.
        // There is no explicit abort command for block lists that aren't committed.
        // We could delete the blob if we want? But that might delete the *previous* version if it exists?
        // Safest is to do nothing.
        Ok(())
    }
}