//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::chunk::FileChunk;
use crate::crypto::{decrypt, encrypt};
use crate::providers::KipProvider;
use anyhow::{bail, Result};
use async_trait::async_trait;
use directories::ProjectDirs;
use drive3::api::{File, Scope};
use drive3::hyper::client::HttpConnector;
use drive3::hyper_rustls::HttpsConnector;
use drive3::{hyper, hyper_rustls, oauth2, DriveHub, Error};
use google_drive3 as drive3;
use linya::{Bar, Progress};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::default::Default;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

const REDIRECT_URI: &str = "http://127.0.0.1";
const AUTH_URI: &str = "https://accounts.google.com/o/oauth2/auth";
const TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
const AUTH_PROVIDER: &str = "https://www.googleapis.com/oauth2/v1/certs";
const TOKEN_STORAGE: &str = "gdrive_tokencache.json";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipGdrive {
    pub parent_folder: Option<String>,
}

impl KipGdrive {
    pub fn new<S: Into<String>>(folder: Option<S>) -> Self {
        if let Some(pf) = folder {
            Self {
                parent_folder: Some(pf.into()),
            }
        } else {
            Self {
                parent_folder: None,
            }
        }
    }
}

#[async_trait]
impl KipProvider for KipGdrive {
    type Item = File;

    async fn upload(
        &self,
        f: &Path,
        chunks_map: HashMap<FileChunk, &[u8]>,
        _job_id: Uuid,
        secret: &str,
        progress: Arc<Mutex<Progress>>,
        bar: &Bar,
    ) -> Result<(Vec<FileChunk>, u64)> {
        // Generate Google Drive Hub
        let hub = generate_gdrive_hub().await?;
        // Upload each chunk
        let mut chunks = vec![];
        let mut bytes_uploaded: u64 = 0;
        for (mut chunk, chunk_bytes) in chunks_map {
            // Always compress before encryption
            let compressed = crate::run::compress(chunk_bytes).await?;
            // Encrypt chunk
            let encrypted = match encrypt(&compressed, secret) {
                Ok(ec) => ec,
                Err(e) => {
                    bail!("failed to encrypt chunk: {e}.")
                }
            };
            // Get amount of bytes uploaded in this chunk
            // after compression and encryption
            let ce_bytes_len = encrypted.len();
            // Upload
            let req = File {
                name: Some(format!("{}.chunk", chunk.hash)),
                parents: Some(vec![self.parent_folder.to_owned().unwrap_or_default()]),
                ..Default::default()
            };
            hub.files()
                .create(req)
                .add_scope(Scope::File)
                .use_content_as_indexable_text(false)
                .supports_all_drives(false)
                .keep_revision_forever(false)
                .ignore_default_visibility(true)
                .upload(
                    Cursor::new(chunk_bytes),
                    "application/octet-stream".parse().unwrap(),
                )
                .await?;
            // Push chunk onto chunks hashmap for return
            chunk.local_path = f.canonicalize()?;
            chunks.push(chunk);
            // Increment progress bar for this file by one
            // since one chunk was uploaded
            progress.lock().await.inc_and_draw(bar, chunk_bytes.len());
            let ce_bytes_len_u64: u64 = ce_bytes_len.try_into()?;
            bytes_uploaded += ce_bytes_len_u64;
        }
        Ok((chunks, bytes_uploaded))
    }

    async fn download(&self, f: &str, secret: &str) -> Result<Vec<u8>> {
        // Generate Google Drive Hub
        let hub = generate_gdrive_hub().await?;
        // Create download request
        let req = hub
            .files()
            .get(f)
            .supports_team_drives(false)
            .supports_all_drives(false)
            .acknowledge_abuse(true)
            .param("alt", "media");
        // Send request and parse response into Vec<u8>
        let result_bytes = match req.doit().await {
            Ok((resp, _)) => Vec::from(hyper::body::to_bytes(resp.into_body()).await?),
            Err(e) => match e {
                Error::HttpError(_)
                | Error::Io(_)
                | Error::MissingAPIKey
                | Error::MissingToken(_)
                | Error::Cancelled
                | Error::UploadSizeLimitExceeded(_, _)
                | Error::Failure(_)
                | Error::BadRequest(_)
                | Error::FieldClash(_)
                | Error::JsonDecodeError(_, _) => bail!("{e}"),
            },
        };
        // Decrypt result_bytes
        let decrypted = match decrypt(&result_bytes, secret) {
            Ok(dc) => dc,
            Err(e) => {
                bail!("failed to decrypt file: {e}.")
            }
        };
        // Decompress decrypted bytes
        let decompressed = crate::run::decompress(&decrypted).await?;
        // Ship it
        Ok(decompressed)
    }

    async fn delete(&self, file_name: &str) -> Result<()> {
        // Generate Google Drive Hub
        let hub = generate_gdrive_hub().await?;
        // Delete file
        match hub.files().delete(file_name).doit().await {
            Ok(_) => Ok(()),
            Err(e) => match e {
                Error::HttpError(_)
                | Error::Io(_)
                | Error::MissingAPIKey
                | Error::MissingToken(_)
                | Error::Cancelled
                | Error::UploadSizeLimitExceeded(_, _)
                | Error::Failure(_)
                | Error::BadRequest(_)
                | Error::FieldClash(_)
                | Error::JsonDecodeError(_, _) => bail!("{e}"),
            },
        }
    }

    async fn contains(&self, _job_id: Uuid, obj_name: &str) -> Result<bool> {
        // Check Google Drive for duplicates of chunk
        let gdrive_objs = self.list_all(_job_id).await?;
        // If Google Drive is empty, no need to check for duplicate chunks
        if !gdrive_objs.is_empty() {
            for obj in gdrive_objs {
                if let Some(gd_name) = obj.name {
                    if gd_name == obj_name {
                        // Duplicate chunk found, return true
                        return Ok(true);
                    }
                } else {
                    bail!("unable to get chunk name from Google Drive.")
                }
            }
        }
        Ok(false)
    }

    async fn list_all(&self, _job_id: Uuid) -> Result<Vec<Self::Item>> {
        // Generate Google Drive Hub
        let hub = generate_gdrive_hub().await?;
        // Create request to collect all files
        let result = hub
            .files()
            .list()
            .supports_team_drives(false)
            .supports_all_drives(true)
            .spaces("drive")
            .page_size(1000)
            .include_team_drive_items(false)
            .include_items_from_all_drives(true);
        // Send request
        match result.doit().await {
            Ok((_, file_list)) => {
                Ok(file_list.files.unwrap())
                // match file_list.next_page_token {
                //     Some(_) => {
                //         self.fetch_files(hub, modified_since, file_list.next_page_token)
                //             .await
                //     }
                //     // Done
                //     None => self,
                // }
            }
            Err(e) => match e {
                Error::HttpError(_)
                | Error::Io(_)
                | Error::MissingAPIKey
                | Error::MissingToken(_)
                | Error::Cancelled
                | Error::UploadSizeLimitExceeded(_, _)
                | Error::Failure(_)
                | Error::BadRequest(_)
                | Error::FieldClash(_)
                | Error::JsonDecodeError(_, _) => bail!("{e}"),
            },
        }
    }
}

async fn generate_gdrive_hub() -> Result<DriveHub<HttpsConnector<HttpConnector>>> {
    // Get client ID and client secret from env
    let client_id = std::env::var("GOOGLE_DRIVE_CLIENT_ID")?;
    let client_secret = std::env::var("GOOGLE_DRIVE_CLIENT_SECRET")?;
    // Create Google OAuth client config
    let gdrive_secret = oauth2::ApplicationSecret {
        client_id,
        client_secret,
        redirect_uris: vec![REDIRECT_URI.to_string()],
        auth_uri: AUTH_URI.to_string(),
        token_uri: TOKEN_URI.to_string(),
        auth_provider_x509_cert_url: Some(AUTH_PROVIDER.to_string()),
        ..Default::default()
    };
    // OAuth2 token storage
    let token_storage = ProjectDirs::from("com", "ciehanski", "kip")
        .unwrap()
        .config_dir()
        .join(TOKEN_STORAGE);
    // OAuth2 client request init
    let gdrive_auth = oauth2::InstalledFlowAuthenticator::builder(
        gdrive_secret,
        oauth2::InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk(token_storage)
    .build()
    .await?;
    // Create Google Drive Hub client
    let hub = DriveHub::new(
        hyper::Client::builder().build(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_or_http()
                .enable_http1()
                .enable_http2()
                .build(),
        ),
        gdrive_auth,
    );
    Ok(hub)
}
