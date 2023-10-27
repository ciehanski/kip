//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use super::KipUploadOpts;
use crate::chunk::FileChunk;
use crate::providers::KipProvider;
use crate::run::KipUploadMsg;
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
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipGdrive {
    pub parent_folder: Option<String>,
}

impl KipGdrive {
    // 20,000 API requests per 100 seconds
    const _API_RATE_LIMIT: u64 = 20_000;
    const _API_RATE_LIMIT_PERIOD: u64 = 100;
    // OAuth Client Settings
    const REDIRECT_URI: &str = "http://127.0.0.1";
    const AUTH_URI: &str = "https://accounts.google.com/o/oauth2/auth";
    const TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
    const AUTH_PROVIDER: &str = "https://www.googleapis.com/oauth2/v1/certs";
    const TOKEN_STORAGE: &str = "gdrive_tokencache.json";
    // Request Consts
    const LIST_PAGE_SIZE: i32 = 5_000;

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

    async fn upload<'b>(
        &mut self,
        opts: KipUploadOpts,
        chunks_map: HashMap<FileChunk, &'b [u8]>,
        progress: Arc<Mutex<Progress>>,
        bar: &Bar,
    ) -> Result<()> {
        // Generate Google Drive Hub
        let hub = generate_gdrive_hub().await?;
        // Check if job's parent folder exists in gdrive
        if self.parent_folder.is_none() {
            // If the KipGdrive parent_folder is empty, create the folder
            // in gdrive
            let req = File {
                name: Some(format!("{}", opts.job_id)),
                mime_type: Some("application/vnd.google-apps.folder".to_string()),
                ..Default::default()
            };
            let (_, result) = hub
                .files()
                .create(req)
                .add_scope(Scope::File)
                .use_content_as_indexable_text(false)
                .supports_all_drives(false)
                .keep_revision_forever(false)
                .ignore_default_visibility(true)
                .upload(
                    Cursor::new(vec![]),
                    "application/vnd.google-apps.folder".parse().unwrap(),
                )
                .await?;
            // Set parent_folder to returned folder ID
            let job_folder = result.id.unwrap();
            let req = File {
                name: Some(String::from("chunks")),
                parents: Some(vec![job_folder]),
                mime_type: Some("application/vnd.google-apps.folder".to_string()),
                ..Default::default()
            };
            let (_, result) = hub
                .files()
                .create(req)
                .add_scope(Scope::File)
                .use_content_as_indexable_text(false)
                .supports_all_drives(false)
                .keep_revision_forever(false)
                .ignore_default_visibility(true)
                .upload(
                    Cursor::new(vec![]),
                    "application/vnd.google-apps.folder".parse().unwrap(),
                )
                .await?;
            self.parent_folder = Some(result.id.unwrap());
        }
        // Upload each chunk
        for (mut chunk, chunk_bytes) in chunks_map {
            // Get amount of bytes uploaded in this chunk
            // after compression and encryption
            let ce_bytes_len = chunk_bytes.len();
            // Upload
            let req = File {
                name: Some(format!("{}.chunk", chunk.hash)),
                parents: Some(vec![self.parent_folder.to_owned().unwrap_or_default()]),
                ..Default::default()
            };
            let (_, result) = hub
                .files()
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
            // Set chunk's remote path
            chunk.set_remote_path(result.id.unwrap());
            // Increment progress bar by chunk bytes len
            progress.lock().await.inc_and_draw(bar, ce_bytes_len);
            // Increment run's uploaded bytes
            opts.msg_tx
                .send(KipUploadMsg::BytesUploaded(ce_bytes_len.try_into()?))?;
        }
        Ok(())
    }

    async fn download(&self, file_name: &str) -> Result<Vec<u8>> {
        // Generate Google Drive Hub
        let hub = generate_gdrive_hub().await?;
        // Create download request
        let req = hub
            .files()
            .get(file_name)
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
        // Return downloaded chunk bytes
        Ok(result_bytes)
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

    async fn contains(&self, _job_id: Uuid, hash: &str) -> Result<bool> {
        // Check Google Drive for duplicates of chunk
        let gdrive_objs = self.list_all(_job_id).await?;
        // If Google Drive is empty, no need to check for duplicate chunks
        if !gdrive_objs.is_empty() {
            for obj in gdrive_objs {
                if let Some(gd_name) = obj.name {
                    if gd_name.contains(hash) {
                        // Duplicate chunk found, return true
                        return Ok(true);
                    }
                } else {
                    bail!("unable to get chunk name from Google Drive")
                }
            }
        }
        Ok(false)
    }

    async fn list_all(&self, job_id: Uuid) -> Result<Vec<Self::Item>> {
        // Generate Google Drive Hub
        let hub = generate_gdrive_hub().await?;
        // Create request to collect all files
        let result = hub
            .files()
            .list()
            .supports_team_drives(false)
            .supports_all_drives(true)
            .spaces("drive")
            .page_size(Self::LIST_PAGE_SIZE)
            .include_team_drive_items(false)
            .include_items_from_all_drives(true);
        // Send request
        let gdrive_contents = match result.doit().await {
            Ok((_, file_list)) => {
                let mut filtered = match file_list.files {
                    Some(files) => files
                        .into_iter()
                        .filter(|f| filter_job_id(f.name.to_owned(), job_id))
                        .collect::<Vec<File>>(),
                    None => vec![],
                };
                // Handle pagination
                let mut paginated = file_list.next_page_token;
                while let Some(pcf) = paginated {
                    let (_, paginated_result) = hub
                        .files()
                        .list()
                        .supports_team_drives(false)
                        .supports_all_drives(true)
                        .spaces("drive")
                        .page_size(Self::LIST_PAGE_SIZE)
                        .page_token(&pcf)
                        .include_team_drive_items(false)
                        .include_items_from_all_drives(true)
                        .doit()
                        .await?;
                    match paginated_result.files {
                        Some(prc) => {
                            filtered.extend(
                                prc.into_iter()
                                    .filter(|obj| filter_job_id(obj.name.clone(), job_id)),
                            );
                        }
                        None => (),
                    };
                    paginated = paginated_result.next_page_token;
                }
                filtered
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
        };
        // Only check chunks that are within this job's
        // folder in Gdrive
        // let mut job_contents = vec![];
        // for obj in gdrive_contents {
        //     if let Some(key) = obj.name.clone() {
        //         // We expect jid to be Some since key was not nil
        //         if let Some((jid, _)) = key.split_once('/') {
        //             if jid == job_id.to_string() {
        //                 job_contents.push(obj);
        //             };
        //         } else {
        //             // error splitting obj key returned from Gdrive
        //             debug!("error splitting chunk name from Gdrive")
        //         };
        //     } else {
        //         // error, no obj key returned from Gdrive
        //         debug!("unable to get chunk name from Gdrive")
        //     }
        // }
        Ok(gdrive_contents)
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
        redirect_uris: vec![KipGdrive::REDIRECT_URI.to_string()],
        auth_uri: KipGdrive::AUTH_URI.to_string(),
        token_uri: KipGdrive::TOKEN_URI.to_string(),
        auth_provider_x509_cert_url: Some(KipGdrive::AUTH_PROVIDER.to_string()),
        ..Default::default()
    };
    // OAuth2 token storage
    let token_storage = ProjectDirs::from("com", "ciehanski", "kip")
        .unwrap()
        .config_dir()
        .join(KipGdrive::TOKEN_STORAGE);
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

/// Retrieves the hash from an Gdrive object name and returns
/// it as a String.
pub fn strip_hash_from_gdrive(gdrive_path: &str) -> String {
    // Split the chunk. Ex: 902938470293847392033874592038473.chunk
    let hs: Vec<&str> = gdrive_path.split('.').collect();
    // Just grab the first split, which is the hash
    hs[0].to_string()
}

fn filter_job_id(provider_path: Option<String>, job_id: Uuid) -> bool {
    if let Some(key) = provider_path {
        // We expect jid to be Some since key was not nil
        if let Some((jid, _)) = key.split_once('/') {
            if jid == job_id.to_string() {
                return true;
            };
        } else {
            debug!("error splitting chunk name from Gdrive")
        };
    } else {
        debug!("unable to get chunk name from Gdrive")
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_hash_from_gdrive() {
        // Split the 902938470293847392033874592038473.chunk
        let hash = strip_hash_from_gdrive(
            "001d46082763b930e5b9f0c52d16841b443bfbcd52af6cd475cb0182548da33a.chunk",
        );
        assert_eq!(
            hash,
            "001d46082763b930e5b9f0c52d16841b443bfbcd52af6cd475cb0182548da33a"
        )
    }
}
