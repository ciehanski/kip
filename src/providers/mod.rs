//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

pub mod gdrive;
pub mod s3;
pub mod usb;
// pub mod smb;

use self::gdrive::KipGdrive;
use self::s3::KipS3;
use self::usb::KipUsb;
use crate::chunk::FileChunk;
use crate::run::KipUploadMsg;
use anyhow::{bail, Result};
use async_trait::async_trait;
use google_drive3::hyper::client::HttpConnector;
use google_drive3::{hyper_rustls::HttpsConnector, DriveHub};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

#[async_trait]
pub trait KipProvider {
    type Client;
    type Item;

    async fn upload<'b>(
        &self,
        client: Option<&Self::Client>,
        opts: KipUploadOpts,
        chunk: &FileChunk,
        chunk_bytes: &'b [u8],
    ) -> Result<usize>;
    async fn download(&self, client: Option<&Self::Client>, source: &str) -> Result<Vec<u8>>;
    async fn delete(&self, client: Option<&Self::Client>, remote_path: &str) -> Result<()>;
    async fn contains(&self, client: Option<&Self::Client>, job: Uuid, hash: &str) -> Result<bool>;
    async fn list_all(&self, client: Option<&Self::Client>, job: Uuid) -> Result<Vec<Self::Item>>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum KipProviders {
    S3(KipS3),
    Usb(KipUsb),
    Gdrive(KipGdrive),
}

impl KipProviders {
    pub fn name(&self) -> String {
        match self {
            Self::S3(s3) => s3.aws_bucket.clone(),
            Self::Usb(usb) => usb.name.clone(),
            Self::Gdrive(gdrive) => gdrive
                .parent_folder
                .clone()
                .unwrap_or(String::from("Google Drive")),
        }
    }

    pub async fn upload<'b>(
        &self,
        client: &KipClient,
        opts: KipUploadOpts,
        chunk: &FileChunk,
        chunk_bytes: &'b [u8],
    ) -> Result<usize> {
        match self {
            Self::S3(s3) => match client {
                KipClient::S3(s3_client) => {
                    s3.upload(Some(s3_client), opts, chunk, chunk_bytes).await
                }
                _ => {
                    bail!("s3 client not provided")
                }
            },
            Self::Usb(usb) => usb.upload(None, opts, chunk, chunk_bytes).await,
            Self::Gdrive(gdrive) => match client {
                KipClient::Gdrive(gdrive_client) => {
                    gdrive
                        .upload(Some(gdrive_client), opts, chunk, chunk_bytes)
                        .await
                }
                _ => {
                    bail!("gdrive client not provided")
                }
            },
        }
    }

    pub async fn download<'b>(&self, client: &KipClient, file_name: &str) -> Result<Vec<u8>> {
        match self {
            Self::S3(s3) => match client {
                KipClient::S3(client) => s3.download(Some(client), file_name).await,
                _ => {
                    bail!("s3 client not provided")
                }
            },
            Self::Usb(usb) => usb.download(None, file_name).await,
            Self::Gdrive(gdrive) => match client {
                KipClient::Gdrive(client) => gdrive.download(Some(client), file_name).await,
                _ => {
                    bail!("gdrive client not provided")
                }
            },
        }
    }

    pub async fn delete(&self, client: &KipClient, remote_path: &str) -> Result<()> {
        match self {
            Self::S3(s3) => match client {
                KipClient::S3(client) => s3.delete(Some(client), remote_path).await,
                _ => {
                    bail!("s3 client not provided")
                }
            },
            Self::Usb(usb) => usb.delete(None, remote_path).await,
            Self::Gdrive(gdrive) => match client {
                KipClient::Gdrive(client) => gdrive.delete(Some(client), remote_path).await,
                _ => {
                    bail!("gdrive client not provided")
                }
            },
        }
    }

    pub async fn get_client(&self) -> Result<KipClient> {
        Ok(match self {
            KipProviders::S3(ref s3) => {
                let s3_conf = aws_config::from_env()
                    .region(aws_sdk_s3::config::Region::new(s3.aws_region.clone()))
                    .credentials_cache(aws_credential_types::cache::CredentialsCache::lazy())
                    .load()
                    .await;
                KipClient::S3(aws_sdk_s3::Client::new(&s3_conf))
            }
            KipProviders::Usb(_) => KipClient::None,
            KipProviders::Gdrive(_) => {
                KipClient::Gdrive(crate::providers::gdrive::generate_gdrive_hub().await?)
            }
        })
    }
}

#[derive(Clone)]
pub enum KipClient {
    S3(aws_sdk_s3::Client),
    Gdrive(DriveHub<HttpsConnector<HttpConnector>>),
    None,
}

impl std::fmt::Debug for KipClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::S3(_) => write!(f, "S3"),
            Self::Gdrive(_) => write!(f, "Gdrive"),
            Self::None => write!(f, "None"),
        }
    }
}

#[derive(Debug)]
pub struct KipUploadOpts {
    pub job_id: Uuid,
    pub msg_tx: UnboundedSender<KipUploadMsg>,
}

impl KipUploadOpts {
    pub fn new(job_id: Uuid, msg_tx: UnboundedSender<KipUploadMsg>) -> Self {
        Self { job_id, msg_tx }
    }
}
