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
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

#[async_trait]
pub trait KipProvider {
    type Uploader;
    type Item;

    async fn upload<'b>(
        &self,
        client: Option<&Self::Uploader>,
        opts: KipUploadOpts,
        chunk: &FileChunk,
        chunk_bytes: &'b [u8],
    ) -> Result<usize>;
    async fn download(&self, source: &str) -> Result<Vec<u8>>;
    async fn delete(&self, remote_path: &str) -> Result<()>;
    async fn contains(&self, job: Uuid, hash: &str) -> Result<bool>;
    async fn list_all(&self, job: Uuid) -> Result<Vec<Self::Item>>;
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum KipProviders {
    S3(KipS3),
    Usb(KipUsb),
    Gdrive(KipGdrive),
}
