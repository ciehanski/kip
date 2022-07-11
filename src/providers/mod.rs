//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

pub mod s3;
// pub mod usb;

use crate::chunk::FileChunk;
use crate::providers::s3::KipS3;
use anyhow::Result;
use async_trait::async_trait;
use linya::{Bar, Progress};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[async_trait]
pub trait KipProvider {
    async fn upload(
        &self,
        source: &Path,
        chunks: HashMap<FileChunk, &[u8]>,
        job_id: Uuid,
        secret: &str,
        progress: Arc<Mutex<Progress>>,
        bar: &Bar,
    ) -> Result<(Vec<FileChunk>, usize)>;
    async fn download(&self, source: &str, secret: &str) -> Result<Vec<u8>>;
    async fn delete(&self, remote_path: &str) -> Result<()>;
    async fn contains(&self, job: Uuid, obj_name: &str) -> Result<bool>;
    async fn list_all(&self, job: Uuid) -> Result<Vec<rusoto_s3::Object>>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum KipProviders {
    S3(KipS3),
    Usb(KipUsb),
}

impl KipProviders {
    pub fn is_s3(&self) -> bool {
        matches!(self, KipProviders::S3(_))
    }

    pub fn is_usb(&self) -> bool {
        matches!(self, KipProviders::Usb(_))
    }

    pub fn s3(&self) -> Option<&KipS3> {
        match self {
            KipProviders::S3(s3) => Some(s3),
            _ => None,
        }
    }

    pub fn usb(&self) -> Option<KipUsb> {
        match self {
            KipProviders::Usb(usb) => Some(*usb),
            _ => None,
        }
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct KipUsb {
    pub vendor_id: u16,
    pub product_id: u16,
}
