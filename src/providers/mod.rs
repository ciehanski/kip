//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

pub mod s3;
// pub mod usb;

use crate::chunk::FileChunk;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

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

    // pub fn s3(&self) -> Option<KipS3> {
    //     match self {
    //         KipProviders::S3(s3) => Some(*s3),
    //         _ => None,
    //     }
    // }

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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipS3 {
    pub aws_bucket: String,
    pub aws_region: String,
}

impl KipS3 {
    pub fn new<S: Into<String>>(aws_bucket: S, aws_region: S) -> Self {
        KipS3 {
            aws_bucket: aws_bucket.into(),
            aws_region: aws_region.into(),
        }
    }
}

pub trait KipProvider {
    fn upload<S: Into<String>>(
        source: &Path,
        dest: S,
        chunks: HashMap<FileChunk, &[u8]>,
        total_bytes: u64,
        secret: S,
    ) -> Result<Vec<FileChunk>, Box<dyn Error>>;
    fn download(source: &str, dest: &Path, secret: &str) -> Result<Vec<u8>, Box<dyn Error>>;
    fn delete(remote_path: &str) -> Result<(), Box<dyn Error>>;
    fn list_all() -> Result<(), Box<dyn Error>>;
    fn contains(remote_path: &str, obj_name: &str) -> Result<bool, Box<dyn Error>>;
}
