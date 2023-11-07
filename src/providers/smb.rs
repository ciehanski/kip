//
// Copyright (c) 2023 Ryan Ciehanski <ryan@ciehanski.com>
//

use super::{KipProvider, ProgressBar};
use crate::compress::KipCompressionOpts;
use crate::crypto::encrypt;
use crate::job::KipFile;
use crate::providers::FileChunk;
use anyhow::{bail, Result};
use async_trait::async_trait;
use linya::{Bar, Progress};
use pavao::{SmbClient, SmbFile, SmbCredentials, SmbOpenOptions, SmbOptions};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

struct KipSmb {
    pub server: SocketAddr,
    pub share: String,
    pub username: String,
    pub workgroup: String,
    pub destination: PathBuf,
}

impl KipSmb {
    pub fn new<S: Into<String>>(
        server: SocketAddr,
        share: S,
        username: S,
        workgroup: S,
        destination: PathBuf,
    ) -> Self {
        Self {
            server,
            share: share.into(),
            username: username.into(),
            workgroup: workgroup.into(),
            destination,
        }
    }
}

#[async_trait]
impl KipProvider for KipSmb {
    type Item = SmbFile;

    async fn upload(
        &self,
        f: &Path,
        chunks_map: HashMap<FileChunk, &[u8]>,
        job_id: Uuid,
        secret: &str,
        compress: KipCompressionOpts,
        progress: Arc<Mutex<Progress>>,
        bar: &Bar,
    ) -> Result<(Vec<FileChunk>, u64)> {
        // Setup SMB client
        let server = format!("{}:{}", self.server.ip(), self.server.port());
        let client = SmbClient::new(
            SmbCredentials::default()
                .server(server)
                .share(self.share)
                .username(self.username)
                .password(password)
                .workgroup(self.workgroup),
            SmbOptions::default().one_share_per_server(true),
        )?;
        // Upload each chunk
        let mut chunks = vec![];
        let mut bytes_uploaded: u64 = 0;
        for (mut chunk, chunk_bytes) in chunks_map {
            // Always compress before encryption
            let mut compressed = Vec::<u8>::new();
            if compress.enabled {
                match compress.alg {
                    KipCompAlg::Zstd => compressed = compress_zstd(chunk_bytes).await?,
                    KipCompAlg::Lzma => compressed = compress_lzma(chunk_bytes).await?,
                    KipCompAlg::Gzip => compressed = compress_gzip(chunk_bytes).await?,
                    KipCompAlg::Brotli => compressed = compress_brotli(chunk_bytes).await?,
                }
            } else {
                compressed.extend_from_slice(chunk_bytes);
            }
            // Encrypt chunk
            let encrypted = match encrypt(&compressed, secret) {
                Ok(ec) => ec,
                Err(e) => {
                    bail!("failed to encrypt chunk: {}.", e)
                }
            };
            // Get amount of bytes uploaded in this chunk
            // after compression and encryption
            let ce_bytes_len = encrypted.len();
            // Upload
            let smb_path = format!(
                "{}\{}\chunks\{}.chunk",
                self.destination.into(),
                job_id,
                chunk.hash
            );
            // Open file to write
            let mut writer =
                client.open_with(smb_path, SmbOpenOptions::default().create(true).write(true))?;
            // Write chunk
            let _ = io::copy(&mut chunk_bytes, &mut writer)?;
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

    async fn download(
        &self,
        f: &str,
        secret: &str,
        compress: KipCompressionOpts,
    ) -> Result<Vec<u8>> {
        // Setup SMB client
        let server = format!("{}:{}", self.server.ip(), self.server.port());
        let client = SmbClient::new(
            SmbCredentials::default()
                .server(server)
                .share(self.share)
                .username(self.username)
                .password(password)
                .workgroup(self.workgroup),
            SmbOptions::default().one_share_per_server(true),
        )?;
        // Read result from SMB and convert to bytes
        let result_bytes = match client.open_with(f, SmbOpenOptions::default().read(true)) {
            Ok(rb) => rb,
            Err(e) => {bail!("failed to read file from SMB: {}", e)}
        };
        // Decrypt result_bytes
        let decrypted = match decrypt(&bytes, secret) {
            Ok(dc) => dc,
            Err(e) => {
                bail!("failed to decrypt file: {}.", e)
            }
        };
        // Decompress decrypted bytes
        let mut decompressed = Vec::<u8>::new();
        if compress.enabled {
            match compress.alg {
                KipCompAlg::Zstd => decompressed = decompress_zstd(&decrypted).await?,
                KipCompAlg::Lzma => decompressed = decompress_lzma(&decrypted).await?,
                KipCompAlg::Gzip => decompressed = decompress_gzip(&decrypted).await?,
                KipCompAlg::Brotli => decompressed = decompress_brotli(&decrypted).await?,
            }
        } else {
            decompressed.extend_from_slice(&decrypted);
        }
        // Drop read lock on chunk
        drop(result_bytes);
        // Return downloaded & decrypted bytes
        Ok(decompressed)
    }
}
