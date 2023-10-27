//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::chunk::chunk_file;
use crate::chunk::{FileChunk, KipFileChunked};
use crate::compress::{
    compress_brotli, compress_gzip, compress_lzma, compress_zstd, decompress_brotli,
    decompress_gzip, decompress_lzma, decompress_zstd, KipCompressAlg, KipCompressOpts,
};
use crate::crypto::{decrypt, encrypt_bytes, encrypt_in_place};
use crate::job::{Job, KipFile, KipStatus};
use crate::providers::KipUploadOpts;
use crate::providers::{KipProvider, KipProviders};
use anyhow::{bail, Result};
use chrono::prelude::*;
use colored::*;
use crypto_hash::{hex_digest, Algorithm};
use futures::stream::FuturesUnordered;
use humantime::format_duration;
use linya::{Bar, Progress};
use memmap2::MmapOptions;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::sync::{mpsc::unbounded_channel, mpsc::UnboundedSender, Mutex};
use tracing::{debug, error, info, instrument, warn};
use walkdir::WalkDir;

// 50 MB
pub const MAX_OPEN_FILE_LEN: u64 = 50 * 1024 * 1024;
const CONCURRENT_FILE_UPLOADS: usize = 30;

/// A "Run" is a backup job with all the metadata
/// pertaining to the backed up files.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Run {
    pub id: u64,
    pub compress: KipCompressOpts,
    pub started: DateTime<Utc>,
    pub time_elapsed: String,
    pub finished: DateTime<Utc>,
    pub bytes_uploaded: u64,
    pub delta: Vec<KipFileChunked>,
    pub status: KipStatus,
    pub logs: Vec<String>,
    pub retain_forever: bool,
}

#[derive(Debug)]
pub enum KipUploadMsg {
    BytesUploaded(u64),
    KipFileChunked(KipFileChunked),
    Log(String),
    Error(String),
    GdriveParentFolder(String),
    Skipped,
    Done,
}

impl Run {
    pub fn new(id: u64, compress: KipCompressOpts) -> Self {
        // Initialize default UTC DateTime variable
        let time_init = match Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).earliest() {
            Some(t) => t,
            None => {
                let ndt = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                DateTime::<Utc>::from_utc(ndt, Utc)
            }
        };
        Self {
            id,
            compress,
            started: time_init,
            time_elapsed: String::from("0d 0h 0m 0s"),
            finished: time_init,
            bytes_uploaded: 0,
            delta: Vec::new(),
            status: KipStatus::NEVER_RUN,
            logs: Vec::<String>::new(),
            retain_forever: false,
        }
    }

    #[instrument]
    pub async fn start(&mut self, job: Job, secret: String, follow_links: bool) -> Result<()> {
        info!("START -- {}-{}", job.name, self.id);

        // Print job start
        let start_log = format!(
            "[{}] {}-{} ⇉ upload started.",
            Utc::now().format("%Y-%m-%d %H:%M:%S"),
            job.name,
            self.id,
        );
        self.logs.push(start_log.clone());
        println!("{start_log}");

        // Create progress bar context
        let progress = Arc::new(Mutex::new(Progress::new()));

        // Set run metadata
        self.started = Utc::now();
        self.status = KipStatus::IN_PROGRESS;
        let started = self.started;
        let mut warn: u32 = 0;
        let (upload_tx, mut upload_rx) = unbounded_channel::<KipUploadMsg>();

        // Create futures handle for each file iteration and join
        // at the end of this function to let for concurrent uploads
        let upload_queue = FuturesUnordered::new();

        // Rate limiting amount of concurrent uploads
        let semaphore = Arc::new(Semaphore::new(CONCURRENT_FILE_UPLOADS));

        // Check if file is excluded
        debug!("checking file exlusions");
        for kf in job.files.clone().into_iter() {
            if !job.excluded_files.is_empty() {
                for fe in job.excluded_files.iter() {
                    if fe.canonicalize()? == kf.path {
                        warn += 1;
                        let log = format!(
                            "[{}] {}-{} ⇉ '{}' is excluded from backups.",
                            Utc::now().format("%Y-%m-%d %H:%M:%S"),
                            job.name,
                            self.id,
                            kf.path_str().red(),
                        );
                        self.logs.push(log.clone());
                        println!("{log}");
                        warn!(warn, "file {} exlcuded from backup", kf.path_str());
                        continue;
                    }
                }
            }

            // Check if file type is excluded
            debug!("checking file extension exlcusions");
            if !job.excluded_file_types.is_empty() {
                for fte in job.excluded_file_types.iter() {
                    if let Some(ext) = kf.path.extension() {
                        let ext = ext.to_str().unwrap_or_default();
                        if fte == ext {
                            warn += 1;
                            let log = format!(
                                "[{}] {}-{} ⇉ '{fte}' file types are excluded from this backup.",
                                Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                job.name,
                                self.id,
                            );
                            self.logs.push(log.clone());
                            println!("{log}");
                            warn!(warn, "file extension .{fte} is excluded");
                            continue;
                        }
                    } else {
                        warn += 1;
                        let log = format!(
                            "[{}] {}-{} ⇉ unable to detect file extension for '{}'.",
                            Utc::now().format("%Y-%m-%d %H:%M:%S"),
                            job.name,
                            self.id,
                            kf.path_str(),
                        );
                        self.logs.push(log.clone());
                        println!("{log}");
                        warn!(warn, "cannot read file extension: {}", kf.path_str());
                        continue;
                    }
                }
            }

            // Check if file or directory exists
            debug!("confirming path exists");
            if !kf.path.exists() {
                warn += 1;
                let log = format!(
                    "[{}] {}-{} ⇉ '{}' can not be found.",
                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                    job.name,
                    self.id,
                    kf.path_str().red(),
                );
                self.logs.push(log.clone());
                println!("{log}");
                warn!(warn, "path is no longer available: {}", kf.path_str());
                continue;
            }

            // Check if f is file or directory
            debug!("confirming if file or directory");
            let fmd = kf.path.metadata()?;
            if fmd.is_file() {
                // Clones for future dispatch for this file
                let progress = Arc::clone(&progress);
                let job = job.clone();
                let secret = secret.clone();
                let run = self.clone();
                let upload_tx = upload_tx.clone();

                // Semaphore rate limiting
                let permit = semaphore.clone().acquire_owned().await?;

                // Create the spawned future for this file
                debug!("upload file future created");
                let upload_file_task = tokio::task::spawn(async move {
                    match run
                        .start_inner(&kf.clone(), job, &secret, progress, upload_tx.clone())
                        .await
                    {
                        Ok(_) => {
                            info!("upload succedded: {}", kf.path_str());
                        }
                        Err(e) => {
                            error!("error during upload: {e}");
                            upload_tx
                                .send(KipUploadMsg::Error(e.to_string()))
                                .unwrap_or_else(|se| {
                                    error!(
                                        "error sending log: {e} to main thread -> send error: {se}"
                                    );
                                });
                        }
                    };
                    // Drop semaphore permit
                    drop(permit);
                });

                // Add file upload future join handler to vec
                // to be run at the same time later in this function
                upload_queue.push(upload_file_task);
                debug!("upload file pushed to task queue");
            } else if fmd.is_dir() {
                // If the listed file entry is a dir, use walkdir to
                // walk all the recursive directories as well. Upload
                // all files found within the directory.
                debug!("walking directory: {}", kf.path_str());
                for entry in WalkDir::new(&kf.path).follow_links(follow_links) {
                    let entry = entry?;
                    let entry_kf = KipFile::new(entry.path().to_path_buf())?;

                    // If a directory, skip since upload will create
                    // the parent folder by default
                    let fmd = entry.path().metadata()?;
                    if fmd.is_dir() {
                        debug!("is dir, continue walking");
                        continue;
                    }

                    // Clones for future dispatch for this file
                    let progress = Arc::clone(&progress);
                    let job = job.clone();
                    let secret = secret.clone();
                    let run = self.clone();
                    let upload_tx = upload_tx.clone();

                    // Semaphore rate limiting
                    let permit = semaphore.clone().acquire_owned().await?;

                    // Create the spawned future for this file
                    debug!("upload directory file future created");
                    let upload_dir_file_future = tokio::task::spawn(async move {
                        match run
                            .start_inner(&entry_kf, job, &secret, progress, upload_tx.clone())
                            .await
                        {
                            Ok(_) => {
                                info!("upload succedded: {}", &entry_kf.path.display());
                            }
                            Err(e) => {
                                error!("error during upload: {e}");
                                upload_tx
                                    .send(KipUploadMsg::Error(e.to_string()))
                                    .unwrap_or_else(|se| {
                                        error!("error sending log: {e} to main thread -> send error: {se}");
                                    });
                            }
                        };
                        // Drop semaphore permit
                        drop(permit);
                    });

                    // Add file upload future join handler to vec
                    // to be run at the same time later in this function
                    upload_queue.push(upload_dir_file_future);
                    debug!("upload directory file future pushed to task queue");
                }
            }
        }
        // Join (execute) all file upload futures and wait for them
        // to finish here
        debug!("joining all upload futures");
        let upload_queue_count = upload_queue.len();
        futures::future::join_all(upload_queue).await;

        let mut err: u32 = 0;
        let mut finished_futures = 0;
        let mut skipped: usize = 0;
        let mut no_changes = false;
        while let Some(msg) = upload_rx.recv().await {
            match msg {
                KipUploadMsg::BytesUploaded(bu) => {
                    self.bytes_uploaded += bu;
                }
                KipUploadMsg::KipFileChunked(mut kfc) => {
                    match &job.provider {
                        KipProviders::S3(_) => {
                            for chunk in kfc.chunks.iter_mut() {
                                if chunk.remote_path.is_empty() {
                                    chunk.set_remote_path(&format!(
                                        "{}/chunks/{}.chunk",
                                        job.id, chunk.hash,
                                    ));
                                }
                            }
                        }
                        KipProviders::Usb(_) => {
                            for chunk in kfc.chunks.iter_mut() {
                                if chunk.remote_path.is_empty() {
                                    chunk.set_remote_path(&format!(
                                        "{}/chunks/{}.chunk",
                                        job.id, chunk.hash,
                                    ));
                                }
                            }
                        }
                        KipProviders::Gdrive(_) => {
                            todo!("a little more complicated")
                        }
                    }
                    self.delta.push(kfc);
                }
                KipUploadMsg::Log(l) => {
                    self.logs.push(l);
                }
                KipUploadMsg::Error(e) => {
                    err += 1;
                    eprintln!("{e}");
                    error!(err, "{e}");
                    self.logs.push(e);
                }
                KipUploadMsg::GdriveParentFolder(_gpf) => {
                    if let KipProviders::Gdrive(ref _gd) = job.provider {
                        //gd.parent_folder = Some(gpf);
                    }
                }
                KipUploadMsg::Skipped => {
                    skipped += 1;
                    if skipped == upload_queue_count {
                        no_changes = true;
                        break;
                    }
                }
                KipUploadMsg::Done => {
                    finished_futures += 1;
                    if finished_futures == upload_queue_count {
                        break;
                    }
                }
            }
        }

        // Finished! Set the run metadata before returning
        debug!("setting finished run metadata");
        self.finished = Utc::now();
        let dur = self.finished.signed_duration_since(started).to_std()?;
        self.time_elapsed = format_duration(dur).to_string();
        if !no_changes {
            if err == 0 && warn == 0 {
                self.status = KipStatus::OK;
            } else if warn > 0 && err == 0 {
                self.status = KipStatus::WARN;
            } else {
                self.status = KipStatus::ERR;
            }
        } else {
            self.status = KipStatus::OK_SKIPPED;
        }

        // Print the run's logs
        let fin_log = format!(
            "[{}] {}-{} ⇉ upload completed.",
            Utc::now().format("%Y-%m-%d %H:%M:%S"),
            job.name,
            self.id,
        );
        self.logs.push(fin_log.clone());
        println!("{fin_log}");
        info!("START done -- {}-{}", job.name, self.id);
        Ok(())
    }

    #[instrument]
    async fn start_inner(
        &self,
        f: &KipFile,
        job: Job,
        secret: &str,
        progress: Arc<Mutex<Progress>>,
        tx: UnboundedSender<KipUploadMsg>,
    ) -> Result<()> {
        info!(
            "START_INNER start -- {}-{} -- {}",
            job.name,
            self.id,
            f.path.display()
        );

        // Open the file
        let file = open_file(&f.path, f.len.try_into()?).await?;

        // If hash is the same and no chunks are missing from S3
        // skip uploading this file
        debug!("comparing chunk's hash");
        let hash_ok = f.hash == hex_digest(Algorithm::SHA256, &file);
        if hash_ok {
            let log = format!(
                "[{}] {}-{} ⇉ '{}' skipped, no changes found.",
                Utc::now().format("%Y-%m-%d %H:%M:%S"),
                job.name,
                self.id,
                f.path.display().to_string().yellow(),
            );
            tx.send(KipUploadMsg::Log(log.clone()))?;
            tx.send(KipUploadMsg::Skipped)?;
            debug!("no changes found");
            println!("{log}");
        } else {
            // Create progress bar
            let progress_cancel = Arc::clone(&progress);
            let bar: Bar = progress.lock().await.bar(
                1, // panics when set to 0
                format!(
                    "{}-{} ⇉ uploading '{}'",
                    job.name,
                    self.id,
                    f.path.file_name().unwrap_or_default().to_string_lossy(),
                ),
            );

            // Encrypt the whole file
            let encrypted_file = encrypt_and_compress(&file, secret, self.compress).await?;

            // Show progress bar
            progress
                .lock()
                .await
                .set_total_and_draw(&bar, encrypted_file.len());

            // Check if all file chunks are already in provider
            // to avoid overwite and needless upload
            debug!("chunking file: {}", f.path.display());
            let (kcf, chunks) =
                chunk_file(&f.path, f.hash.to_owned(), f.len, &encrypted_file).await?;

            //debug!("checking provider for missing chunks");
            //let mut chunks_missing: u32 = 0;
            //for chunk in kcf.chunks.iter() {
            //    match &job.provider {
            //        KipProviders::S3(s3) => {
            //            if !s3.contains(job.id, &chunk.hash).await? {
            //                debug!(
            //                    "missing chunk {}; total missing: {chunks_missing}",
            //                    &chunk.hash
            //                );
            //                chunks_missing += 1;
            //            };
            //        }
            //        KipProviders::Usb(usb) => {
            //            if !usb.contains(job.id, &chunk.hash).await? {
            //                debug!(
            //                    "missing chunk {}; total missing: {chunks_missing}",
            //                    &chunk.hash
            //                );
            //                chunks_missing += 1;
            //            };
            //        }
            //        KipProviders::Gdrive(gdrive) => {
            //            if !gdrive.contains(job.id, &chunk.hash).await? {
            //                debug!(
            //                    "missing chunk {}; total missing: {chunks_missing}",
            //                    &chunk.hash
            //                );
            //                chunks_missing += 1;
            //            };
            //        }
            //    }
            //}

            // Upload to the provider for this job
            // Either S3, Gdrive, or USB
            for (chunk, chunk_bytes) in chunks {
                match job.provider {
                    KipProviders::S3(ref s3) => {
                        // Create S3 client
                        let s3_conf =
                            aws_config::from_env()
                                .region(aws_sdk_s3::config::Region::new(s3.aws_region.clone()))
                                .credentials_cache(
                                    aws_credential_types::cache::CredentialsCache::lazy(),
                                )
                                .load()
                                .await;
                        let s3_client = aws_sdk_s3::Client::new(&s3_conf);

                        debug!("starting S3 upload");
                        match s3
                            .upload(
                                Some(&s3_client),
                                KipUploadOpts::new(job.id, tx.clone()),
                                &chunk,
                                chunk_bytes,
                            )
                            .await
                        {
                            Ok(bu) => {
                                // Increment progress bar by chunk bytes len
                                progress.lock().await.inc_and_draw(&bar, bu);
                                // Increment run's uploaded bytes
                                tx.send(KipUploadMsg::BytesUploaded(bu.try_into()?))?;
                                // Push logs
                                tx.send(KipUploadMsg::Log(format!(
                                    "[{}] {}-{} ⇉ '{}' ({}) uploaded successfully to '{}'.",
                                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                    job.name,
                                    self.id,
                                    f.name.green(),
                                    chunk.hash,
                                    s3.aws_bucket,
                                )))?;
                            }
                            Err(e) => {
                                // Cancel progress bar
                                progress_cancel.lock().await.cancel(bar);
                                // Push logs
                                tx.send(KipUploadMsg::Error(format!(
                                    "[{}] {}-{} ⇉ '{}' ({}) upload failed: {e}.",
                                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                    job.name,
                                    self.id,
                                    f.name.green(),
                                    chunk.hash,
                                )))?;
                                bail!(e);
                            }
                        };
                    }
                    KipProviders::Usb(ref usb) => {
                        debug!("starting USB upload");
                        match usb
                            .upload(
                                None,
                                KipUploadOpts::new(job.id, tx.clone()),
                                &chunk,
                                chunk_bytes,
                            )
                            .await
                        {
                            Ok(bu) => {
                                // Increment progress bar by chunk bytes len
                                progress.lock().await.inc_and_draw(&bar, bu);
                                // Increment run's uploaded bytes
                                tx.send(KipUploadMsg::BytesUploaded(bu.try_into()?))?;
                                // Push logs
                                tx.send(KipUploadMsg::Log(format!(
                                    "[{}] {}-{} ⇉ '{}' ({}) uploaded successfully to '{}'.",
                                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                    job.name,
                                    self.id,
                                    f.name.green(),
                                    chunk.hash,
                                    usb.name,
                                )))?;
                            }
                            Err(e) => {
                                // Cancel progress bar
                                progress_cancel.lock().await.cancel(bar);
                                // Push logs
                                tx.send(KipUploadMsg::Error(format!(
                                    "[{}] {}-{} ⇉ '{}' ({}) upload failed: {e}.",
                                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                    job.name,
                                    self.id,
                                    f.name.green(),
                                    chunk.hash,
                                )))?;
                                bail!(e);
                            }
                        };
                    }
                    KipProviders::Gdrive(ref gdrive) => {
                        // Generate Google Drive Hub
                        let hub = crate::providers::gdrive::generate_gdrive_hub().await?;

                        debug!("starting Gdrive upload");
                        match gdrive
                            .upload(
                                Some(&hub),
                                KipUploadOpts::new(job.id, tx.clone()),
                                &chunk,
                                chunk_bytes,
                            )
                            .await
                        {
                            Ok(bu) => {
                                // Increment progress bar by chunk bytes len
                                progress.lock().await.inc_and_draw(&bar, bu);
                                // Increment run's uploaded bytes
                                tx.send(KipUploadMsg::BytesUploaded(bu.try_into()?))?;
                                // Push logs
                                tx.send(KipUploadMsg::Log(format!(
                                    "[{}] {}-{} ⇉ '{}' ({}) uploaded successfully to Google Drive.",
                                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                    job.name,
                                    self.id,
                                    f.name.green(),
                                    chunk.hash,
                                )))?;
                            }
                            Err(e) => {
                                // Cancel progress bar
                                progress_cancel.lock().await.cancel(bar);
                                // Push logs
                                tx.send(KipUploadMsg::Error(format!(
                                    "[{}] {}-{} ⇉ '{}' ({}) upload failed: {e}.",
                                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                    job.name,
                                    self.id,
                                    f.name.green(),
                                    chunk.hash,
                                )))?;
                                bail!(e);
                            }
                        };
                    }
                }
            }
            // Add completed file
            tx.send(KipUploadMsg::KipFileChunked(kcf))?;
        }

        info!(
            "START_INNER done -- {}-{} -- {}",
            job.name,
            self.id,
            f.path.display()
        );
        // Send done message
        tx.send(KipUploadMsg::Done)?;
        Ok(())
    }

    #[instrument]
    pub async fn restore(&self, job: &Job, secret: &str, output_folder: &str) -> Result<()> {
        println!(
            "[{}] {}-{} ⇉ restore started.",
            Utc::now().format("%Y-%m-%d %H:%M:%S"),
            job.name,
            self.id,
        );

        // Confirm delta is not nil
        if self.delta.is_empty() {
            bail!("nothing to restore, no files were changed on this run.")
        }

        // For each object in the bucket, download it
        let mut counter: u64 = 0;
        for kfc in self.delta.iter() {
            let local_path = kfc.file.path.display().to_string();

            if kfc.is_single_chunk() {
                // Download chunk
                let chunk_bytes = match &job.provider {
                    KipProviders::S3(s3) => match s3.download(&kfc.chunks[0].remote_path).await {
                        Ok(cb) => cb,
                        Err(e) => {
                            let log = format!(
                                "[{}] {}-{} ⇉ '{}' restore failed. ({counter}/{})",
                                Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                job.name,
                                self.id,
                                local_path.red(),
                                self.delta.len(),
                            );
                            error!("{log}: {e}");
                            eprintln!("{log}");
                            continue;
                        }
                    },
                    KipProviders::Gdrive(gdrive) => {
                        // Gdrive takes file ID, not path
                        match gdrive.download(&kfc.chunks[0].remote_path).await {
                            Ok(cb) => cb,
                            Err(e) => {
                                let log = format!(
                                    "[{}] {}-{} ⇉ '{}' restore failed. ({counter}/{})",
                                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                    job.name,
                                    self.id,
                                    local_path.red(),
                                    self.delta.len(),
                                );
                                error!("{log}: {e}");
                                eprintln!("{log}");
                                continue;
                            }
                        }
                    }
                    KipProviders::Usb(usb) => {
                        match usb.download(&kfc.chunks[0].remote_path).await {
                            Ok(cb) => cb,
                            Err(e) => {
                                let log = format!(
                                    "[{}] {}-{} ⇉ '{}' restore failed. ({counter}/{})",
                                    Utc::now().format("%y-%m-%d %h:%m:%s"),
                                    job.name,
                                    self.id,
                                    local_path.red(),
                                    self.delta.len(),
                                );
                                error!("{log}: {e}");
                                eprintln!("{log}");
                                continue;
                            }
                        }
                    }
                };
                // Decrypt before decompression (if enabled)
                let decrypted = decrypt_decompress(&chunk_bytes, secret, self.compress).await?;
                // If a single-chunk file, simply decrypt and write
                let mut cfile = create_file(&kfc.file.path, output_folder)?;
                cfile.write_all(&decrypted)?;
            } else {
                // Create anon mmap to temporarily store chunks
                // during file assembly before writing to disk
                let mut multi_chunks = HashMap::<FileChunk, Vec<u8>>::new();
                let mut chunks_len: usize = 0;
                // Download all chunks
                for chunk in kfc.chunks.iter() {
                    let chunk_bytes = match &job.provider {
                        KipProviders::S3(s3) => match s3.download(&chunk.remote_path).await {
                            Ok(cb) => cb,
                            Err(e) => {
                                error!("error downloading chunk {}: {e}", &chunk.remote_path);
                                vec![]
                            }
                        },
                        KipProviders::Gdrive(gdrive) => {
                            // Gdrive takes file ID, not path
                            match gdrive.download(&chunk.remote_path).await {
                                Ok(cb) => cb,
                                Err(e) => {
                                    error!("error downloading chunk {}: {e}", &chunk.remote_path);
                                    vec![]
                                }
                            }
                        }
                        KipProviders::Usb(usb) => match usb.download(&chunk.remote_path).await {
                            Ok(cb) => cb,
                            Err(e) => {
                                error!("error downloading chunk {}: {e}", &chunk.remote_path);
                                vec![]
                            }
                        },
                    };
                    // Ruh-roh, chunk bytes shouldn't be empty,
                    // download failed
                    if chunk_bytes.is_empty() {
                        let log = format!(
                            "[{}] {}-{} ⇉ '{}' chunk download failed. ({counter}/{})",
                            Utc::now().format("%y-%m-%d %h:%m:%s"),
                            job.name,
                            self.id,
                            chunk.hash.red(),
                            self.delta.len(),
                        );
                        error!("{log}");
                        eprintln!("{log}");
                        break;
                    }
                    // Seeks to the offset where this chunked data
                    // segment begins and write it to completion
                    chunks_len += chunk_bytes.len();
                    multi_chunks.insert(chunk.clone(), chunk_bytes);
                    debug!("chunk written to offset {}", chunk.offset);
                }

                // Error downloading or assembling chunk bytes,
                // vec is empty
                if multi_chunks.is_empty() {
                    let log = format!(
                        "[{}] {}-{} ⇉ '{}' file assembly failed. ({counter}/{})",
                        Utc::now().format("%y-%m-%d %h:%m:%s"),
                        job.name,
                        self.id,
                        kfc.file.path.display().to_string().red(),
                        self.delta.len(),
                    );
                    error!("{log}");
                    eprintln!("{log}");
                    continue;
                }

                // Decrypt before decompression (if enabled)
                debug!("decrypting and decompressing restored file");
                let mut tvec = Vec::with_capacity(chunks_len);
                let mut cursor = Cursor::new(&mut tvec);
                for (chk, cb) in multi_chunks.iter() {
                    cursor.seek(SeekFrom::Start(chk.offset.try_into()?))?;
                    cursor.write_all(cb)?;
                }
                let decrypted = decrypt_decompress(&mut tvec[..], secret, self.compress).await?;
                // Hash the restored file and compare it to
                // the original KipFile hash
                debug!("comparing hash with the original file's hash");
                if hex_digest(Algorithm::SHA256, &decrypted) != kfc.file.hash {
                    let log = format!(
                        "[{}] {}-{} ⇉ '{}' restore failed. ({counter}/{})",
                        Utc::now().format("%Y-%m-%d %H:%M:%S"),
                        job.name,
                        self.id,
                        local_path.red(),
                        self.delta.len(),
                    );
                    error!("{log}: restored hash did not match original file hash");
                    eprintln!("{log}");
                    continue;
                }
                // Creates or opens restored file
                debug!("creating or opening file");
                let mut cfile = create_file(&kfc.file.path, output_folder)?;
                cfile.write_all(&decrypted)?;
                debug!("flushing to disk");
                cfile.flush()?;
            }

            // Increment file resote counter
            counter += 1;
            println!(
                "[{}] {}-{} ⇉ '{}' restored successfully. ({counter}/{})",
                Utc::now().format("%Y-%m-%d %H:%M:%S"),
                job.name,
                self.id,
                local_path.green(),
                self.delta.len(),
            );
        }
        Ok(())
    }
}

/// Find all chunks associated with the same path
/// and combine them in order according to their offsets and
/// lengths and then save to the original local path
// fn assemble_chunks(
//     chunks: &HashMap<&FileChunk, Vec<u8>>,
//     local_path: &Path,
//     output_folder: &str,
// ) -> Result<()> {
//     // Creates or opens file
//     let mut cfile = create_file(local_path, output_folder)?;
//     // Write the file
//     for (chunk, chunk_bytes) in chunks {
//         // Only write chunks that match the local path
//         if chunk.local_path != *local_path {
//             continue;
//         }
//         // Seeks to the offset where this chunked data
//         // segment begins and write it to completion
//         cfile.seek(SeekFrom::Start(chunk.offset.try_into()?))?;
//         cfile.write_all(chunk_bytes)?;
//     }
//     Ok(())
// }

/// Creates a restored file and its parent folders while
/// properly handling file prefixes depending on the running OS.
fn create_file(path: &Path, output_folder: &str) -> Result<File> {
    // Only strip prefix if path has a prefix
    let mut correct_chunk_path = path;
    if !cfg!(windows) && path.starts_with("/") {
        correct_chunk_path = path.strip_prefix("/")?;
    }
    let folder_path = Path::new(&output_folder).join(correct_chunk_path);
    let folder_parent = folder_path.parent().unwrap_or(&folder_path);
    create_dir_all(folder_parent)?;
    // Create the file
    let cfile = OpenOptions::new()
        .write(true)
        .create(true)
        .read(true)
        .truncate(true)
        .open(folder_path)?;
    Ok(cfile)
}

async fn encrypt_and_compress(
    bytes: &[u8],
    secret: &str,
    compress: KipCompressOpts,
) -> Result<Vec<u8>> {
    // Always compress before encryption (if enabled)
    let encrypted = if compress.enabled {
        let compressed = match compress.alg {
            KipCompressAlg::Zstd => compress_zstd(compress.level, bytes).await?,
            KipCompressAlg::Lzma => compress_lzma(compress.level, bytes).await?,
            KipCompressAlg::Gzip => compress_gzip(compress.level, bytes).await?,
            KipCompressAlg::Brotli => compress_brotli(compress.level, bytes).await?,
        };
        // Encrypt compressed chunk bytes
        debug!("encrypting compressed vec in place");
        match encrypt_in_place(compressed, secret) {
            Ok(ec) => ec,
            Err(e) => {
                bail!("failed to encrypt chunk: {e}")
            }
        }
    } else {
        // Encrypt chunk bytes
        debug!("encrpting bytes without compression");
        match encrypt_bytes(bytes, secret) {
            Ok(ec) => ec,
            Err(e) => {
                bail!("failed to encrypt chunk: {e}")
            }
        }
    };
    Ok(encrypted)
}

pub async fn decrypt_decompress(
    bytes: &[u8],
    secret: &str,
    compress: KipCompressOpts,
) -> Result<Vec<u8>> {
    // Decrypt before decompression (if enabled)
    let decrypted = if compress.enabled {
        // Decrypt downloaded chunk bytes
        let decrypted = match decrypt(bytes, secret) {
            Ok(ec) => ec,
            Err(e) => bail!("failed to decrypt chunk: {e}"),
        };
        match compress.alg {
            KipCompressAlg::Zstd => decompress_zstd(&decrypted).await?,
            KipCompressAlg::Lzma => decompress_lzma(&decrypted).await?,
            KipCompressAlg::Gzip => decompress_gzip(&decrypted).await?,
            KipCompressAlg::Brotli => decompress_brotli(&decrypted).await?,
        }
    } else {
        // Decrypt chunk bytes
        match decrypt(bytes, secret) {
            Ok(ec) => ec,
            Err(e) => bail!("failed to decrypt chunk: {e}"),
        }
    };
    Ok(decrypted)
}

pub async fn open_file(path: &Path, file_len: u64) -> Result<Vec<u8>> {
    // Open the file
    let file = if file_len > MAX_OPEN_FILE_LEN {
        debug!("opening {} with mmap", path.display());
        // SAFETY: unsafe used here for mmap
        let mmap = unsafe { MmapOptions::new().populate().map(&File::open(path)?)? };
        mmap.to_vec()
    } else {
        debug!("opening {} with tokio", path.display());
        tokio::fs::read(path).await?
    };
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::compress::{KipCompressAlg, KipCompressLevel, KipCompressOpts};
    use std::fs::read;
    use std::path::PathBuf;
    use tempfile::tempdir;

    // #[test]
    // fn test_is_single_chunk() {
    //     let mut r = Run::new(
    //         9998,
    //         KipCompressOpts::new(true, KipCompressAlg::Zstd, KipCompressLevel::Default),
    //     );
    //     let content_result = read(&PathBuf::from("test/random.txt"));
    //     assert!(content_result.is_ok());
    //     let contents = content_result.unwrap();
    //     let chunk_hmap_result =
    //         chunk_file(PathBuf::from("test/random.txt"), String::new(), &contents);
    //     assert!(chunk_hmap_result.is_ok());
    //     let (_, chunk_hmap) = chunk_hmap_result.unwrap();
    //     let mut t = vec![];
    //     let mut fc = FileChunk::new(&Path::new("test/random.txt"), "", 0, 0, 0);
    //     for c in chunk_hmap {
    //         t.push(c.0.clone());
    //         fc = c.0;
    //     }
    //     for cc in t {
    //         r.delta.push(cc);
    //     }
    //     assert!(r.is_single_chunk(&fc))
    // }

    // #[test]
    // fn test_is_not_single_chunk() {
    //     let mut r = Run::new(
    //         9999,
    //         KipCompressOpts::new(true, KipCompressAlg::Zstd, KipCompressLevel::Default),
    //     );
    //     let content_result = read(&PathBuf::from("test/kip"));
    //     assert!(content_result.is_ok());
    //     let contents = content_result.unwrap();
    //     let chunk_hmap_result = chunk_file(PathBuf::from("test/kip"), String::new(), &contents);
    //     assert!(chunk_hmap_result.is_ok());
    //     let (_, chunk_hmap) = chunk_hmap_result.unwrap();
    //     let mut t = vec![];
    //     let mut fc = FileChunk::new(&Path::new("test/kip"), "", 0, 0, 0);
    //     for c in chunk_hmap {
    //         t.push(c.0.clone());
    //         fc = c.0;
    //     }
    //     for cc in t {
    //         r.delta.push(cc);
    //     }
    //     assert!(!r.is_single_chunk(&fc))
    // }

    // #[test]
    // fn test_assemble_chunks() {
    //     // Chunk test file
    //     let read_result = read(Path::new("test/kip"));
    //     assert!(read_result.is_ok());
    //     let contents = read_result.unwrap();
    //     let (_, chunks) = chunk_file(PathBuf::from("test/kip"), String::new(), &contents);
    //     // Create temp dir for testing
    //     let tmp_dir = tempdir();
    //     assert!(tmp_dir.is_ok());
    //     let tmp_dir = tmp_dir.unwrap();
    //     let dir = tmp_dir.path().to_str().unwrap();
    //     // Convert chunks from HashMap to Vec for reassembly
    //     let mut multi_chunks = HashMap::new();
    //     for (chunk, chunk_bytes) in chunks.iter() {
    //         multi_chunks.insert(chunk, chunk_bytes.to_vec());
    //     }
    //     assert_eq!(multi_chunks.len(), 4);
    //     // Time to assemble
    //     let result = assemble_chunks(&multi_chunks, Path::new("kip"), dir);
    //     assert!(result.is_ok());
    //     // Compare restored file with original
    //     let test_result = read(tmp_dir.path().join("kip"));
    //     assert!(test_result.is_ok());
    //     let test_contents = test_result.unwrap();
    //     assert_eq!(contents, test_contents);
    //     // Destroy temp dir
    //     let dir_result = tmp_dir.close();
    //     assert!(dir_result.is_ok())
    // }

    // #[test]
    // fn test_assemble_chunks() {
    //     // Chunk test file
    //     let read_result = read(Path::new("test/kip"));
    //     assert!(read_result.is_ok());
    //     let contents = read_result.unwrap();
    //     let hash = hex_digest(Algorithm::SHA256, &contents);
    //     let len = contents.len();
    //     let (kfc, chunks) = chunk_file(PathBuf::from("test/kip"), hash, len, &contents);
    //     // Create temp dir for testing
    //     let tmp_dir = tempdir();
    //     assert!(tmp_dir.is_ok());
    //     let tmp_dir = tmp_dir.unwrap();
    //     let dir = tmp_dir.path().to_str().unwrap();
    //     // Creates or opens restored file
    //     let mut cfile = create_file(&kfc.file.path, dir).unwrap();
    //     // Create anon mmap to temporarily store chunks
    //     // during file assembly before writing to disk
    //     let mut mmap = MmapOptions::new().len(kfc.file.len).map_anon().unwrap();
    //     mmap.advise(memmap2::Advice::Random).unwrap();
    //     for chunk in kfc.chunks.iter() {
    //         // Seeks to the offset where this chunked data
    //         // segment begins and write it to completion
    //         let mut mmap_cursor = Cursor::new(&mut mmap[..]);
    //         mmap_cursor
    //             .seek(SeekFrom::Start(chunk.offset.try_into().unwrap()))
    //             .unwrap();
    //         // cfile.seek(SeekFrom::Start(chunk.offset.try_into()?));
    //         mmap_cursor.write_all(&chunks[chunk]).unwrap();
    //         // mmap.deref_mut().write_all(&chunk_bytes)?;
    //         // cfile.write_all(&chunk_bytes)?;
    //     }
    //     mmap.advise(memmap2::Advice::Sequential).unwrap();
    //     // Decrypt before decompression (if enabled)
    //     let decrypted = decrypt_decompress(&mmap[..], secret, self.compress)
    //         .await
    //         .unwrap();
    //     drop(mmap);
    //     // Hash the restored file and compare it to
    //     // the original KipFile hash
    //     assert_eq!(hex_digest(Algorithm::SHA256, &decrypted), kfc.file.hash)
    //     // Seek to beginning of mmap and write (flush) changes to disk
    //     //debug!("resetting mmap cursor to start of file");
    //     // Cursor::new(&mut mmap[..]).seek(SeekFrom::Start(0))?;
    //     // cfile.rewind()?;
    //     // mmap.deref_mut().write_all(&decrypted)?;
    //     cfile.write_all(&decrypted).unwrap();
    //     // mmap.flush_async()?;
    //     cfile.flush().unwrap();
    // }

    #[test]
    fn test_create_file() {
        // Create temp dir for testing
        let tmp_dir = tempdir();
        assert!(tmp_dir.is_ok());
        let tmp_dir = tmp_dir.unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // Create file
        let result = create_file(&PathBuf::from("test.txt"), dir);
        assert!(result.is_ok());
        let test_result = read(tmp_dir.path().join("test.txt"));
        assert!(test_result.is_ok());
        let exists = Path::new(&tmp_dir.path().join("test.txt")).exists();
        assert!(exists);
        // Destroy temp dir
        let dir_result = tmp_dir.close();
        assert!(dir_result.is_ok())
    }

    #[test]
    fn test_create_file_is_dir() {
        // Create temp dir for testing
        let tmp_dir = tempdir();
        assert!(tmp_dir.is_ok());
        let tmp_dir = tmp_dir.unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // Create file
        let result = create_file(&PathBuf::from("test/"), dir);
        assert!(result.is_err());
        // Destroy temp dir
        let dir_result = tmp_dir.close();
        assert!(dir_result.is_ok())
    }

    #[test]
    #[cfg_attr(target_os = "windows", ignore)]
    fn test_create_file_no_prefix() {
        // Create temp dir for testing
        let tmp_dir = tempdir();
        assert!(tmp_dir.is_ok());
        let tmp_dir = tmp_dir.unwrap();
        // Create file
        let path = &PathBuf::from("no_prefix/test.txt");
        let stripped_path = tmp_dir.path().strip_prefix("/");
        assert!(stripped_path.is_ok());
        let stripped_path = stripped_path.unwrap().display().to_string();
        let file_result = create_file(path, &stripped_path);
        assert!(file_result.is_ok());
        let exists_result = file_result.unwrap().metadata();
        assert!(exists_result.is_ok());
        let exists = exists_result.unwrap().is_file();
        assert!(exists);
        // Destroy temp dir
        let dir_result = tmp_dir.close();
        assert!(dir_result.is_ok())
    }

    #[test]
    #[cfg_attr(target_os = "windows", ignore)]
    fn test_create_file_prefix() {
        // Create temp dir for testing
        let tmp_dir = tempdir();
        assert!(tmp_dir.is_ok());
        let tmp_dir = tmp_dir.unwrap();
        // Create file
        let path = &PathBuf::from("/prefix/test.txt");
        let file_result = create_file(path, &tmp_dir.path().display().to_string());
        assert!(file_result.is_ok());
        let exists_result = file_result.unwrap().metadata();
        assert!(exists_result.is_ok());
        let exists = exists_result.unwrap().is_file();
        assert!(exists);
        // Destroy temp dir
        let dir_result = tmp_dir.close();
        assert!(dir_result.is_ok())
    }
}
