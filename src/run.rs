//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::chunk::{chunk_file, FileChunk};
use crate::job::{Job, KipFile, KipStatus};
use crate::providers::{s3::strip_hash_from_s3, KipProvider, KipProviders};
use anyhow::{bail, Result};
use async_compression::tokio::write::{ZstdDecoder, ZstdEncoder};
use chrono::prelude::*;
use colored::*;
use crypto_hash::{hex_digest, Algorithm};
use futures::stream::FuturesUnordered;
use humantime::format_duration;
use linya::{Bar, Progress};
use memmap2::MmapOptions;
use serde::{Deserialize, Serialize};
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::prelude::*;
use std::io::{SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Run {
    pub id: u64,
    pub started: DateTime<Utc>,
    pub time_elapsed: String,
    pub finished: DateTime<Utc>,
    pub bytes_uploaded: u64,
    pub files_changed: Vec<FileChunk>,
    pub status: KipStatus,
    pub logs: Vec<String>,
    pub retain_forever: bool,
}

impl Run {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            started: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            time_elapsed: String::from("0d 0h 0m 0s"),
            finished: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            bytes_uploaded: 0,
            files_changed: Vec::new(),
            status: KipStatus::NEVER_RUN,
            logs: Vec::<String>::new(),
            retain_forever: false,
        }
    }

    pub async fn start(&mut self, job: Job, secret: String) -> Result<()> {
        // Create progress bar context
        let progress = Arc::new(Mutex::new(Progress::new()));
        let start_log = format!(
            "[{}] {}-{} ⇉ upload started.",
            Utc::now().format("%Y-%m-%d %H:%M:%S"),
            job.name,
            self.id,
        );

        // Print job start
        self.logs.push(start_log.clone());
        println!("{}", start_log);

        // Set run metadata
        self.started = Utc::now();
        self.status = KipStatus::IN_PROGRESS;
        let started = self.started;
        let mut warn: u64 = 0;
        let bytes_uploaded: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
        let err = Arc::new(Mutex::new(Vec::<String>::new()));
        let upload_logs = Arc::new(Mutex::new(Vec::<String>::new()));
        let changed_file_chunks = Arc::new(Mutex::new(Vec::<FileChunk>::new()));

        // Create futures for each file iteration and join
        // at the end of this function to let for concurrent uploads
        let upload_futures = FuturesUnordered::new();
        for kf in job.files.clone().into_iter() {
            // Check if file is excluded
            if !job.excluded_files.is_empty() {
                for fe in job.excluded_files.iter() {
                    if fe.canonicalize()? == kf.path {
                        warn += 1;
                        let log = format!(
                            "[{}] {}-{} ⇉ '{}' is excluded from backups.",
                            Utc::now().format("%Y-%m-%d %H:%M:%S"),
                            job.name,
                            self.id,
                            kf.path.display().to_string().red(),
                        );
                        self.logs.push(log.clone());
                        println!("{}", log);
                        continue;
                    }
                }
            }

            // Check if file type is excluded
            if !job.excluded_file_types.is_empty() {
                for fte in job.excluded_file_types.iter() {
                    if let Some(ext) = kf.path.extension() {
                        let ext = ext.to_str().unwrap_or_default();
                        if fte == ext {
                            warn += 1;
                            let log = format!(
                                "[{}] {}-{} ⇉ '{}' file types are excluded from this backup.",
                                Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                job.name,
                                self.id,
                                fte,
                            );
                            self.logs.push(log.clone());
                            println!("{}", log);
                            continue;
                        }
                    } else {
                        warn += 1;
                        let log = format!(
                            "[{}] {}-{} ⇉ unable to detect file extension for '{}'.",
                            Utc::now().format("%Y-%m-%d %H:%M:%S"),
                            job.name,
                            self.id,
                            kf.path.display(),
                        );
                        self.logs.push(log.clone());
                        println!("{}", log);
                        continue;
                    }
                }
            }

            // Check if file or directory exists
            if !kf.path.exists() {
                warn += 1;
                let log = format!(
                    "[{}] {}-{} ⇉ '{}' can not be found.",
                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                    job.name,
                    self.id,
                    kf.path.display().to_string().red(),
                );
                self.logs.push(log.clone());
                println!("{}", log);
                continue;
            }

            // Check if f is file or directory
            let fmd = kf.path.metadata()?;
            if fmd.is_file() {
                // Clones for future dispatch for this file
                let progress = Arc::clone(&progress);
                let bytes_uploaded_arc = Arc::clone(&bytes_uploaded);
                let upload_logs_arc = Arc::clone(&upload_logs);
                let changed_file_chunks_arc = Arc::clone(&changed_file_chunks);
                let err_arc = Arc::clone(&err);
                let job = job.clone();
                let secret = secret.clone();
                let mut run = self.clone();
                let kf = kf.clone();

                // Create the spawned future for this file
                let upload_file_future = tokio::task::spawn(async move {
                    match run
                        .start_inner(&kf, fmd.len(), job, &secret, progress)
                        .await
                    {
                        Ok((logs, mut fc, bu)) => {
                            upload_logs_arc.lock().await.push(logs);
                            changed_file_chunks_arc.lock().await.append(&mut fc);
                            *bytes_uploaded_arc.lock().await += bu;
                        }
                        Err(e) => {
                            err_arc.lock().await.push(e.to_string());
                        }
                    };
                });

                // Add file upload future join handler to vec
                // to be run at the same time later in this function
                upload_futures.push(upload_file_future);
            } else if fmd.is_dir() {
                // If the listed file entry is a dir, use walkdir to
                // walk all the recursive directories as well. Upload
                // all files found within the directory.
                for entry in WalkDir::new(&kf.path).follow_links(true) {
                    let entry = entry?;
                    let entry_kf = KipFile::new(entry.path().to_path_buf());

                    // If a directory, skip since upload will
                    // create the parent folder by default
                    let fmd = entry.path().metadata()?;
                    if fmd.is_dir() {
                        continue;
                    }

                    // Clones for future dispatch for this file
                    let progress = Arc::clone(&progress);
                    let bytes_uploaded_arc = Arc::clone(&bytes_uploaded);
                    let upload_logs_arc = Arc::clone(&upload_logs);
                    let changed_file_chunks_arc = Arc::clone(&changed_file_chunks);
                    let err_arc = Arc::clone(&err);
                    let job = job.clone();
                    let secret = secret.clone();
                    let mut run = self.clone();

                    // Create the spawned future for this file
                    let upload_dir_file_future = tokio::task::spawn(async move {
                        match run
                            .start_inner(&entry_kf, fmd.len(), job, &secret, progress)
                            .await
                        {
                            Ok((logs, mut fc, bu)) => {
                                upload_logs_arc.lock().await.push(logs);
                                changed_file_chunks_arc.lock().await.append(&mut fc);
                                *bytes_uploaded_arc.lock().await += bu;
                            }
                            Err(e) => {
                                err_arc.lock().await.push(e.to_string());
                            }
                        };
                    });

                    // Add file upload future join handler to vec
                    // to be run at the same time later in this function
                    upload_futures.push(upload_dir_file_future);
                }
            }
        }
        // Join (run) all file upload futures and wait for them
        // all to finish here
        futures::future::join_all(upload_futures).await;

        // Finished! Set the run metadata before returning
        self.finished = Utc::now();
        let dur = self.finished.signed_duration_since(started).to_std()?;
        self.time_elapsed = format_duration(dur).to_string();
        self.bytes_uploaded = *bytes_uploaded.lock().await;
        self.logs.extend_from_slice(&*upload_logs.lock().await);
        self.files_changed
            .append(&mut *changed_file_chunks.lock().await);
        let err = &*err.lock().await;
        if err.is_empty() && warn == 0 {
            self.status = KipStatus::OK;
        } else if warn > 0 && err.is_empty() {
            self.status = KipStatus::WARN;
        } else {
            self.status = KipStatus::ERR;
            for e in &**err {
                eprintln!("{}", e);
            }
        }

        // Print the run's logs
        let fin_log = format!(
            "[{}] {}-{} ⇉ upload completed.",
            Utc::now().format("%Y-%m-%d %H:%M:%S"),
            job.name,
            self.id,
        );
        self.logs.push(fin_log.clone());
        println!("{}", fin_log);
        Ok(())
    }

    async fn start_inner(
        &mut self,
        f: &KipFile,
        file_len: u64,
        job: Job,
        secret: &str,
        progress: Arc<Mutex<Progress>>,
    ) -> Result<(String, Vec<FileChunk>, u64)> {
        let mut file_upload_log = String::new();
        let mut files_changed = vec![];
        // Before opening file, determine how large it is
        // If larger than 500 MB, mmap the sucker, if not, just read it
        let mut file = vec![];
        if file_len > (500 * 1024 * 1024) {
            // SAFETY: unsafe used here for mmap
            let mmap = unsafe { MmapOptions::new().populate().map(&File::open(&f.path)?)? };
            file.extend_from_slice(&mmap[..]);
        } else {
            file.extend_from_slice(&tokio::fs::read(&f.path).await?);
        }

        // Check if all file chunks are already in S3
        // to avoid overwite and needless upload
        let mut chunks_missing: u64 = 0;
        let chunks_map = chunk_file(&file);
        for (chunk, _) in chunks_map.iter() {
            match &job.provider {
                KipProviders::S3(s3) => {
                    if !s3.contains(job.id, &chunk.hash).await? {
                        chunks_missing += 1;
                    };
                }
                KipProviders::Usb(usb) => {
                    if !usb.contains(job.id, &chunk.hash).await? {
                        chunks_missing += 1;
                    };
                }
                KipProviders::Gdrive(gdrive) => {
                    if !gdrive.contains(job.id, &chunk.hash).await? {
                        chunks_missing += 1;
                    };
                }
            }
        }

        // If hash is the same and no chunks are missing from S3
        // skip uploading this file
        let mut bytes_uploaded: u64 = 0;
        let hash_ok = f.hash == hex_digest(Algorithm::SHA256, &file);
        if hash_ok && chunks_missing == 0 {
            let log = format!(
                "[{}] {}-{} ⇉ '{}' skipped, no changes found.",
                Utc::now().format("%Y-%m-%d %H:%M:%S"),
                job.name,
                self.id,
                f.path.display().to_string().yellow(),
            );
            file_upload_log = log.clone();
            println!("{}", log);
        } else {
            // Hash is not the same, lets upload these chunks
            // Arc clone progress bar
            let progress = Arc::clone(&progress);
            let progress_cancel = Arc::clone(&progress);
            // Create progress bar
            let bar: Bar = progress.lock().await.bar(
                file_len.try_into()?,
                format!(
                    "{}-{} ⇉ uploading '{}'",
                    job.name,
                    self.id,
                    f.path.display().to_string().cyan(),
                ),
            );

            // Upload to the provider for this job
            // Either S3, or USB
            match job.provider {
                KipProviders::S3(s3) => {
                    match s3
                        .upload(
                            &f.path.canonicalize()?,
                            chunks_map,
                            job.id,
                            secret,
                            progress,
                            &bar,
                        )
                        .await
                    {
                        Ok((chunked_file, bu)) => {
                            // Confirm the chunked file is not empty AKA
                            // this chunk has not been modified, skip it
                            if !chunked_file.is_empty() {
                                // Increase bytes uploaded for this run
                                bytes_uploaded += bu;
                                // Push chunks onto run's changed files
                                for c in chunked_file {
                                    files_changed.push(c);
                                }
                                // Push logs
                                file_upload_log = format!(
                                    "[{}] {}-{} ⇉ '{}' uploaded successfully to '{}'.",
                                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                    job.name,
                                    self.id,
                                    f.path.display().to_string().green(),
                                    s3.aws_bucket,
                                );
                            }
                        }
                        Err(e) => {
                            // Push logs
                            file_upload_log = format!(
                                "[{}] {}-{} ⇉ '{}' upload failed: {}.",
                                Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                job.name,
                                self.id,
                                f.path.display().to_string().red(),
                                e,
                            );
                            progress_cancel.lock().await.cancel(bar);
                        }
                    };
                }
                KipProviders::Usb(usb) => {
                    match usb
                        .upload(
                            &f.path.canonicalize()?,
                            chunks_map,
                            job.id,
                            secret,
                            progress,
                            &bar,
                        )
                        .await
                    {
                        Ok((chunked_file, bu)) => {
                            // Confirm the chunked file is not empty AKA
                            // this chunk has not been modified, skip it
                            if !chunked_file.is_empty() {
                                // Increase bytes uploaded for this run
                                bytes_uploaded += bu;
                                // Push chunks onto run's changed files
                                for c in chunked_file {
                                    files_changed.push(c);
                                }
                                // Push logs
                                file_upload_log = format!(
                                    "[{}] {}-{} ⇉ '{}' uploaded successfully to '{}'.",
                                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                    job.name,
                                    self.id,
                                    f.path.display().to_string().green(),
                                    usb.name,
                                );
                            }
                        }
                        Err(e) => {
                            // Push logs
                            file_upload_log = format!(
                                "[{}] {}-{} ⇉ '{}' upload failed: {}.",
                                Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                job.name,
                                self.id,
                                f.path.display().to_string().red(),
                                e,
                            );
                            progress_cancel.lock().await.cancel(bar);
                        }
                    };
                }
                KipProviders::Gdrive(gdrive) => {
                    match gdrive
                        .upload(
                            &f.path.canonicalize()?,
                            chunks_map,
                            job.id,
                            secret,
                            progress,
                            &bar,
                        )
                        .await
                    {
                        Ok((chunked_file, bu)) => {
                            // Confirm the chunked file is not empty AKA
                            // this chunk has not been modified, skip it
                            if !chunked_file.is_empty() {
                                // Increase bytes uploaded for this run
                                bytes_uploaded += bu;
                                // Push chunks onto run's changed files
                                for c in chunked_file {
                                    files_changed.push(c);
                                }
                                // Push logs
                                file_upload_log = format!(
                                    "[{}] {}-{} ⇉ '{}' uploaded successfully to Google Drive.",
                                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                    job.name,
                                    self.id,
                                    f.path.display().to_string().green(),
                                );
                            }
                        }
                        Err(e) => {
                            // Push logs
                            file_upload_log = format!(
                                "[{}] {}-{} ⇉ '{}' upload failed: {}.",
                                Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                job.name,
                                self.id,
                                f.path.display().to_string().red(),
                                e,
                            );
                            progress_cancel.lock().await.cancel(bar);
                        }
                    };
                }
            }
        }
        Ok((file_upload_log, files_changed, bytes_uploaded))
    }

    pub async fn restore(&self, job: &Job, secret: &str, output_folder: &str) -> Result<()> {
        println!(
            "[{}] {}-{} ⇉ restore started.",
            Utc::now().format("%Y-%m-%d %H:%M:%S"),
            job.name,
            self.id,
        );

        // Confirm files_changed is not nil
        if self.files_changed.is_empty() {
            bail!("nothing to restore, no files were changed on this run.")
        }

        // Get all bucket contents
        // This is done outside of the loops for performance reasons
        let mut s3_objs: Vec<aws_sdk_s3::model::Object> = vec![];
        let mut usb_objs: Vec<KipFile> = vec![];
        let mut gdrive_objs: Vec<google_drive3::api::File> = vec![];
        match &job.provider {
            KipProviders::S3(s3) => {
                s3_objs.append(&mut s3.list_all(job.id).await?);
            }
            KipProviders::Usb(usb) => {
                usb_objs.append(&mut usb.list_all(job.id).await?);
            }
            KipProviders::Gdrive(gdrive) => {
                gdrive_objs.append(&mut gdrive.list_all(job.id).await?);
            }
        };

        // For each object in the bucket, download it
        let mut counter: u64 = 0;
        'files: for fc in self.files_changed.iter() {
            // If file has multiple chunks, store them
            // here for re-assembly later
            let mut multi_chunks: Vec<(&FileChunk, Vec<u8>)> = vec![];
            match &job.provider {
                KipProviders::S3(s3) => {
                    // Check if this S3 object is within this run's changed files
                    's3: for ob in s3_objs.iter() {
                        let s3_key = match &ob.key {
                            Some(k) => k,
                            None => {
                                continue 's3;
                            }
                        };
                        // Strip the hash from S3 key
                        let hash = strip_hash_from_s3(s3_key)?;
                        if fc.hash != hash {
                            // S3 object not found in this run
                            continue 's3;
                        } else {
                            // Found a chunk for this file! Download
                            // this chunk
                            let local_path = fc.local_path.display().to_string().green();
                            // Download chunk
                            match s3.download(s3_key, secret).await {
                                Ok(chunk_bytes) => {
                                    // Determine if single or multi-chunk file
                                    if self.is_single_chunk(fc) {
                                        // Increment file resote counter
                                        counter += 1;
                                        // If a single-chunk file, simply decrypt and write
                                        let mut cfile = create_file(&fc.local_path, output_folder)?;
                                        cfile.write_all(&chunk_bytes)?;
                                        println!(
                                            "[{}] {}-{} ⇉ '{}' restored successfully. ({}/{})",
                                            Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                            job.name,
                                            self.id,
                                            local_path,
                                            counter,
                                            job.files_amt,
                                        );
                                        continue 'files;
                                    } else {
                                        // If a multi-chunk file pass to multi-chunk vec
                                        // for re-assembly after loop finishes
                                        multi_chunks.push((fc, chunk_bytes));
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "[{}] {}-{} ⇉ '{}' restore failed: {}. ({}/{})",
                                        Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                        job.name,
                                        self.id,
                                        local_path,
                                        e,
                                        counter,
                                        job.files_amt,
                                    );
                                }
                            }
                        }
                    }
                }
                KipProviders::Usb(usb) => {
                    // Check if this USB object is within this run's changed files
                    'usb: for ob in usb_objs.iter() {
                        if fc.hash != ob.hash {
                            // USB hash of object not found in this run
                            continue 'usb;
                        } else {
                            // Found a chunk for this file! Download
                            // this chunk
                            let local_path = fc.local_path.display().to_string().green();
                            // Download chunk
                            match usb.download(&ob.path.display().to_string(), secret).await {
                                Ok(chunk_bytes) => {
                                    // Determine if single or multi-chunk file
                                    if self.is_single_chunk(fc) {
                                        // Increment file resote counter
                                        counter += 1;
                                        // If a single-chunk file, simply decrypt and write
                                        let mut cfile = create_file(&fc.local_path, output_folder)?;
                                        cfile.write_all(&chunk_bytes)?;
                                        println!(
                                            "[{}] {}-{} ⇉ '{}' restored successfully. ({}/{})",
                                            Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                            job.name,
                                            self.id,
                                            local_path,
                                            counter,
                                            job.files_amt,
                                        );
                                        continue 'files;
                                    } else {
                                        // If a multi-chunk file pass to multi-chunk vec
                                        // for re-assembly after loop finishes
                                        multi_chunks.push((fc, chunk_bytes));
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "[{}] {}-{} ⇉ '{}' restore failed: {}. ({}/{})",
                                        Utc::now().format("%Y-%m-%d %H:%M:%S"),
                                        job.name,
                                        self.id,
                                        local_path,
                                        e,
                                        counter,
                                        job.files_amt,
                                    );
                                }
                            }
                        }
                    }
                }
                KipProviders::Gdrive(_) => {
                    unimplemented!();
                }
            }

            // Only run if multi_chunks is not empty
            if !multi_chunks.is_empty() {
                // Here, we sort all the chunks by thier offset and
                // combine the chunks and write the file
                assemble_chunks(&multi_chunks, &fc.local_path, output_folder)?;
                // Increment file resote counter
                counter += 1;
                // Print logs
                println!(
                    "[{}] {}-{} ⇉ '{}' restored successfully. ({}/{})",
                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                    job.name,
                    self.id,
                    &fc.local_path.display().to_string().green(),
                    counter,
                    job.files_amt,
                );
            }
        }
        Ok(())
    }

    // Checks if a certain file backed up in a specific run
    // was split into a single or multiple chunks.
    fn is_single_chunk(&self, fc: &FileChunk) -> bool {
        let mut count: usize = 0;
        for c in self.files_changed.iter() {
            if c.local_path == fc.local_path {
                count += 1;
            }
        }
        if count > 1 {
            return false;
        }
        true
    }
}

/// Find all chunks associated with the same path
/// and combine them in order according to their offsets and
/// lengths and then save to the original local path
fn assemble_chunks(
    chunks: &Vec<(&FileChunk, Vec<u8>)>,
    local_path: &Path,
    output_folder: &str,
) -> Result<()> {
    // Creates or opens file
    let mut cfile = create_file(local_path, output_folder)?;
    // Write the file
    for (chunk, chunk_bytes) in chunks {
        // Seeks to the offset where this chunked data
        // segment begins and write it to completion
        cfile.seek(SeekFrom::Start(chunk.offset.try_into()?))?;
        cfile.write_all(chunk_bytes)?;
    }
    Ok(())
}

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
    let cfile = if !folder_path.exists() {
        File::create(folder_path)?
    } else {
        OpenOptions::new().write(true).open(folder_path)?
    };
    Ok(cfile)
}

pub async fn compress(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = ZstdEncoder::new(vec![]);
    encoder.write_all(bytes).await?;
    encoder.shutdown().await?;
    Ok(encoder.into_inner())
}

pub async fn decompress(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = ZstdDecoder::new(vec![]);
    decoder.write_all(bytes).await?;
    decoder.shutdown().await?;
    Ok(decoder.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::chunk_file;
    use std::fs::read;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn test_is_single_chunk() {
        let mut r = Run::new(9998);
        let content_result = read(&PathBuf::from("test/random.txt"));
        assert!(content_result.is_ok());
        let contents = content_result.unwrap();
        let chunk_hmap = chunk_file(&contents);
        let mut t = vec![];
        let mut fc = FileChunk::new("", 0, 0, 0);
        for c in chunk_hmap {
            t.push(c.0.clone());
            fc = c.0;
        }
        for cc in t {
            r.files_changed.push(cc);
        }
        assert!(r.is_single_chunk(&fc))
    }

    #[test]
    fn test_is_not_single_chunk() {
        let mut r = Run::new(9999);
        let content_result = read(&PathBuf::from("test/kip"));
        assert!(content_result.is_ok());
        let contents = content_result.unwrap();
        let chunk_hmap = chunk_file(&contents);
        let mut t = vec![];
        let mut fc = FileChunk::new("", 0, 0, 0);
        for c in chunk_hmap {
            t.push(c.0.clone());
            fc = c.0;
        }
        for cc in t {
            r.files_changed.push(cc);
        }
        assert!(!r.is_single_chunk(&fc))
    }

    #[test]
    fn test_assemble_chunks() {
        use rand::seq::SliceRandom;
        use rand::thread_rng;

        // Chunk test file
        let read_result = read(&PathBuf::from("test/kip"));
        assert!(read_result.is_ok());
        let contents = read_result.unwrap();
        let chunks = chunk_file(&contents);
        // Create temp dir for testing
        let tmp_dir = tempdir();
        assert!(tmp_dir.is_ok());
        let tmp_dir = tmp_dir.unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // Convert chunks from HashMap to Vec for reassembly
        let mut multi_chunks: Vec<(&FileChunk, Vec<u8>)> = vec![];
        for (chunk, chunk_bytes) in chunks.iter().clone() {
            multi_chunks.push((chunk, chunk_bytes.to_vec()));
        }
        assert!(multi_chunks.len() > 1);
        // Shuffle multi_chunks to resemble real life scenario better
        // IRL, chunks will not be in order
        multi_chunks.shuffle(&mut thread_rng());
        // Time to assemble
        let result = assemble_chunks(&multi_chunks, &PathBuf::from("kip"), dir);
        assert!(result.is_ok());
        // Compare restored file with original
        let test_result = read(tmp_dir.path().join("kip"));
        assert!(test_result.is_ok());
        let test_contents = test_result.unwrap();
        assert_eq!(contents, test_contents);
        // Destroy temp dir
        let dir_result = tmp_dir.close();
        assert!(dir_result.is_ok())
    }

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
