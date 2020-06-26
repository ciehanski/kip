use crate::chunk::FileChunk;
use crate::job::{Job, KipStatus};
use crate::s3::{list_s3_bucket, s3_download, s3_upload};
use chrono::prelude::*;
use colored::*;
use crypto_hash::{hex_digest, Algorithm};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::{metadata, File};
use std::io::prelude::*;
use std::path::Path;
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Run {
    pub id: usize,
    pub started: DateTime<Utc>,
    pub time_elapsed: String,
    pub finished: DateTime<Utc>,
    pub bytes_uploaded: usize,
    pub files_changed: Vec<HashMap<String, FileChunk>>,
    pub status: KipStatus,
    pub logs: Vec<String>,
}

impl Run {
    pub fn new(id: usize) -> Self {
        Run {
            id,
            started: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            time_elapsed: String::from("0d 0h 0m 0s"),
            finished: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            bytes_uploaded: 0,
            files_changed: Vec::new(),
            status: KipStatus::NEVER_RUN,
            logs: Vec::<String>::new(),
        }
    }

    pub async fn upload(&mut self, job: &Job, secret: &str) -> Result<(), Box<dyn Error>> {
        // Set run metadata
        self.started = Utc::now();
        self.status = KipStatus::IN_PROGRESS;
        // TODO: S3 by default will allow 10 concurrent requests
        // via ThreadPool I guess?
        let mut warn: usize = 0;
        let mut err: usize = 0;
        let mut bytes_uploaded: usize = 0;
        for f in job.files.iter() {
            // Check if file or directory exists
            if !f.path.exists() {
                warn += 1;
                self.logs.push(format!(
                    "[{}] {}-{} ⇉ '{}' can not be found.",
                    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                    job.name,
                    self.id,
                    f.path.display().to_string().red(),
                ));
                continue;
            }
            // Check if f is file or directory
            let fmd = metadata(&f.path)?;
            if fmd.is_file() {
                // Get file hash and compare with stored file hash in
                // KipFile. If the same, continue the loop
                let c = std::fs::read(&f.path)?.to_vec();
                let digest = hex_digest(Algorithm::SHA256, &c);
                if f.hash == digest {
                    continue;
                }
                // Upload
                match s3_upload(
                    &f.path.canonicalize()?,
                    fmd.len(),
                    job.id,
                    job.aws_bucket.clone(),
                    job.aws_region.clone(),
                    secret,
                )
                .await
                {
                    Ok(chunked_file) => {
                        // Confirm the chunked file is not empty AKA
                        // this chunk has not been modified, skip it
                        if !chunked_file.is_empty() {
                            // Increase bytes uploaded for this run
                            bytes_uploaded += fmd.len() as usize;
                            // Push chunked file onto run's changed files
                            self.files_changed.push(chunked_file);
                            // Push logs
                            self.logs.push(format!(
                                "[{}] {}-{} ⇉ '{}' uploaded successfully to '{}'.",
                                Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                job.name,
                                self.id,
                                f.path.display().to_string().green(),
                                job.aws_bucket.clone(),
                            ));
                        }
                    }
                    Err(e) => {
                        // Set run status to ERR
                        err += 1;
                        // Push logs
                        self.logs.push(format!(
                            "[{}] {}-{} ⇉ '{}' upload failed: {}.",
                            Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                            job.name,
                            self.id,
                            f.path.display().to_string().red(),
                            e,
                        ));
                    }
                };
            } else if fmd.is_dir() {
                // If the listed file entry is a dir, use walkdir to
                // walk all the recursive directories as well. Upload
                // all files found within the directory.
                for entry in WalkDir::new(&f.path) {
                    let entry = entry?;
                    // If a directory, skip since upload will
                    // create the parent folder by default
                    let fmd = metadata(entry.path())?;
                    if fmd.is_dir() {
                        continue;
                    }
                    // Get file hash and compare with stored file hash in
                    // KipFile. If the same, continue the loop
                    let c = std::fs::read(entry.path())?.to_vec();
                    let digest = hex_digest(Algorithm::SHA256, &c);
                    if f.hash == digest {
                        continue;
                    }
                    // Upload
                    match s3_upload(
                        &entry.path().canonicalize()?,
                        fmd.len(),
                        job.id,
                        job.aws_bucket.clone(),
                        job.aws_region.clone(),
                        secret,
                    )
                    .await
                    {
                        Ok(chunked_file) => {
                            // Confirm the chunked file is not empty
                            if !chunked_file.is_empty() {
                                // Increase bytes uploaded for this run
                                bytes_uploaded += fmd.len() as usize;
                                // Push chunked file onto run's changed files
                                self.files_changed.push(chunked_file);
                                // Push logs
                                self.logs.push(format!(
                                    "[{}] {}-{} ⇉ '{}' uploaded successfully to '{}'.",
                                    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                    job.name,
                                    self.id,
                                    entry.path().display().to_string().green(),
                                    job.aws_bucket.clone(),
                                ));
                            }
                        }
                        Err(e) => {
                            // Set run status to ERR
                            err += 1;
                            // Push logs
                            self.logs.push(format!(
                                "[{}] {}-{} ⇉ '{}' upload failed: {}.",
                                Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                job.name,
                                self.id,
                                entry.path().display().to_string().red(),
                                e,
                            ));
                        }
                    };
                }
            }
        }
        // Set run metadata
        self.finished = Utc::now();
        let dur = self.finished.signed_duration_since(self.started);
        self.time_elapsed = format!(
            "{}d {}h {}m {}s",
            dur.num_days(),
            dur.num_hours(),
            dur.num_minutes(),
            dur.num_seconds(),
        );
        self.bytes_uploaded = bytes_uploaded;
        if err == 0 && warn == 0 {
            self.status = KipStatus::OK;
        } else if warn > 0 && err == 0 {
            self.status = KipStatus::WARN;
        } else {
            self.status = KipStatus::ERR;
        }
        Ok(())
    }

    pub async fn restore(
        &self,
        job: &Job,
        secret: &str,
        output_folder: Option<String>,
    ) -> Result<(), Box<dyn Error>> {
        // Get all bucket contents
        let bucket_objects = list_s3_bucket(&job.aws_bucket, job.aws_region.clone()).await?;
        // Get output folder
        let output_folder = output_folder.unwrap_or_default();
        // For each object in the bucket, download it
        let mut counter: usize = 0;
        for fc in self.files_changed.iter() {
            // Check if S3 object is within this run's changed files
            let mut path = String::new();
            for ob in bucket_objects.iter() {
                // pop hash off from S3 path
                let s3_path = ob.key.clone().expect("unable to get chunk's name from S3.");
                let mut fp: Vec<_> = s3_path.split("/").collect();
                let hash = fp.pop().expect("failed to pop chunk's S3 path.");
                let hs: Vec<_> = hash.split(".").collect();
                if !fc.contains_key(hs[0]) {
                    // S3 object not found in this run
                    continue;
                } else {
                    // Found! Download this chunk
                    path.push_str(&ob.key.clone().expect("unable to get chunk's name from S3."));
                }
            }
            // Increment file counter
            counter += 1;
            // Store the returned downloaded and decrypted bytes for chunk
            let mut chunk_bytes: Vec<u8> = Vec::new();
            // Download chunk
            match s3_download(&path, &job.aws_bucket, job.aws_region.clone(), secret).await {
                Ok(cb) => {
                    chunk_bytes = cb;
                    println!(
                        "[{}] {}-{} ⇉ '{}' restored successfully. ({}/{})",
                        Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                        job.name,
                        self.id,
                        &path.to_string().green(),
                        counter,
                        self.files_changed.len(),
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[{}] {}-{} ⇉ '{}' restore failed: {}. ({}/{})",
                        Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                        job.name,
                        self.id,
                        &path.to_string().red(),
                        e,
                        counter,
                        self.files_changed.len(),
                    );
                }
            }
            // Write chunks to disk
            let mut t: Vec<(&FileChunk, Vec<u8>)> = Vec::new();
            for (_, chunk) in fc.into_iter() {
                if self.is_single_chunk(chunk) {
                    write_single_chunk(chunk, &chunk_bytes, &output_folder)?;
                } else {
                    // find all chunks associated with the same path
                    // and combine them according to their offsets and
                    // lengths and then save to the original local path
                    t.push((chunk, chunk_bytes.clone()));
                }
            }
            // let t = t.sort_by(|a, b| b.0.local_path.cmp(&a.0.local_path));
            // println!("{:?}", t);
            // let b = t.windows(2).all(|c| c[0].0.local_path == c[1].0.local_path);
            // println!("{:?}", b);
        }
        // Success
        Ok(())
    }

    fn is_single_chunk(&self, fc: &FileChunk) -> bool {
        let mut count: usize = 0;
        for cf in self.files_changed.iter().into_iter() {
            for (_, c) in cf {
                if c.local_path == fc.local_path {
                    count += 1;
                }
            }
        }
        if count > 1 {
            return false;
        }
        true
    }
}

fn write_single_chunk(
    chunk: &FileChunk,
    chunk_bytes: &[u8],
    output_folder: &str,
) -> Result<(), Box<dyn Error>> {
    // Create parent directory if missing
    let correct_chunk_path = chunk.local_path.strip_prefix("/")?;
    let folder_path = Path::new(&output_folder).join(correct_chunk_path);
    let folder_parent = match folder_path.parent() {
        Some(p) => p,
        _ => &folder_path,
    };
    std::fs::create_dir_all(folder_parent)?;
    // Create the file
    if !Path::new(&output_folder).join(correct_chunk_path).exists() {
        let mut dfile = File::create(Path::new(&output_folder).join(correct_chunk_path))?;
        dfile.write_all(&chunk_bytes)?;
    } else {
        let mut dfile = File::open(Path::new(&output_folder).join(correct_chunk_path))?;
        dfile.write_all(&chunk_bytes)?;
    }
    Ok(())
}
