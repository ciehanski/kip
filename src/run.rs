use crate::chunk::FileChunk;
use crate::job::{Job, KipStatus};
use crate::s3::{list_s3_bucket, s3_download, s3_upload};
use chrono::prelude::*;
use colored::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::metadata;
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
            time_elapsed: String::from(""),
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
        // For each file or dir, upload it
        let mut bytes_uploaded: usize = 0;
        for f in job.files.iter() {
            // Check if file or directory exists
            if !f.exists() {
                self.status = KipStatus::WARN;
                self.logs.push(format!(
                    "[{}] {}-{} ⇉ '{}' can not be found.",
                    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                    job.name,
                    self.id,
                    f.display().to_string().red(),
                ));
                continue;
            }
            // Check if f is file or directory
            let fmd = metadata(f)?;
            if fmd.is_file() {
                // Upload
                match s3_upload(
                    &f.as_path().canonicalize().unwrap(),
                    fmd.len(),
                    job.id,
                    job.aws_bucket.clone(),
                    job.aws_region.clone(),
                    secret,
                )
                .await
                {
                    Ok(chunked_file) => {
                        self.status = KipStatus::OK;
                        // Confirm the chunked file is not empty AKA
                        // this chunk has not been modified, skip it
                        if !chunked_file.is_empty() {
                            // Increase bytes uploaded for this run
                            bytes_uploaded += fmd.len() as usize;
                            // Push chunked file onto run's changed files
                            self.files_changed.push(chunked_file);
                            // Push logs to run
                            self.logs.push(format!(
                                "[{}] {}-{} ⇉ '{}' uploaded successfully to '{}'.",
                                Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                job.name,
                                self.id,
                                f.display().to_string().green(),
                                job.aws_bucket.clone(),
                            ));
                        }
                    }
                    Err(e) => {
                        self.status = KipStatus::ERR;
                        self.logs.push(format!(
                            "[{}] {}-{} ⇉ '{}' upload failed: {}.",
                            Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                            job.name,
                            self.id,
                            f.display().to_string().red(),
                            e,
                        ));
                    }
                };
            } else if fmd.is_dir() {
                // If the listed file entry is a dir, use walkdir to
                // walk all the recursive directories as well. Upload
                // all files found within the directory.
                for entry in WalkDir::new(f) {
                    let entry = entry?;
                    // If a directory, skip since upload will
                    // create the parent folder by default
                    let fmd = metadata(entry.path())?;
                    if fmd.is_dir() {
                        continue;
                    }
                    // Upload
                    match s3_upload(
                        &entry.path().canonicalize().unwrap(),
                        fmd.len(),
                        job.id,
                        job.aws_bucket.clone(),
                        job.aws_region.clone(),
                        secret,
                    )
                    .await
                    {
                        Ok(chunked_file) => {
                            self.status = KipStatus::OK;
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
                            self.status = KipStatus::ERR;
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
        // Done
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
            let mut path = String::from("");
            for ob in bucket_objects.iter() {
                // pop hash off from S3 path
                let s3_path = ob
                    .key
                    .clone()
                    .expect("[ERR] unable to get file name from S3");
                let mut fp: Vec<_> = s3_path.split("/").collect();
                let hash = fp.pop().expect("[ERR] failed to pop S3 path.");
                let hs: Vec<_> = hash.split(".").collect();
                if !fc.contains_key(hs[0]) {
                    // Not found :(
                    continue;
                } else {
                    // Found! Download this chunk
                    path.push_str(&ob.key.clone().unwrap());
                }
            }
            // Increment file counter
            counter += 1;
            // Download file
            match s3_download(
                &path,
                &job.aws_bucket,
                job.aws_region.clone(),
                secret,
                &output_folder,
            )
            .await
            {
                Ok(_) => {
                    println!(
                        "[{}] {}-{} ⇉ '{}' restored successfully. ({}/{})",
                        Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                        job.name,
                        self.id,
                        &path,
                        counter,
                        job.files_amt,
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[{}] {}-{} ⇉ '{}' restore failed: {}. ({}/{})",
                        Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                        job.name,
                        self.id,
                        &path,
                        e,
                        counter,
                        job.files_amt,
                    );
                }
            }
        }
        // Success
        Ok(())
    }
}
