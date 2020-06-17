use crate::job::{Job, KipStatus};
use crate::s3::{list_s3_bucket, s3_download, s3_upload};
use chrono::prelude::*;
use colored::*;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::metadata;
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Run {
    pub id: usize,
    pub started: DateTime<Utc>,
    pub time_elapsed: u64,
    pub finished: DateTime<Utc>,
    pub bytes_uploaded: usize,
    pub files_uploaded: usize,
    pub status: KipStatus,
    pub logs: Vec<String>,
}

impl Run {
    pub fn new(id: usize) -> Self {
        Run {
            id,
            started: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            time_elapsed: 0,
            finished: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            bytes_uploaded: 0,
            files_uploaded: 0,
            status: KipStatus::NEVER_RUN,
            logs: Vec::<String>::new(),
        }
    }

    pub async fn upload(&mut self, job: &Job, secret: &str) -> Result<(), Box<dyn Error>> {
        // Set run metadata
        // TODO: build timer for upload process
        self.started = Utc::now();
        self.status = KipStatus::IN_PROGRESS;
        // TODO: S3 by default will allow 10 concurrent requests
        // For each file or dir, upload it
        let mut counter: usize = 0;
        // TODO: testing
        for f in job.files.iter() {
            // Check if file or directory exists
            if !f.exists() {
                self.status = KipStatus::WARN;
                self.logs.push(format!(
                    "{} | {}-{} ⇉ '{}' can not be found. ({}/{})",
                    Utc::now(),
                    job.name,
                    self.id,
                    f.display().to_string().red(),
                    counter,
                    job.files_amt,
                ));
                continue;
            }
            // Check if f is file or directory
            let fmd = metadata(f)?;
            if fmd.is_file() {
                // Increase file counter
                counter += 1;
                // Upload
                match s3_upload(
                    &f.display().to_string(),
                    fmd.len(),
                    job.id,
                    job.aws_bucket.clone(),
                    Job::parse_s3_region(job.aws_region.clone()),
                    secret,
                )
                .await
                {
                    Ok(_) => {
                        self.status = KipStatus::OK;
                        self.logs.push(format!(
                            "{} | {}-{} ⇉ '{}' uploaded successfully to '{}'. ({}/{})",
                            Utc::now(),
                            job.name,
                            self.id,
                            f.display().to_string().green(),
                            job.aws_bucket.clone(),
                            counter,
                            job.files_amt,
                        ));
                    }
                    Err(e) => {
                        self.status = KipStatus::ERR;
                        self.logs.push(format!(
                            "{} | {}-{} ⇉ '{}' upload failed: {}. ({}/{})",
                            Utc::now(),
                            job.name,
                            self.id,
                            f.display().to_string().red(),
                            e,
                            counter,
                            job.files_amt,
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
                    // Increase file counter
                    counter += 1;
                    // Upload
                    match s3_upload(
                        &entry.path().display().to_string(),
                        fmd.len(),
                        job.id,
                        job.aws_bucket.clone(),
                        Job::parse_s3_region(job.aws_region.clone()),
                        secret,
                    )
                    .await
                    {
                        Ok(_) => {
                            self.status = KipStatus::OK;
                            self.logs.push(format!(
                                "{} | {}-{} ⇉ '{}' uploaded successfully to '{}'. ({}/{})",
                                Utc::now(),
                                job.name,
                                self.id,
                                entry.path().display().to_string().green(),
                                job.aws_bucket.clone(),
                                counter,
                                job.files_amt,
                            ));
                        }
                        Err(e) => {
                            self.status = KipStatus::ERR;
                            self.logs.push(format!(
                                "{} | {}-{} ⇉ '{}' upload failed: {}. ({}/{})",
                                Utc::now(),
                                job.name,
                                self.id,
                                entry.path().display().to_string().red(),
                                e,
                                counter,
                                job.files_amt,
                            ));
                        }
                    };
                }
            }
        }
        // Set run metadata
        self.files_uploaded = counter;
        self.finished = Utc::now();
        // Done
        Ok(())
    }

    pub async fn restore(
        &mut self,
        job: &Job,
        secret: &str,
        output_folder: Option<String>,
    ) -> Result<(), Box<dyn Error>> {
        // Set run metadata
        // TODO: build timer for upload process
        self.started = Utc::now();
        self.status = KipStatus::IN_PROGRESS;
        // Get all bucket contents
        let bucket_objects = list_s3_bucket(
            &job.aws_bucket,
            Job::parse_s3_region(job.aws_region.clone()),
        )
        .await?;
        // Get output folder
        let output_folder = output_folder.unwrap_or_default();
        // For each object in the bucket, download it
        let mut counter: usize = 0;
        for ob in bucket_objects {
            // Increment file counter
            counter += 1;
            // Download file
            match s3_download(
                &ob.key
                    .clone()
                    .expect("[ERR] unable to get file name from S3."),
                &job.aws_bucket,
                Job::parse_s3_region(job.aws_region.clone()),
                secret,
                &output_folder,
            )
            .await
            {
                Ok(_) => {
                    self.status = KipStatus::OK;
                    println!(
                        "{} ⇉ '{}' restored successfully. ({}/{})",
                        job.name,
                        ob.key.unwrap().green(),
                        counter,
                        job.files_amt,
                    );
                }
                Err(e) => {
                    self.status = KipStatus::ERR;
                    eprintln!(
                        "{} ⇉ '{}' restore failed: {}. ({}/{})",
                        job.name,
                        ob.key
                            .expect("[ERR] unable to get file name from S3.")
                            .green(),
                        e,
                        counter,
                        job.files_amt,
                    );
                }
            }
        }
        // Set run metadata
        self.files_uploaded = counter;
        self.finished = Utc::now();
        // Success
        Ok(())
    }
}
