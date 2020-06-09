use crate::crypto::encrypt;
use chrono::prelude::*;
use colored::*;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs;
use std::fs::{metadata, read};
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub name: String,
    pub aws_bucket: String,
    pub aws_region: String,
    pub files: Vec<String>,
    pub first_run: i64,
    pub last_run: i64,
    pub total_runs: usize,
    pub running: bool,
    // pub last_status: enum { OK, IN_PROGRESS, FAILED, NEVER_RUN }
    // pub errors: Vec<KipError> - all reported errors in job's existence
}

impl Job {
    pub fn new(name: &str, aws_bucket: &str, aws_region: &str) -> Self {
        Job {
            id: Uuid::new_v4(),
            name: name.to_string(),
            aws_bucket: aws_bucket.to_string(),
            aws_region: aws_region.to_string(),
            files: Vec::<String>::new(),
            first_run: 0,
            last_run: 0,
            total_runs: 0,
            running: false,
        }
    }

    pub fn upload(
        mut self,
        secret: &str,
        aws_access: &str,
        aws_secret: &str,
    ) -> Result<(), Box<dyn Error>> {
        // Connect to S3
        let region: Region = self.aws_region.parse()?;
        let credentials = Credentials::default_blocking()?;
        // Create new bucket instance
        let bucket = Bucket::new(&self.aws_bucket, region, credentials)?;

        // Set job metadata
        self.running = true;
        self.last_run = Local::now().timestamp();
        self.total_runs += 1;
        if self.first_run == 0 {
            self.first_run = Local::now().timestamp();
        }

        // TODO: S3 by default will allow 10 concurrent requests
        // For each file or dir, upload it
        let mut counter: usize = 0;
        for f in self.files.iter() {
            counter += 1;
            // Check if file or directory exists
            if !Path::new(&f).exists() {
                eprintln!(
                    "{} ⇉ '{}' can not be found. ({}/{})",
                    self.name,
                    f.red(),
                    counter,
                    self.files.len(),
                );
                continue;
            }
            // Check if f is file or directory
            let fmd = metadata(f)?;
            if fmd.is_file() {
                // Encrypt file
                let (encrypted, _) = match encrypt(&read(f)?, secret) {
                    Ok(ef) => ef,
                    Err(e) => {
                        return Result::Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("{} failed to encrypt file: {:?}.", "[ERR]".red(), e),
                        )));
                    }
                };
                // Upload file
                let full_path = fs::canonicalize(PathBuf::from(f))?;
                let (_, status_code) = bucket.put_object_blocking(
                    // Upload path within bucket
                    format!("/{}{}", self.id, full_path.as_path().display()),
                    &encrypted,   // Encrypted File Bytes
                    "text/plain", // Content-Type
                )?;

                // let file_stream = tokio::fs::read(f.to_owned())
                //     .into_stream()
                //     .map_ok(Bytes::from);
                // let s3_client = S3Client::new(Region::UsEast1);
                // s3_client
                //     .put_object(PutObjectRequest {
                //         bucket: self.aws_bucket.clone(),
                //         key: f.to_owned(),
                //         content_length: Some(fmd.len() as i64),
                //         body: Some(StreamingBody::new(file_stream)),
                //         ..Default::default()
                //     })
                //     .await?;

                // Check status code of upload
                if status_code != 200 && status_code != 201 {
                    eprintln!(
                        "{} ⇉ '{}' upload failed: HTTP {}. ({}/{})",
                        self.name,
                        f.red(),
                        status_code,
                        counter,
                        self.files.len(),
                    );
                } else {
                    // Success
                    println!(
                        "{} ⇉ '{}' uploaded successfully. ({}/{})",
                        self.name,
                        f.green(),
                        counter,
                        self.files.len(),
                    );
                }
            } else if fmd.is_dir() {
                // If the listed file entry is a dir, use walkdir to
                // walk all the recursive directories as well. Upload
                // all files found within the directory.
                for entry in WalkDir::new(f) {
                    let entry = entry.expect("[ERR] failed to get directory during upload.");
                    // If a directory, skip since upload will
                    // create the parent folder by default
                    let fmd = metadata(entry.path())?;
                    if fmd.is_dir() {
                        continue;
                    }
                    // Encrypt file
                    let (encrypted, _) = match encrypt(&read(f)?, secret) {
                        Ok(ef) => ef,
                        Err(e) => {
                            return Result::Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("{} failed to encrypt file: {:?}.", "[ERR]".red(), e),
                            )));
                        }
                    };
                    // Upload file
                    let full_path = fs::canonicalize(PathBuf::from(f))?;
                    let (_, status_code) = bucket.put_object_blocking(
                        // Upload path within bucket
                        format!("/{}{}", self.id, full_path.as_path().display()),
                        &encrypted,   // Encrypted File Bytes
                        "text/plain", // Content-Type
                    )?;
                    // Check status code of upload
                    if status_code != 200 && status_code != 201 {
                        println!(
                            "{} ⇉ '{}' upload failed: HTTP {}. ({}/{})",
                            self.name,
                            f.red(),
                            status_code,
                            counter,
                            self.files.len(),
                        );
                    } else {
                        // Success
                        println!(
                            "{} ⇉ '{}' uploaded successfully. ({}/{})",
                            self.name,
                            f.green(),
                            counter,
                            self.files.len(),
                        );
                    }
                }
            }
        }
        // Done
        self.running = false;
        Ok(())
    }

    pub async fn restore() {
        unimplemented!()
    }

    pub fn abort(mut self) {
        if self.running {
            self.running = false;
            println!("{} aborted.", self.name);
        } else {
            println!("{} not running.", self.name);
        }
    }
}
