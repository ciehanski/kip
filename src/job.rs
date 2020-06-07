use crate::crypto::encrypt;
use chrono::prelude::*;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use std::fs::{metadata, read};
use std::path::Path;
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
        &mut self,
        secret: &str,
        aws_access: &str,
        aws_secret: &str,
    ) -> Result<(), Box<dyn Error>> {
        // Connect to S3
        let region: Region = self.aws_region.parse()?;
        // Set access/secret in env as fallback
        env::set_var("AWS_ACCESS_KEY_ID", aws_access);
        env::set_var("AWS_SECRET_ACCESS_KEY", aws_secret);
        let credentials =
            Credentials::new_blocking(Some(aws_access), Some(aws_secret), None, None, None)?;
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
                    "{} > ({}/{}) '{}' can not be found.",
                    self.name,
                    counter,
                    self.files.len(),
                    f
                );
                continue;
            }
            // Check if f is file or directory
            let file_md = metadata(f).expect("failed to get kip configuration metadata.");
            if file_md.is_file() {
                // Encrypt file
                let (encrypted, _) = match encrypt(&read(f)?, secret) {
                    Ok(e) => e,
                    Err(e) => panic!(e),
                };
                // Upload file
                let (_, status_code) = bucket.put_object_blocking(
                    format!("/{}", f), // Folder Name - File Name
                    &encrypted,        // Encrypted File Bytes
                    "text/plain",      // Content-Type
                )?;
                // Check status code of upload
                if status_code != 200 && status_code != 201 {
                    eprintln!(
                        "{} > '{}' upload failed: HTTP {}. ({}/{})",
                        self.name,
                        f,
                        status_code,
                        counter,
                        self.files.len(),
                    );
                } else {
                    // Success
                    println!(
                        "{} > '{}' uploaded successfully. ({}/{})",
                        self.name,
                        f,
                        counter,
                        self.files.len(),
                    );
                }
            } else if file_md.is_dir() {
                // If the listed file entry is a dir, use walkdir to
                // walk all the recursive directories as well. Upload
                // all files found within the directory.
                for entry in WalkDir::new(f) {
                    let entry = entry.expect("failed to get directory during upload.");
                    // If a directory, skip since upload will
                    // create the parent folder by default
                    let fmd = metadata(entry.path())?;
                    if fmd.is_dir() {
                        continue;
                    }
                    // Encrypt file
                    let (encrypted, _) = match encrypt(&read(entry.path())?, secret) {
                        Ok(e) => e,
                        Err(e) => panic!(e),
                    };
                    // Upload file
                    let (_, status_code) = bucket.put_object_blocking(
                        //format!("/{}/{}", self.id, entry.path().display()), // Folder Name - File Name
                        format!("/{}", entry.path().display()),
                        &encrypted,   // Encrypted File Bytes
                        "text/plain", // Content-Type
                    )?;
                    // Check status code of upload
                    if status_code != 200 && status_code != 201 {
                        println!(
                            "{} > '{}' upload failed: HTTP {}. ({}/{})",
                            self.name,
                            f,
                            status_code,
                            counter,
                            self.files.len(),
                        );
                    } else {
                        // Success
                        println!(
                            "{} > '{}' uploaded successfully. ({}/{})",
                            self.name,
                            f,
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

    pub fn abort(&mut self) {
        if self.running {
            self.running = false;
            println!("{} aborted.", self.name);
        } else {
            println!("{} not running.", self.name);
        }
    }
}
