use crate::crypto::{decrypt, encrypt};
use chrono::prelude::*;
use colored::*;
use rusoto_core::Region;
use rusoto_s3::{
    GetObjectRequest, ListObjectsV2Request, Object, PutObjectRequest, S3Client, StreamingBody, S3,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::{metadata, read, File};
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::{env, fs};
use tokio::io::AsyncReadExt;
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub name: String,
    pub aws_bucket: String,
    pub aws_region: String,
    pub files: Vec<String>,
    pub first_run: DateTime<Utc>,
    pub last_run: DateTime<Utc>,
    pub total_runs: usize,
    pub last_status: JobStatus,
    // pub errors: Vec<dyn Error>,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum JobStatus {
    OK,
    ERR,
    IN_PROGRESS,
    NEVER_RUN,
}

impl Job {
    pub fn new(name: &str, aws_bucket: &str, aws_region: &str) -> Self {
        Job {
            id: Uuid::new_v4(),
            name: name.to_string(),
            aws_bucket: aws_bucket.to_string(),
            aws_region: aws_region.to_string(),
            files: Vec::<String>::new(),
            first_run: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            last_run: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            total_runs: 0,
            last_status: JobStatus::NEVER_RUN,
        }
    }

    fn parse_s3_region(s3_region: String) -> Region {
        // Parse aws region input into a Region object
        match &s3_region[..] {
            "ap-east-1" => Region::ApEast1,
            "ap-northeast-1" => Region::ApNortheast1,
            "ap-northeast-2" => Region::ApNortheast2,
            "ap-south-1" => Region::ApSouth1,
            "ap-southeast-1" => Region::ApSoutheast1,
            "ap-southeast-2" => Region::ApSoutheast2,
            "ca-central-1" => Region::CaCentral1,
            "eu-central-1" => Region::EuCentral1,
            "eu-west-1" => Region::EuWest1,
            "eu-west-2" => Region::EuWest2,
            "eu-west-3" => Region::EuWest3,
            "eu-north-1" => Region::EuNorth1,
            "sa-east-1" => Region::SaEast1,
            "us-east-1" => Region::UsEast1,
            "us-east-2" => Region::UsEast2,
            "us-west-1" => Region::UsWest1,
            "us-west-2" => Region::UsWest2,
            "us-gov-east-1" => Region::UsGovEast1,
            "us-gov-west-1" => Region::UsGovWest1,
            _ => Region::UsEast1,
        }
    }

    pub async fn download(
        mut self,
        secret: &str,
        aws_access: &str,
        aws_secret: &str,
    ) -> Result<(), Box<dyn Error>> {
        // Set job metadata
        self.last_run = Utc::now();
        self.total_runs += 1;
        if self.first_run.format("%Y-%m-%d %H:%M:%S").to_string() == "1970-01-01 00:00:00" {
            self.first_run = Utc::now();
        }
        self.last_status = JobStatus::IN_PROGRESS;
        // Set AWS env vars for backup
        env::set_var("AWS_ACCESS_KEY_ID", &aws_access);
        env::set_var("AWS_SECRET_ACCESS_KEY", &aws_secret);
        env::set_var("AWS_REGION", &self.aws_region);
        // get all bucket contents
        let bucket_objects = list_s3_bucket(
            &self.aws_bucket,
            Job::parse_s3_region(self.aws_region.clone()),
        )
        .await?;
        // TODO: remove
        println!("{:?}", bucket_objects);
        //
        // For each object in the bucket, download it
        for ob in bucket_objects {
            s3_download(
                &ob.key.unwrap(),
                &self.aws_bucket,
                Job::parse_s3_region(self.aws_region.clone()),
                secret,
            )
            .await?;
        }
        // Reset AWS env env to nil
        env::set_var("AWS_ACCESS_KEY_ID", "");
        env::set_var("AWS_SECRET_ACCESS_KEY", "");
        env::set_var("AWS_REGION", "");
        // Success
        Ok(())
    }

    pub async fn upload(
        mut self,
        secret: &str,
        aws_access: &str,
        aws_secret: &str,
    ) -> Result<(), Box<dyn Error>> {
        // Set job metadata
        self.last_run = Utc::now();
        self.total_runs += 1;
        if self.first_run.format("%Y-%m-%d %H:%M:%S").to_string() == "1970-01-01 00:00:00" {
            self.first_run = Utc::now();
        }
        self.last_status = JobStatus::IN_PROGRESS;
        // Set AWS env vars for backup
        env::set_var("AWS_ACCESS_KEY_ID", &aws_access);
        env::set_var("AWS_SECRET_ACCESS_KEY", &aws_secret);
        env::set_var("AWS_REGION", &self.aws_region);
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
                // Upload
                match s3_upload(
                    f,
                    fmd.len(),
                    self.id,
                    self.aws_bucket.clone(),
                    Job::parse_s3_region(self.aws_region.clone()),
                    secret,
                )
                .await
                {
                    Ok(_) => {
                        self.last_status = JobStatus::OK;
                        println!(
                            "{} ⇉ '{}' uploaded successfully to '{}'. ({}/{})",
                            self.name,
                            f.green(),
                            self.aws_bucket.clone(),
                            counter,
                            self.files.len(),
                        );
                    }
                    Err(e) => {
                        self.last_status = JobStatus::ERR;
                        eprintln!(
                            "{} ⇉ '{}' upload failed: {}. ({}/{})",
                            self.name,
                            f.red(),
                            e,
                            counter,
                            self.files.len(),
                        );
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
                        &entry.path().display().to_string(),
                        fmd.len(),
                        self.id,
                        self.aws_bucket.clone(),
                        Job::parse_s3_region(self.aws_region.clone()),
                        secret,
                    )
                    .await
                    {
                        Ok(_) => {
                            self.last_status = JobStatus::OK;
                            println!(
                                "{} ⇉ '{}' uploaded successfully to '{}'. ({}/{})",
                                self.name,
                                entry.path().display().to_string().green(),
                                self.aws_bucket.clone(),
                                counter,
                                self.files.len(),
                            );
                        }
                        Err(e) => {
                            self.last_status = JobStatus::ERR;
                            eprintln!(
                                "{} ⇉ '{}' upload failed: {}. ({}/{})",
                                self.name,
                                f.red(),
                                e,
                                counter,
                                self.files.len(),
                            );
                        }
                    };
                }
            }
        }
        // Reset AWS env env to nil
        env::set_var("AWS_ACCESS_KEY_ID", "");
        env::set_var("AWS_SECRET_ACCESS_KEY", "");
        env::set_var("AWS_REGION", "");
        // Done
        Ok(())
    }

    pub fn abort(self) {
        unimplemented!();
    }
}

async fn s3_upload(
    f: &str,
    fmd_len: u64,
    job_id: Uuid,
    aws_bucket: String,
    aws_region: Region,
    secret: &str,
) -> Result<(), Box<dyn Error>> {
    // Encrypt file
    let encrypted = match encrypt(&read(f)?, secret) {
        Ok(ef) => ef,
        Err(e) => {
            return Result::Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{} failed to encrypt file: {:?}.", "[ERR]".red(), e),
            )));
        }
    };
    // Upload file
    // Get full path of file
    let full_path = fs::canonicalize(PathBuf::from(f))?;
    // Create S3 client with specifc region
    let s3_client = S3Client::new(aws_region);
    // PUT!
    s3_client
        .put_object(PutObjectRequest {
            bucket: aws_bucket.clone(),
            key: format!("{}{}", job_id, full_path.as_path().display().to_string()),
            content_length: Some(fmd_len as i64),
            body: Some(StreamingBody::from(encrypted)),
            ..Default::default()
        })
        .await?;
    // Success
    Ok(())
}

async fn s3_download(
    f: &str,
    aws_bucket: &str,
    aws_region: Region,
    secret: &str,
) -> Result<(), Box<dyn Error>> {
    // Get full path of file
    let full_path = fs::canonicalize(PathBuf::from(f))?;
    // Create S3 client with specifc region
    let s3_client = S3Client::new(aws_region);
    // GET!
    let result = s3_client
        .get_object(GetObjectRequest {
            bucket: aws_bucket.clone().to_string(),
            key: full_path.as_path().display().to_string(),
            ..Default::default()
        })
        .await?;
    // Read result from S3 and convert to bytes
    let mut result_stream = result.body.unwrap().into_async_read();
    let mut result_bytes = Vec::<u8>::new();
    result_stream.read_to_end(&mut result_bytes).await?;
    // Decrypt file
    let decrypted = match decrypt(&result_bytes, secret) {
        Ok(df) => df,
        Err(e) => {
            return Result::Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{} failed to decrypt file: {:?}.", "[ERR]".red(), e),
            )));
        }
    };
    // Create and write decrypted file
    let mut dfile = File::create(f)?;
    dfile.write_all(&decrypted)?;
    // Success
    Ok(())
}

async fn list_s3_bucket(
    aws_bucket: &str,
    aws_region: Region,
) -> Result<Vec<Object>, Box<dyn Error>> {
    // Create S3 client with specifc region
    let s3_client = S3Client::new(aws_region);
    let result = s3_client
        .list_objects_v2(ListObjectsV2Request {
            bucket: aws_bucket.to_string().clone(),
            ..ListObjectsV2Request::default()
        })
        .await?;
    let contents = result
        .contents
        .unwrap_or_default()
        .into_iter()
        .collect::<Vec<_>>();
    // Success
    Ok(contents)
}
