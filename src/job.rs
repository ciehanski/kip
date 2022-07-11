//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::crypto::{keyring_delete_secret, keyring_get_secret};
use crate::providers::s3::KipS3;
use crate::providers::{KipProvider, KipProviders};
use crate::run::Run;
use anyhow::{bail, Context, Result};
use chrono::prelude::*;
use colored::*;
use crypto_hash::{hex_digest, Algorithm};
use rusoto_core::Region;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fmt::{Debug, Display};
use std::fs::metadata;
use std::path::PathBuf;
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub name: String,
    pub provider: KipProviders,
    pub compress: bool,
    // pub retain_all: bool,
    // pub retention_policy: Frotate,
    // pub aws_bucket: String,
    // pub aws_region: Region,
    pub files: Vec<KipFile>,
    pub files_amt: usize,
    // pub excluded_files: Vec<String>,
    // pub excluded_file_types: Vec<String>,
    pub runs: HashMap<usize, Run>,
    pub bytes_amt_provider: u64,
    pub first_run: DateTime<Utc>,
    pub last_run: DateTime<Utc>,
    pub total_runs: usize,
    pub last_status: KipStatus,
    pub created: DateTime<Utc>,
    pub paused: bool,
}

impl Job {
    pub fn new(name: &str, aws_bucket: &str, aws_region: &str) -> Self {
        Job {
            id: Uuid::new_v4(),
            name: name.to_string(),
            provider: KipProviders::S3(KipS3::new(aws_bucket, Job::parse_s3_region(aws_region))),
            compress: false,
            // aws_bucket: aws_bucket.to_string(),
            // aws_region: Job::parse_s3_region(aws_region),
            files: Vec::new(),
            files_amt: 0,
            runs: HashMap::new(),
            bytes_amt_provider: 0,
            first_run: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            last_run: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            total_runs: 0,
            last_status: KipStatus::NEVER_RUN,
            created: Utc::now(),
            paused: false,
        }
    }

    // Parse aws region input into a Region object
    fn parse_s3_region(s3_region: &str) -> Region {
        match s3_region {
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
            "us-east-2" => Region::UsEast2,
            "us-west-1" => Region::UsWest1,
            "us-west-2" => Region::UsWest2,
            "us-gov-east-1" => Region::UsGovEast1,
            "us-gov-west-1" => Region::UsGovWest1,
            _ => Region::UsEast1,
        }
    }

    pub async fn run_upload(&mut self, secret: &str) -> Result<()> {
        // Check and confirm that job is not paused
        if self.paused {
            bail!(
                "unable to run. '{}' is paused. Please run 'kip resume <job>' to resume job.",
                self.name
            )
        }
        // Create new run
        let mut r = Run::new(self.total_runs + 1);
        // Set job metadata
        self.last_status = KipStatus::IN_PROGRESS;
        // Set AWS env vars for backup
        self.set_s3_env_vars()?;
        // Clone job for upload metadata
        // TODO: would like to avoid this clone if possible
        // since it will clone all a job's runs and their logs
        let me = self.clone();
        // Tell the run to start uploading
        match r.upload(me, secret.to_string()).await {
            Ok(_) => {}
            Err(e) => {
                // Set job status
                self.last_status = KipStatus::ERR;
                // Add run to job
                self.runs.insert(r.id, r);
                self.total_runs += 1;
                self.last_run = Utc::now();
                if self.first_run.format("%Y-%m-%d %H:%M:%S").to_string() == "1970-01-01 00:00:00" {
                    self.first_run = Utc::now();
                }
                // Reset AWS env to nil
                zeroize_s3_env_vars();
                bail!("{}.", e)
            }
        };
        // Reset AWS env to nil
        zeroize_s3_env_vars();
        // Set job status
        self.last_status = r.status;
        // Print all logs from run
        if !r.logs.is_empty() {
            self.bytes_amt_provider += r.bytes_uploaded;
            // Get new file hashes
            self.get_file_hashes().await?;
            // Add run to job only if anything was uploaded
            self.runs.insert(r.id, r);
            self.total_runs += 1;
            self.last_run = Utc::now();
            if self.first_run.format("%Y-%m-%d %H:%M:%S").to_string() == "1970-01-01 00:00:00" {
                self.first_run = Utc::now();
            }
            println!(
                "{} job '{}' completed uploading to '{}' successfully .",
                "[OK]".green(),
                &self.name,
                &self.provider.s3().unwrap().aws_bucket,
            );
        } else {
            println!("{} no file changes detected.", "[INFO]".yellow());
        }
        // Success
        Ok(())
    }

    pub async fn run_restore(&self, run: usize, secret: &str, output_folder: &str) -> Result<()> {
        // Get run from job
        if let Some(r) = self.runs.get(&run) {
            // Set AWS env vars for backup
            self.set_s3_env_vars()?;
            // Tell the run to start uploading
            match r.restore(self, secret, output_folder).await {
                Ok(_) => (),
                Err(e) => {
                    // Reset AWS env to nil
                    zeroize_s3_env_vars();
                    bail!("{}.", e)
                }
            };
        } else {
            bail!("couldn't find run {}.", run)
        }
        // Reset AWS env to nil
        zeroize_s3_env_vars();
        // Success
        Ok(())
    }

    pub async fn purge_file(&mut self, f: &str) -> Result<()> {
        // Find all the runs that contain this file's chunks
        // and remove them from S3.
        let fpath = PathBuf::from(&f).canonicalize()?;
        self.set_s3_env_vars()?;
        for run in self.runs.iter() {
            for chunk in run.1.files_changed.iter() {
                if chunk.local_path == fpath {
                    let chunk_path = format!("{}/chunks/{}.chunk", self.id, chunk.hash);
                    // Delete
                    self.provider.s3().unwrap().delete(&chunk_path).await?;
                }
            }
        }
        // Reset AWS env env to nil
        zeroize_s3_env_vars();
        // Set job metadata
        self.bytes_amt_provider -= std::fs::metadata(fpath)?.len();
        Ok(())
    }

    pub fn abort(&mut self) {
        unimplemented!();
    }

    // Get correct number of files in job (not just...
    // entries within 'files')
    pub fn get_files_amt(&self) -> Result<usize> {
        let mut correct_files_num: usize = 0;
        for f in self.files.iter() {
            if f.path.exists() && f.path.is_dir() {
                for entry in WalkDir::new(&f.path) {
                    let entry = entry?;
                    if entry.path().is_dir() {
                        continue;
                    }
                    correct_files_num += 1;
                }
            } else if f.path.exists() {
                correct_files_num += 1;
            }
        }
        Ok(correct_files_num)
    }

    async fn get_file_hashes(&mut self) -> Result<()> {
        for f in self.files.iter_mut() {
            // File
            if metadata(&f.path)?.is_file() {
                let c = tokio::fs::read(&f.path).await?;
                let digest = hex_digest(Algorithm::SHA256, &c);
                f.hash = digest;
            } else {
                // Directory
                for entry in WalkDir::new(&f.path) {
                    let entry = entry?;
                    if entry.metadata()?.is_dir() {
                        continue;
                    }
                    let c = tokio::fs::read(&entry.path()).await?;
                    let digest = hex_digest(Algorithm::SHA256, &c);
                    f.hash = digest;
                }
            }
        }
        Ok(())
    }

    fn set_s3_env_vars(&self) -> Result<()> {
        let s3acc = keyring_get_secret(&format!("com.ciehanski.kip.{}.s3acc", self.name))
            .context("couldnt get s3acc from keyring")?;
        let s3acc = s3acc.trim_end();
        let s3sec = keyring_get_secret(&format!("com.ciehanski.kip.{}.s3sec", self.name))
            .context("couldn't get s3sec from keyring")?;
        let s3sec = s3sec.trim_end();
        // Set AWS env vars to user's keys
        env::set_var("AWS_ACCESS_KEY_ID", s3acc);
        env::set_var("AWS_SECRET_ACCESS_KEY", s3sec);
        env::set_var("AWS_REGION", self.provider.s3().unwrap().aws_region.name());
        Ok(())
    }

    pub fn delete_keyring_entries(&self) -> Result<()> {
        let job_secret = keyring_get_secret(&format!("com.ciehanski.kip.{}", self.name))
            .context("couldnt get job secret from keyring")?;
        let job_secret = job_secret.trim_end();
        let s3acc = keyring_get_secret(&format!("com.ciehanski.kip.{}.s3acc", self.name))
            .context("couldnt get s3acc from keyring")?;
        let s3acc = s3acc.trim_end();
        let s3sec = keyring_get_secret(&format!("com.ciehanski.kip.{}.s3sec", self.name))
            .context("couldn't get s3sec from keyring")?;
        let s3sec = s3sec.trim_end();
        keyring_delete_secret(s3acc)?;
        keyring_delete_secret(s3sec)?;
        keyring_delete_secret(job_secret)?;
        Ok(())
    }
}

// Reset AWS env vars to nil
fn zeroize_s3_env_vars() {
    env::set_var("AWS_ACCESS_KEY_ID", "");
    env::set_var("AWS_SECRET_ACCESS_KEY", "");
    env::set_var("AWS_REGION", "");
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub enum KipStatus {
    OK,
    ERR,
    WARN,
    IN_PROGRESS,
    NEVER_RUN,
}

impl Display for KipStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KipStatus::OK => write!(f, "OK"),
            KipStatus::ERR => write!(f, "ERR"),
            KipStatus::WARN => write!(f, "WARN"),
            KipStatus::IN_PROGRESS => write!(f, "IN_PROGRESS"),
            KipStatus::NEVER_RUN => write!(f, "NEVER_RUN"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct KipFile {
    pub path: PathBuf,
    pub hash: String,
}

impl KipFile {
    pub fn new(path: PathBuf) -> Self {
        KipFile {
            path,
            hash: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::crypto::keyring_set_secret;

    use super::*;

    #[test]
    fn test_get_files_amt() {
        let mut j = Job::new("test1", "testing1", "us-east-1");
        j.files.push(KipFile::new(PathBuf::from("test/vandy.jpg")));
        j.files.push(KipFile::new(PathBuf::from("test/vandy.jpg")));
        j.files.push(KipFile::new(PathBuf::from("test/vandy.jpg")));
        j.files.push(KipFile::new(PathBuf::from("test/random.txt")));
        assert!(j.get_files_amt().is_ok());
        assert_eq!(j.get_files_amt().unwrap(), 4)
    }

    #[tokio::test]
    async fn test_get_file_hashes() {
        let mut j = Job::new("test1", "testing1", "us-east-1");
        j.files.push(KipFile::new(PathBuf::from("test/vandy.jpg")));
        j.files.push(KipFile::new(PathBuf::from("test/random.txt")));
        let hash_result = j.get_file_hashes().await;
        assert!(hash_result.is_ok());
        assert_eq!(
            j.files[0].hash,
            "97ad4887a60dfa689660bad732f92a2871dedf97add169267c43e2955415488d"
        );
        assert_eq!(
            j.files[1].hash,
            "44b4cdaf713dfaf961dedb34f07e15604f75eb049c83067ab35bf388b369dbf3"
        )
    }

    #[test]
    fn test_parse_s3_region() {
        let region1 = Job::parse_s3_region("us-west-400");
        assert_eq!(region1, Region::UsEast1);
        let region2 = Job::parse_s3_region("us-west-2");
        assert_eq!(region2, Region::UsWest2);
        let region3 = Job::parse_s3_region("us-east-2");
        assert_eq!(region3, Region::UsEast2)
    }

    #[test]
    fn test_set_get_keyring() {
        let j = Job::new("test1", "testing1", "us-east-1");
        let result = keyring_set_secret(&j.name, "hunter2");
        assert!(result.is_ok());
        let get_result = keyring_get_secret(&j.name);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap(), "hunter2");
        // Cleanup
        let cleanup = keyring_delete_secret(&j.name);
        assert!(cleanup.is_ok())
    }
}
