//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::run::Run;
use chrono::prelude::*;
use colored::*;
use crypto_hash::{hex_digest, Algorithm};
use rusoto_core::Region;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fmt::Display;
use std::fs::metadata;
use std::io::ErrorKind;
use std::path::PathBuf;
use uuid::Uuid;
use walkdir::WalkDir;
use zeroize::Zeroize;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub name: String,
    pub secret: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub aws_bucket: String,
    pub aws_region: Region,
    pub files: Vec<KipFile>,
    pub files_amt: usize,
    pub runs: HashMap<usize, Run>,
    pub first_run: DateTime<Utc>,
    pub last_run: DateTime<Utc>,
    pub total_runs: usize,
    pub last_status: KipStatus,
    pub created: DateTime<Utc>,
}

impl Drop for Job {
    fn drop(&mut self) {
        zeroize_s3_env_vars();
        self.s3_access_key.zeroize();
        self.s3_secret_key.zeroize();
        self.secret.zeroize();
    }
}

impl Job {
    pub fn new(
        name: &str,
        secret: &str,
        s3_acc_key: &str,
        s3_sec_key: &str,
        aws_bucket: &str,
        aws_region: &str,
    ) -> Self {
        Job {
            id: Uuid::new_v4(),
            name: name.to_string(),
            secret: secret.to_string(),
            s3_access_key: s3_acc_key.to_string(),
            s3_secret_key: s3_sec_key.to_string(),
            aws_bucket: aws_bucket.to_string(),
            aws_region: Job::parse_s3_region(aws_region),
            files: Vec::new(),
            files_amt: 0,
            runs: HashMap::new(),
            first_run: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            last_run: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            total_runs: 0,
            last_status: KipStatus::NEVER_RUN,
            created: Utc::now(),
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

    pub async fn run_upload(&mut self, secret: &str) -> Result<(), Box<dyn Error>> {
        // Set AWS env vars for backup
        set_s3_env_vars(
            &self.s3_access_key,
            &self.s3_secret_key,
            self.aws_region.name(),
        );
        // Create new run
        let mut r = Run::new(self.total_runs + 1);
        // Set job metadata
        self.last_status = KipStatus::IN_PROGRESS;
        // Tell the run to start uploading
        match r.upload(self, secret).await {
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
                return Err(Box::new(std::io::Error::new(
                    ErrorKind::InvalidData,
                    format!("{}.", e),
                )));
            }
        };
        // Reset AWS env to nil
        zeroize_s3_env_vars();
        // Set job status
        self.last_status = r.status;
        // Print all logs from run
        if !r.logs.is_empty() {
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
                "{} job '{}' completed successfully to '{}'.",
                "[OK]".green(),
                &self.name,
                &self.aws_bucket,
            );
        } else {
            self.last_status = KipStatus::OK;
            println!("{} no file changes detected.", "[INFO]".yellow());
        }
        // Success
        Ok(())
    }

    pub async fn run_restore(
        &self,
        run: usize,
        secret: &str,
        output_folder: Option<String>,
    ) -> Result<(), Box<dyn Error>> {
        // Set AWS env vars for backup
        set_s3_env_vars(
            &self.s3_access_key,
            &self.s3_secret_key,
            self.aws_region.name(),
        );
        // Get run from job
        if let Some(r) = self.runs.get(&run) {
            // Tell the run to start uploading
            match r.restore(self, secret, output_folder).await {
                Ok(_) => (),
                Err(e) => {
                    // Reset AWS env to nil
                    zeroize_s3_env_vars();
                    return Err(Box::new(std::io::Error::new(
                        ErrorKind::InvalidData,
                        format!("{}.", e),
                    )));
                }
            };
        } else {
            // Reset AWS env to nil
            zeroize_s3_env_vars();
            return Err(Box::new(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("couldn't find run {}.", run),
            )));
        }
        // Reset AWS env to nil
        zeroize_s3_env_vars();
        // Success
        Ok(())
    }

    pub fn abort(&mut self) {
        unimplemented!();
    }

    // Get correct number of files in job (not just...
    // entries within 'files')
    pub fn get_files_amt(&self) -> Result<usize, Box<dyn Error>> {
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

    async fn get_file_hashes(&mut self) -> Result<(), Box<dyn Error>> {
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
}

fn set_s3_env_vars(acc: &str, sec: &str, reg: &str) {
    // Set AWS env vars to user's keys
    env::set_var("AWS_ACCESS_KEY_ID", acc);
    env::set_var("AWS_SECRET_ACCESS_KEY", sec);
    env::set_var("AWS_REGION", reg);
}

fn zeroize_s3_env_vars() {
    // Reset AWS env vars to nil
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
    use super::*;

    #[test]
    fn test_get_files_amt() {
        let mut j = Job::new("test1", "hunter2", "", "", "testing1", "us-east-1");
        j.files.push(KipFile::new(PathBuf::from("test/vandy.jpg")));
        j.files.push(KipFile::new(PathBuf::from("test/vandy.jpg")));
        j.files.push(KipFile::new(PathBuf::from("test/vandy.jpg")));
        j.files.push(KipFile::new(PathBuf::from("test/random.txt")));
        assert!(j.get_files_amt().is_ok());
        assert_eq!(j.get_files_amt().unwrap(), 4)
    }

    #[tokio::test]
    async fn test_get_file_hashes() {
        let mut j = Job::new("test1", "hunter2", "", "", "testing1", "us-east-1");
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
}
