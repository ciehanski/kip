//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::crypto::{keyring_delete_secret, keyring_get_secret};
use crate::providers::{KipProvider, KipProviders};
use crate::run::Run;
use anyhow::{bail, Context, Result};
use chrono::prelude::*;
use colored::*;
use crypto_hash::{hex_digest, Algorithm};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fmt::{Debug, Display};
use std::path::{Path, PathBuf};
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub name: String,
    pub provider: KipProviders,
    pub compress: bool,
    // pub retention_policy: Frotate,
    pub files: Vec<KipFile>,
    pub files_amt: u64,
    pub excluded_files: Vec<PathBuf>,
    pub excluded_file_types: Vec<String>,
    pub runs: HashMap<usize, Run>,
    pub bytes_amt_provider: u64,
    pub first_run: DateTime<Utc>,
    pub last_run: DateTime<Utc>,
    pub total_runs: u64,
    pub last_status: KipStatus,
    pub created: DateTime<Utc>,
    pub paused: bool,
}

impl Job {
    pub fn new<S: Into<String>>(name: S, provider: KipProviders) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            provider,
            compress: true,
            files: Vec::new(),
            files_amt: 0,
            excluded_files: Vec::new(),
            excluded_file_types: Vec::new(),
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

    pub async fn start_run(&mut self, secret: &str) -> Result<()> {
        // Check and confirm that job is not paused
        if self.paused {
            bail!(
                "unable to run. '{}' is paused. Please run 'kip resume {}' to resume job.",
                self.name,
                self.name
            )
        }
        // Create new run
        let mut r = Run::new(self.total_runs + 1);
        // Set job metadata
        self.last_status = KipStatus::IN_PROGRESS;
        // Set AWS env vars for backup
        self.set_provider_env_vars()?;
        // Tell the run to start uploading
        match r.start(self.to_owned(), secret.to_string()).await {
            Ok(_) => {}
            Err(e) => {
                // Set job status
                self.last_status = KipStatus::ERR;
                // Add run to job
                self.runs.insert(r.id.try_into()?, r);
                self.total_runs += 1;
                self.last_run = Utc::now();
                if self.first_run.format("%Y-%m-%d %H:%M:%S").to_string() == "1970-01-01 00:00:00" {
                    self.first_run = Utc::now();
                }
                // Reset provider env vars to nil
                self.zeroize_provider_env_vars();
                bail!("{e}.")
            }
        };
        // Reset provider env vars to nil
        self.zeroize_provider_env_vars();
        // Set job status
        self.last_status = r.status;
        // Print all logs from run
        if !r.logs.is_empty() {
            self.bytes_amt_provider += r.bytes_uploaded;
            // Get new file hashes
            self.get_file_hashes().await?;
            // Add run to job only if anything was uploaded
            self.runs.insert(r.id.try_into()?, r);
            self.total_runs += 1;
            self.last_run = Utc::now();
            if self.first_run.format("%Y-%m-%d %H:%M:%S").to_string() == "1970-01-01 00:00:00" {
                self.first_run = Utc::now();
            }
            let provider = match &self.provider {
                KipProviders::S3(s3) => s3.aws_bucket.to_owned(),
                KipProviders::Usb(usb) => usb.name.to_owned(),
                KipProviders::Gdrive(gdrive) => {
                    if let Some(pf) = gdrive.parent_folder.to_owned() {
                        format!("My Drive/{pf}")
                    } else {
                        "My Drive/".to_string()
                    }
                }
            };
            println!(
                "{} job '{}' completed uploading to '{}' successfully.",
                "[OK]".green(),
                &self.name,
                provider,
            );
        } else {
            println!("{} no file changes detected.", "[INFO]".yellow());
        }
        Ok(())
    }

    /// Performs a restore on the run specified for a job
    pub async fn start_restore(&self, run: usize, secret: &str, output_folder: &str) -> Result<()> {
        // Get run from job
        if let Some(r) = self.runs.get(&run) {
            // Set AWS env vars for backup
            self.set_provider_env_vars()?;
            // Tell the run to start uploading
            match r.restore(self, secret, output_folder).await {
                Ok(_) => (),
                Err(e) => {
                    // Reset provider env vars to nil
                    self.zeroize_provider_env_vars();
                    bail!("{e}.")
                }
            };
        } else {
            bail!("couldn't find run {run}.")
        }
        // Reset provider env vars to nil
        self.zeroize_provider_env_vars();
        // Success
        Ok(())
    }

    pub async fn purge_file(&mut self, f: &str) -> Result<()> {
        // Find all the runs that contain this file's chunks
        // and remove them from S3.
        let fpath = Path::new(&f).canonicalize()?;
        self.set_provider_env_vars()?;
        for run in self.runs.iter() {
            for chunk in run.1.files_changed.iter() {
                if chunk.local_path == fpath {
                    let chunk_path = format!("{}/chunks/{}.chunk", self.id, chunk.hash);
                    // Delete
                    match &self.provider {
                        KipProviders::S3(s3) => {
                            s3.delete(&chunk_path).await?;
                        }
                        KipProviders::Usb(usb) => {
                            usb.delete(&chunk_path).await?;
                        }
                        KipProviders::Gdrive(gdrive) => {
                            gdrive.delete(&chunk_path).await?;
                        }
                    };
                }
            }
        }
        // Reset provider env vars to nil
        self.zeroize_provider_env_vars();
        // Set job metadata
        self.bytes_amt_provider -= fpath.metadata()?.len();
        Ok(())
    }

    pub fn abort(&mut self) {
        unimplemented!();
    }

    // Get correct number of files in job (not just...
    // the len of 'files')
    pub fn get_files_amt(&self) -> Result<u64> {
        let mut correct_files_num: u64 = 0;
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
            if f.path.metadata()?.is_file() {
                let c = tokio::fs::read(&f.path).await?;
                f.hash = hex_digest(Algorithm::SHA256, &c);
            } else {
                // Directory
                for entry in WalkDir::new(&f.path) {
                    let entry = entry?;
                    if entry.metadata()?.is_dir() {
                        continue;
                    }
                    let c = tokio::fs::read(&entry.path()).await?;
                    f.hash = hex_digest(Algorithm::SHA256, &c);
                }
            }
        }
        Ok(())
    }

    fn set_provider_env_vars(&self) -> Result<()> {
        match &self.provider {
            KipProviders::S3(s3) => {
                let s3acc = keyring_get_secret(&format!("com.ciehanski.kip.{}.s3acc", self.name))
                    .context("couldnt get s3acc from keyring")?;
                let s3acc = s3acc.trim_end();
                let s3sec = keyring_get_secret(&format!("com.ciehanski.kip.{}.s3sec", self.name))
                    .context("couldn't get s3sec from keyring")?;
                let s3sec = s3sec.trim_end();
                // Set AWS env vars to user's keys
                env::set_var("AWS_ACCESS_KEY_ID", s3acc);
                env::set_var("AWS_SECRET_ACCESS_KEY", s3sec);
                env::set_var("AWS_REGION", &s3.aws_region);
            }
            KipProviders::Gdrive(_) => {
                let gdrive_id =
                    keyring_get_secret(&format!("com.ciehanski.kip.{}.gdriveid", self.name))
                        .context("couldnt get gdriveid from keyring")?;
                let gdrive_id = gdrive_id.trim_end();
                let gdrive_sec =
                    keyring_get_secret(&format!("com.ciehanski.kip.{}.gdrivesec", self.name))
                        .context("couldn't get gdrivesec from keyring")?;
                let gdrive_sec = gdrive_sec.trim_end();
                // Set AWS env vars to user's keys
                env::set_var("GOOGLE_DRIVE_CLIENT_ID", gdrive_id);
                env::set_var("GOOGLE_DRIVE_CLIENT_SECRET", gdrive_sec);
            }
            _ => {}
        }
        Ok(())
    }

    pub fn delete_keyring_entries(&self) -> Result<()> {
        let job_secret = keyring_get_secret(&format!("com.ciehanski.kip.{}", self.name))
            .context("couldnt get job secret from keyring")?;
        let job_secret = job_secret.trim_end();
        keyring_delete_secret(job_secret)?;
        match self.provider {
            KipProviders::S3(_) => {
                let s3acc = keyring_get_secret(&format!("com.ciehanski.kip.{}.s3acc", self.name))
                    .context("couldnt get s3acc from keyring")?;
                let s3acc = s3acc.trim_end();
                let s3sec = keyring_get_secret(&format!("com.ciehanski.kip.{}.s3sec", self.name))
                    .context("couldn't get s3sec from keyring")?;
                let s3sec = s3sec.trim_end();
                keyring_delete_secret(s3acc)?;
                keyring_delete_secret(s3sec)?;
            }
            KipProviders::Gdrive(_) => {
                let gdrive_id =
                    keyring_get_secret(&format!("com.ciehanski.kip.{}.gdriveid", self.name))
                        .context("couldnt get gdriveid from keyring")?;
                let gdrive_id = gdrive_id.trim_end();
                let gdrive_sec =
                    keyring_get_secret(&format!("com.ciehanski.kip.{}.gdrivesec", self.name))
                        .context("couldn't get gdrivesec from keyring")?;
                let gdrive_sec = gdrive_sec.trim_end();
                keyring_delete_secret(gdrive_id)?;
                keyring_delete_secret(gdrive_sec)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Reset AWS env vars to nil
    pub fn zeroize_provider_env_vars(&self) {
        match &self.provider {
            KipProviders::S3(_) => {
                env::set_var("AWS_ACCESS_KEY_ID", "");
                env::set_var("AWS_SECRET_ACCESS_KEY", "");
                env::set_var("AWS_REGION", "");
            }
            KipProviders::Gdrive(_) => {
                env::set_var("GOOGLE_DRIVE_CLIENT_ID", "");
                env::set_var("GOOGLE_DRIVE_CLIENT_SECRET", "");
            }
            _ => {}
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
            KipStatus::OK => write!(f, "{}", "OK".green()),
            KipStatus::ERR => write!(f, "{}", "ERR".red()),
            KipStatus::WARN => write!(f, "{}", "WARN".yellow()),
            KipStatus::IN_PROGRESS => write!(f, "{}", "IN_PROGRESS".cyan()),
            KipStatus::NEVER_RUN => write!(f, "{}", "NEVER_RUN".bold()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
    use crate::providers::s3::KipS3;
    use aws_sdk_s3::Region;

    use super::*;

    #[test]
    fn test_get_files_amt() {
        let provider = KipProviders::S3(KipS3::new("test1", Region::new("us-east-1".to_owned())));
        let mut j = Job::new("testing1", provider);
        j.files.push(KipFile::new(PathBuf::from("test/vandy.jpg")));
        j.files.push(KipFile::new(PathBuf::from("test/vandy.jpg")));
        j.files.push(KipFile::new(PathBuf::from("test/vandy.jpg")));
        j.files.push(KipFile::new(PathBuf::from("test/random.txt")));
        assert!(j.get_files_amt().is_ok());
        assert_eq!(j.get_files_amt().unwrap(), 4)
    }

    #[tokio::test]
    async fn test_get_file_hashes() {
        let provider = KipProviders::S3(KipS3::new("test1", Region::new("us-east-1".to_owned())));
        let mut j = Job::new("testing1", provider);
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

    #[ignore]
    #[test]
    fn test_set_get_keyring() {
        let provider = KipProviders::S3(KipS3::new(
            "kip_test_bucket",
            Region::new("us-east-1".to_owned()),
        ));
        let j = Job::new("testing2", provider);
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
