//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::compress::KipCompressOpts;
use crate::crypto::{keyring_delete_secret, keyring_get_secret};
use crate::providers::KipProviders;
use crate::run::{open_file, Run};
use anyhow::{bail, Context, Result};
use chrono::prelude::*;
use colored::*;
use crypto_hash::{hex_digest, Algorithm};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::fmt::{Debug, Display};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub name: String,
    pub provider: KipProviders,
    pub compress: KipCompressOpts,
    pub files: Vec<KipFile>,
    pub files_amt: u64,
    pub excluded_files: Vec<PathBuf>,
    pub excluded_file_types: Vec<String>,
    pub runs: BTreeMap<usize, Run>,
    pub bytes_amt_provider: u64,
    pub first_run: DateTime<Utc>,
    pub last_run: DateTime<Utc>,
    pub total_runs: u64,
    pub last_status: KipStatus,
    pub created: DateTime<Utc>,
    pub paused: bool,
}

impl Job {
    pub fn new<S: Into<String>>(
        name: S,
        provider: KipProviders,
        compress: KipCompressOpts,
    ) -> Self {
        // Initialize default UTC DateTime variable
        let time_init = match Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).earliest() {
            Some(t) => t,
            None => {
                let ndt = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                TimeZone::from_utc_datetime(&Utc, &ndt)
            }
        };
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            provider,
            compress,
            files: Vec::new(),
            files_amt: 0,
            excluded_files: Vec::new(),
            excluded_file_types: Vec::new(),
            runs: BTreeMap::new(),
            bytes_amt_provider: 0,
            first_run: time_init,
            last_run: time_init,
            total_runs: 0,
            last_status: KipStatus::NEVER_RUN,
            created: Utc::now(),
            paused: false,
        }
    }

    pub fn provider_name(&self) -> &str {
        match &self.provider {
            KipProviders::S3(s3) => &s3.aws_bucket,
            KipProviders::Usb(usb) => &usb.name,
            KipProviders::Gdrive(_) => "Google Drive",
        }
    }

    pub async fn start_run(&mut self, secret: &str, follow_links: bool) -> Result<()> {
        // Check and confirm that job is not paused
        if self.paused {
            bail!(
                "unable to run. '{}' is paused. Please run 'kip resume {}' to resume job.",
                self.name,
                self.name
            )
        }
        // Create new run
        let mut r = Run::new(
            self.total_runs + 1,
            KipCompressOpts::new(
                self.compress.enabled,
                self.compress.alg,
                self.compress.level,
            ),
        );
        // Create Arc of current job to avoid
        // clones for each run
        let job_arc = Arc::new(self.clone());
        // Set job metadata
        self.last_status = KipStatus::IN_PROGRESS;
        // Set provider env vars for backup
        self.set_provider_env_vars()?;
        // Tell the run to start uploading
        match r.start(job_arc, secret.to_string(), follow_links).await {
            Ok(_) => {
                // Reset provider env vars to nil
                self.zeroize_provider_env_vars();
                // Set job status equal to run's status
                self.last_status = r.status;
                // Print all logs from run
                if self.last_status != KipStatus::OK_SKIPPED {
                    self.bytes_amt_provider += r.bytes_uploaded;
                    // Get new file hashes
                    self.get_file_hashes(follow_links).await?;
                    // Add run to job only if anything was uploaded
                    self.runs.insert(r.id.try_into()?, r);
                    self.total_runs += 1;
                    self.last_run = Utc::now();
                    if self.first_run.format("%Y-%m-%d %H:%M:%S").to_string()
                        == "1970-01-01 00:00:00"
                    {
                        self.first_run = Utc::now();
                    }
                    println!(
                        "{} job '{}' completed uploading to '{}' successfully.",
                        "[OK]".green(),
                        &self.name,
                        self.get_provider(),
                    );
                } else {
                    println!("{} skipped, no file changes detected.", "[INFO]".yellow());
                }
            }
            Err(e) => {
                // Reset provider env vars to nil
                self.zeroize_provider_env_vars();
                // Set job status equal to run's status
                self.bytes_amt_provider += r.bytes_uploaded;
                // Set job status
                self.last_status = KipStatus::ERR;
                // Add run to job
                self.runs.insert(r.id.try_into()?, r);
                self.total_runs += 1;
                self.last_run = Utc::now();
                if self.first_run.format("%Y-%m-%d %H:%M:%S").to_string() == "1970-01-01 00:00:00" {
                    self.first_run = Utc::now();
                }
                println!(
                    "{} job '{}' upload to '{}' failed.",
                    "[ERR]".red(),
                    &self.name,
                    self.get_provider(),
                );
                bail!("{e}.")
            }
        };
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
                Ok(_) => {
                    println!(
                        "{} job '{}' completed restore from '{}' successfully.",
                        "[OK]".green(),
                        &self.name,
                        self.get_provider(),
                    )
                }
                Err(e) => {
                    // Reset provider env vars to nil
                    self.zeroize_provider_env_vars();
                    println!(
                        "{} job '{}' restore from '{}' failed.",
                        "[ERR]".red(),
                        &self.name,
                        self.get_provider(),
                    );
                    bail!("{e}.")
                }
            };
            // Reset provider env vars to nil
            self.zeroize_provider_env_vars();
        } else {
            bail!("couldn't find run {run}.")
        }
        // Success
        Ok(())
    }

    #[instrument]
    pub async fn purge_file(&mut self, f: &str) -> Result<()> {
        // Find all the runs that contain this file's chunks
        // and remove them from S3.
        let fpath = Path::new(&f).canonicalize()?;
        self.set_provider_env_vars()?;

        // Create job's provider client
        let client = self.provider.get_client().await?;
        
        for run in self.runs.iter() {
            for kfc in run.1.delta.iter() {
                if kfc.file.path == fpath {
                    // Convert chunks into async stream
                    let mut chunks_stream = tokio_stream::iter(kfc.chunks.values());
                    // Delete each chunk from provider
                    while let Some(chunk) = chunks_stream.next().await {
                        self.provider.delete(&client, &chunk.remote_path).await?;
                    }
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

    /// Get correct number of files in job (not just...
    /// the len of 'files' Vec)
    pub fn set_files_amt(&mut self, follow_links: bool) -> Result<()> {
        let mut correct_files_num: u64 = 0;
        for f in self.files.iter() {
            if f.path.exists() && f.path.is_dir() {
                for entry in WalkDir::new(&f.path).follow_links(follow_links) {
                    if entry?.path().is_dir() {
                        continue;
                    }
                    correct_files_num += 1;
                }
            } else if f.path.exists() {
                correct_files_num += 1;
            }
        }
        self.files_amt = correct_files_num;
        Ok(())
    }

    /// Read each file in the job and store their SHA256 hashes
    async fn get_file_hashes(&mut self, follow_links: bool) -> Result<()> {
        for kf in self.files.iter_mut() {
            // Set File Hash
            if kf.is_file()? {
                let file = open_file(&kf.path, kf.len.try_into()?).await?;
                let hash = hex_digest(Algorithm::SHA256, &file);
                kf.set_hash(hash);
            } else {
                // Set Directory Hash
                let mut dir_hash_str = String::new();
                for entry in WalkDir::new(&kf.path).follow_links(follow_links) {
                    let entry = entry?;
                    if entry.metadata()?.is_dir() {
                        continue;
                    }
                    let f = open_file(entry.path(), entry.metadata()?.len()).await?;
                    let hash = hex_digest(Algorithm::SHA1, &f);
                    // Push the subfile's path (if renamed, moved)
                    dir_hash_str.push_str(&entry.path().display().to_string());
                    // Push the subfile's hash (if updated, changed)
                    dir_hash_str.push_str(&hash);
                }
                // Hash the colletion of dir's files and their hashes
                let dir_hash = hex_digest(Algorithm::SHA256, dir_hash_str.as_bytes());
                // Set the "file"'s dir hash
                kf.set_hash(dir_hash);
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
        keyring_delete_secret(&format!("com.ciehanski.kip.{}", self.name))
            .context("couldnt delete job secret from keyring")?;
        match self.provider {
            KipProviders::S3(_) => {
                keyring_delete_secret(&format!("com.ciehanski.kip.{}.s3acc", self.name))
                    .context("couldn't delete S3 access key from keyring")?;
                keyring_delete_secret(&format!("com.ciehanski.kip.{}.s3sec", self.name))
                    .context("couldn't delete S3 secret key from keyring")?;
            }
            KipProviders::Gdrive(_) => {
                keyring_delete_secret(&format!("com.ciehanski.kip.{}.gdriveid", self.name))
                    .context("couldnt delete Gdrive access ID from keyring")?;
                keyring_delete_secret(&format!("com.ciehanski.kip.{}.gdrivesec", self.name))
                    .context("couldn't delete Gdrive secret key from keyring")?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Reset provider env vars to nil
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

    fn get_provider(&self) -> String {
        match &self.provider {
            KipProviders::S3(s3) => s3.aws_bucket.to_owned(),
            KipProviders::Usb(usb) => usb.name.to_owned(),
            KipProviders::Gdrive(gdrive) => {
                if let Some(pf) = gdrive.parent_folder.to_owned() {
                    format!("My Drive/{pf}")
                } else {
                    "My Drive/".to_string()
                }
            }
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum KipStatus {
    OK,
    OK_SKIPPED,
    ERR,
    WARN,
    IN_PROGRESS,
    NEVER_RUN,
}

impl Display for KipStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KipStatus::OK => write!(f, "{}", "OK".green()),
            KipStatus::OK_SKIPPED => write!(f, "{}", "OK_SKIPPED".green()),
            KipStatus::ERR => write!(f, "{}", "ERR".red()),
            KipStatus::WARN => write!(f, "{}", "WARN".yellow()),
            KipStatus::IN_PROGRESS => write!(f, "{}", "IN_PROGRESS".cyan()),
            KipStatus::NEVER_RUN => write!(f, "{}", "NEVER_RUN".bold()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KipFile {
    pub name: String,
    pub path: PathBuf,
    pub hash: String,
    pub len: usize,
}

impl KipFile {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        // Get len at time of creation
        let len: usize = path.as_ref().metadata()?.len().try_into()?;
        Ok(KipFile {
            name: path
                .as_ref()
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            path: path.as_ref().to_path_buf(),
            hash: String::new(),
            len,
        })
    }

    pub fn set_hash(&mut self, hash: String) {
        self.hash = hash;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn path_str(&self) -> String {
        self.path.display().to_string()
    }

    pub fn is_empty(&self) -> bool {
        if self.len == 0 {
            return true;
        }
        false
    }

    pub fn is_file(&self) -> Result<bool> {
        Ok(self.path.metadata()?.is_file())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::{KipCompressAlg, KipCompressLevel, KipCompressOpts};
    use crate::providers::s3::KipS3;
    use aws_sdk_s3::config::Region;

    #[test]
    fn test_set_files_amt() {
        let provider = KipProviders::S3(KipS3::new("test1", Region::new("us-east-1".to_owned())));
        let mut j = Job::new(
            "testing1",
            provider,
            KipCompressOpts::new(true, KipCompressAlg::Zstd, KipCompressLevel::Best),
        );
        j.files
            .push(KipFile::new(PathBuf::from("test/vandy.jpg")).unwrap());
        j.files
            .push(KipFile::new(PathBuf::from("test/vandy.jpg")).unwrap());
        j.files
            .push(KipFile::new(PathBuf::from("test/vandy.jpg")).unwrap());
        j.files
            .push(KipFile::new(PathBuf::from("test/random.txt")).unwrap());
        assert!(j.set_files_amt(false).is_ok());
        assert_eq!(j.files_amt, 4)
    }

    #[test]
    fn test_set_files_amt_dir() {
        let provider = KipProviders::S3(KipS3::new("test1", Region::new("us-east-1".to_owned())));
        let mut j = Job::new(
            "testing1",
            provider,
            KipCompressOpts::new(true, KipCompressAlg::Zstd, KipCompressLevel::Best),
        );
        j.files
            .push(KipFile::new(PathBuf::from("test/test_dir/")).unwrap());
        assert!(j.set_files_amt(false).is_ok());
        assert_eq!(j.files_amt, 4)
    }

    #[tokio::test]
    async fn test_get_file_hashes() {
        let provider = KipProviders::S3(KipS3::new("test1", Region::new("us-east-1".to_owned())));
        let mut j = Job::new(
            "testing1",
            provider,
            KipCompressOpts::new(true, KipCompressAlg::Zstd, KipCompressLevel::Best),
        );
        if !cfg!(windows) {
            // Unix, Mac, Linux, etc
            j.files
                .push(KipFile::new(PathBuf::from("test/vandy.jpg")).unwrap());
            j.files
                .push(KipFile::new(PathBuf::from("test/random.txt")).unwrap());
        } else {
            // Windows
            j.files
                .push(KipFile::new(PathBuf::from(r".\test\vandy.jpg")).unwrap());
            j.files
                .push(KipFile::new(PathBuf::from(r".\test\random.txt")).unwrap());
        }
        let hash_result = j.get_file_hashes(false).await;
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

    #[tokio::test]
    async fn test_get_file_hashes_dir() {
        let provider = KipProviders::S3(KipS3::new("test1", Region::new("us-east-1".to_owned())));
        let mut j = Job::new(
            "testing1",
            provider,
            KipCompressOpts::new(true, KipCompressAlg::Zstd, KipCompressLevel::Best),
        );
        if !cfg!(windows) {
            // Unix, Mac, Linux, etc
            j.files
                .push(KipFile::new(PathBuf::from("test/test_dir/")).unwrap());
        } else {
            // Windows
            j.files
                .push(KipFile::new(PathBuf::from(r".\test\test_dir\")).unwrap());
        }
        let hash_result = j.get_file_hashes(false).await;
        assert!(hash_result.is_ok());
        assert_eq!(
            j.files[0].hash,
            "d9317775d9b1dccdad75fa47b521b47d2079e813ff290d74a277944efb701909"
        )
    }
}
