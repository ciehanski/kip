use crate::job::Job;
use chrono::prelude::*;
use dialoguer::Password;
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{create_dir, read, File, OpenOptions};
use std::io::prelude::*;
use std::io::{stdin, stdout, Error};
use std::path::Path;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipConf {
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub backup_interval: u64,
    pub jobs: HashMap<String, Job>,
}

impl KipConf {
    pub fn new() -> Result<(), Error> {
        if let Some(proj_dirs) = ProjectDirs::from("com", "ciehanski", "kip") {
            if proj_dirs.config_dir().join("kip.json").exists() {
                // If kip configuration already exists, just return
                return Ok(());
            }
            // Check if ~/.kip already exists
            if !Path::new(proj_dirs.config_dir()).exists() {
                // Create new ~/.kip dir
                create_dir(proj_dirs.config_dir())?;
            }
            // Create new default kip config
            let default_conf = KipConf {
                s3_access_key: "".to_string(),
                s3_secret_key: "".to_string(),
                backup_interval: 60,
                jobs: HashMap::<String, Job>::new(),
            };
            // Write default config to ~/kip/kip.json
            let mut conf_file = File::create(proj_dirs.config_dir().join("kip.json"))?;
            let json_conf = serde_json::to_string_pretty(&default_conf)?;
            conf_file.write_all(json_conf.as_bytes())?;
            Ok(())
        } else {
            Err(Error::new(
                std::io::ErrorKind::NotFound,
                "unable to determine kip configuration directory",
            ))
        }
    }

    pub fn get() -> Result<KipConf, Error> {
        if let Some(proj_dirs) = ProjectDirs::from("com", "ciehanski", "kip") {
            let file = read(proj_dirs.config_dir().join("kip.json"))?;
            let kc: KipConf = serde_json::from_slice(&file)?;
            Ok(kc)
        } else {
            Err(Error::new(
                std::io::ErrorKind::NotFound,
                "unable to determine kip configuration directory",
            ))
        }
    }

    pub fn save(self) -> Result<(), Error> {
        if let Some(proj_dirs) = ProjectDirs::from("com", "ciehanski", "kip") {
            let mut file = OpenOptions::new()
                .write(true)
                .open(proj_dirs.config_dir().join("kip.json"))?;
            let json_conf = serde_json::to_string_pretty(&self)?;
            // Overwrite the conf file
            file.set_len(0)?;
            file.write_all(json_conf.as_bytes())?;
            Ok(())
        } else {
            Err(Error::new(
                std::io::ErrorKind::NotFound,
                "unable to determine kip configuration directory",
            ))
        }
    }

    pub fn prompt_s3_keys(&mut self) {
        // If user has not provided S3 credentials or this is
        // their first time using kip
        // Get S3 access key from user input
        if self.s3_access_key.is_empty() || self.s3_secret_key.is_empty() {
            print!("Please provide your S3 access key: ");
            stdout().flush().expect("[ERR] failed to flush stdout.");
            let mut acc_key = String::new();
            stdin()
                .read_line(&mut acc_key)
                .expect("[ERR] failed to read from stdin");
            self.s3_access_key = String::from(acc_key.trim_end());
            // Get S3 secret key from user input
            let sec_key = Password::new()
                .with_prompt("Please provide your S3 secret key")
                .interact()
                .expect("[ERR] failed to create S3 secret key prompt.");
            self.s3_secret_key = sec_key;
        }
    }

    pub async fn poll_backup_jobs(&mut self, secret: &str) {
        loop {
            if !self.jobs.is_empty() {
                for (_, j) in self.jobs.iter_mut() {
                    // Get last run start duration
                    let run = j
                        .runs
                        .get(&j.runs.len())
                        .expect("[ERR] failed to get latest run.");
                    let dur_since_run_start = Utc::now().signed_duration_since(run.started);
                    // If the duration since the last run started is more than
                    // the configured backup interval, start an upload run
                    if dur_since_run_start.num_minutes() >= self.backup_interval as i64 {
                        match j
                            .run_upload(secret, &self.s3_access_key, &self.s3_secret_key)
                            .await
                        {
                            Ok(_) => (),
                            Err(e) => panic!("{}", e),
                        }
                    }
                }
            }
        }
    }
}
