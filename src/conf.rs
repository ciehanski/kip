//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::job::Job;
use chrono::prelude::*;
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{create_dir, read, File, OpenOptions};
use std::io::prelude::*;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipConf {
    pub backup_interval: u64,
    pub jobs: HashMap<String, Job>,
}

impl KipConf {
    pub fn new() -> Result<Arc<RwLock<Self>>, Error> {
        // Linux:   /home/alice/.config/kip
        // Windows: C:\Users\Alice\AppData\Roaming\ciehanski\kip
        // macOS:   /Users/Alice/Library/Application Support/com.ciehanski.kip
        if let Some(proj_dirs) = ProjectDirs::from("com", "ciehanski", "kip") {
            if proj_dirs.config_dir().join("kip.json").exists() {
                // If kip configuration already exists, read and return it
                let file = read(proj_dirs.config_dir().join("kip.json"))?;
                let kc: KipConf = serde_json::from_slice(&file)?;
                return Ok(Arc::new(RwLock::new(kc)));
            }
            // Check if ~/.kip already exists
            if !Path::new(proj_dirs.config_dir()).exists() {
                // Create new ~/.kip dir
                create_dir(proj_dirs.config_dir())?;
            }
            // Create new default kip config
            let default_conf = KipConf {
                backup_interval: 60,
                jobs: HashMap::<String, Job>::new(),
            };
            // Write default config to ~/kip/kip.json
            let mut conf_file = File::create(proj_dirs.config_dir().join("kip.json"))?;
            let json_conf = serde_json::to_string_pretty(&default_conf)?;
            conf_file.write_all(json_conf.as_bytes())?;
            Ok(Arc::new(RwLock::new(default_conf)))
        } else {
            Err(Error::new(
                ErrorKind::NotFound,
                "unable to determine kip configuration directory",
            ))
        }
    }

    pub fn save(&mut self) -> Result<(), Error> {
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
                ErrorKind::NotFound,
                "unable to determine kip configuration directory",
            ))
        }
    }

    pub async fn poll_backup_jobs(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.jobs.is_empty() {
            for (_, j) in self.jobs.iter_mut() {
                // Get last run start duration
                let run = match j.runs.get(&j.runs.len()) {
                    Some(r) => r,
                    None => {
                        return Err(Box::new(std::io::Error::new(
                            ErrorKind::NotFound,
                            String::from("run not found."),
                        )));
                    }
                };
                let dur_since_run_start = Utc::now().signed_duration_since(run.started);
                // If the duration since the last run started is more than
                // the configured backup interval, start an upload run
                let secret = j.secret.clone();
                if dur_since_run_start.num_minutes() >= self.backup_interval as i64 {
                    j.run_upload(&secret).await?;
                }
            }
        }
        Ok(())
    }
}
