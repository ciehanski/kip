//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::crypto::keyring_get_secret;
use crate::job::Job;
use anyhow::{bail, Result};
use chrono::prelude::*;
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{create_dir, read, File, OpenOptions};
use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

const KIP_CONF: &str = "kip.toml";
const KIP_METADATA: &str = "kip_metadata.json";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipConf {
    /// Uses TOML
    pub settings: KipConfOpts,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipConfOpts {
    /// How often you would like kip to run automatic
    /// backup jobs
    pub backup_interval: u64,
    /// By default, kip will only dedupe within
    /// a job's remote folder, not the whole provider's repository
    pub dedupe_repo: bool,
    /// Specifiy how many threads you want kip to run on
    pub worker_threads: usize,
    /// Specifiy if you would like kip to store secrets in the
    /// default OS keyring or a BYOK custom keyring service
    /// default: true
    pub os_keyring: bool,
    pub skip_hidden_files: bool,
    pub follow_symlinks: bool,
    pub bandwidth_limit: u64,
    pub email_notification: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipConfMetadata {
    /// This is where we store all the jobs' and runs'
    /// metadata. This is seperate from the conf file because
    /// it can get quite messy and hard to read.
    /// Uses JSON
    pub jobs: HashMap<String, Job>,
}

type KipConfArc = Arc<KipConf>;
type KipConfMetadataArc = Arc<RwLock<KipConfMetadata>>;

impl KipConf {
    fn default() -> Self {
        KipConf {
            settings: KipConfOpts {
                backup_interval: 60,
                dedupe_repo: false,
                worker_threads: num_cpus::get(),
                os_keyring: true,
                skip_hidden_files: false,
                follow_symlinks: true,
                bandwidth_limit: 0,
                email_notification: false,
            },
        }
    }

    /// KipConf directory:
    /// Linux:   /home/alice/.config/kip
    /// Windows: C:\Users\Alice\AppData\Roaming\ciehanski\kip
    /// macOS:   /Users/Alice/Library/Application Support/com.ciehanski.kip
    pub fn new() -> Result<(KipConfArc, KipConfMetadataArc)> {
        if let Some(proj_dirs) = ProjectDirs::from("com", "ciehanski", "kip") {
            if proj_dirs.config_dir().join(KIP_CONF).exists() {
                // If kip configuration already exists, read and return it
                let kc_file = read(proj_dirs.config_dir().join(KIP_CONF))?;
                let kc: KipConf = toml::from_slice(&kc_file)?;
                if proj_dirs.config_dir().join(KIP_METADATA).exists() {
                    let md_file = read(proj_dirs.config_dir().join(KIP_METADATA))?;
                    let md = serde_json::from_slice(&md_file)?;
                    return Ok((Arc::new(kc), Arc::new(RwLock::new(md))));
                }
            }
            // Check if $PROJECT_DIR already exists
            if !Path::new(proj_dirs.config_dir()).exists() {
                // Create new $PROJECT_DIR since it doesn't exist
                create_dir(proj_dirs.config_dir())?;
            }
            // Create new default kip config
            let default_conf = KipConf::default();
            // Write default config to $PROJECT_DIR/kip.toml
            let mut conf_file = File::create(proj_dirs.config_dir().join(KIP_CONF))?;
            let toml_conf = toml::to_string_pretty(&default_conf)?;
            conf_file.write_all(toml_conf.as_bytes())?;
            // Create new default kip metadata
            let default_metadata = KipConfMetadata::default();
            // Write default metadata to $PROJECT_DIR/kip_metadata.json
            let mut metadata_file = File::create(proj_dirs.config_dir().join(KIP_METADATA))?;
            let json_metadata = serde_json::to_string_pretty(&default_metadata)?;
            metadata_file.write_all(json_metadata.as_bytes())?;
            Ok((
                Arc::new(default_conf),
                Arc::new(RwLock::new(default_metadata)),
            ))
        } else {
            bail!("unable to determine kip configuration directory")
        }
    }
}

impl KipConfMetadata {
    fn default() -> Self {
        KipConfMetadata {
            jobs: HashMap::<String, Job>::new(),
        }
    }

    pub fn save(&mut self) -> Result<()> {
        if let Some(proj_dirs) = ProjectDirs::from("com", "ciehanski", "kip") {
            let mut file = OpenOptions::new()
                .write(true)
                .open(proj_dirs.config_dir().join(KIP_METADATA))?;
            let json_conf = serde_json::to_string_pretty(&self)?;
            // Overwrite the metadata file
            file.set_len(0)?;
            file.write_all(json_conf.as_bytes())?;
            Ok(())
        } else {
            bail!("unable to determine kip configuration directory")
        }
    }

    /// Requires "Always Allow" access to your keyring entries for kip
    pub async fn poll_backup_jobs(&mut self, kc: &KipConf) -> Result<()> {
        if !self.jobs.is_empty() {
            for (_, j) in self.jobs.iter_mut() {
                if j.paused {
                    continue;
                }
                // Get last run start duration
                let run = match j.runs.get(&j.runs.len()) {
                    Some(r) => r,
                    None => {
                        continue;
                    }
                };
                let dur_since_run_start = Utc::now().signed_duration_since(run.started);
                // If the duration since the last run started is more than
                // the configured backup interval, start an upload run
                let secret = keyring_get_secret(&format!("com.ciehanski.kip.{}", &j.name))?;
                if dur_since_run_start.num_minutes() >= kc.settings.backup_interval as i64 {
                    j.start_run(&secret).await?;
                }
            }
        }
        Ok(())
    }
}
