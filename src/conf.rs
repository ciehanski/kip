//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::compress::{KipCompressAlg, KipCompressLevel};
use crate::crypto::keyring_get_secret;
use crate::job::Job;
use crate::smtp::{KipSmtpOpts, KipSmtpProtocols};
use anyhow::{bail, Result};
use chrono::prelude::*;
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{create_dir, read, File, OpenOptions};
use std::io::prelude::*;
use std::sync::Arc;
use tokio::sync::RwLock;

const KIP_CONF: &str = "kip.toml";
const KIP_METADATA: &str = "kip_metadata.json";

#[derive(Debug, Deserialize, Serialize)]
pub struct KipConf {
    /// Uses TOML
    pub settings: KipConfOpts,
    pub smtp_config: KipSmtpOpts,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KipConfOpts {
    /// How often (in minutes) you would like kip to run automatic
    /// background uploads of backup jobs.
    pub backup_interval: u64,
    /// Specifiy how many threads you want kip to run on.
    /// default: number of device CPUs
    pub worker_threads: usize,
    /// Enable compression of backups.
    /// default: true
    pub compression: bool,
    /// default: Zstd
    pub compression_alg: KipCompressAlg,
    /// Sets the level of compression of the algorithm
    /// default: Best
    pub compress_level: KipCompressLevel,
    /// Specifiy if you would like kip to store secrets in the
    /// default OS keyring or a BYOK custom keyring service.
    /// default: true
    pub os_keyring: bool,
    /// Indicate if you would like hidden files to be skipped
    /// during backup.
    /// default: false
    pub skip_hidden_files: bool,
    /// Indicate if you would like kip to backup symlink targeted
    /// files during backup.
    /// default: true
    pub follow_symlinks: bool,
    /// Enable or disable email noticiations on job runs.
    /// default false
    pub email_notification: bool,
    /// Whether you would like kip to continue running
    /// backups if your device's battery is low.
    /// default: false
    pub run_on_low_battery: bool,
    /// Sets the verbosity of debug logs.
    /// default: Info
    pub debug_level: KipDebugLevel,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KipConfMetadata {
    /// This is where we store all the jobs' and runs'
    /// metadata. This is seperate from the conf file
    pub jobs: HashMap<String, Job>,
}

type KipConfArc = Arc<KipConf>;
type KipConfMetadataArc = Arc<RwLock<KipConfMetadata>>;

impl KipConf {
    fn default() -> Self {
        KipConf {
            settings: KipConfOpts {
                backup_interval: 60,
                worker_threads: num_cpus::get(),
                compression: true,
                compression_alg: KipCompressAlg::Zstd,
                compress_level: KipCompressLevel::Default,
                os_keyring: true,
                skip_hidden_files: false,
                follow_symlinks: true,
                email_notification: false,
                run_on_low_battery: false,
                debug_level: KipDebugLevel::INFO,
            },
            smtp_config: KipSmtpOpts {
                username: String::from("kip@gmail.com"),
                smtp_host: String::from("smtp.gmail.com"),
                protocol: KipSmtpProtocols::StartTLS,
                recipient: String::from("me@gmail.com"),
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
            // Check if $PROJECT_DIR does not exist
            if !proj_dirs.config_dir().exists() {
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
                if dur_since_run_start.num_minutes() >= kc.settings.backup_interval.try_into()? {
                    j.start_run(&secret, kc.settings.follow_symlinks).await?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum KipDebugLevel {
    INFO,
    ERROR,
    WARN,
    TRACE,
    DEBUG,
}

impl KipDebugLevel {
    pub fn parse(&self) -> tracing::Level {
        match &self {
            KipDebugLevel::INFO => tracing::Level::INFO,
            KipDebugLevel::ERROR => tracing::Level::ERROR,
            KipDebugLevel::WARN => tracing::Level::WARN,
            KipDebugLevel::TRACE => tracing::Level::TRACE,
            KipDebugLevel::DEBUG => tracing::Level::DEBUG,
        }
    }
}
