use crate::job::Job;
use dialoguer::Password;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{create_dir, read, File, OpenOptions};
use std::io::prelude::*;
use std::io::{stdin, stdout, Error};
use std::path::Path;

// TODO: use directories crate
const KIP_CONF: &'static str = "kip_test/kip.json";
const KIP_CONF_DIR: &'static str = "kip_test";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipConf {
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub backup_interval: usize,
    pub jobs: HashMap<String, Job>,
}

impl KipConf {
    pub fn new() -> Result<(), Error> {
        if Path::new(KIP_CONF).exists() {
            // If kip configuration already exists, just return.
            return Ok(());
        }
        // Check if ~/.kip already exists
        if !Path::new(KIP_CONF_DIR).exists() {
            // Create new ~/.kip dir
            create_dir(KIP_CONF_DIR)?;
        }
        // Create new default kip config
        let default_conf = KipConf {
            s3_access_key: "".to_string(),
            s3_secret_key: "".to_string(),
            backup_interval: 60,
            jobs: HashMap::<String, Job>::new(),
        };
        // Write default config to ~/kip/kip.json
        let mut conf_file = File::create(KIP_CONF)?;
        let json_conf = serde_json::to_string_pretty(&default_conf)?;
        conf_file.write_all(json_conf.as_bytes())?;
        Ok(())
    }

    pub fn get() -> Result<KipConf, Error> {
        let file = read(KIP_CONF)?;
        let kc: KipConf = serde_json::from_slice(&file)?;
        Ok(kc)
    }

    pub fn save(self) -> Result<(), Error> {
        let mut file = OpenOptions::new().write(true).open(KIP_CONF)?;
        let json_conf = serde_json::to_string_pretty(&self)?;
        file.write_all(json_conf.as_bytes())?;
        Ok(())
    }

    pub fn prompt_s3_keys(&mut self) {
        // If user has not provided S3 credentials or this is
        // their first time using kip
        // Get S3 access key from user input
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

    pub async fn poll_backup_jobs(self, _secret: &str) {
        // for (_, ref mut j) in self.jobs {
        //     match j
        //         .run_upload(secret, &self.s3_access_key, &self.s3_secret_key)
        //         .await
        //     {
        //         Ok(_) => {
        //             // Print all logs from run
        //             for l in r.logs.iter() {
        //                 println!("{}", l);
        //             }
        //         }
        //         Err(_) => (),
        //     }
        // }
    }
}
