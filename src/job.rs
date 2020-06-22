use crate::run::Run;
use chrono::prelude::*;
use colored::*;
use rusoto_core::Region;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub name: String,
    pub aws_bucket: String,
    pub aws_region: Region,
    pub files: Vec<PathBuf>,
    pub files_amt: usize,
    pub runs: HashMap<usize, Run>,
    pub first_run: DateTime<Utc>,
    pub last_run: DateTime<Utc>,
    pub total_runs: usize,
    pub last_status: KipStatus,
    pub created: DateTime<Utc>,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum KipStatus {
    OK,
    ERR,
    WARN,
    IN_PROGRESS,
    NEVER_RUN,
}

impl fmt::Display for KipStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KipStatus::OK => write!(f, "OK"),
            KipStatus::ERR => write!(f, "ERR"),
            KipStatus::WARN => write!(f, "WARN"),
            KipStatus::IN_PROGRESS => write!(f, "IN_PROGRESS"),
            KipStatus::NEVER_RUN => write!(f, "NEVER_RUN"),
        }
    }
}

impl Job {
    pub fn new(name: &str, aws_bucket: &str, aws_region: &str) -> Self {
        Job {
            id: Uuid::new_v4(),
            name: name.to_string(),
            aws_bucket: aws_bucket.to_string(),
            aws_region: Job::parse_s3_region(aws_region),
            files: Vec::<PathBuf>::new(),
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

    // Get correct number of files in job (not just...
    // entries within 'files')
    pub fn get_files_amt(&self) -> Result<usize, Box<dyn Error>> {
        let mut correct_files_num: usize = 0;
        for f in self.files.iter() {
            if f.exists() && f.is_dir() {
                for entry in WalkDir::new(f) {
                    let entry = entry?;
                    if Path::is_dir(entry.path()) {
                        continue;
                    }
                    correct_files_num += 1;
                }
            } else if f.exists() {
                correct_files_num += 1;
            }
        }
        Ok(correct_files_num)
    }

    pub async fn run_upload(
        &mut self,
        secret: &str,
        aws_access: &str,
        aws_secret: &str,
    ) -> Result<(), Box<dyn Error>> {
        // Set AWS env vars for backup
        env::set_var("AWS_ACCESS_KEY_ID", aws_access);
        env::set_var("AWS_SECRET_ACCESS_KEY", aws_secret);
        env::set_var("AWS_REGION", &self.aws_region.name());
        // Create new run
        let mut r = Run::new(self.total_runs + 1);
        // Set job metadata
        self.last_status = KipStatus::IN_PROGRESS;
        // Tell the run to start uploading
        match r.upload(&self, secret).await {
            Ok(_) => (),
            Err(e) => {
                self.last_status = KipStatus::ERR;
                // Add run to job
                self.runs.insert(r.id, r);
                self.total_runs += 1;
                self.last_run = Utc::now();
                if self.first_run.format("%Y-%m-%d %H:%M:%S").to_string() == "1970-01-01 00:00:00" {
                    self.first_run = Utc::now();
                }
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "{} failed to restore '{}': {}.",
                        "[ERR]".red(),
                        &self.name,
                        e
                    ),
                )));
            }
        };
        // Reset AWS env env to nil
        env::set_var("AWS_ACCESS_KEY_ID", "");
        env::set_var("AWS_SECRET_ACCESS_KEY", "");
        env::set_var("AWS_REGION", "");
        // Set job status
        if r.status == KipStatus::WARN {
            self.last_status = KipStatus::WARN;
        } else if r.status == KipStatus::ERR {
            self.last_status = KipStatus::ERR;
        } else {
            self.last_status = KipStatus::OK;
        }
        // Print all logs from run
        if !r.logs.is_empty() {
            for l in r.logs.iter() {
                println!("{}", l);
            }
            // Add run to job only if anything was uploaded
            self.runs.insert(r.id, r);
            self.total_runs += 1;
            self.last_run = Utc::now();
            if self.first_run.format("%Y-%m-%d %H:%M:%S").to_string() == "1970-01-01 00:00:00" {
                self.first_run = Utc::now();
            }
        } else {
            println!("{} no file changes detected.", "[INFO]".yellow());
        }
        // Success
        Ok(())
    }

    pub async fn run_restore(
        &self,
        run: usize,
        secret: &str,
        aws_access: &str,
        aws_secret: &str,
        output_folder: Option<String>,
    ) -> Result<(), Box<dyn Error>> {
        // Set AWS env vars for backup
        env::set_var("AWS_ACCESS_KEY_ID", aws_access);
        env::set_var("AWS_SECRET_ACCESS_KEY", aws_secret);
        env::set_var("AWS_REGION", &self.aws_region.name());
        // Get run from job
        let r = match self.runs.get(&run) {
            Some(run) => run,
            None => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "{} failed to restore '{}': couldn't find run {}.",
                        "[ERR]".red(),
                        &self.name,
                        run
                    ),
                )));
            }
        };
        // Tell the run to start uploading
        match r.restore(&self, secret, output_folder).await {
            Ok(_) => (),
            Err(e) => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "{} failed to restore '{}': {}.",
                        "[ERR]".red(),
                        &self.name,
                        e
                    ),
                )));
            }
        };
        // Reset AWS env env to nil
        env::set_var("AWS_ACCESS_KEY_ID", "");
        env::set_var("AWS_SECRET_ACCESS_KEY", "");
        env::set_var("AWS_REGION", "");
        // Success
        Ok(())
    }

    pub fn abort(&mut self) {
        unimplemented!();
    }
}
