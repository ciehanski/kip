//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use chrono::prelude::*;
use colored::*;
use dialoguer::Confirm;
use dialoguer::Password;
use kip::args::{Opt, Subcommands};
use kip::conf::KipConf;
use kip::crypto::{keyring_get_secret, keyring_set_secret};
use kip::job::{Job, KipFile};
use pretty_bytes::converter::convert;
use prettytable::{Cell, Row, Table};
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use structopt::StructOpt;

#[tokio::main]
async fn main() {
    // Get config
    let cfg_file = KipConf::new().unwrap_or_else(|e| {
        terminate!(
            5,
            "{} failed to get kip configuration: {}.",
            "[ERR]".red(),
            e
        );
    });
    let cfg = Arc::clone(&cfg_file);

    // Get subcommands and args
    let args: Opt = Opt::from_args();
    // let _debug = args.debug;

    // Match user input command
    match args.subcommands {
        // Create a new job
        Subcommands::Init { job } => {
            let mut cfg = cfg.write().await;
            // Ensure that job does not already exist with
            // the provided name.
            for (j, _) in cfg.jobs.iter() {
                if j == &job {
                    terminate!(17, "{} job '{}' already exists.", "[ERR]".red(), job);
                }
            }
            // Get secret from user input
            let secret = Password::new()
                .with_prompt("Please provide your encryption secret")
                .interact()
                .expect("[ERR] failed to create encryption secret prompt.");
            // Store secret onto local OS keyring
            keyring_set_secret(&format!("com.ciehanski.kip.{}", &job), &secret).unwrap_or_else(
                |e| {
                    terminate!(
                        5,
                        "{} failed to push secret onto keyring: {}.",
                        "[ERR]".red(),
                        e
                    );
                },
            );
            // Get S3 access key from user input
            print!("Please provide the S3 access key: ");
            std::io::stdout()
                .flush()
                .expect("[ERR] failed to flush stdout.");
            let mut s3_acc_key = String::new();
            std::io::stdin()
                .read_line(&mut s3_acc_key)
                .expect("[ERR] failed to read S3 access key from stdin.");
            // Store S3 access key onto local OS keyring
            keyring_set_secret(&format!("com.ciehanski.kip.{}.s3acc", &job), &s3_acc_key)
                .unwrap_or_else(|e| {
                    terminate!(
                        5,
                        "{} failed to push S3 access key onto keyring: {}.",
                        "[ERR]".red(),
                        e
                    );
                });
            // Get S3 secret key from user input
            print!("Please provide the S3 secret key: ");
            std::io::stdout()
                .flush()
                .expect("[ERR] failed to flush stdout.");
            let mut s3_sec_key = String::new();
            std::io::stdin()
                .read_line(&mut s3_sec_key)
                .expect("[ERR] failed to read S3 secret key from stdin.");
            // Store S3 secret key onto local OS keyring
            keyring_set_secret(&format!("com.ciehanski.kip.{}.s3sec", &job), &s3_sec_key)
                .unwrap_or_else(|e| {
                    terminate!(
                        5,
                        "{} failed to push S3 secret key onto keyring: {}.",
                        "[ERR]".red(),
                        e
                    );
                });
            // Get S3 bucket name from user input
            print!("Please provide the S3 bucket name: ");
            std::io::stdout()
                .flush()
                .expect("[ERR] failed to flush stdout.");
            let mut s3_bucket_name = String::new();
            std::io::stdin()
                .read_line(&mut s3_bucket_name)
                .expect("[ERR] failed to read S3 bucket name from stdin.");
            // Get S3 bucket region from user input
            print!("Please provide the S3 region: ");
            std::io::stdout()
                .flush()
                .expect("[ERR] failed to flush stdout.");
            let mut s3_region = String::new();
            std::io::stdin()
                .read_line(&mut s3_region)
                .expect("[ERR] failed to read from stdin.");
            // Create the new job
            let new_job = Job::new(&job, s3_bucket_name.trim_end(), s3_region.trim_end());
            // Push new job in config
            cfg.jobs.insert(job.clone(), new_job);
            // // Store new job in config
            match cfg.save() {
                Ok(_) => println!("{} job '{}' successfully created.", "[OK]".green(), job),
                Err(e) => terminate!(
                    7,
                    "{} failed to save kip configuration: {}",
                    "[ERR]".red(),
                    e
                ),
            }
        }

        // Add more files or directories to job
        Subcommands::Add { job, file_path } => {
            let mut cfg = cfg.write().await;
            // Check if file or directory exists
            for f in &file_path {
                if !Path::new(f).exists() {
                    terminate!(2, "{} '{}' doesn't exist.", "[ERR]".red(), f);
                }
            }
            // Get job from argument provided
            let j = cfg.jobs.get_mut(&job).unwrap_or_else(|| {
                terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
            });
            // Check if files already exist on job
            // to avoid duplication.
            for jf in &j.files {
                for f in &file_path {
                    if jf.path.display().to_string()
                        == PathBuf::from(f)
                            .canonicalize()
                            .expect("[ERR] unable to canonicalize path.")
                            .display()
                            .to_string()
                    {
                        terminate!(
                            17,
                            "{} file(s) already exist on job '{}'.",
                            "[ERR]".red(),
                            &job
                        );
                    }
                }
            }
            // Confirm correct secret from user input
            confirm_secret(&j.name);
            // Push new files to job
            for f in file_path {
                j.files.push(KipFile::new(
                    PathBuf::from(f)
                        .canonicalize()
                        .expect("[ERR] unable to canonicalize path."),
                ));
            }
            // Get new files amount for job
            j.files_amt = j.get_files_amt().unwrap_or_else(|e| {
                terminate!(
                    6,
                    "{} failed to get file amounts for {}: {}.",
                    "[ERR]".red(),
                    &job,
                    e
                );
            });
            // Save changes to config file
            match cfg.save() {
                Ok(_) => println!(
                    "{} files successfully added to job '{}'.",
                    "[OK]".green(),
                    &job
                ),
                Err(e) => terminate!(
                    7,
                    "{} failed to save kip configuration: {}",
                    "[ERR]".red(),
                    e
                ),
            }
        }

        // Remove files from a job
        Subcommands::Remove {
            job,
            file_path,
            purge,
        } => {
            let mut cfg = cfg.write().await;
            // Get job from argument provided
            let j = cfg.jobs.get_mut(&job).unwrap_or_else(|| {
                terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
            });
            // Confirm correct secret from user input
            confirm_secret(&j.name);
            // Confirm removal
            if !Confirm::new()
                .with_prompt("Are you sure you want to remove this?")
                .interact()
                .unwrap_or(false)
            {
                std::process::exit(0);
            }
            // If -f flag was provided or not
            match file_path {
                // -f was provided, delete files from job
                Some(files) => {
                    for f in files {
                        let mut found = false;
                        for kf in j.files.iter() {
                            if kf.path
                                == Path::new(&f)
                                    .canonicalize()
                                    .expect("[ERR] unable to canonicalize path.")
                            {
                                found = true;
                            }
                        }
                        if found {
                            // Retain all elements != files_path argument provided
                            j.files.retain(|kf| {
                                kf.path
                                    != PathBuf::from(&f)
                                        .canonicalize()
                                        .expect("[ERR] unable to canonicalize path.")
                            });
                            // Get new files amount for job
                            j.files_amt = j.get_files_amt().unwrap_or_else(|e| {
                                terminate!(
                                    6,
                                    "{} failed to get file amounts for {}: {}.",
                                    "[ERR]".red(),
                                    &job,
                                    e
                                );
                            });
                            // Find all the runs that contain this file's chunks
                            // and remove them from S3.
                            if purge.unwrap() {
                                j.purge_file(&f).await.unwrap_or_else(|e| {
                                    terminate!(
                                        21,
                                        "{} failed to remove files from S3 for {}: {}.",
                                        "[ERR]".red(),
                                        &job,
                                        e
                                    );
                                })
                            }
                        } else {
                            terminate!(
                                2,
                                "{} job '{}' does not contain '{}'.",
                                "[ERR]".red(),
                                j.name,
                                f,
                            );
                        };
                    }
                    // Update files amt for job
                    j.files_amt = j.get_files_amt().unwrap_or_else(|e| {
                        terminate!(
                            6,
                            "{} failed to get file amounts for {}: {}.",
                            "[ERR]".red(),
                            &job,
                            e
                        );
                    });
                    // Save changes to config file
                    match cfg.save() {
                        Ok(_) => println!(
                            "{} files successfully removed from job '{}'.",
                            "[OK]".green(),
                            &job
                        ),
                        Err(e) => {
                            terminate!(
                                7,
                                "{} failed to save kip configuration: {}",
                                "[ERR]".red(),
                                e
                            );
                        }
                    }
                }
                None => {
                    // -f was not provided, delete the job
                    cfg.jobs.remove(&job);
                    // Save changes to config file
                    match cfg.save() {
                        Ok(_) => {
                            println!("{} job '{}' successfully removed.", "[OK]".green(), &job)
                        }
                        Err(e) => {
                            terminate!(
                                7,
                                "{} failed to save kip configuration: {}",
                                "[ERR]".red(),
                                e
                            );
                        }
                    }
                }
            }
        }

        // Start a job's upload
        Subcommands::Push { job } => {
            let mut cfg = cfg.write().await;
            // Get job from argument provided
            let j = cfg.jobs.get_mut(&job).unwrap_or_else(|| {
                terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
            });
            // Get new files amount for job
            j.files_amt = j.get_files_amt().unwrap_or_else(|e| {
                terminate!(
                    6,
                    "{} failed to get file amounts for {}: {}.",
                    "[ERR]".red(),
                    &job,
                    e
                );
            });
            // Confirm correct secret from user input
            let secret = confirm_secret(&j.name);
            // Upload all files in a seperate thread
            match j.run_upload(&secret).await {
                Ok(_) => {}
                Err(e) => terminate!(
                    8,
                    "{} job '{}' upload to '{}' failed: {}",
                    "[ERR]".red(),
                    &job,
                    &j.aws_bucket,
                    e
                ),
            }
            // Save changes to config file
            cfg.save().unwrap_or_else(|e| {
                terminate!(
                    7,
                    "{} failed to save kip configuration: {}",
                    "[ERR]".red(),
                    e
                );
            });
        }

        // Start a backup restore
        Subcommands::Pull {
            job,
            run,
            output_folder,
        } => {
            let mut cfg = cfg.write().await;
            // Get job from argument provided
            let j = cfg.jobs.get_mut(&job).unwrap_or_else(|| {
                terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
            });
            // Confirm correct secret from user input
            let secret = confirm_secret(&j.name);
            // Only one restore can be happening at a time
            match j.run_restore(run, &secret, output_folder).await {
                Ok(_) => {
                    println!("{} job '{}' restored successfully.", "[OK]".green(), &job,);
                }
                Err(e) => {
                    terminate!(2, "{} job '{}' restore failed: {}", "[ERR]".red(), &job, e);
                }
            };
            // Save changes to config file
            cfg.save().unwrap_or_else(|e| {
                terminate!(
                    7,
                    "{} failed to save kip configuration: {}",
                    "[ERR]".red(),
                    e
                );
            });
        }

        // Abort a running job
        Subcommands::Abort { job } => {
            let mut cfg = cfg.write().await;
            // Get job from argument provided
            let j = cfg.jobs.get_mut(&job).unwrap_or_else(|| {
                terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
            });
            // Confirm removal
            if !Confirm::new()
                .with_prompt(format!("Are you sure you want to abort '{}'?", &job))
                .interact()
                .unwrap_or(false)
            {
                std::process::exit(0);
            }
            // Abort job
            // Grab the job's thread id and thread.join() to kill
            // it. Since we aren't doing multipart, we can't abort
            // from S3's API :/ IDK how to do this lol
            j.abort();
        }

        // Get the status of a job
        Subcommands::Status { job, run } => {
            let cfg = cfg.read().await;
            // Get job from argument provided
            let j = cfg.jobs.get(&job).unwrap_or_else(|| {
                terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
            });
            if let Some(r) = run {
                // Get run from job
                let r = j.runs.get(&run.unwrap()).unwrap_or_else(|| {
                    terminate!(
                        2,
                        "{} run '{}' doesn't exist for job '{}.'",
                        "[ERR]".red(),
                        &r,
                        &job.clone(),
                    );
                });
                // print run status
                println!("{}", r.status)
            } else {
                // print job status
                println!("{}", j.last_status);
            };
        }

        // List all jobs
        // This function is messy. Should probably cleanup.
        Subcommands::List { job, run } => {
            let cfg = cfg.read().await;
            // Create the table
            let mut table = Table::new();
            if job == None && run == None {
                // Create the title row
                table.add_row(Row::new(vec![
                    Cell::new("Name"),
                    Cell::new("ID"),
                    Cell::new("Bucket"),
                    Cell::new("Region"),
                    Cell::new("Files"),
                    Cell::new("Total Runs"),
                    Cell::new("Last Run"),
                    Cell::new("Status"),
                ]));
                // For each job, add a row
                for (_, j) in cfg.jobs.iter() {
                    let correct_last_run = if j.last_run.format("%Y-%m-%d %H:%M:%S").to_string()
                        == "1970-01-01 00:00:00"
                    {
                        "NEVER_RUN".to_string()
                    } else {
                        let converted: DateTime<Local> = DateTime::from(j.last_run);
                        converted.format("%Y-%m-%d %H:%M:%S").to_string()
                    };
                    // Add row with job info
                    table.add_row(Row::new(vec![
                        Cell::new(&j.name.to_string()),
                        Cell::new(&format!("{}", j.id)),
                        Cell::new(&j.aws_bucket.to_string()),
                        Cell::new(j.aws_region.name()),
                        Cell::new(&format!("{}", j.files_amt)),
                        Cell::new(&format!("{}", j.total_runs)),
                        Cell::new(&correct_last_run),
                        Cell::new(&format!("{}", j.last_status)),
                    ]));
                }
                // Print the job table
                table.printstd();
            } else if job != None && run == None {
                table.add_row(Row::new(vec![
                    Cell::new("Name"),
                    Cell::new("ID"),
                    Cell::new("Bucket"),
                    Cell::new("Region"),
                    Cell::new("Selected Files"),
                    Cell::new("Total Runs"),
                    Cell::new("Last Run"),
                    Cell::new("Status"),
                ]));
                let j = cfg.jobs.get(&job.clone().unwrap()).unwrap_or_else(|| {
                    terminate!(
                        2,
                        "{} job '{}' doesn't exist.",
                        "[ERR]".red(),
                        &job.clone().unwrap()
                    );
                });
                // For each job, add a row
                let correct_last_run = if j.last_run.format("%Y-%m-%d %H:%M:%S").to_string()
                    == "1970-01-01 00:00:00"
                {
                    "NEVER_RUN".to_string()
                } else {
                    let converted: DateTime<Local> = DateTime::from(j.last_run);
                    converted.format("%Y-%m-%d %H:%M:%S").to_string()
                };
                // Pretty print files
                let files_vec: Vec<_> = j
                    .files
                    .iter()
                    .map(|e| e.path.display().to_string())
                    .collect();
                let mut correct_files = String::new();
                for f in files_vec {
                    correct_files.push_str(&f);
                    correct_files.push('\n');
                }
                // Add row with job info
                table.add_row(Row::new(vec![
                    Cell::new(&j.name.to_string()),
                    Cell::new(&format!("{}", j.id)),
                    Cell::new(&j.aws_bucket.to_string()),
                    Cell::new(j.aws_region.name()),
                    Cell::new(&correct_files),
                    Cell::new(&format!("{}", j.total_runs)),
                    Cell::new(&correct_last_run),
                    Cell::new(&format!("{}", j.last_status)),
                ]));
                // Print the job table
                table.printstd();
            } else if job != None && run != None {
                table.add_row(Row::new(vec![
                    Cell::new("Name"),
                    Cell::new("Bucket"),
                    Cell::new("Region"),
                    Cell::new("Files Uploaded"),
                    Cell::new("Bytes Uploaded"),
                    Cell::new("Runtime"),
                    Cell::new("Status"),
                ]));
                let j = cfg.jobs.get(&job.clone().unwrap()).unwrap_or_else(|| {
                    terminate!(
                        2,
                        "{} job '{}' doesn't exist.",
                        "[ERR]".red(),
                        &job.clone().unwrap()
                    );
                });
                let r = j.runs.get(&run.unwrap()).unwrap_or_else(|| {
                    terminate!(
                        2,
                        "{} run '{}' doesn't exist for job '{}.'",
                        "[ERR]".red(),
                        &run.unwrap(),
                        &job.clone().unwrap(),
                    );
                });
                // Add row with run info
                table.add_row(Row::new(vec![
                    Cell::new(&format!("{}-{}", j.name, r.id)),
                    Cell::new(&j.aws_bucket.to_string()),
                    Cell::new(j.aws_region.name()),
                    Cell::new(&format!("{}", r.files_changed.len())),
                    Cell::new(&convert(r.bytes_uploaded as f64)),
                    Cell::new(&r.time_elapsed.to_string()),
                    Cell::new(&format!("{}", r.status)),
                ]));
                // Create a table for logs
                let mut logs_table = Table::new();
                logs_table.add_row(Row::new(vec![Cell::new("Logs")]));
                // Pretty print logs
                let mut pretty_logs = String::new();
                for l in r.logs.iter() {
                    pretty_logs.push_str(l);
                    pretty_logs.push('\n');
                }
                if pretty_logs.is_empty() {
                    pretty_logs.push_str("None");
                }
                // Add row to logs table
                logs_table.add_row(Row::new(vec![Cell::new(&pretty_logs)]));
                // Print the job table
                table.printstd();
                logs_table.printstd();
            }
        }

        // Get the status of a job
        Subcommands::Daemon {} => {
            // Arc Clone reference to KipConf
            let daemon_cfg = Arc::clone(&cfg_file);
            // Create background thread to poll backup
            // interval for all jobs
            tokio::spawn(async move {
                // Duration of time to wait between each poll
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                loop {
                    // Get KipConf each loop iteration as to not cause
                    // contention on the RwLock. Lock is dropped at end
                    // of each loop
                    let mut daemon_cfg = daemon_cfg.write().await;
                    // Check if backup needs to be run for all jobs
                    let _ = daemon_cfg.poll_backup_jobs().await;
                    // Drop KipConf RwLock after check is done
                    drop(daemon_cfg);
                    // Wait 60 seconds, then loop again
                    interval.tick().await;
                }
            });
        }
    }
}

// Confirm correct secret from user input
fn confirm_secret(job_name: &str) -> String {
    let secret = Password::new()
        .with_prompt("Please provide your encryption secret")
        .interact()
        .expect("[ERR] failed to create encryption secret prompt.");
    let keyring_secret =
        match keyring_get_secret(format!("com.ciehanski.kip.{}", job_name).trim_end()) {
            Ok(ks) => ks,
            Err(e) => {
                terminate!(
                    5,
                    "{} failed to get secret from keyring: {}.",
                    "[ERR]".red(),
                    e
                );
            }
        };
    if secret != keyring_secret {
        terminate!(1, "{} incorrect secret.", "[ERR]".red());
    };
    secret
}

// A simple macro to remove some boilerplate
// on error or exiting kip.
#[macro_export]
macro_rules! terminate {
    ($xcode:expr, $($arg:tt)*) => {{
        let res = std::fmt::format(format_args!($($arg)*));
        eprintln!("{}", res);
        std::process::exit($xcode);
    }}
}
