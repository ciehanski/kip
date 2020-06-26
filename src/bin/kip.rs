// Kip imports
use kip::args::{Opt, Subcommands};
use kip::conf::KipConf;
use kip::crypto::{argon_hash_secret, compare_argon_secret};
use kip::job::{Job, KipFile};
// External imports
use chrono::prelude::*;
use colored::*;
use dialoguer::Confirm;
use dialoguer::Password;
use pretty_bytes::converter::convert;
use prettytable::{Cell, Row, Table};
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use structopt::StructOpt;

#[tokio::main]
async fn main() {
    // Writes default config if missing
    match KipConf::new() {
        Ok(_) => (),
        Err(e) => terminate!(
            5,
            "{} failed to create new kip configuration: {}",
            "[ERR]".red(),
            e
        ),
    };

    // Get subcommands and args
    let args: Opt = Opt::from_args();
    let _debug = args.debug;

    // Create background thread to poll backup
    // interval for all jobs
    // std::thread::spawn(|| async {
    //     let mut cfg = KipConf::get().unwrap_or_else(|e| {
    //         terminate!(
    //             5,
    //             "{} failed to get kip configuration: {}.",
    //             "[ERR]".red(),
    //             e
    //         );
    //     });
    //     // TODO: get the user's correct secret
    //     cfg.poll_backup_jobs("").await;
    // });

    // Match user input command
    match args.subcommands {
        // Create a new job
        Subcommands::Init { job } => {
            // Get config
            let mut cfg = KipConf::get().unwrap_or_else(|e| {
                terminate!(
                    5,
                    "{} failed to get kip configuration: {}.",
                    "[ERR]".red(),
                    e
                );
            });
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
            // Encrypt provided secret
            let encrypted_secret = argon_hash_secret(&secret).unwrap_or_else(|e| {
                terminate!(5, "{} failed to encrypt secret: {}.", "[ERR]".red(), e);
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
            let new_job = Job::new(
                &job,
                &encrypted_secret,
                &s3_bucket_name.trim_end(),
                &s3_region.trim_end(),
            );
            // Push new job in config
            cfg.jobs.insert(job.clone(), new_job);
            // // Store new job in config
            match cfg.save() {
                Ok(_) => println!("{} job '{}' successfully created.", "[OK]".green(), job),
                Err(e) => eprintln!("{} failed to save kip configuration: {}", "[ERR]".red(), e),
            }
        }

        // Add more files or directories to job
        Subcommands::Add { job, file_path } => {
            // Check if file or directory exists
            for f in &file_path {
                if !Path::new(&f).exists() {
                    terminate!(2, "{} '{}' doesn't exist.", "[ERR]".red(), f);
                }
            }
            // Get config
            let mut cfg = KipConf::get().unwrap_or_else(|e| {
                terminate!(
                    5,
                    "{} failed to get kip configuration: {}.",
                    "[ERR]".red(),
                    e
                );
            });
            // Get job from argument provided
            let mut j = cfg.jobs.get_mut(&job).unwrap_or_else(|| {
                terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
            });
            // Confirm correct secret from user input
            confirm_secret(&j.secret);
            // Check if files already exist on job
            // to avoid duplication.
            for jf in &j.files {
                for f in &file_path {
                    if &jf.path.display().to_string() == f {
                        terminate!(
                            17,
                            "{} file(s) already exist on job '{}'.",
                            "[ERR]".red(),
                            &job
                        );
                    }
                }
            }
            // Push new files to job
            for f in file_path {
                j.files.push(KipFile::new(
                    PathBuf::from(f)
                        .canonicalize()
                        .expect("[ERR] unable to canonicalize path."),
                ));
            }
            // Get new files amount for job
            j.files_amt = j
                .get_files_amt()
                .expect("[ERR] failed to get files amount for job.");
            // Save changes to config file
            match cfg.save() {
                Ok(_) => println!(
                    "{} files successfully added to job '{}'.",
                    "[OK]".green(),
                    &job
                ),
                Err(e) => eprintln!("{} failed to save kip configuration: {}", "[ERR]".red(), e),
            }
        }

        // Remove files from a job
        Subcommands::Remove { job, file_path } => {
            // Get config
            let mut cfg = KipConf::get().unwrap_or_else(|e| {
                terminate!(
                    5,
                    "{} failed to get kip configuration: {}.",
                    "[ERR]".red(),
                    e
                );
            });
            // Get job from argument provided
            let j = cfg.jobs.get_mut(&job).unwrap_or_else(|| {
                terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
            });
            // Confirm correct secret from user input
            confirm_secret(&j.secret);
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
                            if kf.path == Path::new(&f).canonicalize().unwrap() {
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
                            j.files_amt = j
                                .get_files_amt()
                                .expect("[ERR] failed to get files amount for job.");
                            // Find all the runs that contain this file's chunks
                            // and remove them from S3.
                            for run in j.runs.iter() {
                                for fc in run.1.files_changed.iter() {
                                    for chunk in fc.values() {
                                        if chunk.local_path
                                            == PathBuf::from(&f)
                                                .canonicalize()
                                                .expect("[ERR] failed to get files amount for job.")
                                        {
                                            let mut path = String::new();
                                            path.push_str(&j.id.to_string());
                                            let mut cpath = chunk
                                                .local_path
                                                .parent()
                                                .expect("[ERR] unable to canonicalize path.")
                                                .display()
                                                .to_string();
                                            cpath.push_str(&format!("/{}.chunk", chunk.hash));
                                            path.push_str(&cpath);
                                            // Set AWS env vars for backup
                                            std::env::set_var(
                                                "AWS_ACCESS_KEY_ID",
                                                &cfg.s3_access_key,
                                            );
                                            std::env::set_var(
                                                "AWS_SECRET_ACCESS_KEY",
                                                &cfg.s3_secret_key,
                                            );
                                            std::env::set_var("AWS_REGION", &j.aws_region.name());
                                            println!("{}", &path);
                                            match kip::s3::delete_s3_object(
                                                &j.aws_bucket,
                                                j.aws_region.clone(),
                                                &path,
                                            )
                                            .await
                                            {
                                                Ok(_) => (),
                                                Err(e) => {
                                                    terminate!(
                                                        13,
                                                        "{} unable to delete '{}' from S3: '{}'.",
                                                        "[ERR]".red(),
                                                        f,
                                                        e,
                                                    );
                                                }
                                            };
                                            // Reset AWS env env to nil
                                            std::env::set_var("AWS_ACCESS_KEY_ID", "");
                                            std::env::set_var("AWS_SECRET_ACCESS_KEY", "");
                                            std::env::set_var("AWS_REGION", "");
                                        }
                                    }
                                }
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
                    j.files_amt = j
                        .get_files_amt()
                        .expect("[ERR] failed to get files amount for job.");
                    // Save changes to config file
                    match cfg.save() {
                        Ok(_) => println!(
                            "{} files successfully removed from job '{}'.",
                            "[OK]".green(),
                            &job
                        ),
                        Err(e) => {
                            eprintln!("{} failed to save kip configuration: {}", "[ERR]".red(), e)
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
                            eprintln!("{} failed to save kip configuration: {}", "[ERR]".red(), e)
                        }
                    }
                }
            }
        }

        // Start a job's upload
        Subcommands::Push { job } => {
            // Get config
            let mut cfg = KipConf::get().unwrap_or_else(|e| {
                terminate!(
                    5,
                    "{} failed to get kip configuration: {}.",
                    "[ERR]".red(),
                    e
                );
            });
            // Prompt user for s3 info if nil. Probably used
            // regex later but I don't want to right now.
            if cfg.s3_access_key.is_empty() || cfg.s3_secret_key.is_empty() {
                cfg.prompt_s3_keys();
            };
            // Get job from argument provided
            let j = cfg.jobs.get_mut(&job).unwrap_or_else(|| {
                terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
            });
            // Get new files amount for job
            j.files_amt = j
                .get_files_amt()
                .expect("[ERR] failed to get files amount for job.");
            // Confirm correct secret from user input
            let secret = confirm_secret(&j.secret).unwrap_or_default();
            // Upload all files in a seperate thread
            let bkt_name = j.aws_bucket.clone();
            match j
                .run_upload(&secret, &cfg.s3_access_key, &cfg.s3_secret_key)
                .await
            {
                Ok(_) => {
                    println!(
                        "{} job '{}' uploaded successfully to '{}'.",
                        "[OK]".green(),
                        &job,
                        &bkt_name
                    );
                }
                Err(e) => eprintln!(
                    "{} job '{}' upload to '{}' failed: {}",
                    "[ERR]".red(),
                    &job,
                    &bkt_name,
                    e
                ),
            }
            // Save changes to config file
            match cfg.save() {
                Ok(_) => (),
                Err(e) => eprintln!("{} failed to save kip configuration: {}", "[ERR]".red(), e),
            }
        }

        // Start a backup restore
        Subcommands::Pull {
            job,
            run,
            output_folder,
        } => {
            // Get config
            let mut cfg = KipConf::get().unwrap_or_else(|e| {
                terminate!(
                    5,
                    "{} failed to get kip configuration: {}.",
                    "[ERR]".red(),
                    e
                );
            });
            // Prompt user for s3 info if nil. Probably used
            // regex later but I don't want to right now.
            if cfg.s3_access_key.is_empty() || cfg.s3_secret_key.is_empty() {
                cfg.prompt_s3_keys();
            };
            // Get job from argument provided
            let j = cfg.jobs.get_mut(&job).unwrap_or_else(|| {
                terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
            });
            // Confirm correct secret from user input
            let secret = confirm_secret(&j.secret).unwrap_or_default();
            // Only one restore can be happening at a time
            match j
                .run_restore(
                    run,
                    &secret,
                    &cfg.s3_access_key,
                    &cfg.s3_secret_key,
                    output_folder,
                )
                .await
            {
                Ok(_) => {
                    println!("{} job '{}' restored successfully.", "[OK]".green(), &job,);
                }
                Err(e) => {
                    eprintln!("{} job '{}' restore failed: {}", "[ERR]".red(), &job, e);
                }
            };
            // Save changes to config file
            match cfg.save() {
                Ok(_) => (),
                Err(e) => eprintln!("{} failed to save kip configuration: {}", "[ERR]".red(), e),
            }
        }

        // Abort a running job
        Subcommands::Abort { job } => {
            // Get config
            let mut cfg = KipConf::get().unwrap_or_else(|e| {
                terminate!(
                    5,
                    "{} failed to get kip configuration: {}.",
                    "[ERR]".red(),
                    e
                );
            });
            // Prompt user for s3 info if nil. Probably used
            // regex later but I don't want to right now.
            if cfg.s3_access_key.len() == 0 || cfg.s3_secret_key.len() == 0 {
                cfg.prompt_s3_keys();
            };
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
        Subcommands::Status { job } => {
            // Get config
            let cfg = KipConf::get().unwrap_or_else(|e| {
                terminate!(
                    5,
                    "{} failed to get kip configuration: {}.",
                    "[ERR]".red(),
                    e
                );
            });
            // Get job from argument provided
            let j = cfg.jobs.get(&job).unwrap_or_else(|| {
                terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
            });
            println!("{:#?}", j);
        }

        // List all jobs
        // This function is messing. Should probably cleanup.
        Subcommands::List { job, run } => {
            // Get config
            let cfg = KipConf::get().unwrap_or_else(|e| {
                terminate!(
                    5,
                    "{} failed to get kip configuration: {}.",
                    "[ERR]".red(),
                    e
                )
            });
            // Create the table
            let mut table = Table::new();
            //
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
                for (_, j) in cfg.jobs {
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
                        Cell::new(&format!("{}", j.name.to_string())),
                        Cell::new(&format!("{}", j.id)),
                        Cell::new(&format!("{}", j.aws_bucket.to_string())),
                        Cell::new(&format!("{}", j.aws_region.name())),
                        Cell::new(&format!("{}", j.files_amt)),
                        Cell::new(&format!("{}", j.total_runs)),
                        Cell::new(&format!("{}", correct_last_run)),
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
                let j = cfg.jobs.get(&job.unwrap()).unwrap();
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
                    correct_files.push_str("\n");
                }
                // Add row with job info
                table.add_row(Row::new(vec![
                    Cell::new(&format!("{}", j.name.to_string())),
                    Cell::new(&format!("{}", j.id)),
                    Cell::new(&format!("{}", j.aws_bucket.to_string())),
                    Cell::new(&format!("{}", j.aws_region.name())),
                    Cell::new(&format!("{}", correct_files)),
                    Cell::new(&format!("{}", j.total_runs)),
                    Cell::new(&format!("{}", correct_last_run)),
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
                let j = cfg.jobs.get(&job.unwrap()).unwrap();
                let r = j.runs.get(&run.unwrap()).unwrap();
                // Add row with run info
                table.add_row(Row::new(vec![
                    Cell::new(&format!("{}-{}", j.name.to_string(), r.id)),
                    Cell::new(&format!("{}", j.aws_bucket.to_string())),
                    Cell::new(&format!("{}", j.aws_region.name())),
                    Cell::new(&format!("{}", r.files_changed.len())),
                    Cell::new(&format!("{}", convert(r.bytes_uploaded as f64))),
                    Cell::new(&format!("{}", r.time_elapsed)),
                    Cell::new(&format!("{}", r.status)),
                ]));
                // Create a table for logs
                let mut logs_table = Table::new();
                logs_table.add_row(Row::new(vec![Cell::new("Logs")]));
                // Pretty print logs
                let mut pretty_logs = String::new();
                for l in r.logs.iter() {
                    pretty_logs.push_str(&l);
                    pretty_logs.push_str("\n");
                }
                if pretty_logs.is_empty() {
                    pretty_logs.push_str("None");
                }
                // Add row to logs table
                logs_table.add_row(Row::new(vec![Cell::new(&format!("{}", pretty_logs))]));
                // Print the job table
                table.printstd();
                logs_table.printstd();
            }
        }
    }
}

// Confirm correct secret from user input
fn confirm_secret(job_secret: &str) -> Option<String> {
    let secret = Password::new()
        .with_prompt("Please provide your encryption secret")
        .interact()
        .expect("[ERR] failed to create encryption secret prompt.");
    if !compare_argon_secret(&secret, job_secret).unwrap_or_else(|e| {
        terminate!(
            5,
            "{} failed to compare job secret with provided secret: {}.",
            "[ERR]".red(),
            e
        );
    }) {
        terminate!(1, "{} incorrect secret.", "[ERR]".red());
    };
    Some(secret)
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
