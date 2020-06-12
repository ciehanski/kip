mod args;
mod conf;
mod crypto;
mod job;
mod job_pool;

// Kip imports
use crate::args::{Opt, Subcommands};
use crate::conf::KipConf;
use crate::job::Job;
use crate::job_pool::JobPool;

// External imports
use colored::*;
use dialoguer::Confirm;
use prettytable::{Cell, Row, Table};
use std::io::prelude::*;
use std::path::Path;
use std::process;
use structopt::StructOpt;

#[tokio::main]
async fn main() {
    // Writes default config if missing
    match KipConf::new() {
        Ok(_) => (),
        Err(e) => eprintln!(
            "{} failed to create new kip configuration: {}",
            "[ERR]".red(),
            e
        ),
    };

    // Create job pool for backup jobs
    let job_pool = JobPool::new(5);

    // Get subcommands and args
    let args: Opt = Opt::from_args();
    let _debug = args.debug;

    // Match user input command
    match args.subcommands {
        // Create a new job
        Subcommands::Init { job } => {
            // Get config
            let mut cfg = KipConf::get().unwrap_or_else(|e| {
                eprintln!("{} failed to get kip configuration: {}.", "[ERR]".red(), e);
                process::exit(1);
            });
            // Ensure that job does not already exist with
            // the provided name.
            for (j, _) in cfg.jobs.iter() {
                if j == &job {
                    eprintln!("{} job '{}' already exists.", "[ERR]".red(), job);
                    process::exit(1);
                }
            }
            // Get region and bucket name from user input
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
            let new_job = Job::new(&job, &s3_bucket_name.trim_end(), &s3_region.trim_end());
            // Push new job in config
            cfg.jobs.insert(job.clone(), new_job);
            // // Store new job in config
            match cfg.save() {
                Ok(_) => println!("{} job '{}' successfully created.", "[OK]".green(), job),
                Err(e) => eprintln!("{} failed to save kip configuration: {}", "[ERR]".red(), e),
            };
        }

        // TODO: fix all the for loops in add. I don't like them
        // Add more files or directories to job
        Subcommands::Add { job, file_path } => {
            // Check if file or directory exists
            for f in &file_path {
                if !Path::new(&f).exists() {
                    eprintln!("{} '{}' doesn't exist.", "[ERR]".red(), f);
                    process::exit(1);
                }
            }
            // Get config
            let mut cfg = KipConf::get().unwrap_or_else(|e| {
                eprintln!("{} failed to get kip configuration: {}.", "[ERR]".red(), e);
                process::exit(1);
            });
            // Get job from argument provided
            let mut j = cfg.jobs.get_mut(&job).unwrap_or_else(|| {
                eprintln!("{} job '{}' doesn't exist.", "[ERR]".red(), &job);
                process::exit(1);
            });
            // Check if files already exist on job
            // to avoid duplication.
            for jf in &j.files {
                for f in &file_path {
                    if jf == f {
                        eprintln!("{} file(s) already exist on job '{}'.", "[ERR]".red(), &job);
                        process::exit(1);
                    }
                }
            }
            // Push new files to job
            for f in file_path {
                j.files.push(f);
            }
            // Get new files amount for job
            j.files_amt = j.get_files_amt();
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
                eprintln!("{} failed to get kip configuration: {}.", "[ERR]".red(), e);
                process::exit(1);
            });
            // Get job from argument provided
            let j = cfg.jobs.get_mut(&job).unwrap_or_else(|| {
                eprintln!("{} job '{}' doesn't exist.", "[ERR]".red(), &job);
                process::exit(1);
            });
            // Confirm removal
            if !Confirm::new()
                .with_prompt("Are you sure you want to remove this?")
                .interact()
                .unwrap_or(false)
            {
                process::exit(0);
            }
            // If -f flag was provided or not
            match file_path {
                // -f was provided, delete files from job
                Some(file) => {
                    for f in file {
                        if j.files.contains(&f) {
                            // Retain all elements != files_path argument provided
                            j.files.retain(|i| i != &f);
                            // Get new files amount for job
                            j.files_amt = j.get_files_amt();
                        } else {
                            eprintln!("{} job {} does not contain '{}'.", "[err]".red(), j.name, f,);
                            process::exit(1);
                        };
                    }
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
        Subcommands::Push { job, secret } => {
            // Get config
            let mut cfg = KipConf::get().unwrap_or_else(|e| {
                eprintln!("{} failed to get kip configuration: {}.", "[ERR]".red(), e);
                process::exit(1);
            });
            // Prompt user for s3 info if nil. Probably used
            // regex later but I don't want to right now.
            if cfg.s3_access_key.is_empty() || cfg.s3_secret_key.is_empty() {
                cfg.prompt_s3_keys();
            };
            // Get job from argument provided
            let mut j = match cfg.jobs.get_mut(&job) {
                Some(j) => j.clone(),
                None => {
                    eprintln!("{} job '{}' doesn't exist.", "[ERR]".red(), &job);
                    process::exit(1);
                }
            };
            // Get new files amount for job
            j.files_amt = j.get_files_amt();
            // Upload all files in a seperate thread
            // TODO: Propagate and bubble errors through upload
            let bkt_name = j.aws_bucket.clone();
            //job_pool.execute(move || {
            match j.upload(cfg.clone(), &secret).await {
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
            };
            //});
        }

        // Start a backup restore
        Subcommands::Pull {
            job,
            secret,
            output_folder,
        } => {
            // Get config
            let mut cfg = KipConf::get().unwrap_or_else(|e| {
                eprintln!("{} failed to get kip configuration: {}.", "[ERR]".red(), e);
                process::exit(1);
            });
            // Prompt user for s3 info if nil. Probably used
            // regex later but I don't want to right now.
            if cfg.s3_access_key.is_empty() || cfg.s3_secret_key.is_empty() {
                cfg.prompt_s3_keys();
            };
            // Get job from argument provided
            let j = match cfg.jobs.get_mut(&job) {
                Some(j) => j.clone(),
                None => {
                    eprintln!("{} job '{}' doesn't exist.", "[ERR]".red(), &job);
                    process::exit(1);
                }
            };
            // Only one restore can be happening at a time
            //job_pool.execute(move || {
            match j
                .restore(
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
            //})
        }

        // Abort a running job
        Subcommands::Abort { job } => {
            // Get config
            let mut cfg = KipConf::get().unwrap_or_else(|e| {
                eprintln!("{} failed to get kip configuration: {}.", "[ERR]".red(), e);
                process::exit(1);
            });
            // Prompt user for s3 info if nil. Probably used
            // regex later but I don't want to right now.
            if cfg.s3_access_key.len() == 0 || cfg.s3_secret_key.len() == 0 {
                cfg.prompt_s3_keys();
            };
            // Get job from argument provided
            let j = match cfg.jobs.get_mut(&job) {
                Some(j) => j.clone(),
                None => {
                    eprintln!("{} job '{}' does not exist.", "[ERR]".red(), &job);
                    process::exit(1);
                }
            };
            // Confirm removal
            if !Confirm::new()
                .with_prompt(format!("Are you sure you want to abort '{}'?", &job))
                .interact()
                .unwrap_or(false)
            {
                process::exit(0);
            }
            // Abort job
            //job_pool.execute(move || {
            // Grab the job's thread id and
            // thread.join() to kill it. Since we
            // aren't doing multipart, we can't
            // abort from S3's API :/
            // IDK how to do this lol
            j.abort();
            //})
        }

        // Get the status of a job
        Subcommands::Status { job } => {
            // Get config
            let cfg = KipConf::get().unwrap_or_else(|e| {
                eprintln!("{} failed to get kip configuration: {}.", "[ERR]".red(), e);
                process::exit(1);
            });
            // Get job from argument provided
            let j = cfg.jobs.get(&job).unwrap_or_else(|| {
                eprintln!("{} job '{}' doesn't exist.", "[ERR]".red(), &job);
                process::exit(1);
            });
            println!("{:#?}", j);
        }

        // List all jobs
        Subcommands::List {} => {
            // Get config
            let cfg = KipConf::get().unwrap_or_else(|e| {
                eprintln!("{} failed to get kip configuration: {}.", "[ERR]".red(), e);
                process::exit(1);
            });
            // Create the table
            let mut table = Table::new();
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
                    j.last_run.clone().to_string()
                };
                // Add row with job info
                table.add_row(Row::new(vec![
                    Cell::new(&format!("{}", j.name.to_string())),
                    Cell::new(&format!("{}", j.id)),
                    Cell::new(&format!("{}", j.aws_bucket.to_string())),
                    Cell::new(&format!("{}", j.aws_region.to_string())),
                    Cell::new(&format!("{}", j.files_amt)),
                    Cell::new(&format!("{}", j.total_runs)),
                    Cell::new(&format!("{}", correct_last_run.to_string())),
                    Cell::new(&format!("{}", j.last_status)),
                ]));
            }
            // Print the job table
            table.printstd();
        }
    }
}
