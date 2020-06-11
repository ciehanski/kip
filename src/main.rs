#[macro_use]
extern crate prettytable;

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
use prettytable::Table;
use std::io::prelude::*;
use std::path::Path;
use std::process;
use structopt::StructOpt;
use walkdir::WalkDir;

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

        // Add more files or directories to job
        Subcommands::Add { job, file_path } => {
            // Check if file or directory exists
            if !Path::new(&file_path).exists() {
                eprintln!("{} '{}' doesn't exist.", "[ERR]".red(), file_path);
                process::exit(1);
            }
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
            // Check if files already exist on job
            // to avoid duplication.
            for f in &j.files {
                if f == &file_path {
                    eprintln!("{} file(s) already exist on job '{}'.", "[ERR]".red(), &job);
                    process::exit(1);
                }
            }
            // Push new files to job
            j.files.push(file_path);
            // Save changes to config file
            match cfg.save() {
                Ok(_) => println!("{} files successfully added to job.", "[OK]".green()),
                Err(e) => eprintln!("{} failed to save kip configuration: {}", "[ERR]".red(), e),
            };
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
            if j.files.contains(&file_path) {
                // Retain all elements != files_path argument provided
                j.files.retain(|e| e != &file_path);
            }
            // Save changes to config file
            match cfg.save() {
                Ok(_) => println!("{} files successfully removed from job.", "[OK]".green()),
                Err(e) => eprintln!("{} failed to save kip configuration: {}", "[ERR]".red(), e),
            };
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
            let j = match cfg.jobs.get_mut(&job) {
                Some(j) => j.clone(),
                None => {
                    eprintln!("{} job '{}' doesn't exist.", "[ERR]".red(), &job);
                    process::exit(1);
                }
            };
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
                .download(
                    &secret,
                    &cfg.s3_access_key,
                    &cfg.s3_secret_key,
                    &output_folder,
                )
                .await
            {
                Ok(_) => (),
                Err(e) => panic!("{}", e),
            };
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
            // Abort job
            job_pool.execute(move || {
                // Grab the job's thread id and
                // thread.join() to kill it. Since we
                // aren't doing multipart, we can't
                // abort from S3's API :/
                // IDK how to do this lol
                j.abort();
            })
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
            table.add_row(row![
                "Name",
                "ID",
                "Bucket",
                "Region",
                "Files",
                "Total Runs",
                "Last Run",
                "Status"
            ]);
            // For each job, add a row
            for (_, j) in cfg.jobs {
                #[allow(unused_assignments)]
                // let mut correct_last_run = String::from("");
                // Get correct time
                // if j.last_run.format("%Y-%m-%d %H:%M:%S").to_string() == "1970-01-01 00:00:00" {
                //     correct_last_run = "NEVER_RUN".to_string();
                // } else {
                //     correct_last_run = j.last_run.clone().to_string();
                // }
                let correct_last_run = if j.last_run.format("%Y-%m-%d %H:%M:%S").to_string()
                    == "1970-01-01 00:00:00"
                {
                    "NEVER_RUN".to_string()
                } else {
                    j.last_run.clone().to_string()
                };
                // Get correct number of files in job (not just...
                // entries within 'files')
                let mut correct_files_num = 0;
                for f in j.files {
                    if Path::new(&f).exists() && Path::new(&f).is_dir() {
                        for entry in WalkDir::new(f) {
                            let entry = entry.unwrap();
                            if Path::is_dir(entry.path()) {
                                continue;
                            }
                            correct_files_num += 1;
                        }
                    } else if Path::new(&f).exists() {
                        correct_files_num += 1;
                    }
                }
                // Add row with job info
                table.add_row(row![
                    format!("{}", j.name.to_string()),
                    format!("{}", j.id),
                    format!("{}", j.aws_bucket.to_string()),
                    format!("{}", j.aws_region.to_string()),
                    format!("{}", correct_files_num),
                    format!("{}", j.total_runs),
                    format!("{}", correct_last_run.to_string()),
                    format!("{:?}", j.last_status),
                ]);
            }
            // Print the job table
            table.printstd();
        }
    }
}
