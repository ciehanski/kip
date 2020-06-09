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

fn main() {
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
    let job_pool = JobPool::new(4);

    // Get subcommands and args
    let args: Opt = Opt::from_args();
    let _debug = args.debug;

    // Match user input command
    match args.subcommands {
        // Create a new job
        Subcommands::Init { job } => {
            // Get config
            let mut cfg = KipConf::get().expect("[ERR] failed to get kip configuration.");
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
            let new_job = Job::new(
                &job.clone(),
                &s3_bucket_name.clone().trim_end(),
                &s3_region.clone().trim_end(),
            );
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
            let mut cfg = KipConf::get().expect("[ERR] failed to get kip configuration.");
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
            let mut cfg = KipConf::get().expect("[ERR] failed to get kip configuration.");
            // Get job from argument provided
            let j = cfg.jobs.get_mut(&job).unwrap_or_else(|| {
                eprintln!("{} job '{}' doesn't exist.", "[ERR]".red(), &job);
                process::exit(1);
            });
            // Retain all elements != files_path argument provided
            j.files.retain(|e| e != &file_path);
            // Save changes to config file
            match cfg.save() {
                Ok(_) => println!("{} files successfully removed from job.", "[OK]".green()),
                Err(e) => eprintln!("{} failed to save kip configuration: {}", "[ERR]".red(), e),
            };
        }

        // Start a job's upload
        Subcommands::Push { job, secret } => {
            // Get config
            let mut cfg = KipConf::get().expect("[ERR] failed to get kip configuration.");
            // Prompt user for s3 info if nil or wrong format
            // Probably used regex later but I don't want to
            // right now.
            if cfg.s3_access_key.len() == 0 || cfg.s3_secret_key.len() == 0 {
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
            match j.upload(&secret, &cfg.s3_access_key, &cfg.s3_secret_key) {
                Ok(_) => println!(
                    "{} job '{}' uploaded successfully to '{}'.",
                    "[OK]".green(),
                    &job,
                    &bkt_name
                ),
                Err(e) => eprintln!(
                    "{} job '{}' upload to '{}' failed: {}",
                    "[ERR]".red(),
                    &job,
                    &bkt_name,
                    e
                ),
            };
            // Save updates to config
            cfg.save().unwrap_or_else(|e| {
                eprintln!("{} failed to save kip configuration: {}", "[ERR]".red(), e);
            });
            //});
        }

        // Start a backup restore
        Subcommands::Pull { job, secret } => {
            println!("{}{}", job, secret);
            // Only one restore can be happening at a time
            job_pool.execute(|| {
                unimplemented!();
            })
        }

        // Abort a running job
        Subcommands::Abort { job } => {
            // Get config
            let mut cfg = KipConf::get().expect("[ERR] failed to get kip configuration.");
            // Prompt user for s3 info if nil or wrong format
            // Probably used regex later but I don't want to
            // right now.
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
                j.abort();
            })
        }

        // Get the status of a job
        Subcommands::Status { job } => {
            // Get config
            let cfg = KipConf::get().expect("[ERR] failed to get kip configuration.");
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
            let cfg = KipConf::get().expect("[ERR] failed to get kip configuration.");
            // Create the table
            let mut table = Table::new();
            // Create the title row
            table.add_row(row![
                "Name", "ID", "Bucket", "Region", "Files", "Last Run", "Running"
            ]);
            // For each job, add a row
            for (_, j) in cfg.jobs {
                table.add_row(row![
                    format!("{}", j.name),
                    format!("{}", j.id),
                    format!("{}", j.aws_bucket),
                    format!("{}", j.aws_region),
                    format!("{}", j.files.len()),
                    format!("{}", j.last_run),
                    format!("{}", j.running),
                ]);
            }
            // Print the job table
            table.printstd();
        }
    }
}
