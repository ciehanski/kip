extern crate chrono;
extern crate dialoguer;
extern crate rand;
extern crate serde;
extern crate serde_json;
extern crate sha3;
extern crate structopt;
extern crate walkdir;

// Local stuff
mod conf;
mod crypto;
mod job;
mod job_pool;

// Local stuff
use crate::conf::KipConf;
use crate::job::Job;
// External crate imports
use std::io::prelude::*;
use std::path::Path;
use std::process;
use structopt::StructOpt;

// Argument options
#[derive(StructOpt)]
#[structopt(name = "kip", about = "kip, the simple encrypted backups tool.")]
struct Opt {
    /// Subcommands
    #[structopt(subcommand)]
    pub cmd: Subcommands,
    /// Display debug messages
    #[structopt(global = true, short = "d", long = "debug")]
    pub debug: bool,
}

// Subcommands
#[derive(StructOpt)]
enum Subcommands {
    /// Create a new job
    #[structopt(name = "new")]
    New { job: String },

    /// Add files to a job
    #[structopt(name = "add")]
    Add {
        job: String,
        /// OS to build client for
        #[structopt(short = "f", long = "files")]
        file_path: String,
    },

    /// Remove files from a job
    #[structopt(name = "remove")]
    Remove {
        ///
        #[structopt(short = "j", long = "job")]
        job: String,
        /// The IP address the C2 server will bind to
        #[structopt(short = "f", long = "files")]
        file_path: String,
    },

    /// Start the first/manual backup job
    #[structopt(name = "push")]
    Push {
        /// ID of the peer to connect to
        job: String,
        ///
        #[structopt(short = "s", long = "secret")]
        secret: String,
    },

    /// Start a backup restore
    #[structopt(name = "pull")]
    Pull {
        /// ID of the peer to connect to
        job: String,
        ///
        #[structopt(short = "s", long = "secret")]
        secret: String,
    },

    /// Abort any running or future upload on job
    #[structopt(name = "abort")]
    Abort {
        /// ID of the peer to connect to
        job: String,
    },

    /// Get status of a specific job
    #[structopt(name = "status")]
    Status {
        /// Name of the peer to connect to
        job: String,
    },

    /// List all jobs
    #[structopt(name = "list")]
    List {},
}

fn main() {
    // Writes default config if missing
    match KipConf::new() {
        Ok(_) => (),
        Err(e) => eprintln!("failed to create new kip configuration: {}", e),
    };

    // Get subcommands and args
    let args: Opt = Opt::from_args();
    let _debug = args.debug;

    // Match user input command
    match args.cmd {
        // Create a new job
        Subcommands::New { job } => {
            // Get config
            let mut cfg: KipConf = KipConf::get().expect("failed to get kip configuration.");
            // Get region and bucket name from user input
            // Get S3 bucket name from user input
            print!("Please provide the S3 bucket name: ");
            std::io::stdout().flush().expect("failed to flush stdout.");
            let mut s3_bucket_name = String::new();
            std::io::stdin()
                .read_line(&mut s3_bucket_name)
                .expect("failed to read from stdin");
            // Get S3 bucket region from user input
            print!("Please provide the S3 region: ");
            std::io::stdout().flush().expect("failed to flush stdout.");
            let mut s3_region = String::new();
            std::io::stdin()
                .read_line(&mut s3_region)
                .expect("failed to read from stdin");
            // Create the new job
            let new_job = Job::new(
                &job.clone(),
                &s3_bucket_name.clone().trim_end(),
                &s3_region.clone().trim_end(),
            );
            // Push new job in config
            cfg.jobs.insert(job, new_job);
            // // Store new job in config
            match cfg.save() {
                Ok(_) => println!("Job successfully created."),
                Err(e) => eprintln!("failed to save kip configuration: {}", e),
            };
        }

        // Add more files or directories to job
        Subcommands::Add { job, file_path } => {
            // Check if file or directory exists
            if !Path::new(&file_path).exists() {
                eprintln!("file(s) don't exist.");
                process::exit(1);
            }
            // Get config
            let mut cfg: KipConf = KipConf::get().expect("failed to get kip configuration.");
            // Get job from argument provided
            let j = match cfg.jobs.get_mut(&job) {
                Some(j) => j,
                None => {
                    eprintln!("job {} does not exist.", &job);
                    process::exit(1);
                }
            };
            // Push new files to job
            j.files.push(file_path);
            // Save changes to config file
            match cfg.save() {
                Ok(_) => println!("files successfully added to job."),
                Err(e) => eprintln!("failed to save kip configuration: {}", e),
            };
        }

        // Remove files from a job
        Subcommands::Remove { job, file_path } => {
            // Get config
            let mut cfg: KipConf = KipConf::get().expect("failed to get kip configuration.");
            // Get job from argument provided
            let j = match cfg.jobs.get_mut(&job) {
                Some(j) => j,
                None => {
                    eprintln!("job {} does not exist.", &job);
                    process::exit(1);
                }
            };
            // Retain all elements != files_path argument provided
            j.files.retain(|e| e != &file_path);
            // Save changes to config file
            match cfg.save() {
                Ok(_) => println!("files successfully removed from job."),
                Err(e) => eprintln!("failed to save kip configuration: {}", e),
            };
        }

        // Start a job's upload
        Subcommands::Push { job, secret } => {
            // Get config
            let mut cfg: KipConf = KipConf::get().expect("failed to get kip configuration.");
            // Prompt user for s3 info if nil or wrong format
            // Probably used regex later but I don't want to
            // right now.
            if cfg.s3_access_key.len() == 0 || cfg.s3_secret_key.len() == 0 {
                cfg.prompt_s3_keys();
            };
            // Get job from argument provided
            let mut j = match cfg.jobs.get_mut(&job) {
                Some(j) => j.clone(),
                None => {
                    eprintln!("job {} does not exist.", &job);
                    process::exit(1);
                }
            };
            // Upload all files in a seperate thread
            // TODO: Propagate and bubble errors through upload
            match j.upload(&secret, &cfg.s3_access_key, &cfg.s3_secret_key) {
                Ok(_) => println!("job {} uploaded successfully.", &job),
                Err(e) => eprintln!("job {} upload failed: {}", &job, e),
            };
            // Save updates to config
            match cfg.save() {
                Ok(_) => (),
                Err(e) => eprintln!("failed to save kip configuration: {}", e),
            };
        }

        // Start a backup restore
        Subcommands::Pull { job, secret } => {
            // Only one restore can be happening at a time
            unimplemented!();
        }

        // Abort a running job
        Subcommands::Abort { job } => {
            // Get config
            let mut cfg: KipConf = KipConf::get().expect("failed to get kip configuration.");
            // Prompt user for s3 info if nil or wrong format
            // Probably used regex later but I don't want to
            // right now.
            if cfg.s3_access_key.len() == 0 || cfg.s3_secret_key.len() == 0 {
                cfg.prompt_s3_keys();
            };
            // Get job from argument provided
            let j = match cfg.jobs.get_mut(&job) {
                Some(j) => j,
                None => {
                    eprintln!("job {} does not exist.", &job);
                    process::exit(1);
                }
            };
            // Abort job
            j.abort();
        }

        // Get the status of a job
        Subcommands::Status { job } => {
            // Get config
            let cfg: KipConf = KipConf::get().expect("failed to get kip configuration.");
            // Get job from argument provided
            let j = match cfg.jobs.get(&job) {
                Some(j) => j,
                None => {
                    eprintln!("job {} does not exist.", &job);
                    process::exit(1);
                }
            };
            println!("{:#?}", j);
        }

        // List all jobs
        Subcommands::List {} => {
            // Get config
            let cfg: KipConf = KipConf::get().expect("failed to get kip configuration.");
            // List on job must be decrypted
            println!("{:#?}", &cfg.jobs);
        }
    }
}
