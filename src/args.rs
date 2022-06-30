//
// Copyright (c) 2020 Ryan Ciehanski <ryan@ciehanski.com>
//

use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "kip", about = "kip, the simple encrypted backups tool.")]
pub struct Opt {
    /// Subcommands
    #[structopt(subcommand)]
    pub subcommands: Subcommands,
    /// Display debug messages
    #[structopt(global = true, short = "d", long = "debug")]
    pub debug: bool,
}

#[derive(StructOpt)]
pub enum Subcommands {
    /// Creates a new job
    ///
    #[structopt(name = "init")]
    Init {
        /// Name of the job you want to create
        job: String,
    },

    /// Adds file(s) to a job
    ///
    #[structopt(name = "add")]
    Add {
        /// Name of the job you want to add files to
        job: String,
        /// The paths of all files to add to job
        #[structopt(short = "f", long = "files")]
        file_path: Vec<String>,
    },

    /// Removes file(s) from a job
    ///
    #[structopt(name = "remove", alias = "rm")]
    Remove {
        /// Name of the job you want to remove files from
        job: String,
        /// The paths of all files to remove remove job
        #[structopt(short = "f", long = "files")]
        file_path: Option<Vec<String>>,
        /// Purge file from all previous backups
        #[structopt(short = "p", long = "purge")]
        purge: Option<bool>,
    },

    /// Starts a manual backup job
    ///
    #[structopt(name = "push")]
    Push {
        /// Name of the job you want to start
        job: String,
    },

    /// Starts a restore of a job
    ///
    #[structopt(name = "pull")]
    Pull {
        /// Name of the job you want to restore from
        job: String,
        /// Number of the job's run to restore from
        #[structopt(short = "r", long = "run")]
        run: usize,
        /// Folder to restore files into
        #[structopt(short = "o", long = "output")]
        output_folder: Option<String>,
    },

    /// Aborts any running or future upload on job
    ///
    #[structopt(name = "abort")]
    Abort {
        /// Name of the job you want to abort
        job: String,
    },

    /// Returns the status of a job or run
    ///
    #[structopt(name = "status")]
    Status {
        /// Name of the job you want to get status from
        job: String,
        /// Number of the job
        #[structopt(short = "r", long = "run")]
        run: Option<usize>,
    },

    /// Lists jobs, runs, and their configurations
    ///
    #[structopt(name = "list", alias = "ls")]
    List {
        /// Name of the job you want to list
        job: Option<String>,
        /// Name of the run you want to list
        #[structopt(short = "r", long = "run")]
        run: Option<usize>,
    },

    /// Runs kip in daemon service mode
    ///
    #[structopt(name = "daemon", alias = "d")]
    Daemon {},
}
