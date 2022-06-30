//
// Copyright (c) 2020 Ryan Ciehanski <ryan@ciehanski.com>
//

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[clap(name = "kip")]
#[clap(version, about, author, long_about = None)]
pub struct Cli {
    /// Subcommands
    #[clap(subcommand)]
    pub subcommands: Subcommands,
    // Display debug messages
    #[clap(global = true, short = 'd', long = "debug")]
    pub debug: Option<bool>,
}

#[derive(Debug, Subcommand)]
pub enum Subcommands {
    /// Creates a new job
    #[clap(arg_required_else_help = true)]
    Init {
        /// Name of the job you want to create
        #[clap(value_parser)]
        job: String,
    },

    /// Adds file(s) to a job
    #[clap(arg_required_else_help = true)]
    Add {
        /// Name of the job you want to add files to
        #[clap(value_parser)]
        job: String,
        /// The paths of all files to add to job
        #[clap(short = 'f', long = "files", value_parser)]
        file_path: Vec<String>,
    },

    /// Removes file(s) from a job
    #[clap(alias = "rm", arg_required_else_help = true)]
    Remove {
        /// Name of the job you want to remove files from
        #[clap(value_parser)]
        job: String,
        /// The paths of all files to remove from job
        #[clap(short = 'f', long = "files", value_parser)]
        file_path: Option<Vec<String>>,
        /// Purge file from all previous backups
        #[clap(short = 'p', long = "purge", value_parser)]
        purge: Option<bool>,
    },

    /// Starts a manual backup job
    #[clap(arg_required_else_help = true)]
    Push {
        /// Name of the job you want to start
        #[clap(value_parser)]
        job: String,
    },

    /// Starts a restore of a job
    #[clap(arg_required_else_help = true)]
    Pull {
        /// Name of the job you want to restore from
        #[clap(value_parser)]
        job: String,
        /// Number of the job's run to restore from
        #[clap(short = 'r', long = "run", value_parser)]
        run: usize,
        /// Folder to restore files into
        #[clap(short = 'o', long = "output", value_parser)]
        output_folder: Option<String>,
    },

    /// Aborts any running or future upload on job
    #[clap(arg_required_else_help = true)]
    Abort {
        /// Name of the job you want to abort
        #[clap(value_parser)]
        job: String,
    },

    /// Prints the status of a job or run
    #[clap(arg_required_else_help = true)]
    Status {
        /// Name of the job you want to get status from
        #[clap(value_parser)]
        job: String,
        /// Number of the run you want to get status from
        #[clap(short = 'r', long = "run", value_parser)]
        run: Option<usize>,
    },

    /// Lists jobs, runs, and their configurations
    #[clap(alias = "ls")]
    List {
        /// Name of the job you want to list
        #[clap(value_parser)]
        job: Option<String>,
        /// Number of the run you want to list
        #[clap(short = 'r', long = "run", value_parser)]
        run: Option<usize>,
    },

    /// Runs kip in daemon service mode
    Daemon {},
}
