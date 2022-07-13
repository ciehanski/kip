//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[clap(name = "kip")]
#[clap(version, about, author, long_about = None)]
pub struct Cli {
    /// Subcommands
    #[clap(subcommand)]
    pub subcommands: Subcommands,
    /// Print verbose logs and errors
    #[clap(short = 'd', long = "debug", action)]
    pub debug: bool,
}

#[derive(Debug, Subcommand)]
pub enum Subcommands {
    /// Creates a new job
    #[clap(alias = "new", arg_required_else_help = true)]
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
        #[clap(
            required = true,
            short = 'f',
            long = "files",
            min_values = 0,
            value_parser
        )]
        file_path: Vec<String>,
    },

    /// Removes a job or its runs & file(s)
    #[clap(alias = "rm", arg_required_else_help = true)]
    Remove {
        /// Name of the job you want to remove files from
        #[clap(value_parser)]
        job: String,
        /// Number of the job's run to remove
        #[clap(short = 'r', long = "run", value_parser)]
        run: Option<usize>,
        /// The paths of all files to remove from job
        #[clap(short = 'f', long = "files", min_values = 0, value_parser)]
        file_path: Option<Vec<String>>,
        /// Purge file from all previous remote backups
        #[clap(short = 'p', long = "purge", action)]
        purge: bool,
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
        #[clap(required = true, short = 'r', long = "run", value_parser)]
        run: usize,
        /// Folder to restore files into
        #[clap(short = 'o', long = "output", value_parser)]
        output_folder: Option<String>,
    },

    /// Pauses all job uploads until manually resumed
    #[clap(arg_required_else_help = true)]
    Pause {
        /// Name of the job you want to pause
        #[clap(value_parser)]
        job: String,
    },

    /// Resumes all pending and future job uploads
    #[clap(arg_required_else_help = true)]
    Resume {
        /// Name of the job you want to resume
        #[clap(value_parser)]
        job: String,
    },

    /// Aborts any running or future upload on job
    #[clap(arg_required_else_help = true)]
    Abort {
        /// Name of the job you want to abort
        #[clap(value_parser)]
        job: String,
    },

    /// Lists jobs' status, runs, and their configurations
    #[clap(alias = "ls")]
    Status {
        /// Name of the job you want to list
        #[clap(value_parser)]
        job: Option<String>,
        /// Number of the run you want to list
        #[clap(short = 'r', long = "run", value_parser)]
        run: Option<usize>,
    },

    #[clap(hide = true)]
    Daemon {},
}

#[cfg(test)]
mod tests {
    use assert_cmd::Command;

    #[test]
    fn test_cli_runs() {
        let mut cmd = Command::cargo_bin("kip").unwrap();
        cmd.assert().failure().code(2);
    }

    #[test]
    fn test_status() {
        let mut cmd = Command::cargo_bin("kip").unwrap();
        let assert = cmd.arg("status").assert();
        assert.success();
    }

    #[test]
    fn test_status_ls() {
        let mut cmd = Command::cargo_bin("kip").unwrap();
        let assert = cmd.arg("ls").assert();
        assert.success();
    }

    #[ignore]
    #[test]
    fn test_add() {
        let mut cmd = Command::cargo_bin("kip").unwrap();
        let assert = cmd.arg("add").arg("test1").assert();
        assert.failure().code(2);
    }

    #[test]
    fn test_cli_daemon() {
        let mut cmd = Command::cargo_bin("kip").unwrap();
        let assert = cmd.arg("daemon").assert();
        assert.success();
    }
}
