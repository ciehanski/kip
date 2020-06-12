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

    /// Adds files to a job
    ///
    #[structopt(name = "add")]
    Add {
        /// Name of the job you want to add files to
        job: String,
        /// which
        #[structopt(short = "f", long = "files")]
        file_path: Vec<String>,
    },

    /// Removes files from a job
    ///
    #[structopt(name = "remove", alias = "rm")]
    Remove {
        /// Name of the job you want to remove files from
        job: String,
        /// The IP address the C2 server will bind to
        #[structopt(short = "f", long = "files")]
        file_path: Option<Vec<String>>,
    },

    /// Starts a manual backup job
    ///
    #[structopt(name = "push")]
    Push {
        /// Name of the job you want to start
        job: String,
        /// The password used to encrypt the backup
        #[structopt(short = "s", long = "secret")]
        secret: String,
    },

    /// Starts a restore of a job
    ///
    #[structopt(name = "pull")]
    Pull {
        /// Name of the job you want to restore from
        job: String,
        ///
        #[structopt(short = "s", long = "secret")]
        secret: String,
        ///
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

    /// Gets the status of a job
    ///
    #[structopt(name = "status")]
    Status {
        /// Name of the job you want to get status from
        job: String,
    },

    /// Lists all jobs and their configurations
    ///
    #[structopt(name = "list", alias = "ls")]
    List {},
}
