//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use aws_sdk_s3::Region;
use chrono::prelude::*;
use clap::Parser;
use colored::*;
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::*;
use dialoguer::{theme::ColorfulTheme, Confirm, Password, Select};
use kip::cli::{Cli, Subcommands};
use kip::compress::KipCompressOpts;
use kip::conf::KipConf;
use kip::crypto::{keyring_get_secret, keyring_set_secret};
use kip::job::{Job, KipFile, KipStatus};
use kip::providers::{gdrive::KipGdrive, s3::KipS3, usb::KipUsb, KipProviders};
use kip::smtp::{send_email, KipEmail};
use kip::terminate;
use pretty_bytes::converter::convert;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use sysinfo::{DiskExt, System, SystemExt};
use tokio::runtime::Builder;
use tracing::{info, span, warn, Level};

fn main() {
    // Get config and metadata file
    let (cfg_file, md_file) = KipConf::new().unwrap_or_else(|e| {
        terminate!(5, "{} failed to get kip configuration: {e}.", "[ERR]".red());
    });
    let cfg = &*Arc::clone(&cfg_file);
    let md = Arc::clone(&md_file);

    // Custom Tokio Runtime
    let runtime = Builder::new_multi_thread()
        .worker_threads(cfg.settings.worker_threads)
        .thread_name("kip")
        .enable_all()
        .on_thread_start(|| {
            info!("thread started");
        })
        .on_thread_stop(|| {
            info!("thread stopped");
        })
        .build()
        .unwrap_or_else(|e| {
            terminate!(11, "unable to initialize kip runtime: {e}.");
        });

    // Get subcommands and args
    let args = Cli::parse();
    let _debug = args.debug;

    runtime.block_on(async {
        // Setup logging and tracing
        // SAFETY: safe to unwrap proj_dir since KipConf::new()
        // creates the directory if it does not exist
        let proj_dir = directories::ProjectDirs::from("com", "ciehanski", "kip").unwrap();
        let log_file = tracing_appender::rolling::daily(proj_dir.config_dir(), "kip.log");
        let (log_non_blocking, _guard) = tracing_appender::non_blocking(log_file);
        tracing_subscriber::fmt()
            .with_thread_names(true)
            .with_thread_ids(true)
            .with_max_level(cfg.settings.debug_level.parse())
            .with_writer(log_non_blocking)
            .try_init()
            .unwrap_or_else(|e| {
                eprintln!("{} unable to initialize kip tracing: {e}", "[ERR]".red());
            });

        // Prompt for SMTP password to be stored in keyring
        // if SMTP settings have been modified/configured in cfg
        if cfg.settings.email_notification {
            match keyring_get_secret("com.ciehanski.kip.smtp") {
                Ok(_) => {}
                Err(e) => match e {
                    // Only prompt if there is currently no entry in keyring
                    keyring::Error::NoEntry => {
                        // Get SMTP password from user input
                        let smtp_pass = Password::new()
                            .with_prompt("Please provide the SMTP authentication password")
                            .interact()
                            .expect("[ERR] failed to create encryption secret prompt.");
                        // Store SMTP password onto local OS keyring
                        keyring_set_secret("com.ciehanski.kip.smtp", &smtp_pass).unwrap_or_else(|e| {
                            terminate!(
                                10,
                                "{} failed to push SMTP password onto keyring: {e}.",
                                "[ERR]".red(),
                            );
                        });
                    }
                    _ => {
                        terminate!(
                            11,
                            "{} failed to get SMTP secret from keyring: {e}.",
                            "[ERR]".red()
                        );
                    }
                },
            }
        }

        // Execute user input command
        match args.subcommands {
            // Create a new job
            Subcommands::Init { job } => {
                let _trace = span!(Level::DEBUG, "KIP_INIT").entered();
                let mut md = md.write().await;
                // Ensure that job does not already exist with
                // the provided name.
                for (j, _) in md.jobs.iter() {
                    if j == &job {
                        terminate!(17, "{} job '{job}' already exists.", "[ERR]".red());
                    }
                }
                // Get secret from user input
                let secret = Password::new()
                    .with_prompt("Please provide your encryption secret")
                    .interact()
                    .expect("[ERR] failed to create encryption secret prompt.");
                // Store secret onto local OS keyring
                keyring_set_secret(&format!("com.ciehanski.kip.{job}"), &secret).unwrap_or_else(
                    |e| {
                        terminate!(
                            5,
                            "{} failed to push secret onto keyring: {e}.",
                            "[ERR]".red(),
                        );
                    },
                );
                // Confirm if S3 or USB job
                let provider_selection: usize = Select::with_theme(&ColorfulTheme::default())
                    .item("S3")
                    .item("Google Drive")
                    .item("USB")
                    .default(0)
                    .interact()
                    .expect("[ERR] unable to create provider selection menu.");
                match provider_selection {
                    0 => {
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
                        keyring_set_secret(
                            &format!("com.ciehanski.kip.{job}.s3acc"),
                            &s3_acc_key,
                        )
                        .unwrap_or_else(|e| {
                            terminate!(
                                5,
                                "{} failed to push S3 access key onto keyring: {e}.",
                                "[ERR]".red(),
                            );
                        });
                        // Get S3 secret key from user input
                        let s3_sec_key = Password::new()
                            .with_prompt("Please provide the S3 secret key")
                            .interact()
                            .expect("[ERR] failed to create S3 secret key prompt.");
                        // Store S3 secret key onto local OS keyring
                        keyring_set_secret(
                            &format!("com.ciehanski.kip.{job}.s3sec"),
                            &s3_sec_key,
                        )
                        .unwrap_or_else(|e| {
                            terminate!(
                                5,
                                "{} failed to push S3 secret key onto keyring: {e}.",
                                "[ERR]".red(),
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
                        let provider = KipProviders::S3(KipS3::new(
                            s3_bucket_name.trim_end(),
                            Region::new(s3_region.trim_end().to_owned()),
                        ));
                        let new_job = Job::new(
                            &job,
                            provider,
                            KipCompressOpts::new(
                                cfg.settings.compression,
                                cfg.settings.compression_alg,
                                cfg.settings.compress_level
                            ),
                        );
                        // Push new job in config
                        md.jobs.insert(job.clone(), new_job);
                    }
                    1 => {
                        // Google Drive
                        // Get Google Drive client ID from user input
                        print!("Please provide the Google Drive OAuth client ID: ");
                        std::io::stdout()
                            .flush()
                            .expect("[ERR] failed to flush stdout.");
                        let mut gdrive_client_id = String::new();
                        std::io::stdin().read_line(&mut gdrive_client_id).expect(
                            "[ERR] failed to read Google Drive OAuth client ID from stdin.",
                        );
                        // Store Google Drive client ID onto local OS keyring
                        keyring_set_secret(
                            &format!("com.ciehanski.kip.{job}.gdriveid"),
                            &gdrive_client_id,
                        )
                        .unwrap_or_else(|e| {
                            terminate!(
                                5,
                                "{} failed to push Google Drive client ID onto keyring: {e}.",
                                "[ERR]".red(),
                            );
                        });
                        // Get Google Drive client secret from user input
                        print!("Please provide the Google Drive OAuth client secret: ");
                        std::io::stdout()
                            .flush()
                            .expect("[ERR] failed to flush stdout.");
                        let mut gdrive_client_sec = String::new();
                        std::io::stdin().read_line(&mut gdrive_client_sec).expect(
                            "[ERR] failed to read Google Drive OAuth client secret from stdin.",
                        );
                        // Store Google Drive client ID onto local OS keyring
                        keyring_set_secret(
                            &format!("com.ciehanski.kip.{job}.gdrivesec"),
                            &gdrive_client_sec,
                        )
                        .unwrap_or_else(|e| {
                            terminate!(
                                5,
                                "{} failed to push Google Drive client ID onto keyring: {e}.",
                                "[ERR]".red(),
                            );
                        });
                        // Get GDrive parent folder from user input
                        print!("Optionally, provide the parent folder ID: ");
                        std::io::stdout()
                            .flush()
                            .expect("[ERR] failed to flush stdout.");
                        let mut gdrive_folder = String::new();
                        std::io::stdin().read_line(&mut gdrive_folder).expect(
                            "[ERR] failed to read Google Drive parent folder ID from stdin.",
                        );
                        // Create the new job
                        let provider =
                            KipProviders::Gdrive(KipGdrive::new(Some(gdrive_folder.trim_end())));
                        let new_job = Job::new(
                            &job,
                            provider,
                            KipCompressOpts::new(
                                cfg.settings.compression,
                                cfg.settings.compression_alg,
                                cfg.settings.compress_level
                            ),
                        );
                        // Push new job in config
                        md.jobs.insert(job.clone(), new_job);
                    }
                    2 => {
                        // USB
                        let mut sys = System::new();
                        sys.refresh_disks_list();
                        let disks = sys.disks();
                        let disks_str: Vec<String> = disks
                            .iter()
                            .filter_map(|d| {
                                let disk = d
                                    .name()
                                    .to_str()
                                    .expect("[ERR] unable to convert disk's OsStr to String");
                                match disk {
                                    "Macintosh HD - Data" => None,
                                    "VM" => None,
                                    "Preboot" => None,
                                    "Update" => None,
                                    "Recovery" => None,
                                    "" => None,
                                    _ => Some(disk.to_owned()),
                                }
                            })
                            .collect();
                        // Ensure USB devices were found
                        if disks_str.is_empty() {
                            terminate!(1, "no USB devices detected.");
                        };
                        // Confirm which USB device
                        let provider_selection: usize =
                            Select::with_theme(&ColorfulTheme::default())
                                .items(&disks_str)
                                .default(0)
                                .interact()
                                .unwrap_or_else(|_| { terminate!(1, "[ERR] unable to create USB selection menu") });
                        // Create the new job
                        let provider = KipProviders::Usb(KipUsb::new(
                            disks[provider_selection]
                                .name()
                                .to_str()
                                .unwrap_or_else(|| {
                                    terminate!(1, "[ERR] unable to convert disk's OsStr to String");
                                })
                                .to_owned(),
                            disks[provider_selection].mount_point(),
                            disks[provider_selection].total_space(),
                            disks[provider_selection].available_space(),
                        ));
                        let new_job = Job::new(
                            &job,
                            provider,
                            KipCompressOpts::new(
                                cfg.settings.compression,
                                cfg.settings.compression_alg,
                                cfg.settings.compress_level
                            ),
                        );
                        // Push new job in config
                        md.jobs.insert(job.clone(), new_job);
                    }
                    _ => {
                        terminate!(1, "Invalid selection. Please try again.");
                    }
                }
                // Store new job in config
                match md.save() {
                    Ok(_) => println!("{} job '{job}' successfully created.", "[OK]".green()),
                    Err(e) => {
                        terminate!(7, "{} failed to save kip configuration: {e}", "[ERR]".red())
                    }
                }
            }

            // Add more files or directories to job
            Subcommands::Add { job, file_path } => {
                let _trace = span!(Level::DEBUG, "KIP_ADD").entered();
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{job}' doesn't exist.", "[ERR]".red());
                });
                for f in &file_path {
                    // Check if path exists
                    if !Path::new(f).exists() {
                        terminate!(2, "{} '{f}' doesn't exist.", "[ERR]".red());
                    }
                    // Check if files are excluded by this job
                    for jf in &j.excluded_files {
                        if jf
                            == &Path::new(f)
                                .canonicalize()
                                .expect("[ERR] unable to canonicalize path.")
                        {
                            terminate!(
                                17,
                                "{} file(s) are excluded on job '{job}'.",
                                "[ERR]".red(),
                            );
                        }
                    }
                    // Check if files already exist on job
                    // to avoid duplication.
                    for jf in &j.files {
                        if jf.path
                            == Path::new(f)
                                .canonicalize()
                                .expect("[ERR] unable to canonicalize path.")
                        {
                            terminate!(
                                17,
                                "{} file(s) already exist on job '{job}'.",
                                "[ERR]".red(),
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
                    ).expect("[ERR] unable to create KipFile."));
                }
                // Get new files amount for job
                j.files_amt = j.get_files_amt(cfg.settings.follow_symlinks).unwrap_or_else(|e| {
                    terminate!(
                        6,
                        "{} failed to get file amounts for {job}: {e}.",
                        "[ERR]".red(),
                    );
                });
                // Save changes to config file
                match md.save() {
                    Ok(_) => println!(
                        "{} files successfully added to job '{job}'.",
                        "[OK]".green(),
                    ),
                    Err(e) => {
                        terminate!(7, "{} failed to save kip configuration: {e}", "[ERR]".red())
                    }
                }
            }

            // Remove files from a job
            Subcommands::Remove {
                job,
                file_path,
                purge,
            } => {
                let _trace = span!(Level::DEBUG, "KIP_REMOVE").entered();
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{job}' doesn't exist.", "[ERR]".red());
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
                            let fpath = PathBuf::from(&f)
                                .canonicalize()
                                .expect("[ERR] unable to canonicalize path.");
                            let mut found = false;
                            for kf in j.files.iter() {
                                if kf.path == fpath {
                                    found = true;
                                }
                            }
                            if found {
                                // Retain all elements != files_path argument provided
                                j.files.retain(|kf| kf.path != fpath);
                                // Find all the runs that contain this file's chunks
                                // and remove them from S3.
                                if purge {
                                    j.purge_file(&f).await.unwrap_or_else(|e| {
                                        terminate!(
                                            21,
                                            "{} failed to remove files from S3 for {job}: {e}.",
                                            "[ERR]".red(),
                                        );
                                    });
                                }
                            } else {
                                terminate!(
                                    2,
                                    "{} job '{}' does not contain '{f}'.",
                                    "[ERR]".red(),
                                    j.name,
                                );
                            };
                        }
                        // Update files amt for job
                        j.files_amt = j.get_files_amt(cfg.settings.follow_symlinks).unwrap_or_else(|e| {
                            terminate!(
                                6,
                                "{} failed to get file amounts for {job}: {e}.",
                                "[ERR]".red(),
                            );
                        });
                        // Success
                        println!(
                            "{} files successfully removed from job '{job}'.",
                            "[OK]".green(),
                        );
                    }
                    None => {
                        // Remove job's keyring entries
                        j.delete_keyring_entries()
                            // -f and -r were not provided, delete the job
                            .expect("unable to delete keyring entries for job");
                        md.jobs.remove(&job);
                        println!("{} job '{job}' successfully removed.", "[OK]".green())
                    }
                }
                // Save changes to config file
                match md.save() {
                    Ok(_) => {}
                    Err(e) => {
                        terminate!(7, "{} failed to save kip configuration: {e}", "[ERR]".red());
                    }
                }
            }

            // Excludes files or directories from a job
            Subcommands::Exclude {
                job,
                file_path,
                extensions,
            } => {
                let _trace = span!(Level::DEBUG, "KIP_EXCLUDE").entered();
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{job}' doesn't exist.", "[ERR]".red());
                });
                // Confirm correct secret from user input
                confirm_secret(&j.name);
                // Confirm if files or exentions were provided
                if let Some(fp) = file_path {
                    for f in &fp {
                        // Check if file or directory exists
                        if !Path::new(f).exists() {
                            terminate!(2, "{} '{f}' doesn't exist.", "[ERR]".red());
                        }
                        // Check if this file is being backed up by the job
                        // cancel excluding it if so
                        for jf in &j.files {
                            let f_path = Path::new(f)
                                .canonicalize()
                                .expect("[ERR] unable to canonicalize path.");
                            if jf.path == f_path {
                                terminate!(
                                    17,
                                    "{} unable to exclude files that are being backed up by '{job}'.",
                                    "[ERR]".red(),
                                );
                            }
                        }
                        // Check if files are already excluded from job
                        // to avoid duplication.
                        for jf in &j.excluded_files {
                            let jf_path = Path::new(jf)
                                .canonicalize()
                                .expect("[ERR] unable to canonicalize path.");
                            let f_path = Path::new(f)
                                .canonicalize()
                                .expect("[ERR] unable to canonicalize path.");
                            if jf_path == f_path {
                                terminate!(
                                    17,
                                    "{} file(s) already excluded from job '{job}'.",
                                    "[ERR]".red(),
                                );
                            }
                        }
                        // Push exclusions to job
                        j.excluded_files.push(
                            PathBuf::from(f)
                                .canonicalize()
                                .expect("[ERR] unable to canonicalize path."),
                        );
                    }
                } else if let Some(e) = extensions {
                    // Check if extensions are already excluded from job
                    // to avoid duplication.
                    for ext in &e {
                        for jext in &j.excluded_file_types {
                            if jext == ext {
                                terminate!(
                                    17,
                                    "{} file(s) already excluded from job '{job}'.",
                                    "[ERR]".red(),
                                );
                            }
                        }
                        // Push excluded extensions to job
                        j.excluded_file_types.push(ext.to_string());
                    }
                } else {
                    terminate!(99, "no file path or extensions provided.");
                }
                // Save changes to config file
                match md.save() {
                    Ok(_) => println!("{} exclusions added to job '{job}'.", "[OK]".green()),
                    Err(e) => terminate!(
                        7,
                        "{} failed to save kip configuration: {e}",
                        "[ERR]".red(),
                    ),
                }
            }

            // Start a job's upload
            Subcommands::Push { job } => {
                let _trace = span!(Level::DEBUG, "KIP_PUSH").entered();
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{job}' doesn't exist.", "[ERR]".red());
                });
                // Get new files amount for job
                j.files_amt = j.get_files_amt(cfg.settings.follow_symlinks).unwrap_or_else(|e| {
                    terminate!(
                        6,
                        "{} failed to get file amounts for {job}: {e}.",
                        "[ERR]".red(),
                    );
                });
                // Confirm correct secret from user input
                let secret = confirm_secret(&j.name);
                // Check if battery level is charged enough
                if !cfg.settings.run_on_low_battery {
                    match check_battery() {
                        Ok(_) => {},
                        Err(e) => terminate!(29, "{} {e}", "[ERR]".red()),
                    }
                }
                // Upload all files in a seperate thread
                match j
                    .start_run(
                        &secret,
                        cfg.settings.follow_symlinks,
                    )
                    .await
                {
                    Ok(_) => {
                        // Send success email if setting enabled
                        if cfg.settings.email_notification {
                            if let Some(run) = j.runs.get(&j.runs.len()) {
                                // Craft the email
                                let email = KipEmail {
                                    title: format!(
                                        "[ok] {}-{} completed successfully",
                                        j.name, run.id
                                    ),
                                    alert_type: kip::smtp::KipAlertType::Success,
                                    alert_logs: run.logs.to_owned(),
                                };
                                // Send
                                match send_email(cfg.smtp_config.to_owned(), email).await {
                                    Ok(_) => {},
                                    Err(e) => {
                                        warn!("error sending email notification: {e}");
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // Send error email if setting enabled
                        if cfg.settings.email_notification {
                            if let Some(run) = j.runs.get(&j.runs.len()) {
                                // Craft the email
                                let email = KipEmail {
                                    title: format!(
                                        "[fail] {}-{} completed with errors",
                                        j.name, run.id
                                    ),
                                    alert_type: kip::smtp::KipAlertType::Error,
                                    alert_logs: run.logs.to_owned(),
                                };
                                // Send
                                match send_email(cfg.smtp_config.to_owned(), email).await {
                                    Ok(_) => {},
                                    Err(e) => {
                                        warn!("error sending email notification: {e}");
                                    }
                                }
                            }
                        }
                        terminate!(
                            8,
                            "{} job '{job}' upload to '{}' failed: {e}",
                            "[ERR]".red(),
                            j.provider_name(),
                        );
                    }
                }
                // Save changes to config file
                md.save().unwrap_or_else(|e| {
                    terminate!(
                        7,
                        "{} failed to save kip configuration: {e}",
                        "[ERR]".red(),
                    );
                });
            }

            // Start a backup restore
            Subcommands::Pull {
                job,
                run,
                output_folder,
            } => {
                let _trace = span!(Level::DEBUG, "KIP_PULL").entered();
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{job}' doesn't exist.", "[ERR]".red());
                });
                // Confirm correct secret from user input
                let secret = confirm_secret(&j.name);
                // Get output folder
                let output_folder = output_folder.unwrap_or_else(|| {
                    terminate!(2, "{} invalid output folder provided.", "[ERR]".red());
                });
                let dmd = std::fs::metadata(&output_folder)
                    .expect("[ERR] unable to gather output folder metadata");
                if !dmd.is_dir() || output_folder.is_empty() {
                    terminate!(
                        2,
                        "{} output folder provided is not a valid directory.",
                        "[ERR]".red()
                    );
                }
                // Check if battery level is charged enough
                if !cfg.settings.run_on_low_battery {
                    match check_battery() {
                        Ok(_) => {},
                        Err(e) => terminate!(29, "{} {e}", "[ERR]".red()),
                    }
                }
                // Run the restore
                match j.start_restore(run, &secret, &output_folder).await {
                    Ok(_) => {}
                    Err(e) => {
                        terminate!(2, "{} {e}", "[ERR]".red());
                    }
                };
                // Save changes to config file
                md.save().unwrap_or_else(|e| {
                    terminate!(
                        7,
                        "{} failed to save kip configuration: {e}",
                        "[ERR]".red(),
                    );
                });
            }

            // Pauses a job and future runs
            Subcommands::Pause { job } => {
                let _trace = span!(Level::DEBUG, "KIP_PAUSE").entered();
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{job}' doesn't exist.", "[ERR]".red());
                });
                // Confirm correct secret from user input
                let _ = confirm_secret(&j.name);
                // Set job to paused
                j.paused = true;
                // Send paused alert email if setting enabled
                if cfg.settings.email_notification {
                    // Craft the email
                    let email = KipEmail {
                        title: format!(
                            "[info] {} has been paused",
                            j.name
                        ),
                        alert_type: kip::smtp::KipAlertType::Information,
                        alert_logs: vec![],
                    };
                    // Send
                    match send_email(cfg.smtp_config.to_owned(), email).await {
                        Ok(_) => {},
                        Err(e) => {
                            warn!("error sending email notification: {e}");
                        }
                    }
                }
                // Save changes to config file
                md.save().unwrap_or_else(|e| {
                    terminate!(
                        7,
                        "{} failed to save kip configuration: {e}",
                        "[ERR]".red(),
                    );
                });
            }

            // Resumes a job and future runs
            Subcommands::Resume { job } => {
                let _trace = span!(Level::DEBUG, "KIP_RESUME").entered();
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{job}' doesn't exist.", "[ERR]".red());
                });
                // Confirm correct secret from user input
                let secret = confirm_secret(&j.name);
                // Set set to !paused
                j.paused = false;
                // Send resumed alert email if setting enabled
                if cfg.settings.email_notification {
                    // Craft the email
                    let email = KipEmail {
                        title: format!(
                            "[info] {} has been resumed",
                            j.name
                        ),
                        alert_type: kip::smtp::KipAlertType::Information,
                        alert_logs: vec![],
                    };
                    // Send
                    match send_email(cfg.smtp_config.to_owned(), email).await {
                        Ok(_) => {},
                        Err(e) => {
                            warn!("error sending email notification: {e}");
                        }
                    }
                }
                // Run a manual upload
                match j
                    .start_run(
                        &secret,
                        cfg.settings.follow_symlinks,
                    )
                    .await
                {
                    Ok(_) => {
                        // Send success email if setting enabled
                        if cfg.settings.email_notification {
                            if let Some(run) = j.runs.get(&j.runs.len()) {
                                // Craft the email
                                let email = KipEmail {
                                    title: format!(
                                        "[ok] {}-{} completed successfully",
                                        j.name, run.id
                                    ),
                                    alert_type: kip::smtp::KipAlertType::Success,
                                    alert_logs: run.logs.to_owned(),
                                };
                                // Send
                                match send_email(cfg.smtp_config.to_owned(), email).await {
                                    Ok(_) => {},
                                    Err(e) => {
                                        warn!("error sending email notification: {e}");
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // Send error email if setting enabled
                        if cfg.settings.email_notification {
                            if let Some(run) = j.runs.get(&j.runs.len()) {
                                // Craft the email
                                let email = KipEmail {
                                    title: format!(
                                        "[fail] {}-{} completed with errors",
                                        j.name, run.id
                                    ),
                                    alert_type: kip::smtp::KipAlertType::Error,
                                    alert_logs: run.logs.to_owned(),
                                };
                                // Send
                                match send_email(cfg.smtp_config.to_owned(), email).await {
                                    Ok(_) => {},
                                    Err(e) => {
                                        warn!("error sending email notification: {e}");
                                    }
                                }
                            }
                        }
                        terminate!(
                            8,
                            "{} job '{job}' upload to '{}' failed: {e}",
                            "[ERR]".red(),
                            j.provider_name(),
                        );
                    }
                }
                // Save changes to config file
                md.save().unwrap_or_else(|e| {
                    terminate!(
                        7,
                        "{} failed to save kip configuration: {e}",
                        "[ERR]".red(),
                    );
                });
            }

            // Abort a running job
            Subcommands::Abort { job } => {
                let _trace = span!(Level::DEBUG, "KIP_ABORT").entered();
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{job}' doesn't exist.", "[ERR]".red());
                });
                // Confirm removal
                if !Confirm::new()
                    .with_prompt(format!("Are you sure you want to abort '{job}'?"))
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

            // List all jobs
            // This function is messy. Should probably cleanup.
            Subcommands::Status { job, run } => {
                let _trace = span!(Level::DEBUG, "KIP_STATUS").entered();
                let md = md.read().await;
                // Create the table
                let mut table = Table::new();
                table
                    .load_preset(UTF8_FULL)
                    .apply_modifier(UTF8_ROUND_CORNERS)
                    .set_content_arrangement(ContentArrangement::Dynamic);
                if job.is_none() && run.is_none() {
                    // Create the header row
                    table.set_header(&vec![
                        "Name",
                        "ID",
                        "Provider",
                        "Files",
                        "Total Runs",
                        "Last Run",
                        "Status",
                    ]);
                    // For each job, add a row
                    for (_, j) in md.jobs.iter() {
                        let correct_last_run = if j.last_run.format("%Y-%m-%d %H:%M:%S").to_string()
                            == "1970-01-01 00:00:00"
                        {
                            "NEVER_RUN".to_string()
                        } else {
                            let converted: DateTime<Local> = DateTime::from(j.last_run);
                            converted.format("%Y-%m-%d %H:%M:%S").to_string()
                        };
                        let provider = match j.provider {
                            KipProviders::S3(_) => "S3",
                            KipProviders::Usb(_) => "USB",
                            KipProviders::Gdrive(_) => "Google Drive",
                        };
                        // Add row with job info
                        table.add_row(vec![
                            Cell::new(&j.name).fg(comfy_table::Color::Green),
                            Cell::new(j.id),
                            Cell::new(provider),
                            Cell::new(j.files_amt),
                            Cell::new(j.total_runs),
                            Cell::new(correct_last_run),
                            print_status(j.last_status),
                        ]);
                    }
                    // Print the job table
                    println!("{table}");
                } else if job.is_some() && run.is_none() {
                    let job = job.unwrap_or_default();
                    let j = md.jobs.get(&job).unwrap_or_else(|| {
                        terminate!(
                            2,
                            "{} job '{}' doesn't exist.",
                            "[ERR]".red(),
                            &job,
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
                    if !files_vec.is_empty() {
                        for f in files_vec {
                            correct_files.push_str(&f);
                            correct_files.push('\n');
                        }
                    } else {
                        correct_files.push_str("N/A");
                    }
                    match &j.provider {
                        KipProviders::S3(s3) => {
                            table.set_header(&vec![
                                "Name",
                                "ID",
                                "Bucket",
                                "Region",
                                "Selected Files",
                                "Total Runs",
                                "Last Run",
                                "Bytes (in S3)",
                                "Status",
                            ]);
                            // Add row with job info
                            table.add_row(vec![
                                Cell::new(&j.name).fg(comfy_table::Color::Green),
                                Cell::new(j.id),
                                Cell::new(&s3.aws_bucket),
                                Cell::new(&s3.aws_region),
                                Cell::new(correct_files),
                                Cell::new(j.total_runs),
                                Cell::new(correct_last_run),
                                Cell::new(convert(j.bytes_amt_provider as f64)),
                                print_status(j.last_status),
                            ]);
                        }
                        KipProviders::Usb(usb) => {
                            // Create the header row
                            table.set_header(&vec![
                                "Name",
                                "ID",
                                "USB Name",
                                "USB Utilization",
                                "USB Capacity",
                                "Selected Files",
                                "Total Runs",
                                "Last Run",
                                "Bytes (on USB)",
                                "Status",
                            ]);
                            // Add row with job info
                            table.add_row(vec![
                                Cell::new(&j.name).fg(comfy_table::Color::Green),
                                Cell::new(j.id),
                                Cell::new(&usb.name),
                                Cell::new(convert(usb.used_capacity as f64)),
                                Cell::new(convert(usb.capacity as f64)),
                                Cell::new(correct_files),
                                Cell::new(j.total_runs),
                                Cell::new(correct_last_run),
                                Cell::new(convert(j.bytes_amt_provider as f64)),
                                print_status(j.last_status),
                            ]);
                        }
                        KipProviders::Gdrive(gdrive) => {
                            table.set_header(&vec![
                                "Name",
                                "ID",
                                "Gdrive Folder ID",
                                "Selected Files",
                                "Total Runs",
                                "Last Run",
                                "Bytes (Gdrive)",
                                "Status",
                            ]);
                            let parent_folder = match &gdrive.parent_folder {
                                Some(pf) => pf,
                                _ => "/",
                            };
                            // Add row with job info
                            table.add_row(vec![
                                Cell::new(&j.name).fg(comfy_table::Color::Green),
                                Cell::new(j.id),
                                Cell::new(parent_folder),
                                Cell::new(correct_files),
                                Cell::new(j.total_runs),
                                Cell::new(correct_last_run),
                                Cell::new(convert(j.bytes_amt_provider as f64)),
                                print_status(j.last_status),
                            ]);
                        }
                    }
                    // Print the job table
                    println!("{table}");
                } else if job.is_some() && run.is_some() {
                    let job = job.unwrap_or_default();
                    let j = md.jobs.get(&job).unwrap_or_else(|| {
                        terminate!(
                            2,
                            "{} job '{}' doesn't exist.",
                            "[ERR]".red(),
                            &job,
                        );
                    });
                    let rid = run.unwrap_or_else(|| {
                        terminate!(1, "{} unable to find run from provided ID.", "[ERR]".red());
                    });
                    let r = j.runs.get(&rid).unwrap_or_else(|| {
                        terminate!(
                            2,
                            "{} run '{}' doesn't exist for job '{}'.",
                            "[ERR]".red(),
                            rid,
                            &job,
                        );
                    });
                    match &j.provider {
                        KipProviders::S3(s3) => {
                            // Create the header row
                            table.set_header(&vec![
                                "Name",
                                "Bucket",
                                "Region",
                                "Chunks Uploaded",
                                "Bytes Uploaded",
                                "Run Time",
                                "Status",
                            ]);
                            // Add row with run info
                            table.add_row(vec![
                                Cell::new(format!("{}-{}", j.name, r.id))
                                    .fg(comfy_table::Color::Green),
                                Cell::new(&s3.aws_bucket),
                                Cell::new(&s3.aws_region),
                                Cell::new(r.files_changed.len()),
                                Cell::new(convert(r.bytes_uploaded as f64)),
                                Cell::new(&r.time_elapsed),
                                print_status(r.status),
                            ]);
                        }
                        KipProviders::Usb(usb) => {
                            // Create the header row
                            table.set_header(&vec![
                                "Name",
                                "USB Name",
                                "USB Path",
                                "Chunks Uploaded",
                                "Bytes Uploaded",
                                "Run Time",
                                "Status",
                            ]);
                            // Add row with run info
                            table.add_row(vec![
                                Cell::new(format!("{}-{}", j.name, r.id))
                                    .fg(comfy_table::Color::Green),
                                Cell::new(&usb.name),
                                Cell::new(usb.root_path.display().to_string()),
                                Cell::new(r.files_changed.len()),
                                Cell::new(convert(r.bytes_uploaded as f64)),
                                Cell::new(&r.time_elapsed),
                                print_status(r.status),
                            ]);
                        }
                        KipProviders::Gdrive(gdrive) => {
                            // Create the header row
                            table.set_header(&vec![
                                "Name",
                                "Gdrive Folder ID",
                                "Chunks Uploaded",
                                "Bytes Uploaded",
                                "Run Time",
                                "Status",
                            ]);
                            let parent_folder = match &gdrive.parent_folder {
                                Some(pf) => pf,
                                _ => "/",
                            };
                            // Add row with run info
                            table.add_row(vec![
                                Cell::new(format!("{}-{}", j.name, r.id))
                                    .fg(comfy_table::Color::Green),
                                Cell::new(parent_folder),
                                Cell::new(r.files_changed.len()),
                                Cell::new(convert(r.bytes_uploaded as f64)),
                                Cell::new(&r.time_elapsed),
                                print_status(r.status),
                            ]);
                        }
                    }
                    // Create a table for logs
                    let mut logs_table = Table::new();
                    logs_table
                        .load_preset(UTF8_FULL)
                        .apply_modifier(UTF8_ROUND_CORNERS)
                        .set_content_arrangement(ContentArrangement::Dynamic);
                    logs_table.set_header(&vec!["Logs"]);
                    // Pretty print logs
                    let mut pretty_logs = String::new();
                    for (i, l) in r.logs.iter().enumerate() {
                        pretty_logs.push_str(l);
                        if i != r.logs.len() - 1 {
                            pretty_logs.push('\n');
                        }
                    }
                    if pretty_logs.is_empty() {
                        pretty_logs.push_str("None");
                    }
                    // Add row to logs table
                    logs_table.add_row(vec![pretty_logs]);
                    // Print the job table
                    println!("{table}");
                    println!("{logs_table}");
                }
            }

            // Get the status of a job
            Subcommands::Daemon {} => {
                let _trace = span!(Level::DEBUG, "KIP_DAEMON").entered();
                // Arc Clone reference to KipConf
                let daemon_cfg = Arc::clone(&cfg_file);
                let daemon_md = Arc::clone(&md_file);
                // Create background thread to poll backup
                // interval for all jobs
                tokio::spawn(async move {
                    // Duration of time to wait between each poll
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                    loop {
                        // Get KipConf each loop iteration as to not cause contention
                        // on the RwLock. Lock is dropped at end of each loop
                        let mut daemon_md = daemon_md.write().await;
                        // Check if backup needs to be run for all jobs
                        let _ = daemon_md.poll_backup_jobs(&daemon_cfg).await;
                        // Drop KipConf RwLock after check is done
                        drop(daemon_md);
                        // Wait 60 seconds, then loop again
                        interval.tick().await;
                    }
                });
            }
        }
    });
}

// Confirm correct secret from user input
fn confirm_secret(job_name: &str) -> String {
    let secret = Password::new()
        .with_prompt("Please provide your encryption secret")
        .interact()
        .expect("[ERR] failed to create encryption secret prompt.");
    let keyring_secret =
        match keyring_get_secret(format!("com.ciehanski.kip.{job_name}").trim_end()) {
            Ok(ks) => ks,
            Err(e) => {
                terminate!(
                    5,
                    "{} failed to get secret from keyring: {e}",
                    "[ERR]".red(),
                );
            }
        };
    if secret != keyring_secret {
        terminate!(1, "{} incorrect secret.", "[ERR]".red());
    };
    secret
}

#[cfg(not(windows))]
pub fn is_hidden(entry: &walkdir::DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}

#[cfg(windows)]
fn is_hidden(entry: &walkdir::DirEntry) -> bool {
    use std::os::windows::prelude::*;

    let file_path = entry.file_name().to_str().unwrap();
    let metadata = fs::metadata(file_path).unwrap();
    let attributes = metadata.file_attributes();
    if (attributes & 0x2) > 0 {
        true
    } else {
        false
    }
}

fn print_status(status: KipStatus) -> comfy_table::Cell {
    match status {
        KipStatus::OK => Cell::new("OK").fg(comfy_table::Color::Green),
        KipStatus::OK_SKIPPED => Cell::new("OK_SKIPPED").fg(comfy_table::Color::Green),
        KipStatus::ERR => Cell::new("ERR").fg(comfy_table::Color::Red),
        KipStatus::WARN => Cell::new("WARN").fg(comfy_table::Color::Yellow),
        KipStatus::IN_PROGRESS => Cell::new("IN_PROGRESS").fg(comfy_table::Color::Cyan),
        KipStatus::NEVER_RUN => Cell::new("NEVER_RUN").add_attribute(Attribute::Bold),
    }
}

fn check_battery() -> anyhow::Result<()> {
    if let Ok(manager) = battery::Manager::new() {
        match manager.batteries() {
            Ok(mut maybe_batteries) => {
                match maybe_batteries.next() {
                    Some(Ok(battery)) => {
                        // Convert batter ratio to f64
                        let charge = f64::from(
                            battery
                                .state_of_charge()
                                .get::<battery::units::ratio::ratio>(),
                        );
                        // Fail if battery level is at or below 20%
                        if charge < 0.20 {
                            anyhow::bail!(
                                "unable to run. your battery level needs to be above 20%."
                            )
                        }
                    }
                    Some(Err(e)) => {
                        anyhow::bail!("unable to gather battery information: {e}.");
                    }
                    None => { /* Do nothing if no battery detected */ }
                };
            }
            Err(e) => {
                anyhow::bail!("unable to gather battery information: {e}.");
            }
        };
    } else {
        anyhow::bail!("unable to gather battery information.")
    }
    Ok(())
}
