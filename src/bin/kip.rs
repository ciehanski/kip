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
use kip::conf::KipConf;
use kip::crypto::{keyring_get_secret, keyring_set_secret};
use kip::job::{Job, KipFile, KipStatus};
use kip::providers::{gdrive::KipGdrive, s3::KipS3, usb::KipUsb, KipProviders};
use kip::terminate;
use pretty_bytes::converter::convert;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use sysinfo::{DiskExt, System, SystemExt};
use tokio::runtime::Builder;

fn main() {
    // Get config and metadata file
    let (cfg_file, md_file) = KipConf::new().unwrap_or_else(|e| {
        terminate!(
            5,
            "{} failed to get kip configuration: {}.",
            "[ERR]".red(),
            e
        );
    });
    let cfg = &*Arc::clone(&cfg_file);
    let md = Arc::clone(&md_file);

    // Custom Tokio Runtime
    let runtime = Builder::new_current_thread()
        .worker_threads(cfg.settings.worker_threads)
        .thread_name("kip")
        .enable_all()
        .build()
        .unwrap_or_else(|e| {
            terminate!(11, "unable to initialize kip runtime: {}.", e);
        });

    // Get subcommands and args
    let args = Cli::parse();
    let _debug = args.debug;

    runtime.block_on(async {
        // Match user input command
        match args.subcommands {
            // Create a new job
            Subcommands::Init { job } => {
                let mut md = md.write().await;
                // Ensure that job does not already exist with
                // the provided name.
                for (j, _) in md.jobs.iter() {
                    if j == &job {
                        terminate!(17, "{} job '{}' already exists.", "[ERR]".red(), job);
                    }
                }
                // Get secret from user input
                let secret = Password::new()
                    .with_prompt("Please provide your encryption secret")
                    .interact()
                    .expect("[ERR] failed to create encryption secret prompt.");
                // Store secret onto local OS keyring
                keyring_set_secret(&format!("com.ciehanski.kip.{}", &job), &secret).unwrap_or_else(
                    |e| {
                        terminate!(
                            5,
                            "{} failed to push secret onto keyring: {}.",
                            "[ERR]".red(),
                            e
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
                            &format!("com.ciehanski.kip.{}.s3acc", &job),
                            &s3_acc_key,
                        )
                        .unwrap_or_else(|e| {
                            terminate!(
                                5,
                                "{} failed to push S3 access key onto keyring: {}.",
                                "[ERR]".red(),
                                e
                            );
                        });
                        // Get S3 secret key from user input
                        print!("Please provide the S3 secret key: ");
                        std::io::stdout()
                            .flush()
                            .expect("[ERR] failed to flush stdout.");
                        let mut s3_sec_key = String::new();
                        std::io::stdin()
                            .read_line(&mut s3_sec_key)
                            .expect("[ERR] failed to read S3 secret key from stdin.");
                        // Store S3 secret key onto local OS keyring
                        keyring_set_secret(
                            &format!("com.ciehanski.kip.{}.s3sec", &job),
                            &s3_sec_key,
                        )
                        .unwrap_or_else(|e| {
                            terminate!(
                                5,
                                "{} failed to push S3 secret key onto keyring: {}.",
                                "[ERR]".red(),
                                e
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
                        let new_job = Job::new(&job, provider);
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
                            &format!("com.ciehanski.kip.{}.gdriveid", &job),
                            &gdrive_client_id,
                        )
                        .unwrap_or_else(|e| {
                            terminate!(
                                5,
                                "{} failed to push Google Drive client ID onto keyring: {}.",
                                "[ERR]".red(),
                                e
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
                            &format!("com.ciehanski.kip.{}.gdrivesec", &job),
                            &gdrive_client_sec,
                        )
                        .unwrap_or_else(|e| {
                            terminate!(
                                5,
                                "{} failed to push Google Drive client ID onto keyring: {}.",
                                "[ERR]".red(),
                                e
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
                        let new_job = Job::new(&job, provider);
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
                                .expect("[ERR] unable to create USB selection menu");
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
                        let new_job = Job::new(&job, provider);
                        // Push new job in config
                        md.jobs.insert(job.clone(), new_job);
                    }
                    _ => {
                        terminate!(1, "Invalid selection. Please try again.");
                    }
                }
                // Store new job in config
                match md.save() {
                    Ok(_) => println!("{} job '{}' successfully created.", "[OK]".green(), job),
                    Err(e) => terminate!(
                        7,
                        "{} failed to save kip configuration: {}",
                        "[ERR]".red(),
                        e
                    ),
                }
            }

            // Add more files or directories to job
            Subcommands::Add { job, file_path } => {
                let mut md = md.write().await;
                // Check if file or directory exists
                for f in &file_path {
                    if !Path::new(f).exists() {
                        terminate!(2, "{} '{}' doesn't exist.", "[ERR]".red(), f);
                    }
                }
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
                });
                // Check if files are excluded by this job
                for jf in &j.excluded_files {
                    for f in &file_path {
                        if jf
                            == &Path::new(f)
                                .canonicalize()
                                .expect("[ERR] unable to canonicalize path.")
                        {
                            terminate!(
                                17,
                                "{} file(s) are excluded on job '{}'.",
                                "[ERR]".red(),
                                &job
                            );
                        }
                    }
                }
                // Check if files already exist on job
                // to avoid duplication.
                for jf in &j.files {
                    for f in &file_path {
                        if jf.path
                            == Path::new(f)
                                .canonicalize()
                                .expect("[ERR] unable to canonicalize path.")
                        {
                            terminate!(
                                17,
                                "{} file(s) already exist on job '{}'.",
                                "[ERR]".red(),
                                &job
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
                    ));
                }
                // Get new files amount for job
                j.files_amt = j.get_files_amt().unwrap_or_else(|e| {
                    terminate!(
                        6,
                        "{} failed to get file amounts for {}: {}.",
                        "[ERR]".red(),
                        &job,
                        e
                    );
                });
                // Save changes to config file
                match md.save() {
                    Ok(_) => println!(
                        "{} files successfully added to job '{}'.",
                        "[OK]".green(),
                        &job
                    ),
                    Err(e) => terminate!(
                        7,
                        "{} failed to save kip configuration: {}",
                        "[ERR]".red(),
                        e
                    ),
                }
            }

            // Remove files from a job
            Subcommands::Remove {
                job,
                file_path,
                purge,
            } => {
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
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
                                            "{} failed to remove files from S3 for {}: {}.",
                                            "[ERR]".red(),
                                            &job,
                                            e
                                        );
                                    });
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
                        j.files_amt = j.get_files_amt().unwrap_or_else(|e| {
                            terminate!(
                                6,
                                "{} failed to get file amounts for {}: {}.",
                                "[ERR]".red(),
                                &job,
                                e
                            );
                        });
                        // Success
                        println!(
                            "{} files successfully removed from job '{}'.",
                            "[OK]".green(),
                            &job
                        );
                    }
                    None => {
                        // Remove job's keyring entries
                        j.delete_keyring_entries()
                            // -f and -r were not provided, delete the job
                            .expect("unable to delete keyring entries for job");
                        md.jobs.remove(&job);
                        println!("{} job '{}' successfully removed.", "[OK]".green(), &job)
                    }
                }
                // Save changes to config file
                match md.save() {
                    Ok(_) => {}
                    Err(e) => {
                        terminate!(
                            7,
                            "{} failed to save kip configuration: {}",
                            "[ERR]".red(),
                            e
                        );
                    }
                }
            }

            // Excludes files or directories from a job
            Subcommands::Exclude {
                job,
                file_path,
                extensions,
            } => {
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
                });
                // Confirm correct secret from user input
                confirm_secret(&j.name);
                // Confirm if files or exentions were provided
                if let Some(fp) = file_path {
                    // Check if file or directory exists
                    for f in &fp {
                        if !Path::new(f).exists() {
                            terminate!(2, "{} '{}' doesn't exist.", "[ERR]".red(), f);
                        }
                    }
                    // Check if this file is being backed up by the job
                    // cancel excluding it if so
                    for jf in &j.files {
                        for f in &fp {
                            let f_path = Path::new(f)
                                .canonicalize()
                                .expect("[ERR] unable to canonicalize path.");
                            if jf.path == f_path {
                                terminate!(
                                    17,
                                    "{} unable to exclude files that are being backed up by '{}'.",
                                    "[ERR]".red(),
                                    &job
                                );
                            }
                        }
                    }
                    // Check if files are already excluded from job
                    // to avoid duplication.
                    for jf in &j.excluded_files {
                        for f in &fp {
                            let jf_path = Path::new(jf)
                                .canonicalize()
                                .expect("[err] unable to canonicalize path.");
                            let f_path = Path::new(f)
                                .canonicalize()
                                .expect("[err] unable to canonicalize path.");
                            if jf_path == f_path {
                                terminate!(
                                    17,
                                    "{} file(s) already excluded from job '{}'.",
                                    "[err]".red(),
                                    &job
                                );
                            }
                        }
                    }
                    // Push exclusions to job
                    for f in fp {
                        j.excluded_files.push(
                            PathBuf::from(f)
                                .canonicalize()
                                .expect("[ERR] unable to canonicalize path."),
                        );
                    }
                } else if let Some(e) = extensions {
                    // Check if extensions are already excluded from job
                    // to avoid duplication.
                    for jext in &j.excluded_file_types {
                        for ext in &e {
                            if jext == ext {
                                terminate!(
                                    17,
                                    "{} file(s) already excluded from job '{}'.",
                                    "[err]".red(),
                                    &job
                                );
                            }
                        }
                    }
                    // Push excluded extensions to job
                    for ext in e {
                        j.excluded_file_types.push(ext);
                    }
                } else {
                    terminate!(99, "no file path or extensions provided");
                }
                // Save changes to config file
                match md.save() {
                    Ok(_) => println!("{} exclusions added to job '{}'.", "[OK]".green(), &job),
                    Err(e) => terminate!(
                        7,
                        "{} failed to save kip configuration: {}",
                        "[ERR]".red(),
                        e
                    ),
                }
            }

            // Start a job's upload
            Subcommands::Push { job } => {
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
                });
                // Get new files amount for job
                j.files_amt = j.get_files_amt().unwrap_or_else(|e| {
                    terminate!(
                        6,
                        "{} failed to get file amounts for {}: {}.",
                        "[ERR]".red(),
                        &job,
                        e
                    );
                });
                // Confirm correct secret from user input
                let secret = confirm_secret(&j.name);
                // Upload all files in a seperate thread
                match j.start_run(&secret).await {
                    Ok(_) => {}
                    Err(e) => terminate!(
                        8,
                        "{} job '{}' upload to '{}' failed: {}",
                        "[ERR]".red(),
                        &job,
                        &j.provider.s3().unwrap().aws_bucket,
                        e
                    ),
                }
                // Save changes to config file
                md.save().unwrap_or_else(|e| {
                    terminate!(
                        7,
                        "{} failed to save kip configuration: {}",
                        "[ERR]".red(),
                        e
                    );
                });
            }

            // Start a backup restore
            Subcommands::Pull {
                job,
                run,
                output_folder,
            } => {
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
                });
                // Confirm correct secret from user input
                let secret = confirm_secret(&j.name);
                // Get output folder
                let output_folder = output_folder.unwrap_or_else(|| {
                    terminate!(2, "{} invalid output folder provided.", "[ERR]".red());
                });
                let dmd = std::fs::metadata(&output_folder).unwrap();
                if !dmd.is_dir() || output_folder.is_empty() {
                    terminate!(
                        2,
                        "{} output folder provided is not a valid directory.",
                        "[ERR]".red()
                    );
                }
                // Run the restore
                match j.start_restore(run, &secret, &output_folder).await {
                    Ok(_) => {
                        println!("{} job '{}' restored successfully.", "[OK]".green(), &job,);
                    }
                    Err(e) => {
                        terminate!(2, "{} job '{}' restore failed: {}", "[ERR]".red(), &job, e);
                    }
                };
                // Save changes to config file
                md.save().unwrap_or_else(|e| {
                    terminate!(
                        7,
                        "{} failed to save kip configuration: {}",
                        "[ERR]".red(),
                        e
                    );
                });
            }

            // Pauses a job and future runs
            Subcommands::Pause { job } => {
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
                });
                // Confirm correct secret from user input
                let _ = confirm_secret(&j.name);
                // Set job to paused
                j.paused = true;
                // Save changes to config file
                md.save().unwrap_or_else(|e| {
                    terminate!(
                        7,
                        "{} failed to save kip configuration: {}",
                        "[ERR]".red(),
                        e
                    );
                });
            }

            // Resumes a job and future runs
            Subcommands::Resume { job } => {
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
                    terminate!(2, "{} job '{}' doesn't exist.", "[ERR]".red(), &job);
                });
                // Confirm correct secret from user input
                let secret = confirm_secret(&j.name);
                // Set set to !paused
                j.paused = false;
                // Run a manual upload
                match j.start_run(&secret).await {
                    Ok(_) => {}
                    Err(e) => terminate!(
                        8,
                        "{} job '{}' upload to '{}' failed: {}",
                        "[ERR]".red(),
                        &job,
                        &j.provider.s3().unwrap().aws_bucket,
                        e
                    ),
                }
                // Save changes to config file
                md.save().unwrap_or_else(|e| {
                    terminate!(
                        7,
                        "{} failed to save kip configuration: {}",
                        "[ERR]".red(),
                        e
                    );
                });
            }

            // Abort a running job
            Subcommands::Abort { job } => {
                let mut md = md.write().await;
                // Get job from argument provided
                let j = md.jobs.get_mut(&job).unwrap_or_else(|| {
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

            // List all jobs
            // This function is messy. Should probably cleanup.
            Subcommands::Status { job, run } => {
                let md = md.read().await;
                // Create the table
                let mut table = Table::new();
                table
                    .load_preset(UTF8_FULL)
                    .apply_modifier(UTF8_ROUND_CORNERS)
                    .set_content_arrangement(ContentArrangement::Dynamic);
                if job == None && run == None {
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
                } else if job != None && run == None {
                    let j = md.jobs.get(job.as_ref().unwrap()).unwrap_or_else(|| {
                        terminate!(
                            2,
                            "{} job '{}' doesn't exist.",
                            "[ERR]".red(),
                            &job.clone().unwrap()
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
                                "Google Drive Folder ID",
                                "Selected Files",
                                "Total Runs",
                                "Last Run",
                                "Bytes (Provider)",
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
                } else if job != None && run != None {
                    let j = md.jobs.get(job.as_ref().unwrap()).unwrap_or_else(|| {
                        terminate!(
                            2,
                            "{} job '{}' doesn't exist.",
                            "[ERR]".red(),
                            &job.clone().unwrap()
                        );
                    });
                    let r = j.runs.get(&run.unwrap()).unwrap_or_else(|| {
                        terminate!(
                            2,
                            "{} run '{}' doesn't exist for job '{}'.",
                            "[ERR]".red(),
                            &run.unwrap(),
                            &job.clone().unwrap(),
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
                                "Google Drive Folder ID",
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
                // Arc Clone reference to KipConf
                let daemon_cfg = Arc::clone(&cfg_file);
                let daemon_md = Arc::clone(&md_file);
                // Create background thread to poll backup
                // interval for all jobs
                tokio::spawn(async move {
                    // Duration of time to wait between each poll
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                    loop {
                        // Get KipConf each loop iteration as to not cause
                        // contention on the RwLock. Lock is dropped at end
                        // of each loop
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
        match keyring_get_secret(format!("com.ciehanski.kip.{}", job_name).trim_end()) {
            Ok(ks) => ks,
            Err(e) => {
                terminate!(
                    5,
                    "{} failed to get secret from keyring: {}",
                    "[ERR]".red(),
                    e
                );
            }
        };
    if secret != keyring_secret {
        terminate!(1, "{} incorrect secret.", "[ERR]".red());
    };
    secret
}

fn print_status(status: KipStatus) -> comfy_table::Cell {
    match status {
        KipStatus::OK => Cell::new("OK").fg(comfy_table::Color::Green),
        KipStatus::ERR => Cell::new("ERR").fg(comfy_table::Color::Red),
        KipStatus::WARN => Cell::new("WARN").fg(comfy_table::Color::Yellow),
        KipStatus::IN_PROGRESS => Cell::new("IN_PROGRESS").fg(comfy_table::Color::Cyan),
        KipStatus::NEVER_RUN => Cell::new("NEVER_RUN").add_attribute(Attribute::Bold),
    }
}
