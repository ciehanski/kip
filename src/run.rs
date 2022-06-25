//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::chunk::{chunk_file, FileChunk};
use crate::job::{Job, KipStatus};
use crate::s3::{check_bucket, list_s3_bucket, s3_download, s3_upload};
use chrono::prelude::*;
use colored::*;
use crypto_hash::{hex_digest, Algorithm};
use humantime::format_duration;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::{create_dir_all, metadata, File, OpenOptions};
use std::io::prelude::*;
use std::io::Error as IOErr;
use std::io::ErrorKind;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Run {
    pub id: usize,
    pub started: DateTime<Utc>,
    pub time_elapsed: String,
    pub finished: DateTime<Utc>,
    pub bytes_uploaded: u64,
    pub files_changed: Vec<HashMap<String, FileChunk>>,
    pub status: KipStatus,
    pub logs: Vec<String>,
}

impl Run {
    pub fn new(id: usize) -> Self {
        Run {
            id,
            started: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            time_elapsed: String::from("0d 0h 0m 0s"),
            finished: Utc.ymd(1970, 1, 1).and_hms(0, 0, 0),
            bytes_uploaded: 0,
            files_changed: Vec::new(),
            status: KipStatus::NEVER_RUN,
            logs: Vec::<String>::new(),
        }
    }

    pub async fn upload(&mut self, job: &Job, secret: &str) -> Result<(), Box<dyn Error>> {
        // Set run metadata
        self.started = Utc::now();
        self.status = KipStatus::IN_PROGRESS;
        let (mut warn, mut err): (usize, usize) = (0, 0);
        let mut bytes_uploaded: u64 = 0;
        // TODO: create futures for each file iteration
        // and join at the end of this function to let
        // tokio handle thread management
        for f in job.files.iter() {
            // Check if file or directory exists
            if !f.path.exists() {
                warn += 1;
                let log = format!(
                    "[{}] {}-{} ⇉ '{}' can not be found.",
                    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                    job.name,
                    self.id,
                    f.path.display().to_string().red(),
                );
                self.logs.push(log.clone());
                println!("{}", log);
                continue;
            }
            // Check if f is file or directory
            let fmd = metadata(&f.path)?;
            if fmd.is_file() {
                // Get file hash and compare with stored file hash in
                // KipFile. If the same, continue the loop
                // Before opening file, determine how large it is
                // If larger than 100MB, mmap the sucker, if not, just read it
                let file = tokio::fs::read(&f.path).await?;
                let digest = hex_digest(Algorithm::SHA256, &file);
                // Check if all file chunks are already in S3
                // to avoid overwite and needless upload
                let chunks_map = chunk_file(&file);
                let hash_ok = f.hash == digest;
                let mut chunks_missing: usize = 0;
                for (chunk, _) in chunks_map.iter() {
                    let chunk_in_s3 =
                        check_bucket(&job.aws_bucket, job.aws_region.clone(), job.id, &chunk.hash)
                            .await?;
                    if !chunk_in_s3 {
                        chunks_missing += 1;
                    };
                }
                if hash_ok && chunks_missing == 0 {
                    let log = format!(
                        "[{}] {}-{} ⇉ '{}' skipped, no changes found.",
                        Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                        job.name,
                        self.id,
                        f.path.display().to_string().yellow(),
                    );
                    self.logs.push(log.clone());
                    println!("{}", log);
                    continue;
                }
                // Upload
                match s3_upload(
                    &f.path.canonicalize()?,
                    chunks_map,
                    fmd.len(),
                    job.id,
                    job.aws_bucket.clone(),
                    job.aws_region.clone(),
                    secret,
                )
                .await
                {
                    Ok(chunked_file) => {
                        // Confirm the chunked file is not empty AKA
                        // this chunk has not been modified, skip it
                        if !chunked_file.is_empty() {
                            // Increase bytes uploaded for this run
                            bytes_uploaded += fmd.len();
                            // Push chunked file onto run's changed files
                            self.files_changed.push(chunked_file);
                            // Push logs
                            let log = format!(
                                "[{}] {}-{} ⇉ '{}' uploaded successfully to '{}'.",
                                Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                job.name,
                                self.id,
                                f.path.display().to_string().green(),
                                job.aws_bucket.clone(),
                            );
                            self.logs.push(log.clone());
                            println!("{}", log);
                        }
                    }
                    Err(e) => {
                        // Set run status to ERR
                        err += 1;
                        // Push logs
                        let log = format!(
                            "[{}] {}-{} ⇉ '{}' upload failed: {}.",
                            Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                            job.name,
                            self.id,
                            f.path.display().to_string().red(),
                            e,
                        );
                        self.logs.push(log.clone());
                        println!("{}", log);
                    }
                };
            } else if fmd.is_dir() {
                // If the listed file entry is a dir, use walkdir to
                // walk all the recursive directories as well. Upload
                // all files found within the directory.
                for entry in WalkDir::new(&f.path) {
                    let entry = entry?;
                    // If a directory, skip since upload will
                    // create the parent folder by default
                    let fmd = metadata(entry.path())?;
                    if fmd.is_dir() {
                        continue;
                    }
                    // Get file hash and compare with stored file hash in
                    // KipFile. If the same, continue the loop
                    let file = tokio::fs::read(&f.path).await?;
                    let digest = hex_digest(Algorithm::SHA256, &file);
                    // Check if all file chunks are already in S3
                    // to avoid overwite and needless upload
                    let chunks_map = chunk_file(&file);
                    let hash_ok = f.hash == digest;
                    let mut chunks_missing: usize = 0;
                    for (chunk, _) in chunks_map.iter() {
                        let chunk_in_s3 = check_bucket(
                            &job.aws_bucket,
                            job.aws_region.clone(),
                            job.id,
                            &chunk.hash,
                        )
                        .await?;
                        if !chunk_in_s3 {
                            chunks_missing += 1;
                        };
                    }
                    if hash_ok && chunks_missing == 0 {
                        let log = format!(
                            "[{}] {}-{} ⇉ '{}' skipped, no changes found.",
                            Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                            job.name,
                            self.id,
                            f.path.display().to_string().yellow(),
                        );
                        self.logs.push(log.clone());
                        println!("{}", log);
                        continue;
                    }
                    // Upload
                    match s3_upload(
                        &entry.path().canonicalize()?,
                        chunks_map,
                        fmd.len(),
                        job.id,
                        job.aws_bucket.clone(),
                        job.aws_region.clone(),
                        secret,
                    )
                    .await
                    {
                        Ok(chunked_file) => {
                            // Confirm the chunked file is not empty
                            if !chunked_file.is_empty() {
                                // Increase bytes uploaded for this run
                                bytes_uploaded += fmd.len();
                                // Push chunked file onto run's changed files
                                self.files_changed.push(chunked_file);
                                // Push logs
                                let log = format!(
                                    "[{}] {}-{} ⇉ '{}' uploaded successfully to '{}'.",
                                    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                    job.name,
                                    self.id,
                                    entry.path().display().to_string().green(),
                                    job.aws_bucket.clone(),
                                );
                                self.logs.push(log.clone());
                                println!("{}", log);
                            }
                        }
                        Err(e) => {
                            // Set run status to ERR
                            err += 1;
                            // Push logs
                            let log = format!(
                                "[{}] {}-{} ⇉ '{}' upload failed: {}.",
                                Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                job.name,
                                self.id,
                                entry.path().display().to_string().red(),
                                e,
                            );
                            self.logs.push(log.clone());
                            println!("{}", log);
                        }
                    };
                }
            }
        }
        // Set run metadata
        self.finished = Utc::now();
        let dur = self.finished.signed_duration_since(self.started).to_std()?;
        self.time_elapsed = format_duration(dur).to_string();
        self.bytes_uploaded = bytes_uploaded;
        if err == 0 && warn == 0 {
            self.status = KipStatus::OK;
        } else if warn > 0 && err == 0 {
            self.status = KipStatus::WARN;
        } else {
            self.status = KipStatus::ERR;
        }
        Ok(())
    }

    pub async fn restore(
        &self,
        job: &Job,
        secret: &str,
        output_folder: Option<String>,
    ) -> Result<(), Box<dyn Error>> {
        // Get all bucket contents
        let bucket_objects =
            list_s3_bucket(&job.aws_bucket, job.aws_region.clone(), job.id).await?;
        // Get output folder
        let output_folder = output_folder.unwrap_or_default();
        let dmd = metadata(&output_folder)?;
        if !dmd.is_dir() || output_folder == "" {
            return Err(Box::new(IOErr::new(
                ErrorKind::InvalidData,
                "path provided is not a directory.",
            )));
        }
        // For each object in the bucket, download it
        let mut counter: usize = 0;
        'files: for fc in self.files_changed.iter() {
            // If file has multiple chunks, store them
            // here for re-assembly later
            let mut multi_chunks: Vec<(&FileChunk, Vec<u8>)> = vec![];
            // Check if this S3 object is within this run's changed files
            's3: for ob in bucket_objects.iter() {
                let s3_key = match &ob.key {
                    Some(k) => k,
                    _ => {
                        return Err(Box::new(IOErr::new(
                            ErrorKind::NotFound,
                            "unable to get bucket object's S3 path.",
                        )))
                    }
                };
                // Strip the hash from S3 key
                let hash = strip_hash_from_s3(&s3_key)?;
                if !fc.contains_key(&hash) {
                    // S3 object not found in this run
                    continue 's3;
                } else {
                    // Found a chunk for this file! Download
                    // this chunk
                    let local_path = match fc.get(&hash) {
                        Some(c) => c.local_path.display().to_string().green(),
                        _ => {
                            return Err(Box::new(IOErr::new(
                                ErrorKind::NotFound,
                                "unable to get chunk's local path.",
                            )))
                        }
                    };
                    // Store the returned downloaded and decrypted bytes for chunk
                    // TODO: testing
                    // let mut chunk_bytes: Vec<u8> = vec![];
                    // Download chunk
                    match s3_download(&s3_key, &job.aws_bucket, job.aws_region.clone(), secret)
                        .await
                    {
                        Ok(chunk_bytes) => {
                            // Determine if single or multi-chunk file
                            for (_, chunk) in fc.iter() {
                                if self.is_single_chunk(chunk) {
                                    // Increment file resote counter
                                    counter += 1;
                                    // If a single-chunk file, simply decrypt and write
                                    let mut cfile = create_file(&chunk.local_path, &output_folder)?;
                                    cfile.write_all(&chunk_bytes)?;
                                    println!(
                                        "[{}] {}-{} ⇉ '{}' restored successfully. ({}/{})",
                                        Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                        job.name,
                                        self.id,
                                        local_path,
                                        counter,
                                        self.files_changed.len(),
                                    );
                                    continue 'files;
                                } else {
                                    // If a multi-chunk file pass to multi-chunk vec
                                    // for re-assembly after loop finishes
                                    // TODO: I don't like chunk_bytes.clone() here
                                    multi_chunks.push((chunk, chunk_bytes.clone()));
                                }
                            }
                            // Here, we create a vec of all chunks associated with larger,
                            // multi-chunk files. Loops until all files and their chunks
                            // have been sorted by offset and len and written to disk.
                            // let mut mc_len: usize = multi_chunks.len();
                            // while mc_len != 0 {
                            //     let mut same_file_chunks = vec![];
                            //     for (i, c) in multi_chunks.iter().enumerate() {
                            //         if i == multi_chunks.len() - 1 || mc_len == 0 {
                            //             break;
                            //         }
                            //         if c.0.local_path == multi_chunks[i + 1].0.local_path {
                            //             same_file_chunks.push(*c);
                            //             mc_len -= 1;
                            //         }
                            //     }
                            //     // Here, we sort all the chunks by thier offset and
                            //     // combine the chunks and write the file
                            //     assemble_chunks(same_file_chunks, &output_folder)?;
                            // }
                        }
                        Err(e) => {
                            eprintln!(
                                "[{}] {}-{} ⇉ '{}' restore failed: {}. ({}/{})",
                                Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                job.name,
                                self.id,
                                local_path,
                                e,
                                counter,
                                self.files_changed.len(),
                            );
                        }
                    }
                }
            }
            // Only run if multi_chunks is not empty
            if !multi_chunks.is_empty() {
                // All chunks found for file, autobots, assemble!
                // Increment file resote counter
                counter += 1;
                // Get local path for logs
                let local_path = &multi_chunks[0].0.local_path.clone();
                // Here, we sort all the chunks by thier offset and
                // combine the chunks and write the file
                assemble_chunks(multi_chunks, local_path, &output_folder)?;
                // Print logs
                println!(
                    "[{}] {}-{} ⇉ '{}' restored successfully. ({}/{})",
                    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                    job.name,
                    self.id,
                    local_path.display().to_string().green(),
                    counter,
                    self.files_changed.len(),
                );
            }
        }
        Ok(())
    }

    // Checks if a certain file backed up in a specific run
    // was split into a single or multiple chunks.
    fn is_single_chunk(&self, fc: &FileChunk) -> bool {
        let mut count: usize = 0;
        for cf in self.files_changed.iter().into_iter() {
            for c in cf.values() {
                if c.local_path == fc.local_path {
                    count += 1;
                }
            }
        }
        if count > 1 {
            return false;
        }
        true
    }
}

// Find all chunks associated with the same path
// and combine them in order according to their offsets and
// lengths and then save to the original local path
fn assemble_chunks(
    mut chunks: Vec<(&FileChunk, Vec<u8>)>,
    local_path: &PathBuf,
    output_folder: &str,
) -> Result<(), Box<dyn Error>> {
    // Create file
    let mut cfile = create_file(local_path, output_folder)?;
    // Order the chunks by offset
    chunks.sort_by(|a, b| a.0.offset.cmp(&b.0.offset));
    // Write the file
    for (chunk, chunk_bytes) in chunks {
        cfile.seek(SeekFrom::Start(chunk.offset as u64))?;
        cfile.write_all(&chunk_bytes)?;
    }
    Ok(())
}

fn create_file(path: &Path, output_folder: &str) -> Result<File, Box<dyn Error>> {
    // Only strip prefix if path has a prefix
    let mut correct_chunk_path = path;
    if path.starts_with("/") {
        correct_chunk_path = path.strip_prefix("/")?;
    }
    let folder_path = Path::new(&output_folder).join(correct_chunk_path);
    let folder_parent = match folder_path.parent() {
        Some(p) => p,
        _ => &folder_path,
    };
    create_dir_all(folder_parent)?;
    // Create the file
    let cfile = if !Path::new(&output_folder).join(correct_chunk_path).exists() {
        File::create(Path::new(&output_folder).join(correct_chunk_path))?
    } else {
        OpenOptions::new()
            .write(true)
            .open(Path::new(&output_folder).join(correct_chunk_path))?
    };
    Ok(cfile)
}

pub fn strip_hash_from_s3(s3_path: &str) -> Result<String, IOErr> {
    // Pop hash off from S3 path
    let mut fp: Vec<_> = s3_path.split('/').collect();
    let hdt = match fp.pop() {
        Some(h) => h,
        _ => {
            return Err(IOErr::new(
                ErrorKind::UnexpectedEof,
                "failed to pop chunk's S3 path.",
            ))
        }
    };
    // Split the chunk. Ex: 902938470293847392033874592038473.chunk
    let hs: Vec<_> = hdt.split('.').collect();
    // Just grab the first split, which is the hash
    let hash = hs[0].to_string();
    // Ship it
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::chunk_file;
    use std::fs::read;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn test_is_single_chunk() {
        let mut r = Run::new(9998);
        let content_result = read(&PathBuf::from("test/random.txt"));
        assert!(content_result.is_ok());
        let contents = content_result.unwrap();
        let chunk_hmap = chunk_file(&contents);
        let mut t = HashMap::new();
        let mut fc = FileChunk::new("", 0, 0, 0);
        for c in chunk_hmap {
            t.insert(c.0.hash.clone(), c.0.clone());
            fc = c.0;
        }
        r.files_changed.push(t);
        assert!(r.is_single_chunk(&fc))
    }

    #[test]
    fn test_is_not_single_chunk() {
        let mut r = Run::new(9999);
        let content_result = read(&PathBuf::from("test/kip"));
        assert!(content_result.is_ok());
        let contents = content_result.unwrap();
        let chunk_hmap = chunk_file(&contents);
        let mut t = HashMap::new();
        let mut fc = FileChunk::new("", 0, 0, 0);
        for c in chunk_hmap {
            t.insert(c.0.hash.clone(), c.0.clone());
            fc = c.0;
        }
        r.files_changed.push(t);
        assert!(!r.is_single_chunk(&fc))
    }

    #[test]
    fn test_strip_hash_from_s3() {
        // Split the 902938470293847392033874592038473.chunk
        let hash = strip_hash_from_s3("f339aae7-e994-4fb4-b6aa-623681df99aa/chunks/001d46082763b930e5b9f0c52d16841b443bfbcd52af6cd475cb0182548da33a.chunk").unwrap();
        assert_eq!(
            hash,
            "001d46082763b930e5b9f0c52d16841b443bfbcd52af6cd475cb0182548da33a"
        )
    }

    #[test]
    fn test_assemble_chunks() {
        use rand::seq::SliceRandom;
        use rand::thread_rng;

        // Chunk test file
        let read_result = read(&PathBuf::from("test/kip"));
        assert!(read_result.is_ok());
        let contents = read_result.unwrap();
        let chunks = chunk_file(&contents);
        // Create temp dir for testing
        let tmp_dir = tempdir();
        assert!(tmp_dir.is_ok());
        let tmp_dir = tmp_dir.unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // Convert chunks from HashMap to Vec for reassembly
        let mut multi_chunks: Vec<(&FileChunk, Vec<u8>)> = vec![];
        for (chunk, chunk_bytes) in chunks.iter().clone() {
            multi_chunks.push((chunk, chunk_bytes.to_vec()));
        }
        assert!(multi_chunks.len() > 1);
        // Shuffle multi_chunks to resemble real life scenario better
        // IRL, chunks will not be in order
        multi_chunks.shuffle(&mut thread_rng());
        // Time to assemble
        let result = assemble_chunks(multi_chunks, &PathBuf::from("kip"), dir);
        assert!(result.is_ok());
        // Compare restored file with original
        let test_result = read(tmp_dir.path().join("kip"));
        assert!(test_result.is_ok());
        let test_contents = test_result.unwrap();
        assert_eq!(contents, test_contents);
        // Destroy temp dir
        let dir_result = tmp_dir.close();
        assert!(dir_result.is_ok())
    }

    #[test]
    fn test_create_file() {
        // Create temp dir for testing
        let tmp_dir = tempdir();
        assert!(tmp_dir.is_ok());
        let tmp_dir = tmp_dir.unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // Create file
        let result = create_file(&PathBuf::from("test.txt"), dir);
        assert!(result.is_ok());
        let test_result = read(tmp_dir.path().join("test.txt"));
        assert!(test_result.is_ok());
        let exists = Path::new(&tmp_dir.path().join("test.txt")).exists();
        assert_eq!(exists, true);
        // Destroy temp dir
        let dir_result = tmp_dir.close();
        assert!(dir_result.is_ok())
    }

    #[test]
    fn test_create_file_is_dir() {
        // Create temp dir for testing
        let tmp_dir = tempdir();
        assert!(tmp_dir.is_ok());
        let tmp_dir = tmp_dir.unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // Create file
        let result = create_file(&PathBuf::from("test/"), dir);
        assert!(result.is_err());
        // Destroy temp dir
        let dir_result = tmp_dir.close();
        assert!(dir_result.is_ok())
    }

    #[test]
    fn test_create_file_no_prefix() {
        // Create temp dir for testing
        let tmp_dir = tempdir();
        assert!(tmp_dir.is_ok());
        let tmp_dir = tmp_dir.unwrap();
        // Create file
        let path = &PathBuf::from("no_prefix/test.txt");
        let stripped_path = tmp_dir.path().strip_prefix("/");
        assert!(stripped_path.is_ok());
        let stripped_path = stripped_path.unwrap().display().to_string();
        let file_result = create_file(path, &stripped_path);
        assert!(file_result.is_ok());
        let exists_result = file_result.unwrap().metadata();
        assert!(exists_result.is_ok());
        let exists = exists_result.unwrap().is_file();
        assert_eq!(exists, true);
        // Destroy temp dir
        let dir_result = tmp_dir.close();
        assert!(dir_result.is_ok())
    }

    #[test]
    fn test_create_file_prefix() {
        // Create temp dir for testing
        let tmp_dir = tempdir();
        assert!(tmp_dir.is_ok());
        let tmp_dir = tmp_dir.unwrap();
        // Create file
        let path = &PathBuf::from("/prefix/test.txt");
        let file_result = create_file(path, &tmp_dir.path().display().to_string());
        assert!(file_result.is_ok());
        let exists_result = file_result.unwrap().metadata();
        assert!(exists_result.is_ok());
        let exists = exists_result.unwrap().is_file();
        assert_eq!(exists, true);
        // Destroy temp dir
        let dir_result = tmp_dir.close();
        assert!(dir_result.is_ok())
    }
}
