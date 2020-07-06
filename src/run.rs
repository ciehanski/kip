//
// Copyright (c) 2020 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::chunk::FileChunk;
use crate::job::{Job, KipStatus};
use crate::s3::{list_s3_bucket, s3_download, s3_upload};
use chrono::prelude::*;
use colored::*;
use crypto_hash::{hex_digest, Algorithm};
use humantime::format_duration;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::{metadata, File, OpenOptions};
use std::io::prelude::*;
use std::path::Path;
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
        // TODO: S3 by default will allow 10 concurrent requests
        // via ThreadPool I guess?
        let mut warn: usize = 0;
        let mut err: usize = 0;
        let mut bytes_uploaded: u64 = 0;
        for f in job.files.iter() {
            // Check if file or directory exists
            if !f.path.exists() {
                warn += 1;
                self.logs.push(format!(
                    "[{}] {}-{} ⇉ '{}' can not be found.",
                    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                    job.name,
                    self.id,
                    f.path.display().to_string().red(),
                ));
                continue;
            }
            // Check if f is file or directory
            let fmd = metadata(&f.path)?;
            if fmd.is_file() {
                // Get file hash and compare with stored file hash in
                // KipFile. If the same, continue the loop
                let c = std::fs::read(&f.path)?.to_vec();
                let digest = hex_digest(Algorithm::SHA256, &c);
                if f.hash == digest {
                    continue;
                }
                // Upload
                match s3_upload(
                    &f.path.canonicalize()?,
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
                            self.logs.push(format!(
                                "[{}] {}-{} ⇉ '{}' uploaded successfully to '{}'.",
                                Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                job.name,
                                self.id,
                                f.path.display().to_string().green(),
                                job.aws_bucket.clone(),
                            ));
                        }
                    }
                    Err(e) => {
                        // Set run status to ERR
                        err += 1;
                        // Push logs
                        self.logs.push(format!(
                            "[{}] {}-{} ⇉ '{}' upload failed: {}.",
                            Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                            job.name,
                            self.id,
                            f.path.display().to_string().red(),
                            e,
                        ));
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
                    let c = std::fs::read(entry.path())?.to_vec();
                    let digest = hex_digest(Algorithm::SHA256, &c);
                    if f.hash == digest {
                        continue;
                    }
                    // Upload
                    match s3_upload(
                        &entry.path().canonicalize()?,
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
                                self.logs.push(format!(
                                    "[{}] {}-{} ⇉ '{}' uploaded successfully to '{}'.",
                                    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                    job.name,
                                    self.id,
                                    entry.path().display().to_string().green(),
                                    job.aws_bucket.clone(),
                                ));
                            }
                        }
                        Err(e) => {
                            // Set run status to ERR
                            err += 1;
                            // Push logs
                            self.logs.push(format!(
                                "[{}] {}-{} ⇉ '{}' upload failed: {}.",
                                Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                job.name,
                                self.id,
                                entry.path().display().to_string().red(),
                                e,
                            ));
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
        let bucket_objects = list_s3_bucket(&job.aws_bucket, job.aws_region.clone()).await?;
        // Get output folder
        let output_folder = output_folder.unwrap_or_default();
        // For each object in the bucket, download it
        let mut counter: usize = 0;
        'outer: for fc in self.files_changed.iter() {
            // Check if S3 object is within this run's changed files
            for ob in bucket_objects.iter() {
                let s3_key = match ob.key.clone() {
                    Some(k) => k,
                    _ => {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "unable to get bucket object's S3 path.",
                        )))
                    }
                };
                let hash = strip_hash_from_s3(&s3_key)?;
                if !fc.contains_key(&hash) {
                    // S3 object not found in this run
                    continue;
                } else {
                    // Found! Download this chunk
                    // Increment file counter
                    counter += 1;
                    let local_path = match fc.get(&hash) {
                        Some(c) => c.local_path.display().to_string().green(),
                        _ => {
                            return Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::NotFound,
                                "unable to get chunk's local path.",
                            )))
                        }
                    };
                    // Store the returned downloaded and decrypted bytes for chunk
                    let mut chunk_bytes: Vec<u8> = Vec::new();
                    // Download chunk
                    match s3_download(&s3_key, &job.aws_bucket, job.aws_region.clone(), secret)
                        .await
                    {
                        Ok(cb) => {
                            chunk_bytes = cb;
                            println!(
                                "[{}] {}-{} ⇉ '{}' restored successfully. ({}/{})",
                                Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                job.name,
                                self.id,
                                local_path,
                                counter,
                                self.files_changed.len(),
                            );
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
                    // Determine if single or multi-chunk file
                    let mut multi_chunks: Vec<(&FileChunk, &[u8])> = Vec::new();
                    for (_, chunk) in fc.iter() {
                        if self.is_single_chunk(chunk) {
                            // If a single-chunk file, simply decrypt and write
                            write_chunk(&chunk.local_path, &chunk_bytes, &output_folder)?;
                            continue 'outer;
                        } else {
                            // If a multi-chunk file pass to multi-chunk vec
                            multi_chunks.push((chunk, &chunk_bytes));
                        }
                    }
                    // Here, we create a vec of all chunks associated with larger,
                    // multi-chunk files. Loops until all files and their chunks
                    // have been sorted and written to disk.
                    let mut mc_len: usize = multi_chunks.len();
                    while mc_len != 0 {
                        let mut same_file_chunks = Vec::new();
                        for (i, c) in multi_chunks.iter().enumerate() {
                            if i == multi_chunks.len() - 1 || mc_len == 0 {
                                break;
                            }
                            if c.0.local_path == multi_chunks[i + 1].0.local_path {
                                same_file_chunks.push(*c);
                                mc_len -= 1;
                            }
                        }
                        // Here, we sort all the chunks by thier offset and
                        // combine the chunks and write the file
                        assemble_chunks(same_file_chunks, &output_folder)?;
                    }
                }
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

// Takes a FileChunk's bytes and writes them to disk.
fn write_chunk(
    chunk_path: &Path,
    chunk_bytes: &[u8],
    output_folder: &str,
) -> Result<(), Box<dyn Error>> {
    // Create parent directory if missing
    let correct_chunk_path = chunk_path.strip_prefix("/")?;
    let folder_path = Path::new(&output_folder).join(correct_chunk_path);
    let folder_parent = match folder_path.parent() {
        Some(p) => p,
        _ => &folder_path,
    };
    std::fs::create_dir_all(folder_parent)?;
    // Create the file
    if !Path::new(&output_folder).join(correct_chunk_path).exists() {
        let mut dfile = File::create(Path::new(&output_folder).join(correct_chunk_path))?;
        dfile.write_all(&chunk_bytes)?;
    } else {
        let mut dfile = OpenOptions::new()
            .write(true)
            .open(Path::new(&output_folder).join(correct_chunk_path))?;
        dfile.write_all(&chunk_bytes)?;
    }
    Ok(())
}

// TODO: file sizes seem to be doubled when restoring
// multi-chunk files
// Find all chunks associated with the same path
// and combine them in order according to their offsets and
// lengths and then save to the original local path
fn assemble_chunks(
    mut chunks: Vec<(&FileChunk, &[u8])>,
    output_folder: &str,
) -> Result<(), Box<dyn Error>> {
    // Get path for chunks
    let path = chunks[0].0.local_path.clone();
    // Sort all chunks by their offset
    // TODO: this sort isn't working for some reason. On music
    // files for instance, portions of the song are out of order
    // or repeated. The correct length remains intact, however.
    chunks.sort_by(|a, b| a.0.offset.cmp(&b.0.offset));
    // Write all chunk bytes into final collection of bytes
    let mut file_bytes = Vec::<u8>::new();
    for chunk in chunks.iter_mut() {
        file_bytes.append(&mut chunk.1.to_vec());
    }
    // Write the file
    write_chunk(&path, &file_bytes, &output_folder)?;
    Ok(())
}

fn strip_hash_from_s3(s3_path: &str) -> Result<String, std::io::Error> {
    // Pop hash off from S3 path
    let mut fp: Vec<_> = s3_path.split('/').collect();
    let hdt = match fp.pop() {
        Some(h) => h,
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to pop chunk's S3 path.",
            ))
        }
    };
    // Split the 902938470293847392033874592038473.chunk
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

    #[test]
    fn test_is_single_chunk() {
        let mut r = Run::new(9999);
        let f = read(&std::path::PathBuf::from(
            "/Users/Ryan/Documents/ciehanski.com/index.html",
        ))
        .unwrap();
        let chunk_hmap = chunk_file(&f);
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
    fn test_strip_hash_from_s3() {
        // Split the 902938470293847392033874592038473.chunk
        let hash = strip_hash_from_s3("f339aae7-e994-4fb4-b6aa-623681df99aa/chunks/001d46082763b930e5b9f0c52d16841b443bfbcd52af6cd475cb0182548da33a.chunk").unwrap();
        assert_eq!(
            hash,
            "001d46082763b930e5b9f0c52d16841b443bfbcd52af6cd475cb0182548da33a"
        )
    }
}
