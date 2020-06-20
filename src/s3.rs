use crate::chunk::chunk_file;
use crate::chunk::FileChunk;
use crate::crypto::{decrypt, encrypt};
use rusoto_core::Region;
use rusoto_s3::{
    GetObjectRequest, ListObjectsV2Request, Object, PutObjectRequest, S3Client, StreamingBody, S3,
};
// use sha3::{Digest, Sha3_256};
use std::collections::HashMap;
use std::error::Error;
use std::fs::{read, File};
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

pub async fn s3_upload(
    f: &PathBuf,
    fmd_len: u64,
    job_id: Uuid,
    aws_bucket: String,
    aws_region: Region,
    secret: &str,
) -> Result<HashMap<String, FileChunk>, Box<dyn Error>> {
    // Chunk the file
    let (chunked_file, chunk_bytes) = chunk_file(&read(f)?);
    // Upload each chunk
    let mut chunks = HashMap::new();
    // TODO: remove this yucky clone. Im sorry
    for mut chunk in chunked_file.clone() {
        // Get full path of chunked file (SHA256 hash)
        let chunked_path = get_chunk_path(f, &chunk.hash);
        // Check S3 if this chunk aleady exists
        let s3_objs = list_s3_bucket(&aws_bucket, aws_region.clone()).await?;
        let mut cont = false;
        for obj in s3_objs {
            if obj
                .key
                .clone()
                .expect("[ERR] unable to get S3 object key.")
                .contains(&chunked_path.as_path().display().to_string())
            {
                cont = true;
            }
        }
        if cont {
            continue;
        };
        // Encrypt chunk
        let bytes_index = chunked_file
            .iter()
            .position(|r| r == &chunk)
            .expect("[ERR] unable to get chunk position.");
        let encrypted = match encrypt(&chunk_bytes[bytes_index], secret) {
            Ok(ef) => ef,
            Err(e) => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("failed to encrypt file: {}.", e),
                )));
            }
        };
        // Create S3 client with specifc region
        let s3_client = S3Client::new(aws_region.clone());
        // PUT!
        s3_client
            .put_object(PutObjectRequest {
                bucket: aws_bucket.clone(),
                key: format!(
                    "{}/{}.chunk",
                    job_id,
                    chunked_path.as_path().display().to_string()
                ),
                content_length: Some(fmd_len as i64),
                body: Some(StreamingBody::from(encrypted)),
                ..Default::default()
            })
            .await?;
        // Push hash of chunk onto chunks_str
        chunk.local_path = PathBuf::from(f).canonicalize()?;
        chunks.insert(chunk.hash.to_string(), chunk);
    }
    // Success
    Ok(chunks)
}

fn get_chunk_path(path: &PathBuf, new_path: &str) -> PathBuf {
    // Split canonicalized path by folder seperator
    let path_str = path.as_path().display().to_string();
    let mut fp: Vec<_> = path_str.split("/").collect();
    fp.pop().expect("[ERR] failed to pop full path.");
    let mut pp = PathBuf::new();
    for p in fp.iter() {
        pp = pp.join(p);
    }
    pp.join(new_path)
}

pub async fn s3_download(
    f: &str,
    aws_bucket: &str,
    aws_region: Region,
    secret: &str,
    output: &str,
) -> Result<(), Box<dyn Error>> {
    // Create S3 client with specifc region
    let s3_client = S3Client::new(aws_region);
    // GET!
    let result = s3_client
        .get_object(GetObjectRequest {
            bucket: aws_bucket.clone().to_string(),
            key: f.to_string(),
            ..Default::default()
        })
        .await?;
    // Read result from S3 and convert to bytes
    let mut result_stream = result
        .body
        .expect("[ERR] unable to read result body.")
        .into_async_read();
    let mut result_bytes = Vec::<u8>::new();
    result_stream.read_to_end(&mut result_bytes).await?;
    // Decrypt file
    let decrypted = match decrypt(&result_bytes, secret) {
        Ok(df) => df,
        Err(e) => {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to decrypt file: {}.", e),
            )));
        }
    };
    // Create root directory if missing
    let mut folder_path = Path::new(output).join(f);
    folder_path.pop();
    std::fs::create_dir_all(folder_path)?;
    // Create the file
    if !Path::new(output).join(f).exists() {
        let mut dfile = File::create(Path::new(output).join(f))?;
        dfile.write_all(&decrypted)?;
    }
    // Success
    Ok(())
}

pub async fn list_s3_bucket(
    aws_bucket: &str,
    aws_region: Region,
) -> Result<Vec<Object>, Box<dyn Error>> {
    // Create S3 client with specifc region
    let s3_client = S3Client::new(aws_region);
    let result = s3_client
        .list_objects_v2(ListObjectsV2Request {
            bucket: aws_bucket.to_string().clone(),
            ..ListObjectsV2Request::default()
        })
        .await?;
    // Convert S3 result into Vec<S3::Object> which can
    // be used to manipulate the list od files in S3.
    let contents = result
        .contents
        .unwrap_or_default()
        .into_iter()
        .collect::<Vec<_>>();
    // Success
    Ok(contents)
}

// fn hash_folder(path: PathBuf) -> Result<PathBuf, Box<dyn Error>> {
//     // Split canonicalized path by folder seperator
//     let path_str = path.as_path().display().to_string();
//     let fp: Vec<_> = path_str.split("/").collect();
//     let mut hashed_folders = Vec::<String>::new();
//     // Hash each folder and add it to hashed_folders
//     for folder in fp {
//         // Create a SHA3-256 object
//         let mut hasher = Sha3_256::new();
//         // Write password's bytes into hasher
//         hasher.update(folder.as_bytes());
//         // SHA3-256 32-byte secret
//         let hashed_folder = hasher.finalize();
//         // Convert hashed_secret bytes to &str
//         let hf = format!("{:x}", hashed_folder);
//         // Push onto hashed_folders
//         hashed_folders.push(hf);
//     }
//     // Assemle all hashed folders as path here
//     let mut t = PathBuf::new();
//     for p in hashed_folders {
//         t = t.join(p);
//     }
//     // Ship it
//     Ok(t)
// }
//
// fn decode_hashed_folder(path: PathBuf) -> Result<PathBuf, Box<dyn Error>> {
//     // Split canonicalized path by folder seperator
//     let path_str = path.as_path().display().to_string();
//     let fp: Vec<_> = path_str.split("/").collect();
//     let mut hashed_folders = Vec::<String>::new();
//     // Hash each folder and add it to hashed_folders
//     for folder in fp {
//         // Create a SHA3-256 object
//         let mut hasher = Sha3_256::new();
//         // Write password's bytes into hasher
//         hasher.update(folder.as_bytes());
//         // SHA3-256 32-byte secret
//         let hashed_folder = hasher.finalize();
//         // Convert hashed_secret bytes to &str
//         let hf = format!("{:x}", hashed_folder);
//         // Push onto hashed_folders
//         hashed_folders.push(hf);
//     }
//     // Assemle all hashed folders as path here
//     let mut t = PathBuf::new();
//     for p in hashed_folders {
//         t = t.join(p);
//     }
//     // Ship it
//     Ok(t)
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_split_path() {
//         let full_path = fs::canonicalize(PathBuf::from(
//             "/Users/Ryan/Documents/ciehanski.com/index.html",
//         ))
//         .unwrap();
//         let new_path = "blah";
//         let res = split_path(full_path, new_path);
//         assert_eq!(
//             res.as_path().display().to_string(),
//             "Users/Ryan/Documents/ciehanski.com/blah".to_string()
//         )
//     }
//
//     #[test]
//     fn test_hash_folder() {
//         let folder = fs::canonicalize(PathBuf::from("/Users")).unwrap();
//         let hashed = match hash_folder(folder) {
//             Ok(h) => h,
//             Err(e) => panic!("{}", e),
//         };
//         assert_eq!(&hashed.display().to_string(), "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a/91fc838600350089e33572adf52541d04987d4582b7d571e2f6908afef7b27d9");
//     }
// }
