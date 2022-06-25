//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::chunk::FileChunk;
use crate::crypto::{decrypt, encrypt};
use rusoto_core::Region;
use rusoto_s3::{
    DeleteObjectRequest, GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client,
    StreamingBody, S3,
};
use std::collections::HashMap;
use std::error::Error;
use std::io::Error as IOErr;
use std::io::ErrorKind;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

pub async fn s3_upload(
    f: &PathBuf,
    chunks_map: HashMap<FileChunk, &[u8]>,
    fmd_len: u64,
    job_id: Uuid,
    aws_bucket: String,
    aws_region: Region,
    secret: &str,
) -> Result<HashMap<String, FileChunk>, Box<dyn Error>> {
    // Create S3 client
    let s3_client = S3Client::new(aws_region.clone());
    let s3_objs = list_s3_bucket(&aws_bucket, aws_region.clone(), job_id).await?;
    // Upload each chunk
    let mut chunks = HashMap::new();
    'outer: for (mut chunk, chunk_bytes) in chunks_map {
        // Check S3 for duplicates of chunk
        let s3_objs = s3_objs.clone();
        // If the S3 bucket is empty, no need to check for duplicate chunks
        if !s3_objs.is_empty() {
            for obj in s3_objs {
                if let Some(obj_key) = obj.key {
                    if obj_key.contains(&chunk.hash) {
                        // Duplicate chunk found, skip this chunk
                        continue 'outer;
                    }
                } else {
                    return Err(Box::new(IOErr::new(
                        ErrorKind::InvalidInput,
                        format!("unable to get chunk from S3."),
                    )));
                }
            }
        }
        // Encrypt chunk
        let encrypted = match encrypt(chunk_bytes, secret) {
            Ok(ec) => ec,
            Err(e) => {
                return Err(Box::new(IOErr::new(
                    ErrorKind::InvalidData,
                    format!("failed to encrypt chunk: {}.", e),
                )));
            }
        };
        s3_client
            .put_object(PutObjectRequest {
                bucket: aws_bucket.clone(),
                key: format!("{}/chunks/{}.chunk", job_id, chunk.hash),
                content_length: Some(fmd_len as i64),
                body: Some(StreamingBody::from(encrypted)),
                ..Default::default()
            })
            .await?;
        // Push chunk onto chunks hashmap for return
        chunk.local_path = f.canonicalize()?;
        chunks.insert(chunk.hash.to_string(), chunk);
    }
    Ok(chunks)
}

pub async fn s3_download(
    f: &str,
    aws_bucket: &str,
    aws_region: Region,
    secret: &str,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let s3_client = S3Client::new(aws_region);
    let result = s3_client
        .get_object(GetObjectRequest {
            bucket: aws_bucket.to_string(),
            key: f.to_string(),
            ..Default::default()
        })
        .await?;
    // Read result from S3 and convert to bytes
    let mut result_stream = match result.body {
        Some(b) => b.into_async_read(),
        _ => {
            return Err(Box::new(IOErr::new(
                ErrorKind::InvalidData,
                format!("unable to read response from S3."),
            )));
        }
    };
    // Read the downloaded S3 bytes into result_bytes
    let mut result_bytes = Vec::<u8>::new();
    result_stream.read_to_end(&mut result_bytes).await?;
    // Decrypt result_bytes
    let decrypted = match decrypt(&result_bytes, secret) {
        Ok(dc) => dc,
        Err(e) => {
            return Err(Box::new(IOErr::new(
                ErrorKind::InvalidData,
                format!("failed to decrypt file: {}.", e),
            )));
        }
    };
    // Return downloaded & decrypted bytes
    Ok(decrypted)
}

pub async fn delete_s3_object(
    aws_bucket: &str,
    aws_region: Region,
    file_name: &str,
) -> Result<(), Box<dyn Error>> {
    let s3_client = S3Client::new(aws_region);
    s3_client
        .delete_object(DeleteObjectRequest {
            bucket: aws_bucket.to_string(),
            key: file_name.to_string(),
            ..DeleteObjectRequest::default()
        })
        .await?;
    Ok(())
}

pub async fn list_s3_bucket(
    aws_bucket: &str,
    aws_region: Region,
    job_id: Uuid,
) -> Result<Vec<rusoto_s3::Object>, Box<dyn Error>> {
    let s3_client = S3Client::new(aws_region);
    let result = s3_client
        .list_objects_v2(ListObjectsV2Request {
            bucket: aws_bucket.to_string(),
            ..ListObjectsV2Request::default()
        })
        .await?;
    // Convert S3 result into Vec<S3::Object> which can
    // be used to manipulate the list of files in S3
    let contents = match result.contents {
        Some(c) => c.into_iter().collect::<Vec<_>>(),
        _ => {
            // S3 bucket was empty, return an empty Vec
            let empty_bucket: Vec<rusoto_s3::Object> = vec![];
            return Ok(empty_bucket);
        }
    };
    // Only check chunks that are within this job's
    // folder in S3
    let mut corrected_contents = vec![];
    for obj in contents {
        if let Some(key) = obj.key.clone() {
            // We expect jid to be Some since key was not nil
            if let Some((jid, _)) = key.split_once("/") {
                if jid == &job_id.to_string() {
                    corrected_contents.push(obj);
                };
            } else {
                // error splitting obj key returned from S3
                return Err(Box::new(IOErr::new(
                    ErrorKind::InvalidInput,
                    format!("error splitting chunk name from S3."),
                )));
            };
        } else {
            // error, no obj key returned from S3
            return Err(Box::new(IOErr::new(
                ErrorKind::InvalidInput,
                format!("unable to get chunk name from S3."),
            )));
        }
    }
    Ok(corrected_contents)
}

pub async fn check_bucket(
    aws_bucket: &str,
    aws_region: Region,
    job_id: Uuid,
    query: &str,
) -> Result<bool, Box<dyn Error>> {
    // Check S3 for duplicates of chunk
    let s3_objs = list_s3_bucket(aws_bucket, aws_region.clone(), job_id).await?;
    // If the S3 bucket is empty, no need to check for duplicate chunks
    if !s3_objs.is_empty() {
        for obj in s3_objs {
            // let key = strip_hash_from_s3(&obj.key.unwrap())?;
            if let Some(obj_key) = obj.key {
                if obj_key.contains(query) {
                    // Duplicate chunk found, return true
                    return Ok(true);
                }
            } else {
                return Err(Box::new(IOErr::new(
                    ErrorKind::InvalidInput,
                    format!("unable to get chunk name from S3."),
                )));
            }
        }
    }
    Ok(false)
}
