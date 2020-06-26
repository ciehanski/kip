use crate::chunk::chunk_file;
use crate::chunk::FileChunk;
use crate::crypto::{decrypt, encrypt};
use rusoto_core::Region;
use rusoto_s3::{
    DeleteObjectRequest, GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client,
    StreamingBody, S3,
};
use std::collections::HashMap;
use std::error::Error;
use std::fs::read;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

pub async fn s3_upload(
    f: &Path,
    fmd_len: u64,
    job_id: Uuid,
    aws_bucket: String,
    aws_region: Region,
    secret: &str,
) -> Result<HashMap<String, FileChunk>, Box<dyn Error>> {
    // Chunk the file
    let chunk_map = chunk_file(&read(f)?);
    // Upload each chunk
    let mut chunks = HashMap::new();
    'outer: for mut chunk in chunk_map {
        // Get full path of chunked file (SHA256 hash)
        let chunked_path = match f.parent() {
            Some(p) => p.join(&chunk.0.hash),
            _ => f.join(&chunk.0.hash),
        };
        // Check S3 if this chunk aleady exists
        let s3_objs = list_s3_bucket(&aws_bucket, aws_region.clone()).await?;
        for obj in s3_objs {
            if obj
                .key
                .expect("unable to get chunk's name from S3.")
                .contains(&chunked_path.display().to_string())
            {
                continue 'outer;
            }
        }
        // Encrypt chunk
        let encrypted = match encrypt(&chunk.1, secret) {
            Ok(ec) => ec,
            Err(e) => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("failed to encrypt chunk: {}.", e),
                )));
            }
        };
        let s3_client = S3Client::new(aws_region.clone());
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
        // Push chunk onto chunks hashmap for return
        chunk.0.local_path = PathBuf::from(f).canonicalize()?;
        chunks.insert(chunk.0.hash.to_string(), chunk.0);
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
            bucket: aws_bucket.clone().to_string(),
            key: f.to_string(),
            ..Default::default()
        })
        .await?;
    // Read result from S3 and convert to bytes
    let mut result_stream = result
        .body
        .expect("unable to read response from S3.")
        .into_async_read();
    let mut result_bytes = Vec::<u8>::new();
    result_stream.read_to_end(&mut result_bytes).await?;
    // Decrypt bytes
    let decrypted = match decrypt(&result_bytes, secret) {
        Ok(dc) => dc,
        Err(e) => {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
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
) -> Result<Vec<rusoto_s3::Object>, Box<dyn Error>> {
    let s3_client = S3Client::new(aws_region);
    let result = s3_client
        .list_objects_v2(ListObjectsV2Request {
            bucket: aws_bucket.to_string(),
            ..ListObjectsV2Request::default()
        })
        .await?;
    // Convert S3 result into Vec<S3::Object> which can
    // be used to manipulate the list of files in S3.
    let contents = result
        .contents
        .unwrap_or_default()
        .into_iter()
        .collect::<Vec<_>>();
    Ok(contents)
}
