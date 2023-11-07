//
// Copyright (c) 2020 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::job::KipFile;
use anyhow::Result;
use crypto_hash::{hex_digest, Algorithm};
use fastcdc::v2020::AsyncStreamCDC;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio_stream::StreamExt;

// 1 MB is min chunk size
const MIN_SIZE: u32 = 1024 * 1024;
// 4 MB is average chunk size
const AVG_SIZE: u32 = 4 * 1024 * 1024;
// 10 MB is max chunk size
const MAX_SIZE: u32 = 10 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub struct FileChunk {
    pub local_path: PathBuf,
    pub remote_path: String,
    pub hash: String,
    pub offset: usize,
    pub length: usize,
    pub end: usize,
}

impl FileChunk {
    pub fn new<S: Into<String>, P: Into<PathBuf>>(
        local_path: P,
        chunk_hash: S,
        offset: usize,
        length: usize,
        end: usize,
    ) -> Self {
        Self {
            local_path: local_path.into(),
            remote_path: String::new(),
            hash: chunk_hash.into(),
            offset,
            length,
            end,
        }
    }

    pub fn set_remote_path<S: Into<String>>(&mut self, remote_path: S) {
        self.remote_path = remote_path.into();
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipFileChunked {
    pub file: KipFile,
    pub chunks: HashMap<String, FileChunk>,
}

impl KipFileChunked {
    pub fn new<P: AsRef<Path>, S: Into<String>>(path: P, file_hash: S, len: usize) -> Self {
        Self {
            file: KipFile {
                name: path
                    .as_ref()
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string(),
                path: path.as_ref().to_path_buf(),
                hash: file_hash.into(),
                len,
            },
            chunks: HashMap::new(),
        }
    }

    pub fn add_chunk(&mut self, chunk: FileChunk) {
        let hash = chunk.hash.clone();
        self.chunks.insert(hash, chunk);
    }

    // Checks if a certain file backed up in a specific run
    // was split into a single or multiple chunks.
    pub fn is_single_chunk(&self) -> bool {
        if self.chunks.len() == 1 {
            return true;
        }
        false
    }

    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
}

/// chunk_compress_encrypt takes an array of bytes and chunks
/// the contents according to the MIN, AVG, and MAX consts above.
pub async fn chunk_file<P: AsRef<Path>>(
    path: P,
    file_hash: String,
    len: usize,
    bytes: &[u8],
) -> Result<(KipFileChunked, HashMap<FileChunk, &[u8]>)> {
    // Create a new chunker & stream over bytes
    let mut chunker = AsyncStreamCDC::new(bytes, MIN_SIZE, AVG_SIZE, MAX_SIZE);
    let mut stream = Box::pin(chunker.as_stream());

    // For each chunk generated, add it to chunks collection to return
    let mut chunks = HashMap::new();
    let mut kcf = KipFileChunked::new(path.as_ref(), file_hash, len);

    while let Some(result) = stream.next().await {
        let entry = result?;
        let end = entry.offset as usize + entry.length;
        let chunk_bytes = &bytes[entry.offset as usize..end];
        let chunk_hash = hex_digest(Algorithm::SHA256, &entry.data);
        // Create new FileChunk
        let chunk = FileChunk::new(
            path.as_ref(),
            chunk_hash,
            entry.offset.try_into()?,
            entry.length,
            end,
        );
        // Insert newly created chunk for return
        chunks.insert(chunk.clone(), chunk_bytes);
        kcf.add_chunk(chunk);
    }
    // Ship it
    Ok((kcf, chunks))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::fs::read;

    #[tokio::test]
    async fn test_chunk_single_chunk_file() {
        let mut contents = vec![];
        if !cfg!(windows) {
            // Unix, Mac, Linux, etc
            let content_result = read("test/vandy.jpg").await;
            assert!(content_result.is_ok());
            let mut cr = content_result.unwrap();
            contents.append(&mut cr);
        } else {
            // Windows
            let content_result = read(r".\test\vandy.jpg").await;
            assert!(content_result.is_ok());
            let mut cr = content_result.unwrap();
            contents.append(&mut cr);
        }
        let chunk_hmap_result = chunk_file(
            &Path::new("test/vandy.jpg"),
            String::new(),
            contents.len(),
            &contents,
        )
        .await;
        assert!(chunk_hmap_result.is_ok());
        let (_, chunk_hmap) = chunk_hmap_result.unwrap();
        assert_eq!(chunk_hmap.len(), 1);
        for (c, _) in chunk_hmap.iter() {
            assert_eq!(
                c.hash,
                "97ad4887a60dfa689660bad732f92a2871dedf97add169267c43e2955415488d".to_string()
            );
        }
    }

    #[tokio::test]
    async fn test_chunk_multi_chunk_file() {
        let content_result = read("test/dummyfile").await;
        assert!(content_result.is_ok());
        let contents = content_result.unwrap();
        let chunk_hmap_result = chunk_file(
            &Path::new("test/dummyfile"),
            String::new(),
            contents.len(),
            &contents,
        )
        .await;
        assert!(chunk_hmap_result.is_ok());
        let (_, chunk_hmap) = chunk_hmap_result.unwrap();
        assert!(chunk_hmap.len() > 1);
        println!("{:?}", chunk_hmap);
        for (c, _) in chunk_hmap.iter() {
            if c.offset == 0 {
                assert_eq!(
                    c.hash,
                    "86d20d2ebc85db4b54db98e8a9f415225d3b230ff7ee7e07fb8927e16818b6ab".to_string()
                );
            } else if c.offset == 7159842 {
                assert_eq!(
                    c.hash,
                    "0231275934f66653679a7575e96a08495a9ff4234f10eb61ae76f8ef9e3a78fb".to_string()
                );
            } else if c.offset == 23903350 {
                assert_eq!(
                    c.hash,
                    "1913dd7777d0039cf92f904148fa2d3b14b009d5553fb8fb5889bb9b27136e91".to_string()
                );
            } else if c.offset == 15112420 {
                assert_eq!(
                    c.hash,
                    "c3e624f3d44945495fa59b446e2b883e5b2178e2dd4da4052d5c6613b95d3469".to_string()
                );
            }
        }
    }
}
