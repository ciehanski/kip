//
// Copyright (c) 2020 Ryan Ciehanski <ryan@ciehanski.com>
//

use crypto_hash::{hex_digest, Algorithm};
use fastcdc::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

// 5 MB is min
const MIN_SIZE: usize = 5 * 1024 * 1024;
// 10 MB is average - CHANGE TO 25
const AVG_SIZE: usize = 10 * 1024 * 1024;
// 15 MB is max - CHANGE to 50
const MAX_SIZE: usize = 15 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub struct FileChunk {
    pub local_path: PathBuf,
    pub hash: String,
    pub offset: usize,
    pub length: usize,
    pub end: usize,
}

impl FileChunk {
    pub fn new(hash: &str, offset: usize, length: usize, end: usize) -> Self {
        Self {
            local_path: PathBuf::new(),
            hash: hash.to_string(),
            offset,
            length,
            end,
        }
    }
}

// ref: https://github.com/nlfiedler/fastcdc-rs/blob/master/examples/dedupe.rs
pub fn chunk_file(bytes: &[u8]) -> HashMap<FileChunk, &[u8]> {
    // Create a new chunker with an average size per chunk in bytes
    let chunker = FastCDC::new(bytes, MIN_SIZE, AVG_SIZE, MAX_SIZE);
    // For each chunk generated, add it to chunks collection to return
    let mut chunks = HashMap::new();
    for entry in chunker {
        let end = entry.offset + entry.length;
        let digest = hex_digest(Algorithm::SHA256, &bytes[entry.offset..end]);
        chunks.insert(
            FileChunk::new(&digest, entry.offset, entry.length, end),
            &bytes[entry.offset..end],
        );
    }
    // Ship it
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::read;

    #[test]
    fn test_chunk_single_chunk_file() {
        let content_result = read("test/random.txt");
        assert!(content_result.is_ok());
        let contents = content_result.unwrap();
        let chunk_hmap = chunk_file(&contents);
        assert_eq!(chunk_hmap.len(), 1);
        for (c, _) in chunk_hmap.iter() {
            assert_eq!(
                c.hash,
                "44b4cdaf713dfaf961dedb34f07e15604f75eb049c83067ab35bf388b369dbf3".to_string()
            );
        }
    }

    #[test]
    fn test_chunk_multi_chunk_file() {
        let content_result = read("test/kip");
        assert!(content_result.is_ok());
        let contents = content_result.unwrap();
        let chunk_hmap = chunk_file(&contents);
        assert!(chunk_hmap.len() > 1);
        for (c, _) in chunk_hmap.iter() {
            if c.offset == 0 {
                assert_eq!(
                    c.hash,
                    "e2910717492356757f34a9b7ecd2ab8454dfba1805715328f2a20954100b823a".to_string()
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
