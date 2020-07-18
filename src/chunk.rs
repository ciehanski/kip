//
// Copyright (c) 2020 Ryan Ciehanski <ryan@ciehanski.com>
//

use crypto_hash::{hex_digest, Algorithm};
use fastcdc::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

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
        FileChunk {
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
    let avg_size = 131072 as usize;
    let chunker = FastCDC::new(&bytes[..], avg_size / 2, avg_size, avg_size * 2);
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
    fn test_chunk_small_file() {
        let content_result = read("test/random.txt");
        assert!(content_result.is_ok());
        let contents = content_result.unwrap();
        let chunk_hmap = chunk_file(&contents);
        for c in chunk_hmap.iter() {
            assert_eq!(
                c.0.hash,
                "44b4cdaf713dfaf961dedb34f07e15604f75eb049c83067ab35bf388b369dbf3".to_string()
            );
        }
    }
}
