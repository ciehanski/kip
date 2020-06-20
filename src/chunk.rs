use crypto_hash::{hex_digest, Algorithm};
use fastcdc::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
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
pub fn chunk_file(bytes: &[u8]) -> (Vec<FileChunk>, Vec<Vec<u8>>) {
    // Create a new chunker with an average size per chunk
    let avg_size = 131072 as usize;
    let chunker = FastCDC::new(&bytes[..], avg_size / 2, avg_size, avg_size * 2);
    // For each chunk, add it to chunks collection to return
    let mut chunk_vec = Vec::new();
    let mut chunk_bytes = Vec::new();
    for entry in chunker {
        let end = entry.offset + entry.length;
        let digest = hex_digest(Algorithm::SHA256, &bytes[entry.offset..end]);
        chunk_vec.push(FileChunk::new(&digest, entry.offset, entry.length, end));
        chunk_bytes.push(bytes[entry.offset..end].to_vec());
    }
    // Ship it
    (chunk_vec, chunk_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Read;

    #[test]
    fn test_chunk_small_file() {
        let mut f = File::open("/Users/Ryan/Documents/ciehanski.com/index.html").unwrap();
        let metadata = std::fs::metadata("/Users/Ryan/Documents/ciehanski.com/index.html").unwrap();
        let mut buffer = vec![0; metadata.len() as usize];
        f.read(&mut buffer).unwrap();
        let chunk_hmap = chunk_file(&buffer);
        assert_eq!(
            chunk_hmap[0].hash,
            "b2b09a9f4d09b6744568cdfb80d53a221fe3f8c232b03e9ebe343aefcab6b876".to_string()
        )
    }
}
