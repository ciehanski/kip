//
// Copyright (c) 2023 Ryan Ciehanski <ryan@ciehanski.com>
//

use anyhow::Result;
use async_compression::tokio::write::*;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct KipCompressOpts {
    pub enabled: bool,
    pub alg: KipCompressAlg,
    pub level: KipCompressLevel,
}

impl KipCompressOpts {
    pub fn new(enabled: bool, alg: KipCompressAlg, level: KipCompressLevel) -> Self {
        Self {
            enabled,
            alg,
            level,
        }
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum KipCompressAlg {
    Zstd,
    Lzma,
    Gzip,
    Brotli,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum KipCompressLevel {
    Fastest,
    Best,
    Default,
}

impl KipCompressLevel {
    pub fn parse(&self) -> async_compression::Level {
        match self {
            KipCompressLevel::Fastest => async_compression::Level::Fastest,
            KipCompressLevel::Best => async_compression::Level::Best,
            KipCompressLevel::Default => async_compression::Level::Default,
        }
    }
}

pub async fn compress_zstd(level: KipCompressLevel, bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = ZstdEncoder::with_quality(vec![], level.parse());
    encoder.write_all(bytes).await?;
    encoder.shutdown().await?;
    Ok(encoder.into_inner())
}

pub async fn decompress_zstd(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = ZstdDecoder::new(vec![]);
    decoder.write_all(bytes).await?;
    decoder.shutdown().await?;
    Ok(decoder.into_inner())
}

pub async fn compress_gzip(level: KipCompressLevel, bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzipEncoder::with_quality(vec![], level.parse());
    encoder.write_all(bytes).await?;
    encoder.shutdown().await?;
    Ok(encoder.into_inner())
}

pub async fn decompress_gzip(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = GzipDecoder::new(vec![]);
    decoder.write_all(bytes).await?;
    decoder.shutdown().await?;
    Ok(decoder.into_inner())
}

pub async fn compress_brotli(level: KipCompressLevel, bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = BrotliEncoder::with_quality(vec![], level.parse());
    encoder.write_all(bytes).await?;
    encoder.shutdown().await?;
    Ok(encoder.into_inner())
}

pub async fn decompress_brotli(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = BrotliDecoder::new(vec![]);
    decoder.write_all(bytes).await?;
    decoder.shutdown().await?;
    Ok(decoder.into_inner())
}

pub async fn compress_lzma(level: KipCompressLevel, bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = LzmaEncoder::with_quality(vec![], level.parse());
    encoder.write_all(bytes).await?;
    encoder.shutdown().await?;
    Ok(encoder.into_inner())
}

pub async fn decompress_lzma(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = LzmaDecoder::new(vec![]);
    decoder.write_all(bytes).await?;
    decoder.shutdown().await?;
    Ok(decoder.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_compress_zstd() {
        // Open test file
        let file_result = std::fs::read("test/kip");
        assert!(file_result.is_ok());
        let file_len = file_result.as_ref().unwrap().len();
        // Compress test file
        let compressed_result =
            compress_zstd(KipCompressLevel::Default, file_result.as_ref().unwrap()).await;
        assert!(compressed_result.is_ok());
        assert_ne!(
            compressed_result.as_ref().unwrap(),
            file_result.as_ref().unwrap()
        );
        assert!(file_len > compressed_result.unwrap().len())
    }

    #[tokio::test]
    async fn test_decompress_zstd() {
        // Open test file
        let file_result = std::fs::read("test/kip");
        assert!(file_result.is_ok());
        // Compress test file
        let compressed_result =
            compress_zstd(KipCompressLevel::Default, file_result.as_ref().unwrap()).await;
        assert!(compressed_result.is_ok());
        let decompressed_result = decompress_zstd(&compressed_result.unwrap()).await;
        assert!(decompressed_result.is_ok());
        assert_eq!(decompressed_result.unwrap(), file_result.unwrap())
    }

    #[tokio::test]
    async fn test_compress_lzma() {
        // Open test file
        let file_result = std::fs::read("test/kip");
        assert!(file_result.is_ok());
        let file_len = file_result.as_ref().unwrap().len();
        // Compress test file
        let compressed_result =
            compress_lzma(KipCompressLevel::Default, file_result.as_ref().unwrap()).await;
        assert!(compressed_result.is_ok());
        assert_ne!(
            compressed_result.as_ref().unwrap(),
            file_result.as_ref().unwrap()
        );
        assert!(file_len > compressed_result.unwrap().len())
    }

    #[tokio::test]
    async fn test_decompress_lzma() {
        // Open test file
        let file_result = std::fs::read("test/kip");
        assert!(file_result.is_ok());
        // Compress test file
        let compressed_result =
            compress_lzma(KipCompressLevel::Default, file_result.as_ref().unwrap()).await;
        assert!(compressed_result.is_ok());
        let decompressed_result = decompress_lzma(&compressed_result.unwrap()).await;
        assert!(decompressed_result.is_ok());
        assert_eq!(decompressed_result.unwrap(), file_result.unwrap())
    }

    #[tokio::test]
    async fn test_compress_gzip() {
        // Open test file
        let file_result = std::fs::read("test/kip");
        assert!(file_result.is_ok());
        let file_len = file_result.as_ref().unwrap().len();
        // Compress test file
        let compressed_result =
            compress_gzip(KipCompressLevel::Default, file_result.as_ref().unwrap()).await;
        assert!(compressed_result.is_ok());
        assert_ne!(
            compressed_result.as_ref().unwrap(),
            file_result.as_ref().unwrap()
        );
        assert!(file_len > compressed_result.unwrap().len())
    }

    #[tokio::test]
    async fn test_decompress_gzip() {
        // Open test file
        let file_result = std::fs::read("test/kip");
        assert!(file_result.is_ok());
        // Compress test file
        let compressed_result =
            compress_gzip(KipCompressLevel::Default, file_result.as_ref().unwrap()).await;
        assert!(compressed_result.is_ok());
        let decompressed_result = decompress_gzip(&compressed_result.unwrap()).await;
        assert!(decompressed_result.is_ok());
        assert_eq!(decompressed_result.unwrap(), file_result.unwrap())
    }

    #[tokio::test]
    async fn test_compress_brotili() {
        // Open test file
        let file_result = std::fs::read("test/kip");
        assert!(file_result.is_ok());
        let file_len = file_result.as_ref().unwrap().len();
        // Compress test file
        let compressed_result =
            compress_brotli(KipCompressLevel::Fastest, file_result.as_ref().unwrap()).await;
        assert!(compressed_result.is_ok());
        assert_ne!(
            compressed_result.as_ref().unwrap(),
            file_result.as_ref().unwrap()
        );
        assert!(file_len > compressed_result.unwrap().len())
    }

    #[tokio::test]
    async fn test_decompress_brotli() {
        // Open test file
        let file_result = std::fs::read("test/kip");
        assert!(file_result.is_ok());
        // Compress test file
        let compressed_result =
            compress_brotli(KipCompressLevel::Fastest, file_result.as_ref().unwrap()).await;
        assert!(compressed_result.is_ok());
        let decompressed_result = decompress_brotli(&compressed_result.unwrap()).await;
        assert!(decompressed_result.is_ok());
        assert_eq!(decompressed_result.unwrap(), file_result.unwrap())
    }
}
