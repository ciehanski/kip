//
// Copyright (c) 2023 Ryan Ciehanski <ryan@ciehanski.com>
//

use aead::{Aead, AeadCore, AeadInPlace, KeyInit, OsRng};
use anyhow::{bail, Result};
use argon2::{self, Config, ThreadMode, Variant, Version};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use keyring::Entry;
use rand::Rng;
use zeroize::Zeroize;

const SALT_LEN: usize = 32;
const NONCE_LEN: usize = 24;
const ARGON_CONF: Config = Config {
    variant: Variant::Argon2id,
    version: Version::Version13,
    mem_cost: 65536,
    // TODO: change to 2
    time_cost: 1,
    lanes: 1,
    thread_mode: ThreadMode::Parallel,
    secret: &[],
    ad: &[],
    hash_length: 32,
};

pub fn encrypt_in_place(mut plaintext: Vec<u8>, secret: &str) -> Result<Vec<u8>> {
    // Generate salt - 32-bytes
    let mut salt = [0u8; SALT_LEN];
    OsRng.fill(&mut salt);
    // Hash secret with ARGON_CONF and generated salt
    let mut hashed_secret = argon2::hash_raw(secret.as_bytes(), &salt, &ARGON_CONF)?;
    let key = Key::from_slice(&hashed_secret);
    // New XChaCha20Poly1305 from hashed_secret
    let cipher = XChaCha20Poly1305::new(key);
    // Generate nonce - 24-bytes; unique
    let mut nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    // Encrypt plaintext in place
    match cipher.encrypt_in_place(&nonce, b"", &mut plaintext) {
        Ok(_) => {
            // CIPHERTEXT [?-bytes] | SALT [32-bytes] | NONCE [24-bytes]
            // Append salt to end of ciphertext; needed for decryption
            plaintext.extend_from_slice(&salt);
            // Append nonce to end of ciphertext; needed for decryption
            plaintext.extend_from_slice(&nonce);
        }
        Err(e) => {
            // SAFTEY: key is not zeroized since is borrows
            // hashed_secret which is zeroized
            hashed_secret.zeroize();
            salt.zeroize();
            nonce.zeroize();
            bail!("unable to encrypt plaintext: {e}")
        }
    };
    // Zeroize variables
    // SAFTEY: key is not zeroized since is borrows
    // hashed_secret which is zeroized
    hashed_secret.zeroize();
    salt.zeroize();
    nonce.zeroize();
    // Ship it
    Ok(plaintext)
}

pub fn encrypt_bytes(plaintext: &[u8], secret: &str) -> Result<Vec<u8>> {
    // Generate salt - 32-bytes
    let mut salt = [0u8; SALT_LEN];
    OsRng.fill(&mut salt);
    // Hash secret with ARGON_CONF and generated salt
    let mut hashed_secret = argon2::hash_raw(secret.as_bytes(), &salt, &ARGON_CONF)?;
    let key = Key::from_slice(&hashed_secret);
    // New XChaCha20Poly1305 from hashed_secret
    let cipher = XChaCha20Poly1305::new(key);
    // Generate nonce - 24-bytes; unique
    let mut nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    // Encrypt
    let ciphertext = match cipher.encrypt(&nonce, plaintext) {
        Ok(mut ct) => {
            // CIPHERTEXT [?-bytes] | SALT [32-bytes] | NONCE [24-bytes]
            // Append salt to end of ciphertext; needed for decryption
            ct.extend_from_slice(&salt);
            // Append nonce to end of ciphertext; needed for decryption
            ct.extend_from_slice(&nonce);
            ct
        }
        Err(e) => {
            // SAFTEY: key is not zeroized since is borrows
            // hashed_secret which is zeroized
            hashed_secret.zeroize();
            salt.zeroize();
            nonce.zeroize();
            bail!("unable to encrypt plaintext: {e}")
        }
    };
    // Zeroize variables
    // SAFTEY: key is not zeroized since is borrows
    // hashed_secret which is zeroized
    hashed_secret.zeroize();
    salt.zeroize();
    nonce.zeroize();
    // Ship it
    Ok(ciphertext)
}

pub fn decrypt(ciphertext: &[u8], secret: &str) -> Result<Vec<u8>> {
    // Split off salt & nonce from ciphertext for decryption
    let (salt_cipher, nonce) = ciphertext.split_at(ciphertext.len() - NONCE_LEN);
    let (ciphertext_cut, salt) = salt_cipher.split_at(salt_cipher.len() - SALT_LEN);
    // Hash secret with ARGON_CONF and generated salt
    let mut hashed_secret = argon2::hash_raw(secret.as_bytes(), salt, &ARGON_CONF)?;
    let key = Key::from_slice(&hashed_secret);
    // New XChaCha20Poly1305 from hashed_secret
    let cipher = XChaCha20Poly1305::new(key);
    // Decrypt
    let plaintext = match cipher.decrypt(XNonce::from_slice(nonce), ciphertext_cut) {
        Ok(p) => p,
        Err(e) => {
            // SAFTEY: key is not zeroized since is borrows
            // hashed_secret which is zeroized
            hashed_secret.zeroize();
            bail!("unable to decrypt ciphertext: {e}")
        }
    };
    // Zeroize variables
    // SAFTEY: key is not zeroized since is borrows
    // hashed_secret which is zeroized
    hashed_secret.zeroize();
    // Ship it
    Ok(plaintext)
}

pub fn keyring_set_secret(
    job_name: &str,
    password: &str,
) -> std::result::Result<(), keyring::Error> {
    let entry = Entry::new("com.ciehanski.kip", job_name)?;
    entry.set_password(password)?;
    Ok(())
}

pub fn keyring_get_secret(job_name: &str) -> std::result::Result<String, keyring::Error> {
    let entry = Entry::new("com.ciehanski.kip", job_name)?;
    let password = entry.get_password()?;
    Ok(password)
}

pub fn keyring_delete_secret(job_name: &str) -> std::result::Result<(), keyring::Error> {
    let entry = Entry::new("com.ciehanski.kip", job_name)?;
    entry.delete_password()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compress::{KipCompressAlg, KipCompressLevel, KipCompressOpts},
        job::Job,
        providers::{s3::KipS3, KipProviders},
    };
    use aws_sdk_s3::Region;
    use keyring::{
        mock::{self, MockCredential},
        set_default_credential_builder,
    };
    use std::fs::read;

    #[test]
    fn test_encrypt() {
        let plaintext = b"Super secure information. Please do not share or read.".to_vec();
        let encrypt_result = encrypt_in_place(plaintext, "hunter2");
        assert!(encrypt_result.is_ok());
        assert_ne!(
            encrypt_result.unwrap(),
            b"Super secure information. Please do not share or read."
        )
    }

    #[test]
    fn test_encrypt_file() {
        let content_result = read("test/random.txt");
        assert!(content_result.is_ok());
        let contents = content_result.unwrap();
        let t = contents.clone();
        let encrypt_result = encrypt_in_place(contents, "hunter2");
        assert!(encrypt_result.is_ok());
        assert_ne!(t, encrypt_result.unwrap())
    }

    #[test]
    fn test_decrypt() {
        let plaintext = b"Super secure information. Please do not share or read.".to_vec();
        let encrypt_result = encrypt_in_place(plaintext, "hunter2");
        assert!(encrypt_result.is_ok());
        let decrypt_result = decrypt(&encrypt_result.unwrap(), "hunter2");
        assert!(decrypt_result.is_ok());
        let decrypted = decrypt_result.unwrap();
        assert_eq!(
            std::str::from_utf8(&decrypted).unwrap(),
            "Super secure information. Please do not share or read."
        )
    }

    #[test]
    fn test_decrypt_file() {
        let content_result = read("test/random.txt");
        assert!(content_result.is_ok());
        let contents = content_result.unwrap();
        let t = contents.clone();
        let encrypt_result = encrypt_in_place(contents, "hunter2");
        assert!(encrypt_result.is_ok());
        let decrypt_result = decrypt(&encrypt_result.unwrap(), "hunter2");
        assert!(decrypt_result.is_ok());
        let decrypted = decrypt_result.unwrap();
        assert_eq!(decrypted, t)
    }

    #[test]
    fn test_extract_salt_nonce() {
        let encrypted = b"secret00000000000000000000000000000000111111111111111111111111";
        let (salt_cipher, nonce) = encrypted.split_at(encrypted.len() - NONCE_LEN);
        let (ciphertext, salt) = salt_cipher.split_at(salt_cipher.len() - SALT_LEN);
        assert_eq!(salt.len(), SALT_LEN);
        assert_eq!(salt, b"00000000000000000000000000000000");
        assert_eq!(nonce.len(), NONCE_LEN);
        assert_eq!(nonce, b"111111111111111111111111");
        assert_eq!(ciphertext, b"secret")
    }

    #[test]
    fn test_keyring_set_get() {
        // To use mock credential store call this during
        // application startup before creating any entries
        set_default_credential_builder(mock::default_credential_builder());
        // Create job
        let provider = KipProviders::S3(KipS3::new(
            "kip_test_bucket",
            Region::new("us-east-1".to_owned()),
        ));
        let j = Job::new(
            "testing2",
            provider,
            KipCompressOpts::new(true, KipCompressAlg::Zstd, KipCompressLevel::Best),
        );
        // Mock entries
        let entry = Entry::new("com.ciehanski.kip", &j.name).unwrap();
        // Set secret
        let entry_set_result = entry.set_password("hunter2");
        assert!(entry_set_result.is_ok());
        // Get secret
        let mock_result = entry.get_credential().downcast_ref();
        assert!(mock_result.is_some());
        // Compare secrets, confirm correct
        let mock: &MockCredential = mock_result.unwrap();
        let secret = mock.inner.lock().unwrap().take().password.unwrap();
        assert_eq!(secret, "hunter2");
    }
}
