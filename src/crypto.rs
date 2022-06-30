//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//
use aead::{Aead, NewAead};
use argon2::{self, Config, ThreadMode, Variant, Version};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use keyring::Entry;
use rand::rngs::OsRng;
use rand::Rng;
use std::collections::VecDeque;
use std::error::Error;
use zeroize::Zeroize;

const ARGON_CONF: Config = Config {
    variant: Variant::Argon2id,
    version: Version::Version13,
    mem_cost: 65536,
    time_cost: 1,
    lanes: 1,
    thread_mode: ThreadMode::Parallel,
    secret: &[],
    ad: &[],
    hash_length: 32,
};
const SALT_LEN: usize = 32;
const NONCE_LEN: usize = 24;

pub fn encrypt(plaintext: &[u8], secret: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    // Generate salt - 32-bytes
    let mut salt = [0u8; SALT_LEN];
    OsRng.fill(&mut salt);
    // argon2 32-byte secret
    let mut hashed_secret = argon2::hash_raw(secret.as_bytes(), &salt, &ARGON_CONF)?;
    let key = Key::from_slice(&hashed_secret);
    // New XChaCha20Poly1305 from hashed_secret
    let aead = XChaCha20Poly1305::new(key);
    // Generate nonce - 24-bytes; unique
    let mut nonce_raw = [0u8; NONCE_LEN];
    OsRng.fill(&mut nonce_raw);
    let nonce = XNonce::from_slice(&nonce_raw);
    // Encrypt
    let mut ciphertext = match aead.encrypt(nonce, plaintext) {
        Ok(hs) => hs,
        Err(e) => {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unable to decrypt ciphertext: {}.", e),
            )))
        }
    };
    // CIPHERTEXT [?-bytes] | SALT [32-bytes] | NONCE [24-bytes]
    // Append salt to end of ciphertext; needed for decryption
    ciphertext.extend_from_slice(&salt);
    // Append nonce to end of ciphertext; needed for decryption
    ciphertext.extend_from_slice(nonce);
    // Zeroize arguments
    // key and nonce are not zeroized since
    // they reference the below vectors
    salt.zeroize();
    hashed_secret.zeroize();
    nonce_raw.zeroize();
    // Ship it
    Ok(ciphertext)
}

pub fn decrypt(ciphertext: &[u8], secret: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    // Pop off salt & nonce from ciphertext for decryption
    let (mut salt_vec, mut nonce_vec, cipher_vec) = extract_salt_nonce(ciphertext);
    // argon2 32-byte secret
    let mut hashed_secret = argon2::hash_raw(secret.as_bytes(), &salt_vec, &ARGON_CONF)?;
    let key = Key::from_slice(&hashed_secret);
    // New XChaCha20Poly1305 from hashed_secret
    let aead = XChaCha20Poly1305::new(key);
    // Generate generic array from popped off nonce
    let nonce = XNonce::from_slice(&nonce_vec);
    // Decrypt
    let plaintext = match aead.decrypt(nonce, cipher_vec.as_ref()) {
        Ok(hs) => hs,
        Err(e) => {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unable to decrypt ciphertext: {}.", e),
            )))
        }
    };
    // Zeroize arguments
    // key and nonce are not zeroized since
    // they reference the below vectors
    salt_vec.zeroize();
    nonce_vec.zeroize();
    hashed_secret.zeroize();
    // Ship it
    Ok(plaintext)
}

// Extracts the nonce from the end of the ciphertext
fn extract_salt_nonce(ciphertext: &[u8]) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    // Pop off nonce from ciphertext for decryption
    let mut cipher_vec = ciphertext.to_vec();
    let mut nonce_vec = VecDeque::with_capacity(NONCE_LEN);
    let mut salt_vec = VecDeque::with_capacity(SALT_LEN);
    // CIPHERTEXT [?-bytes] | SALT [32-bytes] | NONCE [24-bytes]
    // Start with nonce since it is appended last
    // Pop off the last 24 bytes of ciphertext
    for _ in 0..NONCE_LEN {
        // We always expect pop to produce Some
        let nonce_byte = cipher_vec
            .pop()
            .expect("error popping nonce from ciphertext");
        // Push front since pop works in reverse
        // on the back of the Vec
        nonce_vec.push_front(nonce_byte);
    }
    // Pop off the last 32 bytes of ciphertext
    for _ in 0..SALT_LEN {
        // We always expect pop to produce Some
        let salt_byte = cipher_vec
            .pop()
            .expect("error popping salt from ciphertext");
        // Push front since pop works in reverse
        // on the back of the Vec
        salt_vec.push_front(salt_byte);
    }
    // Ship it
    (Vec::from(salt_vec), Vec::from(nonce_vec), cipher_vec)
}

pub fn keyring_get_secret(job_name: &str) -> Result<String, Box<dyn Error>> {
    let entry = Entry::new("com.ciehanski.kip", job_name);
    let password = entry.get_password()?;
    Ok(password)
}

pub fn keyring_set_secret(job_name: &str, password: &str) -> Result<(), Box<dyn Error>> {
    let entry = Entry::new("com.ciehanski.kip", job_name);
    entry.set_password(password)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::read;

    #[test]
    fn test_encrypt() {
        let encrypt_result = encrypt(
            b"Super secure information. Please do not share or read.",
            "hunter2",
        );
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
        let encrypt_result = encrypt(&contents, "hunter2");
        assert!(encrypt_result.is_ok());
        assert_ne!(contents, encrypt_result.unwrap())
    }

    #[test]
    fn test_decrypt() {
        let encrypt_result = encrypt(
            b"Super secure information. Please do not share or read.",
            "hunter2",
        );
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
        let encrypt_result = encrypt(&contents, "hunter2");
        assert!(encrypt_result.is_ok());
        let decrypt_result = decrypt(&encrypt_result.unwrap(), "hunter2");
        assert!(decrypt_result.is_ok());
        let decrypted = decrypt_result.unwrap();
        assert_eq!(decrypted, contents)
    }

    #[test]
    fn test_extract_salt_nonce() {
        let encrypted = b"secret00000000000000000000000000000000111111111111111111111111";
        let (salt, nonce, cipher) = extract_salt_nonce(encrypted);
        assert_eq!(salt.len(), SALT_LEN);
        assert_eq!(salt, b"00000000000000000000000000000000");
        assert_eq!(nonce.len(), NONCE_LEN);
        assert_eq!(nonce, b"111111111111111111111111");
        assert_eq!(cipher, b"secret")
    }
}
