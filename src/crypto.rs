use aead::{generic_array::GenericArray, Aead, NewAead};
use argon2::{self, Config, ThreadMode, Variant, Version};
use chacha20poly1305::XChaCha20Poly1305;
use rand::rngs::OsRng;
use rand::Rng;
use sha3::digest::generic_array::typenum::U32;
use sha3::{Digest, Sha3_256};

pub fn encrypt(plaintext: &[u8], secret: &str) -> Result<Vec<u8>, aead::Error> {
    // SHA3-256 32-byte secret
    let hashed_secret = hash_secret(secret);
    let key = GenericArray::from_slice(&hashed_secret);
    // New XChaCha20Poly1305 from hashed_secret
    let aead = XChaCha20Poly1305::new(key);
    // Generate nonce - 24-bytes; unique
    let nonce_str = generate_nonce(24);
    let nonce = GenericArray::from_slice(nonce_str.as_bytes());
    // Encrypt
    let mut ciphertext = aead.encrypt(nonce, plaintext.as_ref())?;
    // Embed nonce to end of ciphertext; needed for decryption
    ciphertext.append(&mut nonce_str.as_bytes().to_vec());
    // Ship it
    Ok(ciphertext)
}

pub fn decrypt(ciphertext: &[u8], secret: &str) -> Result<Vec<u8>, aead::Error> {
    // SHA3-256 32-byte secret
    let hashed_secret = hash_secret(secret);
    let key = GenericArray::from_slice(&hashed_secret);
    // New XChaCha20Poly1305 from hashed_secret
    let aead = XChaCha20Poly1305::new(key);
    // Pop off nonce from ciphertext for decryption
    let (nonce_vec, cipher_vec) = extract_nonce(ciphertext);
    // Generate generic array from popped off nonce
    let nonce = GenericArray::from_slice(&nonce_vec);
    // Decrypt
    let plaintext = aead.decrypt(nonce, cipher_vec.as_ref())?;
    // Ship it
    Ok(plaintext)
}

pub fn argon_hash_secret(secret: &str) -> Result<String, argon2::Error> {
    // Generate a 16-byte salt
    let salt = generate_nonce(16);
    // Configure argon2 parameters
    let config = Config {
        variant: Variant::Argon2id,
        version: Version::Version13,
        mem_cost: 65536,
        time_cost: 2,
        lanes: 4,
        thread_mode: ThreadMode::Parallel,
        secret: &[],
        ad: &[],
        hash_length: 32,
    };
    // Generate hash from secret, salt, and argon2 config
    let hash = argon2::hash_encoded(secret.as_bytes(), salt.as_bytes(), &config)?;
    // Ship it
    Ok(hash)
}

pub fn compare_argon_secret(secret: &str, hash: &str) -> Result<bool, argon2::Error> {
    let matches = argon2::verify_encoded(&hash, secret.as_bytes())?;
    Ok(matches)
}

// Generates a 32-byte hash of a provided secret.
fn hash_secret(secret: &str) -> GenericArray<u8, U32> {
    // Create a SHA3-256 digest
    let mut hasher = Sha3_256::new();
    // Write password's bytes into hasher
    hasher.update(secret.as_bytes());
    // Return SHA3-256 32-byte secret hash
    hasher.finalize()
}

// Generates a 24-byte string of random numbers.
fn generate_nonce(len: usize) -> String {
    // Loop 24 times to create 24-byte String of random numbers.
    // Use OsRng to generate more "secure" random randomness.
    let mut nonce = String::new();
    for _ in 0..len {
        let rn = OsRng.gen_range(0, 9);
        nonce.push_str(&rn.to_string());
    }
    // Ship it
    nonce
}

// Extracts the nonce from the end of the ciphertext
fn extract_nonce(ciphertext: &[u8]) -> (Vec<u8>, Vec<u8>) {
    // Pop off nonce from ciphertext for decryption
    let mut cipher_vec = ciphertext.to_vec();
    let mut nonce_vec: Vec<u8> = Vec::with_capacity(24);
    // Pop off the last 24 bytes of ciphertext
    for _ in 0..24 {
        let nonce_byte = cipher_vec.pop().unwrap_or_default();
        nonce_vec.push(nonce_byte);
    }
    // Reverse nonce since pop works backwards
    let nonce_vec: Vec<u8> = nonce_vec.into_iter().rev().collect();
    // Ship it
    (nonce_vec, cipher_vec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_nonce() {
        let t = generate_nonce(24);
        assert_eq!(t.len(), 24);
    }

    #[test]
    fn test_encrypt() {
        match encrypt(
            b"Super secure information. Please do not share or read.",
            "hunter2",
        ) {
            Ok(b) => b,
            Err(e) => panic!("{:#?}", e),
        };
    }

    #[test]
    fn test_decrypt() {
        let en = encrypt(
            b"Super secure information. Please do not share or read.",
            "hunter2",
        )
        .expect("failed to encrypt");
        let de = match decrypt(&en, "hunter2") {
            Ok(b) => b,
            Err(e) => panic!("{:#?}", e),
        };
        assert_eq!(
            std::str::from_utf8(&de).expect("failed to convert utf-8 to str"),
            "Super secure information. Please do not share or read."
        )
    }

    #[test]
    fn test_extract_nonce() {
        let encrypted = encrypt(
            b"Super secure information. Please do not share or read.",
            "hunter2",
        )
        .unwrap();
        let (nonce, _) = extract_nonce(&encrypted);
        assert_eq!(nonce.len(), 24);
        let decrypted = decrypt(&encrypted, "hunter2").unwrap();
        assert_eq!(
            std::str::from_utf8(&decrypted).expect("failed to convert utf-8 to str"),
            "Super secure information. Please do not share or read."
        )
    }

    #[test]
    fn test_argon() {
        let hash = argon_hash_secret("hunter2").expect("should encrypt");
        assert!(compare_argon_secret("hunter2", &hash).unwrap())
    }
}
