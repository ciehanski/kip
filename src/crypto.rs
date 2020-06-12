use aead::{generic_array::GenericArray, Aead, NewAead};
use chacha20poly1305::XChaCha20Poly1305;
use rand::rngs::OsRng;
use rand::Rng;
use sha3::{Digest, Sha3_256};

pub fn encrypt(plaintext: &[u8], secret: &str) -> Result<Vec<u8>, aead::Error> {
    // Create a SHA3-256 object
    let mut hasher = Sha3_256::new();
    // Write password's bytes into hasher
    hasher.update(secret.as_bytes());
    // SHA3-256 32-byte secret
    let hashed_secret = hasher.finalize();
    let key = GenericArray::from_slice(&hashed_secret);
    // New XChaCha20Poly1305 from hashed_secret
    let aead = XChaCha20Poly1305::new(key);
    // Generate nonce - 24-bytes; unique
    let nonce_str = generate_nonce();
    let nonce = GenericArray::from_slice(nonce_str.as_bytes());
    // Encrypt
    let mut ciphertext = aead.encrypt(nonce, plaintext.as_ref())?;
    // Embed nonce to end of ciphertext; needed for decryption
    let mut nonce_vec = nonce_str.as_bytes().to_vec();
    ciphertext.append(&mut nonce_vec);
    // Ship it
    Ok(ciphertext)
}

pub fn decrypt(ciphertext: &[u8], secret: &str) -> Result<Vec<u8>, aead::Error> {
    // Create a SHA3-256 object
    let mut hasher = Sha3_256::new();
    // Write password's bytes into hasher
    hasher.update(secret.as_bytes());
    // SHA3-256 32-byte secret
    let hashed_secret = hasher.finalize();
    let key = GenericArray::from_slice(&hashed_secret);
    // New XChaCha20Poly1305 from hashed_secret
    let aead = XChaCha20Poly1305::new(key);
    // Pop off nonce from ciphertext for decryption
    let (nonce_vec, cipher_vec) = extract_nonce(ciphertext);
    // Generate nonce - 24-bytes; unique
    let nonce = GenericArray::from_slice(&nonce_vec);
    // Decrypt
    let plaintext = aead.decrypt(nonce, cipher_vec.as_ref())?;
    // Ship it
    Ok(plaintext)
}

// Generates a 24-byte string of random numbers.
fn generate_nonce() -> String {
    // Loop 24 times to create 24-byte String
    // of random numbers. Use OsRng to generate
    // more "secure" random randomness.
    let mut nonce = String::new();
    for _ in 0..24 {
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
    use super::{decrypt, encrypt, generate_nonce};

    #[test]
    fn test_generate_nonce() {
        let t = generate_nonce();
        if t.len() != 24 {
            panic!("nonce should be 24-bytes in length")
        }
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
}
