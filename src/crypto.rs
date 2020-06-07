use aead::{generic_array::GenericArray, Aead, NewAead};
use chacha20poly1305::XChaCha20Poly1305;
use rand::{thread_rng, Rng};
use sha3::{Digest, Sha3_256};

pub fn encrypt(plaintext: &[u8], secret: &str) -> Result<(Vec<u8>, String), aead::Error> {
    // create a SHA3-256 object
    let mut hasher = Sha3_256::new();
    // write password
    hasher.input(secret.as_bytes());
    // SHA3-256 32-byte secret
    let hashed_secret = hasher.result();
    // New XChaCha20Poly1305 from key
    let aead = XChaCha20Poly1305::new(hashed_secret);
    // Generate nonce - 24-bytes; unique
    let nonce_str = generate_nonce();
    let nonce = GenericArray::from_slice(&nonce_str.as_bytes());
    // Encrypt
    // TODO: Attach to end of ciphertext for decryption
    let ciphertext = aead.encrypt(nonce, plaintext.as_ref())?;
    // Ship it
    Ok((ciphertext, nonce_str))
}

pub fn decrypt(ciphertext: &[u8], secret: &str, nonce_str: &str) -> Result<Vec<u8>, aead::Error> {
    // create a SHA3-256 object
    let mut hasher = Sha3_256::new();
    // write password
    hasher.input(secret.as_bytes());
    // SHA3-256 32-byte secret
    let hashed_secret = hasher.result();
    // New XChaCha20Poly1305 from hashed secret
    let aead = XChaCha20Poly1305::new(hashed_secret);
    // Generate nonce - 24-bytes; unique
    let nonce = GenericArray::from_slice(nonce_str.as_bytes());
    // Decrypt
    let plaintext = aead.decrypt(nonce, ciphertext.as_ref())?;
    // Ship it
    Ok(plaintext)
}

// Generates a 24-byte string of random numbers.
fn generate_nonce() -> String {
    let mut nonce = String::new();

    for _ in 0..24 {
        let rn = thread_rng().gen_range(0, 9);
        nonce.push_str(&rn.to_string());
    }

    nonce
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
        let (en, nonce) = encrypt(
            b"Super secure information. Please do not share or read.",
            "hunter2",
        )
        .expect("failed to encrypt");
        let de = match decrypt(&en, "hunter2", &nonce) {
            Ok(b) => b,
            Err(e) => panic!("{:#?}", e),
        };
        assert_eq!(
            std::str::from_utf8(&de).expect("failed to convert utf-8 to str"),
            "Super secure information. Please do not share or read."
        )
    }
}
