use anyhow::{Context, Result};
use monad_crypto::{
    certificate_signature::CertificateSignature,
    signing_domain::SigningDomain,
};
use monad_secp::{KeyPair, SecpSignature};
use sha3::{Digest, Keccak256};

/// Signing domain for block builder transaction bundles  
/// Must match monad-eth-txpool/src/pool/builder.rs BlockBuilderDomain
pub struct BlockBuilderDomain;
impl SigningDomain for BlockBuilderDomain {
    const PREFIX: &'static [u8] = b"MONAD_BLOCK_BUILDER_v1";
}

/// A keypair for signing builder bundles using Monad's secp256k1 implementation
pub struct BuilderKeypair {
    keypair: KeyPair,
}

impl BuilderKeypair {
    /// Generate a new random keypair
    pub fn generate() -> Self {
        let mut secret = [0u8; 32];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut secret);
        Self {
            keypair: KeyPair::from_bytes(&mut secret).expect("valid secret key"),
        }
    }

    /// Create from a hex-encoded private key
    pub fn from_hex(hex_str: &str) -> Result<Self> {
        let hex_str = hex_str.trim_start_matches("0x");
        let mut secret = hex::decode(hex_str).context("Invalid hex string")?;
        if secret.len() != 32 {
            anyhow::bail!("Private key must be 32 bytes");
        }
        Ok(Self {
            keypair: KeyPair::from_bytes(&mut secret).context("Invalid private key")?,
        })
    }

    /// Get the private key as hex string
    pub fn private_key_hex(&self) -> String {
        format!("{}", self.keypair.privkey_view())
    }

    /// Get the public key as compressed hex (33 bytes)
    pub fn public_key_hex(&self) -> String {
        hex::encode(self.keypair.pubkey().bytes_compressed())
    }

    /// Sign a builder bundle using Monad's signing implementation
    pub fn sign_bundle(&self, bundle_hash: &[u8; 32]) -> Result<Vec<u8>> {
        // Use Monad's sign function which automatically applies domain tagging with Blake3
        let signature: SecpSignature = self.keypair.sign::<BlockBuilderDomain>(bundle_hash);
        
        // Serialize using Monad's format: r(32) || s(32) || recovery_id(1)
        Ok(signature.serialize().to_vec())
    }
    
    /// Verify a bundle signature (for debugging)
    pub fn verify_bundle_signature(&self, bundle_hash: &[u8; 32], signature_bytes: &[u8]) -> Result<bool> {
        // Deserialize the signature
        let signature = SecpSignature::deserialize(signature_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize signature: {}", e))?;
        
        // Verify using Monad's implementation
        Ok(signature.verify::<BlockBuilderDomain>(bundle_hash, &self.keypair.pubkey()).is_ok())
    }
}

/// Compute the bundle hash that needs to be signed
/// bundle_hash = keccak256(tx_hash_1 || tx_hash_2 || ... || timestamp)
pub fn compute_bundle_hash(tx_hashes: &[&[u8; 32]], timestamp: u64) -> [u8; 32] {
    let mut data = Vec::new();
    
    for (i, tx_hash) in tx_hashes.iter().enumerate() {
        tracing::debug!("  [TEST] Bundle hash TX[{}]: 0x{}", i, hex::encode(tx_hash));
        data.extend_from_slice(*tx_hash);
    }
    
    let timestamp_bytes = timestamp.to_be_bytes();
    tracing::debug!("  [TEST] Bundle hash timestamp bytes: 0x{}", hex::encode(&timestamp_bytes));
    data.extend_from_slice(&timestamp_bytes);
    
    tracing::debug!("  [TEST] Bundle hash input data: 0x{}", hex::encode(&data));
    
    let mut hasher = Keccak256::new();
    hasher.update(&data);
    let result = hasher.finalize();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&result);
    
    tracing::debug!("  [TEST] Computed bundle hash: 0x{}", hex::encode(&hash));
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keypair_generation() {
        let keypair = BuilderKeypair::generate();
        let pubkey = keypair.public_key_hex();
        
        // Compressed pubkey should be 66 hex chars (33 bytes)
        assert_eq!(pubkey.len(), 66);
    }

    #[test]
    fn test_bundle_signing() {
        let keypair = BuilderKeypair::generate();
        let bundle_hash = [0u8; 32];
        let signature = keypair.sign_bundle(&bundle_hash).unwrap();
        
        // Signature should be 65 bytes (r || s || recovery_id)
        assert_eq!(signature.len(), 65);
        
        // Self-verification should work
        assert!(keypair.verify_bundle_signature(&bundle_hash, &signature).unwrap());
    }
}
