use magma_external_blockbuilder_tests::builder_crypto::BuilderKeypair;

fn main() {
    // The test builder private key (same as in main.rs)
    let private_key = "a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b7";
    
    let keypair = BuilderKeypair::from_hex(private_key)
        .expect("Failed to create keypair from private key");
    
    println!("Builder Private Key: {}", keypair.private_key_hex());
    println!("Builder Public Key:  {}", keypair.public_key_hex());
    println!();
    println!("Add this public key to your node.toml:");
    println!("block_builder_authorized_keys = [");
    println!("    \"{}\",", keypair.public_key_hex());
    println!("]");
}
