use std::path::Path;
use std::{env, process};

use crate::phash::*;

mod phash;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <image_file_path>", args[0]);
        process::exit(1);
    }

    let file_path = &args[1];
    let path = Path::new(file_path);

    // image::open detects format automatically from file extension/magic bytes
    let img = match image::open(path) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("Error opening file '{}': {}", file_path, e);
            process::exit(1);
        }
    };

    // Initialize Hasher
    let hasher = DctPhash::new();

    // Calculate Standard Hash
    let hash = hasher.hash_image(&img);
    println!("File: {}", file_path);
    println!("Standard pHash (Hex): {:016x}", hash);
    println!("Standard pHash (Bin): {:064b}", hash);

    // Calculate Rotation Invariant Hash (Optional)
    // This is the single smallest hash among 0, 90, 180, 270 degree rotations
    let invariant_hash = calculate_rotation_invariant_hash(hash);
    println!("Rot-Invariant Hash  : {:016x}", invariant_hash);
}
