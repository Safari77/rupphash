use cargo_lock::Lockfile;
use serde::Serialize;
use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

#[derive(Serialize)]
struct DepInfo {
    name: String,
    version: String,
    checksum: Option<String>,
    source: Option<String>,
}

fn main() {
    // 1. Get Git Hash
    let output = Command::new("git").args(&["rev-parse", "HEAD"]).output();
    let git_hash = match output {
        Ok(o) if o.status.success() => String::from_utf8(o.stdout).unwrap().trim().to_string(),
        _ => "unknown".to_string(),
    };
    println!("cargo:rustc-env=APP_GIT_HASH={}", git_hash);
    println!("cargo:rerun-if-changed=.git/HEAD");

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let lock_path = Path::new(&manifest_dir).join("Cargo.lock");

    // Parse the lock file, rerun this build script if Cargo.lock changes
    println!("cargo:rerun-if-changed=Cargo.lock");
    let lockfile = Lockfile::load(lock_path).expect("Could not load Cargo.lock");

    let deps: Vec<DepInfo> = lockfile
        .packages
        .into_iter()
        .map(|pkg| DepInfo {
            name: pkg.name.as_str().to_string(),
            version: pkg.version.to_string(),
            checksum: pkg.checksum.map(|c| c.to_string()),
            source: pkg.source.map(|s| s.to_string()),
        })
        .collect();

    let json_info = serde_json::to_string(&deps).expect("Failed to serialize deps");

    // --- CHANGE STARTS HERE ---

    // 3. Write to file instead of env var
    // Grab the build output directory
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("deps_info.json");

    // Write the massive JSON string to disk
    fs::write(&dest_path, json_info).expect("Failed to write info to file");

    // 4. Pass the PATH to the file, not the content
    // We pass the absolute path so main.rs can find it easily
    println!("cargo:rustc-env=DEPS_INFO_PATH={}", dest_path.display());
}
