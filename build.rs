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
    let output = Command::new("git").args(["rev-parse", "HEAD"]).output();
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

    let mut deps: Vec<DepInfo> = lockfile
        .packages
        .into_iter()
        .map(|pkg| DepInfo {
            name: pkg.name.as_str().to_string(),
            version: pkg.version.to_string(),
            checksum: pkg.checksum.map(|c| c.to_string()),
            source: pkg.source.map(|s| s.to_string()),
        })
        .collect();

    // From github actions
    println!("cargo:rerun-if-env-changed=EXT_DAV1D_VER");
    println!("cargo:rerun-if-env-changed=EXT_DAV1D_HASH");
    println!("cargo:rerun-if-env-changed=EXT_HEIF_VER");
    println!("cargo:rerun-if-env-changed=EXT_HEIF_HASH");
    println!("cargo:rerun-if-env-changed=EXT_AOM_VER");
    println!("cargo:rerun-if-env-changed=EXT_AOM_HASH");

    let mut add_external_lib = |name: &str, ver_key: &str, hash_key: &str| {
        if let Ok(ver) = env::var(ver_key) {
            let hash = env::var(hash_key).ok();
            deps.push(DepInfo {
                name: name.to_string(),
                version: ver,
                checksum: hash,
                source: Some("GitHub Action Manual Build".to_string()),
            });
        }
    };

    add_external_lib("dav1d (C-Lib)", "EXT_DAV1D_VER", "EXT_DAV1D_HASH");
    add_external_lib("libheif (C-Lib)", "EXT_HEIF_VER", "EXT_HEIF_HASH");
    add_external_lib("libaom (C-Lib)", "EXT_AOM_VER", "EXT_AOM_HASH");

    let json_info = serde_json::to_string(&deps).expect("Failed to serialize deps");

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("deps_info.json");
    fs::write(&dest_path, json_info).expect("Failed to write info to file");
    println!("cargo:rustc-env=DEPS_INFO_PATH={}", dest_path.display());
}
