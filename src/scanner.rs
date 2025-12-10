use std::path::{Path};
use std::fs;
use std::collections::{HashMap, HashSet};
use std::time::UNIX_EPOCH;
use std::os::unix::fs::MetadataExt;
use rayon::prelude::*;
use walkdir::WalkDir;
use chrono::{DateTime, Utc};
use crossbeam_channel::{unbounded, Sender};
use image::GenericImageView;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::{FileMetadata, GroupInfo, GroupStatus};
use crate::rupphash::DctPhash;
use crate::mih::{MIHIndex, find_groups_parallel};
use crate::db::AppContext;

pub const RAW_EXTS: &[&str] = &["nef", "dng", "cr2", "cr3", "arw", "orf", "rw2", "raf"];

// --- Trait Alias Helper ---
// This allows us to create a Box<dyn BufReadSeek> that satisfies both requirements.
trait BufReadSeek: std::io::BufRead + std::io::Seek {}
impl<T: std::io::BufRead + std::io::Seek> BufReadSeek for T {}

/// Helper to get EXIF orientation efficiently (standard + RAW/TIFF)
/// Reads just the header of the file to find orientation tag using kamadak-exif.
/// TODO use rsraw to read orientation if kamadak fails
fn get_orientation(path: &Path, preloaded_bytes: Option<&[u8]>) -> u8 {
    // We use the composite trait object Box<dyn BufReadSeek> so both branches return the same type.
    let mut reader: Box<dyn BufReadSeek> = if let Some(bytes) = preloaded_bytes {
        // Cursor implements both BufRead and Seek
        Box::new(std::io::Cursor::new(bytes))
    } else {
        match fs::File::open(path) {
            // BufReader<File> implements both BufRead and Seek
            Ok(f) => Box::new(std::io::BufReader::new(f)),
            Err(_) => return 1,
        }
    };

    match exif::Reader::new().read_from_container(&mut reader) {
        Ok(exif_data) => {
            if let Some(field) = exif_data.get_field(exif::Tag::Orientation, exif::In::PRIMARY) {
                 match field.value.get_uint(0) {
                    Some(v @ 1..=8) => return v as u8,
                    _ => {}
                 }
            }
            1
        },
        Err(_) => 1
    }
}

/// Get resolution for a file, using rsraw for RAW files
fn get_resolution(path: &Path, bytes: Option<&[u8]>) -> Option<(u32, u32)> {
    if is_raw_ext(path) {
        // Use rsraw for RAW files
        let data = match bytes {
            Some(b) => b.to_vec(),
            None => fs::read(path).ok()?,
        };
        if let Ok(mut raw) = rsraw::RawImage::open(&data) {
            if raw.unpack().is_ok() {
                // Get dimensions from unpacked raw (before processing)
                return Some((raw.width() as u32, raw.height() as u32));
            }
        }
        None
    } else {
        // Use image crate for regular images
        match bytes {
            Some(b) => {
                if let Ok(reader) = image::ImageReader::new(std::io::Cursor::new(b)).with_guessed_format()
                    && let Ok(dims) = reader.into_dimensions() {
                    Some(dims)
                } else {
                    None
                }
            }
            None => {
                if let Ok(reader) = image::ImageReader::open(path)
                    && let Ok(dims) = reader.into_dimensions() {
                    Some(dims)
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct ScanConfig {
    pub paths: Vec<String>,
    pub rehash: bool,
    pub similarity: u32,
    pub group_by: String,
    pub extensions: Vec<String>,
    pub ignore_same_stem: bool,
}

// Update signature to accept optional progress sender
pub fn scan_and_group(
    config: &ScanConfig,
    ctx: &AppContext,
    progress_tx: Option<Sender<(usize, usize)>> // (current, total)
) -> (Vec<Vec<FileMetadata>>, Vec<GroupInfo>) {
    let ctx_ref = ctx;
    let force_rehash = config.rehash;

    // 1. File Walking
    let mut all_files = Vec::new();
    let mut seen_paths = HashSet::new();
    for path_str in &config.paths {
        let path = Path::new(path_str);
        if path.is_dir() {
            for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
                if is_image_ext(entry.path()) {
                    // Canonicalize to handle symlinks and avoid duplicates
                    if let Ok(canonical) = entry.path().canonicalize() {
                        if seen_paths.insert(canonical.clone()) {
                            all_files.push(canonical);
                        }
                    }
                }
            }
        } else if path.is_file() && is_image_ext(path) {
            if let Ok(canonical) = path.canonicalize() {
                if seen_paths.insert(canonical.clone()) {
                    all_files.push(canonical);
                }
            }
        }
    }

    if all_files.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let total_files = all_files.len();
    if let Some(tx) = &progress_tx {
        let _ = tx.send((0, total_files));
    }

    // 2. Database Writer & Hashing
    let (tx, rx) = unbounded();
    let db_handle = ctx.start_db_writer(rx);
    let hasher = DctPhash::new();

    let processed_count = AtomicUsize::new(0);

    let valid_files: Vec<FileMetadata> = all_files.par_iter().filter_map(|path| {
        // --- Progress Reporting ---
        if let Some(prog_tx) = &progress_tx {
            let current = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
            // Limit updates to reduce overhead (e.g., every 50 files)
            if current % 50 == 0 || current == total_files {
                let _ = prog_tx.send((current, total_files));
            }
        }
        // --------------------------

        let metadata = fs::metadata(path).ok()?;
        let size = metadata.len();
        let mtime = metadata.modified().ok().unwrap_or(UNIX_EPOCH);
        let mtime_utc: DateTime<Utc> = DateTime::from(mtime);
        let mtime_ns = mtime.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        let inode = metadata.ino();
        let dev_inode = Some((metadata.dev(), metadata.ino()));

        // Meta Key
        let mut mh = blake3::Hasher::new_keyed(&ctx_ref.meta_key);
        mh.update(&mtime_ns.to_le_bytes());
        mh.update(&inode.to_le_bytes());
        mh.update(&size.to_le_bytes());
        let meta_key: [u8; 32] = *mh.finalize().as_bytes();

        let mut phash = 0u64;
        let mut new_meta = None;
        let mut new_phash = None;
        let mut computed = false;
        let mut resolution = None;
        let mut ck = [0u8; 32];
        let mut orientation = 1;

        // Try reading full file for hashing (needed anyway)
        let bytes = fs::read(path).ok();

        if let Some(ref b) = bytes {
            // Get orientation from bytes (fast header read)
            orientation = get_orientation(path, Some(b));
        }

        if !force_rehash
            && let Ok(Some(ch)) = ctx_ref.get_content_hash(&meta_key) {
                 ck = ch;
                 if let Ok(Some(p)) = ctx_ref.get_phash(&ch) {
                    phash = p;
                    computed = true;
                    // If we didn't read bytes, read orientation from file
                    if bytes.is_none() {
                        orientation = get_orientation(path, None);
                    }
                    resolution = get_resolution(path, None);
                }
            }

        if !computed {
             let b = match bytes { Some(v) => v, None => return None };
             let ch = blake3::keyed_hash(&ctx_ref.content_key, &b);
             ck = *ch.as_bytes();
             new_meta = Some((meta_key, ck));

             if let Ok(Some(p)) = ctx_ref.get_phash(&ck) {
                 phash = p;
                 resolution = get_resolution(path, Some(&b));
             } else if let Ok(img) = image::load_from_memory(&b) {
                 resolution = Some(img.dimensions());
                 phash = hasher.hash_image_invariant(&img);
                 new_phash = Some((ck, phash));
             } else { return None; }
        }

        if new_meta.is_some() || new_phash.is_some() { let _ = tx.send((new_meta, new_phash)); }

        Some(FileMetadata {
            path: path.clone(), size, modified: mtime_utc, phash, resolution,
            content_hash: ck, orientation, dev_inode, })
    }).collect();

    drop(tx);
    db_handle.join().expect("DB writer thread panicked");

    // 3. Grouping
    let hashes: Vec<u64> = valid_files.iter().map(|f| f.phash).collect();
    if hashes.is_empty() { return (Vec::new(), Vec::new()); }

    let mih = MIHIndex::new(hashes.clone()).expect("Failed to create MIH");
    let raw_groups = find_groups_parallel(&mih, config.similarity);

    let mut processed_groups = Vec::new();
    let mut processed_infos = Vec::new();

    let ignore_enabled = config.ignore_same_stem;
    let allowed_exts: HashSet<String> = config.extensions.iter().map(|e| e.to_lowercase()).collect();
    let ext_priorities: HashMap<String, usize> = config.extensions.iter().enumerate().map(|(i, e)| (e.to_lowercase(), i)).collect();

    for group_indices in raw_groups {
        let mut group_data: Vec<FileMetadata> = group_indices.iter().map(|&idx| valid_files[idx as usize].clone()).collect();

        // Filter logic
        if ignore_enabled && group_data.len() >= 2 {
            let first_parent = group_data[0].path.parent();
            let first_stem = group_data[0].path.file_stem();
            let all_same_base = group_data.iter().all(|f| f.path.parent() == first_parent && f.path.file_stem() == first_stem);

            if all_same_base {
                let mut valid_exts = true;
                let mut unique_norm_exts = HashSet::new();
                for f in &group_data {
                    if let Some(ext) = f.path.extension().and_then(|e| e.to_str()) {
                        let lower = ext.to_lowercase();
                        if !allowed_exts.contains(&lower) { valid_exts = false; break; }
                        let norm = if lower == "jpeg" { "jpg" } else { &lower };
                        unique_norm_exts.insert(norm.to_string());
                    } else { valid_exts = false; break; }
                }
                if valid_exts && unique_norm_exts.len() >= 2 { continue; }
            }
        }

        let info = analyze_group(&mut group_data, &config.group_by.to_lowercase(), &ext_priorities);
        processed_groups.push(group_data);
        processed_infos.push(info);
    }

    // Sort Groups
    let mut combined: Vec<_> = processed_groups.into_iter().zip(processed_infos).collect();
    combined.sort_by(|(g1, info1), (g2, info2)| {
        let has_ident1 = info1.status != GroupStatus::None;
        let has_ident2 = info2.status != GroupStatus::None;
        if has_ident1 != has_ident2 { return has_ident2.cmp(&has_ident1); }
        if info1.max_dist != info2.max_dist { return info1.max_dist.cmp(&info2.max_dist); }
        let s1 = g1.first().map(|f| f.size).unwrap_or(0);
        let s2 = g2.first().map(|f| f.size).unwrap_or(0);
        s2.cmp(&s1)
    });

    combined.into_iter().unzip()
}

pub fn analyze_group(
    files: &mut Vec<FileMetadata>,
    group_by: &str,
    ext_priorities: &HashMap<String, usize>
) -> GroupInfo {
    if files.is_empty() {
        return GroupInfo { max_dist: 0, status: GroupStatus::None };
    }

    let mut counts = HashMap::new();
    for f in files.iter() {
        *counts.entry(f.content_hash).or_insert(0) += 1;
    }

    let (mut duplicates, mut unique): (Vec<FileMetadata>, Vec<FileMetadata>) = files.drain(..)
        .partition(|f| *counts.get(&f.content_hash).unwrap_or(&0) > 1);

    let sorter = |a: &FileMetadata, b: &FileMetadata| {
        if group_by == "date" {
            a.modified.cmp(&b.modified)
        } else {
            let ext_a = a.path.extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
            let ext_b = b.path.extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();

            let prio_a = ext_priorities.get(&ext_a).unwrap_or(&usize::MAX);
            let prio_b = ext_priorities.get(&ext_b).unwrap_or(&usize::MAX);

            match prio_a.cmp(prio_b) {
                std::cmp::Ordering::Equal => b.size.cmp(&a.size),
                other => other,
            }
        }
    };

    duplicates.sort_by(sorter);
    unique.sort_by(sorter);

    files.append(&mut duplicates);
    files.append(&mut unique);

    let pivot_phash = files[0].phash;
    let max_d = files.iter().map(|f| (f.phash ^ pivot_phash).count_ones()).max().unwrap_or(0);

    let has_duplicates = !counts.values().all(|&c| c == 1);
    let all_identical = counts.len() == 1;

    let status = if all_identical {
        GroupStatus::AllIdentical
    } else if has_duplicates {
        GroupStatus::SomeIdentical
    } else {
        GroupStatus::None
    };

    GroupInfo {
        max_dist: max_d,
        status,
    }
}

pub fn is_raw_ext(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| {
            let e = e.to_lowercase();
            RAW_EXTS.contains(&e.as_str())
        })
        .unwrap_or(false)
}

fn is_image_ext(path: &Path) -> bool {
    path.extension()
        .and_then(|s| s.to_str())
        .map(|ext| {
            let e = ext.to_lowercase();
            // Check standard extensions OR the centralized raw list
            matches!(
                e.as_str(),
                "jpg" | "jpeg" | "png" | "webp" | "bmp" | "tiff" | "tif" | 
                "avif" | "tga" | "xbm" | "xpm"
            ) || RAW_EXTS.contains(&e.as_str())
        })
        .unwrap_or(false)
}

/// Scan files for view mode (no similarity checking)
/// Returns files in current directory only (non-recursive), sorted according to sort_order
/// Also returns list of subdirectories
pub fn scan_for_view(
    paths: &[String],
    sort_order: &str,
    progress_tx: Option<Sender<(usize, usize)>>
) -> (Vec<Vec<FileMetadata>>, Vec<GroupInfo>, Vec<std::path::PathBuf>) {
    use rand::seq::SliceRandom;

    // 1. File Walking - NON-RECURSIVE, current directory only
    let mut all_files = Vec::new();
    let mut subdirs = Vec::new();
    let mut seen_paths = HashSet::new();

    for path_str in paths {
        let path = Path::new(path_str);
        if path.is_dir() {
            // Read directory entries directly (non-recursive)
            if let Ok(entries) = fs::read_dir(path) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let entry_path = entry.path();
                    if entry_path.is_dir() {
                        // Collect subdirectories
                        if let Ok(canonical) = entry_path.canonicalize() {
                            subdirs.push(canonical);
                        }
                    } else if entry_path.is_file() && is_image_ext(&entry_path) {
                        // Collect image files
                        if let Ok(canonical) = entry_path.canonicalize() {
                            if seen_paths.insert(canonical.clone()) {
                                all_files.push(canonical);
                            }
                        }
                    }
                }
            }
        } else if path.is_file() && is_image_ext(path) {
            if let Ok(canonical) = path.canonicalize() {
                if seen_paths.insert(canonical.clone()) {
                    all_files.push(canonical);
                }
            }
        }
    }

    // Sort subdirectories by name
    subdirs.sort_by(|a, b| {
        a.file_name().cmp(&b.file_name())
    });

    if all_files.is_empty() {
        return (Vec::new(), Vec::new(), subdirs);
    }

    let total_files = all_files.len();
    if let Some(tx) = &progress_tx {
        let _ = tx.send((0, total_files));
    }

    let processed_count = AtomicUsize::new(0);

    // 2. Gather metadata (no hashing needed for view mode)
    let mut files: Vec<FileMetadata> = all_files.par_iter().filter_map(|path| {
        if let Some(prog_tx) = &progress_tx {
            let current = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
            if current % 50 == 0 || current == total_files {
                let _ = prog_tx.send((current, total_files));
            }
        }

        let metadata = fs::metadata(path).ok()?;
        let size = metadata.len();
        let mtime = metadata.modified().ok().unwrap_or(UNIX_EPOCH);
        let mtime_utc: DateTime<Utc> = DateTime::from(mtime);

        // We check orientation even in View Mode
        // This requires opening the file, but it's fast (buffered header only).
        // get_resolution is skipped for speed.
        let orientation = get_orientation(path, None);

        Some(FileMetadata {
            path: path.clone(),
            size,
            modified: mtime_utc,
            phash: 0, // Not needed for view mode
            resolution: None,
            content_hash: [0u8; 32], // Not needed for view mode
            orientation,
            dev_inode: None,
        })
    }).collect();

    // 3. Sort according to sort_order
    match sort_order {
        "name" => files.sort_by(|a, b| a.path.cmp(&b.path)),
        "name-desc" => files.sort_by(|a, b| b.path.cmp(&a.path)),
        "date" => files.sort_by(|a, b| a.modified.cmp(&b.modified)),
        "date-desc" => files.sort_by(|a, b| b.modified.cmp(&a.modified)),
        "size" => files.sort_by(|a, b| a.size.cmp(&b.size)),
        "size-desc" => files.sort_by(|a, b| b.size.cmp(&a.size)),
        "random" => {
            let mut rng = rand::rng();
            files.shuffle(&mut rng);
        },
        _ => {} // Keep original order
    }

    if files.is_empty() {
        return (Vec::new(), Vec::new(), subdirs);
    }

    // Return as single group with no duplicates status
    let info = GroupInfo {
        max_dist: 0,
        status: GroupStatus::None,
    };

    (vec![files], vec![info], subdirs)
}
