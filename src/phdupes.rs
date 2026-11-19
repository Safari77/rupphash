#![recursion_limit = "256"]
use std::{
    collections::HashMap,
    ffi::CStr,
    fs,
    io::{self, Write},
    os::raw::c_char,
    path::PathBuf,
};

use chrono::{DateTime, Utc};
use clap::Parser;
use geo::Point;
#[cfg(not(target_os = "windows"))]
use jemallocator::Jemalloc;
use jiff::Timestamp;
use serde::Deserialize;

use crate::{
    db::{AppContext, HashAlgorithm},
    scanner::ScanConfig,
    state::get_bit_identical_counts,
};

#[cfg(not(target_os = "windows"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// Define the structure to match what we serialized in build.rs
#[derive(Debug, Deserialize)]
struct DepInfo {
    name: String,
    version: String,
    checksum: Option<String>,
    source: Option<String>,
}

mod configs;
mod db;
mod fileops;
mod gui;
mod hdr;
mod raw_exif;
mod scanner;
mod search_index;
mod state;
mod ui;

use rupphash::{
    exif_extract, exif_types, hamminghash, helper_exif, image_features, pdqhash, position,
};

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub path: PathBuf,
    pub size: u64,
    pub modified: DateTime<Utc>,
    pub pdqhash: Option<[u8; 32]>,
    pub pdq_quality: Option<u16>, // PDQ quality 0-100, None if never hashed
    pub resolution: Option<(u32, u32)>,
    pub content_hash: [u8; 32],
    pub pixel_hash: Option<[u8; 32]>,
    pub orientation: u8, // Added: EXIF orientation (1-8)
    pub gps_pos: Option<Point<f64>>,
    pub unique_file_id: u128,        // Always has dev+inode
    pub exif_timestamp: Option<i64>, /* EXIF DateTimeOriginal or DateTimeDigitized (Unix epoch seconds) */
}

/// Render a stored PDQ quality for CLI output, flagging values that are below
/// the fuzzy-matching cutoff.
#[must_use]
fn format_pdq_quality(quality: Option<u16>) -> String {
    match quality {
        Some(q) if q < crate::scanner::PDQ_MIN_QUALITY => format!("{q} (low)"),
        Some(q) => q.to_string(),
        None => "n/a".to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupStatus {
    AllIdentical,
    SomeIdentical,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupInfo {
    pub max_dist: u32,
    pub status: GroupStatus,
}

// --- Runtime Version Checking for dav1d and heif ---
// If "staticbuild" is enabled, link statically
#[cfg_attr(feature = "staticbuild", link(name = "dav1d", kind = "static"))]
#[cfg_attr(not(feature = "staticbuild"), link(name = "dav1d"))]
unsafe extern "C" {
    // dav1d_version() returns a pointer to a static string
    fn dav1d_version() -> *const c_char;
}

#[cfg_attr(feature = "staticbuild", link(name = "heif", kind = "static"))]
#[cfg_attr(not(feature = "staticbuild"), link(name = "heif"))]
unsafe extern "C" {
    // heif_get_version() returns a pointer to a static string
    fn heif_get_version() -> *const c_char;
}

#[cfg_attr(feature = "staticbuild", link(name = "aom", kind = "static"))]
#[cfg_attr(not(feature = "staticbuild"), link(name = "aom"))]
unsafe extern "C" {
    fn aom_codec_version_str() -> *const c_char;
}

/// Converts a static NUL-terminated C string pointer into an owned `String`,
/// returning `"unknown"` if the pointer is null.
///
/// # Safety
/// If `ptr` is non-null, it must point to a valid NUL-terminated C string that
/// remains valid for the duration of this call.
unsafe fn cstr_or_unknown(ptr: *const c_char) -> String {
    if ptr.is_null() {
        return "unknown".to_string();
    }
    // SAFETY: Caller guarantees `ptr` is a valid, NUL-terminated C string when non-null.
    unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() }
}

#[must_use]
fn get_runtime_library_versions() -> String {
    // SAFETY: `dav1d_version`, `heif_get_version`, and `aom_codec_version_str` are
    // thread-safe C library functions that take no arguments and return a pointer
    // to a static, NUL-terminated version string (or null).
    let (dav1d_v, heif_v, aom_v) = unsafe {
        (
            cstr_or_unknown(dav1d_version()),
            cstr_or_unknown(heif_get_version()),
            cstr_or_unknown(aom_codec_version_str()),
        )
    };

    format!("dav1d: {dav1d_v}, libheif: {heif_v}, aom: {aom_v}")
}

// --------------------------------
// --- Helper: Relative Time ---
#[must_use]
pub fn format_relative_time(ts: Timestamp) -> String {
    let now = Timestamp::now();
    let zoned_ts = ts.to_zoned_datetime(jiff::tz::TimeZone::UTC);
    let raw_span = now.since(ts).unwrap_or_default();
    let total_secs = raw_span.total(jiff::Unit::Second).unwrap_or(0.0).abs();

    if total_secs < 60.0 {
        if total_secs < 0.001 {
            return "0s".to_string();
        }
        return format!("{total_secs:.3}s");
    }

    let span = raw_span
        .round(
            jiff::SpanRound::new()
                .largest(jiff::Unit::Year)
                .smallest(jiff::Unit::Second)
                .relative(&zoned_ts),
        )
        .unwrap_or_default();

    let mut parts = Vec::new();
    let y = span.get_years().abs();
    let mo = span.get_months().abs();
    let w = span.get_weeks().abs();
    let d = span.get_days().abs();
    let h = span.get_hours().abs();
    let m = span.get_minutes().abs();
    let s = span.get_seconds().abs();

    if y > 0 {
        parts.push(format!("{y}y"));
    }
    if mo > 0 {
        parts.push(format!("{mo}mo"));
    }
    if w > 0 {
        parts.push(format!("{w}w"));
    }
    if d > 0 {
        parts.push(format!("{d}d"));
    }
    if h > 0 {
        parts.push(format!("{h}h"));
    }
    if m > 0 {
        parts.push(format!("{m}m"));
    }
    if s > 0 {
        parts.push(format!("{s}s"));
    }

    if parts.is_empty() {
        return "0s".to_string();
    }
    parts.into_iter().take(3).collect::<Vec<_>>().join(" ")
}

// --- Analysis Logic ---
#[must_use]
pub fn analyze_group(
    files: &mut Vec<FileMetadata>,
    group_by: &str,
    ext_priorities: &HashMap<String, usize>,
) -> GroupInfo {
    // Delegate to scanner's analyze_group
    scanner::analyze_group(files, group_by, ext_priorities)
}

/// Build a normalized extension-to-priority lookup map (stripping leading dots and lowercasing).
#[must_use]
fn build_ext_priorities(extensions: &[String]) -> HashMap<String, usize> {
    extensions
        .iter()
        .enumerate()
        .map(|(i, e)| (e.trim_start_matches('.').to_ascii_lowercase(), i))
        .collect()
}

/// Filter out ignored files from duplicate groups, re-analyze modified groups,
/// drop groups with fewer than 2 remaining files, and register the surviving groups.
fn filter_and_register_groups(
    ctx: &AppContext,
    groups: &mut Vec<Vec<FileMetadata>>,
    infos: &mut Vec<GroupInfo>,
    sort_order: &str,
    ext_priorities: &HashMap<String, usize>,
) {
    let mut i = 0;
    while i < groups.len() {
        let before = groups[i].len();
        groups[i].retain(|f| f.content_hash == [0u8; 32] || !ctx.is_ignored(&f.content_hash));
        if groups[i].len() < 2 {
            groups.remove(i);
            if i < infos.len() {
                infos.remove(i);
            }
        } else {
            if groups[i].len() != before && i < infos.len() {
                infos[i] = analyze_group(&mut groups[i], sort_order, ext_priorities);
            }
            i += 1;
        }
    }

    let group_data: Vec<Vec<_>> = groups
        .iter()
        .map(|g| {
            g.iter()
                .filter(|f| f.content_hash != [0u8; 32])
                .map(|f| (f.content_hash, f.pdqhash))
                .collect()
        })
        .collect();

    if let Err(e) = ctx.register_duplicate_groups(&group_data) {
        eprintln!("[ERROR-IGNORE] Failed to register groups: {e:?}");
    }
}

// --- CLI Definition ---

#[derive(Parser, Debug)]
#[command(author, version, about = "Finds visually similar images.", long_about = None)]
struct Cli {
    #[arg(required_unless_present = "show_build_info")]
    #[arg(required_unless_present = "show_exif_tags")]
    #[arg(required_unless_present = "prune")]
    #[arg(required_unless_present = "show_ignored")]
    #[arg(required_unless_present = "import_ignored")]
    #[arg(required_unless_present = "unignore")]
    paths: Vec<String>,

    #[arg(long)]
    rehash: bool,
    #[arg(long)]
    rehash_only: bool,
    /// Similarity threshold (default: 40 for PDQ hash)
    #[arg(long)]
    similarity: Option<u32>,
    /// Calculate hash of raw pixel data to find content-identical files (e.g. PNG vs JPG)
    #[arg(long)]
    pixel_hash: bool,

    /// Sort order with --view: name, name-desc, name-natural, name-natural-desc, date, date-desc, size, size-desc, random, exif-date, exif-date-desc, location
    #[arg(long, default_value = "name")]
    sort: String,

    #[arg(long)]
    use_tui: bool,

    #[arg(long)]
    use_gui: bool,

    #[arg(long)]
    delete: bool,

    #[arg(long)]
    relative_times: bool,

    #[arg(long)]
    use_trash: bool,

    /// View mode: browse images without similarity checking
    #[arg(long)]
    view: bool,

    /// View mode with recursive directory scanning (flatten all subdirectories)
    #[arg(long)]
    view_flatten: bool,

    /// Shuffle images randomly (implies --view)
    #[arg(long)]
    shuffle: bool,

    /// Slideshow mode with interval in seconds (implies --view --use-gui)
    #[arg(long, value_name = "SECONDS")]
    slideshow: Option<f32>,

    /// Slideshow transition effect(s) (can specify multiple separated by comma)
    #[arg(long, value_name = "EFFECT", value_delimiter = ',')]
    slideshow_effect: Vec<gui::slideshow::SlideshowEffectChoice>,

    /// Path to 3D LUT file(s) (.cube format)
    #[arg(long = "3dlut", value_name = "FILE", num_args(1..))]
    luts3d: Vec<PathBuf>,

    /// Directory to move marked files to
    #[arg(long, value_name = "DIR")]
    move_marked: Option<PathBuf>,

    /// Use embedded thumbnails from RAW files instead of processing
    #[arg(long)]
    raw_thumbnails: bool,

    /// Show all supported EXIF tag names for use in exif_tags configuration
    #[arg(long)]
    show_exif_tags: bool,

    /// Prune database entries older than SECONDS (removes stale cache)
    #[arg(long, value_name = "SECONDS")]
    prune: Option<u64>,

    /// Show build info from Cargo.lock at time of building
    #[arg(long)]
    show_build_info: bool,

    /// Show all ignored files (blake3 hash, group UUID, timestamp)
    #[arg(long)]
    show_ignored: bool,

    /// Import ignored files from --show-ignored output file (use "-" for stdin)
    #[arg(long, value_name = "FILE")]
    import_ignored: Option<PathBuf>,

    /// Remove file(s) from ignore list by filename(s), group UUID, or PDQ hash
    #[arg(long, value_name = "VALUE", num_args(1..))]
    unignore: Vec<String>,
}

impl Cli {
    fn validate(&self) -> Result<(), String> {
        // Validate similarity based on hash algorithm
        let max_similarity = crate::hamminghash::MAX_SIMILARITY_256;

        let similarity = self.get_similarity();
        if similarity > max_similarity {
            return Err(format!(
                "Similarity must be 0-{max_similarity} for PDQ hash. Got {similarity}."
            ));
        }

        let valid_sorts = [
            "name",
            "name-desc",
            "name-natural",
            "name-natural-desc",
            "date",
            "date-desc",
            "size",
            "size-desc",
            "random",
            "exif-date",
            "exif-date-desc",
            "location",
        ];
        let sort_lower = self.sort.to_lowercase();
        if !valid_sorts.contains(&sort_lower.as_str()) {
            return Err(format!(
                "Invalid sort '{}'. Use one of: {}",
                self.sort,
                valid_sorts.join(", ")
            ));
        }

        if self.use_tui && self.use_gui {
            return Err("Cannot use both --use-tui and --use-gui".to_string());
        }

        if self.use_tui && (self.slideshow.is_some() || !self.slideshow_effect.is_empty()) {
            return Err("Cannot use --slideshow or --slideshow-effect with --use-tui".to_string());
        }

        if let Some(ref dir) = self.move_marked {
            if !dir.exists() {
                return Err(format!("Move target directory does not exist: {dir:?}"));
            }
            if !dir.is_dir() {
                return Err(format!("Move target is not a directory: {dir:?}"));
            }
        }

        if let Some(secs) = self.slideshow
            && (!secs.is_finite() || secs <= 0.0)
        {
            return Err("Slideshow interval must be positive".to_string());
        }

        Ok(())
    }

    /// Check if we're in view mode (explicit or implied)
    #[must_use]
    fn is_view_mode(&self) -> bool {
        self.view
            || self.view_flatten
            || self.shuffle
            || self.slideshow.is_some()
            || !self.slideshow_effect.is_empty()
    }

    /// Get the hash algorithm based on CLI flags
    #[must_use]
    fn hash_algorithm(&self) -> HashAlgorithm {
        HashAlgorithm::PdqHash
    }

    /// Get similarity threshold with algorithm-specific defaults
    #[must_use]
    fn get_similarity(&self) -> u32 {
        self.similarity.unwrap_or(40)
    }
}

// --- CLI Helpers ---

/// Convert a chrono UTC time to a relative-time string via jiff.
/// Falls back to absolute formatting when the value is outside jiff's supported timestamp range.
#[must_use]
pub(crate) fn relative_time_str(modified: &DateTime<Utc>) -> String {
    Timestamp::from_second(modified.timestamp())
        .ok()
        .and_then(|ts| {
            ts.checked_add(jiff::SignedDuration::from_nanos(i64::from(
                modified.timestamp_subsec_nanos(),
            )))
            .ok()
        })
        .map(format_relative_time)
        .unwrap_or_else(|| modified.format("%Y-%m-%d %H:%M:%S").to_string())
}

#[must_use]
fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let kb = bytes as f64 / 1024.0;
    if kb < 1024.0 {
        return format!("{kb:.1} KB");
    }
    let mb = kb / 1024.0;
    if mb < 1024.0 {
        return format!("{mb:.1} MB");
    }
    let gb = mb / 1024.0;
    format!("{gb:.2} GB")
}

/// Format an ignored-entry Unix epoch timestamp as UTC string, or raw integer if out of range.
#[must_use]
fn format_ignored_timestamp(timestamp: u64) -> String {
    i64::try_from(timestamp)
        .ok()
        .and_then(|secs| chrono::DateTime::<Utc>::from_timestamp(secs, 0))
        .map_or_else(|| timestamp.to_string(), |dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
}

/// Parse the text output produced by `--show-ignored` into `(content_hash, IgnoredEntry)` tuples.
fn parse_show_ignored_output(input: &str) -> Result<Vec<([u8; 32], db::IgnoredEntry)>, String> {
    let mut entries = Vec::new();
    let mut current_uuid = [0u8; 16];

    for (line_num, raw_line) in input.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty()
            || line.starts_with("Ignored files")
            || line.starts_with("No ignored files")
        {
            continue;
        }

        if let Some(uuid_part) = line.strip_prefix("Group UUID:") {
            let uuid_str = uuid_part.trim();
            current_uuid = AppContext::parse_uuid(uuid_str).ok_or_else(|| {
                format!("Line {}: invalid Group UUID '{}'", line_num + 1, uuid_str)
            })?;
            continue;
        }

        if let Some(rest) = line.strip_prefix("blake3:") {
            let is_no_group = line.ends_with("(no group)");
            let group_uuid = if is_no_group { [0u8; 16] } else { current_uuid };

            let (blake3_str, after_blake3) = rest
                .trim_start()
                .split_once("pdqhash:")
                .ok_or_else(|| format!("Line {}: missing 'pdqhash:'", line_num + 1))?;

            let content_hash_vec = hex::decode(blake3_str.trim())
                .map_err(|e| format!("Line {}: invalid blake3 hex: {}", line_num + 1, e))?;
            let content_hash: [u8; 32] = content_hash_vec
                .try_into()
                .map_err(|_| format!("Line {}: blake3 hash must be 32 bytes", line_num + 1))?;

            let (pdq_str, after_pdq) = after_blake3
                .trim_start()
                .split_once("quality:")
                .ok_or_else(|| format!("Line {}: missing 'quality:'", line_num + 1))?;

            let pdq_str = pdq_str.trim();
            let pdqhash = if pdq_str.eq_ignore_ascii_case("none") {
                None
            } else {
                let pdq_vec = hex::decode(pdq_str)
                    .map_err(|e| format!("Line {}: invalid pdqhash hex: {}", line_num + 1, e))?;
                let arr: [u8; 32] = pdq_vec
                    .try_into()
                    .map_err(|_| format!("Line {}: pdqhash must be 32 bytes", line_num + 1))?;
                Some(arr)
            };

            let (_, after_quality) = after_pdq
                .split_once("added:")
                .ok_or_else(|| format!("Line {}: missing 'added:'", line_num + 1))?;

            let ts_str = after_quality.trim().trim_end_matches("(no group)").trim();

            let timestamp = if let Ok(secs) = ts_str.parse::<u64>() {
                secs
            } else {
                let ndt = chrono::NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%d %H:%M:%S UTC")
                    .map_err(|e| {
                        format!("Line {}: invalid timestamp '{}': {}", line_num + 1, ts_str, e)
                    })?;
                u64::try_from(ndt.and_utc().timestamp()).map_err(|_| {
                    format!(
                        "Line {}: timestamp '{}' cannot be before Unix epoch",
                        line_num + 1,
                        ts_str
                    )
                })?
            };

            entries.push((
                content_hash,
                db::IgnoredEntry { pdqhash, group_uuid, timestamp, ignored: true },
            ));
            continue;
        }

        return Err(format!("Line {}: unrecognized line '{}'", line_num + 1, line));
    }

    Ok(entries)
}

fn run_interactive_cli_delete(
    groups: Vec<Vec<FileMetadata>>,
    group_infos: Vec<GroupInfo>,
    show_relative_times: bool,
    use_trash: bool,
) {
    let mut input_buf = String::new();
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for (g_idx, group) in groups.iter().enumerate() {
        if group.len() < 2 {
            continue;
        }
        let info = &group_infos[g_idx];
        let green = "\x1b[32m";
        let reset = "\x1b[0m";

        println!("\n========================================================");
        match info.status {
            GroupStatus::AllIdentical => {
                println!("Group {} - {}Bit-identical{}", g_idx + 1, green, reset);
            },
            GroupStatus::SomeIdentical => {
                println!("Group {} - {}Some files Bit-identical{}", g_idx + 1, green, reset);
            },
            GroupStatus::None => {
                println!("Group {}/{} (Max Dist: {})", g_idx + 1, groups.len(), info.max_dist);
            },
        }
        println!("========================================================");

        let counts = get_bit_identical_counts(group);

        for (i, file) in group.iter().enumerate() {
            let time_str = if show_relative_times {
                relative_time_str(&file.modified)
            } else {
                file.modified.format("%Y-%m-%d %H:%M:%S").to_string()
            };
            let res_str =
                file.resolution.map_or_else(|| "???x???".to_string(), |(w, h)| format!("{w}x{h}"));
            let is_identical = *counts.get(&file.content_hash).unwrap_or(&0) > 1;
            let (color_start, color_end, marker) =
                if is_identical { (green, reset, "*") } else { ("", "", " ") };

            println!(
                "{}[{}] {} {} | {} | {} | {}{}",
                color_start,
                i + 1,
                marker,
                time_str,
                format_size(file.size),
                res_str,
                file.path.display(),
                color_end
            );
        }

        let action_verb = if use_trash { "TRASH" } else { "PERMANENTLY delete" };
        print!("\nEnter numbers to {action_verb} (e.g. '1 3'), or ENTER to skip: ");
        stdout.flush().ok();

        input_buf.clear();
        match stdin.read_line(&mut input_buf) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                let line = input_buf.trim();
                if line.is_empty() {
                    continue;
                }
                let mut indices = Vec::new();
                let mut invalid_token = None;
                for token in line.split_whitespace() {
                    match token.parse::<usize>() {
                        Ok(idx) if (1..=group.len()).contains(&idx) => indices.push(idx - 1),
                        _ => {
                            invalid_token = Some(token);
                            break;
                        },
                    }
                }
                if let Some(bad) = invalid_token {
                    println!(
                        "Invalid selection '{bad}' (expected 1..={}). Skipping group.",
                        group.len()
                    );
                    continue;
                }
                // Deduplicate the input
                indices.sort_unstable();
                indices.dedup();

                if indices.is_empty() {
                    println!("No valid selections.");
                    continue;
                }

                for &idx in &indices {
                    let file = &group[idx];
                    print!(
                        "{} {:?} ... ",
                        if use_trash { "Trashing" } else { "Deleting" },
                        file.path.file_name().unwrap_or_default()
                    );
                    stdout.flush().ok();
                    let res = if use_trash {
                        trash::delete(&file.path).map_err(|e| io::Error::other(e.to_string()))
                    } else {
                        fs::remove_file(&file.path)
                    };
                    match res {
                        Ok(()) => println!("OK"),
                        Err(e) => println!("FAILED ({e})"),
                    }
                }
            },
        }
    }
    println!("\nDone.");
}

#[cfg(target_os = "windows")]
mod windows_security {
    use std::os::raw::c_void;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn HeapSetInformation(
            HeapHandle: *mut c_void,
            HeapInformationClass: u32,
            HeapInformation: *mut c_void,
            HeapInformationLength: usize,
        ) -> i32;

        fn GetProcessHeap() -> *mut c_void;
    }

    const HEAP_ENABLE_TERMINATION_ON_CORRUPTION: u32 = 1;

    pub fn enable_heap_termination() {
        // SAFETY: `GetProcessHeap` retrieves the handle to the default heap of the
        // calling process, and `HeapSetInformation` with class
        // `HEAP_ENABLE_TERMINATION_ON_CORRUPTION` requires a null buffer and 0 length.
        unsafe {
            // Enables the terminate-on-corruption feature. If the heap manager detects an error in
            // any heap used by the process, it calls the Windows Error Reporting service and
            // terminates the process.
            let result = HeapSetInformation(
                GetProcessHeap(),
                HEAP_ENABLE_TERMINATION_ON_CORRUPTION,
                std::ptr::null_mut(),
                0,
            );

            if result == 0 {
                let err = std::io::Error::last_os_error();
                eprintln!(
                    "SECURITY WARNING: Failed to enable Heap Termination on Corruption. Error: {err}"
                );
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(target_os = "windows")]
    windows_security::enable_heap_termination();

    env_logger::init();

    hayro_jpeg2000::integration::register_decoding_hook();
    image_extras::register();
    let args = Cli::parse();

    // Handle --show-exif-tags early, before validation
    if args.show_exif_tags {
        println!("Supported EXIF tags for use in [gui] exif_tags configuration:\n");
        println!("{:<25} Description", "Tag Name");
        println!("{}", "-".repeat(70));
        for (name, desc) in scanner::get_supported_exif_tags() {
            println!("{name:<25} {desc}");
        }
        println!("\nExample configuration in phdupes.conf:");
        println!("[gui]");
        println!(
            "exif_tags = [\"Make\", \"Model\", \"LensModel\", \"ExposureTime\", \"FNumber\", \"ISO\"]"
        );
        return Ok(());
    }

    if args.show_build_info {
        println!("Built from Git commit: {}", env!("APP_GIT_HASH"));
        println!("Loaded Libs: {}\n", get_runtime_library_versions());
        const DEP_INFO_RAW: &str = include_str!(env!("DEPS_INFO_PATH"));
        let deps: Vec<DepInfo> = serde_json::from_str(DEP_INFO_RAW)?;

        println!("Found {} dependencies.", deps.len());
        for dep in deps {
            println!("- {} v{}", dep.name, dep.version);
            if let Some(sum) = dep.checksum {
                println!("    Checksum: {sum}");
            }
            if let Some(src) = dep.source {
                println!("    Source:   {src}");
            }
        }
        return Ok(());
    }

    if let Err(e) = args.validate() {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }

    let sort_order = if args.shuffle { "random".to_string() } else { args.sort.to_lowercase() };
    let is_view_mode = args.is_view_mode();
    let hash_algorithm = args.hash_algorithm();

    // --- PRUNE MODE ---
    if let Some(seconds) = args.prune {
        let mut ctx = AppContext::with_algorithm(hash_algorithm)?;

        println!("Pruning entries older than {seconds} seconds from PDQ hash database...");

        match ctx.prune(seconds) {
            Ok((meta_count, hash_count)) => {
                println!("Success.");
                println!("  - Removed {meta_count} expired file entries.");
                println!("  - Removed {hash_count} orphaned hash entries.");
            },
            Err(e) => {
                eprintln!("Pruning failed: {e}");
                std::process::exit(1);
            },
        }
        // Exit immediately after pruning
        return Ok(());
    }

    // --- SHOW IGNORED ---
    if args.show_ignored {
        let ctx = AppContext::with_algorithm(hash_algorithm)?;

        match ctx.get_all_ignored() {
            Ok(entries) => {
                if entries.is_empty() {
                    println!("No ignored files.");
                } else {
                    println!("Ignored files ({} entries):\n", entries.len());

                    // Group by UUID for display
                    let mut by_uuid: HashMap<[u8; 16], Vec<([u8; 32], db::IgnoredEntry)>> =
                        HashMap::new();
                    let mut no_group = Vec::new();
                    for (ch, entry) in &entries {
                        if entry.group_uuid == [0u8; 16] {
                            no_group.push((*ch, entry.clone()));
                        } else {
                            by_uuid.entry(entry.group_uuid).or_default().push((*ch, entry.clone()));
                        }
                    }

                    for (uuid, group_entries) in &by_uuid {
                        let uuid_str = AppContext::format_uuid(uuid);
                        println!("Group UUID: {uuid_str}");
                        for (ch, entry) in group_entries {
                            let pdq_str =
                                entry.pdqhash.map_or_else(|| "none".to_string(), hex::encode);
                            let ts_str = format_ignored_timestamp(entry.timestamp);
                            let q_str = format_pdq_quality(ctx.get_pdq_quality(ch));
                            println!(
                                "  blake3: {}  pdqhash: {}  quality: {}  added: {}",
                                hex::encode(ch),
                                pdq_str,
                                q_str,
                                ts_str
                            );
                        }
                        println!();
                    }

                    for (ch, entry) in &no_group {
                        let pdq_str = entry.pdqhash.map_or_else(|| "none".to_string(), hex::encode);
                        let ts_str = format_ignored_timestamp(entry.timestamp);
                        let q_str = format_pdq_quality(ctx.get_pdq_quality(ch));
                        println!(
                            "blake3: {}  pdqhash: {}  quality: {}  added: {}  (no group)",
                            hex::encode(ch),
                            pdq_str,
                            q_str,
                            ts_str
                        );
                    }
                }
            },
            Err(e) => {
                eprintln!("Failed to read ignored list: {e}");
                std::process::exit(1);
            },
        }
        return Ok(());
    }

    // --- IMPORT IGNORED ---
    if let Some(ref import_path) = args.import_ignored {
        let content = if import_path.as_os_str() == "-" {
            use std::io::Read;
            let mut buf = String::new();
            io::stdin().read_to_string(&mut buf)?;
            buf
        } else {
            fs::read_to_string(import_path)?
        };

        let entries = match parse_show_ignored_output(&content) {
            Ok(parsed) => parsed,
            Err(e) => {
                eprintln!("Failed to parse ignored list: {e}");
                std::process::exit(1);
            },
        };

        if entries.is_empty() {
            println!("No ignored entries found in input.");
            return Ok(());
        }

        let ctx = AppContext::with_algorithm(hash_algorithm)?;
        match ctx.import_ignored_entries(&entries) {
            Ok((entry_count, pdqmap_count)) => {
                println!(
                    "Imported {entry_count} ignored entries and {pdqmap_count} PDQ map entries into redb."
                );
            },
            Err(e) => {
                eprintln!("Failed to import ignored entries: {e}");
                std::process::exit(1);
            },
        }
        return Ok(());
    }

    // --- UNIGNORE ---
    if !args.unignore.is_empty() {
        let ctx = AppContext::with_algorithm(hash_algorithm)?;
        let mut had_error = false;

        for value in &args.unignore {
            eprintln!("[DEBUG-UNIGNORE] Processing: {value}");

            // Try 1: Parse as UUID (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx or 32 hex chars)
            if let Some(uuid) = AppContext::parse_uuid(value) {
                eprintln!("[DEBUG-UNIGNORE] Parsed as UUID: {}", AppContext::format_uuid(&uuid));
                match ctx.remove_ignored_by_uuid(&uuid) {
                    Ok(count) => {
                        println!(
                            "Removed {} ignored entries matching UUID {}",
                            count,
                            AppContext::format_uuid(&uuid)
                        );
                        eprintln!(
                            "[DEBUG-UNIGNORE] Removed {} entries for UUID {}",
                            count,
                            AppContext::format_uuid(&uuid)
                        );
                    },
                    Err(e) => {
                        eprintln!("Failed to unignore by UUID: {e}");
                        had_error = true;
                    },
                }
                continue;
            }

            // Try 2: Parse as hex PDQ hash (64 hex chars = 32 bytes, but not a file path)
            if value.len() == 64
                && !std::path::Path::new(value).exists()
                && let Ok(bytes) = hex::decode(value)
                && let Ok(pdqhash) = <[u8; 32]>::try_from(bytes.as_slice())
            {
                eprintln!("[DEBUG-UNIGNORE] Parsed as PDQ hash: {}", hex::encode(pdqhash));
                match ctx.remove_ignored_by_pdqhash(&pdqhash) {
                    Ok(count) => {
                        println!("Removed {count} ignored entries matching PDQ hash {value}");
                        eprintln!("[DEBUG-UNIGNORE] Removed {count} entries for pdqhash {value}");
                    },
                    Err(e) => {
                        eprintln!("Failed to unignore by PDQ hash: {e}");
                        had_error = true;
                    },
                }
                continue;
            }

            // Try 3: Treat as filename — compute keyed blake3 and look up
            let file_path = std::path::Path::new(value);
            if file_path.exists() {
                eprintln!("[DEBUG-UNIGNORE] Treating as filename: {value}");
                match fs::read(file_path) {
                    Ok(data) => {
                        let content_hash = *blake3::keyed_hash(&ctx.content_key, &data).as_bytes();
                        eprintln!(
                            "[DEBUG-UNIGNORE] Computed keyed blake3: {}",
                            hex::encode(content_hash)
                        );
                        match ctx.remove_ignored(&content_hash) {
                            Ok(true) => {
                                println!(
                                    "Removed {} from ignore list (keyed blake3: {})",
                                    value,
                                    hex::encode(content_hash)
                                );
                                eprintln!(
                                    "[DEBUG-UNIGNORE] Successfully removed keyed blake3={}",
                                    hex::encode(content_hash)
                                );
                            },
                            Ok(false) => {
                                println!(
                                    "File {} (keyed blake3: {}) was not in the ignore list",
                                    value,
                                    hex::encode(content_hash)
                                );
                            },
                            Err(e) => {
                                eprintln!("Failed to unignore: {e}");
                                had_error = true;
                            },
                        }
                    },
                    Err(e) => {
                        eprintln!("Failed to read file {value}: {e}");
                        had_error = true;
                    },
                }
            } else {
                eprintln!(
                    "'{value}' is not a valid UUID, PDQ hash (64 hex chars), or existing filename."
                );
                had_error = true;
            }
        }

        if had_error {
            std::process::exit(1);
        }
        return Ok(());
    }

    #[cfg(unix)]
    if args.use_trash {
        use trash::os_limited::trash_folders;
        let bins = trash_folders()?;
        eprintln!("Trash folders on this system: {bins:#?}");
    }

    // Parse specified 3D lut files
    let mut parsed_luts = Vec::new();
    if !args.luts3d.is_empty() {
        for lut_path in &args.luts3d {
            match gui::image::CubeLut3d::parse_file(lut_path) {
                Ok(lut) => {
                    parsed_luts.push(lut);
                },
                Err(e) => {
                    eprintln!("Error reading 3D LUT file {lut_path:?}: {e}");
                },
            }
        }

        if parsed_luts.is_empty() {
            eprintln!("Error: No valid 3D LUT files could be loaded.");
            std::process::exit(1);
        }
        if args.use_tui {
            eprintln!("Error: TUI mode not compatible with 3D LUT.");
            std::process::exit(1);
        }
    }

    // View mode uses GUI by default unless --use-tui specified
    let use_gui = args.use_gui || (is_view_mode && !args.use_tui);

    // View mode with GUI
    if is_view_mode && use_gui {
        println!("Launching image viewer...");
        let app = gui::GuiApp::new_view_mode(
            args.paths.clone(),
            sort_order,
            args.relative_times,
            args.use_trash,
            args.raw_thumbnails,
            args.view_flatten,
        )
        .with_move_target(args.move_marked.clone())
        .with_slideshow_interval(args.slideshow)
        .with_slideshow_effect(args.slideshow_effect)
        .with_luts(parsed_luts);
        if let Err(e) = app.run() {
            eprintln!("GUI Error: {e}");
            std::process::exit(1);
        }
        return Ok(());
    }

    // View mode with TUI (--view / --view-flatten / --shuffle with --use-tui)
    if is_view_mode {
        let ctx = AppContext::with_algorithm(hash_algorithm)?;
        let (final_groups, final_infos) = if args.view_flatten {
            let (batch_tx, batch_rx) = crossbeam_channel::unbounded::<Vec<FileMetadata>>();
            let _ = scanner::spawn_background_flatten_scan(
                &args.paths,
                sort_order.clone(),
                &ctx,
                batch_tx,
                None,
            );
            let mut all_files: Vec<FileMetadata> = batch_rx.into_iter().flatten().collect();
            scanner::sort_files(&mut all_files, &sort_order);
            if all_files.is_empty() {
                (Vec::new(), Vec::new())
            } else {
                (vec![all_files], vec![GroupInfo { max_dist: 0, status: GroupStatus::None }])
            }
        } else {
            let (groups, infos, _subdirs) =
                scanner::scan_for_view(&args.paths, &sort_order, None, None);
            (groups, infos)
        };

        let mut state = state::AppState::new(
            final_groups,
            final_infos,
            args.relative_times,
            args.use_trash,
            sort_order,
            HashMap::new(),
        );
        state.view_mode = true;
        state.view_mode_flatten = args.view_flatten;
        state.move_target = args.move_marked.clone();

        println!("Launching TUI...");
        let mut app = ui::TuiApp::new(state);
        app.run()?;
        return Ok(());
    }

    // Duplicate detection modes require AppContext with selected algorithm
    let ctx = AppContext::with_algorithm(hash_algorithm)?;

    let similarity = args.get_similarity();
    let scan_config = ScanConfig {
        paths: args.paths.clone(),
        rehash: args.rehash,
        similarity,
        group_by: sort_order.clone(),
        extensions: ctx.grouping_config.extensions.clone(),
        ignore_same_stem: ctx.grouping_config.ignore_same_stem,
        calc_pixel_hash: args.pixel_hash,
    };

    if args.rehash_only {
        let _ = scanner::scan_and_group(&scan_config, &ctx, None);
        return Ok(());
    }

    let ext_priorities = build_ext_priorities(&ctx.grouping_config.extensions);

    // For GUI mode (duplicate detection), let the GUI handle scanning with progress display
    if use_gui {
        println!("Launching GUI with PDQ hash algorithm (similarity: {similarity})...");
        let app = gui::GuiApp::new(
            ctx,
            scan_config,
            args.relative_times,
            args.use_trash,
            ext_priorities,
            args.raw_thumbnails,
        )
        .with_move_target(args.move_marked.clone())
        .with_slideshow_effect(args.slideshow_effect)
        .with_luts(parsed_luts);

        if let Err(e) = app.run() {
            eprintln!("GUI Error: {e}");
            std::process::exit(1);
        }
        return Ok(());
    }

    // For non-GUI modes, scan first then filter ignored files and register duplicate groups
    let (mut final_groups, mut final_infos) = scanner::scan_and_group(&scan_config, &ctx, None);
    filter_and_register_groups(
        &ctx,
        &mut final_groups,
        &mut final_infos,
        &sort_order,
        &ext_priorities,
    );
    println!("Found {} duplicate groups using PDQ hash.", final_groups.len());

    if args.use_tui {
        let mut state = state::AppState::new(
            final_groups,
            final_infos,
            args.relative_times,
            args.use_trash,
            sort_order,
            ext_priorities,
        );
        state.move_target = args.move_marked.clone();

        println!("Launching TUI...");
        let mut app = ui::TuiApp::new(state);
        app.run()?;
    } else if args.delete {
        run_interactive_cli_delete(final_groups, final_infos, args.relative_times, args.use_trash);
    } else {
        let green = "\x1b[32m";
        let reset = "\x1b[0m";
        for (i, group) in final_groups.iter().enumerate() {
            let info = &final_infos[i];
            match info.status {
                GroupStatus::AllIdentical => {
                    println!("\n--- Group {} - {}Bit-identical{} ---", i + 1, green, reset);
                },
                GroupStatus::SomeIdentical => {
                    println!(
                        "\n--- Group {} - {}Some files Bit-identical{} ---",
                        i + 1,
                        green,
                        reset
                    );
                },
                GroupStatus::None => {
                    println!("\n--- Group {} (Max Dist: {}) ---", i + 1, info.max_dist);
                },
            }

            let counts = get_bit_identical_counts(group);

            for file in group {
                let time_str = if args.relative_times {
                    relative_time_str(&file.modified)
                } else {
                    file.modified.format("%Y-%m-%d %H:%M:%S.%f").to_string()
                };
                let res_str =
                    file.resolution.map_or_else(|| "?".to_string(), |(w, h)| format!("{w}x{h}"));
                let is_identical = *counts.get(&file.content_hash).unwrap_or(&0) > 1;
                let (color_start, color_end, marker) =
                    if is_identical { (green, reset, "*") } else { ("", "", " ") };
                println!(
                    "  {}[{}] {} | {} | {} | {}{}",
                    color_start,
                    marker,
                    time_str,
                    format_size(file.size),
                    res_str,
                    file.path.display(),
                    color_end
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(1023), "1023 B");
        assert_eq!(format_size(1024), "1.0 KB");
        assert_eq!(format_size(1024 * 1024), "1.0 MB");
        assert_eq!(format_size(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn test_format_pdq_quality() {
        assert_eq!(format_pdq_quality(None), "n/a");
        assert_eq!(format_pdq_quality(Some(10)), "10 (low)");
        assert_eq!(format_pdq_quality(Some(crate::scanner::PDQ_MIN_QUALITY)), "50");
        assert_eq!(format_pdq_quality(Some(100)), "100");
    }

    #[test]
    fn test_build_ext_priorities_strips_leading_dots_and_lowercases() {
        let exts = vec![".JPG".to_string(), "cr3".to_string(), "..Png".to_string()];
        let map = build_ext_priorities(&exts);
        assert_eq!(map.get("jpg"), Some(&0));
        assert_eq!(map.get("cr3"), Some(&1));
        assert_eq!(map.get("png"), Some(&2));
    }

    #[test]
    fn test_parse_show_ignored_output_valid_and_invalid() {
        let valid_input = "\
Ignored files (2 entries):

Group UUID: 01020304-0506-0708-090a-0b0c0d0e0f10
  blake3: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  pdqhash: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb  quality: 90  added: 2025-01-15 12:30:00 UTC

blake3: cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc  pdqhash: none  quality: n/a  added: 1700000000  (no group)
";
        let parsed = parse_show_ignored_output(valid_input).expect("should parse valid input");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, [0xAA; 32]);
        assert_eq!(parsed[0].1.pdqhash, Some([0xBB; 32]));
        assert_eq!(parsed[0].1.group_uuid, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        assert!(parsed[0].1.ignored);

        assert_eq!(parsed[1].0, [0xCC; 32]);
        assert_eq!(parsed[1].1.pdqhash, None);
        assert_eq!(parsed[1].1.group_uuid, [0u8; 16]);
        assert_eq!(parsed[1].1.timestamp, 1_700_000_000);

        // Pre-epoch date must be rejected instead of wrapping to u64::MAX
        let pre_epoch_input = "\
blake3: cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc  pdqhash: none  quality: n/a  added: 1969-12-31 23:59:59 UTC  (no group)
";
        assert!(parse_show_ignored_output(pre_epoch_input).is_err());

        // Unrecognized non-empty lines must be rejected
        assert!(parse_show_ignored_output("random garbage line").is_err());
    }

    #[test]
    fn test_cli_validate() {
        let base = Cli::try_parse_from(["phdupes", "."]).expect("should parse");
        assert!(base.validate().is_ok());

        let bad_sim =
            Cli::try_parse_from(["phdupes", "--similarity", "300", "."]).expect("should parse");
        assert!(bad_sim.validate().is_err());

        let bad_sort =
            Cli::try_parse_from(["phdupes", "--sort", "invalid", "."]).expect("should parse");
        assert!(bad_sort.validate().is_err());

        let both_ui =
            Cli::try_parse_from(["phdupes", "--use-tui", "--use-gui", "."]).expect("should parse");
        assert!(both_ui.validate().is_err());

        let tui_slideshow = Cli::try_parse_from(["phdupes", "--use-tui", "--slideshow", "3", "."])
            .expect("should parse");
        assert!(tui_slideshow.validate().is_err());

        let nan_slideshow =
            Cli::try_parse_from(["phdupes", "--slideshow", "NaN", "."]).expect("should parse");
        assert!(nan_slideshow.validate().is_err());

        let inf_slideshow =
            Cli::try_parse_from(["phdupes", "--slideshow", "inf", "."]).expect("should parse");
        assert!(inf_slideshow.validate().is_err());
    }
}
