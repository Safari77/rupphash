use chrono::{DateTime, Utc};
use codes_iso_3166::part_1::CountryCode;
use codes_iso_3166::part_2::SubdivisionCode;
use crossbeam_channel::{unbounded, Sender};
use image::GenericImageView;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::UNIX_EPOCH;
use walkdir::WalkDir;

// Platform-specific imports for Metadata
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

use crate::{FileMetadata, GroupInfo, GroupStatus};
use crate::phash::DctPhash;
use crate::hamminghash::{MIHIndex, HammingHash, SparseBitSet};
use crate::db::{AppContext, HashAlgorithm, HashValue};

pub const RAW_EXTS: &[&str] = &["nef", "dng", "cr2", "cr3", "arw", "orf", "rw2", "raf"];

trait BufReadSeek: std::io::BufRead + std::io::Seek {}
impl<T: std::io::BufRead + std::io::Seek> BufReadSeek for T {}

// --- Helper: Cross-Platform ID Extraction ---
// Returns: (id_for_hashing, Option<(dev_id, file_id)>)
fn get_file_identifiers(metadata: &fs::Metadata, ignore_dev_id: bool) -> (u64, Option<(u64, u64)>) {
    #[cfg(unix)]
    {
        let inode = metadata.ino();
        let dev = if ignore_dev_id { 0 } else { metadata.dev() };
        (inode, Some((dev, inode)))
    }
    #[cfg(windows)]
    {
        // On Windows: volume_serial_number ~ dev, file_index ~ inode
        let idx = metadata.file_index().unwrap_or(0);
        let vol = if ignore_dev_id { 0 } else { metadata.volume_serial_number().unwrap_or(0) as u64 };

        if idx == 0 && vol == 0 {
            (0, None)
        } else {
            (idx, Some((vol, idx)))
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        (0, None)
    }
}

/// Read EXIF data from a file and return the exif::Exif structure
/// This is the shared function used by both get_orientation and get_exif_tags
pub fn read_exif_data(path: &Path, preloaded_bytes: Option<&[u8]>) -> Option<exif::Exif> {
    let mut reader: Box<dyn BufReadSeek> = if let Some(bytes) = preloaded_bytes {
        Box::new(std::io::Cursor::new(bytes))
    } else {
        match fs::File::open(path) {
            Ok(f) => Box::new(std::io::BufReader::new(f)),
            Err(_) => return None,
        }
    };

    exif::Reader::new().read_from_container(&mut reader).ok()
}

/// Get orientation from EXIF data (1-8, defaults to 1)
pub fn get_orientation(path: &Path, preloaded_bytes: Option<&[u8]>) -> u8 {
    if let Some(exif_data) = read_exif_data(path, preloaded_bytes)
        && let Some(field) = exif_data.get_field(exif::Tag::Orientation, exif::In::PRIMARY)
            && let Some(v @ 1..=8) = field.value.get_uint(0) {
                return v as u8;
            }
    1
}

/// Get multiple EXIF tags as a vector of (tag_name, value) pairs
/// Only returns tags that exist in the image
/// Supports derived values like DerivedCountry
pub fn get_exif_tags(path: &Path, tag_names: &[String], decimal_coords: bool) -> Vec<(String, String)> {
    let Some(exif_data) = read_exif_data(path, None) else {
        return Vec::new();
    };

    let mut results = Vec::new();

    // Pre-extract GPS coordinates for derived values (only if needed)
    let gps_coords = if tag_names.iter().any(|t| is_derived_tag(t)) {
        extract_gps_coordinates(&exif_data)
    } else {
        None
    };

    for tag_name in tag_names {
        // Check for derived tags first
        if let Some(value) = get_derived_value(tag_name, gps_coords) {
            results.push((format_derived_tag_display_name(tag_name), value));
        } else if let Some((tag, in_value)) = parse_exif_tag_name(tag_name)
            && let Some(field) = exif_data.get_field(tag, in_value) {
                let value_str = format_exif_value(&field.value, tag, decimal_coords);
                results.push((tag_name.clone(), value_str));
            }
    }

    results
}

/// Check if a tag name is a derived value (not a real EXIF tag)
fn is_derived_tag(name: &str) -> bool {
    matches!(name.to_lowercase().as_str(), "derivedcountry" | "country")
}

/// Get the display name for a derived tag
fn format_derived_tag_display_name(name: &str) -> String {
    match name.to_lowercase().as_str() {
        "derivedcountry" => "Country".to_string(),
        _ => name.to_string(),
    }
}

/// Extract GPS coordinates from EXIF data as (latitude, longitude)
fn extract_gps_coordinates(exif_data: &exif::Exif) -> Option<(f64, f64)> {
    let lat_field = exif_data.get_field(exif::Tag::GPSLatitude, exif::In::PRIMARY)?;
    let lon_field = exif_data.get_field(exif::Tag::GPSLongitude, exif::In::PRIMARY)?;
    let lat_ref = exif_data.get_field(exif::Tag::GPSLatitudeRef, exif::In::PRIMARY);
    let lon_ref = exif_data.get_field(exif::Tag::GPSLongitudeRef, exif::In::PRIMARY);

    let lat = parse_gps_coordinate(&lat_field.value)?;
    let lon = parse_gps_coordinate(&lon_field.value)?;

    // Apply reference (N/S for latitude, E/W for longitude)
    let lat = if let Some(ref_field) = lat_ref {
        let ref_str = ref_field.value.display_as(exif::Tag::GPSLatitudeRef).to_string();
        if ref_str.trim().eq_ignore_ascii_case("S") { -lat } else { lat }
    } else { lat };

    let lon = if let Some(ref_field) = lon_ref {
        let ref_str = ref_field.value.display_as(exif::Tag::GPSLongitudeRef).to_string();
        if ref_str.trim().eq_ignore_ascii_case("W") { -lon } else { lon }
    } else { lon };

    Some((lat, lon))
}

/// Parse GPS coordinate from EXIF rational values (degrees, minutes, seconds)
fn parse_gps_coordinate(value: &exif::Value) -> Option<f64> {
    if let exif::Value::Rational(rats) = value
        && rats.len() >= 3 {
            let degrees = rats[0].num as f64 / rats[0].denom as f64;
            let minutes = rats[1].num as f64 / rats[1].denom as f64;
            let seconds = rats[2].num as f64 / rats[2].denom as f64;
            return Some(degrees + minutes / 60.0 + seconds / 3600.0);
        }
    None
}

/// Get a derived value based on tag name and available data
fn get_derived_value(tag_name: &str, gps_coords: Option<(f64, f64)>) -> Option<String> {
    match tag_name.to_lowercase().as_str() {
        "derivedcountry" => {
            let (lat, lon) = gps_coords?;
            derive_country(lat, lon)
        },
        _ => None,
    }
}

/// Derive country name from GPS coordinates using country-boundaries
fn derive_country(lat: f64, lon: f64) -> Option<String> {
    use country_boundaries::{CountryBoundaries, LatLon, BOUNDARIES_ODBL_360X180};

    // Create boundaries instance (this is fast after first load as data is static)
    let boundaries = CountryBoundaries::from_reader(BOUNDARIES_ODBL_360X180).ok()?;

    // Get the position
    let pos = LatLon::new(lat, lon).ok()?;

    // Get country IDs for this position
    let ids = boundaries.ids(pos);

    if ids.is_empty() {
        return None;
    }

    // Find subdivision (like "US-FL") and country code (like "US")
    let subdivision_id = ids.iter().find(|id| id.contains('-')).map(|s| s.as_ref());
    let country_id = ids.iter().find(|id| id.len() == 2).map(|s| s.as_ref());

    // Build the location string
    format_location(country_id, subdivision_id)
}

/// Format location string from country and subdivision codes
fn format_location(country_code: Option<&str>, subdivision_code: Option<&str>) -> Option<String> {
    // 1. Get subdivision name (e.g., "US-FL" -> "Florida")
    let subdivision_name = subdivision_code.and_then(|code| {
        // The crate expects underscores (US_FL) not hyphens (US-FL)
        let formatted_code = code.replace('-', "_");
        SubdivisionCode::from_str(&formatted_code)
            .ok()
            .map(|s| s.name().to_string())
    });

    // 2. Get country name (e.g., "FI" -> "Finland")
    let country_name = country_code.and_then(|code| {
        CountryCode::from_str(code)
            .ok()
            .map(|c| c.short_name().to_string())
    });

    match (country_name, subdivision_name) {
        (Some(country), Some(subdivision)) => Some(format!("{}, {}", subdivision, country)),
        (Some(country), None) => Some(country),
        (None, Some(subdivision)) => Some(subdivision),
        (None, None) => {
            // Fallback: return the raw code if we have one
            country_code.or(subdivision_code).map(|s| s.to_string())
        }
    }
}

/// Parse a tag name string into an exif::Tag and exif::In
fn parse_exif_tag_name(name: &str) -> Option<(exif::Tag, exif::In)> {
    // Common EXIF tags - add more as needed
    let tag = match name.to_lowercase().as_str() {
        "make" => exif::Tag::Make,
        "model" => exif::Tag::Model,
        "orientation" => exif::Tag::Orientation,
        "datetime" | "datetimeoriginal" => exif::Tag::DateTimeOriginal,
        "datetimedigitized" => exif::Tag::DateTimeDigitized,
        "exposuretime" | "exposure" => exif::Tag::ExposureTime,
        "fnumber" | "aperture" => exif::Tag::FNumber,
        "iso" | "isospeedratings" | "photographicsensitivity" => exif::Tag::PhotographicSensitivity,
        "focallength" => exif::Tag::FocalLength,
        "focallengthin35mmfilm" | "focallength35mm" => exif::Tag::FocalLengthIn35mmFilm,
        "exposureprogram" => exif::Tag::ExposureProgram,
        "meteringmode" => exif::Tag::MeteringMode,
        "flash" => exif::Tag::Flash,
        "whitebalance" => exif::Tag::WhiteBalance,
        "lensmodel" | "lens" => exif::Tag::LensModel,
        "lensmake" => exif::Tag::LensMake,
        "software" => exif::Tag::Software,
        "artist" => exif::Tag::Artist,
        "copyright" => exif::Tag::Copyright,
        "imagewidth" | "pixelxdimension" => exif::Tag::PixelXDimension,
        "imageheight" | "pixelydimension" => exif::Tag::PixelYDimension,
        "gpslatitude" => exif::Tag::GPSLatitude,
        "gpslongitude" => exif::Tag::GPSLongitude,
        "gpsaltitude" => exif::Tag::GPSAltitude,
        "exposurebias" | "exposurebiasvalue" => exif::Tag::ExposureBiasValue,
        "colorspace" => exif::Tag::ColorSpace,
        "scenetype" => exif::Tag::SceneType,
        "subjectdistance" => exif::Tag::SubjectDistance,
        "digitalzoomratio" => exif::Tag::DigitalZoomRatio,
        "contrast" => exif::Tag::Contrast,
        "saturation" => exif::Tag::Saturation,
        "sharpness" => exif::Tag::Sharpness,
        _ => return None,
    };

    Some((tag, exif::In::PRIMARY))
}

/// Returns a list of all supported EXIF tag names that can be used in configuration
pub fn get_supported_exif_tags() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Make", "Camera manufacturer"),
        ("Model", "Camera model"),
        ("LensModel", "Lens model name"),
        ("LensMake", "Lens manufacturer"),
        ("DateTime", "Date/time original (alias for DateTimeOriginal)"),
        ("DateTimeOriginal", "Date/time when photo was taken"),
        ("DateTimeDigitized", "Date/time when photo was digitized"),
        ("ExposureTime", "Exposure time (shutter speed)"),
        ("Exposure", "Exposure time (alias)"),
        ("FNumber", "F-number (aperture)"),
        ("Aperture", "F-number (alias)"),
        ("ISO", "ISO sensitivity"),
        ("ISOSpeedRatings", "ISO sensitivity (alias)"),
        ("PhotographicSensitivity", "ISO sensitivity (alias)"),
        ("FocalLength", "Focal length in mm"),
        ("FocalLengthIn35mmFilm", "Focal length equivalent in 35mm"),
        ("FocalLength35mm", "Focal length equivalent in 35mm (alias)"),
        ("ExposureProgram", "Exposure program mode"),
        ("MeteringMode", "Metering mode"),
        ("Flash", "Flash status"),
        ("WhiteBalance", "White balance mode"),
        ("ExposureBias", "Exposure bias/compensation"),
        ("ExposureBiasValue", "Exposure bias/compensation (alias)"),
        ("Software", "Software used"),
        ("Artist", "Artist/creator"),
        ("Copyright", "Copyright information"),
        ("Orientation", "Image orientation (1-8)"),
        ("ImageWidth", "Image width in pixels"),
        ("PixelXDimension", "Image width in pixels (alias)"),
        ("ImageHeight", "Image height in pixels"),
        ("PixelYDimension", "Image height in pixels (alias)"),
        ("ColorSpace", "Color space"),
        ("SceneType", "Scene type"),
        ("SubjectDistance", "Subject distance"),
        ("DigitalZoomRatio", "Digital zoom ratio"),
        ("Contrast", "Contrast setting"),
        ("Saturation", "Saturation setting"),
        ("Sharpness", "Sharpness setting"),
        ("GPSLatitude", "GPS latitude"),
        ("GPSLongitude", "GPS longitude"),
        ("GPSAltitude", "GPS altitude"),
        // Derived values (computed from other EXIF data)
        ("DerivedCountry", "Country name derived from GPS coordinates"),
    ]
}

/// Format an EXIF value for display
fn format_exif_value(value: &exif::Value, tag: exif::Tag, decimal_coords: bool) -> String {
    match tag {
        exif::Tag::GPSLatitude | exif::Tag::GPSLongitude => {
            if decimal_coords {
                // Parse rational D/M/S to decimal degrees
                if let Some(val) = parse_gps_coordinate(value) {
                    return format!("{:.6}", val);
                }
            }
            // Fallback to default Minutes/Seconds display
            clean_exif_string(&value.display_as(tag).to_string())
        },
        exif::Tag::ExposureTime => {
            if let Some(r) = value.get_uint(0) {
                if let exif::Value::Rational(rats) = value
                    && !rats.is_empty() {
                        let num = rats[0].num;
                        let denom = rats[0].denom;
                        if denom > num && num > 0 {
                            return format!("1/{}s", denom / num);
                        } else if denom > 0 {
                            return format!("{:.1}s", num as f64 / denom as f64);
                        }
                    }
                format!("{}s", r)
            } else {
                clean_exif_string(&value.display_as(tag).to_string())
            }
        },
        exif::Tag::FNumber => {
            if let exif::Value::Rational(rats) = value
                && !rats.is_empty() && rats[0].denom > 0 {
                    return format!("f/{:.1}", rats[0].num as f64 / rats[0].denom as f64);
                }
            clean_exif_string(&value.display_as(tag).to_string())
        },
        exif::Tag::FocalLength => {
            if let exif::Value::Rational(rats) = value
                && !rats.is_empty() && rats[0].denom > 0 {
                    return format!("{}mm", rats[0].num / rats[0].denom);
                }
            clean_exif_string(&value.display_as(tag).to_string())
        },
        exif::Tag::PhotographicSensitivity => {
            if let Some(iso) = value.get_uint(0) {
                format!("ISO {}", iso)
            } else {
                clean_exif_string(&value.display_as(tag).to_string())
            }
        },
        exif::Tag::FocalLengthIn35mmFilm => {
            if let Some(fl) = value.get_uint(0) {
                format!("{}mm (35mm equiv)", fl)
            } else {
                clean_exif_string(&value.display_as(tag).to_string())
            }
        },
        _ => {
            // Default: use the library's display formatting, then clean it
            clean_exif_string(&value.display_as(tag).to_string())
        }
    }
}

/// Clean up EXIF string values that may contain garbage or repeated empty entries
fn clean_exif_string(s: &str) -> String {
    // Remove surrounding quotes if present
    let s = s.trim().trim_matches('"');

    // If the string contains comma-separated values (common in some EXIF fields),
    // take only the first non-empty meaningful value
    if s.contains("\", \"") || s.contains(", ") {
        // Split by common separators and find first non-empty value
        let parts: Vec<&str> = s.split([',', '"'])
            .map(|p| p.trim())
            .filter(|p| !p.is_empty() && *p != "'" && p.len() > 1)
            .collect();

        if let Some(first) = parts.first() {
            return first.to_string();
        }
    }

    // Remove any trailing garbage (null bytes represented as empty quotes, etc.)
    let cleaned = s.trim_end_matches(|c: char| c == '"' || c == '\'' || c == ',' || c.is_whitespace() || c == '\0');

    cleaned.to_string()
}

fn get_resolution(path: &Path, bytes: Option<&[u8]>) -> Option<(u32, u32)> {
    if is_raw_ext(path) {
        let data = match bytes {
            Some(b) => b.to_vec(),
            None => fs::read(path).ok()?,
        };
        if let Ok(mut raw) = rsraw::RawImage::open(&data) && raw.unpack().is_ok() {
            return Some((raw.width(), raw.height()));
        }
        None
    } else {
        match bytes {
            Some(b) => {
                if let Ok(reader) = image::ImageReader::new(std::io::Cursor::new(b)).with_guessed_format()
                    && let Ok(dims) = reader.into_dimensions() {
                    Some(dims)
                } else { None }
            }
            None => {
                if let Ok(reader) = image::ImageReader::open(path)
                    && let Ok(dims) = reader.into_dimensions() {
                    Some(dims)
                } else { None }
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
    pub ignore_dev_id: bool,
}

#[derive(Clone)]
struct ScannedFile {
    pub path: std::path::PathBuf,
    pub size: u64,
    pub modified: DateTime<Utc>,
    pub resolution: Option<(u32, u32)>,
    pub content_hash: [u8; 32],
    pub orientation: u8,
    pub dev_inode: Option<(u64, u64)>,
    pub phash: Option<u64>,
    pub pdqhash: Option<[u8; 32]>,
    pub pdq_features: Option<crate::pdqhash::PdqFeatures>,
}

impl ScannedFile {
    fn to_file_metadata(&self) -> FileMetadata {
        FileMetadata {
            path: self.path.clone(),
            size: self.size,
            modified: self.modified,
            phash: self.phash.unwrap_or(0),
            pdqhash: self.pdqhash,
            resolution: self.resolution,
            content_hash: self.content_hash,
            orientation: self.orientation,
            dev_inode: self.dev_inode,
        }
    }
}

pub fn scan_and_group(
    config: &ScanConfig,
    ctx: &AppContext,
    progress_tx: Option<Sender<(usize, usize)>>
) -> (Vec<Vec<FileMetadata>>, Vec<GroupInfo>) {
    use std::time::Instant;

    let ctx_ref = ctx;
    let force_rehash = config.rehash;
    let use_pdqhash = ctx.hash_algorithm == HashAlgorithm::PdqHash;

    let mut all_files = Vec::new();
    let mut seen_paths = HashSet::new();
    for path_str in &config.paths {
        let path = Path::new(path_str);
        if path.is_dir() {
            for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
                if is_image_ext(entry.path())
                    && let Ok(canonical) = entry.path().canonicalize()
                        && seen_paths.insert(canonical.clone()) {
                            all_files.push(canonical);
                        }
            }
        } else if path.is_file() && is_image_ext(path)
            && let Ok(canonical) = path.canonicalize()
                && seen_paths.insert(canonical.clone()) {
                    all_files.push(canonical);
                }
    }

    if all_files.is_empty() { return (Vec::new(), Vec::new()); }

    let total_files = all_files.len();
    if let Some(tx) = &progress_tx { let _ = tx.send((0, total_files)); }

    let hash_start = Instant::now();
    let (tx, rx) = unbounded();
    let db_handle = ctx.start_db_writer(rx);
    let hasher = DctPhash::new();
    let processed_count = AtomicUsize::new(0);

    let valid_files: Vec<ScannedFile> = all_files.par_iter().filter_map(|path| {
        if let Some(prog_tx) = &progress_tx {
            let current = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
            if current.is_multiple_of(50) || current == total_files {
                let _ = prog_tx.send((current, total_files));
            }
        }

        let metadata = fs::metadata(path).ok()?;
        let size = metadata.len();
        let mtime = metadata.modified().ok().unwrap_or(UNIX_EPOCH);
        let mtime_utc: DateTime<Utc> = DateTime::from(mtime);
        let mtime_ns = mtime.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;

        let (id_hash, dev_inode) = get_file_identifiers(&metadata, config.ignore_dev_id);

        let mut mh = blake3::Hasher::new_keyed(&ctx_ref.meta_key);
        mh.update(&mtime_ns.to_le_bytes());
        mh.update(&id_hash.to_le_bytes()); // Updates with inode only if dev is ignored
        mh.update(&size.to_le_bytes());
        let meta_key: [u8; 32] = *mh.finalize().as_bytes();

        let mut phash: Option<u64> = None;
        let mut pdqhash: Option<[u8; 32]> = None;
        let mut pdq_features: Option<crate::pdqhash::PdqFeatures> = None;
        let mut new_meta = None;
        let mut new_hash = None;
        let mut resolution = None;
        let mut ck = [0u8; 32];
        let mut orientation = 1;

        let mut bytes = fs::read(path).ok();
        if let Some(ref b) = bytes {
            orientation = get_orientation(path, Some(b));
        }

        if !force_rehash
            && let Ok(Some(ch)) = ctx_ref.get_content_hash(&meta_key) {
                 ck = ch;
                 if use_pdqhash {
                     if let Ok(Some(h)) = ctx_ref.get_pdqhash(&ch) {
                         pdqhash = Some(h);
                         if bytes.is_none() { bytes = fs::read(path).ok(); }
                         if let Some(ref b) = bytes
                             && let Ok(img) = image::load_from_memory(b) {
                                 if let Some((f, _)) = crate::pdqhash::generate_pdq_features(&img) {
                                     pdq_features = Some(f);
                                 }
                                 // For RAW files, use rsraw to get actual resolution, not thumbnail
                                 resolution = if is_raw_ext(path) {
                                     get_resolution(path, Some(b))
                                 } else {
                                     Some(img.dimensions())
                                 };
                             }
                     }
                 } else if let Ok(Some(h)) = ctx_ref.get_phash(&ch) {
                     phash = Some(h);
                     if bytes.is_none() { orientation = get_orientation(path, None); }
                     resolution = get_resolution(path, None);
                 }
            }

        let needs_calculation = if use_pdqhash {
             pdqhash.is_none() || pdq_features.is_none()
        } else {
             phash.is_none()
        };

        if needs_calculation {
             let b = match bytes { Some(v) => v, None => return None };
             let ch = blake3::keyed_hash(&ctx_ref.content_key, &b);
             ck = *ch.as_bytes();
             new_meta = Some((meta_key, ck));

             if use_pdqhash {
                 if let Ok(img) = image::load_from_memory(&b) {
                     // For RAW files, use rsraw to get actual resolution, not thumbnail
                     resolution = if is_raw_ext(path) {
                         get_resolution(path, Some(&b))
                     } else {
                         Some(img.dimensions())
                     };
                     if let Some((features, _quality)) = crate::pdqhash::generate_pdq_features(&img) {
                         let hash = features.to_hash();
                         pdqhash = Some(hash);
                         pdq_features = Some(features);
                         new_hash = Some((ck, HashValue::PdqHash(hash)));
                         eprintln!("[DEBUG-PDQ] Generated fresh hash+features for: {:?}", path);
                     } else { return None; }
                 } else { return None; }
             } else if let Ok(Some(h)) = ctx_ref.get_phash(&ck) {
                 phash = Some(h);
                 resolution = get_resolution(path, Some(&b));
             } else if let Ok(img) = image::load_from_memory(&b) {
                 resolution = Some(img.dimensions());
                 let hash = hasher.hash_image(&img);
                 phash = Some(hash);
                 new_hash = Some((ck, HashValue::PHash(hash)));
             } else { return None; }
        }

        if new_meta.is_some() || new_hash.is_some() { let _ = tx.send((new_meta, new_hash)); }

        Some(ScannedFile {
            path: path.clone(),
            size,
            modified: mtime_utc,
            resolution,
            content_hash: ck,
            orientation,
            dev_inode,
            phash,
            pdqhash,
            pdq_features,
        })
    }).collect();

    drop(tx);
    db_handle.join().expect("DB writer thread panicked");

    let hash_elapsed = hash_start.elapsed();
    let hash_count = valid_files.len();
    eprintln!("[DEBUG] Algorithm: {}", if use_pdqhash { "PDQ hash" } else { "pHash" });
    eprintln!("[DEBUG] Hashes loaded: {} in {:.2}s", hash_count, hash_elapsed.as_secs_f64());

    let group_start = Instant::now();
    let (processed_groups, processed_infos, comparison_count) = if use_pdqhash {
        group_with_pdqhash(&valid_files, config)
    } else {
        group_with_phash(&valid_files, config)
    };
    let group_elapsed = group_start.elapsed();

    eprintln!("[DEBUG] Grouping: {} groups found in {:.2}s ({} comparisons)",
        processed_groups.len(), group_elapsed.as_secs_f64(), comparison_count);

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

// --- Generic Grouping Implementation ---

fn group_files_generic<H>(
    valid_files: &[ScannedFile],
    config: &ScanConfig,
    extract_hash: impl Fn(&ScannedFile) -> Option<H> + Sync + Send,
    generate_variants: impl Fn(&ScannedFile, H) -> Vec<H> + Sync + Send,
) -> (Vec<Vec<FileMetadata>>, Vec<GroupInfo>, usize)
where H: HammingHash + std::fmt::Debug
{
    let hashes: Vec<H> = valid_files.iter().filter_map(&extract_hash).collect();
    if hashes.is_empty() { return (Vec::new(), Vec::new(), 0); }

    let mih = MIHIndex::new(hashes.clone());
    let n = valid_files.len();
    let comparison_count = AtomicUsize::new(0);

    let chunk_tolerance = config.similarity / (H::NUM_CHUNKS as u32);
    let bits_per_chunk = H::bit_width_per_chunk();

    eprintln!("[DEBUG-GROUP] Files: {}, Chunk Tolerance: {}, Bits/Chunk: {}", n, chunk_tolerance, bits_per_chunk);

    let adjacency: Vec<Vec<u32>> = valid_files
        .par_iter()
        .enumerate()
        .map_init(
            || (SparseBitSet::new(n), Vec::new()),
            |(visited, results), (i, file)| {
                results.clear();

                if let Some(hash) = extract_hash(file) {
                    let variants = generate_variants(file, hash);

                    if i < 3 {
                        eprintln!("[DEBUG-TRACE] File {}: {:?} - Generated {} variants.",
                            i, file.path.file_name().unwrap_or_default(), variants.len());
                    }

                    for variant in variants {
                        visited.clear();

                        for k in 0..H::NUM_CHUNKS {
                            let q_chunk = variant.get_chunk(k);
                            let chunk_base = k * H::NUM_BUCKETS;

                            let mut check_bucket = |val: u16| {
                                let flat_idx = chunk_base + val as usize;
                                let start = unsafe { *mih.offsets.get_unchecked(flat_idx) } as usize;
                                let end = unsafe { *mih.offsets.get_unchecked(flat_idx + 1) } as usize;
                                let bucket = unsafe { mih.values.get_unchecked(start..end) };

                                for &cand_id in bucket {
                                    let cid = cand_id as usize;
                                    if cid == i { continue; }

                                    if !visited.set(cid) {
                                        comparison_count.fetch_add(1, Ordering::Relaxed);
                                        let cand_hash = unsafe { mih.db_hashes.get_unchecked(cid) };

                                        let dist = variant.hamming_distance(cand_hash);
                                        if dist <= config.similarity
                                            && !results.contains(&cand_id) {
                                                results.push(cand_id);
                                                if i < 5 {
                                                    let cand_name = valid_files[cid].path.file_name().unwrap_or_default();
                                                    eprintln!("[DEBUG-MATCH] {:?} matched {:?} (Dist: {})",
                                                        file.path.file_name().unwrap_or_default(), cand_name, dist);
                                                }
                                            }
                                    }
                                }
                            };

                            check_bucket(q_chunk);
                            if chunk_tolerance >= 1 {
                                for bit in 0..bits_per_chunk {
                                    check_bucket(q_chunk ^ (1 << bit));
                                }
                            }
                        }
                    }
                }
                results.clone()
            }
        )
        .collect();

    let mut visited_cluster = vec![false; n];
    let mut groups = Vec::new();

    for i in 0..n {
        if visited_cluster[i] { continue; }
        if adjacency[i].is_empty() { continue; }

        let mut group = vec![i as u32];
        visited_cluster[i] = true;
        let mut stack = adjacency[i].clone();

        while let Some(neighbor) = stack.pop() {
            if !visited_cluster[neighbor as usize] {
                visited_cluster[neighbor as usize] = true;
                group.push(neighbor);
                stack.extend_from_slice(&adjacency[neighbor as usize]);
            }
        }
        if group.len() > 1 { groups.push(group); }
    }

    // Merge groups that share same-stem files (e.g., jpg and nef of same photo)
    let groups = merge_groups_by_stem(groups, valid_files);

    let is_pdq = std::any::type_name::<H>().contains("u8");
    let (g, i) = process_raw_groups(groups, valid_files, config, is_pdq);
    (g, i, comparison_count.load(Ordering::Relaxed))
}

/// Merge groups that contain files with the same stem (e.g., dsc_1335.jpg and dsc_1335.nef).
/// This handles the case where RAW and JPEG have different hashes but should be in the same group.
fn merge_groups_by_stem(groups: Vec<Vec<u32>>, valid_files: &[ScannedFile]) -> Vec<Vec<u32>> {
    let original_count = groups.len();
    if groups.len() < 2 { return groups; }

    // Build stem key -> group indices mapping
    // Key: (parent_dir, stem) -> set of group indices containing files with this stem
    let mut stem_to_groups: HashMap<(std::path::PathBuf, std::ffi::OsString), Vec<usize>> = HashMap::new();

    for (group_idx, group) in groups.iter().enumerate() {
        for &file_idx in group {
            let path = &valid_files[file_idx as usize].path;
            if let (Some(parent), Some(stem)) = (path.parent(), path.file_stem()) {
                let key = (parent.to_path_buf(), stem.to_os_string());
                stem_to_groups.entry(key).or_default().push(group_idx);
            }
        }
    }

    // Union-Find to merge groups
    let mut parent: Vec<usize> = (0..groups.len()).collect();

    fn find(parent: &mut [usize], i: usize) -> usize {
        if parent[i] != i {
            parent[i] = find(parent, parent[i]);
        }
        parent[i]
    }

    fn union(parent: &mut [usize], a: usize, b: usize) {
        let ra = find(parent, a);
        let rb = find(parent, b);
        if ra != rb { parent[ra] = rb; }
    }

    // Merge groups that share a stem
    for (key, group_indices) in &stem_to_groups {
        if group_indices.len() > 1 {
            eprintln!("[DEBUG-MERGE] Merging {} groups via stem {:?}", group_indices.len(), key.1);
            let first = group_indices[0];
            for &other in &group_indices[1..] {
                union(&mut parent, first, other);
            }
        }
    }

    // Collect merged groups
    let mut merged: HashMap<usize, Vec<u32>> = HashMap::new();
    for (group_idx, group) in groups.into_iter().enumerate() {
        let root = find(&mut parent, group_idx);
        merged.entry(root).or_default().extend(group);
    }

    // Deduplicate indices within each merged group
    let result: Vec<Vec<u32>> = merged.into_values().map(|mut g| {
        g.sort_unstable();
        g.dedup();
        g
    }).collect();

    if result.len() != original_count {
        eprintln!("[DEBUG-MERGE] Merged {} groups into {}", original_count, result.len());
    }

    result
}

fn group_with_phash(
    valid_files: &[ScannedFile],
    config: &ScanConfig,
) -> (Vec<Vec<FileMetadata>>, Vec<GroupInfo>, usize) {
    use crate::phash::generate_dihedral_hashes;
    group_files_generic(
        valid_files,
        config,
        |f| f.phash,
        |_f, h| generate_dihedral_hashes(h)
    )
}

fn group_with_pdqhash(
    valid_files: &[ScannedFile],
    config: &ScanConfig,
) -> (Vec<Vec<FileMetadata>>, Vec<GroupInfo>, usize) {
    group_files_generic(
        valid_files,
        config,
        |f| f.pdqhash,
        |f, h| {
            if let Some(features) = &f.pdq_features {
                features.generate_dihedral_hashes()
            } else {
                eprintln!("[DEBUG-WARN] No PDQ features for file. Using fallback (0 deg only).");
                vec![h]
            }
        }
    )
}

fn process_raw_groups(
    raw_groups: Vec<Vec<u32>>,
    valid_files: &[ScannedFile],
    config: &ScanConfig,
    use_pdqhash: bool,
) -> (Vec<Vec<FileMetadata>>, Vec<GroupInfo>) {
    let ext_priorities: HashMap<String, usize> = config.extensions.iter()
        .enumerate()
        .map(|(i, e)| (e.to_lowercase(), i))
        .collect();

    let mut processed_groups = Vec::new();
    let mut processed_infos = Vec::new();

    for group_indices in raw_groups {
        let mut group_data: Vec<FileMetadata> = group_indices.iter()
            .map(|&idx| valid_files[idx as usize].to_file_metadata())
            .collect();

        let info = if use_pdqhash {
            analyze_group_with_features(
                &mut group_data,
                valid_files,
                &config.group_by.to_lowercase(),
                &ext_priorities
            )
        } else {
            analyze_group(&mut group_data, &config.group_by.to_lowercase(), &ext_priorities, false)
        };

        processed_groups.push(group_data);
        processed_infos.push(info);
    }
    (processed_groups, processed_infos)
}

pub fn sort_files(files: &mut [FileMetadata], sort_order: &str) {
    use rand::seq::SliceRandom;
    match sort_order {
        "name" => files.sort_by(|a, b| {
            let name_a = a.path.file_name().map(|s| s.to_string_lossy().to_lowercase());
            let name_b = b.path.file_name().map(|s| s.to_string_lossy().to_lowercase());
            name_a.cmp(&name_b)
        }),
        "name-desc" => files.sort_by(|a, b| {
            let name_a = a.path.file_name().map(|s| s.to_string_lossy().to_lowercase());
            let name_b = b.path.file_name().map(|s| s.to_string_lossy().to_lowercase());
            name_b.cmp(&name_a)
        }),
        "name-natural" => files.sort_by(|a, b| {
            let name_a = a.path.file_name().map(|s| s.to_string_lossy().to_lowercase()).unwrap_or_default();
            let name_b = b.path.file_name().map(|s| s.to_string_lossy().to_lowercase()).unwrap_or_default();
            natord::compare(&name_a, &name_b)
        }),
        "name-natural-desc" => files.sort_by(|a, b| {
            let name_a = a.path.file_name().map(|s| s.to_string_lossy().to_lowercase()).unwrap_or_default();
            let name_b = b.path.file_name().map(|s| s.to_string_lossy().to_lowercase()).unwrap_or_default();
            natord::compare(&name_b, &name_a)
        }),
        "date" => files.sort_by(|a, b| a.modified.cmp(&b.modified)),
        "date-desc" => files.sort_by(|a, b| b.modified.cmp(&a.modified)),
        "size" => files.sort_by(|a, b| a.size.cmp(&b.size)),
        "size-desc" => files.sort_by(|a, b| b.size.cmp(&a.size)),
        "random" => {
            let mut rng = rand::rng();
            files.shuffle(&mut rng);
        },
        _ => {
            files.sort_by(|a, b| {
                let name_a = a.path.file_name().map(|s| s.to_string_lossy().to_lowercase()).unwrap_or_default();
                let name_b = b.path.file_name().map(|s| s.to_string_lossy().to_lowercase()).unwrap_or_default();
                natord::compare(&name_a, &name_b)
            });
        }
    }
}

pub fn analyze_group(
    files: &mut Vec<FileMetadata>,
    sort_order: &str,
    #[allow(unused)] ext_priorities: &HashMap<String, usize>,
    use_pdqhash: bool,
) -> GroupInfo {
    if files.is_empty() { return GroupInfo { max_dist: 0, status: GroupStatus::None }; }

    let mut counts = HashMap::new();
    for f in files.iter() { *counts.entry(f.content_hash).or_insert(0) += 1; }

    let (mut duplicates, mut unique): (Vec<FileMetadata>, Vec<FileMetadata>) = files.drain(..)
        .partition(|f| *counts.get(&f.content_hash).unwrap_or(&0) > 1);

    sort_files(&mut duplicates, "name-natural");
    sort_files(&mut unique, sort_order);
    files.append(&mut duplicates);
    files.append(&mut unique);

    let max_d = if use_pdqhash {
        if let Some(pivot) = files.first().and_then(|f| f.pdqhash) {
            files.iter()
                .filter_map(|f| f.pdqhash)
                .map(|h| pivot.hamming_distance(&h))
                .max()
                .unwrap_or(0)
        } else { 0 }
    } else if let Some(first) = files.first() {
        let pivot = first.phash;
        files.iter()
            .map(|f| pivot.hamming_distance(&f.phash))
            .max()
            .unwrap_or(0)
    } else { 0 };

    let has_duplicates = !counts.values().all(|&c| c == 1);
    let all_identical = counts.len() == 1;
    let status = if all_identical { GroupStatus::AllIdentical } else if has_duplicates { GroupStatus::SomeIdentical } else { GroupStatus::None };

    GroupInfo { max_dist: max_d, status }
}

fn analyze_group_with_features(
    files: &mut Vec<FileMetadata>,
    valid_files: &[ScannedFile],
    sort_order: &str,
    #[allow(unused)] ext_priorities: &HashMap<String, usize>,
) -> GroupInfo {
    if files.is_empty() { return GroupInfo { max_dist: 0, status: GroupStatus::None }; }

    let mut counts = HashMap::new();
    for f in files.iter() { *counts.entry(f.content_hash).or_insert(0) += 1; }

    let (mut duplicates, mut unique): (Vec<FileMetadata>, Vec<FileMetadata>) = files.drain(..)
        .partition(|f| *counts.get(&f.content_hash).unwrap_or(&0) > 1);

    sort_files(&mut duplicates, sort_order);
    sort_files(&mut unique, sort_order);
    files.append(&mut duplicates);
    files.append(&mut unique);

    // Sort so same-stem files are adjacent, with non-raw (jpg) before raw (nef)
    sort_by_stem_then_ext(files);

    // Find pivot features by path lookup (after sorting, first file is the pivot)
    let pivot_features = files.first()
        .and_then(|pivot| valid_files.iter().find(|vf| vf.path == pivot.path))
        .and_then(|vf| vf.pdq_features.as_ref());

    let max_d = if let Some(pivot_feats) = pivot_features {
        let pivot_variants = pivot_feats.generate_dihedral_hashes();
        files.iter().map(|f| {
            if let Some(h) = f.pdqhash {
                pivot_variants.iter()
                    .map(|v| v.hamming_distance(&h))
                    .min()
                    .unwrap_or(255)
            } else { 0 }
        })
        .max()
        .unwrap_or(0)
    } else if let Some(pivot) = files.first().and_then(|f| f.pdqhash) {
        files.iter()
            .filter_map(|f| f.pdqhash)
            .map(|h| pivot.hamming_distance(&h))
            .max()
            .unwrap_or(0)
    } else { 0 };

    let has_duplicates = !counts.values().all(|&c| c == 1);
    let all_identical = counts.len() == 1;
    let status = if all_identical { GroupStatus::AllIdentical } else if has_duplicates { GroupStatus::SomeIdentical } else { GroupStatus::None };

    GroupInfo { max_dist: max_d, status }
}

/// Sort files so same-stem files are adjacent, with non-raw before raw.
/// e.g., dsc_1335.jpg, dsc_1335.nef, dsc_1336.jpg, dsc_1336.nef
fn sort_by_stem_then_ext(files: &mut [FileMetadata]) {
    files.sort_by(|a, b| {
        let stem_a = a.path.file_stem().map(|s| s.to_string_lossy().to_lowercase()).unwrap_or_default();
        let stem_b = b.path.file_stem().map(|s| s.to_string_lossy().to_lowercase()).unwrap_or_default();

        match natord::compare(&stem_a, &stem_b) {
            std::cmp::Ordering::Equal => {
                // Same stem: non-raw first (false < true)
                is_raw_ext(&a.path).cmp(&is_raw_ext(&b.path))
            }
            other => other,
        }
    });
}

pub fn is_raw_ext(path: &Path) -> bool {
    path.extension().and_then(|e| e.to_str()).map(|e| RAW_EXTS.contains(&e.to_lowercase().as_str())).unwrap_or(false)
}

fn is_image_ext(path: &Path) -> bool {
    path.extension().and_then(|s| s.to_str()).map(|ext| {
        let e = ext.to_lowercase();
        matches!(e.as_str(), "jpg"|"jpeg"|"png"|"webp"|"bmp"|"tiff"|"tif"|"avif"|"tga"|"xbm"|"xpm") || RAW_EXTS.contains(&e.as_str())
    }).unwrap_or(false)
}

pub fn scan_for_view(
    paths: &[String],
    sort_order: &str,
    progress_tx: Option<Sender<(usize, usize)>>
) -> (Vec<Vec<FileMetadata>>, Vec<GroupInfo>, Vec<std::path::PathBuf>) {
    let mut all_files = Vec::new();
    let mut subdirs = Vec::new();
    let mut seen_paths = HashSet::new();

    for path_str in paths {
        let path = Path::new(path_str);
        if path.is_dir() {
            if let Ok(entries) = fs::read_dir(path) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let entry_path = entry.path();
                    if entry_path.is_dir() {
                        if let Ok(canonical) = entry_path.canonicalize() { subdirs.push(canonical); }
                    } else if entry_path.is_file() && is_image_ext(&entry_path)
                        && let Ok(canonical) = entry_path.canonicalize() && seen_paths.insert(canonical.clone()) {
                            all_files.push(canonical);
                        }
                }
            }
        } else if path.is_file() && is_image_ext(path) && let Ok(canonical) = path.canonicalize() && seen_paths.insert(canonical.clone()) {
            all_files.push(canonical);
        }
    }
    subdirs.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    if all_files.is_empty() { return (Vec::new(), Vec::new(), subdirs); }

    let total_files = all_files.len();
    if let Some(tx) = &progress_tx { let _ = tx.send((0, total_files)); }

    let processed_count = AtomicUsize::new(0);
    let mut files: Vec<FileMetadata> = all_files.par_iter().filter_map(|path| {
        if let Some(prog_tx) = &progress_tx {
            let current = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
            if current.is_multiple_of(50) || current == total_files { let _ = prog_tx.send((current, total_files)); }
        }
        let metadata = fs::metadata(path).ok()?;
        let size = metadata.len();
        let mtime = DateTime::from(metadata.modified().ok().unwrap_or(UNIX_EPOCH));
        let orientation = get_orientation(path, None);
        // Also apply ignore_dev_id here for consistency in view mode
        let (_, dev_inode) = get_file_identifiers(&metadata, false);

        Some(FileMetadata {
            path: path.clone(),
            size,
            modified: mtime,
            phash: 0,
            pdqhash: None,
            resolution: None,
            content_hash: [0u8; 32],
            orientation,
            dev_inode,
        })
    }).collect();

    sort_files(&mut files, sort_order);
    if files.is_empty() { return (Vec::new(), Vec::new(), subdirs); }
    let info = GroupInfo { max_dist: 0, status: GroupStatus::None };
    (vec![files], vec![info], subdirs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_country_florida() {
        // Simulate the GPS coordinates provided
        let lat = 28.68;
        let lon = -81.31;
        let result = derive_country(lat, lon);
        assert_eq!(
            result,
            Some("Florida, United States of America (the)".to_string())
        );
    }
}
