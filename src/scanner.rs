use chrono::{DateTime, Utc};
use codes_iso_3166::part_1::CountryCode;
use codes_iso_3166::part_2::SubdivisionCode;
use crossbeam_channel::{Sender, unbounded};
use geo::Point;
use image::{DynamicImage, GenericImageView, ImageBuffer, Rgb};
use jpeg_decoder::Decoder as Tier2Decoder;
use libheif_rs::HeifContext;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::UNIX_EPOCH;
use walkdir::WalkDir;
use zune_jpeg::JpegDecoder as ZuneDecoder;

use crate::db::{
    AppContext, CachedCoefficients, DbUpdate, EnrichmentResult, HashValue, compute_meta_key,
    create_feature_update,
};
use crate::exif_extract::extract_gps_lat_lon;
use crate::exif_types::{
    ExifValue, TAG_DERIVED_COUNTRY, TAG_DERIVED_TIMESTAMP, TAG_GPS_LATITUDE, TAG_GPS_LONGITUDE,
    TAG_ORIENTATION,
};
use crate::fileops;
use crate::fileops::get_file_key;
use crate::hamminghash::{HammingHash, MIHIndex, SparseBitSet};
use crate::helper_exif::{get_altitude, get_date_str, get_exif_timestamp, parse_gps_coordinate};
use crate::image_features::ImageFeatures;
use crate::position;
use crate::raw_exif;
use crate::{FileMetadata, GroupInfo, GroupStatus};

pub const RAW_EXTS: &[&str] = &["nef", "dng", "cr2", "cr3", "arw", "orf", "rw2", "raf"];

const JP2_MAX_PIXELS: u64 = 268_435_456;

trait BufReadSeek: std::io::BufRead + std::io::Seek {}
impl<T: std::io::BufRead + std::io::Seek> BufReadSeek for T {}

macro_rules! img_debug {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        eprintln!($($arg)*);
    };
}

pub fn read_exif_data(path: &Path, preloaded_bytes: Option<&[u8]>) -> Option<exif::Exif> {
    let mut reader: Box<dyn BufReadSeek> = match preloaded_bytes {
        Some(bytes) => Box::new(std::io::Cursor::new(bytes)),
        None => {
            let file = fs::File::open(path).ok()?;
            Box::new(std::io::BufReader::new(file))
        }
    };

    exif::Reader::new().read_from_container(&mut reader).ok()
}

pub fn get_orientation(path: &Path, preloaded_bytes: Option<&[u8]>) -> u8 {
    if let Some(exif_data) = read_exif_data(path, preloaded_bytes)
        && let Some(field) = exif_data.get_field(exif::Tag::Orientation, exif::In::PRIMARY)
        && let Some(v @ 1..=8) = field.value.get_uint(0)
    {
        return v as u8;
    }
    1
}

pub fn has_gps_time(path: &Path) -> bool {
    if let Some(exif) = read_exif_data(path, None) {
        return crate::helper_exif::get_date_str(&exif, true).is_some();
    }
    false
}

/// Check if a tag name is a derived value (not a real EXIF tag)
fn is_derived_tag(name: &str) -> bool {
    matches!(name.to_lowercase().as_str(), "derivedcountry" | "country" | "derivedsunposition")
}

/// Get multiple EXIF tags as a vector of (tag_name, value) pairs.
/// For RAW files, falls back to rsraw when kamadak-exif fails.
///
/// Thread Safety: This function reads the file from disk, so it's safe to call
/// from multiple threads on different files. However, calling it on the same
/// file from multiple threads is safe but wasteful (duplicate I/O).
pub fn get_exif_tags(
    path: &Path,
    tag_names: &[String],
    decimal_coords: bool,
    use_gps_utc: bool,
) -> Vec<(String, String)> {
    let exif_data = read_exif_data(path, None);
    let is_raw = is_raw_ext(path);

    eprintln!(
        "[DEBUG-GET-EXIF-TAGS] path='{}', is_raw={}, kamadak_exif_ok={}",
        path.file_name().unwrap_or_default().to_string_lossy(),
        is_raw,
        exif_data.is_some()
    );

    // If kamadak-exif succeeded, use it
    if let Some(ref exif) = exif_data {
        return get_exif_tags_from_kamadak(exif, tag_names, decimal_coords, use_gps_utc);
    }

    // kamadak-exif failed - try rsraw for RAW files
    if is_raw {
        if let Ok(data) = std::fs::read(path) {
            if let Ok(raw) = rsraw::RawImage::open(&data) {
                return get_exif_tags_from_rsraw(&raw, tag_names, decimal_coords);
            }
        }
    }

    Vec::new()
}

/// Extract EXIF tags from kamadak-exif data (original implementation)
fn get_exif_tags_from_kamadak(
    exif_data: &exif::Exif,
    tag_names: &[String],
    decimal_coords: bool,
    use_gps_utc: bool,
) -> Vec<(String, String)> {
    let mut results = Vec::new();

    // Pre-extract GPS coordinates if needed for derived values
    let needs_gps = tag_names.iter().any(|t| is_derived_tag(t));
    let gps_coords = if needs_gps { extract_gps_lat_lon(exif_data) } else { None };

    // Pre-extract Data for Sun Position if needed
    let needs_sun = tag_names.iter().any(|t| t.eq_ignore_ascii_case("DerivedSunPosition"));
    let sun_inputs = if needs_sun && gps_coords.is_some() {
        let alt = get_altitude(exif_data);
        Some(alt)
    } else {
        None
    };

    for tag_name in tag_names {
        // Check for derived tags first
        if let Some(derived_entries) =
            get_derived_value(tag_name, gps_coords, &sun_inputs, exif_data, use_gps_utc)
        {
            results.extend(derived_entries);
        } else if let Some((tag, in_value)) = parse_exif_tag_name(tag_name)
            && let Some(field) = exif_data.get_field(tag, in_value)
        {
            let value_str = format_exif_value(&field.value, tag, decimal_coords);
            results.push((tag_name.clone(), value_str));
        }
    }

    results
}

/// Extract EXIF tags from rsraw data (fallback for RAW files).
/// Maps rsraw's FullRawInfo fields to the requested tag names.
fn get_exif_tags_from_rsraw(
    raw: &rsraw::RawImage,
    tag_names: &[String],
    decimal_coords: bool,
) -> Vec<(String, String)> {
    let info = raw.full_info();
    let mut results = Vec::new();

    eprintln!(
        "[DEBUG-GET-EXIF-TAGS-RSRAW] make='{}', model='{}', iso={}, shutter={}, aperture={}, focal={}",
        info.make, info.model, info.iso_speed, info.shutter, info.aperture, info.focal_len
    );

    for tag_name in tag_names {
        let value = match tag_name.to_lowercase().as_str() {
            "make" => {
                if !info.make.is_empty() {
                    Some(info.make.clone())
                } else {
                    None
                }
            }
            "model" => {
                if !info.model.is_empty() {
                    Some(info.model.clone())
                } else {
                    None
                }
            }
            "software" => {
                if !info.software.is_empty() {
                    Some(info.software.trim().to_string())
                } else {
                    None
                }
            }
            "artist" => {
                if !info.artist.is_empty() {
                    Some(info.artist.clone())
                } else {
                    None
                }
            }
            "iso" | "isospeedratings" | "photographicsensitivity" => {
                if info.iso_speed > 0 {
                    Some(format!("ISO {}", info.iso_speed))
                } else {
                    None
                }
            }
            "exposuretime" | "exposure" => {
                if info.shutter > 0.0 {
                    if info.shutter >= 1.0 {
                        Some(format!("{:.1}s", info.shutter))
                    } else {
                        Some(format!("1/{:.0}s", 1.0 / info.shutter))
                    }
                } else {
                    None
                }
            }
            "fnumber" | "aperture" => {
                if info.aperture > 0.0 {
                    Some(format!("f/{:.1}", info.aperture))
                } else {
                    None
                }
            }
            "focallength" => {
                if info.focal_len > 0.0 {
                    Some(format!("{:.0}mm", info.focal_len))
                } else {
                    None
                }
            }
            "focallengthin35mmfilm" | "focallength35mm" => {
                if info.lens_info.focal_length_in_35mm_format > 0 {
                    Some(format!("{}mm (35mm equiv)", info.lens_info.focal_length_in_35mm_format))
                } else {
                    None
                }
            }
            "lensmodel" | "lens" => {
                if !info.lens_info.lens_name.is_empty() {
                    Some(info.lens_info.lens_name.trim().to_string())
                } else {
                    None
                }
            }
            "lensmake" => {
                if !info.lens_info.lens_make.is_empty() {
                    Some(info.lens_info.lens_make.clone())
                } else {
                    None
                }
            }
            "datetime" | "datetimeoriginal" => {
                if let Some(ref dt) = info.datetime {
                    Some(dt.format("%Y-%m-%d %H:%M:%S").to_string())
                } else {
                    None
                }
            }
            "gpslatitude" => {
                let lat = raw_exif::dms_to_decimal_pub(&info.gps.latitude);
                if lat.abs() > 0.0001 {
                    if decimal_coords {
                        Some(format!("{:.6}°", lat))
                    } else {
                        Some(format_dms_from_decimal(lat as f64))
                    }
                } else {
                    None
                }
            }
            "gpslongitude" => {
                let lon = raw_exif::dms_to_decimal_pub(&info.gps.longitude);
                if lon.abs() > 0.0001 {
                    if decimal_coords {
                        Some(format!("{:.6}°", lon))
                    } else {
                        Some(format_dms_from_decimal(lon as f64))
                    }
                } else {
                    None
                }
            }
            "gpsaltitude" => {
                if info.gps.altitude.abs() > 0.0001 {
                    Some(format!("{:.1}m", info.gps.altitude))
                } else {
                    None
                }
            }
            "derivedcountry" | "country" => {
                let lat = raw_exif::dms_to_decimal_pub(&info.gps.latitude) as f64;
                let lon = raw_exif::dms_to_decimal_pub(&info.gps.longitude) as f64;
                if lat.abs() > 0.0001 || lon.abs() > 0.0001 {
                    derive_country(lat, lon)
                } else {
                    None
                }
            }
            // TODO(rsraw-orientation): When rsraw exposes orientation, add:
            // "orientation" => {
            //     if let Some(orientation) = info.orientation { // or info.flip
            //         Some(format!("{}", orientation))
            //     } else {
            //         None
            //     }
            // }
            // Skip derived sun position for rsraw (requires full timestamp parsing)
            "derivedsunposition" => None,
            // Skip other tags not available in rsraw
            _ => None,
        };

        if let Some(v) = value {
            results.push((tag_name.clone(), v));
        }
    }

    eprintln!("[DEBUG-GET-EXIF-TAGS-RSRAW]   Returning {} tags", results.len());
    results
}

/// Format decimal degrees as DMS string (helper for rsraw GPS display)
fn format_dms_from_decimal(decimal_deg: f64) -> String {
    let abs_deg = decimal_deg.abs();
    let d = abs_deg.floor() as i32;
    let m_float = (abs_deg - d as f64) * 60.0;
    let m = m_float.floor() as i32;
    let s = (m_float - m as f64) * 60.0;

    let sign = if decimal_deg < 0.0 { "-" } else { "" };
    format!("{}{}° {}' {:.1}\"", sign, d, m, s)
}

/// Get derived values based on tag name and available data
/// Returns a vector because one derived tag (like SunPosition) might expand into multiple display lines
fn get_derived_value(
    tag_name: &str,
    gps_coords: Option<(f64, f64)>,
    sun_inputs: &Option<Option<f64>>,
    exif_data: &exif::Exif,
    use_gps_utc: bool,
) -> Option<Vec<(String, String)>> {
    match tag_name.to_lowercase().as_str() {
        "derivedcountry" => {
            let (lat, lon) = gps_coords?;
            let val = derive_country(lat, lon)?;
            Some(vec![("Country".to_string(), val)])
        }
        "derivedsunposition" => {
            let (lat, lon) = gps_coords?;
            let alt_m = sun_inputs.as_ref()?.unwrap_or(0.0);
            // Determine which time to use
            let mut date_str = None;
            let mut final_use_utc = false;

            if use_gps_utc {
                // Try GPS first
                if let Some(d) = get_date_str(exif_data, true) {
                    date_str = Some(d);
                    final_use_utc = true; // Success, we are using UTC
                }
            }

            // Fallback (or default) to Local time
            if date_str.is_none() {
                date_str = get_date_str(exif_data, false);
                // final_use_utc remains false
            }

            if let Some(d) = date_str {
                if let Ok((elev, az, tz)) =
                    position::sun_alt_and_azimuth(&d, lat, lon, Some(alt_m), final_use_utc)
                {
                    // Split into two lines as requested
                    Some(vec![
                        ("Sun Position".to_string(), crate::position::format_sun_pos(elev, az)),
                        ("TZ at GPS pos".to_string(), tz),
                    ])
                } else {
                    None
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

fn jp2_components_to_rgb(
    components: &[jpeg2k::ImageComponent],
    width: u32,
    height: u32,
) -> Option<DynamicImage> {
    if components.len() != 3 {
        return None;
    }

    let r = &components[0];
    let g = &components[1];
    let b = &components[2];

    let pixels = (width * height) as usize;

    // Decide bit depth from component precision
    let is_8bit = r.precision() <= 8 && g.precision() <= 8 && b.precision() <= 8;

    let mut out = Vec::with_capacity(pixels * 3);

    if is_8bit {
        // ---------- 8-bit fast path ----------
        let mut r_it = r.data_u8();
        let mut g_it = g.data_u8();
        let mut b_it = b.data_u8();

        for _ in 0..pixels {
            out.push(r_it.next()?);
            out.push(g_it.next()?);
            out.push(b_it.next()?);
        }
    } else {
        // ---------- 16-bit → normalize to 8-bit ----------
        let mut r_it = r.data_u16();
        let mut g_it = g.data_u16();
        let mut b_it = b.data_u16();

        for _ in 0..pixels {
            out.push((r_it.next()? >> 8) as u8);
            out.push((g_it.next()? >> 8) as u8);
            out.push((b_it.next()? >> 8) as u8);
        }
    }

    let img = ImageBuffer::<Rgb<u8>, _>::from_raw(width, height, out)?;
    Some(DynamicImage::ImageRgb8(img))
}

#[inline]
fn collect_u8<I: Iterator<Item = u8>>(it: I, expected: usize) -> Option<Vec<u8>> {
    let v: Vec<u8> = it.collect();
    (v.len() == expected).then_some(v)
}

#[inline]
fn collect_u16_to_u8<I: Iterator<Item = u16>>(it: I, expected: usize) -> Option<Vec<u8>> {
    let v: Vec<u8> = it.map(|x| (x >> 8) as u8).collect();
    (v.len() == expected).then_some(v)
}

pub fn load_image_fast(path: &Path, bytes: &[u8]) -> Option<image::DynamicImage> {
    let ext =
        path.extension().and_then(|e| e.to_str()).map(|e| e.to_lowercase()).unwrap_or_default();

    img_debug!("[load_image_fast] ext={} bytes={}", ext, bytes.len());

    // Explicitly reject RAWs here so they are handled by the RAW-specific logic
    if crate::scanner::RAW_EXTS.contains(&ext.as_str()) {
        return None;
    }

    match ext.as_str() {
        "jpg" | "jpeg" => {
            // TIER 1: Zune-JPEG
            let mut zune = ZuneDecoder::new(bytes);
            if let Ok(pixels) = zune.decode()
                && let Some(info) = zune.info()
            {
                let w = info.width as u32;
                let h = info.height as u32;
                let len = pixels.len();

                // Robustly handle Grayscale vs RGB based on buffer size
                if len == (w * h) as usize {
                    // Grayscale
                    if let Some(buf) =
                        image::ImageBuffer::<image::Luma<u8>, _>::from_raw(w, h, pixels)
                    {
                        eprintln!(
                            "[DEBUG-LOAD] {:?} -> Zune-JPEG (Grayscale)",
                            path.file_name().unwrap_or_default()
                        );
                        return Some(image::DynamicImage::ImageLuma8(buf));
                    }
                } else if len == (w * h * 3) as usize {
                    // RGB
                    if let Some(buf) =
                        image::ImageBuffer::<image::Rgb<u8>, _>::from_raw(w, h, pixels)
                    {
                        eprintln!(
                            "[DEBUG-LOAD] {:?} -> Zune-JPEG (RGB)",
                            path.file_name().unwrap_or_default()
                        );
                        return Some(image::DynamicImage::ImageRgb8(buf));
                    }
                } else if len == (w * h * 4) as usize {
                    // CMYK or RGBA (Zune might output RGBA for CMYK)
                    if let Some(buf) =
                        image::ImageBuffer::<image::Rgba<u8>, _>::from_raw(w, h, pixels)
                    {
                        eprintln!(
                            "[DEBUG-LOAD] {:?} -> Zune-JPEG (RGBA/CMYK)",
                            path.file_name().unwrap_or_default()
                        );
                        return Some(image::DynamicImage::ImageRgba8(buf));
                    }
                }
            }

            // TIER 2: jpeg-decoder (Fallback)
            let mut decoder = Tier2Decoder::new(std::io::Cursor::new(bytes));
            if let Ok(pixels) = decoder.decode() {
                let info = decoder.info();
                let w = info.unwrap().width as u32;
                let h = info.unwrap().height as u32;
                let len = pixels.len();

                if len == (w * h) as usize {
                    if let Some(buf) =
                        image::ImageBuffer::<image::Luma<u8>, _>::from_raw(w, h, pixels)
                    {
                        eprintln!(
                            "[DEBUG-LOAD] {:?} -> jpeg-decoder (Fallback Grayscale)",
                            path.file_name().unwrap_or_default()
                        );
                        return Some(image::DynamicImage::ImageLuma8(buf));
                    }
                } else if len == (w * h * 3) as usize
                    && let Some(buf) =
                        image::ImageBuffer::<image::Rgb<u8>, _>::from_raw(w, h, pixels)
                {
                    eprintln!(
                        "[DEBUG-LOAD] {:?} -> jpeg-decoder (Fallback RGB)",
                        path.file_name().unwrap_or_default()
                    );
                    return Some(image::DynamicImage::ImageRgb8(buf));
                }
            }
        }
        // TODO: Replace manual component conversion when jpeg2k
        // gains native support for image 0.25+ DynamicImage conversion.
        "jp2" | "j2k" => {
            use jpeg2k::Image;

            img_debug!("[jp2] attempting decode");

            let jp2 = match Image::from_bytes(bytes) {
                Ok(v) => v,
                Err(e) => {
                    img_debug!("[jp2] decode failed: {:?}", e);
                    return None;
                }
            };

            let width = jp2.width();
            let height = jp2.height();
            let components = jp2.components();

            img_debug!("[jp2] decoded: {}x{}, components={}", width, height, components.len());

            if width == 0 || height == 0 {
                img_debug!("[jp2] invalid dimensions");
                return None;
            }

            if (width as u64) * (height as u64) > JP2_MAX_PIXELS {
                img_debug!("[jp2] image too large");
                return None;
            }

            for (i, c) in components.iter().enumerate() {
                img_debug!(
                    "[jp2] component {}: len8={}, len16={}",
                    i,
                    c.data_u8().count(),
                    c.data_u16().count()
                );
            }

            if components.len() == 4 {
                img_debug!("[jp2] treating 4 components as RGBA");

                let pixels = (width * height) as usize;

                let r = collect_u8(components[0].data_u8(), pixels)
                    .or_else(|| collect_u16_to_u8(components[0].data_u16(), pixels))?;
                let g = collect_u8(components[1].data_u8(), pixels)
                    .or_else(|| collect_u16_to_u8(components[1].data_u16(), pixels))?;
                let b = collect_u8(components[2].data_u8(), pixels)
                    .or_else(|| collect_u16_to_u8(components[2].data_u16(), pixels))?;
                let a = collect_u8(components[3].data_u8(), pixels)
                    .or_else(|| collect_u16_to_u8(components[3].data_u16(), pixels))?;

                let mut out = Vec::with_capacity(pixels * 4);
                for i in 0..pixels {
                    out.push(r[i]);
                    out.push(g[i]);
                    out.push(b[i]);
                    out.push(a[i]);
                }

                let img = image::RgbaImage::from_raw(width, height, out)?;
                return Some(image::DynamicImage::ImageRgba8(img));
            }

            if components.len() == 3 {
                return jp2_components_to_rgb(components, width, height);
            }

            img_debug!("[jp2] unsupported component layout");
            return None;
        }

        "jxl" => {
            use image::ImageDecoder;
            use jxl_oxide::integration::JxlDecoder;
            use std::io::Cursor;

            img_debug!("[jxl] attempting decode");

            let decoder = match JxlDecoder::new(Cursor::new(bytes)) {
                Ok(d) => d,
                Err(e) => {
                    img_debug!("[jxl] decoder init failed: {:?}", e);
                    return None;
                }
            };

            let (w, h) = decoder.dimensions();
            img_debug!("[jxl] dimensions: {}x{}", w, h);

            match DynamicImage::from_decoder(decoder) {
                Ok(img) => {
                    img_debug!("[jxl] decode successful");
                    return Some(img);
                }
                Err(e) => {
                    img_debug!("[jxl] decode failed: {:?}", e);
                    return None;
                }
            }
        }

        _ => {}
    }

    // This handles AVIF, WebP, PCX etc (image-extras) and corrupted files.
    eprintln!("[DEBUG-LOAD] {:?} -> Fallback (image crate)", path.file_name().unwrap_or_default());

    // Chain with_guessed_format(). If it fails (IO error), fallback to a fresh reader.
    let mut reader = image::ImageReader::new(std::io::Cursor::new(bytes))
        .with_guessed_format()
        .unwrap_or_else(|_| image::ImageReader::new(std::io::Cursor::new(bytes)));

    // Fallback to extension if format is still unknown
    if reader.format().is_none()
        && let Ok(fmt) = image::ImageFormat::from_path(path)
    {
        reader.set_format(fmt);
    }

    reader.decode().ok()
}

/// Derive country name from GPS coordinates using country-boundaries
fn derive_country(lat: f64, lon: f64) -> Option<String> {
    use country_boundaries::{BOUNDARIES_ODBL_360X180, CountryBoundaries, LatLon};

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
        SubdivisionCode::from_str(&formatted_code).ok().map(|s| s.name().to_string())
    });

    // 2. Get country name (e.g., "FI" -> "Finland")
    let country_name = country_code
        .and_then(|code| CountryCode::from_str(code).ok().map(|c| c.short_name().to_string()));

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
        "gpstimestamp" => exif::Tag::GPSTimeStamp,
        "gpsdatestamp" => exif::Tag::GPSDateStamp,
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
        ("GPSTimeStamp", "Time of last GPS sync in UTC"),
        ("GPSDateStamp", "Date of last GPS sync in UTC"),
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
        ("DerivedSunPosition", "Sun Altitude and Azimuth calculated from time & location"),
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
        }
        exif::Tag::ExposureTime => {
            if let Some(r) = value.get_uint(0) {
                if let exif::Value::Rational(rats) = value
                    && !rats.is_empty()
                {
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
        }
        exif::Tag::FNumber => {
            if let exif::Value::Rational(rats) = value
                && !rats.is_empty()
                && rats[0].denom > 0
            {
                return format!("f/{:.1}", rats[0].num as f64 / rats[0].denom as f64);
            }
            clean_exif_string(&value.display_as(tag).to_string())
        }
        exif::Tag::FocalLength => {
            if let exif::Value::Rational(rats) = value
                && !rats.is_empty()
                && rats[0].denom > 0
            {
                return format!("{}mm", rats[0].num / rats[0].denom);
            }
            clean_exif_string(&value.display_as(tag).to_string())
        }
        exif::Tag::PhotographicSensitivity => {
            if let Some(iso) = value.get_uint(0) {
                format!("ISO {}", iso)
            } else {
                clean_exif_string(&value.display_as(tag).to_string())
            }
        }
        exif::Tag::FocalLengthIn35mmFilm => {
            if let Some(fl) = value.get_uint(0) {
                format!("{}mm (35mm equiv)", fl)
            } else {
                clean_exif_string(&value.display_as(tag).to_string())
            }
        }
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
        let parts: Vec<&str> = s
            .split([',', '"'])
            .map(|p| p.trim())
            .filter(|p| !p.is_empty() && *p != "'" && p.len() > 1)
            .collect();

        if let Some(first) = parts.first() {
            return first.to_string();
        }
    }

    // Remove any trailing garbage (null bytes represented as empty quotes, etc.)
    let cleaned = s.trim_end_matches(|c: char| {
        c == '"' || c == '\'' || c == ',' || c.is_whitespace() || c == '\0'
    });

    cleaned.to_string()
}

fn get_resolution(path: &Path, bytes: Option<&[u8]>) -> Option<(u32, u32)> {
    // 1. Handle RAW images
    if is_raw_ext(path) {
        let data_cow;
        let data_slice = match bytes {
            Some(b) => b,
            None => {
                data_cow = fs::read(path).ok()?;
                &data_cow
            }
        };

        if let Ok(raw) = rsraw::RawImage::open(data_slice) {
            return Some((raw.width(), raw.height()));
        }
        return None;
    }

    // 2. Handle HEIC/HEIF specifically
    if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
        let ext = ext.to_lowercase();
        if ext == "heic" || ext == "heif" {
            let ctx = match bytes {
                Some(b) => HeifContext::read_from_bytes(b).ok()?,
                None => HeifContext::read_from_file(path.to_str()?).ok()?,
            };

            if let Ok(handle) = ctx.primary_image_handle() {
                return Some((handle.width(), handle.height()));
            }
        }
    }

    // 3. Handle Standard Formats
    let reader_obj: Box<dyn BufReadSeek> = match bytes {
        Some(b) => Box::new(std::io::Cursor::new(b)),
        None => {
            // Manually open the file so we can wrap it in BufReader + Box
            let file = fs::File::open(path).ok()?;
            Box::new(std::io::BufReader::new(file))
        }
    };

    // Now we create the ImageReader using the unified Box type
    if let Ok(reader) = image::ImageReader::new(reader_obj).with_guessed_format()
        && let Ok(dims) = reader.into_dimensions()
    {
        return Some(dims);
    }

    None
}

#[derive(Clone)]
pub struct ScanConfig {
    pub paths: Vec<String>,
    pub rehash: bool,
    pub similarity: u32,
    pub group_by: String,
    pub extensions: Vec<String>,
    pub ignore_same_stem: bool,
    pub calc_pixel_hash: bool,
}

#[derive(Clone)]
struct ScannedFile {
    pub path: std::path::PathBuf,
    pub size: u64,
    pub modified: DateTime<Utc>,
    pub resolution: Option<(u32, u32)>,
    pub content_hash: [u8; 32],
    pub orientation: u8,
    pub gps_pos: Option<Point<f64>>,
    pub unique_file_id: u128,
    pub pdqhash: Option<[u8; 32]>,
    pub pdq_features: Option<Arc<crate::pdqhash::PdqFeatures>>,
    pub pixel_hash: Option<[u8; 32]>,
    pub exif_timestamp: Option<i64>,
}

impl ScannedFile {
    fn to_file_metadata(&self) -> FileMetadata {
        FileMetadata {
            path: self.path.clone(),
            size: self.size,
            modified: self.modified,
            pdqhash: self.pdqhash,
            resolution: self.resolution,
            content_hash: self.content_hash,
            orientation: self.orientation,
            gps_pos: self.gps_pos,
            unique_file_id: self.unique_file_id,
            pixel_hash: self.pixel_hash,
            exif_timestamp: self.exif_timestamp,
        }
    }
}

pub fn scan_and_group(
    config: &ScanConfig,
    ctx: &AppContext,
    progress_tx: Option<Sender<(usize, usize)>>,
) -> (Vec<Vec<FileMetadata>>, Vec<GroupInfo>) {
    use std::time::Instant;

    let ctx_ref = ctx;
    let force_rehash = config.rehash;

    let mut all_files = Vec::new();
    let mut seen_paths = HashSet::new();
    for path_str in &config.paths {
        let path = Path::new(path_str);
        if path.is_dir() {
            for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
                if is_image_ext(entry.path())
                    && let Ok(canonical) = entry.path().canonicalize()
                    && seen_paths.insert(canonical.clone())
                {
                    all_files.push(canonical);
                }
            }
        } else if path.is_file()
            && is_image_ext(path)
            && let Ok(canonical) = path.canonicalize()
            && seen_paths.insert(canonical.clone())
        {
            all_files.push(canonical);
        }
    }

    if all_files.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let total_files = all_files.len();
    if let Some(tx) = &progress_tx {
        let _ = tx.send((0, total_files));
    }

    let hash_start = Instant::now();
    let (tx, rx) = unbounded();
    let db_handle = ctx.start_db_writer(rx);
    let processed_count = AtomicUsize::new(0);

    let mut valid_files: Vec<ScannedFile> = all_files
        .par_iter()
        .filter_map(|path| {
            if let Some(prog_tx) = &progress_tx {
                let current = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
                if current.is_multiple_of(10) || current == total_files {
                    let _ = prog_tx.send((current, total_files));
                }
            }

            let metadata = fs::metadata(path).ok()?;
            let size = metadata.len();
            let mtime = metadata.modified().ok().unwrap_or(UNIX_EPOCH);
            let mtime_utc: DateTime<Utc> = DateTime::from(mtime);
            let mtime_ns = mtime.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
            let unique_file_id = fileops::get_file_key(path)?;

            let meta_key = compute_meta_key(&ctx_ref.meta_key, mtime_ns, size, unique_file_id);
            if false {
                eprintln!(
                    "[DEBUG] mtime_ns/size/unique_file_id {} = {} {} {}",
                    path.display(),
                    mtime_ns,
                    size,
                    unique_file_id
                );
            }
            let mut pdqhash: Option<[u8; 32]> = None;
            let mut pdq_features: Option<Arc<crate::pdqhash::PdqFeatures>> = None;
            // IMPORTANT: new_meta tracks updates to the file_metadata DB.
            // Even if we hit the cache, we MUST set this to refresh the timestamp.
            let mut new_meta = None;

            let mut new_hash = None;
            let mut new_features = None;
            let mut new_coeffs = None; // Coefficients stored separately
            let mut resolution = None;
            let mut ck = [0u8; 32];
            let mut orientation = 1;
            let mut gps_pos = None;
            let mut exif_timestamp: Option<i64> = None;
            let mut cache_hit_full = false;
            let mut pixel_hash: Option<[u8; 32]> = None; // Init
            let mut new_pixel = None; // For DB update

            if !force_rehash && let Ok(Some(ch)) = ctx_ref.get_content_hash(&meta_key) {
                eprintln!(
                    "Cache HIT  meta_key {:x?} for {:?}",
                    hex::encode(meta_key),
                    path.display()
                );
                ck = ch;
                // Refresh timestamp
                new_meta = Some((meta_key, ck));
                if let Ok(Some(h)) = ctx_ref.get_pdqhash(&ch) {
                    pdqhash = Some(h);
                    if let Ok(Some(feats)) = ctx_ref.get_features(&ch) {
                        resolution = Some((feats.width, feats.height));
                        orientation = feats.orientation();
                        gps_pos = feats.gps_pos();

                        // Get coefficients from separate db
                        if let Ok(Some(coeff_vec)) = ctx_ref.get_coefficients(&ch)
                            && coeff_vec.len() == 256
                        {
                            let mut coeffs = [0.0; 256];
                            coeffs.copy_from_slice(&coeff_vec);
                            pdq_features = Some(Arc::new(crate::pdqhash::PdqFeatures {
                                coefficients: coeffs,
                            }));
                            cache_hit_full = true;
                        }
                    }
                }
                // If user wants pixel hash, try to fetch it from DB.
                if config.calc_pixel_hash {
                    if let Ok(Some(ph)) = ctx_ref.get_pixel_hash(&ch) {
                        pixel_hash = Some(ph);
                    } else {
                        // Missing in DB! Force load below to calculate it.
                        cache_hit_full = false;
                    }
                }
            }

            if !cache_hit_full {
                eprintln!(
                    "Cache MISS meta_key {:x?} for {:?}",
                    hex::encode(meta_key),
                    path.display()
                );
                let bytes = fs::read(path).ok();

                if let Some(ref b) = bytes {
                    // Read Orientation, GPS location, and EXIF timestamp
                    // For RAW files, we may need to fall back to rsraw if kamadak-exif fails
                    let exif_data = read_exif_data(path, Some(b));

                    if let Some(ref exif) = exif_data {
                        // kamadak-exif succeeded - extract data the normal way
                        if let Some((lat, lon)) = extract_gps_lat_lon(exif) {
                            gps_pos = Some(Point::new(lon, lat)); // Geo uses (x, y) = (lon, lat)
                        }
                        if let Some(field) =
                            exif.get_field(exif::Tag::Orientation, exif::In::PRIMARY)
                            && let Some(v @ 1..=8) = field.value.get_uint(0)
                        {
                            orientation = v as u8;
                        }
                        exif_timestamp = get_exif_timestamp(exif);
                    } else if is_raw_ext(path) {
                        // kamadak-exif failed on RAW file - try rsraw as fallback
                        if let Ok(raw) = rsraw::RawImage::open(b) {
                            // Get GPS from rsraw
                            if let Some(point) = raw_exif::get_gps_point_from_raw(&raw) {
                                gps_pos = Some(point);
                            }
                            // Get timestamp from rsraw
                            exif_timestamp = raw_exif::get_timestamp_from_raw(&raw);
                            // Note: orientation is NOT available from rsraw
                            // It will remain at the default value of 1
                        }
                    }
                    // 2. Calculate file hash if needed
                    if ck == [0u8; 32] {
                        let ch = blake3::keyed_hash(&ctx_ref.content_key, b);
                        ck = *ch.as_bytes();
                        new_meta = Some((meta_key, ck));
                    }

                    // 3. Load Image ONCE using the FAST loader
                    let mut img_for_hashing: Option<image::DynamicImage> = None;

                    if is_raw_ext(path) {
                        // RAW FILE: Extract Largest JPEG Thumbnail
                        // We need the image for PDQ even if pixel_hash is disabled.
                        if let Ok(mut raw) = rsraw::RawImage::open(b)
                            && let Ok(thumbs) = raw.extract_thumbs()
                        {
                            // Find largest JPEG thumbnail
                            if let Some(thumb) = thumbs
                                .into_iter()
                                .filter(|t| matches!(t.format, rsraw::ThumbFormat::Jpeg))
                                .max_by_key(|t| t.width * t.height)
                            {
                                // Decode using our robust fast loader.
                                img_for_hashing =
                                    load_image_fast(Path::new("raw_thumb.jpg"), &thumb.data);

                                if let Some(img) = &img_for_hashing
                                    && resolution.is_none()
                                {
                                    resolution = Some(img.dimensions());
                                }
                            }
                        }
                        // Fallback for resolution if thumbnail extraction failed or we didn't calculate hash
                        if resolution.is_none() {
                            resolution = get_resolution(path, Some(b));
                        }
                    } else {
                        // STANDARD IMAGE: Use fast loader directly
                        img_for_hashing = load_image_fast(path, b);
                    }

                    if let Some(img) = &img_for_hashing {
                        // Get resolution from the loaded image
                        if resolution.is_none() {
                            resolution = Some(img.dimensions());
                        }

                        // 4. Calculate Pixel Hash of 16bit RGBA (Content Identical Check)
                        if config.calc_pixel_hash && pixel_hash.is_none() {
                            // This ensures 16-bit PNGs != 8-bit PNGs unless the extra bits are purely padding.
                            let rgba16 = img.to_rgba16();
                            let raw_u16 = rgba16.as_raw();
                            let raw_bytes = unsafe {
                                std::slice::from_raw_parts(
                                    raw_u16.as_ptr() as *const u8,
                                    raw_u16.len() * 2, // 2 bytes per u16
                                )
                            };
                            let ph = *blake3::hash(raw_bytes).as_bytes();
                            eprintln!(
                                "[DEBUG-PIXEL_HASH 16BIT] {:?} : {}",
                                path.file_name().unwrap_or_default(),
                                hex::encode(ph)
                            );
                            pixel_hash = Some(ph);
                            new_pixel = Some((ck, ph));
                        }

                        // Use 'img' directly - do NOT call load_from_memory again
                        if let Some((features, _)) = crate::pdqhash::generate_pdq_features(img) {
                            let hash = features.to_hash();
                            pdqhash = Some(hash);

                            let mut coeffs = [0.0; 256];
                            coeffs.copy_from_slice(&features.coefficients);
                            let feats = crate::pdqhash::PdqFeatures { coefficients: coeffs };
                            pdq_features = Some(Arc::new(feats.clone()));

                            // Build ImageFeatures from the data we have
                            let (w, h) = resolution.unwrap_or((0, 0));
                            let mut img_features = if let Some(exif) = read_exif_data(path, Some(b))
                            {
                                crate::exif_extract::build_image_features(w, h, &exif, true, false)
                            } else {
                                ImageFeatures::new(w, h)
                            };

                            // Add orientation if not default
                            if orientation != 1 {
                                img_features.insert_tag(
                                    TAG_ORIENTATION,
                                    ExifValue::Short(orientation as u16),
                                );
                            }

                            // Add GPS position if available
                            if let Some(pos) = gps_pos {
                                img_features
                                    .insert_tag(TAG_GPS_LATITUDE, ExifValue::Float(pos.y()));
                                img_features.insert_tag(
                                    TAG_GPS_LONGITUDE,
                                    ExifValue::Float(pos.x()),
                                );
                            }

                            // Add timestamp if available
                            if let Some(ts) = exif_timestamp {
                                img_features
                                    .insert_tag(TAG_DERIVED_TIMESTAMP, ExifValue::Long64(ts));
                            }

                            let cached_coeffs =
                                CachedCoefficients { coefficients: features.coefficients.to_vec() };

                            if new_hash.is_none() {
                                new_hash = Some((ck, HashValue::PdqHash(hash)));
                            }
                            new_features = Some((ck, img_features));
                            new_coeffs = Some((ck, cached_coeffs));
                        }
                    } else {
                        // Fallback: If image failed to decode (e.g. corrupt),
                        // but we might still get resolution from headers for RAWs
                        if resolution.is_none() {
                            resolution = get_resolution(path, Some(b));
                        }
                    }
                }
            }

            if new_meta.is_some()
                || new_hash.is_some()
                || new_features.is_some()
                || new_coeffs.is_some()
                || new_pixel.is_some()
            {
                let _ = tx.send((new_meta, new_hash, new_features, new_coeffs, new_pixel));
            }

            Some(ScannedFile {
                path: path.clone(),
                size,
                modified: mtime_utc,
                resolution,
                content_hash: ck,
                orientation,
                gps_pos,
                unique_file_id,
                pdqhash,
                pdq_features,
                pixel_hash,
                exif_timestamp,
            })
        })
        .collect();

    drop(tx);
    db_handle.join().expect("DB writer thread panicked");

    // Deduplicate PDQ Features for Hardlinks
    let mut feature_cache: HashMap<u128, Arc<crate::pdqhash::PdqFeatures>> = HashMap::new();

    for file in valid_files.iter_mut() {
        let unique_file_id = file.unique_file_id;
        if let Some(features) = &file.pdq_features {
            if let Some(cached) = feature_cache.get(&unique_file_id) {
                // Found existing features for this ID, reuse the pointer!
                file.pdq_features = Some(cached.clone());
            } else {
                // First time seeing this ID, cache it
                feature_cache.insert(unique_file_id, features.clone());
            }
        }
    }

    let hash_elapsed = hash_start.elapsed();
    eprintln!("[DEBUG] Algorithm: PDQ hash");
    eprintln!("[DEBUG] Hashes loaded: {} in {:.2}s", valid_files.len(), hash_elapsed.as_secs_f64());

    let group_start = Instant::now();
    let (processed_groups, processed_infos, comparison_count) =
        group_with_pdqhash(&valid_files, config);
    let group_elapsed = group_start.elapsed();

    eprintln!(
        "[DEBUG] Grouping: {} groups found in {:.2}s ({} comparisons)",
        processed_groups.len(),
        group_elapsed.as_secs_f64(),
        comparison_count
    );

    let mut combined: Vec<_> = processed_groups.into_iter().zip(processed_infos).collect();
    combined.sort_by(|(g1, info1), (g2, info2)| {
        let has_ident1 = info1.status != GroupStatus::None;
        let has_ident2 = info2.status != GroupStatus::None;
        if has_ident1 != has_ident2 {
            return has_ident2.cmp(&has_ident1);
        }
        if info1.max_dist != info2.max_dist {
            return info1.max_dist.cmp(&info2.max_dist);
        }
        let s1 = g1.first().map(|f| f.size).unwrap_or(0);
        let s2 = g2.first().map(|f| f.size).unwrap_or(0);
        s2.cmp(&s1)
    });

    combined.into_iter().unzip()
}

// --- 1. Define Strategy Trait
trait GroupingStrategy<H>: Sync + Send {
    fn extract_hash(&self, file: &ScannedFile) -> Option<H>;
    // Write variants into a fixed-size buffer to avoid Vec allocation.
    // Returns the number of variants written.
    fn generate_variants(&self, file: &ScannedFile, hash: H, out: &mut [H; 8]) -> usize;
}

struct PdqStrategy;
impl GroupingStrategy<[u8; 32]> for PdqStrategy {
    #[inline(always)]
    fn extract_hash(&self, file: &ScannedFile) -> Option<[u8; 32]> {
        file.pdqhash
    }

    #[inline(always)]
    fn generate_variants(
        &self,
        file: &ScannedFile,
        hash: [u8; 32],
        out: &mut [[u8; 32]; 8],
    ) -> usize {
        if let Some(features) = &file.pdq_features {
            let vars = features.generate_dihedral_hashes();
            let count = vars.len().min(8);
            for (i, v) in vars.iter().enumerate().take(count) {
                out[i] = *v;
            }
            count
        } else {
            out[0] = hash;
            1
        }
    }
}

// --- 2. Optimized Generic Grouping ---
fn group_files_generic<H, S>(
    valid_files: &[ScannedFile],
    config: &ScanConfig,
    strategy: S, // Pass the struct, not closures
) -> (Vec<Vec<FileMetadata>>, Vec<GroupInfo>, usize)
where
    H: HammingHash + std::fmt::Debug + Clone + Copy + Default,
    S: GroupingStrategy<H>,
{
    // Collect hashes AND their original indices
    let valid_entries: Vec<(usize, H)> = valid_files
        .iter()
        .enumerate()
        .filter_map(|(i, f)| strategy.extract_hash(f).map(|h| (i, h)))
        .collect();

    if valid_entries.is_empty() {
        return (Vec::new(), Vec::new(), 0);
    }

    let hashes: Vec<H> = valid_entries.iter().map(|(_, h)| *h).collect();
    let dense_to_sparse: Vec<usize> = valid_entries.iter().map(|(i, _)| *i).collect();

    let mih = MIHIndex::new(hashes.clone());
    let n = valid_files.len();

    // Manually chunk the data to amortize allocation costs.
    const CHUNK_SIZE: usize = 2000;

    // Flattened edge list (A, B)
    let edges: Vec<(u32, u32)> = valid_files
        .par_chunks(CHUNK_SIZE)
        .enumerate() // This gives us the chunk index (0, 1, 2...)
        .map_init(
            || (SparseBitSet::new(n), Vec::new(), [H::default(); 8]),
            |(visited, local_edges, variants_buf), (chunk_idx, chunk)| {
                local_edges.clear();

                // Calculate the real global index for the first file in this chunk
                let chunk_base_idx = chunk_idx * CHUNK_SIZE;

                for (offset, file) in chunk.iter().enumerate() {
                    let i = chunk_base_idx + offset; // Global index of current file

                    if let Some(hash) = strategy.extract_hash(file) {
                        // Generate variants into stack buffer (no Vec alloc)
                        let count = strategy.generate_variants(file, hash, variants_buf);
                        let variants = &variants_buf[0..count];

                        for &variant in variants {
                            // Each variant is a distinct query. We must allow the same candidate
                            // to be checked again if it matches a different rotation.
                            visited.clear();

                            // Manual inline of check_bucket to ensure no closure overhead
                            for k in 0..H::NUM_CHUNKS {
                                let q_chunk = variant.get_chunk(k);
                                let chunk_base = k * H::NUM_BUCKETS;

                                // Inline the check_neighbors logic:
                                let check_neighbors =
                                    config.similarity / (H::NUM_CHUNKS as u32) >= 1;
                                let limit =
                                    if check_neighbors { 1 + H::bit_width_per_chunk() } else { 1 };

                                for pass in 0..limit {
                                    let query_val = if pass == 0 {
                                        q_chunk
                                    } else {
                                        q_chunk ^ (1 << (pass - 1))
                                    };

                                    let flat_idx = chunk_base + query_val as usize;
                                    let start =
                                        unsafe { *mih.offsets.get_unchecked(flat_idx) } as usize;
                                    let end = unsafe { *mih.offsets.get_unchecked(flat_idx + 1) }
                                        as usize;

                                    // Direct pointer access to avoid bounds checks in tight loop
                                    if start < end {
                                        let bucket =
                                            unsafe { mih.values.get_unchecked(start..end) };
                                        for &dense_id in bucket {
                                            // dense_id is the index in `hashes` / `dense_to_sparse`
                                            // We need to map it to the real file index `cand_idx`
                                            let cand_idx = unsafe {
                                                *dense_to_sparse.get_unchecked(dense_id as usize)
                                            };

                                            // Only record edges where cand_idx > i
                                            if cand_idx <= i {
                                                continue;
                                            }

                                            if visited.set(cand_idx) {
                                                continue;
                                            }

                                            let cand_hash = unsafe {
                                                mih.db_hashes.get_unchecked(dense_id as usize)
                                            };
                                            if variant.hamming_distance(cand_hash)
                                                <= config.similarity
                                            {
                                                local_edges.push((i as u32, cand_idx as u32));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                local_edges.clone()
            },
        )
        .flatten()
        .collect();

    let comparison_count = edges.len();

    // Union-Find to build groups from Edge List
    let mut parent: Vec<usize> = (0..n).collect();

    fn find(parent: &mut [usize], i: usize) -> usize {
        let mut root = i;
        while root != parent[root] {
            root = parent[root];
        }
        let mut curr = i;
        while curr != root {
            let next = parent[curr];
            parent[curr] = root;
            curr = next;
        }
        root
    }

    fn union(parent: &mut [usize], i: usize, j: usize) {
        let root_i = find(parent, i);
        let root_j = find(parent, j);
        if root_i != root_j {
            parent[root_i] = root_j;
        }
    }

    for (u, v) in edges {
        union(&mut parent, u as usize, v as usize);
    }

    // Collect components
    let mut groups_map: HashMap<usize, Vec<u32>> = HashMap::new();
    for i in 0..n {
        let root = find(&mut parent, i);
        if root != i || groups_map.contains_key(&root) {
            groups_map.entry(root).or_default().push(i as u32);
        }
    }

    let raw_groups: Vec<Vec<u32>> = groups_map.into_values().filter(|g| g.len() > 1).collect();

    // Merge RAW+JPG logic
    let groups = merge_groups_by_stem(raw_groups, valid_files);

    // Process Metadata
    let (g, i) = process_raw_groups(groups, valid_files, config);

    (g, i, comparison_count)
}

// --- 3. Wrapper Functions ---

fn group_with_pdqhash(
    valid_files: &[ScannedFile],
    config: &ScanConfig,
) -> (Vec<Vec<FileMetadata>>, Vec<GroupInfo>, usize) {
    group_files_generic(valid_files, config, PdqStrategy)
}

pub fn analyze_group(
    files: &mut Vec<FileMetadata>,
    sort_order: &str,
    #[allow(unused)] ext_priorities: &HashMap<String, usize>,
) -> GroupInfo {
    if files.is_empty() {
        return GroupInfo { max_dist: 0, status: GroupStatus::None };
    }

    // 1. Count Bit-Identical (Content Hash)
    let mut bit_counts = HashMap::new();
    for f in files.iter() {
        *bit_counts.entry(f.content_hash).or_insert(0) += 1;
    }

    // 2. Count Pixel-Identical (Pixel Hash)
    let mut pixel_counts = HashMap::new();
    for f in files.iter() {
        if let Some(ph) = f.pixel_hash {
            *pixel_counts.entry(ph).or_insert(0) += 1;
        }
    }

    // 3. Partition: Anything that is a duplicate (Bit OR Pixel) goes to the top
    let (mut duplicates, mut unique): (Vec<FileMetadata>, Vec<FileMetadata>) =
        files.drain(..).partition(|f| {
            let is_bit_dupe = *bit_counts.get(&f.content_hash).unwrap_or(&0) > 1;
            let is_pixel_dupe =
                f.pixel_hash.map(|ph| *pixel_counts.get(&ph).unwrap_or(&0) > 1).unwrap_or(false);
            is_bit_dupe || is_pixel_dupe
        });

    duplicates.sort_by_cached_key(|f| {
        (
            f.pixel_hash,
            f.content_hash,
            f.path.file_name().unwrap_or_default().to_string_lossy().to_string(),
        )
    });

    // 5. Sort Unique: Standard user sort
    sort_files(&mut unique, sort_order);

    // 6. Combine
    files.append(&mut duplicates);
    files.append(&mut unique);

    let max_d = if let Some(pivot) = files.first().and_then(|f| f.pdqhash) {
        files
            .iter()
            .filter_map(|f| f.pdqhash)
            .map(|h| pivot.hamming_distance(&h))
            .max()
            .unwrap_or(0)
    } else {
        0
    };

    let has_duplicates = !bit_counts.values().all(|&c| c == 1);
    let all_identical = bit_counts.len() == 1;
    let status = if all_identical {
        GroupStatus::AllIdentical
    } else if has_duplicates {
        GroupStatus::SomeIdentical
    } else {
        GroupStatus::None
    };

    GroupInfo { max_dist: max_d, status }
}

fn merge_groups_by_stem(groups: Vec<Vec<u32>>, valid_files: &[ScannedFile]) -> Vec<Vec<u32>> {
    if groups.len() < 2 {
        return groups;
    }

    use rustc_hash::FxHasher;
    use std::hash::{Hash, Hasher};

    // Helper to hash a path component to u64 quickly
    fn hash_component<T: Hash>(t: T) -> u64 {
        let mut s = FxHasher::default();
        t.hash(&mut s);
        s.finish()
    }

    // Flatten groups into a sortable list of (ParentHash, StemHash, GroupIndex)
    let mut entries: Vec<(u64, u64, usize)> = Vec::with_capacity(valid_files.len());

    for (g_idx, group) in groups.iter().enumerate() {
        for &f_idx in group {
            // Safety: Indices are guaranteed valid by upstream logic
            let f = &valid_files[f_idx as usize];
            if let (Some(parent), Some(stem)) = (f.path.parent(), f.path.file_stem()) {
                let p_hash = hash_component(parent);
                let s_hash = hash_component(stem);
                entries.push((p_hash, s_hash, g_idx));
            }
        }
    }

    // Sort by Parent -> Stem (Grouping identical stems together)
    entries.par_sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    // Union-Find Merge Logic
    let mut parent: Vec<usize> = (0..groups.len()).collect();
    fn find(parent: &mut [usize], i: usize) -> usize {
        let mut root = i;
        while root != parent[root] {
            root = parent[root];
        }
        let mut curr = i;
        while curr != root {
            let next = parent[curr];
            parent[curr] = root;
            curr = next;
        }
        root
    }
    fn union(parent: &mut [usize], i: usize, j: usize) {
        let root_i = find(parent, i);
        let root_j = find(parent, j);
        if root_i != root_j {
            parent[root_i] = root_j;
        }
    }

    // Iterate linearly and merge adjacent identical stems
    for w in entries.windows(2) {
        if w[0].0 == w[1].0 && w[0].1 == w[1].1 {
            union(&mut parent, w[0].2, w[1].2);
        }
    }

    // Rebuild Groups
    let mut merged_map: HashMap<usize, Vec<u32>> = HashMap::new();
    for (old_idx, group) in groups.into_iter().enumerate() {
        let root = find(&mut parent, old_idx);
        merged_map.entry(root).or_default().extend(group);
    }

    merged_map
        .into_values()
        .map(|mut g| {
            g.sort_unstable();
            g.dedup();
            g
        })
        .collect()
}

// PARALLELIZED GROUP PROCESSING
fn process_raw_groups(
    raw_groups: Vec<Vec<u32>>,
    valid_files: &[ScannedFile],
    config: &ScanConfig,
) -> (Vec<Vec<FileMetadata>>, Vec<GroupInfo>) {
    let ext_priorities: HashMap<String, usize> =
        config.extensions.iter().enumerate().map(|(i, e)| (e.to_lowercase(), i)).collect();

    // Build read-only lookup map
    let mut features_map = HashMap::new();
    for vf in valid_files {
        if let Some(feats) = &vf.pdq_features {
            features_map.insert(&vf.path, &**feats);
        }
    }

    // Process groups in parallel using Rayon
    let results: Vec<(Vec<FileMetadata>, GroupInfo)> = raw_groups
        .into_par_iter()
        .map(|group_indices| {
            let mut group_data: Vec<FileMetadata> = group_indices
                .iter()
                .map(|&idx| valid_files[idx as usize].to_file_metadata())
                .collect();

            let info = analyze_group_with_features(
                &mut group_data,
                &features_map,
                &config.group_by.to_lowercase(),
                &ext_priorities,
            );
            (group_data, info)
        })
        .collect();

    results.into_iter().unzip()
}

// Helper struct to force natural sort comparison
#[derive(PartialEq, Eq)]
struct NaturalSortKey(String);

impl PartialOrd for NaturalSortKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NaturalSortKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        natord::compare(&self.0, &other.0)
    }
}

pub fn sort_files(files: &mut [FileMetadata], sort_order: &str) {
    use rand::seq::SliceRandom;
    match sort_order {
        "name" => {
            // OPTIMIZATION: Parse path only once per file using cached key
            files.sort_by_cached_key(|f| {
                f.path.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default()
            });
        }
        "name-desc" => {
            files.sort_by_cached_key(|f| {
                f.path.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default()
            });
            files.reverse();
        }
        "name-natural" => {
            // Use wrapper struct to cache string AND use natural compare
            files.sort_by_cached_key(|f| {
                NaturalSortKey(
                    f.path.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default(),
                )
            });
        }
        "name-natural-desc" => {
            files.sort_by_cached_key(|f| {
                NaturalSortKey(
                    f.path.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default(),
                )
            });
            files.reverse();
        }
        "date" => files.sort_by(|a, b| a.modified.cmp(&b.modified)),
        "date-desc" => files.sort_by(|a, b| b.modified.cmp(&a.modified)),
        "size" => files.sort_by(|a, b| a.size.cmp(&b.size)),
        "size-desc" => files.sort_by(|a, b| b.size.cmp(&a.size)),
        "exif-date" => {
            // Sort by EXIF timestamp (oldest first).
            // Files with EXIF timestamps come first, then files without (sorted by mtime).
            files.sort_by(|a, b| match (a.exif_timestamp, b.exif_timestamp) {
                (Some(ta), Some(tb)) => ta.cmp(&tb),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a.modified.cmp(&b.modified),
            });
        }
        "exif-date-desc" => {
            // Sort by EXIF timestamp (newest first).
            // Files with EXIF timestamps come first, then files without (sorted by mtime desc).
            files.sort_by(|a, b| match (a.exif_timestamp, b.exif_timestamp) {
                (Some(ta), Some(tb)) => tb.cmp(&ta),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => b.modified.cmp(&a.modified),
            });
        }
        "random" => {
            let mut rng = rand::rng();
            files.shuffle(&mut rng);
        }
        "location" => return, // Sorting logic is performed in the GUI layer using GPS state
        _ => {
            // Default fallback (Name Natural)
            files.sort_by_cached_key(|f| {
                NaturalSortKey(
                    f.path.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default(),
                )
            });
        }
    }
}

/// Sort directories by the given sort order (same options as files)
pub fn sort_directories(dirs: &mut [std::path::PathBuf], sort_order: &str) {
    use rand::seq::SliceRandom;
    match sort_order {
        "name" => {
            dirs.sort_by_cached_key(|d| {
                d.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default()
            });
        }
        "name-desc" => {
            dirs.sort_by_cached_key(|d| {
                d.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default()
            });
            dirs.reverse();
        }
        "name-natural" | "" => {
            dirs.sort_by_cached_key(|d| {
                NaturalSortKey(
                    d.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default(),
                )
            });
        }
        "name-natural-desc" => {
            dirs.sort_by_cached_key(|d| {
                NaturalSortKey(
                    d.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default(),
                )
            });
            dirs.reverse();
        }
        "date" => {
            dirs.sort_by_cached_key(|d| {
                fs::metadata(d).ok().and_then(|m| m.modified().ok()).unwrap_or(UNIX_EPOCH)
            });
        }
        "date-desc" => {
            dirs.sort_by_cached_key(|d| {
                std::cmp::Reverse(
                    fs::metadata(d).ok().and_then(|m| m.modified().ok()).unwrap_or(UNIX_EPOCH),
                )
            });
        }
        "size" => {
            // For directories, size doesn't make much sense, so sort by name
            dirs.sort_by_cached_key(|d| {
                NaturalSortKey(
                    d.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default(),
                )
            });
        }
        "size-desc" => {
            dirs.sort_by_cached_key(|d| {
                NaturalSortKey(
                    d.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default(),
                )
            });
        }
        "random" => {
            let mut rng = rand::rng();
            dirs.shuffle(&mut rng);
        }
        _ => {
            // Default fallback (Name Natural)
            dirs.sort_by_cached_key(|d| {
                NaturalSortKey(
                    d.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default(),
                )
            });
        }
    }
}

fn analyze_group_with_features(
    files: &mut Vec<FileMetadata>,
    features_map: &HashMap<&std::path::PathBuf, &crate::pdqhash::PdqFeatures>,
    sort_order: &str,
    #[allow(unused)] ext_priorities: &HashMap<String, usize>,
) -> GroupInfo {
    if files.is_empty() {
        return GroupInfo { max_dist: 0, status: GroupStatus::None };
    }

    let mut counts = HashMap::new();
    for f in files.iter() {
        *counts.entry(f.content_hash).or_insert(0) += 1;
    }

    let (mut duplicates, mut unique): (Vec<FileMetadata>, Vec<FileMetadata>) =
        files.drain(..).partition(|f| *counts.get(&f.content_hash).unwrap_or(&0) > 1);

    duplicates.sort_by_cached_key(|f| {
        (
            f.pixel_hash,
            f.content_hash,
            f.path.file_name().unwrap_or_default().to_string_lossy().to_string(),
        )
    });

    sort_files(&mut unique, sort_order);
    files.append(&mut duplicates);
    files.append(&mut unique);

    sort_by_stem_then_ext(files);

    let pivot_features = files.first().and_then(|pivot| features_map.get(&pivot.path)).copied(); // Dereference &&PdqFeatures to &PdqFeatures

    let max_d = if let Some(pivot_feats) = pivot_features {
        let pivot_variants = pivot_feats.generate_dihedral_hashes();
        files
            .iter()
            .map(|f| {
                if let Some(h) = f.pdqhash {
                    pivot_variants.iter().map(|v| v.hamming_distance(&h)).min().unwrap_or(255)
                } else {
                    0
                }
            })
            .max()
            .unwrap_or(0)
    } else if let Some(pivot) = files.first().and_then(|f| f.pdqhash) {
        files
            .iter()
            .filter_map(|f| f.pdqhash)
            .map(|h| pivot.hamming_distance(&h))
            .max()
            .unwrap_or(0)
    } else {
        0
    };

    let has_duplicates = !counts.values().all(|&c| c == 1);
    let all_identical = counts.len() == 1;
    let status = if all_identical {
        GroupStatus::AllIdentical
    } else if has_duplicates {
        GroupStatus::SomeIdentical
    } else {
        GroupStatus::None
    };

    GroupInfo { max_dist: max_d, status }
}

fn sort_by_stem_then_ext(files: &mut [FileMetadata]) {
    files.sort_by_cached_key(|f| {
        let stem = f.path.file_stem().unwrap_or_default().to_os_string();
        let is_raw = is_raw_ext(&f.path);
        (stem, is_raw)
    });
}

pub fn is_raw_ext(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| RAW_EXTS.contains(&e.to_lowercase().as_str()))
        .unwrap_or(false)
}

pub fn is_image_ext(path: &Path) -> bool {
    path.extension()
        .and_then(|s| s.to_str())
        .map(|ext| {
            let e = ext.to_lowercase();
            matches!(
                e.as_str(),
                "dds"|"exr"|"ff"|"hdr"|"ico"|"pnm"|"qoi"|"gif"|"jpg"|"jpeg"|"png"|"webp"
            |"bmp"|"tiff"|"tif"|"avif"|"heic"|"heif"|"tga"
            |"jp2"|"j2k"|"jxl"
            // image-extras
            |"xbm"|"xpm"|"ora"|"otb"|"pcx"|"sgi"|"wbmp"
            ) || RAW_EXTS.contains(&e.as_str())
        })
        .unwrap_or(false)
}

pub fn scan_for_view(
    paths: &[String],
    sort_order: &str,
    progress_tx: Option<Sender<(usize, usize)>>,
    batch_tx: Option<Sender<Vec<FileMetadata>>>,
) -> (Vec<Vec<FileMetadata>>, Vec<GroupInfo>, Vec<std::path::PathBuf>) {
    let mut subdirs = Vec::new();
    let mut seen_paths = HashSet::new();
    let mut raw_paths = Vec::new();

    // 1. Fast Directory Walk (Collect paths only)
    for path_str in paths {
        let path = Path::new(path_str);
        if path.is_dir() {
            if let Ok(entries) = fs::read_dir(path) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let entry_path = entry.path();
                    if entry_path.is_dir() {
                        if let Ok(canonical) = entry_path.canonicalize() {
                            subdirs.push(canonical);
                        }
                    } else if entry_path.is_file()
                        && is_image_ext(&entry_path)
                        && let Ok(canonical) = entry_path.canonicalize()
                        && seen_paths.insert(canonical.clone())
                    {
                        raw_paths.push(canonical);
                    }
                }
            }
        } else if path.is_file()
            && is_image_ext(path)
            && let Ok(canonical) = path.canonicalize()
            && seen_paths.insert(canonical.clone())
        {
            raw_paths.push(canonical);
        }
    }
    // Apply same sort order to directories as files
    sort_directories(&mut subdirs, sort_order);

    let total_files = raw_paths.len();
    if let Some(tx) = &progress_tx {
        let _ = tx.send((0, total_files));
    }

    if raw_paths.is_empty() {
        return (Vec::new(), Vec::new(), subdirs);
    }

    // 2. Parallel Processing with Streaming
    let chunk_size = 100;
    let processed_count = AtomicUsize::new(0);

    // Split into chunks to stream results to UI
    let chunks: Vec<Vec<std::path::PathBuf>> =
        raw_paths.chunks(chunk_size).map(|c| c.to_vec()).collect();
    let mut all_files = Vec::new();

    for chunk in chunks {
        let batch_results: Vec<FileMetadata> = chunk
            .par_iter()
            .filter_map(|path| {
                if let Some(prog_tx) = &progress_tx {
                    let current = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
                    if current.is_multiple_of(50) || current == total_files {
                        let _ = prog_tx.send((current, total_files));
                    }
                }

                let metadata = fs::metadata(path).ok()?;
                let size = metadata.len();
                let modified = DateTime::from(metadata.modified().ok().unwrap_or(UNIX_EPOCH));

                let mut gps_pos = None;
                let mut exif_timestamp = None;
                if let Some(exif) = read_exif_data(path, None) {
                    if let Some((lat, lon)) = extract_gps_lat_lon(&exif) {
                        gps_pos = Some(Point::new(lon, lat));
                    }
                    exif_timestamp = get_exif_timestamp(&exif);
                }
                // Required for RAWs to look correct immediately.
                // Streaming (batch_tx) ensures the UI is still responsive.
                // Note: For RAW files, the actual orientation used depends on whether thumbnails
                // or full decode is used. The image loader will return the correct value.
                let orientation = get_orientation(path, None);
                eprintln!(
                    "[DEBUG-SCAN] scan_for_view get_orientation={} for {:?}",
                    orientation,
                    path.file_name().unwrap_or_default()
                );

                let unique_file_id = get_file_key(path)?;

                Some(FileMetadata {
                    path: path.clone(),
                    size,
                    modified,
                    pdqhash: None,
                    resolution: None,
                    content_hash: [0u8; 32],
                    pixel_hash: None,
                    orientation,
                    gps_pos,
                    unique_file_id,
                    exif_timestamp,
                })
            })
            .collect();

        // Stream this batch to the GUI immediately
        if !batch_results.is_empty() {
            if let Some(tx) = &batch_tx {
                let _ = tx.send(batch_results.clone());
            }
            all_files.extend(batch_results);
        }
    }

    // 3. Final Sort
    sort_files(&mut all_files, sort_order);

    let info = GroupInfo { max_dist: 0, status: GroupStatus::None };
    (vec![all_files], vec![info], subdirs)
}

/// Scan for view mode with recursive directory traversal (flatten mode).
/// Unlike scan_for_view, this recursively walks all subdirectories.
/// Uses database cache for metadata like spawn_background_dir_scan.
/// Returns (file_count) synchronously for immediate UI setup.
pub fn spawn_background_flatten_scan(
    paths: &[String],
    sort_order: String,
    ctx: &crate::db::AppContext,
    batch_tx: Sender<Vec<FileMetadata>>,
    progress_tx: Option<Sender<(usize, usize)>>,
) -> usize {
    let mut seen_paths = HashSet::new();
    let mut entries: Vec<DirEntry> = Vec::new();

    // Phase 1: Fast recursive directory enumeration using WalkDir
    for path_str in paths {
        let path = Path::new(path_str);
        if path.is_dir() {
            for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
                let entry_path = entry.path();
                if entry_path.is_file()
                    && is_image_ext(entry_path)
                    && let Ok(canonical) = entry_path.canonicalize()
                    && seen_paths.insert(canonical.clone())
                    && let Ok(meta) = fs::metadata(&canonical)
                    && let Some(unique_file_id) = get_file_key(&canonical)
                {
                    entries.push(DirEntry {
                        path: canonical,
                        size: meta.len(),
                        modified: meta.modified().unwrap_or(UNIX_EPOCH).into(),
                        unique_file_id,
                    });
                }
            }
        } else if path.is_file()
            && is_image_ext(path)
            && let Ok(canonical) = path.canonicalize()
            && seen_paths.insert(canonical.clone())
            && let Ok(meta) = fs::metadata(&canonical)
            && let Some(unique_file_id) = get_file_key(&canonical)
        {
            entries.push(DirEntry {
                path: canonical,
                size: meta.len(),
                modified: meta.modified().unwrap_or(UNIX_EPOCH).into(),
                unique_file_id,
            });
        }
    }

    let file_count = entries.len();

    if let Some(ref tx) = progress_tx {
        let _ = tx.send((0, file_count));
    }

    if entries.is_empty() {
        return 0;
    }

    // Prepare batch lookup data
    let lookup_data: Vec<(u128, u64, u64)> = entries
        .iter()
        .map(|e| {
            let mtime_ns = e.modified.timestamp_nanos_opt().unwrap_or(0) as u64;
            (e.unique_file_id, e.size, mtime_ns)
        })
        .collect();

    // Batch database lookup (single transaction)
    let cached: std::collections::HashMap<u128, ImageFeatures> =
        ctx.lookup_cached_features_batch(&lookup_data);

    // Phase 2: Background processing with batch results
    let sort_order_clone = sort_order.clone();
    std::thread::spawn(move || {
        const BATCH_SIZE: usize = 500;

        // Convert entries to FileMetadata using cached data
        let mut files: Vec<FileMetadata> = entries
            .into_iter()
            .map(|e| {
                // Extract fields from ImageFeatures if cached
                let (resolution, orientation, gps_pos, exif_timestamp) =
                    if let Some(feats) = cached.get(&e.unique_file_id) {
                        (
                            feats.resolution(),
                            feats.orientation(),
                            feats.gps_pos(),
                            feats.exif_timestamp(),
                        )
                    } else {
                        (None, 1, None, None)
                    };

                FileMetadata {
                    path: e.path,
                    size: e.size,
                    modified: e.modified,
                    pdqhash: None,
                    resolution,
                    content_hash: [0u8; 32],
                    pixel_hash: None,
                    orientation,
                    gps_pos,
                    unique_file_id: e.unique_file_id,
                    exif_timestamp,
                }
            })
            .collect();

        // Sort all files
        sort_files(&mut files, &sort_order_clone);

        // Stream in batches
        for chunk in files.chunks(BATCH_SIZE) {
            let _ = batch_tx.send(chunk.to_vec());
        }
    });

    file_count
}

/// Spawn a background thread to enrich files with content_hash and GPS data.
/// This function handles:
/// - Computing blake3 content_hash (parallel with rayon)
/// - Reading GPS coordinates from EXIF
/// - Writing results to database via db_tx channel
/// - Sending EnrichmentResult back to GUI via result_tx channel
///
/// The GUI can then use unique_file_id for O(1) lookup to update FileMetadata.
pub fn spawn_background_enrichment(
    files_to_enrich: Vec<(std::path::PathBuf, u128, Option<(u32, u32)>, u8)>, // (path, unique_file_id, resolution, orientation)
    content_key: [u8; 32],
    meta_key_secret: [u8; 32],
    db_tx: Option<Sender<DbUpdate>>,
    result_tx: Sender<EnrichmentResult>,
) {
    if files_to_enrich.is_empty() {
        return;
    }

    std::thread::spawn(move || {
        // Process files in parallel using rayon
        // Thread Safety: Each file is processed independently, no shared mutable state
        // between iterations. The db_tx and result_tx channels are thread-safe.
        files_to_enrich.par_iter().for_each(|(path, unique_file_id, resolution, _orientation)| {
            if let Ok(data) = std::fs::read(path) {
                // Compute content_hash
                let mut hasher = blake3::Hasher::new_keyed(&content_key);
                hasher.update(&data);
                let content_hash = *hasher.finalize().as_bytes();

                // Read EXIF data once for GPS, orientation, and timestamp
                let exif_data = read_exif_data(path, Some(&data));

                // Determine if this is a RAW file for potential rsraw fallback
                let is_raw = is_raw_ext(path);

                // Try to get rsraw data for RAW files (we may need it as fallback)
                // Only open rsraw if kamadak-exif failed - avoids double parsing
                let raw_image = if is_raw && exif_data.is_none() {
                    rsraw::RawImage::open(&data).ok()
                } else {
                    None
                };

                // Read GPS from EXIF, with rsraw fallback for RAW files
                let gps_pos = exif_data
                    .as_ref()
                    .and_then(extract_gps_lat_lon)
                    .map(|(lat, lon)| Point::new(lon, lat))
                    .or_else(|| raw_image.as_ref().and_then(raw_exif::get_gps_point_from_raw));

                // Read orientation from EXIF (fresh, not from stale passed-in value)
                // TODO(rsraw-orientation): rsraw does NOT currently provide orientation.
                // When rsraw exposes it, add fallback like:
                //   let orientation = get_orientation(path, Some(&data))
                //       .or_else(|| raw_image.as_ref().and_then(raw_exif::get_orientation_from_raw))
                //       .unwrap_or(1);
                let orientation = get_orientation(path, Some(&data));

                // Read EXIF timestamp, with rsraw fallback
                let exif_timestamp = exif_data
                    .as_ref()
                    .and_then(get_exif_timestamp)
                    .or_else(|| raw_image.as_ref().and_then(raw_exif::get_timestamp_from_raw));

                // --- 1. BUILD FEATURES (Unified Path) ---
                // We build the features object now so it can be used for BOTH the database
                // and the immediate GUI update (fixing the race condition).
                let (w, h) = resolution.unwrap_or((0, 0));

                // Initialize: Use rich EXIF data if available, or blank slate
                // For RAW files where kamadak-exif failed, use rsraw data
                let mut features = if let Some(exif) = exif_data.as_ref() {
                    // 'true' here enables enrichment (Country, Sun Position, etc.)
                    crate::exif_extract::build_image_features(w, h, exif, true, false)
                } else if let Some(ref raw) = raw_image {
                    // kamadak-exif failed, but we have rsraw data
                    raw_exif::build_features_from_raw_image(raw)
                } else {
                    crate::image_features::ImageFeatures::new(w, h)
                };

                // If we have kamadak-exif data and this is a RAW file, also merge rsraw data
                // (rsraw might have data that kamadak-exif missed, like lens info)
                if exif_data.is_some() && is_raw {
                    if let Ok(raw) = rsraw::RawImage::open(&data) {
                        raw_exif::merge_raw_info_into_features(&mut features, &raw);
                    }
                }
                
                if false {
                    eprintln!("[DEBUG-ENRICH]   Final features for '{}': tag_count={}, has_make={}, has_model={}, has_iso={}",
                        path.file_name().unwrap_or_default().to_string_lossy(),
                        features.tag_count(),
                        features.has_tag(crate::exif_types::TAG_MAKE),
                        features.has_tag(crate::exif_types::TAG_MODEL),
                        features.has_tag(crate::exif_types::TAG_ISO));
                }

                // Enforce critical tags (Orientation, GPS) to ensure consistency
                // (These might be redundant if build_image_features ran, but safe to ensure)
                if orientation != 1 {
                    features.insert_tag(
                        crate::exif_types::TAG_ORIENTATION,
                        crate::exif_types::ExifValue::Short(orientation as u16),
                    );
                }

                if let Some(pos) = gps_pos {
                    features.insert_tag(
                        crate::exif_types::TAG_GPS_LATITUDE,
                        crate::exif_types::ExifValue::Float(pos.y()),
                    );
                    features.insert_tag(
                        crate::exif_types::TAG_GPS_LONGITUDE,
                        crate::exif_types::ExifValue::Float(pos.x()),
                    );

                    // Fallback: If build_image_features didn't derive country (or if we had no EXIF object), try manually
                    if !features.has_tag(crate::exif_types::TAG_DERIVED_COUNTRY) {
                        if let Some(country) = crate::exif_extract::derive_country(pos.y(), pos.x())
                        {
                            features.insert_tag(
                                crate::exif_types::TAG_DERIVED_COUNTRY,
                                crate::exif_types::ExifValue::String(country),
                            );
                        }
                    }
                }

                // Ensure derived timestamp is indexed (critical for range search)
                if let Some(ts) = exif_timestamp {
                    features.insert_tag(
                        crate::exif_types::TAG_DERIVED_TIMESTAMP,
                        crate::exif_types::ExifValue::Long64(ts),
                    );
                }

                // --- 2. SEND TO DATABASE ---
                // We clone 'features' because the DB update takes ownership
                if let Some(ref tx) = db_tx {
                    if let Some(update) = create_feature_update(
                        &meta_key_secret,
                        path,
                        *unique_file_id,
                        content_hash,
                        features.clone(),
                    ) {
                        let _ = tx.send(update);
                    }
                }

                // --- 3. SEND TO GUI ---
                // We pass 'features' directly. The GUI uses this to update the search index immediately.
                let _ = result_tx.send(EnrichmentResult {
                    unique_file_id: *unique_file_id,
                    content_hash,
                    gps_pos,
                    exif_timestamp,
                    features: Some(features),
                });
            }
        });
    });
}

/// Lightweight file entry for initial fast directory scan
#[derive(Clone)]
pub struct DirEntry {
    pub path: std::path::PathBuf,
    pub size: u64,
    pub modified: DateTime<Utc>,
    pub unique_file_id: u128,
}

/// Spawn background directory scanning with batch database lookups.
/// This function:
/// 1. Quickly collects directory entries (paths only) - returns immediately via count_tx
/// 2. Performs batch database lookup for cached features
/// 3. Streams FileMetadata results in batches to the GUI
///
/// Returns (subdirs, file_count) synchronously for immediate UI setup.
pub fn spawn_background_dir_scan(
    dir: std::path::PathBuf,
    sort_order: String,
    ctx: &crate::db::AppContext,
    batch_tx: Sender<Vec<FileMetadata>>,
) -> (Vec<std::path::PathBuf>, usize) {
    let mut subdirs = Vec::new();
    let mut entries: Vec<DirEntry> = Vec::new();

    // Phase 1: Fast directory enumeration (synchronous, no I/O beyond readdir)
    if let Ok(dir_entries) = fs::read_dir(&dir) {
        for entry in dir_entries.flatten() {
            let entry_path = entry.path();
            if let Ok(canonical) = entry_path.canonicalize() {
                if canonical.is_dir() {
                    subdirs.push(canonical);
                } else if is_image_ext(&canonical)
                    && let Ok(meta) = entry.metadata()
                    && let Some(unique_file_id) = get_file_key(&canonical)
                {
                    entries.push(DirEntry {
                        path: canonical,
                        size: meta.len(),
                        modified: meta.modified().unwrap_or(UNIX_EPOCH).into(),
                        unique_file_id,
                    });
                }
            }
        }
    }

    sort_directories(&mut subdirs, &sort_order);
    let file_count = entries.len();

    if entries.is_empty() {
        return (subdirs, 0);
    }

    // Prepare batch lookup data
    let lookup_data: Vec<(u128, u64, u64)> = entries
        .iter()
        .map(|e| {
            let mtime_ns = e.modified.timestamp_nanos_opt().unwrap_or(0) as u64;
            (e.unique_file_id, e.size, mtime_ns)
        })
        .collect();

    // Batch database lookup (single transaction)
    let cached: std::collections::HashMap<u128, ImageFeatures> =
        ctx.lookup_cached_features_batch(&lookup_data);

    // Phase 2: Background processing with batch results
    let sort_order_clone = sort_order.clone();
    std::thread::spawn(move || {
        const BATCH_SIZE: usize = 500;

        // Convert entries to FileMetadata using cached data
        let mut files: Vec<FileMetadata> = entries
            .into_iter()
            .map(|e| {
                // Extract fields from ImageFeatures if cached
                let (resolution, orientation, gps_pos, exif_timestamp) =
                    if let Some(feats) = cached.get(&e.unique_file_id) {
                        (
                            feats.resolution(),
                            feats.orientation(),
                            feats.gps_pos(),
                            feats.exif_timestamp(),
                        )
                    } else {
                        (None, 1, None, None)
                    };

                FileMetadata {
                    path: e.path,
                    size: e.size,
                    modified: e.modified,
                    pdqhash: None,
                    resolution,
                    content_hash: [0u8; 32],
                    pixel_hash: None,
                    orientation,
                    gps_pos,
                    unique_file_id: e.unique_file_id,
                    exif_timestamp,
                }
            })
            .collect();

        // Sort all files
        sort_files(&mut files, &sort_order_clone);

        // Stream in batches
        for chunk in files.chunks(BATCH_SIZE) {
            let _ = batch_tx.send(chunk.to_vec());
        }
    });

    (subdirs, file_count)
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
        assert_eq!(result, Some("Florida, United States of America (the)".to_string()));
    }
}
