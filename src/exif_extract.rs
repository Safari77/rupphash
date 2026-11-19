// EXIF extraction functions that convert exif crate data to our storage format.
// Integrates functionality from helper_exif.rs.
// Filters out thumbnails and large binary blobs.
use std::{collections::BTreeMap, path::Path, str::FromStr};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use exif::{In, Tag, Value};
use num_integer::gcd;

use crate::{
    exif_types::{
        ExifValue, MAX_TAG_SIZE, TAG_DERIVED_COUNTRY, TAG_DERIVED_SUBDIVISION,
        TAG_DERIVED_SUN_ALTITUDE, TAG_DERIVED_SUN_AZIMUTH, TAG_DERIVED_TIMESTAMP,
        TAG_DERIVED_TIMEZONE, TAG_GPS_ALTITUDE, TAG_GPS_LATITUDE, TAG_GPS_LONGITUDE,
        TAG_ORIENTATION, is_excluded_tag,
    },
    image_features::ImageFeatures,
};

// =============================================================================
// Core EXIF Reading (from helper_exif.rs)
// =============================================================================

/// Read EXIF data from a file path or preloaded bytes
pub fn read_exif_data(path: &Path, preloaded_bytes: Option<&[u8]>) -> Option<exif::Exif> {
    trait BufReadSeek: std::io::BufRead + std::io::Seek {}
    impl<T: std::io::BufRead + std::io::Seek> BufReadSeek for T {}

    let mut reader: Box<dyn BufReadSeek> = match preloaded_bytes {
        Some(bytes) => Box::new(std::io::Cursor::new(bytes)),
        None => {
            let file = std::fs::File::open(path).ok()?;
            Box::new(std::io::BufReader::new(file))
        },
    };

    exif::Reader::new().read_from_container(&mut reader).ok()
}

/// Get orientation from EXIF (returns 1 if not found or invalid)
pub fn get_orientation(path: &Path, preloaded_bytes: Option<&[u8]>) -> u8 {
    if let Some(exif_data) = read_exif_data(path, preloaded_bytes)
        && let Some(field) = exif_data.get_field(Tag::Orientation, In::PRIMARY)
        && let Some(v @ 1..=8) = field.value.get_uint(0)
    {
        return v as u8;
    }
    1
}

/// Extract GPS coordinates from EXIF data as (latitude, longitude)
pub fn extract_gps_lat_lon(exif_data: &exif::Exif) -> Option<(f64, f64)> {
    let lat_field = exif_data.get_field(Tag::GPSLatitude, In::PRIMARY)?;
    let lon_field = exif_data.get_field(Tag::GPSLongitude, In::PRIMARY)?;
    let lat_ref = exif_data.get_field(Tag::GPSLatitudeRef, In::PRIMARY);
    let lon_ref = exif_data.get_field(Tag::GPSLongitudeRef, In::PRIMARY);

    let lat = parse_gps_coordinate(&lat_field.value)?;
    let lon = parse_gps_coordinate(&lon_field.value)?;

    // Apply reference (N/S for latitude, E/W for longitude)
    let lat = if let Some(ref_field) = lat_ref {
        let ref_str = ref_field.value.display_as(Tag::GPSLatitudeRef).to_string();
        if ref_str.trim().eq_ignore_ascii_case("S") { -lat } else { lat }
    } else {
        lat
    };

    let lon = if let Some(ref_field) = lon_ref {
        let ref_str = ref_field.value.display_as(Tag::GPSLongitudeRef).to_string();
        if ref_str.trim().eq_ignore_ascii_case("W") { -lon } else { lon }
    } else {
        lon
    };

    Some((lat, lon))
}

/// Parse GPS coordinate magnitude from EXIF rational values (DMS -> Decimal).
/// Returns positive value; caller must apply sign from ref tag.
pub fn parse_gps_coordinate(value: &Value) -> Option<f64> {
    if let Value::Rational(rats) = value
        && let [deg, min, sec, ..] = rats.as_slice()
    {
        if deg.denom == 0 || min.denom == 0 || sec.denom == 0 {
            return None;
        }
        return Some(deg.to_f64() + min.to_f64() / 60.0 + sec.to_f64() / 3600.0);
    }
    None
}

/// Parses date string. If `use_gps` is true, attempts GPS time (UTC).
pub fn get_date_str(exif: &exif::Exif, use_gps: bool) -> Option<String> {
    if use_gps {
        let d_field = exif.get_field(Tag::GPSDateStamp, In::PRIMARY)?;
        let t_field = exif.get_field(Tag::GPSTimeStamp, In::PRIMARY)?;

        // Parse Date: "YYYY:MM:DD"
        let Value::Ascii(vec) = &d_field.value else {
            return None;
        };
        let date_part = std::str::from_utf8(vec.first()?).ok()?.trim().replace(':', "-");

        // Parse Time: 3 Rationals [Hr, Min, Sec]
        let Value::Rational(rats) = &t_field.value else {
            return None;
        };
        let [hr, min, sec, ..] = rats.as_slice() else {
            return None;
        };
        if hr.denom == 0 || min.denom == 0 || sec.denom == 0 {
            return None;
        }
        let h = hr.num as f64 / hr.denom as f64;
        let m = min.num as f64 / min.denom as f64;
        let s = sec.num as f64 / sec.denom as f64;
        let time_part = format!("{:02}:{:02}:{:06.3}", h as u32, m as u32, s);

        return Some(format!("{} {}", date_part, time_part));
    }

    // Default: Use DateTimeOriginal
    let field = exif.get_field(Tag::DateTimeOriginal, In::PRIMARY)?;
    if let Value::Ascii(vec) = &field.value
        && let Some(first) = vec.first()
        && let Ok(dt) = exif::DateTime::from_ascii(first)
    {
        return Some(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second
        ));
    }
    None
}

/// Extract EXIF timestamp as i64 (seconds since Unix epoch).
pub fn get_exif_timestamp(exif: &exif::Exif) -> Option<i64> {
    if let Some(ts) = parse_exif_datetime_tag(exif, Tag::DateTimeOriginal) {
        return Some(ts);
    }
    parse_exif_datetime_tag(exif, Tag::DateTimeDigitized)
}

/// Parse an EXIF DateTime tag to Unix timestamp.
fn parse_exif_datetime_tag(exif: &exif::Exif, tag: Tag) -> Option<i64> {
    let field = exif.get_field(tag, In::PRIMARY)?;
    if let Value::Ascii(vec) = &field.value
        && let Some(first) = vec.first()
        && let Ok(dt) = exif::DateTime::from_ascii(first)
    {
        let date = NaiveDate::from_ymd_opt(dt.year as i32, dt.month as u32, dt.day as u32)?;
        let time = NaiveTime::from_hms_opt(dt.hour as u32, dt.minute as u32, dt.second as u32)?;
        let naive_dt = NaiveDateTime::new(date, time);
        return Some(naive_dt.and_utc().timestamp());
    }
    None
}

/// Gets altitude from EXIF tags
pub fn get_altitude(exif: &exif::Exif) -> Option<f64> {
    let val_field = exif.get_field(Tag::GPSAltitude, In::PRIMARY)?;
    let ref_field = exif.get_field(Tag::GPSAltitudeRef, In::PRIMARY);

    if let Value::Rational(rats) = &val_field.value
        && let Some(first) = rats.first()
    {
        if first.denom == 0 {
            return None;
        }

        let mut alt = first.num as f64 / first.denom as f64;

        if let Some(rf) = ref_field
            && let Value::Byte(bytes) = &rf.value
            && bytes.first() == Some(&1)
        {
            alt = -alt;
        }
        return Some(alt);
    }
    None
}

/// Check if file has GPS time data
pub fn has_gps_time(path: &Path) -> bool {
    if let Some(exif) = read_exif_data(path, None) {
        return get_date_str(&exif, true).is_some();
    }
    false
}

// =============================================================================
// Full EXIF Extraction (BTreeMap format)
// =============================================================================

/// Extract all EXIF tags from exif data into a BTreeMap.
/// Filters out thumbnails and large binary data (>1024 bytes).
pub fn extract_all_exif(exif_data: &exif::Exif) -> BTreeMap<u16, ExifValue> {
    let mut map = BTreeMap::new();

    for field in exif_data.fields() {
        let tag_id = field.tag.number();

        // Skip thumbnail and large binary tags
        if is_excluded_tag(tag_id) {
            continue;
        }

        // Skip thumbnail IFD entirely
        if field.ifd_num == In::THUMBNAIL {
            continue;
        }

        // Convert value to our format
        let value = match &field.value {
            Value::Byte(v) => match v.as_slice() {
                [b] => ExifValue::Byte(*b),
                slice if slice.len() <= MAX_TAG_SIZE => ExifValue::Bytes(v.clone()),
                _ => continue,
            },

            Value::Short(v) => match v.as_slice() {
                [s] => ExifValue::Short(*s),
                slice if slice.len() <= MAX_TAG_SIZE / 2 => ExifValue::Shorts(v.clone()),
                _ => continue,
            },

            Value::Long(v) => match v.as_slice() {
                [l] => ExifValue::Long(*l),
                _ => continue,
            },

            Value::SLong(v) => match v.as_slice() {
                [l] => ExifValue::Signed(*l),
                _ => continue,
            },

            Value::Rational(v) => match v.as_slice() {
                [] => continue,
                [r] => {
                    if r.denom != 0 {
                        ExifValue::Float(r.num as f64 / r.denom as f64)
                    } else {
                        continue;
                    }
                },
                _ => {
                    // Multiple rationals (e.g., GPS coordinates)
                    let floats: Vec<f64> = v
                        .iter()
                        .filter(|r| r.denom != 0)
                        .map(|r| r.num as f64 / r.denom as f64)
                        .collect();
                    if floats.is_empty() {
                        continue;
                    }
                    ExifValue::Floats(floats)
                },
            },

            Value::SRational(v) => match v.as_slice() {
                [] => continue,
                [r] => {
                    if r.denom != 0 {
                        ExifValue::Float(r.num as f64 / r.denom as f64)
                    } else {
                        continue;
                    }
                },
                _ => {
                    let floats: Vec<f64> = v
                        .iter()
                        .filter(|r| r.denom != 0)
                        .map(|r| r.num as f64 / r.denom as f64)
                        .collect();
                    if floats.is_empty() {
                        continue;
                    }
                    ExifValue::Floats(floats)
                },
            },

            Value::Ascii(v) => {
                // Concatenate all ASCII strings, clean null bytes
                let s: String = v
                    .iter()
                    .filter_map(|bytes| std::str::from_utf8(bytes).ok())
                    .collect::<Vec<_>>()
                    .join("")
                    .trim()
                    .replace('\0', "");

                if s.is_empty() || s.len() > MAX_TAG_SIZE {
                    continue;
                }
                ExifValue::String(s)
            },

            Value::Undefined(bytes, _) => {
                // Skip large undefined blobs (MakerNotes, etc.)
                if bytes.len() > MAX_TAG_SIZE {
                    continue;
                }
                // Try to interpret as string first
                if let Ok(s) = std::str::from_utf8(bytes) {
                    let clean = s.trim().replace('\0', "");
                    if !clean.is_empty() {
                        ExifValue::String(clean)
                    } else {
                        continue;
                    }
                } else if bytes.len() <= 64 {
                    // Store small binary data
                    ExifValue::Bytes(bytes.clone())
                } else {
                    continue;
                }
            },

            _ => continue,
        };

        map.insert(tag_id, value);
    }

    map
}

// =============================================================================
// Complete Feature Building
// =============================================================================

/// Build complete ImageFeatures from EXIF data with derived values.
/// This is the main entry point for creating features from a newly scanned image.
pub fn build_image_features(
    width: u32,
    height: u32,
    exif_data: &exif::Exif,
    compute_derived: bool,
    use_gps_utc: bool,
) -> ImageFeatures {
    let mut features = ImageFeatures::new(width, height);

    // Extract all standard EXIF tags
    features.tags = extract_all_exif(exif_data);

    // Store GPS as decimal degrees for easier querying
    if let Some((lat, lon)) = extract_gps_lat_lon(exif_data) {
        features.insert_tag(TAG_GPS_LATITUDE, ExifValue::Float(lat));
        features.insert_tag(TAG_GPS_LONGITUDE, ExifValue::Float(lon));

        // Compute derived values if requested
        if compute_derived {
            add_derived_values(&mut features, lat, lon, exif_data, use_gps_utc);
        }
    }

    // Store altitude separately for easier access
    if let Some(alt) = get_altitude(exif_data) {
        features.insert_tag(TAG_GPS_ALTITUDE, ExifValue::Float(alt));
    }

    // Store EXIF timestamp as derived value
    if let Some(ts) = get_exif_timestamp(exif_data) {
        features.insert_tag(TAG_DERIVED_TIMESTAMP, ExifValue::Long64(ts));
    }

    // Ensure orientation is stored
    if !features.has_tag(TAG_ORIENTATION)
        && let Some(field) = exif_data.get_field(Tag::Orientation, In::PRIMARY)
        && let Some(v @ 1..=8) = field.value.get_uint(0)
    {
        features.insert_tag(TAG_ORIENTATION, ExifValue::Short(v as u16));
    }

    features
}

/// Add derived values (country, sun position, timezone) to features
fn add_derived_values(
    features: &mut ImageFeatures,
    lat: f64,
    lon: f64,
    exif_data: &exif::Exif,
    use_gps_utc: bool,
) {
    // Derive country from coordinates
    if let Some(country) = derive_country(lat, lon) {
        features.insert_tag(TAG_DERIVED_COUNTRY, ExifValue::String(country));
    }

    // Derive subdivision/state
    if let Some(subdivision) = derive_subdivision(lat, lon) {
        features.insert_tag(TAG_DERIVED_SUBDIVISION, ExifValue::String(subdivision));
    }

    // Derive sun position if we have timestamp
    if let Some((azimuth, altitude, timezone)) =
        derive_sun_position(lat, lon, exif_data, use_gps_utc)
    {
        // eprintln!("exif_extract sun az={} alt={} tz={}", azimuth, altitude, timezone);
        features.insert_tag(TAG_DERIVED_SUN_AZIMUTH, ExifValue::Float(azimuth));
        features.insert_tag(TAG_DERIVED_SUN_ALTITUDE, ExifValue::Float(altitude));
        features.insert_tag(TAG_DERIVED_TIMEZONE, ExifValue::String(timezone));
    }
}

static BOUNDARIES: std::sync::OnceLock<Option<country_boundaries::CountryBoundaries>> =
    std::sync::OnceLock::new();

fn get_boundaries() -> Option<&'static country_boundaries::CountryBoundaries> {
    use country_boundaries::{BOUNDARIES_ODBL_360X180, CountryBoundaries};

    BOUNDARIES.get_or_init(|| CountryBoundaries::from_reader(BOUNDARIES_ODBL_360X180).ok()).as_ref()
}

/// Derive country name from GPS coordinates
pub fn derive_country(lat: f64, lon: f64) -> Option<String> {
    use codes_iso_3166::part_1::CountryCode;
    use country_boundaries::LatLon;

    let boundaries = get_boundaries()?;
    let ids = boundaries.ids(LatLon::new(lat, lon).ok()?);

    ids.iter()
        .find(|id| id.len() == 2)
        .and_then(|id| CountryCode::from_str(id).ok().map(|c| c.short_name().to_string()))
}

/// Derive subdivision/state from GPS coordinates
pub fn derive_subdivision(lat: f64, lon: f64) -> Option<String> {
    use codes_iso_3166::part_2::SubdivisionCode;
    use country_boundaries::LatLon;

    let boundaries = get_boundaries()?;
    let ids = boundaries.ids(LatLon::new(lat, lon).ok()?);

    for id in ids {
        if id.contains('-') {
            // The crate expects underscores (US_FL) not hyphens (US-FL)
            let formatted = id.replace('-', "_");
            if let Ok(subdiv) = SubdivisionCode::from_str(&formatted) {
                return Some(subdiv.name().to_string());
            }
        }
    }
    None
}

/// Derive formatted location string (`"Subdivision, Country"` or `"Country"`) from GPS coordinates.
pub fn derive_location(lat: f64, lon: f64) -> Option<String> {
    use country_boundaries::LatLon;

    let boundaries = get_boundaries()?;
    let ids = boundaries.ids(LatLon::new(lat, lon).ok()?);
    if ids.is_empty() {
        return None;
    }

    let country_name = derive_country(lat, lon);
    let subdivision_name = derive_subdivision(lat, lon);

    match (country_name, subdivision_name) {
        (Some(country), Some(subdivision)) => Some(format!("{subdivision}, {country}")),
        (Some(country), None) => Some(country),
        (None, Some(subdivision)) => Some(subdivision),
        (None, None) => {
            let country_id = ids.iter().find(|id| id.len() == 2).map(AsRef::<str>::as_ref);
            let subdivision_id = ids.iter().find(|id| id.contains('-')).map(AsRef::<str>::as_ref);
            country_id.or(subdivision_id).map(|s| s.to_string())
        },
    }
}

/// Derive sun position from GPS coordinates and EXIF timestamp
fn derive_sun_position(
    lat: f64,
    lon: f64,
    exif_data: &exif::Exif,
    use_gps_utc: bool,
) -> Option<(f64, f64, String)> {
    // Try GPS time first if requested, then fall back to local time
    let (date_str, is_utc) = if use_gps_utc {
        if let Some(d) = get_date_str(exif_data, true) {
            (d, true)
        } else {
            let d = get_date_str(exif_data, false)?;
            (d, false)
        }
    } else {
        (get_date_str(exif_data, false)?, false)
    };

    // Get altitude for more accurate sun position
    let altitude = get_altitude(exif_data).unwrap_or(0.0);

    // Calculate sun position using crate::position
    if let Ok((sun_alt, sun_az, tz)) =
        crate::position::sun_alt_and_azimuth(&date_str, lat, lon, Some(altitude), is_utc)
    {
        Some((sun_az, sun_alt, tz))
    } else {
        None
    }
}

// =============================================================================
// Display Formatting
// =============================================================================

/// Format decimal degrees as DMS string
#[allow(dead_code)]
fn format_dms(decimal_deg: f64) -> String {
    let abs_deg = decimal_deg.abs();
    let d = abs_deg.floor() as i32;
    let m_float = (abs_deg - d as f64) * 60.0;
    let m = m_float.floor() as i32;
    let s = (m_float - m as f64) * 60.0;

    let sign = if decimal_deg < 0.0 { "-" } else { "" };
    format!("{}{}° {}' {:.1}\"", sign, d, m, s)
}

/// Format decimal degrees as DMS string with hemisphere and carry-over handling.
pub fn format_dms_from_decimal(decimal_deg: f64, is_latitude: bool) -> String {
    if !decimal_deg.is_finite() {
        return String::new();
    }

    let abs_deg = decimal_deg.abs();
    let mut d = abs_deg.floor() as i64;
    let m_float = (abs_deg - d as f64) * 60.0;
    let mut m = m_float.floor() as i64;
    let s = (m_float - m as f64) * 60.0;

    let mut s_rounded = (s * 10.0).round() / 10.0;
    if s_rounded >= 60.0 {
        s_rounded = 0.0;
        m = m.saturating_add(1);
        if m >= 60 {
            m = 0;
            d = d.saturating_add(1);
        }
    }

    let hemi = if is_latitude {
        if decimal_deg >= 0.0 { "N" } else { "S" }
    } else if decimal_deg >= 0.0 {
        "E"
    } else {
        "W"
    };

    format!("{d}° {m}' {s_rounded:.1}\" {hemi}")
}

/// Format seconds as human-readable time with up to three-decimal accuracy,
/// trimming trailing zeros: 1.22 -> "1.22s", 1.20 -> "1.2s", 2.00 -> "2s",
/// 0.023 -> "0.023s"; sub-millisecond times switch to ms: 0.0001 -> "0.1ms".
pub fn format_seconds(secs: f64) -> String {
    if !secs.is_finite() || secs < 0.0 {
        return "0s".to_string();
    }
    if secs < 0.001 {
        let s = format!("{:.3}", secs * 1000.0);
        let s = s.trim_end_matches('0').trim_end_matches('.');
        format!("{s}ms")
    } else {
        let s = format!("{:.3}", secs);
        let s = s.trim_end_matches('0').trim_end_matches('.');
        format!("{s}s")
    }
}

/// Format an EXIF value for display.
pub fn format_exif_value(value: &exif::Value, tag: exif::Tag, decimal_coords: bool) -> String {
    match tag {
        exif::Tag::GPSLatitude | exif::Tag::GPSLongitude => {
            if decimal_coords {
                // Parse rational D/M/S to decimal degrees
                if let Some(val) = parse_gps_coordinate(value) {
                    return format!("{val:.6}");
                }
            }
            // Fallback to default Minutes/Seconds display
            clean_exif_string(&value.display_as(tag).to_string())
        },
        exif::Tag::ExposureTime => {
            // ExposureTime is stored as Rational; check that first because
            // Value::get_uint() returns None for Rational values.
            if let exif::Value::Rational(rats) = value
                && let Some(first) = rats.first()
                && first.num > 0
                && first.denom > 0
            {
                let g = gcd(first.num, first.denom);
                if let (Some(n), Some(d)) = (first.num.checked_div(g), first.denom.checked_div(g)) {
                    if d == 1 {
                        // Whole seconds, e.g. "2s"
                        return format_seconds(n as f64);
                    }
                    // Exact reduced fraction plus human-readable time, always
                    let secs = n as f64 / d as f64;
                    return format!("{n}/{d}s ({})", format_seconds(secs));
                }
            }
            // Integer-typed fallback (some writers store plain integers)
            if let Some(r) = value.get_uint(0) {
                return format!("{r}s");
            }
            clean_exif_string(&value.display_as(tag).to_string())
        },
        exif::Tag::ExposureBiasValue => {
            // ExposureBias is stored as SRational; widen to i64 first so that
            // negating i32::MIN (-2147483648) never overflows or panics in gcd().
            if let exif::Value::SRational(rats) = value
                && let Some(first) = rats.first()
                && first.denom != 0
            {
                let (mut n, mut d) = (first.num as i64, first.denom as i64);
                if d < 0 {
                    // Normalize sign into the numerator safely in i64
                    n = n.saturating_neg();
                    d = d.saturating_neg();
                }
                let g = gcd(n, d);
                if let (Some(n), Some(d)) = (n.checked_div(g), d.checked_div(g)) {
                    return if n == 0 {
                        "0 EV".to_string()
                    } else if d == 1 {
                        format!("{n:+} EV")
                    } else {
                        format!("{n:+}/{d} EV")
                    };
                }
            }
            clean_exif_string(&value.display_as(tag).to_string())
        },
        exif::Tag::FNumber => {
            if let exif::Value::Rational(rats) = value
                && let Some(first) = rats.first()
                && first.denom > 0
            {
                return format!("f/{:.1}", first.num as f64 / first.denom as f64);
            }
            clean_exif_string(&value.display_as(tag).to_string())
        },
        exif::Tag::FocalLength => {
            if let exif::Value::Rational(rats) = value
                && let Some(first) = rats.first()
                && first.denom > 0
            {
                return format!("{}mm", first.num as f64 / first.denom as f64);
            }
            clean_exif_string(&value.display_as(tag).to_string())
        },
        exif::Tag::PhotographicSensitivity => {
            if let Some(iso) = value.get_uint(0) {
                format!("ISO {iso}")
            } else {
                clean_exif_string(&value.display_as(tag).to_string())
            }
        },
        exif::Tag::FocalLengthIn35mmFilm => {
            if let Some(fl) = value.get_uint(0) {
                format!("{fl}mm (35mm equiv)")
            } else {
                clean_exif_string(&value.display_as(tag).to_string())
            }
        },
        _ => {
            // Default: use the library's display formatting, then clean it
            clean_exif_string(&value.display_as(tag).to_string())
        },
    }
}

/// Clean up EXIF string values that may contain garbage or repeated empty entries.
pub fn clean_exif_string(s: &str) -> String {
    // Remove surrounding quotes, brackets, and null bytes if present
    let s = s.trim().trim_matches(|c: char| c == '"' || c == '[' || c == ']' || c == '\0');

    // If the string contains comma-separated values (common in some EXIF fields),
    // clean individual items, discard empty/garbage entries, and preserve valid values
    if s.contains(',') {
        let mut parts: Vec<&str> = s
            .split(',')
            .map(|p| p.trim().trim_matches(|c: char| c == '"' || c == '\'' || c == '\0').trim())
            .filter(|p| !p.is_empty())
            .collect();

        // Deduplicate consecutive identical entries often produced by buggy EXIF encoders
        parts.dedup();

        if !parts.is_empty() {
            return parts.join(", ");
        }
    }

    // Remove any trailing garbage (null bytes represented as empty quotes, etc.)
    s.trim_matches(|c: char| c == '"' || c == '\'' || c == ',' || c.is_whitespace() || c == '\0')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_country_and_location_florida() {
        let lat = 28.68;
        let lon = -81.31;
        assert_eq!(derive_country(lat, lon), Some("United States of America (the)".to_string()));
        assert_eq!(derive_subdivision(lat, lon), Some("Florida".to_string()));
        assert_eq!(
            derive_location(lat, lon),
            Some("Florida, United States of America (the)".to_string())
        );
    }

    #[test]
    fn test_format_dms_from_decimal_handles_carry_and_non_finite() {
        assert_eq!(format_dms_from_decimal(28.99999, true), "29° 0' 0.0\" N");
        assert_eq!(format_dms_from_decimal(-81.5, false), "81° 30' 0.0\" W");
        assert_eq!(format_dms_from_decimal(f64::NAN, true), "");
        assert_eq!(format_dms_from_decimal(f64::INFINITY, false), "");
    }

    #[test]
    fn test_clean_exif_string() {
        assert_eq!(clean_exif_string("[\"Canon\", \"Canon\", \"\"]"), "Canon");
        assert_eq!(clean_exif_string("\"Nikon\", \"Z8\""), "Nikon, Z8");
    }
}
