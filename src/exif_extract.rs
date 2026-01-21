// EXIF extraction functions that convert exif crate data to our storage format.
// Integrates functionality from helper_exif.rs.
// Filters out thumbnails and large binary blobs.
use crate::exif_types::{
    ExifValue, MAX_TAG_SIZE, TAG_DERIVED_COUNTRY, TAG_DERIVED_SUBDIVISION,
    TAG_DERIVED_SUN_ALTITUDE, TAG_DERIVED_SUN_AZIMUTH, TAG_DERIVED_TIMESTAMP, TAG_DERIVED_TIMEZONE,
    TAG_GPS_ALTITUDE, TAG_GPS_LATITUDE, TAG_GPS_LONGITUDE, TAG_ORIENTATION, is_excluded_tag,
};
use crate::image_features::ImageFeatures;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use exif::{In, Tag, Value};
use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;

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
        }
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
        && rats.len() >= 3
    {
        if rats[0].denom == 0 || rats[1].denom == 0 || rats[2].denom == 0 {
            return None;
        }
        let degrees = rats[0].to_f64();
        let minutes = rats[1].to_f64();
        let seconds = rats[2].to_f64();
        return Some(degrees + minutes / 60.0 + seconds / 3600.0);
    }
    None
}

/// Parses date string. If `use_gps` is true, attempts GPS time (UTC).
pub fn get_date_str(exif: &exif::Exif, use_gps: bool) -> Option<String> {
    if use_gps {
        let date_field = exif.get_field(Tag::GPSDateStamp, In::PRIMARY);
        let time_field = exif.get_field(Tag::GPSTimeStamp, In::PRIMARY);

        if let (Some(d_field), Some(t_field)) = (date_field, time_field) {
            // Parse Date: "YYYY:MM:DD"
            let date_part = if let Value::Ascii(ref vec) = d_field.value {
                if !vec.is_empty() {
                    std::str::from_utf8(&vec[0]).ok()?.trim().replace(':', "-")
                } else {
                    return None;
                }
            } else {
                return None;
            };

            // Parse Time: 3 Rationals [Hr, Min, Sec]
            let time_part = if let Value::Rational(ref rats) = t_field.value {
                if rats.len() >= 3 && rats[0].denom != 0 && rats[1].denom != 0 && rats[2].denom != 0
                {
                    let h = rats[0].num as f64 / rats[0].denom as f64;
                    let m = rats[1].num as f64 / rats[1].denom as f64;
                    let s = rats[2].num as f64 / rats[2].denom as f64;
                    format!("{:02}:{:02}:{:06.3}", h as u32, m as u32, s)
                } else {
                    return None;
                }
            } else {
                return None;
            };

            return Some(format!("{} {}", date_part, time_part));
        }
        return None;
    }

    // Default: Use DateTimeOriginal
    let field = exif.get_field(Tag::DateTimeOriginal, In::PRIMARY)?;
    match field.value {
        Value::Ascii(ref vec) if !vec.is_empty() => {
            if let Ok(dt) = exif::DateTime::from_ascii(&vec[0]) {
                return Some(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second
                ));
            }
        }
        _ => {}
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
    if let Value::Ascii(ref vec) = field.value {
        if vec.is_empty() {
            return None;
        }
        if let Ok(dt) = exif::DateTime::from_ascii(&vec[0]) {
            let date = NaiveDate::from_ymd_opt(dt.year as i32, dt.month as u32, dt.day as u32)?;
            let time = NaiveTime::from_hms_opt(dt.hour as u32, dt.minute as u32, dt.second as u32)?;
            let naive_dt = NaiveDateTime::new(date, time);
            return Some(naive_dt.and_utc().timestamp());
        }
    }
    None
}

/// Gets altitude from EXIF tags
pub fn get_altitude(exif: &exif::Exif) -> Option<f64> {
    let val_field = exif.get_field(Tag::GPSAltitude, In::PRIMARY)?;
    let ref_field = exif.get_field(Tag::GPSAltitudeRef, In::PRIMARY);

    if let Value::Rational(ref rats) = val_field.value {
        if rats.is_empty() || rats[0].denom == 0 {
            return None;
        }

        let mut alt = rats[0].num as f64 / rats[0].denom as f64;

        if let Some(rf) = ref_field
            && let Value::Byte(ref bytes) = rf.value
            && !bytes.is_empty()
            && bytes[0] == 1
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
            Value::Byte(v) => {
                if v.len() == 1 {
                    ExifValue::Byte(v[0])
                } else if v.len() <= MAX_TAG_SIZE {
                    ExifValue::Bytes(v.clone())
                } else {
                    continue;
                }
            }

            Value::Short(v) => {
                if v.len() == 1 {
                    ExifValue::Short(v[0])
                } else if v.len() <= MAX_TAG_SIZE / 2 {
                    ExifValue::Shorts(v.clone())
                } else {
                    continue;
                }
            }

            Value::Long(v) => {
                if v.len() == 1 {
                    ExifValue::Long(v[0])
                } else {
                    continue;
                }
            }

            Value::SLong(v) => {
                if v.len() == 1 {
                    ExifValue::Signed(v[0])
                } else {
                    continue;
                }
            }

            Value::Rational(v) => {
                if v.is_empty() {
                    continue;
                }
                if v.len() == 1 {
                    if v[0].denom != 0 {
                        ExifValue::Float((v[0].num / v[0].denom) as f64)
                    } else {
                        continue;
                    }
                } else {
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
                }
            }

            Value::SRational(v) => {
                if v.is_empty() {
                    continue;
                }
                if v.len() == 1 {
                    if v[0].denom != 0 {
                        ExifValue::Float(v[0].num as f64 / v[0].denom as f64)
                    } else {
                        continue;
                    }
                } else {
                    let floats: Vec<f64> = v
                        .iter()
                        .filter(|r| r.denom != 0)
                        .map(|r| r.num as f64 / r.denom as f64)
                        .collect();
                    if floats.is_empty() {
                        continue;
                    }
                    ExifValue::Floats(floats)
                }
            }

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
            }

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
            }

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
        //eprintln!("exif_extract sun az={} alt={} tz={}", azimuth, altitude, timezone);
        features.insert_tag(TAG_DERIVED_SUN_AZIMUTH, ExifValue::Float(azimuth));
        features.insert_tag(TAG_DERIVED_SUN_ALTITUDE, ExifValue::Float(altitude));
        features.insert_tag(TAG_DERIVED_TIMEZONE, ExifValue::String(timezone));
    }
}

/// Derive country name from GPS coordinates
pub fn derive_country(lat: f64, lon: f64) -> Option<String> {
    use codes_iso_3166::part_1::CountryCode;
    use country_boundaries::{BOUNDARIES_ODBL_360X180, CountryBoundaries, LatLon};

    let boundaries = CountryBoundaries::from_reader(BOUNDARIES_ODBL_360X180).ok()?;
    let ids = boundaries.ids(LatLon::new(lat, lon).ok()?);

    ids.first().and_then(|id| {
        CountryCode::from_str(id.to_string().as_str()).ok().map(|c| c.short_name().to_string())
    })
}

/// Derive subdivision/state from GPS coordinates
pub fn derive_subdivision(lat: f64, lon: f64) -> Option<String> {
    use codes_iso_3166::part_2::SubdivisionCode;
    use country_boundaries::{BOUNDARIES_ODBL_360X180, CountryBoundaries, LatLon};

    let boundaries = CountryBoundaries::from_reader(BOUNDARIES_ODBL_360X180).ok()?;
    let ids = boundaries.ids(LatLon::new(lat, lon).ok()?);

    for id in ids {
        let id_str = id.to_string();
        if id_str.len() > 2
            && let Ok(subdiv) = SubdivisionCode::from_str(&id_str)
        {
            return Some(subdiv.name().to_string());
        }
    }
    None
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
        } else if let Some(d) = get_date_str(exif_data, false) {
            (d, false)
        } else {
            return None;
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
