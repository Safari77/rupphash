use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use exif::{In, Tag, Value};

/// Parse GPS coordinate magnitude from EXIF rational values (DMS -> Decimal).
///
/// Note: This always returns a positive value. The caller must apply the
/// sign based on the GPSLatitudeRef (N/S) or GPSLongitudeRef (E/W) tags.
pub fn parse_gps_coordinate(value: &exif::Value) -> Option<f64> {
    if let exif::Value::Rational(rats) = value
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

/// Parses date string. If `use_gps` is true, attempts to construct it from
/// GPSDateStamp (0x001D) and GPSTimeStamp (0x0007).
pub fn get_date_str(exif: &exif::Exif, use_gps: bool) -> Option<String> {
    if use_gps {
        // Try to fetch GPS Date (Ascii) and GPS Time (Rational)
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
        // If GPS requested but missing, return None (strict)
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
/// Tries DateTimeOriginal first, then DateTimeDigitized.
/// Returns None if neither tag is present or parseable.
pub fn get_exif_timestamp(exif: &exif::Exif) -> Option<i64> {
    // Try DateTimeOriginal first
    if let Some(ts) = parse_exif_datetime_tag(exif, Tag::DateTimeOriginal) {
        return Some(ts);
    }
    // Fallback to DateTimeDigitized
    parse_exif_datetime_tag(exif, Tag::DateTimeDigitized)
}

/// Parse an EXIF DateTime tag to Unix timestamp (seconds since epoch).
/// EXIF DateTime format: "YYYY:MM:DD HH:MM:SS"
fn parse_exif_datetime_tag(exif: &exif::Exif, tag: Tag) -> Option<i64> {
    let field = exif.get_field(tag, In::PRIMARY)?;
    if let Value::Ascii(ref vec) = field.value {
        if vec.is_empty() {
            return None;
        }
        if let Ok(dt) = exif::DateTime::from_ascii(&vec[0]) {
            // Convert to Unix timestamp using chrono
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
