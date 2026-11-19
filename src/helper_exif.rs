use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use exif::{In, Tag, Value};

/// Parse GPS coordinate magnitude from EXIF rational values (DMS -> Decimal).
///
/// Note: This always returns a positive value. The caller must apply the
/// sign based on the GPSLatitudeRef (N/S) or GPSLongitudeRef (E/W) tags.
pub fn parse_gps_coordinate(value: &exif::Value) -> Option<f64> {
    if let exif::Value::Rational(rats) = value
        && let [deg, min, sec, ..] = rats.as_slice()
    {
        if deg.denom == 0 || min.denom == 0 || sec.denom == 0 {
            return None;
        }
        return Some(deg.to_f64() + min.to_f64() / 60.0 + sec.to_f64() / 3600.0);
    }
    None
}

/// Parses date string. If `use_gps` is true, attempts to construct it from
/// GPSDateStamp (0x001D) and GPSTimeStamp (0x0007).
pub fn get_date_str(exif: &exif::Exif, use_gps: bool) -> Option<String> {
    if use_gps {
        // Try to fetch GPS Date (Ascii) and GPS Time (Rational)
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
    if let Value::Ascii(vec) = &field.value
        && let Some(first) = vec.first()
        && let Ok(dt) = exif::DateTime::from_ascii(first)
    {
        // Convert to Unix timestamp using chrono
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
