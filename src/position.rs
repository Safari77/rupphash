use std::{borrow::Cow, sync::OnceLock};

use chrono::Datelike;
use geo::{Bearing, Distance, Geodesic, Point};
use jiff::{ZonedDateTime, civil::DateTime as CivilDateTime};
use solar_positioning::{Location, RefractionCorrection, SolarPositions, delta_t};
use tzf_rs::DefaultFinder;

static TZ_FINDER: OnceLock<DefaultFinder> = OnceLock::new();

fn get_finder() -> &'static DefaultFinder {
    TZ_FINDER.get_or_init(DefaultFinder::new)
}

fn resolve_timezone(lon: f64, lat: f64) -> &'static str {
    let finder = get_finder();
    let original_tz = finder.get_tz_name(lon, lat);
    if !original_tz.starts_with("Etc/") {
        return original_tz;
    }
    let search_offset = 0.5;
    let directions =
        [(0.0, search_offset), (0.0, -search_offset), (search_offset, 0.0), (-search_offset, 0.0)];
    for (d_lat, d_lon) in directions {
        let neighbor_tz = finder.get_tz_name(lon + d_lon, lat + d_lat);
        if !neighbor_tz.starts_with("Etc/") {
            return neighbor_tz;
        }
    }
    original_tz
}

#[inline]
pub fn distance_and_bearing(p1: (f64, f64), p2: (f64, f64)) -> (f64, f64) {
    let start = Point::new(p1.1, p1.0);
    let end = Point::new(p2.1, p2.0);
    let distance = Geodesic.distance(start, end);
    let raw_bearing = Geodesic.bearing(start, end);
    let bearing = (raw_bearing + 360.0) % 360.0;
    (distance, bearing)
}

#[inline]
pub fn distance(p1: (f64, f64), p2: (f64, f64)) -> f64 {
    let start = Point::new(p1.1, p1.0);
    let end = Point::new(p2.1, p2.0);

    Geodesic.distance(start, end)
}

// Returns Result<..., String> for debug info
pub fn sun_alt_and_azimuth(
    local_time_str: &str,
    lat: f64,
    lon: f64,
    altitude: Option<f64>,
    use_gps_utc: bool,
) -> Result<(f64, f64, String), String> {
    if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
        return Err(format!("Coordinates out of bounds: {lat}, {lon}"));
    }

    // If using GPS time, we force UTC. Otherwise, we resolve the local timezone.
    let tz_name: &str = if use_gps_utc { "UTC" } else { resolve_timezone(lon, lat) };

    // Only replace colons if the DATE part actually looks like "YYYY:MM:DD"
    let trimmed = local_time_str.trim();
    let clean_time: Cow<'_, str> = if trimmed.contains(' ') {
        Cow::Owned(trimmed.replace(' ', "T"))
    } else {
        Cow::Borrowed(trimmed)
    };

    // Check if we have "YYYY:MM:DD..." vs "YYYY-MM-DD..."
    // Only replace colons if the 5th byte is a colon (2024:...)
    let final_time_str: Cow<'_, str> = if clean_time.as_bytes().get(4) == Some(&b':') {
        Cow::Owned(clean_time.replacen(':', "-", 2))
    } else {
        clean_time // Already has hyphens or is invalid in a different way
    };

    let civil_dt = final_time_str
        .parse::<CivilDateTime>()
        .map_err(|e| format!("Date Parse Error: '{final_time_str}' -> {e}"))?;

    // Create Zoned time. Jiff handles "UTC" correctly as a timezone name.
    let zoned_time: ZonedDateTime =
        civil_dt.in_tz(tz_name).map_err(|e| format!("Timezone Error ({tz_name}): {e}"))?;

    let offset_seconds = zoned_time.offset().seconds();
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let abs_seconds = offset_seconds.abs();
    let hours = abs_seconds / 3600;
    let minutes = (abs_seconds % 3600) / 60;

    let tz_display = format!("{tz_name} ({sign}{hours:02}:{minutes:02})");

    let timestamp_secs = zoned_time.timestamp().as_second();
    let timestamp_nanos = zoned_time.timestamp().subsec_nanosecond();

    let chrono_time = chrono::DateTime::from_timestamp(
        timestamp_secs,
        u32::try_from(timestamp_nanos).unwrap_or(0),
    )
    .ok_or("Invalid timestamp conversion")?
    .with_timezone(&chrono::Utc);

    let delta_t = delta_t::estimate_from_date(chrono_time.year(), chrono_time.month())
        .map_err(|_| "solar_positioning DeltaT estimation failed")?;

    let elev_meters = altitude.filter(|a| a.is_finite()).unwrap_or(0.0);

    let location = Location { latitude: lat, longitude: lon };

    let pos = SolarPositions::new()
        .at(&chrono_time, location, elev_meters, delta_t, Some(RefractionCorrection::standard()))
        .map_err(|_| "SPA calculation failed")?;

    Ok((pos.elevation_angle(), pos.azimuth(), tz_display))
}

/// Helper to format sun position consistently for UI and parsing
pub fn format_sun_pos(alt: f64, az: f64) -> String {
    format!("Alt: {alt:.3}°, Az: {az:.3}°")
}

/// Helper to parse sun position string back to values (Alt, Az)
pub fn parse_sun_pos_string(s: &str) -> Option<(f64, f64)> {
    // Expected format: "Alt: 12.531°, Az: 123.433°"
    let (alt_part, az_part) = s.split_once(',')?;

    let alt_str = alt_part.trim().strip_prefix("Alt:")?.trim().trim_end_matches('°').trim();
    let az_str = az_part.trim().strip_prefix("Az:")?.trim().trim_end_matches('°').trim();

    let alt = alt_str.parse::<f64>().ok()?;
    let az = az_str.parse::<f64>().ok()?;

    Some((alt, az))
}
