use geo::{Bearing, Distance, Geodesic, Point};
use jiff::{civil::DateTime as CivilDateTime, Zoned};
use solar_positioning::{spa, RefractionCorrection};
use std::sync::OnceLock;
use tzf_rs::DefaultFinder;
use chrono::Datelike;

static TZ_FINDER: OnceLock<DefaultFinder> = OnceLock::new();

fn get_finder() -> &'static DefaultFinder {
    TZ_FINDER.get_or_init(DefaultFinder::new)
}

fn resolve_timezone(lon: f64, lat: f64) -> String {
    let finder = get_finder();
    let original_tz = finder.get_tz_name(lon, lat);
    if !original_tz.starts_with("Etc/") {
        return original_tz.to_string();
    }
    let search_offset = 0.5;
    let directions = [(0.0, search_offset), (0.0, -search_offset), (search_offset, 0.0), (-search_offset, 0.0)];
    for (d_lat, d_lon) in directions {
        let neighbor_tz = finder.get_tz_name(lon + d_lon, lat + d_lat);
        if !neighbor_tz.starts_with("Etc/") {
            return neighbor_tz.to_string();
        }
    }
    original_tz.to_string()
}

pub fn distance_and_bearing(p1: (f64, f64), p2: (f64, f64)) -> (f64, f64) {
    let start = Point::new(p1.1, p1.0);
    let end = Point::new(p2.1, p2.0);
    let distance = Geodesic.distance(start, end);
    let raw_bearing = Geodesic.bearing(start, end);
    let bearing = (raw_bearing + 360.0) % 360.0;
    (distance, bearing)
}

// Returns Result<..., String> for debug info
pub fn sun_alt_and_azimuth(
    local_time_str: &str,
    lat: f64,
    lon: f64,
    altitude: Option<f64>,
) -> Result<(f64, f64, String), String> {

    if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
        return Err(format!("Coordinates out of bounds: {}, {}", lat, lon));
    }

    let tz_name = resolve_timezone(lon, lat);

    // Only replace colons if the DATE part actually looks like "YYYY:MM:DD"
    let clean_time = local_time_str.trim().replace(' ', "T");

    // Check if we have "YYYY:MM:DD..." vs "YYYY-MM-DD..."
    // Only replace colons if the 5th char is a colon (2024:...)
    let final_time_str = if clean_time.chars().nth(4) == Some(':') {
        clean_time.replacen(':', "-", 2)
    } else {
        clean_time // Already has hyphens or is invalid in a different way
    };

    let civil_dt = final_time_str.parse::<CivilDateTime>()
        .map_err(|e| format!("Date Parse Error: '{}' -> {}", final_time_str, e))?;

    let zoned_time: Zoned = civil_dt.in_tz(&tz_name)
        .map_err(|e| format!("Timezone Error ({}): {}", tz_name, e))?;

    let offset_seconds = zoned_time.offset().seconds();
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let abs_seconds = offset_seconds.abs();
    let hours = abs_seconds / 3600;
    let minutes = (abs_seconds % 3600) / 60;

    let tz_display = format!("{} ({}{:02}:{:02})", tz_name, sign, hours, minutes);

    let timestamp_secs = zoned_time.timestamp().as_second();
    let timestamp_nanos = zoned_time.timestamp().subsec_nanosecond();

    let chrono_time = chrono::DateTime::from_timestamp(
        timestamp_secs,
        timestamp_nanos.try_into().unwrap_or(0),
    ).ok_or("Invalid timestamp conversion")?
    .with_timezone(&chrono::Utc);

    let delta_t = solar_positioning::time::DeltaT::estimate_from_date(
        chrono_time.year(),
        chrono_time.month()
    ).map_err(|_| "solar_positioning DeltaT estimation failed")?;

    let elev_meters = altitude.unwrap_or(0.0);

    let pos = spa::solar_position(
        chrono_time,
        lat,
        lon,
        elev_meters,
        delta_t,
        Some(RefractionCorrection::standard()),
    )
    .map_err(|_| "SPA calculation failed")?;

    Ok((pos.elevation_angle(), pos.azimuth(), tz_display))
}
