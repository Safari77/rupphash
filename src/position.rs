use std::{borrow::Cow, sync::OnceLock};

use chrono::Datelike;
use geo::{Bearing, Distance, Geodesic, Point};
use jiff::{ZonedDateTime, civil::DateTime as CivilDateTime};
use rayon::prelude::*;
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

const MEAN_EARTH_RADIUS_M: f64 = 6_371_008.8;
const WGS84_MIN_MERIDIONAL_DEG_M: f64 = 110_574.0;
const WGS84_MAX_DEG_M: f64 = 111_700.0;
const WGS84_SAFE_MIN_RADIUS_M: f64 = 6_335_000.0;
const MAX_EARTH_GEODESIC_M: f64 = 20_005_000.0;
const HAVERSINE_REL_MARGIN: f64 = 0.01;
const HAVERSINE_ABS_MARGIN_M: f64 = 1.0;

/// Precomputed exact WGS84 geodesic distance-range filter `[min_km, max_km]`.
///
/// Uses a 3-tier evaluation pipeline so millions of candidate points can be
/// tested with zero false positives or false negatives compared to `Geodesic.distance`:
/// 1. Flat lat/lon outer and inner bounding-box rejection (~2 ns, 0 trig calls).
/// 2. Spherical Haversine with a ±1% exactness guard band (~15 ns).
/// 3. Exact WGS84 `Geodesic.distance` only for points within 1% of `min_km` or `max_km`.
#[derive(Debug, Clone, Copy)]
pub struct GeoDistanceFilter {
    target_lat: f64,
    target_lon: f64,
    target_lat_rad: f64,
    target_lon_rad: f64,
    cos_target_lat: f64,
    min_m: f64,
    max_m: f64,
    min_lat: f64,
    max_lat: f64,
    max_dlon: f64,
    check_lon_box: bool,
    inner_deg_sum: f64,
    haversine_min_reject: f64,
    haversine_min_accept: f64,
    haversine_max_accept: f64,
    haversine_max_reject: f64,
}

impl GeoDistanceFilter {
    /// Create a distance-range filter around `(target_lat, target_lon)` in degrees.
    pub fn new(target_lat: f64, target_lon: f64, min_km: f64, max_km: f64) -> Self {
        let target_lat_rad = target_lat.to_radians();
        let target_lon_rad = target_lon.to_radians();
        let cos_target_lat = target_lat_rad.cos();

        let empty = !target_lat.is_finite()
            || !target_lon.is_finite()
            || !min_km.is_finite()
            || max_km.is_nan()
            || max_km < 0.0
            || max_km < min_km;

        let min_m = if empty { f64::INFINITY } else { min_km.max(0.0) * 1000.0 };
        let empty = empty || min_m > MAX_EARTH_GEODESIC_M;

        if empty {
            return Self {
                target_lat,
                target_lon,
                target_lat_rad,
                target_lon_rad,
                cos_target_lat,
                min_m: f64::INFINITY,
                max_m: -1.0,
                min_lat: 1.0,
                max_lat: -1.0,
                max_dlon: 0.0,
                check_lon_box: false,
                inner_deg_sum: 0.0,
                haversine_min_reject: f64::INFINITY,
                haversine_min_accept: f64::INFINITY,
                haversine_max_accept: -1.0,
                haversine_max_reject: -1.0,
            };
        }

        let max_m = if max_km.is_infinite() || max_km >= MAX_EARTH_GEODESIC_M / 1000.0 {
            f64::INFINITY
        } else {
            max_km * 1000.0
        };

        let (min_lat, max_lat, check_lon_box, max_dlon) = if max_m.is_finite() {
            let max_dlat = max_m / WGS84_MIN_MERIDIONAL_DEG_M + 1e-4;
            let min_lat = (target_lat - max_dlat).max(-90.0);
            let max_lat = (target_lat + max_dlat).min(90.0);

            let theta = max_m / WGS84_SAFE_MIN_RADIUS_M;
            let (check_lon_box, max_dlon) =
                if min_lat > -89.9 && max_lat < 89.9 && theta < std::f64::consts::FRAC_PI_2 {
                    let sin_val = theta.sin() / cos_target_lat;
                    if sin_val < 0.999 {
                        let bound = sin_val.asin().to_degrees() + 0.05;
                        if bound < 180.0 { (true, bound) } else { (false, 180.0) }
                    } else {
                        (false, 180.0)
                    }
                } else {
                    (false, 180.0)
                };

            (min_lat, max_lat, check_lon_box, max_dlon)
        } else {
            (-90.0, 90.0, false, 180.0)
        };

        let inner_deg_sum = if min_m > 0.0 { min_m / WGS84_MAX_DEG_M } else { 0.0 };

        let haversine_min_reject = if min_m > 0.0 {
            (min_m * (1.0 - HAVERSINE_REL_MARGIN) - HAVERSINE_ABS_MARGIN_M).max(0.0)
        } else {
            0.0
        };
        let haversine_min_accept = if min_m > 0.0 {
            min_m * (1.0 + HAVERSINE_REL_MARGIN) + HAVERSINE_ABS_MARGIN_M
        } else {
            0.0
        };
        let haversine_max_accept = if max_m.is_finite() {
            (max_m * (1.0 - HAVERSINE_REL_MARGIN) - HAVERSINE_ABS_MARGIN_M).max(0.0)
        } else {
            f64::INFINITY
        };
        let haversine_max_reject = if max_m.is_finite() {
            max_m * (1.0 + HAVERSINE_REL_MARGIN) + HAVERSINE_ABS_MARGIN_M
        } else {
            f64::INFINITY
        };

        Self {
            target_lat,
            target_lon,
            target_lat_rad,
            target_lon_rad,
            cos_target_lat,
            min_m,
            max_m,
            min_lat,
            max_lat,
            max_dlon,
            check_lon_box,
            inner_deg_sum,
            haversine_min_reject,
            haversine_min_accept,
            haversine_max_accept,
            haversine_max_reject,
        }
    }

    /// Create a distance-range filter from a `geo::Point` (`x` = lon, `y` = lat).
    #[inline]
    pub fn from_point(target: Point<f64>, min_km: f64, max_km: f64) -> Self {
        Self::new(target.y(), target.x(), min_km, max_km)
    }

    /// Conservative minimum latitude bound in degrees (used by `GeoSpatialIndex`).
    #[inline]
    pub fn min_lat(&self) -> f64 {
        self.min_lat
    }

    /// Conservative maximum latitude bound in degrees (used by `GeoSpatialIndex`).
    #[inline]
    pub fn max_lat(&self) -> f64 {
        self.max_lat
    }

    /// Returns `true` if this filter can never match any point.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.min_lat > self.max_lat
    }

    #[inline]
    fn haversine_distance_m(&self, lat: f64, lon: f64) -> f64 {
        let lat_rad = lat.to_radians();
        let dlat = lat_rad - self.target_lat_rad;
        let dlon = lon.to_radians() - self.target_lon_rad;
        let sin_dlat = (dlat * 0.5).sin();
        let sin_dlon = (dlon * 0.5).sin();
        let a = sin_dlat * sin_dlat + self.cos_target_lat * lat_rad.cos() * sin_dlon * sin_dlon;
        let c = 2.0 * a.clamp(0.0, 1.0).sqrt().asin();
        MEAN_EARTH_RADIUS_M * c
    }

    /// Test whether `(lat, lon)` in degrees falls within `[min_km, max_km]` of the target.
    #[inline]
    pub fn matches(&self, lat: f64, lon: f64) -> bool {
        // Tier 1a: Outer latitude bounding box (also rejects empty/invalid filters)
        if lat < self.min_lat || lat > self.max_lat {
            return false;
        }

        // Fast path for unconstrained [0, ∞) range
        if self.min_m == 0.0 && self.max_m.is_infinite() {
            return true;
        }

        // Tier 1b: Outer longitude bounding box & inner exclusion box
        if self.check_lon_box || self.inner_deg_sum > 0.0 {
            let mut dlon = (lon - self.target_lon).abs();
            if dlon > 180.0 {
                dlon = 360.0 - dlon;
            }
            if self.check_lon_box && dlon > self.max_dlon {
                return false;
            }
            if self.inner_deg_sum > 0.0 {
                let dlat = (lat - self.target_lat).abs();
                if dlat + dlon < self.inner_deg_sum {
                    return false;
                }
            }
        }

        // Tier 2: Fast spherical Haversine with ±1% exactness guard band
        let d_sph = self.haversine_distance_m(lat, lon);
        if d_sph > self.haversine_max_reject || d_sph < self.haversine_min_reject {
            return false;
        }
        if d_sph >= self.haversine_min_accept && d_sph <= self.haversine_max_accept {
            return true;
        }

        // Tier 3: Exact WGS84 ellipsoidal geodesic for boundary candidates only
        let d_geod = distance((lat, lon), (self.target_lat, self.target_lon));
        d_geod >= self.min_m && d_geod <= self.max_m
    }

    /// Test whether a `geo::Point` (`x` = lon, `y` = lat) matches this filter.
    #[inline]
    pub fn matches_point(&self, point: Point<f64>) -> bool {
        self.matches(point.y(), point.x())
    }
}

/// Compact entry stored in `GeoSpatialIndex` (24 bytes, cache-friendly sequential scan).
#[derive(Debug, Clone, Copy)]
pub struct GeoPointEntry {
    pub lat: f64,
    pub lon: f64,
    pub id: u32,
}

/// Latitude-sorted spatial index for fast distance-range queries over millions of points.
///
/// Queries binary-search the sorted latitude slice in `O(log N)` to narrow to the
/// candidate latitude band, then evaluate `GeoDistanceFilter::matches` (in parallel
/// with `rayon` when the candidate slice is large).
#[derive(Debug, Default, Clone)]
pub struct GeoSpatialIndex {
    entries: Vec<GeoPointEntry>,
    is_finalized: bool,
}

const PARALLEL_SLICE_THRESHOLD: usize = 16_384;

impl GeoSpatialIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self { entries: Vec::with_capacity(capacity), is_finalized: true }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.is_finalized = true;
    }

    #[inline]
    pub fn insert(&mut self, id: u32, lat: f64, lon: f64) {
        if lat.is_finite() && lon.is_finite() {
            self.entries.push(GeoPointEntry { lat, lon, id });
            self.is_finalized = false;
        }
    }

    /// Sort entries by latitude in parallel so queries can binary-search in `O(log N)`.
    pub fn finalize(&mut self) {
        if !self.is_finalized {
            self.entries.par_sort_unstable_by(|a, b| a.lat.total_cmp(&b.lat));
            self.is_finalized = true;
        }
    }

    /// Search for all IDs matching a single `GeoDistanceFilter`.
    /// Call `finalize()` after inserting entries before querying.
    pub fn search(&self, filter: &GeoDistanceFilter) -> Vec<u32> {
        if self.entries.is_empty() || filter.is_empty() {
            return Vec::new();
        }
        debug_assert!(
            self.is_finalized,
            "GeoSpatialIndex::finalize() must be called before search"
        );

        let slice = self.lat_slice(filter.min_lat, filter.max_lat);
        if slice.len() >= PARALLEL_SLICE_THRESHOLD {
            slice.par_iter().filter_map(|e| filter.matches(e.lat, e.lon).then_some(e.id)).collect()
        } else {
            slice.iter().filter_map(|e| filter.matches(e.lat, e.lon).then_some(e.id)).collect()
        }
    }

    /// Search for all IDs matching multiple `GeoDistanceFilter`s (AND when `match_any == false`,
    /// OR when `match_any == true`).
    pub fn search_many(&self, filters: &[GeoDistanceFilter], match_any: bool) -> Vec<u32> {
        if self.entries.is_empty() || filters.is_empty() {
            return Vec::new();
        }
        if let [single] = filters {
            return self.search(single);
        }
        debug_assert!(
            self.is_finalized,
            "GeoSpatialIndex::finalize() must be called before search"
        );

        if !match_any {
            // AND mode: intersect latitude bands across all filters first
            let mut min_lat = -90.0_f64;
            let mut max_lat = 90.0_f64;
            for f in filters {
                if f.is_empty() {
                    return Vec::new();
                }
                min_lat = min_lat.max(f.min_lat);
                max_lat = max_lat.min(f.max_lat);
                if min_lat > max_lat {
                    return Vec::new();
                }
            }

            let slice = self.lat_slice(min_lat, max_lat);
            if slice.len() >= PARALLEL_SLICE_THRESHOLD {
                slice
                    .par_iter()
                    .filter_map(|e| filters.iter().all(|f| f.matches(e.lat, e.lon)).then_some(e.id))
                    .collect()
            } else {
                slice
                    .iter()
                    .filter_map(|e| filters.iter().all(|f| f.matches(e.lat, e.lon)).then_some(e.id))
                    .collect()
            }
        } else {
            // OR mode: merge overlapping latitude bands so each entry is visited at most once
            let mut intervals: Vec<(f64, f64)> =
                filters.iter().filter(|f| !f.is_empty()).map(|f| (f.min_lat, f.max_lat)).collect();
            if intervals.is_empty() {
                return Vec::new();
            }
            intervals.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));

            let mut merged: Vec<(f64, f64)> = Vec::with_capacity(intervals.len());
            for (lo, hi) in intervals {
                if let Some(last) = merged.last_mut()
                    && lo <= last.1
                {
                    last.1 = last.1.max(hi);
                } else {
                    merged.push((lo, hi));
                }
            }

            let mut results = Vec::new();
            for (lo, hi) in merged {
                let slice = self.lat_slice(lo, hi);
                if slice.len() >= PARALLEL_SLICE_THRESHOLD {
                    let mut chunk: Vec<u32> = slice
                        .par_iter()
                        .filter_map(|e| {
                            filters.iter().any(|f| f.matches(e.lat, e.lon)).then_some(e.id)
                        })
                        .collect();
                    results.append(&mut chunk);
                } else {
                    results.extend(slice.iter().filter_map(|e| {
                        filters.iter().any(|f| f.matches(e.lat, e.lon)).then_some(e.id)
                    }));
                }
            }
            results
        }
    }

    #[inline]
    fn lat_slice(&self, min_lat: f64, max_lat: f64) -> &[GeoPointEntry] {
        let start = self.entries.partition_point(|e| e.lat < min_lat);
        let tail = self.entries.get(start..).unwrap_or(&[]);
        let len = tail.partition_point(|e| e.lat <= max_lat);
        tail.get(..len).unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exact_matches(target: (f64, f64), point: (f64, f64), min_km: f64, max_km: f64) -> bool {
        let d_km = distance(point, target) / 1000.0;
        d_km >= min_km && d_km <= max_km
    }

    #[test]
    fn test_geo_distance_filter_matches_exact_geodesic() {
        let targets = [
            (60.1699, 24.9384),   // Helsinki (high latitude)
            (0.0, 0.0),           // Equator / prime meridian
            (-33.8688, 151.2093), // Sydney (southern hemisphere)
            (10.0, 179.95),       // Antimeridian (+180 wrap)
            (89.5, 45.0),         // Near North Pole
            (-89.7, -120.0),      // Near South Pole
        ];
        let ranges = [
            (0.0, 0.0),
            (0.0, 5.0),
            (1.0, 5.0),
            (20.0, 50.0),
            (100.0, 500.0),
            (500.0, f64::MAX),
            (50.0, 10.0), // invalid/empty
        ];

        // Deterministic LCG for reproducible coverage across globe + near-boundary points
        let mut seed: u64 = 0x5EED_CAFE_1234_5678;
        let mut next_f64 = || -> f64 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            ((seed >> 11) as f64) / ((1u64 << 53) as f64)
        };

        for &(tlat, tlon) in &targets {
            for &(min_km, max_km) in &ranges {
                let filter = GeoDistanceFilter::new(tlat, tlon, min_km, max_km);
                for i in 0..500 {
                    let (lat, lon) = if i < 250 {
                        // Cluster points near target to stress boundary & inner box
                        let dlat = (next_f64() - 0.5) * 4.0;
                        let dlon = (next_f64() - 0.5) * 6.0;
                        let lat = (tlat + dlat).clamp(-90.0, 90.0);
                        let mut lon = tlon + dlon;
                        if lon > 180.0 {
                            lon -= 360.0;
                        } else if lon < -180.0 {
                            lon += 360.0;
                        }
                        (lat, lon)
                    } else {
                        // Uniform global points
                        (next_f64() * 180.0 - 90.0, next_f64() * 360.0 - 180.0)
                    };

                    let got = filter.matches(lat, lon);
                    let expected = exact_matches((tlat, tlon), (lat, lon), min_km, max_km);
                    assert_eq!(
                        got, expected,
                        "Mismatch for target=({tlat}, {tlon}), point=({lat}, {lon}), range=[{min_km}, {max_km}]"
                    );
                }
            }
        }
    }

    #[test]
    fn test_geo_spatial_index_single_and_many() {
        let mut index = GeoSpatialIndex::new();
        let points = [
            (0, 60.1699, 24.9384),  // Helsinki
            (1, 60.2055, 24.6559),  // Espoo (~16 km from Helsinki)
            (2, 61.4978, 23.7610),  // Tampere (~161 km from Helsinki)
            (3, 59.4370, 24.7536),  // Tallinn (~82 km from Helsinki)
            (4, 35.6762, 139.6503), // Tokyo
        ];
        for &(id, lat, lon) in &points {
            index.insert(id, lat, lon);
        }
        index.finalize();

        let near_helsinki = GeoDistanceFilter::new(60.1699, 24.9384, 1.0, 100.0);
        let mut res = index.search(&near_helsinki);
        res.sort_unstable();
        assert_eq!(res, vec![1, 3]);

        // AND: within 100 km of Helsinki AND within 100 km of Espoo
        let near_espoo = GeoDistanceFilter::new(60.2055, 24.6559, 0.0, 20.0);
        let mut and_res = index.search_many(&[near_helsinki, near_espoo], false);
        and_res.sort_unstable();
        assert_eq!(and_res, vec![1]);

        // OR: near Helsinki [1..100 km] OR near Tokyo [0..10 km]
        let near_tokyo = GeoDistanceFilter::new(35.6762, 139.6503, 0.0, 10.0);
        let mut or_res = index.search_many(&[near_helsinki, near_tokyo], true);
        or_res.sort_unstable();
        assert_eq!(or_res, vec![1, 3, 4]);
    }

    #[test]
    #[ignore]
    fn test_10_million_points_geo_search_performance() {
        const N: u32 = 10_000_000;
        let t0 = std::time::Instant::now();
        let mut index = GeoSpatialIndex::with_capacity(N as usize);
        let mut seed: u64 = 0x1234_5678_9ABC_DEF0;
        for id in 0..N {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let lat = ((seed >> 11) as f64) / ((1u64 << 53) as f64) * 180.0 - 90.0;
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let lon = ((seed >> 11) as f64) / ((1u64 << 53) as f64) * 360.0 - 180.0;
            index.insert(id, lat, lon);
        }
        index.finalize();
        let build_ms = t0.elapsed().as_millis();

        let filter = GeoDistanceFilter::new(60.1699, 24.9384, 1.0, 5.0);
        let t1 = std::time::Instant::now();
        let matches = index.search(&filter);
        let query_us = t1.elapsed().as_micros();

        println!(
            "10M geo index build: {build_ms} ms, query [1-5 km]: {query_us} us ({} matches)",
            matches.len()
        );
        assert!(query_us < 50_000, "10M query took too long: {query_us} us");
    }
}
