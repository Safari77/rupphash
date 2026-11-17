// RAW file EXIF extraction using rsraw's FullRawInfo.
// Provides fallback when kamadak-exif fails to parse RAW files.
// Maps rsraw structures to our ExifValue/ImageFeatures format.
//
// Orientation: rsraw's safe FullRawInfo struct does NOT expose orientation, but
// LibRaw's value IS reachable via rsraw's AsRef<libraw_data_t> as
// `raw.as_ref().sizes.flip`. get_orientation_from_raw() (below) surfaces it and
// is now used by image.rs (RAW thumbnail view) and scanner.rs (scan_and_group,
// spawn_background_enrichment, get_exif_tags_from_rsraw).
//
// build_features_from_raw_image() still does not insert TAG_ORIENTATION itself;
// its callers compute orientation separately. Caveat: process() bakes rotation
// into its output (and reports orientation 1) while embedded thumbnails stay
// sensor-native, so a stored orientation is only valid for the representation
// actually displayed - the viewer re-derives it on load.
//
// GPS Coordinate Handling:
// - rsraw stores GPS as [f32; 3] arrays in DMS format (degrees, minutes, seconds)
// - This is different from kamadak-exif which uses Value::Rational
// - We provide dms_to_decimal() specifically for rsraw's format
// - exif_extract::parse_gps_coordinate() handles kamadak-exif's format

use geo::Point;
use rsraw::{FullRawInfo, RawImage};

use crate::{
    exif_types::{
        ExifValue, TAG_ARTIST, TAG_DERIVED_TIMESTAMP, TAG_EXPOSURE_TIME, TAG_FNUMBER,
        TAG_FOCAL_LENGTH, TAG_FOCAL_LENGTH_35MM, TAG_GPS_ALTITUDE, TAG_GPS_LATITUDE,
        TAG_GPS_LONGITUDE, TAG_ISO, TAG_LENS_MAKE, TAG_LENS_MODEL, TAG_MAKE, TAG_MODEL,
        TAG_SOFTWARE,
    },
    image_features::ImageFeatures,
};

/// Check whether the LibRaw GPS header flag is set and both decoded coordinates
/// are non-zero (`>= 0.0001`).
///
/// Note: Requiring *both* `lat.abs() >= 0.0001` and `lon.abs() >= 0.0001` is
/// intentional because some cameras write uninitialized or bogus GPS headers
/// with one or both axes set to 0.0 when no fix was acquired.
#[inline]
fn raw_gps_coords(raw: &RawImage, info: &FullRawInfo) -> Option<(f64, f64)> {
    if raw.as_ref().other.parsed_gps.gpsparsed == 0 {
        return None;
    }

    let lat = dms_to_decimal(&info.gps.latitude);
    let lon = dms_to_decimal(&info.gps.longitude);

    // Intentional: reject when either lat or lon is near 0.0 because some cameras
    // emit invalid GPS tags with 0 on one axis when GPS lock is missing.
    if lat.abs() >= 0.0001 && lon.abs() >= 0.0001 && gps_is_valid(lat, lon) {
        Some((lat, lon))
    } else {
        None
    }
}

/// Check whether LibRaw parsed valid GPS EXIF metadata from the RAW container.
///
/// Note: Both `lat.abs() >= 0.0001` and `lon.abs() >= 0.0001` are required
/// because some cameras generate invalid GPS metadata with 0.0 on one or both
/// axes when no satellite fix is available.
#[inline]
pub fn raw_has_gps(raw: &RawImage) -> bool {
    if raw.as_ref().other.parsed_gps.gpsparsed == 0 {
        return false;
    }

    let gps = &raw.as_ref().other.parsed_gps;
    let lat = dms_to_decimal(&gps.latitude);
    let lon = dms_to_decimal(&gps.longitude);

    lat.abs() >= 0.0001 && lon.abs() >= 0.0001 && gps_is_valid(lat, lon)
}

/// Extract ImageFeatures from an rsraw RawImage.
/// This is used as a fallback when kamadak-exif fails to parse the RAW file.
///
/// Orientation is intentionally not merged here, for the same reason
/// build_features_from_raw_image() does not insert it — see that function.
///
/// Thread Safety: This function only reads from the RawImage, no mutation occurs.
/// The RawImage should already have been opened by the caller.
pub fn build_features_from_raw_image(raw: &RawImage) -> ImageFeatures {
    let info = raw.full_info();
    let mut features = ImageFeatures::new(info.width, info.height);
    populate_features_from_raw_info(&mut features, raw, &info, true);
    features
}

/// Decide whether a decoded lat/lon pair should be published as a position.
///
/// Two separate rejections:
/// - rsraw returns all-zero arrays for files without GPS, so a coordinate
///   within ~11 m of (0, 0) is read as "absent" rather than Null Island.
/// - Out-of-range or non-finite values come from corrupt or vendor-specific
///   headers. They must not reach GpsMapState: walkers projects them into
///   nonsense screen coordinates and they poison the sort bounds.
#[inline]
fn gps_is_valid(lat: f64, lon: f64) -> bool {
    lat.is_finite()
        && lon.is_finite()
        && (-90.0..=90.0).contains(&lat)
        && (-180.0..=180.0).contains(&lon)
        && (lat.abs() > 0.0001 || lon.abs() > 0.0001)
}

/// Convert GPS coordinates from rsraw's [degrees, minutes, seconds] format to decimal degrees.
///
/// rsraw stores GPS as [f32; 3] arrays. This is different from kamadak-exif's
/// Value::Rational format which is handled by exif_extract::parse_gps_coordinate().
///
/// rsraw folds the N/S/E/W sign into ALL THREE components, so each of
/// degrees/minutes/seconds is negative for southern/western coordinates.
#[inline]
pub fn dms_to_decimal(dms: &[f32; 3]) -> f64 {
    // Recover the hemisphere sign from any component (rsraw signs all three),
    // then sum the magnitudes so minutes/seconds add instead of subtracting.
    let sign = if dms.iter().any(|&c| c < 0.0) { -1.0 } else { 1.0 };
    let degrees = f64::from(dms[0].abs());
    let minutes = f64::from(dms[1].abs());
    let seconds = f64::from(dms[2].abs());

    sign * (degrees + minutes / 60.0 + seconds / 3600.0)
}

/// Get GPS position as geo::Point from an rsraw RawImage.
/// Returns None if GPS data was not parsed by LibRaw or coordinates are invalid.
///
/// Thread Safety: Only reads from RawImage.
pub fn get_gps_point_from_raw(raw: &RawImage) -> Option<Point<f64>> {
    if raw.as_ref().other.parsed_gps.gpsparsed == 0 {
        return None;
    }
    let gps = &raw.as_ref().other.parsed_gps;
    let lat = dms_to_decimal(&gps.latitude);
    let lon = dms_to_decimal(&gps.longitude);

    // Both lat and lon must be >= 0.0001 in magnitude because some cameras
    // write 0.0 on one or both axes when GPS data is invalid/uninitialized.
    if lat.abs() >= 0.0001 && lon.abs() >= 0.0001 && gps_is_valid(lat, lon) {
        Some(Point::new(lon, lat)) // geo uses (x, y) = (lon, lat)
    } else {
        None
    }
}

/// Get EXIF timestamp from an rsraw RawImage as Unix epoch seconds.
/// Returns None if datetime is not available.
///
/// Thread Safety: Only reads from RawImage.
pub fn get_timestamp_from_raw(raw: &RawImage) -> Option<i64> {
    raw.full_info().datetime.as_ref().map(chrono::DateTime::timestamp)
}

/// Map LibRaw's `sizes.flip` orientation code to a standard EXIF Orientation
/// value (1-8).
///
/// LibRaw inherits dcraw's flip numbering, which differs from the TIFF/EXIF
/// Orientation tag. The mapping (libraw flip -> exif orientation) is:
///   0 -> 1, 1 -> 2, 2 -> 4, 3 -> 3, 4 -> 5, 5 -> 8, 6 -> 6, 7 -> 7
/// e.g. flip 5 (rotate 90 CCW) is EXIF 8 (Rotate 270 CW). A handful of old
/// cameras store the angle (90/180/270) directly, which we also accept. -1
/// (LibRaw's "take from RAW" default / unknown) and anything unexpected map to 1.
#[inline]
const fn libraw_flip_to_exif_orientation(flip: i32) -> u8 {
    match flip {
        0 => 1,
        1 => 2,
        2 => 4,
        3 | 180 => 3,
        4 => 5,
        5 | 270 => 8,
        6 | 90 => 6,
        7 => 7,
        _ => 1,
    }
}

/// Get the EXIF orientation (1-8) for a RAW file from LibRaw's computed
/// `sizes.flip` value. Returns 1 when no rotation is required or the value is
/// unknown.
///
/// This works even for containers that kamadak-exif cannot parse (e.g. Canon
/// CR3/CRX), because LibRaw populates `sizes.flip` on open. The full decode path
/// (process()) already bakes this rotation into its output pixels, so this
/// helper is meant for callers that display the *embedded thumbnail*, which is
/// stored in the sensor's native orientation and must be rotated by the viewer.
///
/// Thread Safety: Only reads from RawImage.
#[inline]
pub fn get_orientation_from_raw(raw: &RawImage) -> u8 {
    libraw_flip_to_exif_orientation(raw.as_ref().sizes.flip)
}

/// Merge rsraw EXIF data into existing ImageFeatures.
/// Only fills in missing tags (doesn't overwrite existing ones from kamadak-exif).
///
/// Use this when kamadak-exif partially succeeded but might be missing some tags
/// that rsraw can provide.
///
/// Deliberately does not insert TAG_ORIENTATION: LibRaw's value is available
/// via get_orientation_from_raw(), but a stored orientation is only meaningful
/// for the representation actually displayed (process() bakes the rotation in
/// and reports 1, embedded thumbnails stay sensor-native), so callers derive it
/// themselves for whichever one they render.
///
/// Thread Safety: Mutates features, but RawImage is only read.
pub fn merge_raw_info_into_features(features: &mut ImageFeatures, raw: &RawImage) {
    let info = raw.full_info();
    populate_features_from_raw_info(features, raw, &info, false);
}

fn populate_features_from_raw_info(
    features: &mut ImageFeatures,
    raw: &RawImage,
    info: &FullRawInfo,
    overwrite: bool,
) {
    let mut insert_if_needed = |tag: u16, make_val: &dyn Fn() -> ExifValue| {
        if overwrite || !features.has_tag(tag) {
            features.insert_tag(tag, make_val());
        }
    };

    // Camera info
    if !info.make.is_empty() {
        insert_if_needed(TAG_MAKE, &|| ExifValue::String(info.make.clone()));
    }
    if !info.model.is_empty() {
        insert_if_needed(TAG_MODEL, &|| ExifValue::String(info.model.clone()));
    }
    let software = info.software.trim();
    if !software.is_empty() {
        insert_if_needed(TAG_SOFTWARE, &|| ExifValue::String(software.to_string()));
    }
    if !info.artist.is_empty() {
        insert_if_needed(TAG_ARTIST, &|| ExifValue::String(info.artist.clone()));
    }

    // Exposure settings
    if info.iso_speed > 0 {
        insert_if_needed(TAG_ISO, &|| ExifValue::Long(info.iso_speed));
    }
    if info.shutter > 0.0 {
        insert_if_needed(TAG_EXPOSURE_TIME, &|| ExifValue::Float(f64::from(info.shutter)));
    }
    if info.aperture > 0.0 {
        insert_if_needed(TAG_FNUMBER, &|| ExifValue::Float(f64::from(info.aperture)));
    }
    if info.focal_len > 0.0 {
        insert_if_needed(TAG_FOCAL_LENGTH, &|| ExifValue::Float(f64::from(info.focal_len)));
    }

    // Lens info
    if !info.lens_info.lens_make.is_empty() {
        insert_if_needed(TAG_LENS_MAKE, &|| ExifValue::String(info.lens_info.lens_make.clone()));
    }
    let lens_name = info.lens_info.lens_name.trim();
    if !lens_name.is_empty() {
        insert_if_needed(TAG_LENS_MODEL, &|| ExifValue::String(lens_name.to_string()));
    }
    if info.lens_info.focal_length_in_35mm_format > 0 {
        insert_if_needed(TAG_FOCAL_LENGTH_35MM, &|| {
            ExifValue::Short(info.lens_info.focal_length_in_35mm_format)
        });
    }

    // GPS info
    if let Some((lat, lon)) = raw_gps_coords(raw, info) {
        insert_if_needed(TAG_GPS_LATITUDE, &|| ExifValue::Float(lat));
        insert_if_needed(TAG_GPS_LONGITUDE, &|| ExifValue::Float(lon));

        // Altitude — only alongside a usable position, and only if finite.
        if info.gps.altitude.is_finite() && info.gps.altitude.abs() > 0.0001 {
            insert_if_needed(TAG_GPS_ALTITUDE, &|| ExifValue::Float(f64::from(info.gps.altitude)));
        }
    }

    // Timestamp
    if let Some(ref dt) = info.datetime {
        insert_if_needed(TAG_DERIVED_TIMESTAMP, &|| ExifValue::Long64(dt.timestamp()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gps_is_valid() {
        assert!(gps_is_valid(60.1699, 24.9384)); // Helsinki
        assert!(gps_is_valid(51.4779, 0.0)); // Greenwich: lon 0 is fine
        assert!(!gps_is_valid(0.0, 0.0)); // rsraw can return 0.0,0.0 for some cameras without
        // valid GPS data
        assert!(!gps_is_valid(91.0, 10.0)); // latitude out of range
        assert!(!gps_is_valid(45.0, 200.0)); // longitude out of range
        assert!(!gps_is_valid(f64::NAN, 10.0));
        assert!(!gps_is_valid(45.0, f64::INFINITY));
    }

    #[test]
    fn test_dms_to_decimal() {
        // 48° 51' 24" N (Paris approximate)
        let dms = [48.0, 51.0, 24.0];
        let decimal = dms_to_decimal(&dms);
        assert!((decimal - 48.8567).abs() < 0.001);

        // Already decimal
        let dms_decimal = [48.8567, 0.0, 0.0];
        let result = dms_to_decimal(&dms_decimal);
        assert!((result - 48.8567).abs() < 0.001);

        // Negative (Western hemisphere). rsraw folds the sign into ALL three
        // components, so a real western coordinate looks like [-122, -24, -36].
        let dms_west = [-122.0, -24.0, -36.0];
        let decimal_west = dms_to_decimal(&dms_west);
        assert!((decimal_west - (-122.41)).abs() < 0.001);
    }

    #[test]
    fn test_libraw_flip_to_exif_orientation() {
        // Canonical dcraw/LibRaw flip numbering -> EXIF Orientation tag.
        assert_eq!(libraw_flip_to_exif_orientation(0), 1);
        assert_eq!(libraw_flip_to_exif_orientation(1), 2);
        assert_eq!(libraw_flip_to_exif_orientation(2), 4);
        assert_eq!(libraw_flip_to_exif_orientation(3), 3);
        assert_eq!(libraw_flip_to_exif_orientation(4), 5);
        // flip 5 (90 CCW) == EXIF 8 (Rotate 270 CW)
        assert_eq!(libraw_flip_to_exif_orientation(5), 8);
        assert_eq!(libraw_flip_to_exif_orientation(6), 6);
        assert_eq!(libraw_flip_to_exif_orientation(7), 7);

        // Angle-encoded values used by a few legacy cameras.
        assert_eq!(libraw_flip_to_exif_orientation(90), 6);
        assert_eq!(libraw_flip_to_exif_orientation(180), 3);
        assert_eq!(libraw_flip_to_exif_orientation(270), 8);

        // -1 (LibRaw default "take from RAW") and unexpected values -> normal.
        assert_eq!(libraw_flip_to_exif_orientation(-1), 1);
        assert_eq!(libraw_flip_to_exif_orientation(42), 1);
    }
}
