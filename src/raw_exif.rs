// RAW file EXIF extraction using rsraw's FullRawInfo.
// Provides fallback when kamadak-exif fails to parse RAW files.
// Maps rsraw structures to our ExifValue/ImageFeatures format.
//
// Orientation: rsraw's safe FullRawInfo struct does NOT expose orientation, but
// LibRaw's value IS reachable via rsraw's AsRef<libraw_data_t> as
// `raw.as_ref().sizes.flip`. get_orientation_from_raw() (below) surfaces it.
//
// Optional future integration points that still default to orientation 1:
//   1. build_features_from_raw_image() - could insert TAG_ORIENTATION
//   3. scanner.rs spawn_background_enrichment() - could use it as a fallback
//   4. scanner.rs get_exif_tags_from_rsraw() - could add "orientation" handling
// Caveat: process() bakes rotation into its output (and reports orientation 1)
// while embedded thumbnails stay sensor-native, so any stored orientation is
// only valid for the representation that is actually displayed.
//
// GPS Coordinate Handling:
// - rsraw stores GPS as [f32; 3] arrays in DMS format (degrees, minutes, seconds)
// - This is different from kamadak-exif which uses Value::Rational
// - We provide dms_to_decimal() specifically for rsraw's format
// - exif_extract::parse_gps_coordinate() handles kamadak-exif's format

use crate::exif_types::{
    ExifValue, TAG_ARTIST, TAG_DERIVED_TIMESTAMP, TAG_EXPOSURE_TIME, TAG_FNUMBER, TAG_FOCAL_LENGTH,
    TAG_FOCAL_LENGTH_35MM, TAG_GPS_ALTITUDE, TAG_GPS_LATITUDE, TAG_GPS_LONGITUDE, TAG_ISO,
    TAG_LENS_MAKE, TAG_LENS_MODEL, TAG_MAKE, TAG_MODEL, TAG_SOFTWARE,
};
use crate::image_features::ImageFeatures;
use geo::Point;
use rsraw::RawImage;

/// Extract ImageFeatures from an rsraw RawImage.
/// This is used as a fallback when kamadak-exif fails to parse the RAW file.
///
/// TODO(rsraw-orientation): Orientation is NOT available from rsraw.
/// When rsraw exposes it, add: features.insert_tag(TAG_ORIENTATION, ExifValue::Short(orientation));
///
/// Thread Safety: This function only reads from the RawImage, no mutation occurs.
/// The RawImage should already have been opened by the caller.
pub fn build_features_from_raw_image(raw: &RawImage) -> ImageFeatures {
    let info = raw.full_info();
    let mut features = ImageFeatures::new(info.width, info.height);

    // TODO(rsraw-orientation): When rsraw exposes orientation, extract it here:
    // if let Some(orientation) = info.orientation { // or info.flip
    //     features.insert_tag(TAG_ORIENTATION, ExifValue::Short(orientation as u16));
    // }

    // Camera info
    if !info.make.is_empty() {
        features.insert_tag(TAG_MAKE, ExifValue::String(info.make.clone()));
    }
    if !info.model.is_empty() {
        features.insert_tag(TAG_MODEL, ExifValue::String(info.model.clone()));
    }
    if !info.software.is_empty() {
        features.insert_tag(TAG_SOFTWARE, ExifValue::String(info.software.trim().to_string()));
    }
    if !info.artist.is_empty() {
        features.insert_tag(TAG_ARTIST, ExifValue::String(info.artist.clone()));
    }

    // Exposure settings
    if info.iso_speed > 0 {
        features.insert_tag(TAG_ISO, ExifValue::Long(info.iso_speed));
    }
    if info.shutter > 0.0 {
        features.insert_tag(TAG_EXPOSURE_TIME, ExifValue::Float(info.shutter.into()));
    }
    if info.aperture > 0.0 {
        features.insert_tag(TAG_FNUMBER, ExifValue::Float(info.aperture.into()));
    }
    if info.focal_len > 0.0 {
        features.insert_tag(TAG_FOCAL_LENGTH, ExifValue::Float(info.focal_len.into()));
    }

    // Lens info
    if !info.lens_info.lens_make.is_empty() {
        features.insert_tag(TAG_LENS_MAKE, ExifValue::String(info.lens_info.lens_make.clone()));
    }
    if !info.lens_info.lens_name.is_empty() {
        features.insert_tag(
            TAG_LENS_MODEL,
            ExifValue::String(info.lens_info.lens_name.trim().to_string()),
        );
    }
    if info.lens_info.focal_length_in_35mm_format > 0 {
        features.insert_tag(
            TAG_FOCAL_LENGTH_35MM,
            ExifValue::Short(info.lens_info.focal_length_in_35mm_format),
        );
    }

    // GPS info
    let lat = dms_to_decimal(&info.gps.latitude);
    let lon = dms_to_decimal(&info.gps.longitude);
    let has_valid_gps = lat.abs() > 0.0001 || lon.abs() > 0.0001;

    if has_valid_gps {
        features.insert_tag(TAG_GPS_LATITUDE, ExifValue::Float(lat));
        features.insert_tag(TAG_GPS_LONGITUDE, ExifValue::Float(lon));
    }

    // Altitude
    if info.gps.altitude.abs() > 0.0001 {
        features.insert_tag(TAG_GPS_ALTITUDE, ExifValue::Float(info.gps.altitude.into()));
    }

    // Timestamp
    if let Some(ref dt) = info.datetime {
        let timestamp = dt.timestamp();
        features.insert_tag(TAG_DERIVED_TIMESTAMP, ExifValue::Long64(timestamp));
    }

    features
}

/// Convert GPS coordinates from rsraw's [degrees, minutes, seconds] format to decimal degrees.
///
/// rsraw stores GPS as [f32; 3] arrays. This is different from kamadak-exif's
/// Value::Rational format which is handled by exif_extract::parse_gps_coordinate().
///
/// The sign (N/S, E/W) appears to be embedded in the degrees value by rsraw/LibRaw.
#[inline]
fn dms_to_decimal(dms: &[f32; 3]) -> f64 {
    // Check if coordinates are already in decimal format (minutes and seconds are 0)
    // Some cameras/formats store decimal degrees directly
    if dms[1].abs() < 0.0001 && dms[2].abs() < 0.0001 {
        return dms[0] as f64;
    }

    let degrees = dms[0] as f64;
    let minutes = dms[1] as f64;
    let seconds = dms[2] as f64;

    // Handle negative degrees (southern/western hemispheres)
    let sign = if degrees < 0.0 { -1.0 } else { 1.0 };
    let abs_degrees = degrees.abs();

    sign * (abs_degrees + minutes / 60.0 + seconds / 3600.0)
}

/// Public version of dms_to_decimal for use by scanner.rs get_exif_tags_from_rsraw()
#[inline]
pub fn dms_to_decimal_pub(dms: &[f32; 3]) -> f64 {
    dms_to_decimal(dms)
}

/// Get GPS position as geo::Point from an rsraw RawImage.
/// Returns None if GPS coordinates are all zeros or invalid.
///
/// Thread Safety: Only reads from RawImage.
pub fn get_gps_point_from_raw(raw: &RawImage) -> Option<Point<f64>> {
    let info = raw.full_info();
    let lat = dms_to_decimal(&info.gps.latitude);
    let lon = dms_to_decimal(&info.gps.longitude);

    // Check if we have valid GPS coordinates
    if lat.abs() > 0.0001 || lon.abs() > 0.0001 {
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
    raw.full_info().datetime.as_ref().map(|dt| dt.timestamp())
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
fn libraw_flip_to_exif_orientation(flip: i32) -> u8 {
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
pub fn get_orientation_from_raw(raw: &RawImage) -> u8 {
    libraw_flip_to_exif_orientation(raw.as_ref().sizes.flip as i32)
}

/// Merge rsraw EXIF data into existing ImageFeatures.
/// Only fills in missing tags (doesn't overwrite existing ones from kamadak-exif).
///
/// Use this when kamadak-exif partially succeeded but might be missing some tags
/// that rsraw can provide.
///
/// TODO(rsraw-orientation): Orientation is NOT available from rsraw.
/// When rsraw exposes it, add orientation merging here (only if !features.has_tag(TAG_ORIENTATION)).
///
/// Thread Safety: Mutates features, but RawImage is only read.
pub fn merge_raw_info_into_features(features: &mut ImageFeatures, raw: &RawImage) {
    let info = raw.full_info();

    // TODO(rsraw-orientation): When rsraw exposes orientation, merge it here:
    // if !features.has_tag(TAG_ORIENTATION) {
    //     if let Some(orientation) = info.orientation { // or info.flip
    //         features.insert_tag(TAG_ORIENTATION, ExifValue::Short(orientation as u16));
    //     }
    // }

    // Camera info - only if not already present
    if !features.has_tag(TAG_MAKE) && !info.make.is_empty() {
        features.insert_tag(TAG_MAKE, ExifValue::String(info.make.clone()));
    }
    if !features.has_tag(TAG_MODEL) && !info.model.is_empty() {
        features.insert_tag(TAG_MODEL, ExifValue::String(info.model.clone()));
    }
    if !features.has_tag(TAG_SOFTWARE) && !info.software.is_empty() {
        features.insert_tag(TAG_SOFTWARE, ExifValue::String(info.software.trim().to_string()));
    }
    if !features.has_tag(TAG_ARTIST) && !info.artist.is_empty() {
        features.insert_tag(TAG_ARTIST, ExifValue::String(info.artist.clone()));
    }

    // Exposure settings
    if !features.has_tag(TAG_ISO) && info.iso_speed > 0 {
        features.insert_tag(TAG_ISO, ExifValue::Long(info.iso_speed));
    }
    if !features.has_tag(TAG_EXPOSURE_TIME) && info.shutter > 0.0 {
        features.insert_tag(TAG_EXPOSURE_TIME, ExifValue::Float(info.shutter.into()));
    }
    if !features.has_tag(TAG_FNUMBER) && info.aperture > 0.0 {
        features.insert_tag(TAG_FNUMBER, ExifValue::Float(info.aperture.into()));
    }
    if !features.has_tag(TAG_FOCAL_LENGTH) && info.focal_len > 0.0 {
        features.insert_tag(TAG_FOCAL_LENGTH, ExifValue::Float(info.focal_len.into()));
    }

    // Lens info
    if !features.has_tag(TAG_LENS_MAKE) && !info.lens_info.lens_make.is_empty() {
        features.insert_tag(TAG_LENS_MAKE, ExifValue::String(info.lens_info.lens_make.clone()));
    }
    if !features.has_tag(TAG_LENS_MODEL) && !info.lens_info.lens_name.is_empty() {
        features.insert_tag(
            TAG_LENS_MODEL,
            ExifValue::String(info.lens_info.lens_name.trim().to_string()),
        );
    }
    if !features.has_tag(TAG_FOCAL_LENGTH_35MM) && info.lens_info.focal_length_in_35mm_format > 0 {
        features.insert_tag(
            TAG_FOCAL_LENGTH_35MM,
            ExifValue::Short(info.lens_info.focal_length_in_35mm_format),
        );
    }

    // GPS info
    let lat = dms_to_decimal(&info.gps.latitude);
    let lon = dms_to_decimal(&info.gps.longitude);
    let has_valid_gps = lat.abs() > 0.0001 || lon.abs() > 0.0001;

    if has_valid_gps {
        if !features.has_tag(TAG_GPS_LATITUDE) {
            features.insert_tag(TAG_GPS_LATITUDE, ExifValue::Float(lat));
        }
        if !features.has_tag(TAG_GPS_LONGITUDE) {
            features.insert_tag(TAG_GPS_LONGITUDE, ExifValue::Float(lon));
        }
    }

    if !features.has_tag(TAG_GPS_ALTITUDE) && info.gps.altitude.abs() > 0.0001 {
        features.insert_tag(TAG_GPS_ALTITUDE, ExifValue::Float(info.gps.altitude.into()));
    }

    // Timestamp
    if !features.has_tag(TAG_DERIVED_TIMESTAMP)
        && let Some(ref dt) = info.datetime
    {
        features.insert_tag(TAG_DERIVED_TIMESTAMP, ExifValue::Long64(dt.timestamp()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // Negative (Western hemisphere)
        let dms_west = [-122.0, 24.0, 36.0];
        let decimal_west = dms_to_decimal(&dms_west);
        assert!(decimal_west < 0.0);
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
