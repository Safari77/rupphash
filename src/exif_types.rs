// src/exif_types.rs
//
// Flexible EXIF value storage and tag definitions.
// All EXIF tags are stored as big-endian u16 IDs with corresponding ExifValue.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Maximum size for binary EXIF values (exclude thumbnails and large blobs)
pub const MAX_TAG_SIZE: usize = 1024;

/// Generic value type for EXIF data storage.
/// Designed for space efficiency with postcard serialization.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExifValue {
    /// Single byte value
    Byte(u8),
    /// Multiple bytes (for small arrays like version info)
    Bytes(Vec<u8>),
    /// Unsigned 16-bit integer
    Short(u16),
    /// Multiple unsigned 16-bit integers
    Shorts(Vec<u16>),
    /// Unsigned 32-bit integer
    Long(u32),
    /// Signed 32-bit integer
    Signed(i32),
    /// 64-bit signed integer (for timestamps)
    Long64(i64),
    /// Floating point (converted from Rational/SRational for efficiency)
    Float(f32),
    /// Multiple floats (for GPS coordinates, etc.)
    Floats(Vec<f32>),
    /// ASCII or UTF-8 string (trimmed, null bytes removed)
    String(String),
}

impl ExifValue {
    /// Get value as f32 if possible
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            ExifValue::Byte(v) => Some(*v as f32),
            ExifValue::Short(v) => Some(*v as f32),
            ExifValue::Long(v) => Some(*v as f32),
            ExifValue::Signed(v) => Some(*v as f32),
            ExifValue::Long64(v) => Some(*v as f32),
            ExifValue::Float(v) => Some(*v),
            _ => None,
        }
    }

    /// Get value as i64 if possible (for integer comparisons)
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ExifValue::Byte(v) => Some(*v as i64),
            ExifValue::Short(v) => Some(*v as i64),
            ExifValue::Long(v) => Some(*v as i64),
            ExifValue::Signed(v) => Some(*v as i64),
            ExifValue::Long64(v) => Some(*v),
            _ => None,
        }
    }

    /// Get value as string for display/search
    pub fn as_string(&self) -> String {
        match self {
            ExifValue::Byte(v) => v.to_string(),
            ExifValue::Bytes(v) => format!("{:?}", v),
            ExifValue::Short(v) => v.to_string(),
            ExifValue::Shorts(v) => format!("{:?}", v),
            ExifValue::Long(v) => v.to_string(),
            ExifValue::Signed(v) => v.to_string(),
            ExifValue::Long64(v) => v.to_string(),
            ExifValue::Float(v) => format!("{:.4}", v),
            ExifValue::Floats(v) => {
                v.iter().map(|f| format!("{:.6}", f)).collect::<Vec<_>>().join(", ")
            }
            ExifValue::String(s) => s.clone(),
        }
    }

    /// Compute hash for exact matching in search index
    pub fn hash_key(&self) -> u64 {
        let hash = blake3::hash(self.as_string().to_lowercase().as_bytes());
        u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap())
    }
}

// =============================================================================
// Virtual/Derived Tag IDs
// =============================================================================
// Standard EXIF tags use u16 range 0x0000-0xFFFF
// We use high values (0xF000+) for derived/computed values

/// Derived: Country name from GPS coordinates
pub const TAG_DERIVED_COUNTRY: u16 = 0xF001;
/// Derived: Subdivision/state from GPS coordinates  
pub const TAG_DERIVED_SUBDIVISION: u16 = 0xF002;
/// Derived: Sun azimuth angle (degrees from north)
pub const TAG_DERIVED_SUN_AZIMUTH: u16 = 0xF003;
/// Derived: Sun altitude angle (degrees above horizon)
pub const TAG_DERIVED_SUN_ALTITUDE: u16 = 0xF004;
/// Derived: Timezone at GPS location
pub const TAG_DERIVED_TIMEZONE: u16 = 0xF005;
/// Derived: EXIF timestamp as Unix epoch seconds
pub const TAG_DERIVED_TIMESTAMP: u16 = 0xF006;

// =============================================================================
// Common EXIF Tag IDs (for reference and name mapping)
// =============================================================================
// These are standard TIFF/EXIF tag numbers

pub const TAG_IMAGE_WIDTH: u16 = 0x0100;
pub const TAG_IMAGE_HEIGHT: u16 = 0x0101;
pub const TAG_MAKE: u16 = 0x010F;
pub const TAG_MODEL: u16 = 0x0110;
pub const TAG_ORIENTATION: u16 = 0x0112;
pub const TAG_SOFTWARE: u16 = 0x0131;
pub const TAG_DATETIME: u16 = 0x0132;
pub const TAG_ARTIST: u16 = 0x013B;
pub const TAG_COPYRIGHT: u16 = 0x8298;

pub const TAG_EXPOSURE_TIME: u16 = 0x829A;
pub const TAG_FNUMBER: u16 = 0x829D;
pub const TAG_EXPOSURE_PROGRAM: u16 = 0x8822;
pub const TAG_ISO: u16 = 0x8827;
pub const TAG_DATETIME_ORIGINAL: u16 = 0x9003;
pub const TAG_DATETIME_DIGITIZED: u16 = 0x9004;
pub const TAG_EXPOSURE_BIAS: u16 = 0x9204;
pub const TAG_SUBJECT_DISTANCE: u16 = 0x9206;
pub const TAG_METERING_MODE: u16 = 0x9207;
pub const TAG_FLASH: u16 = 0x9209;
pub const TAG_FOCAL_LENGTH: u16 = 0x920A;
pub const TAG_COLOR_SPACE: u16 = 0xA001;
pub const TAG_PIXEL_X_DIMENSION: u16 = 0xA002;
pub const TAG_PIXEL_Y_DIMENSION: u16 = 0xA003;
pub const TAG_FOCAL_LENGTH_35MM: u16 = 0xA405;
pub const TAG_SCENE_TYPE: u16 = 0xA301;
pub const TAG_WHITE_BALANCE: u16 = 0xA403;
pub const TAG_DIGITAL_ZOOM_RATIO: u16 = 0xA404;
pub const TAG_CONTRAST: u16 = 0xA408;
pub const TAG_SATURATION: u16 = 0xA409;
pub const TAG_SHARPNESS: u16 = 0xA40A;

pub const TAG_LENS_MAKE: u16 = 0xA433;
pub const TAG_LENS_MODEL: u16 = 0xA434;

// GPS tags (in GPS IFD, but we store with these IDs)
pub const TAG_GPS_LATITUDE_REF: u16 = 0x0001;
pub const TAG_GPS_LATITUDE: u16 = 0x0002;
pub const TAG_GPS_LONGITUDE_REF: u16 = 0x0003;
pub const TAG_GPS_LONGITUDE: u16 = 0x0004;
pub const TAG_GPS_ALTITUDE_REF: u16 = 0x0005;
pub const TAG_GPS_ALTITUDE: u16 = 0x0006;
pub const TAG_GPS_TIMESTAMP: u16 = 0x0007;
pub const TAG_GPS_DATESTAMP: u16 = 0x001D;

// Tags to exclude (thumbnails and large binary data)
pub const TAG_JPEG_INTERCHANGE_FORMAT: u16 = 0x0201;
pub const TAG_JPEG_INTERCHANGE_FORMAT_LENGTH: u16 = 0x0202;
pub const TAG_STRIP_OFFSETS: u16 = 0x0111;
pub const TAG_STRIP_BYTE_COUNTS: u16 = 0x0117;
pub const TAG_TILE_OFFSETS: u16 = 0x0144;
pub const TAG_TILE_BYTE_COUNTS: u16 = 0x0145;
pub const TAG_MAKER_NOTE: u16 = 0x927C;

/// Check if a tag should be excluded from storage
pub fn is_excluded_tag(tag_id: u16) -> bool {
    matches!(
        tag_id,
        TAG_JPEG_INTERCHANGE_FORMAT
            | TAG_JPEG_INTERCHANGE_FORMAT_LENGTH
            | TAG_STRIP_OFFSETS
            | TAG_STRIP_BYTE_COUNTS
            | TAG_TILE_OFFSETS
            | TAG_TILE_BYTE_COUNTS
            | TAG_MAKER_NOTE
    )
}

/// Map tag ID to human-readable name
pub fn tag_id_to_name(tag_id: u16) -> Option<&'static str> {
    Some(match tag_id {
        TAG_IMAGE_WIDTH => "ImageWidth",
        TAG_IMAGE_HEIGHT => "ImageHeight",
        TAG_MAKE => "Make",
        TAG_MODEL => "Model",
        TAG_ORIENTATION => "Orientation",
        TAG_SOFTWARE => "Software",
        TAG_DATETIME => "DateTime",
        TAG_ARTIST => "Artist",
        TAG_COPYRIGHT => "Copyright",
        TAG_EXPOSURE_TIME => "ExposureTime",
        TAG_FNUMBER => "FNumber",
        TAG_EXPOSURE_PROGRAM => "ExposureProgram",
        TAG_ISO => "ISO",
        TAG_DATETIME_ORIGINAL => "DateTimeOriginal",
        TAG_DATETIME_DIGITIZED => "DateTimeDigitized",
        TAG_EXPOSURE_BIAS => "ExposureBias",
        TAG_METERING_MODE => "MeteringMode",
        TAG_FLASH => "Flash",
        TAG_FOCAL_LENGTH => "FocalLength",
        TAG_COLOR_SPACE => "ColorSpace",
        TAG_PIXEL_X_DIMENSION => "PixelXDimension",
        TAG_PIXEL_Y_DIMENSION => "PixelYDimension",
        TAG_FOCAL_LENGTH_35MM => "FocalLengthIn35mmFilm",
        TAG_SCENE_TYPE => "SceneType",
        TAG_WHITE_BALANCE => "WhiteBalance",
        TAG_DIGITAL_ZOOM_RATIO => "DigitalZoomRatio",
        TAG_CONTRAST => "Contrast",
        TAG_SATURATION => "Saturation",
        TAG_SHARPNESS => "Sharpness",
        TAG_SUBJECT_DISTANCE => "SubjectDistance",
        TAG_LENS_MAKE => "LensMake",
        TAG_LENS_MODEL => "LensModel",
        TAG_GPS_LATITUDE_REF => "GPSLatitudeRef",
        TAG_GPS_LATITUDE => "GPSLatitude",
        TAG_GPS_LONGITUDE_REF => "GPSLongitudeRef",
        TAG_GPS_LONGITUDE => "GPSLongitude",
        TAG_GPS_ALTITUDE_REF => "GPSAltitudeRef",
        TAG_GPS_ALTITUDE => "GPSAltitude",
        TAG_GPS_TIMESTAMP => "GPSTimeStamp",
        TAG_GPS_DATESTAMP => "GPSDateStamp",
        // Derived tags
        TAG_DERIVED_COUNTRY => "Country",
        TAG_DERIVED_SUBDIVISION => "Subdivision",
        TAG_DERIVED_SUN_AZIMUTH => "SunAzimuth",
        TAG_DERIVED_SUN_ALTITUDE => "SunAltitude",
        TAG_DERIVED_TIMEZONE => "Timezone",
        TAG_DERIVED_TIMESTAMP => "Timestamp",
        _ => return None,
    })
}

/// Map human-readable name to tag ID (case-insensitive)
pub fn name_to_tag_id(name: &str) -> Option<u16> {
    Some(match name.to_lowercase().as_str() {
        "imagewidth" | "width" => TAG_IMAGE_WIDTH,
        "imageheight" | "height" => TAG_IMAGE_HEIGHT,
        "make" => TAG_MAKE,
        "model" => TAG_MODEL,
        "orientation" => TAG_ORIENTATION,
        "software" => TAG_SOFTWARE,
        "datetime" => TAG_DATETIME,
        "artist" => TAG_ARTIST,
        "copyright" => TAG_COPYRIGHT,
        "exposuretime" | "exposure" => TAG_EXPOSURE_TIME,
        "fnumber" | "aperture" => TAG_FNUMBER,
        "exposureprogram" => TAG_EXPOSURE_PROGRAM,
        "iso" | "isospeedratings" | "photographicsensitivity" => TAG_ISO,
        "datetimeoriginal" => TAG_DATETIME_ORIGINAL,
        "datetimedigitized" => TAG_DATETIME_DIGITIZED,
        "exposurebias" | "exposurebiasvalue" => TAG_EXPOSURE_BIAS,
        "meteringmode" => TAG_METERING_MODE,
        "flash" => TAG_FLASH,
        "focallength" => TAG_FOCAL_LENGTH,
        "colorspace" => TAG_COLOR_SPACE,
        "pixelxdimension" => TAG_PIXEL_X_DIMENSION,
        "pixelydimension" => TAG_PIXEL_Y_DIMENSION,
        "focallengthin35mmfilm" | "focallength35mm" => TAG_FOCAL_LENGTH_35MM,
        "scenetype" => TAG_SCENE_TYPE,
        "whitebalance" => TAG_WHITE_BALANCE,
        "digitalzoomratio" => TAG_DIGITAL_ZOOM_RATIO,
        "contrast" => TAG_CONTRAST,
        "saturation" => TAG_SATURATION,
        "sharpness" => TAG_SHARPNESS,
        "subjectdistance" => TAG_SUBJECT_DISTANCE,
        "lensmake" => TAG_LENS_MAKE,
        "lensmodel" | "lens" => TAG_LENS_MODEL,
        "gpslatituderef" => TAG_GPS_LATITUDE_REF,
        "gpslatitude" => TAG_GPS_LATITUDE,
        "gpslongituderef" => TAG_GPS_LONGITUDE_REF,
        "gpslongitude" => TAG_GPS_LONGITUDE,
        "gpsaltituderef" => TAG_GPS_ALTITUDE_REF,
        "gpsaltitude" => TAG_GPS_ALTITUDE,
        "gpstimestamp" => TAG_GPS_TIMESTAMP,
        "gpsdatestamp" => TAG_GPS_DATESTAMP,
        // Derived tags
        "country" | "derivedcountry" => TAG_DERIVED_COUNTRY,
        "subdivision" | "state" | "derivedsubdivision" => TAG_DERIVED_SUBDIVISION,
        "sunazimuth" | "derivedsunazimuth" => TAG_DERIVED_SUN_AZIMUTH,
        "sunaltitude" | "derivedsunaltitude" => TAG_DERIVED_SUN_ALTITUDE,
        "timezone" | "tz" | "derivedtimezone" => TAG_DERIVED_TIMEZONE,
        "timestamp" | "derivedtimestamp" => TAG_DERIVED_TIMESTAMP,
        _ => return None,
    })
}

/// Searchable tags with their display info
/// Returns: (tag_id, display_name, description, is_numeric)
pub fn get_searchable_tags() -> Vec<(u16, &'static str, &'static str, bool)> {
    vec![
        // String tags (exact/contains search)
        (TAG_MAKE, "Make", "Camera manufacturer", false),
        (TAG_MODEL, "Model", "Camera model", false),
        (TAG_LENS_MAKE, "LensMake", "Lens manufacturer", false),
        (TAG_LENS_MODEL, "LensModel", "Lens model name", false),
        (TAG_SOFTWARE, "Software", "Software used", false),
        (TAG_ARTIST, "Artist", "Artist/creator", false),
        (TAG_COPYRIGHT, "Copyright", "Copyright information", false),
        (TAG_DERIVED_COUNTRY, "Country", "Country from GPS", false),
        (TAG_DERIVED_SUBDIVISION, "Subdivision", "State/province from GPS", false),
        (TAG_DERIVED_TIMEZONE, "Timezone", "Timezone at GPS location", false),
        // Numeric tags (range search)
        (TAG_ISO, "ISO", "ISO sensitivity", true),
        (TAG_FNUMBER, "FNumber", "Aperture f-number", true),
        (TAG_FOCAL_LENGTH, "FocalLength", "Focal length (mm)", true),
        (TAG_FOCAL_LENGTH_35MM, "FocalLength35mm", "35mm equivalent focal length", true),
        (TAG_EXPOSURE_TIME, "ExposureTime", "Shutter speed (seconds)", true),
        (TAG_EXPOSURE_BIAS, "ExposureBias", "Exposure compensation", true),
        (TAG_ORIENTATION, "Orientation", "Image orientation (1-8)", true),
        (TAG_GPS_ALTITUDE, "GPSAltitude", "GPS altitude (meters)", true),
        (TAG_DERIVED_SUN_AZIMUTH, "SunAzimuth", "Sun azimuth angle (degrees)", true),
        (TAG_DERIVED_SUN_ALTITUDE, "SunAltitude", "Sun altitude angle (degrees)", true),
        (TAG_DERIVED_TIMESTAMP, "Timestamp", "EXIF timestamp (Unix epoch)", true),
    ]
}

/// Check if a tag is a derived/computed value
pub fn is_derived_tag(tag_id: u16) -> bool {
    tag_id >= 0xF000
}

/// Get all tag IDs that should be indexed for search
pub fn get_indexed_tag_ids() -> Vec<u16> {
    get_searchable_tags().iter().map(|(id, _, _, _)| *id).collect()
}
