// src/image_features.rs
//
// Image features storage for database.
// Uses BTreeMap for flexible EXIF tag storage with postcard serialization.

use crate::exif_types::{
    ExifValue, TAG_DERIVED_TIMESTAMP, TAG_GPS_LATITUDE, TAG_GPS_LONGITUDE, TAG_ORIENTATION,
};
use geo::Point;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Image features with flexible EXIF tag storage
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ImageFeatures {
    /// Image width in pixels
    pub width: u32,
    /// Image height in pixels
    pub height: u32,
    /// All EXIF tags stored as tag_id -> value
    /// Key: u16 tag ID (standard TIFF/EXIF or derived 0xF000+)
    /// Value: ExifValue enum
    pub tags: BTreeMap<u16, ExifValue>,
}

impl ImageFeatures {
    pub fn new(width: u32, height: u32) -> Self {
        Self { width, height, tags: BTreeMap::new() }
    }

    /// Serialize to bytes using postcard
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_stdvec(self)
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }

    /// Get orientation from tags (defaults to 1 if not present)
    pub fn orientation(&self) -> u8 {
        self.tags
            .get(&TAG_ORIENTATION)
            .and_then(|v| match v {
                ExifValue::Short(n) => Some(*n as u8),
                ExifValue::Byte(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(1)
    }

    /// Get GPS position from tags if both lat/lon are present
    pub fn gps_pos(&self) -> Option<Point<f64>> {
        let lat = self.tags.get(&TAG_GPS_LATITUDE).and_then(|v| match v {
            ExifValue::Float(f) => Some(*f),
            ExifValue::Floats(f) if !f.is_empty() => Some(f[0]),
            _ => None,
        })?;

        let lon = self.tags.get(&TAG_GPS_LONGITUDE).and_then(|v| match v {
            ExifValue::Float(f) => Some(*f),
            ExifValue::Floats(f) if !f.is_empty() => Some(f[0]),
            _ => None,
        })?;

        Some(Point::new(lon, lat))
    }

    /// Get EXIF timestamp as Unix epoch seconds
    pub fn exif_timestamp(&self) -> Option<i64> {
        self.tags.get(&TAG_DERIVED_TIMESTAMP).and_then(|v| match v {
            ExifValue::Long64(ts) => Some(*ts),
            ExifValue::Signed(ts) => Some(*ts as i64),
            ExifValue::Long(ts) => Some(*ts as i64),
            _ => None,
        })
    }

    /// Get a tag value by ID
    pub fn get_tag(&self, tag_id: u16) -> Option<&ExifValue> {
        self.tags.get(&tag_id)
    }

    /// Get a tag value as string for display
    pub fn get_tag_string(&self, tag_id: u16) -> Option<String> {
        self.tags.get(&tag_id).map(|v| v.as_string())
    }

    /// Insert a tag value
    pub fn insert_tag(&mut self, tag_id: u16, value: ExifValue) {
        self.tags.insert(tag_id, value);
    }

    /// Get number of stored tags
    pub fn tag_count(&self) -> usize {
        self.tags.len()
    }

    /// Get resolution as tuple if both dimensions are valid
    pub fn resolution(&self) -> Option<(u32, u32)> {
        if self.width > 0 && self.height > 0 { Some((self.width, self.height)) } else { None }
    }

    /// Check if features contain a specific tag
    pub fn has_tag(&self, tag_id: u16) -> bool {
        self.tags.contains_key(&tag_id)
    }

    /// Get all tag IDs present in this features set
    pub fn tag_ids(&self) -> Vec<u16> {
        self.tags.keys().copied().collect()
    }

    /// Merge tags from another ImageFeatures (overwrites existing)
    pub fn merge(&mut self, other: &ImageFeatures) {
        for (k, v) in &other.tags {
            self.tags.insert(*k, v.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let mut features = ImageFeatures::new(1920, 1080);
        features.insert_tag(TAG_ORIENTATION, ExifValue::Short(6));
        features.insert_tag(TAG_GPS_LATITUDE, ExifValue::Float(48.8566));
        features.insert_tag(TAG_GPS_LONGITUDE, ExifValue::Float(2.3522));

        let bytes = features.to_bytes().unwrap();
        let restored = ImageFeatures::from_bytes(&bytes).unwrap();

        assert_eq!(restored.width, 1920);
        assert_eq!(restored.height, 1080);
        assert_eq!(restored.orientation(), 6);
        assert!(restored.gps_pos().is_some());
    }

    #[test]
    fn test_defaults() {
        let features = ImageFeatures::default();
        assert_eq!(features.orientation(), 1);
        assert!(features.gps_pos().is_none());
        assert!(features.exif_timestamp().is_none());
    }
}
