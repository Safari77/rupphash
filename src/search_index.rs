// src/search_index.rs
//
// In-memory search index using RoaringBitmap for O(1) tag-based queries.
// Built on application startup by iterating the feature database.

use crate::exif_types::{ExifValue, is_derived_tag, tag_id_to_name};
use crate::image_features::ImageFeatures;
use roaring::RoaringBitmap;
use std::collections::{HashMap, HashSet};

// Maximum precision in search results for numbers
const SEARCH_VALUE_EPSILON: f64 = 0.00001;

/// Extract a numeric value from a string that may contain units or prefixes.
/// Handles:
/// - "f/2.8" -> 2.8 (Special handling for f-stops: returns denominator)
/// - "1/250" -> 0.004 (Fractions)
/// - "1/250s" -> 0.004 (Fractions with units)
/// - "ISO 100" -> 100
/// - "24mm" -> 24
pub fn extract_number_from_string(s: &str) -> Option<f64> {
    let s = s.trim();

    // 1. Remove "s" suffix (common in exposure time: "1/320s")
    // doing this early ensures it doesn't break fraction parsing
    let s = s.strip_suffix('s').unwrap_or(s).trim();

    // 2. Handle "f/" prefix (e.g. "f/2.8" or "F/2.8")
    // For aperture, we want the number itself (2.8), NOT the fraction (f divided by 2.8)
    if s.to_lowercase().starts_with("f/") {
        if let Ok(val) = s[2..].trim().parse::<f64>() {
            return Some(val);
        }
    }

    // 3. Handle fractions (e.g. "1/320" or "1/37.738...")
    if let Some(slash_pos) = s.find('/') {
        let before = s[..slash_pos].trim();
        let after = &s[slash_pos + 1..].trim();

        if let (Ok(num), Ok(denom)) = (before.parse::<f64>(), after.parse::<f64>()) {
            if denom != 0.0 {
                return Some(num / denom);
            }
        }
    }

    // 4. Handle "ISO" or "mm" style strings (remove non-numeric prefix/suffix)
    // Extract first continuous sequence of digits/dots
    let mut num_str = String::new();
    let mut found_digit = false;

    for c in s.chars() {
        if c.is_ascii_digit() || c == '.' || c == '-' {
            num_str.push(c);
            found_digit = true;
        } else if found_digit {
            // Stop at first non-numeric char after finding numbers (e.g. "24mm" -> stop at m)
            break;
        }
    }

    if !num_str.is_empty() {
        if let Ok(val) = num_str.parse::<f64>() {
            return Some(val);
        }
    }

    // 5. Last resort: direct parse
    s.parse::<f64>().ok()
}

/// Search operator for queries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchOp {
    /// Exact string match (case-insensitive)
    Equals,
    /// String contains (case-insensitive)
    Contains,
    /// Numeric less than
    LessThan,
    /// Numeric less than or equal
    LessOrEqual,
    /// Numeric greater than
    GreaterThan,
    /// Numeric greater than or equal
    GreaterOrEqual,
    /// Numeric range (inclusive)
    Between,
    /// Regex match
    Regex,
}

/// A single search criterion
#[derive(Debug, Clone)]
pub struct SearchCriterion {
    /// Tag ID to search (u16)
    pub tag_id: u16,
    /// Search operator
    pub op: SearchOp,
    /// Value to compare (for single-value ops)
    pub value: String,
    /// Second value for range queries
    pub value2: Option<String>,
    /// Whether this criterion is enabled
    pub enabled: bool,
}

impl SearchCriterion {
    pub fn new(tag_id: u16, op: SearchOp, value: String) -> Self {
        Self { tag_id, op, value, value2: None, enabled: true }
    }

    pub fn with_range(tag_id: u16, min: String, max: String) -> Self {
        Self { tag_id, op: SearchOp::Between, value: min, value2: Some(max), enabled: true }
    }
}

/// In-memory search index for fast EXIF-based queries.
/// Uses RoaringBitmap for memory-efficient set operations.
pub struct SearchIndex {
    /// String interning: Map value hash -> canonical string
    string_table: HashMap<u64, String>,

    /// Exact match index: tag_id -> value_hash -> bitmap of file indices
    exact_index: HashMap<u16, HashMap<u64, RoaringBitmap>>,

    /// Numeric index: tag_id -> sorted list of (value, file_index)
    numeric_index: HashMap<u16, Vec<(f64, u32)>>,

    /// Number of indexed files
    file_count: u32,

    /// Mapping from unique_file_id (u128) to search index (u32)
    id_to_index: HashMap<u128, u32>,

    /// Reverse mapping from search index to unique_file_id
    index_to_id: Vec<u128>,

    /// Track which tags have been indexed
    indexed_tags: HashSet<u16>,

    /// Flag indicating if numeric indices are sorted
    is_finalized: bool,
}

impl Default for SearchIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl SearchIndex {
    pub fn new() -> Self {
        Self {
            string_table: HashMap::new(),
            exact_index: HashMap::new(),
            numeric_index: HashMap::new(),
            file_count: 0,
            id_to_index: HashMap::new(),
            index_to_id: Vec::new(),
            indexed_tags: HashSet::new(),
            is_finalized: false,
        }
    }

    /// Clear the index
    pub fn clear(&mut self) {
        self.string_table.clear();
        self.exact_index.clear();
        self.numeric_index.clear();
        self.file_count = 0;
        self.id_to_index.clear();
        self.index_to_id.clear();
        self.indexed_tags.clear();
        self.is_finalized = false;
    }

    /// Get the number of indexed files
    pub fn len(&self) -> usize {
        self.file_count as usize
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.file_count == 0
    }

    /// Get or create an index for a unique_file_id
    fn get_or_create_index(&mut self, unique_file_id: u128) -> u32 {
        if let Some(&idx) = self.id_to_index.get(&unique_file_id) {
            idx
        } else {
            let idx = self.file_count;
            self.id_to_index.insert(unique_file_id, idx);
            self.index_to_id.push(unique_file_id);
            self.file_count += 1;
            self.is_finalized = false;
            idx
        }
    }

    /// Convert search index back to unique_file_id
    pub fn index_to_file_id(&self, index: u32) -> Option<u128> {
        self.index_to_id.get(index as usize).copied()
    }

    /// Get file index from unique_file_id
    pub fn file_id_to_index(&self, unique_file_id: u128) -> Option<u32> {
        self.id_to_index.get(&unique_file_id).copied()
    }

    /// Insert a file's features into the index
    pub fn insert(&mut self, unique_file_id: u128, features: &ImageFeatures) {
        let file_idx = self.get_or_create_index(unique_file_id);
        self.is_finalized = false;

        for (tag_id, value) in &features.tags {
            self.indexed_tags.insert(*tag_id);

            match value {
                ExifValue::String(s) => {
                    // Index for exact/contains match (case-insensitive)
                    let normalized = s.to_lowercase();
                    let hash = Self::hash_string(&normalized);

                    // Store in string table for later retrieval
                    self.string_table.entry(hash).or_insert_with(|| normalized.clone());

                    // Add to exact index
                    self.exact_index
                        .entry(*tag_id)
                        .or_default()
                        .entry(hash)
                        .or_default()
                        .insert(file_idx);

                    // Also try to extract a numeric value for range queries
                    // This handles cases like "ISO 100", "24mm", "f/2.8", "1/250s", etc.
                    if let Some(num) = extract_number_from_string(s) {
                        self.insert_numeric(*tag_id, num, file_idx);
                    }
                }

                ExifValue::Short(v) => {
                    self.insert_numeric(*tag_id, *v as f64, file_idx);
                    // Also index as string for contains search
                    self.insert_string(*tag_id, &v.to_string(), file_idx);
                }

                ExifValue::Long(v) => {
                    self.insert_numeric(*tag_id, *v as f64, file_idx);
                    self.insert_string(*tag_id, &v.to_string(), file_idx);
                }

                ExifValue::Signed(v) => {
                    self.insert_numeric(*tag_id, *v as f64, file_idx);
                    self.insert_string(*tag_id, &v.to_string(), file_idx);
                }

                ExifValue::Long64(v) => {
                    self.insert_numeric(*tag_id, *v as f64, file_idx);
                    self.insert_string(*tag_id, &v.to_string(), file_idx);
                }

                ExifValue::Float(v) => {
                    self.insert_numeric(*tag_id, (*v).into(), file_idx);
                    // Format float for string search
                    self.insert_string(*tag_id, &format!("{:.2}", v), file_idx);
                }

                ExifValue::Byte(v) => {
                    self.insert_numeric(*tag_id, *v as f64, file_idx);
                    self.insert_string(*tag_id, &v.to_string(), file_idx);
                }

                _ => {
                    // Skip arrays for now
                }
            }
        }
    }

    /// Insert a string value into the exact index
    fn insert_string(&mut self, tag_id: u16, value: &str, file_idx: u32) {
        let normalized = value.to_lowercase();
        let hash = Self::hash_string(&normalized);
        self.string_table.entry(hash).or_insert_with(|| normalized.clone());
        self.exact_index.entry(tag_id).or_default().entry(hash).or_default().insert(file_idx);
    }

    /// Insert a numeric value for range queries
    fn insert_numeric(&mut self, tag_id: u16, value: f64, file_idx: u32) {
        let list = self.numeric_index.entry(tag_id).or_default();
        list.push((value, file_idx));
    }

    /// Hash a string for the exact index
    fn hash_string(s: &str) -> u64 {
        let hash = blake3::hash(s.as_bytes());
        u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap())
    }

    /// Finalize the index after bulk insertion.
    /// Sorts numeric indices for efficient range queries.
    pub fn finalize(&mut self) {
        if self.is_finalized {
            return;
        }
        for list in self.numeric_index.values_mut() {
            list.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        }
        self.is_finalized = true;
    }

    /// Get all indexed tag IDs
    pub fn get_indexed_tags(&self) -> Vec<u16> {
        self.indexed_tags.iter().copied().collect()
    }

    /// Debug: dump index statistics
    #[allow(dead_code)]
    pub fn debug_dump(&self) {
        eprintln!("=== SearchIndex Debug Dump ===");
        eprintln!("Total files indexed: {}", self.file_count);
        eprintln!("Total unique tags: {}", self.indexed_tags.len());
        eprintln!("String table entries: {}", self.string_table.len());

        eprintln!("\nIndexed tags:");
        for tag_id in &self.indexed_tags {
            let name = tag_id_to_name(*tag_id).unwrap_or("Unknown");
            let exact_count = self.exact_index.get(tag_id).map(|m| m.len()).unwrap_or(0);
            let numeric_count = self.numeric_index.get(tag_id).map(|v| v.len()).unwrap_or(0);
            eprintln!(
                "  0x{:04X} {}: {} string values, {} numeric entries",
                tag_id, name, exact_count, numeric_count
            );
        }

        // Show sample values for key derived tags
        for tag_id in &[
            crate::exif_types::TAG_DERIVED_COUNTRY,
            crate::exif_types::TAG_DERIVED_SUN_AZIMUTH,
            crate::exif_types::TAG_DERIVED_SUN_ALTITUDE,
        ] {
            if self.indexed_tags.contains(tag_id) {
                let name = tag_id_to_name(*tag_id).unwrap_or("Unknown");
                let values = self.get_tag_values(*tag_id);
                eprintln!("\n  {} sample values: {:?}", name, &values[..values.len().min(5)]);

                if let Some(list) = self.numeric_index.get(tag_id) {
                    let sample: Vec<_> = list.iter().take(5).map(|(v, _)| *v).collect();
                    eprintln!("    numeric samples: {:?}", sample);
                }
            }
        }
        eprintln!("=== End Debug Dump ===\n");
    }

    /// Search for exact string match (case-insensitive)
    pub fn search_exact(&self, tag_id: u16, value: &str) -> RoaringBitmap {
        let normalized = value.to_lowercase();
        let hash = Self::hash_string(&normalized);

        self.exact_index
            .get(&tag_id)
            .and_then(|tag_map| tag_map.get(&hash))
            .cloned()
            .unwrap_or_default()
    }

    /// Search for string contains (case-insensitive)
    pub fn search_contains(&self, tag_id: u16, substring: &str) -> RoaringBitmap {
        let normalized = substring.to_lowercase();
        let mut result = RoaringBitmap::new();

        if let Some(tag_map) = self.exact_index.get(&tag_id) {
            for (hash, bitmap) in tag_map {
                if let Some(stored) = self.string_table.get(hash) {
                    if stored.contains(&normalized) {
                        result |= bitmap;
                    }
                }
            }
        }

        result
    }

    /// Search with regex
    pub fn search_regex(&self, tag_id: u16, pattern: &str) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();

        let Ok(re) = regex::RegexBuilder::new(pattern).case_insensitive(true).build() else {
            return result;
        };

        if let Some(tag_map) = self.exact_index.get(&tag_id) {
            for (hash, bitmap) in tag_map {
                if let Some(stored) = self.string_table.get(hash) {
                    if re.is_match(stored) {
                        result |= bitmap;
                    }
                }
            }
        }

        result
    }

    pub fn search_numeric(&self, tag_id: u16, op: SearchOp, value: f64) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        let epsilon = SEARCH_VALUE_EPSILON;

        if let Some(list) = self.numeric_index.get(&tag_id) {
            match op {
                SearchOp::Equals => {
                    // Find start of potential equality range
                    let start = list.partition_point(|&(v, _)| v < value - epsilon);
                    for &(v, idx) in &list[start..] {
                        if v > value + epsilon {
                            break;
                        }
                        result.insert(idx);
                    }
                }
                SearchOp::LessThan => {
                    let end = list.partition_point(|&(v, _)| v < value);
                    for &(_, idx) in &list[..end] {
                        result.insert(idx);
                    }
                }
                SearchOp::LessOrEqual => {
                    let end = list.partition_point(|&(v, _)| v <= value + epsilon);
                    for &(_, idx) in &list[..end] {
                        result.insert(idx);
                    }
                }
                SearchOp::GreaterThan => {
                    let start = list.partition_point(|&(v, _)| v <= value);
                    for &(_, idx) in &list[start..] {
                        result.insert(idx);
                    }
                }
                SearchOp::GreaterOrEqual => {
                    let start = list.partition_point(|&(v, _)| v >= value - epsilon);
                    for &(_, idx) in &list[start..] {
                        result.insert(idx);
                    }
                }
                _ => {}
            }
        }
        result
    }

    /// Search for numeric range (inclusive)
    pub fn search_range(&self, tag_id: u16, min: f64, max: f64) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        let epsilon = SEARCH_VALUE_EPSILON;

        if let Some(list) = self.numeric_index.get(&tag_id) {
            let start = list.partition_point(|&(v, _)| v < min - epsilon);
            for &(v, idx) in &list[start..] {
                if v > max + epsilon {
                    break;
                }
                result.insert(idx);
            }
        }
        result
    }

    /// Execute a single search criterion
    pub fn search_criterion(&self, criterion: &SearchCriterion) -> RoaringBitmap {
        if !criterion.enabled {
            // Return all files if criterion is disabled
            let mut all = RoaringBitmap::new();
            for i in 0..self.file_count {
                all.insert(i);
            }
            return all;
        }

        match criterion.op {
            SearchOp::Equals => {
                if let Ok(v) = criterion.value.parse::<f64>() {
                    self.search_numeric(criterion.tag_id, SearchOp::Equals, v)
                } else {
                    self.search_exact(criterion.tag_id, &criterion.value)
                }
            }
            SearchOp::Contains => self.search_contains(criterion.tag_id, &criterion.value),
            SearchOp::Regex => self.search_regex(criterion.tag_id, &criterion.value),
            SearchOp::LessThan
            | SearchOp::LessOrEqual
            | SearchOp::GreaterThan
            | SearchOp::GreaterOrEqual => {
                if let Ok(v) = criterion.value.parse::<f64>() {
                    self.search_numeric(criterion.tag_id, criterion.op, v)
                } else {
                    RoaringBitmap::new()
                }
            }
            SearchOp::Between => {
                if let (Ok(min), Some(Ok(max))) =
                    (criterion.value.parse::<f64>(), criterion.value2.as_ref().map(|v| v.parse()))
                {
                    self.search_range(criterion.tag_id, min, max)
                } else {
                    RoaringBitmap::new()
                }
            }
        }
    }

    /// Execute multiple criteria with AND logic
    pub fn search_and(&self, criteria: &[SearchCriterion]) -> RoaringBitmap {
        let enabled: Vec<_> = criteria.iter().filter(|c| c.enabled).collect();

        if enabled.is_empty() {
            return RoaringBitmap::new();
        }

        let mut result = self.search_criterion(enabled[0]);
        for criterion in &enabled[1..] {
            result &= self.search_criterion(criterion);
        }
        result
    }

    /// Execute multiple criteria with OR logic
    pub fn search_or(&self, criteria: &[SearchCriterion]) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        for criterion in criteria {
            if criterion.enabled {
                result |= self.search_criterion(criterion);
            }
        }
        result
    }

    /// Get all unique values for a string tag (for autocomplete)
    pub fn get_tag_values(&self, tag_id: u16) -> Vec<String> {
        self.exact_index
            .get(&tag_id)
            .map(|tag_map| {
                let mut values: Vec<_> = tag_map
                    .keys()
                    .filter_map(|hash| self.string_table.get(hash).cloned())
                    .collect();
                values.sort();
                values
            })
            .unwrap_or_default()
    }

    /// Get statistics about the index
    pub fn stats(&self) -> IndexStats {
        let total_exact_entries: usize = self
            .exact_index
            .values()
            .map(|m| m.values().map(|b| b.len() as usize).sum::<usize>())
            .sum();
        let total_numeric_entries: usize = self.numeric_index.values().map(|v| v.len()).sum();

        IndexStats {
            file_count: self.file_count as usize,
            tag_count: self.indexed_tags.len(),
            string_table_size: self.string_table.len(),
            exact_entries: total_exact_entries,
            numeric_entries: total_numeric_entries,
        }
    }

    /// Get a bitmap of all file indices
    pub fn all_files(&self) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        for i in 0..self.file_count {
            result.insert(i);
        }
        result
    }
}

/// Statistics about the search index
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub file_count: usize,
    pub tag_count: usize,
    pub string_table_size: usize,
    pub exact_entries: usize,
    pub numeric_entries: usize,
}

impl std::fmt::Display for IndexStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SearchIndex: {} files, {} tags, {} string values, {} exact entries, {} numeric entries",
            self.file_count,
            self.tag_count,
            self.string_table_size,
            self.exact_entries,
            self.numeric_entries
        )
    }
}

/// Parse a search query string into criteria.
/// Supports multiple criteria separated by spaces or semicolons.
/// Format: "tag:operator:value" or "tag:value" (implies equals/contains)
///
/// Examples:
///   "Make:Canon" -> Make contains "Canon"
///   "ISO:>:800" -> ISO greater than 800
///   "FocalLength:24-70" -> FocalLength between 24 and 70
///   "SunAzimuth:170-190" -> Sun azimuth range
///   "SunAltitude:-3-3" -> Sun altitude range (golden hour)
///   "Country:Florida" -> Derived country
///
/// Returns Result with Vec of criteria (for AND logic) or error message
pub fn parse_search_query(query: &str) -> Result<Vec<SearchCriterion>, String> {
    let mut criteria = Vec::new();

    // Split on whitespace or semicolons for multiple criteria
    for part in query.split(|c: char| c.is_whitespace() || c == ';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        match parse_single_criterion(part) {
            Ok(c) => criteria.push(c),
            Err(e) => return Err(e),
        }
    }

    if criteria.is_empty() {
        return Err("No valid search criteria provided".to_string());
    }

    Ok(criteria)
}

/// Parse a single search criterion
fn parse_single_criterion(query: &str) -> Result<SearchCriterion, String> {
    let parts: Vec<&str> = query.splitn(3, ':').collect();

    if parts.is_empty() || parts[0].is_empty() {
        return Err("Empty tag name".to_string());
    }

    let tag_name = parts[0];

    // Try to resolve tag ID, with support for aliases
    let tag_id = resolve_tag_name(tag_name)
        .ok_or_else(|| format!("Unknown tag: '{}'. Use Make, Model, ISO, FocalLength, Country, SunAzimuth, SunAltitude, etc.", tag_name))?;

    if parts.len() == 1 {
        return Err(format!(
            "Missing value for tag '{}'. Use format: {}:value",
            tag_name, tag_name
        ));
    }

    if parts.len() == 2 {
        let value = parts[1];
        // Use extract_number_from_string to handle ranges like "f/2.8-f/11"
        if let Some(range) = parse_range_value(value) {
            return Ok(SearchCriterion::with_range(tag_id, range.0, range.1));
        }

        // Try to parse as number first using the robust extractor.
        // This handles "f/2.8", "1/125", "ISO 100" -> converts to numeric Equals search.
        if let Some(num) = extract_number_from_string(value) {
            return Ok(SearchCriterion::new(tag_id, SearchOp::Equals, num.to_string()));
        }

        // Fallback to standard parse or contains
        let op = if value.parse::<f32>().is_ok() { SearchOp::Equals } else { SearchOp::Contains };
        return Ok(SearchCriterion::new(tag_id, op, value.to_string()));
    }

    // parts.len() == 3: "tag:op:value"
    let op_str = parts[1];
    let value = parts[2];

    let op = match op_str {
        "=" | "==" | "eq" => SearchOp::Equals,
        "~" | "contains" | "like" => SearchOp::Contains,
        "re" | "regex" => SearchOp::Regex,
        "<" | "lt" => SearchOp::LessThan,
        "<=" | "le" | "lte" => SearchOp::LessOrEqual,
        ">" | "gt" => SearchOp::GreaterThan,
        ">=" | "ge" | "gte" => SearchOp::GreaterOrEqual,
        _ => return Err(format!("Unknown operator: '{}'. Use =, ~, <, >, <=, >=, regex", op_str)),
    };

    // Clean the value for numeric operators if possible
    let final_value = if matches!(
        op,
        SearchOp::LessThan
            | SearchOp::LessOrEqual
            | SearchOp::GreaterThan
            | SearchOp::GreaterOrEqual
            | SearchOp::Equals
    ) {
        if let Some(num) = extract_number_from_string(value) {
            num.to_string()
        } else {
            value.to_string()
        }
    } else {
        value.to_string()
    };

    Ok(SearchCriterion::new(tag_id, op, final_value))
}

/// Parse range value like "24-70" or "1600-" (open ended)
pub fn parse_range_value(value: &str) -> Option<(String, String)> {
    let chars: Vec<char> = value.chars().collect();

    // Start at 1 to avoid splitting negative numbers at the sign (e.g. "-5")
    for i in 1..chars.len() {
        if chars[i] == '-' {
            let min_str = &value[..i];
            let max_str = &value[i + 1..];

            // Check 1: Parse Min
            let min_val = extract_number_from_string(min_str);

            // Check 2: Parse Max
            // Handle empty max string as Infinity for open-ended ranges like "1600-"
            let max_val = if max_str.trim().is_empty() {
                Some(f64::MAX)
            } else {
                extract_number_from_string(max_str)
            };

            if let (Some(min), Some(max)) = (min_val, max_val) {
                // Return as strings; the SearchCriterion will parse them back to f32 during search
                return Some((min.to_string(), max.to_string()));
            }
        }
    }
    None
}

/// Resolve tag name to tag ID, supporting common aliases
fn resolve_tag_name(name: &str) -> Option<u16> {
    use crate::exif_types::*;

    // First try the standard name_to_tag_id
    if let Some(id) = name_to_tag_id(name) {
        return Some(id);
    }

    // Handle common aliases (case-insensitive)
    let lower = name.to_lowercase();
    match lower.as_str() {
        // Sun position aliases
        "sunazimuth" | "sun_azimuth" | "sun_az" | "azimuth" | "az" => Some(TAG_DERIVED_SUN_AZIMUTH),
        "sunaltitude" | "sun_altitude" | "sun_alt" | "altitude" | "alt" | "elevation" => {
            Some(TAG_DERIVED_SUN_ALTITUDE)
        }
        "sunposition" | "sun_position" | "sun" => Some(TAG_DERIVED_SUN_AZIMUTH), // Default to azimuth

        // Location aliases
        "country" | "derivedcountry" => Some(TAG_DERIVED_COUNTRY),
        "subdivision" | "state" | "province" | "region" => Some(TAG_DERIVED_SUBDIVISION),
        "timezone" | "tz" => Some(TAG_DERIVED_TIMEZONE),

        // Common EXIF aliases
        "iso" | "isospeed" => Some(TAG_ISO),
        "aperture" | "fnumber" | "f" => Some(TAG_FNUMBER),
        "exposure" | "exposuretime" | "shutter" => Some(TAG_EXPOSURE_TIME),
        "focal" | "focallength" | "fl" => Some(TAG_FOCAL_LENGTH),
        "focal35" | "focallength35" | "focallength35mm" | "fl35" => Some(TAG_FOCAL_LENGTH_35MM),
        "make" | "manufacturer" | "brand" => Some(TAG_MAKE),
        "model" | "camera" => Some(TAG_MODEL),
        "lens" | "lensmodel" => Some(TAG_LENS_MODEL),
        "lensmake" => Some(TAG_LENS_MAKE),
        "date" | "datetime" | "datetimeoriginal" => Some(TAG_DATETIME_ORIGINAL),
        "software" | "app" => Some(TAG_SOFTWARE),
        "artist" | "photographer" => Some(TAG_ARTIST),
        "copyright" => Some(TAG_COPYRIGHT),
        "orientation" => Some(TAG_ORIENTATION),
        "width" | "imagewidth" => Some(TAG_IMAGE_WIDTH),
        "height" | "imageheight" => Some(TAG_IMAGE_HEIGHT),
        "flash" => Some(TAG_FLASH),
        "whitebalance" | "wb" => Some(TAG_WHITE_BALANCE),
        "metering" | "meteringmode" => Some(TAG_METERING_MODE),
        "exposureprogram" | "program" => Some(TAG_EXPOSURE_PROGRAM),
        "exposurebias" | "ev" | "bias" => Some(TAG_EXPOSURE_BIAS),
        "gps" | "gpslat" | "gpslatitude" | "lat" | "latitude" => Some(TAG_GPS_LATITUDE),
        "gpslon" | "gpslongitude" | "lon" | "longitude" => Some(TAG_GPS_LONGITUDE),
        "gpsalt" | "gpsaltitude" => Some(TAG_GPS_ALTITUDE),

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exif_types::TAG_MAKE;

    #[test]
    fn test_exact_search() {
        let mut index = SearchIndex::new();

        let mut features1 = ImageFeatures::new(1920, 1080);
        features1.insert_tag(TAG_MAKE, ExifValue::String("Canon".to_string()));

        let mut features2 = ImageFeatures::new(1920, 1080);
        features2.insert_tag(TAG_MAKE, ExifValue::String("Nikon".to_string()));

        index.insert(1, &features1);
        index.insert(2, &features2);
        index.finalize();

        let result = index.search_exact(TAG_MAKE, "canon");
        assert_eq!(result.len(), 1);
        assert!(result.contains(0));

        let result = index.search_exact(TAG_MAKE, "nikon");
        assert_eq!(result.len(), 1);
        assert!(result.contains(1));
    }

    #[test]
    fn test_range_search() {
        use crate::exif_types::TAG_ISO;

        let mut index = SearchIndex::new();

        for i in 0..10u128 {
            let mut features = ImageFeatures::new(1920, 1080);
            features.insert_tag(TAG_ISO, ExifValue::Short(((i + 1) * 100) as u16));
            index.insert(i, &features);
        }
        index.finalize();

        // ISO > 500
        let result = index.search_numeric(TAG_ISO, SearchOp::GreaterThan, 500.0);
        assert_eq!(result.len(), 5); // 600, 700, 800, 900, 1000

        // ISO between 300-700
        let result = index.search_range(TAG_ISO, 300.0, 700.0);
        assert_eq!(result.len(), 5); // 300, 400, 500, 600, 700
    }

    #[test]
    fn test_parse_query() {
        let criteria = parse_search_query("Make:Canon").unwrap();
        assert_eq!(criteria.len(), 1);
        let q = &criteria[0];
        assert_eq!(q.tag_id, TAG_MAKE);
        assert_eq!(q.op, SearchOp::Contains);
        assert_eq!(q.value, "Canon");

        let criteria = parse_search_query("ISO:>:800").unwrap();
        let q = &criteria[0];
        assert_eq!(q.tag_id, crate::exif_types::TAG_ISO);
        assert_eq!(q.op, SearchOp::GreaterThan);

        let criteria = parse_search_query("FocalLength:24-70").unwrap();
        let q = &criteria[0];
        assert_eq!(q.op, SearchOp::Between);
        assert_eq!(q.value, "24");
        assert_eq!(q.value2, Some("70".to_string()));

        // Test negative range for sun altitude
        let criteria = parse_search_query("SunAltitude:-3-3").unwrap();
        let q = &criteria[0];
        assert_eq!(q.op, SearchOp::Between);
        assert_eq!(q.value, "-3");
        assert_eq!(q.value2, Some("3".to_string()));

        // Test aliases
        let criteria = parse_search_query("sun_az:170-190").unwrap();
        let q = &criteria[0];
        assert_eq!(q.tag_id, crate::exif_types::TAG_DERIVED_SUN_AZIMUTH);

        // Test multiple criteria
        let criteria = parse_search_query("Make:Canon ISO:>:400").unwrap();
        assert_eq!(criteria.len(), 2);
    }

    #[test]
    fn test_extract_number_from_string() {
        // ISO values
        assert_eq!(extract_number_from_string("ISO 100"), Some(100.0));
        assert_eq!(extract_number_from_string("100"), Some(100.0));
        assert_eq!(extract_number_from_string("ISO100"), Some(100.0));

        // Focal length
        assert_eq!(extract_number_from_string("24mm"), Some(24.0));
        assert_eq!(extract_number_from_string("24 mm"), Some(24.0));
        assert_eq!(extract_number_from_string("50.0mm"), Some(50.0));

        // F-number
        assert_eq!(extract_number_from_string("f/2.8"), Some(2.8));
        assert_eq!(extract_number_from_string("F2.8"), Some(2.8));
        assert_eq!(extract_number_from_string("f/1.4"), Some(1.4));

        // Exposure time fractions
        assert!((extract_number_from_string("1/250s").unwrap() - 0.004).abs() < 0.001);
        assert!((extract_number_from_string("1/125s").unwrap() - 0.008).abs() < 0.001);
        assert!((extract_number_from_string("1/60").unwrap() - 0.0167).abs() < 0.001);

        // Negative numbers (sun altitude)
        assert_eq!(extract_number_from_string("-3.5"), Some(-3.5));
        assert_eq!(extract_number_from_string("Alt: -5.0°"), Some(-5.0));

        // Sun position format
        assert_eq!(extract_number_from_string("Alt: 45.5°, Az: 180.2°"), Some(45.5));

        // Percentage
        assert_eq!(extract_number_from_string("50%"), Some(50.0));

        // Edge cases
        assert_eq!(extract_number_from_string(""), None);
        assert_eq!(extract_number_from_string("no numbers here"), None);
    }

    #[test]
    fn test_string_with_units_indexing() {
        use crate::exif_types::TAG_ISO;

        let mut index = SearchIndex::new();

        // Insert ISO values stored as strings (like "ISO 100")
        for (i, iso_str) in
            ["ISO 100", "ISO 200", "ISO 400", "ISO 800", "ISO 1600"].iter().enumerate()
        {
            let mut features = ImageFeatures::new(1920, 1080);
            features.insert_tag(TAG_ISO, ExifValue::String(iso_str.to_string()));
            index.insert(i as u128, &features);
        }
        index.finalize();

        // Should be able to search numerically even though stored as strings
        let result = index.search_numeric(TAG_ISO, SearchOp::GreaterThan, 500.0);
        assert_eq!(result.len(), 2); // 800, 1600

        let result = index.search_range(TAG_ISO, 200.0, 800.0);
        assert_eq!(result.len(), 3); // 200, 400, 800

        // Should also work with contains search
        let result = index.search_contains(TAG_ISO, "800");
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_derived_tag_indexing() {
        use crate::exif_types::{
            TAG_DERIVED_COUNTRY, TAG_DERIVED_SUN_ALTITUDE, TAG_DERIVED_SUN_AZIMUTH,
        };

        let mut index = SearchIndex::new();

        // Simulate files with derived values
        let mut features1 = ImageFeatures::new(1920, 1080);
        features1.insert_tag(
            TAG_DERIVED_COUNTRY,
            ExifValue::String("United States of America".to_string()),
        );
        features1.insert_tag(TAG_DERIVED_SUN_AZIMUTH, ExifValue::Float(180.5));
        features1.insert_tag(TAG_DERIVED_SUN_ALTITUDE, ExifValue::Float(45.0));

        let mut features2 = ImageFeatures::new(1920, 1080);
        features2.insert_tag(TAG_DERIVED_COUNTRY, ExifValue::String("Germany".to_string()));
        features2.insert_tag(TAG_DERIVED_SUN_AZIMUTH, ExifValue::Float(90.0));
        features2.insert_tag(TAG_DERIVED_SUN_ALTITUDE, ExifValue::Float(-5.0));

        index.insert(1, &features1);
        index.insert(2, &features2);
        index.finalize();

        // Country search (contains)
        let result = index.search_contains(TAG_DERIVED_COUNTRY, "united");
        assert_eq!(result.len(), 1);
        assert!(result.contains(0));

        let result = index.search_contains(TAG_DERIVED_COUNTRY, "germany");
        assert_eq!(result.len(), 1);
        assert!(result.contains(1));

        // Sun azimuth range
        let result = index.search_range(TAG_DERIVED_SUN_AZIMUTH, 170.0, 190.0);
        assert_eq!(result.len(), 1);
        assert!(result.contains(0));

        // Sun altitude - golden hour (near horizon)
        let result = index.search_range(TAG_DERIVED_SUN_ALTITUDE, -10.0, 10.0);
        assert_eq!(result.len(), 1);
        assert!(result.contains(1)); // Only the -5.0 one
    }
}
