use std::{
    collections::HashMap,
    fmt, fs,
    io::Write,
    mem,
    path::{Path, PathBuf},
};

use geo::Point;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use zeroize::{Zeroize, Zeroizing};

use crate::scanner::RAW_EXTS;

pub(crate) const CONFIG_FILE_NAME: &str = "phdupes.conf";
pub(crate) const DEFAULT_DB_SIZE_MB: u32 = 2048;

#[derive(Debug)]
pub enum ConfigError {
    NoConfigDir,
    Io(std::io::Error),
    TomlParse { path: PathBuf, source: toml::de::Error },
    TomlSerialize(toml::ser::Error),
    Persist(tempfile::PersistError),
    Rng(getrandom::Error),
    InvalidMasterKey { path: PathBuf, reason: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoConfigDir => write!(f, "no config directory found"),
            Self::Io(err) => write!(f, "config I/O error: {err}"),
            Self::TomlParse { path, source } => {
                write!(f, "failed to parse config at {}: {source}", path.display())
            },
            Self::TomlSerialize(err) => write!(f, "failed to serialize config: {err}"),
            Self::Persist(err) => write!(f, "failed to persist config file: {err}"),
            Self::Rng(err) => write!(f, "failed to generate random master key: {err}"),
            Self::InvalidMasterKey { path, reason } => write!(
                f,
                "invalid master_key in {}: {reason}; expected 64 hex characters (32 bytes, optional 0x prefix) — please fix the config file without regenerating the key",
                path.display()
            ),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::TomlParse { source, .. } => Some(source),
            Self::TomlSerialize(err) => Some(err),
            Self::Persist(err) => Some(err),
            Self::Rng(err) => Some(err),
            Self::NoConfigDir | Self::InvalidMasterKey { .. } => None,
        }
    }
}

impl From<std::io::Error> for ConfigError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<toml::ser::Error> for ConfigError {
    fn from(err: toml::ser::Error) -> Self {
        Self::TomlSerialize(err)
    }
}

impl From<tempfile::PersistError> for ConfigError {
    fn from(err: tempfile::PersistError) -> Self {
        Self::Persist(err)
    }
}

impl From<getrandom::Error> for ConfigError {
    fn from(err: getrandom::Error) -> Self {
        Self::Rng(err)
    }
}

/// Palette sort order for dominant color display.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PaletteSort {
    Hue,
    #[default]
    Luminance,
}

/// Bundled palette extraction configuration.
#[derive(Debug, Clone, Copy)]
pub struct PaletteConfig {
    pub dominant_colors: usize,
    pub saturation_bias: f32,
    pub palette_sort: PaletteSort,
}

impl PaletteConfig {
    #[must_use]
    pub fn from_gui_config(gui: &GuiConfig) -> Self {
        Self {
            dominant_colors: gui.dominant_colors.unwrap_or(5),
            saturation_bias: gui.saturation_bias.unwrap_or(1.0),
            palette_sort: gui.palette_sort.unwrap_or_default(),
        }
    }
}

/// Bundled HDR→SDR tone-mapping configuration.
#[derive(Debug, Clone, Copy)]
pub struct HdrConfig {
    pub sdr_peak_nits: f32,
}

impl HdrConfig {
    #[must_use]
    pub fn from_gui_config(gui: &GuiConfig) -> Self {
        Self { sdr_peak_nits: gui.sdr_peak_nits.unwrap_or(203.0) }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct GroupingConfig {
    pub ignore_same_stem: bool,
    pub extensions: Vec<String>,
}

impl Default for GroupingConfig {
    fn default() -> Self {
        // Start with standard non-raw extensions
        let mut extensions = vec!["jpg".to_string(), "jpeg".to_string()];
        // Dynamically add all raw extensions from the const list
        extensions.extend(RAW_EXTS.iter().map(|s| (*s).to_string()));
        Self { ignore_same_stem: true, extensions }
    }
}

// --- GUI Config ---
// `#[serde(default)]` at the struct level ensures that when a `[gui]` section
// exists in `phdupes.conf` with only a subset of keys specified, any omitted
// fields are populated from `GuiConfig::default()` rather than `Option::None`.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct GuiConfig {
    pub font_monospace: Option<String>,
    pub dominant_colors: Option<usize>,
    pub saturation_bias: Option<f32>,
    pub palette_sort: Option<PaletteSort>,
    pub font_ui: Option<String>,
    pub font_scale: Option<f32>,
    pub font_orthography: Option<String>,
    pub preload_count: Option<usize>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub panel_width: Option<f32>,
    pub decimal_coords: Option<bool>,
    /// Target display peak luminance (nits) for HDR→SDR tone mapping.
    /// 100.0 = strict SDR reference; 203.0 = BT.2408 HDR reference white (default).
    pub sdr_peak_nits: Option<f32>,
    #[serde(default = "default_exif_tags")]
    pub exif_tags: Vec<String>,
}

fn default_exif_tags() -> Vec<String> {
    vec![
        "Make".to_string(),
        "Model".to_string(),
        "LensModel".to_string(),
        "DateTimeOriginal".to_string(),
        "ExposureTime".to_string(),
        "FNumber".to_string(),
        "ISO".to_string(),
        "FocalLength".to_string(),
        "ExposureBias".to_string(),
        "DerivedCountry".to_string(),
    ]
}

impl Default for GuiConfig {
    fn default() -> Self {
        Self {
            font_monospace: None,
            dominant_colors: Some(5),
            saturation_bias: Some(1.0),
            palette_sort: None,
            font_ui: None,
            font_scale: Some(1.0),
            font_orthography: None,
            preload_count: Some(10),
            width: Some(1280),
            height: Some(720),
            panel_width: Some(450.0),
            decimal_coords: Some(true),
            sdr_peak_nits: Some(203.0),
            exif_tags: default_exif_tags(),
        }
    }
}

/// Allows both `[lon, lat]` and `{ lat=..., lon=... }` in TOML.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum LocationOption {
    Named { lat: f64, lon: f64 },
    Array(f64, f64), // Expects [Lon, Lat]
}

impl From<LocationOption> for Point<f64> {
    fn from(loc: LocationOption) -> Self {
        match loc {
            LocationOption::Named { lat, lon } => Point::new(lon, lat),
            LocationOption::Array(lon, lat) => Point::new(lon, lat),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Config {
    master_key: String,
    #[serde(default = "default_db_size_mb")]
    db_size_mb: u32,
    #[serde(default)]
    grouping: GroupingConfig,
    #[serde(default)]
    gui: GuiConfig,
    #[serde(default)]
    locations: HashMap<String, LocationOption>,
    #[serde(default)]
    map_providers: HashMap<String, String>,
    #[serde(default)]
    selected_provider: Option<String>,
}

impl Drop for Config {
    fn drop(&mut self) {
        self.master_key.zeroize();
    }
}

fn default_db_size_mb() -> u32 {
    DEFAULT_DB_SIZE_MB
}

pub(crate) struct LoadedConfig {
    pub master_key_bytes: Zeroizing<[u8; 32]>,
    pub db_size_mb: u32,
    pub grouping: GroupingConfig,
    pub gui: GuiConfig,
    pub locations: HashMap<String, Point<f64>>,
    pub map_providers: HashMap<String, String>,
    pub selected_provider: String,
}

fn default_map_providers() -> HashMap<String, String> {
    let mut providers = HashMap::new();
    providers.insert(
        "OpenStreetMap".to_string(),
        "https://tile.openstreetmap.org/{z}/{x}/{y}.png".to_string(),
    );
    providers.insert(
        "Local Martin".to_string(),
        "http://localhost:3000/rpc/tiles/{z}/{x}/{y}".to_string(),
    );
    providers
}

fn choose_default_provider(providers: &HashMap<String, String>) -> String {
    if providers.contains_key("OpenStreetMap") {
        return "OpenStreetMap".to_string();
    }
    providers.keys().min().cloned().unwrap_or_else(|| "OpenStreetMap".to_string())
}

/// Atomically write the config file using `tempfile::NamedTempFile` in the
/// same directory as `config_path`.
fn write_config(config_path: &Path, config: &Config) -> Result<(), ConfigError> {
    let toml_str = Zeroizing::new(toml::to_string_pretty(config)?);
    let parent_dir = match config_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };

    let mut tmp_file = NamedTempFile::new_in(parent_dir)?;
    tmp_file.write_all(toml_str.as_bytes())?;
    tmp_file.as_file().sync_all()?;
    tmp_file.persist(config_path)?;
    Ok(())
}

/// Decode and validate `master_key` from a hex string.
pub(crate) fn decode_master_key(hex_str: &str) -> Result<[u8; 32], String> {
    let trimmed = hex_str.trim().trim_start_matches("0x").trim_start_matches("0X");
    let bytes =
        Zeroizing::new(hex::decode(trimmed).map_err(|e| format!("hex decode failed: {e}"))?);
    if bytes.len() != 32 {
        return Err(format!("expected 32 bytes, got {}", bytes.len()));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}

/// Load existing `phdupes.conf` or initialize a new one with a random master key.
pub(crate) fn load_or_init_config() -> Result<LoadedConfig, ConfigError> {
    let config_dir = dirs::config_dir().ok_or(ConfigError::NoConfigDir)?;
    fs::create_dir_all(&config_dir)?;
    let config_path = config_dir.join(CONFIG_FILE_NAME);

    let mut cfg = if config_path.exists() {
        let content = Zeroizing::new(fs::read_to_string(&config_path)?);
        eprintln!("[DEBUG-DB] Loading config from {:?}", config_path);

        let mut cfg: Config = toml::from_str(&content)
            .map_err(|source| ConfigError::TomlParse { path: config_path.clone(), source })?;

        // Clamp dominant_colors to 1..=25 range
        let original_colors = cfg.gui.dominant_colors;
        let clamped_val = cfg.gui.dominant_colors.unwrap_or(5).clamp(1, 25);
        cfg.gui.dominant_colors = Some(clamped_val);
        let colors_clamped = original_colors != cfg.gui.dominant_colors;

        eprintln!(
            "[DEBUG-DB] Loaded gui config: width={:?}, height={:?}, panel_width={:?}, dominant_colors={:?}, saturation_bias={:?}, palette_sort={:?}, sdr_peak_nits={:?}",
            cfg.gui.width,
            cfg.gui.height,
            cfg.gui.panel_width,
            cfg.gui.dominant_colors,
            cfg.gui.saturation_bias,
            cfg.gui.palette_sort,
            cfg.gui.sdr_peak_nits
        );
        eprintln!("[DEBUG-DB] redb cache size: {} MiB", cfg.db_size_mb);
        eprintln!("[DEBUG-DB] Locations loaded: {}", cfg.locations.len());

        // Check for missing sections to write back defaults
        let mut raw_table: toml::Table = toml::from_str(&content)
            .map_err(|source| ConfigError::TomlParse { path: config_path.clone(), source })?;
        if let Some(toml::Value::String(mut mk)) = raw_table.remove("master_key") {
            mk.zeroize();
        }

        let missing_grouping = !raw_table.contains_key("grouping");
        let missing_gui = !raw_table.contains_key("gui");
        let missing_db_size = !raw_table.contains_key("db_size_mb");
        let missing_locations = !raw_table.contains_key("locations");
        let missing_map_providers = !raw_table.contains_key("map_providers");
        let missing_selected_provider = !raw_table.contains_key("selected_provider");

        if cfg.map_providers.is_empty() {
            cfg.map_providers = default_map_providers();
        }
        if cfg.selected_provider.is_none() {
            cfg.selected_provider = Some(choose_default_provider(&cfg.map_providers));
        }

        if missing_grouping
            || missing_gui
            || missing_db_size
            || missing_locations
            || missing_map_providers
            || missing_selected_provider
            || colors_clamped
        {
            eprintln!(
                "[DEBUG-DB] Writing back defaults/fixes (grouping={}, gui={}, db_size={}, locations={}, map_providers={}, selected_provider={}, colors_clamped={})",
                missing_grouping,
                missing_gui,
                missing_db_size,
                missing_locations,
                missing_map_providers,
                missing_selected_provider,
                colors_clamped
            );
            if let Err(err) = write_config(&config_path, &cfg) {
                eprintln!(
                    "[WARN-DB] Could not write back config defaults to {:?}: {}",
                    config_path, err
                );
            }
        }
        cfg
    } else {
        eprintln!("[DEBUG-DB] Config file does not exist, creating new one at {:?}", config_path);
        let mut random_bytes = Zeroizing::new([0u8; 32]);
        getrandom::fill(&mut *random_bytes)?;
        let master_key_hex = hex::encode(*random_bytes);
        let cfg = Config {
            master_key: master_key_hex,
            db_size_mb: DEFAULT_DB_SIZE_MB,
            grouping: GroupingConfig::default(),
            gui: GuiConfig::default(),
            locations: HashMap::new(),
            map_providers: default_map_providers(),
            selected_provider: Some("OpenStreetMap".to_string()),
        };

        write_config(&config_path, &cfg)?;
        println!("Generated new master key in {:?}", config_path);
        cfg
    };

    let master_key_bytes = Zeroizing::new(
        decode_master_key(&cfg.master_key)
            .map_err(|reason| ConfigError::InvalidMasterKey { path: config_path, reason })?,
    );
    cfg.master_key.zeroize();

    let raw_locations = mem::take(&mut cfg.locations);
    let locations: HashMap<String, Point<f64>> =
        raw_locations.into_iter().map(|(name, option)| (name, option.into())).collect();
    let map_providers = mem::take(&mut cfg.map_providers);
    let selected_provider =
        cfg.selected_provider.take().unwrap_or_else(|| choose_default_provider(&map_providers));

    Ok(LoadedConfig {
        master_key_bytes,
        db_size_mb: cfg.db_size_mb,
        grouping: mem::take(&mut cfg.grouping),
        gui: mem::take(&mut cfg.gui),
        locations,
        map_providers,
        selected_provider,
    })
}

pub(crate) fn save_map_selection(provider_name: &str) -> Result<(), ConfigError> {
    let config_dir = dirs::config_dir().ok_or(ConfigError::NoConfigDir)?;
    let config_path = config_dir.join(CONFIG_FILE_NAME);

    if config_path.exists() {
        let content = Zeroizing::new(fs::read_to_string(&config_path)?);
        let mut cfg: Config = toml::from_str(&content)
            .map_err(|source| ConfigError::TomlParse { path: config_path.clone(), source })?;
        cfg.selected_provider = Some(provider_name.to_string());
        write_config(&config_path, &cfg)?;
    }
    Ok(())
}

pub(crate) fn save_gui_config(gui_config: &GuiConfig) -> Result<(), ConfigError> {
    let config_dir = dirs::config_dir().ok_or(ConfigError::NoConfigDir)?;
    let config_path = config_dir.join(CONFIG_FILE_NAME);

    if config_path.exists() {
        let content = Zeroizing::new(fs::read_to_string(&config_path)?);
        let mut cfg: Config = toml::from_str(&content)
            .map_err(|source| ConfigError::TomlParse { path: config_path.clone(), source })?;
        let mut clamped_gui = gui_config.clone();
        clamped_gui.dominant_colors = Some(clamped_gui.dominant_colors.unwrap_or(5).clamp(1, 25));
        cfg.gui = clamped_gui;
        write_config(&config_path, &cfg)?;
    } else {
        eprintln!("[DEBUG-DB] Config file does not exist at {:?}", config_path);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_master_key_accepts_valid_and_0x_prefix() {
        let raw = [0xABu8; 32];
        let hex_str = hex::encode(raw);
        assert_eq!(decode_master_key(&hex_str).unwrap(), raw);
        assert_eq!(decode_master_key(&format!("  0x{hex_str}  ")).unwrap(), raw);
        assert_eq!(decode_master_key(&format!("0X{hex_str}")).unwrap(), raw);
    }

    #[test]
    fn decode_master_key_rejects_invalid_lengths_and_hex() {
        assert!(decode_master_key("deadbeef").is_err());
        assert!(decode_master_key("zzzz").is_err());
        assert!(decode_master_key("").is_err());
    }

    #[test]
    fn write_config_handles_relative_filename_without_parent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("phdupes.conf");
        let cfg = Config {
            master_key: "00".repeat(32),
            db_size_mb: DEFAULT_DB_SIZE_MB,
            grouping: GroupingConfig::default(),
            gui: GuiConfig::default(),
            locations: HashMap::new(),
            map_providers: default_map_providers(),
            selected_provider: Some("OpenStreetMap".to_string()),
        };
        write_config(&path, &cfg).unwrap();
        assert!(path.exists());
    }
}
