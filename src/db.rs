use chacha20poly1305::{
    XChaCha20Poly1305,
    XNonce, // XChaCha20 uses 24-byte nonces
    aead::{Aead, KeyInit, Payload},
};
use crossbeam_channel::{Receiver, RecvTimeoutError};
use geo::Point;
use lmdb::{Cursor, Database, DatabaseFlags, Environment, Transaction, WriteFlags};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, path::PathBuf};

const CONFIG_FILE_NAME: &str = "phdupes.conf";
const DB_FILE_NAME_PDQHASH: &str = "phdupes_pdqhash";
const DB_FILE_NAME_FEATURES: &str = "phdupes_features";
const DB_FILE_NAME_PIXELHASH: &str = "phdupes_pixelhash";

// Encryption overhead: 24-byte nonce + 16-byte Poly1305 tag
const ENCRYPTION_OVERHEAD: usize = 24 + 16;

// Total overhead: encryption only
const TOTAL_OVERHEAD: usize = ENCRYPTION_OVERHEAD;

// Default LMDB map size in MiB (2048 MiB = 2 GB)
const DEFAULT_DB_SIZE_MB: u32 = 2048;

use crate::scanner::RAW_EXTS;

/// Hash algorithm selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HashAlgorithm {
    #[default]
    PdqHash,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GroupingConfig {
    pub ignore_same_stem: bool,
    pub extensions: Vec<String>,
}

impl Default for GroupingConfig {
    fn default() -> Self {
        // Start with standard non-raw extensions
        let mut extensions = vec!["jpg".to_string(), "jpeg".to_string()];
        // Dynamically add all raw extensions from the const list
        extensions.extend(RAW_EXTS.iter().map(|s| s.to_string()));
        Self { ignore_same_stem: true, extensions }
    }
}

// --- GUI Config ---
#[derive(Serialize, Deserialize, Clone)]
pub struct GuiConfig {
    pub font_monospace: Option<String>,
    pub font_ui: Option<String>,
    pub font_scale: Option<f32>,
    pub preload_count: Option<usize>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub panel_width: Option<f32>,
    pub decimal_coords: Option<bool>,
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
            font_ui: None,
            font_scale: Some(1.0),
            preload_count: Some(10),
            width: Some(1280),
            height: Some(720),
            panel_width: Some(450.0),
            decimal_coords: Some(true),
            exif_tags: default_exif_tags(),
        }
    }
}

// Allows both [lon, lat] and { lat=..., lon=... } in TOML
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum LocationOption {
    Named { lat: f64, lon: f64 },
    Array(f64, f64), // Expects [Lon, Lat]
}

// Helper to convert directly to geo::Point
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
    pub map_providers: HashMap<String, String>, // Name -> URL pattern
    #[serde(default)]
    pub selected_provider: Option<String>,
}

impl Config {
    /// Retrieve a location by name as a geo::Point
    pub fn get_point(&self, name: &str) -> Option<Point<f64>> {
        self.locations.get(name).cloned().map(Into::into)
    }
}

fn default_db_size_mb() -> u32 {
    DEFAULT_DB_SIZE_MB
}

// Struct to hold cached data to avoid file reads
#[derive(Debug, Clone)]
pub struct CachedFeatures {
    pub width: u32,
    pub height: u32,
    pub orientation: u8,
    pub coefficients: Vec<f32>, // Flat array of coefficients
}

impl CachedFeatures {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(9 + self.coefficients.len() * 4);
        out.extend_from_slice(&self.width.to_le_bytes());
        out.extend_from_slice(&self.height.to_le_bytes());
        out.push(self.orientation);
        for c in &self.coefficients {
            out.extend_from_slice(&c.to_le_bytes());
        }
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 9 {
            return None;
        }
        let w = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        let h = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
        let o = bytes[8];

        let coeff_bytes = &bytes[9..];
        if coeff_bytes.len() % 4 != 0 {
            return None;
        }

        let count = coeff_bytes.len() / 4;
        let mut coeffs = Vec::with_capacity(count);
        for chunk in coeff_bytes.chunks_exact(4) {
            coeffs.push(f32::from_le_bytes(chunk.try_into().ok()?));
        }

        Some(Self { width: w, height: h, orientation: o, coefficients: coeffs })
    }
}

pub struct AppContext {
    pub env: Arc<Environment>,
    pub hash_db: Database,
    pub meta_db: Database,
    pub feature_db: Database,
    pub pixel_db: Database,
    pub content_key: [u8; 32],
    pub meta_key: [u8; 32],
    pub grouping_config: GroupingConfig,
    pub gui_config: GuiConfig,
    pub locations: HashMap<String, Point<f64>>,
    pub map_providers: HashMap<String, String>,
    pub selected_provider: String,
    pub tile_cache_path: PathBuf, // Path for walkers to store images
    cipher: XChaCha20Poly1305,
}

/// Database update type
pub enum HashValue {
    PdqHash([u8; 32]),
}

// (Meta Update, Hash Update, Feature Update)
pub type DbUpdate = (
    Option<([u8; 32], [u8; 32])>,       // Meta
    Option<([u8; 32], HashValue)>,      // Hash
    Option<([u8; 32], CachedFeatures)>, // Features
    Option<([u8; 32], [u8; 32])>,       // Pixel Hash
);

/// Compute the meta_key from file metadata.
///
/// The meta_key is derived from:
/// - mtime_ns: modification time in nanoseconds since UNIX_EPOCH
/// - size: file size in bytes
/// - unique_file_id: filesystem identity (dev, inode or equivalent)
///
/// This allows cache hits even after file renames (same inode).
#[inline]
pub fn compute_meta_key(
    meta_key_secret: &[u8; 32],
    mtime_ns: u64,
    size: u64,
    unique_file_id: u128,
) -> [u8; 32] {
    let mut mh = blake3::Hasher::new_keyed(meta_key_secret);
    mh.update(&mtime_ns.to_le_bytes());
    mh.update(&size.to_le_bytes());
    // Bind to filesystem identity (dev, inode) - survives renames
    mh.update(&unique_file_id.to_le_bytes());
    *mh.finalize().as_bytes()
}

/// Convenience function to compute meta_key from std::fs::Metadata
#[inline]
pub fn compute_meta_key_from_metadata(
    meta_key_secret: &[u8; 32],
    metadata: &std::fs::Metadata,
    unique_file_id: u128,
) -> [u8; 32] {
    let mtime = metadata.modified().unwrap_or(UNIX_EPOCH);
    let mtime_ns = mtime.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    let size = metadata.len();
    compute_meta_key(meta_key_secret, mtime_ns, size, unique_file_id)
}

impl AppContext {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_algorithm(HashAlgorithm::PdqHash)
    }

    pub fn with_algorithm(algorithm: HashAlgorithm) -> Result<Self, Box<dyn std::error::Error>> {
        let config_dir = dirs::config_dir().ok_or("No config dir found")?;
        let cache_dir = dirs::cache_dir().ok_or("No cache dir found")?;

        fs::create_dir_all(&config_dir)?;
        fs::create_dir_all(&cache_dir)?;
        let tile_cache_path = cache_dir.join("phdupes_tiles");
        fs::create_dir_all(&tile_cache_path)?;
        let config_path = config_dir.join(CONFIG_FILE_NAME);

        let db_file_name = match algorithm {
            HashAlgorithm::PdqHash => DB_FILE_NAME_PDQHASH,
        };
        let db_path = cache_dir.join(db_file_name);

        let mut config = if config_path.exists() {
            let content = fs::read_to_string(&config_path)?;
            eprintln!("[DEBUG-DB] Loading config from {:?}", config_path);

            let mut cfg: Config = toml::from_str(&content)
                .map_err(|_| "Failed to parse config. Format might have changed.")?;

            eprintln!(
                "[DEBUG-DB] Loaded gui config: width={:?}, height={:?}, panel_width={:?}",
                cfg.gui.width, cfg.gui.height, cfg.gui.panel_width
            );
            eprintln!("[DEBUG-DB] LMDB map size: {} MiB", cfg.db_size_mb);
            eprintln!("[DEBUG-DB] Locations loaded: {}", cfg.locations.len());

            // Check for missing sections to write back defaults
            let raw_value: toml::Value =
                toml::from_str(&content).unwrap_or(toml::Value::Integer(0));

            let missing_grouping = raw_value.get("grouping").is_none();
            let missing_gui = raw_value.get("gui").is_none();
            let missing_db_size = raw_value.get("db_size_mb").is_none();
            let missing_locations = raw_value.get("locations").is_none();

            if cfg.map_providers.is_empty() {
                cfg.map_providers.insert(
                    "OpenStreetMap".to_string(),
                    "https://tile.openstreetmap.org/{z}/{x}/{y}.png".to_string(),
                );
                cfg.map_providers.insert(
                    "Local Martin".to_string(),
                    "http://localhost:3000/rpc/tiles/{z}/{x}/{y}".to_string(),
                );
            }

            // Ensure a selection exists
            if cfg.selected_provider.is_none() {
                cfg.selected_provider = Some("OpenStreetMap".to_string());
            }

            if missing_grouping || missing_gui || missing_db_size || missing_locations {
                eprintln!(
                    "[DEBUG-DB] Writing back defaults (grouping={}, gui={}, db_size={}, locations={})",
                    missing_grouping, missing_gui, missing_db_size, missing_locations
                );
                let toml_str = toml::to_string_pretty(&cfg)?;
                fs::write(&config_path, toml_str)?;
            }
            cfg
        } else {
            eprintln!(
                "[DEBUG-DB] Config file does not exist, creating new one at {:?}",
                config_path
            );
            let mut random_bytes = [0u8; 32];
            getrandom::fill(&mut random_bytes)?;

            let mut providers = HashMap::new();
            providers.insert(
                "OpenStreetMap".to_string(),
                "https://tile.openstreetmap.org/{z}/{x}/{y}.png".to_string(),
            );
            providers.insert(
                "Local Martin".to_string(),
                "http://localhost:3000/rpc/tiles/{z}/{x}/{y}".to_string(),
            );

            let cfg = Config {
                master_key: hex::encode(random_bytes),
                db_size_mb: DEFAULT_DB_SIZE_MB,
                grouping: GroupingConfig::default(),
                gui: GuiConfig::default(),
                locations: HashMap::new(),
                map_providers: providers,
                selected_provider: Some("OpenStreetMap".to_string()),
            };

            let toml_str = toml::to_string_pretty(&cfg)?;
            fs::write(&config_path, toml_str)?;
            println!("Generated new master key in {:?}", config_path);
            cfg
        };

        // Validate and decode master_key - regenerate if invalid
        let master_key_bytes = match Self::decode_master_key(&config.master_key) {
            Ok(bytes) => bytes,
            Err(e) => {
                eprintln!("[DEBUG-DB] Invalid master_key: {}. Generating new key.", e);

                let mut new_key = [0u8; 32];
                getrandom::fill(&mut new_key)?;
                config.master_key = hex::encode(new_key);

                // Save updated config with new key
                let toml_str = toml::to_string_pretty(&config)?;
                fs::write(&config_path, &toml_str)?;
                eprintln!(
                    "[DEBUG-DB] Saved new master_key to config file. Cache will be invalidated."
                );

                new_key
            }
        };

        // 1. Content Key: For blinding file content (keyed hash)
        let content_key = blake3::derive_key("phdupes:content_key", &master_key_bytes);

        // 2. Meta Key: For blinding metadata (mtime/unique_file_id)
        let meta_key = blake3::derive_key("phdupes:meta_key", &master_key_bytes);

        // 3. Encryption Key: For ChaCha20Poly1305 database encryption
        let encryption_key = blake3::derive_key("phdupes:encryption_key", &master_key_bytes);

        // Initialize the cipher
        let cipher = XChaCha20Poly1305::new(&encryption_key.into());

        fs::create_dir_all(&db_path)?;

        // Calculate map size from config (convert MiB to bytes)
        let map_size = (config.db_size_mb as usize) * 1024 * 1024;
        eprintln!(
            "[DEBUG-DB] Setting LMDB map size to {} bytes ({} MiB)",
            map_size, config.db_size_mb
        );

        let env = Environment::new().set_map_size(map_size).set_max_dbs(10).open(&db_path)?;

        let actual_map_size = match env.info() {
            Ok(info) => info.map_size(),
            Err(e) => {
                eprintln!("[WARN-DB] Could not get LMDB env info: {}, skipping map size check", e);
                map_size
            }
        };

        let configured_map_size = map_size;

        if actual_map_size != configured_map_size {
            let actual_mb = actual_map_size / (1024 * 1024);
            let config_mb = config.db_size_mb as usize;

            if configured_map_size > actual_map_size {
                eprintln!(
                    "[WARN-DB] Config db_size_mb ({} MiB) differs from current DB map size ({} MiB).",
                    config_mb, actual_mb
                );
                eprintln!("[WARN-DB] The database is using its existing size. To increase:");
                eprintln!("[WARN-DB]   - Delete the database directory ({:?}) and rescan", db_path);
                eprintln!("[WARN-DB]   - Or use `mdb_copy -c` to compact/resize the database");
            } else {
                eprintln!(
                    "[WARN-DB] Config db_size_mb ({} MiB) is smaller than current DB map size ({} MiB).",
                    config_mb, actual_mb
                );
                eprintln!("[WARN-DB] LMDB cannot shrink an existing database. Options:");
                eprintln!(
                    "[WARN-DB]   - Update db_size_mb in config to {} to match actual size",
                    actual_mb
                );
                eprintln!(
                    "[WARN-DB]   - Delete the database directory ({:?}) to start fresh",
                    db_path
                );
            }
        }

        let hash_db = env.open_db(None)?;
        let meta_db = env.create_db(Some("file_metadata"), DatabaseFlags::empty())?;
        let feature_db = env.create_db(Some(DB_FILE_NAME_FEATURES), DatabaseFlags::empty())?;
        let pixel_db = env.create_db(Some(DB_FILE_NAME_PIXELHASH), DatabaseFlags::empty())?;
        // Convert the locations into runtime usable Points
        let locations: HashMap<String, Point<f64>> =
            config.locations.into_iter().map(|(name, option)| (name, option.into())).collect();

        Ok(Self {
            env: Arc::new(env),
            hash_db,
            meta_db,
            feature_db,
            pixel_db,
            content_key,
            meta_key,
            grouping_config: config.grouping,
            gui_config: config.gui,
            locations,
            map_providers: config.map_providers,
            selected_provider: config
                .selected_provider
                .unwrap_or_else(|| "OpenStreetMap".to_string()),
            tile_cache_path, // Pass the path to the context
            cipher,
        })
    }

    /// Decode and validate master_key from hex string
    fn decode_master_key(hex_str: &str) -> Result<[u8; 32], String> {
        let trimmed = hex_str.trim().trim_start_matches("0x");

        let bytes = hex::decode(trimmed).map_err(|e| format!("hex decode failed: {}", e))?;

        let arr: [u8; 32] =
            bytes.try_into().map_err(|v: Vec<u8>| format!("expected 32 bytes, got {}", v.len()))?;

        Ok(arr)
    }

    /// Encrypt data using AAD to bind ciphertext to the db_key.
    /// Storage format: [Nonce (24 bytes) || Ciphertext || Tag (16 bytes)]
    ///
    /// We pass `db_key` as AAD (Additional Authenticated Data). The AEAD construction
    /// ensures that if the AAD doesn't match during decryption, the tag verification fails.
    /// This prevents swap attacks where values are moved between keys.
    fn encrypt_value(cipher: &XChaCha20Poly1305, db_key: &[u8], data: &[u8]) -> Vec<u8> {
        // XChaCha20Poly1305 uses a 24-byte nonce.
        let mut nonce_bytes = [0u8; 24];
        getrandom::fill(&mut nonce_bytes).expect("RNG failed");
        let nonce = XNonce::from_slice(&nonce_bytes);

        // Encrypt with AAD = db_key
        let ciphertext = match cipher.encrypt(nonce, Payload { msg: data, aad: db_key }) {
            Ok(ct) => ct,
            Err(_) => panic!("Encryption failed - implementation error"),
        };

        // Storage format: [Nonce (24) || Ciphertext+Tag]
        let mut out = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        out
    }

    /// Decrypt value using AAD.
    /// Returns None if tag verification fails (e.g., wrong key, tampering) or decryption fails.
    fn decrypt_value(&self, db_key: &[u8], data: &[u8]) -> Option<Vec<u8>> {
        // Minimum size: nonce (24) + tag (16) = 40 bytes (for empty plaintext)
        if data.len() < TOTAL_OVERHEAD {
            return None;
        }

        let nonce_bytes = &data[0..24];
        let ciphertext = &data[24..];
        let nonce = XNonce::from_slice(nonce_bytes);

        // Decrypt with AAD = db_key
        // This validates that the ciphertext belongs to this specific db_key
        self.cipher.decrypt(nonce, Payload { msg: ciphertext, aad: db_key }).ok()
    }

    // --- DATABASE ACCESS ---

    /// Get PDQ hash (256-bit) from database
    pub fn get_pdqhash(&self, content_hash: &[u8; 32]) -> Result<Option<[u8; 32]>, lmdb::Error> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.hash_db, content_hash) {
            Ok(encrypted_bytes) => {
                if let Some(decrypted) = self.decrypt_value(content_hash, encrypted_bytes) {
                    let arr: [u8; 32] = decrypted.try_into().map_err(|_| lmdb::Error::Corrupted)?;
                    Ok(Some(arr))
                } else {
                    eprintln!("[ERROR-DB] get_pdqhash Corruped content_hash={:x?}", content_hash);
                    Err(lmdb::Error::Corrupted)
                }
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Get cached features (coeffs + metadata)
    pub fn get_features(
        &self,
        content_hash: &[u8; 32],
    ) -> Result<Option<CachedFeatures>, lmdb::Error> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.feature_db, content_hash) {
            Ok(encrypted_bytes) => {
                if let Some(decrypted) = self.decrypt_value(content_hash, encrypted_bytes) {
                    Ok(CachedFeatures::from_bytes(&decrypted))
                } else {
                    eprintln!("[ERROR] get_features Corrupted ch={:x?}", content_hash);
                    Err(lmdb::Error::Corrupted)
                }
            }
            Err(lmdb::Error::NotFound) => {
                eprintln!("[DEBUG-DB] get_features NotFound ch={:x?}", hex::encode(content_hash));
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Retrieve content hash from metadata key.
    /// STRICTLY expects [ContentHash (32) || Timestamp (8)].
    /// Returns ONLY the ContentHash.
    pub fn get_content_hash(&self, meta_hash: &[u8; 32]) -> Result<Option<[u8; 32]>, lmdb::Error> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.meta_db, meta_hash) {
            Ok(encrypted) => {
                if let Some(decrypted) = self.decrypt_value(meta_hash, encrypted) {
                    if decrypted.len() == 40 {
                        let mut arr = [0u8; 32];
                        arr.copy_from_slice(&decrypted[0..32]);
                        Ok(Some(arr))
                    } else {
                        // Strict check: if size is wrong, it's corrupted or old format
                        eprintln!("[ERROR] get_content_hash Corrupted mh={:x?}", meta_hash);
                        Err(lmdb::Error::Corrupted)
                    }
                } else {
                    Err(lmdb::Error::Corrupted)
                }
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn get_pixel_hash(&self, content_hash: &[u8; 32]) -> Result<Option<[u8; 32]>, lmdb::Error> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.pixel_db, content_hash) {
            Ok(encrypted_bytes) => {
                if let Some(decrypted) = self.decrypt_value(content_hash, encrypted_bytes) {
                    let arr: [u8; 32] = decrypted.try_into().map_err(|_| lmdb::Error::Corrupted)?;
                    Ok(Some(arr))
                } else {
                    Err(lmdb::Error::Corrupted)
                }
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Look up cached resolution and orientation from the database using file metadata.
    /// This is the single canonical function for cache lookups, used by both
    /// the GUI (app.rs) and scanner (scanner.rs).
    ///
    /// Returns (resolution, orientation) - defaults to (None, 1) if not cached.
    pub fn lookup_cached_features(
        &self,
        metadata: &std::fs::Metadata,
        unique_file_id: u128,
    ) -> (Option<(u32, u32)>, u8) {
        let meta_key = compute_meta_key_from_metadata(&self.meta_key, metadata, unique_file_id);

        eprintln!(
            "[DEBUG-CACHE] lookup_cached_features: size={}, unique_file_id={}, meta_key={:?}",
            metadata.len(),
            unique_file_id,
            hex::encode(&meta_key[..8])
        );

        // Look up content_hash from meta_key
        if let Ok(Some(content_hash)) = self.get_content_hash(&meta_key) {
            eprintln!("[DEBUG-CACHE]   Found content_hash: {:?}", hex::encode(&content_hash[..8]));
            // Look up cached features from content_hash
            if let Ok(Some(features)) = self.get_features(&content_hash) {
                let resolution = if features.width > 0 && features.height > 0 {
                    Some((features.width, features.height))
                } else {
                    None
                };
                eprintln!(
                    "[DEBUG-CACHE]   Found features: resolution={:?}, orientation={}",
                    resolution, features.orientation
                );
                return (resolution, features.orientation);
            } else {
                eprintln!("[DEBUG-CACHE]   No features found for content_hash");
            }
        }

        eprintln!("[DEBUG-CACHE]   Not found in database");
        // Not cached
        (None, 1)
    }

    /// Prune entries older than `max_age_seconds`.
    /// 1. Iterates MetaDB: Removes entries where timestamp < cutoff.
    /// 2. Collects active ContentHashes.
    /// 3. Sweeps HashDB/FeatureDB: Removes entries not in active set.
    pub fn prune(
        &self,
        max_age_seconds: u64,
    ) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let cutoff = now.saturating_sub(max_age_seconds);

        let mut txn = self.env.begin_rw_txn()?;
        let mut valid_content_hashes = HashSet::new();
        let mut meta_remove_count = 0;
        let mut hash_remove_count = 0;

        // 1. Scan MetaDB
        if txn.stat(self.meta_db)?.entries() > 0 {
            let mut cursor = txn.open_rw_cursor(self.meta_db)?;
            for iter in cursor.iter_start() {
                if let Ok((key, val_bytes)) = iter {
                    let should_delete = if let Some(decrypted) = self.decrypt_value(key, val_bytes)
                    {
                        if decrypted.len() == 40 {
                            let ts_bytes: [u8; 8] = decrypted[32..40].try_into().unwrap();
                            let last_seen = u64::from_le_bytes(ts_bytes);

                            if last_seen < cutoff {
                                true // Expired
                            } else {
                                let mut ch = [0u8; 32];
                                ch.copy_from_slice(&decrypted[0..32]);
                                valid_content_hashes.insert(ch);
                                false // Keep
                            }
                        } else {
                            true // Corrupted/Old format -> Delete
                        }
                    } else {
                        true // Decrypt fail -> Delete
                    };

                    if should_delete {
                        cursor.del(WriteFlags::empty())?;
                        meta_remove_count += 1;
                    }
                }
            }
        }

        // 2. Sweep HashDB
        if txn.stat(self.hash_db)?.entries() > 0 {
            let mut cursor = txn.open_rw_cursor(self.hash_db)?;
            for iter in cursor.iter_start() {
                if let Ok((key, _)) = iter {
                    if key.len() == 32 {
                        let mut k = [0u8; 32];
                        k.copy_from_slice(key);

                        if !valid_content_hashes.contains(&k) {
                            cursor.del(WriteFlags::empty())?;
                            hash_remove_count += 1;
                        }
                    }
                }
            }
        }

        // 3. Sweep FeatureDB
        if txn.stat(self.feature_db)?.entries() > 0 {
            let mut cursor = txn.open_rw_cursor(self.feature_db)?;
            for iter in cursor.iter_start() {
                if let Ok((key, _)) = iter {
                    if key.len() == 32 {
                        let mut k = [0u8; 32];
                        k.copy_from_slice(key);
                        if !valid_content_hashes.contains(&k) {
                            cursor.del(WriteFlags::empty())?;
                        }
                    }
                }
            }
        }

        // 4. Sweep PixelDB
        if txn.stat(self.pixel_db)?.entries() > 0 {
            let mut cursor = txn.open_rw_cursor(self.pixel_db)?;
            for iter in cursor.iter_start() {
                if let Ok((key, _)) = iter {
                    if key.len() == 32 {
                        let mut k = [0u8; 32];
                        k.copy_from_slice(key);
                        if !valid_content_hashes.contains(&k) {
                            cursor.del(WriteFlags::empty())?;
                        }
                    }
                }
            }
        }
        txn.commit()?;
        Ok((meta_remove_count, hash_remove_count))
    }

    pub fn start_db_writer(&self, rx: Receiver<DbUpdate>) -> thread::JoinHandle<()> {
        let env = self.env.clone();
        let meta_db = self.meta_db;
        let hash_db = self.hash_db;
        let feature_db = self.feature_db;
        let pixel_db = self.pixel_db;
        let cipher = self.cipher.clone();

        thread::spawn(move || {
            let mut meta_updates = Vec::new();
            let mut hash_updates = Vec::new();
            let mut feature_updates = Vec::new();
            let mut pixel_updates = Vec::new();

            let mut last_flush = Instant::now();
            let flush_interval = Duration::from_secs(1);
            let max_buffer = 1000;

            loop {
                let msg = rx.recv_timeout(Duration::from_millis(100));
                match msg {
                    Ok((m, h, f, p)) => {
                        if let Some(up) = m {
                            meta_updates.push(up);
                        }
                        if let Some(up) = h {
                            hash_updates.push(up);
                        }
                        if let Some(up) = f {
                            feature_updates.push(up);
                        }
                        if let Some(up) = p {
                            pixel_updates.push(up);
                        }
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        if let Err(e) = Self::write_batch(
                            &cipher,
                            &env,
                            meta_db,
                            hash_db,
                            feature_db,
                            pixel_db,
                            &meta_updates,
                            &hash_updates,
                            &feature_updates,
                            &pixel_updates,
                        ) {
                            eprintln!("[ERROR-DB] Final write_batch failed: {:?}", e);
                        }
                        break;
                    }
                    _ => {}
                }

                if (last_flush.elapsed() >= flush_interval
                    || meta_updates.len() >= max_buffer
                    || hash_updates.len() >= max_buffer
                    || feature_updates.len() >= max_buffer
                    || pixel_updates.len() >= max_buffer)
                    && (!meta_updates.is_empty()
                        || !hash_updates.is_empty()
                        || !feature_updates.is_empty())
                    || !pixel_updates.is_empty()
                {
                    match Self::write_batch(
                        &cipher,
                        &env,
                        meta_db,
                        hash_db,
                        feature_db,
                        pixel_db,
                        &meta_updates,
                        &hash_updates,
                        &feature_updates,
                        &pixel_updates,
                    ) {
                        Ok(()) => {
                            meta_updates.clear();
                            hash_updates.clear();
                            feature_updates.clear();
                            pixel_updates.clear();
                        }
                        Err(e) => {
                            eprintln!("[ERROR-DB] write_batch failed: {:?}", e);
                            // Don't clear - will retry on next flush
                        }
                    }
                    last_flush = Instant::now();
                }
            }
        })
    }

    fn write_batch(
        cipher: &XChaCha20Poly1305,
        env: &Environment,
        meta_db: Database,
        hash_db: Database,
        feature_db: Database,
        pixel_db: Database,
        meta_updates: &Vec<([u8; 32], [u8; 32])>,
        hash_updates: &Vec<([u8; 32], HashValue)>,
        feature_updates: &Vec<([u8; 32], CachedFeatures)>,
        pixel_updates: &Vec<([u8; 32], [u8; 32])>,
    ) -> Result<(), lmdb::Error> {
        let mut txn = env.begin_rw_txn()?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let now_bytes = now.to_le_bytes();

        // 1. Meta Updates: Append Timestamp (8 bytes) to ContentHash (32 bytes)
        for (key, val) in meta_updates {
            let mut data = Vec::with_capacity(32 + 8);
            data.extend_from_slice(val); // Content Hash
            data.extend_from_slice(&now_bytes); // Timestamp

            let encrypted = Self::encrypt_value(cipher, key, &data);
            txn.put(meta_db, key, &encrypted, WriteFlags::empty())?;
        }

        // 2. Hash Updates
        for (key, val) in hash_updates {
            match val {
                HashValue::PdqHash(pdqhash) => {
                    let encrypted = Self::encrypt_value(cipher, key, pdqhash);
                    txn.put(hash_db, key, &encrypted, WriteFlags::empty())?;
                }
            }
        }

        // 3. Feature Updates
        for (key, features) in feature_updates {
            let bytes = features.to_bytes();
            let encrypted = Self::encrypt_value(cipher, key, &bytes);
            txn.put(feature_db, key, &encrypted, WriteFlags::empty())?;
        }

        // 4. Pixel Updates
        for (key, val) in pixel_updates {
            let encrypted = Self::encrypt_value(cipher, key, val);
            txn.put(pixel_db, key, &encrypted, WriteFlags::empty())?;
        }

        txn.commit()
    }

    pub fn save_map_selection(
        &mut self,
        provider_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Update in-memory
        self.selected_provider = provider_name.to_string();

        // Update on disk
        let config_dir = dirs::config_dir().ok_or("No config dir found")?;
        let config_path = config_dir.join(CONFIG_FILE_NAME);

        if config_path.exists() {
            let content = fs::read_to_string(&config_path)?;
            let mut cfg: Config = toml::from_str(&content)?;

            cfg.selected_provider = Some(provider_name.to_string());

            let toml_str = toml::to_string_pretty(&cfg)?;
            fs::write(&config_path, toml_str)?;
        }
        Ok(())
    }

    pub fn save_gui_config(
        &self,
        gui_config: &GuiConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let config_dir = dirs::config_dir().ok_or("No config dir found")?;
        let config_path = config_dir.join(CONFIG_FILE_NAME);

        eprintln!("[DEBUG-DB] save_gui_config called");
        eprintln!("[DEBUG-DB] config_path = {:?}", config_path);

        if config_path.exists() {
            let content = fs::read_to_string(&config_path)?;
            let mut cfg: Config = toml::from_str(&content)?;
            cfg.gui = gui_config.clone();

            let toml_str = toml::to_string_pretty(&cfg)?;
            fs::write(&config_path, toml_str)?;
        } else {
            eprintln!("[DEBUG-DB] Config file does not exist at {:?}", config_path);
        }
        Ok(())
    }
}
