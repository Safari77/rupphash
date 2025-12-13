use std::sync::Arc;
use std::fs;
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH, SystemTime};
use std::collections::HashSet;
use lmdb::{Environment, Database, Transaction, WriteFlags, DatabaseFlags, Cursor};
use serde::{Deserialize, Serialize};
use crossbeam_channel::{Receiver, RecvTimeoutError};
use chacha20poly1305::{
    aead::{Aead, KeyInit, Payload},
    XChaCha20Poly1305, XNonce // XChaCha20 uses 24-byte nonces
};

const CONFIG_FILE_NAME: &str = "phdupes.conf";
const DB_FILE_NAME_PHASH: &str = "phdupes_phash";
const DB_FILE_NAME_PDQHASH: &str = "phdupes_pdqhash";
const DB_FILE_NAME_FEATURES: &str = "phdupes_features";

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
    PHash,
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
        Self {
            ignore_same_stem: true,
            extensions,
        }
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

#[derive(Serialize, Deserialize)]
struct Config {
    master_key: String,
    #[serde(default = "default_db_size_mb")]
    db_size_mb: u32,
    #[serde(default)]
    grouping: GroupingConfig,
    #[serde(default)]
    gui: GuiConfig,
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
        if bytes.len() < 9 { return None; }
        let w = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        let h = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
        let o = bytes[8];

        let coeff_bytes = &bytes[9..];
        if coeff_bytes.len() % 4 != 0 { return None; }

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
    pub content_key: [u8; 32],
    pub meta_key: [u8; 32],
    pub grouping_config: GroupingConfig,
    pub gui_config: GuiConfig,
    pub hash_algorithm: HashAlgorithm,
    cipher: XChaCha20Poly1305,
}

/// Database update type - supports both pHash (u64) and PDQ hash ([u8; 32])
pub enum HashValue {
    PHash(u64),
    PdqHash([u8; 32]),
}

// (Meta Update, Hash Update, Feature Update)
pub type DbUpdate = (
    Option<([u8; 32], [u8; 32])>,
    Option<([u8; 32], HashValue)>,
    Option<([u8; 32], CachedFeatures)>
);

impl AppContext {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_algorithm(HashAlgorithm::PHash)
    }

    pub fn with_algorithm(algorithm: HashAlgorithm) -> Result<Self, Box<dyn std::error::Error>> {
        let config_dir = dirs::config_dir().ok_or("No config dir found")?;
        let cache_dir = dirs::cache_dir().ok_or("No cache dir found")?;

        fs::create_dir_all(&config_dir)?;
        fs::create_dir_all(&cache_dir)?;

        let config_path = config_dir.join(CONFIG_FILE_NAME);

        // Select database path based on algorithm
        let db_file_name = match algorithm {
            HashAlgorithm::PHash => DB_FILE_NAME_PHASH,
            HashAlgorithm::PdqHash => DB_FILE_NAME_PDQHASH,
        };
        let db_path = cache_dir.join(db_file_name);

        let mut config = if config_path.exists() {
            let content = fs::read_to_string(&config_path)?;
            eprintln!("[DEBUG-DB] Loading config from {:?}", config_path);
            let cfg: Config = toml::from_str(&content)
                .map_err(|_| "Failed to parse config. Format might have changed.")?;

            eprintln!("[DEBUG-DB] Loaded gui config: width={:?}, height={:?}, panel_width={:?}",
                cfg.gui.width, cfg.gui.height, cfg.gui.panel_width);
            eprintln!("[DEBUG-DB] LMDB map size: {} MiB", cfg.db_size_mb);

            // Write back defaults if new sections missing
            let raw_value: toml::Value = toml::from_str(&content).unwrap_or(toml::Value::Integer(0));
            let missing_grouping = raw_value.get("grouping").is_none();
            let missing_gui = raw_value.get("gui").is_none();
            let missing_db_size = raw_value.get("db_size_mb").is_none();

            if missing_grouping || missing_gui || missing_db_size {
                eprintln!("[DEBUG-DB] Writing back defaults (missing_grouping={}, missing_gui={}, missing_db_size={})",
                    missing_grouping, missing_gui, missing_db_size);
                 let toml_str = toml::to_string_pretty(&cfg)?;
                 fs::write(&config_path, toml_str)?;
            }
            cfg
        } else {
            eprintln!("[DEBUG-DB] Config file does not exist, creating new one at {:?}", config_path);
            let mut random_bytes = [0u8; 32];
            getrandom::fill(&mut random_bytes)?;

            let cfg = Config {
                master_key: hex::encode(random_bytes),
                db_size_mb: DEFAULT_DB_SIZE_MB,
                grouping: GroupingConfig::default(),
                gui: GuiConfig::default(),
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
                eprintln!("[DEBUG-DB] Saved new master_key to config file. Cache will be invalidated.");

                new_key
            }
        };

        // 1. Content Key: For blinding file content (keyed hash)
        let content_key = blake3::derive_key("phdupes:content_key", &master_key_bytes);

        // 2. Meta Key: For blinding metadata (mtime/inode)
        let meta_key = blake3::derive_key("phdupes:meta_key", &master_key_bytes);

        // 3. Encryption Key: For ChaCha20Poly1305 database encryption
        let encryption_key = blake3::derive_key("phdupes:encryption_key", &master_key_bytes);

        // Initialize the cipher
        let cipher = XChaCha20Poly1305::new(&encryption_key.into());

        fs::create_dir_all(&db_path)?;

        // Calculate map size from config (convert MiB to bytes)
        let map_size = (config.db_size_mb as usize) * 1024 * 1024;
        eprintln!("[DEBUG-DB] Setting LMDB map size to {} bytes ({} MiB)", map_size, config.db_size_mb);

        let env = Environment::new()
            .set_map_size(map_size)
            .set_max_dbs(10)
            .open(&db_path)?;

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
                eprintln!("[WARN-DB] Config db_size_mb ({} MiB) differs from current DB map size ({} MiB).",
                    config_mb, actual_mb);
                eprintln!("[WARN-DB] The database is using its existing size. To increase:");
                eprintln!("[WARN-DB]   - Delete the database directory ({:?}) and rescan", db_path);
                eprintln!("[WARN-DB]   - Or use `mdb_copy -c` to compact/resize the database");
            } else {
                eprintln!("[WARN-DB] Config db_size_mb ({} MiB) is smaller than current DB map size ({} MiB).",
                    config_mb, actual_mb);
                eprintln!("[WARN-DB] LMDB cannot shrink an existing database. Options:");
                eprintln!("[WARN-DB]   - Update db_size_mb in config to {} to match actual size", actual_mb);
                eprintln!("[WARN-DB]   - Delete the database directory ({:?}) to start fresh", db_path);
            }
        }

        let hash_db = env.open_db(None)?;
        let meta_db = env.create_db(Some("file_metadata"), DatabaseFlags::empty())?;
        let feature_db = env.create_db(Some(DB_FILE_NAME_FEATURES), DatabaseFlags::empty())?;

        Ok(Self {
            env: Arc::new(env),
            hash_db,
            meta_db,
            feature_db,
            content_key,
            meta_key,
            grouping_config: config.grouping,
            gui_config: config.gui,
            hash_algorithm: algorithm,
            cipher,
        })
    }

    /// Decode and validate master_key from hex string
    fn decode_master_key(hex_str: &str) -> Result<[u8; 32], String> {
        let trimmed = hex_str.trim().trim_start_matches("0x");

        let bytes = hex::decode(trimmed)
            .map_err(|e| format!("hex decode failed: {}", e))?;

        let arr: [u8; 32] = bytes.try_into()
            .map_err(|v: Vec<u8>| format!("expected 32 bytes, got {}", v.len()))?;

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
        if data.len() < TOTAL_OVERHEAD { return None; }

        let nonce_bytes = &data[0..24];
        let ciphertext = &data[24..];
        let nonce = XNonce::from_slice(nonce_bytes);

        // Decrypt with AAD = db_key
        // This validates that the ciphertext belongs to this specific db_key
        self.cipher.decrypt(nonce, Payload { msg: ciphertext, aad: db_key }).ok()
    }

    // --- DATABASE ACCESS ---

    /// Get pHash (64-bit) from database
    pub fn get_phash(&self, content_hash: &[u8; 32]) -> Result<Option<u64>, lmdb::Error> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.hash_db, content_hash) {
            Ok(encrypted_bytes) => {
                if let Some(decrypted) = self.decrypt_value(content_hash, encrypted_bytes) {
                    let arr: [u8; 8] = decrypted.try_into().map_err(|_| lmdb::Error::Corrupted)?;
                    Ok(Some(u64::from_le_bytes(arr)))
                } else {
                    // Decryption failed (integrity check)
                    Err(lmdb::Error::Corrupted)
                }
            },
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Get PDQ hash (256-bit) from database
    pub fn get_pdqhash(&self, content_hash: &[u8; 32]) -> Result<Option<[u8; 32]>, lmdb::Error> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.hash_db, content_hash) {
            Ok(encrypted_bytes) => {
                if let Some(decrypted) = self.decrypt_value(content_hash, encrypted_bytes) {
                    let arr: [u8; 32] = decrypted.try_into().map_err(|_| lmdb::Error::Corrupted)?;
                    Ok(Some(arr))
                } else {
                    Err(lmdb::Error::Corrupted)
                }
            },
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Get cached features (coeffs + metadata)
    pub fn get_features(&self, content_hash: &[u8; 32]) -> Result<Option<CachedFeatures>, lmdb::Error> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.feature_db, content_hash) {
            Ok(encrypted_bytes) => {
                if let Some(decrypted) = self.decrypt_value(content_hash, encrypted_bytes) {
                    Ok(CachedFeatures::from_bytes(&decrypted))
                } else {
                    Err(lmdb::Error::Corrupted)
                }
            },
            Err(lmdb::Error::NotFound) => Ok(None),
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
                        Err(lmdb::Error::Corrupted)
                    }
                } else {
                    Err(lmdb::Error::Corrupted)
                }
            },
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Prune entries older than `max_age_seconds`.
    /// 1. Iterates MetaDB: Removes entries where timestamp < cutoff.
    /// 2. Collects active ContentHashes.
    /// 3. Sweeps HashDB/FeatureDB: Removes entries not in active set.
    pub fn prune(&self, max_age_seconds: u64) -> Result<(usize, usize), Box<dyn std::error::Error>> {
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
                    let should_delete = if let Some(decrypted) = self.decrypt_value(key, val_bytes) {
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
                // Fix: Handle Result
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
                // Fix: Handle Result
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
        let cipher = self.cipher.clone();

        thread::spawn(move || {
            let mut meta_updates = Vec::new();
            let mut hash_updates = Vec::new();
            let mut feature_updates = Vec::new();
            let mut last_flush = Instant::now();
            let flush_interval = Duration::from_secs(1);
            let max_buffer = 1000;

            loop {
                let msg = rx.recv_timeout(Duration::from_millis(100));
                match msg {
                    Ok((m, h, f)) => {
                        if let Some(up) = m { meta_updates.push(up); }
                        if let Some(up) = h { hash_updates.push(up); }
                        if let Some(up) = f { feature_updates.push(up); }
                    },
                    Err(RecvTimeoutError::Disconnected) => {
                        let _ = Self::write_batch(&cipher, &env, meta_db, hash_db, feature_db, &meta_updates, &hash_updates, &feature_updates);
                        break;
                    },
                    _ => {}
                }

                if (last_flush.elapsed() >= flush_interval || meta_updates.len() >= max_buffer || hash_updates.len() >= max_buffer || feature_updates.len() >= max_buffer)
                    && (!meta_updates.is_empty() || !hash_updates.is_empty() || !feature_updates.is_empty()) {
                        if Self::write_batch(&cipher, &env, meta_db, hash_db, feature_db, &meta_updates, &hash_updates, &feature_updates).is_ok() {
                            meta_updates.clear();
                            hash_updates.clear();
                            feature_updates.clear();
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
        meta_updates: &Vec<([u8; 32], [u8; 32])>,
        hash_updates: &Vec<([u8; 32], HashValue)>,
        feature_updates: &Vec<([u8; 32], CachedFeatures)>
    ) -> Result<(), lmdb::Error> {
        let mut txn = env.begin_rw_txn()?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let now_bytes = now.to_le_bytes();

        // 1. Meta Updates: Append Timestamp (8 bytes) to ContentHash (32 bytes)
        for (key, val) in meta_updates {
            let mut data = Vec::with_capacity(32 + 8);
            data.extend_from_slice(val);      // Content Hash
            data.extend_from_slice(&now_bytes); // Timestamp

            let encrypted = Self::encrypt_value(cipher, key, &data);
            txn.put(meta_db, key, &encrypted, WriteFlags::empty())?;
        }

        // 2. Hash Updates
        for (key, val) in hash_updates {
            match val {
                HashValue::PHash(phash) => {
                    let val_bytes = phash.to_le_bytes();
                    let encrypted = Self::encrypt_value(cipher, key, &val_bytes);
                    txn.put(hash_db, key, &encrypted, WriteFlags::empty())?;
                },
                HashValue::PdqHash(pdqhash) => {
                    let encrypted = Self::encrypt_value(cipher, key, pdqhash);
                    txn.put(hash_db, key, &encrypted, WriteFlags::empty())?;
                },
            }
        }

        // 3. Feature Updates
        for (key, features) in feature_updates {
            let bytes = features.to_bytes();
            let encrypted = Self::encrypt_value(cipher, key, &bytes);
            txn.put(feature_db, key, &encrypted, WriteFlags::empty())?;
        }

        txn.commit()
    }

    pub fn save_gui_config(&self, gui_config: &GuiConfig) -> Result<(), Box<dyn std::error::Error>> {
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
