use std::sync::Arc;
use std::fs;
use std::thread;
use std::time::{Duration, Instant};
use lmdb::{Environment, Database, Transaction, WriteFlags, DatabaseFlags};
use serde::{Deserialize, Serialize};
use crossbeam_channel::{Receiver, RecvTimeoutError};

const CONFIG_FILE_NAME: &str = "phdupes.conf";
const DB_FILE_NAME_PHASH: &str = "phdupes_phash";
const DB_FILE_NAME_PDQHASH: &str = "phdupes_pdqhash";
const DB_FILE_NAME_FEATURES: &str = "phdupes_features";

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
            preload_count: Some(10), // Default limit 10
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
    #[serde(default)]
    grouping: GroupingConfig,
    #[serde(default)]
    gui: GuiConfig,
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
    // Simple manual serialization to avoid adding new dependencies like bincode
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

        let config = if config_path.exists() {
            let content = fs::read_to_string(&config_path)?;
            eprintln!("[DEBUG-DB] Loading config from {:?}", config_path);
            let cfg: Config = toml::from_str(&content)
                .map_err(|_| "Failed to parse config. Format might have changed.")?;

            eprintln!("[DEBUG-DB] Loaded gui config: width={:?}, height={:?}, panel_width={:?}",
                cfg.gui.width, cfg.gui.height, cfg.gui.panel_width);

            // Write back defaults if new sections missing
            let raw_value: toml::Value = toml::from_str(&content).unwrap_or(toml::Value::Integer(0));
            let missing_grouping = raw_value.get("grouping").is_none();
            let missing_gui = raw_value.get("gui").is_none();

            if missing_grouping || missing_gui {
                eprintln!("[DEBUG-DB] Writing back defaults (missing_grouping={}, missing_gui={})",
                    missing_grouping, missing_gui);
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
                grouping: GroupingConfig::default(),
                gui: GuiConfig::default(),
            };

            let toml_str = toml::to_string_pretty(&cfg)?;
            fs::write(&config_path, toml_str)?;
            println!("Generated new master key in {:?}", config_path);
            cfg
        };

        let mut master_key_bytes = [0u8; 32];
        if hex::decode_to_slice(config.master_key.trim_start_matches("0x"), &mut master_key_bytes).is_err() {
            eprintln!("Invalid master_key hex string, using default");
        }

        let mut content_material = master_key_bytes;
        content_material[0] ^= 0b0000_0001;
        let content_key = *blake3::hash(&content_material).as_bytes();

        let mut meta_material = master_key_bytes;
        meta_material[0] ^= 0b0000_0010;
        let meta_key = *blake3::hash(&meta_material).as_bytes();

        fs::create_dir_all(&db_path)?;

        let env = Environment::new()
            .set_map_size(10485760 * 200)
            .set_max_dbs(10)
            .open(&db_path)?;

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
        })
    }

    /// Get pHash (64-bit) from database
    pub fn get_phash(&self, content_hash: &[u8; 32]) -> Result<Option<u64>, lmdb::Error> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.hash_db, content_hash) {
            Ok(bytes) => {
                let arr: [u8; 8] = bytes.try_into().map_err(|_| lmdb::Error::Corrupted)?;
                Ok(Some(u64::from_le_bytes(arr)))
            },
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Get PDQ hash (256-bit) from database
    pub fn get_pdqhash(&self, content_hash: &[u8; 32]) -> Result<Option<[u8; 32]>, lmdb::Error> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.hash_db, content_hash) {
            Ok(bytes) => {
                let arr: [u8; 32] = bytes.try_into().map_err(|_| lmdb::Error::Corrupted)?;
                Ok(Some(arr))
            },
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    // Get cached features (coeffs + metadata)
    pub fn get_features(&self, content_hash: &[u8; 32]) -> Result<Option<CachedFeatures>, lmdb::Error> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.feature_db, content_hash) {
            Ok(bytes) => Ok(CachedFeatures::from_bytes(bytes)),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn get_content_hash(&self, meta_hash: &[u8; 32]) -> Result<Option<[u8; 32]>, lmdb::Error> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.meta_db, meta_hash) {
            Ok(bytes) => {
                let arr: [u8; 32] = bytes.try_into().map_err(|_| lmdb::Error::Corrupted)?;
                Ok(Some(arr))
            },
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn start_db_writer(&self, rx: Receiver<DbUpdate>) -> thread::JoinHandle<()> {
        let env = self.env.clone();
        let meta_db = self.meta_db;
        let hash_db = self.hash_db;
        let feature_db = self.feature_db;

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
                        let _ = Self::write_batch(&env, meta_db, hash_db, feature_db, &meta_updates, &hash_updates, &feature_updates);
                        break;
                    },
                    _ => {}
                }

                if (last_flush.elapsed() >= flush_interval || meta_updates.len() >= max_buffer || hash_updates.len() >= max_buffer || feature_updates.len() >= max_buffer)
                    && (!meta_updates.is_empty() || !hash_updates.is_empty() || !feature_updates.is_empty()) {
                        if Self::write_batch(&env, meta_db, hash_db, feature_db, &meta_updates, &hash_updates, &feature_updates).is_ok() {
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
        env: &Environment,
        meta_db: Database,
        hash_db: Database,
        feature_db: Database,
        meta_updates: &Vec<([u8; 32], [u8; 32])>,
        hash_updates: &Vec<([u8; 32], HashValue)>,
        feature_updates: &Vec<([u8; 32], CachedFeatures)>
    ) -> Result<(), lmdb::Error> {
        let mut txn = env.begin_rw_txn()?;
        for (key, val) in meta_updates {
            txn.put(meta_db, key, val, WriteFlags::empty())?;
        }
        for (key, val) in hash_updates {
            match val {
                HashValue::PHash(phash) => {
                    let val_bytes = phash.to_le_bytes();
                    txn.put(hash_db, key, &val_bytes, WriteFlags::empty())?;
                },
                HashValue::PdqHash(pdqhash) => {
                    txn.put(hash_db, key, pdqhash, WriteFlags::empty())?;
                },
            }
        }
        for (key, features) in feature_updates {
            let bytes = features.to_bytes();
            txn.put(feature_db, key, &bytes, WriteFlags::empty())?;
        }
        txn.commit()
    }

    /// Save updated gui config (e.g., window size) back to the config file
    pub fn save_gui_config(&self, gui_config: &GuiConfig) -> Result<(), Box<dyn std::error::Error>> {
        let config_dir = dirs::config_dir().ok_or("No config dir found")?;
        let config_path = config_dir.join(CONFIG_FILE_NAME);

        eprintln!("[DEBUG-DB] save_gui_config called");
        eprintln!("[DEBUG-DB] config_path = {:?}", config_path);
        eprintln!("[DEBUG-DB] gui_config to save: width={:?}, height={:?}, panel_width={:?}, decimal_coords={:?}",
            gui_config.width, gui_config.height, gui_config.panel_width, gui_config.decimal_coords);

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
