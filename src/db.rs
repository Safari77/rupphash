use std::{
    collections::{HashMap, HashSet},
    fmt, fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use chacha20poly1305::{
    XChaCha20Poly1305,
    XNonce, // XChaCha20 uses 24-byte nonces
    aead::{Aead, AeadInOut, KeyInit, Payload},
};
use crossbeam_channel::{Receiver, RecvTimeoutError, TryRecvError};
use geo::Point;
use redb::{
    ConcurrencyMode, Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction,
};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

pub use crate::configs::{
    ConfigError, GroupingConfig, GuiConfig, HdrConfig, PaletteConfig, PaletteSort,
};
use crate::{
    configs::{
        load_or_init_config, save_gui_config as save_gui_cfg, save_map_selection as save_map_sel,
    },
    image_features::ImageFeatures,
};

const DB_FILE_NAME_PDQHASH: &str = "phdupes_redb";
const DB_FILE_NAME_FEATURES: &str = "phdupes_features";
const DB_FILE_NAME_PIXELHASH: &str = "phdupes_pixelhash";
const DB_FILE_NAME_COEFFICIENTS: &str = "phdupes_coefficients";
const DB_FILE_NAME_IGNORED: &str = "phdupes_ignored";
const DB_FILE_NAME_IGNORED_PDQMAP: &str = "phdupes_ignored_pdqmap";

pub type DbTable = TableDefinition<'static, &'static [u8; 32], &'static [u8]>;

const TABLE_HASH: DbTable = TableDefinition::new("pdqhash");
const TABLE_META: DbTable = TableDefinition::new("file_metadata");
const TABLE_FEATURES: DbTable = TableDefinition::new(DB_FILE_NAME_FEATURES);
const TABLE_COEFFICIENTS: DbTable = TableDefinition::new(DB_FILE_NAME_COEFFICIENTS);
const TABLE_PIXELHASH: DbTable = TableDefinition::new(DB_FILE_NAME_PIXELHASH);
const TABLE_IGNORED: DbTable = TableDefinition::new(DB_FILE_NAME_IGNORED);
const TABLE_IGNORED_PDQMAP: DbTable = TableDefinition::new(DB_FILE_NAME_IGNORED_PDQMAP);

/// Version of the PDQ pipeline whose output is stored in hash_db and coeff_db.
///
/// Bump this whenever something that changes hash bits changes: DCT_FREQ_OFFSET,
/// JAROSZ_WINDOW_DIVISOR, RESIZE_ALG, the luma weights, or the pre-downsample
/// size. The version is written as a leading byte on the stored value and a
/// mismatch is reported as a cache miss, so stale entries are recomputed and
/// overwritten on the next scan instead of being silently mixed with current
/// ones.
///
/// `ignored_db` and `ignored_pdqmap_db` are deliberately NOT versioned and are
/// NEVER deleted by `--prune`. `ignored_db` is keyed by blake3 `content_hash`,
/// so the ignore list survives algorithm changes and works regardless of which
/// directory the duplicate finder is started in; stale entries are only removed
/// explicitly via `--unignore`.
///
/// 1 = original non-reference pipeline, 2 = reference-compatible PDQ.
pub const PDQ_ALGO_VERSION: u8 = 2;

// Encryption overhead: 24-byte nonce + 16-byte Poly1305 tag
const ENCRYPTION_OVERHEAD: usize = 24 + 16;

// Total overhead: encryption only
const TOTAL_OVERHEAD: usize = ENCRYPTION_OVERHEAD;

/// Domain error type for database, crypto, and config operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum DbError {
    Io(std::io::Error),
    Config(ConfigError),
    Redb(redb::Error),
    Postcard(postcard::Error),
    DecryptionFailed { table: &'static str },
    InvalidFormat { table: &'static str, detail: &'static str },
    EncryptionFailed,
    RngFailed(getrandom::Error),
    SystemTime(std::time::SystemTimeError),
    LockTimeout(Duration),
    NoCacheDir,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "database I/O error: {err}"),
            Self::Config(err) => write!(f, "{err}"),
            Self::Redb(err) => write!(f, "redb error: {err}"),
            Self::Postcard(err) => write!(f, "deserialization error: {err}"),
            Self::DecryptionFailed { table } => {
                write!(f, "decryption failed in table '{table}'")
            },
            Self::InvalidFormat { table, detail } => {
                write!(f, "invalid record format in table '{table}': {detail}")
            },
            Self::EncryptionFailed => write!(f, "AEAD encryption failed"),
            Self::RngFailed(err) => write!(f, "random number generator failed: {err}"),
            Self::SystemTime(err) => write!(f, "system clock error: {err}"),
            Self::LockTimeout(dur) => {
                write!(
                    f,
                    "timeout waiting {}s for database lock from another process",
                    dur.as_secs()
                )
            },
            Self::NoCacheDir => write!(f, "no cache directory found"),
        }
    }
}

impl std::error::Error for DbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Config(err) => Some(err),
            Self::Redb(err) => Some(err),
            Self::Postcard(err) => Some(err),
            Self::RngFailed(err) => Some(err),
            Self::SystemTime(err) => Some(err),
            Self::DecryptionFailed { .. }
            | Self::InvalidFormat { .. }
            | Self::EncryptionFailed
            | Self::LockTimeout(_)
            | Self::NoCacheDir => None,
        }
    }
}

impl From<std::io::Error> for DbError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<ConfigError> for DbError {
    fn from(err: ConfigError) -> Self {
        Self::Config(err)
    }
}

impl From<redb::Error> for DbError {
    fn from(err: redb::Error) -> Self {
        Self::Redb(err)
    }
}

impl From<redb::DatabaseError> for DbError {
    fn from(err: redb::DatabaseError) -> Self {
        Self::Redb(err.into())
    }
}

impl From<redb::TransactionError> for DbError {
    fn from(err: redb::TransactionError) -> Self {
        Self::Redb(err.into())
    }
}

impl From<redb::TableError> for DbError {
    fn from(err: redb::TableError) -> Self {
        Self::Redb(err.into())
    }
}

impl From<redb::CommitError> for DbError {
    fn from(err: redb::CommitError) -> Self {
        Self::Redb(err.into())
    }
}

impl From<redb::StorageError> for DbError {
    fn from(err: redb::StorageError) -> Self {
        Self::Redb(err.into())
    }
}

impl From<postcard::Error> for DbError {
    fn from(err: postcard::Error) -> Self {
        Self::Postcard(err)
    }
}

impl From<getrandom::Error> for DbError {
    fn from(err: getrandom::Error) -> Self {
        Self::RngFailed(err)
    }
}

impl From<std::time::SystemTimeError> for DbError {
    fn from(err: std::time::SystemTimeError) -> Self {
        Self::SystemTime(err)
    }
}

/// Hash algorithm selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum HashAlgorithm {
    #[default]
    PdqHash,
}

/// PDQ coefficients stored separately (only in duplicate finder mode).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedCoefficients {
    pub coefficients: Vec<f32>,
}

impl CachedCoefficients {
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_stdvec(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

/// Entry in the ignored files database.
/// Key: blake3 content_hash (32 bytes).
/// Every file in a duplicate group is registered here on scan completion
/// with `ignored: false`. The flag is set to `true` when the user presses Q.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IgnoredEntry {
    pub pdqhash: Option<[u8; 32]>,
    pub group_uuid: [u8; 16], // 128-bit UUID grouping files by similarity
    pub timestamp: u64,       // Unix epoch seconds when the entry was created/updated
    pub ignored: bool,        // true = user explicitly ignored this file
}

impl IgnoredEntry {
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_stdvec(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

/// Result of background enrichment for a file.
/// This struct is designed to be extensible - add new fields as needed.
#[derive(Debug, Clone)]
pub struct EnrichmentResult {
    pub unique_file_id: u128,
    pub content_hash: [u8; 32],
    pub gps_pos: Option<Point<f64>>,
    pub exif_timestamp: Option<i64>,
    pub features: Option<ImageFeatures>,
}

pub struct AppContext {
    pub env: Arc<Database>,
    pub hash_db: DbTable,
    pub meta_db: DbTable,
    pub feature_db: DbTable,
    pub coeff_db: DbTable, // Separate DB for PDQ coefficients
    pub pixel_db: DbTable,
    pub ignored_db: DbTable, // Registered/ignored files (duplicate finder)
    pub ignored_pdqmap_db: DbTable, // Maps pdqhash → UUID for cross-session stability
    pub content_key: [u8; 32],
    pub meta_key: [u8; 32],
    pub grouping_config: GroupingConfig,
    pub gui_config: GuiConfig,
    pub locations: HashMap<String, Point<f64>>,
    pub map_providers: HashMap<String, String>,
    pub selected_provider: String,
    pub tile_cache_path: PathBuf, // Path for walkers to store images
    cipher: XChaCha20Poly1305,
    logged_ignored_corrupt: AtomicBool,
}

impl fmt::Debug for AppContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppContext")
            .field("grouping_config", &self.grouping_config)
            .field("gui_config", &self.gui_config)
            .field("locations_count", &self.locations.len())
            .field("selected_provider", &self.selected_provider)
            .field("tile_cache_path", &self.tile_cache_path)
            .field("content_key", &"[redacted]")
            .field("meta_key", &"[redacted]")
            .field("cipher", &"[redacted]")
            .finish_non_exhaustive()
    }
}

impl Drop for AppContext {
    fn drop(&mut self) {
        self.content_key.zeroize();
        self.meta_key.zeroize();
    }
}

/// Database update type.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum HashValue {
    PdqHash([u8; 32]),
}

// (Meta Update, Hash Update, Feature Update, Coefficients Update, Pixel Hash)
pub type DbUpdate = (
    Option<([u8; 32], [u8; 32])>,           // Meta: meta_key -> content_hash
    Option<([u8; 32], HashValue)>,          // Hash: content_hash -> pdqhash
    Option<([u8; 32], ImageFeatures)>,      // Features: content_hash -> ImageFeatures
    Option<([u8; 32], CachedCoefficients)>, /* Coefficients: content_hash -> coefficients (dupe mode only) */
    Option<([u8; 32], [u8; 32])>,           // Pixel Hash
);

/// Pair of `(content_hash, optional_pdqhash)` used for duplicate group registration.
pub type GroupEntry = ([u8; 32], Option<[u8; 32]>);

/// Compute the meta_key from file metadata.
///
/// The meta_key is derived from:
/// - `mtime_ns`: modification time in nanoseconds since UNIX_EPOCH
/// - `size`: file size in bytes
/// - `unique_file_id`: filesystem identity (dev, inode or equivalent)
///
/// This allows cache hits even after file renames (same inode).
#[inline]
#[must_use]
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

/// Convenience function to compute meta_key from `std::fs::Metadata`.
#[inline]
#[must_use]
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

/// Create a `DbUpdate` for caching enrichment results (view mode).
/// This is the canonical way to create database updates.
#[must_use]
pub fn create_feature_update(
    meta_key_secret: &[u8; 32],
    path: &Path,
    unique_file_id: u128,
    content_hash: [u8; 32],
    features: ImageFeatures,
) -> Option<DbUpdate> {
    let metadata = fs::metadata(path).ok()?;
    let meta_key = compute_meta_key_from_metadata(meta_key_secret, &metadata, unique_file_id);

    Some((Some((meta_key, content_hash)), None, Some((content_hash, features)), None, None))
}

struct PendingBatch {
    meta: HashMap<[u8; 32], [u8; 32]>,
    hash: HashMap<[u8; 32], HashValue>,
    feature: HashMap<[u8; 32], ImageFeatures>,
    coeff: HashMap<[u8; 32], CachedCoefficients>,
    pixel: HashMap<[u8; 32], [u8; 32]>,
}

impl PendingBatch {
    fn new() -> Self {
        Self {
            meta: HashMap::new(),
            hash: HashMap::new(),
            feature: HashMap::new(),
            coeff: HashMap::new(),
            pixel: HashMap::new(),
        }
    }

    fn push(&mut self, update: DbUpdate) {
        let (m, h, f, c, p) = update;
        if let Some((k, v)) = m {
            self.meta.insert(k, v);
        }
        if let Some((k, v)) = h {
            self.hash.insert(k, v);
        }
        if let Some((k, v)) = f {
            self.feature.insert(k, v);
        }
        if let Some((k, v)) = c {
            self.coeff.insert(k, v);
        }
        if let Some((k, v)) = p {
            self.pixel.insert(k, v);
        }
    }

    fn is_empty(&self) -> bool {
        self.meta.is_empty()
            && self.hash.is_empty()
            && self.feature.is_empty()
            && self.coeff.is_empty()
            && self.pixel.is_empty()
    }

    fn total_len(&self) -> usize {
        self.meta.len() + self.hash.len() + self.feature.len() + self.coeff.len() + self.pixel.len()
    }

    fn is_full(&self, max_buffer: usize) -> bool {
        self.meta.len() >= max_buffer
            || self.hash.len() >= max_buffer
            || self.feature.len() >= max_buffer
            || self.coeff.len() >= max_buffer
            || self.pixel.len() >= max_buffer
    }

    fn clear(&mut self) {
        self.meta.clear();
        self.hash.clear();
        self.feature.clear();
        self.coeff.clear();
        self.pixel.clear();
    }
}

impl AppContext {
    fn open_database_with_retry(db_path: &Path, cache_size: usize) -> Result<Database, DbError> {
        match Database::builder()
            .set_cache_size(cache_size)
            .set_concurrency_mode(ConcurrencyMode::MultiWriter)
            .create(db_path)
        {
            Ok(db) => Ok(db),
            Err(redb::DatabaseError::DatabaseAlreadyOpen) => {
                eprintln!("[WARN-DB] Database is locked by another process. Waiting for lock...");
                let start = Instant::now();
                let timeout = Duration::from_secs(10);
                loop {
                    if start.elapsed() > timeout {
                        return Err(DbError::LockTimeout(timeout));
                    }
                    thread::sleep(Duration::from_millis(100));
                    match Database::builder()
                        .set_cache_size(cache_size)
                        .set_concurrency_mode(ConcurrencyMode::MultiWriter)
                        .create(db_path)
                    {
                        Ok(db) => return Ok(db),
                        Err(redb::DatabaseError::DatabaseAlreadyOpen) => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
            },
            Err(e) => Err(e.into()),
        }
    }

    fn ensure_tables(env: &Database) -> Result<(), DbError> {
        let write_txn = env.begin_write()?;
        let _ = write_txn.open_table(TABLE_HASH)?;
        let _ = write_txn.open_table(TABLE_META)?;
        let _ = write_txn.open_table(TABLE_FEATURES)?;
        let _ = write_txn.open_table(TABLE_COEFFICIENTS)?;
        let _ = write_txn.open_table(TABLE_PIXELHASH)?;
        let _ = write_txn.open_table(TABLE_IGNORED)?;
        let _ = write_txn.open_table(TABLE_IGNORED_PDQMAP)?;
        write_txn.commit()?;
        Ok(())
    }

    fn derive_keys(master_key_bytes: &[u8; 32]) -> ([u8; 32], [u8; 32], XChaCha20Poly1305) {
        let content_key = blake3::derive_key("phdupes:content_key", master_key_bytes);
        let meta_key = blake3::derive_key("phdupes:meta_key", master_key_bytes);
        let encryption_key =
            Zeroizing::new(blake3::derive_key("phdupes:encryption_key", master_key_bytes));
        let cipher = XChaCha20Poly1305::new((&*encryption_key).into());
        (content_key, meta_key, cipher)
    }

    /// Open context using default config and cache directories.
    ///
    /// # Errors
    /// Returns [`DbError`] if configuration loading, master key validation, or
    /// database initialization fails.
    pub fn new() -> Result<Self, DbError> {
        Self::with_algorithm(HashAlgorithm::PdqHash)
    }

    /// Open context with the selected hash algorithm.
    ///
    /// # Errors
    /// Returns [`DbError`] if configuration loading, master key validation,
    /// directory creation, or database opening fails.
    pub fn with_algorithm(algorithm: HashAlgorithm) -> Result<Self, DbError> {
        let cache_dir = dirs::cache_dir().ok_or(DbError::NoCacheDir)?;
        fs::create_dir_all(&cache_dir)?;
        let tile_cache_path = cache_dir.join("phdupes_tiles");
        fs::create_dir_all(&tile_cache_path)?;

        let loaded = load_or_init_config()?;
        let (content_key, meta_key, cipher) = Self::derive_keys(&loaded.master_key_bytes);

        let db_file_name = match algorithm {
            HashAlgorithm::PdqHash => DB_FILE_NAME_PDQHASH,
        };
        let db_path = cache_dir.join(db_file_name);

        let cache_size = (loaded.db_size_mb as usize).saturating_mul(1024 * 1024);
        eprintln!(
            "[DEBUG-DB] Setting redb cache size to {} bytes ({} MiB) at {:?}",
            cache_size, loaded.db_size_mb, db_path
        );

        let env = Self::open_database_with_retry(&db_path, cache_size)?;
        Self::ensure_tables(&env)?;

        Ok(Self {
            env: Arc::new(env),
            hash_db: TABLE_HASH,
            meta_db: TABLE_META,
            feature_db: TABLE_FEATURES,
            coeff_db: TABLE_COEFFICIENTS,
            pixel_db: TABLE_PIXELHASH,
            ignored_db: TABLE_IGNORED,
            ignored_pdqmap_db: TABLE_IGNORED_PDQMAP,
            content_key,
            meta_key,
            grouping_config: loaded.grouping,
            gui_config: loaded.gui,
            locations: loaded.locations,
            map_providers: loaded.map_providers,
            selected_provider: loaded.selected_provider,
            tile_cache_path,
            cipher,
            logged_ignored_corrupt: AtomicBool::new(false),
        })
    }

    #[cfg(test)]
    fn open_for_test(db_path: &Path, master_key: [u8; 32]) -> Result<Self, DbError> {
        let (content_key, meta_key, cipher) = Self::derive_keys(&master_key);
        let env = Self::open_database_with_retry(db_path, 16 * 1024 * 1024)?;
        Self::ensure_tables(&env)?;

        Ok(Self {
            env: Arc::new(env),
            hash_db: TABLE_HASH,
            meta_db: TABLE_META,
            feature_db: TABLE_FEATURES,
            coeff_db: TABLE_COEFFICIENTS,
            pixel_db: TABLE_PIXELHASH,
            ignored_db: TABLE_IGNORED,
            ignored_pdqmap_db: TABLE_IGNORED_PDQMAP,
            content_key,
            meta_key,
            grouping_config: GroupingConfig::default(),
            gui_config: GuiConfig::default(),
            locations: HashMap::new(),
            map_providers: HashMap::new(),
            selected_provider: String::new(),
            tile_cache_path: db_path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf(),
            cipher,
            logged_ignored_corrupt: AtomicBool::new(false),
        })
    }

    fn log_ignored_corrupt_once(&self, context: &str) {
        if !self.logged_ignored_corrupt.swap(true, Ordering::Relaxed) {
            eprintln!(
                "[ERROR-IGNORE] {context}: failed to decrypt or deserialize ignored entry (keeping entry intact)"
            );
        }
    }

    /// Encrypt data using AAD to bind ciphertext to `db_key`, reusing `out`.
    /// Storage format: `[Nonce (24 bytes) || Ciphertext || Tag (16 bytes)]`.
    fn encrypt_into(
        cipher: &XChaCha20Poly1305,
        db_key: &[u8],
        data: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), DbError> {
        let mut nonce_bytes = [0u8; 24];
        getrandom::fill(&mut nonce_bytes)?;
        let nonce = XNonce::from(nonce_bytes);

        out.clear();
        out.reserve(ENCRYPTION_OVERHEAD + data.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(data);

        let tag = cipher
            .encrypt_inout_detached(&nonce, db_key, (&mut out[24..]).into())
            .map_err(|_| DbError::EncryptionFailed)?;
        out.extend_from_slice(tag.as_slice());
        Ok(())
    }

    /// Encrypt data using AAD to bind ciphertext to `db_key`.
    /// Storage format: `[Nonce (24 bytes) || Ciphertext || Tag (16 bytes)]`.
    fn encrypt_value(
        cipher: &XChaCha20Poly1305,
        db_key: &[u8],
        data: &[u8],
    ) -> Result<Vec<u8>, DbError> {
        let mut nonce_bytes = [0u8; 24];
        getrandom::fill(&mut nonce_bytes)?;
        let nonce = XNonce::from(nonce_bytes);

        let ciphertext = cipher
            .encrypt(&nonce, Payload { msg: data, aad: db_key })
            .map_err(|_| DbError::EncryptionFailed)?;

        let mut out = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Decrypt value using AAD.
    /// Returns `None` if tag verification fails (e.g., wrong key, tampering) or decryption fails.
    fn decrypt_value(&self, db_key: &[u8], data: &[u8]) -> Option<Vec<u8>> {
        if data.len() < TOTAL_OVERHEAD {
            return None;
        }

        let nonce = XNonce::try_from(&data[0..24]).ok()?;
        let ciphertext = &data[24..];

        self.cipher.decrypt(&nonce, Payload { msg: ciphertext, aad: db_key }).ok()
    }

    // --- DATABASE ACCESS ---

    /// Get PDQ hash (256-bit) from database.
    ///
    /// # Errors
    /// Returns [`DbError`] on database read failure or AEAD decryption failure.
    pub fn get_pdqhash(&self, content_hash: &[u8; 32]) -> Result<Option<[u8; 32]>, DbError> {
        let txn = self.env.begin_read()?;
        let table = txn.open_table(self.hash_db)?;
        let Some(encrypted_guard) = table.get(content_hash)? else {
            return Ok(None);
        };

        let Some(decrypted) = self.decrypt_value(content_hash, encrypted_guard.value()) else {
            eprintln!("[ERROR-DB] get_pdqhash Corrupted content_hash={:x?}", content_hash);
            return Err(DbError::DecryptionFailed { table: "pdqhash" });
        };

        // Stored as [PDQ_ALGO_VERSION || 32-byte hash]. A different version
        // means the entry came from another PDQ pipeline, so report a miss.
        match decrypted.split_first() {
            Some((&PDQ_ALGO_VERSION, rest)) if rest.len() == 32 => {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(rest);
                Ok(Some(arr))
            },
            _ => Ok(None),
        }
    }

    /// Get cached features from database.
    ///
    /// # Errors
    /// Returns [`DbError`] on database read failure, decryption failure, or deserialization failure.
    pub fn get_features(&self, content_hash: &[u8; 32]) -> Result<Option<ImageFeatures>, DbError> {
        let txn = self.env.begin_read()?;
        let table = txn.open_table(self.feature_db)?;
        let Some(encrypted_guard) = table.get(content_hash)? else {
            return Ok(None);
        };

        let Some(decrypted) = self.decrypt_value(content_hash, encrypted_guard.value()) else {
            eprintln!("[ERROR] get_features Corrupted ch={:x?}", content_hash);
            return Err(DbError::DecryptionFailed { table: DB_FILE_NAME_FEATURES });
        };

        Ok(Some(ImageFeatures::from_bytes(&decrypted)?))
    }

    /// Get the stored PDQ quality (0-100) for a content hash, if any.
    /// `None` means the record predates quality storage, was written by view mode,
    /// or could not be read.
    #[must_use]
    pub fn get_pdq_quality(&self, content_hash: &[u8; 32]) -> Option<u16> {
        self.get_features(content_hash).ok().flatten().and_then(|f| f.pdq_quality())
    }

    /// Get coefficients from the separate `coeff_db`.
    ///
    /// # Errors
    /// Returns [`DbError`] on database read failure, decryption failure, or deserialization failure.
    pub fn get_coefficients(&self, content_hash: &[u8; 32]) -> Result<Option<Vec<f32>>, DbError> {
        let txn = self.env.begin_read()?;
        let table = txn.open_table(self.coeff_db)?;
        let Some(encrypted_guard) = table.get(content_hash)? else {
            return Ok(None);
        };

        let Some(decrypted) = self.decrypt_value(content_hash, encrypted_guard.value()) else {
            return Err(DbError::DecryptionFailed { table: DB_FILE_NAME_COEFFICIENTS });
        };

        // Stored as [PDQ_ALGO_VERSION || postcard(CachedCoefficients)].
        let Some((&PDQ_ALGO_VERSION, rest)) = decrypted.split_first() else {
            // Older algorithm version or empty payload: treat as absent, not corrupt.
            return Ok(None);
        };

        let cached = CachedCoefficients::from_bytes(rest)?;
        Ok(Some(cached.coefficients))
    }

    /// Retrieve content hash from metadata key.
    /// Strictly expects `[ContentHash (32) || Timestamp (8)]`.
    ///
    /// # Errors
    /// Returns [`DbError`] on database read failure, decryption failure, or length mismatch.
    pub fn get_content_hash(&self, meta_hash: &[u8; 32]) -> Result<Option<[u8; 32]>, DbError> {
        let txn = self.env.begin_read()?;
        let table = txn.open_table(self.meta_db)?;
        let Some(encrypted_guard) = table.get(meta_hash)? else {
            return Ok(None);
        };

        let Some(decrypted) = self.decrypt_value(meta_hash, encrypted_guard.value()) else {
            return Err(DbError::DecryptionFailed { table: "file_metadata" });
        };

        if decrypted.len() != 40 {
            eprintln!("[ERROR] get_content_hash Corrupted mh={:x?}", meta_hash);
            return Err(DbError::InvalidFormat {
                table: "file_metadata",
                detail: "expected 40 bytes",
            });
        }

        let mut arr = [0u8; 32];
        arr.copy_from_slice(&decrypted[0..32]);
        Ok(Some(arr))
    }

    /// Retrieve raw pixel hash by `content_hash`.
    ///
    /// # Errors
    /// Returns [`DbError`] on database read failure, decryption failure, or length mismatch.
    pub fn get_pixel_hash(&self, content_hash: &[u8; 32]) -> Result<Option<[u8; 32]>, DbError> {
        let txn = self.env.begin_read()?;
        let table = txn.open_table(self.pixel_db)?;
        let Some(encrypted_guard) = table.get(content_hash)? else {
            return Ok(None);
        };

        let Some(decrypted) = self.decrypt_value(content_hash, encrypted_guard.value()) else {
            return Err(DbError::DecryptionFailed { table: DB_FILE_NAME_PIXELHASH });
        };

        let arr: [u8; 32] = decrypted.try_into().map_err(|_| DbError::InvalidFormat {
            table: DB_FILE_NAME_PIXELHASH,
            detail: "expected 32 bytes",
        })?;
        Ok(Some(arr))
    }

    /// Look up cached features from the database in a single read transaction.
    #[must_use]
    pub fn lookup_cached_features(
        &self,
        metadata: &fs::Metadata,
        unique_file_id: u128,
    ) -> Option<ImageFeatures> {
        let meta_key = compute_meta_key_from_metadata(&self.meta_key, metadata, unique_file_id);

        eprintln!(
            "[DEBUG-CACHE] lookup_cached_features: size={}, unique_file_id={}, meta_key={:?}",
            metadata.len(),
            unique_file_id,
            hex::encode(&meta_key[..8])
        );

        let txn = self.env.begin_read().ok()?;
        let meta_table = txn.open_table(self.meta_db).ok()?;
        let feature_table = txn.open_table(self.feature_db).ok()?;

        if let Ok(Some(encrypted_guard)) = meta_table.get(&meta_key)
            && let Some(decrypted) = self.decrypt_value(&meta_key, encrypted_guard.value())
            && decrypted.len() == 40
        {
            let mut content_hash = [0u8; 32];
            content_hash.copy_from_slice(&decrypted[0..32]);
            if content_hash == [0u8; 32] {
                return None;
            }

            eprintln!("[DEBUG-CACHE]   Found content_hash: {:?}", hex::encode(&content_hash[..8]));

            if let Ok(Some(features_guard)) = feature_table.get(&content_hash)
                && let Some(dec_features) =
                    self.decrypt_value(&content_hash, features_guard.value())
                && let Ok(features) = ImageFeatures::from_bytes(&dec_features)
            {
                eprintln!(
                    "[DEBUG-CACHE]   Found features: resolution={:?}, orientation={}, gps_pos={:#?}, exif_timestamp={:?}",
                    features.resolution(),
                    features.orientation(),
                    features.gps_pos(),
                    features.exif_timestamp()
                );
                return Some(features);
            } else {
                eprintln!("[DEBUG-CACHE]   No features found for content_hash");
            }
        }

        eprintln!("[DEBUG-CACHE]   Not found in database");
        None
    }

    /// Batch lookup of cached features for multiple files.
    /// Returns `HashMap` mapping `unique_file_id` to `ImageFeatures`.
    #[must_use]
    pub fn lookup_cached_features_batch(
        &self,
        files: &[(u128, u64, u64)], // (unique_file_id, size, mtime_ns)
    ) -> HashMap<u128, ImageFeatures> {
        let mut results = HashMap::with_capacity(files.len());

        let Ok(txn) = self.env.begin_read() else {
            return results;
        };
        let Ok(meta_table) = txn.open_table(self.meta_db) else {
            return results;
        };
        let Ok(feature_table) = txn.open_table(self.feature_db) else {
            return results;
        };

        for &(unique_file_id, size, mtime_ns) in files {
            let meta_key = compute_meta_key(&self.meta_key, mtime_ns, size, unique_file_id);

            if let Ok(Some(encrypted_guard)) = meta_table.get(&meta_key)
                && let Some(decrypted) = self.decrypt_value(&meta_key, encrypted_guard.value())
                && decrypted.len() == 40
            {
                let mut content_hash = [0u8; 32];
                content_hash.copy_from_slice(&decrypted[0..32]);

                if content_hash == [0u8; 32] {
                    continue;
                }

                if let Ok(Some(encrypted_features_guard)) = feature_table.get(&content_hash)
                    && let Some(decrypted_features) =
                        self.decrypt_value(&content_hash, encrypted_features_guard.value())
                    && let Ok(features) = ImageFeatures::from_bytes(&decrypted_features)
                {
                    results.insert(unique_file_id, features);
                }
            }
        }

        results
    }

    /// Prune cache entries older than `max_age_seconds`.
    /// 1. Iterates `meta_db`: removes entries where `timestamp < cutoff`.
    /// 2. Collects active `content_hash`es.
    /// 3. Sweeps `hash_db`, `feature_db`, `coeff_db`, `pixel_db`: removes entries not in active set.
    ///
    /// Note: `ignored_db` and `ignored_pdqmap_db` are NEVER touched by `prune`.
    ///
    /// # Errors
    /// Returns [`DbError`] on system clock, transaction, or table operation failures.
    pub fn prune(&self, max_age_seconds: u64) -> Result<(usize, usize), DbError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let cutoff = now.saturating_sub(max_age_seconds);

        let txn = self.env.begin_write()?;
        let mut valid_content_hashes = HashSet::new();
        let mut meta_remove_count = 0;
        let mut hash_remove_count = 0;

        // 1. Scan MetaDB
        {
            let mut table = txn.open_table(self.meta_db)?;
            let mut keys_to_delete = Vec::new();
            for item in table.iter()? {
                let (key_guard, val_guard) = item?;
                let key = key_guard.value();
                let val_bytes = val_guard.value();

                if let Some(decrypted) = self.decrypt_value(key, val_bytes)
                    && decrypted.len() == 40
                    && let Ok(ts_bytes) = <[u8; 8]>::try_from(&decrypted[32..40])
                {
                    let last_seen = u64::from_le_bytes(ts_bytes);
                    if last_seen < cutoff {
                        keys_to_delete.push(*key);
                    } else {
                        let mut ch = [0u8; 32];
                        ch.copy_from_slice(&decrypted[0..32]);
                        valid_content_hashes.insert(ch);
                    }
                } else {
                    keys_to_delete.push(*key);
                }
            }
            for key in keys_to_delete {
                table.remove(&key)?;
                meta_remove_count += 1;
            }
        }

        // 2. Sweep HashDB
        {
            let mut table = txn.open_table(self.hash_db)?;
            let mut keys_to_delete = Vec::new();
            for item in table.iter()? {
                let (key_guard, _) = item?;
                let k = *key_guard.value();
                if !valid_content_hashes.contains(&k) {
                    keys_to_delete.push(k);
                }
            }
            for k in keys_to_delete {
                table.remove(&k)?;
                hash_remove_count += 1;
            }
        }

        // 3. Sweep FeatureDB
        Self::sweep_orphaned_keys(&txn, self.feature_db, &valid_content_hashes)?;

        // 4. Sweep CoeffDB (PDQ coefficients)
        Self::sweep_orphaned_keys(&txn, self.coeff_db, &valid_content_hashes)?;

        // 5. Sweep PixelDB
        Self::sweep_orphaned_keys(&txn, self.pixel_db, &valid_content_hashes)?;

        txn.commit()?;
        Ok((meta_remove_count, hash_remove_count))
    }

    fn sweep_orphaned_keys(
        txn: &WriteTransaction,
        table_def: DbTable,
        valid_content_hashes: &HashSet<[u8; 32]>,
    ) -> Result<(), DbError> {
        let mut table = txn.open_table(table_def)?;
        let mut keys_to_delete = Vec::new();
        for item in table.iter()? {
            let (key_guard, _) = item?;
            let k = *key_guard.value();
            if !valid_content_hashes.contains(&k) {
                keys_to_delete.push(k);
            }
        }
        for k in keys_to_delete {
            table.remove(&k)?;
        }
        Ok(())
    }

    /// Spawn the background writer thread that coalesces and batches cache writes.
    #[must_use]
    pub fn start_db_writer(&self, rx: Receiver<DbUpdate>) -> thread::JoinHandle<()> {
        let env = self.env.clone();
        let meta_db = self.meta_db;
        let hash_db = self.hash_db;
        let feature_db = self.feature_db;
        let coeff_db = self.coeff_db;
        let pixel_db = self.pixel_db;
        let cipher = self.cipher.clone();

        let builder = thread::Builder::new().name("db-writer".to_string());
        let run = move || {
            let mut batch = PendingBatch::new();
            let mut plain_buf = Vec::with_capacity(512);
            let mut enc_buf = Vec::with_capacity(512);

            let mut last_flush = Instant::now();
            let flush_interval = Duration::from_secs(1);
            let max_buffer = 1000;
            // Hard cap on total buffered updates. These updates are a recomputable
            // cache, so if writes keep failing (e.g. disk full) we drop
            // the backlog past this point rather than growing memory without bound.
            let hard_cap = max_buffer * 16;
            // Tracks whether the previous write_batch failed, so retries are
            // throttled to the flush interval instead of spinning on the buffer.
            let mut last_write_failed = false;

            loop {
                let was_empty = batch.is_empty();
                let mut disconnected = false;

                match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(update) => {
                        if was_empty {
                            // Start batch window from the first item after an idle period
                            last_flush = Instant::now();
                        }
                        batch.push(update);

                        // Drain already-queued updates up to max_buffer to maximize batching
                        while !batch.is_full(max_buffer) {
                            match rx.try_recv() {
                                Ok(more) => batch.push(more),
                                Err(TryRecvError::Empty) => break,
                                Err(TryRecvError::Disconnected) => {
                                    disconnected = true;
                                    break;
                                },
                            }
                        }
                    },
                    Err(RecvTimeoutError::Timeout) => {},
                    Err(RecvTimeoutError::Disconnected) => {
                        disconnected = true;
                    },
                }

                if disconnected {
                    if !batch.is_empty() {
                        let mut attempts = 0;
                        loop {
                            match Self::write_batch(
                                &cipher,
                                &env,
                                meta_db,
                                hash_db,
                                feature_db,
                                coeff_db,
                                pixel_db,
                                &batch,
                                &mut plain_buf,
                                &mut enc_buf,
                            ) {
                                Ok(()) => break,
                                Err(e) => {
                                    attempts += 1;
                                    eprintln!(
                                        "[ERROR-DB] Final write_batch failed (attempt {}): {}",
                                        attempts, e
                                    );
                                    if attempts >= 3 {
                                        eprintln!(
                                            "[ERROR-DB] Giving up on final flush; {} cache updates lost",
                                            batch.total_len()
                                        );
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(50));
                                },
                            }
                        }
                    }
                    break;
                }

                let any_pending = !batch.is_empty();
                let buffer_full = batch.is_full(max_buffer);
                let time_elapsed = last_flush.elapsed() >= flush_interval;

                let should_flush =
                    any_pending && (time_elapsed || (buffer_full && !last_write_failed));

                if should_flush {
                    match Self::write_batch(
                        &cipher,
                        &env,
                        meta_db,
                        hash_db,
                        feature_db,
                        coeff_db,
                        pixel_db,
                        &batch,
                        &mut plain_buf,
                        &mut enc_buf,
                    ) {
                        Ok(()) => {
                            batch.clear();
                            last_write_failed = false;
                        },
                        Err(e) => {
                            eprintln!("[ERROR-DB] write_batch failed: {} (will retry)", e);
                            last_write_failed = true;

                            let buffered = batch.total_len();
                            if buffered > hard_cap {
                                eprintln!(
                                    "[ERROR-DB] Dropping {} buffered cache updates after repeated write failures",
                                    buffered
                                );
                                batch.clear();
                            }
                        },
                    }
                    last_flush = Instant::now();
                }
            }
        };

        builder.spawn(run).unwrap_or_else(|e| {
            eprintln!("[WARN-DB] Failed to spawn named db-writer thread ({}); falling back", e);
            thread::spawn(|| {})
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn write_batch(
        cipher: &XChaCha20Poly1305,
        env: &Database,
        meta_db: DbTable,
        hash_db: DbTable,
        feature_db: DbTable,
        coeff_db: DbTable,
        pixel_db: DbTable,
        batch: &PendingBatch,
        plain_buf: &mut Vec<u8>,
        enc_buf: &mut Vec<u8>,
    ) -> Result<(), DbError> {
        let txn = env.begin_write()?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let now_bytes = now.to_le_bytes();

        // 1. Meta Updates: [ContentHash (32) || Timestamp (8)]
        if !batch.meta.is_empty() {
            let mut table = txn.open_table(meta_db)?;
            for (key, val) in &batch.meta {
                plain_buf.clear();
                plain_buf.extend_from_slice(val);
                plain_buf.extend_from_slice(&now_bytes);

                Self::encrypt_into(cipher, key, plain_buf, enc_buf)?;
                table.insert(key, enc_buf.as_slice())?;
            }
        }

        // 2. Hash Updates: [PDQ_ALGO_VERSION (1) || pdqhash (32)]
        if !batch.hash.is_empty() {
            let mut table = txn.open_table(hash_db)?;
            for (key, val) in &batch.hash {
                match val {
                    HashValue::PdqHash(pdqhash) => {
                        plain_buf.clear();
                        plain_buf.push(PDQ_ALGO_VERSION);
                        plain_buf.extend_from_slice(pdqhash);

                        Self::encrypt_into(cipher, key, plain_buf, enc_buf)?;
                        table.insert(key, enc_buf.as_slice())?;
                    },
                }
            }
        }

        // 3. Feature Updates
        if !batch.feature.is_empty() {
            let mut table = txn.open_table(feature_db)?;
            for (key, features) in &batch.feature {
                match features.to_bytes() {
                    Ok(bytes) => {
                        Self::encrypt_into(cipher, key, &bytes, enc_buf)?;
                        table.insert(key, enc_buf.as_slice())?;
                    },
                    Err(e) => {
                        eprintln!(
                            "[ERROR-DB] Skipping ImageFeatures entry due to serialization error: {}",
                            e
                        );
                    },
                }
            }
        }

        // 4. Coefficient Updates: [PDQ_ALGO_VERSION (1) || postcard(CachedCoefficients)]
        if !batch.coeff.is_empty() {
            let mut table = txn.open_table(coeff_db)?;
            for (key, coeffs) in &batch.coeff {
                match coeffs.to_bytes() {
                    Ok(bytes) => {
                        plain_buf.clear();
                        plain_buf.push(PDQ_ALGO_VERSION);
                        plain_buf.extend_from_slice(&bytes);

                        Self::encrypt_into(cipher, key, plain_buf, enc_buf)?;
                        table.insert(key, enc_buf.as_slice())?;
                    },
                    Err(e) => {
                        eprintln!(
                            "[ERROR-DB] Skipping CachedCoefficients entry due to serialization error: {}",
                            e
                        );
                    },
                }
            }
        }

        // 5. Pixel Updates
        if !batch.pixel.is_empty() {
            let mut table = txn.open_table(pixel_db)?;
            for (key, val) in &batch.pixel {
                Self::encrypt_into(cipher, key, val, enc_buf)?;
                table.insert(key, enc_buf.as_slice())?;
            }
        }

        txn.commit()?;
        Ok(())
    }

    /// Iterate all features in the database for building search index.
    /// Calls `callback` for each `(content_hash, ImageFeatures)` pair.
    ///
    /// # Errors
    /// Returns [`DbError`] on database read/table failures.
    #[allow(dead_code)]
    pub fn iterate_all_features<F>(&self, mut callback: F) -> Result<usize, DbError>
    where
        F: FnMut(&[u8; 32], &ImageFeatures),
    {
        let txn = self.env.begin_read()?;
        let table = txn.open_table(self.feature_db)?;
        let mut count = 0;
        let mut corrupt_count = 0usize;

        for result in table.iter()? {
            let (key_guard, encrypted_guard) = result?;
            let content_hash = *key_guard.value();
            let encrypted = encrypted_guard.value();

            let Some(decrypted) = self.decrypt_value(&content_hash, encrypted) else {
                corrupt_count += 1;
                continue;
            };
            match ImageFeatures::from_bytes(&decrypted) {
                Ok(features) => {
                    callback(&content_hash, &features);
                    count += 1;
                },
                Err(_) => {
                    corrupt_count += 1;
                },
            }
        }

        if corrupt_count > 0 {
            eprintln!(
                "[ERROR-DB] iterate_all_features skipped {} corrupted or undecryptable entries",
                corrupt_count
            );
        }

        Ok(count)
    }

    // --- Ignored Files Database ---

    /// Check if a file has been explicitly ignored (`ignored == true`).
    #[must_use]
    pub fn is_ignored(&self, content_hash: &[u8; 32]) -> bool {
        let Ok(txn) = self.env.begin_read() else {
            return false;
        };
        let Ok(table) = txn.open_table(self.ignored_db) else {
            return false;
        };
        match table.get(content_hash) {
            Ok(Some(encrypted_guard)) => {
                let Some(decrypted) = self.decrypt_value(content_hash, encrypted_guard.value())
                else {
                    self.log_ignored_corrupt_once("is_ignored");
                    return false;
                };
                match IgnoredEntry::from_bytes(&decrypted) {
                    Ok(entry) => entry.ignored,
                    Err(_) => {
                        self.log_ignored_corrupt_once("is_ignored");
                        false
                    },
                }
            },
            _ => false,
        }
    }

    /// Get the group UUID for a file (works for both ignored and registered-only files).
    #[must_use]
    pub fn get_group_uuid(&self, content_hash: &[u8; 32]) -> Option<[u8; 16]> {
        let txn = self.env.begin_read().ok()?;
        let table = txn.open_table(self.ignored_db).ok()?;
        let encrypted_guard = table.get(content_hash).ok()??;
        let Some(decrypted) = self.decrypt_value(content_hash, encrypted_guard.value()) else {
            self.log_ignored_corrupt_once("get_group_uuid");
            return None;
        };
        match IgnoredEntry::from_bytes(&decrypted) {
            Ok(entry) => Some(entry.group_uuid),
            Err(_) => {
                self.log_ignored_corrupt_once("get_group_uuid");
                None
            },
        }
    }

    /// Look up an existing UUID for a group within `txn`.
    /// First checks `ignored_pdqmap_db` by `pdqhash`, then falls back to checking
    /// `ignored_db` by `content_hash` so groups without a PDQ hash also preserve their UUID.
    fn find_uuid_in_txn(
        &self,
        txn: &WriteTransaction,
        group: &[GroupEntry],
    ) -> Result<Option<[u8; 16]>, DbError> {
        {
            let pdqmap_table = txn.open_table(self.ignored_pdqmap_db)?;
            for (_, pdqhash_opt) in group {
                if let Some(pdqhash) = pdqhash_opt
                    && let Some(encrypted_guard) = pdqmap_table.get(pdqhash)?
                {
                    if let Some(decrypted) = self.decrypt_value(pdqhash, encrypted_guard.value())
                        && decrypted.len() == 16
                    {
                        let mut uuid = [0u8; 16];
                        uuid.copy_from_slice(&decrypted);
                        return Ok(Some(uuid));
                    }
                    self.log_ignored_corrupt_once("find_uuid_in_txn (pdqmap)");
                }
            }
        }

        // Fallback: check if any file in the group already has an entry in ignored_db
        {
            let ignored_table = txn.open_table(self.ignored_db)?;
            for (content_hash, _) in group {
                if let Some(encrypted_guard) = ignored_table.get(content_hash)? {
                    if let Some(decrypted) =
                        self.decrypt_value(content_hash, encrypted_guard.value())
                        && let Ok(entry) = IgnoredEntry::from_bytes(&decrypted)
                    {
                        return Ok(Some(entry.group_uuid));
                    }
                    self.log_ignored_corrupt_once("find_uuid_in_txn (ignored_db)");
                }
            }
        }

        Ok(None)
    }

    /// Store `pdqhash -> UUID` mappings for all `pdqhash`es in a group, skipping any
    /// protected hashes belonging to untouched entries with a different UUID.
    fn store_pdqmap_entries(
        cipher: &XChaCha20Poly1305,
        txn: &WriteTransaction,
        pdqmap_db: DbTable,
        pdqhashes: &[Option<[u8; 32]>],
        protected_pdqhashes: &HashSet<[u8; 32]>,
        uuid: &[u8; 16],
    ) -> Result<(), DbError> {
        let mut table = txn.open_table(pdqmap_db)?;
        for pdqhash in pdqhashes.iter().flatten() {
            if protected_pdqhashes.contains(pdqhash) {
                continue;
            }
            let encrypted = Self::encrypt_value(cipher, pdqhash, uuid)?;
            table.insert(pdqhash, encrypted.as_slice())?;
        }
        Ok(())
    }

    /// Register all duplicate groups in the database on scan completion.
    /// Each file gets an `IgnoredEntry` with `ignored: false`.
    /// Entries already marked `ignored: true` (or corrupted/undecryptable entries)
    /// are NEVER overwritten.
    ///
    /// # Errors
    /// Returns [`DbError`] on RNG, serialization, encryption, or transaction errors.
    pub fn register_duplicate_groups(&self, groups: &[Vec<GroupEntry>]) -> Result<usize, DbError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let mut total_registered = 0;

        let txn = self.env.begin_write()?;

        for group in groups {
            if group.is_empty() {
                continue;
            }

            let uuid = match self.find_uuid_in_txn(&txn, group)? {
                Some(existing_uuid) => existing_uuid,
                None => {
                    let mut fresh = [0u8; 16];
                    getrandom::fill(&mut fresh)?;
                    fresh
                },
            };

            let mut protected_pdqhashes = HashSet::new();

            {
                let mut ignored_table = txn.open_table(self.ignored_db)?;
                for (content_hash, pdqhash) in group {
                    // Never overwrite entries already marked ignored=true OR entries that
                    // fail decryption/deserialization (e.g. if opened with another key).
                    if let Some(existing_guard) = ignored_table.get(content_hash)? {
                        let Some(decrypted) =
                            self.decrypt_value(content_hash, existing_guard.value())
                        else {
                            self.log_ignored_corrupt_once("register_duplicate_groups");
                            if let Some(pdq) = pdqhash {
                                protected_pdqhashes.insert(*pdq);
                            }
                            continue;
                        };
                        let Ok(existing_entry) = IgnoredEntry::from_bytes(&decrypted) else {
                            self.log_ignored_corrupt_once("register_duplicate_groups");
                            if let Some(pdq) = pdqhash {
                                protected_pdqhashes.insert(*pdq);
                            }
                            continue;
                        };
                        if existing_entry.ignored {
                            if existing_entry.group_uuid != uuid
                                && let Some(existing_pdq) = existing_entry.pdqhash
                            {
                                protected_pdqhashes.insert(existing_pdq);
                            }
                            continue;
                        }
                    }

                    let entry = IgnoredEntry {
                        pdqhash: *pdqhash,
                        group_uuid: uuid,
                        timestamp: now,
                        ignored: false,
                    };
                    let bytes = entry.to_bytes()?;
                    let encrypted = Self::encrypt_value(&self.cipher, content_hash, &bytes)?;
                    ignored_table.insert(content_hash, encrypted.as_slice())?;
                    total_registered += 1;
                }
            }

            let all_pdqhashes: Vec<Option<[u8; 32]>> = group.iter().map(|(_, pdq)| *pdq).collect();
            Self::store_pdqmap_entries(
                &self.cipher,
                &txn,
                self.ignored_pdqmap_db,
                &all_pdqhashes,
                &protected_pdqhashes,
                &uuid,
            )?;
        }

        txn.commit()?;
        eprintln!(
            "[DEBUG-IGNORE] Registered {} files across {} groups",
            total_registered,
            groups.len()
        );
        Ok(total_registered)
    }

    /// Import ignored entries (into both `ignored_db` and `ignored_pdqmap_db`)
    /// parsed from `--show-ignored` output.
    ///
    /// # Errors
    /// Returns [`DbError`] on serialization, encryption, or transaction failures.
    pub fn import_ignored_entries(
        &self,
        entries: &[([u8; 32], IgnoredEntry)],
    ) -> Result<(usize, usize), DbError> {
        let txn = self.env.begin_write()?;
        let mut entry_count = 0;
        let mut pdqmap_count = 0;

        {
            let mut ignored_table = txn.open_table(self.ignored_db)?;
            let mut pdqmap_table = txn.open_table(self.ignored_pdqmap_db)?;

            for (content_hash, entry) in entries {
                let bytes = entry.to_bytes()?;
                let encrypted = Self::encrypt_value(&self.cipher, content_hash, &bytes)?;
                ignored_table.insert(content_hash, encrypted.as_slice())?;
                entry_count += 1;

                if let Some(ref pdqhash) = entry.pdqhash
                    && entry.group_uuid != [0u8; 16]
                {
                    let encrypted_uuid =
                        Self::encrypt_value(&self.cipher, pdqhash, &entry.group_uuid)?;
                    pdqmap_table.insert(pdqhash, encrypted_uuid.as_slice())?;
                    pdqmap_count += 1;
                }
            }
        }

        txn.commit()?;
        eprintln!(
            "[DEBUG-IGNORE] Imported {} ignored entries and {} pdqmap mappings into redb",
            entry_count, pdqmap_count
        );
        Ok((entry_count, pdqmap_count))
    }

    /// Set the ignored flag to `true` for the given content hashes.
    /// Returns the number of entries flipped from `false` to `true`.
    ///
    /// # Errors
    /// Returns [`DbError`] on serialization, encryption, or transaction failures.
    pub fn set_files_ignored(&self, content_hashes: &[[u8; 32]]) -> Result<usize, DbError> {
        let txn = self.env.begin_write()?;
        let mut count = 0;

        {
            let mut table = txn.open_table(self.ignored_db)?;
            for ch in content_hashes {
                let existing_bytes = table.get(ch)?.map(|guard| guard.value().to_vec());
                let Some(encrypted) = existing_bytes else {
                    eprintln!(
                        "[WARN-IGNORE] set_files_ignored: no entry for blake3={}, skipping",
                        hex::encode(ch)
                    );
                    continue;
                };

                let Some(decrypted) = self.decrypt_value(ch, &encrypted) else {
                    self.log_ignored_corrupt_once("set_files_ignored");
                    continue;
                };
                let Ok(mut entry) = IgnoredEntry::from_bytes(&decrypted) else {
                    self.log_ignored_corrupt_once("set_files_ignored");
                    continue;
                };
                if !entry.ignored {
                    entry.ignored = true;
                    let bytes = entry.to_bytes()?;
                    let new_encrypted = Self::encrypt_value(&self.cipher, ch, &bytes)?;
                    table.insert(ch, new_encrypted.as_slice())?;
                    count += 1;
                }
            }
        }

        txn.commit()?;
        Ok(count)
    }

    /// Get an ignored entry by `content_hash` (returns any entry, regardless of flag).
    ///
    /// # Errors
    /// Returns [`DbError`] on database read, decryption, or deserialization failure.
    #[allow(dead_code)]
    pub fn get_ignored(&self, content_hash: &[u8; 32]) -> Result<Option<IgnoredEntry>, DbError> {
        let txn = self.env.begin_read()?;
        let table = txn.open_table(self.ignored_db)?;
        let Some(encrypted_guard) = table.get(content_hash)? else {
            return Ok(None);
        };

        let Some(decrypted) = self.decrypt_value(content_hash, encrypted_guard.value()) else {
            return Err(DbError::DecryptionFailed { table: DB_FILE_NAME_IGNORED });
        };

        Ok(Some(IgnoredEntry::from_bytes(&decrypted)?))
    }

    /// Iterate all explicitly ignored entries (`ignored == true`).
    ///
    /// # Errors
    /// Returns [`DbError`] on database read/table failures.
    pub fn get_all_ignored(&self) -> Result<Vec<([u8; 32], IgnoredEntry)>, DbError> {
        let txn = self.env.begin_read()?;
        let table = txn.open_table(self.ignored_db)?;
        let mut results = Vec::new();
        let mut corrupt_count = 0usize;

        for result in table.iter()? {
            let (key_guard, encrypted_guard) = result?;
            let content_hash = *key_guard.value();
            let encrypted = encrypted_guard.value();

            let Some(decrypted) = self.decrypt_value(&content_hash, encrypted) else {
                corrupt_count += 1;
                continue;
            };
            match IgnoredEntry::from_bytes(&decrypted) {
                Ok(entry) if entry.ignored => results.push((content_hash, entry)),
                Ok(_) => {},
                Err(_) => {
                    corrupt_count += 1;
                },
            }
        }

        if corrupt_count > 0 {
            eprintln!(
                "[ERROR-IGNORE] get_all_ignored skipped {} corrupted or undecryptable entries",
                corrupt_count
            );
        }

        Ok(results)
    }

    /// Remove a single entry by `content_hash`, and clean up its `pdqhash` in
    /// `ignored_pdqmap_db` if no other entry in `ignored_db` still shares that `pdqhash`.
    ///
    /// # Errors
    /// Returns [`DbError`] on database transaction/table failures.
    pub fn remove_ignored(&self, content_hash: &[u8; 32]) -> Result<bool, DbError> {
        let txn = self.env.begin_write()?;

        let (removed, assoc_pdq, still_referenced) = {
            let mut table = txn.open_table(self.ignored_db)?;
            let assoc_pdq = match table.remove(content_hash)? {
                Some(encrypted_guard) => {
                    if let Some(dec) = self.decrypt_value(content_hash, encrypted_guard.value())
                        && let Ok(entry) = IgnoredEntry::from_bytes(&dec)
                    {
                        entry.pdqhash
                    } else {
                        self.log_ignored_corrupt_once("remove_ignored");
                        None
                    }
                },
                None => return Ok(false),
            };

            let mut still_referenced = false;
            if let Some(pdq) = assoc_pdq {
                for result in table.iter()? {
                    let (key_guard, encrypted_guard) = result?;
                    let ch = *key_guard.value();
                    if let Some(dec) = self.decrypt_value(&ch, encrypted_guard.value())
                        && let Ok(entry) = IgnoredEntry::from_bytes(&dec)
                        && entry.pdqhash == Some(pdq)
                    {
                        still_referenced = true;
                        break;
                    }
                }
            }
            (true, assoc_pdq, still_referenced)
        };

        if let Some(pdq) = assoc_pdq
            && !still_referenced
        {
            let mut pdqmap_table = txn.open_table(self.ignored_pdqmap_db)?;
            pdqmap_table.remove(&pdq)?;
        }

        txn.commit()?;
        Ok(removed)
    }

    /// Remove all entries matching a group UUID (both `ignored_db` and `ignored_pdqmap_db`)
    /// within a single atomic read-write transaction.
    ///
    /// # Errors
    /// Returns [`DbError`] on database transaction/table failures.
    pub fn remove_ignored_by_uuid(&self, uuid: &[u8; 16]) -> Result<usize, DbError> {
        let wtxn = self.env.begin_write()?;
        let mut to_remove = Vec::new();
        let mut pdqhashes_to_remove = HashSet::new();
        let mut count = 0;

        {
            let mut ignored_table = wtxn.open_table(self.ignored_db)?;
            for result in ignored_table.iter()? {
                let (key_guard, encrypted_guard) = result?;
                let content_hash = *key_guard.value();
                let Some(decrypted) = self.decrypt_value(&content_hash, encrypted_guard.value())
                else {
                    self.log_ignored_corrupt_once("remove_ignored_by_uuid (ignored_db)");
                    continue;
                };
                let Ok(entry) = IgnoredEntry::from_bytes(&decrypted) else {
                    self.log_ignored_corrupt_once("remove_ignored_by_uuid (ignored_db)");
                    continue;
                };
                if entry.group_uuid == *uuid {
                    eprintln!(
                        "[DEBUG-UNIGNORE] Removing blake3={} ignored={}",
                        hex::encode(content_hash),
                        entry.ignored,
                    );
                    to_remove.push(content_hash);
                    if let Some(pdq) = entry.pdqhash {
                        pdqhashes_to_remove.insert(pdq);
                    }
                }
            }

            for ch in &to_remove {
                if ignored_table.remove(ch)?.is_some() {
                    count += 1;
                }
            }
        }

        // Also collect and remove pdqmap entries pointing to this UUID
        {
            let mut pdqmap_table = wtxn.open_table(self.ignored_pdqmap_db)?;
            for result in pdqmap_table.iter()? {
                let (key_guard, encrypted_guard) = result?;
                let pdqhash = *key_guard.value();
                let Some(decrypted) = self.decrypt_value(&pdqhash, encrypted_guard.value()) else {
                    self.log_ignored_corrupt_once("remove_ignored_by_uuid (pdqmap)");
                    continue;
                };
                if let Ok(stored_uuid) = <[u8; 16]>::try_from(decrypted.as_slice())
                    && stored_uuid == *uuid
                {
                    pdqhashes_to_remove.insert(pdqhash);
                }
            }

            for pdq in &pdqhashes_to_remove {
                pdqmap_table.remove(pdq)?;
            }
        }

        wtxn.commit()?;
        Ok(count)
    }

    /// Remove all entries matching a PDQ hash within a single atomic read-write transaction.
    ///
    /// # Errors
    /// Returns [`DbError`] on database transaction/table failures.
    pub fn remove_ignored_by_pdqhash(&self, pdqhash: &[u8; 32]) -> Result<usize, DbError> {
        let wtxn = self.env.begin_write()?;
        let mut to_remove = Vec::new();
        let mut count = 0;

        {
            let mut ignored_table = wtxn.open_table(self.ignored_db)?;
            for result in ignored_table.iter()? {
                let (key_guard, encrypted_guard) = result?;
                let content_hash = *key_guard.value();
                let Some(decrypted) = self.decrypt_value(&content_hash, encrypted_guard.value())
                else {
                    self.log_ignored_corrupt_once("remove_ignored_by_pdqhash");
                    continue;
                };
                if let Ok(entry) = IgnoredEntry::from_bytes(&decrypted)
                    && entry.pdqhash.as_ref() == Some(pdqhash)
                {
                    to_remove.push(content_hash);
                }
            }

            for ch in &to_remove {
                if ignored_table.remove(ch)?.is_some() {
                    count += 1;
                }
            }
        }

        {
            let mut pdqmap_table = wtxn.open_table(self.ignored_pdqmap_db)?;
            pdqmap_table.remove(pdqhash)?;
        }

        wtxn.commit()?;
        Ok(count)
    }

    /// Format a UUID as a hyphenated string.
    #[must_use]
    pub fn format_uuid(uuid: &[u8; 16]) -> String {
        let mut hex_buf = [0u8; 32];
        let _ = hex::encode_to_slice(uuid, &mut hex_buf);
        let hex_str = std::str::from_utf8(&hex_buf).unwrap_or("");
        let mut out = String::with_capacity(36);
        out.push_str(&hex_str[0..8]);
        out.push('-');
        out.push_str(&hex_str[8..12]);
        out.push('-');
        out.push_str(&hex_str[12..16]);
        out.push('-');
        out.push_str(&hex_str[16..20]);
        out.push('-');
        out.push_str(&hex_str[20..32]);
        out
    }

    /// Parse a hyphenated UUID string back into bytes (accepts with or without hyphens).
    #[must_use]
    pub fn parse_uuid(s: &str) -> Option<[u8; 16]> {
        let mut hex_buf = [0u8; 32];
        let mut idx = 0;
        for b in s.bytes() {
            if b == b'-' {
                continue;
            }
            if idx >= 32 {
                return None;
            }
            hex_buf[idx] = b;
            idx += 1;
        }
        if idx != 32 {
            return None;
        }
        let mut arr = [0u8; 16];
        hex::decode_to_slice(hex_buf, &mut arr).ok()?;
        Some(arr)
    }

    /// Persist the selected map provider to `phdupes.conf`.
    ///
    /// # Errors
    /// Returns [`DbError`] if the config cannot be read, parsed, or written.
    pub fn save_map_selection(&self, provider_name: &str) -> Result<(), DbError> {
        Ok(save_map_sel(provider_name)?)
    }

    /// Persist updated GUI settings to `phdupes.conf`.
    ///
    /// # Errors
    /// Returns [`DbError`] if the config cannot be read, parsed, or written.
    pub fn save_gui_config(&self, gui_config: &GuiConfig) -> Result<(), DbError> {
        Ok(save_gui_cfg(gui_config)?)
    }
}

#[cfg(test)]
mod tests {
    use crossbeam_channel::unbounded;
    use redb::ReadableTableMetadata;
    use tempfile::TempDir;

    use super::*;

    fn temp_ctx(master_key: [u8; 32]) -> (TempDir, AppContext) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_redb");
        let ctx = AppContext::open_for_test(&db_path, master_key).unwrap();
        (dir, ctx)
    }

    #[test]
    fn crypto_roundtrip_and_aad_binding() {
        let (_dir, ctx) = temp_ctx([0x11u8; 32]);
        let (_dir2, other_ctx) = temp_ctx([0x22u8; 32]);

        let key_a = [0xAAu8; 32];
        let key_b = [0xBBu8; 32];
        let plaintext = b"secret-payload-12345";

        let ct = AppContext::encrypt_value(&ctx.cipher, &key_a, plaintext).unwrap();
        assert_ne!(&ct[24..], plaintext);

        // Correct key + AAD succeeds
        assert_eq!(ctx.decrypt_value(&key_a, &ct).as_deref(), Some(plaintext.as_slice()));

        // Encrypting into reusable buffer matches decrypt_value
        let mut scratch = Vec::new();
        AppContext::encrypt_into(&ctx.cipher, &key_a, plaintext, &mut scratch).unwrap();
        assert_eq!(ctx.decrypt_value(&key_a, &scratch).as_deref(), Some(plaintext.as_slice()));

        // Swapped DB key (AAD mismatch) fails
        assert!(ctx.decrypt_value(&key_b, &ct).is_none());

        // Wrong master key fails
        assert!(other_ctx.decrypt_value(&key_a, &ct).is_none());

        // Truncated or tampered ciphertext fails
        assert!(ctx.decrypt_value(&key_a, &ct[..TOTAL_OVERHEAD - 1]).is_none());
        let mut tampered = ct.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0x01;
        assert!(ctx.decrypt_value(&key_a, &tampered).is_none());
    }

    #[test]
    fn uuid_format_and_parse_roundtrip() {
        let uuid = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ];
        let formatted = AppContext::format_uuid(&uuid);
        assert_eq!(formatted, "01234567-89ab-cdef-fedc-ba9876543210");
        assert_eq!(AppContext::parse_uuid(&formatted), Some(uuid));
        assert_eq!(AppContext::parse_uuid("0123456789abcdeffedcba9876543210"), Some(uuid));
        assert_eq!(AppContext::parse_uuid("not-a-uuid"), None);
    }

    #[test]
    fn ignored_db_and_pdqmap_lifecycle_with_temp_redb() {
        let (_dir, ctx) = temp_ctx([0x33u8; 32]);
        let ch1 = [1u8; 32];
        let ch2 = [2u8; 32];
        let ch3 = [3u8; 32];
        let pdq_shared = [0x99u8; 32];
        let pdq_other = [0x88u8; 32];

        // 1. Register two groups
        let registered = ctx
            .register_duplicate_groups(&[
                vec![(ch1, Some(pdq_shared)), (ch2, Some(pdq_shared))],
                vec![(ch3, Some(pdq_other))],
            ])
            .unwrap();
        assert_eq!(registered, 3);
        assert!(!ctx.is_ignored(&ch1));
        assert!(!ctx.is_ignored(&ch2));

        let uuid1 = ctx.get_group_uuid(&ch1).unwrap();
        assert_eq!(ctx.get_group_uuid(&ch2), Some(uuid1));
        let uuid2 = ctx.get_group_uuid(&ch3).unwrap();
        assert_ne!(uuid1, uuid2);

        // 2. Ignore ch1 and ch2
        let flipped = ctx.set_files_ignored(&[ch1, ch2]).unwrap();
        assert_eq!(flipped, 2);
        assert!(ctx.is_ignored(&ch1));
        assert!(ctx.is_ignored(&ch2));
        assert!(!ctx.is_ignored(&ch3));
        assert_eq!(ctx.get_all_ignored().unwrap().len(), 2);

        // 3. Re-registering must NOT overwrite ignored=true entries
        let re_reg = ctx
            .register_duplicate_groups(&[vec![(ch1, Some(pdq_shared)), (ch2, Some(pdq_shared))]])
            .unwrap();
        assert_eq!(re_reg, 0);
        assert!(ctx.is_ignored(&ch1));
        assert_eq!(ctx.get_group_uuid(&ch1), Some(uuid1));

        // 4. Prune must NEVER delete ignored_db or ignored_pdqmap_db entries
        let (_meta_rm, _hash_rm) = ctx.prune(0).unwrap();
        assert!(ctx.is_ignored(&ch1));
        assert!(ctx.is_ignored(&ch2));
        assert!(ctx.get_ignored(&ch3).unwrap().is_some());

        // 5. Removing ch1 keeps pdq_shared in pdqmap because ch2 still references it
        assert!(ctx.remove_ignored(&ch1).unwrap());
        assert!(!ctx.remove_ignored(&ch1).unwrap());
        assert!(!ctx.is_ignored(&ch1));
        {
            let rtxn = ctx.env.begin_read().unwrap();
            let pdqmap = rtxn.open_table(ctx.ignored_pdqmap_db).unwrap();
            assert!(pdqmap.get(&pdq_shared).unwrap().is_some());
        }

        // 6. Removing ch2 (last reference to pdq_shared) cleans up pdq_shared in pdqmap
        assert!(ctx.remove_ignored(&ch2).unwrap());
        {
            let rtxn = ctx.env.begin_read().unwrap();
            let pdqmap = rtxn.open_table(ctx.ignored_pdqmap_db).unwrap();
            assert!(pdqmap.get(&pdq_shared).unwrap().is_none());
            assert!(pdqmap.get(&pdq_other).unwrap().is_some());
        }

        // 7. remove_ignored_by_uuid removes both ignored_db and ignored_pdqmap_db entries
        assert_eq!(ctx.remove_ignored_by_uuid(&uuid2).unwrap(), 1);
        assert!(ctx.get_ignored(&ch3).unwrap().is_none());
        {
            let rtxn = ctx.env.begin_read().unwrap();
            let pdqmap = rtxn.open_table(ctx.ignored_pdqmap_db).unwrap();
            assert_eq!(pdqmap.len().unwrap(), 0);
        }
    }

    #[test]
    fn register_duplicate_groups_does_not_overwrite_corrupt_or_desync_pdqmap() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_corrupt_redb");
        let ch_corrupt = [0x41u8; 32];
        let ch_ignored = [0x42u8; 32];
        let ch_other = [0x43u8; 32];
        let pdq_ignored = [0x51u8; 32];
        let pdq_other = [0x52u8; 32];

        // Write an entry encrypted with a different key into ignored_db
        {
            let other_ctx = AppContext::open_for_test(&db_path, [0x99u8; 32]).unwrap();
            let entry = IgnoredEntry {
                pdqhash: Some(pdq_ignored),
                group_uuid: [0x77u8; 16],
                timestamp: 100,
                ignored: true,
            };
            other_ctx.import_ignored_entries(&[(ch_corrupt, entry)]).unwrap();
        }

        let ctx = AppContext::open_for_test(&db_path, [0x44u8; 32]).unwrap();

        // Registering a group containing ch_corrupt must NOT overwrite ch_corrupt
        let reg = ctx.register_duplicate_groups(&[vec![(ch_corrupt, Some(pdq_ignored))]]).unwrap();
        assert_eq!(reg, 0);

        // Setup: ch_ignored (U1, pdq_ignored) is ignored; ch_other (U2, pdq_other) exists
        ctx.register_duplicate_groups(&[vec![(ch_ignored, Some(pdq_ignored))]]).unwrap();
        ctx.set_files_ignored(&[ch_ignored]).unwrap();
        let u1 = ctx.get_group_uuid(&ch_ignored).unwrap();

        ctx.register_duplicate_groups(&[vec![(ch_other, Some(pdq_other))]]).unwrap();
        let u2 = ctx.get_group_uuid(&ch_other).unwrap();
        assert_ne!(u1, u2);

        // Group ch_other with ch_ignored (find_uuid_in_txn picks u2 first):
        // ch_ignored keeps u1, and pdq_ignored must NOT be overwritten to u2.
        ctx.register_duplicate_groups(&[vec![
            (ch_other, Some(pdq_other)),
            (ch_ignored, Some(pdq_ignored)),
        ]])
        .unwrap();

        let rtxn = ctx.env.begin_read().unwrap();
        let pdqmap = rtxn.open_table(ctx.ignored_pdqmap_db).unwrap();
        let raw = pdqmap.get(&pdq_ignored).unwrap().unwrap();
        let dec = ctx.decrypt_value(&pdq_ignored, raw.value()).unwrap();
        assert_eq!(dec.as_slice(), &u1);
    }

    #[test]
    fn background_writer_batches_and_flushes_on_disconnect() {
        let (_dir, ctx) = temp_ctx([0x55u8; 32]);
        let (tx, rx) = unbounded::<DbUpdate>();
        let handle = ctx.start_db_writer(rx);

        let meta_k = [0x01u8; 32];
        let ch = [0x02u8; 32];
        let pdq = [0x03u8; 32];
        let pix = [0x04u8; 32];
        let feats = ImageFeatures::new(800, 600);
        let coeffs = CachedCoefficients { coefficients: vec![1.0, 2.5, -3.0] };

        tx.send((
            Some((meta_k, ch)),
            Some((ch, HashValue::PdqHash(pdq))),
            Some((ch, feats)),
            Some((ch, coeffs)),
            Some((ch, pix)),
        ))
        .unwrap();
        drop(tx);
        handle.join().unwrap();

        assert_eq!(ctx.get_content_hash(&meta_k).unwrap(), Some(ch));
        assert_eq!(ctx.get_pdqhash(&ch).unwrap(), Some(pdq));
        assert_eq!(ctx.get_features(&ch).unwrap().unwrap().resolution(), Some((800, 600)));
        assert_eq!(ctx.get_coefficients(&ch).unwrap(), Some(vec![1.0, 2.5, -3.0]));
        assert_eq!(ctx.get_pixel_hash(&ch).unwrap(), Some(pix));
    }
}
