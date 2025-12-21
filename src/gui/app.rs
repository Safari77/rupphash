use chrono;
use crossbeam_channel::{Receiver, Sender, unbounded};
use eframe::egui;
use jiff::Timestamp;
use notify::{Event, RecommendedWatcher, RecursiveMode, Result as NotifyResult, Watcher};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::mpsc::{Receiver as StdReceiver, channel};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use super::gps_map::GpsMapState;
use super::image::{GroupViewState, ViewMode};

use crate::GroupStatus;
use crate::db::{AppContext, EnrichmentResult};
use crate::format_relative_time;
use crate::gui::APP_TITLE;
use crate::gui::image::MAX_TEXTURE_SIDE;
use crate::helper_exif::extract_gps_lat_lon;
use crate::position;
use crate::scanner::{self, ScanConfig};
use crate::state::{
    AppState, InputIntent, format_path_depth, get_bit_identical_counts, get_content_subgroups,
    get_hardlink_groups,
};
use crate::{FileMetadata, GroupInfo};

// Define a cache struct to hold the data we previously fetched every frame
#[derive(Clone)]
pub struct DirCacheEntry {
    pub path: std::path::PathBuf,
    pub display_name: String,
    pub modified_display: String,
}

fn truncate_to_width(
    text: &str,
    max_width: f32,
    font_id: &egui::FontId,
    ui: &egui::Ui,
) -> (String, bool) {
    // 1. Layout the FULL text once
    let full_galley =
        ui.painter().layout_no_wrap(text.to_string(), font_id.clone(), egui::Color32::WHITE);

    // If it fits, return immediately
    if full_galley.rect.width() <= max_width {
        return (text.to_string(), false);
    }

    // Using 0.8 is conservative (leaves a tiny bit of extra space).
    let estimated_ellipsis_width = font_id.size * 0.8;

    // 3. Iterate over glyphs to find the cut-off point
    if let Some(row) = full_galley.rows.first() {
        let mut byte_offset = 0;

        for glyph in &row.glyphs {
            let right_edge = glyph.pos.x + glyph.advance_width;

            // Check if this character + estimated ellipsis exceeds the width
            if right_edge + estimated_ellipsis_width > max_width {
                let cut_index = byte_offset;

                if cut_index == 0 {
                    return ("…".to_string(), true);
                }

                let truncated = format!("{}…", &text[..cut_index]);
                return (truncated, true);
            }

            byte_offset += glyph.chr.len_utf8();
        }
    }

    ("…".to_string(), true)
}

pub struct GuiApp {
    pub(super) state: AppState,
    pub(super) group_views: HashMap<usize, GroupViewState>,
    pub(super) initial_scale_applied: bool,
    pub(super) initial_panel_width_applied: bool,
    pub(super) ctx: Arc<AppContext>,
    pub(super) scan_config: ScanConfig,

    pub(super) scan_rx:
        Option<Receiver<(Vec<Vec<FileMetadata>>, Vec<GroupInfo>, Vec<std::path::PathBuf>)>>,
    pub(super) scan_progress_rx: Option<Receiver<(usize, usize)>>,
    pub(super) scan_progress: (usize, usize),

    pub(super) rename_input: String,
    pub(super) show_move_input: bool,
    pub(super) move_input: String,
    pub(super) move_completion_candidates: Vec<String>,
    pub(super) move_completion_index: usize,
    pub(super) last_preload_pos: Option<(usize, usize)>,
    pub(super) slideshow_last_advance: Option<std::time::Instant>,

    // View mode: if Some, use scan_for_view with this sort order instead of scan_and_group
    pub(super) view_mode_sort: Option<String>,

    // --- Raw Preloading ---
    // Cache for raw images (Path -> Texture)
    pub(super) raw_cache: HashMap<std::path::PathBuf, egui::TextureHandle>,
    // Set of paths currently being processed by the worker to avoid dupes
    pub(super) raw_loading: HashSet<std::path::PathBuf>,
    // Channel to send paths to the worker
    pub(super) image_preload_tx: Sender<std::path::PathBuf>,
    // Channel to receive decoded images from the worker.
    pub(super) image_preload_rx:
        Receiver<(std::path::PathBuf, Option<(egui::ColorImage, (u32, u32), u8)>)>,
    pub(super) scan_batch_rx: Option<Receiver<Vec<FileMetadata>>>,

    // Shared state to tell workers which files are still relevant.
    // If a file is not in this set, workers will skip decoding it.
    pub(super) active_window: Arc<RwLock<HashSet<std::path::PathBuf>>>,

    // Track window size and panel width for saving on exit
    pub(super) last_window_size: Option<(u32, u32)>,
    pub(super) panel_width: f32,
    pub(super) saved_panel_width: f32, // Original loaded value, preserved until applied
    pub(super) last_row_height: f32,

    // Directory browsing (view mode only)
    pub(super) current_dir: Option<std::path::PathBuf>,
    pub(super) show_dir_picker: bool,
    pub(super) dir_list: Vec<std::path::PathBuf>,
    pub(super) dir_picker_selection: usize,
    pub(super) dir_picker_scroll_to_selection: bool, // True when keyboard nav should scroll to selection
    pub(super) subdirs: Vec<std::path::PathBuf>,     // Subdirectories in current directory
    pub(super) dir_selection_idx: Option<usize>, // None = files selected, Some(idx) = directory idx selected
    pub(super) dir_scroll_to_selection: bool, // True when keyboard nav should scroll to dir in main panel

    // Tab Completion State
    pub(super) completion_candidates: Vec<String>,
    pub(super) completion_index: usize,

    // Histogram display
    pub(super) show_histogram: bool,

    // EXIF info display
    pub(super) show_exif: bool,
    pub(super) search_sun_azimuth_enabled: bool,
    pub(super) search_sun_altitude_enabled: bool,

    // Cache for current image's histogram and EXIF data (to avoid reloading on toggle)
    pub(super) cached_histogram: Option<(std::path::PathBuf, [u32; 256])>,
    pub(super) cached_exif: Option<(std::path::PathBuf, Vec<(String, String)>)>,
    pub(super) search_input: String,
    pub(super) search_focus_requested: bool,

    // EXIF cache for search (persists across searches)
    pub(super) exif_search_cache: HashMap<std::path::PathBuf, Vec<(String, String)>>,

    // GPS Map state
    pub(super) gps_map: GpsMapState,

    // UI Virtualization State
    pub(super) group_y_offsets: Vec<f32>, // Cached Y position of every group
    pub(super) total_content_height: f32, // Total scrollable height
    pub(super) cache_dirty: bool,         // Flag to rebuild offsets
    //
    pub(super) watcher: Option<RecommendedWatcher>,
    pub(super) fs_event_rx: Option<StdReceiver<NotifyResult<Event>>>,
    pub(super) subdirs_cache: Vec<DirCacheEntry>,
    pub(super) parent_cache: Option<DirCacheEntry>,

    // --- FS Event Debouncing ---
    pub(super) fs_mod_files: HashSet<String>,
    pub(super) fs_mod_dirs: HashSet<String>,
    pub(super) fs_rem_files: HashSet<String>,
    pub(super) fs_rem_dirs: HashSet<String>,
    pub(super) last_fs_refresh: Instant,

    // View mode: Channel to receive enrichment results (content_hash, GPS, etc.)
    pub(super) enrichment_rx: Option<Receiver<EnrichmentResult>>,

    // View mode: Maps unique_file_id -> file_idx within the single group
    pub(super) file_index: HashMap<u128, usize>,

    // Channel to send database updates (view mode caches features without coefficients)
    pub(super) db_tx: Option<Sender<crate::db::DbUpdate>>,

    // Channel to receive file batches from background directory scan
    pub(super) dir_scan_rx: Option<Receiver<Vec<FileMetadata>>>,
    // Total file count from directory (for progress display)
    pub(super) dir_total_count: Option<usize>,
}

impl GuiApp {
    /// Create a new GuiApp for duplicate detection mode
    pub fn new(
        ctx: AppContext,
        scan_config: ScanConfig,
        show_relative_times: bool,
        use_trash: bool,
        group_by: String,
        ext_priorities: HashMap<String, usize>,
        use_raw_thumbnails: bool,
    ) -> Self {
        let mut state = AppState::new(
            Vec::new(),
            Vec::new(),
            show_relative_times,
            use_trash,
            group_by,
            ext_priorities,
        );
        state.is_loading = true;

        let active_window = Arc::new(RwLock::new(HashSet::new()));
        let (tx, rx) =
            super::image::spawn_image_loader_pool(active_window.clone(), use_raw_thumbnails);
        // panel_width is saved in logical points (after font_scale applied)
        let panel_width = ctx.gui_config.panel_width.unwrap_or(450.0);
        // Initialize with configured size so we have a fallback if window size isn't captured
        let initial_window_size =
            Some((ctx.gui_config.width.unwrap_or(1280), ctx.gui_config.height.unwrap_or(720)));
        eprintln!(
            "[DEBUG-CONFIG] new() - config values: width={:?}, height={:?}, panel_width={:?}",
            ctx.gui_config.width, ctx.gui_config.height, ctx.gui_config.panel_width
        );

        // Extract values before moving ctx to Arc
        let tile_cache_path = ctx.tile_cache_path.clone();
        let selected_provider = ctx.selected_provider.clone();
        let provider_url = ctx.map_providers.get(&selected_provider).cloned().unwrap_or_default();

        Self {
            state,
            group_views: HashMap::new(),
            initial_scale_applied: false,
            initial_panel_width_applied: false,
            ctx: Arc::new(ctx),
            scan_config,
            scan_rx: None,
            scan_progress_rx: None,
            scan_progress: (0, 0),
            rename_input: String::new(),
            show_move_input: false,
            move_input: String::new(),
            move_completion_candidates: Vec::new(),
            move_completion_index: 0,
            last_preload_pos: None,
            slideshow_last_advance: None,
            view_mode_sort: None,
            raw_cache: HashMap::new(),
            raw_loading: HashSet::new(),
            scan_batch_rx: None,
            image_preload_tx: tx,
            image_preload_rx: rx,
            active_window,
            last_window_size: initial_window_size,
            panel_width,
            saved_panel_width: panel_width,
            last_row_height: 0.0,
            current_dir: None,
            show_dir_picker: false,
            dir_list: Vec::new(),
            dir_picker_selection: 0,
            dir_picker_scroll_to_selection: false,
            subdirs: Vec::new(),
            dir_selection_idx: None,
            dir_scroll_to_selection: false,
            completion_candidates: Vec::new(),
            completion_index: 0,
            show_histogram: false,
            show_exif: false,
            search_sun_azimuth_enabled: false,
            search_sun_altitude_enabled: false,
            cached_histogram: None,
            cached_exif: None,
            search_input: String::new(),
            search_focus_requested: false,
            exif_search_cache: HashMap::new(),
            group_y_offsets: Vec::new(),
            total_content_height: 0.0,
            cache_dirty: true,
            watcher: None,
            fs_event_rx: None,
            subdirs_cache: Vec::new(),
            parent_cache: None,
            fs_mod_files: HashSet::new(),
            fs_mod_dirs: HashSet::new(),
            fs_rem_files: HashSet::new(),
            fs_rem_dirs: HashSet::new(),
            last_fs_refresh: Instant::now(),
            gps_map: GpsMapState::new(tile_cache_path, selected_provider, provider_url),
            enrichment_rx: None,
            file_index: HashMap::new(),
            db_tx: None,
            dir_scan_rx: None,
            dir_total_count: None,
        }
    }

    /// Create a new GuiApp for view mode (image browser without duplicate detection)
    pub fn new_view_mode(
        paths: Vec<String>,
        sort_order: String,
        show_relative_times: bool,
        use_trash: bool,
        move_target: Option<std::path::PathBuf>,
        slideshow_interval: Option<f32>,
        use_raw_thumbnails: bool,
    ) -> Self {
        let mut state = AppState::new(
            Vec::new(),
            Vec::new(),
            show_relative_times,
            use_trash,
            sort_order.clone(),
            HashMap::new(),
        );
        state.view_mode = true;
        state.move_target = move_target;
        state.slideshow_interval = slideshow_interval;

        // Canonicalize all input paths to ensure absolute paths throughout
        let canonical_paths: Vec<String> = paths
            .iter()
            .filter_map(|p| {
                let path = std::path::Path::new(p);
                path.canonicalize().ok().map(|c| c.to_string_lossy().to_string())
            })
            .collect();

        // Determine initial directory from paths (use canonicalized paths)
        let current_dir = canonical_paths
            .first()
            .map(std::path::PathBuf::from)
            .and_then(|p| if p.is_dir() { Some(p) } else { p.parent().map(|p| p.to_path_buf()) });

        let scan_config = ScanConfig {
            paths: canonical_paths,
            rehash: false,
            similarity: 0,
            group_by: sort_order.clone(),
            extensions: Vec::new(),
            ignore_same_stem: false,
            calc_pixel_hash: false,
        };

        let active_window = Arc::new(RwLock::new(HashSet::new()));
        let (tx, rx) =
            super::image::spawn_image_loader_pool(active_window.clone(), use_raw_thumbnails);
        let ctx = crate::db::AppContext::new().expect("Failed to create context");
        let panel_width = ctx.gui_config.panel_width.unwrap_or(450.0);
        let initial_window_size =
            Some((ctx.gui_config.width.unwrap_or(1280), ctx.gui_config.height.unwrap_or(720)));

        // Start database writer for view mode
        let (db_tx_send, db_rx) = unbounded::<crate::db::DbUpdate>();
        let _db_handle = ctx.start_db_writer(db_rx);
        let db_tx = Some(db_tx_send.clone());

        // Background directory scanning with batch database lookups
        let (subdirs, dir_total_count, dir_scan_rx) = if let Some(ref dir) = current_dir {
            let (batch_tx, batch_rx) = unbounded::<Vec<FileMetadata>>();
            let (subdirs, count) =
                scanner::spawn_background_dir_scan(dir.clone(), sort_order.clone(), &ctx, batch_tx);
            (subdirs, Some(count), Some(batch_rx))
        } else {
            (Vec::new(), None, None)
        };

        // Build subdirs_cache and parent_cache for UI display
        let subdirs_cache: Vec<DirCacheEntry> = subdirs
            .iter()
            .map(|dir| Self::create_dir_cache_entry(dir, show_relative_times))
            .collect();
        let parent_cache = current_dir
            .as_ref()
            .and_then(|d| d.parent())
            .map(|p| Self::create_dir_cache_entry(p, show_relative_times));

        // Set up empty initial state - files will stream in from background
        state.groups = vec![Vec::new()];
        state.group_infos = vec![GroupInfo { max_dist: 0, status: GroupStatus::None }];
        state.is_loading = dir_total_count.map_or(false, |c| c > 0);

        // Extract values before moving ctx to Arc
        let tile_cache_path = ctx.tile_cache_path.clone();
        let selected_provider = ctx.selected_provider.clone();
        let provider_url = ctx.map_providers.get(&selected_provider).cloned().unwrap_or_default();

        Self {
            state,
            group_views: HashMap::new(),
            initial_scale_applied: false,
            initial_panel_width_applied: false,
            ctx: Arc::new(ctx),
            scan_config,
            scan_rx: None,
            scan_progress_rx: None,
            scan_progress: (0, 0),
            rename_input: String::new(),
            show_move_input: false,
            move_input: String::new(),
            move_completion_candidates: Vec::new(),
            move_completion_index: 0,
            last_preload_pos: None,
            slideshow_last_advance: None,
            view_mode_sort: Some(sort_order),
            raw_cache: HashMap::new(),
            raw_loading: HashSet::new(),
            scan_batch_rx: None,
            image_preload_tx: tx,
            image_preload_rx: rx,
            active_window,
            last_window_size: initial_window_size,
            panel_width,
            saved_panel_width: panel_width,
            last_row_height: 0.0,
            current_dir,
            show_dir_picker: false,
            dir_list: Vec::new(),
            dir_picker_selection: 0,
            dir_picker_scroll_to_selection: false,
            subdirs,
            dir_selection_idx: None,
            dir_scroll_to_selection: false,
            completion_candidates: Vec::new(),
            completion_index: 0,
            show_histogram: false,
            show_exif: false,
            search_sun_azimuth_enabled: false,
            search_sun_altitude_enabled: false,
            cached_histogram: None,
            cached_exif: None,
            search_input: String::new(),
            search_focus_requested: false,
            exif_search_cache: HashMap::new(),
            group_y_offsets: Vec::new(),
            total_content_height: 0.0,
            cache_dirty: true,
            watcher: None,
            fs_event_rx: None,
            subdirs_cache,
            parent_cache,
            fs_mod_files: HashSet::new(),
            fs_mod_dirs: HashSet::new(),
            fs_rem_files: HashSet::new(),
            fs_rem_dirs: HashSet::new(),
            last_fs_refresh: Instant::now(),
            gps_map: GpsMapState::new(tile_cache_path, selected_provider, provider_url),
            enrichment_rx: None,
            file_index: HashMap::new(),
            db_tx,
            dir_scan_rx,
            dir_total_count,
        }
    }

    pub fn with_move_target(mut self, target: Option<std::path::PathBuf>) -> Self {
        self.state.move_target = target;
        self
    }

    /// Set status message with automatic 5-second timeout
    pub(super) fn set_status(&mut self, msg: String, is_error: bool) {
        self.state.set_status(msg, is_error);
    }

    /// Get GPS coordinates for a file, using cache for O(1) access.
    /// Returns cached result if available, otherwise reads EXIF.
    /// Uses file_index for O(1) in-memory update when reading from EXIF.
    pub(super) fn get_gps_coords(
        &mut self,
        path: &std::path::Path,
        content_hash: &[u8; 32],
        unique_file_id: Option<u128>,
    ) -> Option<(f64, f64)> {
        // Fast path: query the database using the content hash (only works if content_hash is non-zero)
        if *content_hash != [0u8; 32] {
            if let Ok(Some(features)) = self.ctx.get_features(content_hash) {
                if let Some(coords) = features.gps_pos {
                    self.gps_map.add_marker(path.to_path_buf(), coords.y(), coords.x());
                    return Some((coords.y(), coords.x()));
                }
            }
        }

        // Slow fallback: Read EXIF directly from disk
        if let Some(exif) = scanner::read_exif_data(path, None) {
            if let Some((lat, lon)) = extract_gps_lat_lon(&exif) {
                self.gps_map.add_marker(path.to_path_buf(), lat, lon);

                // O(1) update via file_index if unique_file_id provided
                if let Some(uid) = unique_file_id {
                    if let Some(&file_idx) = self.file_index.get(&uid) {
                        if let Some(group) = self.state.groups.first_mut() {
                            if let Some(file) = group.get_mut(file_idx) {
                                file.gps_pos = Some(geo::Point::new(lon, lat));
                            }
                        }
                    }
                }

                return Some((lat, lon));
            }
        }

        None
    }

    /// Get distance and bearing string from current image to selected location
    /// Returns None if no GPS data or no location selected
    /// Format: "image to home: 1919.99 km @ 88° E" or "home to image: 1919.99 km @ 279° W"
    pub(super) fn get_distance_to_location(&mut self) -> Option<String> {
        // Get current file's content_hash and unique_file_id
        let current_path = self.state.get_current_image_path()?.clone();
        let current_file = self
            .state
            .groups
            .get(self.state.current_group_idx)?
            .get(self.state.current_file_idx)?;
        let content_hash = current_file.content_hash;
        let unique_file_id = current_file.unique_file_id;

        // Get GPS coords for current file
        let (img_lat, img_lon) =
            self.get_gps_coords(&current_path, &content_hash, Some(unique_file_id))?;

        // Get selected location from config
        let (loc_name, loc_point) = self.gps_map.selected_location.as_ref()?;
        let loc_lat = loc_point.y();
        let loc_lon = loc_point.x();

        // Calculate distance and bearing based on direction toggle
        let (distance, bearing) = if self.gps_map.direction_to_image {
            // Location to image
            position::distance_and_bearing((loc_lat, loc_lon), (img_lat, img_lon))
        } else {
            // Image to location
            position::distance_and_bearing((img_lat, img_lon), (loc_lat, loc_lon))
        };

        // Format the result
        let dist_str = super::gps_map::format_distance(distance);
        let bearing_str = super::gps_map::format_bearing(bearing);

        let direction_str = if self.gps_map.direction_to_image {
            format!("{} to image", loc_name)
        } else {
            format!("image to {}", loc_name)
        };

        Some(format!("{}: {} @ {}", direction_str, dist_str, bearing_str))
    }

    /// Toggle the direction of distance/bearing display
    pub(super) fn toggle_distance_direction(&mut self) {
        self.gps_map.direction_to_image = !self.gps_map.direction_to_image;
    }

    // 1. Helper to build cache entry (does the stat() call ONCE)
    fn create_dir_cache_entry(path: &std::path::Path, show_relative: bool) -> DirCacheEntry {
        let modified_display = if let Ok(meta) = fs::metadata(path) {
            if let Ok(modified) = meta.modified() {
                let dt: chrono::DateTime<chrono::Utc> = modified.into();
                if show_relative {
                    let ts = Timestamp::from_second(dt.timestamp()).unwrap();
                    crate::format_relative_time(ts)
                } else {
                    dt.format("%Y-%m-%d %H:%M").to_string()
                }
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        let display_name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| path.to_string_lossy().to_string());

        DirCacheEntry { path: path.to_path_buf(), display_name, modified_display }
    }

    // Setup filesystem watcher for current directory
    fn setup_watcher(&mut self) {
        if !self.state.view_mode {
            return;
        }
        let Some(dir) = &self.current_dir else { return };

        let (tx, rx) = channel();
        match notify::recommended_watcher(tx) {
            Ok(mut watcher) => {
                if let Err(e) = watcher.watch(dir, RecursiveMode::NonRecursive) {
                    eprintln!("Notify watch error: {:?}", e);
                }
                self.watcher = Some(watcher);
                self.fs_event_rx = Some(rx);
            }
            Err(e) => eprintln!("Failed to create watcher: {:?}", e),
        }
    }

    // Setup watcher and populate cache
    pub(super) fn change_directory(&mut self, new_dir: std::path::PathBuf) {
        if !self.state.view_mode {
            return;
        }

        if let Ok(canonical) = new_dir.canonicalize() {
            self.current_dir = Some(canonical.clone());
            self.scan_config.paths = vec![canonical.to_string_lossy().to_string()];

            // Change process working directory so relative paths work
            let _ = std::env::set_current_dir(&canonical);

            self.setup_watcher();

            // Clear caches first
            self.raw_cache.clear();
            self.raw_loading.clear();
            self.exif_search_cache.clear();
            self.gps_map.clear_markers();
            if let Ok(mut w) = self.active_window.write() {
                w.clear();
            }
            self.last_preload_pos = None;
            self.file_index.clear();
            self.enrichment_rx = None;

            // Background directory scanning with batch database lookups
            let sort_order = self.view_mode_sort.clone().unwrap_or_else(|| "name".to_string());
            let (batch_tx, batch_rx) = unbounded::<Vec<FileMetadata>>();
            let (subdirs, count) =
                scanner::spawn_background_dir_scan(canonical, sort_order, &self.ctx, batch_tx);

            self.subdirs = subdirs;
            self.dir_total_count = Some(count);
            self.dir_scan_rx = Some(batch_rx);

            // Set up empty initial state - files will stream in from background
            self.state.groups = vec![Vec::new()];
            self.state.group_infos = vec![GroupInfo { max_dist: 0, status: GroupStatus::None }];
            self.state.current_group_idx = 0;
            self.state.current_file_idx = 0;
            self.state.is_loading = count > 0;

            self.scan_rx = None;
            self.scan_progress_rx = None;
            self.scan_progress = (0, 0);

            // Refresh directory cache for display
            self.refresh_dir_cache(false);
            self.cache_dirty = true;
        }
    }

    fn refresh_dir_cache(&mut self, rescan_fs: bool) {
        self.subdirs_cache.clear();
        self.parent_cache = None;

        let Some(current) = &self.current_dir else {
            return;
        };
        let current = current.clone();
        let show_relative = self.state.show_relative_times;

        if let Some(parent) = current.parent() {
            self.parent_cache = Some(Self::create_dir_cache_entry(parent, show_relative));
        }

        if rescan_fs {
            // Snapshot existing metadata by inode to preserve resolution/hashes across refreshes
            let existing: HashMap<u128, FileMetadata> = if !self.state.groups.is_empty() {
                self.state.groups[0].iter().map(|f| (f.unique_file_id, f.clone())).collect()
            } else {
                HashMap::new()
            };

            self.subdirs.clear();
            let mut new_files = Vec::new();

            if let Ok(entries) = fs::read_dir(&current) {
                for entry in entries.flatten() {
                    let entry_path = entry.path();

                    // Canonicalize each entry path to ensure absolute paths
                    if let Ok(canonical) = entry_path.canonicalize() {
                        if canonical.is_dir() {
                            self.subdirs.push(canonical);
                        } else if self.state.view_mode && crate::scanner::is_image_ext(&canonical) {
                            if let Ok(meta) = entry.metadata() {
                                let size = meta.len();
                                let modified =
                                    meta.modified().unwrap_or(std::time::UNIX_EPOCH).into();
                                if let Some(unique_file_id) =
                                    crate::fileops::get_file_key(&canonical)
                                {
                                    if let Some(old) = existing.get(&unique_file_id) {
                                        // Preserve resolution/hashes from session, update path/size/modified
                                        let mut recovered = old.clone();
                                        recovered.path = canonical;
                                        recovered.size = size;
                                        recovered.modified = modified;
                                        new_files.push(recovered);
                                    } else {
                                        // New file not in session - use centralized cache lookup from db.rs
                                        let (resolution, orientation, gps_pos) =
                                            self.ctx.lookup_cached_features(&meta, unique_file_id);
                                        new_files.push(FileMetadata {
                                            path: canonical,
                                            size,
                                            modified,
                                            pdqhash: None,
                                            resolution,
                                            content_hash: [0u8; 32],
                                            pixel_hash: None,
                                            orientation,
                                            gps_pos,
                                            unique_file_id,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }

            crate::scanner::sort_directories(&mut self.subdirs, &self.state.group_by);

            if self.state.view_mode {
                if let Some(sort_order) = &self.view_mode_sort {
                    crate::scanner::sort_files(&mut new_files, sort_order);
                }

                // Rebuild file_index for O(1) lookup
                self.file_index =
                    new_files.iter().enumerate().map(|(idx, f)| (f.unique_file_id, idx)).collect();

                self.state.groups = vec![new_files];
                self.state.group_infos = vec![GroupInfo { max_dist: 0, status: GroupStatus::None }];
                self.state.last_file_count = self.state.groups.first().map_or(0, |g| g.len());
            }
        }

        for dir in &self.subdirs {
            self.subdirs_cache.push(Self::create_dir_cache_entry(dir, show_relative));
        }
    }

    fn check_fs_events(&mut self, ctx: &egui::Context) {
        let Some(rx) = &self.fs_event_rx else { return };

        // Helper: Decide if a path name "looks like" an image file or is a directory.
        // Since the file might be gone (Remove event), we can't always stat it.
        // We rely on the extension as a heuristic for deleted items.
        let classify = |path: &std::path::Path| -> bool {
            // If it exists on disk, check the real type
            if path.exists() {
                if path.is_dir() {
                    return false;
                }
                return crate::scanner::is_image_ext(path);
            }
            // If it was removed, assume it was a file if it had an image extension
            crate::scanner::is_image_ext(path)
        };

        while let Ok(res) = rx.try_recv() {
            if let Ok(event) = res {
                // 1. Handle Atomic Rename (source -> dest)
                if let notify::EventKind::Modify(notify::event::ModifyKind::Name(
                    notify::event::RenameMode::Both,
                )) = event.kind
                {
                    // event.paths[0] is source (old name), event.paths[1] is dest (new name)
                    if let (Some(_source), Some(dest)) = (event.paths.first(), event.paths.get(1)) {
                        if let Some(name) = dest.file_name() {
                            let name_str = name.to_string_lossy().to_string();
                            // We only report the NEW name as "modified/created"
                            if classify(dest) {
                                self.fs_mod_files.insert(name_str);
                            } else {
                                self.fs_mod_dirs.insert(name_str);
                            }
                        }
                    }
                    continue;
                }

                // 2. Handle Specific Events
                match event.kind {
                    notify::EventKind::Remove(_) => {
                        for path in &event.paths {
                            if let Some(name) = path.file_name() {
                                let name_str = name.to_string_lossy().to_string();
                                if classify(path) {
                                    self.fs_rem_files.insert(name_str);
                                } else {
                                    self.fs_rem_dirs.insert(name_str);
                                }
                            }
                        }
                    }
                    notify::EventKind::Create(_)
                    | notify::EventKind::Modify(notify::event::ModifyKind::Name(_)) => {
                        for path in &event.paths {
                            if let Some(name) = path.file_name() {
                                let name_str = name.to_string_lossy().to_string();
                                if classify(path) {
                                    self.fs_mod_files.insert(name_str);
                                } else {
                                    self.fs_mod_dirs.insert(name_str);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // Debounce Logic: Only refresh if we have events AND enough time passed
        let has_pending = !self.fs_mod_files.is_empty()
            || !self.fs_mod_dirs.is_empty()
            || !self.fs_rem_files.is_empty()
            || !self.fs_rem_dirs.is_empty();

        if has_pending {
            let debounce_dur = Duration::from_millis(500);
            let time_since = self.last_fs_refresh.elapsed();

            if time_since >= debounce_dur {
                // Time to refresh!
                self.refresh_dir_cache(true);
                self.last_preload_pos = None;
                self.last_fs_refresh = Instant::now();

                // Set user preference for max list items
                let list_limit = 8;

                // Build status message from buffered events
                let _format_list = |label: &str, items: &mut HashSet<String>| -> Option<String> {
                    if items.is_empty() {
                        return None;
                    }
                    let count = items.len();
                    let mut sorted: Vec<_> = items.drain().collect();
                    sorted.sort();

                    // Take top N for display based on config
                    let display_items: Vec<String> =
                        sorted.iter().take(list_limit).cloned().collect();
                    let joined = display_items.join(", ");
                    let suffix = if count > list_limit { ", ..." } else { "" };

                    Some(format!(
                        "{} {} {}: {}{}",
                        label,
                        count,
                        if count == 1 { "item" } else { "items" },
                        joined,
                        suffix
                    ))
                };

                let mut parts = Vec::new();

                // 1. Files
                if !self.fs_mod_files.is_empty() {
                    let count = self.fs_mod_files.len();
                    let mut sorted: Vec<_> = self.fs_mod_files.drain().collect();
                    sorted.sort();
                    let display: Vec<_> = sorted.iter().take(list_limit).cloned().collect();
                    let extra = if count > list_limit { ", ..." } else { "" };
                    parts.push(format!("{} files ({}{})", count, display.join(", "), extra));
                }
                if !self.fs_rem_files.is_empty() {
                    let count = self.fs_rem_files.len();
                    let mut sorted: Vec<_> = self.fs_rem_files.drain().collect();
                    sorted.sort();
                    let display: Vec<_> = sorted.iter().take(list_limit).cloned().collect();
                    let extra = if count > list_limit { ", ..." } else { "" };
                    parts.push(format!(
                        "removed {} files ({}{})",
                        count,
                        display.join(", "),
                        extra
                    ));
                }

                // 2. Dirs
                if !self.fs_mod_dirs.is_empty() {
                    let count = self.fs_mod_dirs.len();
                    let mut sorted: Vec<_> = self.fs_mod_dirs.drain().collect();
                    sorted.sort();
                    let display: Vec<_> = sorted.iter().take(list_limit).cloned().collect();
                    let extra = if count > list_limit { ", ..." } else { "" };
                    parts.push(format!("{} dirs ({}{})", count, display.join(", "), extra));
                }
                if !self.fs_rem_dirs.is_empty() {
                    let count = self.fs_rem_dirs.len();
                    let mut sorted: Vec<_> = self.fs_rem_dirs.drain().collect();
                    sorted.sort();
                    let display: Vec<_> = sorted.iter().take(list_limit).cloned().collect();
                    let extra = if count > list_limit { ", ..." } else { "" };
                    parts.push(format!("removed {} dirs ({}{})", count, display.join(", "), extra));
                }

                if !parts.is_empty() && self.state.status_message.is_none() {
                    self.set_status(format!("FS: {}", parts.join("; ")), false);
                }
            } else {
                // Not enough time passed, schedule a repaint soon to check again
                let remaining = debounce_dur - time_since;
                ctx.request_repaint_after(remaining);
            }
        }
    }

    // Handles streaming batches for instant feedback
    pub(super) fn check_reload(&mut self, ctx: &egui::Context) {
        // 1. Start Scan if needed
        if self.state.is_loading && self.scan_rx.is_none() {
            let cfg = self.scan_config.clone();
            let (tx, rx) = unbounded();
            let (prog_tx, prog_rx) = unbounded();

            // Channel for streaming batch results (view mode)
            let (batch_tx, batch_rx) = unbounded();
            self.scan_rx = Some(rx);
            self.scan_progress_rx = Some(prog_rx);
            self.scan_batch_rx = Some(batch_rx);
            self.scan_progress = (0, 0);

            if let Some(ref sort_order) = self.view_mode_sort {
                let sort = sort_order.clone();
                let paths = cfg.paths.clone();
                thread::spawn(move || {
                    let res = scanner::scan_for_view(&paths, &sort, Some(prog_tx), Some(batch_tx));
                    let _ = tx.send(res);
                });
            } else {
                // Duplicate Finder Mode
                let ctx_clone = self.ctx.clone();
                thread::spawn(move || {
                    // Note: scan_and_group doesn't use batch_tx yet, but progress will work
                    let (groups, infos) = scanner::scan_and_group(&cfg, &ctx_clone, Some(prog_tx));
                    let _ = tx.send((groups, infos, Vec::new()));
                });
            }
        }

        let mut needs_repaint = false;

        // 2. Process Partial Batches (Streaming View)
        if let Some(batch_rx) = &self.scan_batch_rx {
            while let Ok(new_files) = batch_rx.try_recv() {
                if self.state.groups.is_empty() {
                    self.state.groups.push(Vec::new());
                    self.state
                        .group_infos
                        .push(GroupInfo { max_dist: 0, status: GroupStatus::None });
                }
                self.state.groups[0].extend(new_files);
                self.cache_dirty = true;
                needs_repaint = true;
            }
            if needs_repaint {
                self.state.last_file_count = self.state.groups[0].len();
            }
        }

        // 3. Process Progress Updates
        if let Some(prog_rx) = &self.scan_progress_rx {
            while let Ok(progress) = prog_rx.try_recv() {
                self.scan_progress = progress;
                needs_repaint = true;
            }
        }

        // 4. Process Final Result
        if let Some(rx) = &self.scan_rx {
            if let Ok((mut new_groups, new_infos, new_subdirs)) = rx.try_recv() {
                eprintln!(
                    "[DEBUG-RELOAD] Replacing groups! Old groups count: {}, New groups count: {}",
                    self.state.groups.len(),
                    new_groups.len()
                );

                // SORTING LOGIC: Ensure content subgroups are contiguous.
                // We sort primarily by pixel_hash, secondarily by path.
                // This keeps "C1" files together, "C2" together, etc.
                for group in &mut new_groups {
                    group.sort_by(|a, b| {
                        match (a.pixel_hash, b.pixel_hash) {
                            (Some(ha), Some(hb)) => {
                                if ha == hb {
                                    a.path.cmp(&b.path)
                                } else {
                                    ha.cmp(&hb)
                                }
                            }
                            // Put files WITH pixel hash (potential content matches) before those without?
                            // Or standard Option ordering: None < Some.
                            // Let's use standard Ord which puts None first.
                            // This means "Unmatched" files might appear at top/bottom,
                            // but all "Some(hash)" will be grouped.
                            (h1, h2) => {
                                let ord = h1.cmp(&h2);
                                if ord == std::cmp::Ordering::Equal {
                                    a.path.cmp(&b.path)
                                } else {
                                    ord
                                }
                            }
                        }
                    });
                }

                if let Some(first_group) = new_groups.first() {
                    for (i, file) in first_group.iter().enumerate().take(5) {
                        eprintln!(
                            "[DEBUG-RELOAD]   new_groups[0][{}]: {:?}, orientation={}",
                            i,
                            file.path.file_name().unwrap_or_default(),
                            file.orientation
                        );
                    }
                }

                // Only replace if we have results (duplicate mode) or finished view mode
                self.state.groups = new_groups;
                self.cache_dirty = true;
                self.state.group_infos = new_infos;
                self.subdirs = new_subdirs;
                self.refresh_dir_cache(false);
                self.state.last_file_count = self.state.groups.iter().map(|g| g.len()).sum();

                // Clamp or reset indices to prevent panic if new list is smaller
                if self.state.groups.is_empty() {
                    self.state.current_group_idx = 0;
                    self.state.current_file_idx = 0;
                } else {
                    if self.state.current_group_idx >= self.state.groups.len() {
                        self.state.current_group_idx = self.state.groups.len() - 1;
                        self.state.current_file_idx = 0; // Reset file index if we jumped groups
                    }
                    // Also check file index bounds for the current group
                    let group_len = self.state.groups[self.state.current_group_idx].len();
                    if self.state.current_file_idx >= group_len {
                        self.state.current_file_idx = group_len.saturating_sub(1);
                    }
                }

                self.state.is_loading = false;
                self.scan_rx = None;
                self.scan_progress_rx = None;
                self.scan_batch_rx = None;

                needs_repaint = true;
            }
        }

        // FORCE UI WAKE-UP
        if needs_repaint {
            ctx.request_repaint();
        }
        // Crucial: If we are still loading, request another frame soon
        // to keep polling the channels even if the user isn't moving the mouse.
        if self.state.is_loading {
            ctx.request_repaint_after(std::time::Duration::from_millis(100));
        }
    }

    pub(super) fn get_title_string(&self) -> String {
        if self.state.view_mode {
            let dir_count = self.subdirs.len()
                + if self.current_dir.as_ref().and_then(|c| c.parent()).is_some() { 1 } else { 0 };
            if dir_count > 0 {
                format!(
                    "{} | Dirs: {} | Files: {}",
                    APP_TITLE, dir_count, self.state.last_file_count
                )
            } else {
                format!("{} | Files: {}", APP_TITLE, self.state.last_file_count)
            }
        } else {
            format!(
                "{} | Groups: {} | Files: {}",
                APP_TITLE,
                self.state.groups.len(),
                self.state.last_file_count
            )
        }
    }

    pub(super) fn update_view_state<F>(&mut self, f: F)
    where
        F: FnOnce(&mut GroupViewState),
    {
        let idx = self.state.current_group_idx;
        let entry = self.group_views.entry(idx).or_default();
        f(entry);
    }

    /// Handles both standard image preloading (via egui) and Raw preloading (via worker pool)
    /// In duplicate mode (multiple groups), preloads files from current and nearby groups.
    pub(super) fn perform_preload(&mut self, _ctx: &egui::Context) {
        if self.state.groups.is_empty() {
            return;
        }

        let current_g = self.state.current_group_idx;
        let current_f = self.state.current_file_idx;

        if let Some((lg, lf)) = self.last_preload_pos
            && lg == current_g
            && lf == current_f
        {
            return;
        }
        self.last_preload_pos = Some((current_g, current_f));

        let preload_limit = self.ctx.gui_config.preload_count.unwrap_or(10);
        let mut active_window_paths = HashSet::new();

        // Collect paths to preload, respecting preload_limit across all groups
        let mut paths_to_preload: Vec<(std::path::PathBuf, bool)> = Vec::new(); // (path, is_current)

        // Single group mode (--view) or multiple groups mode (duplicate finder)
        if self.state.groups.len() == 1 {
            // Original behavior: preload within the single group
            let group = &self.state.groups[0];
            let half = preload_limit / 2;
            let start = current_f.saturating_sub(half);
            let end = (start + preload_limit).min(group.len());
            let start =
                if end - start < preload_limit { end.saturating_sub(preload_limit) } else { start };

            for i in start..end {
                paths_to_preload.push((group[i].path.clone(), i == current_f));
            }
        } else {
            // Multiple groups: preload current group + files from nearby groups
            let current_group = &self.state.groups[current_g];

            // Add all files from current group (these are most important)
            for (i, file) in current_group.iter().enumerate() {
                paths_to_preload.push((file.path.clone(), i == current_f));
            }

            // Calculate remaining preload slots after current group
            let remaining = preload_limit.saturating_sub(current_group.len());

            if remaining > 0 {
                // Preload from adjacent groups (next group first, then previous)
                let mut extra_paths = Vec::new();

                // Next group(s)
                let mut next_g = current_g + 1;
                let mut slots_left = remaining / 2 + remaining % 2; // Give slightly more to next
                while next_g < self.state.groups.len() && slots_left > 0 {
                    let group = &self.state.groups[next_g];
                    for file in group.iter().take(slots_left) {
                        extra_paths.push((file.path.clone(), false));
                        slots_left -= 1;
                    }
                    next_g += 1;
                }

                // Previous group(s)
                slots_left = remaining / 2;
                let mut prev_g = current_g.saturating_sub(1);
                while prev_g < current_g && slots_left > 0 {
                    let group = &self.state.groups[prev_g];
                    for file in group.iter().take(slots_left) {
                        extra_paths.push((file.path.clone(), false));
                        slots_left -= 1;
                    }
                    if prev_g == 0 {
                        break;
                    }
                    prev_g -= 1;
                }

                paths_to_preload.extend(extra_paths);
            }
        }

        // Build active window set
        for (path, _) in &paths_to_preload {
            active_window_paths.insert(path.clone());
        }

        // Update shared active window
        if let Ok(mut w) = self.active_window.write() {
            *w = active_window_paths.clone();
        }

        for (path, is_current) in &paths_to_preload {
            if *is_current {
                // Load EVERYTHING via the pool, not just RAW
                if !self.raw_cache.contains_key(path) && !self.raw_loading.contains(path) {
                    self.raw_loading.insert(path.clone());
                    let _ = self.image_preload_tx.send(path.clone());
                }
                break;
            }
        }

        // Then other files
        for (path, is_current) in &paths_to_preload {
            if *is_current {
                continue;
            }
            if !self.raw_cache.contains_key(path) && !self.raw_loading.contains(path) {
                self.raw_loading.insert(path.clone());
                let _ = self.image_preload_tx.send(path.clone());
            }
        }

        // Cache Eviction
        self.raw_cache.retain(|k, _| active_window_paths.contains(k));
        self.raw_loading.retain(|k| active_window_paths.contains(k));
    }

    /// Get list of subdirectories for directory picker (including "..")
    pub(super) fn get_subdirectories(&self) -> Vec<std::path::PathBuf> {
        let mut dirs = Vec::new();

        // Add parent directory if it exists
        if let Some(ref current) = self.current_dir
            && let Some(parent) = current.parent()
        {
            dirs.push(parent.to_path_buf());
        }

        // Add stored subdirectories
        dirs.extend(self.subdirs.clone());

        dirs
    }

    /// Open directory picker dialog
    pub(super) fn open_dir_picker(&mut self) {
        self.dir_list = self.get_subdirectories();
        self.dir_picker_selection = 0;
        self.show_dir_picker = true;
    }

    /// Go up one directory level
    pub(super) fn go_up_directory(&mut self) {
        if let Some(ref current) = self.current_dir.clone()
            && let Some(parent) = current.parent()
        {
            self.change_directory(parent.to_path_buf());
        }
    }

    pub fn run(self) -> Result<(), eframe::Error> {
        // Config stores physical pixels (screen_rect * ppp after font_scale applied)
        // with_inner_size is called BEFORE font_scale, when ppp=1.0
        // So physical pixels = logical points at that moment
        let width = self.ctx.gui_config.width.unwrap_or(1280) as f32;
        let height = self.ctx.gui_config.height.unwrap_or(720) as f32;

        eprintln!(
            "[DEBUG-RUN] Setting window size to {}x{} (physical pixels = logical points at ppp=1)",
            width, height
        );
        eprintln!("[DEBUG-RUN] self.panel_width at run() = {}", self.panel_width);

        let options = eframe::NativeOptions {
            renderer: eframe::Renderer::Wgpu,
            viewport: egui::ViewportBuilder::default()
                .with_inner_size([width, height])
                .with_decorations(false),
            ..Default::default()
        };

        let gui_config = self.ctx.gui_config.clone();

        eframe::run_native(
            "phdupes",
            options,
            Box::new(move |cc| {
                egui_extras::install_image_loaders(&cc.egui_ctx);
                let mut fonts = egui::FontDefinitions::default();

                #[cfg(feature = "embed-fonts")]
                {
                    const SARASA_TTC: &[u8] =
                        include_bytes!("../../assets/fonts/Sarasa-Regular.ttc");

                    eprintln!("[INFO] Compiling with embedded Sarasa fonts.");
                    // Setup Proportional Font (Sarasa UI SC, Index 7)
                    let mut font_ui = egui::FontData::from_static(SARASA_TTC);
                    font_ui.index = 7;
                    fonts.font_data.insert("Sarasa UI SC".to_owned(), Arc::new(font_ui));

                    // Setup Monospace Font (Sarasa Term SC, Index 25)
                    let mut font_mono = egui::FontData::from_static(SARASA_TTC);
                    font_mono.index = 25;
                    fonts.font_data.insert("Sarasa Term SC".to_owned(), Arc::new(font_mono));

                    // Insert at 0 to make them the primary font for that family
                    if let Some(family) = fonts.families.get_mut(&egui::FontFamily::Proportional) {
                        family.insert(0, "Sarasa UI SC".to_owned());
                    }
                    if let Some(family) = fonts.families.get_mut(&egui::FontFamily::Monospace) {
                        family.insert(0, "Sarasa Term SC".to_owned());
                    }
                }

                let mut configure_font = |name: &str, family: egui::FontFamily| {
                    if let Ok(data) = fs::read(name) {
                        fonts
                            .font_data
                            .insert(name.to_owned(), Arc::new(egui::FontData::from_owned(data)));
                        if let Some(vec) = fonts.families.get_mut(&family) {
                            vec.insert(0, name.to_owned());
                        } else {
                            fonts.families.insert(family, vec![name.to_owned()]);
                        }
                    }
                };

                if let Some(mono) = &gui_config.font_monospace {
                    configure_font(mono, egui::FontFamily::Monospace);
                }
                if let Some(ui_font) = &gui_config.font_ui {
                    configure_font(ui_font, egui::FontFamily::Proportional);
                }

                cc.egui_ctx.set_fonts(fonts);
                Ok(Box::new(self))
            }),
        )
    }
}

impl Drop for GuiApp {
    fn drop(&mut self) {
        // Save window size and panel width to config
        let mut gui_config = self.ctx.gui_config.clone();
        if let Some((w, h)) = self.last_window_size {
            gui_config.width = Some(w);
            gui_config.height = Some(h);
            eprintln!("Saving window size: {}x{}", w, h);
        } else {
            eprintln!("Warning: No window size captured");
        }
        // panel_width is in current logical points (after font_scale)
        // Save it directly - we'll scale when loading
        gui_config.panel_width = Some(self.panel_width);
        eprintln!("Saving panel width: {}", self.panel_width);
        eprintln!(
            "[DEBUG-EXIT] Calling save_gui_config with width={:?}, height={:?}, panel_width={:?}",
            gui_config.width, gui_config.height, gui_config.panel_width
        );
        if let Err(e) = self.ctx.save_gui_config(&gui_config) {
            eprintln!("Error saving config: {}", e);
        } else {
            eprintln!("[DEBUG-EXIT] save_gui_config succeeded");
        }
    }
}

impl eframe::App for GuiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.check_fs_events(ctx);
        // Initial setup for view mode: create watcher (but don't refresh while scanning)
        if self.state.view_mode && self.current_dir.is_some() && self.watcher.is_none() {
            self.setup_watcher();
            // Only refresh cache if not currently scanning (scan will populate groups)
            if !self.state.is_loading {
                self.refresh_dir_cache(true);
            }
        }

        let title_text = if self.state.is_loading {
            format!("{} | Scanning... {}/{}", APP_TITLE, self.scan_progress.0, self.scan_progress.1)
        } else {
            self.get_title_string()
        };

        // Send the title to the OS (updates Alt-Tab / Taskbar name)
        ctx.send_viewport_cmd(egui::ViewportCommand::Title(title_text.clone()));
        if !self.state.is_fullscreen {
            egui::TopBottomPanel::top("custom_title_bar").show(ctx, |ui| {
                ui.horizontal(|ui| {
                    let height = 12.0;
                    ui.label(egui::RichText::new(title_text).strong());
                    // --- Window Dragging Logic ---
                    let available_width = ui.available_width() - 60.0;
                    let response = ui.allocate_response(
                        egui::vec2(available_width, height),
                        egui::Sense::click_and_drag(),
                    );
                    if response.dragged() {
                        ctx.send_viewport_cmd(egui::ViewportCommand::StartDrag);
                    }

                    // --- Window Controls ---
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.button("\u{274c}").clicked() {
                            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                        }
                        if ui.button("\u{1f5d6}").clicked() {
                            let is_maximized =
                                ctx.input(|i| i.viewport().maximized.unwrap_or(false));
                            ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(!is_maximized));
                        }
                        if ui.button("\u{1f5d5}").clicked() {
                            ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(true));
                        }
                    });
                });
            });
        }

        // Sync metadata title (for Alt-Tab / Taskbar)
        let current_title = if self.state.is_loading {
            format!("Scanning... {}/{}", self.scan_progress.0, self.scan_progress.1)
        } else {
            self.get_title_string()
        };
        ctx.send_viewport_cmd(egui::ViewportCommand::Title(current_title));

        // Local flag to force egui to respect our manual resize this frame
        let mut force_panel_resize = false;

        if !self.initial_scale_applied {
            let user_scale = self.ctx.gui_config.font_scale.unwrap_or(1.0);
            ctx.set_pixels_per_point(ctx.pixels_per_point() * user_scale);
            self.initial_scale_applied = true;
        }

        if let Some(set_time) = self.state.status_set_time
            && set_time.elapsed() > std::time::Duration::from_secs(5)
        {
            self.state.status_message = None;
            self.state.status_set_time = None;
        }

        // Receive finished raw images from worker thread pool
        // Use try_recv() which returns Err on empty OR disconnected channel
        loop {
            match self.image_preload_rx.try_recv() {
                Ok((path, maybe_result)) => {
                    if let Some((color_image, actual_resolution, orientation)) = maybe_result {
                        // Now 'orientation' is defined and passed correctly
                        super::image::update_file_metadata(
                            self,
                            &path,
                            actual_resolution.0,
                            actual_resolution.1,
                            orientation,
                        );

                        let name = format!("img_{}", path.display());
                        let texture = ctx.load_texture(name, color_image, Default::default());
                        self.raw_cache.insert(path.clone(), texture);
                    }
                    // Always remove from loading set
                    self.raw_loading.remove(&path);
                    ctx.request_repaint();
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    break;
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    // Channel disconnected (worker threads terminated)
                    eprintln!(
                        "[WARN] Image preload channel disconnected - worker threads may have terminated"
                    );
                    break;
                }
            }
        }

        // View mode: Process background directory scan results
        if let Some(ref rx) = self.dir_scan_rx {
            let mut received_any = false;
            loop {
                match rx.try_recv() {
                    Ok(batch) => {
                        received_any = true;
                        if let Some(group) = self.state.groups.first_mut() {
                            let start_idx = group.len();
                            group.extend(batch);

                            // Update file_index for new files
                            for (i, file) in group[start_idx..].iter().enumerate() {
                                self.file_index.insert(file.unique_file_id, start_idx + i);
                            }
                        }
                        self.cache_dirty = true;
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => break,
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        // Scan complete - start enrichment for files missing GPS
                        self.state.is_loading = false;
                        self.state.last_file_count =
                            self.state.groups.first().map_or(0, |g| g.len());

                        // Collect files needing enrichment
                        if let Some(group) = self.state.groups.first() {
                            let files_to_enrich: Vec<_> = group
                                .iter()
                                .filter(|f| f.gps_pos.is_none())
                                .map(|f| {
                                    (f.path.clone(), f.unique_file_id, f.resolution, f.orientation)
                                })
                                .collect();

                            if !files_to_enrich.is_empty() {
                                let (result_tx, result_rx) = unbounded::<EnrichmentResult>();
                                scanner::spawn_background_enrichment(
                                    files_to_enrich,
                                    self.ctx.content_key,
                                    self.ctx.meta_key,
                                    self.db_tx.clone(),
                                    result_tx,
                                );
                                self.enrichment_rx = Some(result_rx);
                            }
                        }
                        self.dir_scan_rx = None;
                        break;
                    }
                }
            }
            if received_any {
                ctx.request_repaint();
            }
        }

        // Process background enrichment results (view mode)
        // This updates FileMetadata with computed content_hash and GPS coordinates
        // Database writing is handled by scanner::spawn_background_enrichment
        if let Some(ref rx) = self.enrichment_rx {
            loop {
                match rx.try_recv() {
                    Ok(result) => {
                        // O(1) lookup using file_index
                        if let Some(&file_idx) = self.file_index.get(&result.unique_file_id) {
                            if let Some(group) = self.state.groups.first_mut() {
                                if let Some(file) = group.get_mut(file_idx) {
                                    file.content_hash = result.content_hash;
                                    if result.gps_pos.is_some() {
                                        file.gps_pos = result.gps_pos;
                                    }

                                    // Add GPS marker if we found coordinates
                                    if let Some(pos) = result.gps_pos {
                                        self.gps_map.add_marker(
                                            file.path.clone(),
                                            pos.y(),
                                            pos.x(),
                                        );
                                    }
                                }
                            }
                        }
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => break,
                    Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                }
            }
        }

        self.check_reload(ctx);
        self.perform_preload(ctx);

        let intent = RefCell::new(None::<InputIntent>);

        // Handle input and dialogs
        super::dialogs::handle_input(self, ctx, &intent, &mut force_panel_resize);
        super::dialogs::handle_dialogs(self, ctx, &mut force_panel_resize, &intent);

        // Handle RefreshDirCache (Ctrl+L) - preserves resolution data
        if let Some(InputIntent::RefreshDirCache) = *intent.borrow() {
            if self.state.view_mode {
                self.refresh_dir_cache(true);
                self.cache_dirty = true;
                // Force re-preload in case current file changed (e.g., after rename)
                self.last_preload_pos = None;
            } else {
                // Duplicate mode: trigger full rescan
                self.state.is_loading = true;
            }
        }

        // --- RENDER ---
        let current_image_path = self.state.get_current_image_path().cloned();
        let current_group_idx = self.state.current_group_idx;
        let current_view_mode =
            *self.group_views.get(&current_group_idx).unwrap_or(&GroupViewState::default());

        if !self.state.is_fullscreen {
            egui::TopBottomPanel::bottom("status").show(ctx, |ui| {
                if let Some((msg, is_error)) = &self.state.status_message {
                    ui.colored_label(
                        if *is_error { egui::Color32::RED } else { egui::Color32::GREEN },
                        msg,
                    );
                } else {
                    let mode_str = match current_view_mode.mode {
                        ViewMode::FitWindow => "Fit Window",
                        ViewMode::FitWidth => "Fit Width",
                        ViewMode::FitHeight => "Fit Height",
                        ViewMode::ManualZoom(_) => "Zoom",
                    };
                    let extra = match current_view_mode.mode {
                        ViewMode::ManualZoom(z) if (z - 1.0).abs() < 0.1 => {
                            if self.state.zoom_relative {
                                " 1x".to_string()
                            } else {
                                " 1:1".to_string()
                            }
                        }
                        ViewMode::ManualZoom(z) => format!(" {:.0}x", z),
                        _ => "".to_string(),
                    };
                    let rel_tag = if self.state.zoom_relative { " [REL]" } else { " [ABS]" };
                    let filename = current_image_path
                        .as_ref()
                        .map(|p| p.file_name().unwrap_or_default().to_string_lossy().to_string())
                        .unwrap_or_default();

                    let slideshow_status = if self.state.slideshow_interval.is_some() {
                        if self.state.slideshow_paused {
                            " | [S]lideshow: PAUSED"
                        } else {
                            " | [S]lideshow: ON"
                        }
                    } else {
                        ""
                    };

                    let move_status =
                        if self.state.move_target.is_some() { " | [M]ove" } else { "" };
                    let del_key = if self.state.view_mode { " | [Del]ete" } else { "" };
                    let rot_str = if !self.state.manual_rotation.is_multiple_of(4) {
                        format!(" | [O] Rot: {}°", (self.state.manual_rotation % 4) * 90)
                    } else {
                        "".to_string()
                    };
                    let sort_str = if self.state.view_mode { " | [T] Sort" } else { "" };
                    let hist_str = if self.show_histogram { " | [I] Hist" } else { "" };
                    let exif_str = if self.show_exif { " | [E] EXIF" } else { "" };
                    let pos_str = if !self.state.groups.is_empty() {
                        let total: usize = self.state.groups.iter().map(|g| g.len()).sum();
                        let current: usize = self
                            .state
                            .groups
                            .iter()
                            .take(self.state.current_group_idx)
                            .map(|g| g.len())
                            .sum::<usize>()
                            + self.state.current_file_idx
                            + 1;
                        format!(" [{}/{}]", current, total)
                    } else {
                        "".to_string()
                    };

                    // GPS map toggle indicator
                    let gps_map_str = if self.gps_map.visible { " | [N] Map" } else { "" };

                    // Get distance to selected location (right-justified)
                    let distance_str = self.get_distance_to_location();

                    ui.horizontal(|ui| {
                        ui.label(format!(
                            "W: {}{} | Z: Zoom{}{}{}{}{}{}{}{}{}",
                            mode_str,
                            extra,
                            rel_tag,
                            slideshow_status,
                            move_status,
                            del_key,
                            sort_str,
                            rot_str,
                            hist_str,
                            exif_str,
                            gps_map_str
                        ));
                        ui.separator();
                        ui.label(pos_str);
                        if !filename.is_empty() {
                            ui.separator();
                            ui.label(
                                egui::RichText::new(filename)
                                    .size(14.0)
                                    .family(egui::FontFamily::Monospace)
                                    .strong(),
                            );
                        }

                        // Right-justify the distance/bearing info (clickable to toggle direction)
                        if let Some(dist) = distance_str {
                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    let response = ui.add(
                                        egui::Label::new(
                                            egui::RichText::new(&dist)
                                                .size(12.0)
                                                .family(egui::FontFamily::Monospace)
                                                .color(egui::Color32::LIGHT_BLUE),
                                        )
                                        .sense(egui::Sense::click()),
                                    );
                                    if response.clicked() {
                                        self.toggle_distance_direction();
                                    }
                                    response.on_hover_text("Click to toggle direction");
                                },
                            );
                        }
                    });
                }
            });

            // Restore Detailed File List
            // Get actual window width in logical points - try viewport rect first, fall back to used_rect
            let window_width = ctx
                .input(|i| i.viewport().inner_rect.or(i.viewport().outer_rect).map(|r| r.width()))
                .unwrap_or_else(|| ctx.used_rect().width());
            let panel_max_width = window_width * 0.5;

            // Delay panel width restoration until after font_scale is applied
            let ppp = ctx.pixels_per_point();

            // Check for >100 to prevent clamping to 0.0 on the first frame if viewport isn't ready.
            let window_ready = window_width > 100.0;
            let should_apply_saved_width = !self.initial_panel_width_applied && window_ready;

            if should_apply_saved_width {
                eprintln!(
                    "[DEBUG-PANEL] Applying saved panel width {} (ppp={})",
                    self.saved_panel_width, ppp
                );
                self.initial_panel_width_applied = true;
            }

            let panel_builder =
                egui::SidePanel::left("list_panel").resizable(true).min_width(160.0);

            // Apply width logic to the builder
            let panel = if force_panel_resize {
                // User pressed 'V' or 'B'
                panel_builder.exact_width(self.panel_width)
            } else if self.initial_panel_width_applied {
                // Normal running state: Use saved width as default.
                // Since we switched IDs to "list_panel_main", this will be respected on the first "applied" frame.
                panel_builder.default_width(self.saved_panel_width).max_width(panel_max_width)
            } else {
                // Startup state: just keep it functional
                panel_builder.default_width(200.0).max_width(panel_max_width)
            };

            let panel_response = panel.show(ctx, |ui| {
                // Show current directory header in view mode
                if self.state.view_mode
                    && let Some(ref current_dir) = self.current_dir
                {
                    ui.horizontal(|ui| {
                        ui.label(egui::RichText::new("\u{1f4c1}").size(16.0));
                        ui.add(
                            egui::Label::new(
                                egui::RichText::new(current_dir.to_string_lossy())
                                    .size(12.0)
                                    .family(egui::FontFamily::Monospace)
                                    .color(egui::Color32::LIGHT_BLUE),
                            )
                            .wrap_mode(egui::TextWrapMode::Truncate),
                        );
                    });
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new("[C] Change dir  [.] Go down")
                                .size(10.0)
                                .color(egui::Color32::GRAY),
                        );
                    });
                    ui.separator();
                }

                // Calculate target scroll offset if we need to scroll to selected item
                let scroll_to_file =
                    self.state.selection_changed && self.dir_selection_idx.is_none();
                let scroll_to_dir = self.dir_scroll_to_selection;

                egui::ScrollArea::vertical().id_salt("file_list_scroll").show(ui, |ui| {
                    let no_files = self.state.groups.is_empty()
                        || self.state.groups.iter().all(|g| g.is_empty());
                    let no_dirs = self.subdirs.is_empty()
                        && self.current_dir.as_ref().and_then(|c| c.parent()).is_none();

                    if no_files && no_dirs {
                        // Subtract 16.0 to account for egui's frame margins
                        ui.set_min_width((self.saved_panel_width - 16.0).max(100.0));
                        ui.label(if self.state.view_mode {
                            "No images found."
                        } else {
                            "No duplicates found."
                        });
                        return;
                    }

                    let mut dir_to_open: Option<std::path::PathBuf> = None;

                    // Calculate offset caused by directories
                    let files_start_offset = if self.state.view_mode && !self.state.is_loading {
                        let start_cursor_y = ui.cursor().min.y;
                        let mut dir_idx: usize = 0;
                        let available_w = ui.available_width();

                        // --- 1. Render Parent ".." ---
                        if let Some(ref entry) = self.parent_cache {
                            let is_selected = self.dir_selection_idx == Some(dir_idx);
                            let mod_time_str = &entry.modified_display;

                            let row_height = ui.text_style_height(&egui::TextStyle::Body) + 4.0;
                            let (rect, resp) = ui.allocate_exact_size(
                                egui::vec2(available_w, row_height),
                                egui::Sense::click(),
                            );

                            if is_selected {
                                ui.painter().rect_filled(rect, 2.0, ui.visuals().selection.bg_fill);
                            } else if resp.hovered() {
                                ui.painter().rect_filled(
                                    rect,
                                    2.0,
                                    ui.visuals().widgets.hovered.bg_fill,
                                );
                            }

                            ui.painter().text(
                                rect.left_center() + egui::vec2(4.0, 0.0),
                                egui::Align2::LEFT_CENTER,
                                "\u{1f4c1} ..",
                                egui::FontId::default(),
                                egui::Color32::YELLOW,
                            );
                            ui.painter().text(
                                rect.right_center() - egui::vec2(4.0, 0.0),
                                egui::Align2::RIGHT_CENTER,
                                mod_time_str,
                                egui::FontId::new(10.0, egui::FontFamily::Monospace),
                                egui::Color32::GRAY,
                            );

                            if resp.clicked() {
                                dir_to_open = Some(entry.path.clone());
                            }
                            if is_selected && scroll_to_dir {
                                resp.scroll_to_me(Some(egui::Align::Center));
                            }
                            dir_idx += 1;
                        }

                        // --- 2. Render Subdirectories ---
                        for entry in &self.subdirs_cache {
                            let is_selected = self.dir_selection_idx == Some(dir_idx);
                            let dir_name = &entry.display_name;
                            let mod_time_str = &entry.modified_display;

                            let row_height = ui.text_style_height(&egui::TextStyle::Body) + 4.0;
                            let (rect, resp) = ui.allocate_exact_size(
                                egui::vec2(available_w, row_height),
                                egui::Sense::click(),
                            );

                            if is_selected {
                                ui.painter().rect_filled(rect, 2.0, ui.visuals().selection.bg_fill);
                            } else if resp.hovered() {
                                ui.painter().rect_filled(
                                    rect,
                                    2.0,
                                    ui.visuals().widgets.hovered.bg_fill,
                                );
                            }

                            // Calculate available width for directory name
                            // Account for: folder icon prefix, time suffix, and padding
                            let font_id = egui::FontId::default();
                            let time_font = egui::FontId::new(10.0, egui::FontFamily::Monospace);
                            let folder_prefix = "\u{1f4c1} ";
                            let prefix_galley = ui.painter().layout_no_wrap(
                                folder_prefix.to_string(),
                                font_id.clone(),
                                egui::Color32::WHITE,
                            );
                            let time_galley = ui.painter().layout_no_wrap(
                                mod_time_str.to_string(),
                                time_font.clone(),
                                egui::Color32::WHITE,
                            );
                            let dir_name_max_width = (rect.width()
                                - prefix_galley.rect.width()
                                - time_galley.rect.width()
                                - 16.0)
                                .max(20.0);

                            let (display_dir_name, was_truncated) =
                                truncate_to_width(dir_name, dir_name_max_width, &font_id, ui);

                            // Render directory name with truncation
                            if was_truncated && display_dir_name.ends_with('…') {
                                // Draw main part in color, ellipsis in grey
                                let main_part: String = display_dir_name
                                    .chars()
                                    .take(display_dir_name.chars().count() - 1)
                                    .collect();
                                let main_galley = ui.painter().layout_no_wrap(
                                    format!("{}{}", folder_prefix, main_part),
                                    font_id.clone(),
                                    egui::Color32::LIGHT_BLUE,
                                );
                                ui.painter().galley(
                                    rect.left_center()
                                        + egui::vec2(4.0, -main_galley.rect.height() / 2.0),
                                    main_galley,
                                    egui::Color32::LIGHT_BLUE,
                                );
                                let ellipsis_x = rect.left()
                                    + 4.0
                                    + prefix_galley.rect.width()
                                    + ui.painter()
                                        .layout_no_wrap(
                                            main_part,
                                            font_id.clone(),
                                            egui::Color32::WHITE,
                                        )
                                        .rect
                                        .width();
                                ui.painter().text(
                                    egui::pos2(ellipsis_x, rect.center().y),
                                    egui::Align2::LEFT_CENTER,
                                    "…",
                                    font_id.clone(),
                                    egui::Color32::GRAY,
                                );
                            } else {
                                ui.painter().text(
                                    rect.left_center() + egui::vec2(4.0, 0.0),
                                    egui::Align2::LEFT_CENTER,
                                    &format!("{}{}", folder_prefix, display_dir_name),
                                    font_id,
                                    egui::Color32::LIGHT_BLUE,
                                );
                            }

                            ui.painter().text(
                                rect.right_center() - egui::vec2(4.0, 0.0),
                                egui::Align2::RIGHT_CENTER,
                                mod_time_str,
                                egui::FontId::new(10.0, egui::FontFamily::Monospace),
                                egui::Color32::GRAY,
                            );

                            if resp.clicked() {
                                dir_to_open = Some(entry.path.clone());
                            }
                            if is_selected && scroll_to_dir {
                                resp.scroll_to_me(Some(egui::Align::Center));
                            }
                            dir_idx += 1;
                        }

                        if !self.subdirs_cache.is_empty() || self.parent_cache.is_some() {
                            ui.separator();
                        }

                        // Return the height consumed by directories
                        ui.cursor().min.y - start_cursor_y
                    } else {
                        0.0
                    };

                    // Clear the scroll flag after rendering directories
                    self.dir_scroll_to_selection = false;

                    // --- 1. LAYOUT CONSTANTS ---
                    let spacing = ui.spacing().item_spacing.y;
                    let header_height = ui.text_style_height(&egui::TextStyle::Body) + spacing;
                    // Calculate precise row height
                    let font_id_main = egui::TextStyle::Monospace.resolve(ui.style());
                    let font_id_meta = egui::FontId::monospace(10.0);
                    let row_btn_h = ui
                        .painter()
                        .layout_no_wrap("Ij".to_string(), font_id_main, egui::Color32::default())
                        .rect
                        .height()
                        + (ui.spacing().button_padding.y * 2.0);
                    let row_meta_h = ui
                        .painter()
                        .layout_no_wrap("Ij".to_string(), font_id_meta, egui::Color32::default())
                        .rect
                        .height();
                    let file_row_total_h = row_btn_h + spacing + row_meta_h + spacing;
                    let separator_h = 10.0;

                    // Store the row height used for the cache
                    if (self.last_row_height - file_row_total_h).abs() > 0.1 {
                        self.cache_dirty = true;
                        self.last_row_height = file_row_total_h;
                    }
                    let show_headers = !self.state.view_mode;

                    // --- 2. REBUILD LAYOUT CACHE (Once per update if dirty) ---
                    if self.cache_dirty || self.group_y_offsets.len() != self.state.groups.len() {
                        self.group_y_offsets.clear();
                        self.group_y_offsets.reserve(self.state.groups.len());

                        let mut y = 0.0;
                        for group in &self.state.groups {
                            self.group_y_offsets.push(y);
                            let header = if show_headers { header_height } else { 0.0 };
                            let body = group.len() as f32 * file_row_total_h;
                            let sep = if show_headers { separator_h } else { 0.0 };
                            y += header + body + sep;
                        }
                        self.total_content_height = y;
                        self.cache_dirty = false;
                    }

                    // --- 3. HANDLE AUTO-SCROLL (Keyboard Nav) ---
                    if self.state.selection_changed {
                        if let Some(group_start_y) =
                            self.group_y_offsets.get(self.state.current_group_idx)
                        {
                            let header_offset = if show_headers { header_height } else { 0.0 };
                            let file_offset = self.state.current_file_idx as f32 * file_row_total_h;

                            // Offset relative to content top (including directories)
                            let target_y_offset =
                                files_start_offset + group_start_y + header_offset + file_offset;

                            // Convert to SCREEN COORDINATES
                            let scroll_top = ui.min_rect().min;
                            let target_screen_pos =
                                egui::pos2(scroll_top.x, scroll_top.y + target_y_offset);

                            ui.scroll_to_rect(
                                egui::Rect::from_min_size(
                                    target_screen_pos,
                                    egui::vec2(100.0, file_row_total_h),
                                ),
                                Some(egui::Align::Center),
                            );

                            // Center GPS map on new selection if visible
                            if self.gps_map.visible {
                                if let Some(file) = self
                                    .state
                                    .groups
                                    .get(self.state.current_group_idx)
                                    .and_then(|g| g.get(self.state.current_file_idx))
                                {
                                    self.gps_map.center_on_path(&file.path);
                                }
                            }

                            self.state.selection_changed = false;
                        }
                    }

                    // --- 4. ALLOCATE SCROLL SPACE ---
                    // This creates the scrollbar thumb at the correct size
                    // Note: ui.cursor() is now *after* the directories.
                    // total_content_height contains only files. Directories are already accounted for by cursor position.
                    ui.allocate_rect(
                        egui::Rect::from_min_size(
                            ui.cursor().min,
                            egui::vec2(0.0, self.total_content_height),
                        ),
                        egui::Sense::hover(),
                    );

                    // --- 5. VISIBILITY CULLING ---
                    let clip_rect = ui.clip_rect();
                    // Scroll offset relative to FILE LIST start
                    // ui.min_rect().min.y is content top.
                    let scroll_y = (clip_rect.min.y - ui.min_rect().min.y) - files_start_offset;

                    // Binary search for the first visible group
                    let start_idx = if scroll_y <= 0.0 {
                        0
                    } else {
                        match self.group_y_offsets.binary_search_by(|y| {
                            if *y > scroll_y {
                                std::cmp::Ordering::Greater
                            } else {
                                std::cmp::Ordering::Less
                            }
                        }) {
                            Ok(i) => i,
                            Err(i) => i.saturating_sub(1),
                        }
                    };

                    let mut action_rename = false;
                    let mut action_delete = false;
                    let mut copy_path_target: Option<String> = None;

                    // --- 6. RENDER LOOP ---
                    // Base absolute Y for the file list
                    let start_y = ui.min_rect().min.y + files_start_offset;

                    for (g_idx, group) in self.state.groups.iter().enumerate().skip(start_idx) {
                        let group_y = self.group_y_offsets[g_idx];
                        let mut current_y = start_y + group_y;

                        if current_y > clip_rect.max.y {
                            break;
                        }

                        // Render Header
                        if show_headers {
                            let info = &self.state.group_infos[g_idx];
                            let header_rect = egui::Rect::from_min_size(
                                egui::pos2(ui.min_rect().left(), current_y),
                                egui::vec2(ui.available_width(), header_height),
                            );

                            if ui.is_rect_visible(header_rect) {
                                let (txt, col) = match info.status {
                                    GroupStatus::AllIdentical => (
                                        format!("Group {} - Bit-identical", g_idx + 1),
                                        egui::Color32::GREEN,
                                    ),
                                    GroupStatus::SomeIdentical => (
                                        format!("Group {} - Some Identical", g_idx + 1),
                                        egui::Color32::LIGHT_GREEN,
                                    ),
                                    GroupStatus::None => (
                                        format!("Group {} (Dist: {})", g_idx + 1, info.max_dist),
                                        egui::Color32::YELLOW,
                                    ),
                                };
                                ui.put(
                                    header_rect,
                                    egui::Label::new(egui::RichText::new(txt).color(col)),
                                );
                            }
                            current_y += header_height;
                        }

                        // Render Files
                        let counts = get_bit_identical_counts(group);
                        let hardlink_groups = get_hardlink_groups(group);
                        // Pre-calculate subgroups for this group
                        let content_subgroups = get_content_subgroups(group);

                        for (f_idx, file) in group.iter().enumerate() {
                            // 1. Calculate Rects
                            let file_rect = egui::Rect::from_min_size(
                                egui::pos2(ui.min_rect().left(), current_y),
                                egui::vec2(ui.available_width(), file_row_total_h),
                            );

                            if current_y > clip_rect.max.y {
                                break;
                            }

                            if current_y + file_row_total_h > clip_rect.min.y {
                                let is_selected = self.dir_selection_idx.is_none()
                                    && g_idx == self.state.current_group_idx
                                    && f_idx == self.state.current_file_idx;
                                let is_marked = self.state.marked_for_deletion.contains(&file.path);

                                // Status Checks
                                let is_bit_identical =
                                    *counts.get(&file.content_hash).unwrap_or(&0) > 1;
                                let is_hardlinked =
                                    hardlink_groups.contains_key(&file.unique_file_id);

                                // Content Group ID
                                let content_id =
                                    file.pixel_hash.and_then(|ph| content_subgroups.get(&ph));
                                let is_content_identical = content_id.is_some();

                                // --- LAYOUT ---
                                // Two main rects: header_rect (marker + filename) and meta_rect (details)
                                let header_rect = egui::Rect::from_min_size(
                                    egui::pos2(file_rect.min.x, current_y),
                                    egui::vec2(file_rect.width(), row_btn_h),
                                );

                                let meta_rect = egui::Rect::from_min_size(
                                    egui::pos2(file_rect.min.x, current_y + row_btn_h + spacing),
                                    egui::vec2(file_rect.width(), row_meta_h),
                                );

                                // --- TEXT GENERATION ---
                                let c_label = if let Some(id) = content_id {
                                    format!("C{} ", id)
                                } else {
                                    "  ".to_string()
                                };

                                let marker_text = format!(
                                    "{} {} {} ",
                                    if is_marked { "M" } else { " " },
                                    if is_hardlinked { "L" } else { " " },
                                    c_label
                                );
                                let filename_text =
                                    format_path_depth(&file.path, self.state.path_display_depth);

                                // --- COLORS ---
                                let (marker_color, filename_color) = if is_selected {
                                    (None, None)
                                } else if is_marked {
                                    (Some(egui::Color32::MAGENTA), Some(egui::Color32::MAGENTA))
                                } else if is_hardlinked {
                                    (
                                        Some(egui::Color32::LIGHT_BLUE),
                                        Some(egui::Color32::LIGHT_BLUE),
                                    )
                                } else if is_bit_identical {
                                    (Some(egui::Color32::GREEN), Some(egui::Color32::GREEN))
                                } else if is_content_identical {
                                    (Some(egui::Color32::GOLD), Some(egui::Color32::GOLD))
                                } else {
                                    (None, None)
                                };

                                // --- RICH TEXT ---
                                let mut marker_rich = egui::RichText::new(&marker_text)
                                    .family(egui::FontFamily::Monospace);
                                if let Some(col) = marker_color {
                                    marker_rich = marker_rich.color(col);
                                }

                                // Calculate available width for filename (header_rect minus marker width minus padding)
                                let font_id = egui::TextStyle::Monospace.resolve(ui.style());
                                let marker_galley = ui.painter().layout_no_wrap(
                                    marker_text.clone(),
                                    font_id.clone(),
                                    egui::Color32::WHITE,
                                );
                                let marker_width = marker_galley.rect.width();
                                let padding = 8.0; // Small padding for scroll bar and margins
                                let available_filename_width =
                                    (header_rect.width() - marker_width - padding).max(20.0);

                                // Truncate filename if needed
                                let (display_filename, was_truncated) = truncate_to_width(
                                    &filename_text,
                                    available_filename_width,
                                    &font_id,
                                    ui,
                                );

                                let mut filename_rich = egui::RichText::new(&display_filename)
                                    .family(egui::FontFamily::Monospace);
                                if let Some(col) = filename_color {
                                    filename_rich = filename_rich.color(col);
                                }

                                // Highlight peers
                                if let Some(current_file) = group.get(self.state.current_file_idx) {
                                    if !is_selected
                                        && current_file.pixel_hash.is_some()
                                        && current_file.pixel_hash == file.pixel_hash
                                    {
                                        let bg = egui::Color32::from_black_alpha(40);
                                        marker_rich = marker_rich.strong().background_color(bg);
                                        filename_rich = filename_rich.strong().background_color(bg);
                                    }
                                }

                                // --- RENDER ---

                                // 1. Draw Selection Backgrounds
                                if is_selected {
                                    // Draw one solid block for the header (Marker + Filename)
                                    ui.painter().rect_filled(
                                        header_rect,
                                        0.0,
                                        egui::Color32::from_rgb(0, 92, 128),
                                    );
                                    // Draw block for metadata
                                    ui.painter().rect_filled(
                                        meta_rect,
                                        0.0,
                                        egui::Color32::from_rgb(0, 76, 108),
                                    );

                                    // Update text color for contrast
                                    marker_rich = marker_rich.color(egui::Color32::WHITE);
                                    // For truncated filenames, keep the ellipsis grey even when selected
                                    if was_truncated && display_filename.ends_with('…') {
                                        // Split off the ellipsis and make it grey
                                        let main_part: String = display_filename
                                            .chars()
                                            .take(display_filename.chars().count() - 1)
                                            .collect();
                                        filename_rich = egui::RichText::new(&main_part)
                                            .family(egui::FontFamily::Monospace)
                                            .color(egui::Color32::WHITE);
                                    } else {
                                        filename_rich = filename_rich.color(egui::Color32::WHITE);
                                    }
                                }

                                // 2. Draw Text Content inside Header Rect
                                ui.scope_builder(
                                    egui::UiBuilder::new().max_rect(header_rect),
                                    |ui| {
                                        // We use a horizontal layout to put marker and filename side-by-side
                                        ui.horizontal(|ui| {
                                            ui.spacing_mut().item_spacing.x = 0.0;
                                            ui.label(marker_rich);
                                            if was_truncated && display_filename.ends_with('…') {
                                                let main_part: String = display_filename
                                                    .chars()
                                                    .take(display_filename.chars().count() - 1)
                                                    .collect();
                                                let mut main_rich = egui::RichText::new(&main_part)
                                                    .family(egui::FontFamily::Monospace);
                                                if let Some(col) = filename_color {
                                                    main_rich = main_rich.color(col);
                                                }
                                                if is_selected {
                                                    main_rich =
                                                        main_rich.color(egui::Color32::WHITE);
                                                }
                                                // Apply background for peer highlighting
                                                if let Some(current_file) =
                                                    group.get(self.state.current_file_idx)
                                                {
                                                    if !is_selected
                                                        && current_file.pixel_hash.is_some()
                                                        && current_file.pixel_hash
                                                            == file.pixel_hash
                                                    {
                                                        let bg =
                                                            egui::Color32::from_black_alpha(40);
                                                        main_rich =
                                                            main_rich.strong().background_color(bg);
                                                    }
                                                }
                                                ui.label(main_rich);
                                                ui.label(
                                                    egui::RichText::new("…")
                                                        .family(egui::FontFamily::Monospace)
                                                        .color(egui::Color32::GRAY),
                                                );
                                            } else {
                                                ui.label(filename_rich);
                                            }
                                        });
                                    },
                                );

                                // 3. Create Interactions
                                let header_resp = ui.interact(
                                    header_rect,
                                    ui.id().with("hdr").with(g_idx).with(f_idx),
                                    egui::Sense::click(),
                                );

                                let meta_resp = ui.interact(
                                    meta_rect,
                                    ui.id().with("meta").with(g_idx).with(f_idx),
                                    egui::Sense::click(),
                                );

                                // --- INTERACTION HANDLING ---
                                let any_clicked = header_resp.clicked() || meta_resp.clicked();
                                let any_sec_clicked = header_resp.secondary_clicked()
                                    || meta_resp.secondary_clicked();

                                if is_selected && scroll_to_file {
                                    header_resp.scroll_to_me(Some(egui::Align::Center));
                                }

                                if any_clicked || any_sec_clicked {
                                    self.state.current_group_idx = g_idx;
                                    self.state.current_file_idx = f_idx;
                                    self.dir_selection_idx = None;
                                    // Center GPS map on clicked file if visible
                                    if self.gps_map.visible {
                                        if let Some(file) =
                                            self.state.groups.get(g_idx).and_then(|g| g.get(f_idx))
                                        {
                                            self.gps_map.center_on_path(&file.path);
                                        }
                                    }
                                    ctx.request_repaint();
                                }

                                if header_resp.hovered() || meta_resp.hovered() {
                                    ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
                                }

                                // Context Menu (Shared)
                                let context_menu_logic =
                                    |ui: &mut egui::Ui,
                                     action_rename: &mut bool,
                                     action_delete: &mut bool,
                                     copy_target: &mut Option<String>,
                                     path: &std::path::Path| {
                                        if ui.button("Rename (R)").clicked() {
                                            ui.close();
                                            *action_rename = true;
                                        }
                                        if ui.button("Copy full path").clicked() {
                                            ui.close();
                                            *copy_target = Some(path.to_string_lossy().to_string());
                                        }
                                        if ui.button("Delete (Del)").clicked() {
                                            ui.close();
                                            *action_delete = true;
                                        }
                                    };

                                // Attach context menu to both rects
                                header_resp.context_menu(|ui| {
                                    context_menu_logic(
                                        ui,
                                        &mut action_rename,
                                        &mut action_delete,
                                        &mut copy_path_target,
                                        &file.path,
                                    )
                                });
                                meta_resp.context_menu(|ui| {
                                    context_menu_logic(
                                        ui,
                                        &mut action_rename,
                                        &mut action_delete,
                                        &mut copy_path_target,
                                        &file.path,
                                    )
                                });

                                // --- RENDER METADATA ---
                                let size_str = if file.size < 1024 {
                                    format!("{} B", file.size)
                                } else if file.size < 1048576 {
                                    format!("{:.2} KiB", file.size as f32 / 1024.0)
                                } else {
                                    format!("{:.2} MiB", file.size as f32 / 1048576.0)
                                };
                                let time_str = if self.state.show_relative_times {
                                    let ts = Timestamp::from_second(file.modified.timestamp())
                                        .unwrap()
                                        .checked_add(jiff::SignedDuration::from_nanos(
                                            file.modified.timestamp_subsec_nanos() as i64,
                                        ))
                                        .unwrap();
                                    format_relative_time(ts)
                                } else {
                                    file.modified.format("%Y-%m-%d %H:%M:%S").to_string()
                                };
                                let res_str = file
                                    .resolution
                                    .map(|(w, h)| {
                                        if w > MAX_TEXTURE_SIDE.try_into().unwrap()
                                            || h > MAX_TEXTURE_SIDE.try_into().unwrap()
                                        {
                                            format!("<{}x{}  ", w, h)
                                        } else {
                                            format!(" {}x{}  ", w, h)
                                        }
                                    })
                                    .unwrap_or_default();

                                let w_meta = meta_rect.width();
                                let h_meta = meta_rect.height();
                                let x_meta = meta_rect.min.x;
                                let y_meta = meta_rect.min.y;
                                let w_date = w_meta * 0.50;
                                let w_col = w_meta * 0.25;

                                let r_date = egui::Rect::from_min_size(
                                    egui::pos2(x_meta, y_meta),
                                    egui::vec2(w_date, h_meta),
                                );
                                let r_size = egui::Rect::from_min_size(
                                    egui::pos2(x_meta + w_date, y_meta),
                                    egui::vec2(w_col, h_meta),
                                );
                                let r_res = egui::Rect::from_min_size(
                                    egui::pos2(x_meta + w_date + w_col, y_meta),
                                    egui::vec2(w_col, h_meta),
                                );

                                let meta_color = if is_selected {
                                    egui::Color32::WHITE
                                } else {
                                    egui::Color32::GRAY
                                };
                                let make_text =
                                    |s| egui::RichText::new(s).size(10.0).color(meta_color);

                                ui.scope_builder(
                                    egui::UiBuilder::new()
                                        .max_rect(r_date)
                                        .layout(egui::Layout::left_to_right(egui::Align::Center)),
                                    |ui| {
                                        ui.label(make_text(time_str));
                                    },
                                );
                                ui.scope_builder(
                                    egui::UiBuilder::new()
                                        .max_rect(r_size)
                                        .layout(egui::Layout::right_to_left(egui::Align::Center)),
                                    |ui| {
                                        ui.label(make_text(size_str));
                                    },
                                );
                                ui.scope_builder(
                                    egui::UiBuilder::new()
                                        .max_rect(r_res)
                                        .layout(egui::Layout::right_to_left(egui::Align::Center)),
                                    |ui| {
                                        ui.label(make_text(res_str));
                                    },
                                );
                            }
                            current_y += file_row_total_h;
                        }
                    }

                    // Execute Context Menu Actions (Outside Loop)
                    if let Some(path) = copy_path_target {
                        ctx.copy_text(path);
                    }
                    if action_rename {
                        if let Some(path) = self.state.get_current_image_path() {
                            self.rename_input =
                                path.file_name().unwrap_or_default().to_string_lossy().to_string();
                        }
                        self.completion_candidates.clear();
                        self.completion_index = 0;
                        self.state.handle_input(InputIntent::StartRename);
                    }

                    if action_delete {
                        self.state.handle_input(InputIntent::ExecuteDelete);
                    }

                    // Defer directory change to avoid borrow conflict
                    if let Some(dir) = dir_to_open {
                        self.dir_selection_idx = None;
                        self.change_directory(dir);
                    }
                });
            });
            // In Duplicate Mode, groups are empty during the scan.
            let has_content = !self.state.groups.is_empty();

            if window_width > 400.0 && has_content {
                let current_w = panel_response.response.rect.width();
                if current_w > 200.0 {
                    self.panel_width = current_w;
                }
            }
        }

        // GPS Map Panel (right side, when visible)
        let mut map_clicked_path: Option<std::path::PathBuf> = None;
        if self.gps_map.visible {
            egui::SidePanel::right("gps_map_panel")
                .resizable(true)
                .default_width(400.0)
                .min_width(200.0)
                .show(ctx, |ui| {
                    ui.heading("GPS Map");
                    ui.separator();

                    // Provider selector dropdown
                    ui.horizontal(|ui| {
                        ui.label("Provider:");
                        let current_provider = self.gps_map.provider_name.clone();

                        egui::ComboBox::from_id_salt("provider_selector")
                            .selected_text(&current_provider)
                            .show_ui(ui, |ui| {
                                for (name, url) in &self.ctx.map_providers {
                                    let is_selected = current_provider == *name;
                                    if ui.selectable_label(is_selected, name).clicked() {
                                        self.gps_map.set_provider(name.clone(), url.clone(), ctx);
                                        // Save selection to config
                                        let _ = self.ctx.save_map_selection(name);
                                    }
                                }
                            });
                    });

                    // Location selector dropdown
                    ui.horizontal(|ui| {
                        ui.label("Location:");
                        let current_loc = self
                            .gps_map
                            .selected_location
                            .as_ref()
                            .map(|(name, _)| name.clone())
                            .unwrap_or_else(|| "None".to_string());

                        egui::ComboBox::from_id_salt("location_selector")
                            .selected_text(&current_loc)
                            .show_ui(ui, |ui| {
                                if ui.selectable_label(current_loc == "None", "None").clicked() {
                                    self.gps_map.selected_location = None;
                                }
                                for (name, point) in &self.ctx.locations {
                                    let is_selected = self
                                        .gps_map
                                        .selected_location
                                        .as_ref()
                                        .map(|(n, _)| n == name)
                                        .unwrap_or(false);
                                    if ui.selectable_label(is_selected, name).clicked() {
                                        self.gps_map.selected_location =
                                            Some((name.clone(), *point));
                                    }
                                }
                            });
                    });

                    ui.separator();

                    // Map area
                    let map_rect = ui.available_rect_before_wrap();

                    // Get current file's path for highlighting (works in both view mode and duplicate mode)
                    let current_path = self
                        .state
                        .groups
                        .get(self.state.current_group_idx)
                        .and_then(|g| g.get(self.state.current_file_idx))
                        .map(|f| f.path.clone());

                    // Render the map
                    map_clicked_path = super::gps_map::render_gps_map(
                        &mut self.gps_map,
                        ui,
                        map_rect,
                        current_path.as_deref(),
                    );

                    // Statistics
                    ui.with_layout(egui::Layout::bottom_up(egui::Align::LEFT), |ui| {
                        ui.label(format!("Markers: {}", self.gps_map.markers.len()));
                    });
                });
        }

        // Handle map click navigation
        if let Some(clicked_path) = map_clicked_path {
            // Find the file in our groups and navigate to it
            for (g_idx, group) in self.state.groups.iter().enumerate() {
                for (f_idx, file) in group.iter().enumerate() {
                    if file.path == clicked_path {
                        self.state.current_group_idx = g_idx;
                        self.state.current_file_idx = f_idx;
                        self.state.selection_changed = true;
                        break;
                    }
                }
            }
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            let available_rect = ui.available_rect_before_wrap();
            if let Some(path) = current_image_path {
                // 1. Check Raw Cache
                if let Some(texture) = self.raw_cache.get(&path) {
                    super::image::render_image_texture(
                        self,
                        ui,
                        texture.id(),
                        texture.size_vec2(),
                        available_rect,
                        current_group_idx,
                    );
                } else {
                    // 2. Not in cache? It's loading.
                    ui.centered_and_justified(|ui| {
                        ui.spinner();
                        ui.label("Loading...");
                    });

                    // Trigger load if missed (failsafe)
                    if !self.raw_loading.contains(&path) {
                        self.raw_loading.insert(path.clone());
                        let _ = self.image_preload_tx.send(path.clone());
                    }
                }

                // Filename Overlay
                if self.state.is_fullscreen {
                    let name = path.file_name().unwrap_or_default().to_string_lossy();
                    let overlay_rect = egui::Rect::from_min_size(
                        egui::pos2(available_rect.min.x + 10.0, available_rect.max.y - 25.0),
                        egui::vec2(available_rect.width() - 20.0, 20.0),
                    );
                    ui.put(
                        overlay_rect,
                        egui::Label::new(
                            egui::RichText::new(name)
                                .size(12.0)
                                .color(egui::Color32::WHITE)
                                .background_color(egui::Color32::from_black_alpha(150)),
                        ),
                    );
                }

                // Histogram Overlay (toggle with 'I' key)
                if self.show_histogram {
                    super::image::render_histogram(self, ui, available_rect, &path);
                }

                // EXIF Info Overlay (toggle with 'E' key)
                if self.show_exif {
                    super::image::render_exif(self, ui, available_rect, &path);
                }
            } else {
                ui.centered_and_justified(|ui| ui.label("No image selected"));
            }
        });

        // Track window size for saving on exit
        // Use viewport inner_rect or outer_rect for the full window size
        let ppp = ctx.pixels_per_point();

        // Check if window is maximized or fullscreen (don't save size in these states)
        let is_maximized = ctx.input(|i| {
            i.viewport().maximized.unwrap_or(false) || i.viewport().fullscreen.unwrap_or(false)
        });

        // Try to get the actual window size from viewport
        let viewport_size = ctx.input(|i| {
            i.viewport()
                .inner_rect
                .or(i.viewport().outer_rect)
                .map(|r| ((r.width() * ppp) as u32, (r.height() * ppp) as u32))
        });

        if let Some(size) = viewport_size {
            // Only save if:
            // - font_scale has been applied (ppp > 1)
            // - window is not maximized/fullscreen (we want to preserve the user's chosen size)
            // - size is reasonable
            if size.0 > 100 && size.1 > 100 && ppp > 1.0 && !is_maximized {
                self.last_window_size = Some(size);
            }
        } else {
            // Fallback: use ctx.used_rect() which should include everything drawn
            let used = ctx.used_rect();
            let size = ((used.width() * ppp) as u32, (used.height() * ppp) as u32);

            if size.0 > 100 && size.1 > 100 && ppp > 1.0 && !is_maximized {
                self.last_window_size = Some(size);
            }
        }
    }
}
