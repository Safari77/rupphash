use eframe::egui;
use crate::state::{AppState, InputIntent, format_path_depth, get_bit_identical_counts, get_content_identical_counts, get_content_subgroups, get_hardlink_groups};
use crate::format_relative_time;
use crate::GroupStatus;
use crate::db::AppContext;
use crate::scanner::{self, ScanConfig, is_raw_ext};
use crate::{FileMetadata, GroupInfo};
use jiff::Timestamp;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::thread;
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::cell::RefCell;
use std::fs;
use std::path::Path;
use std::f32::consts::PI;
use fast_image_resize::images::Image as FastImage;
use fast_image_resize::{Resizer, ResizeOptions, PixelType};

#[derive(Debug, Clone, Copy, PartialEq)]
#[derive(Default)]
enum ViewMode {
    #[default]
    FitWindow,
    FitWidth,
    FitHeight,
    ManualZoom(f32),
}

#[derive(Clone, Copy)]
struct GroupViewState {
    mode: ViewMode,
    pan_center: egui::Pos2,
}

impl Default for GroupViewState {
    fn default() -> Self {
        Self {
            mode: ViewMode::FitWindow,
            pan_center: egui::Pos2::new(0.5, 0.5),
        }
    }
}

pub struct GuiApp {
    state: AppState,
    group_views: HashMap<usize, GroupViewState>,
    initial_scale_applied: bool,
    initial_panel_width_applied: bool,
    ctx: Arc<AppContext>,
    scan_config: ScanConfig,

    scan_rx: Option<Receiver<(Vec<Vec<FileMetadata>>, Vec<GroupInfo>, Vec<std::path::PathBuf>)>>,
    scan_progress_rx: Option<Receiver<(usize, usize)>>,
    scan_progress: (usize, usize),

    rename_input: String,
    last_preload_pos: Option<(usize, usize)>,
    status_set_time: Option<std::time::Instant>,
    slideshow_last_advance: Option<std::time::Instant>,

    // View mode: if Some, use scan_for_view with this sort order instead of scan_and_group
    view_mode_sort: Option<String>,

    // --- Raw Preloading ---
    // Cache for raw images (Path -> Texture)
    raw_cache: HashMap<std::path::PathBuf, egui::TextureHandle>,
    // Set of paths currently being processed by the worker to avoid dupes
    raw_loading: HashSet<std::path::PathBuf>,
    // Channel to send paths to the worker
    image_preload_tx: Sender<std::path::PathBuf>,
    // Channel to receive decoded images from the worker.
    image_preload_rx: Receiver<(std::path::PathBuf, Option<(egui::ColorImage, (u32, u32), u8)>)>,
    scan_batch_rx: Option<Receiver<Vec<FileMetadata>>>,

    // Shared state to tell workers which files are still relevant.
    // If a file is not in this set, workers will skip decoding it.
    active_window: Arc<RwLock<HashSet<std::path::PathBuf>>>,

    // Track window size and panel width for saving on exit
    last_window_size: Option<(u32, u32)>,
    panel_width: f32,
    saved_panel_width: f32,  // Original loaded value, preserved until applied

    // Directory browsing (view mode only)
    current_dir: Option<std::path::PathBuf>,
    show_dir_picker: bool,
    dir_list: Vec<std::path::PathBuf>,
    dir_picker_selection: usize,
    subdirs: Vec<std::path::PathBuf>,  // Subdirectories in current directory

    // Tab Completion State
    completion_candidates: Vec<String>,
    completion_index: usize,

    // Histogram display
    show_histogram: bool,

    // EXIF info display
    show_exif: bool,

    // Cache for current image's histogram and EXIF data (to avoid reloading on toggle)
    cached_histogram: Option<(std::path::PathBuf, [u32; 256])>,
    cached_exif: Option<(std::path::PathBuf, Vec<(String, String)>)>,
    search_input: String,
    search_focus_requested: bool,
}

impl GuiApp {
    /// Create a new GuiApp for duplicate detection mode
    pub fn new(ctx: AppContext, scan_config: ScanConfig, show_relative_times: bool, use_trash: bool,
        group_by: String, ext_priorities: HashMap<String, usize>, use_raw_thumbnails: bool,) -> Self {
        let use_pdqhash = ctx.hash_algorithm == crate::db::HashAlgorithm::PdqHash;
        let mut state = AppState::new(
            Vec::new(),
            Vec::new(),
            show_relative_times,
            use_trash,
            group_by,
            ext_priorities,
            use_pdqhash,
        );
        state.is_loading = true;

        let active_window = Arc::new(RwLock::new(HashSet::new()));
        let (tx, rx) = Self::spawn_image_loader_pool(active_window.clone(), use_raw_thumbnails);
        // panel_width is saved in logical points (after font_scale applied)
        // Load it as-is - we'll use it directly once ppp stabilizes
        let panel_width = ctx.gui_config.panel_width.unwrap_or(450.0);
        // Initialize with configured size so we have a fallback if window size isn't captured
        let initial_window_size = Some((
            ctx.gui_config.width.unwrap_or(1280),
            ctx.gui_config.height.unwrap_or(720)
        ));

        eprintln!("[DEBUG-CONFIG] new() - Loaded from config: window={}x{}, panel_width={}",
            ctx.gui_config.width.unwrap_or(1280),
            ctx.gui_config.height.unwrap_or(720),
            panel_width);
        eprintln!("[DEBUG-CONFIG] new() - Raw config values: width={:?}, height={:?}, panel_width={:?}",
            ctx.gui_config.width, ctx.gui_config.height, ctx.gui_config.panel_width);

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
            last_preload_pos: None,
            status_set_time: None,
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
            saved_panel_width: panel_width,  // Preserve original value
            current_dir: None,
            show_dir_picker: false,
            dir_list: Vec::new(),
            dir_picker_selection: 0,
            subdirs: Vec::new(),
            completion_candidates: Vec::new(),
            completion_index: 0,
            show_histogram: false,
            show_exif: false,
            cached_histogram: None,
            cached_exif: None,
            search_input: String::new(),
            search_focus_requested: false,
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
            false, // view mode doesn't use hashing
        );
        state.is_loading = true;
        state.view_mode = true;
        state.move_target = move_target;
        state.slideshow_interval = slideshow_interval;

        // Determine initial directory from paths
        let current_dir = paths.first()
            .map(std::path::PathBuf::from)
            .and_then(|p| if p.is_dir() { Some(p) } else { p.parent().map(|p| p.to_path_buf()) })
            .and_then(|p| p.canonicalize().ok());

        let scan_config = ScanConfig {
            paths,
            rehash: false,
            similarity: 0,
            group_by: sort_order.clone(),
            extensions: Vec::new(),
            ignore_same_stem: false,
            ignore_dev_id: false,
            calc_pixel_hash: false,
        };

        let active_window = Arc::new(RwLock::new(HashSet::new()));
        let (tx, rx) = Self::spawn_image_loader_pool(active_window.clone(), use_raw_thumbnails);
        let ctx = crate::db::AppContext::new().expect("Failed to create context");
        // panel_width is saved in logical points (after font_scale applied)
        // Load it as-is - we'll use it directly once ppp stabilizes
        let panel_width = ctx.gui_config.panel_width.unwrap_or(450.0);
        // Initialize with configured size so we have a fallback if window size isn't captured
        let initial_window_size = Some((
            ctx.gui_config.width.unwrap_or(1280),
            ctx.gui_config.height.unwrap_or(720)
        ));

        eprintln!("[DEBUG-CONFIG] new_view_mode() - Loaded from config: window={}x{}, panel_width={}",
            ctx.gui_config.width.unwrap_or(1280),
            ctx.gui_config.height.unwrap_or(720),
            panel_width);
        eprintln!("[DEBUG-CONFIG] new_view_mode() - Raw config values: width={:?}, height={:?}, panel_width={:?}",
            ctx.gui_config.width, ctx.gui_config.height, ctx.gui_config.panel_width);

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
            last_preload_pos: None,
            status_set_time: None,
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
            saved_panel_width: panel_width,  // Preserve original value
            current_dir,
            show_dir_picker: false,
            dir_list: Vec::new(),
            dir_picker_selection: 0,
            subdirs: Vec::new(),
            completion_candidates: Vec::new(),
            completion_index: 0,
            show_histogram: false,
            show_exif: false,
            cached_histogram: None,
            cached_exif: None,
            search_input: String::new(),
            search_focus_requested: false,
        }
    }

    fn spawn_image_loader_pool(active_window: Arc<RwLock<HashSet<std::path::PathBuf>>>, use_thumbnails: bool)
        -> (Sender<std::path::PathBuf>, Receiver<(std::path::PathBuf, Option<(egui::ColorImage, (u32, u32), u8)>)>)
    {
        let (tx, rx) = unbounded::<std::path::PathBuf>();
        let (result_tx, result_rx) = unbounded();

        let num_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).min(8);

        for _ in 0..num_threads {
            let rx_clone = rx.clone();
            let tx_clone = result_tx.clone();
            let window_clone = active_window.clone();

            thread::spawn(move || {
                while let Ok(path) = rx_clone.recv() {
                    // 1. Skip if no longer in active window
                    {
                        if let Ok(window) = window_clone.read() && !window.contains(&path) {
                            let _ = tx_clone.send((path, None));
                            continue;
                        }
                    }

                    // 2. Load & Process (Resize + Orientation)
                    let result = Self::load_and_process_image(&path, use_thumbnails);
                    let _ = tx_clone.send((path, result));
                }
            });
        }

        (tx, result_rx)
    }

    fn load_and_process_image(path: &std::path::Path, use_thumbnails: bool) -> Option<(egui::ColorImage, (u32, u32), u8)> {
        let (mut color_image, real_dims, orientation) = if is_raw_ext(path) {
            // A. RAW FILES
            // We must read bytes first to safely get orientation from them
            if let Ok(data) = fs::read(path) {
                let exif_orientation = crate::scanner::get_orientation(path, Some(&data));
                eprintln!("[DEBUG] load_and_process_image RAW exif_orientation={}", exif_orientation);

                if let Ok(mut raw) = rsraw::RawImage::open(&data) {
                        let dims = (raw.width() as u32, raw.height() as u32);

                        // Try Thumbnail
                        if use_thumbnails {
                            if let Some(thumb) = Self::extract_best_thumbnail(&mut raw) {
                             // Thumbnails are extracted as-is (not rotated by rsraw),
                             // so we need to apply EXIF orientation during rendering
                             eprintln!("[DEBUG] load_and_process_image RAW using thumbnail, applying exif_orientation={}", exif_orientation);
                             return Some((thumb, dims, exif_orientation));
                            }
                        }
                        // Full Decode - rsraw applies rotation automatically,
                        // so we return orientation=1 (no additional rotation needed)
                        raw.set_use_camera_wb(true);
                        if raw.unpack().is_ok() {
                        if let Ok(processed) = raw.process::<{ rsraw::BIT_DEPTH_8 }>() {
                            let w = processed.width() as usize;
                            let h = processed.height() as usize;
                            if processed.len() == w * h * 3 {
                                eprintln!("[DEBUG] load_and_process_image RAW full decode, orientation=1 (rsraw rotates)");
                                return Some((egui::ColorImage::from_rgb([w, h], &processed), dims, 1));
                            }
                        }
                    }
                    return None;
                } else { return None; }
            } else { return None; }
        } else {
            // B. STANDARD FILES (JPG, PNG, HEIC)
            if let Ok(bytes) = fs::read(path) {
                let orientation = crate::scanner::get_orientation(path, Some(&bytes));
                eprintln!("[DEBUG] load_and_process_image OTHER get_orientation={}", orientation);

                // Decode from memory buffer
                if let Ok(dyn_img) = image::load_from_memory(&bytes) {
                    let dims = (dyn_img.width(), dyn_img.height());
                    let buf = dyn_img.to_rgba8();
                    let pixels = buf.as_flat_samples();
                    let img = egui::ColorImage::from_rgba_unmultiplied(
                        [dims.0 as usize, dims.1 as usize],
                        pixels.as_slice()
                    );
                    (img, dims, orientation)
                } else { return None; }
            } else { return None; }
        };

        // --- STEP 2: FAST RESIZING (AVX2/SIMD) ---
        const MAX_TEXTURE_SIDE: usize = 8192;
        let w = color_image.width();
        let h = color_image.height();

        if w > MAX_TEXTURE_SIDE || h > MAX_TEXTURE_SIDE {
            let scale = (MAX_TEXTURE_SIDE as f32) / (w.max(h) as f32);
            let new_w = (w as f32 * scale).round() as usize;
            let new_h = (h as f32 * scale).round() as usize;

            // Convert egui::ColorImage -> fast_image_resize::Image
            // egui uses RGBA or RGB. standard load above is RGBA (4 bytes).
            // RAW load above is RGB (3 bytes). We must handle both.

            let pixel_type = if color_image.pixels.len() * 4 == color_image.as_raw().len() {
                PixelType::U8x4 // Standard/Thumbnail (RGBA)
            } else {
                PixelType::U8x3 // Raw Decode (RGB)
            };

            if let Ok(src_image) = FastImage::from_vec_u8(
                w as u32,
                h as u32,
                color_image.as_raw().to_vec(),
                pixel_type
            ) {
                 let mut dst_image = FastImage::new(
                     new_w as u32,
                     new_h as u32,
                     pixel_type
                 );

                 // Resize using Lanczos3 for quality or Bilinear for speed
                 // FilterType::Bilinear is usually safe and very fast for downscaling
                 let mut resizer = Resizer::new();
                 if resizer.resize(&src_image, &mut dst_image, &ResizeOptions::default()).is_ok() {
                     println!("[DEBUG] Fast-Resized {:?} from {}x{} to {}x{}", path, w, h, new_w, new_h);

                     // Convert back to egui
                     match pixel_type {
                         PixelType::U8x4 => {
                             color_image = egui::ColorImage::from_rgba_unmultiplied(
                                 [new_w, new_h],
                                 dst_image.buffer()
                             );
                         },
                         PixelType::U8x3 => {
                             color_image = egui::ColorImage::from_rgb(
                                 [new_w, new_h],
                                 dst_image.buffer()
                             );
                         },
                         _ => {}
                     }
                 }
            }
        }

        Some((color_image, real_dims, orientation))
    }

    // Handles streaming batches for instant feedback
    fn check_reload(&mut self, ctx: &egui::Context) {
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
                     self.state.group_infos.push(GroupInfo { max_dist:0, status: GroupStatus::None });
                 }
                 self.state.groups[0].extend(new_files);
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
            if let Ok((new_groups, new_infos, new_subdirs)) = rx.try_recv() {
                eprintln!("[DEBUG-RELOAD] Replacing groups! Old groups count: {}, New groups count: {}",
                    self.state.groups.len(), new_groups.len());
                if let Some(first_group) = new_groups.first() {
                    for (i, file) in first_group.iter().enumerate().take(5) {
                        eprintln!("[DEBUG-RELOAD]   new_groups[0][{}]: {:?}, orientation={}",
                            i, file.path.file_name().unwrap_or_default(), file.orientation);
                    }
                }

                // Only replace if we have results (duplicate mode) or finished view mode
                self.state.groups = new_groups;
                self.state.group_infos = new_infos;
                self.subdirs = new_subdirs;
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

    fn update_file_metadata(&mut self, path: &Path, w: u32, h: u32, orientation: u8) {
         eprintln!("[DEBUG-UPDATE] update_file_metadata called: path={:?}, orientation={}", path.file_name().unwrap_or_default(), orientation);

         // Helper to find and update the file in the group list
         let update_file = |file: &mut FileMetadata| {
             if file.path == path {
                 eprintln!("[DEBUG-UPDATE]   Found file! current orientation={}, new orientation={}", file.orientation, orientation);
                 if file.resolution.is_none() { file.resolution = Some((w, h)); }
                 // Always update orientation from loader - it knows the correct value
                 // (e.g., for RAW full decode it's 1, for RAW thumbnails it's EXIF value)
                 if file.orientation != orientation {
                     eprintln!("[DEBUG-UPDATE]   Updated orientation {} -> {}", file.orientation, orientation);
                     file.orientation = orientation;
                 }
                 return true;
             }
             false
         };

         // Check current file first (fast path)
         if let Some(group) = self.state.groups.get_mut(self.state.current_group_idx) {
             if let Some(file) = group.get_mut(self.state.current_file_idx) {
                 if update_file(file) { return; }
             }
         }

         // Fallback search
         for group in &mut self.state.groups {
             for file in group {
                 if update_file(file) { return; }
             }
         }
         eprintln!("[DEBUG-UPDATE]   FILE NOT FOUND in any group!");
    }

    /// Extract the best (largest) thumbnail from a RAW file
    fn extract_best_thumbnail(raw: &mut rsraw::RawImage) -> Option<egui::ColorImage> {
        let thumbs = raw.extract_thumbs().ok()?;

        // Find the largest JPEG thumbnail
        let best_thumb = thumbs.into_iter()
            .filter(|t| matches!(t.format, rsraw::ThumbFormat::Jpeg))
            .max_by_key(|t| t.width * t.height)?;

        // Decode JPEG thumbnail using image crate
        let img = image::load_from_memory(&best_thumb.data).ok()?;
        let rgb = img.to_rgb8();
        let (width, height) = rgb.dimensions();

        Some(egui::ColorImage::from_rgb(
                [width as usize, height as usize],
                rgb.as_raw()
        ))
    }
    pub fn with_move_target(mut self, target: Option<std::path::PathBuf>) -> Self {
        self.state.move_target = target;
        self
    }

    /// Change to a new directory and trigger rescan (view mode only)
    fn change_directory(&mut self, new_dir: std::path::PathBuf) {
        if !self.state.view_mode { return; }

        if let Ok(canonical) = new_dir.canonicalize() {
            self.current_dir = Some(canonical.clone());
            self.scan_config.paths = vec![canonical.to_string_lossy().to_string()];

            // Clear current state and trigger rescan
            self.state.groups.clear();
            self.state.group_infos.clear();
            self.state.current_group_idx = 0;
            self.state.current_file_idx = 0;
            self.state.is_loading = true;
            self.scan_rx = None;
            self.scan_progress_rx = None;
            self.scan_progress = (0, 0);

            // Clear caches
            self.raw_cache.clear();
            self.raw_loading.clear();
            if let Ok(mut w) = self.active_window.write() { w.clear(); }
            self.last_preload_pos = None;
        }
    }

    /// Get list of subdirectories for directory picker (including "..")
    fn get_subdirectories(&self) -> Vec<std::path::PathBuf> {
        let mut dirs = Vec::new();

        // Add parent directory if it exists
        if let Some(ref current) = self.current_dir
            && let Some(parent) = current.parent() {
                dirs.push(parent.to_path_buf());
            }

        // Add stored subdirectories
        dirs.extend(self.subdirs.clone());

        dirs
    }

    /// Open directory picker dialog
    fn open_dir_picker(&mut self) {
        self.dir_list = self.get_subdirectories();
        self.dir_picker_selection = 0;
        self.show_dir_picker = true;
    }

    /// Go up one directory level
    fn go_up_directory(&mut self) {
        if let Some(ref current) = self.current_dir.clone()
            && let Some(parent) = current.parent() {
                self.change_directory(parent.to_path_buf());
            }
    }

    pub fn run(self) -> Result<(), eframe::Error> {
        let initial_title = if self.state.is_loading {
            format!("{} v{} | Scanning...", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        } else {
            self.get_title_string()
        };

        // Config stores physical pixels (screen_rect * ppp after font_scale applied)
        // with_inner_size is called BEFORE font_scale, when ppp=1.0
        // So physical pixels = logical points at that moment
        let width = self.ctx.gui_config.width.unwrap_or(1280) as f32;
        let height = self.ctx.gui_config.height.unwrap_or(720) as f32;

        eprintln!("[DEBUG-RUN] Setting window size to {}x{} (physical pixels = logical points at ppp=1)", width, height);
        eprintln!("[DEBUG-RUN] self.panel_width at run() = {}", self.panel_width);

        let options = eframe::NativeOptions {
            viewport: egui::ViewportBuilder::default()
                .with_inner_size([width, height])
                .with_title(initial_title),
            ..Default::default()
        };

        let gui_config = self.ctx.gui_config.clone();

        eframe::run_native("phdupes", options, Box::new(move |cc| {
            egui_extras::install_image_loaders(&cc.egui_ctx);
            let mut fonts = egui::FontDefinitions::default();

            let mut configure_font = |name: &str, family: egui::FontFamily| {
                if let Ok(data) = fs::read(name) {
                     fonts.font_data.insert(name.to_owned(), Arc::new(egui::FontData::from_owned(data)));
                     if let Some(vec) = fonts.families.get_mut(&family) {
                        vec.insert(0, name.to_owned());
                     } else {
                        fonts.families.insert(family, vec![name.to_owned()]);
                     }
                }
            };

            if let Some(mono) = &gui_config.font_monospace { configure_font(mono, egui::FontFamily::Monospace); }
            if let Some(ui_font) = &gui_config.font_ui { configure_font(ui_font, egui::FontFamily::Proportional); }

            cc.egui_ctx.set_fonts(fonts);
            Ok(Box::new(self))
        }))
    }

    fn get_title_string(&self) -> String {
        format!("{} v{} | Groups: {} | Files: {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"), self.state.groups.len(), self.state.last_file_count)
    }

    fn update_view_state<F>(&mut self, f: F) where F: FnOnce(&mut GroupViewState) {
        let idx = self.state.current_group_idx;
        let entry = self.group_views.entry(idx).or_default();
        f(entry);
    }

    /// Handles both standard image preloading (via egui) and Raw preloading (via worker pool)
    /// In duplicate mode (multiple groups), preloads files from current and nearby groups.
    fn perform_preload(&mut self, _ctx: &egui::Context) {
        if self.state.groups.is_empty() { return; }

        let current_g = self.state.current_group_idx;
        let current_f = self.state.current_file_idx;

        if let Some((lg, lf)) = self.last_preload_pos
            && lg == current_g && lf == current_f { return; }
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
            let start = if end - start < preload_limit { end.saturating_sub(preload_limit) } else { start };

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
                    if prev_g == 0 { break; }
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
            if *is_current { continue; }
            if !self.raw_cache.contains_key(path) && !self.raw_loading.contains(path) {
                self.raw_loading.insert(path.clone());
                let _ = self.image_preload_tx.send(path.clone());
            }
        }

        // Cache Eviction
        self.raw_cache.retain(|k, _| active_window_paths.contains(k));
        self.raw_loading.retain(|k| active_window_paths.contains(k));
    }

    // Helper to render texture with pan/zoom logic
    fn render_image_texture(&mut self, ui: &mut egui::Ui, texture_id: egui::TextureId, texture_size: egui::Vec2, available_rect: egui::Rect, current_group_idx: usize) {
        // --- 1. Calculate Rotation ---
        let orientation = if let Some(group) = self.state.groups.get(self.state.current_group_idx) {
            if let Some(file) = group.get(self.state.current_file_idx) {
                file.orientation
            } else { 1 }
        } else { 1 };

        if false {
            // DEBUG: Trace orientation lookup
            if let Some(group) = self.state.groups.get(self.state.current_group_idx) {
                if let Some(file) = group.get(self.state.current_file_idx) {
                    eprintln!("[DEBUG-RENDER] group_idx={}, file_idx={}, path={:?}, file.orientation={}, used_orientation={}",
                        self.state.current_group_idx, self.state.current_file_idx,
                        file.path.file_name().unwrap_or_default(), file.orientation, orientation);
                } else {
                    eprintln!("[DEBUG-RENDER] group_idx={}, file_idx={} - FILE NOT FOUND, defaulting to 1",
                        self.state.current_group_idx, self.state.current_file_idx);
                }
            } else {
                eprintln!("[DEBUG-RENDER] group_idx={} - GROUP NOT FOUND, defaulting to 1",
                    self.state.current_group_idx);
            }
        }

        let manual_rot = self.state.manual_rotation % 4;

        let exif_angle = match orientation {
            3 => PI,
            6 => PI / 2.0,
            8 => 3.0 * PI / 2.0,
            _ => 0.0,
        };
        let manual_angle = manual_rot as f32 * (PI / 2.0);
        let total_angle = exif_angle + manual_angle;

        // DEBUG: Log rotation calculation (only occasionally to avoid spam)
        static DEBUG_COUNT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let count = DEBUG_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count % 60 == 0 {
            eprintln!("[DEBUG-ROTATION] orientation={}, exif_angle={:.4} rad ({:.1}°), manual_rot={}, total_angle={:.4} rad ({:.1}°)",
                orientation, exif_angle, exif_angle.to_degrees(), manual_rot, total_angle, total_angle.to_degrees());
        }

        let exif_steps = match orientation { 3 => 2, 6 => 1, 8 => 3, _ => 0 };
        let total_steps = (exif_steps + manual_rot) % 4;
        let is_rotated_90_270 = total_steps == 1 || total_steps == 3;

        // --- 2. Determine Visual Size (Swapped if rotated) ---
        let visual_size = if is_rotated_90_270 {
            egui::vec2(texture_size.y, texture_size.x)
        } else {
            texture_size
        };

        // --- 3. Calculate Zoom & Layout ---
        let (screen_w, screen_h) = (available_rect.width(), available_rect.height());
        let view_state = *self.group_views.get(&current_group_idx).unwrap_or(&GroupViewState::default());

        let zoom_factor = match view_state.mode {
            ViewMode::FitWindow => (screen_w / visual_size.x).min(screen_h / visual_size.y).min(2.0),
            ViewMode::FitWidth => screen_w / visual_size.x,
            ViewMode::FitHeight => screen_h / visual_size.y,
            ViewMode::ManualZoom(z) => {
                if self.state.zoom_relative {
                    let fit_scale = (screen_w / visual_size.x).min(screen_h / visual_size.y);
                    z * fit_scale
                } else {
                    z
                }
            },
        };

        // Size of the image on screen (visually)
        let virtual_visual_size = visual_size * zoom_factor;

        // --- 4. Handle Pan (Geometry-based) ---
        // We calculate where the image center should be on screen.
        // pan_center is normalized (0..1) relative to the virtual visual image.
        // 0.5 = center of image is at center of screen.
        // Dragging right (+x) -> Move image right -> Decrease pan_center.x?
        // No, dragging logic below handles delta.
        // Here we just position based on current pan_center.

        // Center of the viewport
        let screen_center = available_rect.center();

        // Offset from screen center to image center.
        // If pan_center is (0.5, 0.5), offset is (0,0).
        // If pan_center is (0,0) (top-left of image), we want top-left of image to be at screen center?
        // Usually pan works such that the point 'pan_center' coincides with screen center.
        let offset_x = (0.5 - view_state.pan_center.x) * virtual_visual_size.x;
        let offset_y = (0.5 - view_state.pan_center.y) * virtual_visual_size.y;

        let visual_center = screen_center + egui::vec2(offset_x, offset_y);

        // If image is smaller than screen, force centering (override pan)
        let final_center = egui::pos2(
            if virtual_visual_size.x <= screen_w { screen_center.x } else { visual_center.x },
            if virtual_visual_size.y <= screen_h { screen_center.y } else { visual_center.y }
        );

        // The target rect represents the bounds of the ROTATED image on screen.
        let target_rect = egui::Rect::from_center_size(final_center, virtual_visual_size);

        // --- 5. Determine Paint Rect (Unrotated Geometry) ---
        // To draw the image rotated without distortion, we define the rect for the UNROTATED image.
        // If rotated 90 deg, the unrotated rect has swapped W/H compared to target_rect.
        // It shares the same center.
        let paint_size = if is_rotated_90_270 {
            egui::vec2(target_rect.height(), target_rect.width())
        } else {
            target_rect.size()
        };
        let paint_rect = egui::Rect::from_center_size(target_rect.center(), paint_size);

        // --- 6. Render ---
        // Allocate space for the WHOLE available area to catch mouse events everywhere
        let response = ui.allocate_rect(available_rect, egui::Sense::click_and_drag());

        // Clip to the available area so zoomed images don't spill out
        let _painter = ui.painter().with_clip_rect(available_rect);

        // Paint the image into the calculated paint_rect, applying rotation.
        // egui::Image logic: Fits texture to paint_rect, then rotates around paint_rect center.
        // Since paint_rect matches the texture aspect ratio (scaled), the fit is 1:1 (no distortion).
        // Then rotation spins it to match target_rect (visual).
        egui::Image::from_texture((texture_id, texture_size))
            .rotate(total_angle, egui::Vec2::splat(0.5))
            .paint_at(ui, paint_rect);

        // --- 7. Interaction ---
        if response.dragged() {
            let d = response.drag_delta();
            // Convert screen delta to UV delta.
            // Moving mouse right (+d.x) means moving the image right.
            // If image moves right, the point under the screen center moves left relative to image.
            // So pan_center (UV) decreases.
            let uv_dx = -d.x / virtual_visual_size.x;
            let uv_dy = -d.y / virtual_visual_size.y;

            let new_cx = (view_state.pan_center.x + uv_dx).clamp(0.0, 1.0);
            let new_cy = (view_state.pan_center.y + uv_dy).clamp(0.0, 1.0);

            self.group_views.entry(current_group_idx).or_default().pan_center = egui::Pos2::new(new_cx, new_cy);
        }

        if response.clicked() {
             // Nothing
        }
    }

    /// Render greyscale histogram, using cached data if available
    fn render_histogram(&mut self, ui: &mut egui::Ui, available_rect: egui::Rect, path: &std::path::Path) {
        // Get window width for histogram sizing (10% of window width)
        let window_width = ui.ctx().input(|i| {
            i.viewport().inner_rect
                .or(i.viewport().outer_rect)
                .map(|r| r.width())
                .unwrap_or(available_rect.width())
        });
        let hist_width = window_width * 0.10;
        let hist_height = hist_width * 0.75; // 4:3 aspect ratio for histogram

        // Position in bottom-left corner with some padding
        let padding = 10.0;
        let hist_rect = egui::Rect::from_min_size(
            egui::pos2(available_rect.min.x + padding, available_rect.max.y - hist_height - padding),
            egui::vec2(hist_width, hist_height),
        );

        // Check cache first
        let histogram = if let Some((cached_path, cached_hist)) = &self.cached_histogram {
            if cached_path == path {
                Some(*cached_hist)
            } else {
                None
            }
        } else {
            None
        };

        // Compute if not cached
        let histogram = histogram.or_else(|| {
            let hist = if is_raw_ext(path) {
                Self::compute_histogram_from_raw(path)
            } else {
                Self::compute_histogram_from_image(path)
            };
            // Cache the result
            if let Some(h) = hist {
                self.cached_histogram = Some((path.to_path_buf(), h));
                Some(h)
            } else {
                None
            }
        });

        if let Some(hist) = histogram {
            Self::draw_histogram(ui, hist_rect, &hist);
        }
    }

    /// Draw histogram bars (pure rendering, no I/O)
    fn draw_histogram(ui: &mut egui::Ui, hist_rect: egui::Rect, hist: &[u32; 256]) {
        // Find max value for normalization (skip extremes which might be clipped)
        let max_val = hist[1..255].iter().copied().max().unwrap_or(1).max(1);

        let painter = ui.painter();
        let hist_width = hist_rect.width();
        let hist_height = hist_rect.height();

        // Draw background
        painter.rect_filled(hist_rect, 0.0, egui::Color32::from_black_alpha(180));

        // Draw histogram bars
        let bar_width = hist_width / 256.0;
        let usable_height = hist_height - 4.0; // Small padding

        for (i, &count) in hist.iter().enumerate() {
            if count == 0 { continue; }

            let normalized = (count as f32 / max_val as f32).min(1.0);
            let bar_height = normalized * usable_height;

            let x = hist_rect.min.x + (i as f32) * bar_width;
            let y_bottom = hist_rect.max.y - 2.0;
            let y_top = y_bottom - bar_height;

            // Color based on luminance value (darker values = darker bars)
            let grey = (i as u8).saturating_add(40).min(220);
            let color = egui::Color32::from_gray(grey);

            painter.rect_filled(
                egui::Rect::from_min_max(
                    egui::pos2(x, y_top),
                    egui::pos2(x + bar_width.max(1.0), y_bottom),
                ),
                0.0,
                color,
            );
        }

        // Draw border
        painter.rect_stroke(hist_rect, 0.0, egui::Stroke::new(1.0, egui::Color32::GRAY), egui::StrokeKind::Outside);
    }

    /// Compute histogram from a standard image file
    fn compute_histogram_from_image(path: &std::path::Path) -> Option<[u32; 256]> {
        let img = image::open(path).ok()?;
        let grey = img.to_luma8();
        let mut hist = [0u32; 256];
        for pixel in grey.pixels() {
            hist[pixel.0[0] as usize] += 1;
        }
        Some(hist)
    }

    /// Compute histogram from a RAW file using rsraw
    fn compute_histogram_from_raw(path: &std::path::Path) -> Option<[u32; 256]> {
        let data = fs::read(path).ok()?;
        let mut raw = rsraw::RawImage::open(&data).ok()?;

        // Try to extract thumbnail first (faster)
        if let Ok(thumbs) = raw.extract_thumbs()
            && let Some(best_thumb) = thumbs.into_iter()
                .filter(|t| matches!(t.format, rsraw::ThumbFormat::Jpeg))
                .max_by_key(|t| t.width * t.height)
                && let Ok(img) = image::load_from_memory(&best_thumb.data) {
                    let grey = img.to_luma8();
                    let mut hist = [0u32; 256];
                    for pixel in grey.pixels() {
                        hist[pixel.0[0] as usize] += 1;
                    }
                    return Some(hist);
                }

        // Fallback: process the full RAW (slower)
        if raw.unpack().is_ok() {
            raw.set_use_camera_wb(true);
            if let Ok(processed) = raw.process::<{ rsraw::BIT_DEPTH_8 }>() {
                let mut hist = [0u32; 256];
                // RGB data - convert to greyscale using luminance formula
                // Y = 0.299*R + 0.587*G + 0.114*B
                for chunk in processed.chunks_exact(3) {
                    let r = chunk[0] as u32;
                    let g = chunk[1] as u32;
                    let b = chunk[2] as u32;
                    let grey = ((299 * r + 587 * g + 114 * b) / 1000) as u8;
                    hist[grey as usize] += 1;
                }
                return Some(hist);
            }
        }
        None
    }

    /// Render EXIF information overlay, using cached data if available
    /// Position: to the right of histogram if shown, otherwise bottom-left corner
    fn render_exif(&mut self, ui: &mut egui::Ui, available_rect: egui::Rect, path: &std::path::Path) {
        let exif_tags = &self.ctx.gui_config.exif_tags;
        if exif_tags.is_empty() {
            return;
        }

        let decimal_mode = &self.ctx.gui_config.decimal_coords.unwrap_or(false);
        // Check cache first
        let tags = if let Some((cached_path, cached_tags)) = &self.cached_exif {
            if cached_path == path {
                cached_tags.clone()
            } else {
                let new_tags = crate::scanner::get_exif_tags(path, exif_tags, *decimal_mode);
                self.cached_exif = Some((path.to_path_buf(), new_tags.clone()));
                new_tags
            }
        } else {
            let new_tags = crate::scanner::get_exif_tags(path, exif_tags, *decimal_mode);
            self.cached_exif = Some((path.to_path_buf(), new_tags.clone()));
            new_tags
        };

        if tags.is_empty() {
            return;
        }

        // Get window width for positioning
        let window_width = ui.ctx().input(|i| {
            i.viewport().inner_rect
                .or(i.viewport().outer_rect)
                .map(|r| r.width())
                .unwrap_or(available_rect.width())
        });

        let padding = 10.0;
        let line_height = 14.0;
        let exif_height = (tags.len() as f32) * line_height + 8.0;

        // Calculate position: to the right of histogram if shown, else bottom-left
        let exif_x = if self.show_histogram {
            let hist_width = window_width * 0.10;
            available_rect.min.x + padding + hist_width + padding
        } else {
            available_rect.min.x + padding
        };

        // Estimate width based on content
        let max_label_width = tags.iter()
            .map(|(name, value)| name.len() + value.len() + 2)
            .max()
            .unwrap_or(20) as f32 * 7.0;
        let exif_width = max_label_width.min(300.0).max(150.0);

        let exif_rect = egui::Rect::from_min_size(
            egui::pos2(exif_x, available_rect.max.y - exif_height - padding),
            egui::vec2(exif_width, exif_height),
        );

        let painter = ui.painter();

        // Draw background
        painter.rect_filled(exif_rect, 4.0, egui::Color32::from_black_alpha(200));

        // Draw EXIF tags
        let text_x = exif_rect.min.x + 6.0;
        let mut text_y = exif_rect.min.y + 4.0;

        for (name, value) in &tags {
            // Draw tag name in gray
            painter.text(
                egui::pos2(text_x, text_y),
                egui::Align2::LEFT_TOP,
                format!("{}: ", name),
                egui::FontId::new(11.0, egui::FontFamily::Monospace),
                egui::Color32::GRAY,
            );

            // Calculate offset for value (approximate)
            let name_width = (name.len() + 2) as f32 * 6.5;

            // Draw value in white
            painter.text(
                egui::pos2(text_x + name_width, text_y),
                egui::Align2::LEFT_TOP,
                value,
                egui::FontId::new(11.0, egui::FontFamily::Monospace),
                egui::Color32::WHITE,
            );

            text_y += line_height;
        }

        // Draw border
        painter.rect_stroke(exif_rect, 4.0, egui::Stroke::new(1.0, egui::Color32::DARK_GRAY), egui::StrokeKind::Outside);
    }
}

impl eframe::App for GuiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Local flag to force egui to respect our manual resize this frame
        let mut force_panel_resize = false;

        if !self.initial_scale_applied {
            let user_scale = self.ctx.gui_config.font_scale.unwrap_or(1.0);
            ctx.set_pixels_per_point(ctx.pixels_per_point() * user_scale);
            self.initial_scale_applied = true;
        }

        if let Some(set_time) = self.status_set_time
            && set_time.elapsed() > std::time::Duration::from_secs(3) {
                self.state.status_message = None;
                self.status_set_time = None;
            }

        // Receive finished raw images from worker thread pool
        while let Ok((path, maybe_result)) = self.image_preload_rx.try_recv() {
            if let Some((color_image, actual_resolution, orientation)) = maybe_result {

                // Now 'orientation' is defined and passed correctly
                self.update_file_metadata(&path, actual_resolution.0, actual_resolution.1, orientation);

                let name = format!("img_{}", path.display());
                let texture = ctx.load_texture(name, color_image, Default::default());
                self.raw_cache.insert(path.clone(), texture);
            }
            // Always remove from loading set
            self.raw_loading.remove(&path);
            ctx.request_repaint();
        }

        self.check_reload(ctx);
        self.perform_preload(ctx);

        let intent = RefCell::new(None::<InputIntent>);

        // Input handling
        if ctx.input(|i| i.key_pressed(egui::Key::Escape)) {
            if self.show_dir_picker {
                self.show_dir_picker = false;
            } else if self.state.show_search {
                 *intent.borrow_mut() = Some(InputIntent::CancelSearch);
            } else if self.state.show_confirmation || self.state.error_popup.is_some() || self.state.renaming.is_some() || self.state.show_sort_selection {
                *intent.borrow_mut() = Some(InputIntent::Cancel);
            }
            else { *intent.borrow_mut() = Some(InputIntent::Quit); }
        }
        // Ctrl+Q to quit (triggers on_exit to save window size)
        if ctx.input(|i| i.modifiers.ctrl && i.key_pressed(egui::Key::Q)) {
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        }

        // Directory picker navigation
        if self.show_dir_picker {
            if ctx.input(|i| i.key_pressed(egui::Key::ArrowUp))
                && self.dir_picker_selection > 0 {
                    self.dir_picker_selection -= 1;
                }
            if ctx.input(|i| i.key_pressed(egui::Key::ArrowDown))
                && self.dir_picker_selection + 1 < self.dir_list.len() {
                    self.dir_picker_selection += 1;
                }
            if ctx.input(|i| i.key_pressed(egui::Key::Enter))
                && let Some(selected_dir) = self.dir_list.get(self.dir_picker_selection).cloned() {
                    self.show_dir_picker = false;
                    self.change_directory(selected_dir);
                }
        } else if !self.state.is_loading && self.state.renaming.is_none() && !self.state.show_sort_selection && !self.state.show_search {
            if ctx.input(|i| i.key_pressed(egui::Key::ArrowRight)) { *intent.borrow_mut() = Some(InputIntent::NextItem); }
            if ctx.input(|i| i.key_pressed(egui::Key::ArrowLeft)) { *intent.borrow_mut() = Some(InputIntent::PrevItem); }
            if ctx.input(|i| i.key_pressed(egui::Key::ArrowDown)) { *intent.borrow_mut() = Some(InputIntent::NextItem); }
            if ctx.input(|i| i.key_pressed(egui::Key::ArrowUp)) { *intent.borrow_mut() = Some(InputIntent::PrevItem); }
            if ctx.input(|i| i.key_pressed(egui::Key::PageDown)) { *intent.borrow_mut() = Some(InputIntent::PageDown); }
            if ctx.input(|i| i.modifiers.shift && i.key_pressed(egui::Key::PageDown)) {
                *intent.borrow_mut() = Some(InputIntent::NextGroupByDist);
            }
            if ctx.input(|i| i.key_pressed(egui::Key::PageUp)) { *intent.borrow_mut() = Some(InputIntent::PageUp); }
            if ctx.input(|i| i.modifiers.shift && i.key_pressed(egui::Key::PageUp)) {
                *intent.borrow_mut() = Some(InputIntent::PreviousGroupByDist);
            }
            if ctx.input(|i| i.key_pressed(egui::Key::Home)) { *intent.borrow_mut() = Some(InputIntent::Home); }
            if ctx.input(|i| i.key_pressed(egui::Key::End)) { *intent.borrow_mut() = Some(InputIntent::End); }
            if ctx.input(|i| i.modifiers.shift && i.key_pressed(egui::Key::Tab)) { *intent.borrow_mut() = Some(InputIntent::PrevGroup); }
            else if ctx.input(|i| i.key_pressed(egui::Key::Tab)) { *intent.borrow_mut() = Some(InputIntent::NextGroup); }
            if ctx.input(|i| i.key_pressed(egui::Key::Space)) { *intent.borrow_mut() = Some(InputIntent::ToggleMark); }
            if ctx.input(|i| i.key_pressed(egui::Key::D)) { *intent.borrow_mut() = Some(InputIntent::ExecuteDelete); }
            if ctx.input(|i| i.key_pressed(egui::Key::H)) { *intent.borrow_mut() = Some(InputIntent::ToggleRelativeTime); }
            if ctx.input(|i| i.key_pressed(egui::Key::W)) { *intent.borrow_mut() = Some(InputIntent::CycleViewMode); }
            if ctx.input(|i| i.key_pressed(egui::Key::Z)) { *intent.borrow_mut() = Some(InputIntent::CycleZoom); }
            if ctx.input(|i| i.key_pressed(egui::Key::R)) { *intent.borrow_mut() = Some(InputIntent::StartRename); }
            if ctx.input(|i| i.modifiers.ctrl && i.key_pressed(egui::Key::L)) { *intent.borrow_mut() = Some(InputIntent::ReloadList); }
            if ctx.input(|i| i.key_pressed(egui::Key::X)) { *intent.borrow_mut() = Some(InputIntent::ToggleZoomRelative); }
            if ctx.input(|i| i.key_pressed(egui::Key::P)) { *intent.borrow_mut() = Some(InputIntent::TogglePathVisibility); }
            if ctx.input(|i| i.key_pressed(egui::Key::Delete)) { *intent.borrow_mut() = Some(InputIntent::DeleteImmediate); }
            if ctx.input(|i| i.key_pressed(egui::Key::M)) { *intent.borrow_mut() = Some(InputIntent::MoveMarked); }
            if ctx.input(|i| i.key_pressed(egui::Key::S)) { *intent.borrow_mut() = Some(InputIntent::ToggleSlideshow); }
            if ctx.input(|i| i.key_pressed(egui::Key::F)) { *intent.borrow_mut() = Some(InputIntent::ToggleFullscreen); }
            if ctx.input(|i| i.key_pressed(egui::Key::O)) { *intent.borrow_mut() = Some(InputIntent::RotateCW); }
            if ctx.input(|i| i.key_pressed(egui::Key::I)) { self.show_histogram = !self.show_histogram; }
            if ctx.input(|i| i.key_pressed(egui::Key::E)) { self.show_exif = !self.show_exif; }

            // View Mode Only
            if self.state.view_mode {
                if ctx.input(|i| i.key_pressed(egui::Key::C)) { self.open_dir_picker(); }
                if ctx.input(|i| i.key_pressed(egui::Key::Period)) { self.go_up_directory(); }
                if ctx.input(|i| i.key_pressed(egui::Key::T)) {
                    *intent.borrow_mut() = Some(InputIntent::ShowSortSelection);
                }
            }

            let window_width = ctx.input(|i| i.viewport().inner_rect.map(|r| r.width()).unwrap_or(1000.0));
            let delta = window_width * 0.02;

            // V to Shrink panel
            if ctx.input(|i| i.key_pressed(egui::Key::V)) {
                self.panel_width = (self.panel_width - delta).max(96.0);
                force_panel_resize = true;
            }
            // B to Expand
            if ctx.input(|i| i.key_pressed(egui::Key::B)) {
                self.panel_width = (self.panel_width + delta).min(window_width * 0.8);
                force_panel_resize = true;
            }
            // Search
            if ctx.input(|i| i.modifiers.ctrl && i.key_pressed(egui::Key::F)) {
                *intent.borrow_mut() = Some(InputIntent::StartSearch);
                self.search_input.clear();
                self.search_focus_requested = false;
            }
            if ctx.input(|i| i.key_pressed(egui::Key::F3)) {
                if ctx.input(|i| i.modifiers.shift) {
                    *intent.borrow_mut() = Some(InputIntent::PrevSearchResult);
                } else {
                    *intent.borrow_mut() = Some(InputIntent::NextSearchResult);
                }
            }
        }

        let pending = intent.borrow().clone();
        if let Some(i) = pending {
            match i {
                InputIntent::CycleViewMode => { self.update_view_state(|v| { v.mode = match v.mode { ViewMode::FitWindow => ViewMode::FitWidth, ViewMode::FitWidth => ViewMode::FitHeight, _ => ViewMode::FitWindow, }; }); },
                InputIntent::CycleZoom => { self.update_view_state(|v| { v.mode = match v.mode {
                    ViewMode::FitWindow => ViewMode::ManualZoom(1.0),  // 1:1 native pixels
                    ViewMode::ManualZoom(z) if (z - 1.0).abs() < 0.1 => ViewMode::ManualZoom(2.0),
                    ViewMode::ManualZoom(z) if (z - 2.0).abs() < 0.1 => ViewMode::ManualZoom(4.0),
                    ViewMode::ManualZoom(z) if (z - 4.0).abs() < 0.1 => ViewMode::ManualZoom(8.0),
                    ViewMode::ManualZoom(_) => ViewMode::FitWindow,
                    _ => ViewMode::ManualZoom(1.0),
                }; }); },
                InputIntent::StartRename => { if let Some(path) = self.state.get_current_image_path() { self.rename_input = path.file_name().unwrap_or_default().to_string_lossy().to_string(); self.state.handle_input(i); } },
                _ => self.state.handle_input(i),
            }
        }

        // Dialogs (Confirmation, Rename, etc.)
        // Handle Y/N keys for confirmation dialogs
        if self.state.show_confirmation {
            if ctx.input(|i| i.key_pressed(egui::Key::Y)) {
                self.state.handle_input(InputIntent::ConfirmDelete);
            } else if ctx.input(|i| i.key_pressed(egui::Key::N)) {
                self.state.handle_input(InputIntent::Cancel);
            }
            egui::Window::new("Confirm Deletion").collapsible(false).show(ctx, |ui| {
               let marked_count = self.state.marked_for_deletion.len();
               let use_trash = self.state.use_trash;
               ui.label(format!("Are you sure you want to {} {} files?", if use_trash { "trash" } else { "permanently delete" }, marked_count));
               if ui.button("Yes (y)").clicked() { self.state.handle_input(InputIntent::ConfirmDelete); }
               if ui.button("No (n)").clicked() { self.state.handle_input(InputIntent::Cancel); }
           });
        }

        if self.state.show_delete_immediate_confirmation {
            if ctx.input(|i| i.key_pressed(egui::Key::Y)) {
                self.state.handle_input(InputIntent::ConfirmDeleteImmediate);
            } else if ctx.input(|i| i.key_pressed(egui::Key::N)) {
                self.state.handle_input(InputIntent::Cancel);
            }
            egui::Window::new("Confirm Delete").collapsible(false).show(ctx, |ui| {
                let filename = self.state.get_current_image_path().map(|p| p.file_name().unwrap_or_default().to_string_lossy().to_string()).unwrap_or_default();
                ui.label(format!("Delete current file?\n{}", filename));
                if ui.button("Yes (y)").clicked() { self.state.handle_input(InputIntent::ConfirmDeleteImmediate); }
                if ui.button("No (n)").clicked() { self.state.handle_input(InputIntent::Cancel); }
            });
        }

        if self.state.show_move_confirmation {
            if ctx.input(|i| i.key_pressed(egui::Key::Y)) {
                self.state.handle_input(InputIntent::ConfirmMoveMarked);
            } else if ctx.input(|i| i.key_pressed(egui::Key::N)) {
                self.state.handle_input(InputIntent::Cancel);
            }
            egui::Window::new("Confirm Move").collapsible(false).show(ctx, |ui| {
                let target = self.state.move_target.as_ref().map(|p| p.display().to_string()).unwrap_or_default();
                ui.label(format!("Move marked files to:\n{}", target));
                if ui.button("Yes (y)").clicked() { self.state.handle_input(InputIntent::ConfirmMoveMarked); }
                if ui.button("No (n)").clicked() { self.state.handle_input(InputIntent::Cancel); }
            });
        }

        // Search Dialog with Fixes
        if self.state.show_search {
            let mut submit = false;
            let mut cancel = false;

            egui::Window::new("Find File (Regex)")
                .collapsible(false)
                .show(ctx, |ui| {
                    ui.label("Case-insensitive regex search:");

                    let res = ui.text_edit_singleline(&mut self.search_input);

                    if !self.search_focus_requested {
                        res.request_focus();
                        self.search_focus_requested = true;
                    }

                    // Check both has_focus (typing) and lost_focus (committed via Enter)
                    let enter_pressed = ui.input(|i| i.key_pressed(egui::Key::Enter));
                    if enter_pressed && (res.has_focus() || res.lost_focus()) {
                        submit = true;
                    }

                    ui.horizontal(|ui| {
                        if ui.button("Find").clicked() { submit = true; }
                        if ui.button("Cancel").clicked() { cancel = true; }
                    });
                });

            if submit {
                self.state.handle_input(InputIntent::SubmitSearch(self.search_input.clone()));
            }
            if cancel {
                self.state.handle_input(InputIntent::CancelSearch);
            }
        }

        if self.state.renaming.is_some() {
            let mut submit = false;
            let mut cancel = false;
            let mut request_focus_back = false;

            egui::Window::new("Rename").collapsible(false).show(ctx, |ui| {
                let res = ui.text_edit_singleline(&mut self.rename_input);
                if !self.state.show_sort_selection && !self.state.show_confirmation {
                    res.request_focus();
                }

                if ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                    submit = true;
                }

                if ui.input(|i| i.key_pressed(egui::Key::Tab)) {
                    request_focus_back = true;
                    let parent = if let Some(state) = &self.state.renaming {
                        state.original_path.parent().map(|p| p.to_path_buf())
                    } else { None };

                    if let Some(parent_dir) = parent {
                        // Calculate previous index to check if input matches what we last auto-completed
                        let prev_idx = if !self.completion_candidates.is_empty() {
                            (self.completion_index + self.completion_candidates.len() - 1) % self.completion_candidates.len()
                        } else { 0 };

                        // Check if the current input matches the candidate we just showed.
                        // This confirms the user hasn't typed something new manually.
                        let input_matches_candidate = !self.completion_candidates.is_empty()
                            && self.completion_candidates[prev_idx] == self.rename_input;

                        // If empty or user typed something new, scan for new candidates
                        if self.completion_candidates.is_empty() || !input_matches_candidate {
                            self.completion_candidates.clear();
                            self.completion_index = 0;
                            if let Ok(entries) = fs::read_dir(&parent_dir) {
                                let prefix = self.rename_input.clone();
                                for entry in entries.flatten() {
                                    let name = entry.file_name().to_string_lossy().to_string();
                                    if name.starts_with(&prefix) {
                                        self.completion_candidates.push(name);
                                    }
                                }
                                self.completion_candidates.sort();
                            }
                        }

                        // Apply the next completion
                        if !self.completion_candidates.is_empty() {
                            self.rename_input = self.completion_candidates[self.completion_index].clone();
                            self.completion_index = (self.completion_index + 1) % self.completion_candidates.len();
                        }
                    }
                }

                if request_focus_back {
                    res.request_focus();
                }

                if ui.button("Rename").clicked() { submit = true; }
                if ui.button("Cancel").clicked() { cancel = true; }
            });

            if submit {
                self.state.handle_input(InputIntent::SubmitRename(self.rename_input.clone()));
                self.completion_candidates.clear();
            }
            if cancel {
                self.state.handle_input(InputIntent::Cancel);
                self.completion_candidates.clear();
            }
        }

        // Sort Selection Dialog
        if self.state.show_sort_selection {
            let mut selected_sort = None;

            egui::Window::new("Sort Order")
                .collapsible(false)
                .show(ctx, |ui| {
                    ui.label("Select sort order (or press 1-9):");
                    ui.separator();

                    let options = [
                        ("1. Name (A-Z)", "name", egui::Key::Num1),
                        ("2. Name (Z-A)", "name-desc", egui::Key::Num2),
                        ("3. Name Natural (A-Z)", "name-natural", egui::Key::Num3),
                        ("4. Name Natural (Z-A)", "name-natural-desc", egui::Key::Num4),
                        ("5. Date (Oldest First)", "date", egui::Key::Num5),
                        ("6. Date (Newest First)", "date-desc", egui::Key::Num6),
                        ("7. Size (Smallest First)", "size", egui::Key::Num7),
                        ("8. Size (Largest First)", "size-desc", egui::Key::Num8),
                        ("9. Random", "random", egui::Key::Num9),
                    ];

                    for (label, value, key) in options {
                        // Check if button clicked OR corresponding number key pressed
                        if ui.button(label).clicked() || ctx.input(|i| i.key_pressed(key)) {
                            selected_sort = Some(value.to_string());
                        }
                    }

                    ui.separator();
                    if ui.button("Cancel (Esc)").clicked() {
                        selected_sort = Some("CANCEL".to_string());
                    }
                });

            // Handle the selection after the UI closure to avoid borrow conflicts
            if let Some(sort) = selected_sort {
                if sort == "CANCEL" {
                    self.state.handle_input(InputIntent::Cancel);
                } else {
                    self.state.handle_input(InputIntent::ChangeSortOrder(sort));
                }
            }
        }

        // Directory picker dialog (view mode)
        if self.show_dir_picker {
            let mut selected_dir: Option<std::path::PathBuf> = None;

            egui::Window::new("Select Directory")
                .collapsible(false)
                .resizable(true)
                .default_width(500.0)
                .show(ctx, |ui| {
                    ui.label("Use ↑/↓ to navigate, Enter to select, Esc to cancel");
                    ui.separator();

                    egui::ScrollArea::vertical().max_height(400.0).show(ui, |ui| {
                        for (idx, dir_path) in self.dir_list.iter().enumerate() {
                            let is_selected = idx == self.dir_picker_selection;
                            let is_parent = idx == 0 && self.current_dir.as_ref().and_then(|c| c.parent()).is_some();

                            let display_name = if is_parent {
                                ".. (parent directory)".to_string()
                            } else {
                                dir_path.file_name()
                                    .map(|n| n.to_string_lossy().to_string())
                                    .unwrap_or_else(|| dir_path.to_string_lossy().to_string())
                            };

                            let resp = ui.selectable_label(is_selected, &display_name);
                            if resp.clicked() {
                                selected_dir = Some(dir_path.clone());
                            }
                            if is_selected {
                                resp.scroll_to_me(Some(egui::Align::Center));
                                resp.request_focus();
                            }
                        }

                        if self.dir_list.is_empty() {
                            ui.label("No subdirectories found");
                        }
                    });

                    ui.separator();
                    if ui.button("Cancel (Esc)").clicked() {
                        self.show_dir_picker = false;
                    }
                });

            // Apply deferred directory change
            if let Some(dir) = selected_dir {
                self.show_dir_picker = false;
                self.change_directory(dir);
            }
        }

        // Slideshow
        if let Some(interval) = self.state.slideshow_interval
            && !self.state.slideshow_paused && !self.state.is_loading && !self.state.groups.is_empty() {
                let should_advance = match self.slideshow_last_advance {
                    Some(last) => last.elapsed().as_secs_f32() >= interval,
                    None => true,
                };
                if should_advance {
                    self.slideshow_last_advance = Some(std::time::Instant::now());
                    self.state.next_item();
                    self.state.selection_changed = true;
                }
                ctx.request_repaint_after(std::time::Duration::from_secs_f32(0.1));
            }

        if let Some(err_text) = self.state.error_popup.clone() {
            egui::Window::new("Error").show(ctx, |ui| { ui.label(err_text); if ui.button("OK").clicked() { self.state.handle_input(InputIntent::Cancel); } });
        }

        // --- RENDER ---
        let current_image_path = self.state.get_current_image_path().cloned();
        let current_group_idx = self.state.current_group_idx;
        let current_view_mode = *self.group_views.get(&current_group_idx).unwrap_or(&GroupViewState::default());

        if !self.state.is_fullscreen {
            // Restore Detailed Status Bar
            egui::TopBottomPanel::bottom("status").show(ctx, |ui| {
                if let Some((msg, is_error)) = &self.state.status_message {
                    ui.colored_label(if *is_error { egui::Color32::RED } else { egui::Color32::GREEN }, msg);
                } else {
                     let mode_str = match current_view_mode.mode { ViewMode::FitWindow => "Fit Window", ViewMode::FitWidth => "Fit Width", ViewMode::FitHeight => "Fit Height", ViewMode::ManualZoom(_) => "Zoom", };
                     let extra = match current_view_mode.mode {
                         ViewMode::ManualZoom(z) if (z - 1.0).abs() < 0.1 => if self.state.zoom_relative { " 1x".to_string() } else { " 1:1".to_string() },
                         ViewMode::ManualZoom(z) => format!(" {:.0}x", z),
                         _ => "".to_string()
                     };
                     let rel_tag = if self.state.zoom_relative { " [REL]" } else { " [ABS]" };
                     let filename = current_image_path.as_ref().map(|p| p.file_name().unwrap_or_default().to_string_lossy().to_string()).unwrap_or_default();

                     let slideshow_status = if self.state.slideshow_interval.is_some() {
                         if self.state.slideshow_paused { " | [S]lideshow: PAUSED" } else { " | [S]lideshow: ON" }
                     } else { "" };

                     let move_status = if self.state.move_target.is_some() { " | [M]ove" } else { "" };
                     let del_key = if self.state.view_mode { " | [Del]ete" } else { "" };
                     let rot_str = if !self.state.manual_rotation.is_multiple_of(4) { format!(" | [O] Rot: {}°", (self.state.manual_rotation % 4) * 90) } else { "".to_string() };
                     let sort_str = if self.state.view_mode { " | [T] Sort" } else { "" };
                     let hist_str = if self.show_histogram { " | [I] Hist" } else { "" };
                     let exif_str = if self.show_exif { " | [E] EXIF" } else { "" };
                     let pos_str = if !self.state.groups.is_empty() {
                         let total: usize = self.state.groups.iter().map(|g| g.len()).sum();
                         let current: usize = self.state.groups.iter().take(self.state.current_group_idx).map(|g| g.len()).sum::<usize>() + self.state.current_file_idx + 1;
                         format!(" [{}/{}]", current, total)
                     } else { "".to_string() };

                     ui.horizontal(|ui| {
                         ui.label(format!("W: {}{} | Z: Zoom{}{}{}{}{}{}{}{}", mode_str, extra, rel_tag, slideshow_status, move_status, del_key, sort_str, rot_str, hist_str, exif_str));
                         ui.separator();
                         ui.label(pos_str);
                         if !filename.is_empty() {
                             ui.separator();
                             ui.label(egui::RichText::new(filename).size(14.0).family(egui::FontFamily::Monospace).strong());
                         }
                     });
                }
            });

            // Restore Detailed File List
            // Get actual window width - try viewport rect first, fall back to used_rect
            // Note: window_width is in logical points (not physical pixels)
            let window_width = ctx.input(|i| {
                i.viewport().inner_rect
                    .or(i.viewport().outer_rect)
                    .map(|r| r.width())
            }).unwrap_or_else(|| ctx.used_rect().width());
            let panel_max_width = window_width * 0.5;

            // Delay panel width restoration until after font_scale is applied
            let ppp = ctx.pixels_per_point();

            // Only print debug every 60 frames to reduce spam
            use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
            static FRAME_COUNT: AtomicU32 = AtomicU32::new(0);
            let frame = FRAME_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
            if frame.is_multiple_of(60) {
                let ppp = ctx.pixels_per_point();
                eprintln!("[DEBUG-PANEL] frame={}, window={}x{}px ({}x{} logical), panel_width={}",
                    frame,
                    (window_width * ppp) as u32, (ctx.used_rect().height() * ppp) as u32,
                    window_width as u32, ctx.used_rect().height() as u32,
                    self.panel_width);
            }

            // Delay panel width restoration until after font_scale is applied
            // On first frames (ppp=1), use a default. Once ppp stabilizes (>1), apply saved width.
            let should_apply_saved_width = !self.initial_panel_width_applied && ppp > 1.5;

            if should_apply_saved_width {
                eprintln!("[DEBUG-PANEL] Applying saved panel width {} (ppp={})", self.saved_panel_width, ppp);
                self.initial_panel_width_applied = true;
            }

            let panel_builder = egui::SidePanel::left("list_panel")
                .resizable(true)
                .min_width(96.0);

            // Apply width logic to the builder
            let panel = if force_panel_resize {
                panel_builder.exact_width(self.panel_width)
            } else if should_apply_saved_width {
                panel_builder.exact_width(self.saved_panel_width.min(panel_max_width))
            } else if self.initial_panel_width_applied {
                panel_builder.default_width(self.panel_width).max_width(panel_max_width)
            } else {
                panel_builder.default_width(200.0).max_width(panel_max_width)
            };

            let panel_response = panel.show(ctx, |ui| {
                // Show current directory header in view mode
                if self.state.view_mode
                    && let Some(ref current_dir) = self.current_dir {
                        ui.horizontal(|ui| {
                            ui.label(egui::RichText::new("📁").size(16.0));
                            ui.add(egui::Label::new(
                                egui::RichText::new(current_dir.to_string_lossy())
                                    .size(12.0)
                                    .family(egui::FontFamily::Monospace)
                                    .color(egui::Color32::LIGHT_BLUE)
                            ).wrap_mode(egui::TextWrapMode::Truncate));
                        });
                        ui.horizontal(|ui| {
                            ui.label(egui::RichText::new("[C] Change dir  [.] Go down").size(10.0).color(egui::Color32::GRAY));
                        });
                        ui.separator();
                    }

                egui::ScrollArea::vertical().show(ui, |ui| {
                    if self.state.is_loading {
                        let (current, total) = self.scan_progress;
                        if total > 0 { ui.label(format!("Scanning: {}/{} files...", current, total)); ui.add(egui::ProgressBar::new(current as f32 / total as f32).show_percentage()); } else { ui.label("Scanning..."); ui.spinner(); }
                    } else if self.state.groups.is_empty() && self.subdirs.is_empty() { ui.label(if self.state.view_mode { "No images found." } else { "No duplicates found." }); }

                    let mut dir_to_open: Option<std::path::PathBuf> = None;
                    if self.state.view_mode && !self.state.is_loading {
                        if let Some(ref current) = self.current_dir && let Some(parent) = current.parent() {
                                if ui.add(egui::Button::new(egui::RichText::new("📁 ..").color(egui::Color32::YELLOW)).wrap_mode(egui::TextWrapMode::Truncate)).clicked() { dir_to_open = Some(parent.to_path_buf()); }
                            }
                        for subdir in &self.subdirs {
                            let dir_name = subdir.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_else(|| subdir.to_string_lossy().to_string());
                            if ui.add(egui::Button::new(egui::RichText::new(format!("📁 {}", dir_name)).color(egui::Color32::LIGHT_BLUE)).wrap_mode(egui::TextWrapMode::Truncate)).clicked() { dir_to_open = Some(subdir.clone()); }
                        }
                        if !self.subdirs.is_empty() || self.current_dir.as_ref().and_then(|c| c.parent()).is_some() { ui.separator(); }
                    }

                    // --- VIRTUALIZATION / CLIPPING LOGIC ---
                    let spacing = ui.spacing().item_spacing.y;
                    let header_height = ui.text_style_height(&egui::TextStyle::Body) + spacing;
                    let font_id_main = egui::TextStyle::Monospace.resolve(ui.style());
                    let font_id_meta = egui::FontId::monospace(10.0);
                    // Use "Ij" to cover both ascenders and descenders for accurate height
                    let button_height = ui.painter().layout_no_wrap("Ij".to_string(), font_id_main.clone(), egui::Color32::default()).rect.height() + (ui.spacing().button_padding.y * 2.0);
                    let meta_height = ui.painter().layout_no_wrap("Ij".to_string(), font_id_meta.clone(), egui::Color32::default()).rect.height();
                    let file_row_height = button_height + spacing + meta_height + spacing;
                    let separator_height = 10.0;

                    let clip_rect = ui.clip_rect();
                    let start_y = ui.cursor().min.y;
                    let mut current_y = start_y;

                    let mut action_rename = false;
                    let mut action_delete = false;
                    let mut new_selection = None;

                    for (g_idx, group) in self.state.groups.iter().enumerate() {
                        let header_visible = !self.state.view_mode;
                        let current_header_height = if header_visible { header_height } else { 0.0 };
                        let group_content_height = (group.len() as f32 * file_row_height) + if header_visible { separator_height } else { 0.0 };
                        let group_total_height = current_header_height + group_content_height;
                        // Check if the SELECTED item is in this group before clipping the whole group
                        let contains_selection = g_idx == self.state.current_group_idx;
                        let is_group_visible = (current_y + group_total_height > clip_rect.min.y) && (current_y < clip_rect.max.y);
                        // If group is not visible AND doesn't contain the selection we need to jump to, skip it.
                        if !is_group_visible && !(contains_selection && self.state.selection_changed) {
                            ui.add_space(group_total_height);
                            current_y += group_total_height;
                            continue;
                        }

                        if header_visible {
                            let info = &self.state.group_infos[g_idx];
                            let (header_text, header_color) = match info.status {
                                GroupStatus::AllIdentical => (format!("Group {} - Bit-identical (hardlinks in light blue)", g_idx + 1), egui::Color32::GREEN),
                                GroupStatus::SomeIdentical => (format!("Group {} - Some Identical", g_idx + 1), egui::Color32::LIGHT_GREEN),
                                GroupStatus::None => (format!("Group {} (Dist: {})", g_idx + 1, info.max_dist), egui::Color32::YELLOW),
                            };
                            ui.colored_label(header_color, header_text);
                            current_y += header_height;
                        }

                        let counts = get_bit_identical_counts(group);
                        let hardlink_groups = get_hardlink_groups(group);
                        let content_subgroups = get_content_subgroups(group);

                        for (f_idx, file) in group.iter().enumerate() {
                            let is_selected = g_idx == self.state.current_group_idx && f_idx == self.state.current_file_idx;
                            // Define visibility
                            let is_file_visible = (current_y + file_row_height > clip_rect.min.y) && (current_y < clip_rect.max.y);
                            // If this is the selected file and selection changed,
                            // we MUST render it even if off-screen so scroll_to_me works.
                            let force_render = is_selected && self.state.selection_changed;

                            if !is_file_visible && !force_render {
                                ui.add_space(file_row_height);
                                current_y += file_row_height;
                                continue;
                            }

                            let is_marked = self.state.marked_for_deletion.contains(&file.path);
                            let exists = file.path.exists();
                            let is_bit_identical = !self.state.view_mode && *counts.get(&file.content_hash).unwrap_or(&0) > 1;
                            let is_hardlinked = !self.state.view_mode && file.dev_inode.map(|di| hardlink_groups.contains_key(&di)).unwrap_or(false);

                            let content_id = file.pixel_hash.and_then(|ph| content_subgroups.get(&ph));
                            let is_content_identical = content_id.is_some();
                            let c_label = if let Some(id) = content_id { format!("C{:<1}", id) } else { "  ".to_string() };

                            let text = format!("{} {} {} {}",
                                if is_marked     { "M" } else { " " },
                                if is_hardlinked { "L" } else { " " },
                                c_label,
                                format_path_depth(&file.path, self.state.path_display_depth)
                            );
                            let mut label_text = egui::RichText::new(text).family(egui::FontFamily::Monospace);
                            if !is_selected {
                                if !exists { label_text = label_text.color(egui::Color32::RED).strikethrough(); }
                                else if is_marked { label_text = label_text.color(egui::Color32::RED); }
                                else if is_hardlinked { label_text = label_text.color(egui::Color32::LIGHT_BLUE); }
                                else if is_bit_identical { label_text = label_text.color(egui::Color32::GREEN); }
                                else if is_content_identical { label_text = label_text.color(egui::Color32::GOLD); }
                            }

                            if let Some(current_file) = group.get(self.state.current_file_idx) {
                                 if !is_selected && current_file.pixel_hash.is_some() && current_file.pixel_hash == file.pixel_hash {
                                     label_text = label_text.strong().background_color(egui::Color32::from_black_alpha(40));
                                 }
                            }

                            let resp = ui.add(egui::Button::new(label_text).selected(is_selected).wrap_mode(egui::TextWrapMode::Truncate));
                            if resp.clicked() { new_selection = Some((g_idx, f_idx)); }

                            resp.context_menu(|ui| {
                                if ui.button("Rename (R)").clicked() { ui.close(); action_rename = true; }
                                if ui.button("Copy full path").clicked() { // Copy to clipboard
                                    ui.ctx().copy_text(file.path.to_string_lossy().to_string());
                                    ui.close();
                                }
                                if ui.button("Delete (Del)").clicked() { ui.close(); action_delete = true; }
                            });

                            if is_selected && self.state.selection_changed {
                                resp.scroll_to_me(Some(egui::Align::Center));
                            }

                            let size_kb = file.size / 1024;
                            let time_str = if self.state.show_relative_times {
                                let ts = Timestamp::from_second(file.modified.timestamp()).unwrap().checked_add(jiff::SignedDuration::from_nanos(file.modified.timestamp_subsec_nanos() as i64)).unwrap();
                                format_relative_time(ts)
                            } else { file.modified.format("%Y-%m-%d %H:%M:%S").to_string() };
                            let res_str = file.resolution.map(|(w, h)| format!("{}x{}", w, h)).unwrap_or("?".to_string());

                            ui.add(egui::Label::new(egui::RichText::new(format!("{} | {} KB | {}", time_str, size_kb, res_str)).size(10.0).family(egui::FontFamily::Monospace).color(if is_selected { egui::Color32::WHITE } else { egui::Color32::GRAY })).wrap_mode(egui::TextWrapMode::Truncate));

                            current_y += file_row_height;
                        }
                        if header_visible {
                            ui.separator();
                            current_y += separator_height;
                        }
                    }

                    if let Some((g, f)) = new_selection { self.state.current_group_idx = g;
                        self.state.current_file_idx = f; self.state.selection_changed = true; }
                    if action_rename { if let Some(path) = self.state.get_current_image_path() { self.rename_input = path.file_name().unwrap_or_default().to_string_lossy().to_string(); } self.state.handle_input(InputIntent::StartRename); }
                    if action_delete { self.state.handle_input(InputIntent::DeleteImmediate); }
                    if let Some(dir) = dir_to_open { self.change_directory(dir); }
                    self.state.selection_changed = false;
                });
            });
            let rendered_width = panel_response.response.rect.width();
            if !force_panel_resize && (rendered_width - self.panel_width).abs() > 1.0 {
                self.panel_width = rendered_width;
            }
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            let available_rect = ui.available_rect_before_wrap();
            if let Some(path) = current_image_path {
                 // 1. Check Raw Cache
                 if let Some(texture) = self.raw_cache.get(&path) {
                     self.render_image_texture(ui, texture.id(), texture.size_vec2(), available_rect, current_group_idx);
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
                         egui::vec2(available_rect.width() - 20.0, 20.0)
                     );
                     ui.put(overlay_rect, egui::Label::new(
                         egui::RichText::new(name).size(12.0).color(egui::Color32::WHITE).background_color(egui::Color32::from_black_alpha(150))
                     ));
                 }

                 // Histogram Overlay (toggle with 'I' key)
                 if self.show_histogram {
                     self.render_histogram(ui, available_rect, &path);
                 }

                 // EXIF Info Overlay (toggle with 'E' key)
                 if self.show_exif {
                     self.render_exif(ui, available_rect, &path);
                 }
            } else { ui.centered_and_justified(|ui| ui.label("No image selected")); }
        });

        // Track window size for saving on exit
        // Use viewport inner_rect or outer_rect for the full window size
        // available_rect excludes panels so it's not what we want
        let ppp = ctx.pixels_per_point();

        // Try to get the actual window size from viewport
        let viewport_size = ctx.input(|i| {
            i.viewport().inner_rect
                .or(i.viewport().outer_rect)
                .map(|r| ((r.width() * ppp) as u32, (r.height() * ppp) as u32))
        });

        // Debug every 60 frames
        use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
        static FRAME: AtomicU32 = AtomicU32::new(0);
        let f = FRAME.fetch_add(1, AtomicOrdering::Relaxed);

        if let Some(size) = viewport_size {
            // Detect if window was maximized by comparing to full screen size
            // Your screen is 3840x2160, so if size is close to that, window was maximized
            let is_maximized = size.0 >= 3800 || size.1 >= 2100;

            if f.is_multiple_of(60) {
                eprintln!("[DEBUG-WINSIZE] viewport_size={:?}, ppp={}, is_maximized={}",
                    size, ppp, is_maximized);
            }

            // Only save if:
            // - font_scale has been applied (ppp > 1)
            // - window is not maximized (we want to preserve the user's chosen size)
            // - size is reasonable
            if size.0 > 100 && size.1 > 100 && ppp > 1.0 && !is_maximized {
                self.last_window_size = Some(size);
            }
        } else {
            // Fallback: use ctx.used_rect() which should include everything drawn
            let used = ctx.used_rect();
            let size = ((used.width() * ppp) as u32, (used.height() * ppp) as u32);
            let is_maximized = size.0 >= 3800 || size.1 >= 2100;

            if f.is_multiple_of(60) {
                eprintln!("[DEBUG-WINSIZE] used_rect={:?}, ppp={}, size_physical={:?}, is_maximized={}",
                    used, ppp, size, is_maximized);
            }

            if size.0 > 100 && size.1 > 100 && ppp > 1.0 && !is_maximized {
                self.last_window_size = Some(size);
            }
        }
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        eprintln!("[DEBUG-EXIT] on_exit called");
        eprintln!("[DEBUG-EXIT] last_window_size = {:?}", self.last_window_size);
        eprintln!("[DEBUG-EXIT] panel_width = {}", self.panel_width);

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
        eprintln!("[DEBUG-EXIT] Calling save_gui_config with width={:?}, height={:?}, panel_width={:?}",
            gui_config.width, gui_config.height, gui_config.panel_width);
        if let Err(e) = self.ctx.save_gui_config(&gui_config) {
            eprintln!("Error saving config: {}", e);
        } else {
            eprintln!("[DEBUG-EXIT] save_gui_config succeeded");
        }
    }
}
