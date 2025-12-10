use eframe::egui;
use crate::state::{AppState, InputIntent, format_path_depth, get_bit_identical_counts, get_hardlink_groups};
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
    raw_preload_tx: Sender<std::path::PathBuf>,
    // Channel to receive decoded images from the worker.
    // Format: (Path, Option<Image>) - None indicates failure/skip so we can clear loading state
    raw_preload_rx: Receiver<(std::path::PathBuf, Option<egui::ColorImage>)>,

    // Shared state to tell workers which files are still relevant.
    // If a file is not in this set, workers will skip decoding it.
    active_window: Arc<RwLock<HashSet<std::path::PathBuf>>>,

    // Track window size and panel width for saving on exit
    last_window_size: Option<(u32, u32)>,
    panel_width: f32,

    // Directory browsing (view mode only)
    current_dir: Option<std::path::PathBuf>,
    show_dir_picker: bool,
    dir_list: Vec<std::path::PathBuf>,
    dir_picker_selection: usize,
    subdirs: Vec<std::path::PathBuf>,  // Subdirectories in current directory
}

impl GuiApp {
    /// Create a new GuiApp for duplicate detection mode
    pub fn new(ctx: AppContext, scan_config: ScanConfig, show_relative_times: bool, use_trash: bool, group_by: String, ext_priorities: HashMap<String, usize>) -> Self {
        let mut state = AppState::new(
            Vec::new(),
            Vec::new(),
            show_relative_times,
            use_trash,
            group_by,
            ext_priorities
        );
        state.is_loading = true;

        let active_window = Arc::new(RwLock::new(HashSet::new()));
        let (tx, rx) = Self::spawn_raw_loader_pool(active_window.clone());
        let panel_width = ctx.gui_config.panel_width.unwrap_or(450.0);

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
            raw_preload_tx: tx,
            raw_preload_rx: rx,
            active_window,
            last_window_size: None,
            panel_width,
            current_dir: None,
            show_dir_picker: false,
            dir_list: Vec::new(),
            dir_picker_selection: 0,
            subdirs: Vec::new(),
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
    ) -> Self {
        let mut state = AppState::new(
            Vec::new(),
            Vec::new(),
            show_relative_times,
            use_trash,
            sort_order.clone(),
            HashMap::new()
        );
        state.is_loading = true;
        state.view_mode = true;
        state.move_target = move_target;
        state.slideshow_interval = slideshow_interval;

        // Determine initial directory from paths
        let current_dir = paths.first()
            .map(|p| std::path::PathBuf::from(p))
            .and_then(|p| if p.is_dir() { Some(p) } else { p.parent().map(|p| p.to_path_buf()) })
            .and_then(|p| p.canonicalize().ok());

        let scan_config = ScanConfig {
            paths,
            rehash: false,
            similarity: 0,
            group_by: sort_order.clone(),
            extensions: Vec::new(),
            ignore_same_stem: false,
        };

        let active_window = Arc::new(RwLock::new(HashSet::new()));
        let (tx, rx) = Self::spawn_raw_loader_pool(active_window.clone());
        let ctx = crate::db::AppContext::new().expect("Failed to create context");
        let panel_width = ctx.gui_config.panel_width.unwrap_or(450.0);

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
            raw_preload_tx: tx,
            raw_preload_rx: rx,
            active_window,
            last_window_size: None,
            panel_width,
            current_dir,
            show_dir_picker: false,
            dir_list: Vec::new(),
            dir_picker_selection: 0,
            subdirs: Vec::new(),
        }
    }

    /// Spawns a pool of background threads that decode raw images.
    /// Workers check `active_window` before processing to skip stale requests.
    fn spawn_raw_loader_pool(active_window: Arc<RwLock<HashSet<std::path::PathBuf>>>)
        -> (Sender<std::path::PathBuf>, Receiver<(std::path::PathBuf, Option<egui::ColorImage>)>)
    {
        let (tx, rx) = unbounded::<std::path::PathBuf>();
        let (result_tx, result_rx) = unbounded();

        // Spawn threads equal to logical cores (capped reasonably to avoid choking GUI thread)
        let num_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).min(8);

        for _ in 0..num_threads {
            let rx_clone = rx.clone();
            let tx_clone = result_tx.clone();
            let window_clone = active_window.clone();

            thread::spawn(move || {
                while let Ok(path) = rx_clone.recv() {
                    // 1. FAST SKIP: Check if this file is still relevant
                    {
                        if let Ok(window) = window_clone.read() {
                            if !window.contains(&path) {
                                // Inform UI to clear loading state, but return None for image
                                let _ = tx_clone.send((path, None));
                                continue;
                            }
                        }
                    }

                    // 2. Decode
                    let mut success = false;
                    if let Ok(data) = fs::read(&path) {
                        if let Ok(mut raw) = rsraw::RawImage::open(&data) {
                            // Unpack is fast, Process is slow
                            if raw.unpack().is_ok() {
                                raw.set_use_camera_wb(true);
                                // 3. LATE SKIP: Check again before the heavy 'process' call
                                let still_relevant = {
                                    if let Ok(window) = window_clone.read() {
                                        window.contains(&path)
                                    } else { true }
                                };

                                if still_relevant {
                                    if let Ok(processed) = raw.process::<{ rsraw::BIT_DEPTH_8 }>() {
                                        let width = processed.width() as usize;
                                        let height = processed.height() as usize;

                                        // Safety check: Ensure buffer size matches dimensions (RGB = 3 bytes)
                                        if processed.len() == width * height * 3 {
                                            let size = [width, height];
                                            let image = egui::ColorImage::from_rgb(size, &processed);
                                            let _ = tx_clone.send((path.clone(), Some(image)));
                                            success = true;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if !success {
                        let _ = tx_clone.send((path, None));
                    }
                }
            });
        }

        (tx, result_rx)
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
        if let Some(ref current) = self.current_dir {
            if let Some(parent) = current.parent() {
                dirs.push(parent.to_path_buf());
            }
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
        if let Some(ref current) = self.current_dir.clone() {
            if let Some(parent) = current.parent() {
                self.change_directory(parent.to_path_buf());
            }
        }
    }

    /// Updates the resolution metadata for a specific file path
    fn update_file_resolution(&mut self, path: &Path, w: u32, h: u32) {
        // Fast path: Check current item first (most common case during view)
        if let Some(group) = self.state.groups.get_mut(self.state.current_group_idx) {
            if let Some(file) = group.get_mut(self.state.current_file_idx) {
                if file.path == path {
                    if file.resolution.is_none() {
                        file.resolution = Some((w, h));
                    }
                    return;
                }
            }
        }

        // Scan to update resolution for preloaded/background items
        for group in &mut self.state.groups {
            for file in group {
                if file.path == path {
                    if file.resolution.is_none() {
                        file.resolution = Some((w, h));
                    }
                    return;
                }
            }
        }
    }

    pub fn run(self) -> Result<(), eframe::Error> {
        let initial_title = if self.state.is_loading {
            format!("{} v{} | Scanning...", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        } else {
            self.get_title_string()
        };

        let width = self.ctx.gui_config.width.unwrap_or(1280) as f32;
        let height = self.ctx.gui_config.height.unwrap_or(720) as f32;

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
    fn perform_preload(&mut self, ctx: &egui::Context) {
        if self.state.groups.is_empty() { return; }

        let current_g = self.state.current_group_idx;
        let current_f = self.state.current_file_idx;

        if let Some((lg, lf)) = self.last_preload_pos {
            if lg == current_g && lf == current_f { return; }
        }
        self.last_preload_pos = Some((current_g, current_f));

        let preload_limit = self.ctx.gui_config.preload_count.unwrap_or(10);

        let group = &self.state.groups[current_g];

        // Calculate the range of files to preload
        let half = preload_limit / 2;
        let start = current_f.saturating_sub(half);
        let end = (start + preload_limit).min(group.len());
        let start = if end - start < preload_limit { end.saturating_sub(preload_limit) } else { start };

        // 1. Update the Active Window (Shared with Workers)
        let mut active_window_paths = HashSet::new();
        for i in start..end {
            active_window_paths.insert(group[i].path.clone());
        }
        if let Ok(mut w) = self.active_window.write() {
            *w = active_window_paths.clone();
        }

        // Helper to trigger load
        let mut trigger_load = |file_idx: usize| {
            if file_idx >= group.len() { return; }
            let path = &group[file_idx].path;

            if is_raw_ext(path) {
                if !self.raw_cache.contains_key(path) && !self.raw_loading.contains(path) {
                    self.raw_loading.insert(path.clone());
                    let _ = self.raw_preload_tx.send(path.clone());
                }
            } else if file_idx != current_f {
                 let _ = egui::Image::new(format!("file://{}", path.display())).load_for_size(ctx, egui::Vec2::new(2048.0, 2048.0));
            }
        };

        // 2. Prioritize Current Image
        trigger_load(current_f);

        // 3. Queue Neighbors
        if preload_limit > 0 {
            for i in start..end {
                if i != current_f { trigger_load(i); }
            }
        }

        // Cache Eviction
        self.raw_cache.retain(|k, _| active_window_paths.contains(k));
        self.raw_loading.retain(|k| active_window_paths.contains(k));
    }

    fn check_reload(&mut self, ctx: &egui::Context) {
        if self.state.is_loading && self.scan_rx.is_none() {
            let cfg = self.scan_config.clone();
            let ctx_clone = self.ctx.clone();
            let (tx, rx) = unbounded();
            let (prog_tx, prog_rx) = unbounded();

            self.scan_rx = Some(rx);
            self.scan_progress_rx = Some(prog_rx);
            self.scan_progress = (0, 0);

            if let Some(ref sort_order) = self.view_mode_sort {
                let sort = sort_order.clone();
                let paths = cfg.paths.clone();
                thread::spawn(move || {
                    let res = scanner::scan_for_view(&paths, &sort, Some(prog_tx));
                    let _ = tx.send(res);
                });
            } else {
                thread::spawn(move || {
                    let (groups, infos) = scanner::scan_and_group(&cfg, &ctx_clone, Some(prog_tx));
                    let _ = tx.send((groups, infos, Vec::new())); // No subdirs for duplicate mode
                });
            }
        }

        if let Some(prog_rx) = &self.scan_progress_rx {
            while let Ok(progress) = prog_rx.try_recv() {
                self.scan_progress = progress;
                ctx.request_repaint();
            }
        }

        if let Some(rx) = &self.scan_rx {
            if let Ok((new_groups, new_infos, new_subdirs)) = rx.try_recv() {
                // Restore state logic
                let mut target_group_paths: Vec<std::path::PathBuf> = Vec::new();
                let mut target_selected_path = None;
                if self.state.current_group_idx < self.state.groups.len() {
                    let group = &self.state.groups[self.state.current_group_idx];
                    target_group_paths = group.iter().map(|f| f.path.clone()).collect();
                    if self.state.current_file_idx < group.len() { target_selected_path = Some(group[self.state.current_file_idx].path.clone()); }
                }

                let new_total = new_groups.iter().map(|g| g.len()).sum::<usize>();
                let msg = format!("Loaded {} files.", new_total);

                self.state.groups = new_groups;
                self.state.group_infos = new_infos;
                self.state.last_file_count = new_total;
                self.subdirs = new_subdirs;  // Store subdirectories

                // Restore selection
                let mut found_group_idx = None;
                let mut found_file_idx = 0;
                if let Some(selected_path) = &target_selected_path {
                    for (g_idx, group) in self.state.groups.iter().enumerate() {
                        if let Some(f_idx) = group.iter().position(|f| &f.path == selected_path) { found_group_idx = Some(g_idx); found_file_idx = f_idx; break; }
                    }
                }
                if found_group_idx.is_none() && !target_group_paths.is_empty() {
                    'outer: for (g_idx, group) in self.state.groups.iter().enumerate() {
                        for file in group { if target_group_paths.contains(&file.path) { found_group_idx = Some(g_idx); found_file_idx = 0; break 'outer; } }
                    }
                }
                if let Some(g) = found_group_idx { self.state.current_group_idx = g; self.state.current_file_idx = found_file_idx; }
                else { self.state.current_group_idx = 0; self.state.current_file_idx = 0; }

                self.state.status_message = Some((msg, false));
                self.status_set_time = Some(std::time::Instant::now());
                self.state.is_loading = false;
                self.scan_rx = None;
                self.scan_progress_rx = None;
                self.state.selection_changed = true;
                self.last_preload_pos = None;

                // Clear raw caches on reload
                self.raw_cache.clear();
                self.raw_loading.clear();
                if let Ok(mut w) = self.active_window.write() { w.clear(); }

                ctx.send_viewport_cmd(egui::ViewportCommand::Title(self.get_title_string()));
            }
        }
    }

    // Helper to render texture with pan/zoom logic
    fn render_image_texture(&mut self, ui: &mut egui::Ui, texture_id: egui::TextureId, texture_size: egui::Vec2, available_rect: egui::Rect, current_group_idx: usize) {
        // --- 1. Calculate Rotation ---
        let orientation = if let Some(group) = self.state.groups.get(self.state.current_group_idx) {
            if let Some(file) = group.get(self.state.current_file_idx) {
                file.orientation
            } else { 1 }
        } else { 1 };

        let manual_rot = self.state.manual_rotation % 4;

        let exif_angle = match orientation {
            3 => PI,
            6 => PI / 2.0,
            8 => 3.0 * PI / 2.0,
            _ => 0.0,
        };
        let manual_angle = manual_rot as f32 * (PI / 2.0);
        let total_angle = exif_angle + manual_angle;

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
            .paint_at(&ui, paint_rect);

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
}

impl eframe::App for GuiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if !self.initial_scale_applied {
            let user_scale = self.ctx.gui_config.font_scale.unwrap_or(1.0);
            ctx.set_pixels_per_point(ctx.pixels_per_point() * user_scale);
            self.initial_scale_applied = true;
        }

        if let Some(set_time) = self.status_set_time {
            if set_time.elapsed() > std::time::Duration::from_secs(3) {
                self.state.status_message = None;
                self.status_set_time = None;
            }
        }

        // Receive finished raw images from worker thread pool
        while let Ok((path, maybe_image)) = self.raw_preload_rx.try_recv() {
            if let Some(color_image) = maybe_image {
                // Update resolution in metadata now that we have loaded the image
                let size = color_image.size;
                self.update_file_resolution(&path, size[0] as u32, size[1] as u32);

                let name = format!("raw_{}", path.display());
                let texture = ctx.load_texture(name, color_image, Default::default());
                self.raw_cache.insert(path.clone(), texture);
            }
            // Always remove from loading set (even on failure/skip) so it can be retried if needed
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
            } else if self.state.show_confirmation || self.state.error_popup.is_some() || self.state.renaming.is_none() { *intent.borrow_mut() = Some(InputIntent::Cancel); }
            else { *intent.borrow_mut() = Some(InputIntent::Quit); }
        }
        // Ctrl+Q to quit (triggers on_exit to save window size)
        if ctx.input(|i| i.modifiers.ctrl && i.key_pressed(egui::Key::Q)) {
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        }

        // Directory picker navigation
        if self.show_dir_picker {
            if ctx.input(|i| i.key_pressed(egui::Key::ArrowUp)) {
                if self.dir_picker_selection > 0 {
                    self.dir_picker_selection -= 1;
                }
            }
            if ctx.input(|i| i.key_pressed(egui::Key::ArrowDown)) {
                if self.dir_picker_selection + 1 < self.dir_list.len() {
                    self.dir_picker_selection += 1;
                }
            }
            if ctx.input(|i| i.key_pressed(egui::Key::Enter)) {
                if let Some(selected_dir) = self.dir_list.get(self.dir_picker_selection).cloned() {
                    self.show_dir_picker = false;
                    self.change_directory(selected_dir);
                }
            }
        } else if !self.state.is_loading && self.state.renaming.is_none() {
            if ctx.input(|i| i.key_pressed(egui::Key::ArrowRight)) { *intent.borrow_mut() = Some(InputIntent::NextItem); }
            if ctx.input(|i| i.key_pressed(egui::Key::ArrowLeft)) { *intent.borrow_mut() = Some(InputIntent::PrevItem); }
            if ctx.input(|i| i.key_pressed(egui::Key::ArrowDown)) { *intent.borrow_mut() = Some(InputIntent::NextItem); }
            if ctx.input(|i| i.key_pressed(egui::Key::ArrowUp)) { *intent.borrow_mut() = Some(InputIntent::PrevItem); }
            if ctx.input(|i| i.key_pressed(egui::Key::PageDown)) { *intent.borrow_mut() = Some(InputIntent::PageDown); }
            if ctx.input(|i| i.key_pressed(egui::Key::PageUp)) { *intent.borrow_mut() = Some(InputIntent::PageUp); }
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
            if ctx.input(|i| i.key_pressed(egui::Key::Delete)) || ctx.input(|i| i.key_pressed(egui::Key::Backspace)) { *intent.borrow_mut() = Some(InputIntent::DeleteImmediate); }
            if ctx.input(|i| i.key_pressed(egui::Key::M)) { *intent.borrow_mut() = Some(InputIntent::MoveMarked); }
            if ctx.input(|i| i.key_pressed(egui::Key::S)) { *intent.borrow_mut() = Some(InputIntent::ToggleSlideshow); }
            if ctx.input(|i| i.key_pressed(egui::Key::F)) { *intent.borrow_mut() = Some(InputIntent::ToggleFullscreen); }
            if ctx.input(|i| i.key_pressed(egui::Key::O)) { *intent.borrow_mut() = Some(InputIntent::RotateCW); } // Added 'O' key

            // Directory navigation (view mode only)
            if self.state.view_mode {
                if ctx.input(|i| i.key_pressed(egui::Key::C)) {
                    self.open_dir_picker();
                }
                if ctx.input(|i| i.key_pressed(egui::Key::Period)) {
                    self.go_up_directory();
                }
            }
        }

        let pending = intent.borrow().clone();
        if let Some(i) = pending {
            match i {
                InputIntent::CycleViewMode => { self.update_view_state(|v| { v.mode = match v.mode { ViewMode::FitWindow => ViewMode::FitWidth, ViewMode::FitWidth => ViewMode::FitHeight, _ => ViewMode::FitWindow, }; }); },
                // UPDATED: Fit -> 1:1 -> 2x -> 4x -> 8x -> Fit
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
        if self.state.show_confirmation {
            let marked_count = self.state.marked_for_deletion.len();
            let use_trash = self.state.use_trash;
            egui::Window::new("Confirm Deletion").collapsible(false).show(ctx, |ui| {
               ui.label(format!("Are you sure you want to {} {} files?", if use_trash { "trash" } else { "permanently delete" }, marked_count));
               if ui.button("Yes (y)").clicked() { self.state.handle_input(InputIntent::ConfirmDelete); }
               if ui.button("No (n)").clicked() { self.state.handle_input(InputIntent::Cancel); }
           });
       }

       if self.state.show_delete_immediate_confirmation {
           egui::Window::new("Confirm Delete").collapsible(false).show(ctx, |ui| {
               let filename = self.state.get_current_image_path().map(|p| p.file_name().unwrap_or_default().to_string_lossy().to_string()).unwrap_or_default();
               ui.label(format!("Delete current file?\n{}", filename));
               if ui.button("Yes (y)").clicked() { self.state.handle_input(InputIntent::ConfirmDeleteImmediate); }
               if ui.button("No (n)").clicked() { self.state.handle_input(InputIntent::Cancel); }
           });
       }

       if self.state.show_move_confirmation {
           egui::Window::new("Confirm Move").collapsible(false).show(ctx, |ui| {
               let target = self.state.move_target.as_ref().map(|p| p.display().to_string()).unwrap_or_default();
               ui.label(format!("Move marked files to:\n{}", target));
               if ui.button("Yes (y)").clicked() { self.state.handle_input(InputIntent::ConfirmMoveMarked); }
               if ui.button("No (n)").clicked() { self.state.handle_input(InputIntent::Cancel); }
           });
       }

       if self.state.renaming.is_some() {
            egui::Window::new("Rename").collapsible(false).show(ctx, |ui| {
               ui.text_edit_singleline(&mut self.rename_input);
               if ui.button("Rename").clicked() { self.state.handle_input(InputIntent::SubmitRename(self.rename_input.clone())); }
               if ui.button("Cancel").clicked() { self.state.handle_input(InputIntent::Cancel); }
           });
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
        if let Some(interval) = self.state.slideshow_interval {
            if !self.state.slideshow_paused && !self.state.is_loading && !self.state.groups.is_empty() {
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
                     let rot_str = if self.state.manual_rotation % 4 != 0 { format!(" | [O] Rot: {}°", (self.state.manual_rotation % 4) * 90) } else { "".to_string() };

                     let pos_str = if !self.state.groups.is_empty() {
                         let total: usize = self.state.groups.iter().map(|g| g.len()).sum();
                         let current: usize = self.state.groups.iter().take(self.state.current_group_idx).map(|g| g.len()).sum::<usize>() + self.state.current_file_idx + 1;
                         format!(" [{}/{}]", current, total)
                     } else { "".to_string() };

                     ui.horizontal(|ui| {
                         ui.label(format!("W: {}{} | Z: Zoom{}{}{}{}{}", mode_str, extra, rel_tag, slideshow_status, move_status, del_key, rot_str));
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
            // On first frame, reset egui's cached panel width to use our saved value
            if !self.initial_panel_width_applied {
                ctx.memory_mut(|mem| {
                    mem.data.remove::<egui::panel::PanelState>(egui::Id::new("list_panel"));
                });
                self.initial_panel_width_applied = true;
            }
            let window_width = ctx.input(|i| i.viewport().inner_rect.map(|r| r.width()).unwrap_or(1280.0));
            let panel_max_width = window_width * 0.5;
            let panel_response = egui::SidePanel::left("list_panel").resizable(true).default_width(self.panel_width).max_width(panel_max_width).show(ctx, |ui| {
                // Show current directory header in view mode
                if self.state.view_mode {
                    if let Some(ref current_dir) = self.current_dir {
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
                }

                egui::ScrollArea::vertical().show(ui, |ui| {
                    // Show progress when loading
                    if self.state.is_loading {
                        let (current, total) = self.scan_progress;
                        if total > 0 {
                            ui.label(format!("Scanning: {}/{} files...", current, total));
                            let progress = current as f32 / total as f32;
                            ui.add(egui::ProgressBar::new(progress).show_percentage());
                        } else {
                            ui.label("Scanning...");
                            ui.spinner();
                        }
                    } else if self.state.groups.is_empty() && self.subdirs.is_empty() {
                        ui.label(if self.state.view_mode { "No images found." } else { "No duplicates found." });
                    }

                    // In view mode, show subdirectories at the top
                    let mut dir_to_open: Option<std::path::PathBuf> = None;
                    if self.state.view_mode && !self.state.is_loading {
                        // Parent directory entry
                        if let Some(ref current) = self.current_dir {
                            if let Some(parent) = current.parent() {
                                let resp = ui.add(egui::Button::new(
                                    egui::RichText::new("📁 ..")
                                        .color(egui::Color32::YELLOW)
                                ).wrap_mode(egui::TextWrapMode::Truncate));
                                if resp.clicked() {
                                    dir_to_open = Some(parent.to_path_buf());
                                }
                            }
                        }

                        // Subdirectories
                        for subdir in &self.subdirs {
                            let dir_name = subdir.file_name()
                                .map(|n| n.to_string_lossy().to_string())
                                .unwrap_or_else(|| subdir.to_string_lossy().to_string());

                            let resp = ui.add(egui::Button::new(
                                egui::RichText::new(format!("📁 {}", dir_name))
                                    .color(egui::Color32::LIGHT_BLUE)
                            ).wrap_mode(egui::TextWrapMode::Truncate));
                            if resp.clicked() {
                                dir_to_open = Some(subdir.clone());
                            }
                        }

                        // Separator between directories and files
                        if !self.subdirs.is_empty() || self.current_dir.as_ref().and_then(|c| c.parent()).is_some() {
                            ui.separator();
                        }
                    }

                    // We collect actions to perform after the loop
                    let mut action_rename = false;
                    let mut new_selection = None;

                    for (g_idx, group) in self.state.groups.iter().enumerate() {
                        // Only show group headers in duplicate mode
                        if !self.state.view_mode {
                            let info = &self.state.group_infos[g_idx];
                            let (header_text, header_color) = match info.status {
                                GroupStatus::AllIdentical => (format!("Group {} - Bit-identical (hardlinks in light blue)", g_idx + 1), egui::Color32::GREEN),
                                GroupStatus::SomeIdentical => (format!("Group {} - Some Identical", g_idx + 1), egui::Color32::LIGHT_GREEN),
                                GroupStatus::None => (format!("Group {} (Dist: {})", g_idx + 1, info.max_dist), egui::Color32::YELLOW),
                            };
                            ui.colored_label(header_color, header_text);
                        }

                        let counts = get_bit_identical_counts(group);
                        let hardlink_groups = get_hardlink_groups(group);

                        for (f_idx, file) in group.iter().enumerate() {
                            let is_selected = g_idx == self.state.current_group_idx && f_idx == self.state.current_file_idx;
                            let is_marked = self.state.marked_for_deletion.contains(&file.path);
                            let exists = file.path.exists();
                            let is_bit_identical = !self.state.view_mode && *counts.get(&file.content_hash).unwrap_or(&0) > 1;
                            let is_hardlinked = !self.state.view_mode && file.dev_inode
                                .map(|di| hardlink_groups.contains_key(&di))
                                .unwrap_or(false);

                            let text = format!("{} {}{}",
                                if is_marked     { "M" } else { " " },
                                if is_hardlinked { "L " } else { "  " },
                                format_path_depth(&file.path, self.state.path_display_depth)
                            );
                            let mut label_text = egui::RichText::new(text).family(egui::FontFamily::Monospace);
                            if !is_selected {
                                if !exists { label_text = label_text.color(egui::Color32::RED).strikethrough(); }
                                else if is_marked { label_text = label_text.color(egui::Color32::RED); }
                                else if is_hardlinked { label_text = label_text.color(egui::Color32::LIGHT_BLUE); }
                                else if is_bit_identical { label_text = label_text.color(egui::Color32::GREEN); }
                            }

                            // Use Button with selected and wrap_mode to prevent word wrap
                            let resp = ui.add(egui::Button::new(label_text).selected(is_selected).wrap_mode(egui::TextWrapMode::Truncate));
                            if resp.clicked() {
                                new_selection = Some((g_idx, f_idx));
                            }

                            // Restore Context Menu
                            resp.context_menu(|ui| { if ui.button("Rename").clicked() { ui.close(); action_rename = true; } ui.label("Press 'R' to rename selected."); });

                            if is_selected && self.state.selection_changed { resp.scroll_to_me(Some(egui::Align::Center)); }

                            // Restore Metadata Row (Compact) - also no wrap
                            let size_kb = file.size / 1024;
                            let time_str = if self.state.show_relative_times {
                                let ts = Timestamp::from_second(file.modified.timestamp()).unwrap().checked_add(jiff::SignedDuration::from_nanos(file.modified.timestamp_subsec_nanos() as i64)).unwrap();
                                format_relative_time(ts)
                            } else {
                                file.modified.format("%Y-%m-%d %H:%M:%S").to_string()
                            };
                            let res_str = file.resolution.map(|(w, h)| format!("{}x{}", w, h)).unwrap_or("?".to_string());

                            ui.add(egui::Label::new(egui::RichText::new(format!("{} | {} KB | {}", time_str, size_kb, res_str))
                                .size(10.0)
                                .family(egui::FontFamily::Monospace)
                                .color(if is_selected { egui::Color32::WHITE } else { egui::Color32::GRAY })).wrap_mode(egui::TextWrapMode::Truncate));
                        }
                        if !self.state.view_mode { ui.separator(); }
                    }

                    // Apply deferred actions
                    if let Some((g, f)) = new_selection {
                        self.state.current_group_idx = g;
                        self.state.current_file_idx = f;
                        self.state.selection_changed = true;
                    }
                    if action_rename {
                        self.state.handle_input(InputIntent::StartRename);
                    }
                    if let Some(dir) = dir_to_open {
                        self.change_directory(dir);
                    }
                });
            });
            // Track panel width for saving
            self.panel_width = panel_response.response.rect.width();
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            let available_rect = ui.available_rect_before_wrap();
            if let Some(path) = current_image_path {
                 // 1. Check Raw Cache
                 if let Some(texture) = self.raw_cache.get(&path) {
                     self.render_image_texture(ui, texture.id(), texture.size_vec2(), available_rect, current_group_idx);
                 } else {
                     // 2. Fallback / Standard Egui Load
                     let uri = format!("file://{}", path.display());
                     let img_src = egui::Image::new(&uri);
                     match img_src.load_for_size(ui.ctx(), ui.available_size()) {
                         Ok(egui::load::TexturePoll::Ready { texture }) => {
                             // Update metadata with actual resolution if missing
                             if let Some(group) = self.state.groups.get_mut(current_group_idx) {
                                 if let Some(file) = group.get_mut(self.state.current_file_idx) {
                                     if file.resolution.is_none() {
                                         file.resolution = Some((texture.size.x as u32, texture.size.y as u32));
                                     }
                                 }
                             }

                             self.render_image_texture(ui, texture.id, texture.size, available_rect, current_group_idx);
                         },
                         Ok(egui::load::TexturePoll::Pending { .. }) => { ui.spinner(); },
                         Err(_) => {
                             // If egui fails and it's raw, it might still be loading in background
                             if is_raw_ext(&path) {
                                 ui.spinner();
                                 ui.label("Loading raw...");
                                 // Trigger load if not already (failsafe)
                                 if !self.raw_loading.contains(&path) {
                                     self.raw_loading.insert(path.clone());
                                     let _ = self.raw_preload_tx.send(path.clone());
                                 }
                             } else {
                                 ui.label("Failed to load.");
                             }
                         }
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
            } else { ui.centered_and_justified(|ui| ui.label("No image selected")); }
        });

        // Track window size for saving on exit (in logical points, matching with_inner_size)
        let size = ctx.input(|i| {
            // Try outer_rect first (full window), fall back to inner_rect
            i.viewport().outer_rect
                .or(i.viewport().inner_rect)
                .map(|r| (r.width() as u32, r.height() as u32))
        });
        if let Some((w, h)) = size {
            if w > 100 && h > 100 {
                self.last_window_size = Some((w, h));
            }
        }
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        // Save window size and panel width to config
        let mut gui_config = self.ctx.gui_config.clone();
        if let Some((w, h)) = self.last_window_size {
            gui_config.width = Some(w);
            gui_config.height = Some(h);
            eprintln!("Saving window size: {}x{}", w, h);
        } else {
            eprintln!("Warning: No window size captured");
        }
        gui_config.panel_width = Some(self.panel_width);
        eprintln!("Saving panel width: {}", self.panel_width);
        if let Err(e) = self.ctx.save_gui_config(&gui_config) {
            eprintln!("Error saving config: {}", e);
        }
    }
}
