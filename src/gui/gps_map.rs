// GPS Map widget using walkers crate for displaying image locations on a map
use eframe::egui;
use geo::Point;
use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;
use std::path::{Path, PathBuf};
use walkers::sources::{Attribution, TileSource};
use walkers::{HttpTiles, Map, MapMemory, Plugin, Position, Projector};

/// Custom tile source that uses a URL pattern
#[derive(Debug, Clone)]
pub struct CustomTileSource {
    pub name: String,
    pub url_pattern: String,
}

impl CustomTileSource {
    pub fn new(name: String, url_pattern: String) -> Self {
        Self { name, url_pattern }
    }
}

impl TileSource for CustomTileSource {
    fn tile_url(&self, tile_id: walkers::TileId) -> String {
        self.url_pattern
            .replace("{z}", &tile_id.zoom.to_string())
            .replace("{x}", &tile_id.x.to_string())
            .replace("{y}", &tile_id.y.to_string())
    }

    fn attribution(&self) -> Attribution {
        // Attribution requires 'static lifetime, so we leak the string
        // This is acceptable since providers are rarely changed
        let text: &'static str = Box::leak(self.name.clone().into_boxed_str());
        Attribution { text, url: "", logo_light: None, logo_dark: None }
    }
}

/// Approximate squared distance for sorting (handles Date Line & Latitude distortion)
/// Still faster than Haversine, but accounts for the fact that longitude shrinks near poles.
fn dist_sq_approx(p1: (f64, f64), p2: (f64, f64)) -> f64 {
    let dy = p1.0 - p2.0; // Latitude difference
    let mut dx = (p1.1 - p2.1).abs(); // Longitude difference
    if dx > 180.0 {
        dx = 360.0 - dx;
    } // Handle crossing the 180th meridian

    // Adjust longitude delta based on latitude.
    // At 60°N, cos(60°) = 0.5, so 1° lon is only half the distance of 1° lat.
    // We use the average latitude for a cheap local approximation.
    let avg_lat_rad = (p1.0 + p2.0).to_radians() * 0.5;
    let dx_corrected = dx * avg_lat_rad.cos();

    // Euclidean distance on the "squashed" grid
    dx_corrected * dx_corrected + dy * dy
}

/// Greedy Nearest Neighbor Sort (O(N^2))
/// Best for visual paths (circles, routes) on datasets < 2000 items.
pub fn sort_nearest_neighbor(markers: &mut Vec<GpsMarker>) {
    let len = markers.len();
    if len < 2 {
        return;
    }

    // Start from index 0. Find the closest remaining point and swap it to index 1.
    // Then find closest to 1 and swap to 2, etc.
    for i in 0..(len - 1) {
        let current_pos = (markers[i].lat, markers[i].lon);
        let mut best_dist = f64::MAX;
        let mut best_idx = i + 1;

        // Scan all subsequent markers
        for j in (i + 1)..len {
            let candidate_pos = (markers[j].lat, markers[j].lon);
            let d2 = dist_sq_approx(current_pos, candidate_pos);

            if d2 < best_dist {
                best_dist = d2;
                best_idx = j;
            }
        }

        // Move the closest found neighbor to the next position in the chain
        markers.swap(i + 1, best_idx);
    }
}

/// Improves a path by uncrossing lines (2-Opt Algorithm).
/// This fixes the "stranding" issue where the greedy sort leaves a long jump at the end.
fn optimize_2opt(markers: &mut Vec<GpsMarker>) {
    let len = markers.len();
    if len < 4 {
        return;
    } // Need at least 4 points to have crossing lines

    let mut improved = true;
    let mut passes = 0;

    // Run up to 5 passes (usually converges in 1 or 2 for simple maps)
    while improved && passes < 5 {
        improved = false;
        passes += 1;

        for i in 0..(len - 2) {
            for j in (i + 2)..(len - 1) {
                // -1 because we are looking at segments
                let p1 = (markers[i].lat, markers[i].lon);
                let p2 = (markers[i + 1].lat, markers[i + 1].lon);
                let p3 = (markers[j].lat, markers[j].lon);
                let p4 = (markers[j + 1].lat, markers[j + 1].lon);

                // Current distance: p1->p2 and p3->p4
                let d_current = dist_sq_approx(p1, p2) + dist_sq_approx(p3, p4);

                // Swapped distance: p1->p3 and p2->p4 (uncrossing)
                let d_swap = dist_sq_approx(p1, p3) + dist_sq_approx(p2, p4);

                if d_swap < d_current {
                    // Reverse the segment between i+1 and j to uncross
                    markers[i + 1..=j].reverse();
                    improved = true;
                }
            }
        }
    }
}

type FnTy = fn(u32) -> u64;

static PART1BY1: Lazy<FnTy> = Lazy::new(|| {
    if std::is_x86_feature_detected!("bmi2") { part1by1_bmi2_safe } else { part1by1_scalar }
});

#[inline]
pub fn part1by1(n: u32) -> u64 {
    (PART1BY1)(n)
}

#[inline]
fn part1by1_bmi2_safe(n: u32) -> u64 {
    unsafe { part1by1_bmi2(n) }
}

#[inline(never)]
#[target_feature(enable = "bmi2")]
unsafe fn part1by1_bmi2(n: u32) -> u64 {
    core::arch::x86_64::_pdep_u64(n as u64, 0x5555_5555_5555_5555)
}

fn part1by1_scalar(mut n: u32) -> u64 {
    n &= 0x0000ffff;
    n = (n ^ (n << 8)) & 0x00ff00ff;
    n = (n ^ (n << 4)) & 0x0f0f0f0f;
    n = (n ^ (n << 2)) & 0x33333333;
    n = (n ^ (n << 1)) & 0x55555555;
    n as u64
}

/// Sorts markers spatially using a Z-Order curve.
/// Includes logic to handle International Date Line wrapping.
pub fn sort_by_hilbert_curve(markers: &mut Vec<GpsMarker>) {
    if markers.len() < 2 {
        return;
    }

    // 1. Analyze bounds to detect Date Line crossing
    let mut min_lon = 180.0;
    let mut max_lon = -180.0;
    let mut min_lat = 90.0;
    let mut max_lat = -90.0;

    for m in markers.iter() {
        if m.lon < min_lon {
            min_lon = m.lon;
        }
        if m.lon > max_lon {
            max_lon = m.lon;
        }
        if m.lat < min_lat {
            min_lat = m.lat;
        }
        if m.lat > max_lat {
            max_lat = m.lat;
        }
    }

    // Heuristic: If markers span > 180 degrees, assume we are crossing the Pacific (Date Line)
    // and shift negative longitudes (-179) to be positive (> 180) for sorting purposes.
    let cross_date_line = (max_lon - min_lon) > 180.0;

    // Recalculate bounds if wrapping
    if cross_date_line {
        min_lon = 360.0; // reset
        max_lon = -360.0;
        for m in markers.iter() {
            // Shift: -179 becomes 181. 179 stays 179.
            let eff_lon = if m.lon < 0.0 { m.lon + 360.0 } else { m.lon };
            if eff_lon < min_lon {
                min_lon = eff_lon;
            }
            if eff_lon > max_lon {
                max_lon = eff_lon;
            }
        }
    }

    let lat_h = (max_lat - min_lat).max(0.000001);
    let lon_h = (max_lon - min_lon).max(0.000001);

    // 2. Sort using Z-Order Curve
    markers.sort_by_cached_key(|m| {
        // Calculate effective longitude for sorting
        let eff_lon = if cross_date_line && m.lon < 0.0 { m.lon + 360.0 } else { m.lon };

        // Normalize to 0..65535
        let x = ((eff_lon - min_lon) / lon_h * 65535.0).clamp(0.0, 65535.0) as u32;
        let y = ((m.lat - min_lat) / lat_h * 65535.0).clamp(0.0, 65535.0) as u32;

        // Interleave bits to get 1D Morton code
        part1by1(x) | (part1by1(y) << 1)
    });
}

/// A single GPS position with associated file path
#[derive(Debug, Clone)]
pub struct GpsMarker {
    pub path: PathBuf,
    pub lat: f64,
    pub lon: f64,
    /// Sun azimuth in degrees (0=North, 90=East, 180=South, 270=West), None if not calculated
    pub sun_azimuth: Option<f64>,
    /// Sun elevation in degrees (negative = below horizon)
    pub sun_elevation: Option<f64>,
    /// EXIF timestamp (Unix epoch seconds) for chronological sorting
    pub exif_timestamp: Option<i64>,
}

impl GpsMarker {
    pub fn position(&self) -> Position {
        walkers::lat_lon(self.lat, self.lon)
    }
}

/// GPS Map state for the application
pub struct GpsMapState {
    /// Whether the GPS map panel is visible
    pub visible: bool,
    /// Whether to draw lines connecting the markers
    pub show_path_lines: bool,
    /// Map memory for walkers (stores zoom, center, etc.)
    pub map_memory: MapMemory,
    /// All GPS markers from loaded images
    pub markers: Vec<GpsMarker>,
    /// Flag to trigger re-sorting when new markers are added to prevent spiderwebs
    pub markers_needs_sort: bool,
    /// Map path to marker index
    pub path_to_marker: FxHashMap<PathBuf, usize>,
    /// Currently selected marker index (if any)
    pub selected_marker: Option<usize>,
    /// Tile provider (lazy initialized)
    pub tiles: Option<HttpTiles>,
    /// Selected map provider name
    pub provider_name: String,
    /// Provider URL pattern
    pub provider_url: String,
    /// Selected location from config (for distance calculation)
    pub selected_location: Option<(String, Point<f64>)>,
    /// Initial center position (used when map first opens)
    pub initial_center: Option<Position>,
    /// Direction toggle: false = "image to location", true = "location to image"
    pub direction_to_image: bool,
    /// Error message if tile provider failed to initialize
    pub tile_error: Option<String>,
    /// Sort mode for markers: true = sort by EXIF timestamp, false = sort by distance
    pub sort_by_exif_timestamp: bool,
    /// Last viewed position for movement calculation
    pub last_pos: Option<(f64, f64)>,
    /// Movement text display string
    pub move_text: Option<String>,
}

impl Default for GpsMapState {
    fn default() -> Self {
        Self {
            visible: false,
            show_path_lines: false,
            markers_needs_sort: false,
            map_memory: MapMemory::default(),
            markers: Vec::new(),
            path_to_marker: FxHashMap::default(),
            selected_marker: None,
            tiles: None,
            provider_name: "OpenStreetMap".to_string(),
            provider_url: "https://tile.openstreetmap.org/{z}/{x}/{y}.png".to_string(),
            selected_location: None,
            initial_center: None,
            direction_to_image: false,
            tile_error: None,
            sort_by_exif_timestamp: false,
            last_pos: None,
            move_text: None,
        }
    }
}

impl GpsMapState {
    pub fn new(_cache_path: PathBuf, provider_name: String, provider_url: String) -> Self {
        Self { provider_name, provider_url, last_pos: None, move_text: None, ..Default::default() }
    }

    /// Remove a marker by path and mark the list for sorting
    pub fn remove_marker(&mut self, path: &Path) {
        if let Some(idx) = self.path_to_marker.remove(path) {
            // 1. Remove from the vector using swap_remove (O(1))
            // This moves the last element into the 'idx' spot to fill the gap.
            if idx < self.markers.len() {
                self.markers.swap_remove(idx);

                // 2. If we didn't just remove the very last element,
                // an element has moved from the end to 'idx'.
                // We must update the lookup map for that moved element.
                if idx < self.markers.len() {
                    let moved_path = self.markers[idx].path.clone();
                    self.path_to_marker.insert(moved_path, idx);
                }
            }

            // 3. Clear selection if the removed marker was selected
            if self.selected_marker == Some(idx) {
                self.selected_marker = None;
            }
            // Note: If the selected marker was the one that *moved* (was at the end),
            // its index effectively changed to 'idx'.
            // However, since we set 'markers_needs_sort', the indices will likely
            // change again on the next frame, so simply deselecting or leaving it
            // is acceptable until the sort logic handles selection mapping.

            // 4. Mark dirty to trigger re-sort/re-line-draw
            self.markers_needs_sort = true;
        }
    }

    /// Add a GPS marker, returns true if this is a new marker
    pub fn add_marker(
        &mut self,
        path: PathBuf,
        lat: f64,
        lon: f64,
        exif_timestamp: Option<i64>,
    ) -> bool {
        // Check uniqueness by path
        if self.path_to_marker.contains_key(&path) {
            return false;
        }

        let idx = self.markers.len();
        self.markers.push(GpsMarker {
            path: path.clone(),
            lat,
            lon,
            sun_azimuth: None,
            sun_elevation: None,
            exif_timestamp,
        });
        self.path_to_marker.insert(path, idx);
        self.markers_needs_sort = true;
        true
    }

    /// Reorder markers based on current sort mode.
    /// If sort_by_exif_timestamp is true: sort chronologically by EXIF timestamp.
    /// Otherwise: sort by spatial distance (nearest neighbor + 2-opt optimization).
    pub fn optimize_path(&mut self) -> f64 {
        let count = self.markers.len();
        if count < 2 {
            self.markers_needs_sort = false;
            return 0.0;
        }

        if self.sort_by_exif_timestamp {
            // Sort by EXIF timestamp (oldest first), files without timestamp go to the end
            self.markers.sort_by(|a, b| {
                match (a.exif_timestamp, b.exif_timestamp) {
                    (Some(ta), Some(tb)) => ta.cmp(&tb),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => a.path.cmp(&b.path), // Fallback: sort by path for consistency
                }
            });

            // Rebuild the lookup map
            self.path_to_marker.clear();
            for (i, m) in self.markers.iter().enumerate() {
                self.path_to_marker.insert(m.path.clone(), i);
            }

            // Calculate total distance
            let mut total_dist = 0.0;
            for i in 0..self.markers.len() - 1 {
                let p1 = (self.markers[i].lat, self.markers[i].lon);
                let p2 = (self.markers[i + 1].lat, self.markers[i + 1].lon);
                let dist = crate::position::distance(p1, p2);
                total_dist += dist;
            }

            let with_ts = self.markers.iter().filter(|m| m.exif_timestamp.is_some()).count();
            eprintln!(
                "[GPS] Chronological Sort Complete. {} of {} markers have EXIF timestamps. Path Length: {}",
                with_ts,
                count,
                format_distance(total_dist)
            );
            self.markers_needs_sort = false;
            return total_dist;
        }

        // Distance-based sorting (default)
        if count < 2000 {
            // 1. Initial Guess: Nearest Neighbor (Greedy)
            // Fast and good, but leaves "stranded" long lines at the end.
            sort_nearest_neighbor(&mut self.markers);

            // 2. Refinement: 2-Opt (Uncrossing)
            // Fixes the ugly cross-country jumps the greedy sort left behind.
            optimize_2opt(&mut self.markers);
        } else {
            // For massive datasets, stick to the instant spatial sort
            sort_by_hilbert_curve(&mut self.markers);
        }

        // Rebuild the lookup map
        self.path_to_marker.clear();
        for (i, m) in self.markers.iter().enumerate() {
            self.path_to_marker.insert(m.path.clone(), i);
        }

        // Calculate and log Total Distance using exact geodesic formula
        let mut total_dist = 0.0;
        for i in 0..self.markers.len() - 1 {
            let p1 = (self.markers[i].lat, self.markers[i].lon);
            let p2 = (self.markers[i + 1].lat, self.markers[i + 1].lon);
            let dist = crate::position::distance(p1, p2);
            total_dist += dist;
        }
        eprintln!("[GPS] Spatial Sort Complete. Path Length: {}", format_distance(total_dist));
        self.markers_needs_sort = false;
        total_dist
    }

    /// Set sun position for a marker by path
    pub fn set_sun_position(&mut self, path: &Path, elevation: f64, azimuth: f64) {
        if let Some(&idx) = self.path_to_marker.get(path)
            && let Some(marker) = self.markers.get_mut(idx)
        {
            marker.sun_elevation = Some(elevation);
            marker.sun_azimuth = Some(azimuth);
        }
    }

    /// Helper to check if we already have a marker for this path
    pub fn get_marker_by_path(&self, path: &Path) -> Option<&GpsMarker> {
        self.path_to_marker.get(path).map(|&idx| &self.markers[idx])
    }

    /// Find the closest marker to a given position
    pub fn find_closest_marker(&self, lat: f64, lon: f64) -> Option<(usize, f64)> {
        if self.markers.is_empty() {
            return None;
        }

        let click_point = (lat, lon);
        let mut closest_idx = 0;
        let mut closest_dist = f64::MAX;

        for (idx, marker) in self.markers.iter().enumerate() {
            let marker_point = (marker.lat, marker.lon);
            let (dist, _bearing) = crate::position::distance_and_bearing(click_point, marker_point);
            if dist < closest_dist {
                closest_dist = dist;
                closest_idx = idx;
            }
        }

        Some((closest_idx, closest_dist))
    }

    /// Clear all markers (e.g., when changing directory)
    pub fn clear_markers(&mut self) {
        self.markers.clear();
        self.path_to_marker.clear();
        self.selected_marker = None;
        self.last_pos = None;
        self.move_text = None;
    }

    /// Center map on a specific marker
    pub fn center_on_marker(&mut self, marker_idx: usize) {
        if let Some(marker) = self.markers.get(marker_idx) {
            self.map_memory.center_at(marker.position());
            self.selected_marker = Some(marker_idx);
        }
    }

    /// Center map on the marker for a specific path
    pub fn center_on_path(&mut self, path: &Path) {
        if let Some(&idx) = self.path_to_marker.get(path)
            && let Some(marker) = self.markers.get(idx)
        {
            self.map_memory.center_at(marker.position());
            self.selected_marker = Some(idx);
        }
    }

    /// Center map on a position
    pub fn center_on_position(&mut self, lat: f64, lon: f64) {
        self.map_memory.center_at(walkers::lat_lon(lat, lon));
    }

    /// Set initial center position for when map opens
    pub fn set_initial_center(&mut self, lat: f64, lon: f64) {
        self.initial_center = Some(walkers::lat_lon(lat, lon));
    }

    /// Initialize tiles with the current provider
    fn init_tiles(&mut self, ctx: &egui::Context) {
        let source = CustomTileSource::new(self.provider_name.clone(), self.provider_url.clone());
        self.tiles = Some(HttpTiles::new(source, ctx.clone()));
        self.tile_error = None;
    }

    /// Initialize tiles if not already done
    pub fn ensure_tiles(&mut self, ctx: &egui::Context) {
        if self.tiles.is_none() {
            self.init_tiles(ctx);
        }
    }

    /// Change map provider - recreates tiles with new source
    pub fn set_provider(&mut self, name: String, url: String, ctx: &egui::Context) {
        self.provider_name = name;
        self.provider_url = url;
        self.tile_error = None;
        // Recreate tiles with new provider
        self.init_tiles(ctx);
    }

    /// Set error message for tile loading failure
    pub fn set_tile_error(&mut self, error: String) {
        self.tile_error = Some(error);
    }
}

/// Plugin for drawing GPS markers on the map and detecting clicks
pub struct GpsMarkersPlugin {
    pub markers: Vec<(Position, egui::Color32, f32, usize)>, // (pos, color, radius, index)
    pub clicked_idx: std::sync::Arc<std::sync::atomic::AtomicI32>, // -1 = no click, >= 0 = clicked marker index
    /// Sun position for current marker: (marker_position, azimuth, elevation)
    pub current_sun: Option<(Position, f64, f64)>,
    /// Map rect for clipping sun indicator to edges
    pub map_rect: egui::Rect,
    pub draw_lines: bool,
}

impl Plugin for GpsMarkersPlugin {
    fn run(
        self: Box<Self>,
        ui: &mut egui::Ui,
        response: &egui::Response,
        projector: &Projector,
        _memory: &MapMemory,
    ) {
        // 1. Create clipped painter
        let painter = ui.painter().with_clip_rect(self.map_rect);

        // --- VIEWPORT CALCULATIONS ---
        let top_left = projector.unproject(self.map_rect.min.to_vec2());
        let bottom_right = projector.unproject(self.map_rect.max.to_vec2());

        let max_lat = top_left.y().max(bottom_right.y());
        let min_lat = top_left.y().min(bottom_right.y());
        let left_lon = top_left.x();
        let right_lon = bottom_right.x();
        let crosses_date_line = left_lon > right_lon;

        // --- DRAW LINES (Restored) ---
        // We check 'self.draw_lines' here. If false, this block is skipped.
        if self.draw_lines && self.markers.len() > 1 {
            let line_stroke = egui::Stroke::new(2.5, egui::Color32::from_rgb(255, 60, 0));

            for pair in self.markers.windows(2) {
                let pos1 = pair[0].0;
                let pos2 = pair[1].0;

                // Optimization: Skip lines that are completely off-screen (Latitude only for speed)
                // If both points are too far North or too far South, don't draw.
                let p1_lat = pos1.y();
                let p2_lat = pos2.y();
                if (p1_lat > max_lat + 0.1 && p2_lat > max_lat + 0.1)
                    || (p1_lat < min_lat - 0.1 && p2_lat < min_lat - 0.1)
                {
                    continue;
                }

                // Naive anti-spiderweb check for world wrapping (Longitude jump > 180)
                if (pos1.x() - pos2.x()).abs() > 180.0 {
                    continue;
                }

                let p1 = projector.project(pos1);
                let p2 = projector.project(pos2);

                painter.line_segment([egui::pos2(p1.x, p1.y), egui::pos2(p2.x, p2.y)], line_stroke);
            }
        }

        // --- DRAW MARKERS (Optimized) ---
        let click_pos = if response.clicked() { response.interact_pointer_pos() } else { None };

        let mut closest_dist = f32::MAX;
        let mut closest_idx = -1;

        for (pos, color, radius, idx) in &self.markers {
            let p_lat = pos.y();
            let p_lon = pos.x();

            // Latitude Culling
            if p_lat > max_lat + 0.1 || p_lat < min_lat - 0.1 {
                continue;
            }

            // Longitude Culling
            let is_visible_lon = if crosses_date_line {
                p_lon >= left_lon - 0.1 || p_lon <= right_lon + 0.1
            } else {
                p_lon >= left_lon - 0.1 && p_lon <= right_lon + 0.1
            };

            if !is_visible_lon {
                continue;
            }

            // Project & Draw
            let screen_vec = projector.project(*pos);
            let screen_pos = egui::pos2(screen_vec.x, screen_vec.y);

            painter.circle_filled(screen_pos, *radius, *color);
            painter.circle_stroke(
                screen_pos,
                *radius,
                egui::Stroke::new(1.5, egui::Color32::WHITE),
            );

            // Magnetic Selection
            if let Some(c_pos) = click_pos {
                let dist = screen_pos.distance(c_pos);
                if dist < closest_dist {
                    closest_dist = dist;
                    closest_idx = *idx as i32;
                }
            }
        }

        // Apply Selection
        if closest_idx >= 0 && closest_dist < 50.0 {
            self.clicked_idx.store(closest_idx, std::sync::atomic::Ordering::Relaxed);
        }

        // --- DRAW SUN ---
        if let Some((marker_pos, azimuth, elevation)) = self.current_sun {
            let marker_screen = projector.project(marker_pos);
            let marker_screen_pos = egui::pos2(marker_screen.x, marker_screen.y);
            draw_sun_indicator(&painter, self.map_rect, marker_screen_pos, azimuth, elevation);
        }
    }
}

/// Render the GPS map panel
/// current_path is used for highlighting the current file's marker (works in both view mode and duplicate mode)
/// Returns the path of a clicked marker if any
pub fn render_gps_map(
    state: &mut GpsMapState,
    ui: &mut egui::Ui,
    _available_rect: egui::Rect,
    current_path: Option<&Path>,
) -> Option<PathBuf> {
    state.ensure_tiles(ui.ctx());

    // Update movement text if current image position changed
    let current_marker_pos =
        current_path.and_then(|p| state.get_marker_by_path(p)).map(|m| (m.lat, m.lon));

    if let Some(pos) = current_marker_pos {
        if let Some(last) = state.last_pos {
            // Check if position actually changed
            if (last.0 - pos.0).abs() > 1e-8 || (last.1 - pos.1).abs() > 1e-8 {
                let (dist, bearing) = crate::position::distance_and_bearing(last, pos);
                state.move_text = Some(format!(
                    "Moved {} into direction {}",
                    format_distance(dist),
                    format_bearing(bearing)
                ));
                state.last_pos = Some(pos);
            }
        } else {
            // First valid GPS image encountered since map was enabled
            state.last_pos = Some(pos);
            state.move_text = None;
        }
    } else {
        // Clear move text if current file has no GPS data
        state.move_text = None;
    }

    // If there's a tile error, display it instead of the map
    if let Some(ref error) = state.tile_error {
        ui.vertical_centered(|ui| {
            ui.add_space(50.0);
            ui.colored_label(egui::Color32::RED, "⚠ Map Provider Error");
            ui.add_space(10.0);
            ui.colored_label(egui::Color32::RED, format!("Provider: {}", state.provider_name));
            ui.colored_label(egui::Color32::RED, error);
            ui.add_space(20.0);
            ui.label("Try selecting a different provider");
        });
        return None;
    }
    if state.show_path_lines && state.markers_needs_sort {
        state.optimize_path();
    }
    let default_center = walkers::lat_lon(51.0, 17.0);

    // Use path-based lookup for current marker position (works in view mode where content_hash is zeroed)
    let my_position = current_path
        .and_then(|p| state.get_marker_by_path(p))
        .map(|m| m.position())
        .or_else(|| state.markers.first().map(|m| m.position()))
        .unwrap_or(default_center);

    // Apply initial center if set (from N key press)
    if let Some(pos) = state.initial_center.take() {
        state.map_memory.center_at(pos);
    }

    let markers_data: Vec<_> = state
        .markers
        .iter()
        .enumerate()
        .map(|(idx, marker)| {
            let is_selected = state.selected_marker == Some(idx);
            // Use path-based comparison for current marker
            let is_current = current_path.map(|p| p == marker.path).unwrap_or(false);

            let (color, radius) = if is_current {
                (egui::Color32::GREEN, 8.0)
            } else if is_selected {
                (egui::Color32::YELLOW, 7.0)
            } else {
                (egui::Color32::GRAY, 5.0)
            };

            (marker.position(), color, radius, idx)
        })
        .collect();

    let mut clicked_path: Option<PathBuf> = None;

    // Shared atomic to communicate clicked marker from plugin
    let clicked_idx = std::sync::Arc::new(std::sync::atomic::AtomicI32::new(-1));

    // Get the available rect for the map before adding it
    let map_rect = ui.available_rect_before_wrap();

    // Get sun position for current marker if available
    let current_sun = current_path
        .and_then(|p| state.path_to_marker.get(p))
        .and_then(|&idx| state.markers.get(idx))
        .and_then(|marker| match (marker.sun_azimuth, marker.sun_elevation) {
            (Some(az), Some(el)) => Some((marker.position(), az, el)),
            _ => None,
        });

    if let Some(ref mut tiles) = state.tiles {
        let markers_plugin = GpsMarkersPlugin {
            markers: markers_data,
            clicked_idx: clicked_idx.clone(),
            current_sun,
            map_rect,
            draw_lines: state.show_path_lines,
        };
        let map =
            Map::new(Some(tiles), &mut state.map_memory, my_position).with_plugin(markers_plugin);
        ui.add(map);

        // Check if a marker was clicked
        let idx = clicked_idx.load(std::sync::atomic::Ordering::Relaxed);
        if idx >= 0 {
            let idx = idx as usize;
            if idx < state.markers.len() {
                state.selected_marker = Some(idx);
                clicked_path = Some(state.markers[idx].path.clone());
            }
        }

        // Draw attribution at bottom right of the map area
        let attribution_text = format!("© {}", state.provider_name);
        ui.painter().text(
            map_rect.max - egui::vec2(5.0, 5.0),
            egui::Align2::RIGHT_BOTTOM,
            attribution_text,
            egui::FontId::proportional(10.0),
            egui::Color32::from_black_alpha(150),
        );
    }
    clicked_path
}

/// Draw sun indicator from marker position to the edge of the map based on azimuth
/// marker_pos: screen position of the current marker
/// azimuth: 0=North, 90=East, 180=South, 270=West
/// elevation: positive = above horizon, negative = below
fn draw_sun_indicator(
    painter: &egui::Painter,
    map_rect: egui::Rect,
    marker_pos: egui::Pos2,
    azimuth: f64,
    elevation: f64,
) {
    // Convert azimuth to radians (0=North=up, clockwise)
    // In screen coords: up is -Y, right is +X
    // azimuth 0 (North) -> angle -90° in standard coords
    // azimuth 90 (East) -> angle 0° in standard coords
    let angle_rad = (azimuth - 90.0).to_radians();

    // Calculate direction vector
    let dir_x = angle_rad.cos() as f32;
    let dir_y = angle_rad.sin() as f32;

    // Find intersection with rectangle edge from marker position
    // We need to find t such that marker_pos + t*dir hits the edge
    let margin = 20.0;
    let left = map_rect.left() + margin;
    let right = map_rect.right() - margin;
    let top = map_rect.top() + margin;
    let bottom = map_rect.bottom() - margin;

    let t_left = if dir_x < -0.001 { (left - marker_pos.x) / dir_x } else { f32::MAX };
    let t_right = if dir_x > 0.001 { (right - marker_pos.x) / dir_x } else { f32::MAX };
    let t_top = if dir_y < -0.001 { (top - marker_pos.y) / dir_y } else { f32::MAX };
    let t_bottom = if dir_y > 0.001 { (bottom - marker_pos.y) / dir_y } else { f32::MAX };

    // Find the smallest positive t (first edge we hit)
    let t = [t_left, t_right, t_top, t_bottom]
        .into_iter()
        .filter(|&t| t > 0.0)
        .fold(f32::MAX, f32::min);

    if t == f32::MAX || t < 30.0 {
        // Sun would be too close to marker or no valid intersection
        return;
    }

    // Sun position on edge
    let sun_x = marker_pos.x + dir_x * t;
    let sun_y = marker_pos.y + dir_y * t;
    let sun_pos = egui::pos2(sun_x, sun_y);

    // Sun color based on elevation (yellow when high, orange/red when low)
    let sun_color = if elevation > 20.0 {
        egui::Color32::from_rgb(255, 220, 50) // Bright yellow
    } else if elevation > 0.0 {
        egui::Color32::from_rgb(255, 180, 50) // Orange-yellow
    } else {
        egui::Color32::from_rgb(200, 100, 50) // Reddish (below horizon)
    };

    let sun_radius = 10.0;

    // Draw dotted line from marker to sun
    let line_color = egui::Color32::from_rgba_unmultiplied(255, 200, 50, 150);
    let num_dots = ((t / 20.0) as i32).clamp(5, 25);
    for i in 0..num_dots {
        let frac = (i as f32 + 0.5) / num_dots as f32;
        // Start from 15% to avoid cluttering near the marker
        if frac > 0.15 {
            let dot_x = marker_pos.x + dir_x * t * frac;
            let dot_y = marker_pos.y + dir_y * t * frac;
            painter.circle_filled(egui::pos2(dot_x, dot_y), 2.0, line_color);
        }
    }

    // Draw sun circle
    painter.circle_filled(sun_pos, sun_radius, sun_color);
    painter.circle_stroke(
        sun_pos,
        sun_radius,
        egui::Stroke::new(2.0, egui::Color32::from_rgb(200, 150, 0)),
    );

    // Draw sun rays
    let ray_color = egui::Color32::from_rgba_unmultiplied(255, 220, 50, 180);
    let num_rays = 12;
    let inner_radius = sun_radius + 3.0;
    let outer_radius = sun_radius + 8.0;
    for i in 0..num_rays {
        let ray_angle =
            (i as f32 * std::f32::consts::TAU / num_rays as f32) + std::f32::consts::FRAC_PI_8;
        let inner_x = sun_pos.x + ray_angle.cos() * inner_radius;
        let inner_y = sun_pos.y + ray_angle.sin() * inner_radius;
        let outer_x = sun_pos.x + ray_angle.cos() * outer_radius;
        let outer_y = sun_pos.y + ray_angle.sin() * outer_radius;
        painter.line_segment(
            [egui::pos2(inner_x, inner_y), egui::pos2(outer_x, outer_y)],
            egui::Stroke::new(2.0, ray_color),
        );
    }

    // Draw elevation text near sun
    let elev_text = format!("{:.2}°", elevation);
    let font_id = egui::FontId::proportional(10.0);
    let text_color = egui::Color32::from_rgb(100, 80, 0);

    // Position text slightly offset from sun (toward map center)
    let center = map_rect.center();
    let text_offset_x = if sun_x > center.x { -25.0 } else { 15.0 };
    let text_offset_y = if sun_y > center.y { -15.0 } else { 5.0 };
    let text_pos = egui::pos2(sun_pos.x + text_offset_x, sun_pos.y + text_offset_y);

    painter.text(text_pos, egui::Align2::LEFT_TOP, elev_text, font_id, text_color);
}

/// >= 1000m: show as km with 2 decimal places
pub fn format_distance(meters: f64) -> String {
    if meters < 1000.0 { format!("{:.0} m", meters) } else { format!("{:.2} km", meters / 1000.0) }
}

/// Format bearing for display (compass direction)
pub fn format_bearing(degrees: f64) -> String {
    let directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"];
    let idx = ((degrees + 22.5) / 45.0) as usize % 8;
    format!("{:.2}° {}", degrees, directions[idx])
}

/// Get distance and bearing string between two points
/// Returns None if either point is invalid
#[allow(dead_code)]
pub fn get_distance_bearing_string(
    from_lat: f64,
    from_lon: f64,
    to_lat: f64,
    to_lon: f64,
) -> Option<String> {
    if !(-90.0..=90.0).contains(&from_lat)
        || !(-180.0..=180.0).contains(&from_lon)
        || !(-90.0..=90.0).contains(&to_lat)
        || !(-180.0..=180.0).contains(&to_lon)
    {
        return None;
    }

    let (distance, bearing) =
        crate::position::distance_and_bearing((from_lat, from_lon), (to_lat, to_lon));

    Some(format!("{} @ {}", format_distance(distance), format_bearing(bearing)))
}
