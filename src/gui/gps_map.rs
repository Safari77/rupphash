// GPS Map widget using walkers crate for displaying image locations on a map
use eframe::egui;
use geo::Point;
use rustc_hash::FxHashMap;
use std::path::{Path, PathBuf};
use walkers::{HttpTiles, Map, MapMemory, Plugin, Position, Projector};

/// A single GPS position with associated file path
#[derive(Debug, Clone)]
pub struct GpsMarker {
    pub path: PathBuf,
    pub lat: f64,
    pub lon: f64,
    pub content_hash: [u8; 32],
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
    /// Map memory for walkers (stores zoom, center, etc.)
    pub map_memory: MapMemory,
    /// All GPS markers from loaded images
    pub markers: Vec<GpsMarker>,
    /// FIX: Map path to marker index instead of content_hash to support View mode
    pub path_to_marker: FxHashMap<PathBuf, usize>,
    /// Currently selected marker index (if any)
    pub selected_marker: Option<usize>,
    /// Tile provider (lazy initialized)
    pub tiles: Option<HttpTiles>,
    /// Selected map provider name
    pub provider_name: String,
    /// Provider URL pattern
    pub provider_url: String,
    /// Tile cache path
    pub cache_path: PathBuf,
    /// Selected location from config (for distance calculation)
    pub selected_location: Option<(String, Point<f64>)>,
    /// Clicked marker index
    pub clicked_marker_idx: Option<usize>,
    /// Initial center position (used when map first opens)
    pub initial_center: Option<Position>,
    /// Direction toggle: false = "image to location", true = "location to image"
    pub direction_to_image: bool,
}

impl Default for GpsMapState {
    fn default() -> Self {
        Self {
            visible: false,
            map_memory: MapMemory::default(),
            markers: Vec::new(),
            path_to_marker: FxHashMap::default(), // FIX
            selected_marker: None,
            tiles: None,
            provider_name: "OpenStreetMap".to_string(),
            provider_url: "https://tile.openstreetmap.org/{z}/{x}/{y}.png".to_string(),
            cache_path: PathBuf::new(),
            selected_location: None,
            clicked_marker_idx: None,
            initial_center: None,
            direction_to_image: false,
        }
    }
}

impl GpsMapState {
    pub fn new(cache_path: PathBuf, provider_name: String, provider_url: String) -> Self {
        Self { cache_path, provider_name, provider_url, ..Default::default() }
    }

    /// Toggle map visibility
    pub fn toggle(&mut self) {
        self.visible = !self.visible;
    }

    /// Add a GPS marker, returns true if this is a new marker
    pub fn add_marker(
        &mut self,
        path: PathBuf,
        lat: f64,
        lon: f64,
        content_hash: [u8; 32],
    ) -> bool {
        // FIX: Check uniqueness by path, not content_hash, to allow multiple images
        // with the same hash (or empty hashes in view mode) to appear.
        if self.path_to_marker.contains_key(&path) {
            return false;
        }

        let idx = self.markers.len();
        self.markers.push(GpsMarker { path: path.clone(), lat, lon, content_hash });
        self.path_to_marker.insert(path, idx);
        true
    }

    /// Get marker by content_hash for highlighting
    pub fn get_marker_by_hash(&self, content_hash: &[u8; 32]) -> Option<&GpsMarker> {
        self.markers.iter().find(|m| m.content_hash == *content_hash)
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
        self.path_to_marker.clear(); // FIX
        self.selected_marker = None;
    }

    /// Center map on a specific marker
    pub fn center_on_marker(&mut self, marker_idx: usize) {
        if let Some(marker) = self.markers.get(marker_idx) {
            self.map_memory.center_at(marker.position());
            self.selected_marker = Some(marker_idx);
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

    /// Initialize tiles if not already done
    pub fn ensure_tiles(&mut self, ctx: &egui::Context) {
        if self.tiles.is_none() {
            let tiles = HttpTiles::new(walkers::sources::OpenStreetMap, ctx.clone());
            self.tiles = Some(tiles);
        }
    }
}

/// Plugin for drawing GPS markers on the map
pub struct GpsMarkersPlugin {
    pub markers: Vec<(Position, egui::Color32, f32, usize)>,
}

impl Plugin for GpsMarkersPlugin {
    fn run(
        self: Box<Self>,
        ui: &mut egui::Ui,
        _response: &egui::Response,
        projector: &Projector,
        _memory: &MapMemory,
    ) {
        let painter = ui.painter();

        for (pos, color, radius, _idx) in &self.markers {
            let screen_vec = projector.project(*pos);
            let screen_pos = egui::pos2(screen_vec.x, screen_vec.y);

            painter.circle_filled(screen_pos, *radius, *color);
            painter.circle_stroke(
                screen_pos,
                *radius,
                egui::Stroke::new(1.5, egui::Color32::WHITE),
            );
        }
    }
}

/// Render the GPS map panel
/// current_path is used for highlighting the current file's marker (works in both view mode and duplicate mode)
pub fn render_gps_map(
    state: &mut GpsMapState,
    ui: &mut egui::Ui,
    _available_rect: egui::Rect,
    current_path: Option<&Path>,
) -> Option<PathBuf> {
    state.ensure_tiles(ui.ctx());

    let default_center = walkers::lat_lon(51.0, 17.0);

    // Use path-based lookup for current marker position (works in view mode where content_hash is zeroed)
    let my_position = current_path
        .and_then(|p| state.get_marker_by_path(p))
        .map(|m| m.position())
        .or_else(|| state.markers.first().map(|m| m.position()))
        .unwrap_or(default_center);

    if let Some(pos) = state.initial_center.take() {
        state.map_memory.center_at(pos);
    }

    let markers_data: Vec<_> = state
        .markers
        .iter()
        .enumerate()
        .map(|(idx, marker)| {
            let is_selected = state.selected_marker == Some(idx);
            // Use path-based comparison for current marker (works in view mode)
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

    if let Some(ref mut tiles) = state.tiles {
        let markers_plugin = GpsMarkersPlugin { markers: markers_data };
        let map =
            Map::new(Some(tiles), &mut state.map_memory, my_position).with_plugin(markers_plugin);
        ui.add(map);
    }

    None
}
/// >= 1000m: show as km with 2 decimal places
pub fn format_distance(meters: f64) -> String {
    if meters < 1000.0 { format!("{:.0} m", meters) } else { format!("{:.2} km", meters / 1000.0) }
}

/// Format bearing for display (compass direction)
pub fn format_bearing(degrees: f64) -> String {
    let directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"];
    let idx = ((degrees + 22.5) / 45.0) as usize % 8;
    format!("{:.0}° {}", degrees, directions[idx])
}

/// Get distance and bearing string between two points
/// Returns None if either point is invalid
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
