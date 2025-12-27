use crate::scanner::is_raw_ext;
use crossbeam_channel::{Receiver, Sender, unbounded};
use eframe::egui;
use fast_image_resize::images::Image as FastImage;
use fast_image_resize::{PixelType, ResizeOptions, Resizer};
use image::GenericImageView;
use std::f32::consts::PI;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;

use super::app::GuiApp;
use crate::scanner;

pub const MAX_TEXTURE_SIDE: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(super) enum ViewMode {
    #[default]
    FitWindow,
    FitWidth,
    FitHeight,
    ManualZoom(f32),
}

#[derive(Clone, Copy)]
pub(super) struct GroupViewState {
    pub(super) mode: ViewMode,
    pub(super) pan_center: egui::Pos2,
}

pub enum ImageLoadResult {
    Loaded(egui::ColorImage, (u32, u32), u8, [u8; 32], Option<i64>), // image, resolution, orientation, content_hash, exif_timestamp
    Failed(String),                                                  // Failure with error message
}

impl Default for GroupViewState {
    fn default() -> Self {
        Self { mode: ViewMode::FitWindow, pan_center: egui::Pos2::new(0.5, 0.5) }
    }
}

pub(super) fn spawn_image_loader_pool(
    use_thumbnails: bool,
    content_key: [u8; 32],
) -> (Sender<PathBuf>, Receiver<(PathBuf, ImageLoadResult)>) {
    let (tx, rx) = unbounded::<PathBuf>();
    let (result_tx, result_rx): (
        Sender<(PathBuf, ImageLoadResult)>,
        Receiver<(PathBuf, ImageLoadResult)>,
    ) = unbounded();

    let num_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).min(8);

    for _ in 0..num_threads {
        let rx_clone = rx.clone();
        let tx_clone = result_tx.clone();
        let content_key = content_key; // Copy for each thread

        thread::spawn(move || {
            while let Ok(path) = rx_clone.recv() {
                // Load & Process (Resize + Orientation)
                // Note: We removed the "active window" check here because it caused race conditions
                // where images would fail to load. The cache eviction handles cleanup instead.
                let result =
                    match load_and_process_image_with_hash(&path, use_thumbnails, &content_key) {
                        Ok((img, dims, orientation, content_hash, exif_timestamp)) => {
                            ImageLoadResult::Loaded(
                                img,
                                dims,
                                orientation,
                                content_hash,
                                exif_timestamp,
                            )
                        }
                        Err(err_msg) => ImageLoadResult::Failed(err_msg),
                    };
                let _ = tx_clone.send((path, result));
            }
        });
    }

    (tx, result_rx)
}

fn dynamic_image_to_egui(img: image::DynamicImage) -> egui::ColorImage {
    let rgba = img.to_rgba8();
    let width = rgba.width() as usize;
    let height = rgba.height() as usize;

    let pixels = rgba
        .into_raw()
        .chunks_exact(4)
        .map(|p| egui::Color32::from_rgba_unmultiplied(p[0], p[1], p[2], p[3]))
        .collect();

    egui::ColorImage {
        size: [width, height],
        pixels,
        source_size: egui::vec2(width as f32, height as f32),
    }
}

fn load_and_process_image_with_hash(
    path: &Path,
    use_thumbnails: bool,
    content_key: &[u8; 32],
) -> Result<(egui::ColorImage, (u32, u32), u8, [u8; 32], Option<i64>), String> {
    // Read file once for both hashing and image processing
    let bytes = fs::read(path).map_err(|e| format!("Failed to read file: {}", e))?;

    // Compute content_hash using BLAKE3
    let content_hash = {
        let mut hasher = blake3::Hasher::new_keyed(content_key);
        hasher.update(&bytes);
        *hasher.finalize().as_bytes()
    };

    // Read EXIF timestamp
    let exif_timestamp = crate::scanner::read_exif_data(path, Some(&bytes))
        .and_then(|exif| crate::helper_exif::get_exif_timestamp(&exif));

    // Process the image using existing logic
    let (img, dims, orientation) = load_and_process_image_from_bytes(path, &bytes, use_thumbnails)?;

    Ok((img, dims, orientation, content_hash, exif_timestamp))
}

/// Resize image if it exceeds MAX_TEXTURE_SIDE to prevent egui panics
fn maybe_resize_image(
    mut color_image: egui::ColorImage,
    real_dims: (u32, u32),
    orientation: u8,
    path: &Path,
) -> (egui::ColorImage, (u32, u32), u8) {
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

        if let Ok(src_image) =
            FastImage::from_vec_u8(w as u32, h as u32, color_image.as_raw().to_vec(), pixel_type)
        {
            let mut dst_image = FastImage::new(new_w as u32, new_h as u32, pixel_type);
            // Resize using default options (Lanczos3 for quality)
            let mut resizer = Resizer::new();
            if resizer.resize(&src_image, &mut dst_image, &ResizeOptions::default()).is_ok() {
                eprintln!(
                    "[DEBUG] Fast-Resized {:?} from {}x{} to {}x{}",
                    path, w, h, new_w, new_h
                );
                // Convert back to egui
                match pixel_type {
                    PixelType::U8x4 => {
                        color_image = egui::ColorImage::from_rgba_unmultiplied(
                            [new_w, new_h],
                            dst_image.buffer(),
                        );
                    }
                    PixelType::U8x3 => {
                        color_image =
                            egui::ColorImage::from_rgb([new_w, new_h], dst_image.buffer());
                    }
                    _ => {}
                }
            }
        }
    }

    (color_image, real_dims, orientation)
}

fn load_and_process_image_from_bytes(
    path: &Path,
    bytes: &[u8],
    use_thumbnails: bool,
) -> Result<(egui::ColorImage, (u32, u32), u8), String> {
    // ---------------------------------------------------------------------
    // RAW FILES
    // ---------------------------------------------------------------------
    if is_raw_ext(path) {
        let exif_orientation = crate::scanner::get_orientation(path, Some(bytes));

        let mut raw =
            rsraw::RawImage::open(bytes).map_err(|e| format!("Failed to open RAW file: {}", e))?;
        let dims = (raw.width(), raw.height());

        // Try thumbnail first
        if use_thumbnails && let Some(thumb) = extract_best_thumbnail(&mut raw) {
            // Thumbnails are typically small, but resize if needed
            return Ok(maybe_resize_image(thumb, dims, exif_orientation, path));
        }

        // Full RAW decode
        raw.set_use_camera_wb(true);
        raw.unpack().map_err(|e| format!("Failed to unpack RAW: {}", e))?;
        let processed = raw
            .process::<{ rsraw::BIT_DEPTH_8 }>()
            .map_err(|e| format!("Failed to process RAW: {}", e))?;

        let w = processed.width() as usize;
        let h = processed.height() as usize;

        if processed.len() != w * h * 3 {
            return Err(format!(
                "RAW size mismatch: expected {}x{}x3={}, got {}",
                w,
                h,
                w * h * 3,
                processed.len()
            ));
        }

        let img = egui::ColorImage::from_rgb([w, h], &processed);
        // rsraw handles rotation, orientation=1
        return Ok(maybe_resize_image(img, dims, 1, path));
    }

    // ---------------------------------------------------------------------
    // STANDARD FILES (JPEG, PNG, HEIC, JP2, JXL, etc.)
    // ---------------------------------------------------------------------
    let orientation = crate::scanner::get_orientation(path, Some(bytes));

    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();

    // ---------------------------------------------------------------------
    // JP2 / JXL FAST PATH
    // ---------------------------------------------------------------------
    if matches!(ext.as_str(), "jp2" | "j2k" | "jxl") {
        eprintln!("[DEBUG-GUI] attempting scanner decode for {:?}", path);

        if let Some(dyn_img) = crate::scanner::load_image_fast(path, bytes) {
            let (w, h) = dyn_img.dimensions();
            let color_image = dynamic_image_to_egui(dyn_img);

            eprintln!("[DEBUG-GUI] scanner decode SUCCESS for {:?}", path);
            return Ok(maybe_resize_image(color_image, (w, h), orientation, path));
        } else {
            eprintln!("[DEBUG-GUI] scanner decode FAILED for {:?} (unsupported or corrupt)", path);
            return Err(format!(
                "Failed to decode {} file (unsupported or corrupt)",
                ext.to_uppercase()
            ));
        }
    }

    // ---------------------------------------------------------------------
    // IMAGE CRATE FALLBACK
    // ---------------------------------------------------------------------
    let mut reader = image::ImageReader::new(std::io::Cursor::new(bytes))
        .with_guessed_format()
        .unwrap_or_else(|_| image::ImageReader::new(std::io::Cursor::new(bytes)));

    if reader.format().is_none()
        && let Ok(fmt) = image::ImageFormat::from_path(path)
    {
        reader.set_format(fmt);
    }

    let format_name =
        reader.format().map(|f| format!("{:?}", f)).unwrap_or_else(|| "unknown".to_string());

    let dyn_img =
        reader.decode().map_err(|e| format!("Failed to decode {}: {}", format_name, e))?;
    let dims = (dyn_img.width(), dyn_img.height());

    let rgba = dyn_img.to_rgba8();
    let img = egui::ColorImage::from_rgba_unmultiplied(
        [dims.0 as usize, dims.1 as usize],
        rgba.as_flat_samples().as_slice(),
    );

    Ok(maybe_resize_image(img, dims, orientation, path))
}

pub(super) fn update_file_metadata(
    app: &mut GuiApp,
    path: &Path,
    w: u32,
    h: u32,
    orientation: u8,
    content_hash: [u8; 32],
    exif_timestamp: Option<i64>,
) {
    eprintln!(
        "[DEBUG-UPDATE] update_file_metadata called: path={:?}, orientation={}, content_hash={:?}, exif_ts={:?}",
        path.file_name().unwrap_or_default(),
        orientation,
        &content_hash[..4],
        exif_timestamp,
    );

    // Helper to find and update the file in the group list
    // Returns Some((unique_file_id, gps_pos, exif_timestamp, changed)) if file was found
    let update_file =
        |file: &mut crate::FileMetadata| -> Option<(u128, Option<geo::Point<f64>>, Option<i64>, bool)> {
            if file.path == path {
                eprintln!(
                    "[DEBUG-UPDATE]   Found file! current orientation={}, new orientation={}",
                    file.orientation, orientation
                );
                let mut changed = false;
                if file.resolution.is_none() {
                    file.resolution = Some((w, h));
                    changed = true;
                }
                // Always update orientation from loader - it knows the correct value
                // (e.g., for RAW full decode it's 1, for RAW thumbnails it's EXIF value)
                if file.orientation != orientation {
                    eprintln!(
                        "[DEBUG-UPDATE]   Updated orientation {} -> {}",
                        file.orientation, orientation
                    );
                    file.orientation = orientation;
                    changed = true;
                }
                // Update exif_timestamp if we have a new value and file doesn't have one
                if exif_timestamp.is_some() && file.exif_timestamp.is_none() {
                    file.exif_timestamp = exif_timestamp;
                    changed = true;
                }
                // Return the exif_timestamp to store in database (prefer new value if available)
                let ts_for_db = exif_timestamp.or(file.exif_timestamp);
                return Some((file.unique_file_id, file.gps_pos, ts_for_db, changed));
            }
            None
        };

    // Check current file first (fast path)
    let mut found_info: Option<(u128, Option<geo::Point<f64>>, Option<i64>, bool)> = None;
    if let Some(group) = app.state.groups.get_mut(app.state.current_group_idx)
        && let Some(file) = group.get_mut(app.state.current_file_idx)
    {
        found_info = update_file(file);
    }

    // Fallback search if not found
    if found_info.is_none() {
        for group in &mut app.state.groups {
            for file in group {
                if let Some(info) = update_file(file) {
                    found_info = Some(info);
                    break;
                }
            }
            if found_info.is_some() {
                break;
            }
        }
    }

    // Persist to database if we found the file and something changed
    if let Some((unique_file_id, gps_pos, exif_timestamp, changed)) = found_info {
        if changed && let Some(ref db_tx) = app.db_tx {
            // Use create_feature_update with the real content_hash computed by the image loader
            // This creates/updates the full database entry on first load
            if let Some(update) = crate::db::create_feature_update(
                &app.ctx.meta_key,
                path,
                unique_file_id,
                content_hash,
                Some((w, h)),
                orientation,
                gps_pos,
                exif_timestamp,
            ) {
                let _ = db_tx.send(update);
                eprintln!(
                    "[DEBUG-UPDATE]   Sent DB update for {:?}: resolution={}x{}, orientation={}",
                    path.file_name().unwrap_or_default(),
                    w,
                    h,
                    orientation
                );
            }
        }
    } else {
        eprintln!("[DEBUG-UPDATE]   FILE NOT FOUND in any group!");
    }
}

/// Extract the best (largest) thumbnail from a RAW file
fn extract_best_thumbnail(raw: &mut rsraw::RawImage) -> Option<egui::ColorImage> {
    let thumbs = raw.extract_thumbs().ok()?;

    // Find the largest JPEG thumbnail
    let best_thumb = thumbs
        .into_iter()
        .filter(|t| matches!(t.format, rsraw::ThumbFormat::Jpeg))
        .max_by_key(|t| t.width * t.height)?;

    // Decode JPEG thumbnail using image crate
    let img = image::load_from_memory(&best_thumb.data).ok()?;
    let rgb = img.to_rgb8();
    let (width, height) = rgb.dimensions();

    Some(egui::ColorImage::from_rgb([width as usize, height as usize], rgb.as_raw()))
}

// Helper to render texture with pan/zoom logic
pub(super) fn render_image_texture(
    app: &mut GuiApp,
    ui: &mut egui::Ui,
    texture_id: egui::TextureId,
    texture_size: egui::Vec2,
    available_rect: egui::Rect,
    current_group_idx: usize,
) {
    // --- 1. Calculate Rotation and Flip ---
    let orientation = if let Some(group) = app.state.groups.get(app.state.current_group_idx) {
        if let Some(file) = group.get(app.state.current_file_idx) { file.orientation } else { 1 }
    } else {
        1
    };

    // Get per-file transform state
    let file_transform = app.state.get_current_file_transform();

    // Use per-file rotation instead of global manual_rotation
    let manual_rot = file_transform.rotation % 4;

    let exif_angle = match orientation {
        3 => PI,
        6 => PI / 2.0,
        8 => 3.0 * PI / 2.0,
        _ => 0.0,
    };
    let manual_angle = manual_rot as f32 * (PI / 2.0);
    let total_angle = exif_angle + manual_angle;

    let exif_steps = match orientation {
        3 => 2,
        6 => 1,
        8 => 3,
        _ => 0,
    };
    let total_steps = (exif_steps + manual_rot) % 4;
    let is_rotated_90_270 = total_steps == 1 || total_steps == 3;

    // --- 2. Determine Visual Size (Swapped if rotated) ---
    let visual_size =
        if is_rotated_90_270 { egui::vec2(texture_size.y, texture_size.x) } else { texture_size };

    // --- 3. Calculate Zoom & Layout ---
    let (screen_w, screen_h) = (available_rect.width(), available_rect.height());
    let view_state = *app.group_views.get(&current_group_idx).unwrap_or(&GroupViewState::default());

    let zoom_factor = match view_state.mode {
        ViewMode::FitWindow => (screen_w / visual_size.x).min(screen_h / visual_size.y).min(2.0),
        ViewMode::FitWidth => screen_w / visual_size.x,
        ViewMode::FitHeight => screen_h / visual_size.y,
        ViewMode::ManualZoom(z) => {
            if app.state.zoom_relative {
                let fit_scale = (screen_w / visual_size.x).min(screen_h / visual_size.y);
                z * fit_scale
            } else {
                z
            }
        }
    };

    // Size of the image on screen (visually)
    let virtual_visual_size = visual_size * zoom_factor;

    // --- 4. Handle Pan (Geometry-based) ---
    // Center of the viewport
    let screen_center = available_rect.center();

    // Offset from screen center to image center.
    let offset_x = (0.5 - view_state.pan_center.x) * virtual_visual_size.x;
    let offset_y = (0.5 - view_state.pan_center.y) * virtual_visual_size.y;

    let visual_center = screen_center + egui::vec2(offset_x, offset_y);

    // If image is smaller than screen, force centering (override pan)
    let final_center = egui::pos2(
        if virtual_visual_size.x <= screen_w { screen_center.x } else { visual_center.x },
        if virtual_visual_size.y <= screen_h { screen_center.y } else { visual_center.y },
    );

    // The target rect represents the bounds of the ROTATED image on screen.
    let target_rect = egui::Rect::from_center_size(final_center, virtual_visual_size);

    // --- 5. Determine Paint Rect (Unrotated Geometry) ---
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

    // Calculate UV coordinates for flipping
    let (u_min, u_max) = if file_transform.flip_horizontal { (1.0, 0.0) } else { (0.0, 1.0) };
    let (v_min, v_max) = if file_transform.flip_vertical { (1.0, 0.0) } else { (0.0, 1.0) };
    let uv = egui::Rect::from_min_max(egui::pos2(u_min, v_min), egui::pos2(u_max, v_max));

    // Paint the image into the calculated paint_rect, applying rotation and flips.
    egui::Image::from_texture((texture_id, texture_size))
        .uv(uv)
        .rotate(total_angle, egui::Vec2::splat(0.5))
        .paint_at(ui, paint_rect);

    // --- 7. Interaction ---
    if response.dragged() {
        let d = response.drag_delta();
        let uv_dx = -d.x / virtual_visual_size.x;
        let uv_dy = -d.y / virtual_visual_size.y;

        let new_cx = (view_state.pan_center.x + uv_dx).clamp(0.0, 1.0);
        let new_cy = (view_state.pan_center.y + uv_dy).clamp(0.0, 1.0);

        app.group_views.entry(current_group_idx).or_default().pan_center =
            egui::Pos2::new(new_cx, new_cy);
    }
}

/// Render greyscale histogram, using cached data if available
pub(super) fn render_histogram(
    app: &mut GuiApp,
    ui: &mut egui::Ui,
    available_rect: egui::Rect,
    path: &Path,
) {
    // Get window width for histogram sizing (10% of window width)
    let window_width = ui.ctx().input(|i| {
        i.viewport()
            .inner_rect
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
    let histogram = if let Some((cached_path, cached_hist)) = &app.cached_histogram {
        if cached_path == path { Some(*cached_hist) } else { None }
    } else {
        None
    };

    // Compute if not cached
    let histogram = histogram.or_else(|| {
        let hist = if is_raw_ext(path) {
            compute_histogram_from_raw(path)
        } else {
            compute_histogram_from_image(path)
        };
        // Cache the result
        if let Some(h) = hist {
            app.cached_histogram = Some((path.to_path_buf(), h));
            Some(h)
        } else {
            None
        }
    });

    if let Some(hist) = histogram {
        draw_histogram(ui, hist_rect, &hist);
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
        if count == 0 {
            continue;
        }

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
    painter.rect_stroke(
        hist_rect,
        0.0,
        egui::Stroke::new(1.0, egui::Color32::GRAY),
        egui::StrokeKind::Outside,
    );
}

/// Compute histogram from a standard image file
fn compute_histogram_from_image(path: &Path) -> Option<[u32; 256]> {
    let img = image::open(path).ok()?;
    let grey = img.to_luma8();
    let mut hist = [0u32; 256];
    for pixel in grey.pixels() {
        hist[pixel.0[0] as usize] += 1;
    }
    Some(hist)
}

/// Compute histogram from a RAW file using rsraw
fn compute_histogram_from_raw(path: &Path) -> Option<[u32; 256]> {
    let data = fs::read(path).ok()?;
    let mut raw = rsraw::RawImage::open(&data).ok()?;

    // Try to extract thumbnail first (faster)
    if let Ok(thumbs) = raw.extract_thumbs()
        && let Some(best_thumb) = thumbs
            .into_iter()
            .filter(|t| matches!(t.format, rsraw::ThumbFormat::Jpeg))
            .max_by_key(|t| t.width * t.height)
        && let Ok(img) = image::load_from_memory(&best_thumb.data)
    {
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
pub(super) fn render_exif(
    app: &mut GuiApp,
    ui: &mut egui::Ui,
    available_rect: egui::Rect,
    path: &Path,
) {
    let exif_tags = &app.ctx.gui_config.exif_tags;
    if exif_tags.is_empty() {
        return;
    }

    let decimal_mode = &app.ctx.gui_config.decimal_coords.unwrap_or(false);
    let use_gps = app.state.use_gps_utc;

    // Check cache first
    let tags = if let Some((cached_path, cached_tags)) = &app.cached_exif {
        if cached_path == path {
            cached_tags.clone()
        } else {
            // Cache miss (or invalidated by 'G')
            let new_tags = scanner::get_exif_tags(path, exif_tags, *decimal_mode, use_gps);
            app.cached_exif = Some((path.to_path_buf(), new_tags.clone()));
            // Check fallback warning during load
            if use_gps && !crate::scanner::has_gps_time(path) {
                // We only warn if the user explicitly wanted Sun Position
                if exif_tags.iter().any(|t| t.eq_ignore_ascii_case("DerivedSunPosition")) {
                    app.state.status_message =
                        Some(("Sun Position: GPS Time missing, using Local.".to_string(), true));
                    app.state.status_set_time = Some(std::time::Instant::now());
                }
            }

            new_tags
        }
    } else {
        let new_tags = scanner::get_exif_tags(path, exif_tags, *decimal_mode, use_gps);
        app.cached_exif = Some((path.to_path_buf(), new_tags.clone()));
        new_tags
    };

    if tags.is_empty() {
        return;
    }

    // Extract Sun Position if present and update GPS map
    if let Some((_, val_str)) = tags.iter().find(|(k, _)| k == "Sun Position")
        && let Some((elevation, azimuth)) = crate::position::parse_sun_pos_string(val_str)
    {
        app.gps_map.set_sun_position(path, elevation, azimuth);
    }

    // Get window width for positioning
    let window_width = ui.ctx().input(|i| {
        i.viewport()
            .inner_rect
            .or(i.viewport().outer_rect)
            .map(|r| r.width())
            .unwrap_or(available_rect.width())
    });

    let padding = 10.0;
    let line_height = 14.0;
    let exif_height = (tags.len() as f32) * line_height + 8.0;

    // Calculate position: to the right of histogram if shown, else bottom-left
    let exif_x = if app.show_histogram {
        let hist_width = window_width * 0.10;
        available_rect.min.x + padding + hist_width + padding
    } else {
        available_rect.min.x + padding
    };

    // Estimate width based on content
    let max_label_width =
        tags.iter().map(|(name, value)| name.len() + value.len() + 2).max().unwrap_or(20) as f32
            * 7.0;
    let exif_width = max_label_width.clamp(150.0, 300.0);

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
    painter.rect_stroke(
        exif_rect,
        4.0,
        egui::Stroke::new(1.0, egui::Color32::DARK_GRAY),
        egui::StrokeKind::Outside,
    );
}
