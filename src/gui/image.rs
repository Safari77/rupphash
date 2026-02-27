use crate::scanner::is_raw_ext;
use crossbeam_channel::{Receiver, Sender, unbounded};
use eframe::egui;
use fast_image_resize::images::Image as FastImage;
use fast_image_resize::{PixelType, ResizeOptions, Resizer};
use image::GenericImageView;
use oklab::{LinearRgb, Oklab, linear_srgb_to_oklab, oklab_to_linear_srgb};
use std::f32::consts::PI;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use super::app::GuiApp;
use crate::exif_types::{
    ExifValue, TAG_DERIVED_TIMESTAMP, TAG_GPS_LATITUDE, TAG_GPS_LONGITUDE, TAG_ORIENTATION,
};
use crate::image_features::ImageFeatures;
use crate::raw_exif;
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
    Loaded(
        egui::ColorImage,
        (u32, u32),
        u8,
        [u8; 32],
        Option<i64>,
        Option<([u32; 256], Vec<egui::Color32>)>,
    ), // image, resolution, orientation, content_hash, exif_timestamp, histogram+palette
    Failed(String), // Failure with error message
}

impl Default for GroupViewState {
    fn default() -> Self {
        Self { mode: ViewMode::FitWindow, pan_center: egui::Pos2::new(0.5, 0.5) }
    }
}

pub(super) fn spawn_image_loader_pool(
    use_thumbnails: bool,
    content_key: [u8; 32],
    dominant_colors: usize,
    sat_bias: f32,
    histogram_enabled: Arc<AtomicBool>,
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
        let hist_flag = Arc::clone(&histogram_enabled);

        thread::spawn(move || {
            while let Ok(path) = rx_clone.recv() {
                // Load & Process (Resize + Orientation)
                // Note: We removed the "active window" check here because it caused race conditions
                // where images would fail to load. The cache eviction handles cleanup instead.
                let result =
                    match load_and_process_image_with_hash(&path, use_thumbnails, &content_key) {
                        Ok((img, dims, orientation, content_hash, exif_timestamp)) => {
                            // Only compute histogram + palette when the overlay is enabled;
                            // the disk-based fallback in render_histogram handles cache misses
                            // when the user toggles it on later.
                            let hist_palette = if hist_flag.load(Ordering::Relaxed) {
                                let hp = compute_histogram_from_colorimage(
                                    &img,
                                    dominant_colors,
                                    sat_bias,
                                );

                                // Log dominant colors as gamma-encoded sRGB values
                                let colors_str: Vec<String> =
                                    hp.1.iter()
                                        .map(|c| format!("({}, {}, {})", c.r(), c.g(), c.b()))
                                        .collect();
                                eprintln!(
                                    "[PALETTE] {:?}: [{}]",
                                    path.file_name().unwrap_or_default(),
                                    colors_str.join(", ")
                                );

                                Some(hp)
                            } else {
                                None
                            };

                            ImageLoadResult::Loaded(
                                img,
                                dims,
                                orientation,
                                content_hash,
                                exif_timestamp,
                                hist_palette,
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

    // Read EXIF timestamp - with rsraw fallback for RAW files
    let exif_timestamp = crate::exif_extract::read_exif_data(path, Some(&bytes))
        .and_then(|exif| crate::exif_extract::get_exif_timestamp(&exif))
        .or_else(|| {
            // Fallback to rsraw for RAW files if kamadak-exif failed
            if is_raw_ext(path) {
                rsraw::RawImage::open(&bytes)
                    .ok()
                    .and_then(|raw| raw_exif::get_timestamp_from_raw(&raw))
            } else {
                None
            }
        });

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
        let pixel_type = PixelType::U8x4;

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
                color_image =
                    egui::ColorImage::from_rgba_unmultiplied([new_w, new_h], dst_image.buffer());
            }
        }
    }

    (color_image, real_dims, orientation)
}

/// Fallback: Manually carve out the largest embedded JPEG (PreviewImage)
/// using EXIF/TIFF tags when the RAW decoder completely fails to open the file.
fn extract_biggest_exif_preview(path: &Path, bytes: &[u8]) -> Option<egui::ColorImage> {
    let mut cursor = std::io::Cursor::new(bytes);
    let reader = exif::Reader::new().read_from_container(&mut cursor).ok()?;

    let mut best_offset = 0;
    let mut max_length = 0;
    // Track which Image File Directory (IFD) we found the best preview in
    let mut best_ifd = exif::In::PRIMARY;

    // Iterate through ALL fields across ALL Image File Directories (IFDs)
    for field in reader.fields() {
        if field.tag == exif::Tag::JPEGInterchangeFormatLength {
            let length = match field.value {
                exif::Value::Long(ref v) if !v.is_empty() => v[0] as usize,
                exif::Value::Short(ref v) if !v.is_empty() => v[0] as usize,
                _ => continue,
            };

            // If this is the biggest one we've seen, find its matching offset
            if length > max_length {
                if let Some(offset_field) =
                    reader.get_field(exif::Tag::JPEGInterchangeFormat, field.ifd_num)
                {
                    let offset = match offset_field.value {
                        exif::Value::Long(ref v) if !v.is_empty() => v[0] as usize,
                        exif::Value::Short(ref v) if !v.is_empty() => v[0] as usize,
                        _ => continue,
                    };

                    max_length = length;
                    best_offset = offset;
                    best_ifd = field.ifd_num;
                }
            }
        }
    }

    if max_length == 0 || best_offset + max_length > bytes.len() {
        return None;
    }

    let jpeg_bytes = &bytes[best_offset..best_offset + max_length];

    let img = image::load_from_memory(jpeg_bytes).ok()?;
    let rgb = img.to_rgb8();
    let (width, height) = rgb.dimensions();

    eprintln!(
        "[DEBUG] EXIF Fallback extracted thumbnail for {:?}:\n  \
         - IFD Source: {:?}\n  \
         - Byte Offset: {}\n  \
         - File Size: {} bytes\n  \
         - Resolution: {}x{}",
        path, best_ifd, best_offset, max_length, width, height
    );

    Some(egui::ColorImage::from_rgb([width as usize, height as usize], rgb.as_raw()))
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
        let exif_orientation = crate::exif_extract::get_orientation(path, Some(bytes));

        let mut raw = match rsraw::RawImage::open(bytes) {
            Ok(r) => r,
            Err(e) => {
                // rsraw failed (likely unsupported RAW/ARW)
                if use_thumbnails {
                    if let Some(thumb) = extract_biggest_exif_preview(path, bytes) {
                        let dims = (thumb.width() as u32, thumb.height() as u32);
                        return Ok(maybe_resize_image(thumb, dims, exif_orientation, path));
                    }
                }

                // If the fallback fails or we aren't using thumbnails, return the original error
                return Err(format!("Failed to open RAW file (and EXIF fallback failed): {}", e));
            }
        };

        let dims = (raw.width(), raw.height());

        // Try standard rsraw thumbnail extraction first if it managed to open
        if use_thumbnails && let Some(thumb) = extract_best_thumbnail(&mut raw) {
            // Thumbnails are typically small, but resize if needed
            return Ok(maybe_resize_image(thumb, dims, exif_orientation, path));
        }

        // 2. Full RAW decode mode
        raw.set_use_camera_wb(true);
        match raw.unpack() {
            Ok(_) => match raw.process::<{ rsraw::BIT_DEPTH_8 }>() {
                Ok(processed) => {
                    let w = processed.width() as usize;
                    let h = processed.height() as usize;
                    let total_pixels = w * h;
                    // Determine channels dynamically
                    let img = if processed.len() == total_pixels {
                        // Monochrome: 1 byte per pixel
                        egui::ColorImage::from_gray([w, h], &processed)
                    } else if processed.len() == total_pixels * 3 {
                        // RGB: 3 bytes per pixel
                        egui::ColorImage::from_rgb([w, h], &processed)
                    } else {
                        return Err(format!(
                            "RAW size mismatch: expected {} (Mono) or {} (RGB) bytes, got {}",
                            total_pixels,
                            total_pixels * 3,
                            processed.len()
                        ));
                    };

                    // rsraw handles rotation, orientation=1
                    return Ok(maybe_resize_image(img, dims, 1, path));
                }
                Err(e) => {
                    // Fallback to thumbnail on process error
                    if let Some(thumb) = extract_best_thumbnail(&mut raw) {
                        return Ok(maybe_resize_image(thumb, dims, exif_orientation, path));
                    }
                    return Err(format!("Failed to process RAW: {}", e));
                }
            },
            Err(e) => {
                // Fallback to thumbnail on unpack error (unsupported full decode formats)
                if let Some(thumb) = extract_best_thumbnail(&mut raw) {
                    return Ok(maybe_resize_image(thumb, dims, exif_orientation, path));
                }
                return Err(format!("Failed to unpack RAW: {}", e));
            }
        }
    }

    // ---------------------------------------------------------------------
    // STANDARD FILES (JPEG, PNG, HEIC, JP2, JXL, etc.)
    // ---------------------------------------------------------------------
    let orientation = crate::exif_extract::get_orientation(path, Some(bytes));

    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();

    // ---------------------------------------------------------------------
    // JP2 / JXL FAST PATH
    // ---------------------------------------------------------------------
    if matches!(ext.as_str(), "jp2" | "j2k" | "jxl" | "pdf") {
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
    // Helper to find and update the file in the group list
    // Returns Some((unique_file_id, gps_pos, exif_timestamp, changed)) if file was found
    let update_file =
        |file: &mut crate::FileMetadata| -> Option<(u128, Option<geo::Point<f64>>, Option<i64>, bool)> {
            if file.path == path {
                let mut changed = false;
                if file.resolution.is_none() {
                    file.resolution = Some((w, h));
                    changed = true;
                }
                // Always update orientation from loader - it knows the correct value
                // (e.g., for RAW full decode it's 1, for RAW thumbnails it's EXIF value)
                if file.orientation != orientation {
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
            // Build ImageFeatures from the data we have
            let mut features = ImageFeatures::new(w, h);

            // Add orientation if not default
            if orientation != 1 {
                features.insert_tag(TAG_ORIENTATION, ExifValue::Short(orientation as u16));
            }

            // Add GPS position if available
            if let Some(pos) = gps_pos {
                features.insert_tag(TAG_GPS_LATITUDE, ExifValue::Float(pos.y()));
                features.insert_tag(TAG_GPS_LONGITUDE, ExifValue::Float(pos.x()));
            }

            // Add timestamp if available
            if let Some(ts) = exif_timestamp {
                features.insert_tag(TAG_DERIVED_TIMESTAMP, ExifValue::Long64(ts));
            }

            // Use create_feature_update with ImageFeatures
            if let Some(update) = crate::db::create_feature_update(
                &app.ctx.meta_key,
                path,
                unique_file_id,
                content_hash,
                features,
            ) {
                let _ = db_tx.send(update);
                eprintln!(
                    "[DEBUG-UPDATE]   Sent DB update for {:?}: resolution={}x{}, orientation={}, exif_ts={:?}",
                    path.file_name().unwrap_or_default(),
                    w,
                    h,
                    orientation,
                    exif_timestamp,
                );
            }
        }
    } else {
        eprintln!(
            "[DEBUG-UPDATE]   FILE {:?} NOT FOUND in any group",
            path.file_name().unwrap_or_default()
        );
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
                // Relative zoom implicitly handles texture downscaling because fit_scale
                // dynamically maps whatever the visual_size is to the screen bounds.
                let fit_scale = (screen_w / visual_size.x).min(screen_h / visual_size.y);
                z * fit_scale
            } else {
                // 1. Divide by pixels_per_point so 1 image pixel = 1 physical screen pixel (ignores font_scale)
                let ppp = ui.ctx().pixels_per_point();

                // 2. Compensate for textures downscaled due to MAX_TEXTURE_SIDE (>8192px limits)
                let resolution_scale = app
                    .state
                    .groups
                    .get(app.state.current_group_idx)
                    .and_then(|g| g.get(app.state.current_file_idx))
                    .and_then(|f| f.resolution)
                    .map(|(w, _)| w as f32 / texture_size.x)
                    .unwrap_or(1.0);

                (z * resolution_scale) / ppp
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

#[inline(always)]
fn srgb_to_linear(c: f32) -> f32 {
    if c <= 0.04045 { c / 12.92 } else { ((c + 0.055) / 1.055).powf(2.4) }
}

#[inline(always)]
fn linear_to_srgb(c: f32) -> f32 {
    if c <= 0.0031308 { c * 12.92 } else { 1.055 * c.powf(1.0 / 2.4) - 0.055 }
}

/// Compute luminance histogram and dominant color palette directly from an egui::ColorImage.
/// Downsamples to 128x128 once, converts to Oklab, then computes both histogram (from L)
/// and palette (via K-means++) from the same pixel buffer. This avoids running expensive
/// srgb_to_oklab_l on every pixel of the full-resolution image.
fn compute_histogram_from_colorimage(
    img: &egui::ColorImage,
    dominant_colors: usize,
    sat_bias: f32,
) -> ([u32; 256], Vec<egui::Color32>) {
    let (src_w, src_h) = (img.size[0], img.size[1]);
    let (dst_w, dst_h) = (128usize, 128usize);

    // Downsample to 128x128 using nearest-neighbour and convert to Oklab (one pass)
    let mut oklab_pixels = Vec::with_capacity(dst_w * dst_h);
    for dy in 0..dst_h {
        let sy = (dy * src_h) / dst_h;
        for dx in 0..dst_w {
            let sx = (dx * src_w) / dst_w;
            let px = img.pixels[sy * src_w + sx];
            let lr = srgb_to_linear(px.r() as f32 / 255.0);
            let lg = srgb_to_linear(px.g() as f32 / 255.0);
            let lb = srgb_to_linear(px.b() as f32 / 255.0);
            oklab_pixels.push(linear_srgb_to_oklab(LinearRgb { r: lr, g: lg, b: lb }));
        }
    }

    // --- Histogram: from Oklab L values of the downsampled pixels ---
    let mut hist = [0u32; 256];
    for p in &oklab_pixels {
        let bin = (p.l.clamp(0.0, 1.0) * 255.0).round() as usize;
        hist[bin] += 1;
    }

    // --- Palette: K-means++ on the same Oklab pixels ---
    let palette = kmeans_palette(&oklab_pixels, dominant_colors, sat_bias);

    (hist, palette)
}

/// K-means++ clustering in Oklab space with 3 restarts, 20 iterations, early convergence.
/// Shared implementation used by all palette extraction paths.
/// Returns colors sorted by Oklab perceived lightness.
fn kmeans_palette(pixels: &[Oklab], dominant_colors: usize, sat_bias: f32) -> Vec<egui::Color32> {
    let k = dominant_colors.clamp(1, 25);

    if pixels.is_empty() {
        return vec![egui::Color32::BLACK; k];
    }

    // Stretch the a and b color axes to force K-means to care more about color differences than brightness differences
    let chroma_mult = sat_bias.max(1.0);

    // Calculate weights using an exponential curve for BOTH Saturation and Brightness.
    let weights: Vec<f32> = pixels
        .iter()
        .map(|p| {
            if (sat_bias - 1.0).abs() < f32::EPSILON {
                1.0 // Baseline
            } else {
                // 1. Chroma (Saturation) Weight
                let chroma = (p.a.powi(2) + p.b.powi(2)).sqrt();
                let chroma_weight = chroma * sat_bias * 5.0;

                // 2. Brightness (Luminance) Weight
                let bright_weight = p.l.powi(4) * sat_bias * 1.5;

                // 3. Logarithmic Darkness Penalty
                // Human vision is logarithmic. We use log10 to gently taper mid-shadows
                // but aggressively crush the weight of absolute pitch black.
                let log_curve = (p.l * 9.0 + 1.0).log10();
                let darkness_penalty = log_curve * 0.9 + 0.1;

                // Combine the weights, square the bonuses, and apply the darkness penalty
                (1.0 + (chroma_weight + bright_weight).powi(2)) * darkness_penalty
            }
        })
        .collect();

    let mut best_centroids = vec![pixels[0]; k];
    let mut best_cost = f32::MAX;

    // Run K-means 3 times with different seeds, keep the best result
    for restart in 0..3u64 {
        // K-means++ initialization: pick centroids proportional to squared distance
        let mut centroids = vec![pixels[0]; k];
        let mut rng_state: u64 = 0xDEAD_BEEF_CAFE_0000 ^ (restart * 0x9E3779B97F4A7C15);
        let mut xorshift = || -> u64 {
            rng_state ^= rng_state << 13;
            rng_state ^= rng_state >> 7;
            rng_state ^= rng_state << 17;
            rng_state
        };

        centroids[0] = pixels[(xorshift() as usize) % pixels.len()];

        // 1. Weighted Initialization with Stretched Axes
        for ki in 1..k {
            let mut dists: Vec<f32> = pixels
                .iter()
                .enumerate()
                .map(|(i, p)| {
                    let mut min_d = f32::MAX;
                    for c in &centroids[..ki] {
                        // DISTANCE FORMULA: L is normal, A and B are stretched by chroma_mult
                        let d = (p.l - c.l).powi(2)
                            + ((p.a - c.a) * chroma_mult).powi(2)
                            + ((p.b - c.b) * chroma_mult).powi(2);
                        if d < min_d {
                            min_d = d;
                        }
                    }
                    min_d * weights[i]
                })
                .collect();

            let total: f32 = dists.iter().sum();
            if total <= 0.0 {
                centroids[ki] = pixels[(xorshift() as usize) % pixels.len()];
                continue;
            }
            for i in 1..dists.len() {
                dists[i] += dists[i - 1];
            }

            let threshold = (xorshift() as f32 / u64::MAX as f32) * total;
            let idx = dists.partition_point(|&d| d < threshold).min(pixels.len() - 1);
            centroids[ki] = pixels[idx];
        }

        // 2. Weighted K-means Iterations
        for _ in 0..20 {
            let mut counts = vec![0.0f32; k];
            let mut sums = vec![(0.0f32, 0.0f32, 0.0f32); k];

            for (idx, &p) in pixels.iter().enumerate() {
                let mut min_dist = f32::MAX;
                let mut best_idx = 0;
                for (i, c) in centroids.iter().enumerate() {
                    // Apply stretched axes here too
                    let dist_sq = (p.l - c.l).powi(2)
                        + ((p.a - c.a) * chroma_mult).powi(2)
                        + ((p.b - c.b) * chroma_mult).powi(2);

                    if dist_sq < min_dist {
                        min_dist = dist_sq;
                        best_idx = i;
                    }
                }

                let w = weights[idx];
                counts[best_idx] += w;
                sums[best_idx].0 += p.l * w;
                sums[best_idx].1 += p.a * w;
                sums[best_idx].2 += p.b * w;
            }

            let mut max_shift = 0.0f32;
            for i in 0..k {
                if counts[i] > 0.0 {
                    let new_l = sums[i].0 / counts[i];
                    let new_a = sums[i].1 / counts[i];
                    let new_b = sums[i].2 / counts[i];

                    // Keep tracking shift in stretched space to determine convergence
                    let shift = (centroids[i].l - new_l).powi(2)
                        + ((centroids[i].a - new_a) * chroma_mult).powi(2)
                        + ((centroids[i].b - new_b) * chroma_mult).powi(2);

                    max_shift = max_shift.max(shift);
                    centroids[i].l = new_l;
                    centroids[i].a = new_a;
                    centroids[i].b = new_b;
                }
            }
            if max_shift < 1e-6 {
                break;
            }
        }

        // 3. Weighted Cost Evaluation
        let cost: f32 = pixels
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let min_dist = centroids
                    .iter()
                    .map(|c| {
                        (p.l - c.l).powi(2)
                            + ((p.a - c.a) * chroma_mult).powi(2)
                            + ((p.b - c.b) * chroma_mult).powi(2)
                    })
                    .fold(f32::MAX, f32::min);
                min_dist * weights[i]
            })
            .sum();

        if cost < best_cost {
            best_cost = cost;
            best_centroids = centroids;
        }
    }

    // Convert centroids to sRGB and sort by Oklab perceived lightness
    let mut result: Vec<egui::Color32> = Vec::with_capacity(k);
    let mut lightness: Vec<f32> = Vec::with_capacity(k);
    for i in 0..k {
        lightness.push(best_centroids[i].l);
        let srgb_linear = oklab_to_linear_srgb(best_centroids[i]);
        let r = (linear_to_srgb(srgb_linear.r).clamp(0.0, 1.0) * 255.0).round() as u8;
        let g = (linear_to_srgb(srgb_linear.g).clamp(0.0, 1.0) * 255.0).round() as u8;
        let b = (linear_to_srgb(srgb_linear.b).clamp(0.0, 1.0) * 255.0).round() as u8;
        result.push(egui::Color32::from_rgb(r, g, b));
    }

    let mut indices: Vec<usize> = (0..k).collect();
    indices.sort_by(|&a, &b| lightness[a].partial_cmp(&lightness[b]).unwrap());
    indices.iter().map(|&i| result[i]).collect()
}

/// Compute histogram and palette from a DynamicImage by thumbnailing to 128x128,
/// converting to Oklab once, then computing both histogram (from L) and palette
/// (via K-means++) from the same buffer.
/// Shared by the disk-based fallback paths (standard images and RAW).
fn compute_histogram_from_dynamic_image(
    img: &image::DynamicImage,
    dominant_colors: usize,
    sat_bias: f32,
) -> ([u32; 256], Vec<egui::Color32>) {
    let thumb = img.thumbnail_exact(128, 128).to_rgb8();
    let oklab_pixels: Vec<Oklab> = thumb
        .pixels()
        .map(|p| {
            let lr = srgb_to_linear(p[0] as f32 / 255.0);
            let lg = srgb_to_linear(p[1] as f32 / 255.0);
            let lb = srgb_to_linear(p[2] as f32 / 255.0);
            linear_srgb_to_oklab(LinearRgb { r: lr, g: lg, b: lb })
        })
        .collect();

    let mut hist = [0u32; 256];
    for p in &oklab_pixels {
        let bin = (p.l.clamp(0.0, 1.0) * 255.0).round() as usize;
        hist[bin] += 1;
    }

    let palette = kmeans_palette(&oklab_pixels, dominant_colors, sat_bias);
    (hist, palette)
}

/// Compute histogram and palette from a standard image file
fn compute_histogram_from_image(
    path: &Path,
    dominant_colors: usize,
    sat_bias: f32,
) -> Option<([u32; 256], Vec<egui::Color32>)> {
    let img = image::open(path).ok()?;
    Some(compute_histogram_from_dynamic_image(&img, dominant_colors, sat_bias))
}

/// Compute histogram and palette from a RAW file using rsraw
fn compute_histogram_from_raw(
    path: &Path,
    dominant_colors: usize,
    sat_bias: f32,
) -> Option<([u32; 256], Vec<egui::Color32>)> {
    let data = fs::read(path).ok()?;
    let mut raw = rsraw::RawImage::open(&data).ok()?;

    // Try to extract thumbnail first (faster)
    if let Ok(thumbs) = raw.extract_thumbs() {
        if let Some(best_thumb) = thumbs
            .into_iter()
            .filter(|t| matches!(t.format, rsraw::ThumbFormat::Jpeg))
            .max_by_key(|t| t.width * t.height)
        {
            if let Ok(img) = image::load_from_memory(&best_thumb.data) {
                return Some(compute_histogram_from_dynamic_image(&img, dominant_colors, sat_bias));
            }
        }
    }

    // Fallback: process the full RAW (slower)
    if raw.unpack().is_ok() {
        raw.set_use_camera_wb(true);
        if let Ok(processed) = raw.process::<{ rsraw::BIT_DEPTH_8 }>() {
            let w = raw.width() as usize;
            let h = raw.height() as usize;

            if processed.len() == w * h {
                // Monochrome: construct grayscale DynamicImage
                if let Some(gray_buf) =
                    image::GrayImage::from_raw(w as u32, h as u32, processed.to_vec())
                {
                    return Some(compute_histogram_from_dynamic_image(
                        &image::DynamicImage::ImageLuma8(gray_buf),
                        dominant_colors,
                        sat_bias,
                    ));
                }
            } else if processed.len() == w * h * 3 {
                // RGB: construct DynamicImage
                if let Some(img_buf) =
                    image::RgbImage::from_raw(w as u32, h as u32, processed.to_vec())
                {
                    return Some(compute_histogram_from_dynamic_image(
                        &image::DynamicImage::ImageRgb8(img_buf),
                        dominant_colors,
                        sat_bias,
                    ));
                }
            }
        }
    }
    None
}

/// Render greyscale histogram and dominant palette, using cached data if available
pub(super) fn render_histogram(
    app: &mut GuiApp,
    ui: &mut egui::Ui,
    available_rect: egui::Rect,
    path: &Path,
) {
    let dominant_colors = app.ctx.gui_config.dominant_colors.unwrap_or(5);
    let sat_bias = app.ctx.gui_config.saturation_bias.unwrap_or(1.0);
    let num_rows = (dominant_colors + 4) / 5; // ceiling division by 5

    let window_width = ui.ctx().input(|i| {
        i.viewport()
            .inner_rect
            .or(i.viewport().outer_rect)
            .map(|r| r.width())
            .unwrap_or(available_rect.width())
    });

    let hist_width = window_width * 0.10;
    let hist_height = hist_width * 0.75;
    let swatch_height = 16.0;

    // Total height accounts for multiple palette rows (each row = swatch_height + 4.0 gap)
    let palette_total_height = (num_rows as f32) * swatch_height + ((num_rows as f32) * 4.0);
    let total_height = hist_height + palette_total_height;
    let padding = 10.0;

    let hist_rect = egui::Rect::from_min_size(
        egui::pos2(available_rect.min.x + padding, available_rect.max.y - total_height - padding),
        egui::vec2(hist_width, hist_height),
    );

    // Check cache first (HashMap keyed by path, populated during preload)
    let histogram_data = if let Some(cached_data) = app.cached_histogram.get(path) {
        Some(cached_data.clone())
    } else {
        None
    };

    // Fallback: compute from disk if not preloaded (shouldn't happen often)
    let histogram_data = histogram_data.or_else(|| {
        let data = if is_raw_ext(path) {
            compute_histogram_from_raw(path, dominant_colors, sat_bias)
        } else {
            compute_histogram_from_image(path, dominant_colors, sat_bias)
        };
        // Cache the result
        if let Some(d) = data {
            let colors_str: Vec<String> =
                d.1.iter().map(|c| format!("({}, {}, {})", c.r(), c.g(), c.b())).collect();
            eprintln!(
                "[PALETTE-FALLBACK] {:?}: [{}]",
                path.file_name().unwrap_or_default(),
                colors_str.join(", ")
            );
            app.cached_histogram.insert(path.to_path_buf(), d.clone());
            Some(d)
        } else {
            None
        }
    });

    if let Some((hist, palette)) = histogram_data {
        draw_histogram(ui, hist_rect, &hist, &palette);
    }
}

/// Draw histogram bars and dominant color palette
fn draw_histogram(
    ui: &mut egui::Ui,
    hist_rect: egui::Rect,
    hist: &[u32; 256],
    palette: &[egui::Color32],
) {
    // Find max value for normalization
    let max_val = hist[1..255].iter().copied().max().unwrap_or(1).max(1);
    let painter = ui.painter();
    let hist_width = hist_rect.width();
    let hist_height = hist_rect.height();

    // 1. Draw Histogram Background
    painter.rect_filled(hist_rect, 0.0, egui::Color32::from_black_alpha(180));

    // 2. Draw Histogram Bars
    let bar_width = hist_width / 256.0;
    let usable_height = hist_height - 4.0;

    for (i, &count) in hist.iter().enumerate() {
        if count == 0 {
            continue;
        }

        let normalized = (count as f32 / max_val as f32).min(1.0);
        let bar_height = normalized * usable_height;

        let x = hist_rect.min.x + (i as f32) * bar_width;
        let y_bottom = hist_rect.max.y - 2.0;
        let y_top = y_bottom - bar_height;

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

    painter.rect_stroke(
        hist_rect,
        0.0,
        egui::Stroke::new(1.0, egui::Color32::GRAY),
        egui::StrokeKind::Outside,
    );

    // 3. Draw Palette Swatches in rows of 5
    let swatch_height = 16.0;
    let colors_per_row = 5;
    let swatch_width = hist_width / colors_per_row as f32;
    let num_rows = (palette.len() + colors_per_row - 1) / colors_per_row;

    for row in 0..num_rows {
        let row_start = row * colors_per_row;
        let row_end = (row_start + colors_per_row).min(palette.len());
        let row_y = hist_rect.max.y + 4.0 + (row as f32) * (swatch_height + 4.0);

        for (i, &color) in palette[row_start..row_end].iter().enumerate() {
            let x = hist_rect.min.x + (i as f32) * swatch_width;

            let swatch_rect = egui::Rect::from_min_size(
                egui::pos2(x, row_y),
                egui::vec2(swatch_width, swatch_height),
            );

            painter.rect_filled(swatch_rect, 0.0, color);
        }

        // Draw a border encompassing this row's color strip
        let row_color_count = row_end - row_start;
        let row_strip_width = row_color_count as f32 * swatch_width;
        let palette_rect = egui::Rect::from_min_size(
            egui::pos2(hist_rect.min.x, row_y),
            egui::vec2(row_strip_width, swatch_height),
        );

        painter.rect_stroke(
            palette_rect,
            0.0,
            egui::Stroke::new(1.0, egui::Color32::GRAY),
            egui::StrokeKind::Outside,
        );
    }
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
            if use_gps && !crate::exif_extract::has_gps_time(path) {
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
