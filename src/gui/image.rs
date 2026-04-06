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
use std::time::{Duration, Instant};

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
        Option<([u32; 256], [u32; 256], [u32; 256], Vec<(egui::Color32, f32)>)>,
    ), // image, resolution, orientation, content_hash, exif_timestamp, histogram+palette
    AnimatedLoaded {
        frames: Vec<egui::ColorImage>,
        durations: Vec<Duration>,
        resolution: (u32, u32),
        orientation: u8,
        content_hash: [u8; 32],
        exif_timestamp: Option<i64>,
    },
    Failed(String), // Failure with error message
}

/// Playback state for an animated image (e.g. animated WebP)
pub struct AnimationState {
    pub frames: Vec<egui::TextureHandle>,
    pub frame_durations: Vec<Duration>,
    pub current_frame: usize,
    pub last_frame_time: Instant,
}

impl Default for GroupViewState {
    fn default() -> Self {
        Self { mode: ViewMode::FitWindow, pan_center: egui::Pos2::new(0.5, 0.5) }
    }
}

pub(super) fn spawn_image_loader_pool(
    use_thumbnails: bool,
    content_key: [u8; 32],
    palette_config: crate::db::PaletteConfig,
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

        let pcfg = palette_config;
        thread::spawn(move || {
            while let Ok(path) = rx_clone.recv() {
                // Load & Process (Resize + Orientation)
                // Note: We removed the "active window" check here because it caused race conditions
                // where images would fail to load. The cache eviction handles cleanup instead.

                // Check for animated WebP/GIF before standard loading
                let ext_lower = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e.to_ascii_lowercase())
                    .unwrap_or_default();
                let is_webp = ext_lower == "webp";
                let is_gif = ext_lower == "gif";

                if (is_webp || is_gif)
                    && let Ok(bytes) = std::fs::read(&path)
                {
                    let animated_result = if is_webp && is_animated_webp(&bytes) {
                        Some(decode_animated_webp_frames(&path, &bytes))
                    } else if is_gif && is_animated_gif(&bytes) {
                        Some(decode_animated_gif_frames(&path, &bytes))
                    } else {
                        None
                    };

                    if let Some(decode_result) = animated_result {
                        let result = match decode_result {
                            Ok((frames, durations, dims, orientation)) => {
                                // Compute content_hash
                                let content_hash = {
                                    let mut hasher = blake3::Hasher::new_keyed(&content_key);
                                    hasher.update(&bytes);
                                    *hasher.finalize().as_bytes()
                                };
                                let exif_timestamp =
                                    crate::exif_extract::read_exif_data(&path, Some(&bytes))
                                        .and_then(|exif| {
                                            crate::exif_extract::get_exif_timestamp(&exif)
                                        });

                                ImageLoadResult::AnimatedLoaded {
                                    frames,
                                    durations,
                                    resolution: dims,
                                    orientation,
                                    content_hash,
                                    exif_timestamp,
                                }
                            }
                            Err(e) => ImageLoadResult::Failed(e),
                        };
                        let _ = tx_clone.send((path, result));
                        continue;
                    }
                }

                let result =
                    match load_and_process_image_with_hash(&path, use_thumbnails, &content_key) {
                        Ok((img, dims, orientation, content_hash, exif_timestamp)) => {
                            // Only compute histogram + palette when the overlay is enabled;
                            // the disk-based fallback in render_histogram handles cache misses
                            // when the user toggles it on later.
                            let hist_palette = if hist_flag.load(Ordering::Relaxed) {
                                let hp = compute_histogram_from_colorimage(
                                    &img,
                                    pcfg,
                                    img.size[0] != dims.0 as usize
                                        || img.size[1] != dims.1 as usize,
                                );

                                // Log dominant colors as gamma-encoded sRGB values
                                let colors_str: Vec<String> =
                                    hp.3.iter()
                                        .map(|(c, w)| {
                                            format!(
                                                "({}, {}, {} {:.0}%)",
                                                c.r(),
                                                c.g(),
                                                c.b(),
                                                w * 100.0
                                            )
                                        })
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
            if length > max_length
                && let Some(offset_field) =
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

/// Check if a WebP file contains animation by looking for the ANIM chunk in RIFF header
fn is_animated_webp(bytes: &[u8]) -> bool {
    // WebP files start with RIFF....WEBP
    if bytes.len() < 21 || &bytes[0..4] != b"RIFF" || &bytes[8..12] != b"WEBP" {
        return false;
    }
    // VP8X extended header at offset 12, flags byte at offset 20
    // Bit 1 (0x02) of flags indicates animation
    if &bytes[12..16] == b"VP8X" && bytes.len() > 20 {
        return bytes[20] & 0x02 != 0;
    }
    false
}

/// Check if a GIF file contains multiple frames (animation).
/// Scans for more than one Image Descriptor block (0x2C) past the header.
fn is_animated_gif(bytes: &[u8]) -> bool {
    // GIF87a / GIF89a header is 6 bytes
    if bytes.len() < 10 || (&bytes[0..4] != b"GIF8") {
        return false;
    }
    // Count 0x2C (Image Descriptor) introducers; > 1 means animated
    let mut count = 0u32;
    let mut i = 6; // skip past header
    while i < bytes.len() {
        if bytes[i] == 0x2C {
            count += 1;
            if count > 1 {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Convert a slice of `image::Frame`s into egui ColorImages and durations,
/// resizing any frame that exceeds MAX_TEXTURE_SIDE.
fn convert_animation_frames(raw_frames: &[image::Frame]) -> (Vec<egui::ColorImage>, Vec<Duration>) {
    let mut frames = Vec::with_capacity(raw_frames.len());
    let mut durations = Vec::with_capacity(raw_frames.len());

    for frame in raw_frames {
        let delay = frame.delay();
        let (numer, denom) = delay.numer_denom_ms();
        let ms = if denom == 0 { 100 } else { numer / denom };
        // Clamp very short durations (some encoders use 0 or 10ms meaning ~100ms)
        let ms = if ms < 20 { 100 } else { ms };
        durations.push(Duration::from_millis(ms as u64));

        let rgba = frame.buffer();
        let w = rgba.width() as usize;
        let h = rgba.height() as usize;
        let pixels: Vec<egui::Color32> = rgba
            .as_raw()
            .chunks_exact(4)
            .map(|p| egui::Color32::from_rgba_unmultiplied(p[0], p[1], p[2], p[3]))
            .collect();

        let mut color_image =
            egui::ColorImage { size: [w, h], pixels, source_size: egui::vec2(w as f32, h as f32) };

        // Resize individual frames if they exceed texture limits
        if w > MAX_TEXTURE_SIDE || h > MAX_TEXTURE_SIDE {
            let scale = (MAX_TEXTURE_SIDE as f32) / (w.max(h) as f32);
            let new_w = (w as f32 * scale).round() as usize;
            let new_h = (h as f32 * scale).round() as usize;

            let pixel_type = fast_image_resize::PixelType::U8x4;
            if let Ok(src_image) = fast_image_resize::images::Image::from_vec_u8(
                w as u32,
                h as u32,
                color_image.as_raw().to_vec(),
                pixel_type,
            ) {
                let mut dst_image =
                    fast_image_resize::images::Image::new(new_w as u32, new_h as u32, pixel_type);
                let mut resizer = fast_image_resize::Resizer::new();
                if resizer
                    .resize(
                        &src_image,
                        &mut dst_image,
                        &fast_image_resize::ResizeOptions::default(),
                    )
                    .is_ok()
                {
                    color_image = egui::ColorImage::from_rgba_unmultiplied(
                        [new_w, new_h],
                        dst_image.buffer(),
                    );
                }
            }
        }

        frames.push(color_image);
    }

    (frames, durations)
}

/// Decode all frames from an animated WebP file using the image crate's decoder
fn decode_animated_webp_frames(
    path: &Path,
    bytes: &[u8],
) -> Result<(Vec<egui::ColorImage>, Vec<Duration>, (u32, u32), u8), String> {
    use image::AnimationDecoder;
    use image::ImageDecoder;
    use image::codecs::webp::WebPDecoder;

    let orientation = crate::exif_extract::get_orientation(path, Some(bytes));

    let decoder = WebPDecoder::new(std::io::Cursor::new(bytes))
        .map_err(|e| format!("Failed to create WebP decoder: {}", e))?;

    let (img_w, img_h) = decoder.dimensions();
    let dims = (img_w, img_h);

    let raw_frames: Vec<image::Frame> = decoder
        .into_frames()
        .collect_frames()
        .map_err(|e| format!("Failed to decode animated WebP frames: {}", e))?;

    if raw_frames.is_empty() {
        return Err("Animated WebP has no frames".to_string());
    }

    let (frames, durations) = convert_animation_frames(&raw_frames);

    eprintln!(
        "[DEBUG] Decoded animated WebP {:?}: {} frames, dims={}x{}",
        path.file_name().unwrap_or_default(),
        frames.len(),
        dims.0,
        dims.1
    );

    Ok((frames, durations, dims, orientation))
}

/// Decode all frames from an animated GIF file using the image crate's decoder
fn decode_animated_gif_frames(
    path: &Path,
    bytes: &[u8],
) -> Result<(Vec<egui::ColorImage>, Vec<Duration>, (u32, u32), u8), String> {
    use image::AnimationDecoder;
    use image::ImageDecoder;
    use image::codecs::gif::GifDecoder;

    let orientation = crate::exif_extract::get_orientation(path, Some(bytes));

    let decoder = GifDecoder::new(std::io::Cursor::new(bytes))
        .map_err(|e| format!("Failed to create GIF decoder: {}", e))?;

    let (img_w, img_h) = decoder.dimensions();
    let dims = (img_w, img_h);

    let raw_frames: Vec<image::Frame> = decoder
        .into_frames()
        .collect_frames()
        .map_err(|e| format!("Failed to decode animated GIF frames: {}", e))?;

    if raw_frames.is_empty() {
        return Err("Animated GIF has no frames".to_string());
    }

    let (frames, durations) = convert_animation_frames(&raw_frames);

    eprintln!(
        "[DEBUG] Decoded animated GIF {:?}: {} frames, dims={}x{}",
        path.file_name().unwrap_or_default(),
        frames.len(),
        dims.0,
        dims.1
    );

    Ok((frames, durations, dims, orientation))
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
                if use_thumbnails && let Some(thumb) = extract_biggest_exif_preview(path, bytes) {
                    let dims = (thumb.width() as u32, thumb.height() as u32);
                    return Ok(maybe_resize_image(thumb, dims, exif_orientation, path));
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
    // JXL / PDF FAST PATH
    // ---------------------------------------------------------------------
    if matches!(ext.as_str(), "jxl" | "pdf" | "tif" | "tiff") {
        eprintln!("[DEBUG-GUI] attempting scanner decode for {:?}", path);

        match crate::scanner::load_image_fast(path, bytes) {
            Ok(dyn_img) => {
                let (w, h) = dyn_img.dimensions();
                let color_image = dynamic_image_to_egui(dyn_img);

                eprintln!("[DEBUG-GUI] scanner decode SUCCESS for {:?}", path);
                return Ok(maybe_resize_image(color_image, (w, h), orientation, path));
            }
            Err(err_msg) => {
                eprintln!("[DEBUG-GUI] scanner decode FAILED for {:?}: {}", path, err_msg);
                return Err(format!("Failed to decode {} file: {}", ext.to_uppercase(), err_msg));
            }
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

/// Compute a contrasting border color by inverting lightness and rotating hue 180° in Oklab.
fn opposite_color(color: egui::Color32) -> egui::Color32 {
    let lr = srgb_to_linear(color.r() as f32 / 255.0);
    let lg = srgb_to_linear(color.g() as f32 / 255.0);
    let lb = srgb_to_linear(color.b() as f32 / 255.0);
    let ok = linear_srgb_to_oklab(LinearRgb { r: lr, g: lg, b: lb });
    let opp = Oklab {
        l: 1.0 - ok.l, // invert lightness
        a: -ok.a,      // rotate hue 180°
        b: -ok.b,
    };
    let lin = oklab_to_linear_srgb(opp);
    let r = (linear_to_srgb(lin.r).clamp(0.0, 1.0) * 255.0).round() as u8;
    let g = (linear_to_srgb(lin.g).clamp(0.0, 1.0) * 255.0).round() as u8;
    let b = (linear_to_srgb(lin.b).clamp(0.0, 1.0) * 255.0).round() as u8;
    egui::Color32::from_rgb(r, g, b)
}

/// Helper to extract L, A, and B channel histograms simultaneously
fn build_histograms(oklab_pixels: &[Oklab]) -> ([u32; 256], [u32; 256], [u32; 256]) {
    let mut hist_l = [0u32; 256];
    let mut hist_a = [0u32; 256];
    let mut hist_b = [0u32; 256];

    for p in oklab_pixels {
        // Lightness [0.0, 1.0]
        let bin_l = (p.l.clamp(0.0, 1.0) * 255.0).round() as usize;

        // A and B for sRGB practically bound between -0.3 and 0.3.
        // We map [-0.3, 0.3] to [0.0, 1.0]. This beautifully keeps pure grey (0.0)
        // exactly centered at bin 127 in the UI.
        let bin_a = (((p.a + 0.3) / 0.6).clamp(0.0, 1.0) * 255.0).round() as usize;
        let bin_b = (((p.b + 0.3) / 0.6).clamp(0.0, 1.0) * 255.0).round() as usize;

        hist_l[bin_l] += 1;
        hist_a[bin_a] += 1;
        hist_b[bin_b] += 1;
    }

    (hist_l, hist_a, hist_b)
}

/// Compute luminance histogram and dominant color palette directly from an egui::ColorImage.
/// Downsamples to 128x128 once, converts to Oklab, then computes both histogram (from L)
/// and palette (via K-means++) from the same pixel buffer. This avoids running expensive
/// srgb_to_oklab_l on every pixel of the full-resolution image.
fn compute_histogram_from_colorimage(
    img: &egui::ColorImage,
    palette_config: crate::db::PaletteConfig,
    pre_resized: bool,
) -> ([u32; 256], [u32; 256], [u32; 256], Vec<(egui::Color32, f32)>) {
    let crate::db::PaletteConfig {
        dominant_colors,
        saturation_bias: sat_bias,
        palette_sort: pal_sort,
    } = palette_config;
    let (src_w, src_h) = (img.size[0] as u32, img.size[1] as u32);
    let (dst_w, dst_h) = (128u32, 128u32);

    // Detect low-color images (1-bit, indexed, etc.) by sampling the pixels
    // for unique RGB values. If there are fewer unique colors than requested,
    // we return them directly and skip k-means entirely.
    //
    // This only works when the pixels are original (not Lanczos-resampled),
    // because resampling creates intermediate colors at edges. When the image
    // was pre-resized (e.g. exceeding MAX_TEXTURE_SIDE), we skip this check
    // and let k-means handle it.
    let k = dominant_colors.clamp(1, 25);
    let low_color_palette: Option<Vec<(egui::Color32, f32)>> = if !pre_resized {
        let total_pixels = (src_w as usize) * (src_h as usize);
        let sample_count = total_pixels.min(4096);
        let step = (total_pixels / sample_count).max(1);
        let mut counts: std::collections::HashMap<(u8, u8, u8), u32> =
            std::collections::HashMap::new();
        let mut idx = 0;
        while idx < total_pixels && counts.len() <= k {
            let px = img.pixels[idx];
            *counts.entry((px.r(), px.g(), px.b())).or_insert(0) += 1;
            idx += step;
        }
        if counts.len() <= k {
            // Re-count with the full sample to get accurate pixel distribution
            // (the first pass may have stopped early once unique count exceeded k)
            counts.clear();
            idx = 0;
            while idx < total_pixels {
                let px = img.pixels[idx];
                *counts.entry((px.r(), px.g(), px.b())).or_insert(0) += 1;
                idx += step;
            }
            let total_sampled: u32 = counts.values().sum();
            let mut colors: Vec<(Oklab, egui::Color32, f32)> = counts
                .iter()
                .map(|(&(r, g, b), &count)| {
                    let lr = srgb_to_linear(r as f32 / 255.0);
                    let lg = srgb_to_linear(g as f32 / 255.0);
                    let lb = srgb_to_linear(b as f32 / 255.0);
                    let ok = linear_srgb_to_oklab(LinearRgb { r: lr, g: lg, b: lb });
                    (ok, egui::Color32::from_rgb(r, g, b), count as f32 / total_sampled as f32)
                })
                .collect();
            colors.sort_by(|a, b| a.0.l.partial_cmp(&b.0.l).unwrap_or(std::cmp::Ordering::Equal));
            Some(colors.into_iter().map(|(_, c, w)| (c, w)).collect())
        } else {
            None
        }
    } else {
        None
    };

    let mut oklab_pixels = Vec::with_capacity((dst_w * dst_h) as usize);
    let pixel_type = PixelType::U8x4;

    // 1. High-quality downsample using fast_image_resize
    let resized_successfully =
        FastImage::from_vec_u8(src_w, src_h, img.as_raw().to_vec(), pixel_type).ok().and_then(
            |src_image| {
                let mut dst_image = FastImage::new(dst_w, dst_h, pixel_type);
                let mut resizer = Resizer::new();

                resizer
                    .resize(&src_image, &mut dst_image, &ResizeOptions::default())
                    .ok()
                    .map(|_| dst_image)
            },
        );

    if let Some(dst_image) = resized_successfully {
        // 2. Convert smoothed pixels to Oklab
        for chunk in dst_image.buffer().chunks_exact(4) {
            let lr = srgb_to_linear(chunk[0] as f32 / 255.0);
            let lg = srgb_to_linear(chunk[1] as f32 / 255.0);
            let lb = srgb_to_linear(chunk[2] as f32 / 255.0);
            oklab_pixels.push(linear_srgb_to_oklab(LinearRgb { r: lr, g: lg, b: lb }));
        }
    } else {
        // Fallback: Original nearest-neighbor logic if the high-quality resize fails
        let src_w_usize = src_w as usize;
        let src_h_usize = src_h as usize;
        let dst_w_usize = dst_w as usize;
        let dst_h_usize = dst_h as usize;

        for dy in 0..dst_h_usize {
            let sy = (dy * src_h_usize) / dst_h_usize;
            for dx in 0..dst_w_usize {
                let sx = (dx * src_w_usize) / dst_w_usize;
                let px = img.pixels[sy * src_w_usize + sx];
                let lr = srgb_to_linear(px.r() as f32 / 255.0);
                let lg = srgb_to_linear(px.g() as f32 / 255.0);
                let lb = srgb_to_linear(px.b() as f32 / 255.0);
                oklab_pixels.push(linear_srgb_to_oklab(LinearRgb { r: lr, g: lg, b: lb }));
            }
        }
    }

    let (hist_l, hist_a, hist_b) = build_histograms(&oklab_pixels);

    // Use pre-computed palette for low-color images, otherwise run k-means
    let palette = low_color_palette
        .unwrap_or_else(|| kmeans_palette(&oklab_pixels, dominant_colors, sat_bias, pal_sort));

    (hist_l, hist_a, hist_b, palette)
}

/// K-means++ clustering with Logarithmic Culling and Oklch Distance.
fn kmeans_palette(
    pixels: &[Oklab],
    dominant_colors: usize,
    sat_bias: f32,
    pal_sort: crate::db::PaletteSort,
) -> Vec<(egui::Color32, f32)> {
    let k = dominant_colors.clamp(1, 25);

    if pixels.is_empty() {
        return vec![(egui::Color32::BLACK, 1.0 / k as f32); k];
    }

    // 1. logarithmic filtering & exponential weights
    let mut working_pixels = Vec::with_capacity(pixels.len());
    let mut weights = Vec::with_capacity(pixels.len());

    // The cutoff is now 1.0 / 5.5 = 0.18 Oklab Lightness.
    // sRGB(1, 4, 6) is L~0.10 -> Dead.
    // sRGB(25, 38, 54) is L~0.25 -> Alive and well!
    let dark_tuning = 8.0;

    for &p in pixels {
        // THE HARD FLOOR: Don't even run the math if it's visually near-black.
        // This ruthlessly culls the abyss before it can infect the K-means weights.
        if p.l < 0.05 {
            continue;
        }

        let chroma = (p.a.powi(2) + p.b.powi(2)).sqrt();
        let l_weight = (p.l * dark_tuning).log10();

        if l_weight > 0.0 {
            let color_boost = 1.0 + (chroma * 15.0).powf(1.5) * sat_bias;

            working_pixels.push(p);
            weights.push(l_weight * color_boost);
        }
    }

    // Fallback: If the image is literally a pitch-black square, don't crash.
    if working_pixels.len() < k {
        working_pixels = pixels.to_vec();
        weights = vec![1.0; pixels.len()];
    }

    // We group every pixel into one of 4 dominant color zones using Oklab's native axes.
    let mut zone_weights = [0.0f32; 4];

    for (p, &w) in working_pixels.iter().zip(weights.iter()) {
        if p.a.abs() > p.b.abs() {
            if p.a > 0.0 {
                zone_weights[0] += w;
            }
            // RED dominant
            else {
                zone_weights[1] += w;
            } // GREEN dominant
        } else {
            if p.b > 0.0 {
                zone_weights[2] += w;
            }
            // YELLOW dominant
            else {
                zone_weights[3] += w;
            } // BLUE dominant
        }
    }

    // Find the average weight of an active zone
    let active_zones = zone_weights.iter().filter(|&&w| w > 0.0).count() as f32;
    let avg_zone_weight =
        if active_zones > 0.0 { zone_weights.iter().sum::<f32>() / active_zones } else { 1.0 };

    // Equalize! If Orange/Brown (Red+Yellow) is hoarding 1,000,000 weight
    // and Blue only has 10,000, this will mathematically level the playing field.
    for (p, w) in working_pixels.iter().zip(weights.iter_mut()) {
        let zone = if p.a.abs() > p.b.abs() {
            if p.a > 0.0 { 0 } else { 1 }
        } else {
            if p.b > 0.0 { 2 } else { 3 }
        };

        if zone_weights[zone] > 0.0 {
            // We use .sqrt() on the ratio so we don't accidentally give a single
            // stray blue noise pixel the power of a million suns. It's a gentle but firm equalization.
            let equalization_factor = (avg_zone_weight / zone_weights[zone]).sqrt();
            *w *= equalization_factor;
        }
    }

    // 2. Pre-pack data (Array of Structs) & Precompute expensive math
    // This entirely removes millions of .sqrt() and .atan2() calls from the inner loops
    // and guarantees sequential L1 cache access.
    #[derive(Clone, Copy)]
    struct PackedPixel {
        l: f32,
        a: f32,
        b: f32,
        c: f32,
        h: f32,
        weight: f32,
        is_dark: bool,
    }

    let packed_pixels: Vec<PackedPixel> = working_pixels
        .iter()
        .zip(weights.iter())
        .map(|(p, &w)| PackedPixel {
            l: p.l,
            a: p.a,
            b: p.b,
            c: (p.a.powi(2) + p.b.powi(2)).sqrt(),
            h: p.b.atan2(p.a),
            weight: w,
            is_dark: p.l < 0.6,
        })
        .collect();

    // 3. K-Means++ initialization
    let mut centroids = vec![working_pixels[0]; k];
    let mut centroid_chromas = vec![0.0f32; k];
    let mut centroid_hues = vec![0.0f32; k];
    let mut min_dists = vec![f32::MAX; packed_pixels.len()]; // Cache nearest distance

    let mut rng_state: u64 = 0x5EED_C0DE_1234_5678;
    let mut xorshift = || -> u64 {
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        rng_state
    };

    let total_w: f32 = packed_pixels.iter().map(|p| p.weight).sum();
    let mut threshold = (xorshift() as f32 / u64::MAX as f32) * total_w;
    for (i, p) in packed_pixels.iter().enumerate() {
        threshold -= p.weight;
        if threshold <= 0.0 {
            centroids[0] = working_pixels[i];
            break;
        }
    }

    // Helper data struct for centroid comparisons
    #[derive(Clone, Copy)]
    struct CentroidData {
        l: f32,
        a: f32,
        b: f32,
        c: f32,
        h: f32,
        is_dark: bool,
    }

    centroid_chromas[0] = (centroids[0].a.powi(2) + centroids[0].b.powi(2)).sqrt();
    centroid_hues[0] = centroids[0].b.atan2(centroids[0].a);
    let mut new_cd = CentroidData {
        l: centroids[0].l,
        a: centroids[0].a,
        b: centroids[0].b,
        c: centroid_chromas[0],
        h: centroid_hues[0],
        is_dark: centroids[0].l < 0.6,
    };

    // Fast packed distance calculator for initialization
    let calc_packed_dist = |p: &PackedPixel, cd: &CentroidData| -> f32 {
        let dl = (p.l - cd.l) * 2.0;
        let dc = (p.c - cd.c) * 4.0;
        let mut d_h_angle = (p.h - cd.h).abs();
        if d_h_angle > std::f32::consts::PI {
            d_h_angle = std::f32::consts::PI * 2.0 - d_h_angle;
        }
        let mut eff_chroma = p.c.max(cd.c);
        if p.is_dark && cd.is_dark && eff_chroma > 0.015 {
            eff_chroma = eff_chroma.max(0.04);
        }
        eff_chroma = eff_chroma.min(0.25);
        let dh_weighted = d_h_angle * eff_chroma * 3.0;

        dl * dl + dc * dc + dh_weighted * dh_weighted
    };

    // Initial pass: populate min_dists for the first centroid
    for (i, p) in packed_pixels.iter().enumerate() {
        min_dists[i] = calc_packed_dist(p, &new_cd);
    }

    for ki in 1..k {
        let total: f32 =
            min_dists.iter().zip(packed_pixels.iter()).map(|(&d, p)| d * p.weight).sum();

        if total <= 0.0 {
            centroids[ki] = working_pixels[(xorshift() as usize) % working_pixels.len()];
        } else {
            let mut target = (xorshift() as f32 / u64::MAX as f32) * total;
            for i in 0..packed_pixels.len() {
                target -= min_dists[i] * packed_pixels[i].weight;
                if target <= 0.0 {
                    centroids[ki] = working_pixels[i];
                    break;
                }
            }
        }

        // Only check pixels against the newly added centroid
        centroid_chromas[ki] = (centroids[ki].a.powi(2) + centroids[ki].b.powi(2)).sqrt();
        centroid_hues[ki] = centroids[ki].b.atan2(centroids[ki].a);
        new_cd = CentroidData {
            l: centroids[ki].l,
            a: centroids[ki].a,
            b: centroids[ki].b,
            c: centroid_chromas[ki],
            h: centroid_hues[ki],
            is_dark: centroids[ki].l < 0.6,
        };

        for (i, p) in packed_pixels.iter().enumerate() {
            let d = calc_packed_dist(p, &new_cd);
            if d < min_dists[i] {
                min_dists[i] = d;
            }
        }
    }

    // Helper for shift checking and deduplication later
    let calc_dist_centroids = |p: &Oklab, c: &Oklab| -> f32 {
        let p_c = (p.a.powi(2) + p.b.powi(2)).sqrt();
        let c_c = (c.a.powi(2) + c.b.powi(2)).sqrt();
        let mut d_h_angle = (p.b.atan2(p.a) - c.b.atan2(c.a)).abs();
        if d_h_angle > std::f32::consts::PI {
            d_h_angle = std::f32::consts::PI * 2.0 - d_h_angle;
        }
        let mut effective_chroma = p_c.max(c_c);
        if p.l < 0.6 && c.l < 0.6 && effective_chroma > 0.015 {
            effective_chroma = effective_chroma.max(0.04);
        }
        effective_chroma = effective_chroma.min(0.25);
        let dl = (p.l - c.l) * 2.0;
        let dc = (p_c - c_c) * 4.0;
        let dh_weighted = d_h_angle * effective_chroma * 3.0;
        dl * dl + dc * dc + dh_weighted * dh_weighted
    };

    // 4. K-Means iterations
    let mut final_counts = vec![0.0f32; k];
    for _ in 0..20 {
        let mut counts = vec![0.0f32; k];
        let mut sums = vec![(0.0f32, 0.0f32, 0.0f32); k];

        // Pre-pack ONLY the centroids to save billions of operations
        let cent_data: Vec<CentroidData> = centroids
            .iter()
            .map(|c| CentroidData {
                l: c.l,
                a: c.a,
                b: c.b,
                c: (c.a.powi(2) + c.b.powi(2)).sqrt(),
                h: c.b.atan2(c.a),
                is_dark: c.l < 0.6,
            })
            .collect();
        let cd_slice = &cent_data[..];

        for p in &packed_pixels {
            let mut min_dist = f32::MAX;
            let mut best_idx = 0;

            for (i, cd) in cd_slice.iter().enumerate() {
                // ── EARLY EXIT 1: Lightness ──
                let dl = (p.l - cd.l) * 2.0;
                let dl_sq = dl * dl;
                if dl_sq >= min_dist {
                    continue;
                }

                // ── EARLY EXIT 2: Chroma ──
                let dc = (p.c - cd.c) * 4.0;
                let dc_sq = dc * dc;
                let base_dist = dl_sq + dc_sq;
                if base_dist >= min_dist {
                    continue;
                }

                // ── EXPENSIVE MATH ──
                let mut d_h_angle = (p.h - cd.h).abs();
                if d_h_angle > std::f32::consts::PI {
                    d_h_angle = std::f32::consts::PI * 2.0 - d_h_angle;
                }

                let mut eff_chroma = p.c.max(cd.c);
                if p.is_dark && cd.is_dark && eff_chroma > 0.015 {
                    eff_chroma = eff_chroma.max(0.04);
                }
                eff_chroma = eff_chroma.min(0.25);

                let dh_weighted = d_h_angle * eff_chroma * 3.0;
                let dist_sq = base_dist + dh_weighted * dh_weighted;

                if dist_sq < min_dist {
                    min_dist = dist_sq;
                    best_idx = i;
                }
            }

            counts[best_idx] += p.weight;
            sums[best_idx].0 += p.l * p.weight;
            sums[best_idx].1 += p.a * p.weight;
            sums[best_idx].2 += p.b * p.weight;
        }

        let mut max_shift = 0.0f32;
        for i in 0..k {
            if counts[i] > 0.0 {
                let new_l = sums[i].0 / counts[i];
                let new_a = sums[i].1 / counts[i];
                let new_b = sums[i].2 / counts[i];

                let shift =
                    calc_dist_centroids(&centroids[i], &Oklab { l: new_l, a: new_a, b: new_b });

                max_shift = max_shift.max(shift);
                centroids[i].l = new_l;
                centroids[i].a = new_a;
                centroids[i].b = new_b;
            }
        }
        if max_shift < 1e-6 {
            final_counts = counts;
            break;
        }
        final_counts = counts;
    }

    // 4.5. Anti-crowding deduplication (the tuning knob)
    let mut clusters: Vec<(f32, Oklab)> =
        centroids.into_iter().enumerate().map(|(i, c)| (final_counts[i], c)).collect();

    clusters.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    let mut unique_centroids: Vec<(f32, Oklab)> = Vec::new();
    let total_pixels: f32 = clusters.iter().map(|(count, _)| count).sum();

    for (count, c) in clusters {
        if count == 0.0 {
            continue;
        }

        let mut too_close = false;
        // Lower the tiny spot threshold so it doesn't trigger on medium-small details
        let is_tiny_spot = count < (total_pixels * 0.015);

        for &(kept_count, kept_c) in &unique_centroids {
            let dist = calc_dist_centroids(&c, &kept_c);

            // Give dark colors a "shield" against aggressive merging.
            // If both colors are dark, they must be VERY close to merge.
            let is_dark_collision = c.l < 0.35 && kept_c.l < 0.35;

            let tiny_merge_dist = if is_dark_collision { 0.0005 } else { 0.0015 };
            let standard_merge_dist = if is_dark_collision { 0.0001 } else { 0.0003 };

            if dist < standard_merge_dist
                || (is_tiny_spot && dist < tiny_merge_dist && count < kept_count * 0.5)
            {
                too_close = true;
                break;
            }
        }

        if !too_close {
            unique_centroids.push((count, c));
        }
    }
    let unique_centroids: Vec<(f32, Oklab)> = unique_centroids;

    // 5. Convert and sort (hue buckets + lightness)
    // ==========================================================
    // Returning 6 distinct, beautiful colors is vastly superior to
    // returning 10 colors where 4 are identical bright spots.
    let final_k = unique_centroids.len();

    // Classify each centroid as chromatic or achromatic.
    // When chroma is near-zero the hue angle from atan2 is meaningless
    // noise, so we must not let it drive sorting.
    let grey_threshold = 0.01;

    // Compute hue buckets only for chromatic centroids; count distinct buckets
    // to decide whether hue-bucket sorting adds value or just scrambles things.
    let mut chromatic_buckets: std::collections::HashSet<i32> = std::collections::HashSet::new();
    let mut bucket_counts: [u32; 8] = [0; 8];
    for (_, c) in &unique_centroids {
        let chroma = (c.a.powi(2) + c.b.powi(2)).sqrt();
        if chroma >= grey_threshold {
            let mut h = c.b.atan2(c.a);
            if h < 0.0 {
                h += std::f32::consts::PI * 2.0;
            }
            let bucket = ((h * 8.0) / (std::f32::consts::PI * 2.0)).round() as i32 % 8;
            chromatic_buckets.insert(bucket);
            bucket_counts[bucket as usize] += 1;
        }
    }

    // If the palette spans 2 or fewer hue buckets (earth tones, monochrome,
    // all-grey), a clean dark-to-light gradient is more useful than hue grouping.
    // When luminance sort is explicitly requested, always sort by lightness.
    let use_lightness_only =
        pal_sort == crate::db::PaletteSort::Luminance || chromatic_buckets.len() <= 2;

    // When hue-bucket sorting IS used, achromatic colors get the dominant
    // (most populated) hue bucket so they blend in by lightness instead of
    // landing in a random bucket from atan2 noise.
    let dominant_bucket = bucket_counts
        .iter()
        .enumerate()
        .max_by_key(|&(_, cnt)| cnt)
        .map(|(i, _)| i as i32)
        .unwrap_or(0);

    // Total weight for computing fractions
    let total_weight: f32 = unique_centroids.iter().map(|(w, _)| w).sum();
    let total_weight = if total_weight > 0.0 { total_weight } else { 1.0 };

    let mut result: Vec<(egui::Color32, f32)> = Vec::with_capacity(final_k);
    let mut sort_keys: Vec<(i32, u32)> = Vec::with_capacity(final_k);

    for i in 0..final_k {
        let (weight, ref centroid) = unique_centroids[i];
        let l_key = (centroid.l * 1000.0) as u32;

        if use_lightness_only {
            // Pure lightness sort: hue bucket forced to 0 so only l_key matters
            sort_keys.push((0, l_key));
        } else {
            let chroma = (centroid.a.powi(2) + centroid.b.powi(2)).sqrt();
            if chroma < grey_threshold {
                // Achromatic: slot into the dominant hue bucket so it sorts
                // by lightness among its chromatic neighbours
                sort_keys.push((dominant_bucket, l_key));
            } else {
                let mut h = centroid.b.atan2(centroid.a);
                if h < 0.0 {
                    h += std::f32::consts::PI * 2.0;
                }
                let hue_bucket = ((h * 8.0) / (std::f32::consts::PI * 2.0)).round() as i32 % 8;
                sort_keys.push((hue_bucket, l_key));
            }
        }

        let srgb_linear = oklab_to_linear_srgb(*centroid);
        let r = (linear_to_srgb(srgb_linear.r).clamp(0.0, 1.0) * 255.0).round() as u8;
        let g = (linear_to_srgb(srgb_linear.g).clamp(0.0, 1.0) * 255.0).round() as u8;
        let b = (linear_to_srgb(srgb_linear.b).clamp(0.0, 1.0) * 255.0).round() as u8;
        result.push((egui::Color32::from_rgb(r, g, b), weight / total_weight));
    }

    let mut indices: Vec<usize> = (0..final_k).collect();
    indices.sort_by(|&a, &b| {
        let cmp = sort_keys[a].0.cmp(&sort_keys[b].0);
        if cmp == std::cmp::Ordering::Equal { sort_keys[a].1.cmp(&sort_keys[b].1) } else { cmp }
    });

    indices.iter().map(|&i| result[i]).collect()
}

/// Compute histogram and palette from a DynamicImage by thumbnailing to 128x128,
/// converting to Oklab once, then computing both histogram (from L) and palette
/// (via K-means++) from the same buffer.
/// Shared by the disk-based fallback paths (standard images and RAW).
fn compute_histogram_from_dynamic_image(
    img: &image::DynamicImage,
    palette_config: crate::db::PaletteConfig,
) -> ([u32; 256], [u32; 256], [u32; 256], Vec<(egui::Color32, f32)>) {
    let crate::db::PaletteConfig {
        dominant_colors,
        saturation_bias: sat_bias,
        palette_sort: pal_sort,
    } = palette_config;
    let rgba = img.to_rgba8();
    let (src_w, src_h) = rgba.dimensions();
    let (dst_w, dst_h) = (128u32, 128u32);
    let pixel_type = PixelType::U8x4;

    // Detect low-color images before Lanczos downsampling destroys the information.
    // This path always receives original (non-resized) pixels.
    let k = dominant_colors.clamp(1, 25);
    let raw_pixels = rgba.as_raw();
    let total_pixels = (src_w as usize) * (src_h as usize);
    let sample_count = total_pixels.min(4096);
    let step = (total_pixels / sample_count).max(1);
    let mut unique_rgb: std::collections::HashMap<(u8, u8, u8), u32> =
        std::collections::HashMap::new();
    let mut idx = 0;
    while idx < total_pixels && unique_rgb.len() <= k {
        let base = idx * 4;
        *unique_rgb
            .entry((raw_pixels[base], raw_pixels[base + 1], raw_pixels[base + 2]))
            .or_insert(0) += 1;
        idx += step;
    }

    // Build the low-color palette now while raw_pixels is still valid
    // (rgba.into_raw() below will consume it)
    let low_color_palette: Option<Vec<(egui::Color32, f32)>> = if unique_rgb.len() <= k {
        // Re-count with the full sample to get accurate pixel distribution
        // (the first pass may have stopped early once unique count exceeded k)
        unique_rgb.clear();
        let mut full_idx = 0;
        while full_idx < total_pixels {
            let base = full_idx * 4;
            *unique_rgb
                .entry((raw_pixels[base], raw_pixels[base + 1], raw_pixels[base + 2]))
                .or_insert(0) += 1;
            full_idx += step;
        }
        let total_sampled: u32 = unique_rgb.values().sum();
        let mut colors: Vec<(Oklab, egui::Color32, f32)> = unique_rgb
            .iter()
            .map(|(&(r, g, b), &count)| {
                let lr = srgb_to_linear(r as f32 / 255.0);
                let lg = srgb_to_linear(g as f32 / 255.0);
                let lb = srgb_to_linear(b as f32 / 255.0);
                let ok = linear_srgb_to_oklab(LinearRgb { r: lr, g: lg, b: lb });
                (ok, egui::Color32::from_rgb(r, g, b), count as f32 / total_sampled as f32)
            })
            .collect();
        colors.sort_by(|a, b| a.0.l.partial_cmp(&b.0.l).unwrap_or(std::cmp::Ordering::Equal));
        Some(colors.into_iter().map(|(_, c, w)| (c, w)).collect())
    } else {
        None
    };

    // 1. High-quality downsample using fast_image_resize (matches ColorImage path)
    let resized_successfully = FastImage::from_vec_u8(src_w, src_h, rgba.into_raw(), pixel_type)
        .ok()
        .and_then(|src_image| {
            let mut dst_image = FastImage::new(dst_w, dst_h, pixel_type);
            let mut resizer = Resizer::new();

            resizer
                .resize(&src_image, &mut dst_image, &ResizeOptions::default())
                .ok()
                .map(|_| dst_image)
        });

    let oklab_pixels: Vec<Oklab> = if let Some(dst_image) = resized_successfully {
        // Parse the smoothed buffer
        dst_image
            .buffer()
            .chunks_exact(4)
            .map(|chunk| {
                let lr = srgb_to_linear(chunk[0] as f32 / 255.0);
                let lg = srgb_to_linear(chunk[1] as f32 / 255.0);
                let lb = srgb_to_linear(chunk[2] as f32 / 255.0);
                linear_srgb_to_oklab(LinearRgb { r: lr, g: lg, b: lb })
            })
            .collect()
    } else {
        // Fallback: If fast_image_resize fails, use the image crate's high-quality Lanczos3 filter
        // instead of the lower quality thumbnail_exact() method.
        let thumb =
            img.resize_exact(dst_w, dst_h, image::imageops::FilterType::Lanczos3).to_rgba8();
        thumb
            .pixels()
            .map(|p| {
                let lr = srgb_to_linear(p[0] as f32 / 255.0);
                let lg = srgb_to_linear(p[1] as f32 / 255.0);
                let lb = srgb_to_linear(p[2] as f32 / 255.0);
                linear_srgb_to_oklab(LinearRgb { r: lr, g: lg, b: lb })
            })
            .collect()
    };

    let (hist_l, hist_a, hist_b) = build_histograms(&oklab_pixels);

    let palette = low_color_palette
        .unwrap_or_else(|| kmeans_palette(&oklab_pixels, dominant_colors, sat_bias, pal_sort));

    (hist_l, hist_a, hist_b, palette)
}

/// Compute histogram and palette from a standard image file
fn compute_histogram_from_image(
    path: &Path,
    palette_config: crate::db::PaletteConfig,
) -> Option<([u32; 256], [u32; 256], [u32; 256], Vec<(egui::Color32, f32)>)> {
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();

    // Fast path for formats the `image` crate doesn't support natively (PDF, JP2, JXL)
    if matches!(ext.as_str(), "jp2" | "j2k" | "jxl" | "pdf" | "tif" | "tiff") {
        if let Ok(bytes) = std::fs::read(path) {
            match crate::scanner::load_image_fast(path, &bytes) {
                Ok(dyn_img) => {
                    // Explicit return here exits the function early with our data
                    return Some(compute_histogram_from_dynamic_image(&dyn_img, palette_config));
                }
                Err(e) => {
                    // Log the actual error that bubbled up and explicitly return None
                    eprintln!(
                        "[DEBUG-HISTOGRAM] scanner::load_image_fast failed for {:?}: {}",
                        path, e
                    );
                    return None;
                }
            }
        } else {
            eprintln!("[DEBUG-HISTOGRAM] Failed to read bytes for {:?}", path);
            return None;
        }
    }

    // Standard fallback using the `image` crate
    let img = match image::open(path) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("[DEBUG-HISTOGRAM] image::open failed for {:?}: {}", path, e);
            return None;
        }
    };

    Some(compute_histogram_from_dynamic_image(&img, palette_config))
}

/// Compute histogram and palette from a RAW file using rsraw
fn compute_histogram_from_raw(
    path: &Path,
    palette_config: crate::db::PaletteConfig,
) -> Option<([u32; 256], [u32; 256], [u32; 256], Vec<(egui::Color32, f32)>)> {
    let data = match fs::read(path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("[DEBUG-HISTOGRAM] Failed to read RAW file {:?}: {}", path, e);
            return None;
        }
    };

    let mut raw = match rsraw::RawImage::open(&data) {
        Ok(r) => r,
        Err(e) => {
            eprintln!(
                "[DEBUG-HISTOGRAM] rsraw failed to open {:?}: {}. Trying EXIF fallback...",
                path, e
            );
            // Re-use the fallback from the main loader!
            if let Some(color_img) = extract_biggest_exif_preview(path, &data) {
                eprintln!("[DEBUG-HISTOGRAM] EXIF fallback success for {:?}", path);
                return Some(compute_histogram_from_colorimage(&color_img, palette_config, false));
            } else {
                eprintln!("[DEBUG-HISTOGRAM] EXIF fallback also failed for {:?}", path);
                return None;
            }
        }
    };

    // Try to extract thumbnail first (faster)
    match raw.extract_thumbs() {
        Ok(thumbs) => {
            if let Some(best_thumb) = thumbs
                .into_iter()
                .filter(|t| matches!(t.format, rsraw::ThumbFormat::Jpeg))
                .max_by_key(|t| t.width * t.height)
            {
                match image::load_from_memory(&best_thumb.data) {
                    Ok(img) => {
                        return Some(compute_histogram_from_dynamic_image(&img, palette_config));
                    }
                    Err(e) => eprintln!(
                        "[DEBUG-HISTOGRAM] image::load_from_memory failed on rsraw thumb for {:?}: {}",
                        path, e
                    ),
                }
            } else {
                eprintln!(
                    "[DEBUG-HISTOGRAM] No suitable JPEG thumbnail found by rsraw for {:?}",
                    path
                );
            }
        }
        Err(e) => eprintln!("[DEBUG-HISTOGRAM] rsraw.extract_thumbs failed for {:?}: {}", path, e),
    }

    // Fallback: process the full RAW (slower)
    eprintln!("[DEBUG-HISTOGRAM] Falling back to full RAW decode for {:?}", path);
    if let Err(e) = raw.unpack() {
        eprintln!("[DEBUG-HISTOGRAM] rsraw.unpack failed for {:?}: {}", path, e);
        return None;
    }

    raw.set_use_camera_wb(true);
    match raw.process::<{ rsraw::BIT_DEPTH_8 }>() {
        Ok(processed) => {
            let w = raw.width() as usize;
            let h = raw.height() as usize;

            if processed.len() == w * h {
                // Monochrome: construct grayscale DynamicImage
                if let Some(gray_buf) =
                    image::GrayImage::from_raw(w as u32, h as u32, processed.to_vec())
                {
                    return Some(compute_histogram_from_dynamic_image(
                        &image::DynamicImage::ImageLuma8(gray_buf),
                        palette_config,
                    ));
                } else {
                    eprintln!("[DEBUG-HISTOGRAM] Failed to create GrayImage buffer for {:?}", path);
                }
            } else if processed.len() == w * h * 3 {
                // RGB: construct DynamicImage
                if let Some(img_buf) =
                    image::RgbImage::from_raw(w as u32, h as u32, processed.to_vec())
                {
                    return Some(compute_histogram_from_dynamic_image(
                        &image::DynamicImage::ImageRgb8(img_buf),
                        palette_config,
                    ));
                } else {
                    eprintln!("[DEBUG-HISTOGRAM] Failed to create RgbImage buffer for {:?}", path);
                }
            } else {
                eprintln!(
                    "[DEBUG-HISTOGRAM] RAW size mismatch for {:?}: w={}, h={}, len={}",
                    path,
                    w,
                    h,
                    processed.len()
                );
            }
        }
        Err(e) => {
            eprintln!("[DEBUG-HISTOGRAM] rsraw.process failed for {:?}: {}", path, e);
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
    let palette_config = crate::db::PaletteConfig::from_gui_config(&app.ctx.gui_config);
    let histogram_mode = app.histogram_mode;

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

    // Dynamic height: standard grid uses multiple rows, proportional strip uses one row
    let palette_total_height = if histogram_mode == 2 {
        // Proportional strip: single row
        swatch_height + 4.0
    } else {
        // Standard grid: rows of 5
        let num_rows = palette_config.dominant_colors.div_ceil(5);
        (num_rows as f32) * swatch_height + ((num_rows as f32) * 4.0)
    };
    let total_height = hist_height + palette_total_height;
    let padding = 10.0;

    let hist_rect = egui::Rect::from_min_size(
        egui::pos2(available_rect.min.x + padding, available_rect.max.y - total_height - padding),
        egui::vec2(hist_width, hist_height),
    );

    // Check cache first (HashMap keyed by path, populated during preload)
    let histogram_data = app.cached_histogram.get(path).cloned();

    // Fallback: compute from disk if not preloaded (shouldn't happen often)
    let histogram_data = histogram_data.or_else(|| {
        let data = if is_raw_ext(path) {
            compute_histogram_from_raw(path, palette_config)
        } else {
            compute_histogram_from_image(path, palette_config)
        };
        // Cache the result
        if let Some(d) = data {
            let colors_str: Vec<String> =
                d.3.iter()
                    .map(|(c, w)| format!("({}, {}, {} {:.0}%)", c.r(), c.g(), c.b(), w * 100.0))
                    .collect();
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

    if let Some((hist_l, hist_a, hist_b, palette)) = histogram_data {
        // Use a thread-local timer to debounce scroll wheel events so it doesn't flicker wildly
        thread_local! {
            static LAST_HIST_SWITCH: std::cell::RefCell<std::time::Instant> = std::cell::RefCell::new(std::time::Instant::now());
        }

        let response = ui.interact(hist_rect, ui.id().with("hist_scroll"), egui::Sense::hover());
        if response.hovered() {
            let scroll = ui.input(|i| i.smooth_scroll_delta.y);

            if scroll.abs() > 0.1 {
                LAST_HIST_SWITCH.with(|last| {
                    let mut last_mut = last.borrow_mut();
                    // 200ms debounce
                    if last_mut.elapsed().as_secs_f32() > 0.2 {
                        if scroll > 0.0 {
                            app.histogram_channel = (app.histogram_channel + 1) % 3;
                        } else {
                            app.histogram_channel = (app.histogram_channel + 2) % 3;
                        }
                        *last_mut = std::time::Instant::now();
                    }
                });
            }
        }

        let hist_to_draw = match app.histogram_channel {
            1 => &hist_a,
            2 => &hist_b,
            _ => &hist_l,
        };

        draw_histogram(
            ui,
            hist_rect,
            hist_to_draw,
            &palette,
            app.histogram_channel,
            histogram_mode,
        );
    }
}

/// Draw histogram bars and dominant color palette
fn draw_histogram(
    ui: &mut egui::Ui,
    hist_rect: egui::Rect,
    hist: &[u32; 256],
    palette: &[(egui::Color32, f32)],
    channel: usize,
    histogram_mode: u8,
) {
    // Find max value for normalization
    let max_val = hist[1..255].iter().copied().max().unwrap_or(1).max(1);
    let hist_width = hist_rect.width();
    let hist_height = hist_rect.height();

    // 1. Draw Histogram Background
    ui.painter().rect_filled(hist_rect, 0.0, egui::Color32::from_black_alpha(180));

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

        ui.painter().rect_filled(
            egui::Rect::from_min_max(
                egui::pos2(x, y_top),
                egui::pos2(x + bar_width.max(1.0), y_bottom),
            ),
            0.0,
            color,
        );
    }

    ui.painter().rect_stroke(
        hist_rect,
        0.0,
        egui::Stroke::new(1.0, egui::Color32::GRAY),
        egui::StrokeKind::Outside,
    );

    // Draw the active channel label on top of the histogram
    let label = match channel {
        1 => "A",
        2 => "B",
        _ => "L",
    };
    ui.painter().text(
        hist_rect.min + egui::vec2(6.0, 4.0),
        egui::Align2::LEFT_TOP,
        label,
        egui::FontId::new(14.0, egui::FontFamily::Proportional),
        egui::Color32::WHITE,
    );

    // 3. Draw Palette Swatches
    let swatch_height = 16.0;

    if histogram_mode == 2 {
        // Proportional strip: each swatch width is proportional to its pixel weight
        let strip_y = hist_rect.max.y + 4.0;
        let mut x = hist_rect.min.x;

        for &(color, weight) in palette {
            let w = (weight * hist_width).max(1.0);
            let swatch_rect =
                egui::Rect::from_min_size(egui::pos2(x, strip_y), egui::vec2(w, swatch_height));
            ui.painter().rect_filled(swatch_rect, 0.0, color);
            // Allocate the rect for interaction and attach a tooltip
            ui.allocate_rect(swatch_rect, egui::Sense::hover()).on_hover_ui(|tip| {
                tip.horizontal(|tip| {
                    let box_size = swatch_height + 8.0; // 16 + 4px border on each side
                    let (tip_rect, _) = tip
                        .allocate_exact_size(egui::vec2(box_size, box_size), egui::Sense::hover());
                    let border_color = opposite_color(color);
                    let inset = tip_rect.shrink(2.0); // shrink by half the stroke width
                    tip.painter().rect_filled(inset, 0.0, color);
                    tip.painter().rect_stroke(
                        tip_rect,
                        2.0,
                        egui::Stroke::new(4.0, border_color),
                        egui::StrokeKind::Inside,
                    );
                    tip.label(format!(
                        "RGB: {}, {}, {}\n{:.1}%",
                        color.r(),
                        color.g(),
                        color.b(),
                        weight * 100.0
                    ));
                });
            });
            x += w;
        }

        // Border around the full strip
        let strip_rect = egui::Rect::from_min_size(
            egui::pos2(hist_rect.min.x, strip_y),
            egui::vec2(hist_width, swatch_height),
        );
        ui.painter().rect_stroke(
            strip_rect,
            0.0,
            egui::Stroke::new(1.0, egui::Color32::GRAY),
            egui::StrokeKind::Outside,
        );
    } else {
        // Standard grid: rows of 5 equal-width swatches
        let colors_per_row = 5;
        let swatch_width = hist_width / colors_per_row as f32;
        let num_rows = palette.len().div_ceil(colors_per_row);

        for row in 0..num_rows {
            let row_start = row * colors_per_row;
            let row_end = (row_start + colors_per_row).min(palette.len());
            let row_y = hist_rect.max.y + 4.0 + (row as f32) * (swatch_height + 4.0);

            for (i, &(color, weight)) in palette[row_start..row_end].iter().enumerate() {
                let x = hist_rect.min.x + (i as f32) * swatch_width;

                let swatch_rect = egui::Rect::from_min_size(
                    egui::pos2(x, row_y),
                    egui::vec2(swatch_width, swatch_height),
                );

                ui.painter().rect_filled(swatch_rect, 0.0, color);

                // Allocate the rect for interaction and attach a tooltip
                ui.allocate_rect(swatch_rect, egui::Sense::hover()).on_hover_ui(|tip| {
                    tip.horizontal(|tip| {
                        let box_size = swatch_height + 8.0; // 16 + 4px border on each side
                        let (tip_rect, _) = tip.allocate_exact_size(
                            egui::vec2(box_size, box_size),
                            egui::Sense::hover(),
                        );
                        let border_color = opposite_color(color);
                        let inset = tip_rect.shrink(2.0); // shrink by half the stroke width
                        tip.painter().rect_filled(inset, 0.0, color);
                        tip.painter().rect_stroke(
                            tip_rect,
                            2.0,
                            egui::Stroke::new(4.0, border_color),
                            egui::StrokeKind::Inside,
                        );
                        tip.label(format!(
                            "RGB: {}, {}, {}\n{:.1}%",
                            color.r(),
                            color.g(),
                            color.b(),
                            weight * 100.0
                        ));
                    });
                });
            }

            // Draw a border encompassing this row's color strip
            let row_color_count = row_end - row_start;
            let row_strip_width = row_color_count as f32 * swatch_width;
            let palette_rect = egui::Rect::from_min_size(
                egui::pos2(hist_rect.min.x, row_y),
                egui::vec2(row_strip_width, swatch_height),
            );

            ui.painter().rect_stroke(
                palette_rect,
                0.0,
                egui::Stroke::new(1.0, egui::Color32::GRAY),
                egui::StrokeKind::Outside,
            );
        }
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
    let exif_x = if app.histogram_mode > 0 {
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
