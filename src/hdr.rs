//! HDR → SDR conversion for still images.
//!
//! Handles PNG `cICP` (Coding-Independent Code Points) metadata so that HDR
//! images carrying BT.2020 primaries + PQ (SMPTE 2084) or HLG (ARIB STD-B67)
//! transfer functions are tone-mapped and gamut-converted down to 8-bit sRGB
//! for display, instead of being silently truncated to 8 bits per channel as
//! if they were already sRGB-encoded (which is what makes HDR PNGs look dim
//! and desaturated in the default `image::DynamicImage::to_rgba8()` path).
//!
//! Pipeline (applied per pixel, in f32, in this order):
//!   1. Normalize 16-bit code values to [0, 1].
//!   2. Apply the inverse transfer function (PQ or HLG) to get linear light
//!      in nits. PQ peak is 10000 nits; HLG is system-gamma-corrected.
//!   3. Convert BT.2020 primaries → BT.709 primaries via a 3x3 matrix in
//!      linear light.
//!   4. Tone-map HDR (up to 10000 nits) → SDR peak (user-configurable, in
//!      nits) using the ITU-R BT.2390 EETF. This is the algorithm mpv uses
//!      by default for HDR→SDR.
//!   5. Normalize to [0, 1] relative to SDR peak.
//!   6. Apply the sRGB OETF (gamma encoding).
//!   7. Quantize to u8 or Rgb10a2.

use crate::img_debug;
use image::{DynamicImage, ImageBuffer, ImageDecoder, Rgba, RgbaImage};
use rayon::prelude::*;

// ---------------------------------------------------------------------------
// cICP detection
// ---------------------------------------------------------------------------

/// ITU-T H.273 Coding-Independent Code Points. The four bytes of the PNG
/// `cICP` chunk (and equivalent metadata in AVIF, HEIF, MKV, etc.).
#[derive(Debug, Clone, Copy)]
pub struct Cicp {
    /// H.273 Table 2 — e.g. 1 = BT.709, 9 = BT.2020.
    pub color_primaries: u8,
    /// H.273 Table 3 — e.g. 13 = sRGB, 16 = SMPTE 2084 (PQ), 18 = ARIB HLG.
    pub transfer_characteristics: u8,
    /// H.273 Table 4 — e.g. 0 = identity/RGB, 1 = BT.709, 9 = BT.2020 NCL.
    pub matrix_coefficients: u8,
    /// 1 = full-range (PC), 0 = limited-range (TV).
    pub full_range: bool,
}

impl Cicp {
    /// True if the image needs HDR→SDR processing (PQ or HLG transfer).
    pub fn is_hdr(&self) -> bool {
        matches!(self.transfer_characteristics, 16 | 18)
    }
}

/// Extract `cICP` from ISOBMFF (AVIF/HEIC) files.
/// Uses an ultra-fast byte-scanner to sidestep container parser failures,
/// falling back to `mp4parse` only as a last resort.
fn detect_cicp_isobmff(bytes: &[u8]) -> Option<Cicp> {
    // Restrict the scan to the first 256 KiB
    let search_limit = bytes.len().min(1024 * 256);
    let haystack = &bytes[..search_limit];

    // 1. Fast-scan for NCLX (cICP metadata)
    // Layout: [size: 4] c o l r n c l x [prim: 2] [trans: 2] [matrix: 2] [flags: 1]
    if let Some(pos) = haystack.windows(8).position(|w| w == b"colrnclx")
        && pos + 15 <= haystack.len()
    {
        let primaries = u16::from_be_bytes([haystack[pos + 8], haystack[pos + 9]]);
        let transfer = u16::from_be_bytes([haystack[pos + 10], haystack[pos + 11]]);
        let matrix = u16::from_be_bytes([haystack[pos + 12], haystack[pos + 13]]);
        let flags = haystack[pos + 14];
        let full_range = (flags & 0x80) != 0;

        let tc = transfer as u8;
        // H.273: 2 = Unspecified, 0 = Reserved
        if tc != 2 && tc != 0 {
            let cicp = Cicp {
                color_primaries: primaries as u8,
                transfer_characteristics: tc,
                matrix_coefficients: matrix as u8,
                full_range,
            };
            img_debug!("[DEBUG-CICP] Fast-scan successfully extracted NCLX: {:?}", cicp);
            return Some(cicp);
        } else {
            img_debug!("[DEBUG-CICP] NCLX transfer is Unspecified. Looking for ICC profile...");
        }
    }

    // 2. Fast-scan for embedded ICC profiles ('prof' or 'rICC')
    // Layout: [size: 4] c o l r p r o f [ICC bytes...]
    for icc_magic in [b"colrprof", b"colrrICC"] {
        if let Some(pos) = haystack.windows(8).position(|w| w == icc_magic) {
            // Ensure we can safely read the 4-byte box size preceding 'colr'
            if pos >= 4 {
                let box_size = u32::from_be_bytes([
                    haystack[pos - 4],
                    haystack[pos - 3],
                    haystack[pos - 2],
                    haystack[pos - 1],
                ]) as usize;

                // Normal box sizes include the 12-byte header.
                // A size of 1 indicates a 64-bit extended size, which we skip.
                if box_size > 12 && (pos - 4 + box_size) <= bytes.len() {
                    let icc_start = pos + 8;
                    let icc_end = pos - 4 + box_size;
                    let icc_bytes = &bytes[icc_start..icc_end];

                    img_debug!(
                        "[DEBUG-CICP] Fast-scan extracted ICC profile ({} bytes)",
                        icc_bytes.len()
                    );
                    if let Some(cicp) = detect_cicp_icc(icc_bytes) {
                        return Some(cicp);
                    }
                }
            }
        }
    }

    // 3. Last-ditch fallback: Let the strict parser attempt to read it
    let mut cursor = std::io::Cursor::new(bytes);
    if let Ok(ctx) = mp4parse::read_avif(&mut cursor, mp4parse::ParseStrictness::Permissive)
        && let Some(Ok(icc_bytes)) = ctx.icc_colour_information()
    {
        img_debug!(
            "[DEBUG-CICP] mp4parse fallback extracted ICC profile ({} bytes)",
            icc_bytes.len()
        );
        if let Some(cicp) = detect_cicp_icc(icc_bytes) {
            return Some(cicp);
        }
    }

    None
}

/// Dispatcher: detect cICP values from any supported still-image byte stream.
pub fn detect_cicp(bytes: &[u8]) -> Option<Cicp> {
    if bytes.len() < 12 {
        img_debug!("[DEBUG-CICP] File too small ({} bytes)", bytes.len());
        return None;
    }

    let _brand = std::str::from_utf8(&bytes[4..8]).unwrap_or("????");
    img_debug!("[DEBUG-CICP] File signature: {}", _brand);

    // 1. Check for ISOBMFF (AVIF / HEIC).
    if &bytes[4..8] == b"ftyp" {
        img_debug!("[DEBUG-CICP] Routing to ISOBMFF parser...");
        if let Some(c) = detect_cicp_isobmff(bytes) {
            return Some(c);
        }
        img_debug!("[DEBUG-CICP] ISOBMFF parser returned None. Falling through...");
    }

    // 2. PNG signature: 89 50 4E 47
    if bytes[0..4] == [0x89, 0x50, 0x4E, 0x47]
        && let Some(c) = detect_cicp_png(bytes)
    {
        return Some(c);
    }

    // 3. Fallback (JPEG, WebP, etc.)
    detect_cicp_from_icc_profile(bytes)
}

/// Scan a PNG byte stream for the `cICP` chunk. Returns `None` if the file
/// is not a PNG or the chunk is absent. Reads only as far as necessary —
/// stops at the first `IDAT` (image data) chunk.
///
/// The PNG `cICP` chunk is a dedicated PNG-only chunk, not an ICC profile —
/// so `image::ImageDecoder::icc_profile` does not surface it. PNGs with an
/// `iCCP` (ICC profile) chunk instead are picked up by the ICC path below.
pub fn detect_cicp_png(bytes: &[u8]) -> Option<Cicp> {
    // PNG signature: 8 bytes.
    const PNG_SIG: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];
    if bytes.len() < 8 || bytes[..8] != PNG_SIG {
        return None;
    }

    let mut i: usize = 8;
    while i + 8 <= bytes.len() {
        let len = u32::from_be_bytes([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]]) as usize;
        let chunk_type = &bytes[i + 4..i + 8];
        let data_start = i + 8;
        let data_end = data_start.checked_add(len)?;
        // +4 for the CRC that follows.
        let next = data_end.checked_add(4)?;
        if next > bytes.len() {
            return None;
        }

        match chunk_type {
            b"cICP" if len == 4 => {
                return Some(Cicp {
                    color_primaries: bytes[data_start],
                    transfer_characteristics: bytes[data_start + 1],
                    matrix_coefficients: bytes[data_start + 2],
                    full_range: bytes[data_start + 3] != 0,
                });
            }
            // IDAT marks start of image data; cICP is always before it.
            b"IDAT" => return None,
            // IEND is the terminating chunk.
            b"IEND" => return None,
            _ => {}
        }

        i = next;
    }
    None
}

/// Detect cICP from an embedded ICC profile by asking the image crate's
/// decoders to extract it. `ImageDecoder::icc_profile` handles the format-
/// specific packaging for free — JPEG APP2 `ICC_PROFILE` segment reassembly,
/// PNG `iCCP` zlib decompression, WebP `ICCP` chunks, TIFF tag 34675, etc.
/// Returns `None` if the format isn't supported, the file has no ICC
/// profile, or the profile lacks a v4.4 `cicp` tag.
fn detect_cicp_from_icc_profile(bytes: &[u8]) -> Option<Cicp> {
    // Dispatch to a concrete decoder based on magic bytes. Using the
    // specific decoder types (instead of the generic `ImageReader` path)
    // avoids any lifetime gymnastics around borrowed byte slices and keeps
    // the set of supported formats explicit.
    let icc: Vec<u8> = match image::guess_format(bytes).ok()? {
        image::ImageFormat::Jpeg => {
            let mut d = image::codecs::jpeg::JpegDecoder::new(std::io::Cursor::new(bytes)).ok()?;
            d.icc_profile().ok().flatten()?
        }
        image::ImageFormat::Png => {
            let mut d = image::codecs::png::PngDecoder::new(std::io::Cursor::new(bytes)).ok()?;
            d.icc_profile().ok().flatten()?
        }
        _ => return None,
    };
    detect_cicp_icc(&icc)
}

/// Extract cICP values from a reassembled ICC profile buffer. Looks up the
/// v4.4 `cicp` tag in the tag table and decodes its 12-byte body:
///
/// ```text
///   4 bytes   signature 'cicp'
///   4 bytes   reserved (0)
///   1 byte    ColourPrimaries
///   1 byte    TransferCharacteristics
///   1 byte    MatrixCoefficients
///   1 byte    VideoFullRangeFlag
/// ```
fn detect_cicp_icc(icc: &[u8]) -> Option<Cicp> {
    // ICC layout: 128-byte header, then u32 tag count, then (tag_count * 12)
    // bytes of tag table entries.
    if icc.len() < 132 {
        return None;
    }
    let tag_count = u32::from_be_bytes([icc[128], icc[129], icc[130], icc[131]]) as usize;
    let tag_table_start = 132usize;
    let tag_table_end = tag_table_start.checked_add(tag_count.checked_mul(12)?)?;
    if tag_table_end > icc.len() {
        return None;
    }

    for idx in 0..tag_count {
        let entry = tag_table_start + idx * 12;
        if &icc[entry..entry + 4] != b"cicp" {
            continue;
        }
        let offset =
            u32::from_be_bytes([icc[entry + 4], icc[entry + 5], icc[entry + 6], icc[entry + 7]])
                as usize;
        let size =
            u32::from_be_bytes([icc[entry + 8], icc[entry + 9], icc[entry + 10], icc[entry + 11]])
                as usize;
        let end = offset.checked_add(size)?;
        if end > icc.len() || size < 12 {
            return None;
        }
        let data = &icc[offset..end];
        if &data[0..4] != b"cicp" {
            return None;
        }
        return Some(Cicp {
            color_primaries: data[8],
            transfer_characteristics: data[9],
            matrix_coefficients: data[10],
            full_range: data[11] != 0,
        });
    }
    None
}

// ---------------------------------------------------------------------------
// Transfer functions
// ---------------------------------------------------------------------------

// PQ (SMPTE ST 2084) constants.
const PQ_M1: f32 = 2610.0 / 16384.0;
const PQ_M2: f32 = 2523.0 / 4096.0 * 128.0;
const PQ_C1: f32 = 3424.0 / 4096.0;
const PQ_C2: f32 = 2413.0 / 4096.0 * 32.0;
const PQ_C3: f32 = 2392.0 / 4096.0 * 32.0;

/// Inverse PQ EOTF: non-linear PQ code value in [0,1] → linear light in nits
/// (0 to 10000 nits).
#[inline]
fn pq_eotf(e: f32) -> f32 {
    let e = e.max(0.0);
    let ep = e.powf(1.0 / PQ_M2);
    let num = (ep - PQ_C1).max(0.0);
    let den = PQ_C2 - PQ_C3 * ep;
    let y = (num / den).powf(1.0 / PQ_M1);
    // 10000 nits peak by definition.
    y * 10000.0
}

/// HLG inverse OETF → scene-linear in [0,1], then apply system OOTF to get
/// display-referred linear light in nits. This is the "system gamma" path
/// from ITU-R BT.2100.
#[inline]
fn hlg_eotf(e: f32, peak_nits: f32) -> f32 {
    // This is only called componentwise; the full per-pixel version below
    // applies the system gamma using the scene luma. For channel-wise use
    // we fall back to treating each channel independently (acceptable near
    // grayscale but not ideal for saturated colors). Kept for completeness.
    const A: f32 = 0.17883277;
    const B: f32 = 1.0 - 4.0 * A; // 0.28466892
    const C: f32 = 0.559_910_7; // 0.5 - A * ln(4.0 * A)
    let e = e.max(0.0);
    let scene = if e <= 0.5 { (e * e) / 3.0 } else { (((e - C) / A).exp() + B) / 12.0 };
    // System gamma for nominal 1000 nit display.
    let gamma = 1.2 + 0.42 * (peak_nits / 1000.0).log10();
    scene.powf(gamma) * peak_nits
}

// sRGB OETF (linear → non-linear, [0,1] → [0,1]).
#[inline]
fn srgb_oetf(v: f32) -> f32 {
    let v = v.max(0.0).min(1.0);
    if v <= 0.0031308 { 12.92 * v } else { 1.055 * v.powf(1.0 / 2.4) - 0.055 }
}

// ---------------------------------------------------------------------------
// Primaries conversion: BT.2020 linear → BT.709 linear
// ---------------------------------------------------------------------------

// Derived from the standard BT.2020 and BT.709 primaries + D65 white point.
// Source: ITU-R BT.2087 / commonly reproduced in e.g. libplacebo.
const BT2020_TO_BT709: [[f32; 3]; 3] = [
    [1.660_491, -0.587_641, -0.072_850],
    [-0.124_550, 1.132_9, -0.008_349],
    [-0.018_151, -0.100_579, 1.118_73],
];

#[inline]
fn bt2020_to_bt709_linear(r: f32, g: f32, b: f32) -> (f32, f32, f32) {
    let m = &BT2020_TO_BT709;
    (
        m[0][0] * r + m[0][1] * g + m[0][2] * b,
        m[1][0] * r + m[1][1] * g + m[1][2] * b,
        m[2][0] * r + m[2][1] * g + m[2][2] * b,
    )
}

// ---------------------------------------------------------------------------
// Tone mapping: ITU-R BT.2390 EETF
// ---------------------------------------------------------------------------

/// ITU-R BT.2390 tone-mapping EETF applied to PQ-encoded luma.
///
/// Input `e` is already PQ-encoded in [0,1]. `src_peak_pq` and `dst_peak_pq`
/// are the source and destination peaks expressed as PQ code values in [0,1]
/// (i.e. the PQ of the peak nits). This operator is applied to the MaxRGB
/// component of each pixel, and then the three channels are scaled by the
/// resulting ratio — this preserves hue better than per-channel tone mapping.
#[inline]
fn bt2390_eetf(e: f32, src_peak_pq: f32, dst_peak_pq: f32) -> f32 {
    // Normalize input to [0, src_peak_pq] -> [0, 1].
    let e1 = (e / src_peak_pq).clamp(0.0, 1.0);
    let max_lum = dst_peak_pq / src_peak_pq;

    // Knee point. BT.2390 uses ks = 1.5 * max_lum - 0.5.
    let ks = 1.5 * max_lum - 0.5;

    let e2 = if e1 < ks {
        e1
    } else {
        // Hermite spline segment.
        let t = (e1 - ks) / (1.0 - ks);
        let t2 = t * t;
        let t3 = t2 * t;
        (2.0 * t3 - 3.0 * t2 + 1.0) * ks
            + (t3 - 2.0 * t2 + t) * (1.0 - ks)
            + (-2.0 * t3 + 3.0 * t2) * max_lum
    };

    // Scale back.
    (e2 * src_peak_pq).clamp(0.0, 1.0)
}

/// Inverse of the PQ EOTF for a value in nits. Returns PQ code value in [0,1].
#[inline]
fn pq_inverse_eotf_nits(nits: f32) -> f32 {
    let y = (nits / 10000.0).clamp(0.0, 1.0);
    let ym = y.powf(PQ_M1);
    let num = PQ_C1 + PQ_C2 * ym;
    let den = 1.0 + PQ_C3 * ym;
    (num / den).powf(PQ_M2)
}

// ---------------------------------------------------------------------------
// Main conversion
// ---------------------------------------------------------------------------

/// Per-image constants shared by every pixel, derived once from the cICP
/// signalling and the target display peak.
#[derive(Debug, Clone, Copy)]
struct TonemapParams {
    use_pq: bool,
    use_hlg: bool,
    /// BT.2020 primaries, needing conversion to BT.709.
    wide_gamut: bool,
    sdr_peak_nits: f32,
    src_peak_pq: f32,
    dst_peak_pq: f32,
}

impl TonemapParams {
    fn new(cicp: Cicp, sdr_peak_nits: f32) -> Self {
        // Source peak. For PQ we assume full 10000 nits unless we had MaxCLL
        // (which PNG doesn't carry) — BT.2390 copes fine with this.
        let src_peak_nits: f32 = 10000.0;
        Self {
            use_pq: cicp.transfer_characteristics == 16,
            use_hlg: cicp.transfer_characteristics == 18,
            wide_gamut: cicp.color_primaries == 9, // BT.2020
            sdr_peak_nits,
            src_peak_pq: pq_inverse_eotf_nits(src_peak_nits),
            dst_peak_pq: pq_inverse_eotf_nits(sdr_peak_nits),
        }
    }
}

/// Flatten any `DynamicImage` into 16-bit RGBA. Everything goes through the
/// same math afterwards regardless of the source variant.
fn to_rgba16_flat(img: &DynamicImage) -> (u32, u32, Vec<u16>) {
    match img {
        DynamicImage::ImageRgb16(buf) => {
            let (w, h) = (buf.width(), buf.height());
            let mut out = Vec::with_capacity((w * h * 4) as usize);
            for p in buf.pixels() {
                out.push(p[0]);
                out.push(p[1]);
                out.push(p[2]);
                out.push(u16::MAX);
            }
            (w, h, out)
        }
        DynamicImage::ImageRgba16(buf) => {
            let (w, h) = (buf.width(), buf.height());
            (w, h, buf.as_raw().to_vec())
        }
        // For 8-bit or other formats we still do the math if cICP says HDR
        // (unusual but possible): promote to 16-bit first.
        other => {
            let rgba16 = other.to_rgba16();
            let (w, h) = (rgba16.width(), rgba16.height());
            (w, h, rgba16.into_raw())
        }
    }
}

/// Tone-map one source pixel to display-encoded sRGB in [0, 1].
///
/// This is steps 1-6 of the pipeline described at the top of this module.
/// Quantization (step 7) is left to the caller so the same math can feed
/// both the 8-bit and the 10-bit output paths.
#[inline]
fn tonemap_px(src: &[u16], p: TonemapParams) -> (f32, f32, f32, f32) {
    // Normalize to [0,1].
    let mut r = src[0] as f32 / 65535.0;
    let mut g = src[1] as f32 / 65535.0;
    let mut b = src[2] as f32 / 65535.0;
    let a = src[3] as f32 / 65535.0;

    // 1. EOTF → linear light in nits.
    if p.use_pq {
        r = pq_eotf(r);
        g = pq_eotf(g);
        b = pq_eotf(b);
    } else if p.use_hlg {
        // Peak assumption of 1000 nits is a sensible default for HLG.
        r = hlg_eotf(r, 1000.0);
        g = hlg_eotf(g, 1000.0);
        b = hlg_eotf(b, 1000.0);
    } else {
        // sRGB / BT.709 — no HDR processing; just treat as normal.
        r = srgb_to_linear_simple(r) * p.sdr_peak_nits;
        g = srgb_to_linear_simple(g) * p.sdr_peak_nits;
        b = srgb_to_linear_simple(b) * p.sdr_peak_nits;
    }

    // 2. Primaries: BT.2020 → BT.709 in linear light.
    if p.wide_gamut {
        let (nr, ng, nb) = bt2020_to_bt709_linear(r, g, b);
        r = nr;
        g = ng;
        b = nb;
    }

    // 3. Tone map via BT.2390 on MaxRGB, then scale channels.
    // Convert each channel back to PQ space, take max, run EETF, and
    // apply the resulting ratio to the linear-light triple. This
    // preserves hue reasonably well.
    let rp = pq_inverse_eotf_nits(r.max(0.0));
    let gp = pq_inverse_eotf_nits(g.max(0.0));
    let bp = pq_inverse_eotf_nits(b.max(0.0));
    let max_pq = rp.max(gp).max(bp);
    let mapped_pq = bt2390_eetf(max_pq, p.src_peak_pq, p.dst_peak_pq);
    let scale = if max_pq > 1e-6 {
        // Work out the linear-light ratio that corresponds to the PQ
        // compression we just applied on the max channel.
        let before = pq_eotf(max_pq);
        let after = pq_eotf(mapped_pq);
        if before > 1e-6 { after / before } else { 0.0 }
    } else {
        1.0
    };
    r *= scale;
    g *= scale;
    b *= scale;

    // 4. Normalize to SDR display range [0,1].
    let inv = 1.0 / p.sdr_peak_nits;
    r = (r * inv).clamp(0.0, 1.0);
    g = (g * inv).clamp(0.0, 1.0);
    b = (b * inv).clamp(0.0, 1.0);

    // 5. sRGB OETF (gamma encoding).
    (srgb_oetf(r), srgb_oetf(g), srgb_oetf(b), a.clamp(0.0, 1.0))
}

const NOISE_DATA: &[u8] = include_bytes!("../assets/blue-noise-256.bin");
const NOISE_DATA_WIDTH_AND_HEIGHT: usize = 256;
/// Get the noise value at the given coordinates. If the coordinates are out of bounds,
/// they will wrap around. Means we don't need a noise texture as large as the image.
#[inline]
fn get_noise(x: u32, y: u32) -> u8 {
    let wrap_x = (x as usize) % NOISE_DATA_WIDTH_AND_HEIGHT;
    let wrap_y = (y as usize) % NOISE_DATA_WIDTH_AND_HEIGHT;
    NOISE_DATA[wrap_y * NOISE_DATA_WIDTH_AND_HEIGHT + wrap_x]
}

const _: () =
    assert!(NOISE_DATA.len() == NOISE_DATA_WIDTH_AND_HEIGHT * NOISE_DATA_WIDTH_AND_HEIGHT);

/// Blue-noise dither offset for the pixel at (x, y), in LSB units.
/// The stored field is uniformly distributed. Remapping it through the inverse
/// triangular CDF gives a triangular PDF over (-1, 1) — that is what makes the
/// quantization error independent of the signal, where a plain uniform offset
/// leaves a noise floor that pumps with input level. The remap is monotonic, so
/// it preserves the field's rank ordering and therefore its blue spectrum.
///
/// One offset is shared by R, G and B: correlated dither reads as monochrome
/// grain, which is much less objectionable than the chroma speckle you get from
/// three independent draws.
#[inline]
fn blue_noise_dither(x: u32, y: u32) -> f32 {
    // Map the byte to (0, 1), symmetric about 0.5. The +0.5 balances the two
    // halves of the remap and keeps the sqrt away from zero at the extremes.
    let u = (get_noise(x, y) as f32 + 0.5) / 256.0;
    if u < 0.5 { (2.0 * u).sqrt() - 1.0 } else { 1.0 - (2.0 - 2.0 * u).sqrt() }
}

/// Convert an HDR `DynamicImage` (expected to be 16-bit RGB or RGBA) with
/// the given cICP signalling into an 8-bit sRGB RgbaImage ready for display.
///
/// `sdr_peak_nits` is the target display peak. Common values:
///   * 100.0 — strict SDR reference (classic mpv default).
///   * 203.0 — HDR reference white per ITU-R BT.2408; produces a brighter,
///     more pleasant mapping on typical desktop monitors.
///
/// Output is dithered: the tone curve packs 10000 nits into 256 codes, so
/// plain rounding leaves visible banding across skies and gradients.
pub fn process_hdr_to_sdr(img: &DynamicImage, cicp: Cicp, sdr_peak_nits: f32) -> RgbaImage {
    let params = TonemapParams::new(cicp, sdr_peak_nits);
    let (width, height, raw_rgba_u16) = to_rgba16_flat(img);

    let pixel_count = (width as usize) * (height as usize);
    let mut out: Vec<u8> = vec![0u8; pixel_count * 4];
    let w = width as usize;
    raw_rgba_u16.par_chunks_exact(4).zip(out.par_chunks_exact_mut(4)).enumerate().for_each(
        |(i, (src, dst))| {
            let (r, g, b, a) = tonemap_px(src, params);
            let d = blue_noise_dither((i % w) as u32, (i / w) as u32);
            let q = |v: f32| (v * 255.0 + d).round().clamp(0.0, 255.0) as u8;
            dst[0] = q(r);
            dst[1] = q(g);
            dst[2] = q(b);
            dst[3] = (a * 255.0).round() as u8;
        },
    );

    ImageBuffer::<Rgba<u8>, _>::from_raw(width, height, out).unwrap_or_else(|| {
        // Extremely defensive: should never happen since length is exact.
        ImageBuffer::new(width, height)
    })
}

/// As `process_hdr_to_sdr`, but packed for `wgpu::TextureFormat::Rgb10a2Unorm`
/// (Vulkan `A2B10G10R10_UNORM_PACK32`): a little-endian u32 laid out as
/// `(a << 30) | (b << 20) | (g << 10) | r`.
///
/// Same 4 bytes per pixel as the 8-bit path, so cache memory is unchanged.
/// Alpha is only 2 bits — fine for PQ/HLG stills, which are opaque, but do
/// not route images with real transparency through this.
///
/// Returns `(width, height, packed)`. Use `bytemuck::cast_slice(&packed)` for
/// `wgpu::Queue::write_texture`, with `bytes_per_row = width * 4`.
pub fn process_hdr_to_rgb10a2(
    img: &DynamicImage,
    cicp: Cicp,
    sdr_peak_nits: f32,
) -> (u32, u32, Vec<u32>) {
    let params = TonemapParams::new(cicp, sdr_peak_nits);
    let (width, height, raw_rgba_u16) = to_rgba16_flat(img);

    let mut out: Vec<u32> = vec![0u32; (width as usize) * (height as usize)];
    let w = width as usize;
    raw_rgba_u16.par_chunks_exact(4).zip(out.par_iter_mut()).enumerate().for_each(
        |(i, (src, dst))| {
            let (r, g, b, a) = tonemap_px(src, params);
            // 1023 codes is close to the visible threshold, but the BT.2390
            // curve is steep in the shadows, so dither still earns its keep.
            let d = blue_noise_dither((i % w) as u32, (i / w) as u32);
            let q = |v: f32| (v * 1023.0 + d).round().clamp(0.0, 1023.0) as u32;
            *dst = (((a * 3.0).round() as u32) << 30) | (q(b) << 20) | (q(g) << 10) | q(r);
        },
    );

    (width, height, out)
}

/// Requantize a 16-bit sRGB image to packed Rgb10a2Unorm with blue-noise dither.
/// No transfer or gamut conversion: source and target are both sRGB-encoded.
pub fn requantize_srgb16_to_rgb10a2(img: &DynamicImage) -> (u32, u32, Vec<u32>) {
    let (width, height, raw) = to_rgba16_flat(img);
    let w = width as usize;
    let mut out: Vec<u32> = vec![0u32; w * (height as usize)];

    raw.par_chunks_exact(4).zip(out.par_iter_mut()).enumerate().for_each(|(i, (src, dst))| {
        let d = blue_noise_dither((i % w) as u32, (i / w) as u32);
        let q = |v: u16| ((v as f32 / 65535.0) * 1023.0 + d).round().clamp(0.0, 1023.0) as u32;
        let a = (src[3] as f32 / 65535.0 * 3.0).round() as u32;
        *dst = (a << 30) | (q(src[2]) << 20) | (q(src[1]) << 10) | q(src[0]);
    });

    (width, height, out)
}

#[inline]
fn srgb_to_linear_simple(v: f32) -> f32 {
    if v <= 0.04045 { v / 12.92 } else { ((v + 0.055) / 1.055).powf(2.4) }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pq_eotf_landmarks() {
        // PQ(0) = 0 nits, PQ(1) = 10000 nits.
        assert!((pq_eotf(0.0)).abs() < 1e-3);
        assert!((pq_eotf(1.0) - 10000.0).abs() < 1.0);
        // Round-trip.
        for &n in &[1.0_f32, 100.0, 203.0, 1000.0, 4000.0] {
            let pq = pq_inverse_eotf_nits(n);
            let back = pq_eotf(pq);
            assert!((back - n).abs() / n < 1e-3, "round-trip {} -> {}", n, back);
        }
    }

    #[test]
    fn cicp_chunk_parse() {
        // Minimal PNG with only signature + IHDR + cICP + IEND.
        // We fake the chunks; parser doesn't check CRCs.
        let mut b = Vec::<u8>::new();
        b.extend_from_slice(&[137, 80, 78, 71, 13, 10, 26, 10]);
        // IHDR (13 bytes), content arbitrary
        b.extend_from_slice(&13u32.to_be_bytes());
        b.extend_from_slice(b"IHDR");
        b.extend_from_slice(&[0u8; 13]);
        b.extend_from_slice(&[0u8; 4]); // fake CRC
        // cICP: 9 (BT.2020), 16 (PQ), 0 (identity), 1 (full)
        b.extend_from_slice(&4u32.to_be_bytes());
        b.extend_from_slice(b"cICP");
        b.extend_from_slice(&[9, 16, 0, 1]);
        b.extend_from_slice(&[0u8; 4]); // fake CRC
        // IEND
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(b"IEND");
        b.extend_from_slice(&[0u8; 4]);

        let c = detect_cicp_png(&b).expect("cICP found");
        assert_eq!(c.color_primaries, 9);
        assert_eq!(c.transfer_characteristics, 16);
        assert_eq!(c.matrix_coefficients, 0);
        assert!(c.full_range);
        assert!(c.is_hdr());
    }

    #[test]
    fn cicp_icc_profile_parse() {
        // Build a minimal ICC profile containing only a cicp tag, and feed
        // it directly to the ICC parser. (The previous version of this test
        // wrapped the profile in a handcrafted APP2-only JPEG, but now that
        // we rely on the image crate's `icc_profile()`, the JPEG has to be
        // a real decodable image — which is cumbersome to synthesize. The
        // ICC parser itself is format-agnostic, so test it directly.)
        let mut icc = vec![0u8; 128]; // empty header
        let tag_count: u32 = 1;
        icc.extend_from_slice(&tag_count.to_be_bytes());
        // Single tag table entry pointing to the cicp data.
        let cicp_data_offset: u32 = 128 + 4 + 12; // header + count + one entry
        let cicp_data_size: u32 = 12;
        icc.extend_from_slice(b"cicp");
        icc.extend_from_slice(&cicp_data_offset.to_be_bytes());
        icc.extend_from_slice(&cicp_data_size.to_be_bytes());
        // cicpType body
        icc.extend_from_slice(b"cicp"); // signature
        icc.extend_from_slice(&[0u8; 4]); // reserved
        icc.push(9); //  BT.2020 primaries
        icc.push(18); // HLG transfer
        icc.push(0); //  identity matrix
        icc.push(1); //  full range

        let c = detect_cicp_icc(&icc).expect("cicp from ICC profile");
        assert_eq!(c.color_primaries, 9);
        assert_eq!(c.transfer_characteristics, 18);
        assert_eq!(c.matrix_coefficients, 0);
        assert!(c.full_range);
        assert!(c.is_hdr());
    }
}
