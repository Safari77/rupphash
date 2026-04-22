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
//!   7. Quantize to u8.

use image::{DynamicImage, ImageBuffer, Rgba, RgbaImage};
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

/// Scan a PNG byte stream for the `cICP` chunk. Returns `None` if the file
/// is not a PNG or the chunk is absent. Reads only as far as necessary —
/// stops at the first `IDAT` (image data) chunk.
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
    const C: f32 = 0.55991073; // 0.5 - A * ln(4.0 * A)
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
    [-0.124_550, 1.132_900, -0.008_349],
    [-0.018_151, -0.100_579, 1.118_730],
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

/// Convert an HDR `DynamicImage` (expected to be 16-bit RGB or RGBA) with
/// the given cICP signalling into an 8-bit sRGB RgbaImage ready for display.
///
/// `sdr_peak_nits` is the target display peak. Common values:
///   * 100.0 — strict SDR reference (classic mpv default).
///   * 203.0 — HDR reference white per ITU-R BT.2408; produces a brighter,
///     more pleasant mapping on typical desktop monitors.
pub fn process_hdr_to_sdr(img: &DynamicImage, cicp: Cicp, sdr_peak_nits: f32) -> RgbaImage {
    // Source peak. For PQ we assume full 10000 nits unless we had MaxCLL
    // (which PNG doesn't carry) — BT.2390 copes fine with this.
    let src_peak_nits: f32 = 10000.0;
    let src_peak_pq = pq_inverse_eotf_nits(src_peak_nits);
    let dst_peak_pq = pq_inverse_eotf_nits(sdr_peak_nits);

    // Extract a flat [R,G,B,A] f32 iterator depending on the concrete image
    // variant. Everything goes through the same math afterwards.
    let (width, height, raw_rgba_u16): (u32, u32, Vec<u16>) = match img {
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
    };

    let use_pq = cicp.transfer_characteristics == 16;
    let use_hlg = cicp.transfer_characteristics == 18;
    let wide_gamut = cicp.color_primaries == 9; // BT.2020

    // Pre-allocate output and fill in parallel by zipping u16 input chunks
    // with u8 output chunks. This avoids relying on arrays-as-iterators and
    // gives the cleanest ownership story for rayon.
    let pixel_count = (width as usize) * (height as usize);
    let mut out: Vec<u8> = vec![0u8; pixel_count * 4];

    raw_rgba_u16.par_chunks_exact(4).zip(out.par_chunks_exact_mut(4)).for_each(|(src, dst)| {
        // Normalize to [0,1].
        let mut r = src[0] as f32 / 65535.0;
        let mut g = src[1] as f32 / 65535.0;
        let mut b = src[2] as f32 / 65535.0;
        let a = src[3] as f32 / 65535.0;

        // 1. EOTF → linear light in nits.
        if use_pq {
            r = pq_eotf(r);
            g = pq_eotf(g);
            b = pq_eotf(b);
        } else if use_hlg {
            // Peak assumption of 1000 nits is a sensible default for HLG.
            r = hlg_eotf(r, 1000.0);
            g = hlg_eotf(g, 1000.0);
            b = hlg_eotf(b, 1000.0);
        } else {
            // sRGB / BT.709 — no HDR processing; just treat as normal.
            r = srgb_to_linear_simple(r) * sdr_peak_nits;
            g = srgb_to_linear_simple(g) * sdr_peak_nits;
            b = srgb_to_linear_simple(b) * sdr_peak_nits;
        }

        // 2. Primaries: BT.2020 → BT.709 in linear light.
        if wide_gamut {
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
        let mapped_pq = bt2390_eetf(max_pq, src_peak_pq, dst_peak_pq);
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
        let inv = 1.0 / sdr_peak_nits;
        r = (r * inv).clamp(0.0, 1.0);
        g = (g * inv).clamp(0.0, 1.0);
        b = (b * inv).clamp(0.0, 1.0);

        // 5. sRGB OETF → quantize.
        dst[0] = (srgb_oetf(r) * 255.0).round().clamp(0.0, 255.0) as u8;
        dst[1] = (srgb_oetf(g) * 255.0).round().clamp(0.0, 255.0) as u8;
        dst[2] = (srgb_oetf(b) * 255.0).round().clamp(0.0, 255.0) as u8;
        dst[3] = (a.clamp(0.0, 1.0) * 255.0).round() as u8;
    });

    ImageBuffer::<Rgba<u8>, _>::from_raw(width, height, out).unwrap_or_else(|| {
        // Extremely defensive: should never happen since length is exact.
        ImageBuffer::new(width, height)
    })
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
}
