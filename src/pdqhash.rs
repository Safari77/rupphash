// ---------------------------------------------------------------------------
// COMPATIBILITY WITH REFERENCE PDQ (facebook/ThreatExchange)
//
//   4. Luma: the `image` crate uses Rec.709 weights, reference PDQ uses
//      Rec.601 (0.299/0.587/0.114). Also, reference does not pre-downsample
//      to 512px.
//
// ---------------------------------------------------------------------------

use fast_image_resize as fr;
use fast_image_resize::images::{Image, ImageRef};
use fast_image_resize::{FilterType, ResizeAlg, ResizeOptions};
pub use image;
use std::borrow::Cow;
use std::cell::RefCell;
use std::f32::consts::PI;
use std::sync::LazyLock;

const MIN_HASHABLE_DIM: u32 = 5;
const PDQ_NUM_JAROSZ_XY_PASSES: usize = 2;
const DOWNSAMPLE_DIMS: u32 = 512;
const BUFFER_W_H: usize = 64;
const DCT_OUTPUT_W_H: usize = 16;
const DCT_OUTPUT_MATRIX_SIZE: usize = DCT_OUTPUT_W_H * DCT_OUTPUT_W_H;
const HASH_LENGTH: usize = DCT_OUTPUT_MATRIX_SIZE / 8;

// Jarosz box filter window = ceil(dim / JAROSZ_WINDOW_DIVISOR).
// BUFFER_W_H matches reference PDQ.
const JAROSZ_WINDOW_DIVISOR: usize = BUFFER_W_H;

// Lowest DCT frequency kept in the 16x16 output. 0 keeps the DC term,
// 1 matches reference PDQ.
const DCT_FREQ_OFFSET: usize = 1;

// Pre-downsample filter. Lanczos3 is the fast_image_resize default; Box is
// roughly 3x faster for large grayscale downscales and, since the next step is
// a box blur down to 64x64 anyway, visually equivalent.
const RESIZE_ALG: ResizeAlg = ResizeAlg::Convolution(FilterType::Box);

thread_local! {
    // Resizer holds internal scratch buffers; reusing it across images avoids
    // re-allocating them for every file scanned.
    static RESIZER: RefCell<fr::Resizer> = RefCell::new(fr::Resizer::new());
}

// The DCT matrix only depends on constants
static DCT_MATRIX: LazyLock<[[f32; BUFFER_W_H]; DCT_OUTPUT_W_H]> =
    LazyLock::new(compute_dct_matrix);

#[derive(Clone, Debug)]
pub struct PdqFeatures {
    pub coefficients: [f32; DCT_OUTPUT_MATRIX_SIZE],
}

impl PdqFeatures {
    fn new(buffer64x64: &[[f32; BUFFER_W_H]; BUFFER_W_H]) -> Self {
        let coefficients = dct64_to_16(buffer64x64);
        Self { coefficients }
    }

    pub fn to_hash(&self) -> [u8; HASH_LENGTH] {
        pack_bit_rows(&self.bit_rows(false, false))
    }

    /// The eight dihedral variants, in the original order:
    /// identity, rot90, rot180, rot270, mirror-x, mirror-y, transpose, anti-transpose.
    ///
    /// Each variant is a sign pattern optionally followed by a transpose. A
    /// transpose only permutes coefficients, so it cannot change the median,
    /// which means the transposed hash is just the bit-transpose of the plain
    /// one. That halves the number of medians from 8 to 4 and removes all of
    /// the intermediate 256-float copies.
    pub fn generate_dihedral_hashes(&self) -> [[u8; HASH_LENGTH]; 8] {
        let id = self.bit_rows(false, false);
        let neg_cols = self.bit_rows(false, true);
        let neg_rows = self.bit_rows(true, false);
        let neg_both = self.bit_rows(true, true);

        [
            pack_bit_rows(&id),
            pack_bit_rows(&transpose_bit_rows(&neg_rows)),
            pack_bit_rows(&neg_both),
            pack_bit_rows(&transpose_bit_rows(&neg_cols)),
            pack_bit_rows(&neg_cols),
            pack_bit_rows(&neg_rows),
            pack_bit_rows(&transpose_bit_rows(&id)),
            pack_bit_rows(&transpose_bit_rows(&neg_both)),
        ]
    }

    /// One packed bit per coefficient, row r of the 16x16 matrix in rows[r],
    /// bit c = (coefficient(r, c) > median).
    fn bit_rows(&self, neg_rows: bool, neg_cols: bool) -> [u16; DCT_OUTPUT_W_H] {
        let median = self.coefficient_median(neg_rows, neg_cols);
        let mut rows = [0u16; DCT_OUTPUT_W_H];
        for (r, row) in rows.iter_mut().enumerate() {
            let base = r * DCT_OUTPUT_W_H;
            let mut bits = 0u16;
            for c in 0..DCT_OUTPUT_W_H {
                // Compare all coeffs (including DC) against the median
                if apply_sign(self.coefficients[base + c], r, c, neg_rows, neg_cols) > median {
                    bits |= 1 << c;
                }
            }
            *row = bits;
        }
        rows
    }

    /// Median over all 256 coefficients, matching reference PDQ's torben().
    /// For an even count torben returns the *lower* of the two middle values -
    /// the 128th smallest of 256 - so the index is (len - 1) / 2, not len / 2.
    ///
    /// A transpose only permutes coefficients, so it cannot move the median;
    /// that is what lets generate_dihedral_hashes() share one median between
    /// each variant and its transpose. select_nth_unstable is O(n) vs O(n log n)
    /// for a sort, and total_cmp is a real total order (unlike partial_cmp).
    fn coefficient_median(&self, neg_rows: bool, neg_cols: bool) -> f32 {
        let mut buffer = [0.0f32; DCT_OUTPUT_MATRIX_SIZE];
        for (idx, slot) in buffer.iter_mut().enumerate() {
            let (r, c) = (idx / DCT_OUTPUT_W_H, idx % DCT_OUTPUT_W_H);
            *slot = apply_sign(self.coefficients[idx], r, c, neg_rows, neg_cols);
        }
        let mid = (buffer.len() - 1) / 2;
        *buffer.select_nth_unstable_by(mid, f32::total_cmp).1
    }
}

#[inline(always)]
fn apply_sign(v: f32, r: usize, c: usize, neg_rows: bool, neg_cols: bool) -> f32 {
    if (neg_rows && (r & 1 == 1)) ^ (neg_cols && (c & 1 == 1)) { -v } else { v }
}

/// Bit (r, c) of the transposed matrix is bit (c, r) of the original.
fn transpose_bit_rows(rows: &[u16; DCT_OUTPUT_W_H]) -> [u16; DCT_OUTPUT_W_H] {
    let mut out = [0u16; DCT_OUTPUT_W_H];
    for (r, &row) in rows.iter().enumerate() {
        let mut bits = row;
        while bits != 0 {
            let c = bits.trailing_zeros() as usize;
            out[c] |= 1 << r;
            bits &= bits - 1;
        }
    }
    out
}

/// Coefficient i*8+j lands in bit j of byte HASH_LENGTH-1-i, i.e. the low byte
/// of matrix row r goes to hash[31 - 2r] and the high byte to hash[30 - 2r].
fn pack_bit_rows(rows: &[u16; DCT_OUTPUT_W_H]) -> [u8; HASH_LENGTH] {
    let mut hash = [0u8; HASH_LENGTH];
    for (r, &row) in rows.iter().enumerate() {
        hash[HASH_LENGTH - 2 * r - 1] = (row & 0xFF) as u8;
        hash[HASH_LENGTH - 2 * r - 2] = (row >> 8) as u8;
    }
    hash
}

// --- PUBLIC API ---

pub fn generate_pdq_features(image: &image::DynamicImage) -> Option<(PdqFeatures, f32)> {
    if image.width() < MIN_HASHABLE_DIM || image.height() < MIN_HASHABLE_DIM {
        return None;
    }

    // Resizing 1 channel is 3x faster than resizing 3 channels (RGB).
    let luma_image: Cow<image::GrayImage> = match image {
        image::DynamicImage::ImageLuma8(x) => Cow::Borrowed(x),
        other => Cow::Owned(to_luma601(other)),
    };

    let w = luma_image.width();
    let h = luma_image.height();

    // Resize if larger than 512x512
    let processed_image = if w > DOWNSAMPLE_DIMS || h > DOWNSAMPLE_DIMS {
        // Calculate new dimensions maintaining aspect ratio (thumbnail behavior)
        let (new_w, new_h) = calculate_target_dimensions(w, h, DOWNSAMPLE_DIMS);
        // On failure, hash at full resolution rather than panicking.
        match resize_luma_fast(&luma_image, new_w, new_h) {
            Some(resized) => Cow::Owned(resized),
            None => luma_image,
        }
    } else {
        luma_image
    };

    // We can pass the Luma8 directly to a specialized internal function
    // to avoid re-converting it in the next step.
    Some(generate_pdq_from_luma(&processed_image))
}

#[allow(unused)]
pub fn generate_pdq(image: &image::DynamicImage) -> Option<([u8; HASH_LENGTH], f32)> {
    generate_pdq_features(image).map(|(feats, quality)| (feats.to_hash(), quality))
}

fn resize_luma_fast(img: &image::GrayImage, w: u32, h: u32) -> Option<image::GrayImage> {
    if w == 0 || h == 0 {
        return None;
    }

    // Borrow the source buffer instead of copying it into a new Image.
    let src_view =
        ImageRef::new(img.width(), img.height(), img.as_raw(), fr::PixelType::U8).ok()?;

    // Create container for destination
    let mut dst_view = Image::new(w, h, fr::PixelType::U8);

    let options = ResizeOptions::new().resize_alg(RESIZE_ALG);
    RESIZER.with(|r| r.borrow_mut().resize(&src_view, &mut dst_view, &options)).ok()?;

    // Convert back to image::GrayImage
    image::GrayImage::from_raw(w, h, dst_view.into_vec())
}

/// Integer math, and clamped to at least 1px: the old float version could
/// return 0 for extreme aspect ratios (e.g. 4000x5), which panicked in resize.
fn calculate_target_dimensions(w: u32, h: u32, max_dim: u32) -> (u32, u32) {
    if w == 0 || h == 0 {
        return (w.max(1), h.max(1));
    }
    if w > h {
        let new_h = (u64::from(h) * u64::from(max_dim) / u64::from(w)).max(1) as u32;
        (max_dim, new_h)
    } else {
        let new_w = (u64::from(w) * u64::from(max_dim) / u64::from(h)).max(1) as u32;
        (new_w, max_dim)
    }
}

// accepts GrayImage directly
fn generate_pdq_from_luma(img: &image::GrayImage) -> (PdqFeatures, f32) {
    let num_cols = img.width() as usize;
    let num_rows = img.height() as usize;

    // Convert u8 pixels to f32 for processing (straight over the raw buffer,
    // which skips the per-pixel Luma wrapper)
    let mut luma_buffer: Vec<f32> = img.as_raw().iter().map(|&p| p as f32).collect();

    let window_size_along_rows = num_cols.div_ceil(JAROSZ_WINDOW_DIVISOR);
    let window_size_along_cols = num_rows.div_ceil(JAROSZ_WINDOW_DIVISOR);

    jarosz_filter_float(
        &mut luma_buffer,
        num_rows,
        num_cols,
        window_size_along_rows,
        window_size_along_cols,
        PDQ_NUM_JAROSZ_XY_PASSES,
    );

    let buffer64x64 = decimate_float::<BUFFER_W_H, BUFFER_W_H>(&luma_buffer, num_rows, num_cols);
    let quality = pdq_image_domain_quality_metric(&buffer64x64);
    let features = PdqFeatures::new(&buffer64x64);
    (features, quality)
}

// --- INTERNAL HELPERS ---

/// Rec.601 luma (0.299/0.587/0.114), as used by reference PDQ. The `image`
/// crate's to_luma8() is hardcoded to Rec.709, so this has to be done by hand.
fn to_luma601(img: &image::DynamicImage) -> image::GrayImage {
    #[inline(always)]
    fn luma601(px: &[u8]) -> u8 {
        // (299r + 587g + 114b + 500) / 1000, rounded, integer only
        ((299 * px[0] as u32 + 587 * px[1] as u32 + 114 * px[2] as u32 + 500) / 1000) as u8
    }

    let (w, h) = (img.width(), img.height());
    let mut buf = Vec::with_capacity(w as usize * h as usize);
    match img {
        image::DynamicImage::ImageRgb8(src) => buf.extend(src.pixels().map(|p| luma601(&p.0))),
        image::DynamicImage::ImageRgba8(src) => buf.extend(src.pixels().map(|p| luma601(&p.0))),
        image::DynamicImage::ImageLuma8(src) => buf.extend_from_slice(src.as_raw()),
        other => buf.extend(other.to_rgb8().pixels().map(|p| luma601(&p.0))),
    }
    image::GrayImage::from_raw(w, h, buf).expect("buffer is exactly w*h bytes")
}

// Computed once and cached in DCT_MATRIX.
fn compute_dct_matrix() -> [[f32; BUFFER_W_H]; DCT_OUTPUT_W_H] {
    let mut matrix = [[0.0; BUFFER_W_H]; DCT_OUTPUT_W_H];
    let num_cols = BUFFER_W_H;
    let inv_sqrt_cols = 1.0 / (num_cols as f32).sqrt();
    let sqrt_2 = 2.0_f32.sqrt();

    for (i, row) in matrix.iter_mut().enumerate() {
        // Rows (Frequency)
        let freq = (i + DCT_FREQ_OFFSET) as f32;
        let normalization = if freq == 0.0 { inv_sqrt_cols } else { inv_sqrt_cols * sqrt_2 };
        for (j, cell) in row.iter_mut().enumerate() {
            // Cols (Space)
            let angle = (PI * freq * (2.0 * (j as f32) + 1.0)) / (2.0 * (num_cols as f32));
            *cell = normalization * angle.cos();
        }
    }
    matrix
}

fn dct64_to_16(input: &[[f32; BUFFER_W_H]; BUFFER_W_H]) -> [f32; DCT_OUTPUT_MATRIX_SIZE] {
    let dct_mat = &*DCT_MATRIX;

    let mut intermediate = [[0.0f32; BUFFER_W_H]; DCT_OUTPUT_W_H];

    // Pass 1: Rows. k is the outer loop so both operands are walked
    // row-major; the accumulation order over k is unchanged, so the result is
    // bit identical to the naive triple loop.
    for (i, inter_row) in intermediate.iter_mut().enumerate() {
        for (k, in_row) in input.iter().enumerate() {
            let coeff = dct_mat[i][k];
            for j in 0..BUFFER_W_H {
                inter_row[j] += coeff * in_row[j];
            }
        }
    }

    let mut output = [0.0; DCT_OUTPUT_MATRIX_SIZE];

    // Pass 2: Columns (Using same matrix, effectively Transposed * Matrix)
    for i in 0..DCT_OUTPUT_W_H {
        for j in 0..DCT_OUTPUT_W_H {
            let mut sum = 0.0;
            for k in 0..BUFFER_W_H {
                sum += intermediate[i][k] * dct_mat[j][k];
            }
            output[i * DCT_OUTPUT_W_H + j] = sum;
        }
    }
    output
}

// --- Filters & Decimation ---

#[inline(always)]
fn box_one_d_float(
    invec: &[f32],
    in_start: usize,
    outvec: &mut [f32],
    out_start: usize,
    vec_len: usize,
    stride: usize,
    win_size: usize,
) {
    // A window wider than the vector would read past the end of the line.
    let win_size = win_size.clamp(1, vec_len.max(1));
    let half_win = (win_size + 2) / 2;

    let phase_1 = half_win - 1;
    let phase_2 = win_size - half_win + 1;
    let phase_3 = vec_len.saturating_sub(win_size);
    let phase_4 = half_win - 1;

    let mut li = in_start;
    let mut ri = in_start;
    let mut oi = out_start;
    let mut sum = 0.0;
    let mut curr_win = 0.0;

    // Accumulate the leading half window without emitting anything.
    for _ in 0..phase_1 {
        sum += invec[ri];
        curr_win += 1.0;
        ri += stride;
    }
    // Grow the window up to full size.
    for _ in 0..phase_2 {
        sum += invec[ri];
        curr_win += 1.0;
        outvec[oi] = sum / curr_win;
        ri += stride;
        oi += stride;
    }
    // Slide the full window.
    for _ in 0..phase_3 {
        sum += invec[ri];
        sum -= invec[li];
        outvec[oi] = sum / curr_win;
        li += stride;
        ri += stride;
        oi += stride;
    }
    // Shrink the trailing half window.
    for _ in 0..phase_4 {
        sum -= invec[li];
        curr_win -= 1.0;
        outvec[oi] = sum / curr_win;
        li += stride;
        oi += stride;
    }
}

fn box_along_rows_float(input: &[f32], output: &mut [f32], rows: usize, cols: usize, win: usize) {
    for i in 0..rows {
        box_one_d_float(input, i * cols, output, i * cols, cols, 1, win);
    }
}

fn box_along_cols_float(input: &[f32], output: &mut [f32], rows: usize, cols: usize, win: usize) {
    for j in 0..cols {
        box_one_d_float(input, j, output, j, rows, cols, win);
    }
}

fn jarosz_filter_float(
    buf: &mut [f32],
    rows: usize,
    cols: usize,
    w_rows: usize,
    w_cols: usize,
    nreps: usize,
) {
    // The column pass walks with a stride instead of transposing the whole
    // buffer twice per repetition; same arithmetic, four fewer full-image
    // copies per rep.
    let mut tmp = vec![0.0; buf.len()];
    for _ in 0..nreps {
        box_along_rows_float(buf, &mut tmp, rows, cols, w_rows);
        box_along_cols_float(&tmp, buf, rows, cols, w_cols);
    }
}

fn decimate_float<const R: usize, const C: usize>(
    input: &[f32],
    in_r: usize,
    in_c: usize,
) -> [[f32; C]; R] {
    let mut out = [[0.0; C]; R];
    for i in 0..R {
        let ini = ((i * 2 + 1) * in_r) / (R * 2);
        for j in 0..C {
            let inj = ((j * 2 + 1) * in_c) / (C * 2);
            out[i][j] = input[ini * in_c + inj];
        }
    }
    out
}

fn pdq_image_domain_quality_metric<const R: usize, const C: usize>(buf: &[[f32; C]; R]) -> f32 {
    // Reference PDQ scales each gradient by 100/255 and truncates it to an
    // integer before summing.
    let mut sum = 0.0;
    for i in 0..(R - 1) {
        for j in 0..C {
            sum += (((buf[i][j] - buf[i + 1][j]) * 100.0) / 255.0).abs().trunc();
        }
    }
    for i in 0..R {
        for j in 0..(C - 1) {
            sum += (((buf[i][j] - buf[i][j + 1]) * 100.0) / 255.0).abs().trunc();
        }
    }
    let q = sum / 90.0;
    if q > 1.0 { 1.0 } else { q }
}

// --- CORRECTNESS TESTS ---

#[cfg(test)]
mod tests {
    use super::*;

    // Naive, pre-optimisation implementations kept as ground truth for the
    // packed-bit fast path.
    fn naive_to_hash(f: &PdqFeatures) -> [u8; HASH_LENGTH] {
        let mut buffer = f.coefficients;
        buffer.sort_unstable_by(f32::total_cmp);
        let median = buffer[(buffer.len() - 1) / 2];

        let mut hash = [0; HASH_LENGTH];
        for i in 0..HASH_LENGTH {
            let mut byte = 0;
            for j in 0..8 {
                if f.coefficients[i * 8 + j] > median {
                    byte |= 1 << j;
                }
            }
            hash[HASH_LENGTH - i - 1] = byte;
        }
        hash
    }

    fn naive_transpose(f: &PdqFeatures) -> PdqFeatures {
        let mut new_coeffs = [0.0; DCT_OUTPUT_MATRIX_SIZE];
        for r in 0..DCT_OUTPUT_W_H {
            for c in 0..DCT_OUTPUT_W_H {
                new_coeffs[c * DCT_OUTPUT_W_H + r] = f.coefficients[r * DCT_OUTPUT_W_H + c];
            }
        }
        PdqFeatures { coefficients: new_coeffs }
    }

    fn naive_flip_x(f: &PdqFeatures) -> PdqFeatures {
        let mut new_coeffs = f.coefficients;
        for r in 0..DCT_OUTPUT_W_H {
            for c in 0..DCT_OUTPUT_W_H {
                if c % 2 != 0 {
                    let idx = r * DCT_OUTPUT_W_H + c;
                    new_coeffs[idx] = -new_coeffs[idx];
                }
            }
        }
        PdqFeatures { coefficients: new_coeffs }
    }

    fn naive_flip_y(f: &PdqFeatures) -> PdqFeatures {
        let mut new_coeffs = f.coefficients;
        for r in 0..DCT_OUTPUT_W_H {
            if r % 2 != 0 {
                for c in 0..DCT_OUTPUT_W_H {
                    let idx = r * DCT_OUTPUT_W_H + c;
                    new_coeffs[idx] = -new_coeffs[idx];
                }
            }
        }
        PdqFeatures { coefficients: new_coeffs }
    }

    fn naive_dihedral(f: &PdqFeatures) -> Vec<[u8; HASH_LENGTH]> {
        vec![
            naive_to_hash(f),
            naive_to_hash(&naive_flip_x(&naive_transpose(f))),
            naive_to_hash(&naive_flip_y(&naive_flip_x(f))),
            naive_to_hash(&naive_flip_y(&naive_transpose(f))),
            naive_to_hash(&naive_flip_x(f)),
            naive_to_hash(&naive_flip_y(f)),
            naive_to_hash(&naive_transpose(f)),
            naive_to_hash(&naive_flip_y(&naive_flip_x(&naive_transpose(f)))),
        ]
    }

    fn pseudo_random_features(seed: u32) -> PdqFeatures {
        let mut state = seed;
        let mut coefficients = [0.0f32; DCT_OUTPUT_MATRIX_SIZE];
        for c in coefficients.iter_mut() {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            *c = (state >> 8) as f32 / 65_536.0 - 128.0;
        }
        PdqFeatures { coefficients }
    }

    #[test]
    fn fast_dihedral_matches_naive() {
        for seed in [1u32, 42, 0x1234_5678, 0xDEAD_BEEF] {
            let f = pseudo_random_features(seed);
            assert_eq!(f.to_hash(), naive_to_hash(&f), "to_hash mismatch, seed {seed}");
            assert_eq!(
                f.generate_dihedral_hashes().to_vec(),
                naive_dihedral(&f),
                "dihedral mismatch, seed {seed}"
            );
        }
    }

    #[test]
    fn dihedral_set_is_the_full_group() {
        // Eight distinct variants; random coefficients should not collide.
        let f = pseudo_random_features(7);
        let hashes = f.generate_dihedral_hashes();
        for i in 0..8 {
            for j in (i + 1)..8 {
                assert_ne!(hashes[i], hashes[j], "variants {i} and {j} collided");
            }
        }
    }

    #[test]
    fn quality_metric_scaling() {
        // Flat image: no gradient at all.
        let flat = [[128.0f32; BUFFER_W_H]; BUFFER_W_H];
        assert_eq!(pdq_image_domain_quality_metric(&flat), 0.0);

        // Two horizontal gradients of 10, each trunc(10 * 100 / 255) = 3.
        let buf = [[0.0f32, 10.0], [0.0f32, 10.0]];
        assert!((pdq_image_domain_quality_metric(&buf) - 6.0 / 90.0).abs() < 1e-6);
    }

    #[test]
    fn target_dimensions_never_collapse_to_zero() {
        assert_eq!(calculate_target_dimensions(4000, 5, 512), (512, 1));
        assert_eq!(calculate_target_dimensions(5, 4000, 512), (1, 512));
        assert_eq!(calculate_target_dimensions(1024, 1024, 512), (512, 512));
        assert_eq!(calculate_target_dimensions(1024, 512, 512), (512, 256));
    }
}

// --- BENCHMARK TESTS ---

#[cfg(test)]
mod benchmarks {
    use super::*;
    use std::path::Path;
    use std::time::Instant;

    #[test]
    fn bench_pdq_performance() {
        let path = Path::new("./tests/bench.jpg");
        let img = image::open(path)
            .expect("Failed to open './tests/bench.jpg'. Please ensure the test image exists.");

        // Number of iterations for the benchmark
        let iterations_feats = 100;
        let iterations_dihed = 30000;

        // ---------------------------------------------------------
        // Benchmark 1: generate_pdq_features
        // Measures full pipeline: resize -> luma -> filter -> DCT
        // ---------------------------------------------------------

        // Warmup (ensure code is loaded/caches warm)
        let _ = generate_pdq_features(&img);

        let start = Instant::now();
        for _ in 0..iterations_feats {
            // Use black_box to prevent compiler from optimizing away the loop
            std::hint::black_box(generate_pdq_features(&img));
        }
        let duration = start.elapsed();
        let avg_time = duration / iterations_feats;

        println!("\n=== Benchmark Results ===");
        println!("generate_pdq_features ({} iterations):", iterations_feats);
        println!("  Total time: {:?}", duration);
        println!("  Avg time:   {:?}", avg_time);

        // ---------------------------------------------------------
        // Benchmark 2: generate_dihedral_hashes
        // Measures hashing and bit manipulations on existing features
        // ---------------------------------------------------------

        let (features, _) = generate_pdq_features(&img).expect("Failed to generate features");

        let start = Instant::now();
        for _ in 0..iterations_dihed {
            std::hint::black_box(features.generate_dihedral_hashes());
        }
        let duration = start.elapsed();
        let avg_time = duration / iterations_dihed;

        println!("generate_dihedral_hashes ({} iterations):", iterations_dihed);
        println!("  Total time: {:?}", duration);
        println!("  Avg time:   {:?}", avg_time);
        println!("=========================\n");
    }
}
