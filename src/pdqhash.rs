///! Compute PDQ hash of an image.
pub use image;
use std::f32::consts::PI;
use fast_image_resize as fr;
use fast_image_resize::images::Image;
use fast_image_resize::ResizeOptions;

const MIN_HASHABLE_DIM: u32 = 5;
const PDQ_NUM_JAROSZ_XY_PASSES: usize = 2;
const DOWNSAMPLE_DIMS: u32 = 512;
const BUFFER_W_H: usize = 64;
const DCT_OUTPUT_W_H: usize = 16;
const DCT_OUTPUT_MATRIX_SIZE: usize = DCT_OUTPUT_W_H * DCT_OUTPUT_W_H;
const HASH_LENGTH: usize = DCT_OUTPUT_MATRIX_SIZE / 8;

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
        // Exclude DC component (index 0) from median calculation for rotation robustness
        let mut buffer = [0.0; DCT_OUTPUT_MATRIX_SIZE - 1];
        buffer.copy_from_slice(&self.coefficients[1..]);

        buffer.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Less));
        let median = buffer[buffer.len() / 2];

        let mut hash = [0; HASH_LENGTH];

        for i in 0..HASH_LENGTH {
            let mut byte = 0;
            for j in 0..8 {
                // Compare all coeffs (including DC) against AC median
                if self.coefficients[i * 8 + j] > median {
                    byte |= 1 << j;
                }
            }
            hash[HASH_LENGTH - i - 1] = byte;
        }
        hash
    }

    pub fn generate_dihedral_hashes(&self) -> Vec<[u8; HASH_LENGTH]> {
        let mut results = Vec::with_capacity(8);
        results.push(self.to_hash());
        results.push(self.transform_transpose().transform_flip_x().to_hash());
        results.push(self.transform_flip_x().transform_flip_y().to_hash());
        results.push(self.transform_transpose().transform_flip_y().to_hash());
        results.push(self.transform_flip_x().to_hash());
        results.push(self.transform_flip_y().to_hash());
        results.push(self.transform_transpose().to_hash());
        results.push(self.transform_transpose().transform_flip_x().transform_flip_y().to_hash());
        results
    }

    fn transform_transpose(&self) -> Self {
        let mut new_coeffs = [0.0; DCT_OUTPUT_MATRIX_SIZE];
        for r in 0..DCT_OUTPUT_W_H {
            for c in 0..DCT_OUTPUT_W_H {
                new_coeffs[c * DCT_OUTPUT_W_H + r] = self.coefficients[r * DCT_OUTPUT_W_H + c];
            }
        }
        Self { coefficients: new_coeffs }
    }

    fn transform_flip_x(&self) -> Self {
        let mut new_coeffs = self.coefficients;
        for r in 0..DCT_OUTPUT_W_H {
            for c in 0..DCT_OUTPUT_W_H {
                if c % 2 != 0 {
                    let idx = r * DCT_OUTPUT_W_H + c;
                    new_coeffs[idx] = -new_coeffs[idx];
                }
            }
        }
        Self { coefficients: new_coeffs }
    }

    fn transform_flip_y(&self) -> Self {
        let mut new_coeffs = self.coefficients;
        for r in 0..DCT_OUTPUT_W_H {
            if r % 2 != 0 {
                for c in 0..DCT_OUTPUT_W_H {
                    let idx = r * DCT_OUTPUT_W_H + c;
                    new_coeffs[idx] = -new_coeffs[idx];
                }
            }
        }
        Self { coefficients: new_coeffs }
    }
}

// --- PUBLIC API ---

pub fn generate_pdq_features(image: &image::DynamicImage) -> Option<(PdqFeatures, f32)> {
    if image.width() < MIN_HASHABLE_DIM || image.height() < MIN_HASHABLE_DIM {
        return None;
    }

    // Optimization: Convert to Luma ONCE before resizing.
    // Resizing 1 channel is 3x faster than resizing 3 channels (RGB).
    let luma_image = if let image::DynamicImage::ImageLuma8(x) = image {
        x.clone()
    } else {
        image.to_luma8()
    };

    let w = luma_image.width();
    let h = luma_image.height();

    // Resize if larger than 512x512
    let processed_image = if w > DOWNSAMPLE_DIMS || h > DOWNSAMPLE_DIMS {
        // Calculate new dimensions maintaining aspect ratio (thumbnail behavior)
        let (new_w, new_h) = calculate_target_dimensions(w, h, DOWNSAMPLE_DIMS);
        resize_luma_fast(&luma_image, new_w, new_h)
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

fn resize_luma_fast(img: &image::GrayImage, w: u32, h: u32) -> image::GrayImage {
    let src_width = img.width();
    let src_height = img.height();
    let dst_width = w;
    let dst_height = h;

    // Create container for source image
    let src_view = Image::from_vec_u8(
        src_width,
        src_height,
        img.as_raw().clone(),
        fr::PixelType::U8,
    ).unwrap();

    // Create container for destination
    let mut dst_view = Image::new(
        dst_width,
        dst_height,
        fr::PixelType::U8
    );

    let mut resizer = fr::Resizer::new();
    let options = ResizeOptions::default();
    resizer.resize(&src_view, &mut dst_view, &options).unwrap();
    // Convert back to image::GrayImage
    image::GrayImage::from_raw(w, h, dst_view.into_vec()).unwrap()
}

fn calculate_target_dimensions(w: u32, h: u32, max_dim: u32) -> (u32, u32) {
    let ratio = w as f32 / h as f32;
    if w > h {
        (max_dim, (max_dim as f32 / ratio) as u32)
    } else {
        ((max_dim as f32 * ratio) as u32, max_dim)
    }
}

// Slightly modified internal helper that accepts GrayImage directly
fn generate_pdq_from_luma(img: &image::GrayImage) -> (PdqFeatures, f32) {
    let num_cols = img.width() as usize;
    let num_rows = img.height() as usize;

    // Convert u8 pixels to f32 for processing
    let mut luma_buffer: Vec<f32> = img.pixels().map(|p| p.0[0] as f32).collect();

    let window_size_along_rows = num_cols.div_ceil(2 * BUFFER_W_H);
    let window_size_along_cols = num_rows.div_ceil(2 * BUFFER_W_H);

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

// Compute DCT matrix on the fly to ensure mathematical correctness
fn get_dct_matrix() -> [[f32; 64]; 16] {
    let mut matrix = [[0.0; 64]; 16];
    let num_cols = 64;
    let inv_sqrt_cols = 1.0 / (num_cols as f32).sqrt();
    let sqrt_2 = 2.0_f32.sqrt();

    for i in 0..16 { // Rows (Frequency)
        let normalization = if i == 0 { inv_sqrt_cols } else { inv_sqrt_cols * sqrt_2 };
        for j in 0..64 { // Cols (Space)
            let angle = (PI * (i as f32) * (2.0 * (j as f32) + 1.0)) / (2.0 * (num_cols as f32));
            matrix[i][j] = normalization * angle.cos();
        }
    }
    matrix
}

fn dct64_to_16<const OUT_NUM_ROWS: usize, const OUT_NUM_COLS: usize>(
    input: &[[f32; OUT_NUM_COLS]; OUT_NUM_ROWS],
) -> [f32; DCT_OUTPUT_MATRIX_SIZE] {
    // We compute the matrix locally. For high performance, this should be cached (lazy_static),
    // but for "scanning" speed (disk IO limited), recomputing 16x64 cos values is negligible.
    let dct_mat = get_dct_matrix();

    let mut intermediate = [[0.0; OUT_NUM_COLS]; DCT_OUTPUT_W_H];

    // Pass 1: Rows
    for i in 0..DCT_OUTPUT_W_H {
        for j in 0..OUT_NUM_COLS {
            let mut sum = 0.0;
            for k in 0..BUFFER_W_H {
                sum += dct_mat[i][k] * input[k][j];
            }
            intermediate[i][j] = sum;
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

fn transpose(input: &[f32], output: &mut [f32], width: usize, height: usize) {
    for y in 0..height { for x in 0..width { output[x * height + y] = input[y * width + x]; } }
}

#[inline(always)]
fn box_one_d_float(invec: &[f32], in_start: usize, outvec: &mut [f32], vec_len: usize, win_size: usize) {
    let half_win = (win_size + 2) / 2;
    let oi_off = half_win - 1;
    let li_off = win_size - half_win + 1;
    let mut sum = 0.0;
    let mut curr_win = 0.0;
    let p1_end = in_start + oi_off;
    for ri in in_start..p1_end { sum += invec[ri]; curr_win += 1.0; }
    let p2_end = in_start + win_size;
    for ri in p1_end..p2_end { let oi = ri - oi_off; sum += invec[ri]; curr_win += 1.0; outvec[oi] = sum / curr_win; }
    let p3_end = in_start + vec_len;
    for ri in p2_end..p3_end { let oi = ri - oi_off; let li = oi - li_off; sum += invec[ri]; sum -= invec[li]; outvec[oi] = sum / curr_win; }
    let p4_start = in_start + vec_len - half_win + 1;
    for oi in p4_start..p3_end { let li = oi - li_off; sum -= invec[li]; curr_win -= 1.0; outvec[oi] = sum / curr_win; }
}

fn box_along_rows_float(input: &[f32], output: &mut [f32], rows: usize, cols: usize, win: usize) {
    for i in 0..rows { box_one_d_float(input, i * cols, output, cols, win); }
}

fn jarosz_filter_float(buf: &mut [f32], rows: usize, cols: usize, w_rows: usize, w_cols: usize, nreps: usize) {
    let mut tmp = vec![0.0; buf.len()];
    for _ in 0..nreps {
        box_along_rows_float(buf, &mut tmp, rows, cols, w_rows);
        transpose(&tmp, buf, cols, rows);
        box_along_rows_float(buf, &mut tmp, cols, rows, w_cols);
        transpose(&tmp, buf, rows, cols);
    }
}

fn decimate_float<const R: usize, const C: usize>(input: &[f32], in_r: usize, in_c: usize) -> [[f32; C]; R] {
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
    let mut sum = 0.0;
    for i in 0..(R - 1) { for j in 0..C { sum += ((buf[i][j] - buf[i+1][j])/255.0).abs(); } }
    for i in 0..R { for j in 0..(C - 1) { sum += ((buf[i][j] - buf[i][j+1])/255.0).abs(); } }
    let q = sum / 90.0;
    if q > 1.0 { 1.0 } else { q }
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
