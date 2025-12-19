use image::DynamicImage;
use rustdct::{DctPlanner, TransformType2And3};
use std::sync::Arc;

// Before crying, there can be 1-2-bit differences when comparing to Python imagehash,
// it is caused by the interpolation artifacts when Python rotates the actual pixel grid (img.rotate).
// The Rust method is mathematically cleaner because it transforms the DCT coefficients directly without
// pixel resampling noise.

//     This program is free software: you can redistribute it and/or modify it under the terms of the
//     GNU General Public License as published by the Free Software Foundation, either version 3 of
//     the License, or (at your option) any later version.
//     This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
//     without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
//     the GNU General Public License for more details.
//     You should have received a copy of the GNU General Public License along with this program.
//     If not, see <https://www.gnu.org/licenses/>.

/// Standard pHash constants
const DCT_SIZE: usize = 32; // The standard pHash uses a 32x32 DCT
const HASH_SIZE: usize = 8; // The hash is based on the top-left 8x8 low frequencies
// DCT_SIZE/HASH_SIZE=4: This multiplier (4) is implicit in the standard (32x32 DCT -> 8x8 Hash).

pub struct DctPhash {
    row_dct: Arc<dyn TransformType2And3<f32>>,
    col_dct: Arc<dyn TransformType2And3<f32>>,
    scratch_len: usize,
}

impl DctPhash {
    /// Initialize the DCT planner.
    /// Standard pHash uses 32x32, so this sets up the planners accordingly.
    pub fn new() -> Self {
        let mut planner = DctPlanner::new();

        // Plan for 32x32 2D DCT
        let row_dct = planner.plan_dct2(DCT_SIZE);
        let col_dct = planner.plan_dct2(DCT_SIZE);

        // Calculate required scratch space once
        let scratch_len = std::cmp::max(row_dct.get_scratch_len(), col_dct.get_scratch_len());
        let scratch_len = std::cmp::max(scratch_len, DCT_SIZE); // Also needs space for transpose

        Self { row_dct, col_dct, scratch_len }
    }

    /// Calculates the 64-bit perceptual hash of an image.
    pub fn hash_image(&self, img: &DynamicImage) -> u64 {
        // 1. Resize to 32x32 using Triangle (Bilinear) filter and convert to Grayscale
        //    pHash standard specifically requires 32x32.
        let gray_img = img
            .resize_exact(DCT_SIZE as u32, DCT_SIZE as u32, image::imageops::FilterType::Triangle)
            .to_luma8();

        // 2. Convert to f32 vector for DCT
        let mut pixels: Vec<f32> = gray_img.as_raw().iter().map(|&b| b as f32).collect();

        // 3. Perform 2D DCT (Separable: Rows then Cols)
        self.perform_dct_2d(&mut pixels);

        // 4. Crop to top-left 8x8
        let low_freqs = self.crop_8x8(&pixels);

        // 5. Compute Median (Standard pHash compares against Median)
        //    Note: Some implementations exclude the DC coefficient (0,0) from median calculation
        //    because it represents flat luminance. We exclude it here for better robustness.
        let mut sorted = low_freqs.clone();
        // Remove DC term (0) for median calc
        sorted.remove(0);
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = sorted[sorted.len() / 2];

        // 6. Generate Hash bits
        let mut hash: u64 = 0;
        for (i, &val) in low_freqs.iter().enumerate() {
            if val > median {
                // Set bit i (Big Endian style mapping: bit 63 is (0,0), bit 0 is (7,7))
                hash |= 1 << (63 - i);
            }
        }

        hash
    }

    /// Calculates a rotation-invariant 64-bit perceptual hash.
    /// It computes the standard hash and then finds the minimum hash
    /// among all 90-degree rotations (0, 90, 180, 270).
    #[allow(dead_code)]
    pub fn hash_image_invariant(&self, img: &DynamicImage) -> u64 {
        let base_hash = self.hash_image(img);
        calculate_rotation_invariant_hash(base_hash)
    }

    /// Helper: In-place 2D DCT on a 32x32 buffer
    fn perform_dct_2d(&self, buffer: &mut Vec<f32>) {
        // Ensure we have scratch space
        let mut scratch = vec![0.0f32; self.scratch_len];

        // Rows
        for row in buffer.chunks_mut(DCT_SIZE) {
            self.row_dct.process_dct2_with_scratch(row, &mut scratch);
        }

        // Transpose
        let mut transposed = vec![0.0f32; DCT_SIZE * DCT_SIZE];
        transpose::transpose(buffer, &mut transposed, DCT_SIZE, DCT_SIZE);
        *buffer = transposed;

        // Cols (which are now rows)
        for row in buffer.chunks_mut(DCT_SIZE) {
            self.col_dct.process_dct2_with_scratch(row, &mut scratch);
        }

        // Transpose back
        let mut final_buf = vec![0.0f32; DCT_SIZE * DCT_SIZE];
        transpose::transpose(buffer, &mut final_buf, DCT_SIZE, DCT_SIZE);
        *buffer = final_buf;
    }

    /// Helper: Extract 8x8 crop from 32x32 buffer
    fn crop_8x8(&self, full_dct: &[f32]) -> Vec<f32> {
        let mut crop = Vec::with_capacity(HASH_SIZE * HASH_SIZE);
        for y in 0..HASH_SIZE {
            let start = y * DCT_SIZE;
            crop.extend_from_slice(&full_dct[start..start + HASH_SIZE]);
        }
        crop
    }
}

// =========================================================================
//  Rotation Optimization (Bitwise Operations on u64)
// =========================================================================

/// Returns the smallest hash among the original and its 90, 180, 270 degree rotations.
/// Useful for rotation-invariant image search.
pub fn calculate_rotation_invariant_hash(hash: u64) -> u64 {
    let h90 = rotate_hash_90(hash);
    let h180 = rotate_hash_180(hash);
    let h270 = rotate_hash_270(hash);

    hash.min(h90).min(h180).min(h270)
}

/// Rotates a DCT pHash by 90 degrees clockwise by manipulating bits.
///
/// Theory: Rotating an image 90 degrees is equivalent to transposing the DCT matrix
/// and reversing the signs of the coefficients in odd columns (or rows depending on definition).
/// In pHash, sign reversal flips the bit relative to the median (assuming median approx 0).
pub fn rotate_hash_90(hash: u64) -> u64 {
    let mut result = 0u64;
    for y in 0..8 {
        for x in 0..8 {
            let src_idx = 8 * y + x;

            // Transpose indices: (x, y) -> (y, x)
            let dst_x = y;
            let dst_y = x;
            let dst_idx = 8 * dst_y + dst_x;

            let bit = (hash >> (63 - src_idx)) & 1;

            // Logic: Rot90 implies sign change on odd horizontal frequencies.
            let flip = dst_x % 2 != 0;

            let final_bit = if flip { bit ^ 1 } else { bit };
            result |= final_bit << (63 - dst_idx);
        }
    }
    result
}

/// Rotates a DCT pHash by 180 degrees.
/// Logic: Sign change on (x+y) odd. No transpose.
pub fn rotate_hash_180(hash: u64) -> u64 {
    let mut result = 0u64;
    for i in 0..64 {
        let x = i % 8;
        let y = i / 8;

        let flip = (x + y) % 2 != 0;
        let bit = (hash >> (63 - i)) & 1;

        let final_bit = if flip { bit ^ 1 } else { bit };
        result |= final_bit << (63 - i);
    }
    result
}

/// Rotates a DCT pHash by 270 degrees clockwise (90 CCW).
pub fn rotate_hash_270(hash: u64) -> u64 {
    let mut result = 0u64;
    for y in 0..8 {
        for x in 0..8 {
            let src_idx = 8 * y + x;

            // Transpose
            let dst_x = y;
            let dst_y = x;
            let dst_idx = 8 * dst_y + dst_x;

            let bit = (hash >> (63 - src_idx)) & 1;

            // Logic: Sign change on odd vertical frequencies
            let flip = dst_y % 2 != 0;

            let final_bit = if flip { bit ^ 1 } else { bit };
            result |= final_bit << (63 - dst_idx);
        }
    }
    result
}

// =========================================================================
//  Flip Operations (Bitwise Operations on u64)
// =========================================================================

/// Flips a DCT pHash horizontally.
/// Logic: Horizontal flip changes sign of odd horizontal frequencies.
pub fn flip_hash_horizontal(hash: u64) -> u64 {
    let mut result = 0u64;
    for i in 0..64 {
        let x = i % 8;
        let flip = x % 2 != 0;
        let bit = (hash >> (63 - i)) & 1;
        let final_bit = if flip { bit ^ 1 } else { bit };
        result |= final_bit << (63 - i);
    }
    result
}

// =========================================================================
//  All 8 Dihedral Variants (Store 1 / Query 8 Strategy)
// =========================================================================

/// Generates all 8 dihedral variants of a pHash.
/// Returns: [original, rot90, rot180, rot270, flip_h, flip_h+rot90, flip_h+rot180, flip_h+rot270]
///
/// This is used for the "Store 1 / Query 8" strategy:
/// - Store: Only the original pHash (Rotation 0°)
/// - Query: Generate all 8 variants and check if any match stored hashes
pub fn generate_dihedral_hashes(hash: u64) -> Vec<u64> {
    let h0 = hash;
    let h90 = rotate_hash_90(hash);
    let h180 = rotate_hash_180(hash);
    let h270 = rotate_hash_270(hash);

    // Flipped variants
    let h_flip = flip_hash_horizontal(hash);
    let h_flip_90 = rotate_hash_90(h_flip);
    let h_flip_180 = rotate_hash_180(h_flip);
    let h_flip_270 = rotate_hash_270(h_flip);

    vec![h0, h90, h180, h270, h_flip, h_flip_90, h_flip_180, h_flip_270]
}
