use rayon::prelude::*;

// 15 bits for 64-bit hash (approx 23% difference)
#[allow(unused)]
pub const MAX_SIMILARITY_64: u32 = 15;
// 60 bits for 256-bit hash (approx 23% difference to match pHash strictness)
// Note: If you want strictly "near duplicates", use 30. If you want "visually similar", use 60.
pub const MAX_SIMILARITY_256: u32 = 60;

/// Trait to support generic Hamming distance indexes.
pub trait HammingHash: Copy + Send + Sync + 'static {
    const NUM_CHUNKS: usize;
    const NUM_BUCKETS: usize;
    #[allow(dead_code)]
    const MAX_DIST: u32;

    fn get_chunk(&self, chunk_idx: usize) -> u16;
    fn hamming_distance(&self, other: &Self) -> u32;
    fn bit_width_per_chunk() -> usize;
}

// --- Implementation for 64-bit pHash ---
impl HammingHash for u64 {
    const NUM_CHUNKS: usize = 8;
    const NUM_BUCKETS: usize = 256;
    const MAX_DIST: u32 = MAX_SIMILARITY_64;

    #[inline(always)]
    fn get_chunk(&self, chunk_idx: usize) -> u16 {
        ((self >> (chunk_idx * 8)) & 0xFF) as u16
    }

    #[inline(always)]
    fn hamming_distance(&self, other: &Self) -> u32 {
        (*self ^ *other).count_ones()
    }

    fn bit_width_per_chunk() -> usize {
        8
    }
}

// --- Implementation for 256-bit PDQ ---
impl HammingHash for [u8; 32] {
    const NUM_CHUNKS: usize = 16;
    const NUM_BUCKETS: usize = 65536;
    const MAX_DIST: u32 = MAX_SIMILARITY_256;

    #[inline(always)]
    fn get_chunk(&self, chunk_idx: usize) -> u16 {
        let offset = chunk_idx * 2;
        u16::from_le_bytes([self[offset], self[offset + 1]])
    }

    #[inline(always)]
    fn hamming_distance(&self, other: &Self) -> u32 {
        self.iter().zip(other.iter()).map(|(a, b)| (a ^ b).count_ones()).sum()
    }

    fn bit_width_per_chunk() -> usize {
        16
    }
}

// --- The Index Struct (CSR Memory Layout) ---

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct DenseId(u32);

impl DenseId {
    #[inline(always)]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BucketId(u32);

pub struct MIHIndex<H: HammingHash> {
    db_hashes: Box<[H]>,
    offsets: Box<[u32]>,
    values: Box<[DenseId]>,
}

impl<H: HammingHash> MIHIndex<H> {
    pub fn new(hashes: Vec<H>) -> Self {
        let num_buckets = H::NUM_CHUNKS * H::NUM_BUCKETS;
        let mut offsets = vec![0u32; num_buckets + 1];
        let mut values = Vec::<DenseId>::new();

        // Count phase
        for (i, h) in hashes.iter().enumerate() {
            let dense = DenseId(i as u32);
            for k in 0..H::NUM_CHUNKS {
                let bucket = h.get_chunk(k) as usize;
                let flat = k * H::NUM_BUCKETS + bucket;
                offsets[flat + 1] += 1;
            }
            let _ = dense; // silence warnings if unused in some builds
        }

        // Prefix sum
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }

        values.resize(offsets.last().copied().unwrap() as usize, DenseId(0));

        // Fill phase
        let mut cursor = offsets.clone();
        for (i, h) in hashes.iter().enumerate() {
            let dense = DenseId(i as u32);
            for k in 0..H::NUM_CHUNKS {
                let bucket = h.get_chunk(k) as usize;
                let flat = k * H::NUM_BUCKETS + bucket;
                let pos = cursor[flat] as usize;
                values[pos] = dense;
                cursor[flat] += 1;
            }
        }

        Self {
            db_hashes: hashes.into_boxed_slice(),
            offsets: offsets.into_boxed_slice(),
            values: values.into_boxed_slice(),
        }
    }

    #[inline(always)]
    pub fn bucket(&self, chunk: usize, value: u16) -> &[DenseId] {
        let flat = chunk * H::NUM_BUCKETS + value as usize;
        let start = self.offsets[flat] as usize;
        let end = self.offsets[flat + 1] as usize;
        &self.values[start..end]
    }

    #[inline(always)]
    pub fn hash(&self, id: DenseId) -> &H {
        &self.db_hashes[id.index()]
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.db_hashes.len()
    }
}

// --- Helper: Sparse BitSet ---
pub struct SparseBitSet {
    data: Vec<u64>,
    dirty: Vec<usize>,
}

impl SparseBitSet {
    pub fn new(size: usize) -> Self {
        Self { data: vec![0; size.div_ceil(64)], dirty: Vec::with_capacity(512) }
    }

    #[inline(always)]
    pub fn set(&mut self, idx: usize) -> bool {
        debug_assert!(idx / 64 < self.data.len());
        let word_idx = idx / 64;
        let bit = idx % 64;
        let mask = 1u64 << bit;

        let word = &mut self.data[word_idx];
        let was_set = (*word & mask) != 0;

        if !was_set {
            if *word == 0 {
                self.dirty.push(word_idx);
            }
            *word |= mask;
        }

        was_set
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        for &idx in &self.dirty {
            self.data[idx] = 0;
        }
        self.dirty.clear();
    }
}

pub fn find_groups<H: HammingHash>(index: &MIHIndex<H>, max_dist: u32) -> Vec<Vec<u32>> {
    let n = index.len();
    let chunk_tolerance = max_dist / H::NUM_CHUNKS as u32;
    let bits_per_chunk = H::bit_width_per_chunk();

    let adjacency: Vec<Vec<u32>> = (0..n)
        .into_par_iter()
        .map_init(
            || (SparseBitSet::new(n), Vec::new()),
            |(visited, results), i| {
                visited.clear();
                results.clear();

                let query_hash = index.hash(DenseId(i as u32));

                for k in 0..H::NUM_CHUNKS {
                    let q_chunk = query_hash.get_chunk(k);

                    let check_bucket =
                        |val: u16, visited: &mut SparseBitSet, results: &mut Vec<u32>| {
                            let bucket = index.bucket(k, val);

                            for dense in bucket {
                                let dense_idx = dense.index();

                                if dense_idx == i {
                                    continue;
                                }

                                if visited.set(dense_idx) {
                                    continue;
                                }

                                let cand_hash = index.hash(*dense);
                                if query_hash.hamming_distance(cand_hash) <= max_dist {
                                    results.push(dense_idx as u32);
                                }
                            }
                        };

                    check_bucket(q_chunk, visited, results);

                    if chunk_tolerance >= 1 {
                        for bit in 0..bits_per_chunk {
                            check_bucket(q_chunk ^ (1 << bit), visited, results);
                        }
                    }
                }

                results.clone()
            },
        )
        .collect();

    // --- Greedy clustering ---
    let mut visited = vec![false; n];
    let mut groups = Vec::new();

    for i in 0..n {
        if visited[i] || adjacency[i].is_empty() {
            continue;
        }

        let mut group = vec![i as u32];
        visited[i] = true;

        for &neighbor in &adjacency[i] {
            let idx = neighbor as usize;
            if !visited[idx] {
                visited[idx] = true;
                group.push(neighbor);
            }
        }

        if group.len() > 1 {
            groups.push(group);
        }
    }

    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdqhash;
    use rand::prelude::*;
    use std::path::Path;
    use std::time::Instant;

    // --- TEST 1: High Similarity Logic (Generics check) ---
    #[test]
    fn test_high_similarity_support() {
        println!("\n--- TEST: High Similarity (Distance > 7) ---");

        // A. Test u64 (pHash)
        {
            println!("Testing u64 (pHash)...");
            let base = 0u64;
            // Create a target distance 12 away (0xFFF has 12 bits set)
            let target: u64 = 0xFFF;
            assert_eq!(target.count_ones(), 12);

            let hashes = vec![base, target];
            let index = MIHIndex::new(hashes);

            // Query using find_groups (wrapper around the internal query logic)
            // We use distance 12, which is high for pHash but valid for testing limits.
            let groups = find_groups(&index, 12);
            println!("Querying distance 12 (u64)... Found: {:?}", groups);

            // Expecting 1 group containing [0, 1] (order may vary)
            assert!(!groups.is_empty(), "Failed to find any groups for u64");
            let group = &groups[0];
            assert!(group.contains(&0) && group.contains(&1), "Group should contain both indices");
            assert_eq!(group.len(), 2, "Should find exactly 2 results (self + match)");
        }

        // B. Test [u8; 32] (PDQ)
        {
            println!("Testing [u8; 32] (PDQ)...");
            let base = [0u8; 32];
            let mut target = [0u8; 32];
            // Manually set 30 bits to create distance 30
            for i in 0..30 {
                target[i / 8] |= 1 << (i % 8);
            }

            let hashes = vec![base, target];
            let index = MIHIndex::new(hashes);

            let groups = find_groups(&index, 30);
            println!("Querying distance 30 (PDQ)... Found: {:?}", groups);

            assert!(!groups.is_empty(), "Failed to find any groups for PDQ");
            let group = &groups[0];
            assert!(
                group.contains(&0) && group.contains(&1),
                "PDQ Group should contain both indices"
            );
        }
    }

    // --- TEST 2: Performance & Accuracy (1 Million Items) ---
    #[test]
    fn test_1_million_images_performance() {
        let n = 1_000_000;
        println!("\n--- PERFORMANCE TEST: {} Images (Generic u64) ---", n);

        let mut rng = rand::rng();

        // 1. Generate Data
        println!("Generating {} random hashes...", n);
        let mut hashes: Vec<u64> = (0..n).map(|_| rng.random()).collect();

        // 2. Define a "Cluster" of 5 similar images
        // We use u64 here for simplicity, but the logic holds for [u8; 32] too
        let target = 0xABCD_1234_5678_90EF;
        let cluster_values = vec![
            target,
            target ^ 1,      // Dist 1
            target ^ 2,      // Dist 1
            target ^ 0x8000, // Dist 1
            target ^ 0x8001, // Dist 2 from target
        ];

        // 3. Inject them at RANDOM positions
        let mut injected_indices = Vec::new();
        while injected_indices.len() < cluster_values.len() {
            let idx = rng.random_range(0..n);
            if !injected_indices.contains(&idx) {
                injected_indices.push(idx);
            }
        }

        println!("Injecting similar hashes at random indices: {:?}", injected_indices);
        for (i, &idx) in injected_indices.iter().enumerate() {
            hashes[idx] = cluster_values[i];
        }

        // 4. Build Index
        let start_index = Instant::now();
        let index = MIHIndex::new(hashes.clone()); // Cloning just to simulate real usage
        let duration_index = start_index.elapsed();
        println!("Building Index took: {:.2?}", duration_index);

        // 5. Run Grouping (Parallel)
        let max_dist = 5;
        println!(
            "Grouping (max_dist={}) with {} threads...",
            max_dist,
            rayon::current_num_threads()
        );

        let start_group = Instant::now();
        let groups = find_groups(&index, max_dist);
        let duration_group = start_group.elapsed();

        println!("Grouping took:        {:.2?}", duration_group);
        println!("Total Time (Compute): {:.2?}", duration_index + duration_group);

        // 6. Verify Results
        println!("Found {} groups.", groups.len());

        // Find the group that contains the first injected index
        let target_idx = injected_indices[0] as u32;
        let found_group = groups.iter().find(|g| g.contains(&target_idx));

        assert!(found_group.is_some(), "The injected images were not found in any group!");

        let g = found_group.unwrap();
        println!("Found Target Group Size: {} (Indices: {:?})", g.len(), g);

        // Verify that ALL injected indices are present in this group
        for &expected_idx in &injected_indices {
            assert!(
                g.contains(&(expected_idx as u32)),
                "Group missing injected index {}",
                expected_idx
            );
        }
    }

    // --- TEST 3: Dihedral (Rotation/Flip) Robustness ---
    #[test]
    fn test_pdq_dihedral_robustness() {
        println!("\n--- TEST: Dihedral Robustness (Rotation/Flips) ---");

        // 1. Load Image
        let path = Path::new("./tests/bench.jpg");
        let img =
            image::open(path).expect("Failed to open './tests/bench.jpg'. Ensure file exists.");

        // 2. Generate features and Ground Truth Dihedral Hashes
        let (features, _) = pdqhash::generate_pdq_features(&img)
            .expect("Failed to generate features for original image");

        let dihedral_hashes = features.generate_dihedral_hashes();
        println!("Generated {} dihedral hashes from original features.", dihedral_hashes.len());

        // 3. Define the Physical Transformations
        // We will perform these ops on pixels and verify the result matches one of the dihedral hashes.
        let transformations = vec![
            ("Original", img.clone()),
            ("Rotate 90", img.rotate90()),
            ("Rotate 180", img.rotate180()),
            ("Rotate 270", img.rotate270()),
            ("Flip Horizontal", img.fliph()),
            ("Flip Vertical", img.flipv()),
            ("Transpose (Rot90 + FlipH)", img.rotate90().fliph()), // Transpose approximation
            ("Transverse (Rot90 + FlipV)", img.rotate90().flipv()), // Transverse approximation
        ];

        // 4. Test each transformation
        for (name, transformed_img) in transformations {
            // Generate hash for the physically transformed image
            let (hash_bytes, _) =
                pdqhash::generate_pdq(&transformed_img).expect("Failed to hash transformed image");

            // Find best match in the dihedral set
            let mut min_dist = u32::MAX;
            let mut best_idx = 0;

            for (i, ground_truth) in dihedral_hashes.iter().enumerate() {
                let dist = hash_bytes.hamming_distance(ground_truth);
                if dist < min_dist {
                    min_dist = dist;
                    best_idx = i;
                }
            }

            println!(
                "Transform: {:<25} | Best Match Index: {} | Hamming Distance: {}",
                name, best_idx, min_dist
            );

            // Pixel-domain rotation causes resampling artifacts that can flip bits.
            // A distance of < 20 is still considered a "Match" in PDQ terms (threshold is usually ~30-60).
            let tolerance = 22;

            assert!(
                min_dist <= tolerance,
                "FAIL: Transform '{}' resulted in distance {} (expected <= {})",
                name,
                min_dist,
                tolerance
            );
        }

        println!("PASSED: All physical transformations matched the computed dihedral set.");
    }
}
