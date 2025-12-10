use rayon::prelude::*;

// 15 bits for 64-bit hash (approx 23% difference)
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

    fn bit_width_per_chunk() -> usize { 8 }
}

// --- Implementation for 256-bit PDQ ---
impl HammingHash for [u8; 32] {
    const NUM_CHUNKS: usize = 16;
    const NUM_BUCKETS: usize = 65536;
    const MAX_DIST: u32 = MAX_SIMILARITY_256;

    #[inline(always)]
    fn get_chunk(&self, chunk_idx: usize) -> u16 {
        let offset = chunk_idx * 2;
        u16::from_le_bytes([self[offset], self[offset+1]])
    }

    #[inline(always)]
    fn hamming_distance(&self, other: &Self) -> u32 {
        let mut dist = 0;
        let a_u64 = unsafe { std::mem::transmute::<&[u8; 32], &[u64; 4]>(self) };
        let b_u64 = unsafe { std::mem::transmute::<&[u8; 32], &[u64; 4]>(other) };
        for i in 0..4 {
            dist += (a_u64[i] ^ b_u64[i]).count_ones();
        }
        dist
    }

    fn bit_width_per_chunk() -> usize { 16 }
}

// --- The Index Struct (CSR Memory Layout) ---
pub struct MIHIndex<H: HammingHash> {
    pub values: Vec<u32>,
    pub offsets: Vec<u32>,
    pub db_hashes: Vec<H>,
}

impl<H: HammingHash> MIHIndex<H> {
    pub fn new(hashes: Vec<H>) -> Self {
        let num_items = hashes.len();
        let total_buckets = H::NUM_CHUNKS * H::NUM_BUCKETS;

        // 1. Histogram
        let mut counts = vec![0u32; total_buckets];
        for hash in &hashes {
            for k in 0..H::NUM_CHUNKS {
                let val = hash.get_chunk(k);
                let flat_idx = (k * H::NUM_BUCKETS) + val as usize;
                counts[flat_idx] += 1;
            }
        }

        // 2. Prefix Sum
        let mut offsets = vec![0u32; total_buckets + 1];
        let mut running_sum = 0;
        for i in 0..total_buckets {
            offsets[i] = running_sum;
            running_sum += counts[i];
        }
        offsets[total_buckets] = running_sum;

        // 3. Fill Values
        let mut write_pos = offsets.clone();
        let mut values = vec![0u32; num_items * H::NUM_CHUNKS];

        for (id, hash) in hashes.iter().enumerate() {
            let id_u32 = id as u32;
            for k in 0..H::NUM_CHUNKS {
                let val = hash.get_chunk(k);
                let flat_idx = (k * H::NUM_BUCKETS) + val as usize;
                let pos = write_pos[flat_idx];
                values[pos as usize] = id_u32;
                write_pos[flat_idx] += 1;
            }
        }

        MIHIndex { values, offsets, db_hashes: hashes }
    }
}

// --- Helper: Sparse BitSet ---
pub struct SparseBitSet {
    data: Vec<u64>,
    dirty: Vec<usize>,
}
impl SparseBitSet {
    pub fn new(size: usize) -> Self {
        Self {
            data: vec![0; size.div_ceil(64)],
            dirty: Vec::with_capacity(512)
        }
    }
    #[inline(always)]
    pub fn set(&mut self, idx: usize) -> bool {
        let word_idx = idx / 64;
        let mask = 1 << (idx % 64);
        let word = unsafe { self.data.get_unchecked_mut(word_idx) };
        let is_set = (*word & mask) != 0;
        if !is_set {
            if *word == 0 { self.dirty.push(word_idx); }
            *word |= mask;
        }
        is_set
    }
    pub fn clear(&mut self) {
        for &idx in &self.dirty { unsafe { *self.data.get_unchecked_mut(idx) = 0; } }
        self.dirty.clear();
    }
}

// --- Main Grouping Function ---
pub fn find_groups<H: HammingHash>(index: &MIHIndex<H>, max_dist: u32) -> Vec<Vec<u32>> {
    let n = index.db_hashes.len();
    let chunk_tolerance = max_dist / (H::NUM_CHUNKS as u32);
    let bits_per_chunk = H::bit_width_per_chunk();

    // Step 1: Parallel Neighbor Discovery
    let adjacency: Vec<Vec<u32>> = index.db_hashes
        .par_iter()
        .enumerate()
        .map_init(
            || (SparseBitSet::new(n), Vec::new()),
            |(visited, results), (i, query_hash)| {
                visited.clear();
                results.clear();

                for k in 0..H::NUM_CHUNKS {
                    let q_chunk = query_hash.get_chunk(k);
                    let chunk_base = k * H::NUM_BUCKETS;

                    let mut check_bucket = |val: u16| {
                        let flat_idx = chunk_base + val as usize;
                        let start = unsafe { *index.offsets.get_unchecked(flat_idx) } as usize;
                        let end = unsafe { *index.offsets.get_unchecked(flat_idx + 1) } as usize;
                        let bucket = unsafe { index.values.get_unchecked(start..end) };

                        for &cand_id in bucket {
                            if cand_id as usize == i { continue; }

                            if !visited.set(cand_id as usize) {
                                let cand = unsafe { index.db_hashes.get_unchecked(cand_id as usize) };

                                // --- VALID DISTANCE CHECK IS HERE ---
                                if query_hash.hamming_distance(cand) <= max_dist {
                                    results.push(cand_id);
                                }
                            }
                        }
                    };

                    check_bucket(q_chunk);

                    if chunk_tolerance >= 1 {
                        for bit in 0..bits_per_chunk {
                            check_bucket(q_chunk ^ (1 << bit));
                        }
                    }
                }
                results.clone()
            }
        )
        .collect();

    // Step 2: Greedy Clustering
    let mut visited = vec![false; n];
    let mut groups = Vec::new();

    for i in 0..n {
        if visited[i] { continue; }
        if adjacency[i].is_empty() { continue; }

        let mut group = vec![i as u32];
        visited[i] = true;

        for &neighbor in &adjacency[i] {
            if !visited[neighbor as usize] {
                visited[neighbor as usize] = true;
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
    use rand::prelude::*;
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
                target[i/8] |= 1 << (i%8);
            }

            let hashes = vec![base, target];
            let index = MIHIndex::new(hashes);

            let groups = find_groups(&index, 30);
            println!("Querying distance 30 (PDQ)... Found: {:?}", groups);

            assert!(!groups.is_empty(), "Failed to find any groups for PDQ");
            let group = &groups[0];
            assert!(group.contains(&0) && group.contains(&1), "PDQ Group should contain both indices");
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
            target ^ 1,          // Dist 1
            target ^ 2,          // Dist 1
            target ^ 0x8000,     // Dist 1
            target ^ 0x8001,     // Dist 2 from target
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
        println!("Grouping (max_dist={}) with {} threads...", max_dist, rayon::current_num_threads());

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
            assert!(g.contains(&(expected_idx as u32)), "Group missing injected index {}", expected_idx);
        }
    }
}
