use rayon::prelude::*;
use std::error::Error;

// Max supported distance is 15 (floor(15/8) = 1).
const NUM_CHUNKS: usize = 8;
const CHUNK_BITS: usize = 8;
const NUM_BUCKETS: usize = 1 << CHUNK_BITS; // 256

// Logic: With 8 chunks and 1-bit tolerance per chunk, max distance is 15.
// (16 would require 2-bit tolerance: 16/8 = 2).
pub const MAX_SIMILARITY: u32 = 15;

// The Thread-Safe MIH Index
#[derive(Debug)]
pub struct MIHIndex {
    pub db_hashes: Vec<u64>,
    tables: [Vec<Vec<u32>>; NUM_CHUNKS],
}

impl MIHIndex {
    pub fn new(input_hashes: Vec<u64>) -> Result<Self, Box<dyn Error>> {
        if input_hashes.is_empty() {
            return Err("Input hash vector cannot be empty.".into());
        }

        // Initialize empty tables
        let empty_table = vec![Vec::new(); NUM_BUCKETS];
        let mut tables: [Vec<Vec<u32>>; NUM_CHUNKS] = std::array::from_fn(|_| empty_table.clone());

        // Populate tables
        for (id, &hash) in input_hashes.iter().enumerate() {
            let id_u32 = id as u32;
            for k in 0..NUM_CHUNKS {
                let chunk_val = Self::get_chunk(hash, k);
                tables[k][chunk_val as usize].push(id_u32);
            }
        }

        Ok(MIHIndex {
            db_hashes: input_hashes,
            tables,
        })
    }

    #[inline(always)]
    fn get_chunk(hash: u64, chunk_idx: usize) -> u16 {
        // Mask is now 0xFF (8 bits)
        ((hash >> (chunk_idx * CHUNK_BITS)) & 0xFF) as u16
    }

    /// Optimized thread-safe query
    /// Checks Hamming distance immediately to avoid sorting massive candidate lists.
    pub fn query(&self, query_hash: u64, max_dist: u32) -> Vec<u32> {
        // Use a small capacity to avoid allocation for expected result count
        let mut results = Vec::with_capacity(10); 
        let chunk_tolerance = max_dist / (NUM_CHUNKS as u32);

        for k in 0..NUM_CHUNKS {
            let q_chunk = Self::get_chunk(query_hash, k);

            // Helper closure to process a specific bucket
            let mut process_bucket = |bucket_val: u16| {
                let bucket = &self.tables[k][bucket_val as usize];
                for &idx in bucket {
                    // Check distance *before* collecting/sorting
                    // Accessing db_hashes is a cache miss risk, but faster than sorting thousands
                    // of items
                    let candidate_hash = self.db_hashes[idx as usize];
                    if (candidate_hash ^ query_hash).count_ones() <= max_dist {
                        results.push(idx);
                    }
                }
            };

            // 1. Exact match bucket
            process_bucket(q_chunk);

            // 2. Neighbor buckets
            if chunk_tolerance >= 1 {
                for bit in 0..CHUNK_BITS {
                    let neighbor = q_chunk ^ (1 << bit);
                    process_bucket(neighbor);
                }
            }
        }

        // Deduplicate the *results* (which is tiny, usually < 5 items)
        // instead of the *candidates* (which is huge, ~30,000 items)
        if !results.is_empty() {
            results.sort_unstable();
            results.dedup();
        }

        results
    }
}

// --- Parallel Grouping Logic ---

pub fn find_groups_parallel(index: &MIHIndex, max_dist: u32) -> Vec<Vec<u32>> {
    let n = index.db_hashes.len();

    // STEP 1: Parallel Neighbor Discovery
    println!("Step 1: finding neighbors in parallel...");

    let adjacency: Vec<Vec<u32>> = (0..n)
        .into_par_iter()
        .map(|i| {
            let hash = index.db_hashes[i];
            let mut neighbors = index.query(hash, max_dist);
            // Remove self from neighbors list
            neighbors.retain(|&x| x != i as u32);
            neighbors
        })
        .collect();

    // STEP 2: Greedy Clustering (Star Topology)
    println!("Step 2: grouping (greedy strict mode)...");

    let mut visited = vec![false; n];
    let mut groups = Vec::new();

    for i in 0..n {
        if visited[i] { continue; }

        let neighbors = &adjacency[i];

        if neighbors.is_empty() {
            continue;
        }

        let mut group = Vec::new();
        group.push(i as u32);
        visited[i] = true;

        for &neighbor in neighbors {
            let n_idx = neighbor as usize;
            if !visited[n_idx] {
                visited[n_idx] = true;
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

    #[test]
    fn test_high_similarity_support() {
        println!("\n--- TEST: High Similarity (Distance > 7) ---");

        // 1. Create a baseline hash (all zeros)
        let base = 0u64;

        // 2. Create a "target" that is distance 12 away.
        let target: u64 = 0xFFF;
        assert_eq!(target.count_ones(), 12);

        let hashes = vec![base, target];
        let index = MIHIndex::new(hashes).expect("Failed to build index");

        // 3. Query for the target using the base with max_dist 12
        let results = index.query(base, 12);

        println!("Querying distance 12... Found: {:?}", results);

        // Should find both index 0 (self) and index 1 (target)
        assert!(results.contains(&1), "Failed to find match with distance 12!");
        assert_eq!(results.len(), 2, "Should find exactly 2 results (self + match)");
    }

    #[test]
    fn test_1_million_images_performance() {
        let n = 1_000_000;
        println!("\n--- PERFORMANCE TEST: {} Images ---", n);

        let mut rng = rand::rng();

        // 1. Generate Data
        println!("Generating {} random hashes...", n);
        let mut hashes: Vec<u64> = (0..n).map(|_| rng.random()).collect();

        // 2. Define a "Cluster" of 5 similar images
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
        let index = MIHIndex::new(hashes.clone()).expect("Failed to build index");
        let duration_index = start_index.elapsed();
        println!("Building Index took: {:.2?}", duration_index);

        // 5. Run Grouping (Parallel)
        let max_dist = 5;
        println!("Grouping (max_dist={}) with {} threads...", max_dist, rayon::current_num_threads());

        let start_group = Instant::now();
        let groups = find_groups_parallel(&index, max_dist);
        let duration_group = start_group.elapsed();

        println!("Grouping took:       {:.2?}", duration_group);
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
