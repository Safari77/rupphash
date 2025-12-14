test performance of Multi-Index Hashing:

$ cargo test --release -- --nocapture
...

running 3 tests

--- PERFORMANCE TEST: 1000000 Images (Generic u64) ---
Generating 1000000 random hashes...

--- TEST: High Similarity (Distance > 7) ---
Testing u64 (pHash)...
Querying distance 12 (u64)... Found: [[0, 1]]
Testing [u8; 32] (PDQ)...
Querying distance 30 (PDQ)... Found: [[0, 1]]
test hamminghash::tests::test_high_similarity_support ... ok
Injecting similar hashes at random indices: [518780, 241878, 542465, 318989, 850151]
Building Index took: 26.95ms
Grouping (max_dist=5) with 14 threads...

--- TEST: Dihedral Robustness (Rotation/Flips) ---
Querying distance 12 (u64)... Found: [[0, 1]]
Testing [u8; 32] (PDQ)...
Querying distance 30 (PDQ)... Found: [[0, 1]]
test hamminghash::tests::test_high_similarity_support ... ok
Injecting similar hashes at random indices: [506066, 304664, 531336, 222969, 513496]
Generated 8 dihedral hashes from original features.
Building Index took: 25.99ms
Grouping (max_dist=5) with 14 threads...
Transform: Original                  | Best Match Index: 0 | Hamming Distance: 0
Transform: Rotate 90                 | Best Match Index: 1 | Hamming Distance: 2
Transform: Rotate 180                | Best Match Index: 2 | Hamming Distance: 18
Transform: Rotate 270                | Best Match Index: 3 | Hamming Distance: 20
Transform: Flip Horizontal           | Best Match Index: 4 | Hamming Distance: 20
Transform: Flip Vertical             | Best Match Index: 5 | Hamming Distance: 0
Transform: Transpose (Rot90 + FlipH) | Best Match Index: 6 | Hamming Distance: 0
Transform: Transverse (Rot90 + FlipV) | Best Match Index: 7 | Hamming Distance: 18
PASSED: All physical transformations matched the computed dihedral set.
test hamminghash::tests::test_pdq_dihedral_robustness ... ok

=== Benchmark Results ===
generate_pdq_features (100 iterations):
  Total time: 428.563759ms
  Avg time:   4.285637ms
generate_dihedral_hashes (30000 iterations):
  Total time: 295.707917ms
  Avg time:   9.856µs
=========================

test pdqhash::benchmarks::bench_pdq_performance ... ok
Grouping took:        12.27s
Total Time (Compute): 12.30s
Found 1 groups.
Found Target Group Size: 5 (Indices: [241878, 850151, 518780, 542465, 318989])
test hamminghash::tests::test_1_million_images_performance ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 12.31s



test against one other phash implementation:

$ time ./target/release/rupphash /home/safari/Documents/img/paris_1920.jpg
File: /home/safari/Documents/img/paris_1920.jpg
Standard pHash (Hex): deb1e20c136f983c
Standard pHash (Bin): 1101111010110001111000100000110000010011011011111001100000111100
Rot-Invariant Hash  : 8b1bb7a646c5cd96

real	0m0,005s
user	0m0,003s
sys	0m0,002s

$ time python ./py-imagehash.py /home/safari/Documents/img/paris_1920.jpg
File: /home/safari/Documents/img/paris_1920.jpg
Standard pHash (Hex): deb1e20c136f983c
Standard pHash (Bin): 1101111010110001111000100000110000010011011011111001100000111100

Rotational variations:
Rot   0°: deb1e20c136f983c  (Bin: 1101111010110001111000100000110000010011011011111001100000111100)
Rot  90°: b7f1309ec2c0f91a  (Bin: 1011011111110001001100001001111011000010110000001111100100011010)
Rot 180°: 8b1bb7a642c5cc96  (Bin: 1000101100011011101101111010011001000010110001011100110010010110)
Rot 270°: e25b6534976aacb0  (Bin: 1110001001011011011001010011010010010111011010101010110010110000)
Min Hash: 8b1bb7a642c5cc96

real	0m0,204s
user	0m1,628s
sys	0m0,032s

$ calc  "popcnt(xor(0x8b1bb7a642c5cc96,0x8b1bb7a646c5cd96))"
	2


