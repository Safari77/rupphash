test performance of Multi-Index Hashing:

$ cargo test --release -- --nocapture
...
--- PERFORMANCE TEST: 1000000 Images (Generic u64) ---

--- TEST: High Similarity (Distance > 7) ---
Testing u64 (pHash)...
Generating 1000000 random hashes...
Querying distance 12 (u64)... Found: [[0, 1]]
Testing [u8; 32] (PDQ)...
Querying distance 30 (PDQ)... Found: [[0, 1]]
test hamminghash::tests::test_high_similarity_support ... ok
Injecting similar hashes at random indices: [773023, 836470, 728951, 945586, 558794]
Building Index took: 27.21ms
Grouping (max_dist=5) with 14 threads...
Grouping took:        12.22s
Total Time (Compute): 12.25s
Found 2 groups.
Found Target Group Size: 5 (Indices: [558794, 836470, 945586, 728951, 773023])
test hamminghash::tests::test_1_million_images_performance ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 12.25s



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


