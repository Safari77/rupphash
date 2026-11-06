use std::path::Path;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use image::{DynamicImage, Rgb, RgbImage};
use rand::prelude::*;
use rupphash::{hamminghash::HammingHash, pdqhash};

fn bench_hamming_distance(c: &mut Criterion) {
    let mut group = c.benchmark_group("hamming_distance");
    let mut rng = rand::rng();

    // --- 1. Single-Pair Comparisons ---
    let h1_64: u64 = rng.random();
    let h2_64: u64 = rng.random();
    group.bench_function("u64_single", |b| {
        b.iter(|| std::hint::black_box(h1_64).hamming_distance(std::hint::black_box(&h2_64)))
    });

    let mut h1_256 = [0u8; 32];
    let mut h2_256 = [0u8; 32];
    rng.fill(&mut h1_256);
    rng.fill(&mut h2_256);
    group.bench_function("pdq_256_single", |b| {
        b.iter(|| std::hint::black_box(h1_256).hamming_distance(std::hint::black_box(&h2_256)))
    });

    // --- 2. Batch Linear Scan (10,000 hashes) ---
    const BATCH_SIZE: usize = 10_000;
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    let batch_64: Vec<u64> = (0..BATCH_SIZE).map(|_| rng.random()).collect();
    group.bench_with_input(
        BenchmarkId::new("u64_batch_scan", BATCH_SIZE),
        &batch_64,
        |b, hashes| {
            b.iter(|| {
                let mut sum = 0u64;
                for h in hashes {
                    sum += std::hint::black_box(h1_64).hamming_distance(h) as u64;
                }
                std::hint::black_box(sum)
            })
        },
    );

    let batch_256: Vec<[u8; 32]> = (0..BATCH_SIZE)
        .map(|_| {
            let mut val = [0u8; 32];
            rng.fill(&mut val);
            val
        })
        .collect();

    group.bench_with_input(
        BenchmarkId::new("pdq_256_batch_scan", BATCH_SIZE),
        &batch_256,
        |b, hashes| {
            b.iter(|| {
                let mut sum = 0u64;
                for h in hashes {
                    sum += std::hint::black_box(h1_256).hamming_distance(h) as u64;
                }
                std::hint::black_box(sum)
            })
        },
    );

    group.finish();
}

fn bench_generate_pdq(c: &mut Criterion) {
    let mut group = c.benchmark_group("generate_pdq");

    // Load benchmark image if present; fallback to a synthetic 1024x768 RGB image
    let path = Path::new("./tests/bench.jpg");
    let img = if path.exists() {
        image::open(path).expect("Failed to open './tests/bench.jpg'")
    } else {
        DynamicImage::ImageRgb8(RgbImage::from_fn(1024, 768, |x, y| {
            Rgb([(x % 256) as u8, (y % 256) as u8, ((x + y) % 256) as u8])
        }))
    };

    // Full pipeline from DynamicImage (RGB -> Luma601 -> Downsample -> Filter -> DCT -> Hash)
    group.bench_function("rgb_image", |b| {
        b.iter(|| std::hint::black_box(pdqhash::generate_pdq(std::hint::black_box(&img))))
    });

    // Fast path when source is already a GrayImage (DynamicImage::ImageLuma8)
    let luma_img = DynamicImage::ImageLuma8(img.to_luma8());
    group.bench_function("luma8_image", |b| {
        b.iter(|| std::hint::black_box(pdqhash::generate_pdq(std::hint::black_box(&luma_img))))
    });

    group.finish();
}

criterion_group!(benches, bench_hamming_distance, bench_generate_pdq);
criterion_main!(benches);
