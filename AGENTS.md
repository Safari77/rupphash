# AGENTS.md

## Repository Overview

`rupphash` (primary binary: `phdupes`) is a Rust 2024 duplicate image detector, metadata indexer, and image browser/viewer.

Key capabilities:
- **Duplicate & similarity detection**: Perceptual hashing via PDQ (`pdqhash`) with dihedral symmetry support and Multi-Index Hashing (`MIHIndex` in `hamminghash`), DCT pHash (`phash`), 16-bit raw pixel hashing (`--pixel-hash`), and whole-file BLAKE3 hashing.
- **Encrypted metadata cache**: Persistent `redb` database encrypted with XChaCha20Poly1305 (`src/db.rs`, configured via `phdupes.conf` in `src/configs.rs`).
- **Image, RAW & HDR support**: Standard formats, HEIC/AVIF (`libheif-rs`), JPEG XL (`jxl-oxide`), and camera RAW (`rsraw`), plus HDR-to-SDR/deep-color processing and 3D `.cube` LUTs (`src/hdr.rs`, `src/gui/image.rs`).
- **EXIF & derived geospatial search**: Full-text/range/regex search index (`src/search_index.rs`) over EXIF tags and derived attributes such as country/subdivision (`country-boundaries`), timezone (`tzf-rs`), and solar altitude/azimuth (`solar-positioning`, `src/position.rs`).
- **Interfaces**: GPU-accelerated desktop GUI (`eframe`/`egui`/`egui-wgpu` + `walkers` GPS map + `wgpu` slideshow transitions) and terminal UI (`ratatui`/`crossterm`).

---

## Build & Verification

- **NEVER pass `--release` to `cargo` commands** (including `cargo check`, `cargo clippy`, `cargo test`, and `cargo build`) unless explicitly instructed by the user.
- `[profile.release]` uses `lto = "fat"`, which takes ~5 minutes per build and wastes time/disk space on a separate target cache.
- Always use debug builds for verification:
  - Type-check: `cargo check --all-targets`
  - Lint: `cargo clippy --all-targets -- -D warnings`
  - Test: `cargo test`
  - Check deny: `cargo deny check`

---

## Repository Structure

### Crate Targets (`Cargo.toml`)
- **Library (`rupphash`, `src/lib.rs`)**: Core reusable modules:
  - `pdqhash.rs` — PDQ perceptual hash & dihedral hash generation
  - `hamminghash.rs` — `HammingHash` trait, `MIHIndex` multi-index hashing, `SparseBitSet`, and grouping
  - `image_features.rs` — Compact serialized `ImageFeatures` record
  - `exif_types.rs`, `exif_extract.rs`, `helper_exif.rs` — EXIF tag definitions, extraction, and derived location/time helpers
  - `position.rs` — Geospatial distance/bearing and solar position calculations
- **Main binary (`phdupes`, `src/phdupes.rs`)**: CLI (`clap`), orchestration, and application modules:
  - `configs.rs` — `phdupes.conf` TOML loading/saving and master key handling
  - `db.rs` — Encrypted `redb` cache, background DB writer, and ignored-group persistence
  - `scanner.rs` — Parallel directory traversal, image decoding, hashing, enrichment, and grouping
  - `search_index.rs` — In-memory inverted/numeric/range search index
  - `state.rs` — Shared navigation, marking, rename, and transform state
  - `fileops.rs` — File open/move/hardlink key helpers
  - `hdr.rs` — CICP detection and HDR/SDR color conversions
  - `raw_exif.rs` — RAW image EXIF/GPS/orientation extraction
  - `ui.rs` — `ratatui` TUI (`--use-tui`)
  - `gui/` — `egui` GUI (`mod.rs`, `app.rs`, `image.rs`, `gps_map.rs`, `slideshow.rs`, `dialogs.rs`, `fonts.rs`)
- **Additional binaries**:
  - `phash_test` (`src/phash_test.rs`, `src/phash.rs`) — DCT pHash testing utility
  - `position_test` (`src/position_test.rs`) — Solar/geospatial testing utility

### Other Key Paths
- `build.rs` — Embeds `APP_GIT_HASH` and `DEPS_INFO_PATH` (parsed from `Cargo.lock` plus optional `EXT_*` C-lib env vars).
- `vendor/egui-wgpu/` — Local patched `egui-wgpu` crate wired via `[patch.crates-io]` in `Cargo.toml`.
- `benches/benchmarks.rs` — Criterion benchmarks.
- `tests/` — Test image fixtures and metadata text files.
- `assets/` — Blue noise texture and optional fonts (`Sarasa-Regular.ttc` for `--features embed-fonts`).
- `deny.toml`, `supply-chain/` — `cargo-deny` and `cargo-vet` policy/audit configuration.
- `KEYS.txt` — GUI keyboard shortcuts reference.

---

## Toolchain, Build & Verification

- **Toolchain**: `rust-toolchain.toml` pins `nightly` with `rustfmt` and `rust-analyzer` components. Edition is `2024`.
- **System dependencies**: Building `phdupes` links C libraries including `libdav1d`, `libheif`, `libaom`, `libde265`, `libx265`, `libopenjp2`, and `libjpeg` (see `.github/workflows/pr.yml` for the Ubuntu package list).
- **Formatting**: `rustfmt.toml` uses nightly-only options (`group_imports = "StdExternalCrate"`, `imports_granularity = "Crate"`, `max_width = 100`, `trailing_comma = "Vertical"`).

Common commands (aligned with `release.sh`):
```bash
# Check / build
cargo check --locked
cargo build --release --bin phdupes
# Optional features:
#   --features embed-fonts   (embeds assets/fonts/Sarasa-Regular.ttc)
#   --features staticbuild   (static link for dav1d/heif/aom)

# Format & lint
cargo fmt --check
cargo clippy --all-targets --all-features

# Tests & benchmarks
cargo test
cargo test --release
cargo bench

# Dependency & supply-chain checks
cargo deny check
cargo vet --locked
cargo +nightly udeps --all-targets
```

---

## Coding Conventions

- Keep `rustfmt.toml` formatting clean (`cargo fmt`).
- `src/phdupes.rs` uses `jemallocator::Jemalloc` as the global allocator on non-Windows targets.
- Note that `[profile.dev]` in `Cargo.toml` sets `opt-level = 3` and `debug = true` for usable image/hashing performance during development.
- Long-running benchmark-style unit tests (such as `test_1_million_images_performance` in `src/hamminghash.rs`) are marked `#[ignore]`.
