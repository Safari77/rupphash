#!/bin/sh
cargo build --release --bin phdupes && cargo test --release && ./target/release/phdupes --version && cargo release patch --no-publish --execute
