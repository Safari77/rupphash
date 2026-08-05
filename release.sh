#!/bin/sh
cargo build --release --bin phdupes && cargo +nightly udeps --all-targets && \
	cargo test --release && ./target/release/phdupes --version && cargo release patch --no-publish --execute
