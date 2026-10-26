#!/bin/sh
cargo deny check advisories && cargo vet --locked && \
	cargo build --release --bin phdupes && \
	cargo +nightly udeps --all-targets && \
	cargo test --release && \
	./target/release/phdupes --version && \
	cargo release patch --no-publish --execute
