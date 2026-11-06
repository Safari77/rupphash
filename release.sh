#!/bin/sh
cargo fmt --check && \
    cargo clippy --all-targets --all-features && \
    cargo deny check && \
    cargo vet --locked && \
    cargo build --release --bin phdupes && \
    cargo +nightly udeps --all-targets && \
    cargo test --release && \
    ./target/release/phdupes --version && \
    cargo release patch --no-publish --execute

# Eventually:
# RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
# cargo clippy --all-targets --all-features -- -D warnings

# vetting if feeling lazy:
# cargo vet regenerate exemptions
