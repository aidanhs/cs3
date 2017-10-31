#!/bin/bash
set -o errexit;set -o pipefail;set -o nounset;set -o xtrace;

OPENSSL_DIR=/usr OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu OPENSSL_STATIC=1 PATH=$PATH:'/tools/rust/bin' CC='/tools/clang/bin/clang' RUST_BACKTRACE=1 RUSTFLAGS='-Z no-landing-pads' cargo build --release
