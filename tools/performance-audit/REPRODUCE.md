# Portable reproduction from this evidence branch

This branch combines Drafts #1–#3 solely for validation; it is not an upstream merge.
The reference Cargo.lock pins the registry versions used for these results.

From a checkout of `geonwoo-jeong/lightyear` on `codex/performance-validation`:

```sh
git worktree add ../lightyear-baseline 320f825bbeeab62715f0158d07d03320fb1c9cd0
python3 tools/performance-audit/prepare.py --name baseline --source-root ../lightyear-baseline --api-mode upstream
python3 tools/performance-audit/prepare.py --name candidate --source-root . --api-mode gated
mkdir -p tools/performance-audit/binaries
export CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0
cargo build --release --locked --manifest-path tools/performance-audit/variants/baseline/Cargo.toml --target-dir tools/performance-audit/target
cp tools/performance-audit/target/release/performance-audit tools/performance-audit/binaries/baseline-final
cargo build --release --locked --manifest-path tools/performance-audit/variants/candidate/Cargo.toml --target-dir tools/performance-audit/target
cp tools/performance-audit/target/release/performance-audit tools/performance-audit/binaries/candidate-default-final
cargo build --release --locked --manifest-path tools/performance-audit/variants/candidate/Cargo.toml --target-dir tools/performance-audit/target --features udp_observation,transport_observation
cp tools/performance-audit/target/release/performance-audit tools/performance-audit/binaries/candidate-observed-final
cd tools/performance-audit
python3 -m unittest -v test_compare.py
python3 compare.py --baseline binaries/baseline-final --candidate binaries/candidate-default-final --iterations 2000 --repetitions 21 --require-within-threshold --output results/new-default.json
```

Finish all builds before timing. Select a new result path for each invocation;
existing evidence is never overwritten. See RESULTS.md for the longer precision
comparison and enabled-observer measurement. A nonzero performance-gate result
must remain visible; it is not permission to rerun until a passing result appears.

The optional old-draft comparison can be prepared from a checkout/archive of
`92b3003` using `--api-mode old`. Enable both local observation features when
building it; that mode does not forward the newly introduced crate feature names.
