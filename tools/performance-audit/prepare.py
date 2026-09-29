#!/usr/bin/env python3
"""Generate a manifest variant without building or editing the dependency checkout."""
import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--name", required=True)
p.add_argument("--source-root", required=True, type=Path)
p.add_argument("--api-mode", choices=("upstream", "old", "gated"), required=True)
args = p.parse_args()
if not args.name or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for c in args.name):
    p.error("name must contain lowercase letters, digits, hyphens or underscores")
source = args.source_root.resolve()
crates = {
    "lightyear_core": "crates/core/core",
    "lightyear_link": "crates/io/link",
    "lightyear_udp": "crates/io/udp",
    "lightyear_transport": "crates/transport/transport",
}
for path in crates.values():
    if not (source / path / "Cargo.toml").is_file():
        p.error(f"missing crate manifest: {source / path}")
variant = ROOT / "variants" / args.name
variant.mkdir(parents=True, exist_ok=True)
udp_feature = '["lightyear_udp/send_observation"]' if args.api_mode == "gated" else "[]"
transport_feature = '["lightyear_transport/packet_admission_observation"]' if args.api_mode == "gated" else "[]"
manifest = f'''[package]
name = "lightyear-performance-audit"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]

[[bin]]
name = "performance-audit"
path = {json.dumps(str(ROOT / 'src/main.rs'))}

[features]
default = []
udp_observation = {udp_feature}
transport_observation = {transport_feature}

[dependencies]
bevy_app = {{ version = "=0.19.1", default-features = false }}
bevy_ecs = {{ version = "=0.19.1", default-features = false, features = ["std"] }}
bevy_time = {{ version = "=0.19.1", default-features = false }}
bytes = {{ version = "1.8", default-features = false }}
aeronet_io = {{ version = "0.21", default-features = false }}
'''
for name, path in crates.items():
    features = ', features = ["server"]' if name == "lightyear_udp" else ""
    manifest += f'{name} = {{ path = {json.dumps(str(source / path))}{features} }}\n'
manifest += '''
[profile.release]
opt-level = 3
debug = false
codegen-units = 1
lto = "thin"
'''
(variant / "Cargo.toml").write_text(manifest)
reference_lock = ROOT / "Cargo.lock.reference"
if reference_lock.exists() and not (variant / "Cargo.lock").exists():
    (variant / "Cargo.lock").write_bytes(reference_lock.read_bytes())
print(json.dumps({
    "manifest": str(variant / "Cargo.toml"), "source_root": str(source),
    "api_mode": args.api_mode,
    "harness_sha256": hashlib.sha256((ROOT / "src/main.rs").read_bytes()).hexdigest(),
    "feature_note": "upstream must not enable observation features; old APIs use local flags without dependency feature forwarding",
}))
