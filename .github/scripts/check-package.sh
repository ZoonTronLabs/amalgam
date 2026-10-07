#!/usr/bin/env bash
set -euo pipefail

amalgam_repo_root="$(git rev-parse --show-toplevel)"
amalgam_package_dir="$(mktemp -d)"
trap 'rm -rf "$amalgam_package_dir"' EXIT
cd "$amalgam_repo_root"

cargo package --locked --all-features
python3 .github/scripts/check-package.py "$amalgam_package_dir"

for resolution in fresh historic; do
  amalgam_consumer_dir="$amalgam_package_dir/consumer-$resolution"
  mkdir -p "$amalgam_consumer_dir/src"
  cp .github/fixtures/package-consumer.rs "$amalgam_consumer_dir/src/main.rs"
  python3 - "$amalgam_package_dir" "$amalgam_consumer_dir" <<'PY'
import json, pathlib, sys
archive = pathlib.Path(sys.argv[1])
consumer = pathlib.Path(sys.argv[2])
package = json.loads((archive / 'package.json').read_text())
manifest = f'''[package]
name = "amalgam-package-consumer"
version = "0.1.0"
edition = "2024"
rust-version = "1.88"

[dependencies]
amalgam = {{ package = "amalgam-cache", version = "{package['version']}", path = {json.dumps(package['source'])} }}
tokio = {{ version = "1.52.3", features = ["macros", "rt-multi-thread"] }}
'''
(consumer / 'Cargo.toml').write_text(manifest)
PY
  if [[ "$resolution" == historic ]]; then
    cp .github/fixtures/consumer-Cargo.lock "$amalgam_consumer_dir/Cargo.lock"
  fi
  cargo run --manifest-path "$amalgam_consumer_dir/Cargo.toml"
  # Persist full in the manifest so later metadata/README commands retain its lock graph.
  python3 - "$amalgam_consumer_dir/Cargo.toml" <<'PY_FULL'
import pathlib, sys
manifest = pathlib.Path(sys.argv[1])
text = manifest.read_text()
# Match the dependency line independently of the prepared source release number.
lines = text.splitlines()
for index, line in enumerate(lines):
    if line.startswith('amalgam = {'):
        lines[index] = line[:-2] + ', features = ["full"] }'
        break
else:
    sys.exit('Packaged dependency declaration is missing')
manifest.write_text('\n'.join(lines) + '\n')
PY_FULL
  cargo run --manifest-path "$amalgam_consumer_dir/Cargo.toml" --all-features
  python3 - "$amalgam_package_dir" "$amalgam_consumer_dir" <<'PY_README'
import json, pathlib, re, sys
package = json.loads((pathlib.Path(sys.argv[1]) / 'package.json').read_text())
readme = (pathlib.Path(package['source']) / 'README.md').read_text()
section = readme.split('## Basic use', 1)[1].split('\n## ', 1)[0]
blocks = re.findall(r'^```rust\s*\n(.*?)^```\s*$', section, re.M | re.S)
if len(blocks) != 1:
    sys.exit('Expected exactly one executable README basic-use Rust block')
(pathlib.Path(sys.argv[2]) / 'src/main.rs').write_text(blocks[0])
PY_README
  cargo run --manifest-path "$amalgam_consumer_dir/Cargo.toml" --all-features --features amalgam/full
  python3 - "$amalgam_package_dir" "$amalgam_consumer_dir" <<'PY_SYNC'
import json, pathlib, re, sys
package = json.loads((pathlib.Path(sys.argv[1]) / 'package.json').read_text())
document = (pathlib.Path(package['source']) / 'docs/SYNC.md').read_text()
blocks = re.findall(r'^```rust\s*\n(.*?)^```\s*$', document, re.M | re.S)
if len(blocks) != 1:
    sys.exit('Expected exactly one executable SYNC Rust block')
(pathlib.Path(sys.argv[2]) / 'src/main.rs').write_text(blocks[0])
PY_SYNC
  cargo run --manifest-path "$amalgam_consumer_dir/Cargo.toml" --all-features --features amalgam/full
  python3 - "$amalgam_consumer_dir/Cargo.lock" <<'PY_LOCK'
import pathlib, sys, tomllib
lock = tomllib.loads(pathlib.Path(sys.argv[1]).read_text())
names = {package['name'] for package in lock['package']}
required = {'redis', 'opentelemetry', 'opentelemetry-otlp', 'h2', 'postcard', 'rmp-serde', 'metrics'}
missing = required - names
if missing:
    sys.exit('Full downstream lock is incomplete: ' + ', '.join(sorted(missing)))
print(f"Full downstream lock retains {len(lock['package'])} packages for advisory inspection")
PY_LOCK
  cargo audit --file "$amalgam_consumer_dir/Cargo.lock" --deny warnings
done
