#!/usr/bin/env python3
"""Instrument the exact primitive, without setting cfg(loom) on dependencies."""
import json
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
env = dict(os.environ)
# cfg(loom) is a final-target rustc argument, deliberately not global RUSTFLAGS.
command = ["cargo", "rustc", "--locked", "--test", "reader_slots_model",
           "--message-format=json", "--", "--cfg", "loom"]
build = subprocess.run(command, cwd=root, env=env, text=True, capture_output=True)
sys.stderr.write(build.stderr)
artifacts = [json.loads(line) for line in build.stdout.splitlines() if line.startswith("{")]
for artifact in artifacts:
    if artifact.get("reason") == "compiler-message":
        sys.stderr.write(artifact["message"].get("rendered") or "")
if build.returncode:
    raise SystemExit(build.returncode)
binaries = [row["executable"] for row in artifacts
            if row.get("reason") == "compiler-artifact"
            and row.get("target", {}).get("name") == "reader_slots_model"
            and row.get("executable")]
if len(binaries) != 1:
    raise SystemExit("Expected exactly one actual ReaderSlots model executable")
raise SystemExit(subprocess.run([binaries[0], "--test-threads=1"], cwd=root, env=env).returncode)
