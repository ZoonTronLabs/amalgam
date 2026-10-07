#!/usr/bin/env python3
"""Counterbalanced Rust diagnostics on the same runner and frozen workload."""
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess


def execute(command, root, env):
    result = subprocess.run(command, cwd=root, env=env, capture_output=True)
    if result.returncode:
        raise SystemExit(result.stderr.decode(errors="replace"))
    return result.stdout


def digest(value):
    return hashlib.sha256(value).hexdigest()


def source_identity(root):
    paths = [root / "Cargo.toml", root / "Cargo.lock", root / "benches/scaling.rs"]
    paths.extend(sorted((root / "src").rglob("*.rs")))
    return {str(path.relative_to(root)): digest(path.read_bytes()) for path in paths}


def build(label, root, env, output, args):
    command = ["cargo", f"+{args.toolchain}", "build", "--locked", "--release",
               "--bench", "scaling", "--message-format=json"]
    if args.rust_target:
        command.extend(["--target", args.rust_target])
    run = subprocess.run(command, cwd=root, env=env, text=True, capture_output=True)
    (output / f"{label}-build.jsonl").write_text(run.stdout)
    (output / f"{label}-build.stderr").write_text(run.stderr)
    if run.returncode:
        raise SystemExit(run.stderr)
    artifacts = [json.loads(line) for line in run.stdout.splitlines() if line.startswith("{")]
    binaries = [row["executable"] for row in artifacts
                if row.get("reason") == "compiler-artifact"
                and row.get("target", {}).get("name") == "scaling" and row.get("executable")]
    if len(binaries) != 1:
        raise SystemExit(f"Expected one scaling binary, got {binaries}")
    frozen = output / f"{label}-scaling"
    shutil.copy2(binaries[0], frozen)
    return frozen


def rows(text, case):
    values = list(csv.DictReader(io.StringIO(text)))
    keyed = {}
    for row in values:
        key = (row["scenario"], int(row.get("threads", 1)))
        if key in keyed:
            raise SystemExit(f"Duplicate scenario in {case}: {key}")
        if int(row["operations"]) <= 0 or float(row["ns_per_op"]) <= 0:
            raise SystemExit(f"Invalid measurement: {row}")
        keyed[key] = row
    if not keyed:
        raise SystemExit(f"No measurements in {case}")
    return keyed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, help="Available local Git commit/ref")
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--toolchain", default="1.88.0")
    parser.add_argument("--rust-target")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.pairs < 3:
        parser.error("at least three counterbalanced pairs are required")
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env.setdefault("CARGO_TARGET_DIR", "/tmp/amalgam-build-shared")
    env.update(CARGO_INCREMENTAL="0", CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0")
    baseline_ref = execute(["git", "rev-parse", "--verify", "--end-of-options", f"{args.baseline}^{{commit}}"], root, env).decode().strip()
    # API/workload/dependency changes need their own matched fixture rather than
    # silently attributing a different benchmark to a storage change.
    for relative in ["Cargo.toml", "Cargo.lock", "benches/scaling.rs"]:
        baseline = execute(["git", "show", f"{baseline_ref}:{relative}"], root, env)
        if baseline != (root / relative).read_bytes():
            raise SystemExit(f"Frozen workload/dependencies differ at {relative}")
    paths = execute(["git", "ls-tree", "-r", "--name-only", baseline_ref, "--", "src"], root, env).decode().splitlines()
    baseline_sources = {path: execute(["git", "show", f"{baseline_ref}:{path}"], root, env)
                        for path in paths if path.endswith(".rs")}
    current_sources = {str(path.relative_to(root)): path.read_bytes()
                       for path in (root / "src").rglob("*.rs")}
    current_identity = source_identity(root)
    conflicts = []
    try:
        for relative in current_sources.keys() - baseline_sources.keys():
            (root / relative).unlink()
        for relative, content in baseline_sources.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
        baseline_identity = source_identity(root)
        baseline_binary = build("baseline", root, env, output, args)
    finally:
        # Restore every unaffected file even if one concurrent editor changed a
        # path. Preserve that edit and stop before building a mixed candidate.
        for relative in current_sources.keys() | baseline_sources.keys():
            path = root / relative
            actual = path.read_bytes() if path.exists() else None
            if actual not in (baseline_sources.get(relative), current_sources.get(relative)):
                conflicts.append(relative)
                continue
            if relative in current_sources:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(current_sources[relative])
            elif path.exists():
                path.unlink()
        if conflicts:
            raise SystemExit(f"Concurrent edits preserved; diagnostic stopped: {conflicts}")
    current_binary = build("candidate", root, env, output, args)
    if source_identity(root) != current_identity:
        raise SystemExit("Source changed during candidate build")
    cases = {"metadata": ["--metadata-costs"], "mutations": ["--mutations"],
             "read": ["--api", "read"], "get-or-set": ["--api", "get-or-set"]}
    samples = {case: {"baseline": [], "candidate": []} for case in cases}
    for pair in range(1, args.pairs + 1):
        order = [("baseline", baseline_binary), ("candidate", current_binary)]
        if pair % 2 == 0:
            order.reverse()
        for case, arguments in cases.items():
            for label, binary in order:
                run = subprocess.run([str(binary), *arguments], cwd=root, env=env, text=True, capture_output=True)
                (output / f"{case}-{label}-{pair}.csv").write_text(run.stdout)
                (output / f"{case}-{label}-{pair}.stderr").write_text(run.stderr)
                if run.returncode:
                    raise SystemExit(run.stderr)
                samples[case][label].append(rows(run.stdout, case))
            print(f"Completed {case} pair {pair}", flush=True)
    measurements = []
    for case, paired in samples.items():
        expected = paired["baseline"][0]
        for sequence in paired.values():
            for sample in sequence:
                if sample.keys() != expected.keys():
                    raise SystemExit(f"Scenario mismatch in {case}")
                if any(sample[key]["operations"] != expected[key]["operations"] for key in expected):
                    raise SystemExit(f"Operation count mismatch in {case}")
        for key in expected:
            result = {"case": case, "scenario": key[0], "threads": key[1]}
            for label, sequence in paired.items():
                selected = [sample[key] for sample in sequence]
                timings = [float(row["ns_per_op"]) for row in selected]
                result[f"{label}_ns"] = statistics.median(timings)
                result[f"{label}_range_ns"] = [min(timings), max(timings)]
                result[f"{label}_allocations_per_op"] = statistics.median(
                    int(row["allocations"]) / int(row["operations"]) for row in selected)
            result["candidate_over_baseline"] = result["candidate_ns"] / result["baseline_ns"]
            measurements.append(result)
    identity = {"baseline_commit": baseline_ref, "platform": platform.platform(),
                "rust_target": args.rust_target, "rust": execute(["rustc", f"+{args.toolchain}", "-Vv"], root, env).decode(),
                "driver_sha256": digest(Path(__file__).read_bytes()),
                "baseline_sources_sha256": baseline_identity, "candidate_sources_sha256": current_identity,
                "binaries_sha256": {"baseline": digest(baseline_binary.read_bytes()),
                                    "candidate": digest(current_binary.read_bytes())}}
    report = {"identity": identity, "pairs": args.pairs, "measurements": measurements}
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    for measurement in measurements:
        print(json.dumps(measurement), flush=True)


if __name__ == "__main__":
    main()
