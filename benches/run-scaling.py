#!/usr/bin/env python3
"""Paired public-API measurements against the pinned FusionCache package."""
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys


def execute(command, cwd, env, stdout=None):
    result = subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True)
    if stdout is not None:
        stdout.write_text(result.stdout)
        stdout.with_suffix(".stderr").write_text(result.stderr)
    if result.returncode:
        sys.stderr.write(result.stderr)
        sys.stderr.write(result.stdout)
        raise SystemExit(f"Failed ({result.returncode}): {' '.join(map(str, command))}")
    return result


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sources(root):
    paths = [root / "Cargo.toml", root / "Cargo.lock"]
    for directory, suffix in [("src", ".rs"), ("benches", ".rs"), ("benches/fusioncache", ".cs")]:
        paths.extend((root / directory).rglob(f"*{suffix}"))
    paths.extend([root / "benches/fusioncache/FusionBench.csproj", root / "benches/fusioncache/packages.lock.json"])
    return {str(path.relative_to(root)): digest(path) for path in sorted(set(paths))}


def measurements(output, allocation_column):
    rows = {}
    for row in csv.DictReader(io.StringIO(output)):
        key = (row["scenario"], int(row["threads"]))
        if key in rows:
            raise SystemExit(f"Duplicate measurement: {key}")
        rows[key] = {
            "ns": float(row["ns_per_op"]),
            "operations": int(row["operations"]),
            "allocated": int(row[allocation_column]),
        }
        if rows[key]["ns"] <= 0 or rows[key]["operations"] <= 0:
            raise SystemExit(f"Invalid measurement: {row}")
    expected = {(kind, count) for kind in ["same", "distinct"] for count in [1, 2, 4, 8]}
    expected.update({("sync", 1), ("set", 1), ("cold", 1)})
    if rows.keys() != expected:
        raise SystemExit(f"Measurement scenarios differ: {rows.keys() ^ expected}")
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", type=int, default=7)
    parser.add_argument("--gate", choices=["report", "hot", "cold", "all"], default="hot")
    parser.add_argument("--output", type=Path, default=Path(os.environ.get("TMPDIR", "/tmp")) / "amalgam-scaling")
    args = parser.parse_args()
    if args.pairs < 3:
        parser.error("At least three alternating pairs are required")
    root = Path(__file__).resolve().parent.parent
    output = args.output.resolve()
    if output == root or root in output.parents:
        parser.error("Keep generated artifacts outside the checkout")
    output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env.setdefault("CARGO_TARGET_DIR", str(Path(os.environ.get("TMPDIR", "/tmp")) / "amalgam-build-shared"))
    env.update(CARGO_INCREMENTAL="0", CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0")
    # Measure fully optimized JIT code without a tier transition inside a timed loop.
    # This is shared by every reference run, including counterbalanced pairs.
    env["DOTNET_TieredCompilation"] = "0"
    build = execute(["cargo", "build", "--locked", "--release", "--bench", "scaling", "--message-format=json"], root, env, output / "cargo-build.jsonl")
    artifacts = [json.loads(line) for line in build.stdout.splitlines() if line.startswith("{")]
    binaries = [row["executable"] for row in artifacts if row.get("reason") == "compiler-artifact" and row.get("target", {}).get("name") == "scaling" and row.get("executable")]
    if len(binaries) != 1:
        raise SystemExit("Cargo did not identify exactly one scaling executable")
    binary = Path(binaries[0])
    project = root / "benches/fusioncache/FusionBench.csproj"
    properties = [f"-p:BaseIntermediateOutputPath={output / 'dotnet-obj'}/", f"-p:MSBuildProjectExtensionsPath={output / 'dotnet-obj'}/"]
    execute(["dotnet", "restore", str(project), "--locked-mode", *properties], root, env, output / "dotnet-restore.log")
    execute(["dotnet", "build", str(project), "--no-restore", "-c", "Release", "-o", str(output / "dotnet-bin"), *properties], root, env, output / "dotnet-build.log")
    reference = output / "dotnet-bin/FusionBench.dll"
    frozen = sources(root)
    records = {"rust": [], "fusion": []}
    commands = {"rust": [str(binary)], "fusion": ["dotnet", str(reference)]}
    identity = None
    for pair in range(args.pairs):
        order = ["rust", "fusion"] if pair % 2 == 0 else ["fusion", "rust"]
        for label in order:
            print(f"Pair {pair + 1}/{args.pairs}: {label}", file=sys.stderr, flush=True)
            result = execute(commands[label], root, env, output / f"pair-{pair + 1}-{label}.csv")
            records[label].append(measurements(result.stdout, "allocations" if label == "rust" else "allocated_bytes"))
            if label == "fusion":
                lines = result.stderr.strip().splitlines()
                if len(lines) < 3 or not lines[0].startswith("2.9.0+"):
                    raise SystemExit("The reference is not the released FusionCache 2.9.0 package")
                current = lines[:3]
                if identity is not None and current != identity:
                    raise SystemExit("The reference package or runtime changed during measurement")
                identity = current
        if sources(root) != frozen:
            raise SystemExit("Sources changed during the paired measurement")
    rows = []
    failures = []
    for key in records["rust"][0]:
        rust = [trial[key] for trial in records["rust"]]
        fusion = [trial[key] for trial in records["fusion"]]
        if {trial["operations"] for trial in rust + fusion} != {rust[0]["operations"]}:
            raise SystemExit(f"Operation counts differ: {key}")
        ns = {label: statistics.median(trial[key]["ns"] for trial in records[label]) for label in records}
        allocations = statistics.median(row["allocated"] / row["operations"] for row in rust)
        if key[0] in {"same", "distinct", "sync"} and any(row["allocated"] for row in rust):
            failures.append(f"{key}: warmed Rust hit allocated")
        rows.append({
            "scenario": key[0], "threads": key[1], "rust_ns": ns["rust"], "fusion_ns": ns["fusion"],
            "rust_over_fusion": ns["rust"] / ns["fusion"], "rust_allocations_per_op": allocations,
            "rust_range_ns": [min(row["ns"] for row in rust), max(row["ns"] for row in rust)],
            "fusion_range_ns": [min(row["ns"] for row in fusion), max(row["ns"] for row in fusion)],
        })
    by_key = {(row["scenario"], row["threads"]): row for row in rows}
    if args.gate != "report":
        limits = {("same", 1): 0.5, ("distinct", 1): 0.5, ("sync", 1): 0.5, ("same", 8): 0.5, ("distinct", 8): 0.75}
        if args.gate in {"cold", "all"}:
            limits[("cold", 1)] = 0.75
            if by_key[("cold", 1)]["rust_allocations_per_op"] > 6:
                failures.append("cold: more than six allocations per operation")
        if args.gate == "all":
            limits[("set", 1)] = 0.75
            if by_key[("set", 1)]["rust_allocations_per_op"] > 3:
                failures.append("set: more than three allocations per operation")
        for key, limit in limits.items():
            if by_key[key]["rust_over_fusion"] > limit:
                failures.append(f"{key}: {by_key[key]['rust_over_fusion']:.3f} x FC exceeds {limit:.2f}")
    cpu_count = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count() or 1
    scaling = by_key[("distinct", 1)]["rust_ns"] / by_key[("distinct", 8)]["rust_ns"]
    # An oversubscribed hosted runner cannot demonstrate eight-core scaling.
    # It still runs all eight scenarios and checks the same relative FC budgets.
    scaling_limit = min(6, 0.75 * cpu_count)
    if args.gate != "report" and scaling < scaling_limit:
        failures.append(f"distinct scaling: {scaling:.2f} below {scaling_limit:.2f} for {cpu_count} available CPUs")
    report = {
        "gate": args.gate, "pairs": args.pairs, "environment": {
            "platform": platform.platform(), "available_cpus": cpu_count,
            "rust": execute(["rustc", "--version", "--verbose"], root, env).stdout.strip(),
            "dotnet": execute(["dotnet", "--version"], root, env).stdout.strip(),
            "dotnet_tiered_compilation": "0", "fusion_identity": identity,
        }, "sources_sha256": frozen, "binaries_sha256": {"rust": digest(binary), "fusion_fixture": digest(reference)},
        "distinct_scaling": scaling, "scaling_limit": scaling_limit,
        "eight_core_scaling_verified": cpu_count >= 8 and scaling >= 6,
        "measurements": rows, "failures": failures,
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print("scenario threads Rust_ns FC_ns Rust/FC Rust_alloc/op")
    for row in rows:
        print(f"{row['scenario']:8} {row['threads']:7} {row['rust_ns']:7.2f} {row['fusion_ns']:7.2f} {row['rust_over_fusion']:7.3f} {row['rust_allocations_per_op']:13.3f}")
    print(f"Distinct scaling: {scaling:.2f}x; required {scaling_limit:.2f}x on {cpu_count} available CPUs")
    if failures:
        raise SystemExit("Gate failed:\n" + "\n".join(failures))


if __name__ == "__main__":
    main()
