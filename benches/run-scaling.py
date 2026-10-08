#!/usr/bin/env python3
"""Paired public-API measurements against the pinned FusionCache package."""
import argparse
import csv
import hashlib
import io
import json
import math
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
    paths = [root / "Cargo.toml", root / "Cargo.lock", root / "benches/run-scaling.py"]
    for directory, suffix in [("src", ".rs"), ("benches", ".rs"), ("benches/fusioncache", ".cs")]:
        paths.extend(path for path in (root / directory).rglob(f"*{suffix}")
                     if not {"obj", "bin"}.intersection(path.relative_to(root).parts))
    paths.extend([root / "benches/fusioncache/FusionBench.csproj", root / "benches/fusioncache/packages.lock.json"])
    return {str(path.relative_to(root)): digest(path) for path in sorted(set(paths))}


HOT_SCENARIOS = {(kind, count) for kind in ["same", "distinct"] for count in [1, 2, 4, 8]}
HOT_SCENARIOS.add(("sync", 1))
MUTATION_SCENARIOS = {("set", 1), ("cold", 1)}
DISTRIBUTED_SCENARIOS = {("l2_json", 1)}


def measurements(output, allocation_column, expected):
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
        if not math.isfinite(rows[key]["ns"]) or rows[key]["ns"] <= 0 or rows[key]["operations"] <= 0:
            raise SystemExit(f"Invalid measurement: {row}")
    if rows.keys() != expected:
        raise SystemExit(f"Measurement scenarios differ: {rows.keys() ^ expected}")
    return rows


def default_jit_environment(inherited):
    # Keep runtime discovery (DOTNET_ROOT, PATH, etc.), but remove inherited
    # JIT overrides so the primary reference uses actual runtime defaults.
    prefixes = ("Tiered", "TC_", "Jit", "ReadyToRun", "OSR")
    removed = sorted(key for key in inherited
                     if key.startswith(("DOTNET_", "COMPlus_"))
                     and key.split("_", 1)[1].startswith(prefixes))
    return {key: value for key, value in inherited.items() if key not in removed}, removed


def reference_environment(base):
    # FusionCache 2.9 compares keys culture-sensitively on hits and L2 reads, so
    # the process culture would otherwise set the reference cost: under ru-RU the
    # ICU collation made an M4 hit ~2.4x slower than under en-US. Measure the
    # culture services usually run with: invariant globalization.
    removed = sorted(key for key in base
                     if key.startswith(("DOTNET_SYSTEM_GLOBALIZATION_", "LC_", "LANG")))
    reference = {key: value for key, value in base.items() if key not in removed}
    reference["DOTNET_SYSTEM_GLOBALIZATION_INVARIANT"] = "1"
    return reference, removed


def warmup_records(stderr, fixture):
    records = [json.loads(line.removeprefix("warmup "))
               for line in stderr.splitlines() if line.startswith("warmup ")]
    labels = {
        "warm": {f"{scenario}:{workers}:{worker}" for scenario in ["same", "distinct"]
                 for workers in [1, 2, 4, 8] for worker in range(workers)} | {"sync"},
        "mutations": {"set", "cold"}, "distributed": {"l2_json"},
    }[fixture]
    if len(records) != len(labels) or {row["label"] for row in records} != labels:
        raise SystemExit(f"Incomplete warmup evidence for {fixture}")
    for row in records:
        samples = row["windows_ns"]
        if row["seconds"] < 3 or row["operations"] <= 0 or not samples:
            raise SystemExit(f"Invalid warmup evidence: {row['label']}")
        if any(not math.isfinite(ns) or ns <= 0 for ns in samples):
            raise SystemExit(f"Invalid settling samples: {row['label']}")
        stable = len(samples) >= 5 and max(samples[-5:]) / min(samples[-5:]) <= 1.10
        if row["stable"] is not stable:
            raise SystemExit(f"Inconsistent settling verdict: {row['label']}")
    return records


def cpu_topology(available):
    # Keep reported physical cores separate from logical scheduling capacity.
    # Unavailable topology is unknown, never proof of eight-core scaling.
    physical = None
    model = None
    if platform.system() == "Linux":
        affinity = os.sched_getaffinity(0) if hasattr(os, "sched_getaffinity") else set(range(available))
        try:
            rows = []
            for block in Path("/proc/cpuinfo").read_text().split("\n\n"):
                row = dict(line.split(":", 1) for line in block.splitlines() if ":" in line)
                row = {key.strip(): value.strip() for key, value in row.items()}
                if "processor" in row and int(row["processor"]) in affinity:
                    rows.append(row)
            if len(rows) == len(affinity) and all("physical id" in row and "core id" in row for row in rows):
                physical = len({(row["physical id"], row["core id"]) for row in rows})
            if rows:
                model = rows[0].get("model name")
        except (OSError, ValueError):
            pass
    elif platform.system() == "Darwin":
        try:
            result = subprocess.run(["sysctl", "-n", "hw.physicalcpu"], capture_output=True, text=True, check=True)
            physical = min(int(result.stdout.strip()), available)
        except (OSError, ValueError, subprocess.CalledProcessError):
            pass
    return {"physical_cores_available": physical, "logical_cpus_available": available, "model": model}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", type=int, default=7)
    parser.add_argument("--api", choices=["read", "get-or-set"], default="read")
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
    env, removed_jit_overrides = default_jit_environment(dict(os.environ))
    env.setdefault("CARGO_TARGET_DIR", str(Path(os.environ.get("TMPDIR", "/tmp")) / "amalgam-build-shared"))
    env.update(CARGO_INCREMENTAL="0", CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0")
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
    runtime_config = json.loads(reference.with_suffix(".runtimeconfig.json").read_text())
    properties = runtime_config["runtimeOptions"].get("configProperties", {})
    if any(key.startswith(("System.Runtime.Tiered", "System.Runtime.ReadyToRun")) for key in properties):
        raise SystemExit("The fixture embeds a JIT override rather than runtime defaults")
    frozen = sources(root)
    records = {"rust": [], "fusion": [], "fusion_no_tiering": []}
    warmups = {label: [] for label in records}
    commands = {"rust": [str(binary)], "fusion": ["dotnet", str(reference)],
                "fusion_no_tiering": ["dotnet", str(reference)]}
    reference_env, removed_locale_overrides = reference_environment(env)
    environments = {"rust": env, "fusion": reference_env,
                    "fusion_no_tiering": dict(reference_env, DOTNET_TieredCompilation="0")}
    fixtures = [
        ("warm", ["--api", args.api], HOT_SCENARIOS),
        ("mutations", ["--mutations"], MUTATION_SCENARIOS),
        ("distributed", ["--l2", "--api", args.api], DISTRIBUTED_SCENARIOS),
    ]
    identity = None
    for pair in range(args.pairs):
        # Rotate all six orderings; neither FC mode consistently runs first.
        orders = [("rust", "fusion", "fusion_no_tiering"),
                  ("fusion", "fusion_no_tiering", "rust"),
                  ("fusion_no_tiering", "rust", "fusion"),
                  ("fusion_no_tiering", "fusion", "rust"),
                  ("rust", "fusion_no_tiering", "fusion"),
                  ("fusion", "rust", "fusion_no_tiering")]
        order = orders[pair % len(orders)]
        for label in order:
            trial = {}
            settling = {}
            for fixture, arguments, expected in fixtures:
                print(f"Pair {pair + 1}/{args.pairs}: {label}/{fixture}", file=sys.stderr, flush=True)
                result = execute(commands[label] + arguments, root, environments[label], output / f"pair-{pair + 1}-{label}-{fixture}.csv")
                rows = measurements(result.stdout, "allocations" if label == "rust" else "allocated_bytes", expected)
                if not trial.keys().isdisjoint(rows):
                    raise SystemExit("Fixture scenarios overlap")
                trial.update(rows)
                settling[fixture] = warmup_records(result.stderr, fixture)
                if label != "rust":
                    lines = result.stderr.strip().splitlines()
                    if len(lines) < 4 or not lines[0].startswith("2.9.0+"):
                        raise SystemExit("The reference is not the released FusionCache 2.9.0 package")
                    if lines[3] != "culture=invariant":
                        raise SystemExit(f"The reference must run with the invariant culture, got {lines[3]}")
                    current = lines[:4]
                    if identity is not None and current != identity:
                        raise SystemExit("The reference package or runtime changed during measurement")
                    identity = current
            records[label].append(trial)
            warmups[label].append(settling)
        if sources(root) != frozen:
            raise SystemExit("Sources changed during the paired measurement")
    rows = []
    failures = [f"{label} pair {pair + 1} {fixture}/{row['label']}: warmup did not settle"
                for label, trials in warmups.items() for pair, trial in enumerate(trials)
                for fixture, values in trial.items() for row in values if not row["stable"]]
    for key in records["rust"][0]:
        rust = [trial[key] for trial in records["rust"]]
        fusion = [trial[key] for trial in records["fusion"]]
        legacy = [trial[key] for trial in records["fusion_no_tiering"]]
        if {trial["operations"] for trial in rust + fusion + legacy} != {rust[0]["operations"]}:
            raise SystemExit(f"Operation counts differ: {key}")
        ns = {label: statistics.median(trial[key]["ns"] for trial in records[label]) for label in records}
        allocations = statistics.median(row["allocated"] / row["operations"] for row in rust)
        if key[0] in {"same", "distinct", "sync"} and any(row["allocated"] for row in rust):
            failures.append(f"{key}: warmed Rust hit allocated")
        rows.append({
            "scenario": key[0], "threads": key[1], "rust_ns": ns["rust"], "fusion_ns": ns["fusion"],
            "rust_over_fusion": ns["rust"] / ns["fusion"], "rust_allocations_per_op": allocations,
            "fusion_no_tiering_ns": ns["fusion_no_tiering"],
            "rust_over_fusion_no_tiering": ns["rust"] / ns["fusion_no_tiering"],
            "rust_range_ns": [min(row["ns"] for row in rust), max(row["ns"] for row in rust)],
            "fusion_range_ns": [min(row["ns"] for row in fusion), max(row["ns"] for row in fusion)],
            "fusion_no_tiering_range_ns": [min(row["ns"] for row in legacy), max(row["ns"] for row in legacy)],
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
            limits[("l2_json", 1)] = 1.0
            if by_key[("set", 1)]["rust_allocations_per_op"] > 3:
                failures.append("set: more than three allocations per operation")
        for key, limit in limits.items():
            if by_key[key]["rust_over_fusion"] > limit:
                failures.append(f"{key}: {by_key[key]['rust_over_fusion']:.3f} x FC exceeds {limit:.2f}")
    cpu_count = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count() or 1
    topology = cpu_topology(cpu_count)
    scaling = by_key[("distinct", 1)]["rust_ns"] / by_key[("distinct", 8)]["rust_ns"]
    # An oversubscribed hosted runner cannot demonstrate eight-core scaling.
    # It still runs all eight scenarios and checks the same relative FC budgets.
    # SMT siblings are logical threads, not additional physical cores. The
    # required sixfold qualification still needs eight actual physical cores.
    # Unknown topology cannot produce a supported proportional scaling claim.
    physical = topology["physical_cores_available"]
    scaling_limit = min(6, 0.75 * physical) if physical is not None else None
    if args.gate != "report" and scaling_limit is not None and scaling < scaling_limit:
        failures.append(f"distinct scaling: {scaling:.2f} below {scaling_limit:.2f} for {physical} available physical cores")
    report = {
        "schema_version": 2, "gate_reference": "fusion_default_tiering_pgo",
        "warmup_policy": {"minimum_seconds": 3, "maximum_seconds": 15,
                          "operation_window_milliseconds": 100, "stable_windows": 5,
                          "maximum_window_ratio": 1.10}, "warmup_evidence": warmups,
        "api": args.api, "fixture_processes": ["warm", "mutations", "distributed"], "gate": args.gate, "pairs": args.pairs, "environment": {
            "platform": platform.platform(), "available_cpus": cpu_count, "cpu_topology": topology,
            "rust": execute(["rustc", "--version", "--verbose"], root, env).stdout.strip(),
            "dotnet": execute(["dotnet", "--version"], root, env).stdout.strip(),
            "dotnet_tiered_compilation": "default", "fusion_identity": identity,
            "removed_jit_override_names": removed_jit_overrides,
            "dotnet_globalization": "invariant (DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1)",
            "removed_locale_override_names": removed_locale_overrides,
            "fusion_modes": {"fusion": "runtime defaults; gate reference",
                             "fusion_no_tiering": "DOTNET_TieredCompilation=0; diagnostic only"},
        }, "sources_sha256": frozen, "binaries_sha256": {"rust": digest(binary), "fusion_fixture": digest(reference)},
        "distinct_scaling": scaling, "scaling_limit": scaling_limit,
        "eight_core_scaling_verified": (topology["physical_cores_available"] or 0) >= 8 and scaling >= 6,
        "measurements": rows, "failures": failures,
    }
    execute([str(binary), "--costs"], root, env, output / "ready-costs.csv")
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print("scenario threads Rust_ns FC_default_ns FC_TC0_ns Rust/FC_default Rust/FC_TC0 Rust_alloc/op")
    for row in rows:
        print(f"{row['scenario']:8} {row['threads']:7} {row['rust_ns']:7.2f} {row['fusion_ns']:7.2f} {row['fusion_no_tiering_ns']:7.2f} {row['rust_over_fusion']:7.3f} {row['rust_over_fusion_no_tiering']:7.3f} {row['rust_allocations_per_op']:13.3f}")
    if scaling_limit is None:
        print(f"Distinct scaling: {scaling:.2f}x; physical topology unavailable, qualification unverified")
    else:
        print(f"Distinct scaling: {scaling:.2f}x; required {scaling_limit:.2f}x on {physical} available physical cores ({cpu_count} logical CPUs)")
    if failures:
        raise SystemExit("Gate failed:\n" + "\n".join(failures))


if __name__ == "__main__":
    main()
