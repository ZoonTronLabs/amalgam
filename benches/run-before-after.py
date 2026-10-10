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


DEPENDENCY_TABLES = ("dependencies", "dev-dependencies", "build-dependencies")


def parse_toml(content):
    # Imported lazily: only a candidate with a changed manifest needs Python 3.11+.
    import tomllib
    return tomllib.loads(content.decode())


def locked_packages(lockfile):
    return {(package["name"], package["version"], package.get("source"), package.get("checksum"))
            for package in parse_toml(lockfile).get("package", [])}


def split_dependency_tables(manifest):
    """The manifest's dependency tables, keyed by (target cfg or None, kind), and the rest."""
    rest = dict(manifest)
    tables = {(None, kind): rest.pop(kind) for kind in DEPENDENCY_TABLES if kind in rest}
    targets = {}
    for cfg, table in rest.pop("target", {}).items():
        table = dict(table)
        tables.update({(cfg, kind): table.pop(kind) for kind in DEPENDENCY_TABLES if kind in table})
        if table:
            targets[cfg] = table
    if targets:
        rest["target"] = targets
    return tables, rest


def manifest_only_adds_dependencies(baseline_manifest, candidate_manifest):
    """True when the candidate manifest differs only by new dependency entries."""
    baseline, baseline_rest = split_dependency_tables(parse_toml(baseline_manifest))
    candidate, candidate_rest = split_dependency_tables(parse_toml(candidate_manifest))
    return baseline_rest == candidate_rest and all(
        candidate.get(key, {}).get(name) == spec
        for key, table in baseline.items() for name, spec in table.items())


def added_dependencies(baseline_lock, candidate_lock):
    """Packages the candidate adds, or None when it changes or drops any baseline package."""
    baseline, candidate = locked_packages(baseline_lock), locked_packages(candidate_lock)
    if not baseline <= candidate:
        return None
    return sorted(f"{name} {version}" for name, version, _, _ in candidate - baseline)


def source_identity(root):
    paths = [root / "Cargo.toml", root / "Cargo.lock"]
    paths.extend(sorted((root / "benches").rglob("*.rs")))
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
    parser.add_argument("--allow-added-dependencies", action="store_true",
                        help="Accept a candidate whose manifest and lockfile only add dependencies; "
                             "both builds use its manifest (needs Python 3.11+)")
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
    # silently attributing a different benchmark to a storage change. With
    # --allow-added-dependencies a candidate may add packages: its manifest may
    # differ only by new dependency entries, every baseline package must stay
    # locked at the same version, source and checksum, both builds use the
    # candidate manifest, and the additions and both manifest hashes go into the
    # report.
    added = []
    manifests = {relative: execute(["git", "show", f"{baseline_ref}:{relative}"], root, env)
                 for relative in ["Cargo.toml", "Cargo.lock"]}
    candidate_manifest = (root / "Cargo.toml").read_bytes()
    if any(content != (root / relative).read_bytes() for relative, content in manifests.items()):
        added = None
        if args.allow_added_dependencies and manifest_only_adds_dependencies(
                manifests["Cargo.toml"], candidate_manifest):
            added = added_dependencies(manifests["Cargo.lock"], (root / "Cargo.lock").read_bytes())
        if added is None:
            changed = [relative for relative, content in manifests.items()
                       if content != (root / relative).read_bytes()]
            raise SystemExit(f"Frozen workload/dependencies differ at {', '.join(changed)}")
        print(f"Candidate adds locked packages: {', '.join(added) or 'none'}", flush=True)
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
    # Both versions compile the current harness. It may gain a new scenario
    # since the baseline commit; its exact bytes must still match both builds.
    if baseline_identity["benches/scaling.rs"] != current_identity["benches/scaling.rs"]:
        raise SystemExit("Different benchmark workloads between builds")
    current_binary = build("candidate", root, env, output, args)
    if source_identity(root) != current_identity:
        raise SystemExit("Source changed during candidate build")
    cases = {"metadata": ["--metadata-costs"], "mutations": ["--mutations"],
             "read": ["--api", "read"], "get-or-set": ["--api", "get-or-set"],
             "l2-read": ["--l2", "--api", "read"],
             "l2-get-or-set": ["--l2", "--api", "get-or-set"]}
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
                "added_dependencies": added,
                "manifests_sha256": {"baseline": digest(manifests["Cargo.toml"]),
                                     "candidate": digest(candidate_manifest)},
                "baseline_sources_sha256": baseline_identity, "candidate_sources_sha256": current_identity,
                "binaries_sha256": {"baseline": digest(baseline_binary.read_bytes()),
                                    "candidate": digest(current_binary.read_bytes())}}
    report = {"identity": identity, "pairs": args.pairs, "measurements": measurements}
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    for measurement in measurements:
        print(json.dumps(measurement), flush=True)


if __name__ == "__main__":
    main()
