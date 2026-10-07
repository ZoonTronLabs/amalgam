#!/usr/bin/env python3
"""Untimed Linux CPU profiles of the exact frozen before/after binaries."""
import argparse
import glob
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def find_perf():
    candidates = [shutil.which("perf")]
    candidates.extend(sorted(glob.glob("/usr/lib/linux-tools*/perf"), reverse=True))
    candidates.extend(sorted(glob.glob("/usr/lib/linux-tools/*/perf"), reverse=True))
    for candidate in dict.fromkeys(candidates):
        if candidate and os.access(candidate, os.X_OK):
            result = subprocess.run([candidate, "--version"], capture_output=True, text=True)
            if result.returncode == 0:
                return candidate, result.stdout.strip()
    return None, None


def workload(binary, api, repeats):
    for _ in range(repeats):
        subprocess.run([str(binary), "--l2", "--api", api], check=True)


def profile(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    report = json.loads((args.reports / "report.json").read_text())
    perf, version = find_perf()
    identity = {
        "status": "unavailable",
        "platform": platform.platform(),
        "driver_sha256": digest(Path(__file__).resolve()),
        "paired_report_sha256": digest(args.reports / "report.json"),
        "paired_identity": report["identity"],
        "perf": version,
        "profiles": [],
    }
    if not perf:
        identity["reason"] = "No runnable perf tool was found."
        (output / "report.json").write_text(json.dumps(identity, indent=2) + "\n")
        print(identity["reason"])
        return 0
    privilege = []
    if os.geteuid() != 0 and shutil.which("sudo"):
        if subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode == 0:
            privilege = ["sudo", "-n"]
    raw = output / "raw"
    raw.mkdir(exist_ok=True)
    for label in ["baseline", "candidate"]:
        binary = (args.reports / (label + "-scaling")).resolve()
        if digest(binary) != report["identity"]["binaries_sha256"][label]:
            raise SystemExit("Frozen binary hash does not match paired report: " + label)
        for api in ["read", "get-or-set"]:
            case = label + "-" + api
            data = raw / (case + ".data")
            command = [*privilege, perf, "record", "--event", "cpu-clock:u",
                       "--freq", "997", "--call-graph", "dwarf,16384",
                       "--no-buildid-cache", "--output", str(data), "--",
                       sys.executable, str(Path(__file__).resolve()), "--workload",
                       "--binary", str(binary), "--api", api, "--repeats", "6"]
            with (output / (case + ".csv")).open("w") as csv:
                run = subprocess.run(command, stdout=csv, stderr=subprocess.PIPE, text=True)
            (output / (case + ".stderr")).write_text(run.stderr)
            row = {"case": case, "record_exit": run.returncode,
                   "binary_sha256": digest(binary), "event": "cpu-clock:u",
                   "frequency": 997, "repeats": 6, "reports": {}}
            if run.returncode == 0:
                for kind, options in [("self", ["--no-children"]),
                                      ("stacks", ["--children", "--call-graph", "graph"])]:
                    rendered = subprocess.run(
                        [*privilege, perf, "report", "--input", str(data), "--stdio",
                         "--show-nr-samples", "--percent-limit", "0.5", *options],
                        text=True, capture_output=True)
                    target = output / (case + "-" + kind + ".txt")
                    target.write_text(rendered.stdout)
                    (output / (case + "-" + kind + ".stderr")).write_text(rendered.stderr)
                    row["reports"][kind] = {"exit": rendered.returncode,
                                            "sha256": digest(target)}
            identity["profiles"].append(row)
            print(case + ": record exit=" + str(run.returncode), flush=True)
    complete = all(row["record_exit"] == 0 and len(row["reports"]) == 2
                   and all(item["exit"] == 0 for item in row["reports"].values())
                   for row in identity["profiles"])
    identity["status"] = "complete" if complete else "incomplete"
    (output / "report.json").write_text(json.dumps(identity, indent=2) + "\n")
    return 0 if complete else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--workload", action="store_true")
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--api", choices=["read", "get-or-set"])
    parser.add_argument("--repeats", type=int, default=6)
    args = parser.parse_args()
    if args.workload:
        if not args.binary or not args.api or args.repeats <= 0:
            parser.error("workload requires a binary, API and positive repeats")
        workload(args.binary, args.api, args.repeats)
        return 0
    if not args.reports or not args.output:
        parser.error("profiling requires paired reports and an output directory")
    if platform.system() != "Linux":
        parser.error("native CPU profiling runs on Linux")
    return profile(args)


if __name__ == "__main__":
    raise SystemExit(main())
