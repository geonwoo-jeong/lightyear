#!/usr/bin/env python3
"""Run two fixed binaries serially, alternating order; keep raw paired evidence."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys

SCENARIOS = ("udp-single", "udp-endpoint", "udp-receive", "transport-single", "transport-many", "empty-udp", "empty-transport")
PRACTICAL_REGRESSION_RATIO = 1.05  # Declared before collecting samples.
ROOT = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def percentile(values, p):
    ordered = sorted(values)
    position = (len(ordered) - 1) * p
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def check_work(left, right):
    for key in ("scenario", "iterations", "packets", "bytes", "messages", "checksum"):
        if left["result"][key] != right["result"][key]:
            raise ValueError(f"workload differs for {key}: {left['result'][key]} != {right['result'][key]}")


def classify(low, high):
    if low > PRACTICAL_REGRESSION_RATIO:
        return "regression above 5% supported"
    if high <= PRACTICAL_REGRESSION_RATIO:
        return "95% interval upper bound within 5% threshold"
    return "inconclusive against 5% threshold"


def require_new_output(path):
    if path.exists():
        raise FileExistsError(f"refusing to overwrite existing evidence: {path}")


def launch_spec(executable, scenario, iterations, observe=False, count=False):
    command = ["performance-audit", "--scenario", scenario, "--iterations", str(iterations)]
    if observe:
        command.append("--observe")
    if count:
        command.append("--count-allocations")
    return {"args": command, "executable": str(executable), "cwd": str(ROOT)}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--baseline", type=Path, required=True)
    p.add_argument("--candidate", type=Path, required=True)
    p.add_argument("--baseline-observe", action="store_true")
    p.add_argument("--candidate-observe", action="store_true")
    p.add_argument("--scenario", action="append", choices=SCENARIOS)
    p.add_argument("--iterations", type=int, default=1000)
    p.add_argument("--repetitions", type=int, default=9)
    p.add_argument("--allocation-iterations", type=int, default=100)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--require-within-threshold", action="store_true", help="exit 1 after saving unless every 95%% interval upper bound is <=1.05")
    args = p.parse_args()
    require_new_output(args.output)
    if args.repetitions < 7:
        p.error("at least seven paired repetitions are required")
    if not 1 <= args.iterations <= 10000 or not 1 <= args.allocation_iterations <= 10000:
        p.error("iteration counts must be 1..=10000")
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    hashes = {name: digest(path) for name, path in binaries.items()}
    source_hash = digest(ROOT / "src/main.rs")
    report = {
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(), "machine": platform.machine(), "logical_cpus": os.cpu_count(),
        "binaries": {name: {"path": str(path), "sha256": hashes[name]} for name, path in binaries.items()},
        "harness_sha256": source_hash,
        "launch": {"argv0": "performance-audit", "cwd": str(ROOT), "executable_selection": "subprocess executable parameter"},
        "metric": "schedule_elapsed_ns (wall-clock elapsed time, NOT process CPU time)",
        "practical_regression_ratio": PRACTICAL_REGRESSION_RATIO,
        "bootstrap": {"statistic": "median of paired candidate/baseline ratios", "resamples": 10000, "seed": 320825, "interval": "percentile 95%"},
        "scenarios": {},
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def save():
        args.output.write_text(json.dumps(report, indent=2) + "\n")

    def fail(message, **details):
        report["failure"] = {"message": message, **details}
        save()
        raise RuntimeError(message)

    def run(name, scenario, iterations, count=False):
        if digest(binaries[name]) != hashes[name] or digest(ROOT / "src/main.rs") != source_hash:
            fail("binary or harness source changed during comparison")
        observe = args.baseline_observe if name == "baseline" else args.candidate_observe
        launch = launch_spec(binaries[name], scenario, iterations, observe, count)
        context = {"command": launch["args"], "executable": launch["executable"], "cwd": launch["cwd"]}
        try:
            process = subprocess.run(**launch, text=True, capture_output=True, timeout=180)
        except subprocess.TimeoutExpired as error:
            fail("workload process exceeded 180 seconds", **context,
                 stdout=str(error.stdout), stderr=str(error.stderr))
        if process.returncode:
            fail("workload returned nonzero", **context, exit_code=process.returncode,
                 stdout=process.stdout, stderr=process.stderr)
        lines = [line for line in process.stdout.splitlines() if line.startswith("{")]
        if len(lines) != 1:
            fail("expected one result JSON", **context, stdout=process.stdout, stderr=process.stderr)
        try:
            result = json.loads(lines[0])
        except ValueError:
            fail("invalid result JSON", **context, stdout=process.stdout, stderr=process.stderr)
        if result["allocation_counting"] != count or result["observe"] != observe:
            fail("measurement mode mismatch", **context, result=result)
        return {**context, "result": result, "stderr": process.stderr}

    def same_work(left, right):
        try:
            check_work(left, right)
        except ValueError as error:
            fail(str(error), baseline=left, candidate=right)

    for scenario in args.scenario or SCENARIOS:
        if scenario == "udp-receive" and (args.baseline_observe or args.candidate_observe):
            p.error("udp-receive has no send observations; select applicable scenarios explicitly")
        case = {"pairs": [], "allocations": {}, "summary": None}
        report["scenarios"][scenario] = case
        for index in range(args.repetitions):
            order = ("baseline", "candidate") if index % 2 == 0 else ("candidate", "baseline")
            pair = {"order": order}
            for name in order:
                pair[name] = run(name, scenario, args.iterations)
            same_work(pair["baseline"], pair["candidate"])
            case["pairs"].append(pair)
            save()
            print(f"{scenario}: pair {index + 1}/{args.repetitions} complete", flush=True)
        ratios = [pair["candidate"]["result"]["schedule_elapsed_ns"] / pair["baseline"]["result"]["schedule_elapsed_ns"] for pair in case["pairs"]]
        rng = random.Random(320825)
        boot = [statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(10000)]
        low, high = percentile(boot, .025), percentile(boot, .975)
        verdict = classify(low, high)
        case["summary"] = {
            "baseline_median_ns_per_batch": statistics.median(pair["baseline"]["result"]["schedule_elapsed_ns"] / args.iterations for pair in case["pairs"]),
            "candidate_median_ns_per_batch": statistics.median(pair["candidate"]["result"]["schedule_elapsed_ns"] / args.iterations for pair in case["pairs"]),
            "median_paired_ratio": statistics.median(ratios),
            "paired_ratio_95_percent_interval": [low, high], "verdict": verdict,
        }
        # Separate equivalent runs: atomic counter costs are excluded from ratios above.
        for name in ("baseline", "candidate"):
            case["allocations"][name] = run(name, scenario, args.allocation_iterations, count=True)
        same_work(case["allocations"]["baseline"], case["allocations"]["candidate"])
        save()
        print(json.dumps({"scenario": scenario, **case["summary"]}), flush=True)
    report["finished_utc"] = datetime.now(timezone.utc).isoformat()
    report["all_within_threshold"] = all(case["summary"]["paired_ratio_95_percent_interval"][1] <= PRACTICAL_REGRESSION_RATIO for case in report["scenarios"].values())
    save()
    if args.require_within_threshold and not report["all_within_threshold"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
