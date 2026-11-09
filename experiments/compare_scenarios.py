import argparse
import asyncio
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from fault_tolerance import SCENARIOS, run_once


ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "experiments" / "results"


async def compare_scenarios(runs: int):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    all_results = []
    summary = []

    print("=" * 60)
    print("P2P FAULT TOLERANCE EXPERIMENTS")
    print("=" * 60)

    for scenario in SCENARIOS:
        print(f"\nСценарий: {scenario}")

        scenario_results = []

        for run_number in range(1, runs + 1):
            try:
                result = await run_once(scenario, run_number)

                scenario_results.append(result)
                all_results.append(result)

                status = "OK" if result["success"] else "FAIL"

                print(
                    f"  {run_number}/{runs}: {status} | "
                    f"{result['lookup']['duration_ms']:.2f} мс"
                )

            except Exception as exc:
                print(
                    f"  {run_number}/{runs}: ERROR | {exc}"
                )

                failed_result = {
                    "scenario": scenario,
                    "run": run_number,
                    "success": False,
                    "error": str(exc),
                }

                scenario_results.append(failed_result)
                all_results.append(failed_result)

        successful = [
            result
            for result in scenario_results
            if result["success"]
        ]

        lookup_results = [
            result["lookup"]
            for result in scenario_results
            if "lookup" in result
        ]

        def average(key):
            values = [
                result[key]
                for result in lookup_results
                if result.get(key) is not None
            ]
            return (
                round(sum(values) / len(values), 3)
                if values else None
            )

        summary.append({
            "scenario": scenario,
            "runs": runs,
            "successful": len(successful),
            "success_rate": round(
                len(successful) / runs * 100, 2
            ),
            "average_duration_ms": average("duration_ms"),
            "average_rpc": average("rpc_count"),
            "average_iterations": average("iterations"),
            "average_find_node_timeouts": average("timeouts"),
            "average_verification_failures": average(
                "verification_failures"
            ),
        })

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    json_path = RESULTS_DIR / f"comparison_{timestamp}.json"
    csv_path = RESULTS_DIR / f"comparison_{timestamp}.csv"

    json_path.write_text(
        json.dumps(
            {
                "summary": summary,
                "runs": all_results,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with csv_path.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=list(summary[0].keys()),
        )
        writer.writeheader()
        writer.writerows(summary)

    print("\n" + "=" * 60)
    print("ИТОГОВОЕ СРАВНЕНИЕ")
    print("=" * 60)

    for item in summary:
        print(
            f"{item['scenario']:<20} "
            f"{item['successful']:>2}/{runs} | "
            f"{item['average_duration_ms']} мс | "
            f"RPC: {item['average_rpc']}"
        )

    print(f"\nJSON: {json_path}")
    print(f"CSV:  {csv_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Compare P2P fault tolerance scenarios"
    )

    parser.add_argument(
        "--runs",
        type=int,
        default=10,
        help="Number of runs per scenario",
    )

    args = parser.parse_args()

    if args.runs < 1:
        parser.error("--runs must be >= 1")

    asyncio.run(compare_scenarios(args.runs))


if __name__ == "__main__":
    main()