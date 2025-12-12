import argparse
import json
from pathlib import Path
from typing import List, Optional

from .core import auto_output_path, save_result
from .scenarios import (
    experiment_minimal,
    experiment_many_small,
    experiment_memory_pressure,
    experiment_cpu_bound,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m swarminho.experiments",
        description="Executa experimentos do Swarminho.",
    )

    sub = parser.add_subparsers(dest="experiment", required=True)

    p = sub.add_parser("minimal", help="Experimento simples com 1 container.")
    p.add_argument("--sleep-seconds", type=float, default=2.0)
    p.add_argument("--memory-limit-mb", type=int, default=64)
    p.add_argument("--sample-interval", type=float, default=0.5)
    p.add_argument("--output", type=Path, default=None)

    p = sub.add_parser("many-small", help="Vários containers pequenos em paralelo.")
    p.add_argument("--n-containers", type=int, default=10)
    p.add_argument("--sleep-seconds", type=float, default=3.0)
    p.add_argument("--memory-limit-mb", type=int, default=32)
    p.add_argument("--sample-interval", type=float, default=0.5)
    p.add_argument("--output", type=Path, default=None)

    p = sub.add_parser("mem-pressure", help="Cria containers até bater na política de memória.")
    p.add_argument("--per-container-mb", type=int, default=128)
    p.add_argument("--max-containers", type=int, default=50)
    p.add_argument("--sample-interval", type=float, default=0.5)
    p.add_argument("--output", type=Path, default=None)

    p = sub.add_parser("cpu-bound", help="Workload CPU-bound com vários containers.")
    p.add_argument("--n-containers", type=int, default=4)
    p.add_argument("--duration-seconds", type=float, default=5.0)
    p.add_argument("--memory-limit-mb", type=int, default=64)
    p.add_argument("--sample-interval", type=float, default=0.5)
    p.add_argument("--output", type=Path, default=None)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.experiment == "minimal":
        result = experiment_minimal(args.sleep_seconds, args.memory_limit_mb, args.sample_interval)
    elif args.experiment == "many-small":
        result = experiment_many_small(
            n_containers=args.n_containers,
            sleep_seconds=args.sleep_seconds,
            memory_limit_mb=args.memory_limit_mb,
            sample_interval=args.sample_interval,
        )
    elif args.experiment == "mem-pressure":
        result = experiment_memory_pressure(
            per_container_mb=args.per_container_mb,
            max_containers=args.max_containers,
            sample_interval=args.sample_interval,
        )
    elif args.experiment == "cpu-bound":
        result = experiment_cpu_bound(
            n_containers=args.n_containers,
            duration_seconds=args.duration_seconds,
            memory_limit_mb=args.memory_limit_mb,
            sample_interval=args.sample_interval,
        )
    else:
        parser.error("Experimento inválido.")

    output_path = args.output or auto_output_path(result.name)
    save_result(result, output_path)

    if result.snapshots:
        last = result.snapshots[-1]
        print("\n=== RESUMO FINAL (último snapshot de orch_metrics) ===")
        print(json.dumps(last.orch_metrics, indent=2))

    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv[1:]))