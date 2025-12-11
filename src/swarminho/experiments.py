"""
Módulo de experimentos do Swarminho.

Implementa três experimentos:
  - minimal
  - many-small
  - mem-pressure

Pode ser executado via:

    python -m swarminho.experiments minimal
    python -m swarminho.experiments many-small --n-containers 20
    python -m swarminho.experiments mem-pressure --per-container-mb 256
"""

from __future__ import annotations
import argparse
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .orchestrator import Orchestrator, MEMORY_THRESHOLD_FRACTION
from .containerstatus import ContainerStatus
from .metrics import (
    collect_orchestrator_metrics,
    count_containers_on_disk,
    total_logs_size_bytes,
    get_total_memory_mb,
)


@dataclass
class Snapshot:
    timestamp: float
    label: str
    orch_metrics: Dict[str, Any]
    fs_metrics: Dict[str, Any]


@dataclass
class ExperimentResult:
    name: str
    parameters: Dict[str, Any]
    snapshots: List[Snapshot] = field(default_factory=list)
    notes: str = ""



def _snapshot(orch: Orchestrator, label: str) -> Snapshot:
    orch_metrics = collect_orchestrator_metrics(orch, MEMORY_THRESHOLD_FRACTION)
    fs_metrics = {
        "containers_on_disk": count_containers_on_disk(),
        "total_logs_size_bytes": total_logs_size_bytes(),
    }
    return Snapshot(time.time(), label, orch_metrics, fs_metrics)


def _wait_all_finished(orch: Orchestrator, poll_interval: float = 0.5, timeout: Optional[float] = None):
    start = time.time()
    while True:
        running = [c for c in orch.list_containers() if c.status is ContainerStatus.RUNNING]
        if not running:
            return
        if timeout and (time.time() - start > timeout):
            return
        time.sleep(poll_interval)


def _auto_output_path(experiment_name: str) -> Path:
    """
    Gera automaticamente o caminho:
        results/<experimento>_<timestamp>.json
    """
    ts = time.strftime("%Y-%m-%d_%H-%M-%S")
    return Path("results") / f"{experiment_name}_{ts}.json"


def _save_result(result: ExperimentResult, output: Path):
    """
    Salva o resultado completo do experimento em JSON.
    Inclui snapshots e métricas finais.
    """
    output.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "name": result.name,
        "parameters": result.parameters,
        "notes": result.notes,
        "snapshots": [
            {
                "timestamp": s.timestamp,
                "label": s.label,
                "orch_metrics": s.orch_metrics,
                "fs_metrics": s.fs_metrics,
            }
            for s in result.snapshots
        ],
        "final_metrics": result.snapshots[-1].orch_metrics,
    }

    output.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"\n[OK] Resultado salvo em: {output}")



def experiment_minimal(sleep_seconds: float = 2.0, memory_limit_mb: int = 64, sample_interval: float = 0.5):
    orch = Orchestrator()
    result = ExperimentResult(
        name="minimal",
        parameters={
            "sleep_seconds": sleep_seconds,
            "memory_limit_mb": memory_limit_mb,
            "sample_interval": sample_interval,
        },
        notes="Sanity check: sobe 1 container com 'sleep'.",
    )

    result.snapshots.append(_snapshot(orch, "before"))

    orch.create_container(
        name="exp_minimal",
        command=f"sleep {sleep_seconds}",
        memory_limit_mb=memory_limit_mb,
    )

    while True:
        result.snapshots.append(_snapshot(orch, "running"))
        running = [c for c in orch.list_containers() if c.status is ContainerStatus.RUNNING]
        if not running:
            break
        time.sleep(sample_interval)

    result.snapshots.append(_snapshot(orch, "after"))
    return result


def experiment_many_small(n_containers: int = 10, sleep_seconds: float = 3.0, memory_limit_mb: int = 32, sample_interval: float = 0.5):
    orch = Orchestrator()

    result = ExperimentResult(
        name="many_small",
        parameters={
            "n_containers": n_containers,
            "sleep_seconds": sleep_seconds,
            "memory_limit_mb": memory_limit_mb,
            "sample_interval": sample_interval,
        },
        notes="Cria muitos containers pequenos para testar concorrência.",
    )

    result.snapshots.append(_snapshot(orch, "before"))

    for i in range(n_containers):
        orch.create_container(
            name=f"exp_many_{i:03d}",
            command=f"sleep {sleep_seconds}",
            memory_limit_mb=memory_limit_mb,
        )

    while True:
        result.snapshots.append(_snapshot(orch, "running"))
        running = [c for c in orch.list_containers() if c.status is ContainerStatus.RUNNING]
        if not running:
            break
        time.sleep(sample_interval)

    result.snapshots.append(_snapshot(orch, "after"))
    return result


def experiment_memory_pressure(per_container_mb: int = 128, max_containers: int = 50, sample_interval: float = 0.5):
    orch = Orchestrator()
    total_mem_mb = get_total_memory_mb()
    policy_limit_mb = int(total_mem_mb * MEMORY_THRESHOLD_FRACTION)

    result = ExperimentResult(
        name="mem_pressure",
        parameters={
            "per_container_mb": per_container_mb,
            "max_containers": max_containers,
            "sample_interval": sample_interval,
            "system_total_memory_mb": total_mem_mb,
            "policy_limit_mb": policy_limit_mb,
        },
        notes="Cria containers até atingir a política de memória.",
    )

    result.snapshots.append(_snapshot(orch, "before"))

    for i in range(max_containers):
        try:
            orch.create_container(
                name=f"exp_mem_{i:03d}",
                command="sleep 10",
                memory_limit_mb=per_container_mb,
            )
        except RuntimeError as e:
            print(f"[mem-pressure] Container rejeitado: {e}")
            break

        result.snapshots.append(_snapshot(orch, f"after-create-{i:03d}"))

    _wait_all_finished(orch, timeout=60.0)
    result.snapshots.append(_snapshot(orch, "after"))
    return result



def build_arg_parser():
    parser = argparse.ArgumentParser(
        prog="python -m swarminho.experiments",
        description="Executa experimentos do Swarminho.",
    )

    sub = parser.add_subparsers(dest="experiment", required=True)

    p = sub.add_parser("minimal")
    p.add_argument("--sleep-seconds", type=float, default=2.0)
    p.add_argument("--memory-limit-mb", type=int, default=64)
    p.add_argument("--sample-interval", type=float, default=0.5)
    p.add_argument("--output", type=Path, default=None)

    p = sub.add_parser("many-small")
    p.add_argument("--n-containers", type=int, default=10)
    p.add_argument("--sleep-seconds", type=float, default=3.0)
    p.add_argument("--memory-limit-mb", type=int, default=32)
    p.add_argument("--sample-interval", type=float, default=0.5)
    p.add_argument("--output", type=Path, default=None)

    p = sub.add_parser("mem-pressure")
    p.add_argument("--per-container-mb", type=int, default=128)
    p.add_argument("--max-containers", type=int, default=50)
    p.add_argument("--sample-interval", type=float, default=0.5)
    p.add_argument("--output", type=Path, default=None)

    return parser


def main(argv=None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.experiment == "minimal":
        result = experiment_minimal(args.sleep_seconds, args.memory_limit_mb, args.sample_interval)
    elif args.experiment == "many-small":
        result = experiment_many_small(args.n_containers, args.sleep_seconds, args.memory_limit_mb, args.sample_interval)
    elif args.experiment == "mem-pressure":
        result = experiment_memory_pressure(args.per_container_mb, args.max_containers, args.sample_interval)
    else:
        parser.error("Experimento inválido.")

    output_path = args.output or _auto_output_path(result.name)

    _save_result(result, output_path)

    last = result.snapshots[-1]
    print("\n=== RESUMO FINAL ===")
    print(json.dumps(last.orch_metrics, indent=2))
    return 0




if __name__ == "__main__":
    import sys
    sys.exit(main(sys.argv[1:]))
