"""
Módulo de experimentos do Swarminho.

Implementa seis experimentos principais:

  - minimal          → sanity check do orquestrador; 1 container com sleep.
  - many-small       → criação concorrente de N containers pequenos.
  - mem-pressure     → força a política de memória até containers serem rejeitados.
  - cpu-bound        → container com loop contínuo, consumindo 100% de CPU.
  - log-heavy        → container que gera muitos logs (I/O-bound).
  - lifetime         → containers sequenciais com tempos de vida variados (sleep X).

Todos os experimentos coletam snapshots de métricas do orquestrador e do filesystem, 
geram um ExperimentResult e salvam automaticamente os resultados em JSON na pasta:

    results/<experimento>_<timestamp>.json

Execução:

    python -m swarminho.experiments minimal
    python -m swarminho.experiments many-small --n-containers 20
    python -m swarminho.experiments mem-pressure --per-container-mb 256
    python -m swarminho.experiments cpu-bound --duration-seconds 5
    python -m swarminho.experiments log-heavy --lines 500 --delay 0.01
    python -m swarminho.experiments lifetime --durations 1 2 4 8
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

# =============================================================================
# DATA MODELS
# =============================================================================

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

# =============================================================================
# INTERNAL HELPERS
# =============================================================================

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
    ts = time.strftime("%Y-%m-%d_%H-%M-%S")
    return Path("results") / f"{experiment_name}_{ts}.json"


def _save_result(result: ExperimentResult, output: Path):
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

# =============================================================================
# EXPERIMENT 1 — minimal
# =============================================================================

def experiment_minimal(sleep_seconds: float = 2.0, memory_limit_mb: int = 64, sample_interval: float = 0.5):
    orch = Orchestrator()

    result = ExperimentResult(
        name="minimal",
        parameters={
            "sleep_seconds": sleep_seconds,
            "memory_limit_mb": memory_limit_mb,
            "sample_interval": sample_interval,
        },
        notes="Sanity check: 1 container com sleep.",
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

# =============================================================================
# EXPERIMENT 2 — many-small
# =============================================================================

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
        notes="Muitos containers pequenos simultâneos.",
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

# =============================================================================
# EXPERIMENT 3 — mem-pressure
# =============================================================================

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

# =============================================================================
# EXPERIMENT 4 — cpu-bound
# =============================================================================

def experiment_cpu_bound(duration_seconds: float = 3.0, memory_limit_mb: int = 64, sample_interval: float = 0.5):
    orch = Orchestrator()

    result = ExperimentResult(
        name="cpu_bound",
        parameters={
            "duration_seconds": duration_seconds,
            "memory_limit_mb": memory_limit_mb,
            "sample_interval": sample_interval,
        },
        notes="Loop contínuo, gerando carga alta de CPU.",
    )

    result.snapshots.append(_snapshot(orch, "before"))

    # Usa Python para girar CPU continuamente até o prazo expirar.
    command = "\n".join([
        "python - <<'PY'",
        "import time",
        f"end = time.time() + {duration_seconds}",
        "x = 0",
        "while time.time() < end:",
        "    x = (x * 3 + 1) % 1000003",
        "print(x)",
        "PY",
    ])

    orch.create_container(
        name="exp_cpu",
        command=command,
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

# =============================================================================
# EXPERIMENT 5 — log-heavy
# =============================================================================

def experiment_log_heavy(lines: int = 300, delay: float = 0.01, memory_limit_mb: int = 64, sample_interval: float = 0.5):
    orch = Orchestrator()

    result = ExperimentResult(
        name="log_heavy",
        parameters={
            "lines": lines,
            "delay": delay,
            "memory_limit_mb": memory_limit_mb,
            "sample_interval": sample_interval,
        },
        notes="Cria muitos logs escritos lentamente (I/O-bound).",
    )

    result.snapshots.append(_snapshot(orch, "before"))

    command = f"sh -c 'for i in $(seq 1 {lines}); do echo $i; sleep {delay}; done'"

    orch.create_container(
        name="exp_log",
        command=command,
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

# =============================================================================
# EXPERIMENT 6 — lifetime
# =============================================================================

def experiment_lifetime(durations: List[float] = [1.0, 2.0, 4.0], memory_limit_mb: int = 64, sample_interval: float = 0.5):
    orch = Orchestrator()

    result = ExperimentResult(
        name="lifetime",
        parameters={
            "durations": durations,
            "memory_limit_mb": memory_limit_mb,
            "sample_interval": sample_interval,
        },
        notes="Avalia variações de tempo de vida dos containers (sleep X).",
    )

    result.snapshots.append(_snapshot(orch, "before"))

    for idx, d in enumerate(durations):
        orch.create_container(
            name=f"exp_life_{idx}",
            command=f"sleep {d}",
            memory_limit_mb=memory_limit_mb,
        )

        while True:
            result.snapshots.append(_snapshot(orch, f"running_{idx}"))
            running = [c for c in orch.list_containers() if c.status is ContainerStatus.RUNNING]
            if not running:
                break
            time.sleep(sample_interval)

        result.snapshots.append(_snapshot(orch, f"after_{idx}"))

    result.snapshots.append(_snapshot(orch, "final"))
    return result

# =============================================================================
# CLI PARSER
# =============================================================================

def build_arg_parser():
    parser = argparse.ArgumentParser(
        prog="python -m swarminho.experiments",
        description="Executa experimentos do Swarminho.",
    )

    sub = parser.add_subparsers(dest="experiment", required=True)

    # minimal
    p = sub.add_parser("minimal")
    p.add_argument("--sleep-seconds", type=float, default=2.0)
    p.add_argument("--memory-limit-mb", type=int, default=64)
    p.add_argument("--sample-interval", type=float, default=0.5)
    p.add_argument("--output", type=Path, default=None)

    # many-small
    p = sub.add_parser("many-small")
    p.add_argument("--n-containers", type=int, default=10)
    p.add_argument("--sleep-seconds", type=float, default=3.0)
    p.add_argument("--memory-limit-mb", type=int, default=32)
    p.add_argument("--sample-interval", type=float, default=0.5)
    p.add_argument("--output", type=Path, default=None)

    # mem-pressure
    p = sub.add_parser("mem-pressure")
    p.add_argument("--per-container-mb", type=int, default=128)
    p.add_argument("--max-containers", type=int, default=50)
    p.add_argument("--sample-interval", type=float, default=0.5)
    p.add_argument("--output", type=Path, default=None)

    # cpu-bound
    p = sub.add_parser("cpu-bound")
    p.add_argument("--duration-seconds", type=float, default=3.0)
    p.add_argument("--memory-limit-mb", type=int, default=64)
    p.add_argument("--sample-interval", type=float, default=0.5)

    # log-heavy
    p = sub.add_parser("log-heavy")
    p.add_argument("--lines", type=int, default=300)
    p.add_argument("--delay", type=float, default=0.01)
    p.add_argument("--memory-limit-mb", type=int, default=64)
    p.add_argument("--sample-interval", type=float, default=0.5)

    # lifetime
    p = sub.add_parser("lifetime")
    p.add_argument("--durations", nargs="+", type=float, default=[1.0, 2.0, 4.0])
    p.add_argument("--memory-limit-mb", type=int, default=64)
    p.add_argument("--sample-interval", type=float, default=0.5)

    return parser

# =============================================================================
# MAIN
# =============================================================================

def main(argv=None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.experiment == "minimal":
        result = experiment_minimal(args.sleep_seconds, args.memory_limit_mb, args.sample_interval)

    elif args.experiment == "many-small":
        result = experiment_many_small(args.n_containers, args.sleep_seconds, args.memory_limit_mb, args.sample_interval)

    elif args.experiment == "mem-pressure":
        result = experiment_memory_pressure(args.per_container_mb, args.max_containers, args.sample_interval)

    elif args.experiment == "cpu-bound":
        result = experiment_cpu_bound(args.duration_seconds, args.memory_limit_mb, args.sample_interval)

    elif args.experiment == "log-heavy":
        result = experiment_log_heavy(args.lines, args.delay, args.memory_limit_mb, args.sample_interval)

    elif args.experiment == "lifetime":
        result = experiment_lifetime(args.durations, args.memory_limit_mb, args.sample_interval)

    else:
        parser.error("Experimento inválido.")

    output_path = getattr(args, "output", None) or _auto_output_path(result.name)

    _save_result(result, output_path)

    last = result.snapshots[-1]
    print("\n=== RESUMO FINAL ===")
    print(json.dumps(last.orch_metrics, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main(sys.argv[1:]))
