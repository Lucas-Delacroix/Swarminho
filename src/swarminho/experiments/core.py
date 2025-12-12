import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..orchestrator import Orchestrator, MEMORY_THRESHOLD_FRACTION
from ..containerstatus import ContainerStatus
from ..metrics import (
    collect_orchestrator_metrics,
    count_containers_on_disk,
    total_logs_size_bytes,
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


def take_snapshot(orch: Orchestrator, label: str) -> Snapshot:
    orch_metrics = collect_orchestrator_metrics(orch, MEMORY_THRESHOLD_FRACTION)
    fs_metrics = {
        "containers_on_disk": count_containers_on_disk(),
        "total_logs_size_bytes": total_logs_size_bytes(),
    }
    return Snapshot(time.time(), label, orch_metrics, fs_metrics)


def wait_all_finished(
    orch: Orchestrator,
    poll_interval: float = 0.5,
    timeout: Optional[float] = None,
) -> None:
    start = time.time()
    while True:
        running = [c for c in orch.list_containers() if c.status is ContainerStatus.RUNNING]
        if not running:
            return
        if timeout is not None and (time.time() - start > timeout):
            return
        time.sleep(poll_interval)


def auto_output_path(experiment_name: str) -> Path:
    ts = time.strftime("%Y-%m-%d_%H-%M-%S")
    return Path("results") / f"{experiment_name}_{ts}.json"


def save_result(result: ExperimentResult, output: Path) -> None:
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
        "final_metrics": result.snapshots[-1].orch_metrics if result.snapshots else {},
    }

    output.write_text(json.dumps(data, indent=2), encoding="utf-8")