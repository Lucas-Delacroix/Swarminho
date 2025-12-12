import time

from ..orchestrator import Orchestrator, MEMORY_THRESHOLD_FRACTION
from ..containerstatus import ContainerStatus
from ..metrics import get_total_memory_mb
from .core import (
    ExperimentResult,
    take_snapshot,
    wait_all_finished,
)


def experiment_minimal(
    sleep_seconds: float = 2.0,
    memory_limit_mb: int = 64,
    sample_interval: float = 0.5,
) -> ExperimentResult:
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

    result.snapshots.append(take_snapshot(orch, "before"))

    orch.create_container(
        name="exp_minimal",
        command=f"sleep {sleep_seconds}",
        memory_limit_mb=memory_limit_mb,
    )

    while True:
        result.snapshots.append(take_snapshot(orch, "running"))
        running = [c for c in orch.list_containers() if c.status is ContainerStatus.RUNNING]
        if not running:
            break
        time.sleep(sample_interval)

    result.snapshots.append(take_snapshot(orch, "after"))
    return result


def experiment_many_small(
    n_containers: int = 10,
    sleep_seconds: float = 3.0,
    memory_limit_mb: int = 32,
    sample_interval: float = 0.5,
) -> ExperimentResult:
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

    result.snapshots.append(take_snapshot(orch, "before"))

    for i in range(n_containers):
        orch.create_container(
            name=f"exp_many_{i:03d}",
            command=f"sleep {sleep_seconds}",
            memory_limit_mb=memory_limit_mb,
        )

    while True:
        result.snapshots.append(take_snapshot(orch, "running"))
        running = [c for c in orch.list_containers() if c.status is ContainerStatus.RUNNING]
        if not running:
            break
        time.sleep(sample_interval)

    result.snapshots.append(take_snapshot(orch, "after"))
    return result


def experiment_memory_pressure(
    per_container_mb: int = 128,
    max_containers: int = 50,
    sample_interval: float = 0.5,
) -> ExperimentResult:
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

    result.snapshots.append(take_snapshot(orch, "before"))

    for i in range(max_containers):
        try:
            orch.create_container(
                name=f"exp_mem_{i:03d}",
                command="sleep 10",
                memory_limit_mb=per_container_mb,
            )
        except RuntimeError as e:
            print(f"[mem-pressure] Container rejeitado: {e}")
            result.snapshots.append(take_snapshot(orch, f"rejected-at-{i:03d}"))
            break

        result.snapshots.append(take_snapshot(orch, f"after-create-{i:03d}"))

    wait_all_finished(orch, timeout=60.0)
    result.snapshots.append(take_snapshot(orch, "after"))
    return result


def _cpu_burn_command(duration_seconds: float) -> str:
    """
    Retorna um comando shell que executa um loop CPU-bound em Python
    por aproximadamente `duration_seconds` segundos.
    """
    return f"""python - << 'EOF'
import time
x = 0
end = time.time() + {duration_seconds}
while time.time() < end:
    x += 1
print(x)
EOF
"""


def experiment_cpu_bound(
    n_containers: int = 4,
    duration_seconds: float = 5.0,
    memory_limit_mb: int = 64,
    sample_interval: float = 0.5,
) -> ExperimentResult:
    """
    Workload CPU-bound.

    Sobe vários containers que executam um loop pesado de CPU em Python
    por `duration_seconds` segundos cada.

    Objetivos:
      - observar running_containers_total_cpu_seconds crescendo ao longo do tempo;
      - analisar average_cpu_seconds_per_running_container;
      - comparar tempo de CPU vs. uptime/lifetime dos containers;
      - ver impacto de aumentar n_containers na soma total de CPU.
    """
    orch = Orchestrator()

    result = ExperimentResult(
        name="cpu_bound",
        parameters={
            "n_containers": n_containers,
            "duration_seconds": duration_seconds,
            "memory_limit_mb": memory_limit_mb,
            "sample_interval": sample_interval,
        },
        notes=(
            "Workload CPU-bound: vários containers rodando um loop de CPU "
            "em Python por alguns segundos."
        ),
    )

    result.snapshots.append(take_snapshot(orch, "before"))

    cpu_cmd = _cpu_burn_command(duration_seconds)

    for i in range(n_containers):
        orch.create_container(
            name=f"exp_cpu_{i:03d}",
            command=cpu_cmd,
            memory_limit_mb=memory_limit_mb,
        )

    while True:
        result.snapshots.append(take_snapshot(orch, "running"))
        running = [c for c in orch.list_containers() if c.status is ContainerStatus.RUNNING]
        if not running:
            break
        time.sleep(sample_interval)

    result.snapshots.append(take_snapshot(orch, "after"))
    return result