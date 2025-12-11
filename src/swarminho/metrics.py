import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, TYPE_CHECKING

from src.swarminho.filesystem import rootfs_dir, stderr_log_path, stdout_log_path
from src.swarminho.orchestrator import ContainerStatus
from src.swarminho.runtime import is_container_running, memory_usage_kb

if TYPE_CHECKING:
    from src.swarminho.orchestrator import ContainerInfo


@dataclass
class ContainerMetrics:
    name: str
    status: ContainerStatus
    pid: Optional[int]
    timestamp: datetime
    rss_kb: Optional[int]
    cpu_time_s: Optional[float]
    log_bytes: int
    rootfs_bytes: int
    cpu_percent: Optional[float] = None


def collect_metrics(container: "ContainerInfo") -> ContainerMetrics:
    """
    Coleta métricas pontuais para um container.

    - status é usado como fallback; será atualizado se o processo estiver rodando.
    - cpu_time_s é o tempo de usuário+sistema em segundos, quando disponível.
    - rss_kb é a memória residente em kB (VmRSS).
    """
    now = datetime.now(timezone.utc)

    rss_kb: Optional[int] = None
    cpu_time_s: Optional[float] = None
    final_status: ContainerStatus = container.status

    if container.pid is not None:
        running = is_container_running(container.pid)
        if running:
            final_status = ContainerStatus.RUNNING
            rss_kb = memory_usage_kb(container.pid)
            cpu_time_s = _read_cpu_time_s(container.pid)
        elif container.status == ContainerStatus.RUNNING:
            final_status = ContainerStatus.TERMINATED

    log_bytes = _logs_size(container.name)
    rootfs_bytes = _rootfs_size(container.name)

    return ContainerMetrics(
        name=container.name,
        status=final_status,
        pid=container.pid,
        timestamp=now,
        rss_kb=rss_kb,
        cpu_time_s=cpu_time_s,
        log_bytes=log_bytes,
        rootfs_bytes=rootfs_bytes,
    )


def _read_cpu_time_s(pid: int) -> Optional[float]:
    """Lê /proc/<pid>/stat e retorna utime+stime em segundos."""
    stat_path = Path(f"/proc/{pid}/stat")
    if not stat_path.exists():
        return None

    try:
        parts = stat_path.read_text(errors="ignore").split()
        utime = float(parts[13])
        stime = float(parts[14])
        ticks_per_second = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        return (utime + stime) / ticks_per_second
    except (OSError, IndexError, ValueError):
        return None


def cpu_percent(prev: ContainerMetrics, curr: ContainerMetrics) -> Optional[float]:
    """Calcula CPU% entre dois snapshots."""
    if (
        prev.pid != curr.pid
        or prev.cpu_time_s is None
        or curr.cpu_time_s is None
    ):
        return None

    dt = (curr.timestamp - prev.timestamp).total_seconds()
    if dt <= 0:
        return None

    delta_cpu = curr.cpu_time_s - prev.cpu_time_s
    if delta_cpu < 0:
        return None

    n_cpus = os.cpu_count() or 1
    return (delta_cpu / dt) * 100 / n_cpus


def _logs_size(name: str) -> int:
    """Soma o tamanho de stdout/stderr.log (bytes)."""
    total = 0
    for path in (stdout_log_path(name), stderr_log_path(name)):
        if path.exists():
            try:
                total += path.stat().st_size
            except OSError:
                continue
    return total


def _rootfs_size(name: str) -> int:
    """Calcula o tamanho do rootfs (bytes)."""
    rfs = rootfs_dir(name)
    if not rfs.exists():
        return 0

    total = 0
    for path in rfs.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except OSError:
            continue
    return total
