from pathlib import Path
from typing import Dict, Optional, Any
import os
import time

from .runtime import memory_usage_kb, cpu_time_seconds
from .filesystem import container_path, CONTAINERS_ROOT, stdout_log_path, stderr_log_path
from .orchestrator import ContainerStatus


def get_total_memory_mb() -> int:
    """
    Lê /proc/meminfo e retorna a MemTotal em MB.
    """
    meminfo_path = Path("/proc/meminfo")
    if not meminfo_path.exists():
        raise RuntimeError("/proc/meminfo não encontrado; só funciona em Linux.")

    for line in meminfo_path.read_text().splitlines():
        if line.startswith("MemTotal:"):
            parts = line.split()
            kb = int(parts[1])
            return kb // 1024

    raise RuntimeError("Linha 'MemTotal' não encontrada em /proc/meminfo.")


def get_available_memory_mb() -> int:
    """
    Retorna MemAvailable em MB quando disponível; caso contrário tenta
    estimar como MemFree + Buffers + Cached.
    """
    meminfo_path = Path("/proc/meminfo")
    if not meminfo_path.exists():
        raise RuntimeError("/proc/meminfo não encontrado; só funciona em Linux.")

    values_kb: Dict[str, int] = {}
    for line in meminfo_path.read_text().splitlines():
        parts = line.split()
        if not parts:
            continue
        key = parts[0].rstrip(':')
        try:
            val = int(parts[1])
        except (IndexError, ValueError):
            continue
        values_kb[key] = val

    if "MemAvailable" in values_kb:
        return values_kb["MemAvailable"] // 1024

    free = values_kb.get("MemFree", 0)
    buffers = values_kb.get("Buffers", 0)
    cached = values_kb.get("Cached", 0)
    return (free + buffers + cached) // 1024


def get_process_memory_kb(pid: int) -> Optional[int]:
    """
    Retorna VmRSS do processo em kB (ou None se não encontrável).
    Usa a função memory_usage_kb do módulo runtime.
    """
    try:
        return memory_usage_kb(pid)
    except Exception:
        return None


def get_memory_info_mb() -> Dict[str, int]:
    """
    Retorna um dicionário com várias métricas de memória (em MB):
    {
        'MemTotal': ...,
        'MemAvailable': ...,
        'MemFree': ...,
        'Buffers': ...,
        'Cached': ...,
        'SwapTotal': ...,
        'SwapFree': ...,
    }
    Campos ausentes serão omitidos.
    """
    meminfo_path = Path("/proc/meminfo")
    if not meminfo_path.exists():
        raise RuntimeError("/proc/meminfo não encontrado; só funciona em Linux.")

    info: Dict[str, int] = {}
    for line in meminfo_path.read_text().splitlines():
        parts = line.split()
        if not parts:
            continue
        key = parts[0].rstrip(':')
        try:
            val_kb = int(parts[1])
        except (IndexError, ValueError):
            continue

        info[key] = val_kb // 1024

    wanted = ["MemTotal", "MemAvailable", "MemFree", "Buffers", "Cached", "SwapTotal", "SwapFree"]
    return {k: v for k, v in info.items() if k in wanted}


def _dir_size_bytes(path: Path) -> int:
    """
    Retorna soma de tamanhos em bytes de todos os arquivos sob `path`.
    Se path não existir, retorna 0.
    """
    if not path.exists():
        return 0
    total = 0
    for root, dirs, files in os.walk(str(path)):
        for fname in files:
            try:
                fp = os.path.join(root, fname)
                total += os.path.getsize(fp)
            except OSError:
                continue
    return total


def get_container_disk_usage_bytes(name: str) -> int:
    """
    Retorna o espaço em bytes utilizado pelo diretório do container (containers/<name>),
    incluindo rootfs e logs. Retorna 0 se o container não existir no disco.
    """
    cpath = container_path(name)
    return _dir_size_bytes(cpath)


def get_container_logs_size_bytes(name: str) -> int:
    """
    Retorna o tamanho (bytes) dos arquivos stdout.log e stderr.log do container.
    """
    total = 0
    s = stdout_log_path(name)
    e = stderr_log_path(name)
    if s.exists():
        try:
            total += s.stat().st_size
        except OSError:
            pass
    if e.exists():
        try:
            total += e.stat().st_size
        except OSError:
            pass
    return total


def count_containers_on_disk() -> int:
    """
    Conta quantos subdiretórios existem em CONTAINERS_ROOT.
    """
    root = Path(CONTAINERS_ROOT)
    if not root.exists():
        return 0
    return sum(1 for _ in root.iterdir() if _.is_dir())


def total_logs_size_bytes() -> int:
    """
    Soma o tamanho de todos os arquivos de log (stdout + stderr) em containers/.
    """
    root = Path(CONTAINERS_ROOT)
    if not root.exists():
        return 0
    total = 0
    for cdir in root.iterdir():
        if not cdir.is_dir():
            continue
        total += _dir_size_bytes(cdir / "logs")
    return total


def collect_orchestrator_metrics(orch: Any, memory_threshold_fraction: float = 0.2) -> Dict[str, Any]:
    """
    Coleta métricas a partir de uma instância de Orchestrator passada como argumento.

    Retorna um dicionário com, por exemplo:
      - total_containers, running, terminated, failed, pending
      - created_count, rejected_containers, peak_running
      - committed_memory_limit_mb (soma dos memory_limit_mb de containers RUNNING)
      - memory_policy_limit_mb (total_system_mem * memory_threshold_fraction)
      - running_containers_total_rss_kb (soma do VmRSS em kB para containers RUNNING)
      - running_containers_total_cpu_seconds (soma do tempo de CPU)
      - average_cpu_seconds_per_running_container
      - average_container_memory_limit_mb
      - average_uptime_running_seconds
      - average_lifetime_terminated_seconds
    """
    metrics: Dict[str, Any] = {}
    containers = getattr(orch, "containers", {}) or {}

    counts = {"running": 0, "terminated": 0, "failed": 0, "pending": 0}
    committed_mb = 0
    running_rss_total_kb = 0
    running_cpu_total_sec = 0.0
    limits: list[int] = []

    total_uptime_running = 0.0
    total_lifetime_terminated = 0.0
    count_uptime_running = 0
    count_lifetime_terminated = 0

    now = time.time()

    for c in containers.values():
        status = getattr(c, "status", None)
        started_at = getattr(c, "started_at", None)
        stopped_at = getattr(c, "stopped_at", None)

        if status is ContainerStatus.RUNNING:
            counts["running"] += 1

            limit = getattr(c, "memory_limit_mb", None)
            if limit is not None:
                committed_mb += int(limit)

            pid = getattr(c, "pid", None)
            if pid:
                rss = get_process_memory_kb(pid)
                if rss is not None:
                    running_rss_total_kb += rss

                cpu = cpu_time_seconds(pid)
                if cpu is not None:
                    running_cpu_total_sec += cpu

            if started_at is not None:
                total_uptime_running += (now - started_at)
                count_uptime_running += 1

        elif status is ContainerStatus.TERMINATED:
            counts["terminated"] += 1
            if started_at is not None and stopped_at is not None:
                total_lifetime_terminated += (stopped_at - started_at)
                count_lifetime_terminated += 1

        elif status is ContainerStatus.FAILED:
            counts["failed"] += 1
            if started_at is not None and stopped_at is not None:
                total_lifetime_terminated += (stopped_at - started_at)
                count_lifetime_terminated += 1

        elif status is ContainerStatus.PENDING:
            counts["pending"] += 1

        limit_for_avg = getattr(c, "memory_limit_mb", None)
        if limit_for_avg is not None:
            limits.append(int(limit_for_avg))

    total_mb = None
    try:
        total_mb = get_total_memory_mb()
    except Exception:
        total_mb = None

    memory_policy_limit_mb = None
    if total_mb is not None:
        memory_policy_limit_mb = int(total_mb * float(memory_threshold_fraction))

    avg_limit = int(sum(limits) / len(limits)) if limits else None

    avg_uptime_running = (
        total_uptime_running / count_uptime_running if count_uptime_running > 0 else None
    )
    avg_lifetime_terminated = (
        total_lifetime_terminated / count_lifetime_terminated if count_lifetime_terminated > 0 else None
    )

    avg_cpu_per_running = (
        running_cpu_total_sec / counts["running"] if counts["running"] > 0 else None
    )

    metrics.update({
        "total_containers": len(containers),
        "running": counts["running"],
        "terminated": counts["terminated"],
        "failed": counts["failed"],
        "pending": counts["pending"],
        "created_count": getattr(orch, "created_count", None),
        "rejected_containers": getattr(orch, "rejected_containers", None),
        "peak_running": getattr(orch, "peak_running", None),
        "committed_memory_limit_mb": committed_mb,
        "memory_policy_limit_mb": memory_policy_limit_mb,
        "running_containers_total_rss_kb": running_rss_total_kb,
        "running_containers_total_cpu_seconds": running_cpu_total_sec,
        "average_cpu_seconds_per_running_container": avg_cpu_per_running,
        "average_container_memory_limit_mb": avg_limit,
        "system_total_memory_mb": total_mb,
        "average_uptime_running_seconds": avg_uptime_running,
        "average_lifetime_terminated_seconds": avg_lifetime_terminated,
    })

    return metrics
