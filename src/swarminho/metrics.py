"""
Métricas do Swarminho (versão estendida)

Este módulo provê utilitários e um coletor de métricas para uso nos
experimentos do orquestrador.

Principais acréscimos nesta versão:
- snapshots por-container (inclui PID, limite de memória, uso atual em kB/MB,
  tamanhos de logs e tamanho do rootfs quando aplicável);
- métricas globais adicionais: MemAvailable, swap, load average, uptime;
- informações relacionadas à política de memória (se for informado o
  `policy_fraction` no coletor): policy_limit_mb e committed_fraction;
- contadores adicionais (failed_containers, peak_running) quando o
  objeto Orchestrator os expor.

Design: para evitar import circular com orchestrator.py, este módulo NÃO
importa Orchestrator. Em vez disso, `MetricsCollector` aceita uma
instância qualquer que siga a interface esperada (atributo `containers` e
opcionais `created_count`, `rejected_containers`, `peak_running`,
`_current_committed_memory_limit_mb`).
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, Any, Dict, Tuple
import os
import time

from .filesystem import stdout_log_path, stderr_log_path, rootfs_dir

def get_total_memory_mb() -> int:
    """Lê /proc/meminfo e retorna MemTotal em MB."""
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
    """Retorna MemAvailable em MB quando disponível; caso contrário tenta
    estimar como MemFree + Buffers + Cached."""
    meminfo_path = Path("/proc/meminfo")
    if not meminfo_path.exists():
        raise RuntimeError("/proc/meminfo não encontrado; só funciona em Linux.")

    fields = {}
    for line in meminfo_path.read_text().splitlines():
        parts = line.split()
        if not parts:
            continue
        key = parts[0].rstrip(":")
        try:
            val = int(parts[1])
        except (IndexError, ValueError):
            continue
        fields[key] = val

    if "MemAvailable" in fields:
        return fields["MemAvailable"] // 1024


    memfree = fields.get("MemFree", 0)
    buffers = fields.get("Buffers", 0)
    cached = fields.get("Cached", 0)
    return (memfree + buffers + cached) // 1024


def get_swap_info_mb() -> Tuple[int, int]:
    """Retorna (swap_total_mb, swap_free_mb)."""
    meminfo_path = Path("/proc/meminfo")
    if not meminfo_path.exists():
        return (0, 0)

    swap_total = 0
    swap_free = 0
    for line in meminfo_path.read_text().splitlines():
        if line.startswith("SwapTotal:"):
            parts = line.split()
            swap_total = int(parts[1]) // 1024
        elif line.startswith("SwapFree:"):
            parts = line.split()
            swap_free = int(parts[1]) // 1024
    return swap_total, swap_free


def get_uptime_seconds() -> float:
    """Retorna o tempo de atividade do sistema em segundos, lendo /proc/uptime."""
    uptime_path = Path("/proc/uptime")
    if not uptime_path.exists():
        raise RuntimeError("/proc/uptime não encontrado; só funciona em Linux.")
    contents = uptime_path.read_text().strip().split()
    return float(contents[0])


def get_cpu_count() -> int:
    """Número lógico de CPUs (via os.cpu_count())."""
    return os.cpu_count() or 1


def get_load_average() -> Tuple[float, float, float]:
    """Retorna a carga média (1, 5, 15 minutos)."""
    try:
        return os.getloadavg()
    except OSError:
        return (0.0, 0.0, 0.0)


def get_pid_memory_kb(pid: Optional[int]) -> Optional[int]:
    """Retorna VmRSS do processo em kB, ou None se não existir."""
    if pid is None:
        return None
    status_path = Path(f"/proc/{pid}/status")
    if not status_path.exists():
        return None

    for line in status_path.read_text(errors="ignore").splitlines():
        if line.startswith("VmRSS:"):
            parts = line.split()
            try:
                return int(parts[1])
            except (IndexError, ValueError):
                return None
    return None


def file_size_bytes(path: Path) -> int:
    """Retorna tamanho do arquivo em bytes, 0 se não existir."""
    try:
        return path.stat().st_size
    except Exception:
        return 0


def dir_size_bytes(path: Path) -> int:
    """Retorna soma dos tamanhos de arquivos em um diretório recursivamente."""
    if not path.exists():
        return 0
    total = 0
    try:
        for p in path.rglob("*"):
            if p.is_file():
                try:
                    total += p.stat().st_size
                except Exception:
                    pass
    except Exception:
        return 0
    return total



@dataclass
class MetricsSnapshot:
    timestamp: float
    total_mem_mb: int
    available_mem_mb: int
    swap_total_mb: int
    swap_free_mb: int
    cpu_count: int
    load_avg_1m: float
    load_avg_5m: float
    load_avg_15m: float
    uptime_seconds: float


    running_containers: Optional[int] = None
    pending_containers: Optional[int] = None
    terminated_containers: Optional[int] = None
    failed_containers: Optional[int] = None
    created_count: Optional[int] = None
    rejected_containers: Optional[int] = None
    committed_memory_mb: Optional[int] = None
    policy_limit_mb: Optional[int] = None
    committed_fraction: Optional[float] = None
    peak_running: Optional[int] = None


    per_container: Optional[Dict[str, Dict]] = None

    def to_dict(self) -> Dict:
        return asdict(self)


def snapshot_container(info: Any) -> Dict:
    """Gera um snapshot por container a partir de um ContainerInfo-like.

    Campos produzidos (por convenção):
    - name, pid, status (string), mem_limit_mb, mem_usage_kb, mem_usage_mb
    - stdout_size_bytes, stderr_size_bytes, rootfs_size_bytes

    Observação: funções de logs/rootfs usam as rotas de filesystem.
    """
    name = getattr(info, "name", None)
    pid = getattr(info, "pid", None)
    status = getattr(info, "status", None)

    mem_limit = getattr(info, "memory_limit_mb", None)
    if mem_limit is None:
        mem_limit = getattr(info, "mem_mb", None)

    usage_kb = get_pid_memory_kb(pid) if pid is not None else None
    usage_mb = None
    if usage_kb is not None:
        usage_mb = usage_kb // 1024


    stdout_path = stdout_log_path(name) if name is not None else None
    stderr_path = stderr_log_path(name) if name is not None else None
    rootfs_path = rootfs_dir(name) if name is not None else None

    stdout_size = file_size_bytes(stdout_path) if stdout_path is not None else 0
    stderr_size = file_size_bytes(stderr_path) if stderr_path is not None else 0
    rfs_size = dir_size_bytes(rootfs_path) if rootfs_path is not None else 0

    return {
        "name": name,
        "pid": pid,
        "status": str(status) if status is not None else None,
        "mem_limit_mb": mem_limit,
        "mem_usage_kb": usage_kb,
        "mem_usage_mb": usage_mb,
        "stdout_size_bytes": stdout_size,
        "stderr_size_bytes": stderr_size,
        "rootfs_size_bytes": rfs_size,
    }


class MetricsCollector:
    """Coletor que monta um MetricsSnapshot.

    `orchestrator` (opcional): qualquer objeto que exponha `containers`
    (dict nome -> ContainerInfo-like). Se `policy_fraction` for fornecido (float
    entre 0 e 1), o coletor calculará `policy_limit_mb = int(total_mem_mb * policy_fraction)` e
    `committed_fraction` = committed_mem_mb / policy_limit_mb.
    """

    def __init__(self, orchestrator: Optional[Any] = None, policy_fraction: Optional[float] = None):
        self.orch = orchestrator
        self.policy_fraction = policy_fraction

    def collect(self) -> MetricsSnapshot:
        ts = time.time()
        total = get_total_memory_mb()
        avail = get_available_memory_mb()
        swap_total, swap_free = get_swap_info_mb()
        cpu_count = get_cpu_count()
        load1, load5, load15 = get_load_average()
        uptime = get_uptime_seconds()


        running = pending = terminated = failed = None
        created = rejected = committed = None
        per_container_snap = None
        policy_limit = None
        committed_fraction = None
        peak_running = None

        if self.orch is not None:
            try:
                containers = getattr(self.orch, "containers", {})


                running = sum(1 for c in containers.values() if getattr(c, "status", None) and str(getattr(c, "status")).upper() == "RUNNING")
                pending = sum(1 for c in containers.values() if getattr(c, "status", None) and str(getattr(c, "status")).upper() == "PENDING")
                terminated = sum(1 for c in containers.values() if getattr(c, "status", None) and str(getattr(c, "status")).upper() == "TERMINATED")
                failed = sum(1 for c in containers.values() if getattr(c, "status", None) and str(getattr(c, "status")).upper() == "FAILED")

                created = getattr(self.orch, "created_count", None)
                rejected = getattr(self.orch, "rejected_containers", None)
                peak_running = getattr(self.orch, "peak_running", None)


                committed = None
                if hasattr(self.orch, "_current_committed_memory_limit_mb"):
                    try:
                        committed = self.orch._current_committed_memory_limit_mb()
                    except Exception:
                        committed = None
                else:
                    s = 0
                    for c in containers.values():
                        mem = getattr(c, "memory_limit_mb", None)
                        if mem:
                            s += mem
                    committed = s


                per_container_snap = {}
                for name, c in containers.items():
                    try:
                        per_container_snap[name] = snapshot_container(c)
                    except Exception:
                        per_container_snap[name] = {
                            "name": getattr(c, "name", None),
                        }


                if self.policy_fraction is not None:
                    try:
                        policy_limit = int(total * float(self.policy_fraction))
                        if policy_limit > 0 and committed is not None:
                            committed_fraction = float(committed) / float(policy_limit)
                    except Exception:
                        policy_limit = None
                        committed_fraction = None

            except Exception:

                pass

        snapshot = MetricsSnapshot(
            timestamp=ts,
            total_mem_mb=total,
            available_mem_mb=avail,
            swap_total_mb=swap_total,
            swap_free_mb=swap_free,
            cpu_count=cpu_count,
            load_avg_1m=load1,
            load_avg_5m=load5,
            load_avg_15m=load15,
            uptime_seconds=uptime,
            running_containers=running,
            pending_containers=pending,
            terminated_containers=terminated,
            failed_containers=failed,
            created_count=created,
            rejected_containers=rejected,
            committed_memory_mb=committed,
            policy_limit_mb=policy_limit,
            committed_fraction=committed_fraction,
            peak_running=peak_running,
            per_container=per_container_snap,
        )

        return snapshot


def pretty_print_snapshot(snapshot: MetricsSnapshot) -> None:
    d = snapshot.to_dict()
    print("Metrics snapshot:")
    for k, v in sorted(d.items()):
        print(f"  {k}: {v}")
