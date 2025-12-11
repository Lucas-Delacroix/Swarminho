from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional
import os
import signal
import time

from .filesystem import read_logs, remove_container_storage
from .runtime import start_container, is_container_running, memory_usage_kb
from .metrics import get_total_memory_mb

MEMORY_THRESHOLD_FRACTION = 0.2

class ContainerStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    FAILED = "FAILED"

@dataclass
class ContainerInfo:
    name: str
    command: str
    memory_limit_mb: Optional[int]
    pid: Optional[int] = None
    status: ContainerStatus = ContainerStatus.PENDING

class Orchestrator:
    def __init__(self):
        self.containers: Dict[str, ContainerInfo] = {}
        self.rejected_containers: int = 0
        self.created_count: int = 0
        self.peak_running: int = 0

    def create_container(self, name: str, command: str, memory_limit_mb: Optional[int] = None) -> ContainerInfo:
        if name in self.containers:
            raise ValueError(f"Container with name {name} already exists.")

        try:
            self._ensure_memory_policy_allows(name, memory_limit_mb)
        except RuntimeError:
            self.rejected_containers += 1
            raise

        container_info = ContainerInfo(name=name, command=command, memory_limit_mb=memory_limit_mb)

        pid = start_container(name, command, memory_limit_mb)

        container_info.pid = pid
        container_info.status = ContainerStatus.RUNNING
        self.containers[name] = container_info

        self.created_count += 1

        current_running = sum(1 for c in self.containers.values() if c.status == ContainerStatus.RUNNING)
        if current_running > self.peak_running:
            self.peak_running = current_running

        return container_info

    def list_containers(self) -> List[ContainerInfo]:
        for container in self.containers.values():
            if container.pid is None:
                continue
            if is_container_running(container.pid):
                container.status = ContainerStatus.RUNNING
            else:
                if container.status == ContainerStatus.RUNNING:
                    container.status = ContainerStatus.TERMINATED
        return list(self.containers.values())

    def get_logs(self, name: str) -> tuple[str, str]:
        if name not in self.containers:
            raise ValueError(f"No container found with name {name}.")
        return read_logs(name)

    def get_memory_usage_kb(self, name: str) -> Optional[int]:
        info = self.containers.get(name)
        if not info or info.pid is None:
            return None
        return memory_usage_kb(info.pid)

    def stop_container(self, name: str, timeout: float = 3.0) -> None:
        """
        Tenta terminar gracilmente o processo do container (SIGTERM) e, se necessário,
        força com SIGKILL depois de `timeout` segundos.
        Atualiza o status do ContainerInfo.
        """
        info = self.containers.get(name)
        if not info:
            raise ValueError(f"Container {name!r} não encontrado.")
        if info.pid is None:
            info.status = ContainerStatus.TERMINATED
            return

        pid = info.pid
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            info.status = ContainerStatus.TERMINATED
            return
        except PermissionError:
            info.status = ContainerStatus.FAILED
            return

        deadline = time.time() + timeout
        while time.time() < deadline:
            if not is_container_running(pid):
                info.status = ContainerStatus.TERMINATED
                return
            time.sleep(0.1)

        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            info.status = ContainerStatus.TERMINATED
            return
        except PermissionError:
            info.status = ContainerStatus.FAILED
            return

        time.sleep(0.1)
        if not is_container_running(pid):
            info.status = ContainerStatus.TERMINATED
        else:
            info.status = ContainerStatus.FAILED

    def remove_container(self, name: str, force_stop: bool = True) -> None:
        """
        Remove o container da tabela + remove dados em disco.
        Se force_stop for True tenta parar antes de remover.
        """
        info = self.containers.get(name)
        if not info:
            raise ValueError(f"Container {name!r} não encontrado.")

        if force_stop and info.pid is not None:
            try:
                self.stop_container(name)
            except Exception:
                info.status = ContainerStatus.FAILED

        try:
            remove_container_storage(name)
        except Exception:
            pass

        del self.containers[name]

    def _current_committed_memory_limit_mb(self) -> int:
        total = 0
        for c in self.containers.values():
            if c.status == ContainerStatus.RUNNING and c.memory_limit_mb is not None:
                total += c.memory_limit_mb
        return total

    def _ensure_memory_policy_allows(self, name: str, memory_limit_mb: Optional[int]) -> None:
        requested_mb = memory_limit_mb or 0
        if requested_mb <= 0:
            return

        total_mb = get_total_memory_mb()
        max_allowed_mb = int(total_mb * MEMORY_THRESHOLD_FRACTION)
        committed_mb = self._current_committed_memory_limit_mb()

        if committed_mb + requested_mb > max_allowed_mb:
            raise RuntimeError(
                f"Memória insuficiente para iniciar container {name!r}: "
                f"solicitado {requested_mb} MB, comprometido {committed_mb} MB, "
                f"limite da política {max_allowed_mb} MB (de {total_mb} MB totais)."
            )
