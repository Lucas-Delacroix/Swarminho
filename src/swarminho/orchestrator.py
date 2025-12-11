from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

from .filesystem import read_logs
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
        
    def create_container(self, name: str, command: str, memory_limit_mb: Optional[int] = None):
        """_summary_

        Args:
            name (str): _description_
            command (str): _description_
            memory_limit_mb (Optional[int], optional): _description_. Defaults to None.

        Raises:
            ValueError: _description_

        Returns:
            _type_: _description_
        """
        if name in self.containers:
            raise ValueError(f"Container with name {name} already exists.")
        
        self._ensure_memory_policy_allows(name, memory_limit_mb)
        container_info = ContainerInfo(name=name, command=command, memory_limit_mb=memory_limit_mb)

        pid = start_container(name, command, memory_limit_mb)
        
        container_info.pid = pid
        container_info.status = ContainerStatus.RUNNING
        self.containers[name] = container_info

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
    
    def _current_committed_memory_limit_mb(self) -> int:
        """
        Soma os memory_limit_mb dos containers RUNNING.
        Política simples: usamos o limite configurado, não o uso real.
        """
        total = 0
        for c in self.containers.values():
            if c.status == ContainerStatus.RUNNING and c.memory_limit_mb is not None:
                total += c.memory_limit_mb
        return total
    
    def _ensure_memory_policy_allows(self, name: str, memory_limit_mb: Optional[int]) -> None:
        """
        Aplica a política de admissão de memória.

        Levanta RuntimeError se o novo container não couber dentro
        da fração configurada de MemTotal.
        """
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