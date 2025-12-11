from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, TYPE_CHECKING

from src.swarminho.filesystem import read_logs
from src.swarminho.runtime import start_container, is_container_running, memory_usage_kb

if TYPE_CHECKING:
    from src.swarminho.metrics import ContainerMetrics


class ContainerStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    FAILED = "FAILED"


@dataclass
class ContainerInfo:
    name: str
    command: str
    mem_mb: Optional[int]
    pid: Optional[int] = None
    status: ContainerStatus = ContainerStatus.PENDING


class Orchestrator:
    def __init__(self):
        self.containers: Dict[str, ContainerInfo] = {}
        self.metrics_cache: Dict[str, "ContainerMetrics"] = {}
        
    def create_container(self, name: str, command: str, mem_mb: Optional[int] = None):
        """Create and start a container process, tracking its metadata."""
        if name in self.containers:
            raise ValueError(f"Container with name {name} already exists.")
        
        container_info = ContainerInfo(name=name, command=command, mem_mb=mem_mb)

        pid = start_container(name, command, mem_mb)
        container_info.pid = pid
        container_info.status = ContainerStatus.RUNNING
        self.containers[name] = container_info
        return container_info

    def list_containers(self) -> List[ContainerInfo]:
        for container in self.containers.values():
            self._refresh_status(container)
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

    def get_metrics(self, name: str) -> "ContainerMetrics":
        from src.swarminho.metrics import collect_metrics, cpu_percent

        info = self.containers.get(name)
        if not info:
            raise ValueError(f"No container found with name {name}.")
        self._refresh_status(info)
        current = collect_metrics(info)
        previous = self.metrics_cache.get(name)
        if previous:
            current.cpu_percent = cpu_percent(previous, current)
        self.metrics_cache[name] = current
        return current

    def _refresh_status(self, container: ContainerInfo) -> None:
        """Sync in-memory status with the actual process state."""
        if container.pid is None:
            return
        if is_container_running(container.pid):
            container.status = ContainerStatus.RUNNING
        elif container.status == ContainerStatus.RUNNING:
            container.status = ContainerStatus.TERMINATED
