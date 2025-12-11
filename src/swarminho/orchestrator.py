from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

from .filesystem import read_logs
from .runtime import start_container, is_container_running, memory_usage_kb


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
        
    def create_container(self, name: str, command: str, mem_mb: Optional[int] = None):
        """_summary_

        Args:
            name (str): _description_
            command (str): _description_
            mem_mb (Optional[int], optional): _description_. Defaults to None.

        Raises:
            ValueError: _description_

        Returns:
            _type_: _description_
        """
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