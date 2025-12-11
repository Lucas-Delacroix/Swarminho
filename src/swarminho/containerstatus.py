from enum import Enum


class ContainerStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    FAILED = "FAILED"