import subprocess 
from pathlib import Path
from typing import Optional

from .filesystem import (
    prepare_rootfs,
    prepare_logs_dir,
    stdout_log_path,
    stderr_log_path,
)

def start_container(name:str, command: str, memory_limit_mb: Optional[int] = None) -> int:
    rootfs: Path = prepare_rootfs(name)
    logs_dir: Path = prepare_logs_dir(name)

    stdout_f = stdout_log_path(name).open("ab")
    stderr_f = stderr_log_path(name).open("ab")
    wrapped_cmd = _build_wrapped_command(command, memory_limit_mb)
    proc = subprocess.Popen(
        ["bash", "-lc", wrapped_cmd],
        cwd=str(rootfs),
        stdout=stdout_f,
        stderr=stderr_f,
    )

    return proc.pid


def is_container_running(pid: int) -> bool:
    try:
        subprocess.run(
            ["kill", "-0", str(pid)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except subprocess.CalledProcessError:
        return False

    
def memory_usage_kb(pid: int) -> Optional[int]:
    status_path = Path(f"/proc/{pid}/status")
    if not status_path.exists():
        return None

    for line in status_path.read_text(errors="ignore").splitlines():
        if line.startswith("VmRSS:"):
            parts = line.split()
            if len(parts) >= 2:
                try:
                    return int(parts[1])  # em kB
                except ValueError:
                    return None
    return None

def _build_wrapped_command(command: str, memory_limit_mb: Optional[int]) -> str:
    if memory_limit_mb is not None:
        mem_kb = memory_limit_mb * 1024
        return f"ulimit -v {mem_kb}; exec {command}"
    return f"exec {command}"
