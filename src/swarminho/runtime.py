import subprocess 
from pathlib import Path
from typing import Optional
import os

from .filesystem import (
    prepare_rootfs,
    prepare_logs_dir,
    stdout_log_path,
    stderr_log_path,
)

def start_container(name:str, command: str, memory_limit_mb: Optional[int] = None) -> int:
    rootfs: Path = prepare_rootfs(name, copy_base=True)
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


def cpu_time_seconds(pid: int) -> Optional[float]:
    """
    Retorna o tempo total de CPU (user+system) usado pelo processo, em segundos,
    ou None se não for possível ler /proc/<pid>/stat.
    """
    stat_path = Path(f"/proc/{pid}/stat")
    if not stat_path.exists():
        return None

    try:
        fields = stat_path.read_text().split()
        utime = int(fields[13])
        stime = int(fields[14])
    except (IndexError, ValueError):
        return None

    clock_ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    return (utime + stime) / clock_ticks
    
    
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
