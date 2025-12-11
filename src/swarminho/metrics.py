from pathlib import Path

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