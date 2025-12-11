from dataclasses import dataclass, field
from typing import Dict, List, Optional
import os
import signal
import time

from .filesystem import read_logs, remove_container_storage
from .runtime import start_container, is_container_running, memory_usage_kb
from .metrics import get_total_memory_mb
from .containerstatus import ContainerStatus

MEMORY_THRESHOLD_FRACTION = 0.2

@dataclass
class ContainerInfo:
    name: str
    command: str
    memory_limit_mb: Optional[int]
    pid: Optional[int] = None
    status: ContainerStatus = ContainerStatus.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    stopped_at: Optional[float] = None

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
        container_info.started_at = time.time()

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
                    self._mark_terminated(container)
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


    def _mark_terminated(self, info: ContainerInfo) -> None:
        """
        Marca o container como TERMINATED e registra o timestamp de parada
        se ainda não tiver sido registrado.
        """
        info.status = ContainerStatus.TERMINATED
        if info.stopped_at is None:
            info.stopped_at = time.time()

    
    def _mark_failed(self, info: ContainerInfo) -> None:
        """
        Marca o container como FAILED e registra o timestamp de parada
        se ainda não tiver sido registrado.
        """
        info.status = ContainerStatus.FAILED
        if info.stopped_at is None:
            info.stopped_at = time.time()


    def stop_container(self, name: str, timeout: float = 3.0) -> None:
        """
        Tenta terminar gracilmente o processo do container (SIGTERM) e, se necessário,
        força com SIGKILL depois de `timeout` segundos.
        Atualiza o status e o timestamp de parada.
        """
        info = self.containers.get(name)
        if not info:
            raise ValueError(f"Container {name!r} não encontrado.")

        if info.pid is None:
            self._mark_terminated(info)
            return

        pid = info.pid

        if not self._send_signal_with_status(info, pid, signal.SIGTERM):
            return

        if self._wait_until_stopped(info, pid, timeout):
            return

        if not self._send_signal_with_status(info, pid, signal.SIGKILL):
            return

        
        time.sleep(0.1)
        if not is_container_running(pid):
            self._mark_terminated(info)
        else:
            self._mark_failed(info)


    def _send_signal_with_status(self, info: ContainerInfo, pid: int, sig: int) -> bool:
        """
        Envia um sinal ao processo e ajusta o status em caso de erro.

        Returns:
            True se o sinal foi enviado com sucesso e faz sentido continuar o fluxo.
            False se o processo já não existe ou houve erro de permissão
            (nesse caso o status é atualizado e o chamador deve interromper o fluxo).
        """
        try:
            os.kill(pid, sig)
            return True
        except ProcessLookupError:
            self._mark_terminated(info)
            return False
        except PermissionError:
            self._mark_failed(info)
            return False

    def _wait_until_stopped(self, info: ContainerInfo, pid: int, timeout: float) -> bool:
        """
        Aguarda até `timeout` segundos para o processo terminar.

        Returns:
            True se o processo terminou dentro do prazo (status ajustado para TERMINATED).
            False se ainda estiver rodando após o timeout.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not is_container_running(pid):
                self._mark_terminated(info)
                return True
            time.sleep(0.1)
        return False
        
    
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
                self._mark_failed(info)

        try:
            remove_container_storage(name)
        except Exception:
            pass

        del self.containers[name]


    def _current_committed_memory_limit_mb(self) -> int:
        """
        Calcula a soma dos limites de memória (em MB) dos containers RUNNING.

        Política usada:
        - Considera apenas containers com status RUNNING.
        - Usa o valor configurado em `memory_limit_mb`, não o uso real de memória.

        """
        total = 0
        for c in self.containers.values():
            if c.status == ContainerStatus.RUNNING and c.memory_limit_mb is not None:
                total += c.memory_limit_mb
        return total


    def _ensure_memory_policy_allows(self, name: str, memory_limit_mb: Optional[int]) -> None:
        """
        Aplica a política de admissão baseada em memória antes de criar um container.

        A política atual é:
        - Ler a memória total do sistema (MemTotal, em MB).
        - Calcular um limite máximo para containers como
          `MEMORY_THRESHOLD_FRACTION * MemTotal`.
        - Somar os limites de memória dos containers RUNNING.
        - Recusar a criação se `comprometido + solicitado` ultrapassar esse limite.
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
