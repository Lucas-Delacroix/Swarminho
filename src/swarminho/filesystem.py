from pathlib import Path 
import shutil 
from typing import Final


CONTAINERS_ROOT: Final[Path] = Path("./containers")

# Será utilizado depois para implementar o debootstrap ou semelhante
BASE_ROOTFS: Final[Path] = Path("./base_rootfs")

def _ensure_containers_root() -> None:
    """Garante que o diretório raiz dos containers existe."""
    CONTAINERS_ROOT.mkdir(parents=True, exist_ok=True)

import re

def _validate_container_name(name: str) -> None:
    """Valida o nome do container para evitar problemas de segurança.

    Regras:
    - permitido: letras, números, '.', '_' e '-'
    - proibido: '/', '\\', sequência '..' (path traversal)
    - não pode ser vazio
    - não pode começar com '.' (evita arquivos ocultos) nem com '-' (pode confundir flags)
    - comprimento máximo: 255 caracteres
    """
    if not name:
        raise ValueError("Nome de container inválido: nome vazio")

    # proíbe barras (paths) e path traversal
    if "/" in name or "\\" in name:
        raise ValueError(f"Nome de container inválido (não deve conter '/' ou '\\\\'): {name!r}")

    if ".." in name:
        raise ValueError(f"Nome de container inválido (não deve conter '..'): {name!r}")

    # não permitir nomes iniciando com '.' ou '-' (evita hidden files e confusão com flags)
    if name[0] in {".", "-"}:
        raise ValueError(f"Nome de container inválido (não pode começar com '.' ou '-'): {name!r}")

    if len(name) > 255:
        raise ValueError(f"Nome de container inválido (comprimento maior que 255): {name!r}")

    # somente caracteres alfanuméricos, ponto, underline e hífen
    if not re.fullmatch(r"[A-Za-z0-9._-]+", name):
        raise ValueError(
            f"Nome de container inválido (caracteres permitidos: letras, números, '.', '_' e '-'): {name!r}"
        )


def container_path(name: str) -> Path:
    """Retorna o caminho completo do container dado seu nome."""
    _ensure_containers_root()
    _validate_container_name(name)
    return CONTAINERS_ROOT / name

def rootfs_dir(name: str) -> Path:
    """
    Retorna o caminho do diretório root filesystem lógico do container.
    containers/<name>/rootfs
    """
    return container_path(name) / "rootfs"

def logs_dir(name: str) -> Path:
    """
    Retorna o caminho do diretório de logs do container.
    containers/<name>/logs
    """
    return container_path(name) / "logs"

def stdout_log_path(name: str) -> Path:
    """
    Retorna o caminho do arquivo de log de stdout do container.
    containers/<name>/logs/stdout.log
    """
    return logs_dir(name) / "stdout.log"

def stderr_log_path(name: str) -> Path:
    """
    Retorna o caminho do arquivo de log de stderr do container.
    containers/<name>/logs/stderr.log
    """
    return logs_dir(name) / "stderr.log"

def container_struct_exists(name: str) -> bool:
    """Verifica se já existe estrutura de diretórios para o container."""
    return container_path(name).exists()

def prepare_rootfs(name: str, copy_base: bool = False) -> Path:
    """
    Prepara o rootfs do container.

    - Garante que containers/<name>/rootfs existe.
    - Se BASE_ROOTFS existir e copy_base=True, copia o conteúdo para o rootfs.
    - Operação idempotente: se o rootfs já existe, apenas o retorna.

    Retorna o Path do rootfs.
    """
    rfs = rootfs_dir(name)
    if rfs.exists():
        return rfs

    rfs.mkdir(parents=True, exist_ok=True)

    if copy_base and BASE_ROOTFS.exists():
        shutil.copytree(BASE_ROOTFS, rfs, dirs_exist_ok=True)

    return rfs

def prepare_logs_dir(name: str) -> Path:
    """
    Prepara o diretório de logs do container.

    - Garante que containers/<name>/logs existe.
    - Operação idempotente: se o diretório já existe, apenas o retorna.

    Retorna o Path do diretório de logs.
    """
    ld = logs_dir(name)
    ld.mkdir(parents=True, exist_ok=True)
    return ld

def read_logs(name: str) -> tuple[str, str]:
    """
    Lê os arquivos de log stdout e stderr do container.

    Retorna uma tupla (stdout_text, stderr_text) com o conteúdo dos logs.
    Se os arquivos não existirem, retorna strings vazias.
    """
    stdout_path = stdout_log_path(name)
    stderr_path = stderr_log_path(name)

    stdout = ""
    stderr = ""

    if stdout_path.exists():
        stdout = stdout_path.read_text(encoding="utf-8", errors="ignore")
    if stderr_path.exists():
        stderr = stderr_path.read_text(encoding="utf-8", errors="ignore")

    return stdout, stderr


def remove_container_storage(name: str) -> None:
    """
    Remove totalmente os dados de um container (rootfs + logs).

    ⚠️ Cuidado: isso apaga tudo em containers/<name>.
    Não deve ser chamado se o processo ainda estiver rodando.
    """
    cdir = container_path(name)
    if cdir.exists():
        shutil.rmtree(cdir)