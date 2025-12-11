import argparse
import sys
import shlex
import readline
from typing import Optional, Sequence

from src.swarminho.orchestrator import Orchestrator
from src.swarminho.axaloti import print_banner

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="swarminho", add_help=False)
    subparsers = parser.add_subparsers(dest="cmd")

    # swarminho run NAME --mem 64 --cmd "python -c 'print(42)'"
    p_run = subparsers.add_parser("run", help="Criar e iniciar um container")
    p_run.add_argument("name")
    p_run.add_argument("--mem", type=int, default=None, help="Memória em MB")
    p_run.add_argument("--cmd", dest="command", required=True, help="Comando a executar")

    # swarminho ps
    p_ps = subparsers.add_parser("ps", help="Listar containers")

    # swarminho logs NAME
    p_logs = subparsers.add_parser("logs", help="Mostrar logs de um container")
    p_logs.add_argument("name")

    # help interno do shell
    p_help = subparsers.add_parser("help", help="Mostrar ajuda")

    return parser


def handle_run(orch: Orchestrator, args: argparse.Namespace) -> int:
    info = orch.create_container(args.name, args.command, args.mem)
    print(f"Container {info.name} iniciado com PID={info.pid}")
    return 0


def handle_ps(orch: Orchestrator, args: argparse.Namespace) -> int:
    containers = orch.list_containers()
    print(f"{'NAME':15} {'PID':8} {'STATUS':10} {'MEM_LIMIT(MB)':15}")
    print("-" * 60)
    for c in containers:
        print(f"{c.name:15} {str(c.pid or '-'):8} {c.status.value:10} {str(c.mem_mb or '-'):15}")
    return 0


def handle_logs(orch: Orchestrator, args: argparse.Namespace) -> int:
    stdout, stderr = orch.get_logs(args.name)
    print("=== STDOUT ===")
    print(stdout or "(vazio)")
    print("\n=== STDERR ===")
    print(stderr or "(vazio)")
    return 0


def handle_help(parser: argparse.ArgumentParser) -> int:
    parser.print_help()
    print("\nComandos disponíveis no shell interativo:")
    print("  run NAME --mem N --cmd 'COMANDO'")
    print("  ps")
    print("  logs NAME")
    print("  help")
    print("  exit / quit")
    return 0


def dispatch_command(orch: Orchestrator, parser: argparse.ArgumentParser, argv: Sequence[str]) -> int:
    """
    parser para executar comandos (tanto no modo
    'uma vez só' quanto dentro do shell interativo).
    """
    if not argv:
        return 0

    try:
        args = parser.parse_args(list(argv))
    except SystemExit:
        return 1

    cmd = args.cmd

    try:
        if cmd == "run":
            return handle_run(orch, args)
        elif cmd == "ps":
            return handle_ps(orch, args)
        elif cmd == "logs":
            return handle_logs(orch, args)
        elif cmd == "help":
            return handle_help(parser)
        else:
            print(f"Comando desconhecido: {cmd!r}")
            return 1
    except (ValueError, RuntimeError) as e:
        print(f"Erro: {e}")
        return 1


def repl() -> int:
    """
    Shell interativo do Swarminho.
    """
    orch = Orchestrator()
    parser = build_parser()
    print_banner()

    print("Swarminho interactive shell! Type 'help' for commands, 'exit' to quit.")

    while True:
        try:
            line = input("swarminho> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not line:
            continue

        if line in {"exit", "quit"}:
            break
        
        if readline is not None:
            readline.add_history(line)
            
        try:
            argv = shlex.split(line)
        except ValueError as e:
            print(f"Erro ao interpretar comando: {e}")
            continue

        dispatch_command(orch, parser, argv)

    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Se argv vazio/None → entra no modo interativo (REPL).
    Se argv tem algo (ex.: ['run', ...]) → executa só aquele comando.
    """
    if argv is None:
        argv = sys.argv[1:]

    if not argv:
        return repl()


    orch = Orchestrator()
    parser = build_parser()
    return dispatch_command(orch, parser, argv)


if __name__ == "__main__":
    raise SystemExit(main())