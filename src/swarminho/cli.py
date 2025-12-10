import argparse
from typing import Optional, Sequence

from src.swarminho.orchestrator import Orchestrator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Swarminho CLI")
    subparsers = parser.add_subparsers(dest="cmd")
    subparsers.required = True

    # swarminho run NAME --mem 64 --cmd "python -c 'print(42)'"
    p_run = subparsers.add_parser("run", help="Criar e iniciar um container")
    p_run.add_argument("name")
    p_run.add_argument("--mem", type=int, default=None, help="Memória em MB")
    p_run.add_argument(
        "--cmd",
        dest="command",
        required=True,
        help="Comando a executar",
    )
    p_run.set_defaults(handler=handle_run)

    # swarminho ps
    p_ps = subparsers.add_parser("ps", help="Listar containers")
    p_ps.set_defaults(handler=handle_ps)

    # swarminho logs NAME
    p_logs = subparsers.add_parser("logs", help="Mostrar logs de um container")
    p_logs.add_argument("name")
    p_logs.set_defaults(handler=handle_logs)

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
        print(
            f"{c.name:15} {str(c.pid or '-'):8} "
            f"{c.status.value:10} {str(c.mem_mb or '-'):15}"
        )
    return 0


def handle_logs(orch: Orchestrator, args: argparse.Namespace) -> int:
    stdout, stderr = orch.get_logs(args.name)
    print("=== STDOUT ===")
    print(stdout or "(vazio)")
    print("\n=== STDERR ===")
    print(stderr or "(vazio)")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    orch = Orchestrator()
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 1

    return handler(orch, args)


if __name__ == "__main__":
    raise SystemExit(main())
