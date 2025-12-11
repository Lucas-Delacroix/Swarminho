import argparse
import time
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

    # swarminho metrics NAME
    p_metrics = subparsers.add_parser("metrics", help="Mostrar métricas de um container")
    p_metrics.add_argument("name")
    p_metrics.add_argument(
        "--interval",
        type=float,
        default=0.0,
        help="Intervalo em segundos para amostrar CPU%% (ex.: 1.0).",
    )
    p_metrics.set_defaults(handler=handle_metrics)

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


def handle_metrics(orch: Orchestrator, args: argparse.Namespace) -> int:
    metrics = orch.get_metrics(args.name)
    if metrics.cpu_percent is None and args.interval > 0:
        time.sleep(args.interval)
        metrics = orch.get_metrics(args.name)

    print(f"Container: {metrics.name}")
    print(f"Status: {metrics.status.value}")
    print(f"PID: {metrics.pid or '-'}")
    print(
        "Timestamp (UTC): "
        f"{metrics.timestamp.isoformat().replace('+00:00', 'Z')}"
    )
    print(f"RSS (kB): {metrics.rss_kb if metrics.rss_kb is not None else '-'}")
    print(f"CPU time (s): {metrics.cpu_time_s if metrics.cpu_time_s is not None else '-'}")
    print(
        f"CPU (%): {metrics.cpu_percent:.2f}"
        if metrics.cpu_percent is not None
        else "CPU (%): -"
    )
    print(f"Logs (bytes): {metrics.log_bytes}")
    print(f"Rootfs (bytes): {metrics.rootfs_bytes}")
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
