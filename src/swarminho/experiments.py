"""experiments.py

Script para executar experimentos controlados com o Orchestrator e o
MetricsCollector do projeto `swarminho`.

Objetivos:
- executar cenários típicos (ramp-up, burst, até rejeição) criando containers
  com diferentes limites de memória;
- durante a execução coletar snapshots de métricas (sistema + por-container);
- salvar resultados em disco para posterior análise (JSON).

Como usar (ex.: após rodar ./install.sh):

  # executa todos os cenários com configurações padrão
  ./.venv/bin/python experiments.py --outdir results

  # executa apenas o cenário "ramp" e coleta a cada 1s
  ./.venv/bin/python experiments.py --scenarios ramp --sample-interval 1.0

Observações de segurança e execução:
- Este script criará subprocessos que executarão comandos Python simples que
  alocam memória via bytearray(). Tenha cuidado ao escolher memórias grandes
  em máquinas reais (pode esgotar memória e tornar o sistema lento).
- Para manter independência do CLI do projeto, o script importa
  `swarminho.orchestrator.Orchestrator` diretamente.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from swarminho.orchestrator import Orchestrator, ContainerInfo
from swarminho.metrics import MetricsCollector, snapshot_container


logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")
logger = logging.getLogger("experiments")


def now_ts() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def make_mem_alloc_command(mem_mb: int, duration_s: int = 60) -> str:
    """Gera um comando Python que aloca ~mem_mb megabytes usando bytearray e
    dorme por duration_s segundos.

    Usamos bytearray para ter controle direto do número de bytes alocados.
    """
    return (
        "python3 -c \"import time; a=bytearray(%d*1024*1024); time.sleep(%d)\""
        % (mem_mb, duration_s)
    )


def create_and_register(orch: Orchestrator, name: str, mem_mb: int, duration: int) -> Dict[str, Any]:
    cmd = make_mem_alloc_command(mem_mb, duration)
    try:
        info = orch.create_container(name=name, command=cmd, memory_limit_mb=mem_mb)
        logger.info(f"created container {name} pid={info.pid} mem_limit={mem_mb}MB")
        return {"name": name, "status": "created", "mem_limit_mb": mem_mb, "pid": info.pid}
    except Exception as e:
        logger.info(f"failed to create container {name}: {e}")
        return {"name": name, "status": "rejected", "mem_limit_mb": mem_mb, "error": str(e)}


def collect_periodic(mc: MetricsCollector, duration: float, sample_interval: float) -> List[Dict[str, Any]]:
    """Coleta snapshots por `duration` segundos a cada `sample_interval` segundos."""
    end = time.time() + duration
    snaps: List[Dict[str, Any]] = []
    while time.time() < end:
        snap = mc.collect()
        snaps.append(snap.to_dict())
        time.sleep(sample_interval)
    return snaps


def scenario_ramp(
    orch: Orchestrator,
    outdir: Path,
    base_mem: int = 32,
    count: int = 10,
    create_interval: float = 1.0,
    container_duration: int = 60,
    sample_interval: float = 0.5,
    policy_fraction: Optional[float] = None,
) -> None:
    """Cria containers um-a-um (ramp-up) e coleta métricas durante o processo."""
    logger.info("starting scenario: ramp")
    mc = MetricsCollector(orchestrator=orch, policy_fraction=policy_fraction)

    events: List[Dict[str, Any]] = []

    # timeline: antes, durante criação e depois
    total_duration = count * create_interval + container_duration + 1
    start_ts = now_ts()

    for i in range(count):
        name = f"ramp-{start_ts}-{i}"
        ev = create_and_register(orch, name, base_mem, container_duration)
        events.append({"time": time.time(), **ev})
        # coleta um snapshot após cada criação
        snap = mc.collect().to_dict()
        events.append({"time": time.time(), "type": "snapshot_post_create", "snapshot": snap})
        time.sleep(create_interval)

    # coleta final por total_duration
    logger.info("collecting final snapshots")
    snaps = collect_periodic(mc, duration=container_duration + 1, sample_interval=sample_interval)

    result = {"scenario": "ramp", "start_ts": start_ts, "events": events, "snapshots": snaps}
    outpath = outdir / f"ramp_{start_ts}.json"
    outpath.write_text(json.dumps(result, indent=2))
    logger.info(f"ramp scenario finished - results saved to {outpath}")


def scenario_burst(
    orch: Orchestrator,
    outdir: Path,
    base_mem: int = 64,
    count: int = 10,
    container_duration: int = 60,
    sample_interval: float = 0.5,
    policy_fraction: Optional[float] = None,
) -> None:
    logger.info("starting scenario: burst")
    mc = MetricsCollector(orchestrator=orch, policy_fraction=policy_fraction)
    events: List[Dict[str, Any]] = []
    start_ts = now_ts()

    # create all quickly
    for i in range(count):
        name = f"burst-{start_ts}-{i}"
        ev = create_and_register(orch, name, base_mem, container_duration)
        events.append({"time": time.time(), **ev})
        time.sleep(0.05)

    # collect for container_duration
    snaps = collect_periodic(mc, duration=container_duration + 1, sample_interval=sample_interval)

    result = {"scenario": "burst", "start_ts": start_ts, "events": events, "snapshots": snaps}
    outpath = outdir / f"burst_{start_ts}.json"
    outpath.write_text(json.dumps(result, indent=2))
    logger.info(f"burst scenario finished - results saved to {outpath}")


def scenario_until_reject(
    orch: Orchestrator,
    outdir: Path,
    base_mem: int = 64,
    max_attempts: int = 200,
    create_interval: float = 0.5,
    container_duration: int = 60,
    sample_interval: float = 0.5,
    policy_fraction: Optional[float] = None,
) -> None:
    logger.info("starting scenario: until_reject")
    mc = MetricsCollector(orchestrator=orch, policy_fraction=policy_fraction)
    events: List[Dict[str, Any]] = []
    start_ts = now_ts()

    rejects = 0
    for i in range(max_attempts):
        name = f"until-{start_ts}-{i}"
        ev = create_and_register(orch, name, base_mem, container_duration)
        events.append({"time": time.time(), **ev})
        if ev.get("status") == "rejected":
            rejects += 1
            # if we see consecutive rejects, assume saturation
            if rejects >= 3:
                logger.info("multiple consecutive rejects detected, stopping creation")
                break
        else:
            rejects = 0
        time.sleep(create_interval)

    # collect for a while to observe system
    snaps = collect_periodic(mc, duration=container_duration + 1, sample_interval=sample_interval)

    result = {"scenario": "until_reject", "start_ts": start_ts, "events": events, "snapshots": snaps}
    outpath = outdir / f"until_reject_{start_ts}.json"
    outpath.write_text(json.dumps(result, indent=2))
    logger.info(f"until_reject scenario finished - results saved to {outpath}")


def cleanup_all(orch: Orchestrator) -> None:
    logger.info("cleaning up containers (attempting to stop and remove all)")
    for name in list(orch.containers.keys()):
        try:
            orch.stop_container(name)
        except Exception:
            pass
        try:
            orch.remove_container(name)
        except Exception:
            pass


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run experiments with swarminho orchestrator")
    parser.add_argument("--outdir", default="experiments_results", help="directory to save results")
    parser.add_argument("--scenarios", default="all", help="comma-separated: ramp,burst,until_reject or 'all'")
    parser.add_argument("--sample-interval", type=float, default=0.5, help="seconds between metric samples")
    parser.add_argument("--policy-fraction", type=float, default=None, help="(optional) memory policy fraction for MetricsCollector (e.g. 0.2)")
    parser.add_argument("--no-cleanup", dest="cleanup", action="store_false", help="do not stop/remove containers at the end")
    args = parser.parse_args(argv)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    orch = Orchestrator()

    scenarios = [s.strip() for s in args.scenarios.split(",")] if args.scenarios else ["all"]
    if "all" in scenarios:
        scenarios = ["ramp", "burst", "until_reject"]

    try:
        if "ramp" in scenarios:
            scenario_ramp(
                orch=orch,
                outdir=outdir,
                base_mem=32,
                count=8,
                create_interval=1.0,
                container_duration=30,
                sample_interval=args.sample_interval,
                policy_fraction=args.policy_fraction,
            )

        if "burst" in scenarios:
            scenario_burst(
                orch=orch,
                outdir=outdir,
                base_mem=64,
                count=6,
                container_duration=30,
                sample_interval=args.sample_interval,
                policy_fraction=args.policy_fraction,
            )

        if "until_reject" in scenarios:
            scenario_until_reject(
                orch=orch,
                outdir=outdir,
                base_mem=64,
                max_attempts=200,
                create_interval=0.5,
                container_duration=30,
                sample_interval=args.sample_interval,
                policy_fraction=args.policy_fraction,
            )

    finally:
        if args.cleanup:
            cleanup_all(orch)
        else:
            logger.info("skipping cleanup as requested")

    logger.info("experiments finished")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
