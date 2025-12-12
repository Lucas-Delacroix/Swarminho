import json
import os
import matplotlib.pyplot as plt

file = "results/many_small_2025-12-11_22-38-40.json"

with open(file, "r") as f:
    data = json.load(f)

snapshots = data["snapshots"]

timestamps = [s["timestamp"] - snapshots[0]["timestamp"] for s in snapshots]

def extract(path):
    """Extrai uma lista de valores seguindo um caminho tipo 'orch_metrics.running'."""
    keys = path.split(".")
    out = []
    for s in snapshots:
        cur = s
        for k in keys:
            cur = cur[k]
        out.append(cur)
    return out

def save_plot(fig, filename, folder="graphs"):
    
    os.makedirs(folder, exist_ok=True)

    path = os.path.join(folder, filename)

    fig.savefig(path, dpi=300, bbox_inches="tight")
    print(f"Gráfico salvo em: {path}")


running = extract("orch_metrics.running")
terminated = extract("orch_metrics.terminated")
cpu_seconds = extract("orch_metrics.running_containers_total_cpu_seconds")
rss_kb = extract("orch_metrics.running_containers_total_rss_kb")
avg_uptime = extract("orch_metrics.average_uptime_running_seconds")
committed_mem = extract("orch_metrics.committed_memory_limit_mb")

containers_on_disk = extract("fs_metrics.containers_on_disk")
logs_size = extract("fs_metrics.total_logs_size_bytes")

plt.figure(figsize=(13, 7))
plt.plot(timestamps, running, label="Containers rodando")
plt.plot(timestamps, terminated, label="Containers terminados")
plt.xlabel("Tempo (s)")
plt.ylabel("Quantidade")
plt.title("Estado dos containers ao longo do tempo")
plt.legend()
plt.grid(True)
plt.savefig("src/graphics/plots/01-containers-rodando-pelo-tempo.png")

plt.figure(figsize=(13, 7))
plt.plot(timestamps, cpu_seconds)
plt.xlabel("Tempo (s)")
plt.ylabel("CPU total (segundos)")
plt.title("CPU usada pelos containers")
plt.grid(True)
plt.savefig("src/graphics/plots/02-cpu-usada-pelo-tempo.png")


plt.figure(figsize=(13, 7))
plt.plot(timestamps, rss_kb)
plt.xlabel("Tempo (s)")
plt.ylabel("RSS total (kB)")
plt.title("Uso de memória RSS dos containers")
plt.grid(True)
plt.savefig("src/graphics/plots/03-rss-total-pelo-tempo.png")

plt.figure(figsize=(13, 7))
plt.plot(timestamps, committed_mem)
plt.xlabel("Tempo (s)")
plt.ylabel("Memória comprometida (MB)")
plt.title("Memória alocada pelo orquestrador")
plt.grid(True)
plt.savefig("src/graphics/plots/04-memoria-alocada-pelo-tempo.png")


plt.figure(figsize=(13, 7))
plt.plot(timestamps, containers_on_disk)
plt.xlabel("Tempo (s)")
plt.ylabel("Quantidade de diretórios de containers")
plt.title("Containers presentes no disco")
plt.grid(True)
plt.savefig("src/graphics/plots/05-containers-presentes-no-disco.png")


plt.figure(figsize=(13, 7))
plt.plot(timestamps, logs_size)
plt.xlabel("Tempo (s)")
plt.ylabel("Tamanho total dos logs (bytes)")
plt.title("Tamanho cumulativo dos logs")
plt.grid(True)
plt.savefig("src/graphics/plots/06-tamanho-dos-logs.png")
