# Swarminho
Simulação de um Orquestrador de Containers para Estudo de Virtualização em Nível de Sistema Operacional, Gerenciamento de Memória e Sistemas de Arquivos

![Logo Axoloti](swarminhologo.webp)


## Pré-requisitos
- Linux 

## Instalação e setup
Opção rápida (cria `.venv`, instala em modo editável e gera um wrapper `~/.local/bin/swarminho`):

```bash
./install.sh
```

Instalação manual:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Comandos principais (`swarminho`)
Depois de instalar, o entrypoint `swarminho` fica disponível. Você pode rodá-lo em modo interativo ou passando o comando direto.

## Comandos exemplo
- swarminho run yes1 --mem 64 --cmd 'yes "EU AMO SO" > /dev/null'

### Shell interativo
```bash
swarminho
```
Dentro do shell:
- `run NAME --mem N --cmd "COMANDO"`: cria e inicia um container (N em MB; `--mem` é opcional).
- `ps`: lista containers conhecidos pelo orquestrador.
- `logs NAME`: mostra stdout/stderr do container.
- `help`: imprime a ajuda embutida.
- `exit` ou `quit`: sai do shell.

### Execução direta (modo one-shot)
- `swarminho run NAME --mem N --cmd "COMANDO"`: cria um container e retorna imediatamente.
- `swarminho ps`: lista containers.
- `swarminho logs NAME`: exibe os logs.

Os dados ficam em `containers/<nome>/rootfs` e os logs em `containers/<nome>/logs`.
