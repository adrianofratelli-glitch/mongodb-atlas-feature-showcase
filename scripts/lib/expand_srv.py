#!/usr/bin/env python3
"""
Converte uma URI `mongodb+srv://` na forma padrão `mongodb://host1,host2,host3`.

Motivo: o MongoDB Kafka source connector reparseia `connection.uri` a cada start
de task, e uma URI SRV obriga uma consulta DNS SRV + TXT nesse instante. Se o
resolver falhar (VPN, proxy, DNS do container), a task morre com

    ConfigException: Invalid value mongodb+srv://... for configuration
    connection.uri: Failed looking up TXT record for host <cluster>

deixando o connector RUNNING com a única task FAILED — silencioso no meio da
demo. Resolvendo o SRV uma vez, no setup, o restart da task deixa de depender
de DNS.

Lê a URI no stdin e escreve a URI expandida no stdout: a credencial nunca vira
argumento de linha de comando (visível em `ps`). Se a expansão não for possível,
devolve a URI original inalterada e avisa no stderr — degradar para o
comportamento antigo é melhor que travar o setup.
"""
from __future__ import annotations

import subprocess
import sys
from urllib.parse import parse_qsl, urlencode


def _dig(tipo: str, nome: str) -> list[str]:
    saida = subprocess.run(
        ["dig", "+short", "+time=3", "+tries=2", tipo, nome],
        capture_output=True, text=True, timeout=15,
    )
    if saida.returncode != 0:
        raise RuntimeError(f"dig {tipo} {nome} falhou: {saida.stderr.strip()[:120]}")
    return [linha.strip() for linha in saida.stdout.splitlines() if linha.strip()]


def expandir(uri: str) -> str:
    if not uri.startswith("mongodb+srv://"):
        return uri

    resto = uri[len("mongodb+srv://"):]
    autoridade, _, cauda = resto.partition("/")
    caminho, _, query = cauda.partition("?")
    # A senha pode conter '@' percent-encoded ou não; o último '@' separa.
    credencial, _, host = autoridade.rpartition("@")
    host = host.split(",")[0].split(":")[0]
    if not host:
        raise RuntimeError("host ausente na URI")

    # SRV: "0 0 27017 alvo." — prioridade, peso, porta, alvo.
    alvos = []
    for linha in _dig("SRV", f"_mongodb._tcp.{host}"):
        campos = linha.split()
        if len(campos) >= 4:
            alvos.append(f"{campos[3].rstrip('.')}:{campos[2]}")
    if not alvos:
        raise RuntimeError(f"nenhum registro SRV para _mongodb._tcp.{host}")

    # TXT traz as opções default do cluster (replicaSet, authSource).
    opcoes: list[tuple[str, str]] = []
    for linha in _dig("TXT", host):
        for par in parse_qsl(linha.strip().strip('"')):
            opcoes.append(par)

    # As opções da URI original vencem as do TXT; `tls` é implícito no SRV e
    # precisa ser explicitado na forma padrão, senão o driver conecta em claro.
    chaves_uri = {chave for chave, _ in parse_qsl(query)}
    finais = [(c, v) for c, v in opcoes if c not in chaves_uri] + parse_qsl(query)
    if not any(c.lower() in ("tls", "ssl") for c, _ in finais):
        finais.append(("tls", "true"))

    prefixo = f"{credencial}@" if credencial else ""
    return f"mongodb://{prefixo}{','.join(sorted(alvos))}/{caminho}?{urlencode(finais)}"


def main() -> int:
    original = sys.stdin.read().strip()
    if not original:
        print("expand_srv: URI vazia no stdin", file=sys.stderr)
        return 1
    try:
        expandida = expandir(original)
    except Exception as exc:  # noqa: BLE001 - qualquer falha degrada para a URI original
        print(f"   (SRV não expandido: {type(exc).__name__}: {exc}; usando a URI original)", file=sys.stderr)
        expandida = original
    sys.stdout.write(expandida)
    return 0


if __name__ == "__main__":
    sys.exit(main())
