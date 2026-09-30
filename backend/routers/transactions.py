from concurrent.futures import ThreadPoolExecutor
from statistics import median

from fastapi import APIRouter, Query
from pymongo import WriteConcern
from pymongo.errors import PyMongoError
from database import db, client
from datetime import datetime, timezone
import time
import uuid
import logging

router = APIRouter(prefix="/transactions", tags=["Transactions"])
logger = logging.getLogger("showcase.transactions")

DEMO_COLLECTIONS = ["pedidos_demo", "pagamentos_demo", "estoque_demo"]


class SimulatedPaymentError(RuntimeError):
    """Falha deliberada do passo de pagamento, distinta de erros reais."""


@router.get("/status")
def status():
    """Retorna quantos documentos existem nas coleções de demo."""
    result = {}
    for col in DEMO_COLLECTIONS:
        result[col] = db[col].count_documents({})
    return result


@router.post("/executar")
def executar_transacao(simular_falha: bool = False):
    """
    Simula uma compra em 4 steps dentro de uma única transação ACID:
      1. Ler produto disponível
      2. Inserir pedido
      3. Reservar estoque
      4. Registrar pagamento

    Usa a callback API `session.with_transaction()` — o padrão recomendado:
    o driver faz retry automático de TransientTransactionError e de
    UnknownTransactionCommitResult, e commita ao fim do callback.

    Se simular_falha=True, força erro no step 4 (gateway de pagamento) —
    os writes dos steps 2 e 3 são revertidos (ROLLBACK) e nenhuma coleção
    fica com dados parciais.
    """
    steps: list = []
    pedido_id = str(uuid.uuid4())

    def _compra(session):
        # O driver pode re-executar o callback em erros transientes —
        # zera os steps para a timeline não duplicar.
        steps.clear()

        # ── Step 1: Buscar produto disponível ─────────────────────────
        produto = db["produtos"].find_one(
            {"em_estoque": True, "categoria": "Eletrônicos"},
            {"nome": 1, "produto_id": 1, "preco": 1, "categoria": 1},
            session=session,
        )
        if not produto:
            raise Exception("Nenhum produto em estoque encontrado.")

        steps.append({
            "step": 1, "ok": True,
            "descricao": "Produto localizado no estoque",
            "detalhe": f"{produto['nome']} — R$ {produto['preco']:.2f}",
        })

        # ── Step 2: Inserir pedido ────────────────────────────────────
        pedido = {
            "pedido_id":  pedido_id,
            "produto_id": produto["produto_id"],
            "nome":       produto["nome"],
            "preco":      produto["preco"],
            "usuario":    "usuario_demo",
            "status":     "aguardando_pagamento",
            "created_at": datetime.now(timezone.utc),
        }
        db["pedidos_demo"].insert_one(pedido, session=session)
        steps.append({
            "step": 2, "ok": True,
            "descricao": "Pedido inserido em pedidos_demo",
            "detalhe": f"pedido_id: {pedido_id}",
        })

        # ── Step 3: Reservar estoque ──────────────────────────────────
        db["estoque_demo"].update_one(
            {"produto_id": produto["produto_id"]},
            {"$inc": {"reservado": 1}, "$set": {"updated_at": datetime.now(timezone.utc)}},
            upsert=True,
            session=session,
        )
        steps.append({
            "step": 3, "ok": True,
            "descricao": "Estoque reservado em estoque_demo",
            "detalhe": f"produto_id: {produto['produto_id'][:8]}… +1 reservado",
        })

        # ── Step 4: Registrar pagamento ───────────────────────────────
        if simular_falha:
            raise SimulatedPaymentError("Timeout no gateway de pagamento (simulado)")

        pagamento_id = str(uuid.uuid4())
        pagamento = {
            "pagamento_id": pagamento_id,
            "pedido_id":    pedido_id,
            "valor":        produto["preco"],
            "metodo":       "pix",
            "status":       "aprovado",
            "created_at":   datetime.now(timezone.utc),
        }
        db["pagamentos_demo"].insert_one(pagamento, session=session)
        steps.append({
            "step": 4, "ok": True,
            "descricao": "Pagamento registrado em pagamentos_demo",
            "detalhe": f"pagamento_id: {pagamento_id}",
        })
        return {"produto": produto["nome"], "valor": produto["preco"], "pagamento_id": pagamento_id}

    with client.start_session() as session:
        try:
            # with_transaction: commit automático + retry de erros transientes
            resultado = session.with_transaction(_compra)
            steps.append({
                "step": "COMMIT", "ok": True,
                "descricao": "Transação confirmada — 3 coleções escritas atomicamente",
                "detalhe": "pedidos_demo ✓  estoque_demo ✓  pagamentos_demo ✓",
            })
            return {
                "success":      True,
                "pedido_id":    pedido_id,
                "pagamento_id": resultado["pagamento_id"],
                "produto":      resultado["produto"],
                "valor":        resultado["valor"],
                "steps":        steps,
            }

        except Exception as e:
            # with_transaction já abortou a transação antes de propagar
            expected_failure = isinstance(e, SimulatedPaymentError)
            public_error = str(e) if expected_failure else "A transação não pôde ser concluída. Consulte o log do backend."
            if not expected_failure:
                logger.exception("Falha na demonstração de transação")
            steps.append({
                "step": "ROLLBACK", "ok": False,
                "descricao": f"Rollback executado: {public_error}",
                "detalhe":   "Pedido (step 2) e reserva de estoque (step 3) revertidos — banco permanece consistente",
            })
            return {
                "success": False,
                "error":   public_error,
                "steps":   steps,
            }


@router.post("/reset")
def reset():
    """Remove os dados de demo das coleções transacionais."""
    for col in DEMO_COLLECTIONS:
        db[col].drop()
    return {"reset": True, "collections": DEMO_COLLECTIONS}


# ── Medição ──────────────────────────────────────────────────────────────────
# Existe porque "o MongoDB reverte automaticamente" é uma frase que um time de
# dados já ouviu sobre o banco que ele opera hoje. A pergunta que vem depois é
# sempre a mesma — *quanto isso custa?* — e uma demo que só mostra o rollback
# funcionando não responde. Aqui os três números que respondem:
#
#   A. transação multi-documento sobre 3 coleções, com writeConcern majority
#   B. a MESMA intenção de negócio como escrita de um documento só
#   C. a mesma transação sob contenção, com N sessões disputando o documento
#
# B é o número desconfortável e é ele que precisa aparecer: no MongoDB a escrita
# de um documento já é atômica, e quando a transação multi-documento aparece em
# todo caminho de escrita, normalmente é um modelo relacional transplantado, não
# um requisito do domínio. Esconder isso não sobrevive à primeira pergunta da
# sala; medir e mostrar é o que dá crédito ao resto da demo.

BENCH_COL_PEDIDOS = "bench_pedidos"
BENCH_COL_ESTOQUE = "bench_estoque"
BENCH_COL_PAGAMENTOS = "bench_pagamentos"
BENCH_COLLECTIONS = [BENCH_COL_PEDIDOS, BENCH_COL_ESTOQUE, BENCH_COL_PAGAMENTOS, "bench_ordens"]

# Piso de amostra. Abaixo disso um p95 é anedota: um único GC do Python ou uma
# eleição de rede domina o percentil e o painel reporta ruído como medição.
MIN_AMOSTRA = 30


def _percentis(amostras_ms: list[float]) -> dict:
    """p50/p95/p99 sobre latências já ordenadas na chamada."""
    if not amostras_ms:
        return {"p50": None, "p95": None, "p99": None, "min": None, "max": None}
    ordenado = sorted(amostras_ms)

    def _p(q: float) -> float:
        # Índice pelo método do vizinho mais próximo: com 30 amostras a
        # interpolação linear inventa um valor entre duas medições reais.
        i = min(len(ordenado) - 1, max(0, round(q * (len(ordenado) - 1))))
        return round(ordenado[i], 2)

    return {
        "p50": round(median(ordenado), 2),
        "p95": _p(0.95),
        "p99": _p(0.99),
        "min": round(ordenado[0], 2),
        "max": round(ordenado[-1], 2),
    }


def _rtt_base_ms(n: int = 15) -> list[float]:
    """
    Ida e volta pura até o cluster, sem trabalho de banco: `ping` no admin.

    Sem esta linha de base o painel é uma armadilha. Uma transação com
    `majority` custa várias viagens de rede (início, operações, commit), então
    num notebook a 260 ms de RTT ela mede ~1 s — e a plateia conclui "transação
    no MongoDB leva um segundo", que é uma afirmação sobre o enlace do
    apresentador, não sobre o produto. O mesmo erro já foi cometido e corrigido
    no módulo 07; aqui ele nasce corrigido.
    """
    amostras = []
    for _ in range(n):
        t0 = time.perf_counter()
        client.admin.command("ping")
        amostras.append((time.perf_counter() - t0) * 1000)
    return amostras


def _limite_transacao() -> dict:
    """
    Lê `transactionLifetimeLimitSeconds` do servidor em vez de citar o padrão
    de cabeça. Em vários tiers do Atlas o `getParameter` é negado ao usuário da
    aplicação — nesse caso a página diz que não pôde ler, e não finge o número.
    """
    try:
        resposta = client.admin.command({"getParameter": 1, "transactionLifetimeLimitSeconds": 1})
        return {"segundos": resposta.get("transactionLifetimeLimitSeconds"), "fonte": "servidor"}
    except PyMongoError as erro:
        return {"segundos": None, "fonte": "indisponível", "motivo": type(erro).__name__}


def _transacao_multi(session, run_id: str, i: int) -> None:
    """Três coleções, uma transação. É o desenho relacional transplantado."""
    agora = datetime.now(timezone.utc)
    pedido_id = f"{run_id}-{i}"
    db[BENCH_COL_PEDIDOS].insert_one(
        {"pedido_id": pedido_id, "run_id": run_id, "valor": 100 + i, "ts": agora}, session=session)
    db[BENCH_COL_ESTOQUE].update_one(
        {"produto_id": f"{run_id}-produto", "run_id": run_id},
        {"$inc": {"reservado": 1}, "$set": {"ts": agora}},
        upsert=True, session=session)
    db[BENCH_COL_PAGAMENTOS].insert_one(
        {"pedido_id": pedido_id, "run_id": run_id, "valor": 100 + i, "ts": agora}, session=session)


@router.post("/benchmark")
def benchmark(
    amostras: int = Query(default=60, ge=MIN_AMOSTRA, le=500),
    concorrencia: int = Query(default=8, ge=2, le=16),
):
    """
    Mede o custo real da transação multi-documento contra o cluster ligado.

    Três medições, nesta ordem, sempre com `writeConcern: majority` — é o que
    torna o commit durável a uma eleição, e comparar contra um write concern
    mais fraco seria comparar garantias diferentes.
    """
    run_id = uuid.uuid4().hex[:12]
    # A rede é medida ANTES de qualquer coisa: todo número abaixo é lido contra
    # ela, e um painel que reporta latência sem reportar o RTT que a produziu
    # atribui ao produto um limite que é do enlace.
    p_rede = _percentis(_rtt_base_ms())
    # majority explícito nas duas pontas: sem isso a comparação A×B mediria
    # durabilidades diferentes e o resultado não significaria nada.
    wc = WriteConcern("majority")
    inicio = time.time()

    # ── A. transação multi-documento ─────────────────────────────────────────
    lat_multi: list[float] = []
    tentativas_total = 0
    with client.start_session() as session:
        for i in range(amostras):
            tentativas = {"n": 0}

            def _callback(s, _i=i, _t=tentativas):
                # O driver re-executa o callback em erro transiente; contar as
                # entradas aqui é a única forma honesta de saber quantos retries
                # aconteceram — `with_transaction` não os expõe.
                _t["n"] += 1
                _transacao_multi(s, run_id, _i)

            t0 = time.perf_counter()
            session.with_transaction(_callback, write_concern=wc)
            lat_multi.append((time.perf_counter() - t0) * 1000)
            tentativas_total += tentativas["n"]

    # ── B. a mesma intenção como um documento só ─────────────────────────────
    ordens = db.get_collection("bench_ordens", write_concern=wc)
    lat_single: list[float] = []
    for i in range(amostras):
        agora = datetime.now(timezone.utc)
        doc = {
            "pedido_id": f"{run_id}-s{i}", "run_id": run_id, "valor": 100 + i,
            "pagamento": {"valor": 100 + i, "metodo": "pix", "status": "aprovado"},
            "reserva": {"produto_id": f"{run_id}-produto", "quantidade": 1},
            "ts": agora,
        }
        t0 = time.perf_counter()
        ordens.insert_one(doc)
        lat_single.append((time.perf_counter() - t0) * 1000)

    # ── C. contenção: N sessões disputando o MESMO documento de estoque ──────
    por_worker = max(5, amostras // concorrencia)

    def _worker(w: int) -> tuple[list[float], int, int]:
        lats: list[float] = []
        tentativas_w = 0
        abortos = 0
        with client.start_session() as s:
            for i in range(por_worker):
                tentativas = {"n": 0}

                def _cb(sess, _w=w, _i=i, _t=tentativas):
                    _t["n"] += 1
                    # Todos os workers batem na MESMA chave: é isso que produz
                    # conflito de escrita, e não a quantidade de threads.
                    db[BENCH_COL_ESTOQUE].update_one(
                        {"produto_id": f"{run_id}-quente", "run_id": run_id},
                        {"$inc": {"reservado": 1}},
                        upsert=True, session=sess)
                    db[BENCH_COL_PEDIDOS].insert_one(
                        {"pedido_id": f"{run_id}-c{_w}-{_i}", "run_id": run_id, "ts": datetime.now(timezone.utc)},
                        session=sess)

                t0 = time.perf_counter()
                try:
                    s.with_transaction(_cb, write_concern=wc)
                    lats.append((time.perf_counter() - t0) * 1000)
                except PyMongoError:
                    # Aborto que sobreviveu ao retry do driver. Conta, e não
                    # entra na latência: misturar os dois esconde os dois.
                    abortos += 1
                tentativas_w += tentativas["n"]
        return lats, tentativas_w, abortos

    lat_conc: list[float] = []
    tentativas_conc = 0
    abortos_conc = 0
    t_conc = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concorrencia) as pool:
        for lats, tent, ab in pool.map(_worker, range(concorrencia)):
            lat_conc.extend(lats)
            tentativas_conc += tent
            abortos_conc += ab
    dur_conc = time.perf_counter() - t_conc

    # ── limpeza: o benchmark não deixa resíduo no cluster ────────────────────
    for col in BENCH_COLLECTIONS:
        db[col].delete_many({"run_id": run_id})

    p_multi = _percentis(lat_multi)
    p_single = _percentis(lat_single)
    p_conc = _percentis(lat_conc)
    custo = (round(p_multi["p50"] / p_single["p50"], 1)
             if p_multi["p50"] and p_single["p50"] else None)

    # Quantas idas e voltas a transação gastou. É o número que transforma
    # "1 segundo" em algo defensável: com `majority` o commit são várias
    # viagens, então a latência escala com o RTT, não com o custo do motor. Num
    # enlace de 7 ms a mesma transação mede dezenas de milissegundos — e é o
    # mesmo número de viagens.
    viagens = (round(p_multi["p50"] / p_rede["p50"], 1)
               if p_multi["p50"] and p_rede["p50"] else None)

    # Veredito explícito, no lugar de deixar a plateia inferir do número cru.
    if p_rede["p50"] is None or p_single["p50"] is None:
        veredito = "indeterminado"
    elif p_rede["p50"] >= 40:
        # Acima disso o enlace domina qualquer coisa que o cluster faça e o
        # painel não deve ser lido como medição do produto. 40 ms é o mesmo
        # limiar que o módulo 07 usa para atribuir o teto à rede.
        veredito = "limitado_pela_rede"
    elif p_rede["p50"] >= 0.5 * p_single["p50"]:
        veredito = "rede_relevante"
    else:
        veredito = "medicao_valida"

    return {
        "run_id": run_id,
        "amostras": amostras,
        "write_concern": "majority",
        "rede": {**p_rede, "amostras": 15, "metodo": "ping no admin, sem trabalho de banco"},
        "veredito": veredito,
        "viagens_de_rede_estimadas": viagens,
        "duracao_total_s": round(time.time() - inicio, 1),
        "limite_transacao": _limite_transacao(),
        "multi_documento": {
            **p_multi,
            "colecoes": 3,
            "retries": tentativas_total - amostras,
            "tps": round(amostras / (sum(lat_multi) / 1000), 1) if lat_multi else None,
        },
        "documento_unico": {
            **p_single,
            "colecoes": 1,
            "tps": round(amostras / (sum(lat_single) / 1000), 1) if lat_single else None,
        },
        "custo_da_transacao": custo,
        "contencao": {
            **p_conc,
            "sessoes": concorrencia,
            "operacoes": len(lat_conc),
            # Retries por operação é o número que descreve contenção. Ele sobe
            # com a disputa pela chave; a latência p50 quase não se move, e
            # olhar só para ela faria a contenção passar despercebida.
            "retries": max(0, tentativas_conc - len(lat_conc) - abortos_conc),
            "abortos": abortos_conc,
            "tps": round(len(lat_conc) / dur_conc, 1) if dur_conc > 0 else None,
        },
    }
