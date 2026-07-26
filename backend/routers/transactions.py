from fastapi import APIRouter
from database import db, client
from datetime import datetime, timezone
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
