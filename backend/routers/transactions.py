from fastapi import APIRouter
from database import db, client
from datetime import datetime
import uuid

router = APIRouter(prefix="/transactions", tags=["Transactions"])

DEMO_COLLECTIONS = ["pedidos_demo", "pagamentos_demo", "estoque_demo"]


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
      3. Atualizar estoque
      4. Registrar pagamento

    Se simular_falha=True, força erro no step 3 — todos os writes
    anteriores são revertidos (ROLLBACK) e nenhuma coleção fica
    com dados parciais.
    """
    steps = []
    pedido_id = str(uuid.uuid4())

    with client.start_session() as session:
        try:
            session.start_transaction()

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
                "created_at": datetime.now(),
            }
            pedido_result = db["pedidos_demo"].insert_one(pedido, session=session)
            steps.append({
                "step": 2, "ok": True,
                "descricao": "Pedido inserido em pedidos_demo",
                "detalhe": f"pedido_id: {pedido_id}",
            })

            # ── Step 3: Simular falha opcional ───────────────────────────
            if simular_falha:
                raise Exception("Timeout no gateway de pagamento (simulado)")

            # ── Step 3: Atualizar estoque ─────────────────────────────────
            db["estoque_demo"].update_one(
                {"produto_id": produto["produto_id"]},
                {"$inc": {"reservado": 1}, "$set": {"updated_at": datetime.now()}},
                upsert=True,
                session=session,
            )
            steps.append({
                "step": 3, "ok": True,
                "descricao": "Estoque reservado em estoque_demo",
                "detalhe": f"produto_id: {produto['produto_id'][:8]}… +1 reservado",
            })

            # ── Step 4: Registrar pagamento ───────────────────────────────
            pagamento_id = str(uuid.uuid4())
            pagamento = {
                "pagamento_id": pagamento_id,
                "pedido_id":    pedido_id,
                "valor":        produto["preco"],
                "metodo":       "pix",
                "status":       "aprovado",
                "created_at":   datetime.now(),
            }
            db["pagamentos_demo"].insert_one(pagamento, session=session)
            steps.append({
                "step": 4, "ok": True,
                "descricao": "Pagamento registrado em pagamentos_demo",
                "detalhe": f"pagamento_id: {pagamento_id}",
            })

            # ── COMMIT ────────────────────────────────────────────────────
            session.commit_transaction()
            steps.append({
                "step": "COMMIT", "ok": True,
                "descricao": "Transação confirmada — 3 coleções escritas atomicamente",
                "detalhe": "pedidos_demo ✓  estoque_demo ✓  pagamentos_demo ✓",
            })

            return {
                "success":    True,
                "pedido_id":  pedido_id,
                "pagamento_id": pagamento_id,
                "produto":    produto["nome"],
                "valor":      produto["preco"],
                "steps":      steps,
            }

        except Exception as e:
            session.abort_transaction()
            steps.append({
                "step": "ROLLBACK", "ok": False,
                "descricao": f"Erro: {e}",
                "detalhe":   "Todos os writes revertidos — banco permanece consistente",
            })
            return {
                "success": False,
                "error":   str(e),
                "steps":   steps,
            }


@router.post("/reset")
def reset():
    """Remove os dados de demo das coleções transacionais."""
    for col in DEMO_COLLECTIONS:
        db[col].drop()
    return {"reset": True, "collections": DEMO_COLLECTIONS}
