from fastapi import APIRouter
from database import db

router = APIRouter(prefix="/aggregations", tags=["Aggregations"])


@router.get("/lookup")
def lookup_produtos_avaliacoes(limit: int = 5):
    """`$lookup` com sub-pipeline — parte de avaliacoes para garantir join sempre populado."""
    pipeline = [
        # Agrupa por produto_id usando o índice produto_id_1
        {"$group": {
            "_id": "$produto_id",
            "total_reviews": {"$sum": 1},
            "avg_nota":      {"$avg": "$nota"},
            "top_reviews":   {"$push": {
                "usuario": "$usuario",
                "nota":    "$nota",
                "titulo":  "$titulo",
            }},
        }},
        {"$sort": {"total_reviews": -1}},
        {"$limit": limit},
        # Sub-pipeline: busca somente os campos necessários do produto
        {"$lookup": {
            "from":          "produtos",
            "localField":    "_id",
            "foreignField":  "produto_id",
            "as":            "produto",
            "pipeline": [
                {"$project": {"nome": 1, "categoria": 1, "preco": 1, "marca": 1, "_id": 0}},
            ],
        }},
        {"$unwind": "$produto"},
        {"$project": {
            "_id":           0,
            "produto_id":    "$_id",
            "nome":          "$produto.nome",
            "categoria":     "$produto.categoria",
            "preco":         "$produto.preco",
            "marca":         "$produto.marca",
            "total_reviews": 1,
            "avg_nota":      {"$round": ["$avg_nota", 2]},
            "top_reviews":   {"$slice": ["$top_reviews", 3]},
        }},
    ]
    return {"results": list(db["avaliacoes"].aggregate(pipeline))}


@router.get("/facet")
def facet_analytics():
    """`$facet` — múltiplas agregações em paralelo; $match inicial usa índice em_estoque_1."""
    pipeline = [
        {"$match": {"em_estoque": True}},
        {"$facet": {
            "por_categoria": [
                {"$group": {"_id": "$categoria", "count": {"$sum": 1}, "avg_preco": {"$avg": "$preco"}}},
                {"$sort": {"count": -1}}, {"$limit": 6},
            ],
            "por_faixa_preco": [
                {"$bucket": {
                    "groupBy":    "$preco",
                    "boundaries": [0, 100, 500, 1000, 5000, 999999],
                    "default":    "Outro",
                    "output":     {"count": {"$sum": 1}, "avg": {"$avg": "$preco"}},
                }},
            ],
            "por_avaliacao": [
                {"$bucket": {
                    "groupBy":    "$avaliacao_media",
                    "boundaries": [0, 2, 3, 4, 5],
                    "default":    "Sem avaliação",
                    "output":     {"count": {"$sum": 1}},
                }},
            ],
            "top_marcas": [
                {"$group": {"_id": "$marca", "count": {"$sum": 1}, "avg_preco": {"$avg": "$preco"}}},
                {"$sort": {"count": -1}}, {"$limit": 5},
            ],
        }},
    ]
    result = list(db["produtos"].aggregate(pipeline))
    return {"data": result[0] if result else {}}


@router.get("/union-with")
def union_with():
    """`$unionWith` — combina reviews recentes (avaliacoes) com produtos destaque (produtos).
    Ambos os lados usam sort + limit sobre índices, sem aggregation cara."""
    pipeline = [
        # Lado 1: reviews recentes com nota alta — usa índice data_-1 + cat_nota_idx
        {"$sort":  {"data": -1}},
        {"$match": {"nota": {"$gte": 4}}},
        {"$limit": 8},
        {"$project": {
            "_id":        0,
            "source":     {"$literal": "avaliacoes"},
            "tipo":       {"$literal": "Review recente"},
            "referencia": "$produto_id",
            "descricao":  "$titulo",
            "valor":      "$nota",
            "usuario":    "$usuario",
            "categoria":  "$categoria",
        }},
        # Lado 2: produtos em destaque — usa total_av_idx + avaliacao_media_-1
        {"$unionWith": {
            "coll": "produtos",
            "pipeline": [
                {"$match": {"avaliacao_media": {"$gte": 4.5}, "em_estoque": True}},
                {"$sort":  {"total_avaliacoes": -1}},
                {"$limit": 8},
                {"$project": {
                    "_id":       0,
                    "source":    {"$literal": "produtos"},
                    "tipo":      {"$literal": "Produto destaque"},
                    "referencia":"$produto_id",
                    "descricao": "$nome",
                    "valor":     "$avaliacao_media",
                    "usuario":   {"$literal": "—"},
                    "categoria": "$categoria",
                }},
            ],
        }},
        {"$sort": {"source": 1, "valor": -1}},
    ]
    return {"results": list(db["avaliacoes"].aggregate(pipeline))}


@router.get("/group-advanced")
def group_advanced():
    """`$group` + `$addFields` — métricas por categoria; $match inicial usa em_estoque_1."""
    pipeline = [
        {"$match": {"em_estoque": True}},
        {"$group": {
            "_id":           "$categoria",
            "total_produtos":{"$sum": 1},
            "preco_medio":   {"$avg": "$preco"},
            "preco_max":     {"$max": "$preco"},
            "preco_min":     {"$min": "$preco"},
            "em_estoque":    {"$sum": {"$cond": ["$em_estoque", 1, 0]}},
            "avaliacao_media":{"$avg": "$avaliacao_media"},
        }},
        {"$addFields": {
            "pct_estoque": {"$multiply": [{"$divide": ["$em_estoque", "$total_produtos"]}, 100]}
        }},
        {"$sort": {"total_produtos": -1}},
        {"$limit": 8},
    ]
    result = list(db["produtos"].aggregate(pipeline))
    for r in result:
        r["preco_medio"]     = round(r.get("preco_medio") or 0, 2)
        r["avaliacao_media"] = round(r.get("avaliacao_media") or 0, 2)
        r["pct_estoque"]     = round(r.get("pct_estoque") or 0, 1)
    return {"results": result}


@router.get("/window-functions")
def window_functions():
    """`$setWindowFields` — rank, acumulado e média móvel.
    Working set reduzido: match + sort + limit ANTES das janelas."""
    pipeline = [
        # Limita o working set a 100 docs via índice (categoria + total_avaliacoes)
        {"$match": {"categoria": "Eletrônicos", "em_estoque": True}},
        {"$sort":  {"total_avaliacoes": -1}},
        {"$limit": 100},
        # Janelas sobre o conjunto pequeno — particionado por marca
        {"$setWindowFields": {
            "partitionBy": "$marca",
            "sortBy":      {"total_avaliacoes": -1},
            "output": {
                "rank_marca": {"$rank": {}},
                "acumulado_avaliacoes": {
                    "$sum": "$total_avaliacoes",
                    "window": {"documents": ["unbounded", "current"]},
                },
                "media_movel_preco": {
                    "$avg": "$preco",
                    "window": {"documents": [-2, 2]},
                },
            },
        }},
        {"$sort": {"marca": 1, "rank_marca": 1}},
        {"$limit": 20},
        {"$project": {
            "_id": 0, "nome": 1, "marca": 1, "categoria": 1,
            "preco": 1, "avaliacao_media": 1, "total_avaliacoes": 1,
            "rank_marca": 1,
            "acumulado_avaliacoes": 1,
            "media_movel_preco": {"$round": ["$media_movel_preco", 2]},
        }},
    ]
    result = list(db["produtos"].aggregate(pipeline))
    for r in result:
        r["preco"]             = round(r.get("preco") or 0, 2)
        r["media_movel_preco"] = round(r.get("media_movel_preco") or 0, 2)
        r["avaliacao_media"]   = round(r.get("avaliacao_media") or 0, 2)
    return {"results": result}


@router.get("/bucket-auto")
def bucket_auto():
    """`$bucketAuto` — faixas automáticas; $match inicial usa em_estoque_1."""
    pipeline = [
        {"$match": {"em_estoque": True}},
        {"$bucketAuto": {
            "groupBy": "$preco",
            "buckets": 6,
            "output": {
                "count":         {"$sum": 1},
                "avg_preco":     {"$avg": "$preco"},
                "avg_avaliacao": {"$avg": "$avaliacao_media"},
            },
        }},
    ]
    result = list(db["produtos"].aggregate(pipeline))
    for r in result:
        r["avg_preco"]     = round(r.get("avg_preco") or 0, 2)
        r["avg_avaliacao"] = round(r.get("avg_avaliacao") or 0, 2)
        if "_id" in r:
            lo = r["_id"].get("min", 0)
            hi = r["_id"].get("max", 0)
            r["faixa"] = f"R$ {lo:.0f} – R$ {hi:.0f}"
    return {"results": result}
