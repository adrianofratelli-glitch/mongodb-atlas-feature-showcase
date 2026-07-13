from pymongo import MongoClient

from settings import settings

# URI local somente evita que a importação masque uma configuração ausente.
# O endpoint de readiness continua indisponível até MONGO_URI ser configurada.
_uri = settings.mongo_uri or "mongodb://127.0.0.1:27017"
client = MongoClient(
    _uri,
    appname="mongodb-atlas-feature-showcase",
    serverSelectionTimeoutMS=settings.mongo_timeout_ms,
    connectTimeoutMS=settings.mongo_timeout_ms,
    socketTimeoutMS=max(settings.mongo_timeout_ms * 2, 10_000),
    connect=False,
)
db = client[settings.mongo_db]


def readiness() -> tuple[bool, str]:
    if not settings.mongo_uri:
        return False, "MONGO_URI não configurada"
    try:
        client.admin.command("ping")
        return True, "MongoDB conectado"
    except Exception as exc:  # mensagem curta; detalhes ficam apenas nos logs do backend
        return False, f"MongoDB indisponível: {type(exc).__name__}"
