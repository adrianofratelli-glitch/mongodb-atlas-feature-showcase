"""Configuração centralizada da PoV, sem dependências adicionais."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _csv_env(name: str, default: str) -> tuple[str, ...]:
    return tuple(value.strip() for value in os.getenv(name, default).split(",") if value.strip())


@dataclass(frozen=True)
class Settings:
    mongo_uri: str = os.getenv("MONGO_URI", "").strip()
    mongo_db: str = os.getenv("MONGO_DB", "POC").strip() or "POC"
    mongo_timeout_ms: int = _int_env("MONGO_TIMEOUT_MS", 8_000)
    atlas_public_key: str = os.getenv("ATLAS_PUBLIC_KEY", "").strip()
    atlas_private_key: str = os.getenv("ATLAS_PRIVATE_KEY", "").strip()
    atlas_project_id: str = os.getenv("ATLAS_PROJECT_ID", "").strip()
    atlas_cluster: str = os.getenv("ATLAS_CLUSTER", "").strip()
    demo_admin_token: str = os.getenv("DEMO_ADMIN_TOKEN", "").strip()
    allowed_origins: tuple[str, ...] = _csv_env(
        "ALLOWED_ORIGINS",
        "http://localhost:5174,http://127.0.0.1:5174",
    )
    max_request_bytes: int = _int_env("MAX_REQUEST_BYTES", 1_048_576)

    @property
    def atlas_configured(self) -> bool:
        return all(
            (self.atlas_public_key, self.atlas_private_key, self.atlas_project_id, self.atlas_cluster)
        )


settings = Settings()
