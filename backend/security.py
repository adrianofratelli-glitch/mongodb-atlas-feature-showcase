"""Guardas leves para uma PoV local que possui endpoints destrutivos."""

from __future__ import annotations

import hmac
import ipaddress

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from settings import settings


SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


def _is_loopback(host: str | None) -> bool:
    if not host:
        return False
    if host in {"localhost", "testclient"}:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class MutationGuardMiddleware(BaseHTTPMiddleware):
    """Bloqueia mutações remotas e CSRF; token opcional endurece ambientes compartilhados."""

    async def dispatch(self, request, call_next):
        if request.method in SAFE_METHODS:
            return await call_next(request)

        origin = request.headers.get("origin")
        if origin and origin not in settings.allowed_origins:
            return JSONResponse(
                status_code=403,
                content={"detail": "Origem não autorizada para operações de demonstração."},
            )

        supplied_token = request.headers.get("x-demo-token", "")
        if settings.demo_admin_token:
            if not hmac.compare_digest(supplied_token, settings.demo_admin_token):
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Token de administração da demonstração ausente ou inválido."},
                    headers={"WWW-Authenticate": "DemoToken"},
                )
        elif not _is_loopback(request.client.host if request.client else None):
            return JSONResponse(
                status_code=403,
                content={
                    "detail": (
                        "Mutações remotas estão desabilitadas. Configure DEMO_ADMIN_TOKEN "
                        "para executar a PoV em uma rede compartilhada."
                    )
                },
            )

        return await call_next(request)


class ApiHardeningMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                too_large = int(content_length) > settings.max_request_bytes
            except ValueError:
                too_large = True
            if too_large:
                return JSONResponse(status_code=413, content={"detail": "Corpo da requisição muito grande."})

        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Cache-Control"] = "no-store"
        return response
