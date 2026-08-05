"""FastAPI application entrypoint: `uvicorn analyzer.api.app:app`. A
separate process/container from `analyzer.main` (the reassembly/
detection/suppression loop) — see docs/DECISIONS.md — sharing only this
package's ClickHouse client and config code, not runtime state.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from analyzer import chclient, config
from analyzer.api import routes
from analyzer.logging_setup import configure


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure()
    cfg = config.load()
    client = chclient.connect(cfg)
    app.dependency_overrides[routes.get_client] = lambda: client
    app.dependency_overrides[routes.get_config] = lambda: cfg
    try:
        yield
    finally:
        client.close()


app = FastAPI(title="Distributed Tracing Platform — Query API", lifespan=lifespan)

# Wildcard origin, GET-only: this API is read-only, unauthenticated, and
# serves no cookies or credentials — there's nothing a permissive CORS
# policy exposes here that an unauthenticated GET from a browser doesn't
# already have. Revisit if auth is ever added. See docs/DECISIONS.md.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(routes.router)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
