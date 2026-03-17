from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import api_lookup, api_search
from app.config import get_settings
from app.dependencies import AppContainer, build_container

container: AppContainer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global container
    settings = get_settings()
    container = build_container(settings)
    container.saved_queries.ensure_schema()
    container.gateway.ensure_index()
    yield


app = FastAPI(title="PMC Retrieval", version="0.1.0", lifespan=lifespan)


def _get_container() -> AppContainer:
    if container is None:
        raise RuntimeError("Container has not been initialized")
    return container


api_lookup.set_container_provider(_get_container)
api_search.set_container_provider(_get_container)

app.include_router(api_lookup.router)
app.include_router(api_search.router)


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}
