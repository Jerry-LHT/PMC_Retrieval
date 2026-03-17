from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import AppContainer
from app.models import LookupRequest, SearchResponse

router = APIRouter(prefix="/lookup", tags=["lookup"])

_container_provider: Callable[[], AppContainer] | None = None


def set_container_provider(provider: Callable[[], AppContainer]) -> None:
    global _container_provider
    _container_provider = provider


def get_container() -> AppContainer:
    if _container_provider is None:
        raise RuntimeError("Container dependency is not configured")
    return _container_provider()


@router.post("", response_model=SearchResponse)
def lookup(payload: LookupRequest, container: AppContainer = Depends(get_container)) -> SearchResponse:
    try:
        return container.lookup_service.lookup(
            query=payload.query,
            page=payload.page,
            size=payload.size,
            cursor=payload.cursor,
            highlight=payload.highlight,
            include_fields=payload.include_fields,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
