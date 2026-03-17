from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, Field


class LookupRequest(BaseModel):
    query: str
    page: int = 1
    size: int = 20
    cursor: str | None = None
    highlight: bool | None = None
    include_fields: list[str] = Field(default_factory=list)


class SearchRequest(BaseModel):
    query: str
    page: int = 1
    size: int = 20
    cursor: str | None = None
    highlight: bool | None = None
    include_fields: list[str] = Field(default_factory=list)


class SearchHit(BaseModel):
    pmcid: str | None = None
    title: str | None = None
    abstract_text: str | None = None
    publication_date: date | None = None
    score: float | None = None
    highlight: dict[str, Any] | None = None
    source: dict[str, Any] | None = None


class SearchResponse(BaseModel):
    total: int
    page: int
    size: int
    hits: list[SearchHit]
    next_cursor: str | None = None
