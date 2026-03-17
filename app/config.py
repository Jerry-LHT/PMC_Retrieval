from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class PathsConfig(BaseModel):
    raw_json_dir: str = "/data/papers/raw"


class PostgresConfig(BaseModel):
    dsn: str


class OpenSearchConfig(BaseModel):
    hosts: list[str]
    index_name: str = "articles_core_v2"
    index_alias: str = "articles_current"
    verify_certs: bool = False


class SearchWeights(BaseModel):
    title: float = 5
    mesh_terms: float = 4
    keywords: float = 3
    abstract_text: float = 2
    full_text_clean: float = 0


class SearchPaginationConfig(BaseModel):
    enabled: bool = True
    pit_keep_alive: str = "2m"


class SearchConfig(BaseModel):
    default_page_size: int = 20
    max_page_size: int = 100
    highlight: bool = True
    highlight_fragment_size: int = 120
    highlight_number_of_fragments: int = 2
    lookup_highlight_default: bool = False
    pagination: SearchPaginationConfig = Field(default_factory=SearchPaginationConfig)
    weights: SearchWeights = Field(default_factory=SearchWeights)


class IngestConfig(BaseModel):
    parse_workers: int = 4
    failed_log_path: str = "logs/ingest_failed.ndjson"
    progress_every: int = 5000
    include_full_text: bool = False
    estimate_total_files: bool = True


class Settings(BaseModel):
    paths: PathsConfig
    postgres: PostgresConfig
    opensearch: OpenSearchConfig
    search: SearchConfig
    ingest: IngestConfig = Field(default_factory=IngestConfig)
    os_username: str | None = None
    os_password: str | None = None


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data


def _parse_hosts(value: str) -> list[str]:
    return [host.strip() for host in value.split(",") if host.strip()]


@lru_cache
def get_settings(config_path: str = "config/app.yaml") -> Settings:
    data = _load_yaml(Path(config_path))
    settings = Settings(
        **data,
        os_username=os.getenv("OS_USERNAME") or None,
        os_password=os.getenv("OS_PASSWORD") or None,
    )

    pg_dsn = os.getenv("PG_DSN")
    if pg_dsn:
        settings.postgres.dsn = pg_dsn

    opensearch_hosts = os.getenv("OPENSEARCH_HOSTS")
    if opensearch_hosts:
        parsed = _parse_hosts(opensearch_hosts)
        if parsed:
            settings.opensearch.hosts = parsed

    return settings
