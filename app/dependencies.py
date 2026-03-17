from __future__ import annotations

from dataclasses import dataclass

from app.config import Settings
from search.lookup_service import LookupService
from search.opensearch_client import OpenSearchGateway
from search.search_service import SearchService
from storage.saved_queries import SavedQueryRepository


@dataclass
class AppContainer:
    settings: Settings
    gateway: OpenSearchGateway
    lookup_service: LookupService
    search_service: SearchService
    saved_queries: SavedQueryRepository


def build_container(settings: Settings) -> AppContainer:
    gateway = OpenSearchGateway(settings)
    lookup_service = LookupService(gateway=gateway, settings=settings)
    search_service = SearchService(gateway=gateway, settings=settings)
    saved_queries = SavedQueryRepository(settings.postgres.dsn)
    return AppContainer(
        settings=settings,
        gateway=gateway,
        lookup_service=lookup_service,
        search_service=search_service,
        saved_queries=saved_queries,
    )
