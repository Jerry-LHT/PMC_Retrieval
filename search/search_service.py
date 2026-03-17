from __future__ import annotations

from typing import Any

from app.config import Settings
from app.models import SearchHit, SearchResponse
from parser.pubmed_parser import PubMedParser
from parser.translator import OpenSearchTranslator, Weights
from search.pagination import CursorToken, decode_cursor, encode_cursor
from search.opensearch_client import OpenSearchGateway


class SearchService:
    def __init__(self, gateway: OpenSearchGateway, settings: Settings):
        self.gateway = gateway
        self.settings = settings
        self.parser = PubMedParser()
        self.translator = OpenSearchTranslator(
            weights=Weights(
                title=settings.search.weights.title,
                mesh_terms=settings.search.weights.mesh_terms,
                keywords=settings.search.weights.keywords,
                abstract_text=settings.search.weights.abstract_text,
                full_text_clean=settings.search.weights.full_text_clean,
            )
        )

    def _highlight_body(self) -> dict[str, Any]:
        fragment_size = self.settings.search.highlight_fragment_size
        fragments = self.settings.search.highlight_number_of_fragments
        return {
            "fields": {
                "title": {"fragment_size": fragment_size, "number_of_fragments": fragments},
                "abstract_text": {"fragment_size": fragment_size, "number_of_fragments": fragments},
            }
        }

    def search(
        self,
        query: str,
        page: int,
        size: int,
        cursor: str | None,
        highlight: bool | None,
        include_fields: list[str],
    ) -> SearchResponse:
        ast = self.parser.parse(query)
        translated = self.translator.translate(ast)
        size = min(max(size, 1), self.settings.search.max_page_size)
        offset = (max(page, 1) - 1) * size
        highlight_enabled = self.settings.search.highlight if highlight is None else bool(highlight)

        if self.settings.search.pagination.enabled and (cursor is not None or page <= 1):
            return self._search_with_cursor(
                translated=translated,
                page=page,
                size=size,
                cursor=cursor,
                highlight_enabled=highlight_enabled,
                include_fields=include_fields,
            )

        body = {"from": offset, "size": size, "query": translated}
        if highlight_enabled:
            body["highlight"] = self._highlight_body()
        body["_source"] = list({*DEFAULT_FIELDS, *include_fields}) if include_fields else DEFAULT_FIELDS

        raw = self.gateway.search(body)
        total = raw.get("hits", {}).get("total", {}).get("value", 0)
        hits: list[SearchHit] = []
        for item in raw.get("hits", {}).get("hits", []):
            source = item.get("_source", {})
            hits.append(
                SearchHit(
                    pmcid=source.get("pmcid"),
                    title=source.get("title"),
                    abstract_text=source.get("abstract_text"),
                    publication_date=source.get("publication_date"),
                    score=item.get("_score"),
                    highlight=item.get("highlight"),
                    source=source,
                )
            )

        return SearchResponse(total=total, page=page, size=size, hits=hits, next_cursor=None)

    def _search_with_cursor(
        self,
        *,
        translated: dict[str, Any],
        page: int,
        size: int,
        cursor: str | None,
        highlight_enabled: bool,
        include_fields: list[str],
    ) -> SearchResponse:
        keep_alive = self.settings.search.pagination.pit_keep_alive
        pit_id: str
        search_after: list[Any] | None = None
        if cursor:
            token = decode_cursor(cursor)
            pit_id = token.pit_id
            search_after = token.search_after
            size = token.size
        else:
            pit_id = self.gateway.create_point_in_time(keep_alive=keep_alive)

        body: dict[str, Any] = {
            "size": size,
            "query": translated,
            "pit": {"id": pit_id, "keep_alive": keep_alive},
            "sort": [{"_score": "desc"}, {"pmcid": "asc"}],
        }
        if search_after:
            body["search_after"] = search_after
        if highlight_enabled:
            body["highlight"] = self._highlight_body()
        body["_source"] = list({*DEFAULT_FIELDS, *include_fields}) if include_fields else DEFAULT_FIELDS

        raw = self.gateway.search(body)
        total = raw.get("hits", {}).get("total", {}).get("value", 0)
        items = raw.get("hits", {}).get("hits", [])
        hits: list[SearchHit] = []
        for item in items:
            source = item.get("_source", {})
            hits.append(
                SearchHit(
                    pmcid=source.get("pmcid"),
                    title=source.get("title"),
                    abstract_text=source.get("abstract_text"),
                    publication_date=source.get("publication_date"),
                    score=item.get("_score"),
                    highlight=item.get("highlight"),
                    source=source,
                )
            )

        next_cursor: str | None = None
        if len(items) == size:
            last_sort = items[-1].get("sort")
            if isinstance(last_sort, list) and last_sort:
                next_cursor = encode_cursor(CursorToken(pit_id=pit_id, search_after=last_sort, size=size))

        if next_cursor is None:
            self.gateway.close_point_in_time(pit_id)

        return SearchResponse(total=total, page=page, size=size, hits=hits, next_cursor=next_cursor)


DEFAULT_FIELDS = ["pmcid", "title", "abstract_text", "publication_date"]
