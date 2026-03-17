from __future__ import annotations

import re
from typing import Any

from app.config import Settings
from app.models import SearchHit, SearchResponse
from search.pagination import CursorToken, decode_cursor, encode_cursor
from search.opensearch_client import OpenSearchGateway


PMCID_RE = re.compile(r"^PMC\d+$", re.IGNORECASE)
PMCID_NUMERIC_RE = re.compile(r"^\d+$")


class LookupService:
    def __init__(self, gateway: OpenSearchGateway, settings: Settings):
        self.gateway = gateway
        self.settings = settings

    def _highlight_body(self) -> dict[str, Any]:
        fragment_size = self.settings.search.highlight_fragment_size
        fragments = self.settings.search.highlight_number_of_fragments
        return {
            "fields": {
                "title": {"fragment_size": fragment_size, "number_of_fragments": fragments},
                "abstract_text": {"fragment_size": fragment_size, "number_of_fragments": fragments},
            }
        }

    def lookup(
        self,
        query: str,
        page: int,
        size: int,
        cursor: str | None,
        highlight: bool | None,
        include_fields: list[str],
    ) -> SearchResponse:
        size = min(max(size, 1), self.settings.search.max_page_size)
        offset = (max(page, 1) - 1) * size
        highlight_enabled = (
            self.settings.search.lookup_highlight_default if highlight is None else bool(highlight)
        )

        should = []
        q = query.strip()
        if PMCID_RE.match(q):
            normalized = q.upper()
            should.extend(
                [
                    {"term": {"pmcid": {"value": normalized, "boost": 100}}},
                    {
                        "term": {
                            "pmcid_numeric": {
                                "value": normalized.replace("PMC", ""),
                                "boost": 90,
                            }
                        }
                    },
                ]
            )
        elif PMCID_NUMERIC_RE.match(q):
            should.extend(
                [
                    {"term": {"pmcid_numeric": {"value": q, "boost": 95}}},
                    {"term": {"pmcid": {"value": f"PMC{q}", "boost": 90}}},
                ]
            )

        should.extend(
            [
                {"term": {"title.keyword": {"value": q, "boost": 80}}},
                {"term": {"title_normalized": {"value": q.lower(), "boost": 70}}},
                {"match_phrase": {"title": {"query": q, "boost": 60}}},
                {"match": {"title": {"query": q, "boost": 40}}},
            ]
        )

        query_body = {"bool": {"should": should, "minimum_should_match": 1}}

        if self.settings.search.pagination.enabled and (cursor is not None or page <= 1):
            return self._lookup_with_cursor(
                query=query_body,
                page=page,
                size=size,
                cursor=cursor,
                highlight_enabled=highlight_enabled,
                include_fields=include_fields,
            )

        body = {"from": offset, "size": size, "query": query_body}
        if highlight_enabled:
            body["highlight"] = self._highlight_body()
        body["_source"] = list({*DEFAULT_FIELDS, *include_fields}) if include_fields else DEFAULT_FIELDS

        raw = self.gateway.search(body)
        return _to_response(raw, page=page, size=size, next_cursor=None)

    def _lookup_with_cursor(
        self,
        *,
        query: dict[str, Any],
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
            "query": query,
            "pit": {"id": pit_id, "keep_alive": keep_alive},
            "sort": [{"_score": "desc"}, {"pmcid": "asc"}],
        }
        if search_after:
            body["search_after"] = search_after
        if highlight_enabled:
            body["highlight"] = self._highlight_body()
        body["_source"] = list({*DEFAULT_FIELDS, *include_fields}) if include_fields else DEFAULT_FIELDS

        raw = self.gateway.search(body)
        hits = raw.get("hits", {}).get("hits", [])
        next_cursor: str | None = None
        if len(hits) == size:
            last_sort = hits[-1].get("sort")
            if isinstance(last_sort, list) and last_sort:
                next_cursor = encode_cursor(CursorToken(pit_id=pit_id, search_after=last_sort, size=size))
        if next_cursor is None:
            self.gateway.close_point_in_time(pit_id)

        return _to_response(raw, page=page, size=size, next_cursor=next_cursor)


DEFAULT_FIELDS = ["pmcid", "title", "abstract_text", "publication_date"]


def _to_response(raw: dict, page: int, size: int, next_cursor: str | None) -> SearchResponse:
    total = raw.get("hits", {}).get("total", {}).get("value", 0)
    items: list[SearchHit] = []
    for hit in raw.get("hits", {}).get("hits", []):
        source = hit.get("_source", {})
        items.append(
            SearchHit(
                pmcid=source.get("pmcid"),
                title=source.get("title"),
                abstract_text=source.get("abstract_text"),
                publication_date=source.get("publication_date"),
                score=hit.get("_score"),
                highlight=hit.get("highlight"),
                source=source,
            )
        )
    return SearchResponse(total=total, page=page, size=size, hits=items, next_cursor=next_cursor)
