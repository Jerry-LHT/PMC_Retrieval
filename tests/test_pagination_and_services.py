from __future__ import annotations

from collections import deque
from pathlib import Path

from app.config import Settings
from search.lookup_service import LookupService
from search.pagination import CursorToken, decode_cursor, encode_cursor
from search.search_service import SearchService


class FakeGateway:
    def __init__(self, responses: list[dict]):
        self.responses = deque(responses)
        self.bodies: list[dict] = []
        self.created_pits: list[str] = []
        self.closed_pits: list[str] = []

    def create_point_in_time(self, keep_alive: str = "2m") -> str:
        self.created_pits.append(keep_alive)
        return "pit-1"

    def close_point_in_time(self, pit_id: str) -> None:
        self.closed_pits.append(pit_id)

    def search(self, body: dict) -> dict:
        self.bodies.append(body)
        return self.responses.popleft()


def _build_settings(*, pagination_enabled: bool = True, lookup_highlight_default: bool = False) -> Settings:
    return Settings(
        paths={"raw_json_dir": str(Path("/tmp"))},
        postgres={"dsn": "postgresql://pmc:pmc@localhost:5445/pmc_retrieval"},
        opensearch={"hosts": ["http://localhost:9200"]},
        search={
            "max_page_size": 100,
            "highlight": True,
            "lookup_highlight_default": lookup_highlight_default,
            "pagination": {"enabled": pagination_enabled, "pit_keep_alive": "2m"},
        },
    )


def test_cursor_encode_decode_roundtrip() -> None:
    cursor = encode_cursor(CursorToken(pit_id="pit-1", search_after=[1.0, "PMC2"], size=20))
    decoded = decode_cursor(cursor)
    assert decoded.pit_id == "pit-1"
    assert decoded.search_after == [1.0, "PMC2"]
    assert decoded.size == 20


def test_search_service_cursor_pagination() -> None:
    first_page = {
        "hits": {
            "total": {"value": 3},
            "hits": [
                {
                    "_source": {
                        "pmcid": "PMC1",
                        "title": "t1",
                        "abstract_text": "a1",
                        "publication_date": "2024-01-01",
                    },
                    "_score": 2.0,
                    "sort": [2.0, "PMC1"],
                },
                {
                    "_source": {
                        "pmcid": "PMC2",
                        "title": "t2",
                        "abstract_text": "a2",
                        "publication_date": "2024-01-02",
                    },
                    "_score": 1.0,
                    "sort": [1.0, "PMC2"],
                },
            ],
        }
    }
    second_page = {"hits": {"total": {"value": 3}, "hits": []}}
    gateway = FakeGateway([first_page, second_page])
    service = SearchService(gateway=gateway, settings=_build_settings())

    first = service.search(query="heart", page=1, size=2, cursor=None, highlight=None, include_fields=[])
    assert first.next_cursor is not None
    assert gateway.created_pits == ["2m"]
    assert gateway.closed_pits == []

    second = service.search(
        query="heart",
        page=1,
        size=2,
        cursor=first.next_cursor,
        highlight=None,
        include_fields=[],
    )
    assert second.next_cursor is None
    assert gateway.closed_pits == ["pit-1"]


def test_lookup_highlight_default_off_and_override() -> None:
    raw = {
        "hits": {
            "total": {"value": 1},
            "hits": [{"_source": {"pmcid": "PMC1", "title": "A"}, "_score": 1.0}],
        }
    }
    gateway = FakeGateway([raw, raw])
    service = LookupService(
        gateway=gateway,
        settings=_build_settings(pagination_enabled=False, lookup_highlight_default=False),
    )

    service.lookup(query="PMC1", page=1, size=10, cursor=None, highlight=None, include_fields=[])
    assert "highlight" not in gateway.bodies[0]

    service.lookup(query="PMC1", page=1, size=10, cursor=None, highlight=True, include_fields=[])
    assert "highlight" in gateway.bodies[1]
