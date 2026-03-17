from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

try:
    from opensearchpy import OpenSearch, helpers
    from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError
except ModuleNotFoundError:  # pragma: no cover - exercised only in minimal test envs.
    class OpenSearch:  # type: ignore[no-redef]
        def __init__(self, *_: Any, **__: Any) -> None:
            raise ModuleNotFoundError("opensearchpy is required for OpenSearch access")

    class _MissingHelpers:
        @staticmethod
        def parallel_bulk(*_: Any, **__: Any) -> Any:
            raise ModuleNotFoundError("opensearchpy is required for bulk ingest")

    helpers = _MissingHelpers()  # type: ignore[assignment]

    class OpenSearchConnectionError(Exception):
        pass

from app.config import Settings


@dataclass
class BulkItemResult:
    ok: bool
    doc_id: str | None
    status: int | None
    error_type: str | None = None
    error_reason: str | None = None


@dataclass
class BulkIngestResult:
    indexed_count: int
    failed_count: int


class OpenSearchGateway:
    def __init__(self, settings: Settings):
        auth = None
        if settings.os_username and settings.os_password:
            auth = (settings.os_username, settings.os_password)
        self.client = OpenSearch(
            hosts=settings.opensearch.hosts,
            http_auth=auth,
            verify_certs=settings.opensearch.verify_certs,
        )
        self.index_name = settings.opensearch.index_name
        self.index_alias = settings.opensearch.index_alias

    def _wait_until_ready(self, max_attempts: int = 30, delay_seconds: float = 1.0) -> None:
        last_error: Exception | None = None
        for _ in range(max_attempts):
            try:
                if self.client.ping():
                    return
            except OpenSearchConnectionError as exc:
                last_error = exc
            time.sleep(delay_seconds)
        if last_error is not None:
            raise last_error
        raise RuntimeError("OpenSearch is not ready after retrying")

    def wait_until_ready(self, max_attempts: int = 30, delay_seconds: float = 1.0) -> None:
        self._wait_until_ready(max_attempts=max_attempts, delay_seconds=delay_seconds)

    def ensure_index(self, mapping_path: str = "search/mapping.json", index_name: str | None = None) -> None:
        # OpenSearch container may still be booting when API starts.
        self._wait_until_ready()
        target_index = index_name or self.index_name
        self.create_index_if_missing(target_index, mapping_path)
        if not self.client.indices.exists_alias(name=self.index_alias):
            self.client.indices.put_alias(index=target_index, name=self.index_alias)

    def create_index_if_missing(self, index_name: str, mapping_path: str = "search/mapping.json") -> None:
        if not self.client.indices.exists(index=index_name):
            mapping = json.loads(Path(mapping_path).read_text(encoding="utf-8"))
            self.client.indices.create(index=index_name, body=mapping)

    def switch_alias_to_index(self, index_name: str) -> None:
        aliases = self.client.indices.get_alias(name=self.index_alias, ignore=[404])
        actions: list[dict[str, Any]] = []
        if isinstance(aliases, dict):
            for existing_index in aliases:
                if existing_index != index_name:
                    actions.append({"remove": {"index": existing_index, "alias": self.index_alias}})
        actions.append({"add": {"index": index_name, "alias": self.index_alias}})
        self.client.indices.update_aliases(body={"actions": actions})

    def reindex(
        self,
        *,
        source_index: str,
        target_index: str,
        wait_for_completion: bool = True,
        request_timeout: int = 3600,
    ) -> dict[str, Any]:
        return self.client.reindex(
            body={"source": {"index": source_index}, "dest": {"index": target_index}},
            wait_for_completion=wait_for_completion,
            request_timeout=request_timeout,
            refresh=True,
        )

    def create_point_in_time(self, keep_alive: str = "2m") -> str:
        response = self.client.transport.perform_request(
            method="POST",
            url=f"/{self.index_alias}/_search/point_in_time",
            body={"keep_alive": keep_alive},
        )
        pit_id = response.get("pit_id")
        if not isinstance(pit_id, str) or not pit_id:
            raise RuntimeError("failed to create point in time")
        return pit_id

    def close_point_in_time(self, pit_id: str) -> None:
        self.client.transport.perform_request(
            method="DELETE",
            url="/_search/point_in_time",
            body={"pit_id": pit_id},
        )

    def _bulk_actions(self, docs: Iterable[dict]) -> Iterable[dict]:
        for doc in docs:
            yield {
                "_op_type": "index",
                "_index": self.index_alias,
                "_id": doc["doc_id"],
                "_source": doc,
            }

    def get_index_write_settings(self) -> dict[str, Any]:
        response = self.client.indices.get_settings(index=self.index_name)
        index_settings = response.get(self.index_name, {}).get("settings", {}).get("index", {})
        return {
            "refresh_interval": index_settings.get("refresh_interval", "1s"),
            "number_of_replicas": str(index_settings.get("number_of_replicas", "1")),
        }

    def set_index_write_settings(self, *, refresh_interval: str, number_of_replicas: str | int) -> None:
        self.client.indices.put_settings(
            index=self.index_name,
            body={
                "index": {
                    "refresh_interval": refresh_interval,
                    "number_of_replicas": str(number_of_replicas),
                }
            },
        )

    def optimize_for_bulk_ingest(self) -> dict[str, Any]:
        previous = self.get_index_write_settings()
        self.set_index_write_settings(refresh_interval="-1", number_of_replicas=0)
        return previous

    def finalize_bulk_ingest(self, previous_settings: dict[str, Any]) -> None:
        self.set_index_write_settings(
            refresh_interval=str(previous_settings.get("refresh_interval", "1s")),
            number_of_replicas=str(previous_settings.get("number_of_replicas", "1")),
        )
        self.client.indices.refresh(index=self.index_alias)

    def bulk_upsert(
        self,
        docs: list[dict],
        *,
        chunk_size: int = 1000,
        max_chunk_bytes: int = 10 * 1024 * 1024,
        thread_count: int = 4,
        refresh: bool = False,
    ) -> int:
        result = self.bulk_upsert_iter(
            docs,
            chunk_size=chunk_size,
            max_chunk_bytes=max_chunk_bytes,
            thread_count=thread_count,
            refresh=refresh,
        )
        return result.indexed_count

    @staticmethod
    def _extract_bulk_meta(item: dict[str, Any]) -> dict[str, Any]:
        if not item:
            return {}
        if len(item) == 1:
            first_key = next(iter(item))
            first_value = item.get(first_key)
            if isinstance(first_value, dict):
                return first_value
        return item

    def bulk_upsert_iter(
        self,
        docs: Iterable[dict],
        *,
        chunk_size: int = 1000,
        max_chunk_bytes: int = 10 * 1024 * 1024,
        thread_count: int = 4,
        refresh: bool = False,
        on_item_result: Callable[[BulkItemResult], None] | None = None,
    ) -> BulkIngestResult:
        success_count = 0
        failed_count = 0
        for ok, _ in helpers.parallel_bulk(
            self.client,
            self._bulk_actions(docs),
            chunk_size=chunk_size,
            max_chunk_bytes=max_chunk_bytes,
            thread_count=max(1, thread_count),
            queue_size=max(4, thread_count * 2),
            refresh=refresh,
            raise_on_error=False,
            raise_on_exception=True,
        ):
            meta = self._extract_bulk_meta(_)
            doc_id = meta.get("_id")
            status = meta.get("status")
            error_type: str | None = None
            error_reason: str | None = None
            if isinstance(meta.get("error"), dict):
                error_type = str(meta["error"].get("type")) if meta["error"].get("type") is not None else None
                error_reason = (
                    str(meta["error"].get("reason")) if meta["error"].get("reason") is not None else None
                )

            if ok:
                success_count += 1
            else:
                failed_count += 1

            if on_item_result is not None:
                on_item_result(
                    BulkItemResult(
                        ok=bool(ok),
                        doc_id=str(doc_id) if doc_id is not None else None,
                        status=int(status) if isinstance(status, int) else None,
                        error_type=error_type,
                        error_reason=error_reason,
                    )
                )

        return BulkIngestResult(indexed_count=int(success_count), failed_count=int(failed_count))

    def search(self, body: dict) -> dict:
        return self.client.search(index=self.index_alias, body=body)
