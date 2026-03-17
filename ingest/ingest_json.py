from __future__ import annotations

import argparse
import json
import os
import time
from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

from app.config import get_settings
from ingest.document_builder import MeshExpander, build_document
from search.opensearch_client import BulkItemResult


@dataclass
class ParseResult:
    source_path: Path
    doc: dict | None
    error_type: str | None = None
    error_message: str | None = None


class FailureLogger:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = self.path.open("a", encoding="utf-8")

    def close(self) -> None:
        self._fp.close()

    def log(
        self,
        *,
        stage: str,
        source_json_path: str | None,
        doc_id: str | None,
        error_type: str | None,
        error_message: str | None,
        status: int | None = None,
    ) -> None:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "stage": stage,
            "source_json_path": source_json_path,
            "doc_id": doc_id,
            "error_type": error_type,
            "error_message": error_message,
            "status": status,
        }
        self._fp.write(json.dumps(payload, ensure_ascii=True) + "\n")
        self._fp.flush()


class IngestProgress:
    def __init__(self, *, progress_every: int, total_files: int | None) -> None:
        self.progress_every = max(1, progress_every)
        self.total_files = total_files
        self.started_at = time.monotonic()
        self.parse_inflight = 0
        self.parse_failed = 0
        self.parse_success = 0
        self.indexed_ok = 0
        self.indexed_failed = 0
        self.pending_index = 0
        self._last_emitted = 0

    def set_parse_inflight(self, count: int) -> None:
        self.parse_inflight = max(0, count)

    def on_parse(self, result: ParseResult) -> None:
        if result.doc is None:
            self.parse_failed += 1
            self._maybe_emit(force=False)
            return
        self.parse_success += 1
        self.pending_index += 1

    def on_index(self, ok: bool) -> None:
        self.pending_index = max(0, self.pending_index - 1)
        if ok:
            self.indexed_ok += 1
        else:
            self.indexed_failed += 1
        self._maybe_emit(force=False)

    def emit_final(self) -> None:
        self._emit()

    @property
    def completed(self) -> int:
        return self.parse_failed + self.indexed_ok + self.indexed_failed

    def _maybe_emit(self, *, force: bool) -> None:
        if force:
            self._emit()
            return
        if self.completed - self._last_emitted >= self.progress_every:
            self._emit()

    def _emit(self) -> None:
        elapsed = max(time.monotonic() - self.started_at, 1e-6)
        rate = self.completed / elapsed
        queue_depth = self.parse_inflight + self.pending_index

        total = self.total_files
        if total is not None and total > 0:
            percent = min(100.0, (self.completed / total) * 100.0)
            remaining = max(0, total - self.completed)
            eta = _format_duration(remaining / rate) if rate > 0 else "unknown"
            total_str = str(total)
            percent_str = f"{percent:.1f}%"
        else:
            eta = "unknown"
            total_str = "unknown"
            percent_str = "unknown"

        print(
            "progress "
            f"completed={self.completed}/{total_str} "
            f"pct={percent_str} "
            f"rate={rate:.1f}/s "
            f"indexed_ok={self.indexed_ok} "
            f"index_failed={self.indexed_failed} "
            f"parse_failed={self.parse_failed} "
            f"queue_depth={queue_depth} "
            f"eta={eta}"
        )
        self._last_emitted = self.completed


def _format_duration(seconds: float) -> str:
    seconds_i = max(0, int(seconds))
    h, rem = divmod(seconds_i, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def iter_json_files(raw_dir: Path) -> Iterator[Path]:
    for root, dirs, files in os.walk(raw_dir):
        dirs.sort()
        for name in sorted(files):
            if name.endswith(".json"):
                yield Path(root) / name


def count_json_files(raw_dir: Path) -> int:
    return sum(1 for _ in iter_json_files(raw_dir))


def _parse_json_path(path: Path, *, include_full_text: bool) -> ParseResult:
    expander = MeshExpander()
    try:
        with path.open("r", encoding="utf-8") as fp:
            payload = json.load(fp)
    except json.JSONDecodeError as exc:
        return ParseResult(
            source_path=path,
            doc=None,
            error_type="json_decode_error",
            error_message=str(exc),
        )
    except UnicodeDecodeError as exc:
        return ParseResult(
            source_path=path,
            doc=None,
            error_type="unicode_decode_error",
            error_message=str(exc),
        )
    except OSError as exc:
        return ParseResult(
            source_path=path,
            doc=None,
            error_type="io_error",
            error_message=str(exc),
        )

    if not isinstance(payload, dict):
        return ParseResult(
            source_path=path,
            doc=None,
            error_type="invalid_payload_type",
            error_message=f"payload type={type(payload).__name__}",
        )

    doc = build_document(
        payload,
        source_path=path,
        mesh_expander=expander,
        include_full_text=include_full_text,
    )
    if not doc:
        return ParseResult(
            source_path=path,
            doc=None,
            error_type="missing_doc_id",
            error_message="article_accession_id is empty",
        )

    return ParseResult(source_path=path, doc=doc)


def iter_paths_from_failed_log(failed_log: Path) -> Iterator[Path]:
    seen: set[str] = set()
    with failed_log.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            source = item.get("source_json_path")
            if not isinstance(source, str) or not source:
                continue
            if source in seen:
                continue
            seen.add(source)
            yield Path(source)


def count_failed_log_paths(failed_log: Path) -> int:
    return sum(1 for _ in iter_paths_from_failed_log(failed_log))


def iter_documents(raw_dir: Path, *, include_full_text: bool = True) -> Iterator[dict]:
    for path in iter_json_files(raw_dir):
        result = _parse_json_path(path, include_full_text=include_full_text)
        if result.doc:
            yield result.doc


def build_documents(raw_dir: Path) -> list[dict]:
    # Keep deterministic dedup behavior for tests and small local runs.
    docs_by_id: dict[str, dict] = {}
    for doc in iter_documents(raw_dir):
        docs_by_id[doc["doc_id"]] = doc
    return list(docs_by_id.values())


def _iter_parsed(
    paths: Iterable[Path],
    *,
    parse_workers: int,
    include_full_text: bool,
    progress: IngestProgress,
) -> Iterator[ParseResult]:
    workers = max(1, parse_workers)
    max_in_flight = max(8, workers * 4)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        running: set[Future[ParseResult]] = set()

        for path in paths:
            while len(running) >= max_in_flight:
                done, running = wait(running, return_when=FIRST_COMPLETED)
                progress.set_parse_inflight(len(running))
                for future in done:
                    yield future.result()
            running.add(executor.submit(_parse_json_path, path, include_full_text=include_full_text))
            progress.set_parse_inflight(len(running))

        while running:
            done, running = wait(running, return_when=FIRST_COMPLETED)
            progress.set_parse_inflight(len(running))
            for future in done:
                yield future.result()

    progress.set_parse_inflight(0)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest PMC JSON docs into OpenSearch")
    parser.add_argument("--config", default="config/app.yaml", help="Path to YAML config")
    parser.add_argument("--raw-dir", default=None, help="Override raw JSON directory")
    parser.add_argument("--chunk-size", type=_positive_int, default=1000, help="Bulk ingest chunk size")
    parser.add_argument(
        "--max-chunk-bytes",
        type=_positive_int,
        default=10 * 1024 * 1024,
        help="Bulk ingest max bytes per chunk",
    )
    parser.add_argument("--thread-count", type=_positive_int, default=4, help="Bulk ingest worker threads")
    parser.add_argument("--parse-workers", type=_positive_int, default=None, help="JSON parse worker threads")
    parser.add_argument(
        "--failed-log-path",
        default=None,
        help="Path to write parse/index failures as NDJSON",
    )
    parser.add_argument(
        "--retry-failed-from",
        default=None,
        help="Read previous failure NDJSON and retry those source_json_path items",
    )
    parser.add_argument(
        "--progress-every",
        type=_positive_int,
        default=None,
        help="Emit progress every N completed items",
    )
    parser.add_argument(
        "--include-full-text",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Include full_text_clean field in indexed document",
    )
    parser.add_argument(
        "--estimate-total-files",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Estimate total files for progress and ETA",
    )
    parser.add_argument(
        "--optimize-index-settings",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Temporarily disable refresh and replicas while ingesting",
    )
    parser.add_argument(
        "--refresh-after-ingest",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Refresh index after ingestion",
    )
    return parser.parse_args()


def _resolve_bool(cli_value: bool | None, config_value: bool) -> bool:
    if cli_value is None:
        return bool(config_value)
    return bool(cli_value)


def main() -> None:
    args = _parse_args()

    settings = get_settings(args.config)
    raw_dir = Path(args.raw_dir or settings.paths.raw_json_dir)

    from search.opensearch_client import OpenSearchGateway

    parse_workers = args.parse_workers or settings.ingest.parse_workers
    failed_log_path = Path(args.failed_log_path or settings.ingest.failed_log_path)
    progress_every = args.progress_every or settings.ingest.progress_every
    include_full_text = _resolve_bool(args.include_full_text, settings.ingest.include_full_text)
    estimate_total_files = _resolve_bool(args.estimate_total_files, settings.ingest.estimate_total_files)

    if args.retry_failed_from:
        retry_log = Path(args.retry_failed_from)
        paths: Iterable[Path] = iter_paths_from_failed_log(retry_log)
        total_files = count_failed_log_paths(retry_log) if estimate_total_files else None
    else:
        paths = iter_json_files(raw_dir)
        total_files = count_json_files(raw_dir) if estimate_total_files else None

    failure_logger = FailureLogger(failed_log_path)
    progress = IngestProgress(progress_every=progress_every, total_files=total_files)

    gateway = OpenSearchGateway(settings)
    gateway.ensure_index()

    previous_settings: dict | None = None
    doc_source_paths: dict[str, deque[str]] = defaultdict(deque)

    def docs_iter() -> Iterator[dict]:
        for parsed in _iter_parsed(
            paths,
            parse_workers=parse_workers,
            include_full_text=include_full_text,
            progress=progress,
        ):
            progress.on_parse(parsed)
            if parsed.doc is None:
                failure_logger.log(
                    stage="parse",
                    source_json_path=str(parsed.source_path),
                    doc_id=None,
                    error_type=parsed.error_type,
                    error_message=parsed.error_message,
                )
                continue

            doc = parsed.doc
            doc_id = str(doc.get("doc_id") or "")
            if doc_id:
                doc_source_paths[doc_id].append(str(parsed.source_path))
            yield doc

    def on_item_result(result: BulkItemResult) -> None:
        progress.on_index(result.ok)

        source_path: str | None = None
        if result.doc_id:
            candidates = doc_source_paths.get(result.doc_id)
            if candidates:
                source_path = candidates.popleft()
                if not candidates:
                    doc_source_paths.pop(result.doc_id, None)

        if not result.ok:
            failure_logger.log(
                stage="index",
                source_json_path=source_path,
                doc_id=result.doc_id,
                error_type=result.error_type,
                error_message=result.error_reason,
                status=result.status,
            )

    indexed_count = 0
    index_failed_count = 0
    try:
        if args.optimize_index_settings:
            previous_settings = gateway.optimize_for_bulk_ingest()

        result = gateway.bulk_upsert_iter(
            docs_iter(),
            chunk_size=args.chunk_size,
            max_chunk_bytes=args.max_chunk_bytes,
            thread_count=args.thread_count,
            refresh=False,
            on_item_result=on_item_result,
        )
        indexed_count = result.indexed_count
        index_failed_count = result.failed_count
    finally:
        failure_logger.close()
        progress.emit_final()
        if args.optimize_index_settings and previous_settings is not None:
            gateway.finalize_bulk_ingest(previous_settings)
        elif args.refresh_after_ingest:
            gateway.client.indices.refresh(index=gateway.index_alias)

    print(
        f"indexed={indexed_count} "
        f"index_failed={index_failed_count} "
        f"parse_failed={progress.parse_failed} "
        f"failed_log={failed_log_path}"
    )


if __name__ == "__main__":
    main()
