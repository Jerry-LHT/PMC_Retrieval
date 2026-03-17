from __future__ import annotations

import json
from pathlib import Path

from ingest.ingest_json import build_documents, count_failed_log_paths, iter_documents, iter_paths_from_failed_log


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_build_documents_json_only_and_dedup(tmp_path: Path) -> None:
    payload_a = {
        "article_accession_id": "PMC1",
        "article_title": "A",
        "article_journal": "J",
        "article_date": "1728284400",
        "article_abstract": "abs",
        "article_mesh_terms": ["Humans"],
        "article_keywords": None,
        "article_subject": ["Research Article"],
        "article_text": "full",
    }
    payload_b = {
        **payload_a,
        "article_title": "A updated",
    }

    _write_json(tmp_path / "a.json", payload_a)
    _write_json(tmp_path / "a.txt", {"ignored": True})
    _write_json(tmp_path / "nested.json", {"article_accession_id": "PMC2", "article_title": "B"})
    (tmp_path / "img.jpg").write_text("x", encoding="utf-8")
    _write_json(tmp_path / "dup.json", payload_b)

    docs = build_documents(tmp_path)
    assert len(docs) == 2

    by_id = {d["doc_id"]: d for d in docs}
    assert by_id["PMC1"]["title"] == "A updated"
    assert by_id["PMC1"]["keywords"] == []
    assert by_id["PMC1"]["publication_date"] == "2024-10-07"


def test_iter_documents_streaming_keeps_duplicates_for_last_write_wins(tmp_path: Path) -> None:
    base = {
        "article_accession_id": "PMC1",
        "article_title": "A",
    }
    _write_json(tmp_path / "a.json", base)
    _write_json(tmp_path / "b.json", {**base, "article_title": "A2"})

    docs = list(iter_documents(tmp_path))
    assert len(docs) == 2
    assert docs[-1]["title"] == "A2"


def test_iter_paths_from_failed_log_dedup(tmp_path: Path) -> None:
    failed = tmp_path / "failed.ndjson"
    p1 = tmp_path / "a.json"
    p2 = tmp_path / "b.json"
    failed.write_text(
        "\n".join(
            [
                json.dumps({"source_json_path": str(p1)}),
                json.dumps({"source_json_path": str(p1)}),
                json.dumps({"source_json_path": str(p2)}),
                "{bad json",
                json.dumps({"source_json_path": ""}),
            ]
        ),
        encoding="utf-8",
    )

    paths = list(iter_paths_from_failed_log(failed))
    assert paths == [p1, p2]
    assert count_failed_log_paths(failed) == 2
