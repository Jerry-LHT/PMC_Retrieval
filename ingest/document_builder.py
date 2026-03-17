from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from ingest.date_utils import parse_publication_date


PMCID_RE = re.compile(r"PMC(\d+)", re.IGNORECASE)


class MeshExpander:
    """Pluggable interface for future MeSH tree expansion."""

    def expand(self, mesh_terms: list[str]) -> list[str]:
        return mesh_terms


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else []
    return [str(value).strip()]


def build_document(
    source: dict[str, Any],
    source_path: Path,
    mesh_expander: MeshExpander | None = None,
    include_full_text: bool = True,
) -> dict | None:
    pmcid = str(source.get("article_accession_id") or "").strip()
    if not pmcid:
        return None

    match = PMCID_RE.search(pmcid)
    pmcid_numeric = match.group(1) if match else ""

    title = str(source.get("article_title") or "").strip()
    mesh_raw = _as_list(source.get("article_mesh_terms"))
    expander = mesh_expander or MeshExpander()
    mesh_expanded = expander.expand(mesh_raw)
    full_text: str | None = str(source.get("article_text") or "").strip() if include_full_text else None

    return {
        "doc_id": pmcid.upper(),
        "pmcid": pmcid.upper(),
        "pmcid_numeric": pmcid_numeric,
        "pmid": None,
        "title": title,
        "title_normalized": title.lower(),
        "journal": str(source.get("article_journal") or "").strip(),
        "publication_date": parse_publication_date(source.get("article_date")),
        "publication_type": _as_list(source.get("article_subject")),
        "abstract_text": str(source.get("article_abstract") or "").strip(),
        "mesh_terms_raw": mesh_raw,
        "mesh_terms_expanded": mesh_expanded,
        "keywords": _as_list(source.get("article_keywords")),
        "full_text_clean": full_text,
        "source_json_path": str(source_path),
    }
