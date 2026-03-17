from __future__ import annotations

import re
from dataclasses import dataclass

from parser.ast_nodes import BinaryNode, Node, NotNode, TermNode


@dataclass
class Weights:
    title: float = 5
    mesh_terms: float = 4
    keywords: float = 3
    abstract_text: float = 2
    full_text_clean: float = 0


class OpenSearchTranslator:
    FIELD_MAP = {
        "ti": ["title"],
        "tiab": ["title", "abstract_text", "keywords"],
        "ta": ["journal"],
        "mh": ["mesh_terms_expanded"],
        "mh:noexp": ["mesh_terms_raw"],
        "pt": ["publication_type"],
        "dp": ["publication_date"],
        "pdat": ["publication_date"],
        "pmid": ["pmid"],
    }

    PMCID_RE = re.compile(r"^PMC\d+$", re.IGNORECASE)

    def __init__(self, weights: Weights | None = None) -> None:
        self.weights = weights or Weights()

    def translate(self, node: Node) -> dict:
        return self._node_to_query(node)

    def _node_to_query(self, node: Node) -> dict:
        if isinstance(node, BinaryNode):
            if node.operator == "AND":
                return {
                    "bool": {
                        "must": [self._node_to_query(node.left), self._node_to_query(node.right)]
                    }
                }
            if node.operator == "OR":
                return {
                    "bool": {
                        "should": [self._node_to_query(node.left), self._node_to_query(node.right)],
                        "minimum_should_match": 1,
                    }
                }
            raise ValueError(f"Unsupported operator: {node.operator}")
        if isinstance(node, NotNode):
            return {"bool": {"must_not": [self._node_to_query(node.operand)]}}
        if isinstance(node, TermNode):
            return self._term_to_query(node)
        raise TypeError(f"Unsupported node: {type(node)}")

    def _term_to_query(self, term: TermNode) -> dict:
        field = (term.field or "").lower().strip()
        if field in ("dp", "pdat"):
            return self._date_query(term.value)

        if self.PMCID_RE.match(term.value):
            return {
                "bool": {
                    "should": [
                        {"term": {"pmcid": term.value.upper()}},
                        {"term": {"pmcid_numeric": term.value.upper().replace("PMC", "")}},
                    ],
                    "minimum_should_match": 1,
                }
            }

        if term.is_phrase and field.startswith("tiab:~"):
            distance = int(field.split("~", 1)[1])
            return {
                "match_phrase": {
                    "abstract_text": {
                        "query": term.value,
                        "slop": distance,
                    }
                }
            }

        fields = self.FIELD_MAP.get(field)
        if fields:
            return self._multi_field_query(term, fields)

        return self._default_query(term)

    def _multi_field_query(self, term: TermNode, fields: list[str]) -> dict:
        if fields == ["publication_date"]:
            return self._date_query(term.value)
        if len(fields) == 1:
            field = fields[0]
            if term.value.endswith("*"):
                return self._prefix_query(field, term.value[:-1])
            if term.is_phrase:
                return {"match_phrase": {field: term.value}}
            return {"match": {field: {"query": term.value}}}

        weighted = []
        for f in fields:
            if f == "title":
                weighted.append(f"{f}^{self.weights.title}")
            elif f == "abstract_text":
                weighted.append(f"{f}^{self.weights.abstract_text}")
            elif f == "keywords":
                weighted.append(f"{f}^{self.weights.keywords}")
            else:
                weighted.append(f)

        return {
            "multi_match": {
                "query": term.value,
                "fields": weighted,
                "type": "phrase" if term.is_phrase else "best_fields",
            }
        }

    def _default_query(self, term: TermNode) -> dict:
        fields = [
            f"title^{self.weights.title}",
            f"mesh_terms_raw^{self.weights.mesh_terms}",
            f"mesh_terms_expanded^{self.weights.mesh_terms}",
            f"keywords^{self.weights.keywords}",
            f"abstract_text^{self.weights.abstract_text}",
        ]
        if self.weights.full_text_clean > 0:
            fields.append(f"full_text_clean^{self.weights.full_text_clean}")
        if term.value.endswith("*"):
            prefix = term.value[:-1]
            return {
                "multi_match": {
                    "query": prefix,
                    "fields": fields,
                    "type": "bool_prefix",
                }
            }
        return {
            "multi_match": {
                "query": term.value,
                "fields": fields,
                "type": "phrase" if term.is_phrase else "best_fields",
            }
        }

    def _prefix_query(self, field: str, prefix: str) -> dict:
        if field == "title":
            return {"prefix": {"title_normalized": {"value": prefix.lower()}}}
        if field == "journal":
            return {"prefix": {"journal.keyword": {"value": prefix}}}
        if field in {"title_normalized", "pmcid", "pmcid_numeric", "pmid", "publication_type"}:
            return {"prefix": {field: {"value": prefix}}}
        if field in {"keywords", "mesh_terms_raw", "mesh_terms_expanded"}:
            return {"prefix": {field: {"value": prefix}}}
        return {"match_phrase_prefix": {field: {"query": prefix}}}

    def _date_query(self, value: str) -> dict:
        return {
            "bool": {
                "must": [
                    {"exists": {"field": "publication_date"}},
                    {"term": {"publication_date": value}},
                ]
            }
        }
