from __future__ import annotations

from pathlib import Path

from lark import Lark, Transformer, v_args

from parser.ast_nodes import BinaryNode, Node, NotNode, TermNode

GRAMMAR_PATH = Path(__file__).with_name("grammar.lark")


@v_args(inline=True)
class AstTransformer(Transformer):
    def atom(self, *items):
        # Parenthesized expressions arrive as: LPAREN, expr, RPAREN.
        # Keep only the inner expression node so translator receives AST Node objects.
        if len(items) == 3:
            return items[1]
        if len(items) == 1:
            return items[0]
        raise ValueError(f"Unexpected atom items: {items!r}")

    def phrase_term(self, phrase, field=None):
        value = str(phrase)[1:-1]
        return TermNode(value=value, field=str(field) if field is not None else None, is_phrase=True)

    def word_term(self, word, field=None):
        return TermNode(value=str(word), field=str(field) if field is not None else None, is_phrase=False)

    def field(self, _l, body, _r):
        return str(body)

    def and_op(self, left, _and, right):
        return BinaryNode(operator="AND", left=left, right=right)

    def or_op(self, left, _or, right):
        return BinaryNode(operator="OR", left=left, right=right)

    def not_op(self, _not, operand):
        return NotNode(operand=operand)


class PubMedParser:
    def __init__(self) -> None:
        self._parser = Lark(GRAMMAR_PATH.read_text(encoding="utf-8"), parser="lalr")
        self._transformer = AstTransformer()

    def parse(self, query: str) -> Node:
        tree = self._parser.parse(query)
        return self._transformer.transform(tree)
