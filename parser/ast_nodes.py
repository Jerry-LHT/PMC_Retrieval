from __future__ import annotations

from dataclasses import dataclass


class Node:
    pass


@dataclass
class TermNode(Node):
    value: str
    field: str | None = None
    is_phrase: bool = False


@dataclass
class NotNode(Node):
    operand: Node


@dataclass
class BinaryNode(Node):
    operator: str
    left: Node
    right: Node
