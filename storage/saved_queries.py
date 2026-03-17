from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from storage.postgres import pg_connection


@dataclass
class SavedQuery:
    id: int
    name: str
    query_text: str
    tags: list[str]
    created_at: datetime
    updated_at: datetime
    last_used_at: datetime | None


class SavedQueryRepository:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def ensure_schema(self) -> None:
        sql = """
        CREATE TABLE IF NOT EXISTS saved_queries (
          id BIGSERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          query_text TEXT NOT NULL,
          tags TEXT[] NOT NULL DEFAULT '{}',
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          last_used_at TIMESTAMPTZ NULL
        );
        """
        with pg_connection(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

    def create(self, name: str, query_text: str, tags: list[str] | None = None) -> SavedQuery:
        tags = tags or []
        sql = """
        INSERT INTO saved_queries(name, query_text, tags)
        VALUES (%s, %s, %s)
        RETURNING id, name, query_text, tags, created_at, updated_at, last_used_at;
        """
        with pg_connection(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (name, query_text, tags))
                row = cur.fetchone()
        return _to_saved_query(row)

    def get(self, query_id: int) -> SavedQuery | None:
        sql = """
        SELECT id, name, query_text, tags, created_at, updated_at, last_used_at
        FROM saved_queries
        WHERE id=%s;
        """
        with pg_connection(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (query_id,))
                row = cur.fetchone()
        return _to_saved_query(row) if row else None

    def list(self, limit: int = 100, offset: int = 0) -> list[SavedQuery]:
        sql = """
        SELECT id, name, query_text, tags, created_at, updated_at, last_used_at
        FROM saved_queries
        ORDER BY updated_at DESC
        LIMIT %s OFFSET %s;
        """
        with pg_connection(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (limit, offset))
                rows = cur.fetchall()
        return [_to_saved_query(r) for r in rows]

    def update(self, query_id: int, *, name: str | None = None, query_text: str | None = None, tags: list[str] | None = None) -> SavedQuery | None:
        existing = self.get(query_id)
        if not existing:
            return None

        new_name = name if name is not None else existing.name
        new_query_text = query_text if query_text is not None else existing.query_text
        new_tags = tags if tags is not None else existing.tags
        now = datetime.now(timezone.utc)

        sql = """
        UPDATE saved_queries
        SET name=%s, query_text=%s, tags=%s, updated_at=%s
        WHERE id=%s
        RETURNING id, name, query_text, tags, created_at, updated_at, last_used_at;
        """
        with pg_connection(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (new_name, new_query_text, new_tags, now, query_id))
                row = cur.fetchone()
        return _to_saved_query(row)

    def delete(self, query_id: int) -> bool:
        sql = "DELETE FROM saved_queries WHERE id=%s;"
        with pg_connection(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (query_id,))
                return cur.rowcount > 0

    def mark_used(self, query_id: int) -> SavedQuery | None:
        now = datetime.now(timezone.utc)
        sql = """
        UPDATE saved_queries
        SET last_used_at=%s, updated_at=%s
        WHERE id=%s
        RETURNING id, name, query_text, tags, created_at, updated_at, last_used_at;
        """
        with pg_connection(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (now, now, query_id))
                row = cur.fetchone()
        return _to_saved_query(row) if row else None


def _to_saved_query(row: Any) -> SavedQuery:
    return SavedQuery(
        id=row[0],
        name=row[1],
        query_text=row[2],
        tags=row[3] or [],
        created_at=row[4],
        updated_at=row[5],
        last_used_at=row[6],
    )
