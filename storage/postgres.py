from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg


@contextmanager
def pg_connection(dsn: str) -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
