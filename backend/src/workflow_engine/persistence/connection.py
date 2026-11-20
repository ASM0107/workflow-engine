import os
from contextlib import contextmanager
from typing import Any, Generator

import psycopg
from psycopg.rows import dict_row

DEFAULT_DATABASE_URL = "postgresql://engine_user@127.0.0.1:5433/workflow_engine"


def get_database_url() -> str:
    url = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
    if "+psycopg" in url:
        url = url.replace("+psycopg", "")
    return url


@contextmanager
def get_connection(
    autocommit: bool = False,
) -> Generator[psycopg.Connection[dict[str, Any]], None, None]:
    url = get_database_url()
    conn = psycopg.connect(url, row_factory=dict_row, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()
