"""Connection + execution helpers. Follows Week 03's db.py pattern
(get_connection / execute_sql_file, try/except/rollback/finally) — using
psycopg2 rather than Week 03's psycopg (v3) purely because that's what
installs cleanly in this environment; the usage is equivalent for our
synchronous, single-connection-per-script needs.
"""

import psycopg2
import psycopg2.extras


def get_connection(config):
    conn = psycopg2.connect(**config)
    conn.autocommit = False
    return conn


def execute_sql_file(config, path):
    conn = get_connection(config)
    try:
        with open(path, "r", encoding="utf-8") as f:
            sql = f.read()
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def dict_cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
