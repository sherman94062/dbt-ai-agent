"""Query execution tool for running SQL against the warehouse."""

import os
import re
import time
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


def execute_query(
    sql: str,
    connection_string: str | None = None,
    log_query: bool = True
) -> dict[str, Any]:
    """
    Execute validated SQL against the tpch Postgres database.

    Args:
        sql: The SQL query to execute (must be SELECT only)
        connection_string: Optional connection string override
        log_query: Whether to log the query to the audit table

    Returns:
        Dict containing:
        - rows: List of result rows (as dicts)
        - columns: List of column names
        - row_count: Number of rows returned
        - elapsed_ms: Query execution time in milliseconds
    """
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 not installed. Run: pip install psycopg2-binary")

    # Validate query is SELECT only
    validation = _validate_select_only(sql)
    if not validation["valid"]:
        raise ValueError(f"Query validation failed: {validation['error']}")

    # Default connection
    conn_str = connection_string or os.environ.get(
        "DATABASE_URL",
        "postgresql://arthursherman@localhost:5432/tpch"
    )

    start_time = time.time()

    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()

        cursor.execute(sql)

        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows_raw = cursor.fetchall()

        # Convert to list of dicts
        rows = [dict(zip(columns, row)) for row in rows_raw]

        elapsed_ms = int((time.time() - start_time) * 1000)

        result = {
            "rows": rows,
            "columns": columns,
            "row_count": len(rows),
            "elapsed_ms": elapsed_ms,
        }

        cursor.close()
        conn.close()

        # Log query to audit table
        if log_query:
            _log_query(sql, result)

        return result

    except Exception as e:
        elapsed_ms = int((time.time() - start_time) * 1000)

        error_result = {
            "rows": [],
            "columns": [],
            "row_count": 0,
            "elapsed_ms": elapsed_ms,
            "error": str(e),
        }

        if log_query:
            _log_query(sql, error_result)

        raise


def _validate_select_only(sql: str) -> dict:
    """Validate that the query is SELECT only."""
    sql_upper = sql.upper().strip()

    # Must start with SELECT or WITH (for CTEs)
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        return {"valid": False, "error": "Query must start with SELECT or WITH"}

    # Check for dangerous keywords
    dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
                 "TRUNCATE", "GRANT", "REVOKE", "EXEC", "EXECUTE"]

    for keyword in dangerous:
        # Use word boundary to avoid false positives
        if re.search(rf'\b{keyword}\b', sql_upper):
            return {"valid": False, "error": f"Dangerous keyword not allowed: {keyword}"}

    return {"valid": True}


def _log_query(sql: str, result: dict) -> None:
    """Log query execution to SQLite audit table."""
    audit_db = Path(__file__).parent.parent / "audit.db"

    conn = sqlite3.connect(audit_db)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS query_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            query TEXT NOT NULL,
            row_count INTEGER,
            elapsed_ms INTEGER,
            error TEXT
        )
    """)

    cursor.execute("""
        INSERT INTO query_log (timestamp, query, row_count, elapsed_ms, error)
        VALUES (?, ?, ?, ?, ?)
    """, (
        datetime.now().isoformat(),
        sql,
        result.get("row_count", 0),
        result.get("elapsed_ms", 0),
        result.get("error"),
    ))

    conn.commit()
    conn.close()


def format_results(result: dict, max_rows: int = 20) -> str:
    """Format query results as a readable table string."""
    if result.get("error"):
        return f"Error: {result['error']}"

    if not result["rows"]:
        return "No results returned."

    columns = result["columns"]
    rows = result["rows"][:max_rows]

    # Calculate column widths
    widths = {}
    for col in columns:
        widths[col] = len(str(col))
        for row in rows:
            val_len = len(str(row.get(col, "")))
            if val_len > widths[col]:
                widths[col] = min(val_len, 40)  # Cap at 40 chars

    # Build table
    lines = []

    # Header
    header = " | ".join(str(col).ljust(widths[col])[:widths[col]] for col in columns)
    lines.append(header)
    lines.append("-" * len(header))

    # Rows
    for row in rows:
        line = " | ".join(
            str(row.get(col, "")).ljust(widths[col])[:widths[col]]
            for col in columns
        )
        lines.append(line)

    if result["row_count"] > max_rows:
        lines.append(f"... and {result['row_count'] - max_rows} more rows")

    lines.append(f"\n({result['row_count']} rows, {result['elapsed_ms']}ms)")

    return "\n".join(lines)


if __name__ == "__main__":
    # Test queries
    test_queries = [
        "SELECT market_segment, SUM(line_revenue) as total_revenue FROM fct_order_items GROUP BY market_segment ORDER BY total_revenue DESC",
        "SELECT COUNT(*) as total_orders FROM fct_order_items",
    ]

    for sql in test_queries:
        print(f"Query: {sql[:80]}...")
        try:
            result = execute_query(sql)
            print(format_results(result))
        except Exception as e:
            print(f"Error: {e}")
        print()
