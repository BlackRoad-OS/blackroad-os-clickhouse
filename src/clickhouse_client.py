"""
ClickHouse Analytics Client Wrapper

Provides a Python interface to ClickHouse using HTTP protocol.
No external dependencies required beyond urllib.
"""

import urllib.request
import urllib.parse
import json
import csv
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
import argparse


@dataclass
class ClickHouseConnection:
    """ClickHouse connection configuration."""
    host: str
    port: int = 8123
    database: str = "default"
    user: str = "default"
    password: str = ""

    @property
    def base_url(self) -> str:
        """Get base URL for ClickHouse."""
        return f"http://{self.host}:{self.port}"


class ClickHouseClient:
    """ClickHouse analytics client."""

    def __init__(
        self,
        host: str,
        port: int = 8123,
        database: str = "default",
        user: str = "default",
        password: str = "",
    ):
        """Initialize ClickHouse client."""
        self.conn = ClickHouseConnection(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )

    def query(self, sql: str, format: str = "JSON") -> List[Dict]:
        """Execute query and return list of dicts."""
        url = f"{self.conn.base_url}/?database={self.conn.database}&format={format}"

        if self.conn.user:
            url += f"&user={self.conn.user}"
        if self.conn.password:
            url += f"&password={self.conn.password}"

        try:
            req = urllib.request.Request(
                url,
                data=sql.encode(),
                method="POST",
            )
            req.add_header("Content-Type", "text/plain")

            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode())
                return result.get("data", [])
        except urllib.error.URLError as e:
            raise Exception(f"ClickHouse query failed: {e}")

    def query_df(self, sql: str) -> List[Dict]:
        """Execute query and return as list of dicts (DataFrame-like)."""
        return self.query(sql, format="JSON")

    def insert(self, table: str, rows: List[Dict]) -> bool:
        """Batch insert rows."""
        if not rows:
            return True

        # Convert rows to TabSeparated format
        columns = list(rows[0].keys())
        values = []

        for row in rows:
            values.append([str(row.get(col, "")) for col in columns])

        # Build INSERT statement
        columns_str = ", ".join(columns)
        values_lines = ["\t".join(row) for row in values]
        insert_data = "\n".join(values_lines)

        sql = f"INSERT INTO {table} ({columns_str}) FORMAT TabSeparated"

        url = f"{self.conn.base_url}/?database={self.conn.database}"
        if self.conn.user:
            url += f"&user={self.conn.user}"
        if self.conn.password:
            url += f"&password={self.conn.password}"

        try:
            req = urllib.request.Request(
                url,
                data=insert_data.encode(),
                method="POST",
            )
            req.add_header("Content-Type", "text/plain")
            req.add_header("X-ClickHouse-Query", sql)

            with urllib.request.urlopen(req, timeout=30) as response:
                response.read()
                return True
        except urllib.error.URLError as e:
            raise Exception(f"ClickHouse insert failed: {e}")

    def create_table(
        self,
        name: str,
        schema: str,
        engine: str = "MergeTree()",
        order_by: str = "tuple()",
    ) -> bool:
        """Create a table."""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {name} (
            {schema}
        )
        ENGINE = {engine}
        ORDER BY {order_by}
        """

        try:
            self.query(sql)
            return True
        except Exception:
            return False

    def describe_table(self, name: str) -> List[Dict]:
        """Get table schema information."""
        sql = f"DESCRIBE TABLE {name}"
        return self.query(sql)

    def list_tables(self, database: Optional[str] = None) -> List[str]:
        """List tables in database."""
        db = database or self.conn.database
        sql = f"SELECT name FROM system.tables WHERE database = '{db}'"
        results = self.query(sql)
        return [row["name"] for row in results]

    def table_stats(self, name: str) -> Dict[str, Any]:
        """Get table statistics."""
        sql = f"""
        SELECT
            rows,
            bytes_on_disk as compressed_size,
            bytes_uncompressed as uncompressed_size
        FROM system.tables
        WHERE name = '{name}'
        """
        try:
            results = self.query(sql)
            if results:
                row = results[0]
                return {
                    "rows": row.get("rows", 0),
                    "compressed_size_mb": row.get("compressed_size", 0) / (1024 * 1024),
                    "uncompressed_size_mb": row.get("uncompressed_size", 0) / (1024 * 1024),
                }
            return {"rows": 0, "compressed_size_mb": 0, "uncompressed_size_mb": 0}
        except Exception:
            return {"rows": 0, "compressed_size_mb": 0, "uncompressed_size_mb": 0}

    def execute_file(self, path: str) -> bool:
        """Execute SQL from file."""
        try:
            with open(path, "r") as f:
                sql = f.read()
            self.query(sql)
            return True
        except Exception as e:
            raise Exception(f"Failed to execute file {path}: {e}")

    def export_csv(self, sql: str, output_path: str) -> bool:
        """Export query results to CSV."""
        try:
            results = self.query(sql)

            if not results:
                with open(output_path, "w") as f:
                    f.write("")
                return True

            # Get column names from first row
            columns = list(results[0].keys())

            with open(output_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()
                writer.writerows(results)

            return True
        except Exception as e:
            raise Exception(f"Failed to export CSV: {e}")


class ClickHouseQuery:
    """Query builder for ClickHouse SQL."""

    def __init__(self):
        """Initialize query builder."""
        self._select_cols = []
        self._from_table = None
        self._where_conditions = []
        self._group_cols = []
        self._order_cols = []
        self._limit_count = None
        self._desc = False

    def select(self, *cols) -> "ClickHouseQuery":
        """Add SELECT columns."""
        self._select_cols.extend(cols)
        return self

    def from_(self, table: str) -> "ClickHouseQuery":
        """Add FROM table."""
        self._from_table = table
        return self

    def where(self, *conditions) -> "ClickHouseQuery":
        """Add WHERE conditions."""
        self._where_conditions.extend(conditions)
        return self

    def group_by(self, *cols) -> "ClickHouseQuery":
        """Add GROUP BY columns."""
        self._group_cols.extend(cols)
        return self

    def order_by(self, *cols, desc: bool = False) -> "ClickHouseQuery":
        """Add ORDER BY columns."""
        self._order_cols.extend(cols)
        self._desc = desc
        return self

    def limit(self, n: int) -> "ClickHouseQuery":
        """Add LIMIT."""
        self._limit_count = n
        return self

    def build(self) -> str:
        """Build SQL query string."""
        parts = []

        # SELECT
        if self._select_cols:
            parts.append(f"SELECT {', '.join(self._select_cols)}")
        else:
            parts.append("SELECT *")

        # FROM
        if self._from_table:
            parts.append(f"FROM {self._from_table}")

        # WHERE
        if self._where_conditions:
            parts.append(f"WHERE {' AND '.join(self._where_conditions)}")

        # GROUP BY
        if self._group_cols:
            parts.append(f"GROUP BY {', '.join(self._group_cols)}")

        # ORDER BY
        if self._order_cols:
            order_str = ", ".join(self._order_cols)
            if self._desc:
                order_str += " DESC"
            parts.append(f"ORDER BY {order_str}")

        # LIMIT
        if self._limit_count:
            parts.append(f"LIMIT {self._limit_count}")

        return " ".join(parts)


def main():
    """CLI interface."""
    parser = argparse.ArgumentParser(description="ClickHouse Client")
    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=8123, help="ClickHouse port")
    parser.add_argument("--database", default="default", help="Database")
    parser.add_argument("--user", default="default", help="Username")
    parser.add_argument("--password", default="", help="Password")

    subparsers = parser.add_subparsers(dest="command")

    # Query command
    query_parser = subparsers.add_parser("query", help="Execute query")
    query_parser.add_argument("sql", help="SQL query")

    # Tables command
    tables_parser = subparsers.add_parser("tables", help="List tables")

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Table statistics")
    stats_parser.add_argument("table", help="Table name")

    args = parser.parse_args()

    client = ClickHouseClient(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password,
    )

    if args.command == "query":
        try:
            results = client.query(args.sql)
            print(json.dumps(results, indent=2))
        except Exception as e:
            print(f"Error: {e}")

    elif args.command == "tables":
        try:
            tables = client.list_tables()
            for table in tables:
                print(table)
        except Exception as e:
            print(f"Error: {e}")

    elif args.command == "stats":
        try:
            stats = client.table_stats(args.table)
            print(json.dumps(stats, indent=2))
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
