<!-- BlackRoad SEO Enhanced -->

# ulackroad os clickhouse

> Part of **[BlackRoad OS](https://blackroad.io)** — Sovereign Computing for Everyone

[![BlackRoad OS](https://img.shields.io/badge/BlackRoad-OS-ff1d6c?style=for-the-badge)](https://blackroad.io)
[![BlackRoad-Forge](https://img.shields.io/badge/Org-BlackRoad-Forge-2979ff?style=for-the-badge)](https://github.com/BlackRoad-Forge)

**ulackroad os clickhouse** is part of the **BlackRoad OS** ecosystem — a sovereign, distributed operating system built on edge computing, local AI, and mesh networking by **BlackRoad OS, Inc.**

### BlackRoad Ecosystem
| Org | Focus |
|---|---|
| [BlackRoad OS](https://github.com/BlackRoad-OS) | Core platform |
| [BlackRoad OS, Inc.](https://github.com/BlackRoad-OS-Inc) | Corporate |
| [BlackRoad AI](https://github.com/BlackRoad-AI) | AI/ML |
| [BlackRoad Hardware](https://github.com/BlackRoad-Hardware) | Edge hardware |
| [BlackRoad Security](https://github.com/BlackRoad-Security) | Cybersecurity |
| [BlackRoad Quantum](https://github.com/BlackRoad-Quantum) | Quantum computing |
| [BlackRoad Agents](https://github.com/BlackRoad-Agents) | AI agents |
| [BlackRoad Network](https://github.com/BlackRoad-Network) | Mesh networking |

**Website**: [blackroad.io](https://blackroad.io) | **Chat**: [chat.blackroad.io](https://chat.blackroad.io) | **Search**: [search.blackroad.io](https://search.blackroad.io)

---


Python HTTP client for ClickHouse analytics database with query builder and data export.

## Features

- **HTTP Interface**: Uses ClickHouse HTTP API (no TCP driver required)
- **Query Execution**: Direct SQL query execution
- **Batch Operations**: Bulk insert and select
- **Query Builder**: Fluent SQL builder for common queries
- **Data Export**: CSV export for analytics pipelines
- **Schema Operations**: Create tables, describe schema, list tables
- **Table Statistics**: Row counts, compression metrics
- **File Execution**: Run SQL scripts from files

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### CLI

```bash
# Execute query
python src/clickhouse_client.py query "SELECT COUNT(*) as count FROM my_table" --host localhost

# List tables
python src/clickhouse_client.py tables --host localhost

# Get table stats
python src/clickhouse_client.py stats my_table --host localhost
```

### Python API

```python
from src.clickhouse_client import ClickHouseClient, ClickHouseQuery

# Create client
client = ClickHouseClient(host="localhost", port=8123)

# Execute query
results = client.query("SELECT * FROM events LIMIT 10")

# Build query
query = ClickHouseQuery().select("id", "count()").from_("events").group_by("id").limit(10).build()
results = client.query(query)

# Insert data
rows = [
    {"id": 1, "value": "a"},
    {"id": 2, "value": "b"},
]
client.insert("my_table", rows)

# Export to CSV
client.export_csv("SELECT * FROM events", "events.csv")

# Get table stats
stats = client.table_stats("events")
```

## Configuration

Connect to ClickHouse instance:

```python
client = ClickHouseClient(
    host="localhost",
    port=8123,
    database="default",
    user="default",
    password=""
)
```

## License

MIT
