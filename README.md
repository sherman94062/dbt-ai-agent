# dbt Semantic Layer Agent

A conversational AI agent that understands your dbt semantic layer, translates natural language questions into SQL, executes queries, and explains data lineage — bringing governed NL-to-SQL to your analytics stack.

[![Built with Claude](https://img.shields.io/badge/Built%20with-Claude-blueviolet)](https://claude.ai)
[![dbt](https://img.shields.io/badge/dbt-1.10+-orange)](https://www.getdbt.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-blue)](https://www.postgresql.org)

## Why This Project?

Modern data teams invest heavily in dbt to create a semantic layer — metrics, dimensions, and business logic defined in code. But accessing this layer still requires SQL knowledge. This agent bridges that gap:

1. **Ask questions in plain English** → Get SQL that respects your semantic definitions
2. **Understand where data comes from** → Full lineage from metrics to raw sources
3. **Governed by design** → Only SELECT queries, validated against your manifest

## Features

- **Natural Language to SQL**: Convert questions like "What is total revenue by market segment?" into executable SQL
- **Semantic Layer Aware**: Understands dbt metrics, dimensions, and measures from `semantic_manifest.json`
- **Data Lineage**: Traces data provenance from metrics back to raw source tables
- **Query Validation**: Ensures generated SQL only references existing tables and columns
- **Audit Logging**: All queries logged to SQLite for governance and debugging
- **Two Query Modes**: Pattern matching (fast, no API) or Claude LLM (flexible, requires API key)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│ INTERFACE LAYER                                                     │
│  CLI / Chat UI  ◄──►  Agent orchestrator  ──►  Project context      │
└────────────────────────────┬────────────────────────────────────────┘
                             │
             ┌───────────────┼───────────────┐
             ▼               ▼               ▼
    ┌─────────────┐  ┌──────────────┐  ┌───────────────┐
    │  Project    │  │   Query      │  │   Lineage     │
    │ introspector│  │  generator   │  │  explainer    │
    └──────┬──────┘  └──────┬───────┘  └───────┬───────┘
           │                │                   │
┌──────────▼────────────────▼───────────────────▼───────────────────┐
│ dbt LAYER                                                         │
│  manifest.json    semantic_manifest.json    Metrics/Dimensions    │
└──────────┬────────────────────────────────────────────────────────┘
           │
┌──────────▼────────────────────────────────────────────────────────┐
│ WAREHOUSE                                                          │
│  PostgreSQL (tpch)         Query execution         Audit logging  │
└───────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL with TPC-H data loaded (see [WALKTHROUGH.md](WALKTHROUGH.md) for setup)
- dbt-core and dbt-postgres

### Installation

```bash
# Clone the repository
git clone https://github.com/sherman94062/dbt-ai-agent.git
cd dbt-ai-agent

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure dbt profile (edit with your credentials)
mkdir -p ~/.dbt
cp profiles.yml.example ~/.dbt/profiles.yml

# Build dbt models
dbt build

# Run the agent
python agent/cli.py "What is total revenue by market segment?"
```

### Usage

```bash
# Ask a question (uses pattern matching)
python agent/cli.py "What are the top 10 suppliers by revenue?"

# Use Claude LLM for more flexible queries (requires ANTHROPIC_API_KEY)
export ANTHROPIC_API_KEY=your_key_here
python agent/cli.py --llm "Show me customers with declining order values"

# Show project context (models, metrics, dimensions)
python agent/cli.py --context

# Show metric lineage
python agent/cli.py --lineage total_revenue

# Interactive REPL mode
python agent/cli.py -i

# Dry run (generate SQL without executing)
python agent/cli.py --dry-run "What is the return rate by region?"
```

## Project Structure

```
dbt-ai-agent/
├── README.md                   # This file
├── WALKTHROUGH.md              # Detailed tutorial
├── PLANNING.md                 # Original project plan
├── requirements.txt            # Python dependencies
├── dbt_project.yml             # dbt project configuration
├── models/
│   ├── staging/
│   │   ├── _sources.yml        # Raw TPC-H table declarations
│   │   ├── stg_orders.sql      # Staging: orders
│   │   ├── stg_lineitems.sql   # Staging: line items with revenue calc
│   │   ├── stg_customers.sql   # Staging: customers
│   │   ├── stg_suppliers.sql   # Staging: suppliers
│   │   ├── stg_parts.sql       # Staging: parts catalog
│   │   └── stg_nations.sql     # Staging: nations with regions
│   └── marts/
│       ├── _models.yml         # Model docs + semantic layer config
│       ├── fct_order_items.sql # Fact: order line items (600K rows)
│       ├── dim_customers.sql   # Dimension: customers (15K rows)
│       ├── dim_suppliers.sql   # Dimension: suppliers (1K rows)
│       └── metricflow_time_spine.sql
└── agent/
    ├── __init__.py
    ├── cli.py                  # Command-line interface
    ├── introspection.py        # Reads dbt manifest & semantic manifest
    ├── query_generator.py      # NL → SQL (pattern matching + LLM)
    ├── executor.py             # Query execution with validation
    └── lineage.py              # Data lineage explanation
```

## Semantic Layer

### Metrics

| Metric | Description | Type |
|--------|-------------|------|
| `total_revenue` | SUM(extended_price * (1 - discount)) | simple |
| `order_count` | COUNT(DISTINCT order_id) | simple |
| `avg_order_value` | total_revenue / order_count | derived |
| `total_quantity` | SUM(quantity) | simple |
| `avg_discount` | AVG(discount) | simple |
| `fulfillment_days` | AVG(ship_date - order_date) | simple |
| `return_rate` | returned_items / line_count | derived |

### Dimensions

| Dimension | Type | Values |
|-----------|------|--------|
| `order_date` | time | 1992-01-01 to 1998-08-02 |
| `ship_date` | time | Date item shipped |
| `order_status` | categorical | F (fulfilled), O (open), P (pending) |
| `order_priority` | categorical | 1-URGENT, 2-HIGH, 3-MEDIUM, 4-NOT SPECIFIED, 5-LOW |
| `market_segment` | categorical | AUTOMOBILE, BUILDING, FURNITURE, MACHINERY, HOUSEHOLD |
| `customer_nation` | categorical | 25 nations |
| `customer_region` | categorical | AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST |
| `return_flag` | categorical | R (returned), A (accepted), N (none) |

## Example Session

```
$ python agent/cli.py -i

🤖 dbt Semantic Layer Agent
========================================
Ask questions about your data in natural language.
Commands: /context, /lineage <metric>, /quit

❓ What is total revenue by market segment?

📊 Question: What is total revenue by market segment?

🔍 Generating SQL query...

📝 Generated SQL (confidence: high):
SELECT
    market_segment,
    SUM(line_revenue) as total_revenue
FROM fct_order_items
GROUP BY market_segment
ORDER BY total_revenue DESC

💡 Aggregates revenue by market segment from the fact table

⚡ Executing query...

📊 Results:
market_segment | total_revenue
--------------------------------
BUILDING       | 4281563957.54
MACHINERY      | 4161899266.50
AUTOMOBILE     | 4093302441.07
HOUSEHOLD      | 4013057197.05
FURNITURE      | 3985249369.26

(5 rows, 241ms)

============================================================
## Data Provenance

### fct_order_items
*Fact table containing order line items with customer and order context*

**Source chain:**
  ← stg_orders ← Source: tpch.orders (raw table)
  ← stg_lineitems ← Source: tpch.lineitem (raw table)
  ← stg_customers ← Source: tpch.customer (raw table)
  ← stg_nations ← Source: tpch.nation, tpch.region (raw tables)
```

## Database

- **Engine:** PostgreSQL 15+
- **Dataset:** TPC-H scale factor 0.1
- **Size:** ~600K line items, 150K orders, 15K customers
- **Tables:** region, nation, customer, supplier, part, partsupp, orders, lineitem

## Tech Stack

| Component | Technology |
|-----------|------------|
| Agent orchestration | Python 3.10+, Claude API (optional) |
| Data modeling | dbt-core 1.10+, dbt-postgres |
| Semantic layer | dbt Semantic Layer / MetricFlow |
| Warehouse | PostgreSQL |
| Audit logging | SQLite |

## Roadmap

- [ ] Streamlit web UI
- [ ] MetricFlow query mode (mf query)
- [ ] Vector search for semantic column matching
- [ ] MCP server for Claude Desktop integration
- [ ] Databricks/Snowflake adapters

## Contributing

Contributions welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- [dbt Labs](https://www.getdbt.com) for dbt and the Semantic Layer
- [Anthropic](https://www.anthropic.com) for Claude
- [TPC-H](http://www.tpc.org/tpch/) benchmark for the sample dataset
