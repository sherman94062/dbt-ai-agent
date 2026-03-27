# dbt Semantic Layer Agent — Walkthrough

This document provides a step-by-step guide to understanding and using the dbt Semantic Layer Agent. Whether you're setting up the project from scratch or exploring how it works, this walkthrough covers everything.

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Setting Up the Database](#setting-up-the-database)
4. [Understanding the dbt Project](#understanding-the-dbt-project)
5. [How the Agent Works](#how-the-agent-works)
6. [Using the CLI](#using-the-cli)
7. [Understanding Data Lineage](#understanding-data-lineage)
8. [Extending the Agent](#extending-the-agent)

---

## Introduction

### What is this project?

This project demonstrates how to build a **governed natural language interface** on top of a dbt semantic layer. Instead of writing SQL directly, users can ask questions like:

- "What is total revenue by market segment?"
- "Which suppliers have the highest sales?"
- "How has our return rate changed over time?"

The agent:
1. **Reads** your dbt project's manifest and semantic manifest
2. **Generates** SQL that respects your defined metrics and dimensions
3. **Executes** the query against your warehouse
4. **Explains** where the data came from (lineage)

### Why is this useful?

| Problem | Solution |
|---------|----------|
| Business users can't write SQL | Natural language interface |
| Metrics defined inconsistently | Uses dbt semantic layer definitions |
| No visibility into data sources | Automatic lineage explanation |
| Security concerns with raw SQL access | Query validation, SELECT only |

---

## Prerequisites

### Required Software

```bash
# Check versions
python --version    # 3.10+ required
psql --version      # PostgreSQL 12+ required
dbt --version       # 1.10+ required
```

### Python Dependencies

```bash
pip install dbt-core dbt-postgres psycopg2-binary anthropic
```

### Optional: Claude API Key

For LLM-powered query generation (more flexible than pattern matching):

```bash
export ANTHROPIC_API_KEY=sk-ant-api03-...
```

---

## Setting Up the Database

### Step 1: Create the PostgreSQL Database

```bash
createdb tpch
```

### Step 2: Load TPC-H Data

The project includes a `load.py` script that generates TPC-H benchmark data:

```bash
python load.py
```

This creates 8 tables with realistic e-commerce data:

| Table | Rows | Description |
|-------|------|-------------|
| region | 5 | Geographic regions |
| nation | 25 | Countries |
| customer | 15,000 | Customer records |
| supplier | 1,000 | Supplier records |
| part | 20,000 | Product catalog |
| partsupp | 80,000 | Part-supplier relationships |
| orders | 150,000 | Customer orders |
| lineitem | 600,572 | Order line items |

### Step 3: Configure dbt Profile

Create `~/.dbt/profiles.yml`:

```yaml
tpch_agent:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: your_username
      password: ""  # or your password
      dbname: tpch
      schema: public
      threads: 4
```

### Step 4: Build dbt Models

```bash
cd dbt-ai-agent
dbt build
```

Expected output:
```
Completed successfully
Done. PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10
```

---

## Understanding the dbt Project

### Data Model Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        RAW SOURCES                               │
│  region → nation → customer → orders → lineitem                 │
│                     nation → supplier ↗                          │
│                              part ↗                              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                     STAGING MODELS                               │
│  stg_nations (nation + region)                                  │
│  stg_customers, stg_suppliers, stg_orders, stg_lineitems        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                       MART MODELS                                │
│  fct_order_items (central fact table - 600K rows)               │
│  dim_customers (15K rows)                                        │
│  dim_suppliers (1K rows)                                         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                     SEMANTIC LAYER                               │
│  Metrics: total_revenue, order_count, avg_order_value, etc.     │
│  Dimensions: order_date, market_segment, customer_region, etc.  │
└─────────────────────────────────────────────────────────────────┘
```

### Staging Models

Staging models clean and rename raw columns:

**`stg_lineitems.sql`** (key transformations):
```sql
-- Calculate revenue at the line item level
l_extendedprice::numeric * (1 - l_discount::numeric) as line_revenue
```

**`stg_nations.sql`** (joins region):
```sql
-- Denormalize nation with region name
select
    n.n_nationkey as nation_id,
    n.n_name as nation_name,
    r.r_name as region_name
from nation n
left join region r on n.n_regionkey = r.r_regionkey
```

### Mart Models

**`fct_order_items.sql`** — The main fact table joins everything:
- Orders + Line Items (the grain is one line item)
- Customer info (name, market segment)
- Geographic info (nation, region)

Key columns:
- `order_id`, `line_number` — Composite primary key
- `line_revenue` — Revenue after discount
- `fulfillment_days` — Days from order to ship
- `market_segment`, `customer_nation`, `customer_region` — Dimensions

### Semantic Layer Configuration

The semantic layer is defined in `models/marts/_models.yml`:

```yaml
semantic_models:
  - name: order_items
    model: ref('fct_order_items')
    defaults:
      agg_time_dimension: order_date

    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
      - name: market_segment
        type: categorical
      # ... more dimensions

    measures:
      - name: total_revenue
        agg: sum
        expr: line_revenue
      - name: order_count
        agg: count_distinct
        expr: order_id
      # ... more measures

metrics:
  - name: total_revenue
    type: simple
    type_params:
      measure: total_revenue

  - name: avg_order_value
    type: derived
    type_params:
      expr: total_revenue / nullif(order_count, 0)
      metrics:
        - name: total_revenue
        - name: order_count
```

---

## How the Agent Works

### Architecture Deep Dive

```
User Question
     │
     ▼
┌─────────────────────┐
│  introspection.py   │  ← Reads manifest.json & semantic_manifest.json
│  - Models           │     from target/ directory
│  - Metrics          │
│  - Dimensions       │
│  - Lineage graph    │
└─────────────────────┘
     │
     ▼
┌─────────────────────┐
│  query_generator.py │  ← Converts NL to SQL
│  - Pattern matching │     (or calls Claude API)
│  - LLM generation   │
│  - Validation       │
└─────────────────────┘
     │
     ▼
┌─────────────────────┐
│  executor.py        │  ← Runs SQL against PostgreSQL
│  - SELECT only      │     Logs to audit.db
│  - Query validation │
│  - Result formatting│
└─────────────────────┘
     │
     ▼
┌─────────────────────┐
│  lineage.py         │  ← Explains data provenance
│  - Metric → Measure │
│  - Model → Staging  │
│  - Staging → Source │
└─────────────────────┘
```

### 1. Project Introspection (`introspection.py`)

The introspector reads dbt artifacts and builds a context dictionary:

```python
context = introspect_project(".")

# Returns:
{
    "models": [...],           # dbt models with columns
    "sources": [...],          # Raw source tables
    "metrics": [...],          # Semantic layer metrics
    "semantic_models": [...],  # Dimensions, measures, entities
    "lineage": {...},          # Dependency graph
}
```

### 2. Query Generation (`query_generator.py`)

Two modes available:

**Pattern Matching** (default, no API needed):
```python
result = generate_query_simple(question, context)
# Matches patterns like "revenue by market segment"
# Fast, deterministic, but limited flexibility
```

**LLM Generation** (requires API key):
```python
result = generate_query(question, context)
# Sends schema context to Claude
# More flexible, handles complex questions
```

Both return:
```python
{
    "query": "SELECT ...",
    "explanation": "This query aggregates...",
    "metrics_used": ["total_revenue"],
    "tables_referenced": ["fct_order_items"],
    "confidence": "high"
}
```

### 3. Query Execution (`executor.py`)

Safety checks before execution:
- Must start with `SELECT` or `WITH`
- Blocks dangerous keywords: `INSERT`, `UPDATE`, `DELETE`, `DROP`, etc.

```python
result = execute_query(sql)

# Returns:
{
    "rows": [{"market_segment": "BUILDING", "total_revenue": 4281563957.54}, ...],
    "columns": ["market_segment", "total_revenue"],
    "row_count": 5,
    "elapsed_ms": 241
}
```

All queries logged to `audit.db` (SQLite):
```sql
SELECT * FROM query_log ORDER BY timestamp DESC LIMIT 5;
```

### 4. Lineage Explanation (`lineage.py`)

Traces data flow from metrics to raw sources:

```python
explain_lineage("total_revenue", context)

# Output:
"""
## Lineage for metric: total_revenue

**Description:** Total revenue generated (extended_price * (1 - discount))
**Type:** Simple metric
**Measure:** total_revenue

Aggregation `sum(line_revenue)` from `fct_order_items`

### Data Flow
**fct_order_items**
  ← stg_orders ← raw tables: tpch.orders
  ← stg_lineitems ← raw tables: tpch.lineitem
  ← stg_customers ← raw tables: tpch.customer
  ← stg_nations ← raw tables: tpch.nation, tpch.region
"""
```

---

## Using the CLI

### Basic Usage

```bash
# Single question
python agent/cli.py "What is total revenue by market segment?"

# With lineage (default)
python agent/cli.py "Top 10 suppliers by revenue"

# Dry run (SQL only, no execution)
python agent/cli.py --dry-run "Return rate by region"
```

### Interactive Mode

```bash
python agent/cli.py -i

🤖 dbt Semantic Layer Agent
========================================
Ask questions about your data in natural language.
Commands: /context, /lineage <metric>, /quit

❓ What is the average order value?
# ... results ...

❓ /lineage avg_order_value
# ... shows lineage ...

❓ /context
# ... shows all models, metrics, dimensions ...

❓ /quit
Goodbye!
```

### Using Claude LLM

For more flexible query generation:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
python agent/cli.py --llm "Show me the trend of returns over the last 3 years"
```

The LLM mode can handle:
- More complex phrasing
- Questions not covered by pattern matching
- Ambiguous requests (asks clarifying questions)

---

## Understanding Data Lineage

### Why Lineage Matters

When someone asks "What is our total revenue?", they need to trust the answer. Lineage answers:

1. **Where does this number come from?** → `fct_order_items.line_revenue`
2. **How is it calculated?** → `SUM(extended_price * (1 - discount))`
3. **What raw data feeds it?** → `lineitem` and `orders` tables

### Reading Lineage Output

```
## Lineage for metric: avg_order_value

**Type:** Derived metric
**Formula:** `total_revenue / nullif(order_count, 0)`
**Derived from:** total_revenue, order_count

  - total_revenue: Aggregation `sum(line_revenue)` from fct_order_items
  - order_count: Aggregation `count_distinct(order_id)` from fct_order_items

### Data Flow

**fct_order_items**
  ← stg_orders ← Source: tpch.orders (raw table)
  ← stg_lineitems ← Source: tpch.lineitem (raw table)
```

This tells us:
- `avg_order_value` is computed from two other metrics
- Those metrics come from `fct_order_items`
- The fact table joins `stg_orders` and `stg_lineitems`
- Those staging models read from raw TPC-H tables

---

## Extending the Agent

### Adding New Metrics

1. Add measure to `models/marts/_models.yml`:

```yaml
measures:
  - name: total_tax
    agg: sum
    expr: "line_revenue * tax"
```

2. Add metric:

```yaml
metrics:
  - name: total_tax
    type: simple
    type_params:
      measure: total_tax
```

3. Rebuild dbt:
```bash
dbt build
```

4. Add pattern to `query_generator.py` (optional):

```python
if "tax" in question_lower:
    return {
        "query": "SELECT SUM(line_revenue * tax) as total_tax FROM fct_order_items",
        ...
    }
```

### Adding New Dimensions

1. Add to fact table SQL (`fct_order_items.sql`):

```sql
li.ship_mode,
```

2. Add to semantic model:

```yaml
dimensions:
  - name: ship_mode
    type: categorical
    expr: ship_mode
```

3. Rebuild dbt.

### Connecting to Different Warehouses

1. Install the adapter:
```bash
pip install dbt-snowflake  # or dbt-bigquery, dbt-databricks
```

2. Update `profiles.yml`:
```yaml
tpch_agent:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: xyz123
      # ... credentials
```

3. Update `executor.py` connection logic.

### Adding a Web UI (Future)

The agent is designed for easy UI integration:

```python
from agent import introspect_project, generate_query_simple, execute_query

# Streamlit example
import streamlit as st

context = introspect_project(".")
question = st.text_input("Ask a question about your data")

if question:
    result = generate_query_simple(question, context)
    if result["query"]:
        data = execute_query(result["query"])
        st.dataframe(data["rows"])
```

---

## Troubleshooting

### "manifest.json not found"

Run `dbt build` first to generate artifacts in `target/`.

### "Connection refused" to PostgreSQL

Check PostgreSQL is running:
```bash
pg_isready -h localhost -p 5432
```

### Query returns wrong results

1. Check the generated SQL with `--dry-run`
2. Verify metric definitions in `_models.yml`
3. Test the SQL directly in `psql`

### Pattern matching doesn't recognize my question

Either:
1. Add a new pattern to `query_generator.py`
2. Use `--llm` mode for more flexibility

---

## Summary

This project demonstrates:

1. **dbt Semantic Layer** — Defining metrics and dimensions in code
2. **Project Introspection** — Reading dbt artifacts programmatically
3. **NL-to-SQL** — Converting questions to valid, governed SQL
4. **Data Lineage** — Explaining where data comes from

The combination creates a **governed, explainable** natural language interface to your data warehouse — exactly what modern data teams need.
