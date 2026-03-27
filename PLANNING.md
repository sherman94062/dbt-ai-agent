# dbt Semantic Layer Agent — Project Plan

## Project Summary

Build a conversational agent that introspects a dbt project, translates natural language
questions into MetricFlow queries or SQL, executes them against a warehouse, and returns
results with data lineage context. A governed NL-to-SQL layer that understands semantics,
not just schema.

**Primary goals:** Learn dbt Semantic Layer / MetricFlow, multi-step Claude tool-use,
governed NL-to-SQL patterns.

**Portfolio angle:** Demonstrates modern analytics stack knowledge + governed AI on top of
it. Natural Castellan use case (governed agents over data queries). TPC-H is the standard
data engineering benchmark dataset — immediately recognizable to interviewers at Databricks,
dbt Labs, ClickHouse, and similar companies.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│ INTERFACE LAYER                                                      │
│  User / chat UI  ◄──►  Claude (orchestrator)  ──►  Agent memory    │
│                         Tool-use + reasoning        Schema cache     │
└────────────────────────────┬────────────────────────────────────────┘
                             │ agent tools (Claude tool-use)
             ┌───────────────┼───────────────┐
             ▼               ▼               ▼
    ┌─────────────┐  ┌──────────────┐  ┌───────────────┐
    │  Project    │  │   Query      │  │   Lineage     │
    │ introspector│  │  generator   │  │  explainer    │
    │ Read models,│  │ NL → Metric  │  │ Trace column  │
    │ metrics yaml│  │  Flow / SQL  │  │  provenance   │
    └──────┬──────┘  └──────┬───────┘  └───────┬───────┘
           │                │                   │
┌──────────▼────────────────▼───────────────────▼───────────────────┐
│ dbt LAYER                                                           │
│  manifest.json    semantic_manifest.json    Semantic Layer API      │
│  Models/sources   Metrics, dimensions       GraphQL execution       │
└──────────┬────────────────┬───────────────────────────────────────┘
           │                │
┌──────────▼────────────────▼───────────────────────────────────────┐
│ WAREHOUSE / STORAGE                                                 │
│  Postgres (tpch DB)        Schema vector store    dbt artifacts    │
│  Query execution           Semantic search        target/ on disk  │
└───────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Agent orchestration | Claude API (tool-use), Python |
| dbt project | dbt Core (local), targeting Postgres `tpch` DB |
| Semantic layer | dbt Semantic Layer / MetricFlow |
| Warehouse | Postgres (dev), Databricks (stretch) |
| Schema cache | pgvector or ChromaDB (stretch goal) |
| UI | CLI first, Streamlit (Phase 5) |
| MCP integration | Expose agent as MCP tool for Claude Desktop |

---

## Database

**Engine:** PostgreSQL (local)
**Connection:** `postgresql://arthursherman@localhost:5432/tpch`
**Dataset:** TPC-H scale factor 0.1 (~6M rows across 8 tables)
**profiles.yml name:** `tpch_agent`

### Source Tables

| Table | Description | Key Columns |
|---|---|---|
| `orders` | Customer orders | o_orderkey, o_custkey, o_totalprice, o_orderdate, o_orderstatus, o_orderpriority |
| `lineitem` | Order line items | l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate, l_returnflag |
| `customer` | Customers | c_custkey, c_name, c_nationkey, c_mktsegment, c_acctbal |
| `supplier` | Suppliers | s_suppkey, s_name, s_nationkey, s_acctbal |
| `part` | Parts/products | p_partkey, p_name, p_type, p_size, p_retailprice |
| `partsupp` | Part-supplier relationships | ps_partkey, ps_suppkey, ps_supplycost, ps_availqty |
| `nation` | Nations (25 rows) | n_nationkey, n_name, n_regionkey |
| `region` | Regions (5 rows) | r_regionkey, r_name |

### Key Relationships
```
region (1) ──► (N) nation (1) ──► (N) customer (1) ──► (N) orders (1) ──► (N) lineitem
                                  nation (1) ──► (N) supplier (1) ──► (N) lineitem
                                                      part (1) ──► (N) lineitem
```

---

## profiles.yml

Location: `~/.dbt/profiles.yml`

```yaml
tpch_agent:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: arthursherman
      password: ""
      dbname: tpch
      schema: public
      threads: 4
```

---

## dbt Project Structure

```
dbt-ai-agent/
├── CLAUDE.md                        # Claude Code session context
├── PLANNING.md                      # This file
├── load.py                          # TPC-H data loader (already run)
├── venv/                            # Python virtual environment
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── _sources.yml             # Raw TPC-H table declarations
│   │   ├── stg_orders.sql
│   │   ├── stg_lineitems.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_suppliers.sql
│   │   ├── stg_parts.sql
│   │   └── stg_nations.sql
│   └── marts/
│       ├── fct_order_items.sql      # orders joined to lineitem (main fact table)
│       ├── dim_customers.sql        # customer + nation + region
│       └── dim_suppliers.sql        # supplier + nation + region
└── semantic_models/
    └── tpch_semantic.yml            # Metrics, dimensions, entities
```

---

## Semantic Model Design

### Entities
- `order_id` (primary: `o_orderkey`)
- `customer_id` (primary: `c_custkey`)
- `supplier_id` (primary: `s_suppkey`)
- `part_id` (primary: `p_partkey`)

### Dimensions
- `order_date` (time dimension — grain: day, month, quarter, year)
- `ship_date` (time dimension)
- `order_status` (`F` = fulfilled, `O` = open, `P` = pending)
- `order_priority` (1-URGENT, 2-HIGH, 3-MEDIUM, 4-NOT SPECIFIED, 5-LOW)
- `market_segment` (AUTOMOBILE, BUILDING, FURNITURE, MACHINERY, HOUSEHOLD)
- `nation_name` (customer or supplier nation)
- `region_name` (AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST)
- `part_type` (e.g. ECONOMY ANODIZED STEEL)
- `return_flag` (`R` = returned, `A` = accepted, `N` = none)

### Measures / Metrics

| Metric | Formula | Business Question |
|---|---|---|
| `total_revenue` | SUM(l_extendedprice * (1 - l_discount)) | How much revenue did we generate? |
| `order_count` | COUNT(DISTINCT o_orderkey) | How many orders were placed? |
| `avg_order_value` | total_revenue / order_count | What is the average order size? |
| `total_quantity` | SUM(l_quantity) | How many units were shipped? |
| `avg_discount` | AVG(l_discount) | What is the average discount rate? |
| `fulfillment_days` | AVG(l_shipdate - o_orderdate) | How long does fulfillment take? |
| `return_rate` | COUNT(l_returnflag='R') / COUNT(*) | What percentage of items are returned? |

---

## Phase Plan

### Phase 1 — dbt Project Foundation (Days 1–3)

**Goal:** Working dbt project against `tpch` Postgres with semantic models defined.

**Tasks:**
1. `pip install dbt-core dbt-postgres` in venv
2. `dbt init tpch_agent` and configure `profiles.yml` (see above)
3. Define `_sources.yml` pointing at the 8 raw TPC-H tables in `public` schema
4. Create staging models (light renaming + type casting only):
   - `stg_orders.sql` — cast dates, rename o_ prefixed columns
   - `stg_lineitems.sql` — cast dates, compute `line_revenue = l_extendedprice * (1 - l_discount)`
   - `stg_customers.sql`
   - `stg_suppliers.sql`
   - `stg_parts.sql`
   - `stg_nations.sql`
5. Create mart models:
   - `fct_order_items.sql` — join stg_orders + stg_lineitems (main fact table)
   - `dim_customers.sql` — join stg_customers + stg_nations + region
   - `dim_suppliers.sql` — join stg_suppliers + stg_nations + region
6. Define semantic models in `tpch_semantic.yml` (entities, dimensions, measures per above)
7. `dbt parse` → confirm `target/manifest.json` and `target/semantic_manifest.json` exist
8. `dbt build` → confirm all models materialize cleanly

**Deliverable:** `dbt build` runs clean; `target/semantic_manifest.json` has all 7 metrics defined.

---

### Phase 2 — Project Introspection Tool (Days 4–5)

**Goal:** Claude tool that reads dbt artifacts and returns structured project context.

**Tool signature:**
```python
def introspect_project(project_path: str) -> dict:
    """
    Reads manifest.json and semantic_manifest.json from target/.
    Returns: models, sources, metrics, dimensions, entities,
             column descriptions, lineage graph.
    """
```

**Implementation notes:**
- Parse `manifest.json` → extract `nodes` (models), `sources`, column descriptions
- Parse `semantic_manifest.json` → extract `semantic_models`, `metrics`, `saved_queries`
- Build simplified lineage dict: `{model_name: [upstream_deps]}`
- Return structured dict that Claude can reason over in subsequent tool calls

**Key learning:** dbt manifest JSON schema, node graph structure, ref() resolution.

**Deliverable:** Tool returns clean structured context; Claude can answer "what models exist?"
and "what does the total_revenue metric measure and how is it calculated?"

---

### Phase 3 — Query Generation Tool (Days 6–8)

**Goal:** Claude tool that takes a NL question + semantic context and produces executable SQL.

**Tool signature:**
```python
def generate_query(
    question: str,
    semantic_context: dict,   # output of introspect_project()
    mode: str = "sql"         # "sql" | "metricflow"
) -> dict:
    """
    Returns: {
        "query": str,
        "explanation": str,
        "metrics_used": list,
        "tables_referenced": list,
        "confidence": str       # "high" | "medium" | "low"
    }
    """
```

**Example questions this must handle:**
- "What is total revenue by market segment?"
- "Which region has the highest average order value?"
- "What are the top 10 suppliers by revenue?"
- "How has fulfillment time trended by month?"

**Hallucination prevention:**
- Ground every generated query against actual column names from manifest
- Validate all table references exist in `nodes` or `sources`
- Reject/retry if generated SQL references columns not in manifest
- SELECT only — reject any DDL/DML

**Key learning:** Structured tool-use, grounded code generation, SQL generation prompt
engineering.

**Deliverable:** All 5 acceptance test questions produce valid, executable SQL.

---

### Phase 4 — Execution + Lineage Tool (Days 9–11)

**Goal:** Execute queries and return results with data provenance explanation.

**Two tools:**

```python
def execute_query(sql: str) -> dict:
    """
    Executes validated SQL against tpch Postgres.
    Returns: {"rows": list, "columns": list, "row_count": int, "elapsed_ms": int}
    """

def explain_lineage(metric_name: str, manifest: dict) -> str:
    """
    Traces metric → semantic model → dbt mart → staging models → raw source tables.
    Returns human-readable provenance paragraph.
    Example: "total_revenue is calculated from fct_order_items, which joins stg_orders
    and stg_lineitems, sourced from the raw TPC-H orders and lineitem tables."
    """
```

**Full answer assembly:**
```
User: "What is total revenue by market segment?"
Agent: [introspect_project()]
    → [generate_query(question, context)]
    → [execute_query(sql)]
    → [explain_lineage("total_revenue", manifest)]
Response: Result table + provenance paragraph
```

**Key learning:** Multi-tool chaining, result synthesis, recursive graph traversal.

**Deliverable:** Full end-to-end answer with result + provenance for all 5 test questions.

---

### Phase 5 — Polish + Demo (Days 12–14)

**Goal:** Deployable demo with clean interface and README.

**Tasks:**
1. Streamlit UI: text input → result table + lineage paragraph
2. Cache manifest on startup (re-parse only when `target/` mtime changes)
3. Graceful ambiguity handling: agent asks clarifying questions when vague
4. Query audit log: every NL question, generated SQL, row count, elapsed_ms → SQLite
5. `README.md` framing: "governed NL-to-SQL over a dbt Semantic Layer"
6. (Stretch) Expose as MCP tool for Claude Desktop
7. (Stretch) Databricks target: reuse existing Databricks MCP server connection
8. Push to GitHub under `sherman94062`

---

## Test Questions (Acceptance Criteria)

The agent must answer all 5 with valid SQL, correct results, and a lineage explanation:

1. "What is total revenue by market segment?"
2. "Which are the top 10 suppliers by total revenue?"
3. "How has average order value trended month over month?"
4. "What is the return rate by region?"
5. "Which nation has the longest average fulfillment time?"

---

## Stretch Goals (Post-MVP)

- **MetricFlow mode:** Replace SQL generation with `mf query` CLI calls
- **Databricks target:** Switch warehouse target; reuse existing Databricks MCP server
- **Vector search:** Embed model/column descriptions; semantic search to find relevant
  models before generating SQL (reduces context window pressure)
- **MCP server:** Wrap agent as MCP tool callable from Claude Desktop
- **Castellan integration:** Feed query audit log into Castellan governance layer as a
  demo of governed agent activity over data infrastructure

---

## Claude Code Kickoff Prompt

Paste this at the start of each Claude Code session:

```
I'm building a dbt Semantic Layer Agent — a conversational agent that reads a dbt project,
generates SQL from natural language questions, and returns results with data lineage.

Project plan is in PLANNING.md. Database context is in CLAUDE.md.
Current phase: [PHASE NUMBER AND NAME]

Tech stack: Python (venv), dbt Core + dbt-postgres, Claude API (tool-use).
Database: PostgreSQL tpch (TPC-H sf=0.1) at localhost:5432, user arthursherman.
Tables: orders, lineitem, customer, supplier, part, partsupp, nation, region.

Today's task: [DESCRIBE SPECIFIC TASK]

Constraints:
- SQL generation must be grounded against manifest.json (no hallucinated column names)
- All tools must have typed signatures and docstrings
- One responsibility per tool function
- Log all generated queries to SQLite audit table from Phase 4 onward
- SELECT only — no DDL/DML ever reaches the database
```
