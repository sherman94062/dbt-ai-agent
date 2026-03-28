"""
dbt Semantic Layer Agent — Streamlit UI

A visual interface for asking natural language questions against your dbt semantic layer.
"""

import streamlit as st
import pandas as pd
import time
from pathlib import Path
import sys

# Add agent module to path
sys.path.insert(0, str(Path(__file__).parent / "agent"))

from introspection import introspect_project, format_context_for_llm
from query_generator import generate_query_simple, generate_query
from executor import execute_query
from lineage import explain_query_lineage, explain_lineage

# Page config
st.set_page_config(
    page_title="dbt Semantic Layer Agent",
    page_icon="🔮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #666;
        margin-top: 0;
    }
    .metric-card {
        background: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        margin: 5px 0;
    }
    .sql-box {
        background: #1e1e1e;
        border-radius: 8px;
        padding: 15px;
        font-family: 'Monaco', 'Menlo', monospace;
        font-size: 13px;
        overflow-x: auto;
    }
    .connection-info {
        background: #e8f4ea;
        border-left: 4px solid #28a745;
        padding: 10px 15px;
        border-radius: 0 8px 8px 0;
        margin: 10px 0;
    }
    .stats-row {
        display: flex;
        gap: 20px;
        margin: 10px 0;
    }
    .stDataFrame {
        border: 1px solid #ddd;
        border-radius: 8px;
    }
    .raw-table {
        background: #fff3cd;
        border: 2px solid #ffc107;
        border-radius: 8px;
        padding: 10px;
        margin: 5px;
    }
    .staging-view {
        background: #cce5ff;
        border: 2px solid #007bff;
        border-radius: 8px;
        padding: 10px;
        margin: 5px;
    }
    .mart-table {
        background: #d4edda;
        border: 2px solid #28a745;
        border-radius: 8px;
        padding: 10px;
        margin: 5px;
    }
    .layer-header {
        font-weight: bold;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def load_context():
    """Load and cache dbt project context."""
    return introspect_project(".")


@st.cache_data
def get_connection_info():
    """Get database connection details."""
    import os
    conn_str = os.environ.get(
        "DATABASE_URL",
        "postgresql://arthursherman@localhost:5432/tpch"
    )
    # Parse connection string
    parts = conn_str.replace("postgresql://", "").split("@")
    user = parts[0].split(":")[0] if parts else "unknown"
    host_db = parts[1] if len(parts) > 1 else "localhost/unknown"
    host = host_db.split("/")[0]
    db = host_db.split("/")[1] if "/" in host_db else "unknown"

    return {
        "connection_string": conn_str,
        "user": user,
        "host": host,
        "database": db,
    }


@st.cache_data
def get_table_counts():
    """Get row counts for all tables."""
    counts = {}
    tables = [
        # Raw tables
        ("region", "raw"), ("nation", "raw"), ("customer", "raw"),
        ("supplier", "raw"), ("part", "raw"), ("partsupp", "raw"),
        ("orders", "raw"), ("lineitem", "raw"),
        # dbt models
        ("stg_orders", "staging"), ("stg_lineitems", "staging"),
        ("stg_customers", "staging"), ("stg_suppliers", "staging"),
        ("stg_parts", "staging"), ("stg_nations", "staging"),
        ("fct_order_items", "mart"), ("dim_customers", "mart"),
        ("dim_suppliers", "mart"),
    ]

    for table, layer in tables:
        try:
            result = execute_query(f"SELECT COUNT(*) as cnt FROM {table}")
            counts[table] = {"count": result["rows"][0]["cnt"], "layer": layer}
        except:
            counts[table] = {"count": "N/A", "layer": layer}

    return counts


def run_query(question: str, context: dict, use_llm: bool = False):
    """Generate and execute query, returning all results."""
    start_time = time.time()

    # Generate SQL
    gen_start = time.time()
    if use_llm:
        try:
            result = generate_query(question, context)
        except ImportError:
            st.warning("Anthropic package not available. Using pattern matching.")
            result = generate_query_simple(question, context)
    else:
        result = generate_query_simple(question, context)
    gen_time = (time.time() - gen_start) * 1000

    if not result.get("query"):
        return {
            "success": False,
            "error": result.get("error", "Could not generate query"),
            "generation_time_ms": gen_time,
        }

    # Execute SQL
    exec_start = time.time()
    try:
        exec_result = execute_query(result["query"])
        exec_time = exec_result.get("elapsed_ms", 0)

        return {
            "success": True,
            "query": result["query"],
            "explanation": result.get("explanation", ""),
            "confidence": result.get("confidence", "unknown"),
            "metrics_used": result.get("metrics_used", []),
            "tables_referenced": result.get("tables_referenced", []),
            "rows": exec_result["rows"],
            "columns": exec_result["columns"],
            "row_count": exec_result["row_count"],
            "generation_time_ms": gen_time,
            "execution_time_ms": exec_time,
            "total_time_ms": (time.time() - start_time) * 1000,
        }
    except Exception as e:
        return {
            "success": False,
            "query": result["query"],
            "error": str(e),
            "generation_time_ms": gen_time,
            "execution_time_ms": 0,
        }


def render_query_tab(context: dict, conn_info: dict):
    """Render the main query interface tab."""
    # Settings from session state
    use_llm = st.session_state.get("use_llm", False)
    show_lineage = st.session_state.get("show_lineage", True)

    # Query input
    col1, col2 = st.columns([5, 1])
    with col1:
        question = st.text_input(
            "Ask a question",
            placeholder="e.g., What is total revenue by market segment?",
            label_visibility="collapsed"
        )
    with col2:
        run_button = st.button("🚀 Run", type="primary", use_container_width=True)

    # Example questions
    st.markdown("**Try these:**")
    example_cols = st.columns(3)
    examples = [
        "What is total revenue by market segment?",
        "Top 10 suppliers by revenue",
        "Return rate by region"
    ]

    for i, example in enumerate(examples):
        with example_cols[i]:
            if st.button(example, key=f"example_{i}", use_container_width=True):
                question = example
                run_button = True

    st.markdown("---")

    # Process query
    if question and run_button:
        with st.spinner("🔍 Generating and executing query..."):
            result = run_query(question, context, use_llm)

        if result["success"]:
            # Stats row
            stat_cols = st.columns(4)
            with stat_cols[0]:
                st.metric("Rows Returned", f"{result['row_count']:,}")
            with stat_cols[1]:
                st.metric("Query Time", f"{result['execution_time_ms']:.0f}ms")
            with stat_cols[2]:
                st.metric("Generation Time", f"{result['generation_time_ms']:.0f}ms")
            with stat_cols[3]:
                confidence_emoji = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(result['confidence'], "⚪")
                st.metric("Confidence", f"{confidence_emoji} {result['confidence'].title()}")

            # Two column layout
            col_left, col_right = st.columns([3, 2])

            with col_left:
                # Results
                st.markdown("### 📋 Results")
                if result["rows"]:
                    df = pd.DataFrame(result["rows"])
                    st.dataframe(df, use_container_width=True, height=400)

                    # Download button
                    csv = df.to_csv(index=False)
                    st.download_button(
                        "📥 Download CSV",
                        csv,
                        "query_results.csv",
                        "text/csv",
                        use_container_width=True
                    )
                else:
                    st.info("No results returned.")

            with col_right:
                # Generated SQL
                st.markdown("### 🔧 Generated SQL")
                st.code(result["query"], language="sql")

                if result.get("explanation"):
                    st.info(f"💡 {result['explanation']}")

                # Tables & metrics used
                if result.get("tables_referenced"):
                    st.markdown("**Tables used:**")
                    for table in result["tables_referenced"]:
                        st.markdown(f"- `{table}`")

                if result.get("metrics_used"):
                    st.markdown("**Metrics used:**")
                    for metric in result["metrics_used"]:
                        st.markdown(f"- `{metric}`")

            # Lineage section
            if show_lineage and result.get("tables_referenced"):
                st.markdown("---")
                st.markdown("### 🔗 Data Lineage")
                lineage_text = explain_query_lineage(result["tables_referenced"], context)
                st.markdown(lineage_text)

        else:
            # Error display
            st.error(f"❌ {result.get('error', 'Unknown error')}")
            if result.get("query"):
                st.markdown("**Generated SQL (failed to execute):**")
                st.code(result["query"], language="sql")


def render_schema_tab(context: dict):
    """Render the database schema visualization tab."""

    st.markdown("### 🗄️ TPC-H Database Schema")
    st.markdown("Visual representation of raw tables and dbt transformations")

    # Get table counts
    with st.spinner("Loading table statistics..."):
        counts = get_table_counts()

    # Legend
    st.markdown("""
    **Legend:**
    🟡 Raw Tables (TPC-H) → 🔵 Staging Views (dbt) → 🟢 Mart Tables (dbt)
    """)

    st.markdown("---")

    # Data Flow Diagram using Graphviz
    st.markdown("### 📊 Data Flow Diagram")

    graphviz_diagram = """
    digraph G {
        rankdir=TB;
        node [shape=box, style="rounded,filled", fontname="Helvetica"];
        edge [color="#666666"];

        // Raw tables cluster
        subgraph cluster_raw {
            label="🟡 Raw Tables (TPC-H Source)";
            style=filled;
            fillcolor="#fff3cd";
            color="#ffc107";

            region [label="region\\n5 rows", fillcolor="#ffecb3"];
            nation [label="nation\\n25 rows", fillcolor="#ffecb3"];
            customer [label="customer\\n15K rows", fillcolor="#ffecb3"];
            supplier [label="supplier\\n1K rows", fillcolor="#ffecb3"];
            part [label="part\\n20K rows", fillcolor="#ffecb3"];
            partsupp [label="partsupp\\n80K rows", fillcolor="#ffecb3"];
            orders [label="orders\\n150K rows", fillcolor="#ffecb3"];
            lineitem [label="lineitem\\n600K rows", fillcolor="#ffecb3"];
        }

        // Staging views cluster
        subgraph cluster_staging {
            label="🔵 Staging Views (dbt)";
            style=filled;
            fillcolor="#cce5ff";
            color="#007bff";

            stg_nations [label="stg_nations", fillcolor="#b3d9ff"];
            stg_customers [label="stg_customers", fillcolor="#b3d9ff"];
            stg_suppliers [label="stg_suppliers", fillcolor="#b3d9ff"];
            stg_parts [label="stg_parts", fillcolor="#b3d9ff"];
            stg_orders [label="stg_orders", fillcolor="#b3d9ff"];
            stg_lineitems [label="stg_lineitems", fillcolor="#b3d9ff"];
        }

        // Mart tables cluster
        subgraph cluster_marts {
            label="🟢 Mart Tables (dbt)";
            style=filled;
            fillcolor="#d4edda";
            color="#28a745";

            dim_customers [label="dim_customers\\n15K rows", fillcolor="#b8e6c1"];
            dim_suppliers [label="dim_suppliers\\n1K rows", fillcolor="#b8e6c1"];
            fct_order_items [label="fct_order_items\\n600K rows", fillcolor="#b8e6c1"];
        }

        // Semantic layer cluster
        subgraph cluster_semantic {
            label="🔮 Semantic Layer";
            style=filled;
            fillcolor="#e2d5f1";
            color="#6f42c1";

            metrics [label="Metrics:\\ntotal_revenue\\norder_count\\navg_order_value\\nreturn_rate", fillcolor="#d4c4e8"];
        }

        // Raw to Staging edges
        region -> stg_nations;
        nation -> stg_nations;
        customer -> stg_customers;
        supplier -> stg_suppliers;
        part -> stg_parts;
        orders -> stg_orders;
        lineitem -> stg_lineitems;

        // Staging to Marts edges
        stg_nations -> dim_customers;
        stg_customers -> dim_customers;
        stg_nations -> dim_suppliers;
        stg_suppliers -> dim_suppliers;
        stg_orders -> fct_order_items;
        stg_lineitems -> fct_order_items;
        stg_customers -> fct_order_items;
        stg_nations -> fct_order_items;

        // Marts to Semantic edges
        fct_order_items -> metrics;
        dim_customers -> metrics;
        dim_suppliers -> metrics;
    }
    """

    st.graphviz_chart(graphviz_diagram, use_container_width=True)

    st.markdown("---")

    # Detailed table view
    st.markdown("### 📋 Table Details")

    # Three columns for the three layers
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown('<div class="layer-header" style="background: #fff3cd;">🟡 Raw Tables (TPC-H)</div>', unsafe_allow_html=True)

        raw_tables = [
            ("region", "Geographic regions", "r_regionkey, r_name"),
            ("nation", "Countries", "n_nationkey, n_name, n_regionkey"),
            ("customer", "Customers", "c_custkey, c_name, c_nationkey, c_mktsegment"),
            ("supplier", "Suppliers", "s_suppkey, s_name, s_nationkey"),
            ("part", "Products", "p_partkey, p_name, p_type, p_retailprice"),
            ("partsupp", "Part-Supplier links", "ps_partkey, ps_suppkey, ps_supplycost"),
            ("orders", "Orders", "o_orderkey, o_custkey, o_orderdate, o_totalprice"),
            ("lineitem", "Order lines", "l_orderkey, l_partkey, l_quantity, l_extendedprice"),
        ]

        for table, desc, cols in raw_tables:
            count = counts.get(table, {}).get("count", "N/A")
            with st.expander(f"**{table}** ({count:,} rows)" if isinstance(count, int) else f"**{table}**"):
                st.markdown(f"*{desc}*")
                st.markdown(f"**Key columns:** `{cols}`")

    with col2:
        st.markdown('<div class="layer-header" style="background: #cce5ff;">🔵 Staging Views (dbt)</div>', unsafe_allow_html=True)

        staging_views = [
            ("stg_nations", "nation + region joined", "nation_id, nation_name, region_name"),
            ("stg_customers", "Cleaned customers", "customer_id, customer_name, market_segment"),
            ("stg_suppliers", "Cleaned suppliers", "supplier_id, supplier_name, nation_id"),
            ("stg_parts", "Cleaned parts", "part_id, part_name, part_type, retail_price"),
            ("stg_orders", "Cleaned orders", "order_id, customer_id, order_date, total_price"),
            ("stg_lineitems", "Lines + revenue calc", "order_id, quantity, line_revenue"),
        ]

        for view, desc, cols in staging_views:
            count = counts.get(view, {}).get("count", "N/A")
            with st.expander(f"**{view}** ({count:,} rows)" if isinstance(count, int) else f"**{view}**"):
                st.markdown(f"*{desc}*")
                st.markdown(f"**Key columns:** `{cols}`")

                # Show transformation
                if view == "stg_lineitems":
                    st.code("line_revenue = extended_price * (1 - discount)", language="sql")
                elif view == "stg_nations":
                    st.code("JOIN region ON nation.n_regionkey = region.r_regionkey", language="sql")

    with col3:
        st.markdown('<div class="layer-header" style="background: #d4edda;">🟢 Mart Tables (dbt)</div>', unsafe_allow_html=True)

        mart_tables = [
            ("dim_customers", "Customer dimension", "customer_id, customer_name, nation_name, region_name, market_segment"),
            ("dim_suppliers", "Supplier dimension", "supplier_id, supplier_name, nation_name, region_name"),
            ("fct_order_items", "Fact table", "order_id, line_revenue, fulfillment_days, market_segment, customer_region"),
        ]

        for table, desc, cols in mart_tables:
            count = counts.get(table, {}).get("count", "N/A")
            with st.expander(f"**{table}** ({count:,} rows)" if isinstance(count, int) else f"**{table}**"):
                st.markdown(f"*{desc}*")
                st.markdown(f"**Key columns:** `{cols}`")

        st.markdown("---")
        st.markdown('<div class="layer-header" style="background: #e2d5f1;">🔮 Semantic Layer</div>', unsafe_allow_html=True)

        with st.expander("**Metrics**"):
            for metric in context['metrics']:
                st.markdown(f"• **{metric['name']}**: {metric.get('description', '')}")

        with st.expander("**Dimensions**"):
            for sm in context['semantic_models']:
                for dim in sm['dimensions']:
                    st.markdown(f"• **{dim['name']}** ({dim['type']})")

    st.markdown("---")

    # Relationships diagram
    st.markdown("### 🔗 Table Relationships (TPC-H)")

    st.code("""
    region (1) ──────► (N) nation (1) ──────► (N) customer (1) ──────► (N) orders (1) ──────► (N) lineitem
                              │                                                                    ▲
                              │                                                                    │
                              └──────► (N) supplier (1) ──────────────────────────────────────────┘
                                              │                                                    ▲
                                              │                                                    │
                                              └──────► (N) partsupp (N) ◄────── (1) part ─────────┘
    """, language="text")


def render_metrics_tab(context: dict):
    """Render the metrics explorer tab."""

    st.markdown("### 📈 Semantic Layer Metrics")
    st.markdown("Explore all available metrics and their definitions")

    # Metrics table
    metrics_data = []
    for metric in context['metrics']:
        metrics_data.append({
            "Metric": metric['name'],
            "Type": metric.get('type', 'simple'),
            "Description": metric.get('description', ''),
            "Measure/Formula": metric.get('measure', metric.get('expression', '')),
        })

    df = pd.DataFrame(metrics_data)
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.markdown("---")

    # Metric lineage explorer
    st.markdown("### 🔍 Metric Lineage Explorer")

    metric_names = [m['name'] for m in context['metrics']]
    selected_metric = st.selectbox("Select a metric to trace:", metric_names)

    if selected_metric:
        lineage = explain_lineage(selected_metric, context)
        st.markdown(lineage)


def main():
    # Load context
    try:
        context = load_context()
    except FileNotFoundError:
        st.error("❌ dbt artifacts not found. Run `dbt build` first.")
        st.stop()

    conn_info = get_connection_info()

    # Sidebar
    with st.sidebar:
        st.markdown("### 🔌 Database Connection")
        st.markdown(f"""
        <div class="connection-info">
            <strong>Host:</strong> {conn_info['host']}<br>
            <strong>Database:</strong> {conn_info['database']}<br>
            <strong>User:</strong> {conn_info['user']}
        </div>
        """, unsafe_allow_html=True)

        st.markdown("---")

        st.markdown("### 📊 Semantic Layer")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Models", len(context['models']))
            st.metric("Metrics", len(context['metrics']))
        with col2:
            st.metric("Sources", len(context['sources']))
            st.metric("Dimensions", sum(len(sm['dimensions']) for sm in context['semantic_models']))

        st.markdown("---")

        # Metrics list
        with st.expander("📈 Available Metrics", expanded=False):
            for metric in context['metrics']:
                st.markdown(f"**{metric['name']}**")
                st.caption(metric.get('description', 'No description'))

        # Dimensions list
        with st.expander("🏷️ Available Dimensions", expanded=False):
            for sm in context['semantic_models']:
                for dim in sm['dimensions']:
                    st.markdown(f"**{dim['name']}** ({dim['type']})")

        st.markdown("---")

        # Settings
        st.markdown("### ⚙️ Settings")
        st.session_state["use_llm"] = st.checkbox(
            "Use Claude LLM", value=False,
            help="Use Claude API for more flexible query generation"
        )
        st.session_state["show_lineage"] = st.checkbox("Show Data Lineage", value=True)

    # Main content - Header
    st.markdown('<p class="main-header">🔮 dbt Semantic Layer Agent</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Ask questions about your data in natural language</p>', unsafe_allow_html=True)

    # Tabs
    tab1, tab2, tab3 = st.tabs(["💬 Query", "🗄️ Schema", "📈 Metrics"])

    with tab1:
        render_query_tab(context, conn_info)

    with tab2:
        render_schema_tab(context)

    with tab3:
        render_metrics_tab(context)

    # Footer
    st.markdown("---")
    st.markdown(
        "<center><small>Built with dbt + Claude | "
        "<a href='https://github.com/sherman94062/dbt-ai-agent'>GitHub</a></small></center>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
