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
        use_llm = st.checkbox("Use Claude LLM", value=False,
                              help="Use Claude API for more flexible query generation")
        show_lineage = st.checkbox("Show Data Lineage", value=True)

    # Main content
    st.markdown('<p class="main-header">🔮 dbt Semantic Layer Agent</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Ask questions about your data in natural language</p>', unsafe_allow_html=True)

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

    # Footer
    st.markdown("---")
    st.markdown(
        "<center><small>Built with dbt + Claude | "
        "<a href='https://github.com/sherman94062/dbt-ai-agent'>GitHub</a></small></center>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
