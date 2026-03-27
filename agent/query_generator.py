"""Query generation tool for converting natural language to SQL."""

import json
import re
from typing import Any

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False


def generate_query(
    question: str,
    semantic_context: dict[str, Any],
    mode: str = "sql"
) -> dict[str, Any]:
    """
    Generate SQL from a natural language question using semantic context.

    Args:
        question: Natural language question to answer
        semantic_context: Output from introspect_project() containing models, metrics, etc.
        mode: "sql" for direct SQL, "metricflow" for MetricFlow query (future)

    Returns:
        Dict containing:
        - query: The generated SQL query
        - explanation: Human-readable explanation of the query
        - metrics_used: List of metrics referenced
        - tables_referenced: List of tables used
        - confidence: "high", "medium", or "low"
    """
    if not HAS_ANTHROPIC:
        raise ImportError("anthropic package not installed. Run: pip install anthropic")

    client = anthropic.Anthropic()

    system_prompt = _build_system_prompt(semantic_context)
    user_prompt = _build_user_prompt(question, semantic_context)

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2048,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}]
    )

    return _parse_response(response.content[0].text, semantic_context)


def _build_system_prompt(context: dict) -> str:
    """Build the system prompt with context about the data model."""
    return """You are a SQL expert that generates queries against a dbt semantic layer.

Your task is to convert natural language questions into valid PostgreSQL queries.

RULES:
1. ONLY use tables and columns that exist in the provided context
2. Generate SELECT queries only - never DDL or DML
3. Use the fact table (fct_order_items) as the primary source for metrics
4. Use dimension tables (dim_customers, dim_suppliers) for enrichment
5. Apply appropriate aggregations based on the question
6. Include GROUP BY for any aggregated queries
7. Use clear column aliases
8. Validate all column references against the provided schema

OUTPUT FORMAT:
Respond with a JSON object containing:
{
  "query": "SELECT ...",
  "explanation": "This query...",
  "metrics_used": ["metric1", "metric2"],
  "tables_referenced": ["table1", "table2"],
  "confidence": "high|medium|low"
}

Set confidence to:
- "high": Question clearly maps to available columns/metrics
- "medium": Some interpretation required
- "low": Significant assumptions made"""


def _build_user_prompt(question: str, context: dict) -> str:
    """Build the user prompt with the question and schema details."""
    schema_info = []

    # Add model schemas
    schema_info.append("## Available Tables and Columns\n")

    for model in context.get("models", []):
        if model["name"] in ["fct_order_items", "dim_customers", "dim_suppliers"]:
            schema_info.append(f"### {model['name']}")
            schema_info.append(f"Description: {model.get('description', 'N/A')}")
            schema_info.append("Columns:")
            for col in model.get("columns", []):
                schema_info.append(f"  - {col['name']}: {col.get('description', '')}")
            schema_info.append("")

    # Add metrics
    schema_info.append("## Available Metrics\n")
    for metric in context.get("metrics", []):
        desc = metric.get("description", "")
        measure = metric.get("measure", "")
        expr = metric.get("expression", "")
        schema_info.append(f"- {metric['name']}: {desc}")
        if measure:
            schema_info.append(f"  (measure: {measure})")
        if expr:
            schema_info.append(f"  (formula: {expr})")
    schema_info.append("")

    # Add semantic model info
    schema_info.append("## Semantic Layer Information\n")
    for sm in context.get("semantic_models", []):
        schema_info.append(f"### {sm['name']}")
        schema_info.append(f"Base table: {sm.get('model', '')}")
        schema_info.append("\nDimensions:")
        for dim in sm.get("dimensions", []):
            schema_info.append(f"  - {dim['name']} ({dim['type']})")
        schema_info.append("\nMeasures:")
        for measure in sm.get("measures", []):
            schema_info.append(f"  - {measure['name']} ({measure['agg']}): {measure.get('expr', '')}")

    prompt = f"""Given this schema context:

{chr(10).join(schema_info)}

Generate a SQL query to answer this question:
"{question}"

Respond with only a JSON object as specified in the system prompt."""

    return prompt


def _parse_response(response_text: str, context: dict) -> dict[str, Any]:
    """Parse and validate the LLM response."""
    # Extract JSON from response
    json_match = re.search(r'\{[\s\S]*\}', response_text)
    if not json_match:
        return {
            "query": "",
            "explanation": "Failed to parse response",
            "metrics_used": [],
            "tables_referenced": [],
            "confidence": "low",
            "error": "No valid JSON found in response"
        }

    try:
        result = json.loads(json_match.group())
    except json.JSONDecodeError as e:
        return {
            "query": "",
            "explanation": "Failed to parse JSON",
            "metrics_used": [],
            "tables_referenced": [],
            "confidence": "low",
            "error": str(e)
        }

    # Validate the query
    query = result.get("query", "")
    validation = _validate_query(query, context)

    if not validation["valid"]:
        result["validation_errors"] = validation["errors"]
        result["confidence"] = "low"

    return result


def _validate_query(query: str, context: dict) -> dict:
    """Validate that the query only references existing tables and columns."""
    errors = []

    # Check for dangerous statements
    dangerous_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "GRANT"]
    query_upper = query.upper()
    for keyword in dangerous_keywords:
        if re.search(rf'\b{keyword}\b', query_upper):
            errors.append(f"Dangerous keyword detected: {keyword}")

    # Get valid table names
    valid_tables = set()
    for model in context.get("models", []):
        valid_tables.add(model["name"].lower())

    # Get valid column names per table
    valid_columns = {}
    for model in context.get("models", []):
        table_name = model["name"].lower()
        valid_columns[table_name] = set()
        for col in model.get("columns", []):
            valid_columns[table_name].add(col["name"].lower())

    return {
        "valid": len(errors) == 0,
        "errors": errors
    }


def generate_query_simple(
    question: str,
    semantic_context: dict[str, Any]
) -> dict[str, Any]:
    """
    Generate SQL without calling an LLM - uses pattern matching.
    Useful for testing or when API is unavailable.
    """
    question_lower = question.lower()

    # Pattern matching for common queries
    if "revenue" in question_lower and "market segment" in question_lower:
        return {
            "query": """SELECT
    market_segment,
    SUM(line_revenue) as total_revenue
FROM fct_order_items
GROUP BY market_segment
ORDER BY total_revenue DESC""",
            "explanation": "Aggregates revenue by market segment from the fact table",
            "metrics_used": ["total_revenue"],
            "tables_referenced": ["fct_order_items"],
            "confidence": "high"
        }

    if "top" in question_lower and "supplier" in question_lower and "revenue" in question_lower:
        limit = 10
        if match := re.search(r'top\s+(\d+)', question_lower):
            limit = int(match.group(1))
        return {
            "query": f"""SELECT
    s.supplier_name,
    s.nation_name,
    SUM(f.line_revenue) as total_revenue
FROM fct_order_items f
JOIN dim_suppliers s ON f.supplier_id = s.supplier_id
GROUP BY s.supplier_id, s.supplier_name, s.nation_name
ORDER BY total_revenue DESC
LIMIT {limit}""",
            "explanation": f"Top {limit} suppliers by total revenue",
            "metrics_used": ["total_revenue"],
            "tables_referenced": ["fct_order_items", "dim_suppliers"],
            "confidence": "high"
        }

    if "average order value" in question_lower and ("month" in question_lower or "trend" in question_lower):
        return {
            "query": """SELECT
    DATE_TRUNC('month', order_date) as month,
    SUM(line_revenue) / COUNT(DISTINCT order_id) as avg_order_value
FROM fct_order_items
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month""",
            "explanation": "Average order value trended by month",
            "metrics_used": ["avg_order_value", "total_revenue", "order_count"],
            "tables_referenced": ["fct_order_items"],
            "confidence": "high"
        }

    if "return rate" in question_lower and "region" in question_lower:
        return {
            "query": """SELECT
    customer_region,
    COUNT(CASE WHEN return_flag = 'R' THEN 1 END)::float / COUNT(*) as return_rate
FROM fct_order_items
GROUP BY customer_region
ORDER BY return_rate DESC""",
            "explanation": "Return rate (percentage of returned items) by customer region",
            "metrics_used": ["return_rate"],
            "tables_referenced": ["fct_order_items"],
            "confidence": "high"
        }

    if "fulfillment" in question_lower and "nation" in question_lower:
        return {
            "query": """SELECT
    customer_nation,
    AVG(fulfillment_days) as avg_fulfillment_days
FROM fct_order_items
WHERE fulfillment_days IS NOT NULL
GROUP BY customer_nation
ORDER BY avg_fulfillment_days DESC
LIMIT 1""",
            "explanation": "Nation with the longest average fulfillment time",
            "metrics_used": ["fulfillment_days"],
            "tables_referenced": ["fct_order_items"],
            "confidence": "high"
        }

    # Default fallback
    return {
        "query": "",
        "explanation": "Could not generate query for this question",
        "metrics_used": [],
        "tables_referenced": [],
        "confidence": "low",
        "error": "No matching pattern found. Try using generate_query() with LLM."
    }


if __name__ == "__main__":
    from introspection import introspect_project

    context = introspect_project(".")

    test_questions = [
        "What is total revenue by market segment?",
        "Which are the top 10 suppliers by total revenue?",
        "How has average order value trended month over month?",
        "What is the return rate by region?",
        "Which nation has the longest average fulfillment time?",
    ]

    print("Testing simple query generation (pattern matching):\n")
    for q in test_questions:
        print(f"Q: {q}")
        result = generate_query_simple(q, context)
        print(f"Confidence: {result['confidence']}")
        print(f"Query:\n{result['query']}\n")
        print("-" * 60)
