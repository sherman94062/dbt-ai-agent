"""Lineage explanation tool for tracing data provenance."""

from typing import Any


def explain_lineage(metric_name: str, context: dict[str, Any]) -> str:
    """
    Trace metric lineage from semantic model to raw source tables.

    Args:
        metric_name: Name of the metric to trace
        context: Output from introspect_project()

    Returns:
        Human-readable provenance paragraph explaining the data flow.
    """
    # Find the metric
    metric = None
    for m in context.get("metrics", []):
        if m["name"] == metric_name:
            metric = m
            break

    if not metric:
        return f"Metric '{metric_name}' not found in the semantic layer."

    # Build the lineage explanation
    lines = []
    lines.append(f"## Lineage for metric: {metric_name}")
    lines.append("")
    lines.append(f"**Description:** {metric.get('description', 'No description')}")
    lines.append("")

    # Determine the metric type and trace
    metric_type = metric.get("type", "simple")

    if metric_type == "derived":
        # Derived metric - show the formula and dependencies
        lines.append(f"**Type:** Derived metric")
        lines.append(f"**Formula:** `{metric.get('expression', 'N/A')}`")
        derived_from = metric.get("derived_from", [])
        if derived_from:
            lines.append(f"**Derived from:** {', '.join(derived_from)}")
            lines.append("")
            for dep_metric in derived_from:
                dep_lineage = _trace_simple_metric(dep_metric, context)
                lines.append(f"  - {dep_metric}: {dep_lineage}")
    else:
        # Simple metric - trace to measure
        measure_name = metric.get("measure", "")
        lines.append(f"**Type:** Simple metric")
        lines.append(f"**Measure:** {measure_name}")
        lines.append("")
        lineage_text = _trace_simple_metric(metric_name, context)
        lines.append(lineage_text)

    # Add model lineage
    lines.append("")
    lines.append("### Data Flow")
    lines.append("")
    lines.append(_build_model_lineage(context))

    return "\n".join(lines)


def _trace_simple_metric(metric_name: str, context: dict) -> str:
    """Trace a simple metric to its source."""
    # Find the metric
    metric = None
    for m in context.get("metrics", []):
        if m["name"] == metric_name:
            metric = m
            break

    if not metric:
        return f"Metric {metric_name} not found"

    measure_name = metric.get("measure", "")

    # Find the measure in semantic models
    for sm in context.get("semantic_models", []):
        for measure in sm.get("measures", []):
            if measure["name"] == measure_name:
                agg = measure.get("agg", "")
                expr = measure.get("expr", "")
                model = sm.get("model", "")
                return f"Aggregation `{agg}({expr})` from `{model}`"

    return f"Measure {measure_name} source not found"


def _build_model_lineage(context: dict) -> str:
    """Build a textual representation of the model lineage."""
    lineage = context.get("lineage", {})

    # Focus on the key models
    key_models = ["fct_order_items", "dim_customers", "dim_suppliers"]

    lines = []
    for model in key_models:
        if model in lineage:
            deps = lineage[model]
            staging_deps = [d for d in deps if d.startswith("stg_")]
            source_deps = [d for d in deps if d.startswith("source:")]

            lines.append(f"**{model}**")

            if staging_deps:
                for stg in staging_deps:
                    stg_sources = lineage.get(stg, [])
                    source_names = [s.replace("source:", "") for s in stg_sources if s.startswith("source:")]
                    if source_names:
                        lines.append(f"  ← {stg} ← raw tables: {', '.join(source_names)}")
                    else:
                        lines.append(f"  ← {stg}")

    return "\n".join(lines)


def explain_query_lineage(tables_referenced: list[str], context: dict) -> str:
    """
    Explain the lineage for a set of tables used in a query.

    Args:
        tables_referenced: List of table names used in the query
        context: Output from introspect_project()

    Returns:
        Human-readable explanation of where the data comes from.
    """
    lineage = context.get("lineage", {})

    lines = ["## Data Provenance", ""]

    for table in tables_referenced:
        lines.append(f"### {table}")

        # Find model description
        for model in context.get("models", []):
            if model["name"] == table:
                if model.get("description"):
                    lines.append(f"*{model['description']}*")
                break

        # Trace dependencies
        if table in lineage:
            deps = lineage[table]
            lines.append("")
            lines.append("**Source chain:**")

            visited = set()
            trace = _recursive_trace(table, lineage, visited)
            for step in trace:
                lines.append(f"  {step}")

        lines.append("")

    return "\n".join(lines)


def _recursive_trace(model: str, lineage: dict, visited: set, depth: int = 0) -> list[str]:
    """Recursively trace model dependencies."""
    if model in visited or depth > 5:
        return []

    visited.add(model)
    result = []

    deps = lineage.get(model, [])
    prefix = "  " * depth

    for dep in deps:
        if dep.startswith("source:"):
            source_name = dep.replace("source:", "")
            result.append(f"{prefix}← Source: {source_name} (raw table)")
        else:
            result.append(f"{prefix}← {dep}")
            # Recurse
            child_trace = _recursive_trace(dep, lineage, visited, depth + 1)
            result.extend(child_trace)

    return result


if __name__ == "__main__":
    from introspection import introspect_project

    context = introspect_project(".")

    print("=" * 60)
    print(explain_lineage("total_revenue", context))
    print()
    print("=" * 60)
    print(explain_lineage("avg_order_value", context))
    print()
    print("=" * 60)
    print(explain_query_lineage(["fct_order_items", "dim_suppliers"], context))
