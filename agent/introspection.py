"""Project introspection tool for reading dbt artifacts."""

import json
from pathlib import Path
from typing import Any


def introspect_project(project_path: str) -> dict[str, Any]:
    """
    Reads manifest.json and semantic_manifest.json from target/.

    Returns a structured dict containing:
    - models: list of dbt models with columns and descriptions
    - sources: list of source tables
    - metrics: list of metrics with formulas
    - dimensions: list of dimensions from semantic models
    - entities: list of entities from semantic models
    - lineage: dict mapping model names to their upstream dependencies

    Args:
        project_path: Path to the dbt project root directory

    Returns:
        Dict containing structured project context for query generation
    """
    project_dir = Path(project_path)
    target_dir = project_dir / "target"

    manifest_path = target_dir / "manifest.json"
    semantic_manifest_path = target_dir / "semantic_manifest.json"

    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found at {manifest_path}. Run 'dbt build' first.")

    with open(manifest_path) as f:
        manifest = json.load(f)

    semantic_manifest = None
    if semantic_manifest_path.exists():
        with open(semantic_manifest_path) as f:
            semantic_manifest = json.load(f)

    return {
        "models": _extract_models(manifest),
        "sources": _extract_sources(manifest),
        "metrics": _extract_metrics(semantic_manifest),
        "semantic_models": _extract_semantic_models(semantic_manifest),
        "lineage": _build_lineage(manifest),
        "project_name": manifest.get("metadata", {}).get("project_name", "unknown"),
    }


def _extract_models(manifest: dict) -> list[dict]:
    """Extract model information from manifest."""
    models = []
    nodes = manifest.get("nodes", {})

    for node_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue

        columns = []
        for col_name, col_info in node.get("columns", {}).items():
            columns.append({
                "name": col_name,
                "description": col_info.get("description", ""),
                "data_type": col_info.get("data_type", ""),
            })

        models.append({
            "name": node.get("name"),
            "description": node.get("description", ""),
            "schema": node.get("schema"),
            "database": node.get("database"),
            "materialized": node.get("config", {}).get("materialized"),
            "columns": columns,
            "depends_on": node.get("depends_on", {}).get("nodes", []),
            "unique_id": node_id,
        })

    return models


def _extract_sources(manifest: dict) -> list[dict]:
    """Extract source information from manifest."""
    sources = []
    source_nodes = manifest.get("sources", {})

    for source_id, source in source_nodes.items():
        columns = []
        for col_name, col_info in source.get("columns", {}).items():
            columns.append({
                "name": col_name,
                "description": col_info.get("description", ""),
            })

        sources.append({
            "name": source.get("name"),
            "source_name": source.get("source_name"),
            "description": source.get("description", ""),
            "schema": source.get("schema"),
            "database": source.get("database"),
            "columns": columns,
            "unique_id": source_id,
        })

    return sources


def _extract_metrics(semantic_manifest: dict | None) -> list[dict]:
    """Extract metrics from semantic manifest."""
    if not semantic_manifest:
        return []

    metrics = []
    for metric in semantic_manifest.get("metrics", []):
        metric_info = {
            "name": metric.get("name"),
            "description": metric.get("description", ""),
            "type": metric.get("type", {}).get("value") if isinstance(metric.get("type"), dict) else metric.get("type"),
            "label": metric.get("label", ""),
        }

        type_params = metric.get("type_params", {})
        if type_params:
            if "measure" in type_params:
                measure = type_params["measure"]
                if isinstance(measure, dict):
                    metric_info["measure"] = measure.get("name")
                else:
                    metric_info["measure"] = measure
            if "expr" in type_params:
                metric_info["expression"] = type_params["expr"]
            if "metrics" in type_params:
                metric_info["derived_from"] = [
                    m.get("name") if isinstance(m, dict) else m
                    for m in type_params["metrics"]
                ]

        metrics.append(metric_info)

    return metrics


def _extract_semantic_models(semantic_manifest: dict | None) -> list[dict]:
    """Extract semantic models with dimensions and measures."""
    if not semantic_manifest:
        return []

    semantic_models = []
    for sm in semantic_manifest.get("semantic_models", []):
        dimensions = []
        for dim in sm.get("dimensions", []):
            dimensions.append({
                "name": dim.get("name"),
                "type": dim.get("type", {}).get("value") if isinstance(dim.get("type"), dict) else dim.get("type"),
                "expr": dim.get("expr", ""),
                "description": dim.get("description", ""),
            })

        measures = []
        for measure in sm.get("measures", []):
            measures.append({
                "name": measure.get("name"),
                "agg": measure.get("agg", {}).get("value") if isinstance(measure.get("agg"), dict) else measure.get("agg"),
                "expr": measure.get("expr", ""),
                "description": measure.get("description", ""),
            })

        entities = []
        for entity in sm.get("entities", []):
            entities.append({
                "name": entity.get("name"),
                "type": entity.get("type", {}).get("value") if isinstance(entity.get("type"), dict) else entity.get("type"),
                "expr": entity.get("expr", ""),
            })

        semantic_models.append({
            "name": sm.get("name"),
            "description": sm.get("description", ""),
            "model": sm.get("node_relation", {}).get("relation_name", ""),
            "dimensions": dimensions,
            "measures": measures,
            "entities": entities,
        })

    return semantic_models


def _build_lineage(manifest: dict) -> dict[str, list[str]]:
    """Build lineage graph from manifest dependencies."""
    lineage = {}
    nodes = manifest.get("nodes", {})

    for node_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue

        model_name = node.get("name")
        deps = node.get("depends_on", {}).get("nodes", [])

        upstream = []
        for dep in deps:
            if dep.startswith("model."):
                upstream.append(dep.split(".")[-1])
            elif dep.startswith("source."):
                parts = dep.split(".")
                upstream.append(f"source:{parts[-2]}.{parts[-1]}")

        lineage[model_name] = upstream

    return lineage


def format_context_for_llm(context: dict) -> str:
    """
    Format the introspected context as a readable string for LLM consumption.

    Args:
        context: Output from introspect_project()

    Returns:
        Formatted string representation of the project context
    """
    lines = []
    lines.append(f"# dbt Project: {context['project_name']}")
    lines.append("")

    # Sources
    lines.append("## Sources")
    for source in context['sources']:
        lines.append(f"- {source['source_name']}.{source['name']}: {source['description']}")
        if source['columns']:
            for col in source['columns'][:5]:  # Limit columns shown
                lines.append(f"  - {col['name']}: {col['description']}")
            if len(source['columns']) > 5:
                lines.append(f"  - ... and {len(source['columns']) - 5} more columns")
    lines.append("")

    # Models
    lines.append("## Models")
    for model in context['models']:
        mat = model.get('materialized', 'view')
        lines.append(f"- {model['name']} ({mat}): {model['description']}")
        if model['columns']:
            for col in model['columns'][:5]:
                lines.append(f"  - {col['name']}: {col['description']}")
            if len(model['columns']) > 5:
                lines.append(f"  - ... and {len(model['columns']) - 5} more columns")
    lines.append("")

    # Metrics
    lines.append("## Metrics")
    for metric in context['metrics']:
        lines.append(f"- {metric['name']} ({metric.get('type', 'simple')}): {metric['description']}")
        if metric.get('measure'):
            lines.append(f"  - measure: {metric['measure']}")
        if metric.get('expression'):
            lines.append(f"  - expr: {metric['expression']}")
        if metric.get('derived_from'):
            lines.append(f"  - derived from: {', '.join(metric['derived_from'])}")
    lines.append("")

    # Semantic Models
    lines.append("## Semantic Models")
    for sm in context['semantic_models']:
        lines.append(f"### {sm['name']}")
        lines.append(f"Base table: {sm['model']}")
        lines.append("")

        lines.append("Dimensions:")
        for dim in sm['dimensions']:
            lines.append(f"  - {dim['name']} ({dim['type']}): {dim.get('description', dim.get('expr', ''))}")

        lines.append("Measures:")
        for measure in sm['measures']:
            lines.append(f"  - {measure['name']} ({measure['agg']}): {measure.get('description', measure.get('expr', ''))}")

        lines.append("Entities:")
        for entity in sm['entities']:
            lines.append(f"  - {entity['name']} ({entity['type']})")

    return "\n".join(lines)


if __name__ == "__main__":
    # Test the introspection
    import sys
    project_path = sys.argv[1] if len(sys.argv) > 1 else "."
    context = introspect_project(project_path)
    print(format_context_for_llm(context))
