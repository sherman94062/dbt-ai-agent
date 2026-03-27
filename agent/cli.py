#!/usr/bin/env python3
"""
dbt Semantic Layer Agent CLI

A conversational agent that translates natural language questions
into SQL queries against a dbt semantic layer.
"""

import argparse
import sys
from pathlib import Path

from introspection import introspect_project, format_context_for_llm
from query_generator import generate_query, generate_query_simple
from executor import execute_query, format_results
from lineage import explain_lineage, explain_query_lineage


def main():
    parser = argparse.ArgumentParser(
        description="dbt Semantic Layer Agent - NL to SQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ask a question
  python cli.py "What is total revenue by market segment?"

  # Show project context
  python cli.py --context

  # Show metric lineage
  python cli.py --lineage total_revenue

  # Use LLM for query generation (requires ANTHROPIC_API_KEY)
  python cli.py --llm "What are the top 5 customers by order count?"
        """
    )

    parser.add_argument(
        "question",
        nargs="?",
        help="Natural language question to answer"
    )
    parser.add_argument(
        "--project", "-p",
        default=".",
        help="Path to dbt project (default: current directory)"
    )
    parser.add_argument(
        "--context", "-c",
        action="store_true",
        help="Show project context and exit"
    )
    parser.add_argument(
        "--lineage", "-l",
        metavar="METRIC",
        help="Show lineage for a specific metric"
    )
    parser.add_argument(
        "--llm",
        action="store_true",
        help="Use Claude LLM for query generation (requires ANTHROPIC_API_KEY)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate query but don't execute"
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Run in interactive REPL mode"
    )

    args = parser.parse_args()

    # Load project context
    try:
        context = introspect_project(args.project)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        print("Make sure to run 'dbt build' first.", file=sys.stderr)
        sys.exit(1)

    # Handle context display
    if args.context:
        print(format_context_for_llm(context))
        return

    # Handle lineage display
    if args.lineage:
        print(explain_lineage(args.lineage, context))
        return

    # Handle interactive mode
    if args.interactive:
        run_interactive(context, args)
        return

    # Handle single question
    if args.question:
        answer_question(args.question, context, args)
    else:
        parser.print_help()


def answer_question(question: str, context: dict, args) -> None:
    """Answer a single question."""
    print(f"\n📊 Question: {question}\n")

    # Generate query
    print("🔍 Generating SQL query...")
    if args.llm:
        try:
            result = generate_query(question, context)
        except ImportError:
            print("Warning: anthropic package not available. Falling back to pattern matching.")
            result = generate_query_simple(question, context)
    else:
        result = generate_query_simple(question, context)

    if result.get("error") or not result.get("query"):
        print(f"❌ Could not generate query: {result.get('error', 'Unknown error')}")
        return

    query = result["query"]
    print(f"\n📝 Generated SQL (confidence: {result.get('confidence', 'unknown')}):")
    print(f"```sql\n{query}\n```\n")

    if result.get("explanation"):
        print(f"💡 {result['explanation']}\n")

    # Execute query
    if not args.dry_run:
        print("⚡ Executing query...")
        try:
            exec_result = execute_query(query)
            print("\n📊 Results:")
            print(format_results(exec_result))
        except Exception as e:
            print(f"❌ Execution error: {e}")
            return

        # Show lineage
        tables = result.get("tables_referenced", [])
        if tables:
            print("\n" + "=" * 60)
            print(explain_query_lineage(tables, context))


def run_interactive(context: dict, args) -> None:
    """Run interactive REPL mode."""
    print("\n🤖 dbt Semantic Layer Agent")
    print("=" * 40)
    print("Ask questions about your data in natural language.")
    print("Commands: /context, /lineage <metric>, /quit\n")

    while True:
        try:
            question = input("❓ ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not question:
            continue

        if question.lower() in ["/quit", "/exit", "/q"]:
            print("Goodbye!")
            break

        if question.lower() == "/context":
            print(format_context_for_llm(context))
            continue

        if question.lower().startswith("/lineage"):
            parts = question.split(maxsplit=1)
            if len(parts) > 1:
                print(explain_lineage(parts[1], context))
            else:
                print("Usage: /lineage <metric_name>")
            continue

        answer_question(question, context, args)
        print()


if __name__ == "__main__":
    main()
