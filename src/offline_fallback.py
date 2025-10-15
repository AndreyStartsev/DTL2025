# if DB is unavailable to parse & analyze the schema and queries,
import re
from collections import Counter
from typing import List, Dict, Any


def analyze_queries(queries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyzes a list of query execution data to extract key insights.

    Args:
        queries: A list of dictionaries, where each dictionary represents a query
                 with 'query', 'runquantity', and 'executiontime'.

    Returns:
        A dictionary containing structured analysis including stats, table usage,
        most expensive queries, and optimization recommendations.
    """
    if not queries:
        return {
            "db_stats": {"total_queries": 0, "total_runs": 0, "total_time": 0},
            "most_used_tables": [],
            "most_expensive_queries": [],
            "optimization_recommendations": {
                "indexing": {"join_keys": [], "filter_group_keys": []},
                "materialized_views": [],
                "query_rewrites": [],
            },
        }

    table_counter = Counter()
    join_key_counter = Counter()

    processed_queries = []
    total_runs = 0
    total_db_time = 0

    # Pattern to find tables after FROM or JOIN clauses.
    # It handles schemas like db.schema.table and quoted identifiers.
    table_pattern = re.compile(r'(?:FROM|JOIN)\s+([\w\."`]+)', re.IGNORECASE)

    # Pattern to find simple columns in a GROUP BY clause.
    # This is a heuristic and won't capture complex expressions.
    group_by_pattern = re.compile(r'GROUP BY\s+([\w\.\s,`]+)', re.IGNORECASE)

    # Pattern to find columns used in ON clauses for joins.
    on_clause_pattern = re.compile(r'ON\s+([\w\."`]+\.[\w\."`]+)\s*=\s*([\w\."`]+\.[\w\."`]+)', re.IGNORECASE)

    for q in queries:
        query_text = q['query']
        run_quantity = q.get('runquantity', 1)
        execution_time = q.get('executiontime', 0)
        total_time = run_quantity * execution_time

        total_runs += run_quantity
        total_db_time += total_time

        # 1. Extract table names and count usage weighted by run quantity
        tables_found = [t.strip('`"') for t in table_pattern.findall(query_text)]
        for table in set(tables_found):  # Use set to count each table only once per query
            table_counter[table] += run_quantity

        # 2. Find potential query anti-patterns
        problems = []
        if 'order by random()' in query_text.lower() or 'order by rand()' in query_text.lower():
            problems.append({
                "type": "Performance Anti-Pattern",
                "details": "Uses `ORDER BY random()`, which is very inefficient and requires a full data scan and sort. Consider using `TABLESAMPLE` or fetching random IDs separately."
            })
        if len(re.findall(r'cross join', query_text, re.IGNORECASE)) > 0:
            problems.append({
                "type": "Potential Inefficiency",
                "details": "Uses `CROSS JOIN`, which can create a massive number of rows. Ensure this is intentional and not a mistake."
            })

        # 3. Extract join keys for indexing recommendations
        join_keys = on_clause_pattern.findall(query_text)
        for key_pair in join_keys:
            for key in key_pair:
                join_key_counter[key.strip('`"')] += run_quantity

        processed_queries.append({**q, 'total_time': total_time, 'problems': problems})

    # Sort queries by total time to find the most expensive ones
    most_expensive_queries = sorted(processed_queries, key=lambda x: x['total_time'], reverse=True)

    # Consolidate Recommendations
    # Indexing
    top_join_keys = [k for k, v in join_key_counter.most_common(5)]

    # Materialized Views: suggest if top 3 queries have > 2 joins
    mv_suggestions = []
    top_expensive_ids = {d['queryid'] for d in most_expensive_queries[:3]}
    frequent_join_sets = Counter()
    for q in most_expensive_queries[:5]:
        tables_in_query = set(table_pattern.findall(q['query']))
        if len(tables_in_query) > 2:  # Heuristic: queries with 3+ tables are candidates
            frequent_join_sets[tuple(sorted(tables_in_query))] += q['runquantity']

    if frequent_join_sets:
        most_frequent_set, count = frequent_join_sets.most_common(1)[0]
        mv_suggestions.append({
            "tables": list(most_frequent_set),
            "reason": f"These tables are frequently joined together in high-cost queries (contributing to ~{count} total runs). Pre-joining them can significantly boost performance."
        })

    # Query Rewrites
    rewrite_suggestions = []
    for q in most_expensive_queries:
        if q['problems']:
            for problem in q['problems']:
                rewrite_suggestions.append({
                    "queryid": q['queryid'],
                    **problem
                })

    analysis = {
        "db_stats": {
            "total_queries": len(queries),
            "total_runs": total_runs,
            "total_time": total_db_time
        },
        "most_used_tables": table_counter.most_common(5),
        "most_expensive_queries": most_expensive_queries[:5],
        "optimization_recommendations": {
            "indexing": {
                "join_keys": top_join_keys,
            },
            "materialized_views": mv_suggestions,
            "query_rewrites": rewrite_suggestions,
        }
    }
    return analysis


def generate_report(analysis: Dict[str, Any]) -> str:
    """
    Generates a human-readable Markdown report from query analysis data.

    Args:
        analysis: The dictionary of insights produced by the analyze_queries function.

    Returns:
        A string containing the report in Markdown format.
    """
    report = ["# Query Performance Analysis Report"]

    # --- Most Used Tables ---
    report.append("\n## 1. Most Used Tables")
    report.append("These tables appear most frequently in the executed queries, weighted by run quantity.")
    report.append("| Table Name | Weighted Usage Count |")
    report.append("|:---|:---|")
    for table, count in analysis['most_used_tables']:
        report.append(f"| `{table}` | {count:,} |")

    # --- Most Expensive Queries ---
    report.append("\n## 3. Most Expensive Queries")
    report.append(
        "These queries consume the most total database time (`executiontime` * `runquantity`). They are the top priority for optimization.")
    report.append("| Query ID (short) | Run Quantity | Avg. Time (s) | Total Time (s) | Key Problem(s) |")
    report.append("|:---|:---|:---|:---|:---|")
    for q in analysis['most_expensive_queries']:
        queryid_short = q['queryid'].split('-')[0]
        query = q['query']
        problems = q.get('problems', [])
        problem_str = " / ".join(p['type'] for p in problems) if problems else "N/A"
        report.append(
            f"| `{query}...` | {q['runquantity']:,} | {q['executiontime']} | {int(q['total_time']):,} | {problem_str} |")

    # --- Optimization Recommendations ---
    report.append("\n## 4. Recommendations for Optimization")
    recs = analysis['optimization_recommendations']

    # Indexing
    if recs['indexing']['join_keys']:
        report.append("\n### 4.1 Indexing")
        report.append(
            "**Suggestion:** Add indexes to columns frequently used in `JOIN` conditions to accelerate data linking.")
        report.append("- **Recommended columns for indexing:**")
        for key in recs['indexing']['join_keys']:
            report.append(f"  - `{key}`")

    # Materialized Views
    if recs['materialized_views']:
        report.append("\n### 4.2 Materialized Views")
        report.append(
            "**Suggestion:** Create materialized views for complex, high-frequency joins to pre-compute results.")
        for mv_rec in recs['materialized_views']:
            tables_str = ", ".join([f"`{t}`" for t in mv_rec['tables']])
            report.append(f"- **Candidate Tables:** {tables_str}")
            report.append(f"  - **Reason:** {mv_rec['reason']}")

    # Query Rewrites
    if recs['query_rewrites']:
        report.append("\n### 4.3 Query Rewrites")
        report.append("**Suggestion:** The queries contain performance anti-patterns and should be refactored.")
        # Use a set to only report each queryid once
        reported_query_ids = set()
        antipatterns = []
        for rw_rec in recs['query_rewrites']:
            qid = rw_rec['queryid']
            rec = rw_rec['details']
            if qid in reported_query_ids:
                continue
            if rec in antipatterns:
                continue
            antipatterns.append(rw_rec['details'])
            reported_query_ids.add(qid)
            report.append(f"  - **Antipattern:** {rw_rec['details']}")

    return "\n".join(report)

def fallback_analysis(queries: List[Dict[str, Any]]) -> str:
    """
    Performs a fallback analysis of queries and generates a report.

    Args:
        queries: A list of dictionaries, where each dictionary represents a query
                 with 'query', 'runquantity', and 'executiontime'.

    Returns:
        A string containing the analysis report in Markdown format.
    """
    analysis = analyze_queries(queries)
    report = generate_report(analysis)
    return report