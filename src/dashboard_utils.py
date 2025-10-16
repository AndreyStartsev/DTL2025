# optimization_analyzer.py - ADD THESE FUNCTIONS
from typing import Dict, List, Any, Tuple, Set
from collections import defaultdict
import re

from src.ddl_parser import DDLParser

def create_insights_report(ddl_statements: List[Dict], queries: List[Dict]) -> Dict[str, Any]:
    """
    Create comprehensive schema insights report from DDL and queries.
    Used by frontend for visualization.

    Args:
        ddl_statements: List of DDL statement dicts with 'statement' key
        queries: List of query dicts with 'queryid', 'query', 'runquantity' keys

    Returns:
        Dict containing detailed schema insights for visualization
    """
    parser = DDLParser()

    # Parse tables from DDL
    tables = parser.parse_ddl_statements(ddl_statements)

    if not tables:
        return {
            "total_columns": 0,
            "total_tables": 0,
            "tables": [],
            "column_types_distribution": {},
            "index_coverage": {
                "indexed_tables": 0,
                "total_indexes": 0,
                "coverage_percent": 0,
                "recommendations": "No tables found in schema."
            },
            "data_quality": {
                "nullable_columns_percent": 0,
                "tables_without_pk": 0,
                "orphaned_tables": 0
            }
        }

    # Get schema insights using DDLParser
    schema_insights = parser.get_schema_insights(tables, queries)

    # Get column type distribution
    stats = parser.get_table_stats(tables)

    # Enhance with additional analysis
    data_quality = _analyze_data_quality(tables, queries)
    query_coverage = _analyze_query_coverage(tables, queries)

    return {
        "total_columns": schema_insights["total_columns"],
        "total_tables": schema_insights["total_tables"],
        "tables": schema_insights["tables"],
        "column_types_distribution": stats["column_types_distribution"],
        "index_coverage": schema_insights["index_coverage"],
        "data_quality": data_quality,
        "query_coverage": query_coverage,
        "partitioning_candidates": _identify_partitioning_candidates(tables),
        "denormalization_opportunities": _identify_denormalization_opportunities(tables, queries),
        "statistics": stats
    }


def _analyze_data_quality(tables: List, queries: List[Dict]) -> Dict[str, Any]:
    """Analyze data quality metrics from schema."""
    if not tables:
        return {
            "nullable_columns_percent": 0,
            "tables_without_pk": 0,
            "orphaned_tables": 0,
            "recommendations": []
        }

    total_columns = sum(len(t.columns) for t in tables)
    nullable_columns = sum(1 for t in tables for c in t.columns if c.nullable)

    # Check for primary keys (generic approach)
    tables_without_pk = sum(1 for t in tables if not _has_primary_key(t))

    # Check for orphaned tables (not referenced in queries)
    orphaned_tables = 0
    query_text = ' '.join(q.get('query', '').lower() for q in queries)
    for table in tables:
        table_name = table.name.lower()
        # Check if table appears in queries
        if table_name not in query_text:
            orphaned_tables += 1

    # Generate recommendations
    recommendations = []
    nullable_pct = round((nullable_columns / total_columns * 100), 1) if total_columns > 0 else 0

    if nullable_pct > 50:
        recommendations.append("High percentage of nullable columns may indicate schema design issues")
    if tables_without_pk > 0:
        recommendations.append(
            f"{tables_without_pk} table(s) missing primary keys - consider adding for data integrity")
    if orphaned_tables > 0:
        recommendations.append(f"{orphaned_tables} table(s) not referenced in queries - consider archiving or removal")

    return {
        "nullable_columns_percent": nullable_pct,
        "tables_without_pk": tables_without_pk,
        "orphaned_tables": orphaned_tables,
        "total_columns": total_columns,
        "recommendations": recommendations
    }


def _has_primary_key(table) -> bool:
    """Check if table has a primary key - handles None values safely"""
    # Check if any column is marked as primary key
    if hasattr(table, 'columns') and table.columns:
        for col in table.columns:
            # Check column's primary_key attribute
            if hasattr(col, 'primary_key') and col.primary_key:
                return True

            # Check column constraints (safely)
            if hasattr(col, 'constraints') and col.constraints is not None:
                try:
                    for constraint in col.constraints:
                        if constraint and 'PRIMARY KEY' in str(constraint).upper():
                            return True
                except (TypeError, AttributeError):
                    pass

    # Check table-level constraints (safely)
    if hasattr(table, 'constraints') and table.constraints is not None:
        try:
            for constraint in table.constraints:
                if constraint and 'PRIMARY KEY' in str(constraint).upper():
                    return True
        except (TypeError, AttributeError):
            pass

    return False


def _analyze_query_coverage(tables: List, queries: List[Dict]) -> Dict[str, Any]:
    """Analyze which tables are used in queries and how frequently (schema-aware)."""
    if not queries:
        # If no queries, return structured empty response
        tbl_names = [t.name for t in tables] if tables else []
        return {
            "table_usage": {name.lower(): 0 for name in tbl_names},
            "unused_tables": tbl_names,
            "most_queried_table": None,
            "most_queried_count": 0
        }

    # Build a normalized index of known tables from DDL
    def normalize(s: str) -> str:
        return re.sub(r'["`]', '', s).strip().lower()

    known_keys = {}  # key -> canonical_name
    for t in (tables or []):
        t_name = normalize(getattr(t, 'name', '') or '')
        t_schema = normalize(getattr(t, 'schema', '') or '')
        # Canonical name
        canon = f"{t_schema}.{t_name}" if t_schema else t_name
        # Fill index keys for matching
        if t_name:
            known_keys[t_name] = canon
        if t_schema and t_name:
            known_keys[f"{t_schema}.{t_name}"] = canon
        # Support db.schema.table if available
        db = normalize(getattr(t, 'database', '') or '')
        if db and t_schema and t_name:
            known_keys[f"{db}.{t_schema}.{t_name}"] = canon

    # Initialize usage with known tables
    usage = {canon: 0 for canon in set(known_keys.values())}

    # Regex to extract referenced tables after FROM/JOIN
    ref_re = re.compile(r'\b(from|join)\s+([a-zA-Z0-9_\."]+)', re.IGNORECASE)

    def all_forms(ref: str) -> List[str]:
        """Return possible matching keys"""
        ref_norm = normalize(ref)
        parts = [p for p in ref_norm.split('.') if p]
        forms = []
        if parts:
            forms.append(parts[-1])  # table
        if len(parts) >= 2:
            forms.append('.'.join(parts[-2:]))  # schema.table
        if len(parts) >= 3:
            forms.append('.'.join(parts[-3:]))  # db.schema.table
        return list(dict.fromkeys(forms))

    for q in queries:
        text = (q.get('query') or '')
        runq = int(q.get('runquantity', 0) or 0)
        if not text:
            continue

        for _, ref in ref_re.findall(text):
            matched = False
            for form in all_forms(ref):
                if form in known_keys:
                    usage[known_keys[form]] += runq
                    matched = True
                    break

            # If not matched and we want to show referenced-but-unknown tables
            if not matched:
                forms = all_forms(ref)
                key = forms[1] if len(forms) >= 2 else forms[0] if forms else None
                if key:
                    usage.setdefault(key, 0)
                    usage[key] += runq

    # Compute unused among known tables
    known_canons = set(known_keys.values())
    unused_tables = [k for k in known_canons if usage.get(k, 0) == 0]

    # Identify most queried
    most_queried_table = None
    most_queried_count = 0
    if usage:
        most_queried_table, most_queried_count = max(usage.items(), key=lambda x: x[1])

    # Sort usage descending
    usage_sorted = {k: v for k, v in sorted(usage.items(), key=lambda x: x[1], reverse=True)}

    return {
        "table_usage": usage_sorted,
        "unused_tables": unused_tables,
        "most_queried_table": most_queried_table,
        "most_queried_count": most_queried_count
    }


def _identify_partitioning_candidates(tables: List) -> List[Dict[str, Any]]:
    """Identify columns suitable for partitioning - GENERIC"""
    candidates = []

    # Common patterns for partitionable columns
    date_patterns = ['date', 'time', 'year', 'month', 'quarter', 'day', 'timestamp',
                     'created', 'updated', 'modified']
    category_patterns = ['type', 'category', 'status', 'region', 'country', 'state']

    for table in tables:
        table_candidates = []

        for column in table.columns:
            col_name_lower = column.name.lower()
            col_type_lower = column.data_type.lower() if hasattr(column, 'data_type') else ''

            # Check for date/time columns
            if any(pattern in col_name_lower for pattern in date_patterns) or \
                    any(dt in col_type_lower for dt in ['date', 'time', 'timestamp']):
                table_candidates.append({
                    "column": column.name,
                    "type": getattr(column, 'data_type', 'unknown'),
                    "reason": "Temporal column suitable for time-based partitioning",
                    "strategy": "RANGE partitioning by date/time"
                })

            # Check for categorical columns with low cardinality
            elif any(pattern in col_name_lower for pattern in category_patterns):
                table_candidates.append({
                    "column": column.name,
                    "type": getattr(column, 'data_type', 'unknown'),
                    "reason": "Categorical column suitable for list partitioning",
                    "strategy": "LIST partitioning by category"
                })

        if table_candidates:
            table_name = f"{table.schema}.{table.name}" if hasattr(table, 'schema') and table.schema else table.name
            candidates.append({
                "table": table_name,
                "candidates": table_candidates
            })

    return candidates


def _identify_denormalization_opportunities(tables: List, queries: List[Dict]) -> Dict[str, Any]:
    """Identify opportunities for denormalization based on query patterns - GENERIC"""
    if len(tables) <= 1:
        return {
            "opportunity_level": "low",
            "reason": "Single table schema - already denormalized",
            "recommendations": []
        }

    # Count joins in queries
    total_joins = 0
    complex_joins = 0
    join_patterns = defaultdict(int)

    for query in queries:
        query_text = query.get('query', '').upper()
        run_qty = query.get('runquantity', 0)

        # Count different join types
        join_count = query_text.count('JOIN')
        total_joins += join_count * run_qty

        if join_count > 3:
            complex_joins += run_qty

        # Track specific join patterns
        if 'INNER JOIN' in query_text:
            join_patterns['INNER'] += run_qty
        if 'LEFT JOIN' in query_text or 'LEFT OUTER JOIN' in query_text:
            join_patterns['LEFT'] += run_qty
        if 'RIGHT JOIN' in query_text or 'RIGHT OUTER JOIN' in query_text:
            join_patterns['RIGHT'] += run_qty

    recommendations = []
    opportunity_level = "low"

    if complex_joins > 1000:
        opportunity_level = "high"
        recommendations.append(
            "High frequency of complex joins (>3 tables) detected - consider denormalizing frequently joined tables")
        recommendations.append("Identify and pre-join common query patterns into wider tables")
        recommendations.append("Consider creating aggregate tables for common metrics")
    elif total_joins > 5000:
        opportunity_level = "medium"
        recommendations.append("Moderate join activity - consider selective denormalization for hot query paths")
        recommendations.append("Analyze most frequent join pairs for potential denormalization")
    else:
        recommendations.append("Current join patterns appear manageable - maintain normalized structure")

    return {
        "opportunity_level": opportunity_level,
        "total_join_operations": total_joins,
        "complex_join_queries": complex_joins,
        "join_type_distribution": dict(join_patterns),
        "recommendations": recommendations
    }