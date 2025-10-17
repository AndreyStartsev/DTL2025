# optimization_summary.py - IMPROVED VERSION

import json
import re
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
import pandas as pd

from src.ddl_parser import DDLParser
from src.query_analyzer import QueryAnalyzer


@dataclass
class OptimizationSummary:
    database_profile: Dict[str, Any]
    performance_bottlenecks: List[Dict[str, Any]]
    schema_insights: Dict[str, Any]
    query_patterns: Dict[str, Any]
    optimization_recommendations: List[Dict[str, Any]]


class OptimizationAnalyzer:
    def __init__(self, analysis_data: Dict):
        self.data = analysis_data

    def create_compact_summary(self) -> OptimizationSummary:
        """Создает компактное резюме для оптимизации"""
        return OptimizationSummary(
            database_profile=self._get_database_profile(),
            performance_bottlenecks=self._identify_bottlenecks(),
            schema_insights=self._analyze_schema_structure(),
            query_patterns=self._extract_key_patterns(),
            optimization_recommendations=self._generate_recommendations()
        )

    def _get_database_profile(self) -> Dict[str, Any]:
        """Основные характеристики БД"""
        db_stats = self.data.get('database_stats', {})
        schema_stats = self.data.get('schema_analysis', {}).get('statistics', {})

        overview = db_stats.get('overview', {})
        table_stats = db_stats.get('table_statistics', [])

        total_tables = len(table_stats)
        total_rows = sum(t.get('row_count', 0) for t in table_stats)
        total_size_bytes = sum(t.get('size_bytes', 0) for t in table_stats)

        return {
            'database_type': overview.get('driver', 'unknown'),
            'table_count': total_tables,
            'total_rows': f"{total_rows:,}",
            'total_rows_numeric': total_rows,
            'total_size_gb': round(total_size_bytes / (1024 ** 3), 2),
            'total_columns': schema_stats.get('total_columns', 0),
            'column_distribution': schema_stats.get('column_types_distribution', {}),
            'largest_table': {
                'name': table_stats[0].get('table_name', '') if table_stats else '',
                'rows': f"{table_stats[0].get('row_count', 0):,}" if table_stats else '0',
                'rows_numeric': table_stats[0].get('row_count', 0) if table_stats else 0,
                'size_gb': round(table_stats[0].get('size_bytes', 0) / (1024 ** 3), 2) if table_stats else 0
            } if table_stats else None
        }

    def _identify_bottlenecks(self) -> List[Dict[str, Any]]:
        """Выявляет узкие места производительности"""
        bottlenecks = []
        query_patterns = self.data.get('query_analysis', {}).get('patterns', [])

        # Slow queries (>30 sec)
        slow_queries = [q for q in query_patterns if q.get('execution_time', 0) > 30]
        if slow_queries:
            bottlenecks.append({
                'type': 'slow_queries',
                'severity': 'high',
                'count': len(slow_queries),
                'details': [
                    {
                        'query_id': q['query_id'],
                        'execution_time': q['execution_time'],
                        'run_quantity': q['run_quantity'],
                        'impact_score': q['execution_time'] * q['run_quantity']
                    }
                    for q in sorted(slow_queries, key=lambda x: x['execution_time'] * x['run_quantity'], reverse=True)
                ]
            })

        # High volume queries (>1000 executions)
        high_volume_queries = [q for q in query_patterns if q.get('run_quantity', 0) > 1000]
        if high_volume_queries:
            bottlenecks.append({
                'type': 'high_volume_queries',
                'severity': 'medium',
                'count': len(high_volume_queries),
                'total_executions': sum(q['run_quantity'] for q in high_volume_queries),
                'details': [
                               {
                                   'query_id': q['query_id'],
                                   'run_quantity': q['run_quantity'],
                                   'execution_time': q['execution_time'],
                                   'total_time': q['execution_time'] * q['run_quantity']
                               }
                               for q in sorted(high_volume_queries, key=lambda x: x['run_quantity'], reverse=True)
                           ][:5]
            })

        return bottlenecks

    def _analyze_schema_structure(self) -> Dict[str, Any]:
        """Анализ структуры схемы для оптимизации"""
        tables = self.data.get('schema_analysis', {}).get('tables', [])

        if not tables:
            return {}

        main_table = tables[0]
        columns = main_table.get('columns', [])

        # Group columns by functionality
        time_columns = [c for c in columns if any(word in c['name'].lower()
                                                  for word in ['date', 'time', 'year', 'month', 'quarter', 'day'])]

        location_columns = [c for c in columns if any(word in c['name'].lower()
                                                      for word in ['origin', 'dest', 'airport', 'city', 'state'])]

        airline_columns = [c for c in columns if any(word in c['name'].lower()
                                                     for word in ['airline', 'flight', 'tail'])]

        delay_columns = [c for c in columns if any(word in c['name'].lower()
                                                   for word in ['delay', 'cancel', 'divert'])]

        return {
            'total_columns': len(columns),
            'partitioning_candidates': [c['name'] for c in time_columns],
            'indexing_candidates': {
                'location': [c['name'] for c in location_columns[:3]],
                'airline': [c['name'] for c in airline_columns[:2]],
                'performance': [c['name'] for c in delay_columns[:3]]
            },
            'denormalization_potential': 'low',
        }

    def _extract_key_patterns(self) -> Dict[str, Any]:
        """Извлекает ключевые паттерны запросов"""
        query_stats = self.data.get('query_analysis', {}).get('statistics', {})
        patterns = self.data.get('query_analysis', {}).get('patterns', [])

        aggregation_patterns = {}
        for pattern in patterns:
            for agg in pattern.get('aggregations', []):
                aggregation_patterns[agg] = aggregation_patterns.get(agg, 0) + pattern.get('run_quantity', 0)

        return {
            'total_query_volume': sum(p.get('run_quantity', 0) for p in patterns),
            'avg_execution_time': query_stats.get('avg_execution_time', 0),
            'cte_usage_percent': round((query_stats.get('cte_usage', 0) / len(patterns)) * 100, 1) if patterns else 0,
            'join_frequency': query_stats.get('join_patterns', {}),
            'top_aggregations': dict(sorted(aggregation_patterns.items(), key=lambda x: x[1], reverse=True)[:5]),
            'materialized_view_candidates': self._identify_mv_candidates(patterns)
        }

    def _identify_mv_candidates(self, patterns: List[Dict]) -> List[Dict[str, Any]]:
        """Определяет кандидатов для материализованных представлений"""
        candidates = []
        aggregation_groups = {}

        for pattern in patterns:
            if pattern.get('run_quantity', 0) > 500:
                agg_key = tuple(sorted(pattern.get('aggregations', [])))
                if agg_key not in aggregation_groups:
                    aggregation_groups[agg_key] = []
                aggregation_groups[agg_key].append(pattern)

        for agg_key, group_patterns in aggregation_groups.items():
            if len(group_patterns) >= 2:
                total_volume = sum(p.get('run_quantity', 0) for p in group_patterns)
                candidates.append({
                    'aggregations': list(agg_key),
                    'query_count': len(group_patterns),
                    'total_executions': total_volume,
                    'avg_execution_time': sum(p.get('execution_time', 0) for p in group_patterns) / len(group_patterns),
                    'potential_savings': 'high' if total_volume > 5000 else 'medium'
                })

        return sorted(candidates, key=lambda x: x['total_executions'], reverse=True)[:3]

    def _generate_recommendations(self) -> List[Dict[str, Any]]:
        """Генерирует рекомендации по оптимизации"""
        recommendations = []

        # Partitioning recommendation
        recommendations.append({
            'type': 'partitioning',
            'priority': 'high',
            'description': 'Implement date-based partitioning',
            'implementation': 'Partition by year/month columns for better query pruning',
            'expected_improvement': 'Query performance improvement 30-70%',
            'effort': 'medium'
        })

        # Materialized views
        mv_candidates = self._identify_mv_candidates(
            self.data.get('query_analysis', {}).get('patterns', [])
        )

        if mv_candidates:
            recommendations.append({
                'type': 'materialized_views',
                'priority': 'high',
                'description': f'Create {len(mv_candidates)} materialized views for common aggregations',
                'implementation': 'Pre-aggregate frequently used COUNT, SUM, AVG operations',
                'expected_improvement': 'Query performance improvement 50-90%',
                'effort': 'low'
            })

        # Indexing recommendation
        recommendations.append({
            'type': 'indexing',
            'priority': 'medium',
            'description': 'Create composite indexes on frequently filtered columns',
            'implementation': 'Index on origin+dest+flightdate, airline+flightdate combinations',
            'expected_improvement': 'Query performance improvement 20-50%',
            'effort': 'low'
        })

        # Compression recommendation
        recommendations.append({
            'type': 'compression',
            'priority': 'medium',
            'description': 'Optimize storage format and compression',
            'implementation': 'Use columnar storage with better compression for analytical workload',
            'expected_improvement': 'Storage reduction 40-60%, I/O improvement 20-40%',
            'effort': 'medium'
        })

        return recommendations

    def create_concise_report_for_agent(self) -> Dict[str, Any]:
        """Creates data-driven report for AI agent schema design"""
        query_stats = self.data.get('query_analysis', {}).get('statistics', {})
        schema_tables = self.data.get('schema_analysis', {}).get('tables', [])

        num_tables = len(schema_tables)
        total_joins = sum(query_stats.get('join_patterns', {}).values())
        total_queries = query_stats.get('total_queries', 1)

        archetype = "unknown"
        if num_tables == 1:
            archetype = "single_big_table"
        elif num_tables > 1 and total_joins > (total_queries / 2):
            archetype = "normalized_multitable"
        elif num_tables > 1:
            archetype = "denormalized_multitable"

        return {
            'source_schema_archetype': archetype,
            'workload_profile': {
                'top_group_by_columns': query_stats.get('most_used_group_by_columns', {}),
                'top_filter_columns': query_stats.get('most_used_filter_columns', {}),
                'top_joined_tables': query_stats.get('most_used_tables', {}),
                'top_aggregated_functions': query_stats.get('aggregation_usage', {})
            },
            'source_tables': [
                {
                    'name': t.get('name', ''),
                    'column_count': len(t.get('columns', [])),
                    'columns': t.get('columns', [])  # ADDED: Include full column list
                }
                for t in schema_tables
            ]
        }


def create_optimization_report(analysis_data: Dict) -> Dict[str, Any]:
    """Создает компактный отчет оптимизации - WITH ENHANCED MARKDOWN"""
    analyzer = OptimizationAnalyzer(analysis_data)
    summary = analyzer.create_compact_summary()
    agent_input = analyzer.create_concise_report_for_agent()

    db_profile = summary.database_profile
    table_count = db_profile.get('table_count', 0)

    if table_count == 1:
        opt_potential = 'High - Single large table with heavy analytical workload'
    elif table_count <= 5:
        opt_potential = 'Medium - Few tables with focused workload'
    else:
        opt_potential = 'Moderate - Many small tables with distributed workload'

    return {
        'executive_summary': {
            'database_size': db_profile.get('total_size_gb', 0),
            'total_tables': table_count,
            'total_rows': db_profile.get('total_rows', '0'),
            'query_volume_per_day': sum(p.get('total_executions', 0) for p in summary.performance_bottlenecks if
                                        p.get('type') == 'high_volume_queries'),
            'critical_issues': len([b for b in summary.performance_bottlenecks if b.get('severity') == 'high']),
            'optimization_potential': opt_potential
        },
        'database_profile': summary.database_profile,
        'performance_bottlenecks': summary.performance_bottlenecks,
        'schema_insights': summary.schema_insights,
        'query_patterns': summary.query_patterns,
        'recommendations': summary.optimization_recommendations,
        'implementation_priority': [
            '1. Date-based partitioning (High Impact, Medium Effort)',
            '2. Materialized views for aggregations (High Impact, Low Effort)',
            '3. Composite indexes on filter columns (Medium Impact, Low Effort)',
            '4. Storage format optimization (Medium Impact, Medium Effort)'
        ],
        'agent_input': agent_input,
        'design_document': _generate_enhanced_markdown_document(analysis_data, agent_input, summary)
    }


def _determine_target_schema_type(archetype: str, workload: Dict, db_profile: Dict) -> Tuple[str, str]:
    """
    Determines the recommended target schema type based on source archetype and workload.
    Returns: (schema_type, reasoning)
    """
    group_by_cols = workload.get('top_group_by_columns', {})
    filter_cols = workload.get('top_filter_columns', {})

    num_group_by = len(group_by_cols)
    num_filters = len(filter_cols)

    if archetype == "single_big_table":
        # For single table with many GROUP BY columns, recommend star schema
        if num_group_by >= 3:
            return ("star_schema",
                    "The source is a single denormalized table with heavy analytical workload. "
                    "A Star Schema will improve query performance by separating dimensions from facts, "
                    "reduce data redundancy, and enable better indexing and partitioning strategies.")
        else:
            return ("optimized_single_table",
                    "The source is a single table with limited grouping patterns. "
                    "Recommend optimizing the existing structure with partitioning and better file formats "
                    "rather than full restructuring.")

    elif archetype == "normalized_multitable":
        # For normalized schema with many joins, consider denormalization
        total_joins = sum(workload.get('top_joined_tables', {}).values())
        if total_joins > 10000:
            return ("denormalized_fact_table",
                    "The source is highly normalized with frequent join operations. "
                    "Denormalizing into a wider fact table will reduce join overhead and improve query performance.")
        else:
            return ("refined_star_schema",
                    "The source is normalized but join volume is manageable. "
                    "Refine into a clean Star Schema with well-defined dimensions and a central fact table.")

    else:  # denormalized_multitable
        return ("consolidated_star_schema",
                "The source has multiple denormalized tables. "
                "Consolidate into a unified Star Schema to improve consistency and query patterns.")


def _identify_dimension_candidates(workload: Dict, source_tables: List[Dict]) -> Dict[str, List[str]]:
    """
    Identifies columns that should become dimensions based on workload patterns.
    Returns: dict of dimension_name -> [column_names]
    """
    group_by_cols = workload.get('top_group_by_columns', {})

    dimensions = {}

    # Analyze column names to group into logical dimensions
    time_keywords = ['date', 'time', 'year', 'month', 'quarter', 'day', 'week']
    location_keywords = ['origin', 'dest', 'city', 'state', 'country', 'airport', 'location']
    entity_keywords = ['airline', 'carrier', 'company', 'operator', 'customer', 'supplier']

    time_cols = []
    location_cols = []
    entity_cols = []
    other_cols = []

    for col in group_by_cols.keys():
        col_lower = col.lower()
        if any(kw in col_lower for kw in time_keywords):
            time_cols.append(col)
        elif any(kw in col_lower for kw in location_keywords):
            location_cols.append(col)
        elif any(kw in col_lower for kw in entity_keywords):
            entity_cols.append(col)
        else:
            other_cols.append(col)

    if time_cols:
        dimensions['dim_date'] = time_cols
    if location_cols:
        dimensions['dim_location'] = location_cols
    if entity_cols:
        # Infer dimension name from column prefix
        first_entity = entity_cols[0].lower()
        if 'airline' in first_entity or 'carrier' in first_entity:
            dimensions['dim_airline'] = entity_cols
        else:
            dimensions['dim_entity'] = entity_cols

    return dimensions


def _identify_fact_measures(source_tables: List[Dict], group_by_cols: Dict) -> List[str]:
    """
    Identifies numeric columns that should be measures in the fact table.
    """
    if not source_tables:
        return []

    main_table = source_tables[0]
    columns = main_table.get('columns', [])

    # Get all column names that are NOT in group by (dimensions)
    dimension_cols = set(group_by_cols.keys())

    measure_keywords = ['amount', 'total', 'sum', 'count', 'delay', 'time', 'distance', 'duration',
                        'quantity', 'price', 'cost', 'revenue', 'minutes', 'hours']

    numeric_types = ['int', 'integer', 'bigint', 'decimal', 'numeric', 'float', 'double', 'real']

    measures = []
    for col in columns:
        col_name = col.get('name', '')
        col_type = col.get('data_type', '').lower()

        # Skip if it's a dimension column
        if col_name in dimension_cols:
            continue

        # Include if numeric type or has measure keyword in name
        if any(nt in col_type for nt in numeric_types):
            measures.append(col_name)
        elif any(kw in col_name.lower() for kw in measure_keywords):
            measures.append(col_name)

    return measures


def _generate_enhanced_markdown_document(analysis_data: Dict, agent_input: Dict,
                                         summary: OptimizationSummary) -> str:
    """
    ENHANCED: Generates comprehensive markdown design document with schema transformation guidance.
    """
    archetype = agent_input['source_schema_archetype']
    workload = agent_input['workload_profile']
    db_profile = summary.database_profile
    query_patterns = summary.query_patterns

    # Determine target schema strategy
    target_schema, schema_reasoning = _determine_target_schema_type(archetype, workload, db_profile)

    # Identify dimensions and facts - NOW USING agent_input['source_tables']
    dimension_candidates = _identify_dimension_candidates(workload, agent_input['source_tables'])
    fact_measures = _identify_fact_measures(agent_input['source_tables'],
                                            workload.get('top_group_by_columns', {}))

    sections = []

    # ==================== HEADER ====================
    sections.append("# Design Document: Optimized Schema for Analytics Database\n")
    sections.append("Based on analysis of the source schema and query workload, this document outlines "
                    "the proposed design for a new, optimized analytical database.\n")

    # ==================== SECTION 1: SOURCE ANALYSIS ====================
    sections.append("## 1. Source System Analysis\n")
    sections.append(f"*   **Schema Archetype:** `{archetype}`")
    sections.append(f"    *   **Observation:** {schema_reasoning}")

    # Source table profile
    if agent_input['source_tables']:
        sections.append("*   **Source Table Profile:**")
        for table in agent_input['source_tables'][:3]:  # Show top 3 tables
            table_name = table.get('name', 'Unknown')
            col_count = table.get('column_count', 0)
            sections.append(f"    *   **Table:** `{table_name}` ({col_count} columns)")

        if db_profile.get('total_rows_numeric', 0) > 0:
            total_rows_m = db_profile['total_rows_numeric'] / 1_000_000
            sections.append(f"    *   **Total Rows:** ~{total_rows_m:.1f} Million")

        sections.append(f"    *   **Total Size:** {db_profile.get('total_size_gb', 0):.1f} GB")

        if archetype == "single_big_table":
            sections.append("    *   **Key Challenge:** Executing aggregations and filters on a table "
                            "of this size without proper structuring is inefficient and costly.")
    sections.append("")

    # ==================== SECTION 2: WORKLOAD ANALYSIS ====================
    sections.append("## 2. Workload Profile Analysis\n")
    sections.append(
        "The query workload is heavily analytical, focusing on aggregations, filtering, and segmentation.\n")

    # Top grouping columns
    group_by_cols = workload.get('top_group_by_columns', {})
    if group_by_cols:
        sections.append("*   **Top Grouping Columns (Candidates for Dimensions):**")
        for i, (col, count) in enumerate(sorted(group_by_cols.items(),
                                                key=lambda x: x[1], reverse=True)[:5], 1):
            sections.append(f"    {i}.  `{col}` (Used in ~{count / 1000:.1f}k query executions)")
        sections.append("")

    # Top filtering columns
    filter_cols = workload.get('top_filter_columns', {})
    if filter_cols:
        sections.append("*   **Top Filtering Columns (Candidates for Partitioning & Indexing):**")
        for i, (col, count) in enumerate(sorted(filter_cols.items(),
                                                key=lambda x: x[1], reverse=True)[:5], 1):
            sections.append(f"    {i}.  `{col}` (Used in ~{count / 1000:.1f}k query executions)")
        sections.append("")

    # Aggregation patterns
    top_aggs = query_patterns.get('top_aggregations', {})
    if top_aggs:
        sections.append("*   **Common Aggregation Patterns:**")
        for agg, count in list(top_aggs.items())[:3]:
            sections.append(f"    *   `{agg}` - {count:,} executions")
        sections.append("")

    # ==================== SECTION 3: TARGET SCHEMA ====================
    sections.append(f"## 3. Proposed Target Schema: {target_schema.replace('_', ' ').title()}\n")

    if "star" in target_schema.lower():
        sections.append("A **Star Schema** is recommended to separate descriptive attributes (Dimensions) "
                        "from quantitative metrics (Facts). This design optimizes analytical queries and "
                        "improves maintainability.\n")

        # Dimension Tables
        if dimension_candidates:
            sections.append("### 3.1. Dimension Tables\n")
            sections.append("The following dimension tables will store unique, low-cardinality attributes.\n")

            for dim_name, cols in dimension_candidates.items():
                sections.append(f"#### `{dim_name}`")
                sections.append(
                    f"*   **Purpose:** Stores unique information about {dim_name.replace('dim_', '')} attributes.")
                sections.append(f"*   **Source Columns:** {', '.join([f'`{c}`' for c in cols[:5]])}")
                sections.append("*   **Proposed Columns:**")
                sections.append(
                    f"    *   `{dim_name.replace('dim_', '')}_key` (INTEGER, Primary Key, Auto-incrementing)")

                # Add specific columns based on dimension type
                for col in cols[:3]:  # Show first 3 source columns
                    clean_col = col.lower().replace(' ', '_')
                    sections.append(f"    *   `{clean_col}` (VARCHAR)")

                sections.append("")

        # Fact Table
        sections.append("### 3.2. Fact Table\n")
        sections.append("A central fact table will store the quantitative measures and foreign keys to dimensions.\n")

        sections.append("#### `fact_main`")
        sections.append("*   **Purpose:** Granular record of each event/transaction with numeric measures.")
        sections.append("*   **Grain:** One row per event.")
        sections.append("*   **Proposed Columns:**")

        # Foreign keys
        for dim_name in dimension_candidates.keys():
            fk_name = f"{dim_name.replace('dim_', '')}_key"
            sections.append(f"    *   `{fk_name}` (INTEGER, Foreign Key to `{dim_name}`)")

        # Measures
        if fact_measures:
            for measure in fact_measures[:10]:  # Show first 10 measures
                measure_clean = measure.lower().replace(' ', '_')
                sections.append(f"    *   `{measure_clean}` (FLOAT/INTEGER)")

        sections.append("")

    elif "denormalized" in target_schema.lower():
        sections.append("A **Denormalized Fact Table** is recommended to reduce join overhead while "
                        "maintaining analytical query performance.\n")
        sections.append("*   **Strategy:** Pre-join dimension attributes into the fact table")
        sections.append("*   **Trade-off:** Larger table size for faster query execution")
        sections.append("*   **Use Case:** When join performance is the primary bottleneck\n")

    # ==================== SECTION 4: PHYSICAL DESIGN ====================
    sections.append("## 4. Physical Design Recommendations\n")

    # Partitioning
    if filter_cols:
        top_filter = sorted(filter_cols.items(), key=lambda x: x[1], reverse=True)[0]
        sections.append("*   **Partitioning:** The fact table **must be partitioned**.")
        sections.append(f"    *   **Partition Key:** `{top_filter[0]}` "
                        f"({top_filter[1]:,} filter operations)")

        # Determine partition type
        col_lower = top_filter[0].lower()
        if any(kw in col_lower for kw in ['year', 'month', 'date', 'time', 'quarter']):
            sections.append("    *   **Partition Type:** Time-based (RANGE or LIST by year/month)")
            sections.append("    *   **Justification:** Time-based partitioning enables partition pruning "
                            "for the majority of queries, dramatically reducing data scanned.")
        else:
            sections.append("    *   **Partition Type:** LIST or HASH partitioning")
            sections.append("    *   **Justification:** Partitioning by this high-filter column will "
                            "enable partition pruning and improve query performance.")
        sections.append("")

    # File Format
    sections.append("*   **File Format:** Use a columnar format like **Parquet** or **ORC**.")
    sections.append("    *   **Justification:** Columnar formats are optimal for analytical queries "
                    "that read specific columns rather than entire rows. Compression ratios are "
                    "significantly better (40-60% storage savings).")
    sections.append("    *   **Compression:** Use **Snappy** (balanced) or **ZSTD** (higher compression).\n")

    # Indexing (if applicable)
    if dimension_candidates and not target_schema.startswith("denormalized"):
        sections.append("*   **Indexing Strategy:**")
        sections.append("    *   Create indexes on foreign key columns in the fact table for join optimization")

        if filter_cols:
            top_filters = sorted(filter_cols.items(), key=lambda x: x[1], reverse=True)[:3]
            if len(top_filters) > 1:
                filter_names = ', '.join([f'`{f[0]}`' for f in top_filters])
                sections.append(f"    *   Consider composite indexes on frequently filtered columns: {filter_names}")
        sections.append("")

    # Data distribution (for MPP databases)
    if db_profile.get('database_type', '').lower() in ['trino', 'presto', 'redshift', 'snowflake']:
        sections.append("*   **Data Distribution:**")

        if "star" in target_schema.lower() and dimension_candidates:
            sections.append("    *   **Fact Table:** DISTRIBUTE by date key or high-cardinality dimension key")
            sections.append("    *   **Dimension Tables:** REPLICATE (broadcast) for small dimensions")
        else:
            sections.append("    *   **Distribution Key:** Choose high-cardinality column used in joins")

        sections.append("    *   **Justification:** Proper distribution minimizes data movement during joins.\n")

    # ==================== SECTION 5: MIGRATION NOTES ====================
    sections.append("## 5. Implementation Notes\n")

    # Trino-specific notes
    if db_profile.get('database_type', '').lower() in ['trino', 'presto']:
        sections.append("### Trino-Specific Considerations\n")
        sections.append("*   **Materialized Views:** Trino does not support materialized views. "
                        "Use external tools like **dbt** or pre-aggregation tables managed by Airflow.")
        sections.append("*   **Partitioning:** Leverage Hive-style partitioning in the underlying storage (S3, HDFS).")
        sections.append("*   **Table Format:** Consider **Iceberg** or **Delta Lake** for ACID transactions "
                        "and time travel capabilities.\n")

    # General recommendations
    sections.append("### Migration Strategy\n")
    sections.append("1.  **Phase 1:** Create dimension tables and populate with deduplicated data")
    sections.append(
        "    *   ***Clarification:*** In performance-focused strategies like creating a single, wide denormalized table, the data from these dimension tables is merged directly into the main table. In that case, separate `dim_` tables may not be needed in the final optimized schema."
    )
    sections.append("2.  **Phase 2:** Build fact table with foreign key references")
    sections.append("3.  **Phase 3:** Implement partitioning and optimize file formats")
    sections.append("4.  **Phase 4:** Create summary/aggregate tables for common queries")
    sections.append("5.  **Phase 5:** Validate performance and gradually migrate workload\n")

    # Expected improvements
    sections.append("### Expected Performance Improvements\n")
    sections.append("*   **Query Performance:** 30-70% improvement for aggregation queries")
    sections.append("*   **Storage Efficiency:** 40-60% reduction with columnar format and compression")
    sections.append("*   **Maintenance:** Simplified schema updates and easier data quality enforcement")
    sections.append("*   **Scalability:** Better performance scaling with data volume growth\n")

    return "\n".join(sections)


# ========== Keep all existing functions unchanged ==========

def create_summary_tables(optimization_report: Dict) -> Dict[str, pd.DataFrame]:
    """Создает таблицы для удобного просмотра"""
    bottlenecks_data = []
    for bottleneck in optimization_report.get('performance_bottlenecks', []):
        for detail in bottleneck.get('details', [])[:3]:
            bottlenecks_data.append({
                'Issue_Type': bottleneck['type'],
                'Severity': bottleneck['severity'],
                'Query_ID': detail['query_id'][:8] + '...',
                'Execution_Time_Sec': detail.get('execution_time', 0),
                'Run_Quantity': detail.get('run_quantity', 0),
                'Impact_Score': detail.get('impact_score', detail.get('total_time', 0))
            })

    recommendations_data = []
    for i, rec in enumerate(optimization_report.get('recommendations', []), 1):
        recommendations_data.append({
            'Priority': i,
            'Type': rec['type'],
            'Priority_Level': rec['priority'],
            'Description': rec['description'],
            'Expected_Improvement': rec['expected_improvement'],
            'Implementation_Effort': rec['effort']
        })

    query_patterns = optimization_report.get('query_patterns', {})
    mv_candidates = query_patterns.get('materialized_view_candidates', [])

    mv_data = []
    for mv in mv_candidates:
        mv_data.append({
            'Aggregation_Types': ', '.join(mv['aggregations']),
            'Similar_Queries': mv['query_count'],
            'Total_Executions': mv['total_executions'],
            'Avg_Exec_Time_Sec': round(mv['avg_execution_time'], 2),
            'Potential_Savings': mv['potential_savings']
        })

    return {
        'performance_bottlenecks': pd.DataFrame(bottlenecks_data),
        'optimization_recommendations': pd.DataFrame(recommendations_data),
        'materialized_view_candidates': pd.DataFrame(mv_data) if mv_data else pd.DataFrame()
    }


def create_insights_report(ddl_statements: List[Dict], queries: List[Dict]) -> Dict[str, Any]:
    """Create comprehensive schema insights report from DDL and queries."""
    parser = DDLParser()
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

    schema_insights = parser.get_schema_insights(tables, queries)
    stats = parser.get_table_stats(tables)
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
        "denormalization_opportunities": _identify_denormalization_opportunities(tables, queries)
    }


def _analyze_data_quality(tables: List, queries: List[Dict]) -> Dict[str, Any]:
    """Analyze data quality metrics"""
    if not tables:
        return {
            "nullable_columns_percent": 0,
            "tables_without_pk": 0,
            "orphaned_tables": 0,
            "recommendations": []
        }

    total_columns = sum(len(t.columns) for t in tables if hasattr(t, 'columns') and t.columns)
    nullable_columns = sum(1 for t in tables if hasattr(t, 'columns') and t.columns
                           for c in t.columns if hasattr(c, 'nullable') and c.nullable)

    parser = DDLParser()
    tables_without_pk = sum(1 for t in tables if not parser._has_primary_key(t))

    orphaned_tables = 0
    query_text = ' '.join(q.get('query', '').lower() for q in queries if q.get('query'))
    for table in tables:
        table_name = getattr(table, 'name', '').lower()
        if table_name and table_name not in query_text:
            orphaned_tables += 1

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


def _analyze_query_coverage(tables: List, queries: List[Dict]) -> Dict[str, Any]:
    """Analyze table usage in queries"""
    if not queries:
        tbl_names = [t.name for t in tables] if tables else []
        return {
            "table_usage": {name.lower(): 0 for name in tbl_names},
            "unused_tables": tbl_names,
            "most_queried_table": None,
            "most_queried_count": 0
        }

    def normalize(s: str) -> str:
        return re.sub(r'["`]', '', s).strip().lower()

    known_keys = {}
    for t in (tables or []):
        t_name = normalize(getattr(t, 'name', '') or '')
        t_schema = normalize(getattr(t, 'schema', '') or '')
        canon = f"{t_schema}.{t_name}" if t_schema else t_name
        if t_name:
            known_keys[t_name] = canon
        if t_schema and t_name:
            known_keys[f"{t_schema}.{t_name}"] = canon
        db = normalize(getattr(t, 'database', '') or '')
        if db and t_schema and t_name:
            known_keys[f"{db}.{t_schema}.{t_name}"] = canon

    usage = {canon: 0 for canon in set(known_keys.values())}
    ref_re = re.compile(r'\b(from|join)\s+([a-zA-Z0-9_\."]+)', re.IGNORECASE)

    def all_forms(ref: str) -> List[str]:
        ref_norm = normalize(ref)
        parts = [p for p in ref_norm.split('.') if p]
        forms = []
        if parts:
            forms.append(parts[-1])
        if len(parts) >= 2:
            forms.append('.'.join(parts[-2:]))
        if len(parts) >= 3:
            forms.append('.'.join(parts[-3:]))
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
            if not matched:
                forms = all_forms(ref)
                key = forms[1] if len(forms) >= 2 else forms[0] if forms else None
                if key:
                    usage.setdefault(key, 0)
                    usage[key] += runq

    known_canons = set(known_keys.values())
    unused_tables = [k for k in known_canons if usage.get(k, 0) == 0]

    most_queried_table = None
    most_queried_count = 0
    if usage:
        most_queried_table, most_queried_count = max(usage.items(), key=lambda x: x[1])

    usage_sorted = {k: v for k, v in sorted(usage.items(), key=lambda x: x[1], reverse=True)}

    return {
        "table_usage": usage_sorted,
        "unused_tables": unused_tables,
        "most_queried_table": most_queried_table,
        "most_queried_count": most_queried_count
    }


def _identify_partitioning_candidates(tables: List) -> List[Dict[str, Any]]:
    """Identify partitioning candidates"""
    candidates = []
    date_keywords = ['date', 'time', 'year', 'month', 'quarter', 'day', 'timestamp', 'created', 'updated']

    for table in tables:
        if not hasattr(table, 'columns') or not table.columns:
            continue

        table_candidates = []
        for column in table.columns:
            col_name_lower = getattr(column, 'name', '').lower()
            col_type_lower = getattr(column, 'data_type', '').lower()

            if any(keyword in col_name_lower for keyword in date_keywords) or 'date' in col_type_lower:
                table_candidates.append({
                    "column": column.name,
                    "type": column.data_type,
                    "reason": "Date/time column suitable for time-based partitioning"
                })

        if table_candidates:
            table_name = f"{table.schema}.{table.name}" if hasattr(table, 'schema') and table.schema else table.name
            candidates.append({
                "table": table_name,
                "candidates": table_candidates,
                "recommended_strategy": "RANGE or LIST partitioning by date"
            })

    return candidates


def _identify_denormalization_opportunities(tables: List, queries: List[Dict]) -> Dict[str, Any]:
    """Identify denormalization opportunities"""
    if len(tables) <= 1:
        return {
            "opportunity_level": "low",
            "reason": "Single table - already denormalized",
            "recommendations": []
        }

    total_joins = 0
    complex_joins = 0

    for query in queries:
        query_text = query.get('query', '').upper()
        join_count = query_text.count('JOIN')
        total_joins += join_count * query.get('runquantity', 0)
        if join_count > 3:
            complex_joins += query.get('runquantity', 0)

    recommendations = []
    opportunity_level = "low"

    if complex_joins > 1000:
        opportunity_level = "high"
        recommendations.append(
            "High frequency of complex joins detected - consider denormalizing frequently joined tables")
        recommendations.append("Create materialized views for common join patterns")
    elif total_joins > 5000:
        opportunity_level = "medium"
        recommendations.append("Moderate join activity - consider selective denormalization for hot paths")

    return {
        "opportunity_level": opportunity_level,
        "total_join_operations": total_joins,
        "complex_join_queries": complex_joins,
        "recommendations": recommendations
    }