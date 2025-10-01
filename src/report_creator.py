# optimization_summary.py
import json
import re
from typing import Dict, List, Any
from dataclasses import dataclass
import pandas as pd

from src.ddl_parser import DDLParser


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

        # Calculate totals across all tables
        total_tables = len(table_stats)
        total_rows = sum(t.get('row_count', 0) for t in table_stats)
        total_size_bytes = sum(t.get('size_bytes', 0) for t in table_stats)

        return {
            'database_type': overview.get('driver', 'unknown'),
            'table_count': total_tables,
            'total_rows': f"{total_rows:,}",
            'total_size_gb': round(total_size_bytes / (1024 ** 3), 2),
            'total_columns': schema_stats.get('total_columns', 0),
            'column_distribution': schema_stats.get('column_types_distribution', {}),
            'largest_table': {
                'name': table_stats[0].get('table_name', '') if table_stats else '',
                'rows': f"{table_stats[0].get('row_count', 0):,}" if table_stats else '0',
                'size_gb': round(table_stats[0].get('size_bytes', 0) / (1024 ** 3), 2) if table_stats else 0
            } if table_stats else None
        }

    def _identify_bottlenecks(self) -> List[Dict[str, Any]]:
        """Выявляет узкие места производительности"""
        bottlenecks = []

        query_patterns = self.data.get('query_analysis', {}).get('patterns', [])

        # Медленные запросы (>30 сек)
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

        # Высоконагруженные запросы (>1000 выполнений)
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
                           ][:5]  # Топ 5
            })

        return bottlenecks

    def _analyze_schema_structure(self) -> Dict[str, Any]:
        """Анализ структуры схемы для оптимизации"""
        tables = self.data.get('schema_analysis', {}).get('tables', [])

        if not tables:
            return {}

        main_table = tables[0]
        columns = main_table.get('columns', [])

        # Группируем колонки по функциональности
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
                'location': [c['name'] for c in location_columns[:3]],  # Топ 3
                'airline': [c['name'] for c in airline_columns[:2]],  # Топ 2
                'performance': [c['name'] for c in delay_columns[:3]]  # Топ 3
            },
            'denormalization_potential': 'low',  # Уже денормализованная таблица
        }

    def _extract_key_patterns(self) -> Dict[str, Any]:
        """Извлекает ключевые паттерны запросов"""
        query_stats = self.data.get('query_analysis', {}).get('statistics', {})
        patterns = self.data.get('query_analysis', {}).get('patterns', [])

        # Анализ паттернов GROUP BY
        group_by_patterns = {}
        aggregation_patterns = {}

        for pattern in patterns:
            # Подсчет агрегаций
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

        # Группируем запросы по схожим агрегациям
        aggregation_groups = {}

        for pattern in patterns:
            if pattern.get('run_quantity', 0) > 500:  # Только часто выполняемые
                agg_key = tuple(sorted(pattern.get('aggregations', [])))
                if agg_key not in aggregation_groups:
                    aggregation_groups[agg_key] = []
                aggregation_groups[agg_key].append(pattern)

        for agg_key, group_patterns in aggregation_groups.items():
            if len(group_patterns) >= 2:  # Минимум 2 запроса с похожими агрегациями
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

        # Рекомендация по партиционированию
        recommendations.append({
            'type': 'partitioning',
            'priority': 'high',
            'description': 'Implement date-based partitioning',
            'implementation': 'Partition by year/month columns for better query pruning',
            'expected_improvement': 'Query performance improvement 30-70%',
            'effort': 'medium'
        })

        # Рекомендация по материализованным представлениям
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

        # Рекомендация по индексам
        recommendations.append({
            'type': 'indexing',
            'priority': 'medium',
            'description': 'Create composite indexes on frequently filtered columns',
            'implementation': 'Index on origin+dest+flightdate, airline+flightdate combinations',
            'expected_improvement': 'Query performance improvement 20-50%',
            'effort': 'low'
        })

        # Рекомендация по компрессии
        recommendations.append({
            'type': 'compression',
            'priority': 'medium',
            'description': 'Optimize storage format and compression',
            'implementation': 'Use columnar storage with better compression for analytical workload',
            'expected_improvement': 'Storage reduction 40-60%, I/O improvement 20-40%',
            'effort': 'medium'
        })

        return recommendations


def create_optimization_report(analysis_data: Dict) -> Dict[str, Any]:
    """Создает компактный отчет оптимизации"""
    analyzer = OptimizationAnalyzer(analysis_data)
    summary = analyzer.create_compact_summary()

    # Determine optimization potential based on actual database structure
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
        ]
    }


# Функция для создания таблиц в удобном формате
def create_summary_tables(optimization_report: Dict) -> Dict[str, pd.DataFrame]:
    """Создает таблицы для удобного просмотра"""

    # 1. Таблица узких мест производительности
    bottlenecks_data = []
    for bottleneck in optimization_report.get('performance_bottlenecks', []):
        for detail in bottleneck.get('details', [])[:3]:  # Топ 3 из каждой категории
            bottlenecks_data.append({
                'Issue_Type': bottleneck['type'],
                'Severity': bottleneck['severity'],
                'Query_ID': detail['query_id'][:8] + '...',
                'Execution_Time_Sec': detail.get('execution_time', 0),
                'Run_Quantity': detail.get('run_quantity', 0),
                'Impact_Score': detail.get('impact_score', detail.get('total_time', 0))
            })

    # 2. Таблица рекомендаций
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

    # 3. Таблица паттернов запросов
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

# ========== Insights and Reporting ==========
def create_insights_report(ddl_statements: List[Dict], queries: List[Dict]) -> Dict[str, Any]:
    """
    Create comprehensive schema insights report from DDL and queries.

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
        "denormalization_opportunities": _identify_denormalization_opportunities(tables, queries)
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

    parser = DDLParser()
    tables_without_pk = sum(1 for t in tables if not parser._has_primary_key(t))

    # Check for orphaned tables (not referenced in queries)
    orphaned_tables = 0
    query_text = ' '.join(q.get('query', '').lower() for q in queries)
    for table in tables:
        table_name = table.name.lower()
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
    # Each table is referenced by multiple keys: name, schema.name, db.schema.name (if available)
    def normalize(s: str) -> str:
        return re.sub(r'["`]', '', s).strip().lower()

    known_keys = {}  # key -> canonical_name (e.g., 'schema.table' if schema exists else 'table')
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
        # Optionally support db.schema.table if DDLParser provides db (often not)
        db = normalize(getattr(t, 'database', '') or '')
        if db and t_schema and t_name:
            known_keys[f"{db}.{t_schema}.{t_name}"] = canon

    # Initialize usage with known tables (from DDL)
    usage = {canon: 0 for canon in set(known_keys.values())}

    # Regex to extract referenced tables after FROM/JOIN (grabs possibly qualified names)
    ref_re = re.compile(r'\b(from|join)\s+([a-zA-Z0-9_\."]+)', re.IGNORECASE)

    def all_forms(ref: str) -> List[str]:
        # Return possible matching keys to look up in known_keys OR for direct inclusion
        ref_norm = normalize(ref)
        parts = [p for p in ref_norm.split('.') if p]
        forms = []
        if parts:
            # table
            forms.append(parts[-1])
        if len(parts) >= 2:
            forms.append('.'.join(parts[-2:]))  # schema.table
        if len(parts) >= 3:
            forms.append('.'.join(parts[-3:]))  # db.schema.table
        return list(dict.fromkeys(forms))  # dedupe, preserve order

    for q in queries:
        text = (q.get('query') or '')
        runq = int(q.get('runquantity', 0) or 0)
        if not text:
            continue
        # Lower for matching, but keep original to avoid breaking any edge cases
        for _, ref in ref_re.findall(text):
            # Try match against known tables
            matched = False
            for form in all_forms(ref):
                if form in known_keys:
                    usage[known_keys[form]] += runq
                    matched = True
                    break
            # If not matched and we want to show referenced-but-unknown tables, include them
            if not matched:
                # Use the most specific available (schema.table or table)
                forms = all_forms(ref)
                key = forms[1] if len(forms) >= 2 else forms[0] if forms else None
                if key:
                    usage.setdefault(key, 0)
                    usage[key] += runq

    # Compute unused among known tables (those from DDL only)
    known_canons = set(known_keys.values())
    unused_tables = [k for k in known_canons if usage.get(k, 0) == 0]

    # Identify most queried
    most_queried_table = None
    most_queried_count = 0
    if usage:
        most_queried_table, most_queried_count = max(usage.items(), key=lambda x: x[1])

    # Sort usage descending for nicer display
    usage_sorted = {k: v for k, v in sorted(usage.items(), key=lambda x: x[1], reverse=True)}

    return {
        "table_usage": usage_sorted,
        "unused_tables": unused_tables,
        "most_queried_table": most_queried_table,
        "most_queried_count": most_queried_count
    }


def _identify_partitioning_candidates(tables: List) -> List[Dict[str, Any]]:
    """Identify columns suitable for partitioning."""
    candidates = []

    date_keywords = ['date', 'time', 'year', 'month', 'quarter', 'day', 'timestamp', 'created', 'updated']

    for table in tables:
        table_candidates = []
        for column in table.columns:
            col_name_lower = column.name.lower()
            col_type_lower = column.data_type.lower()

            # Check if column name or type suggests it's date-related
            if any(keyword in col_name_lower for keyword in date_keywords) or 'date' in col_type_lower:
                table_candidates.append({
                    "column": column.name,
                    "type": column.data_type,
                    "reason": "Date/time column suitable for time-based partitioning"
                })

        if table_candidates:
            candidates.append({
                "table": f"{table.schema}.{table.name}" if table.schema else table.name,
                "candidates": table_candidates,
                "recommended_strategy": "RANGE or LIST partitioning by date"
            })

    return candidates


def _identify_denormalization_opportunities(tables: List, queries: List[Dict]) -> Dict[str, Any]:
    """Identify opportunities for denormalization based on query patterns."""
    if len(tables) <= 1:
        return {
            "opportunity_level": "low",
            "reason": "Single table - already denormalized",
            "recommendations": []
        }

    # Count joins in queries
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



# Пример использования
if __name__ == "__main__":
    initial_file = "../data/flights.json"
    analysis_file = "../backup/analysis_output.json"
    with open(analysis_file, 'r') as f:
        analysis_data = json.load(f)

    optimization_report = create_optimization_report(analysis_data)
    tables = create_summary_tables(optimization_report)

    print("=== DATABASE OPTIMIZATION SUMMARY ===\n")

    print("Executive Summary:")
    exec_summary = optimization_report['executive_summary']
    print(f"  Database Size: {exec_summary['database_size']} GB")
    print(f"  Total Rows: {exec_summary['total_rows']}")
    print(f"  Critical Issues: {exec_summary['critical_issues']}")
    print(f"  Optimization Potential: {exec_summary['optimization_potential']}\n")

    print("Performance Bottlenecks:")
    if not tables['performance_bottlenecks'].empty:
        print(tables['performance_bottlenecks'].to_string(index=False))
    else:
        print("  No critical bottlenecks identified")
    print()

    print("Optimization Recommendations:")
    print(tables['optimization_recommendations'].to_string(index=False))
    print()

    print("Materialized View Candidates:")
    if not tables['materialized_view_candidates'].empty:
        print(tables['materialized_view_candidates'].to_string(index=False))
    else:
        print("  No suitable candidates identified")
    print()

    print("Implementation Priority:")
    for priority in optimization_report['implementation_priority']:
        print(f"  {priority}")

    with open('../backup/optimization_summary.json', 'w') as f:
        json.dump(optimization_report, f, indent=2, ensure_ascii=False)

    print(f"\nFull report saved to: optimization_summary.json")