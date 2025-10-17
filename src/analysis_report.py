from fastapi import HTTPException, Depends
from sqlalchemy.orm import Session

from src import crud


def create_analysis_report(
        task_id: str,
        db: Session
):
    """
    Retrieve the comprehensive database analysis report.

    **Includes**:
    - **Executive Summary**: High-level metrics and optimization potential
    - **Schema Insights**: Column distributions and data types
    - **Query Patterns**: Join frequency, aggregations, and common patterns
    - **Performance Bottlenecks**: Slow queries and high-volume operations
    - **Recommendations**: Prioritized optimization suggestions with effort estimates
    - **Materialized View Candidates**: Opportunities for query caching

    **Visualization Data**: Structured data ready for charts and graphs including:
    - Column distribution pie charts
    - Query performance metrics
    - Join pattern analysis
    - Priority matrices for recommendations

    Perfect for generating dashboards and detailed performance reports.
    """
    task = crud.get_task(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if not task.db_analysis_report:
        return {"message": "Analysis report has not been generated yet."}

    raw_report = task.db_analysis_report

    # Helper functions to safely extract data
    def get_bottleneck_by_type(bottleneck_type: str):
        """Find a specific bottleneck by type."""
        bottlenecks = raw_report.get('performance_bottlenecks', [])
        return next((b for b in bottlenecks if b.get('type') == bottleneck_type), None)

    def get_top_queries():
        """Get top queries from slow_queries or high_volume_queries."""
        # First try high_volume_queries since that's what the chart shows
        high_volume = get_bottleneck_by_type('high_volume_queries')
        if high_volume and 'details' in high_volume:
            return [
                {
                    "id": detail.get('query_id', 'Unknown')[:8] + "...",
                    "executions": detail.get('run_quantity', 0),
                    "avg_time": detail.get('execution_time', 'N/A')
                }
                for detail in high_volume['details'][:5]
            ]

        # Fallback to slow_queries if high_volume not available
        slow_queries = get_bottleneck_by_type('slow_queries')
        if slow_queries and 'details' in slow_queries:
            return [
                {
                    "id": detail.get('query_id', 'Unknown')[:8] + "...",
                    "executions": detail.get('run_quantity', 0),
                    "avg_time": detail.get('execution_time', 'N/A')
                }
                for detail in slow_queries['details'][:5]
            ]

        return []

    def get_total_executions():
        """Get total executions from high_volume_queries."""
        high_volume = get_bottleneck_by_type('high_volume_queries')
        return high_volume.get('total_executions', 0) if high_volume else 0

    # Get schema_overview from raw_report (where we stored it during processing)
    schema_overview_data = raw_report.get('schema_overview', {})

    # Agent input
    # {
    #     'source_schema_archetype': archetype,
    #     # one of 'single_big_table', 'normalized_multitable', 'denormalized_multitable'
    #     'source_tables_profile': source_tables,
    #     # list of {'name', 'row_count', 'columns': [{'name', 'type', 'cardinality'}]}
    #     'workload_profile': workload_profile
    #     # dict with keys: top_group_by_columns, top_filter_columns, top_joined_tables, top_aggregated_functions
    # }
    agent_input = raw_report.get('agent_input', '')


    from loguru import logger
    # logger.info(f"🟢🟢🟢🟢🟢 {schema_overview_data}")

    # Fallback to old schema_insights if schema_overview doesn't exist
    if not schema_overview_data or isinstance(schema_overview_data, str):
        # Old format or error message - try to build from schema_insights
        schema_insights = raw_report.get('schema_insights', {})
        schema_overview_data = {
            "tables": [
                {
                    "name": table.get('name', 'Unknown'),
                    "column_count": table.get('column_count', 0),
                    "estimated_rows": table.get('estimated_rows', 0),
                    "has_primary_key": table.get('has_primary_key', False)
                }
                for table in schema_insights.get('tables', [])
            ],
            "index_coverage": schema_insights.get('index_coverage', {
                "indexed_tables": 0,
                "total_indexes": 0,
                "coverage_percent": 0,
                "recommendations": "No index data available"
            })
        }

    viz_data = {
        "raw_report": raw_report,
        "schema_overview": schema_overview_data,
        "visualizations": {
            "executive_summary": {
                "metrics": [
                    {"label": "Database Size", "value": f"{raw_report['executive_summary']['database_size']} GB",
                     "icon": "database"},
                    {"label": "Total Rows", "value": raw_report['executive_summary']['total_rows'], "icon": "table"},
                    {"label": "Daily Queries", "value": f"{raw_report['executive_summary']['query_volume_per_day']:,}",
                     "icon": "search"},
                    {"label": "Critical Issues", "value": raw_report['executive_summary']['critical_issues'],
                     "icon": "exclamation-triangle", "alert": raw_report['executive_summary']['critical_issues'] > 0}
                ],
                "optimization_potential": raw_report['executive_summary']['optimization_potential']
            },
            "column_distribution": {
                "labels": list(raw_report['database_profile']['column_distribution'].keys()),
                "data": list(raw_report['database_profile']['column_distribution'].values()),
                "total_columns": raw_report['schema_insights']['total_columns']
            },
            "query_performance": {
                "top_queries": get_top_queries(),
                "total_executions": get_total_executions()
            },
            "aggregation_usage": {
                "labels": list(raw_report['query_patterns']['top_aggregations'].keys()),
                "data": list(raw_report['query_patterns']['top_aggregations'].values())
            },
            "join_patterns": {
                "labels": list(raw_report['query_patterns']['join_frequency'].keys()),
                "data": list(raw_report['query_patterns']['join_frequency'].values())
            },
            "recommendations": {
                "priority_matrix": [
                    {
                        "name": rec['type'].replace('_', ' ').title(),
                        "priority": rec['priority'],
                        "effort": rec['effort'],
                        "improvement": rec['expected_improvement'],
                        "description": rec['description']
                    }
                    for rec in raw_report['recommendations']
                ],
                "implementation_order": raw_report['implementation_priority']
            },
            "materialized_views": [
                {
                    "aggregations": ", ".join(mv['aggregations']),
                    "executions": mv['total_executions'],
                    "savings": mv['potential_savings'],
                    "queries": mv['query_count']
                }
                for mv in raw_report['query_patterns']['materialized_view_candidates'][:3]
            ]
        },
        "agent_input": agent_input
    }

    return viz_data