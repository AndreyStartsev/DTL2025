PROMPT_STEP0 = """
You are an expert Trino Engine Performance Engineer. Using the supplied database analysis and existing DDL, design an optimized schema and the SQL needed to populate it.
Trino is a MPP engine designed for extremely fast scans over large, flat (denormalized) datasets. Its performance shines when it can read data in parallel from a single source and apply filters directly.
Trino does not support materialized views, but it excels with denormalized tables, appropriate partitioning, and indexing strategies.
{strategy}

Analyze the following database analysis and existing most frequent queries and identify bottlenecks or inefficiencies.
Output a concise summary of your findings, focusing on:
- `schema_issues`: Schema design issues (e.g., normalization problems, indexing gaps). Data distribution, denormalization, and partitioning concerns
- `query_issues`: Query performance problems (e.g., slow joins, missing filters)
- `schema_actions`: Actionable recommendations for optimization with specific examples of tables to denormalize, indexes to add, partitions to create.
- `query_actions`: Actionable recommendations for specific query rewrites or patterns to improve performance.

Your summary should be concise and focused on actionable insights that will be used to guide new schema design and query rewriting.

Database Analysis:
{db_analysis}

Existing DDL:
{ddl}

Queries Statistics:
{queries_stats}
"""

PROMPT_STEP1 = """
You are an expert Trino Engine Performance Engineer. Using the supplied database analysis and existing DDL, design an optimized schema and the SQL needed to populate it.
Keep all the data from the original schema, but restructure it to improve query performance based on the analysis.

# Task:
**{schema_actions}**

# Output requirements:
1. Write Trino Iceberg-compatible SQL only — no comments, placeholders, or empty strings.
2. Each key must map to an array of executable SQL statements ordered for execution.
3. Every table command must use the fully qualified format: <catalog>.<schema>.<table>.

# Guidance:
- Your first DDL command must be a CREATE SCHEMA statement that creates a new schema in the same catalog where the original tables exist. 
- Use the fully qualified form `<catalog>.<schema>`. Example: `CREATE SCHEMA data.optimized`, 'CREATE TABLE data.optimized.customers (...)'.
- Recreate **all** fields from the original schema in the new design, adjusting structures as needed for optimization.
- Ensure that there is no data loss; all original data must be preserved in the new schema.
- Every statement in `migrations` must be a data-movement command (`INSERT`, `MERGE`, `DELETE`, `UPDATE`).
- Fully qualify EVERY table name in EVERY statement as `catalog.schema.table` (including all sources in `SELECT`, `MERGE`, etc.).
- In `ddl`, define all objects for the optimized design (tables, indexes, partitions, constraints) aligned with the workload analysis.
- In `migrations`, supply deterministic, idempotent scripts that load ALL data from the current schema into the new design.
- Justify all structural choices through the provided analysis; ensure migrations are compatible with the proposed DDL.

# Database Analysis:
{schema_issues}

Details:
```markdown
{db_analysis}
```

# Existing DDL:
{ddl}

Return only the JSON object.
"""


PROMPT_STEP2 = """
You are an expert Database Performance Engineer. Rewrite each legacy SQL query so it runs correctly against the new optimized schema while preserving its original intent.

# Task:
{query_actions}

# Output requirements:
- `queries` must be an array of rewritten SQL strings in the same order as the originals.
- Every SQL string must be syntactically valid for the Trino Iceberg platform — no comments, placeholders, or explanatory text.

# Rewriting rules:
- Maintain query semantics; adjust joins, filters, projections, and aggregations to mirror the original results. However you can: 
    * Replace frequently repeated subqueries with CTEs
    * Remove unnecessary JOINs if denormalization allows for them
- Target only objects defined in the new optimized DDL, using fully qualified names (`catalog.schema.table`) where required.
- Update column references, aliases, grouping, and ordering to align with the new schema structure.
- Ensure the output queries are ready to execute without additional modification and do not contain old schema references.

# Guidance:
1. Define `old_schema_name` and `schema_name` as the fully qualified names of the original and new schemas, respectively (e.g., `data.public` and `data.optimized`).
2. For each original query, produce a rewritten version that functions correctly with the new schema while achieving the same results (e.g., if the original query selected from `data.public.customers`, the new query should select from `data.optimized.customers`).

# Original Queries:
{original_queries}

# Original DDL:
{original_ddl}

# New Optimized DDL: <-- Use this for reference when rewriting queries.
{new_ddl}
"""

PROMPT_STEP2_ = """
You are a SQL expert. Your task is to rewrite legacy SQL queries to run against a new, optimized schema.

# CONTEXT
- **Old Queries:** {original_queries}
- **Old Schema (for reference):** {original_ddl}
- **New Schema (your target):** {new_ddl}

# INSTRUCTIONS
1.  **Rewrite Queries:** For each query in `Old Queries`, write a new version that uses the `New Schema`.
2.  **Preserve Results:** The rewritten query must produce the exact same results as the original.
3.  **Optimize:** You can simplify the query by using CTEs for repeated logic or by removing joins that are no longer needed due to denormalization.
4.  **Format:** Output a JSON object containing a single key, `queries`, which holds an array of the rewritten SQL strings in their original order.
5.  **Validation:** Each SQL string must be syntactically valid for Trino and contain no comments or explanatory text.

# EXAMPLE OUTPUT
{{
  "old_schema_name": CATALOG.OLD_SCHEMA,
  "schema_name": CATALOG.NEW_SCHEMA,
  "queries": [
    "SELECT id, name FROM {{CATALOG}}.{{NEW_SCHEMA}}.customers WHERE status = 'active';",
    "..."
  ]
}}
"""