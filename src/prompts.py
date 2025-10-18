PROMPT_STEP1 = """
You are an expert Database Performance Engineer. Using the supplied database analysis and existing DDL, design an optimized schema and the SQL needed to populate it.

Output requirements:
1. Write Trino Iceberg-compatible SQL only — no comments, placeholders, or empty strings.
2. Each key must map to an array of executable SQL statements ordered for execution.
3. Every table command must use the fully qualified format: <catalog>.<schema>.<table>.

# Schema creation rules:
- Your first DDL command must be a CREATE SCHEMA statement that creates a new schema in the same catalog where the original tables exist. 
(For example, if the original DDL includes a statement like "CREATE TABLE data.company.events...", then the new schema should be created in the "data" catalog; if the original DDL uses "CREATE TABLE analytics.sales.orders...", then create the new schema in the "analytics" catalog.)
- Use the fully qualified form `<catalog>.<schema>`. Example: `CREATE SCHEMA data.optimized`

# Migration rules:
- Every statement in `migrations` must be a data-movement command (`INSERT`, `MERGE`, `DELETE`, `UPDATE`).
- Fully qualify EVERY table name in EVERY statement as `catalog.schema.table` (including all sources in `SELECT`, `MERGE`, etc.).

# Tasks:
- In `ddl`, define all objects for the optimized design (tables, indexes, partitions, constraints) aligned with the workload analysis.
- In `migrations`, supply deterministic, idempotent scripts that load ALL data from the current schema into the new design.
- Justify all structural choices through the provided analysis; ensure migrations are compatible with the proposed DDL.

# Database Analysis:
{db_analysis}

# Existing DDL:
{ddl}

Return only the JSON object.

# Example Output:
{{
  "ddl": [
    "CREATE SCHEMA IF NOT EXISTS {{default_catalog}}.optimized;",
    "CREATE TABLE {{default_catalog}}.optimized.customers (customer_id INT, name VARCHAR, PRIMARY KEY (customer_id)) WITH (format = 'ICEBERG');",
    "... additional DDL statements, all using {{default_catalog}}.optimized.<table> ..."
  ],
  "migrations": [
    "INSERT INTO {{default_catalog}}.optimized.customers (customer_id, name) SELECT c.customer_id, c.name FROM {{default_catalog}}.public.customers c;",
    "... additional migration statements, all fully qualified on both source and target ..."
  ]
}}
"""


PROMPT_STEP2 = """
You are an expert Database Performance Engineer. Rewrite each legacy SQL query so it runs correctly against the new optimized schema while preserving its original intent.

Output requirements:
- `queries` must be an array of rewritten SQL strings in the same order as the originals.
- Every SQL string must be syntactically valid for the Trino Iceberg platform — no comments, placeholders, or explanatory text.

Rewriting rules:
- Maintain query semantics; adjust joins, filters, projections, and aggregations to mirror the original results. However you can: 
    * Replace frequently repeated subqueries with CTEs
    * Remove unnecessary JOINs if denormalization allows for them
    * Push filters to sources
    * Replace CASE/NULLIF with equivalents to improve readability
- Target only objects defined in the new optimized DDL, using fully qualified names (`catalog.schema.table`) where required.
- Update column references, aliases, grouping, and ordering to align with the new schema structure.
- Ensure the output queries are ready to execute without additional modification and do not contain old schema references.

Guidance:
1. Define `old_schema_name` and `schema_name` as the fully qualified names of the original and new schemas, respectively (e.g., `data.public` and `data.optimized`).
2. For each original query, produce a rewritten version that functions correctly with the new schema while achieving the same results (e.g., if the original query selected from `data.public.customers`, the new query should select from `data.optimized.customers`).

# Original Queries:
{original_queries}

# Original DDL:
{original_ddl}

# New Optimized DDL: <-- Use this for reference when rewriting queries.
{new_ddl}
"""