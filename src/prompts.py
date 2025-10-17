PROMPT_STEP1 = """
You are an expert Database Performance Engineer. Using the supplied database analysis and existing DDL, design an optimized schema and the SQL needed to populate it.

Output requirements:
1. Emit a single JSON object with exactly two keys: `ddl` and `migrations`.
2. Each key must map to an array of executable SQL statements ordered for execution.
3. The first statement in `ddl` must be `CREATE SCHEMA` with a descriptive name such as `{{default_catalog}}_optimized`. Then define tables, indexes, partitions, and constraints within that schema.
4. Create tables with PRIMARY KEY and FOREIGN KEY constraints as appropriate to enforce data integrity.
5. Every statement in `migrations` must be a data-movement command (`INSERT`, `MERGE`, `DELETE`, `UPDATE`).
6. Fully qualify every table name in every statement as `catalog.schema.table`.
7. Produce valid SQL only—no comments, placeholders, or empty strings.

Tasks:
- In `ddl`, define all objects for the optimized design (tables, indexes, partitions, constraints) aligned with the workload analysis.
- In `migrations`, supply deterministic, idempotent scripts that load **ALL** data from the current schema into the new design.
- Justify all structural choices through the provided analysis; ensure migrations are compatible with the proposed DDL.

# Database Analysis:
{db_analysis}

# Existing DDL:
{ddl}

Return only the JSON object.

# Example Output:
{{
  "ddl": [
    "CREATE SCHEMA default_catalog_optimized;",
    "CREATE TABLE default_catalog_optimized.customers (customer_id INT PRIMARY KEY, name VARCHAR(100), ...);",
    "... additional DDL statements ..."
  ],
  "migrations": [
    "INSERT INTO default_catalog_optimized.customers (customer_id, name, ...) SELECT customer_id, name, ... FROM default_catalog.public.customers;",
    "... additional migration statements ..."
  ]
}}
"""


PROMPT_STEP2 = """
You are an expert Database Performance Engineer. Rewrite each legacy SQL query so it runs correctly against the new optimized schema while preserving its original intent.

Output requirements:
1. Return exactly one JSON object with the top-level key `queries`.
2. `queries` must be an array of rewritten SQL strings in the same order as the originals.
3. Every SQL string must be syntactically valid for the target platform—no comments, placeholders, or explanatory text.

Rewriting rules:
- Maintain query semantics; adjust joins, filters, projections, and aggregations to mirror the original results.
- Target only objects defined in the new optimized DDL, using fully qualified names (`catalog.schema.table`) where required.
- Update column references, aliases, grouping, and ordering to align with the new schema structure.
- Ensure the output queries are ready to execute without additional modification.

# Original Queries:
{original_queries}

# Original DDL:
{original_ddl}

# New Optimized DDL:
{new_ddl}
"""