PROMPT_STEP1 = """
You are an expert Database Performance Engineer. Your task is to analyze a database info to recommend performance optimizations.

Based on the analysis, you will propose a new schema and data migration scripts.

**Constraint Checklist:**
1.  Your ENTIRE output must be a single, valid JSON object.
2.  The JSON must have three top-level keys: `ddl`, `migrations`, `queries`.
3.  The first DDL statement **MUST** be `CREATE SCHEMA`. Use a new, descriptive schema name like `{{default_catalog}}_optimized`.
4.  All table names in all generated SQL statements (DDL, migrations) **MUST** be fully qualified: `catalog.schema.table`.

Based on all the information above, provide an optimization plan. 
Generate the necessary SQL scripts to perform this optimization, following all constraints.

# Database Analysis:
{db_analysis}

# Existing DDL:
{ddl}
    """


PROMPT_STEP2 = """
You are an expert Database Performance Engineer. Your task is to rewrite old SQL queries to work with a new optimized database schema.
You will be provided with:
1. The original SQL queries.
2. The original database schema (DDL).
3. The new optimized database schema (DDL).

**Constraint Checklist:**
1.  Your ENTIRE output must be a single, valid JSON object.
2.  The JSON must have a single top-level key: `queries`.
3.  The value of `queries` must be a list of rewritten SQL queries.
4.  Each rewritten SQL query must be syntactically correct and compatible with the new schema.

Based on the information provided, rewrite the original SQL queries to be compatible with the new optimized schema.

# Original Queries:
{original_queries}

# Original DDL:
{original_ddl}

# New Optimized DDL:
{new_ddl}

    """