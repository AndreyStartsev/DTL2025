# app/pipeline.py
import json
from time import time
from dotenv import load_dotenv, find_dotenv
from loguru import logger
from sqlalchemy.orm import Session

from src.prompts import PROMPT_STEP1, PROMPT_STEP2
from src.analyzer import DataAnalyzer
from src.database import SessionLocal
from src import crud, models
from src.report_creator import create_optimization_report
from src.dashboard_utils import create_insights_report
from src.llm_connector import get_llm, llm_call_with_so_and_fallback
from src.offline_fallback import fallback_analysis

# Load environment variables once
load_dotenv(find_dotenv())

# DB Logger Sink
def db_log_sink(msg):
    record = msg.record
    # The 'extra' field is where we'll put the task_id
    task_id = record["extra"].get("task_id")
    if task_id:
        db: Session = SessionLocal()
        try:
            crud.create_log_entry(
                db=db,
                task_id=task_id,
                level=record["level"].name,
                message=record["message"]
            )
        finally:
            db.close()

logger.add(db_log_sink, format="{message}", level="INFO")


def run_analysis_pipeline(task_id: str, request_data: models.NewTaskRequest):
    """
    The main long-running task that performs the database analysis and optimization,
    saving intermediate results to the database at each step.
    """
    # Bind the task_id to the logger context for this specific run
    log = logger.bind(task_id=task_id)
    db: Session = SessionLocal()

    try:
        log.info(f"Starting analysis for task_id: {task_id}")
        config = request_data.config

        # --- Step 0: Initial setup ---
        llm = get_llm(model_name=config.model_id, max_tokens=config.context_length)
        input_dict = request_data.model_dump()

        # Prepare DDL and Queries as simple strings for prompts
        ddl_str = ";\n".join([item['statement'] for item in input_dict.get('ddl', [])])
        queries_str = "\n---\n".join([
            f"-- Query ID: {q['queryid']}\n-- Runs: {q['runquantity']}\n{q['query']};"
            for q in input_dict.get('queries', [])
        ])

        # --- Step 1: Analyze DB structure ---
        log.info(f"[{task_id}] Performing initial data analysis...")
        start_time = time()

        try:
            analyzer = DataAnalyzer()
            start_task_time = time()
            analysis_result = analyzer.analyze_input_data(input_dict)
            log.info(f"Data analysis was performed in {time() - start_task_time:.2f}s")
            start_task_time = time()
            db_analysis_report = create_optimization_report(analysis_result)
            log.info(f"Optimization report was created in {time() - start_task_time:.2f}s")

            # Additionally create a human-readable insights report and add as "schema_overview"
            db_insights_report_dict = create_insights_report(input_dict.get('ddl', []), input_dict.get('queries', []))
            db_analysis_report['schema_overview'] = db_insights_report_dict
            log.success(f"✅ [{task_id}] DB analysis completed in {time() - start_time:.2f}s")
        except Exception as e:
            raise e
            log.error(f"[{task_id}] Analysis failed, falling back to offline analysis: {e}", exc_info=True)
            db_analysis_report = fallback_analysis(input_dict.get('queries', []))
            log.success(f"✅ [{task_id}] Fallback DB analysis completed in {time() - start_time:.2f}s")

        # Save analysis result to the DB
        try:
            crud.update_task_with_analysis(db, task_id, db_analysis_report)
            log.info(f"[{task_id}] Saved analysis report to database.")

        except json.JSONDecodeError as e:
            log.warning(f"[{task_id}] Could not parse analysis report as JSON to save to DB: {e}")
            # As a fallback, save the raw string inside a dictionary
            crud.update_task_with_analysis(db, task_id, {"raw_report": db_analysis_report})

        # --- Step 2: Generate new DDL & Migrations ---
        # remove schema_overview from report and agent input before passing to LLM
        # db_agent_input = db_analysis_report.copy()
        # db_agent_input.pop('schema_overview', None)
        # db_agent_input.pop('agent_input', None)

        db_agent_input = db_analysis_report.get('design_document', db_analysis_report)
        log.debug(f"[{task_id}] Agent Input for LLM: {db_agent_input}")
        prompt1 = PROMPT_STEP1.format(
            db_analysis=db_agent_input,
            ddl=ddl_str,
            strategy=config.strategy
        )

        log.info(f"[{task_id}] Calling LLM ({config.model_id}) for DDL and migration optimization...")
        start_time = time()
        opt_response = llm_call_with_so_and_fallback(llm, prompt1, models.DBOptimizationResponse)
        log.success(f"✅ [{task_id}] DDL/Migration generation completed in {time() - start_time:.2f}s")

        ddl_string = "\n".join(opt_response.ddl)
        log.debug(f"[{task_id}] Optimized DDL from LLM:\n{ddl_string}")
        migration_string = "\n".join(opt_response.migrations)
        log.debug(f"[{task_id}] Migration Scripts from LLM:\n{migration_string}")

        log.info(f"✎ [{task_id}] {opt_response.design_note}")

        # Save DDL and migration results to the DB
        # crud.update_task_after_step1(db, task_id, opt_response.ddl, opt_response.migrations)
        crud.update_task_after_step1(db, task_id, ddl_string, migration_string)
        log.info(f"[{task_id}] Saved optimized DDL and migrations to database.")

        # --- Step 3: Rewrite original queries for the new DDL ---
        log.info(f"[{task_id}] Calling LLM for query rewriting...")
        start_time = time()
        prompt2 = PROMPT_STEP2.format(
            original_queries=queries_str,
            original_ddl=ddl_str,
            new_ddl=opt_response.ddl,
            migration_ddl=migration_string
        )

        BATCH_SIZE = config.batch_size  # Define a batch size for processing queries
        queries_list = input_dict.get('queries', [])
        total_queries = len(queries_list)

        if total_queries > BATCH_SIZE:
            log.info(
                f"[{task_id}] Large number of queries detected ({total_queries}). "
                f"Processing in batches of {BATCH_SIZE}."
            )
            all_rewritten_queries = []

            for i in range(0, total_queries, BATCH_SIZE):
                batch_end = min(i + BATCH_SIZE, total_queries)
                batch_num = (i // BATCH_SIZE) + 1
                batch_size = batch_end - i

                log.info(
                    f"[{task_id}] Processing batch {batch_num}: "
                    f"queries {i + 1} to {batch_end} ({batch_size} queries)..."
                )

                batch_queries = queries_list[i:batch_end]
                batch_queries_str = "\n---\n".join([
                    f"-- Query ID: {q['queryid']}\n-- Runs: {q['runquantity']}\n{q['query']};"
                    for q in batch_queries
                ])
                batch_prompt = PROMPT_STEP2.format(
                    original_queries=batch_queries_str,
                    original_ddl=ddl_str,
                    migration_ddl=migration_string,
                    new_ddl=opt_response.ddl
                ) + f"\n\n**IMPORTANT**: You must return EXACTLY {batch_size} rewritten queries, one for each query in this batch."

                batch_response = llm_call_with_so_and_fallback(llm, batch_prompt, models.RewrittenQueries)
                log.debug(f"[{task_id}] ⚠️⚠️⚠️⚠️⚠️ Schema change: {batch_response.old_schema_name} -> {batch_response.schema_name}")
                log.debug(f"[{task_id}] Rewritten Queries in Batch {batch_num}: {batch_response.queries}")

                # VALIDATION: Check batch response count
                if len(batch_response.queries) != batch_size:
                    error_msg = (
                        f"Batch {batch_num} failed: LLM returned {len(batch_response.queries)} "
                        f"rewritten queries but expected {batch_size} queries."
                    )
                    log.error(f"[{task_id}] {error_msg}")
                    raise ValueError(error_msg)

                all_rewritten_queries.extend(batch_response.queries)

            rewrite_response = models.RewrittenQueries(queries=all_rewritten_queries)
        else:
            rewrite_response = llm_call_with_so_and_fallback(llm, prompt2, models.RewrittenQueries)

        log.success(f"✅ [{task_id}] Query rewriting completed in {time() - start_time:.2f}s")

        # FINAL VALIDATION: Ensure total count matches
        if len(rewrite_response.queries) != total_queries:
            # Enhanced debug logging
            log.error(
                f"[{task_id}] Query count mismatch: "
                f"Expected {total_queries}, Got {len(rewrite_response.queries)}"
            )
            for i in range(max(len(queries_list), len(rewrite_response.queries))):
                orig_text = queries_list[i]['query'][:50] + "..." if i < len(queries_list) else "MISSING"
                rewrit_text = rewrite_response.queries[i][:50] + "..." if i < len(
                    rewrite_response.queries) else "MISSING"
                log.debug(
                    f"Query {i + 1}: Original: {orig_text} | Rewritten: {rewrit_text}"
                )

            raise ValueError(
                f"LLM returned {len(rewrite_response.queries)} rewritten queries "
                f"but expected {total_queries} queries."
            )

        rewritten_queries_list = rewrite_response.model_dump().get('queries', [])
        crud.update_task_after_step2(db, task_id, rewritten_queries_list)
        log.info(f"[{task_id}] Saved rewritten queries to database.")

        final_result_summary = {"message": "Task completed successfully."}
        crud.update_task_status(db, task_id, "DONE", final_result_summary)
        log.success(f"Task {task_id} finished successfully.")

    except Exception as e:
        raise e
        log.error(f"Task {task_id} failed: {str(e)}", exc_info=True)
        crud.update_task_status(db, task_id, "FAILED", {"error": str(e)})
    finally:
        db.close()
        log.info(f"[{task_id}] Pipeline finished and database session closed.")
