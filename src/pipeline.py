# app/pipeline.py
import json
from time import time
from dotenv import load_dotenv, find_dotenv
from langchain_community.vectorstores.aperturedb import BATCHSIZE
from loguru import logger
from sqlalchemy.orm import Session

from src.prompts import PROMPT_STEP1, PROMPT_STEP2
from src.analyzer import DataAnalyzer
from src.database import SessionLocal
from src import crud, schemas
from src.report_creator import create_optimization_report
from src.llm_connector import get_llm, llm_call_with_so_and_fallback
from .models import (
    NewTaskRequest,
    DBOptimizationResponse,
    RewrittenQueries,
)

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


def run_analysis_pipeline(task_id: str, request_data: NewTaskRequest):
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
        llm = get_llm(model_name=config.model_id)
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
        analyzer = DataAnalyzer()
        analysis_result = analyzer.analyze_input_data(input_dict)
        db_analysis_report_str = create_optimization_report(analysis_result)
        log.success(f"[{task_id}] Data analysis completed in {time() - start_time:.2f}s")

        # Save analysis result to the DB
        try:
            crud.update_task_with_analysis(db, task_id, db_analysis_report_str)
            log.info(f"[{task_id}] Saved analysis report to database.")
        except json.JSONDecodeError as e:
            log.warning(f"[{task_id}] Could not parse analysis report as JSON to save to DB: {e}")
            # As a fallback, save the raw string inside a dictionary
            crud.update_task_with_analysis(db, task_id, {"raw_report": db_analysis_report_str})

        # --- Step 2: Generate new DDL & Migrations ---
        prompt1 = PROMPT_STEP1.format(
            db_analysis=db_analysis_report_str,
            ddl=ddl_str,
            strategy=config.strategy
        )

        log.info(f"[{task_id}] Calling LLM ({config.model_id}) for DDL and migration optimization...")
        start_time = time()
        opt_response = llm_call_with_so_and_fallback(llm, prompt1, DBOptimizationResponse)
        log.success(f"[{task_id}] DDL/Migration generation completed in {time() - start_time:.2f}s")

        # Save DDL and migration results to the DB
        crud.update_task_after_step1(db, task_id, opt_response.ddl, opt_response.migrations)
        log.info(f"[{task_id}] Saved optimized DDL and migrations to database.")

        # --- Step 3: Rewrite original queries for the new DDL ---
        log.info(f"[{task_id}] Calling LLM for query rewriting...")
        start_time = time()
        prompt2 = PROMPT_STEP2.format(
            original_queries=queries_str,
            original_ddl=ddl_str,
            new_ddl=opt_response.ddl
        )

        # Handle large number of queries by batching
        BATCH_SIZE = 10  # Define a batch size for processing queries TODO: Make configurable
        queries_list = input_dict.get('queries', [])  # FIX: Use dict version consistently

        if len(queries_list) > BATCH_SIZE:
            log.info(
                f"[{task_id}] Large number of queries detected ({len(queries_list)}). Processing in batches of {BATCH_SIZE}.")
            all_rewritten_queries = []
            for i in range(0, len(queries_list), BATCH_SIZE):
                log.info(f"[{task_id}] Processing queries {i + 1} to {min(i + BATCH_SIZE, len(queries_list))}...")
                batch_queries = queries_list[i:i + BATCH_SIZE]  # FIX: Use queries_list
                batch_queries_str = "\n---\n".join([
                    f"-- Query ID: {q['queryid']}\n-- Runs: {q['runquantity']}\n{q['query']};"
                    for q in batch_queries
                ])
                batch_prompt = PROMPT_STEP2.format(
                    original_queries=batch_queries_str,
                    original_ddl=ddl_str,
                    new_ddl=opt_response.ddl
                )
                batch_response = llm_call_with_so_and_fallback(llm, batch_prompt, RewrittenQueries)
                all_rewritten_queries.extend(batch_response.queries)
            rewrite_response = RewrittenQueries(queries=all_rewritten_queries)
        else:
            rewrite_response = llm_call_with_so_and_fallback(llm, prompt2, RewrittenQueries)

        log.success(f"[{task_id}] Query rewriting completed in {time() - start_time:.2f}s")

        # FIX: Validate BEFORE saving to database
        if len(rewrite_response.queries) != len(queries_list):
            raise ValueError(
                f"LLM returned {len(rewrite_response.queries)} rewritten queries "
                f"but expected {len(queries_list)} queries."
            )

        rewritten_queries_list = rewrite_response.model_dump().get('queries', [])
        crud.update_task_after_step2(db, task_id, rewritten_queries_list)
        log.info(f"[{task_id}] Saved rewritten queries to database.")

        final_result_summary = {"message": "Task completed successfully."}
        crud.update_task_status(db, task_id, "DONE", final_result_summary)
        log.success(f"Task {task_id} finished successfully.")

    except Exception as e:
        log.error(f"Task {task_id} failed: {str(e)}", exc_info=True)
        crud.update_task_status(db, task_id, "FAILED", {"error": str(e)})
    finally:
        db.close()
        log.info(f"[{task_id}] Pipeline finished and database session closed.")
