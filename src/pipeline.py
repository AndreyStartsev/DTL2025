# app/pipeline.py
from time import time
from typing import Dict, Any
from dotenv import load_dotenv, find_dotenv
from loguru import logger

from src.prompts import PROMPT_STEP1, PROMPT_STEP2
from src.analyzer import DataAnalyzer
from src.report_creator import create_optimization_report
from src.llm_connector import get_llm, llm_call_with_so
from .models import (
    NewTaskRequest,
    DBOptimizationResponse,
    RewrittenQueries,
    ResultResponse,
    DDLResultStatement,
    MigrationStatement,
    RewrittenQuery
)

# Load environment variables once
load_dotenv(find_dotenv())


def run_analysis_pipeline(task_id: str, request_data: NewTaskRequest, tasks_db: Dict):
    """
    The main long-running task that performs the database analysis and optimization.
    """
    logger.info(f"Starting analysis for task_id: {task_id}")
    try:
        # --- Step 0: Initial setup ---
        llm = get_llm(model_name="google/gemini-2.5-flash")
        input_dict = request_data.model_dump()

        # Prepare DDL and Queries as simple strings for prompts
        ddl_str = ";\n".join([item['statement'] for item in input_dict.get('ddl', [])])
        queries_str = "\n---\n".join([
            f"-- Query ID: {q['queryid']}\n-- Runs: {q['runquantity']}\n{q['query']};"
            for q in input_dict.get('queries', [])
        ])

        # --- Step 1: Analyze DB structure and generate new DDL & Migrations ---
        logger.info(f"[{task_id}] Performing initial data analysis...")
        start_time = time()
        analyzer = DataAnalyzer()
        analysis_result = analyzer.analyze_input_data(input_dict)
        db_analysis_report = create_optimization_report(analysis_result)
        logger.success(f"[{task_id}] Data analysis completed in {time() - start_time:.2f}s")

        prompt1 = PROMPT_STEP1.format(
            db_analysis=db_analysis_report,
            ddl=ddl_str
        )

        logger.info(f"[{task_id}] Calling LLM for DDL and migration optimization...")
        opt_response = llm_call_with_so(llm, prompt1, DBOptimizationResponse)
        logger.success(f"[{task_id}] DDL/Migration generation completed in {time() - start_time:.2f}s")

        # --- Step 2: Rewrite original queries for the new DDL ---
        logger.info(f"[{task_id}] Calling LLM for query rewriting...")
        start_time = time()
        prompt2 = PROMPT_STEP2.format(
            original_queries=queries_str,
            original_ddl=ddl_str,
            new_ddl=opt_response.ddl
        )

        rewrite_response = llm_call_with_so(llm, prompt2, RewrittenQueries)
        logger.success(f"[{task_id}] Query rewriting completed in {time() - start_time:.2f}s")

        # --- Step 3: Format the final response ---
        original_queries = request_data.queries
        if len(rewrite_response.queries) != len(original_queries):
            raise ValueError("LLM did not return the same number of rewritten queries as original queries.")

        final_result = ResultResponse(
            ddl=[DDLResultStatement(statement=s.strip()) for s in opt_response.ddl.split(';') if s.strip()],
            migrations=[MigrationStatement(statement=s.strip()) for s in opt_response.migrations.split(';') if
                        s.strip()],
            queries=[
                RewrittenQuery(queryid=original_queries[i].queryid, query=rewritten_query)
                for i, rewritten_query in enumerate(rewrite_response.queries)
            ]
        )

        # --- Step 4: Update task status and store result ---
        tasks_db[task_id]['status'] = 'DONE'
        tasks_db[task_id]['result'] = final_result.model_dump()
        logger.success(f"Task {task_id} finished successfully.")

    except Exception as e:
        logger.error(f"Task {task_id} failed: {e}")
        tasks_db[task_id]['status'] = 'FAILED'
        tasks_db[task_id]['result'] = {"error": str(e)}