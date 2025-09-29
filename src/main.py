# app/main.py
import uuid
import difflib
import datetime
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Depends, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from loguru import logger

from src import crud, models
from src.database import SessionLocal, create_db_and_tables
from src.pipeline import run_analysis_pipeline

app = FastAPI(
    title="Database Structure Analysis Service",
    description="An API to asynchronously analyze and optimize database schemas and queries.",
    version="1.0.0"
)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

TASK_TIMEOUT_MINUTES = 20

create_db_and_tables()

app = FastAPI(title="Enhanced Database Analysis Service")

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serves the main HTML user interface."""
    return templates.TemplateResponse("index.html", {"request": request})


# Dependency for getting a DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/new", response_model=models.NewTaskResponse, status_code=202)
def create_new_task(
        request_data: models.NewTaskRequest,
        background_tasks: BackgroundTasks,
        db: Session = Depends(get_db)
):
    task_id = str(uuid.uuid4())
    crud.create_task(db=db, task_id=task_id, request_data=request_data)
    background_tasks.add_task(run_analysis_pipeline, task_id, request_data)
    return models.NewTaskResponse(taskid=task_id)


@app.get("/status", response_model=models.TaskStatusResponse)
def get_task_status(task_id: str = Query(...), db: Session = Depends(get_db)):
    task = crud.get_task(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    # This logic keeps the task but changes its status to FAILED.
    if task.status == "RUNNING":
        elapsed_time = datetime.datetime.now(datetime.UTC) - task.submitted_at
        if elapsed_time > datetime.timedelta(minutes=TASK_TIMEOUT_MINUTES):
            error_result = {"error": f"Task exceeded the {TASK_TIMEOUT_MINUTES}-minute time limit."}
            crud.update_task_status(db, task_id, "FAILED", error_result)
            return models.TaskStatusResponse(status="FAILED")

    return models.TaskStatusResponse(status=task.status)


@app.get("/getresult", response_model=models.ResultResponse)
def get_task_result(task_id: str = Query(...), db: Session = Depends(get_db)):
    task = crud.get_task(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status == "RUNNING":
        raise HTTPException(status_code=202, detail="Task is still running.")
    if task.status == "FAILED":
        raise HTTPException(status_code=500, detail=f"Task failed: {task.result.get('error', 'Unknown error')}")
    if task.status != "DONE":
        raise HTTPException(status_code=400, detail=f"Task is in an invalid state '{task.status}' to fetch results.")

    # --- ASSEMBLE RESPONSE FROM DB COLUMNS ---
    original_queries = task.original_input.get('queries', [])
    rewritten_queries_list = task.rewritten_queries or []

    if len(original_queries) != len(rewritten_queries_list):
         raise HTTPException(status_code=500, detail="Mismatch between original and rewritten query counts.")

    # Reconstruct the final response structure
    response = models.ResultResponse(
        ddl=[models.DDLStatement(statement=s.strip()) for s in (task.optimized_ddl or "").split(';') if s.strip()],
        migrations=[models.DDLStatement(statement=s.strip()) for s in (task.migration_scripts or "").split(';') if s.strip()],
        queries=[
            models.QueryStatement(
                queryid=original_queries[i]['queryid'],
                query=rewritten_query,
                runquantity=original_queries[i]['runquantity']
            )
            for i, rewritten_query in enumerate(rewritten_queries_list)
        ]
    )
    return response


# Task Management

@app.get("/tasks", response_model=List[models.TaskSummary])
def list_tasks(
        status: Optional[str] = Query(None, description="Filter by status: RUNNING, DONE, FAILED"),
        skip: int = 0,
        limit: int = 20,
        db: Session = Depends(get_db)
):
    tasks = crud.get_tasks(db, skip=skip, limit=limit, status=status)
    return [{"taskid": t.id, "status": t.status,
             "submitted_at": str(t.submitted_at), "completed_at": str(t.completed_at)} for t
            in tasks]


@app.delete("/task/{task_id}", status_code=204)
def delete_task(task_id: str, db: Session = Depends(get_db)):
    success = crud.delete_task(db, task_id=task_id)
    if not success:
        raise HTTPException(status_code=404, detail="Task not found")
    return None


@app.get("/task/{task_id}/log", response_model=List[models.LogEntryResponse])
def get_task_log(task_id: str, db: Session = Depends(get_db)):
    task = crud.get_task(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    response_logs = []
    for log_entry in task.logs:
        response_logs.append(
            models.LogEntryResponse(
                timestamp=log_entry.timestamp,
                level=log_entry.level,
                message=log_entry.message
            )
        )
    return response_logs


# Enhanced Results

@app.get("/task/{task_id}/diff", response_model=models.QueryDiffResponse)
def get_query_diffs(task_id: str, db: Session = Depends(get_db)):
    task = crud.get_task(db, task_id)
    if not task or task.status != "DONE":
        raise HTTPException(status_code=404, detail="Completed task with results not found")

    original_queries = {q['queryid']: q['query'] for q in task.original_input.get('queries', [])}
    rewritten_queries = {q['queryid']: q['query'] for q in task.result.get('queries', [])}

    diffs = []
    for queryid, original_query in original_queries.items():
        rewritten_query = rewritten_queries.get(queryid, "")
        diff_text = "\n".join(difflib.unified_diff(
            original_query.splitlines(keepends=True),
            rewritten_query.splitlines(keepends=True),
            fromfile='original',
            tofile='optimized',
        ))
        diffs.append(models.QueryDiff(queryid=queryid, diff=diff_text))

    return models.QueryDiffResponse(diffs=diffs)


# @app.get("/task/{task_id}/analysis")
# def get_analysis_report(task_id: str, db: Session = Depends(get_db)):
#     """Retrieves the DB analysis report for a specific task."""
#     task = crud.get_task(db, task_id)
#     if not task:
#         raise HTTPException(status_code=404, detail="Task not found")
#
#     if not task.db_analysis_report:
#         return {"message": "Analysis report has not been generated yet."}
#
#     return task.db_analysis_report

@app.get("/task/{task_id}/analysis")
def get_analysis_report(task_id: str, db: Session = Depends(get_db)):
    """Retrieves the DB analysis report with visualization data for a specific task."""
    task = crud.get_task(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if not task.db_analysis_report:
        return {"message": "Analysis report has not been generated yet."}

    # Process raw report for visualizations
    raw_report = task.db_analysis_report

    # Prepare visualization data
    viz_data = {
        "raw_report": raw_report,  # Keep original for reference
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
                "top_queries": [
                    {
                        "id": detail['query_id'][:8] + "...",
                        "executions": detail['run_quantity'],
                        "avg_time": detail['execution_time']
                    }
                    for detail in raw_report['performance_bottlenecks'][0]['details'][:5]
                ] if raw_report['performance_bottlenecks'] else [],
                "total_executions": raw_report['performance_bottlenecks'][0]['total_executions'] if raw_report[
                    'performance_bottlenecks'] else 0
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
        }
    }

    return viz_data