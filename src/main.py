# app/main.py
import uuid
import difflib
import datetime
import sqlparse
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Depends, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, FileResponse
from sqlalchemy.orm import Session
from loguru import logger

from src import crud, models
from src.database import SessionLocal, create_db_and_tables
from src.pipeline import run_analysis_pipeline
from src.analysis_report import create_analysis_report

# Define tags metadata for grouping endpoints
tags_metadata = [
    {
        "name": "Tasks",
        "description": "Operations for creating and managing database optimization tasks. "
                       "Tasks run asynchronously in the background and process database schemas and queries.",
    },
    {
        "name": "Details",
        "description": "Retrieve optimization info including access detailed analysis reports and performance metrics.",
    },
    {
        "name": "Utilities",
        "description": "Additional utilities for task management, logs, and debugging.",
    },
    {
        "name": "Web UI",
        "description": "Web interface for interacting with the service.",
    },
]

app = FastAPI(
    title="Database Optimizer",
    description="""
An intelligent API service that asynchronously analyzes and optimizes database schemas and SQL queries.

### Features

* **Schema Analysis**: Deep analysis of database structure and relationships
* **Query Optimization**: Automatic rewriting of SQL queries for better performance
* **Migration Scripts**: Generate safe migration scripts for schema changes
* **Performance Insights**: Identify bottlenecks and optimization opportunities
* **Async Processing**: Long-running tasks execute in the background
* **Visual Reports**: Rich analysis reports with performance metrics

### Workflow

1. **Submit** a new optimization task with your database DDL and queries
2. **Monitor** task status using the task ID
3. **Retrieve** optimized results when complete
4. **Review** detailed analysis reports and recommendations

### Task Lifecycle

- `RUNNING`: Task is being processed
- `DONE`: Task completed successfully
- `FAILED`: Task encountered an error

Tasks that exceed {TASK_TIMEOUT_MINUTES} minutes are automatically marked as `FAILED`.
    """,
    version="1.0.0",
    contact={
        "email": "vvirsys@gmail.com"
    },
    openapi_tags=tags_metadata,
    docs_url="/docs",
    redoc_url="/redoc",
)

TASK_TIMEOUT_MINUTES = 20

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
logger.add("logs/app.log", level="INFO", rotation="1 week", serialize=True)
create_db_and_tables()


# Dependency for getting a DB session
def get_db():
    """
    Database session dependency.

    Yields a SQLAlchemy session and ensures proper cleanup after request completion.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get(
    "/",
    response_class=HTMLResponse,
    tags=["Web UI"],
    summary="Main web interface",
    description="Serves the interactive HTML user interface for the Database Optimization Service.",
    include_in_schema=True
)
async def read_root(request: Request):
    """
    Serves the main HTML user interface.

    Access this endpoint in a browser to use the web-based interface for submitting
    and monitoring optimization tasks.
    """
    return templates.TemplateResponse("index.html", {"request": request})


@app.post(
    "/new",
    response_model=models.NewTaskResponse,
    status_code=202,
    tags=["Tasks"],
    summary="Create a new optimization task",
    description="Submit a new database optimization task with DDL and queries. "
                "The task will be processed asynchronously in the background.",
    responses={
        202: {
            "description": "Task successfully created and queued for processing",
            "content": {
                "application/json": {
                    "example": {"taskid": "550e8400-e29b-41d4-a716-446655440000"}
                }
            }
        },
        422: {"description": "Invalid request data"}
    }
)
def create_new_task(
        request_data: models.NewTaskRequest,
        background_tasks: BackgroundTasks,
        db: Session = Depends(get_db)
):
    """
    Create a new database optimization task.

    Submit your database DDL statements and SQL queries for analysis and optimization.
    The service will:

    - Analyze the database schema structure
    - Identify performance bottlenecks
    - Optimize query execution plans
    - Generate migration scripts for schema improvements
    - Rewrite queries for better performance

    **Returns**: A unique task ID to track the optimization progress.

    **Processing Time**: Tasks typically complete within 5-15 minutes depending on complexity.
    """
    task_id = str(uuid.uuid4())
    crud.create_task(db=db, task_id=task_id, request_data=request_data)
    background_tasks.add_task(run_analysis_pipeline, task_id, request_data)
    logger.success(f"Received new task. Assigned task_id: {task_id}")
    return models.NewTaskResponse(taskid=task_id)


@app.get("/task_info/{task_id}", response_model=models.TaskInfoFromDB,
         tags=["Utilities"],
         summary="Get detailed task information",
         )
def get_task_info(
        task_id: str,
        db: Session = Depends(get_db)
):
    """
    Retrieve all information about a specific task except logs.

    Useful for debugging and monitoring task details.
    """
    task = crud.get_task(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return {
        "id": task.id,
        "status": task.status,
        "submitted_at": task.submitted_at,
        "completed_at": task.completed_at,
        "original_input": task.original_input
    }


@app.get(
    "/status",
    response_model=models.TaskStatusResponse,
    tags=["Tasks"],
    summary="Check task status",
    description="Retrieve the current status of an optimization task.",
    responses={
        200: {
            "description": "Task status retrieved successfully",
            "content": {
                "application/json": {
                    "examples": {
                        "running": {"value": {"status": "RUNNING"}},
                        "done": {"value": {"status": "DONE"}},
                        "failed": {"value": {"status": "FAILED"}}
                    }
                }
            }
        },
        404: {"description": "Task not found"}
    }
)
def get_task_status(
        task_id: str = Query(
            ...,
            description="Unique task identifier returned from the `/new` endpoint",
            example="550e8400-e29b-41d4-a716-446655440000"
        ),
        db: Session = Depends(get_db)
):
    """
    Check the status of an optimization task.

    **Status Values**:
    - `RUNNING`: Task is currently being processed
    - `DONE`: Task completed successfully, results are available
    - `FAILED`: Task encountered an error during processing

    **Timeout**: Tasks running longer than {TASK_TIMEOUT_MINUTES} minutes are automatically marked as `FAILED`.

    Use this endpoint to poll for task completion before retrieving results.
    """
    task = crud.get_task(db, task_id)
    if not task:
        logger.warning(f"Task ID {task_id} not found in database.")
        raise HTTPException(status_code=404, detail="Task not found")

    # Check for timeout
    if task.status == "RUNNING":
        try:
            # convert to aware datetime in UTC
            submitted_at = task.submitted_at
            if submitted_at.tzinfo is None:
                submitted_at = submitted_at.replace(tzinfo=datetime.timezone.utc)
            elapsed_time = datetime.datetime.now(datetime.UTC) - submitted_at
            if elapsed_time > datetime.timedelta(minutes=TASK_TIMEOUT_MINUTES):
                error_result = {"error": f"Task exceeded the {TASK_TIMEOUT_MINUTES}-minute time limit."}
                crud.update_task_status(db, task_id, "FAILED", error_result)
                logger.warning(f"Task {task_id} timed out after {TASK_TIMEOUT_MINUTES} minutes.")
                return models.TaskStatusResponse(status="FAILED")
        except Exception as e:
            logger.error(f"Error checking timeout for task {task_id}: {e}")

    logger.info(f"Task ID {task_id} status: {task.status}")
    return models.TaskStatusResponse(status=task.status)


@app.get(
    "/getresult",
    response_model=models.ResultResponse,
    tags=["Tasks"],
    summary="Get optimization results",
    description="Retrieve the complete optimization results for a completed task.",
    responses={
        200: {
            "description": "Results retrieved successfully",
        },
        202: {"description": "Task is still running, results not yet available"},
        404: {"description": "Task not found"},
        500: {"description": "Task failed during processing"}
    }
)
def get_task_result(
        task_id: str = Query(
            ...,
            description="Unique task identifier",
            example="550e8400-e29b-41d4-a716-446655440000"
        ),
        db: Session = Depends(get_db)
):
    """
    Retrieve optimization results for a completed task.

    **Returns**:
    - **DDL Statements**: Optimized database schema definition
    - **Migration Scripts**: Safe migration scripts to implement changes
    - **Queries**: Rewritten SQL queries with improved performance

    **Prerequisites**: Task must be in `DONE` status. Use `/status` endpoint first to verify completion.

    **Error Handling**:
    - Returns 202 if task is still running
    - Returns 500 with error details if task failed
    """
    task = crud.get_task(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status == "RUNNING":
        raise HTTPException(status_code=202, detail="Task is still running.")
    if task.status == "FAILED":
        raise HTTPException(status_code=500, detail=f"Task failed: {task.result.get('error', 'Unknown error')}")
    if task.status != "DONE":
        raise HTTPException(status_code=400, detail=f"Task is in an invalid state '{task.status}' to fetch results.")

    # Assemble response from DB columns
    original_queries = task.original_input.get('queries', [])
    rewritten_queries_list = task.rewritten_queries or []

    if len(original_queries) != len(rewritten_queries_list):
        raise HTTPException(status_code=500, detail="Mismatch between original and rewritten query counts.")

    response = models.ResultResponse(
        ddl=[models.DDLStatement(statement=s.strip()) for s in (task.optimized_ddl or "").split(';') if s.strip()],
        migrations=[models.DDLStatement(statement=s.strip()) for s in (task.migration_scripts or "").split(';') if
                    s.strip()],
        queries=[
            models.QueryStatement(
                queryid=original_queries[i]['queryid'],
                query=rewritten_query,
                runquantity=original_queries[i]['runquantity'],
                executiontime=original_queries[i].get('executiontime', -1)
            )
            for i, rewritten_query in enumerate(rewritten_queries_list)
        ]
    )
    logger.success(f"Results for task ID {task_id} retrieved successfully.")
    return response


@app.get(
    "/tasks",
    response_model=List[models.TaskSummary],
    tags=["Utilities"],
    summary="List all tasks",
    description="Retrieve a paginated list of all optimization tasks with optional status filtering.",
    include_in_schema=True
)
def list_tasks(
        status: Optional[str] = Query(
            None,
            description="Filter tasks by status",
            example="DONE",
            enum=["RUNNING", "DONE", "FAILED"]
        ),
        skip: int = Query(0, ge=0, description="Number of tasks to skip for pagination"),
        limit: int = Query(20, ge=1, le=100, description="Maximum number of tasks to return"),
        order: str = Query("newest", description="Order of tasks by submission time", enum=["newest", "oldest"]),
        db: Session = Depends(get_db)
):
    """
    List all tasks with optional filtering and pagination.

    Useful for monitoring multiple tasks or reviewing historical optimizations.
    """
    if order == "newest":
        tasks = crud.get_tasks_with_newest_first(db, skip=skip, limit=limit, status=status)
    else:
        tasks = crud.get_tasks(db, skip=skip, limit=limit, status=status)
    task_list = [
        {
            "taskid": t.id,
            "status": t.status,
            "submitted_at": str(t.submitted_at),
            "completed_at": str(t.completed_at),
            "model_id": t.original_input.get('config', {}).get('model_id')
        }
        for t in tasks
    ]
    logger.info(f"Listed {len(task_list)} tasks: {task_list}")
    return task_list


@app.delete(
    "/task/{task_id}",
    status_code=204,
    tags=["Utilities"],
    summary="Delete a task",
    description="Permanently delete a task and all associated data.",
    responses={
        204: {"description": "Task successfully deleted"},
        404: {"description": "Task not found"}
    },
    include_in_schema=True
)
def delete_task(
        task_id: str,
        db: Session = Depends(get_db)
):
    """
    Delete a task and all associated data.

    **Warning**: This action is permanent and cannot be undone.
    """
    success = crud.delete_task(db, task_id=task_id)
    if not success:
        raise HTTPException(status_code=404, detail="Task not found")
    logger.success(f"Task ID {task_id} deleted successfully.")
    return None


@app.get(
    "/task/{task_id}/log",
    response_model=List[models.LogEntryResponse],
    tags=["Utilities"],
    summary="Get task logs",
    description="Retrieve detailed execution logs for debugging and monitoring.",
    responses={
        200: {"description": "Logs retrieved successfully"},
        404: {"description": "Task not found"}
    },
    include_in_schema=True
)
def get_task_log(
        task_id: str,
        db: Session = Depends(get_db)
):
    """
    Retrieve detailed execution logs for a task.

    Logs include timestamps, severity levels, and messages for each step of the optimization process.
    Useful for debugging failed tasks or understanding optimization decisions.
    """
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


@app.get(
    "/task/{task_id}/diff",
    response_model=models.QueryDiffResponse,
    tags=["Details"],
    summary="Get query differences",
    description="Compare original and optimized queries with unified diff format.",
    responses={
        200: {"description": "Diffs generated successfully"},
        404: {"description": "Task not found or not completed"}
    },
    include_in_schema=True
)
def get_query_diffs(
        task_id: str,
        db: Session = Depends(get_db)
):
    """
    Get side-by-side comparison of original and optimized queries.
    """
    try:
        task = crud.get_task(db, task_id)
        if not task or task.status != "DONE":
            raise HTTPException(status_code=404, detail="Completed task with results not found")

        # Get original queries (list of dicts with queryid and query)
        original_input = task.original_input or {}
        original_queries_list = original_input.get('queries', [])

        # Get rewritten queries (list of strings)
        rewritten_queries_list = task.rewritten_queries if task.rewritten_queries else []

        diffs = []
        for idx, orig_query_obj in enumerate(original_queries_list):
            queryid = orig_query_obj.get('queryid', f'query_{idx}')
            original_query = orig_query_obj.get('query', '')

            # Get rewritten query by index
            rewritten_query = ''
            if idx < len(rewritten_queries_list):
                rewritten_query = rewritten_queries_list[idx]

            # Format SQL for readability
            formatted_original = sqlparse.format(
                original_query,
                reindent=True,
                keyword_case='upper',
                indent_width=2,
                wrap_after=80
            )

            formatted_rewritten = sqlparse.format(
                rewritten_query,
                reindent=True,
                keyword_case='upper',
                indent_width=2,
                wrap_after=80
            )

            diffs.append(models.QueryDiff(
                queryid=queryid,
                original=formatted_original,
                optimized=formatted_rewritten,
                diff="",  # Not used anymore
                original_length=len(original_query),
                optimized_length=len(rewritten_query),
                queryid_match=bool(rewritten_query)
            ))

        debug_info = {
            "original_count": len(original_queries_list),
            "rewritten_count": len(rewritten_queries_list),
            "formatted": True
        }

        return models.QueryDiffResponse(diffs=diffs, debug_info=debug_info)

    except Exception as e:
        logger.error(f"Error in get_query_diffs: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error generating diffs:{str(e)}"
        )


@app.get(
    "/task/{task_id}/analysis",
    tags=["Details"],
    summary="Get detailed analysis report",
    description="Retrieve comprehensive database analysis with performance metrics and visualization data.",
    responses={
        200: {"description": "Analysis report with visualization data"},
        404: {"description": "Task not found"}
    },
    include_in_schema=True
)
def get_analysis_report(
        task_id: str,
        db: Session = Depends(get_db)
):
    return create_analysis_report(task_id, db)


@app.get(
    "/logs/{admin_key}",
    tags=["Utilities"],
    summary="Get recent application logs",
    description="Retrieve recent application logs for monitoring and debugging.",
    responses={
        200: {"description": "Logs retrieved successfully"}
    },
    include_in_schema=True
)
def get_recent_logs(
        admin_key: str
):
    """
    Retrieve app.log file
    """
    ADMIN_KEY = "test_admin_key"
    if admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized access")
    try:
        log_file_path = "logs/app.log"
        return FileResponse(log_file_path, media_type='application/octet-stream', filename="app.log")
    except Exception as e:
        logger.error(f"Error retrieving log file: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving log file")


@app.get("/debug/task/{task_id}", tags=["Debug"], summary="Debug task data",
         description="Temporary debug endpoint to inspect task data structures.",
         include_in_schema=False)
def debug_task(task_id: str, db: Session = Depends(get_db)):
    """Temporary debug endpoint to inspect task data"""
    task = crud.get_task(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return {
        "task_id": task.id,
        "status": task.status,
        "original_input_type": str(type(task.original_input)),
        "original_input_sample": str(task.original_input)[:200] if task.original_input else None,
        "rewritten_queries_type": str(type(task.rewritten_queries)),
        "rewritten_queries_sample": str(task.rewritten_queries)[:200] if task.rewritten_queries else None,
        "rewritten_queries_is_list": isinstance(task.rewritten_queries, list),
        "rewritten_queries_length": len(task.rewritten_queries) if task.rewritten_queries else 0,
    }