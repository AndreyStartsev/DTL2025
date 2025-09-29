# app/main.py
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any

from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from loguru import logger

from src import models
from src.pipeline import run_analysis_pipeline

app = FastAPI(
    title="Database Structure Analysis Service",
    description="An API to asynchronously analyze and optimize database schemas and queries.",
    version="1.0.0"
)

# In-memory database to store task status and results.
# For production, use a persistent store like Redis or a proper DB.
tasks: Dict[str, Dict[str, Any]] = {}
TASK_TIMEOUT_MINUTES = 20


@app.post("/new", response_model=models.NewTaskResponse, status_code=202)
def create_new_task(
        request_data: models.NewTaskRequest,
        background_tasks: BackgroundTasks
):
    """
    Accepts a new database analysis task and starts it in the background.
    Returns a unique task ID to track its progress.
    """
    task_id = str(uuid.uuid4())
    logger.info(f"Received new task. Assigning task_id: {task_id}")

    # Store initial task state
    tasks[task_id] = {
        "status": "RUNNING",
        "result": None,
        "start_time": datetime.utcnow()
    }

    # Add the long-running job to the background
    background_tasks.add_task(run_analysis_pipeline, task_id, request_data, tasks)

    return models.NewTaskResponse(taskid=task_id)


@app.get("/status", response_model=models.TaskStatusResponse)
def get_task_status(task_id: str = Query(..., description="The unique ID of the task")):
    """
    Retrieves the current status of a task (RUNNING, DONE, or FAILED).
    """
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    # Check for timeout
    if task["status"] == "RUNNING":
        elapsed_time = datetime.utcnow() - task["start_time"]
        if elapsed_time > timedelta(minutes=TASK_TIMEOUT_MINUTES):
            logger.warning(f"Task {task_id} timed out after {TASK_TIMEOUT_MINUTES} minutes.")
            task["status"] = "FAILED"
            task["result"] = {"error": "Task exceeded the 20-minute time limit."}

    return models.TaskStatusResponse(status=task["status"])


@app.get("/getresult", response_model=models.ResultResponse)
def get_task_result(task_id: str = Query(..., description="The unique ID of the task")):
    """
    Retrieves the results of a completed task.
    Only available if the task status is 'DONE'.
    """
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    status = task["status"]
    if status == "RUNNING":
        raise HTTPException(status_code=202, detail="Task is still running. Please check status again later.")
    elif status == "FAILED":
        error_detail = task.get("result", {}).get("error", "An unknown error occurred.")
        raise HTTPException(status_code=500, detail=f"Task failed: {error_detail}")
    elif status == "DONE":
        return task["result"]
    else:
        raise HTTPException(status_code=500, detail=f"Unknown task status: {status}")


@app.get("/")
def read_root():
    return {"message": "Database Analysis Service is running."}