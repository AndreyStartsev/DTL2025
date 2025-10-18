# app/crud.py
from sqlalchemy.orm import Session
import datetime

from src import models

def get_task(db: Session, task_id: str):
    return db.query(models.Task).filter(models.Task.id == task_id).first()

def get_tasks(db: Session, skip: int = 0, limit: int = 50, status: str = None):
    query = db.query(models.Task)
    if status:
        query = query.filter(models.Task.status == status)
    return query.offset(skip).limit(limit).all()

def get_tasks_with_newest_first(db: Session, skip: int = 0, limit: int = 50, status: str = None):
    query = db.query(models.Task)
    if status:
        query = query.filter(models.Task.status == status)
    return query.order_by(models.Task.submitted_at.desc()).offset(skip).limit(limit).all()

def create_task(db: Session, task_id: str, request_data: models.NewTaskRequest):
    db_task = models.Task(
        id=task_id,
        status="RUNNING",
        original_input=request_data.model_dump()
    )
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    return db_task

def update_task_status(db: Session, task_id: str, status: str, result: dict = None):
    db_task = get_task(db, task_id)
    if db_task:
        db_task.status = status
        db_task.completed_at = datetime.datetime.now(datetime.UTC)
        if result:
            db_task.result = result
        db.commit()

def update_task_with_analysis(db: Session, task_id: str, report: dict):
    db_task = get_task(db, task_id)
    if db_task:
        db_task.db_analysis_report = report
        db.commit()

def update_task_after_step1(db: Session, task_id: str, ddl: str, migrations: str):
    db_task = get_task(db, task_id)
    if db_task:
        db_task.optimized_ddl = ddl
        db_task.migration_scripts = migrations
        db.commit()

def update_task_after_step2(db: Session, task_id: str, queries: list):
    db_task = get_task(db, task_id)
    if db_task:
        db_task.rewritten_queries = queries
        db.commit()

def delete_task(db: Session, task_id: str):
    db_task = get_task(db, task_id)
    if db_task:
        db.delete(db_task)
        db.commit()
        return True
    return False

def create_log_entry(db: Session, task_id: str, level: str, message: str):
    db_log = models.LogEntry(task_id=task_id, level=level, message=message)
    db.add(db_log)
    db.commit()

def get_logs_for_task(db: Session, task_id: str):
    return db.query(models.LogEntry).filter(models.LogEntry.task_id == task_id).all()