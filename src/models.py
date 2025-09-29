# app/models.py
from sqlalchemy import Column, String, DateTime, JSON, ForeignKey, Integer, Text
from sqlalchemy.orm import relationship
from pydantic import BaseModel, Field
from typing import List
import datetime
from src.database import Base


# --- Database Models ---

class Task(Base):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True, index=True)
    status = Column(String, index=True)
    submitted_at = Column(DateTime, default=datetime.datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    original_input = Column(JSON)
    result = Column(JSON, nullable=True)

    logs = relationship("LogEntry", back_populates="task", cascade="all, delete-orphan")


class LogEntry(Base):
    __tablename__ = "log_entries"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(String, ForeignKey("tasks.id"))
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    level = Column(String)
    message = Column(Text)

    task = relationship("Task", back_populates="logs")

# --- Request Models ---

class DDLStatement(BaseModel):
    statement: str

class QueryStatement(BaseModel):
    queryid: str
    query: str
    runquantity: int

class NewTaskRequest(BaseModel):
    url: str
    ddl: List[DDLStatement]
    queries: List[QueryStatement]

# --- Response Models ---

class NewTaskResponse(BaseModel):
    taskid: str

class TaskStatusResponse(BaseModel):
    status: str # RUNNING, DONE, FAILED

class DDLResultStatement(BaseModel):
    statement: str

class MigrationStatement(BaseModel):
    statement: str

class RewrittenQuery(BaseModel):
    queryid: str
    query: str

class ResultResponse(BaseModel):
    ddl: List[DDLResultStatement]
    migrations: List[MigrationStatement]
    queries: List[RewrittenQuery]

# --- Internal LLM Models ---

class DBOptimizationResponse(BaseModel):
    ddl: str = Field(..., description="A single string of semicolon-separated optimized DDL statements")
    migrations: str = Field(..., description="A single string of semicolon-separated data migration scripts")

class RewrittenQueries(BaseModel):
    queries: list[str] = Field(..., description="List of rewritten SQL queries")