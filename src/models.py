# app/models.py
from sqlalchemy import Column, String, DateTime, JSON, ForeignKey, Integer, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from pydantic import BaseModel, Field
from typing import List, Optional
import datetime
from src.database import Base

# Database Models

class Task(Base):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True, index=True)
    status = Column(String, index=True)
    submitted_at = Column(DateTime, default=datetime.datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)

    # Stores the original POST /new request body
    original_input = Column(JSON)

    # Stores the JSON report from the initial analysis step
    db_analysis_report = Column(JSON, nullable=True)
    # Stores the raw string of optimized DDL statements from the LLM
    optimized_ddl = Column(Text, nullable=True)
    # Stores the raw string of migration statements from the LLM
    migration_scripts = Column(Text, nullable=True)
    # Stores the JSON list of rewritten queries from the LLM
    rewritten_queries = Column(JSON, nullable=True)
    # Generic column for storing the final result or error message
    result = Column(JSON, nullable=True)

    logs = relationship("LogEntry", back_populates="task", cascade="all, delete-orphan")


class LogEntry(Base):
    __tablename__ = "log_entries"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(String, ForeignKey("tasks.id"))
    timestamp: datetime = Column(DateTime, server_default=func.now())
    level = Column(String)
    message = Column(Text)
    task = relationship("Task", back_populates="logs")

# Request Models

class DDLStatement(BaseModel):
    statement: str

class QueryStatement(BaseModel):
    queryid: str
    query: str
    runquantity: int

class TaskConfig(BaseModel):
    strategy: str = Field("balanced", description="Optimization strategy: 'read_optimized', 'write_optimized', 'balanced', 'storage_optimized'")
    model_id: str = Field("meta-llama/llama-4-maverick", description="The model ID to use from OpenRouter")
    context_length: Optional[int] = Field(None, description="Optional context length for the model")

class NewTaskRequest(BaseModel):
    url: str
    ddl: List[DDLStatement]
    queries: List[QueryStatement]
    config: TaskConfig = Field(default_factory=TaskConfig)

# Response Models

class NewTaskResponse(BaseModel):
    taskid: str

class TaskStatusResponse(BaseModel):
    status: str # RUNNING, DONE, FAILED

class ResultResponse(BaseModel):
    ddl: List[DDLStatement]
    migrations: List[DDLStatement]
    queries: List[QueryStatement]

# --- NEW: Task Management Response Models ---
class TaskSummary(BaseModel):
    taskid: str
    status: str
    submitted_at: str
    completed_at: Optional[str] = None

class LogEntryResponse(BaseModel):
    timestamp: datetime.datetime
    level: str
    message: str

class QueryDiff(BaseModel):
    queryid: str
    diff: str

class QueryDiffResponse(BaseModel):
    diffs: List[QueryDiff]

# Internal LLM Models

class DBOptimizationResponse(BaseModel):
    ddl: str = Field(..., description="A single string of semicolon-separated optimized DDL statements")
    migrations: str = Field(..., description="A single string of semicolon-separated data migration scripts")

class RewrittenQueries(BaseModel):
    queries: list[str] = Field(..., description="List of rewritten SQL queries")