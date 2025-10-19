# app/models.py
from sqlalchemy import Column, String, DateTime, JSON, ForeignKey, Integer, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict
import datetime

from src.database import Base

# Database Models

class Task(Base):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True, index=True)
    status = Column(String, index=True)
    submitted_at = Column(DateTime, default=datetime.datetime.now(datetime.UTC))
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

class TaskInfoFromDB(BaseModel):
    """All info from DB Task model except logs (for summary endpoints)"""
    id: str
    status: str
    submitted_at: datetime.datetime
    completed_at: Optional[datetime.datetime] = None
    original_input: dict


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
    executiontime: int # in seconds

class TaskConfig(BaseModel):
    strategy: str = Field("balanced", description="Optimization strategy: 'read_optimized', 'write_optimized', 'balanced', 'storage_optimized'")
    model_id: str = Field("meta-llama/llama-4-maverick", description="The model ID to use from OpenRouter")
    context_length: Optional[int] = Field(16000, description="Optional context length for the model")
    batch_size: Optional[int] = Field(5, description="Optional batch size for processing queries")
    use_ollama: Optional[bool] = Field(False, description="Whether to use Ollama as the LLM provider. In this case, model ID is ignored.")

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
    submitted_at: datetime.datetime
    completed_at: Optional[str] = None
    model_id: Optional[str] = None

class LogEntryResponse(BaseModel):
    timestamp: datetime.datetime
    level: str
    message: str

class QueryDiff(BaseModel):
    queryid: str
    diff: str
    original: str
    optimized: str
    original_length: int
    optimized_length: int
    queryid_match: bool

class QueryDiffResponse(BaseModel):
    diffs: List[QueryDiff]
    debug_info: Optional[dict] = None

# Internal LLM Models
class TablePlan(BaseModel):
    table: str
    plan: Literal['recreate_as_is', 'recreate_with_changes', 'split', 'merged']

class DBRecomendationResponse(BaseModel):
    schema_issues: Optional[str] = Field(None, description="Schema design issues")
    query_issues: Optional[str] = Field(None, description="Query performance problems")
    schema_actions: Optional[str] = Field(None, description="Actionable recommendations for schema optimization with examples")
    query_actions: Optional[str] = Field(None, description="Actionable recommendations for query optimization with examples")

class DBOptimizationResponse(BaseModel):
    """Field(..., description="One plan per original table, e.g. 'recreate_as_is', 'recreate_with_changes', 'repartition_only', 'recluster_only', 'auxiliary_optimized'")"""
    catalog_name: Optional[str] = Field(None, description="The name of the Trino database catalog, defined from the original DDL")
    original_tables: List[str] = Field(..., description="Complete list of ALL original tables discovered from the DDL: e.g. ['catalog.schema.table1', 'catalog.schema.table2', ...]")
    original_table_plans: List[TablePlan] = Field(..., description="One plan per original table, e.g. 'recreate_as_is', 'recreate_with_changes', 'split', 'merged'")
    ddl: list[str] = Field(..., description="The optimized DDL statements")
    migrations: list[str] = Field(..., description="The data migration scripts")
    design_note: Optional[str] = Field(None, description="Optional design note explaining the optimization choices")

class RewrittenQueries(BaseModel):
    old_schema_name: Optional[str] = Field(None, description="The name of the ORIGINAL Trino database schema in the format of <catalog>.<schema>, used with FROM and JOIN clauses")
    schema_name: Optional[str] = Field(None, description="The name of the NEW Trino database schema in the format of <catalog>.<schema> to be used with FROM and JOIN clauses")
    queries: list[str] = Field(..., description="List of rewritten SQL queries")