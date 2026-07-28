"""Request/response models for the API."""

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class TaskNode(BaseModel):
    id: str = Field(min_length=1)
    depends_on: list[str] = Field(default_factory=list)
    max_retries: int | None = Field(default=None, ge=0)


class WorkflowCreate(BaseModel):
    name: str = Field(min_length=1)
    workflow: list[TaskNode] = Field(min_length=1)


class WorkflowOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    definition: dict
    created_at: datetime


class TaskOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    run_id: uuid.UUID
    task_name: str
    depends_on: list[str]
    status: str
    retry_count: int
    max_retries: int
    worker_id: str | None
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None


class RunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    workflow_id: uuid.UUID
    status: str
    triggered_at: datetime
    completed_at: datetime | None


class RunDetail(RunOut):
    tasks: list[TaskOut]
    runnable_now: list[str]
