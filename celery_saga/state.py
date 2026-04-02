from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


class SagaStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPENSATING = "compensating"
    COMPLETED = "completed"
    COMPENSATED = "compensated"
    FAILED = "failed"


class StepStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    SKIPPED = "skipped"


@dataclass
class StepExecution:
    step_index: int
    task_name: str
    task_id: str | None = None
    status: StepStatus = StepStatus.PENDING
    output: dict[str, Any] | None = None
    compensation_data: Any | None = None
    compensation_task_name: str | None = None
    compensation_task_id: str | None = None
    no_compensation: bool = False
    error: str | None = None
    parallel_group: int | None = None

    def to_dict(self) -> dict:
        return {
            "step_index": self.step_index,
            "task_name": self.task_name,
            "task_id": self.task_id,
            "status": self.status.value,
            "output": self.output,
            "compensation_data": self.compensation_data,
            "compensation_task_name": self.compensation_task_name,
            "compensation_task_id": self.compensation_task_id,
            "no_compensation": self.no_compensation,
            "error": self.error,
            "parallel_group": self.parallel_group,
        }

    @classmethod
    def from_dict(cls, data: dict) -> StepExecution:
        data = data.copy()
        data["status"] = StepStatus(data["status"])
        return cls(**data)


@dataclass
class SagaExecution:
    saga_id: str
    saga_name: str
    status: SagaStatus = SagaStatus.PENDING
    idempotency_key: str | None = None
    input_data: dict[str, Any] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)
    steps: list[StepExecution] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def touch(self):
        self.updated_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return {
            "saga_id": self.saga_id,
            "saga_name": self.saga_name,
            "status": self.status.value,
            "idempotency_key": self.idempotency_key,
            "input_data": self.input_data,
            "context": self.context,
            "steps": [s.to_dict() for s in self.steps],
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> SagaExecution:
        data = data.copy()
        data["status"] = SagaStatus(data["status"])
        data["steps"] = [StepExecution.from_dict(s) for s in data["steps"]]
        return cls(**data)
