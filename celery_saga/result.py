from __future__ import annotations

import time
from typing import Any

from celery_saga.backends.base import SagaBackend
from celery_saga.state import SagaExecution, SagaStatus, StepStatus


TERMINAL_STATUSES = {SagaStatus.COMPLETED, SagaStatus.COMPENSATED, SagaStatus.FAILED}


class SagaResult:
    """Handle to a running or completed saga execution."""

    def __init__(self, saga_id: str, backend: SagaBackend):
        self.saga_id = saga_id
        self._backend = backend

    def _load(self) -> SagaExecution | None:
        return self._backend.load(self.saga_id)

    @property
    def status(self) -> SagaStatus | None:
        execution = self._load()
        return execution.status if execution else None

    @property
    def steps(self) -> list[dict] | None:
        execution = self._load()
        if not execution:
            return None
        return [
            {
                "step_index": s.step_index,
                "task_name": s.task_name,
                "status": s.status.value,
                "error": s.error,
            }
            for s in execution.steps
        ]

    @property
    def context(self) -> dict | None:
        execution = self._load()
        return execution.context if execution else None

    @property
    def is_complete(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def get(self, timeout: float = None, interval: float = 0.5) -> dict:
        """Block until the saga completes and return the accumulated context.

        Raises RuntimeError if the saga was compensated or failed.
        """
        start = time.monotonic()
        while True:
            execution = self._load()
            if not execution:
                raise RuntimeError(f"Saga {self.saga_id} not found")

            if execution.status == SagaStatus.COMPLETED:
                return execution.context

            if execution.status == SagaStatus.COMPENSATED:
                failed_steps = [
                    s for s in execution.steps if s.status == StepStatus.FAILED
                ]
                errors = "; ".join(f"{s.task_name}: {s.error}" for s in failed_steps)
                raise SagaCompensated(
                    f"Saga {execution.saga_name} was compensated. Failures: {errors}",
                    execution=execution,
                )

            if execution.status == SagaStatus.FAILED:
                raise SagaFailed(
                    f"Saga {execution.saga_name} failed (compensation also failed)",
                    execution=execution,
                )

            if timeout and (time.monotonic() - start) >= timeout:
                raise TimeoutError(
                    f"Saga {self.saga_id} did not complete within {timeout}s"
                )

            time.sleep(interval)

    def __repr__(self):
        return f"<SagaResult saga_id={self.saga_id} status={self.status}>"


class SagaCompensated(Exception):
    """Raised when a saga was rolled back successfully."""

    def __init__(self, message: str, execution: SagaExecution):
        super().__init__(message)
        self.execution = execution


class SagaFailed(Exception):
    """Raised when a saga failed and compensation also failed."""

    def __init__(self, message: str, execution: SagaExecution):
        super().__init__(message)
        self.execution = execution
