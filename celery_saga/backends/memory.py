from __future__ import annotations

from celery_saga.backends.base import SagaBackend
from celery_saga.state import SagaExecution


class MemorySagaBackend(SagaBackend):
    """In-memory saga state backend. Useful for testing."""

    def __init__(self):
        self._store: dict[str, SagaExecution] = {}
        self._idem: dict[str, str] = {}

    def save(self, execution: SagaExecution) -> None:
        self._store[execution.saga_id] = execution
        if execution.idempotency_key:
            self._idem[execution.idempotency_key] = execution.saga_id

    def load(self, saga_id: str) -> SagaExecution | None:
        return self._store.get(saga_id)

    def delete(self, saga_id: str) -> None:
        execution = self._store.pop(saga_id, None)
        if execution and execution.idempotency_key:
            self._idem.pop(execution.idempotency_key, None)

    def find_by_idempotency_key(self, key: str) -> SagaExecution | None:
        saga_id = self._idem.get(key)
        if saga_id is None:
            return None
        return self.load(saga_id)

    def list_all(self) -> list[SagaExecution]:
        return list(self._store.values())
