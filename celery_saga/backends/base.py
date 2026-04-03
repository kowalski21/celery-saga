from __future__ import annotations

from abc import ABC, abstractmethod

from celery_saga.state import SagaExecution


class SagaBackend(ABC):
    """Abstract base for saga state persistence."""

    @abstractmethod
    def save(self, execution: SagaExecution) -> None:
        ...

    @abstractmethod
    def load(self, saga_id: str) -> SagaExecution | None:
        ...

    @abstractmethod
    def delete(self, saga_id: str) -> None:
        ...

    @abstractmethod
    def find_by_idempotency_key(self, key: str) -> SagaExecution | None:
        ...

    def list_all(self) -> list[SagaExecution]:
        """List all saga executions. Override for efficient backend-specific implementation."""
        return []
