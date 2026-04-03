"""Saga pattern for Celery with automatic compensation."""

from celery_saga.core import Saga, saga, step, transform, parallelize
from celery_saga.step import StepResponse, PermanentFailure, saga_step, saga_task
from celery_saga.result import SagaResult, SagaCompensated, SagaFailed
from celery_saga.state import SagaStatus, StepStatus
from celery_saga.executor import set_default_backend

__all__ = [
    # Core API
    "Saga",
    "saga",
    "step",
    "transform",
    "parallelize",
    # Step
    "StepResponse",
    "PermanentFailure",
    "saga_step",
    "saga_task",
    # Result
    "SagaResult",
    "SagaCompensated",
    "SagaFailed",
    # State
    "SagaStatus",
    "StepStatus",
    # Config
    "set_default_backend",
]

__version__ = "0.1.0"
