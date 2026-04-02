from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


class PermanentFailure(Exception):
    """Raised to immediately fail a step and trigger compensation."""

    def __init__(self, message: str, compensation_data: Any = None):
        super().__init__(message)
        self.compensation_data = compensation_data


class StepResponse:
    """Return value from a saga step.

    Separates forward output (passed to next steps) from compensation data
    (passed to the compensation function on rollback).
    """

    def __init__(self, output: Any = None, compensation_data: Any = None):
        self.output = output
        self.compensation_data = compensation_data if compensation_data is not None else output

    @staticmethod
    def permanent_failure(message: str, compensation_data: Any = None):
        """Immediately fail this step, skip retries, trigger compensation.

        Use when partial work was done and you need to pass cleanup data.
        e.g. created 3 of 5 records — pass the 3 IDs for rollback.
        """
        raise PermanentFailure(message, compensation_data)


# ── Metadata attached to tasks via @saga_step decorator ──


SAGA_STEP_ATTR = "_saga_step_meta"


@dataclass
class SagaStepMeta:
    compensate: str | Callable | None = None
    no_compensation: bool = False


def saga_step(
    compensate: str | Callable | None = None,
    no_compensation: bool = False,
):
    """Decorator to attach saga metadata to a Celery task.

    @saga_step(compensate="refund_payment")
    @app.task(bind=True)
    def charge_payment(self, order_id, amount):
        ...
    """

    def decorator(fn):
        setattr(fn, SAGA_STEP_ATTR, SagaStepMeta(
            compensate=compensate,
            no_compensation=no_compensation,
        ))
        return fn

    return decorator


# ── StepRef: placeholder returned by step() during saga definition ──


@dataclass
class StepRef:
    """Placeholder returned by step() during saga definition.

    Not a real value — resolved at execution time by the orchestrator.
    """

    step_index: int
    task: Any  # Celery task
    compensate: Any | None = None
    no_compensation: bool = False
    input_ref: Any = None  # StepRef | TransformRef | dict
    input_fn: Callable | None = None


@dataclass
class TransformRef:
    """Placeholder returned by transform() during saga definition."""

    sources: tuple
    transform_fn: Callable | None = None


@dataclass
class ParallelRef:
    """Placeholder returned by parallelize() during saga definition."""

    refs: list[StepRef] = field(default_factory=list)
