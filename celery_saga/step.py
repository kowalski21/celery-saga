from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Callable


def _is_pydantic_model(obj) -> bool:
    """Check if an object is a Pydantic BaseModel instance without importing Pydantic."""
    return hasattr(obj, "model_dump") and hasattr(obj, "model_fields")


def _serialize_if_model(data: Any) -> Any:
    """Convert Pydantic models to dicts for JSON serialization. Pass through everything else."""
    if data is not None and _is_pydantic_model(data):
        return data.model_dump()
    return data


def _get_compensation_type(task) -> type | None:
    """Extract the type annotation for 'compensation_data' from a compensation task."""
    fn = task.run if hasattr(task, "run") else task
    hints = getattr(fn, "__annotations__", {})
    comp_type = hints.get("compensation_data")
    if comp_type is not None and hasattr(comp_type, "model_validate"):
        return comp_type
    return None


def _deserialize_compensation_data(data: Any, task) -> Any:
    """Deserialize compensation data into a Pydantic model if the task has type annotations."""
    if data is None or not isinstance(data, dict):
        return data
    comp_type = _get_compensation_type(task)
    if comp_type is not None:
        return comp_type.model_validate(data)
    return data


class PermanentFailure(Exception):
    """Raised to immediately fail a step and trigger compensation."""

    def __init__(self, message: str, compensation_data: Any = None):
        super().__init__(message)
        self.compensation_data = _serialize_if_model(compensation_data)


class StepResponse:
    """Return value from a saga step.

    Separates forward output (passed to next steps) from compensation data
    (passed to the compensation function on rollback).

    Accepts Pydantic models — they are automatically serialized to dicts
    for storage/transport and deserialized back when passed to the compensation function.
    """

    def __init__(self, output: Any = None, compensation_data: Any = None):
        self.output = _serialize_if_model(output)
        self.compensation_data = _serialize_if_model(compensation_data)

    @staticmethod
    def permanent_failure(message: str, compensation_data: Any = None):
        """Immediately fail this step, skip retries, trigger compensation.

        Use when partial work was done and you need to pass cleanup data.
        e.g. created 3 of 5 records — pass the 3 IDs for rollback.
        """
        raise PermanentFailure(message, compensation_data)


# ── Metadata attached to tasks via @saga_step decorator ──


SAGA_STEP_ATTR = "_saga_step_meta"
SAGA_HOOK_INSTALLED_ATTR = "_saga_call_hook_installed"


@dataclass
class SagaStepMeta:
    compensate: str | Callable | None = None
    no_compensation: bool = False


def _install_saga_call_hook(celery_task) -> None:
    """Make this task auto-register as a saga step when called inside a @saga plan.

    Celery's worker invocation goes through Task.__call__ which delegates to
    self.run(). We wrap __call__ on the task's *class* so that direct calls like
    `my_task(input)` inside an @saga function register a StepRef, while normal
    worker execution (which never sets the plan ContextVar) passes through to
    the original __call__ unchanged.
    """
    cls = type(celery_task)
    if getattr(cls, SAGA_HOOK_INSTALLED_ATTR, False):
        return

    # Late import to avoid a circular dependency (core imports from step).
    from celery_saga.core import _plan_active, auto_register_step

    original_call = cls.__call__

    def saga_aware_call(self, *args, **kwargs):
        if _plan_active() and getattr(self, SAGA_STEP_ATTR, None) is not None:
            return auto_register_step(self, args, kwargs)
        return original_call(self, *args, **kwargs)

    cls.__call__ = saga_aware_call
    setattr(cls, SAGA_HOOK_INSTALLED_ATTR, True)


def _resolve_compensate_name(compensate: str | Callable | None) -> str | None:
    """Resolve a compensate reference to a task name string."""
    if compensate is None:
        return None
    if isinstance(compensate, str):
        return compensate
    if hasattr(compensate, "name"):
        return compensate.name
    return getattr(compensate, "__name__", str(compensate))


def saga_step(
    compensate: str | Callable | None = None,
    no_compensation: bool = False,
):
    """Decorator to attach saga metadata to a Celery task.

    Accepts both string task names and direct callable/task references for compensate.

    @saga_step(compensate=refund_payment)
    @app.task
    def charge_payment(**kwargs):
        ...
    """

    def decorator(fn):
        setattr(fn, SAGA_STEP_ATTR, SagaStepMeta(
            compensate=compensate,
            no_compensation=no_compensation,
        ))
        # fn here is typically a Celery task instance (decorator order: @saga_step
        # over @app.task). Install the call hook if so.
        if hasattr(fn, "name") and hasattr(fn, "apply_async"):
            _install_saga_call_hook(fn)
        return fn

    return decorator


def saga_task(
    app,
    *,
    compensate: str | Callable | None = None,
    no_compensation: bool = False,
    name: str | None = None,
    **task_kwargs,
):
    """Combined decorator: registers a Celery task with saga step metadata.

    @saga_task(app, compensate=refund_payment)
    def charge_payment(**kwargs):
        ...

    Equivalent to:
        @saga_step(compensate=refund_payment)
        @app.task(name="charge_payment")
        def charge_payment(**kwargs):
            ...
    """

    def decorator(fn):
        task_name = name or fn.__qualname__
        celery_task = app.task(name=task_name, **task_kwargs)(fn)
        setattr(celery_task, SAGA_STEP_ATTR, SagaStepMeta(
            compensate=compensate,
            no_compensation=no_compensation,
        ))
        _install_saga_call_hook(celery_task)
        return celery_task

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
