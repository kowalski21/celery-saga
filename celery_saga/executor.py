from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable

from celery import shared_task, current_app, group, chain, chord

from celery_saga.backends.base import SagaBackend
from celery_saga.backends.memory import MemorySagaBackend
from celery_saga.core import SagaPlan, deserialize_callable
from celery_saga.result import SagaResult
from celery_saga.state import (
    SagaExecution,
    SagaStatus,
    StepExecution,
    StepStatus,
)
from celery_saga.step import (
    ChildSagaCompensated,
    PermanentFailure,
    StepResponse,
    _deserialize_compensation_data,
)

logger = logging.getLogger(__name__)

# Module-level default backend (can be overridden)
_default_backend: SagaBackend | None = None

def get_backend(backend=None) -> SagaBackend:
    global _default_backend
    if backend is not None:
        return backend
    if _default_backend is None:
        _default_backend = MemorySagaBackend()
    return _default_backend


def set_default_backend(backend: SagaBackend):
    global _default_backend
    _default_backend = backend


def execute_saga(
    saga_id: str,
    plan: SagaPlan,
    input_data: dict,
    backend: SagaBackend | None = None,
    idempotency_key: str | None = None,
    queue: str | None = None,
    transforms: list | None = None,
) -> SagaResult:
    """Build and dispatch a saga as Celery tasks."""
    backend = get_backend(backend)

    # Check idempotency
    if idempotency_key:
        existing = backend.find_by_idempotency_key(idempotency_key)
        if existing and existing.status in (SagaStatus.RUNNING, SagaStatus.COMPLETED):
            return SagaResult(existing.saga_id, backend)

    # Flatten plan into step descriptors
    descriptors = plan.flatten()

    # Create execution record
    step_executions = []
    for desc in descriptors:
        step_executions.append(StepExecution(
            step_index=desc["step_index"],
            task_name=desc["task_name"],
            compensation_task_name=desc["compensate_task_name"],
            no_compensation=desc["no_compensation"],
            parallel_group=desc["parallel_group"],
            input_spec=desc.get("input_spec"),
            input_mapper=desc.get("input_mapper"),
            child_saga_name=desc.get("child_saga_name"),
        ))

    execution = SagaExecution(
        saga_id=saga_id,
        saga_name=plan.name,
        status=SagaStatus.PENDING,
        idempotency_key=idempotency_key,
        input_data=input_data,
        context=dict(input_data),
        steps=step_executions,
        transforms=transforms or [],
    )
    backend.save(execution)

    # Build the Celery task chain
    task_options = {}
    if queue:
        task_options["queue"] = queue

    # Group steps by execution order (parallel groups execute together)
    ordered_groups = _build_execution_order(descriptors)

    # Build chain: start → [step_groups with checkpoints] → complete
    chain_tasks = [
        saga_start.si(saga_id).set(**task_options),
    ]

    for exec_group in ordered_groups:
        if len(exec_group) == 1:
            desc = exec_group[0]
            chain_tasks.append(
                saga_run_step.si(saga_id, desc["step_index"]).set(**task_options)
            )
        else:
            parallel_tasks = group(
                saga_run_step.si(saga_id, desc["step_index"]).set(**task_options)
                for desc in exec_group
            )
            chain_tasks.append(
                chord(parallel_tasks, saga_parallel_done.si(saga_id).set(**task_options))
            )

    chain_tasks.append(
        saga_complete.si(saga_id).set(**task_options)
    )

    # Link error handler for compensation
    workflow = chain(*chain_tasks)

    is_eager = current_app.conf.task_always_eager
    if is_eager:
        # In eager mode, errors propagate directly — catch and compensate
        try:
            workflow.apply()
        except Exception:
            saga_on_error.apply(args=(saga_id,))
    else:
        workflow.apply_async(
            link_error=saga_on_error.si(saga_id).set(**task_options),
        )

    return SagaResult(saga_id, backend)


def _build_execution_order(descriptors: list[dict]) -> list[list[dict]]:
    """Group descriptors into execution order. Parallel steps are grouped together."""
    groups = []
    current_parallel = None
    current_group = []

    for desc in descriptors:
        pg = desc["parallel_group"]
        if pg is not None:
            if pg == current_parallel:
                current_group.append(desc)
            else:
                if current_group:
                    groups.append(current_group)
                current_parallel = pg
                current_group = [desc]
        else:
            if current_group:
                groups.append(current_group)
                current_group = []
                current_parallel = None
            groups.append([desc])

    if current_group:
        groups.append(current_group)

    return groups


# ── Internal Celery Tasks ──
# Using @shared_task so they register with whatever Celery app is active.


@shared_task(name="celery_saga.start", bind=True, max_retries=0)
def saga_start(self, saga_id: str):
    backend = get_backend()
    execution = backend.load(saga_id)
    if not execution:
        raise RuntimeError(f"Saga {saga_id} not found")

    execution.status = SagaStatus.RUNNING
    execution.touch()
    backend.save(execution)
    logger.info("Saga %s (%s) started", saga_id, execution.saga_name)


@shared_task(name="celery_saga.run_step", bind=True, max_retries=0)
def saga_run_step(self, saga_id: str, step_index: int):
    backend = get_backend()
    execution = backend.load(saga_id)
    if not execution:
        raise RuntimeError(f"Saga {saga_id} not found")

    step_exec = _find_step(execution, step_index)

    # Child-saga step: run the named child saga atomically. Branch here before
    # any "Task not found" lookup, since child steps have no Celery task.
    if step_exec.child_saga_name:
        _run_child_saga_step(self, saga_id, execution, step_exec, backend)
        return

    # Find the actual task
    task = current_app.tasks.get(step_exec.task_name)
    if not task:
        raise RuntimeError(f"Task {step_exec.task_name} not found in Celery app")

    # Mark step as running
    step_exec.status = StepStatus.RUNNING
    step_exec.started_at = datetime.now(timezone.utc).isoformat()
    step_exec.task_id = self.request.id
    execution.touch()
    backend.save(execution)

    _apply_pending_transforms(execution, step_index, backend)
    step_input = _resolve_step_input(execution, step_exec)

    try:
        if current_app.conf.task_always_eager:
            async_result = task.apply(kwargs=step_input)
            if async_result.failed():
                async_result.maybe_throw()
            result = async_result.result
        else:
            async_result = task.apply_async(kwargs=step_input)
            result = async_result.get(disable_sync_subtasks=False, propagate=True)

        # Handle StepResponse
        if isinstance(result, StepResponse):
            output = result.output or {}
            compensation_data = result.compensation_data
        elif isinstance(result, dict):
            output = result
            compensation_data = None
        else:
            output = {"result": result} if result is not None else {}
            compensation_data = None

        # Update step
        step_exec.status = StepStatus.SUCCESS
        step_exec.completed_at = datetime.now(timezone.utc).isoformat()
        step_exec.output = output if isinstance(output, dict) else {"result": output}
        step_exec.compensation_data = compensation_data

        # Merge output into context
        if isinstance(output, dict):
            execution.context.update(output)

        execution.touch()
        backend.save(execution)
        logger.info("Saga %s step %d (%s) succeeded", saga_id, step_index, step_exec.task_name)

    except PermanentFailure as e:
        step_exec.status = StepStatus.FAILED
        step_exec.completed_at = datetime.now(timezone.utc).isoformat()
        step_exec.error = str(e)
        step_exec.compensation_data = e.compensation_data
        execution.touch()
        backend.save(execution)
        logger.error("Saga %s step %d (%s) permanent failure: %s", saga_id, step_index, step_exec.task_name, e)
        raise

    except Exception as e:
        step_exec.status = StepStatus.FAILED
        step_exec.completed_at = datetime.now(timezone.utc).isoformat()
        step_exec.error = str(e)
        execution.touch()
        backend.save(execution)
        logger.error("Saga %s step %d (%s) failed: %s", saga_id, step_index, step_exec.task_name, e)
        raise


def _run_child_saga_step(
    parent_task,
    parent_saga_id: str,
    execution: SagaExecution,
    step_exec: StepExecution,
    backend: SagaBackend,
) -> None:
    """Atomically execute a child saga as a single step in the parent.

    Outcomes:
      - Child COMPLETED → step SUCCESS, output = child's final context,
        compensation_data = {"child_saga_id": ...} for the user compensate task.
      - Child COMPENSATED → step FAILED via ChildSagaCompensated; parent skips
        compensation for THIS step (child already cleaned itself) but still
        compensates earlier parent steps.
      - Child FAILED (catastrophic) → step FAILED via generic exception;
        parent's compensation chain runs as normal.
    """
    from celery_saga.core import _lookup_saga
    from celery_saga.result import SagaCompensated, SagaFailed

    step_exec.status = StepStatus.RUNNING
    step_exec.started_at = datetime.now(timezone.utc).isoformat()
    step_exec.task_id = parent_task.request.id
    execution.touch()
    backend.save(execution)

    _apply_pending_transforms(execution, step_exec.step_index, backend)
    child_input = _resolve_step_input(execution, step_exec)

    import uuid as _uuid
    from celery_saga.core import SagaPlan, serialize_callable

    child_saga = _lookup_saga(step_exec.child_saga_name)
    child_plan = SagaPlan(child_saga.name, child_saga._plan_entries)
    child_saga_id = str(_uuid.uuid4())
    idempotency_key = f"{parent_saga_id}:child:{step_exec.step_index}"

    child_result = execute_saga(
        saga_id=child_saga_id,
        plan=child_plan,
        input_data=child_input,
        backend=backend,
        idempotency_key=idempotency_key,
        transforms=[
            {
                "before_step_index": before_step_index,
                "callable": serialize_callable(transform_fn),
            }
            for before_step_index, transform_fn in child_saga._transforms
        ],
    )

    try:
        child_context = child_result.get(timeout=None, interval=0.2)
    except SagaCompensated:
        step_exec.status = StepStatus.FAILED
        step_exec.completed_at = datetime.now(timezone.utc).isoformat()
        step_exec.error = f"Child saga {step_exec.child_saga_name} self-compensated"
        # Child already undid itself — suppress this step's compensation in the
        # parent. Earlier parent steps still compensate.
        step_exec.no_compensation = True
        step_exec.compensation_data = None
        step_exec.output = {"child_saga_id": child_result.saga_id}
        execution.touch()
        backend.save(execution)
        raise ChildSagaCompensated(child_result.saga_id)
    except SagaFailed as e:
        step_exec.status = StepStatus.FAILED
        step_exec.completed_at = datetime.now(timezone.utc).isoformat()
        step_exec.error = f"Child saga {step_exec.child_saga_name} failed catastrophically: {e}"
        execution.touch()
        backend.save(execution)
        raise

    # Child completed successfully.
    step_exec.status = StepStatus.SUCCESS
    step_exec.completed_at = datetime.now(timezone.utc).isoformat()
    step_exec.output = dict(child_context) if isinstance(child_context, dict) else {"result": child_context}
    step_exec.compensation_data = {"child_saga_id": child_result.saga_id}
    if isinstance(child_context, dict):
        execution.context.update(child_context)
    execution.touch()
    backend.save(execution)
    logger.info(
        "Saga %s step %d ran child saga %s (id=%s) successfully",
        parent_saga_id, step_exec.step_index, step_exec.child_saga_name, child_result.saga_id,
    )


@shared_task(name="celery_saga.parallel_done", bind=True, max_retries=0)
def saga_parallel_done(self, saga_id: str):
    """Checkpoint after parallel group completes."""
    backend = get_backend()
    execution = backend.load(saga_id)
    if not execution:
        raise RuntimeError(f"Saga {saga_id} not found")
    logger.info("Saga %s parallel group completed", saga_id)


@shared_task(name="celery_saga.complete", bind=True, max_retries=0)
def saga_complete(self, saga_id: str):
    backend = get_backend()
    execution = backend.load(saga_id)
    if not execution:
        raise RuntimeError(f"Saga {saga_id} not found")

    execution.status = SagaStatus.COMPLETED
    execution.touch()
    backend.save(execution)
    logger.info("Saga %s (%s) completed successfully", saga_id, execution.saga_name)


@shared_task(name="celery_saga.on_error", bind=True, max_retries=0)
def saga_on_error(self, saga_id: str):
    """Error handler — triggers compensation for all completed steps in reverse order."""
    backend = get_backend()
    execution = backend.load(saga_id)
    if not execution:
        logger.error("Saga %s not found during compensation", saga_id)
        return

    execution.status = SagaStatus.COMPENSATING
    execution.touch()
    backend.save(execution)
    logger.info("Saga %s (%s) compensating...", saga_id, execution.saga_name)

    # Collect steps that need compensation, in reverse order.
    # Include: successful steps; failed/running steps with compensation_data (from
    # PermanentFailure or from a worker crash mid-step where the side effect may
    # have already executed).
    steps_to_compensate = [
        s for s in reversed(execution.steps)
        if not s.no_compensation
        and s.compensation_task_name
        and (
            s.status == StepStatus.SUCCESS
            or (
                s.status in (StepStatus.FAILED, StepStatus.RUNNING)
                and s.compensation_data is not None
            )
        )
    ]

    if not steps_to_compensate:
        execution.status = SagaStatus.COMPENSATED
        execution.touch()
        backend.save(execution)
        logger.info("Saga %s no steps to compensate", saga_id)
        return

    # Build compensation chain in reverse order
    comp_tasks = []
    for step_exec in steps_to_compensate:
        comp_task = current_app.tasks.get(step_exec.compensation_task_name)
        if comp_task:
            step_exec.status = StepStatus.COMPENSATING
            comp_tasks.append(
                saga_run_compensation.si(saga_id, step_exec.step_index)
            )
        else:
            logger.warning(
                "Compensation task %s not found for step %d",
                step_exec.compensation_task_name,
                step_exec.step_index,
            )

    backend.save(execution)

    if comp_tasks:
        compensation_chain = chain(*comp_tasks)
        compensation_chain.apply_async(
            link=saga_compensation_complete.si(saga_id),
            link_error=saga_compensation_failed.si(saga_id),
        )
    else:
        execution.status = SagaStatus.COMPENSATED
        execution.touch()
        backend.save(execution)


@shared_task(name="celery_saga.run_compensation", bind=True, max_retries=3)
def saga_run_compensation(self, saga_id: str, step_index: int):
    """Run a single compensation task."""
    backend = get_backend()
    execution = backend.load(saga_id)
    if not execution:
        raise RuntimeError(f"Saga {saga_id} not found")

    step_exec = _find_step(execution, step_index)
    comp_task = current_app.tasks.get(step_exec.compensation_task_name)
    if not comp_task:
        raise RuntimeError(f"Compensation task {step_exec.compensation_task_name} not found")

    try:
        comp_data = _deserialize_compensation_data(step_exec.compensation_data, comp_task)
        if current_app.conf.task_always_eager:
            comp_result = comp_task.apply(args=(comp_data,))
            if comp_result.failed():
                comp_result.maybe_throw()
        else:
            comp_task.apply_async(args=(comp_data,)).get(disable_sync_subtasks=False, propagate=True)
        step_exec.status = StepStatus.COMPENSATED
        execution.touch()
        backend.save(execution)
        logger.info(
            "Saga %s step %d (%s) compensated",
            saga_id, step_index, step_exec.task_name,
        )
    except Exception as e:
        logger.error(
            "Saga %s step %d compensation failed: %s",
            saga_id, step_index, e,
        )
        raise


@shared_task(name="celery_saga.compensation_complete", bind=True, max_retries=0)
def saga_compensation_complete(self, saga_id: str):
    backend = get_backend()
    execution = backend.load(saga_id)
    if not execution:
        return

    execution.status = SagaStatus.COMPENSATED
    execution.touch()
    backend.save(execution)
    logger.info("Saga %s (%s) fully compensated", saga_id, execution.saga_name)


@shared_task(name="celery_saga.compensation_failed", bind=True, max_retries=0)
def saga_compensation_failed(self, saga_id: str):
    backend = get_backend()
    execution = backend.load(saga_id)
    if not execution:
        return

    execution.status = SagaStatus.FAILED
    execution.touch()
    backend.save(execution)
    logger.error("Saga %s (%s) compensation FAILED", saga_id, execution.saga_name)


def _find_step(execution: SagaExecution, step_index: int) -> StepExecution:
    """Find a step by index, handling cases where step_index != list position."""
    for s in execution.steps:
        if s.step_index == step_index:
            return s
    raise RuntimeError(f"Step {step_index} not found in saga {execution.saga_id}")


def _apply_pending_transforms(
    execution: SagaExecution,
    step_index: int,
    backend: SagaBackend,
) -> None:
    applied_indexes = set(execution.applied_transform_indexes)
    changed = False

    for transform_index, transform_entry in enumerate(execution.transforms):
        before_step_index = transform_entry["before_step_index"]
        if transform_index in applied_indexes or before_step_index > step_index:
            continue

        transform_fn = deserialize_callable(transform_entry["callable"])
        execution.context = transform_fn(dict(execution.context))
        execution.applied_transform_indexes.append(transform_index)
        changed = True

    if changed:
        backend.save(execution)


def _resolve_step_input(execution: SagaExecution, step_exec: StepExecution) -> dict[str, Any]:
    input_mapper = deserialize_callable(step_exec.input_mapper)
    if input_mapper:
        return input_mapper(dict(execution.context))

    if step_exec.input_spec is None:
        return dict(execution.context)

    resolved = _resolve_input_spec(execution, step_exec.input_spec)
    if isinstance(resolved, dict):
        return resolved
    if resolved is None:
        return {}
    return {"result": resolved}


def _resolve_input_spec(execution: SagaExecution, spec: dict[str, Any]) -> Any:
    spec_type = spec["type"]

    if spec_type == "literal":
        return spec["value"]

    if spec_type == "context":
        return dict(execution.context)

    if spec_type == "step" or spec_type == "step_output":
        step_output = _find_step(execution, spec["step_index"]).output
        return dict(step_output or {})

    if spec_type == "transform":
        resolved_sources = [
            _resolve_input_spec(execution, source_spec)
            for source_spec in spec["sources"]
        ]
        transform_fn = deserialize_callable(spec["callable"])
        if transform_fn is None:
            merged = {}
            for source in resolved_sources:
                if isinstance(source, dict):
                    merged.update(source)
            return merged
        if len(resolved_sources) == 1:
            return transform_fn(resolved_sources[0])
        return transform_fn(*resolved_sources)

    raise ValueError(f"Unsupported input spec type: {spec_type}")
