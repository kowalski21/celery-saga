from __future__ import annotations

import logging
from typing import Any, Callable

from celery import shared_task, current_app, group, chain, chord

from celery_saga.backends.base import SagaBackend
from celery_saga.backends.memory import MemorySagaBackend
from celery_saga.core import SagaPlan, ParallelRef, StepRef
from celery_saga.result import SagaResult
from celery_saga.state import (
    SagaExecution,
    SagaStatus,
    StepExecution,
    StepStatus,
)
from celery_saga.step import PermanentFailure, StepResponse, _deserialize_compensation_data

logger = logging.getLogger(__name__)

# Module-level default backend (can be overridden)
_default_backend: SagaBackend | None = None

# Registry for callable transforms/input_fns (not serializable, so stored in-process)
# Key: saga_id, Value: {"transforms": [...], "input_fns": {step_index: fn}}
_saga_registry: dict[str, dict] = {}


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
        ))

    execution = SagaExecution(
        saga_id=saga_id,
        saga_name=plan.name,
        status=SagaStatus.PENDING,
        idempotency_key=idempotency_key,
        input_data=input_data,
        context=dict(input_data),
        steps=step_executions,
    )
    backend.save(execution)

    # Store callable transforms and input_fns in registry
    input_fns = {}
    for desc in descriptors:
        if desc.get("input_fn"):
            input_fns[desc["step_index"]] = desc["input_fn"]

    _saga_registry[saga_id] = {
        "transforms": transforms or [],
        "input_fns": input_fns,
    }

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

    # Find the actual task
    task = current_app.tasks.get(step_exec.task_name)
    if not task:
        raise RuntimeError(f"Task {step_exec.task_name} not found in Celery app")

    # Mark step as running
    step_exec.status = StepStatus.RUNNING
    step_exec.task_id = self.request.id
    execution.touch()
    backend.save(execution)

    # Resolve input — apply transforms and input_fns
    registry = _saga_registry.get(saga_id, {})

    # Apply any global transforms that come before this step
    for transform_before_index, transform_fn in registry.get("transforms", []):
        if transform_before_index <= step_index:
            execution.context = transform_fn(execution.context)
            backend.save(execution)

    # Apply per-step input mapper, or use full context
    input_fn = registry.get("input_fns", {}).get(step_index)
    if input_fn:
        step_input = input_fn(execution.context)
    else:
        step_input = dict(execution.context)

    try:
        eager_result = task.apply(kwargs=step_input)
        if eager_result.failed():
            eager_result.maybe_throw()
        result = eager_result.result

        # Handle StepResponse
        if isinstance(result, StepResponse):
            output = result.output or {}
            compensation_data = result.compensation_data
        elif isinstance(result, dict):
            output = result
            compensation_data = result
        else:
            output = {"result": result} if result is not None else {}
            compensation_data = result

        # Update step
        step_exec.status = StepStatus.SUCCESS
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
        step_exec.error = str(e)
        step_exec.compensation_data = e.compensation_data
        execution.touch()
        backend.save(execution)
        logger.error("Saga %s step %d (%s) permanent failure: %s", saga_id, step_index, step_exec.task_name, e)
        raise

    except Exception as e:
        step_exec.status = StepStatus.FAILED
        step_exec.error = str(e)
        execution.touch()
        backend.save(execution)
        logger.error("Saga %s step %d (%s) failed: %s", saga_id, step_index, step_exec.task_name, e)
        raise


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
    # Include: successful steps AND failed steps with compensation_data (from PermanentFailure)
    steps_to_compensate = [
        s for s in reversed(execution.steps)
        if not s.no_compensation
        and s.compensation_task_name
        and (
            s.status == StepStatus.SUCCESS
            or (s.status == StepStatus.FAILED and s.compensation_data is not None)
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
        comp_result = comp_task.apply(args=(comp_data,))
        if comp_result.failed():
            comp_result.maybe_throw()
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
