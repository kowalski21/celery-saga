from __future__ import annotations

import uuid
from typing import Any, Callable

from celery_saga.step import (
    SAGA_STEP_ATTR,
    ParallelRef,
    SagaStepMeta,
    StepRef,
    TransformRef,
)

# ── Module-level collector used during saga function definition ──

_current_plan: list | None = None
_step_counter: int = 0


def _get_step_meta(task) -> SagaStepMeta:
    """Extract saga metadata from a task, if decorated with @saga_step."""
    return getattr(task, SAGA_STEP_ATTR, SagaStepMeta())


# ── Public API: step(), transform(), parallelize() ──


def step(
    task,
    input_ref: StepRef | TransformRef | dict | None = None,
    *,
    compensate: Any = None,
    no_compensation: bool = False,
    input_fn: Callable | None = None,
) -> StepRef:
    """Register a step during saga definition. Returns a StepRef placeholder.

    Usage inside a @saga function:
        order = step(validate_order, input)
        payment = step(charge_payment, order)
        step(charge_payment, order, compensate=refund_payment)
    """
    global _step_counter

    meta = _get_step_meta(task)
    comp = compensate or meta.compensate
    no_comp = no_compensation or meta.no_compensation

    ref = StepRef(
        step_index=_step_counter,
        task=task,
        compensate=comp,
        no_compensation=no_comp,
        input_ref=input_ref,
        input_fn=input_fn,
    )
    _step_counter += 1

    if _current_plan is not None:
        _current_plan.append(ref)

    return ref


def transform(
    sources: StepRef | tuple | dict,
    transform_fn: Callable | None = None,
) -> TransformRef:
    """Create a data transformation between steps.

    Usage:
        charge_input = transform(order, lambda data: {"amount": data["amount"] * 100})
        combined = transform((order, payment), lambda o, p: {**o, **p})
        merged = transform((order, payment))  # merge without transform
    """
    if not isinstance(sources, tuple):
        sources = (sources,)
    return TransformRef(sources=sources, transform_fn=transform_fn)


def parallelize(*step_refs: StepRef) -> ParallelRef:
    """Run multiple steps in parallel. Returns a ParallelRef.

    Usage:
        payment, inventory = parallelize(
            step(charge_payment, order),
            step(reserve_inventory, order),
        )
    """
    return ParallelRef(refs=list(step_refs))


# ── Saga class: built via @saga decorator or Saga builder ──


class SagaPlan:
    """Internal representation of a saga's execution plan."""

    def __init__(self, name: str, steps: list):
        self.name = name
        self.steps = steps  # list of StepRef | ParallelRef

    def flatten(self) -> list[dict]:
        """Flatten the plan into an ordered list of step descriptors for the orchestrator."""
        result = []
        parallel_group = 0

        for entry in self.steps:
            if isinstance(entry, ParallelRef):
                for ref in entry.refs:
                    result.append(self._ref_to_descriptor(ref, parallel_group=parallel_group))
                parallel_group += 1
            elif isinstance(entry, StepRef):
                result.append(self._ref_to_descriptor(ref=entry))
            # TransformRefs are not steps — they're resolved inline

        return result

    @staticmethod
    def _ref_to_descriptor(ref: StepRef, parallel_group: int | None = None) -> dict:
        comp_name = None
        if ref.compensate:
            if isinstance(ref.compensate, str):
                comp_name = ref.compensate
            elif hasattr(ref.compensate, "name"):
                comp_name = ref.compensate.name
            else:
                comp_name = getattr(ref.compensate, "__name__", str(ref.compensate))

        # Resolve input mapping function
        input_fn = ref.input_fn
        if input_fn is None and isinstance(ref.input_ref, TransformRef):
            input_fn = ref.input_ref.transform_fn

        return {
            "step_index": ref.step_index,
            "task_name": ref.task.name if hasattr(ref.task, "name") else ref.task.__name__,
            "task": ref.task,
            "compensate_task_name": comp_name,
            "compensate_task": ref.compensate if not isinstance(ref.compensate, str) else None,
            "no_compensation": ref.no_compensation,
            "parallel_group": parallel_group,
            "input_fn": input_fn,
        }


class Saga:
    """Saga definition via builder pattern or @saga decorator.

    Builder usage:
        order_saga = (
            Saga("order_saga")
            .step(validate_order)
            .step(charge_payment, compensate=refund_payment)
            .step(send_confirmation, no_compensation=True)
        )

    Functional usage:
        @saga("order_saga")
        def order_saga(input):
            order = step(validate_order, input)
            payment = step(charge_payment, order)
            return payment
    """

    def __init__(self, name: str, backend=None):
        self.name = name
        self._backend = backend
        self._plan_entries: list = []
        self._transforms: list = []
        self._next_step_index: int = 0

    # ── Builder API ──

    def add_step(
        self,
        task,
        *,
        compensate: Any = None,
        no_compensation: bool = False,
        input: Callable | None = None,
    ) -> Saga:
        meta = _get_step_meta(task)
        comp = compensate or meta.compensate
        no_comp = no_compensation or meta.no_compensation

        ref = StepRef(
            step_index=self._next_step_index,
            task=task,
            compensate=comp,
            no_compensation=no_comp,
            input_fn=input,
        )
        self._next_step_index += 1
        self._plan_entries.append(ref)
        return self

    def add_parallel(
        self,
        *tasks_or_tuples,
        input: Callable | None = None,
    ) -> Saga:
        """Add parallel steps. Each arg is a task or (task, compensate_task) tuple."""
        refs = []
        for entry in tasks_or_tuples:
            if isinstance(entry, tuple):
                task, comp = entry
            else:
                task = entry
                comp = None

            meta = _get_step_meta(task)
            comp = comp or meta.compensate
            no_comp = meta.no_compensation

            ref = StepRef(
                step_index=self._next_step_index,
                task=task,
                compensate=comp,
                no_compensation=no_comp,
                input_fn=input,
            )
            self._next_step_index += 1
            refs.append(ref)

        self._plan_entries.append(ParallelRef(refs=refs))
        return self

    def add_transform(self, fn: Callable) -> Saga:
        """Add a context transform applied before the next step."""
        self._transforms.append((self._next_step_index, fn))
        return self

    # ── Execution ──

    def run(
        self,
        *,
        idempotency_key: str | None = None,
        queue: str | None = None,
        **input_data,
    ):
        """Execute the saga."""
        from celery_saga.executor import execute_saga

        saga_id = str(uuid.uuid4())
        plan = SagaPlan(self.name, self._plan_entries)

        return execute_saga(
            saga_id=saga_id,
            plan=plan,
            input_data=input_data,
            backend=self._backend,
            idempotency_key=idempotency_key,
            queue=queue,
            transforms=self._transforms,
        )

    @property
    def plan(self) -> SagaPlan:
        return SagaPlan(self.name, self._plan_entries)


def saga(name: str, backend=None):
    """Decorator to define a saga from a function.

    @saga("order_saga")
    def order_saga(input):
        order = step(validate_order, input)
        payment = step(charge_payment, order)
        return payment
    """

    def decorator(fn: Callable) -> Saga:
        global _current_plan, _step_counter

        # Run the function once to collect the plan
        _current_plan = []
        _step_counter = 0

        try:
            # Pass a sentinel dict as input — the function builds the plan
            fn({})
        finally:
            plan_entries = _current_plan
            _current_plan = None
            _step_counter = 0

        s = Saga(name, backend=backend)
        s._plan_entries = plan_entries
        return s

    return decorator
