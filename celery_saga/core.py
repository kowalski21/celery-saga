from __future__ import annotations

import base64
import hmac
import hashlib
import importlib
import marshal
import os
import pickle
import types
import uuid
from typing import Any, Callable

# ── Signing for "code" payloads ──
# Code payloads embed marshal/pickle blobs that execute arbitrary code on
# deserialization. To prevent RCE when the backend (e.g. Redis) is shared or
# untrusted, every code payload is HMAC-signed at serialize time and verified at
# deserialize time. Set CELERY_SAGA_SIGNING_KEY in the environment (same value on
# every process) to use lambdas / local functions in sagas. Otherwise use
# module-level functions (serialized as "import" payloads — no code embedded).

_SIGNING_KEY_ENV = "CELERY_SAGA_SIGNING_KEY"


def _get_signing_key() -> bytes | None:
    key = os.environ.get(_SIGNING_KEY_ENV)
    return key.encode("utf-8") if key else None


def _sign_code_payload(parts: dict[str, str]) -> str:
    key = _get_signing_key()
    if key is None:
        raise RuntimeError(
            f"Cannot serialize non-importable callable: {_SIGNING_KEY_ENV} is not set. "
            "Either set this env var to an HMAC secret (must match on every worker), "
            "or define the callable at module scope so it can be serialized as an import reference."
        )
    msg = "|".join(parts[k] for k in ("name", "code", "defaults", "kwdefaults", "globals"))
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).hexdigest()


def _verify_code_payload(payload: dict[str, Any]) -> None:
    key = _get_signing_key()
    if key is None:
        raise RuntimeError(
            f"Refusing to deserialize signed code payload: {_SIGNING_KEY_ENV} is not set."
        )
    sig = payload.get("sig")
    if not sig:
        raise RuntimeError("Refusing to deserialize unsigned code payload (possible tampering).")
    msg = "|".join(payload[k] for k in ("name", "code", "defaults", "kwdefaults", "globals"))
    expected = hmac.new(key, msg.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        raise RuntimeError("Code payload HMAC verification failed (possible tampering).")

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


def serialize_callable(fn: Callable | None) -> dict[str, Any] | None:
    if fn is None:
        return None

    module_name = getattr(fn, "__module__", None)
    qualname = getattr(fn, "__qualname__", None)
    if module_name and qualname and "<locals>" not in qualname and "<lambda>" not in qualname:
        try:
            current = importlib.import_module(module_name)
            for attr in qualname.split("."):
                current = getattr(current, attr)
            if current is fn:
                return {
                    "type": "import",
                    "module": module_name,
                    "qualname": qualname,
                }
        except Exception:
            pass

    if fn.__closure__:
        raise ValueError("Cannot serialize callables with closures for distributed execution")

    globals_payload = {}
    for name in fn.__code__.co_names:
        if name in fn.__globals__ and name != "__builtins__":
            value = fn.__globals__[name]
            try:
                pickle.dumps(value)
            except Exception:
                continue
            globals_payload[name] = value

    parts = {
        "name": fn.__name__,
        "code": base64.b64encode(marshal.dumps(fn.__code__)).decode("ascii"),
        "defaults": base64.b64encode(pickle.dumps(fn.__defaults__)).decode("ascii"),
        "kwdefaults": base64.b64encode(pickle.dumps(fn.__kwdefaults__)).decode("ascii"),
        "globals": base64.b64encode(pickle.dumps(globals_payload)).decode("ascii"),
    }
    return {
        "type": "code",
        **parts,
        "sig": _sign_code_payload(parts),
    }


def deserialize_callable(payload: dict[str, Any] | None) -> Callable | None:
    if payload is None:
        return None

    payload_type = payload["type"]
    if payload_type == "import":
        current = importlib.import_module(payload["module"])
        for attr in payload["qualname"].split("."):
            current = getattr(current, attr)
        return current

    if payload_type == "code":
        _verify_code_payload(payload)
        code = marshal.loads(base64.b64decode(payload["code"].encode("ascii")))
        defaults = pickle.loads(base64.b64decode(payload["defaults"].encode("ascii")))
        kwdefaults = pickle.loads(base64.b64decode(payload["kwdefaults"].encode("ascii")))
        globals_payload = pickle.loads(base64.b64decode(payload["globals"].encode("ascii")))
        namespace = {"__builtins__": __builtins__, **globals_payload}
        fn = types.FunctionType(code, namespace, payload["name"], defaults)
        fn.__kwdefaults__ = kwdefaults
        return fn

    raise ValueError(f"Unsupported callable payload type: {payload_type}")


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
    ref = ParallelRef(refs=list(step_refs))

    if _current_plan is not None:
        _current_plan[:] = [
            entry for entry in _current_plan
            if not (isinstance(entry, StepRef) and entry in step_refs)
        ]
        _current_plan.append(ref)

    return ref


def _serialize_input_ref(
    input_ref: StepRef | TransformRef | dict | None,
    *,
    for_transform_source: bool = False,
) -> dict[str, Any] | None:
    if input_ref is None:
        return None

    if isinstance(input_ref, StepRef):
        ref_type = "step_output" if for_transform_source else "step"
        return {"type": ref_type, "step_index": input_ref.step_index}

    if isinstance(input_ref, TransformRef):
        return {
            "type": "transform",
            "sources": [
                _serialize_input_ref(source, for_transform_source=True)
                for source in input_ref.sources
            ],
            "callable": serialize_callable(input_ref.transform_fn),
        }

    if isinstance(input_ref, dict):
        if not input_ref:
            return {"type": "context"}
        return {"type": "literal", "value": input_ref}

    raise TypeError(f"Unsupported input reference: {type(input_ref)!r}")


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

        return {
            "step_index": ref.step_index,
            "task_name": ref.task.name if hasattr(ref.task, "name") else ref.task.__name__,
            "task": ref.task,
            "compensate_task_name": comp_name,
            "compensate_task": ref.compensate if not isinstance(ref.compensate, str) else None,
            "no_compensation": ref.no_compensation,
            "parallel_group": parallel_group,
            "input_spec": _serialize_input_ref(ref.input_ref),
            "input_mapper": serialize_callable(ref.input_fn),
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
            transforms=[
                {
                    "before_step_index": before_step_index,
                    "callable": serialize_callable(transform_fn),
                }
                for before_step_index, transform_fn in self._transforms
            ],
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
