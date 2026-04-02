# celery-saga Documentation

## Table of Contents

- [Installation](#installation)
- [Core Concepts](#core-concepts)
- [Getting Started](#getting-started)
- [Defining Steps](#defining-steps)
- [Defining Sagas](#defining-sagas)
  - [Builder API](#builder-api)
  - [Functional API](#functional-api)
- [Parallel Steps](#parallel-steps)
- [Data Flow & Transforms](#data-flow--transforms)
- [Compensation](#compensation)
- [Permanent Failure](#permanent-failure)
- [Idempotency](#idempotency)
- [Saga Result](#saga-result)
- [State Backends](#state-backends)
- [Retry Semantics](#retry-semantics)
- [Testing](#testing)
- [API Reference](#api-reference)

---

## Installation

```bash
pip install celery-saga
```

With Redis state backend:

```bash
pip install celery-saga[redis]
```

## Core Concepts

**celery-saga** adds the saga pattern to Celery. A saga is a sequence of steps where each step can have a **compensation** (rollback) function. If any step fails, all previously completed steps are compensated in **reverse order**.

Key ideas:

- **Step** — a Celery task that does work and optionally returns data for the next step
- **Compensation** — a Celery task that undoes the work of its corresponding step
- **StepResponse** — separates data flowing forward (output) from data needed for rollback (compensation_data)
- **Saga** — a named sequence of steps with automatic compensation on failure
- **SagaResult** — a handle to track the saga's progress and retrieve its output

## Getting Started

```python
from celery import Celery
from celery_saga import Saga, StepResponse, saga_step, set_default_backend
from celery_saga.backends import RedisSagaBackend

app = Celery("myapp", broker="redis://localhost:6379/0")

# Configure the state backend
set_default_backend(RedisSagaBackend(url="redis://localhost:6379/1"))


# ── Define steps ──

@saga_step(compensate="myapp.tasks.refund_payment")
@app.task
def charge_payment(**kwargs):
    txn = payment_service.charge(kwargs["order_id"], kwargs["amount"])
    return StepResponse(
        output={"transaction_id": txn.id},
        compensation_data={"transaction_id": txn.id, "amount": kwargs["amount"]},
    )


@app.task
def refund_payment(compensation_data):
    payment_service.refund(
        compensation_data["transaction_id"],
        compensation_data["amount"],
    )


# ── Define and run the saga ──

order_saga = (
    Saga("order_saga")
    .add_step(charge_payment)
)

result = order_saga.run(order_id="abc-123", amount=99.99)
output = result.get(timeout=30)
print(output["transaction_id"])  # "txn-..."
```

## Defining Steps

A saga step is a regular Celery task that optionally uses `@saga_step` to attach compensation metadata.

### Step function signature

Step tasks receive the **accumulated context** as keyword arguments. The context starts with the input passed to `saga.run()` and is enriched by each step's output.

```python
@saga_step(compensate="myapp.tasks.undo_reservation")
@app.task
def reserve_inventory(**kwargs):
    # kwargs contains all data from previous steps + original input
    order_id = kwargs["order_id"]
    items = kwargs["items"]

    reservation = inventory_service.reserve(order_id, items)

    return StepResponse(
        output={"reservation_id": reservation.id},
        compensation_data={"reservation_id": reservation.id},
    )
```

### Compensation function signature

Compensation tasks receive a single argument: the `compensation_data` from the step's `StepResponse`.

```python
@app.task
def undo_reservation(compensation_data):
    inventory_service.release(compensation_data["reservation_id"])
```

### @saga_step decorator

Attaches saga metadata to a task. Must be applied **before** (above) the `@app.task` decorator.

```python
@saga_step(compensate="myapp.tasks.refund")   # task with compensation
@app.task
def charge(**kwargs): ...

@saga_step(no_compensation=True)               # fire-and-forget step
@app.task
def send_email(**kwargs): ...
```

Parameters:
- `compensate` — string (task name) or task reference pointing to the compensation task
- `no_compensation` — if `True`, this step is skipped during rollback (e.g., notifications)

### Return values

Steps can return:

| Return type | Behavior |
|-------------|----------|
| `StepResponse(output, compensation_data)` | Output merged into context; compensation_data saved for rollback |
| `StepResponse(output)` | Output merged into context; compensation_data defaults to output |
| `dict` | Treated as both output and compensation_data |
| Any other value | Wrapped as `{"result": value}` |
| `None` | Nothing added to context |

## Defining Sagas

Two ways to define a saga: **Builder API** and **Functional API**.

### Builder API

Chain methods to build the saga step by step. Best for straightforward linear or parallel flows.

```python
from celery_saga import Saga

order_saga = (
    Saga("order_saga")
    .add_step(validate_order, no_compensation=True)
    .add_step(charge_payment)
    .add_step(reserve_inventory)
    .add_step(send_confirmation, no_compensation=True)
)

result = order_saga.run(order_id="abc", amount=99.99, items=["sku-1"])
```

#### `Saga(name, backend=None)`

Creates a new saga definition.

- `name` — unique saga name (used in logs and state records)
- `backend` — optional `SagaBackend` instance (overrides the default)

#### `.add_step(task, *, compensate=None, no_compensation=False, input=None)`

Adds a sequential step.

- `task` — Celery task (decorated with `@app.task`)
- `compensate` — compensation task name or reference (overrides `@saga_step`)
- `no_compensation` — skip this step during rollback (overrides `@saga_step`)
- `input` — callable `(context) -> dict` that maps the context to step kwargs

```python
.add_step(
    charge_payment,
    input=lambda ctx: {"order_id": ctx["order_id"], "amount": ctx["total_cents"]},
)
```

#### `.add_parallel(*tasks_or_tuples, input=None)`

Adds steps that execute concurrently. Accepts tasks or `(task, compensate_task)` tuples.

```python
# Tasks with @saga_step metadata
.add_parallel(charge_payment, reserve_inventory)

# Explicit compensation via tuples
.add_parallel(
    (charge_payment, refund_payment),
    (reserve_inventory, release_inventory),
)
```

#### `.add_transform(fn)`

Adds a context transformation applied before the next step. The function receives the current context dict and returns a new context dict.

```python
.add_transform(lambda ctx: {**ctx, "amount_cents": int(ctx["amount"] * 100)})
```

#### `.run(*, idempotency_key=None, queue=None, **input_data)`

Dispatches the saga and returns a `SagaResult`.

- `idempotency_key` — prevents duplicate execution (see [Idempotency](#idempotency))
- `queue` — route all saga tasks to a specific Celery queue
- `**input_data` — the initial context passed to the first step

### Functional API

Define a saga as a Python function using `step()`, `transform()`, and `parallelize()`. The function runs once at definition time to build the execution plan — variables inside are **placeholders**, not real values.

```python
from celery_saga import saga, step, transform, parallelize

@saga("order_saga")
def order_saga(input):
    order = step(validate_order, input)

    charge_input = transform(order, lambda data: {
        "order_id": data["order_id"],
        "amount_cents": data["amount"] * 100,
    })

    payment = step(charge_payment, charge_input)
    inventory = step(reserve_inventory, order)

    step(send_confirmation, payment)
    return payment

# Run it
result = order_saga.run(order_id="abc-123", amount=99.99)
```

#### `step(task, input_ref=None, *, compensate=None, no_compensation=False)`

Registers a step in the saga plan. Returns a `StepRef` placeholder.

```python
order = step(validate_order, input)
payment = step(charge_payment, order, compensate=refund_payment)
step(send_email, payment, no_compensation=True)
```

#### `transform(sources, transform_fn=None)`

Creates a data transformation. The result is a `TransformRef` that can be passed to `step()`.

```python
# Single source
cents = transform(order, lambda d: {**d, "amount": d["amount"] * 100})

# Multiple sources
combined = transform((order, payment), lambda o, p: {**o, **p})
```

#### `parallelize(*step_refs)`

Runs multiple steps concurrently.

```python
payment, inventory = parallelize(
    step(charge_payment, order),
    step(reserve_inventory, order),
)
```

## Parallel Steps

Parallel steps are dispatched simultaneously as a Celery `chord`. The saga waits for **all** parallel steps to complete before proceeding.

```python
order_saga = (
    Saga("order_saga")
    .add_step(validate_order)
    .add_parallel(charge_payment, reserve_inventory)  # run at the same time
    .add_step(send_confirmation)                      # waits for both
)
```

If any parallel step fails, all other completed parallel steps (and all prior steps) are compensated in reverse order.

Both parallel steps receive the same context. Their outputs are merged — if both return a key with the same name, the last one wins.

## Data Flow & Transforms

Data flows through a saga via a **context dictionary**:

1. `saga.run(**input_data)` initializes the context with the input
2. Each step receives the context as `**kwargs`
3. Each step's `StepResponse.output` (if it's a dict) is merged into the context
4. The next step receives the updated context

```
Input: {"order_id": "abc", "amount": 99}
              │
              ▼
    ┌─────────────────┐
    │ validate_order   │ → output: {"order_id": "abc", "amount": 99}
    └─────────────────┘
              │
     context: {"order_id": "abc", "amount": 99}
              │
              ▼
    ┌─────────────────┐
    │ charge_payment   │ → output: {"transaction_id": "txn-1"}
    └─────────────────┘
              │
     context: {"order_id": "abc", "amount": 99, "transaction_id": "txn-1"}
              │
              ▼
    ┌─────────────────┐
    │ send_confirmation│ → receives all three keys as kwargs
    └─────────────────┘
```

### Transforms

Use transforms when you need to reshape data between steps.

**Global transform** — modifies the context for all subsequent steps:

```python
.add_step(validate_order)
.add_transform(lambda ctx: {**ctx, "amount_cents": int(ctx["amount"] * 100)})
.add_step(charge_in_cents)  # receives amount_cents in kwargs
```

**Per-step input mapper** — shapes kwargs for one step only, doesn't modify the shared context:

```python
.add_step(
    charge_payment,
    input=lambda ctx: {
        "order_id": ctx["order_id"],
        "amount": ctx["total_cents"],
    },
)
```

## Compensation

When a step fails, celery-saga automatically compensates all previously completed steps in **reverse order**.

```
Step 1: validate_order   ✓  (no_compensation=True)
Step 2: charge_payment   ✓  → compensation_data saved
Step 3: reserve_inventory ✓  → compensation_data saved
Step 4: send_shipping    ✗  FAILS
                              │
                              ▼ COMPENSATING (reverse order)
                         3. release_inventory(compensation_data)
                         2. refund_payment(compensation_data)
                         1. (skipped — no_compensation)
```

### What gets compensated

- Steps with `status == SUCCESS` and a compensation task defined
- Steps with `status == FAILED` that have `compensation_data` (from `PermanentFailure`)
- Steps with `no_compensation=True` are always skipped

### Compensation tasks

Compensation tasks are regular Celery tasks. They receive a single positional argument: the `compensation_data` from the original step.

```python
@app.task
def refund_payment(compensation_data):
    # compensation_data is whatever was passed to StepResponse
    payment_service.refund(
        compensation_data["transaction_id"],
        compensation_data["amount"],
    )
```

Compensation tasks are retried up to 3 times by default. If compensation itself fails, the saga enters `FAILED` status.

## Permanent Failure

Use `StepResponse.permanent_failure()` when a step has done **partial work** and needs to fail immediately (skipping Celery's retry mechanism) while passing cleanup data to the compensation function.

```python
@saga_step(compensate="myapp.tasks.cleanup_records")
@app.task
def process_batch(**kwargs):
    items = kwargs["items"]
    processed_ids = []

    for item in items:
        if item.is_invalid():
            # Fail immediately — pass the IDs we already processed
            StepResponse.permanent_failure(
                f"Invalid item: {item.id}",
                compensation_data={"processed_ids": processed_ids},
            )
        record = db.create(item)
        processed_ids.append(record.id)

    return StepResponse(
        output={"processed_ids": processed_ids},
        compensation_data={"processed_ids": processed_ids},
    )


@app.task
def cleanup_records(compensation_data):
    for record_id in compensation_data["processed_ids"]:
        db.delete(record_id)
```

`permanent_failure()` raises a `PermanentFailure` exception internally. The saga orchestrator catches it, saves the compensation data, and triggers the compensation chain.

## Idempotency

Prevent duplicate saga executions with `idempotency_key`:

```python
result = order_saga.run(
    order_id="abc-123",
    amount=99.99,
    idempotency_key="order-abc-123",
)
```

If a saga with the same idempotency key is already `RUNNING` or `COMPLETED`, `run()` returns the existing `SagaResult` instead of starting a new saga. This is useful for:

- Webhook handlers that may fire multiple times
- API endpoints with retry logic
- Ensuring exactly-once saga execution per business event

## Saga Result

`saga.run()` returns a `SagaResult` — a handle to the running saga.

### Properties

```python
result = order_saga.run(order_id="abc")

result.saga_id      # str — unique execution ID
result.status       # SagaStatus — current status
result.steps        # list[dict] — step-level status details
result.context      # dict — accumulated output from all steps
result.is_complete  # bool — True if in a terminal state
```

### Blocking wait

```python
try:
    output = result.get(timeout=30)
    # output is the final context dict
    print(output["transaction_id"])
except SagaCompensated as e:
    # Saga was rolled back successfully
    print(f"Rolled back: {e}")
    print(e.execution.steps)  # inspect step-level details
except SagaFailed as e:
    # Compensation itself failed — manual intervention needed
    print(f"Critical failure: {e}")
except TimeoutError:
    # Saga is still running
    print(f"Still running: {result.status}")
```

### Status values

| Status | Meaning |
|--------|---------|
| `PENDING` | Saga created, not yet started |
| `RUNNING` | Steps are executing |
| `COMPENSATING` | A step failed, compensation in progress |
| `COMPLETED` | All steps succeeded |
| `COMPENSATED` | Rollback completed successfully |
| `FAILED` | Compensation itself failed — needs manual intervention |

## State Backends

celery-saga persists saga state in a pluggable backend.

### Memory (default)

In-memory storage. Data is lost when the process exits. Good for testing.

```python
from celery_saga.backends import MemorySagaBackend
from celery_saga import set_default_backend

set_default_backend(MemorySagaBackend())
```

### Redis

Persistent storage with TTL. Recommended for production.

```python
from celery_saga.backends import RedisSagaBackend
from celery_saga import set_default_backend

# From URL
set_default_backend(RedisSagaBackend(url="redis://localhost:6379/0"))

# From existing client
import redis
client = redis.Redis(host="localhost", port=6379, db=0)
set_default_backend(RedisSagaBackend(redis_client=client, ttl=86400))
```

Parameters:
- `url` — Redis connection URL
- `redis_client` — existing `redis.Redis` instance
- `ttl` — seconds to keep saga records (default: 86400 = 24h)

### Per-saga backend

Override the default for a specific saga:

```python
from celery_saga.backends import RedisSagaBackend

critical_backend = RedisSagaBackend(url="redis://localhost:6379/2", ttl=604800)

payment_saga = Saga("payment_saga", backend=critical_backend)
```

### Custom backend

Implement the `SagaBackend` interface:

```python
from celery_saga.backends.base import SagaBackend
from celery_saga.state import SagaExecution

class PostgresSagaBackend(SagaBackend):
    def save(self, execution: SagaExecution) -> None: ...
    def load(self, saga_id: str) -> SagaExecution | None: ...
    def delete(self, saga_id: str) -> None: ...
    def find_by_idempotency_key(self, key: str) -> SagaExecution | None: ...
```

`SagaExecution` has `.to_dict()` and `.from_dict()` for serialization.

## Retry Semantics

Step retries are handled by **Celery's native retry mechanism** — celery-saga doesn't reinvent this.

```python
@saga_step(compensate="myapp.tasks.refund")
@app.task(
    bind=True,
    autoretry_for=(ConnectionError, TimeoutError),
    max_retries=3,
    retry_backoff=True,
    retry_backoff_max=60,
)
def charge_payment(self, **kwargs):
    return StepResponse(
        output={"transaction_id": "txn-1"},
        compensation_data={"transaction_id": "txn-1"},
    )
```

- **Retryable errors** — use Celery's `autoretry_for` and `max_retries`
- **Permanent failure** — use `StepResponse.permanent_failure()` to skip retries and trigger compensation immediately
- **Retries exhausted** — when Celery gives up retrying, the saga catches the final exception and triggers compensation

## Testing

Use Celery's **eager mode** to test sagas without a broker:

```python
import pytest
from celery import Celery
from celery_saga import Saga, SagaCompensated, set_default_backend
from celery_saga.backends import MemorySagaBackend

app = Celery("test")
app.config_from_object({
    "task_always_eager": True,
    "task_eager_propagates": True,
    "result_backend": "cache+memory://",
})

backend = MemorySagaBackend()


@pytest.fixture(autouse=True)
def setup():
    backend._store.clear()
    backend._idem.clear()
    set_default_backend(backend)
    yield


def test_order_saga_completes():
    saga = Saga("order", backend=backend).add_step(my_task)
    result = saga.run(order_id="test-1")
    output = result.get(timeout=5)
    assert output["order_id"] == "test-1"


def test_order_saga_compensates_on_failure():
    saga = (
        Saga("order", backend=backend)
        .add_step(charge_payment)
        .add_step(failing_task)
    )
    result = saga.run(order_id="test-2", amount=100)
    with pytest.raises(SagaCompensated):
        result.get(timeout=5)
```

## API Reference

### `celery_saga`

| Export | Type | Description |
|--------|------|-------------|
| `Saga` | class | Builder for saga definitions |
| `saga` | decorator | Define a saga from a function |
| `step` | function | Register a step in a functional saga |
| `transform` | function | Create a data transform between steps |
| `parallelize` | function | Run steps in parallel |
| `StepResponse` | class | Return value from a step |
| `PermanentFailure` | exception | Raised by `StepResponse.permanent_failure()` |
| `saga_step` | decorator | Attach compensation metadata to a task |
| `SagaResult` | class | Handle to a running/completed saga |
| `SagaCompensated` | exception | Raised when a saga was rolled back |
| `SagaFailed` | exception | Raised when compensation failed |
| `SagaStatus` | enum | `PENDING`, `RUNNING`, `COMPENSATING`, `COMPLETED`, `COMPENSATED`, `FAILED` |
| `StepStatus` | enum | `PENDING`, `RUNNING`, `SUCCESS`, `FAILED`, `COMPENSATING`, `COMPENSATED`, `SKIPPED` |
| `set_default_backend` | function | Set the global state backend |

### `celery_saga.backends`

| Export | Type | Description |
|--------|------|-------------|
| `SagaBackend` | ABC | Base class for state backends |
| `MemorySagaBackend` | class | In-memory backend (testing) |
| `RedisSagaBackend` | class | Redis-backed state persistence |

### `StepResponse`

```python
StepResponse(output=None, compensation_data=None)
```

- `output` — data merged into the saga context, passed to subsequent steps
- `compensation_data` — data passed to the compensation function on rollback; defaults to `output` if not provided

```python
StepResponse.permanent_failure(message, compensation_data=None)
```

- Raises `PermanentFailure` — immediately fails the step, skips retries, triggers compensation

### `SagaResult`

```python
result.saga_id       # str
result.status        # SagaStatus
result.steps         # list[dict] with step_index, task_name, status, error
result.context       # dict — accumulated step outputs
result.is_complete   # bool

result.get(timeout=None, interval=0.5)  # blocks, returns context dict
# Raises: SagaCompensated, SagaFailed, TimeoutError
```
