# celery-saga by example

A practical walkthrough of using `celery-saga` to build distributed transactions on Celery, with automatic compensation on failure.

## What is a saga?

A saga is a sequence of local transactions where each step has a **compensating action** that undoes it. If step 3 fails, sagas roll back by running the compensations for steps 2 and 1 in reverse order — restoring the system to a consistent state without needing a distributed lock.

## Install

```bash
pip install celery-saga[redis]
```

Set an HMAC signing key for any saga that uses lambdas or locally-defined functions (required since v0.1+ for safe deserialization of code payloads from Redis):

```bash
export CELERY_SAGA_SIGNING_KEY="some-long-random-secret-shared-across-workers"
```

If you only use module-level (importable) functions in your saga, you can skip this — they are stored as import references, not code blobs.

---

## Example 1: order processing saga

A canonical e-commerce flow: charge the customer, reserve inventory, send a confirmation. If anything fails midway, refund and release inventory.

### `app.py`

```python
from celery import Celery
from celery_saga import set_default_backend
from celery_saga.backends import RedisSagaBackend

app = Celery("shop", broker="redis://localhost:6379/0")
app.conf.result_backend = "redis://localhost:6379/0"

set_default_backend(RedisSagaBackend(url="redis://localhost:6379/1"))
```

### `tasks.py`

```python
from app import app
from celery_saga import saga_task, StepResponse


@saga_task(app, compensate="tasks.refund_payment")
def charge_payment(**kwargs):
    order_id = kwargs["order_id"]
    amount = kwargs["amount"]
    txn_id = payment_gateway.charge(order_id, amount)  # your code
    return StepResponse(
        output={"transaction_id": txn_id},
        compensation_data={"transaction_id": txn_id, "amount": amount},
    )


@app.task
def refund_payment(compensation_data):
    payment_gateway.refund(
        compensation_data["transaction_id"],
        compensation_data["amount"],
    )


@saga_task(app, compensate="tasks.release_inventory")
def reserve_inventory(**kwargs):
    res_id = warehouse.reserve(kwargs["order_id"])
    return StepResponse(
        output={"reservation_id": res_id},
        compensation_data={"reservation_id": res_id},
    )


@app.task
def release_inventory(compensation_data):
    warehouse.release(compensation_data["reservation_id"])


@saga_task(app, no_compensation=True)
def send_confirmation(**kwargs):
    email.send(kwargs["order_id"], kwargs["transaction_id"])
    return StepResponse(output={"email_sent": True})
```

Key things to notice:

- `@saga_task(app, compensate=...)` registers a Celery task **and** marks its compensation.
- `compensate=` accepts a string task name or a direct task reference.
- `compensation_data` is **opt-in** — it is only saved if you explicitly return it. The output of a step is **not** silently used as compensation data.
- `no_compensation=True` flags pure side-effects (emails, notifications) that can't be undone.

### `run_saga.py`

```python
from celery_saga import Saga, SagaCompensated, SagaFailed
from tasks import charge_payment, reserve_inventory, send_confirmation

order_saga = (
    Saga("order_saga")
    .add_step(charge_payment)
    .add_step(reserve_inventory)
    .add_step(send_confirmation)
)

result = order_saga.run(order_id="order-42", amount=59.99)

try:
    output = result.get(timeout=30)
    print("Order completed:", output)
except SagaCompensated as e:
    print("Order rolled back cleanly:", e)
except SagaFailed as e:
    print("Catastrophic failure — manual intervention needed:", e)
```

Start a worker:

```bash
celery -A app worker --loglevel=info
```

---

## Example 2: parallel steps

When two steps don't depend on each other, run them in parallel.

```python
from celery_saga import Saga

booking_saga = (
    Saga("booking")
    .add_step(reserve_flight)
    .add_parallel(
        reserve_hotel,
        reserve_car,
    )
    .add_step(charge_payment)
)
```

`add_parallel` dispatches its tasks as a Celery `group`. If any branch fails, **all** previously successful steps — including parallel siblings — are compensated in reverse order.

Pass `(task, compensate_task)` tuples when the compensation isn't already declared via `@saga_task`:

```python
.add_parallel(
    (reserve_hotel, cancel_hotel),
    (reserve_car, cancel_car),
)
```

---

## Example 3: passing data between steps (functional API)

For pipelines where step N needs output from step N-1, use the `@saga` decorator. Tasks decorated with `@saga_task` / `@saga_step` auto-register as steps when you call them inside the saga function — no need to wrap each one in `step(...)`:

```python
from celery_saga import saga, transform

@saga("checkout")
def checkout_saga(input):
    order = create_order(input)

    # Transform order output into the shape charge_payment expects:
    charge_input = transform(order, lambda o: {"amount": o["total"] * 100, "customer": o["customer_id"]})
    payment = charge_payment(charge_input)

    # Pass both order and payment into the final step:
    combined = transform((order, payment), lambda o, p: {**o, **p})
    ship_order(combined)


result = checkout_saga.run(customer_id="cust-7", cart=[...])
```

A few things to know:

- Inside an `@saga` function, calling a `@saga_task`-decorated task **does not execute it** — it registers a `StepRef` placeholder in the plan. Outside this context (workers, tests, direct invocation) the same call runs normally.
- The call accepts at most one positional argument (the input ref — a `StepRef`, `TransformRef`, or literal dict). Pure-literal inputs can be passed as kwargs: `create_order(customer_id="cust-7")`.
- `transform(sources, fn)` runs `fn` on the resolved sources at execution time. Sources can be a single ref or a tuple.
- Lambdas and local functions are serialized for distributed execution — this is why `CELERY_SAGA_SIGNING_KEY` is required.
- The saga function runs **once at decoration time** with `{}` as input to build the plan. Don't put real I/O in it; just declare step calls and transforms.

If you prefer being explicit, the original `step()` form still works and is interoperable with the auto-registering form:

```python
from celery_saga import saga, step, transform

@saga("checkout")
def checkout_saga(input):
    order = step(create_order, input)              # explicit
    payment = charge_payment(order)                 # auto-registering
    ship_order(transform((order, payment), merge))  # mixed is fine
```

---

## Example 4: permanent failure with cleanup data

Sometimes a step does partial work before discovering it can't finish. Raise `PermanentFailure` with the partial state so compensation can clean it up:

```python
from celery_saga import StepResponse, PermanentFailure

@saga_task(app, compensate="tasks.delete_uploaded_files")
def upload_batch(**kwargs):
    uploaded = []
    for file in kwargs["files"]:
        try:
            file_id = storage.upload(file)
            uploaded.append(file_id)
        except QuotaExceeded:
            # We created `uploaded` files but can't finish — clean them up.
            raise PermanentFailure(
                "Quota exceeded mid-upload",
                compensation_data={"file_ids": uploaded},
            )
    return StepResponse(output={"file_ids": uploaded}, compensation_data={"file_ids": uploaded})


@app.task
def delete_uploaded_files(compensation_data):
    for fid in compensation_data["file_ids"]:
        storage.delete(fid)
```

`PermanentFailure` skips retries and immediately triggers compensation — including for the step that raised it.

---

## Example 5: Pydantic models in compensation data

Annotate the `compensation_data` parameter with a Pydantic model and `celery-saga` will serialize/deserialize automatically:

```python
from pydantic import BaseModel

class PaymentRefund(BaseModel):
    transaction_id: str
    amount: float


@saga_task(app, compensate="tasks.refund_payment")
def charge_payment(**kwargs):
    txn = PaymentRefund(transaction_id="txn-1", amount=kwargs["amount"])
    return StepResponse(output={"transaction_id": txn.transaction_id}, compensation_data=txn)


@app.task
def refund_payment(compensation_data: PaymentRefund):
    # compensation_data arrives as a PaymentRefund instance, not a dict
    payment_gateway.refund(compensation_data.transaction_id, compensation_data.amount)
```

---

## Example 6: idempotency

Pass `idempotency_key=` to `run()` to dedupe accidental re-invocations:

```python
result = order_saga.run(
    order_id="order-42",
    amount=59.99,
    idempotency_key="order-42",  # typically the request/order id
)
```

If a saga with the same key is already `RUNNING` or `COMPLETED`, you get a handle to the existing run rather than a duplicate.

---

## Inspecting saga state

```python
from celery_saga import SagaResult, SagaStatus
from app import default_backend  # whatever you passed to set_default_backend

execution = default_backend.load(saga_id)
print(execution.status)               # SagaStatus.COMPLETED | COMPENSATED | FAILED | ...
for step in execution.steps:
    print(step.task_name, step.status, step.output, step.error)
```

---

## Example 7: nested sagas (atomic child model)

A saga can be used as a single step inside another saga. The child runs to completion atomically.

```python
from celery_saga import Saga, saga

payment_saga = (
    Saga("payment")
    .add_step(charge_payment)
    .add_step(reserve_inventory)
)

# Functional form:
@saga("checkout")
def checkout_saga(input):
    order = create_order(input)
    payment_saga.as_step(order, compensate=undo_payment)
    ship_order(order)

# Builder form:
checkout = (
    Saga("checkout")
    .add_step(create_order)
    .add_child(payment_saga, compensate=undo_payment)
    .add_step(ship_order)
)
```

**Semantics:**

- **Child completes** → parent step's output is the child's final context. If the parent later fails, your `compensate` task is called with `{"child_saga_id": "..."}` so you can undo the child as one unit.
- **Child compensates itself** (a step inside the child failed and the child rolled itself back) → parent treats this step as failed but **does not** call your `compensate` for the child (it already cleaned itself up). Earlier parent steps still compensate.
- **Child catastrophically fails** (child's own compensation broke) → parent goes to `SagaFailed`.

**Constraints:**

- The child saga module must be importable on every worker — it's looked up at execution time by name, not serialized across the wire.
- A child saga consumes a worker thread for its entire duration (the parent step blocks on `child_result.get()`).
- Idempotency key for the child is derived as `f"{parent_saga_id}:child:{step_index}"` automatically — parent retries won't double-run the child.

---

## When NOT to use a saga

- **Single-database transactions** — use a real DB transaction.
- **Read-only workflows** — no compensation needed; a plain Celery chain is simpler.
- **Steps that can't be compensated and can't be retried** — sagas only help if you have a meaningful undo.

---

## Operational notes

- **Signing key**: any worker that runs a saga using lambdas must have the same `CELERY_SAGA_SIGNING_KEY` set, otherwise it will refuse to deserialize and the saga will fail.
- **Compensation on worker crash**: if a worker is killed mid-step, the step's saved status is `RUNNING`. If you provided `compensation_data`, it will still be compensated on recovery — so always save compensation data eagerly for steps that have already produced side effects.
- **Queue routing**: pass `queue=` to `.run()` to route saga orchestrator tasks to a specific queue. User task routing (`@app.task(queue=...)`) is honored independently.
