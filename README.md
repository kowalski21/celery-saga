# celery-saga

Saga pattern for [Celery](https://docs.celeryq.dev/) with automatic compensation.

Define distributed transactions as a series of steps, each with a compensation (rollback) function. If any step fails, previously completed steps are automatically compensated in reverse order.

## Install

```bash
pip install celery-saga
```

With Redis state backend:
```bash
pip install celery-saga[redis]
```

## Quick Start

### Define Steps

```python
from celery_saga import saga_step, StepResponse

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
```

### Define and Run a Saga

**Builder API:**

```python
from celery_saga import Saga

order_saga = (
    Saga("order_saga")
    .add_step(validate_order, no_compensation=True)
    .add_step(charge_payment)
    .add_step(reserve_inventory)
    .add_step(send_confirmation, no_compensation=True)
)

result = order_saga.run(order_id="abc-123", amount=99.99)
output = result.get(timeout=30)
```

**Functional API:**

```python
from celery_saga import saga, step

@saga("order_saga")
def order_saga(input):
    order = step(validate_order, input)
    payment = step(charge_payment, order)
    step(send_confirmation, payment)
    return payment

result = order_saga.run(order_id="abc-123", amount=99.99)
```

## Features

### Parallel Steps

```python
from celery_saga import Saga, parallelize

order_saga = (
    Saga("order_saga")
    .add_step(validate_order)
    .add_parallel(charge_payment, reserve_inventory)
    .add_step(send_confirmation)
)
```

### Data Transforms

```python
order_saga = (
    Saga("order_saga")
    .add_step(validate_order)
    # Global context transform
    .add_transform(lambda ctx: {**ctx, "amount_cents": ctx["amount"] * 100})
    .add_step(charge_in_cents)
    # Per-step input mapper
    .add_step(
        send_confirmation,
        input=lambda ctx: {"order_id": ctx["order_id"], "txn": ctx["transaction_id"]},
    )
)
```

### StepResponse

Separates forward output from compensation data:

```python
return StepResponse(
    output={"transaction_id": txn.id},           # passed to next steps
    compensation_data={"transaction_id": txn.id}, # passed to rollback function
)
```

### Permanent Failure

Skip retries and trigger compensation with partial cleanup data:

```python
@saga_step(compensate="cleanup_records")
@app.task
def process_batch(**kwargs):
    processed = []
    for item in kwargs["items"]:
        if item.invalid:
            raise StepResponse.permanent_failure(
                "Invalid item found",
                compensation_data={"processed_ids": processed},
            )
        processed.append(do_work(item))
    return StepResponse(output={"processed": processed})
```

### Idempotency

```python
result = order_saga.run(
    order_id="abc-123",
    idempotency_key="order-abc-123",
)
```

### State Backends

```python
from celery_saga import set_default_backend
from celery_saga.backends import RedisSagaBackend, MemorySagaBackend

# Redis (production)
set_default_backend(RedisSagaBackend(url="redis://localhost:6379/0"))

# Memory (testing)
set_default_backend(MemorySagaBackend())
```

## Saga Lifecycle

```
PENDING → RUNNING → COMPLETED                    (happy path)
                  ↘ COMPENSATING → COMPENSATED    (step failed, rollback succeeded)
                                 ↘ FAILED         (rollback also failed)
```

## How It Works

Under the hood, `saga.run()`:

1. Creates a `SagaExecution` record in the state backend
2. Builds a Celery `chain` with orchestrator tasks between each step
3. Each orchestrator task records step results and merges output into context
4. On failure, reads completed steps and dispatches a reverse compensation chain
5. Compensation tasks run in reverse order of completion

## License

MIT
