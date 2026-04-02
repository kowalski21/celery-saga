# Quickstart

Get a saga running in 5 minutes.

## Prerequisites

- Python 3.10+
- A running Redis instance (for both Celery broker and saga state)

## 1. Install

```bash
pip install celery-saga[redis]
```

## 2. Create your Celery app

```python
# app.py
from celery import Celery

app = Celery("myapp", broker="redis://localhost:6379/0")
app.conf.update(result_backend="redis://localhost:6379/0")
```

## 3. Configure celery-saga

```python
# app.py (continued)
from celery_saga import set_default_backend
from celery_saga.backends import RedisSagaBackend

set_default_backend(RedisSagaBackend(url="redis://localhost:6379/1"))
```

## 4. Define your steps

```python
# tasks.py
from app import app
from celery_saga import saga_step, StepResponse


@saga_step(compensate="tasks.refund_payment")
@app.task
def charge_payment(**kwargs):
    order_id = kwargs["order_id"]
    amount = kwargs["amount"]
    # ... call your payment provider ...
    transaction_id = f"txn-{order_id}"
    return StepResponse(
        output={"transaction_id": transaction_id},
        compensation_data={"transaction_id": transaction_id, "amount": amount},
    )


@app.task
def refund_payment(compensation_data):
    # ... call your payment provider to refund ...
    print(f"Refunded {compensation_data['amount']} for {compensation_data['transaction_id']}")


@saga_step(compensate="tasks.release_inventory")
@app.task
def reserve_inventory(**kwargs):
    order_id = kwargs["order_id"]
    # ... reserve items in warehouse ...
    reservation_id = f"res-{order_id}"
    return StepResponse(
        output={"reservation_id": reservation_id},
        compensation_data={"reservation_id": reservation_id},
    )


@app.task
def release_inventory(compensation_data):
    # ... release the reservation ...
    print(f"Released reservation {compensation_data['reservation_id']}")


@saga_step(no_compensation=True)
@app.task
def send_confirmation_email(**kwargs):
    order_id = kwargs["order_id"]
    transaction_id = kwargs["transaction_id"]
    # ... send email ...
    print(f"Confirmation sent for order {order_id}, txn {transaction_id}")
    return StepResponse(output={"email_sent": True})
```

## 5. Define and run the saga

```python
# run_saga.py
from celery_saga import Saga, SagaCompensated, SagaFailed
from tasks import charge_payment, reserve_inventory, send_confirmation_email

order_saga = (
    Saga("order_saga")
    .add_step(charge_payment)
    .add_step(reserve_inventory)
    .add_step(send_confirmation_email)
)

result = order_saga.run(order_id="order-42", amount=59.99)

try:
    output = result.get(timeout=30)
    print(f"Order completed: {output}")
except SagaCompensated as e:
    print(f"Order rolled back: {e}")
except SagaFailed as e:
    print(f"Order failed critically: {e}")
```

## 6. Start a Celery worker

```bash
celery -A app worker --loglevel=info
```

Then in another terminal:

```bash
python run_saga.py
```

## What happens on failure

If `reserve_inventory` fails:

1. `charge_payment` already succeeded — `refund_payment` is called with the saved compensation data
2. `reserve_inventory` failed — nothing to compensate (it didn't complete)
3. `result.get()` raises `SagaCompensated`

If `send_confirmation_email` fails:

1. `reserve_inventory` is compensated via `release_inventory`
2. `charge_payment` is compensated via `refund_payment`
3. `send_confirmation_email` has `no_compensation=True` — skipped
4. Compensation runs in reverse: inventory first, then payment

---

Next: see [Examples](examples.md) for real-world patterns, or [Guide](guide.md) for the full API.
