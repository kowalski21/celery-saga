"""Example 4: atomic child saga.

A payment-sub-saga is composed into the parent checkout saga. The child runs
to completion atomically; the parent treats it as one step.
"""

from _setup import app, backend  # noqa: F401

from celery_saga import Saga, SagaCompensated, StepResponse, saga_task


@app.task
def undo_charge(compensation_data):
    print(f"    [child] undo_charge: {compensation_data['txn']}")


@app.task
def undo_record(compensation_data):
    print(f"    [child] undo_record: {compensation_data['ledger_id']}")


@saga_task(app, compensate=undo_charge)
def charge(**kwargs):
    print(f"    [child] charge: ${kwargs['amount']}")
    return StepResponse(
        output={"txn": "txn-x"},
        compensation_data={"txn": "txn-x", "amount": kwargs["amount"]},
    )


@saga_task(app, compensate=undo_record)
def record_ledger(**kwargs):
    print(f"    [child] record_ledger: {kwargs.get('txn')}")
    return StepResponse(output={"ledger_id": "L-1"}, compensation_data={"ledger_id": "L-1"})


# Child payment saga
payment_saga = (
    Saga("payment")
    .add_step(charge)
    .add_step(record_ledger)
)


@saga_task(app, no_compensation=True)
def validate(**kwargs):
    print(f"  [parent] validate: {kwargs['order_id']}")
    return StepResponse(output={"order_id": kwargs["order_id"], "amount": kwargs["amount"]})


@app.task
def undo_payment_child(compensation_data):
    # Called only when child succeeded AND a later parent step failed.
    print(f"  [parent] undo_payment_child: child_saga_id={compensation_data['child_saga_id']}")


@saga_task(app)
def ship_or_fail(**kwargs):
    raise ValueError("shipping carrier down")


def main():
    print("→ happy path:")
    checkout = (
        Saga("checkout_happy")
        .add_step(validate)
        .add_child(payment_saga, compensate=undo_payment_child)
    )
    result = checkout.run(order_id="ord-1", amount=20.0)
    result.get(timeout=5)
    print(f"  ✓ completed: status={result.status.value}\n")

    print("→ parent step after child fails — user compensate runs:")
    checkout_fail = (
        Saga("checkout_fail")
        .add_step(validate)
        .add_child(payment_saga, compensate=undo_payment_child)
        .add_step(ship_or_fail)
    )
    result = checkout_fail.run(order_id="ord-2", amount=33.0)
    try:
        result.get(timeout=5)
    except SagaCompensated:
        print(f"  ✓ compensated via user undo_payment_child: status={result.status.value}")


if __name__ == "__main__":
    main()
