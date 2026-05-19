"""Example 1: builder API.

A classic order-processing saga: charge payment, reserve inventory, send
confirmation. If anything fails, earlier successful steps are compensated.
"""

from _setup import app, backend  # noqa: F401

from celery_saga import Saga, SagaCompensated, StepResponse, saga_task


@app.task
def refund_payment(compensation_data):
    print(f"  refund_payment: ${compensation_data['amount']} ({compensation_data['transaction_id']})")


@app.task
def release_inventory(compensation_data):
    print(f"  release_inventory: {compensation_data['reservation_id']}")


@saga_task(app, compensate=refund_payment)
def charge_payment(**kwargs):
    print(f"  charge_payment: ${kwargs['amount']} for {kwargs['order_id']}")
    return StepResponse(
        output={"transaction_id": f"txn-{kwargs['order_id']}"},
        compensation_data={
            "transaction_id": f"txn-{kwargs['order_id']}",
            "amount": kwargs["amount"],
        },
    )


@saga_task(app, compensate=release_inventory)
def reserve_inventory(**kwargs):
    print(f"  reserve_inventory: {kwargs['order_id']}")
    return StepResponse(
        output={"reservation_id": f"res-{kwargs['order_id']}"},
        compensation_data={"reservation_id": f"res-{kwargs['order_id']}"},
    )


@saga_task(app, no_compensation=True)
def send_confirmation(**kwargs):
    print(f"  send_confirmation: {kwargs['order_id']}")
    return StepResponse(output={"emailed": True})


def main():
    print("→ happy path:")
    order_saga = (
        Saga("order_saga")
        .add_step(charge_payment)
        .add_step(reserve_inventory)
        .add_step(send_confirmation)
    )
    result = order_saga.run(order_id="order-1", amount=49.99)
    context = result.get(timeout=5)
    print(f"  ✓ completed: status={result.status.value}\n")

    # Force a failure by passing bad input — reserve will raise.
    print("→ failure rolls back:")

    @saga_task(app)
    def will_fail(**kwargs):
        raise ValueError("inventory system down")

    failing_saga = (
        Saga("failing_saga")
        .add_step(charge_payment)
        .add_step(will_fail)
    )
    result = failing_saga.run(order_id="order-2", amount=12.50)
    try:
        result.get(timeout=5)
    except SagaCompensated:
        print(f"  ✓ compensated: status={result.status.value}")


if __name__ == "__main__":
    main()
