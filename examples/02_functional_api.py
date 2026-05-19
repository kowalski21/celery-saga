"""Example 2: functional API with auto-registration.

Inside an @saga function, calling a @saga_task-decorated task auto-registers
it as a step. transform() wires data between steps.
"""

from _setup import app, backend  # noqa: F401

from celery_saga import StepResponse, saga, saga_task, transform


@saga_task(app, no_compensation=True)
def create_order(**kwargs):
    print(f"  create_order: customer={kwargs['customer_id']}")
    return StepResponse(output={"order_id": "ord-9", "total": 75.0, "customer_id": kwargs["customer_id"]})


@app.task
def refund(compensation_data):
    print(f"  refund: {compensation_data['txn']}")


@saga_task(app, compensate=refund)
def charge(**kwargs):
    print(f"  charge: ${kwargs['amount_cents'] / 100:.2f} for {kwargs['customer']}")
    return StepResponse(
        output={"txn": "txn-9"},
        compensation_data={"txn": "txn-9", "amount_cents": kwargs["amount_cents"]},
    )


@saga_task(app, no_compensation=True)
def ship(**kwargs):
    print(f"  ship: order={kwargs.get('order_id')} txn={kwargs.get('txn')}")
    return StepResponse(output={"shipped": True})


@saga("checkout")
def checkout_saga(input):
    order = create_order(input)
    # Reshape order output to match charge's expected input
    charge_input = transform(
        order,
        lambda o: {"amount_cents": int(o["total"] * 100), "customer": o["customer_id"]},
    )
    payment = charge(charge_input)
    # Pass both order and payment into ship
    ship_input = transform((order, payment), lambda o, p: {**o, **p})
    ship(ship_input)


def main():
    result = checkout_saga.run(customer_id="cust-7")
    result.get(timeout=5)
    print(f"  ✓ completed: status={result.status.value}")


if __name__ == "__main__":
    main()
