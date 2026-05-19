"""Example 3: parallel steps.

Reserve a flight, then reserve hotel and car in parallel, then charge.
A failure anywhere triggers reverse-order compensation including parallel siblings.
"""

from _setup import app, backend  # noqa: F401

from celery_saga import Saga, SagaCompensated, StepResponse, saga_task


@app.task
def cancel_flight(compensation_data):
    print(f"  cancel_flight: {compensation_data['flight']}")


@app.task
def cancel_hotel(compensation_data):
    print(f"  cancel_hotel: {compensation_data['hotel']}")


@app.task
def cancel_car(compensation_data):
    print(f"  cancel_car: {compensation_data['car']}")


@saga_task(app, compensate=cancel_flight)
def reserve_flight(**kwargs):
    print(f"  reserve_flight: {kwargs['trip_id']}")
    return StepResponse(output={"flight": "F-1"}, compensation_data={"flight": "F-1"})


@saga_task(app, compensate=cancel_hotel)
def reserve_hotel(**kwargs):
    print(f"  reserve_hotel: {kwargs['trip_id']}")
    return StepResponse(output={"hotel": "H-1"}, compensation_data={"hotel": "H-1"})


@saga_task(app, compensate=cancel_car)
def reserve_car(**kwargs):
    print(f"  reserve_car: {kwargs['trip_id']}")
    return StepResponse(output={"car": "C-1"}, compensation_data={"car": "C-1"})


@saga_task(app)
def charge_trip(**kwargs):
    raise ValueError("card declined")


def main():
    booking = (
        Saga("trip_booking")
        .add_step(reserve_flight)
        .add_parallel(reserve_hotel, reserve_car)
        .add_step(charge_trip)  # fails — rolls back everything
    )
    result = booking.run(trip_id="trip-42")
    try:
        result.get(timeout=5)
    except SagaCompensated:
        print(f"  ✓ all reservations cancelled: status={result.status.value}")


if __name__ == "__main__":
    main()
