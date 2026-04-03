# Examples

Real-world use cases for celery-saga.

## Table of Contents

- [E-Commerce Order Fulfillment](#e-commerce-order-fulfillment)
- [User Registration with External Services](#user-registration-with-external-services)
- [Money Transfer Between Accounts](#money-transfer-between-accounts)
- [Batch Processing with Partial Failure](#batch-processing-with-partial-failure)
- [Multi-Service Booking (Travel)](#multi-service-booking-travel)
- [Data Pipeline with Cleanup](#data-pipeline-with-cleanup)
- [Subscription Upgrade](#subscription-upgrade)

---

## E-Commerce Order Fulfillment

The classic saga example. An order involves payment, inventory, shipping, and notifications — each managed by a different service.

```python
from celery_saga import Saga, StepResponse, saga_task

# ── Steps ──

@saga_task(app, compensate="orders.tasks.refund_payment")
def charge_payment(**kwargs):
    txn = stripe.PaymentIntent.create(
        amount=kwargs["amount_cents"],
        currency="usd",
        customer=kwargs["customer_id"],
    )
    return StepResponse(
        output={"payment_intent_id": txn.id},
        compensation_data={"payment_intent_id": txn.id},
    )


@app.task
def refund_payment(compensation_data):
    stripe.Refund.create(payment_intent=compensation_data["payment_intent_id"])


@saga_task(app, compensate="orders.tasks.release_inventory")
def reserve_inventory(**kwargs):
    reservation = inventory_service.reserve(
        order_id=kwargs["order_id"],
        items=kwargs["items"],
    )
    return StepResponse(
        output={"reservation_id": reservation.id},
        compensation_data={
            "reservation_id": reservation.id,
            "items": kwargs["items"],
        },
    )


@app.task
def release_inventory(compensation_data):
    inventory_service.release(compensation_data["reservation_id"])


@saga_task(app, compensate="orders.tasks.cancel_shipment")
def create_shipment(**kwargs):
    shipment = shipping_service.create(
        order_id=kwargs["order_id"],
        address=kwargs["shipping_address"],
        items=kwargs["items"],
    )
    return StepResponse(
        output={"tracking_number": shipment.tracking_number},
        compensation_data={"shipment_id": shipment.id},
    )


@app.task
def cancel_shipment(compensation_data):
    shipping_service.cancel(compensation_data["shipment_id"])


@saga_task(app, no_compensation=True)
def send_order_confirmation(**kwargs):
    email_service.send(
        to=kwargs["customer_email"],
        template="order_confirmed",
        data={
            "order_id": kwargs["order_id"],
            "tracking_number": kwargs["tracking_number"],
        },
    )


# ── Saga ──

order_saga = (
    Saga("order_fulfillment")
    .add_step(charge_payment)
    .add_parallel(reserve_inventory, create_shipment)
    .add_step(send_order_confirmation)
)


# ── Usage (e.g. in a Django/FastAPI view) ──

def create_order(request):
    result = order_saga.run(
        order_id="ord-123",
        customer_id="cus-456",
        customer_email="user@example.com",
        amount_cents=4999,
        items=[{"sku": "SHIRT-L", "qty": 2}],
        shipping_address={"street": "123 Main St", "city": "Portland"},
        idempotency_key=f"order-ord-123",
    )
    return {"saga_id": result.saga_id, "status": result.status.value}
```

**Why parallel?** Inventory reservation and shipment creation are independent — running them concurrently cuts latency. If either fails, both are compensated.

---

## User Registration with External Services

Register a user across your database, email provider, and analytics platform. If any integration fails, clean up the others.

```python
from celery_saga import Saga, StepResponse, saga_task

@saga_task(app, compensate="users.tasks.delete_user_record")
@app.task
def create_user_record(**kwargs):
    user = db.users.create(
        email=kwargs["email"],
        name=kwargs["name"],
    )
    return StepResponse(
        output={"user_id": user.id},
        compensation_data={"user_id": user.id},
    )


@app.task
def delete_user_record(compensation_data):
    db.users.delete(compensation_data["user_id"])


@saga_task(app, compensate="users.tasks.remove_from_mailchimp")
def add_to_email_list(**kwargs):
    subscriber = mailchimp.lists.add_member(
        list_id=MAIN_LIST_ID,
        email=kwargs["email"],
        name=kwargs["name"],
    )
    return StepResponse(
        output={"subscriber_id": subscriber.id},
        compensation_data={"subscriber_id": subscriber.id},
    )


@app.task
def remove_from_mailchimp(compensation_data):
    mailchimp.lists.remove_member(MAIN_LIST_ID, compensation_data["subscriber_id"])


@saga_task(app, compensate="users.tasks.delete_analytics_user")
def create_analytics_profile(**kwargs):
    segment.identify(
        user_id=kwargs["user_id"],
        traits={"email": kwargs["email"], "name": kwargs["name"]},
    )
    return StepResponse(
        output={"analytics_synced": True},
        compensation_data={"user_id": kwargs["user_id"]},
    )


@app.task
def delete_analytics_user(compensation_data):
    segment.delete(user_id=compensation_data["user_id"])


@saga_task(app, no_compensation=True)
def send_welcome_email(**kwargs):
    email_service.send(to=kwargs["email"], template="welcome")


# ── Saga ──

registration_saga = (
    Saga("user_registration")
    .add_step(create_user_record)
    .add_parallel(add_to_email_list, create_analytics_profile)
    .add_step(send_welcome_email)
)
```

**Key point:** The DB record, email list, and analytics profile must all be consistent. If Mailchimp fails, the DB record and analytics profile are cleaned up — no orphaned data.

---

## Money Transfer Between Accounts

Debit one account and credit another. The classic distributed transaction.

```python
from celery_saga import Saga, StepResponse, saga_task

@saga_task(app, compensate="transfers.tasks.reverse_debit")
def debit_source_account(**kwargs):
    ledger.debit(
        account_id=kwargs["from_account"],
        amount=kwargs["amount"],
        reference=kwargs["transfer_id"],
    )
    return StepResponse(
        output={"debited": True},
        compensation_data={
            "account_id": kwargs["from_account"],
            "amount": kwargs["amount"],
            "reference": kwargs["transfer_id"],
        },
    )


@app.task
def reverse_debit(compensation_data):
    ledger.credit(
        account_id=compensation_data["account_id"],
        amount=compensation_data["amount"],
        reference=f"reversal-{compensation_data['reference']}",
    )


@saga_task(app, compensate="transfers.tasks.reverse_credit")
def credit_destination_account(**kwargs):
    ledger.credit(
        account_id=kwargs["to_account"],
        amount=kwargs["amount"],
        reference=kwargs["transfer_id"],
    )
    return StepResponse(
        output={"credited": True},
        compensation_data={
            "account_id": kwargs["to_account"],
            "amount": kwargs["amount"],
            "reference": kwargs["transfer_id"],
        },
    )


@app.task
def reverse_credit(compensation_data):
    ledger.debit(
        account_id=compensation_data["account_id"],
        amount=compensation_data["amount"],
        reference=f"reversal-{compensation_data['reference']}",
    )


@saga_task(app, no_compensation=True)
def record_transfer(**kwargs):
    db.transfers.update(
        transfer_id=kwargs["transfer_id"],
        status="completed",
    )


transfer_saga = (
    Saga("money_transfer")
    .add_step(debit_source_account)
    .add_step(credit_destination_account)
    .add_step(record_transfer)
)

# Usage
result = transfer_saga.run(
    transfer_id="txfr-789",
    from_account="acc-001",
    to_account="acc-002",
    amount=250_00,  # cents
    idempotency_key="txfr-789",
)
```

**Why idempotency?** A transfer retry (e.g. from a webhook) must not debit twice.

---

## Batch Processing with Partial Failure

Process a list of items. If processing fails partway through, clean up only the items that were already processed using `permanent_failure`.

```python
from celery_saga import Saga, StepResponse, saga_task, PermanentFailure

@saga_task(app, compensate="etl.tasks.delete_imported_records")
def import_records(**kwargs):
    records = kwargs["records"]
    imported_ids = []

    for record in records:
        try:
            result = db.imports.create(data=record)
            imported_ids.append(result.id)
        except ValidationError as e:
            # Fail permanently — pass the IDs we already imported for cleanup
            StepResponse.permanent_failure(
                f"Validation failed on record {record['id']}: {e}",
                compensation_data={"imported_ids": imported_ids},
            )

    return StepResponse(
        output={"imported_ids": imported_ids, "count": len(imported_ids)},
        compensation_data={"imported_ids": imported_ids},
    )


@app.task
def delete_imported_records(compensation_data):
    for record_id in compensation_data["imported_ids"]:
        db.imports.delete(record_id)


@saga_task(app, compensate="etl.tasks.unindex_records")
def index_in_elasticsearch(**kwargs):
    es.bulk_index("imports", kwargs["imported_ids"])
    return StepResponse(
        output={"indexed": True},
        compensation_data={"imported_ids": kwargs["imported_ids"]},
    )


@app.task
def unindex_records(compensation_data):
    es.bulk_delete("imports", compensation_data["imported_ids"])


@saga_task(app, no_compensation=True)
def notify_completion(**kwargs):
    slack.post(f"Imported {kwargs['count']} records successfully.")


import_saga = (
    Saga("batch_import")
    .add_step(import_records)
    .add_step(index_in_elasticsearch)
    .add_step(notify_completion)
)
```

**Key point:** `permanent_failure` lets you fail fast while preserving exactly which records need cleanup. The compensation function doesn't guess — it knows.

---

## Multi-Service Booking (Travel)

Book a flight, hotel, and car rental. All three must succeed or all are cancelled.

```python
from celery_saga import Saga, StepResponse, saga_task

@saga_task(app, compensate="bookings.tasks.cancel_flight")
def book_flight(**kwargs):
    booking = airline_api.book(
        origin=kwargs["origin"],
        destination=kwargs["destination"],
        date=kwargs["departure_date"],
        passenger=kwargs["passenger"],
    )
    return StepResponse(
        output={
            "flight_confirmation": booking.confirmation_code,
            "flight_price": booking.price,
        },
        compensation_data={"confirmation_code": booking.confirmation_code},
    )


@app.task
def cancel_flight(compensation_data):
    airline_api.cancel(compensation_data["confirmation_code"])


@saga_task(app, compensate="bookings.tasks.cancel_hotel")
def book_hotel(**kwargs):
    booking = hotel_api.reserve(
        city=kwargs["destination"],
        checkin=kwargs["departure_date"],
        checkout=kwargs["return_date"],
        guest=kwargs["passenger"],
    )
    return StepResponse(
        output={
            "hotel_confirmation": booking.confirmation_code,
            "hotel_price": booking.price,
        },
        compensation_data={"confirmation_code": booking.confirmation_code},
    )


@app.task
def cancel_hotel(compensation_data):
    hotel_api.cancel(compensation_data["confirmation_code"])


@saga_task(app, compensate="bookings.tasks.cancel_car")
def book_rental_car(**kwargs):
    booking = car_api.reserve(
        city=kwargs["destination"],
        pickup=kwargs["departure_date"],
        dropoff=kwargs["return_date"],
        driver=kwargs["passenger"],
    )
    return StepResponse(
        output={
            "car_confirmation": booking.confirmation_code,
            "car_price": booking.price,
        },
        compensation_data={"confirmation_code": booking.confirmation_code},
    )


@app.task
def cancel_car(compensation_data):
    car_api.cancel(compensation_data["confirmation_code"])


@saga_task(app, no_compensation=True)
def send_itinerary(**kwargs):
    total = kwargs["flight_price"] + kwargs["hotel_price"] + kwargs["car_price"]
    email_service.send(
        to=kwargs["email"],
        template="travel_itinerary",
        data={
            "flight": kwargs["flight_confirmation"],
            "hotel": kwargs["hotel_confirmation"],
            "car": kwargs["car_confirmation"],
            "total": total,
        },
    )


travel_saga = (
    Saga("travel_booking")
    .add_parallel(book_flight, book_hotel, book_rental_car)
    .add_step(send_itinerary)
)

# Usage
result = travel_saga.run(
    passenger="Jane Doe",
    email="jane@example.com",
    origin="SFO",
    destination="JFK",
    departure_date="2026-06-15",
    return_date="2026-06-22",
)
```

**Why parallel?** The three bookings are independent. If the car rental API is down, the flight and hotel are automatically cancelled — no orphaned bookings.

---

## Data Pipeline with Cleanup

Extract data from a source, transform it, load it into a warehouse. If loading fails, clean up the staging artifacts.

```python
from celery_saga import Saga, StepResponse, saga_task

@saga_task(app, compensate="pipeline.tasks.delete_staging_file")
def extract_to_staging(**kwargs):
    path = s3.extract(
        source=kwargs["source_table"],
        bucket=STAGING_BUCKET,
        prefix=f"staging/{kwargs['pipeline_run_id']}/",
    )
    return StepResponse(
        output={"staging_path": path, "row_count": s3.count_rows(path)},
        compensation_data={"staging_path": path},
    )


@app.task
def delete_staging_file(compensation_data):
    s3.delete_prefix(STAGING_BUCKET, compensation_data["staging_path"])


@saga_task(app, compensate="pipeline.tasks.drop_temp_table")
def transform_and_stage(**kwargs):
    temp_table = f"tmp_{kwargs['pipeline_run_id']}"
    warehouse.execute(f"""
        CREATE TABLE {temp_table} AS
        SELECT * FROM staging.read_parquet('{kwargs["staging_path"]}')
        WHERE valid = true
    """)
    return StepResponse(
        output={"temp_table": temp_table},
        compensation_data={"temp_table": temp_table},
    )


@app.task
def drop_temp_table(compensation_data):
    warehouse.execute(f"DROP TABLE IF EXISTS {compensation_data['temp_table']}")


@saga_task(app, no_compensation=True)
def swap_into_production(**kwargs):
    warehouse.execute(f"""
        BEGIN;
        DROP TABLE IF EXISTS {kwargs['target_table']};
        ALTER TABLE {kwargs['temp_table']} RENAME TO {kwargs['target_table']};
        COMMIT;
    """)
    return StepResponse(output={"loaded": True})


@saga_task(app, no_compensation=True)
def cleanup_staging(**kwargs):
    s3.delete_prefix(STAGING_BUCKET, kwargs["staging_path"])


etl_saga = (
    Saga("data_pipeline")
    .add_step(extract_to_staging)
    .add_step(transform_and_stage)
    .add_step(swap_into_production)
    .add_step(cleanup_staging)
)
```

**Key point:** If the warehouse load fails, the temp table and staging file are cleaned up automatically. No manual garbage collection.

---

## Subscription Upgrade

Upgrade a user's subscription: update the billing, provision new features, notify the user. Uses `add_transform` to convert pricing.

```python
from celery_saga import Saga, StepResponse, saga_task

@saga_task(app, no_compensation=True)
def validate_upgrade(**kwargs):
    user = db.users.get(kwargs["user_id"])
    plan = db.plans.get(kwargs["new_plan_id"])

    if user.plan_id == plan.id:
        StepResponse.permanent_failure("Already on this plan")

    return StepResponse(output={
        "user_id": user.id,
        "current_plan_id": user.plan_id,
        "new_plan_id": plan.id,
        "plan_name": plan.name,
        "price": plan.price,
    })


@saga_task(app, compensate="billing.tasks.revert_subscription")
def update_billing(**kwargs):
    subscription = stripe.Subscription.modify(
        kwargs["stripe_subscription_id"],
        items=[{"price": kwargs["stripe_price_id"]}],
        proration_behavior="create_prorations",
    )
    return StepResponse(
        output={"subscription_updated": True},
        compensation_data={
            "stripe_subscription_id": kwargs["stripe_subscription_id"],
            "previous_price_id": kwargs["current_stripe_price_id"],
        },
    )


@app.task
def revert_subscription(compensation_data):
    stripe.Subscription.modify(
        compensation_data["stripe_subscription_id"],
        items=[{"price": compensation_data["previous_price_id"]}],
    )


@saga_task(app, compensate="billing.tasks.revert_plan_record")
def update_plan_record(**kwargs):
    db.users.update(kwargs["user_id"], plan_id=kwargs["new_plan_id"])
    return StepResponse(
        output={"plan_updated": True},
        compensation_data={
            "user_id": kwargs["user_id"],
            "previous_plan_id": kwargs["current_plan_id"],
        },
    )


@app.task
def revert_plan_record(compensation_data):
    db.users.update(
        compensation_data["user_id"],
        plan_id=compensation_data["previous_plan_id"],
    )


@saga_task(app, compensate="billing.tasks.revoke_features")
def provision_features(**kwargs):
    features = feature_service.provision(
        user_id=kwargs["user_id"],
        plan_id=kwargs["new_plan_id"],
    )
    return StepResponse(
        output={"features_provisioned": features},
        compensation_data={
            "user_id": kwargs["user_id"],
            "features": features,
        },
    )


@app.task
def revoke_features(compensation_data):
    feature_service.revoke(
        user_id=compensation_data["user_id"],
        features=compensation_data["features"],
    )


@saga_task(app, no_compensation=True)
def notify_upgrade(**kwargs):
    email_service.send(
        to=kwargs["user_email"],
        template="plan_upgraded",
        data={"plan_name": kwargs["plan_name"]},
    )


upgrade_saga = (
    Saga("subscription_upgrade")
    .add_step(validate_upgrade)
    .add_step(update_billing)
    .add_parallel(update_plan_record, provision_features)
    .add_step(notify_upgrade)
)

# Usage
result = upgrade_saga.run(
    user_id="usr-123",
    user_email="user@example.com",
    new_plan_id="plan-pro",
    stripe_subscription_id="sub_abc",
    stripe_price_id="price_pro_monthly",
    current_stripe_price_id="price_free",
)
```

**Why saga?** If feature provisioning fails after billing was updated, the user would be charged for features they can't use. The saga rolls back billing automatically.

---

## Patterns Summary

| Pattern | When to use | Key features |
|---------|-------------|--------------|
| **Sequential** | Steps depend on each other | `add_step` chain |
| **Parallel** | Independent steps that all must succeed | `add_parallel` |
| **Partial failure** | Batch processing, loop-and-fail | `permanent_failure` with partial data |
| **Idempotent** | Webhook handlers, retryable APIs | `idempotency_key` |
| **Transform** | Steps need data in different shapes | `add_transform` or `input=` |
| **Fire-and-forget** | Notifications, logging | `no_compensation=True` |
