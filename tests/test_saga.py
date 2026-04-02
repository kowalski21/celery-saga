"""Tests for celery-saga using Celery's eager mode (no broker needed)."""

import pytest
from celery import Celery

from celery_saga import (
    Saga,
    SagaCompensated,
    SagaStatus,
    StepResponse,
    StepStatus,
    saga,
    saga_step,
    set_default_backend,
    step,
    parallelize,
    transform,
)
from celery_saga.backends.memory import MemorySagaBackend

# ── Test Celery App (eager mode) ──

app = Celery("test")
app.config_from_object({
    "task_always_eager": True,
    "task_eager_propagates": True,
    "result_backend": "cache+memory://",
})

# ── Shared state for tracking side effects ──

side_effects = []
backend = MemorySagaBackend()


@pytest.fixture(autouse=True)
def reset():
    side_effects.clear()
    backend._store.clear()
    backend._idem.clear()
    set_default_backend(backend)
    yield


# ── Sample Tasks ──

@saga_step(no_compensation=True)
@app.task
def validate_order(**kwargs):
    order_id = kwargs.get("order_id")
    side_effects.append(f"validate:{order_id}")
    return StepResponse(
        output={"order_id": order_id, "amount": kwargs.get("amount", 0)},
    )


@saga_step(compensate="tests.test_saga.refund_payment")
@app.task
def charge_payment(**kwargs):
    order_id = kwargs.get("order_id")
    amount = kwargs.get("amount", 0)
    side_effects.append(f"charge:{order_id}:{amount}")
    return StepResponse(
        output={"transaction_id": f"txn-{order_id}"},
        compensation_data={"transaction_id": f"txn-{order_id}", "amount": amount},
    )


@app.task
def refund_payment(compensation_data):
    side_effects.append(f"refund:{compensation_data['transaction_id']}:{compensation_data['amount']}")


@saga_step(compensate="tests.test_saga.release_inventory")
@app.task
def reserve_inventory(**kwargs):
    order_id = kwargs.get("order_id")
    side_effects.append(f"reserve:{order_id}")
    return StepResponse(
        output={"reservation_id": f"res-{order_id}"},
        compensation_data={"reservation_id": f"res-{order_id}"},
    )


@app.task
def release_inventory(compensation_data):
    side_effects.append(f"release:{compensation_data['reservation_id']}")


@saga_step(no_compensation=True)
@app.task
def send_confirmation(**kwargs):
    order_id = kwargs.get("order_id")
    txn = kwargs.get("transaction_id")
    side_effects.append(f"confirm:{order_id}:{txn}")
    return StepResponse(output={"confirmed": True})


@saga_step(compensate="tests.test_saga.refund_payment")
@app.task
def charge_payment_fails(**kwargs):
    side_effects.append("charge:fail")
    raise ValueError("Payment declined")


# ── Tests: Builder API ──


class TestBuilderAPI:
    def test_simple_saga_completes(self):
        order_saga = (
            Saga("order_saga", backend=backend)
            .add_step(validate_order)
            .add_step(charge_payment)
            .add_step(send_confirmation)
        )

        result = order_saga.run(order_id="abc", amount=99)
        output = result.get(timeout=5)

        assert result.status == SagaStatus.COMPLETED
        assert "validate:abc" in side_effects
        assert "charge:abc:99" in side_effects
        assert "confirm:abc:txn-abc" in side_effects
        assert output["transaction_id"] == "txn-abc"
        assert output["confirmed"] is True

    def test_compensation_on_failure(self):
        order_saga = (
            Saga("order_saga", backend=backend)
            .add_step(validate_order)
            .add_step(charge_payment)
            .add_step(charge_payment_fails)  # this will fail
        )

        result = order_saga.run(order_id="abc", amount=50)

        # Should compensate
        with pytest.raises(SagaCompensated):
            result.get(timeout=5)

        # charge_payment should have been compensated (refunded)
        assert "refund:txn-abc:50" in side_effects
        # validate_order has no_compensation — should not be compensated
        assert not any(e.startswith("release:") for e in side_effects)

    def test_idempotency(self):
        order_saga = Saga("order_saga", backend=backend)
        order_saga.add_step(validate_order)

        r1 = order_saga.run(order_id="abc", idempotency_key="order-abc")
        r1.get(timeout=5)

        r2 = order_saga.run(order_id="abc", idempotency_key="order-abc")
        assert r1.saga_id == r2.saga_id


class TestStepResponse:
    def test_output_and_compensation_data_separate(self):
        resp = StepResponse(output={"id": 1}, compensation_data={"id": 1, "extra": True})
        assert resp.output == {"id": 1}
        assert resp.compensation_data == {"id": 1, "extra": True}

    def test_compensation_data_defaults_to_output(self):
        resp = StepResponse(output={"id": 1})
        assert resp.compensation_data == {"id": 1}

    def test_permanent_failure(self):
        from celery_saga.step import PermanentFailure

        with pytest.raises(PermanentFailure) as exc_info:
            StepResponse.permanent_failure("boom", compensation_data={"id": 1})

        assert exc_info.value.compensation_data == {"id": 1}
        assert str(exc_info.value) == "boom"


class TestState:
    def test_saga_execution_serialization(self):
        from celery_saga.state import SagaExecution, StepExecution

        execution = SagaExecution(
            saga_id="test-123",
            saga_name="test_saga",
            steps=[
                StepExecution(step_index=0, task_name="task_a", status=StepStatus.SUCCESS),
                StepExecution(step_index=1, task_name="task_b", status=StepStatus.FAILED),
            ],
        )

        data = execution.to_dict()
        restored = SagaExecution.from_dict(data)

        assert restored.saga_id == "test-123"
        assert restored.steps[0].status == StepStatus.SUCCESS
        assert restored.steps[1].status == StepStatus.FAILED


class TestFunctionalAPI:
    def test_functional_saga(self):
        @saga("func_saga")
        def my_saga(input):
            order = step(validate_order, input)
            payment = step(charge_payment, order)
            step(send_confirmation, payment)
            return payment

        my_saga._backend = backend
        result = my_saga.run(order_id="func-1", amount=200)
        output = result.get(timeout=5)

        assert result.status == SagaStatus.COMPLETED
        assert "validate:func-1" in side_effects
        assert "charge:func-1:200" in side_effects

    def test_functional_saga_with_compensation(self):
        @saga("func_fail_saga")
        def my_saga(input):
            order = step(validate_order, input)
            payment = step(charge_payment, order)
            step(charge_payment_fails, payment)
            return payment

        my_saga._backend = backend
        result = my_saga.run(order_id="func-2", amount=75)

        with pytest.raises(SagaCompensated):
            result.get(timeout=5)

        assert "charge:func-2:75" in side_effects
        assert "charge:fail" in side_effects
        assert "refund:txn-func-2:75" in side_effects


# ── Tests: Parallel Steps ──


@saga_step(compensate="tests.test_saga.undo_shipping")
@app.task
def create_shipping(**kwargs):
    order_id = kwargs.get("order_id")
    side_effects.append(f"ship:{order_id}")
    return StepResponse(
        output={"shipping_id": f"ship-{order_id}"},
        compensation_data={"shipping_id": f"ship-{order_id}"},
    )


@app.task
def undo_shipping(compensation_data):
    side_effects.append(f"undo_ship:{compensation_data['shipping_id']}")


@app.task
def fail_after_parallel(**kwargs):
    side_effects.append("fail_after_parallel")
    raise ValueError("Post-parallel failure")


class TestParallelSteps:
    def test_parallel_steps_complete(self):
        order_saga = (
            Saga("parallel_saga", backend=backend)
            .add_step(validate_order)
            .add_parallel(charge_payment, reserve_inventory)
            .add_step(send_confirmation)
        )

        result = order_saga.run(order_id="par-1", amount=100)
        output = result.get(timeout=5)

        assert result.status == SagaStatus.COMPLETED
        assert "validate:par-1" in side_effects
        assert "charge:par-1:100" in side_effects
        assert "reserve:par-1" in side_effects
        assert "confirm:par-1:txn-par-1" in side_effects
        # Both parallel outputs should be in context
        assert output["transaction_id"] == "txn-par-1"
        assert output["reservation_id"] == "res-par-1"

    def test_parallel_steps_with_compensation(self):
        """When a step after parallel group fails, parallel steps should compensate."""
        order_saga = (
            Saga("parallel_comp_saga", backend=backend)
            .add_step(validate_order)
            .add_parallel(charge_payment, reserve_inventory)
            .add_step(charge_payment_fails)  # fails after parallel
        )

        result = order_saga.run(order_id="par-2", amount=60)

        with pytest.raises(SagaCompensated):
            result.get(timeout=5)

        # Both parallel steps should be compensated in reverse
        assert "refund:txn-par-2:60" in side_effects
        assert "release:res-par-2" in side_effects

    def test_parallel_with_tuples(self):
        """Builder API with (task, compensate) tuples."""
        order_saga = (
            Saga("tuple_saga", backend=backend)
            .add_step(validate_order, no_compensation=True)
            .add_parallel(
                (charge_payment, refund_payment),
                (reserve_inventory, release_inventory),
            )
        )

        result = order_saga.run(order_id="tup-1", amount=200)
        output = result.get(timeout=5)

        assert result.status == SagaStatus.COMPLETED
        assert "charge:tup-1:200" in side_effects
        assert "reserve:tup-1" in side_effects


# ── Tests: Transform ──


@app.task
def charge_in_cents(**kwargs):
    amount_cents = kwargs.get("amount_cents", 0)
    order_id = kwargs.get("order_id")
    side_effects.append(f"charge_cents:{order_id}:{amount_cents}")
    return StepResponse(
        output={"transaction_id": f"txn-{order_id}", "amount_cents": amount_cents},
        compensation_data={"transaction_id": f"txn-{order_id}", "amount_cents": amount_cents},
    )


class TestTransform:
    def test_builder_transform(self):
        order_saga = (
            Saga("transform_saga", backend=backend)
            .add_step(validate_order)
            .add_transform(lambda ctx: {**ctx, "amount_cents": ctx["amount"] * 100})
            .add_step(charge_in_cents, no_compensation=True)
        )

        result = order_saga.run(order_id="tr-1", amount=49)
        output = result.get(timeout=5)

        assert result.status == SagaStatus.COMPLETED
        assert "charge_cents:tr-1:4900" in side_effects
        assert output["amount_cents"] == 4900

    def test_builder_input_fn(self):
        """Per-step input mapper."""
        order_saga = (
            Saga("input_fn_saga", backend=backend)
            .add_step(validate_order)
            .add_step(
                charge_in_cents,
                no_compensation=True,
                input=lambda ctx: {
                    "order_id": ctx["order_id"],
                    "amount_cents": int(ctx["amount"] * 100),
                },
            )
        )

        result = order_saga.run(order_id="ifn-1", amount=29.99)
        output = result.get(timeout=5)

        assert result.status == SagaStatus.COMPLETED
        assert "charge_cents:ifn-1:2999" in side_effects


# ── Tests: Permanent Failure with Compensation Data ──


@saga_step(compensate="tests.test_saga.cleanup_partial")
@app.task
def process_batch(**kwargs):
    """Processes items one by one, fails partway through."""
    items = kwargs.get("items", [])
    processed = []
    for item in items:
        if item == "bad":
            raise PermanentFailure(
                f"Failed on item 'bad' after processing {len(processed)}",
                compensation_data={"processed_ids": processed},
            )
        processed.append(item)
        side_effects.append(f"process:{item}")
    return StepResponse(
        output={"processed": processed},
        compensation_data={"processed_ids": processed},
    )


@app.task
def cleanup_partial(compensation_data):
    for pid in compensation_data["processed_ids"]:
        side_effects.append(f"cleanup:{pid}")


from celery_saga.step import PermanentFailure


class TestPermanentFailure:
    def test_permanent_failure_triggers_compensation_with_partial_data(self):
        batch_saga = (
            Saga("batch_saga", backend=backend)
            .add_step(process_batch)
        )

        result = batch_saga.run(items=["a", "b", "bad", "c"])

        with pytest.raises(SagaCompensated):
            result.get(timeout=5)

        # Should have processed a and b before failing
        assert "process:a" in side_effects
        assert "process:b" in side_effects
        assert "process:bad" not in side_effects  # failed before appending

        # Compensation should clean up the two that succeeded
        assert "cleanup:a" in side_effects
        assert "cleanup:b" in side_effects


# ── Tests: Edge Cases ──


class TestEdgeCases:
    def test_single_step_saga(self):
        s = Saga("single", backend=backend).add_step(validate_order)
        result = s.run(order_id="single-1")
        output = result.get(timeout=5)
        assert result.status == SagaStatus.COMPLETED
        assert "validate:single-1" in side_effects

    def test_no_compensation_steps_skip_during_rollback(self):
        """Steps marked no_compensation should not be compensated."""
        s = (
            Saga("nocomp_saga", backend=backend)
            .add_step(validate_order)          # no_compensation=True via decorator
            .add_step(send_confirmation)       # no_compensation=True via decorator
            .add_step(charge_payment_fails)    # fails
        )

        result = s.run(order_id="nc-1", amount=10)

        with pytest.raises(SagaCompensated):
            result.get(timeout=5)

        # Neither validate nor send_confirmation should be compensated
        # (they have no_compensation=True)
        # charge_payment_fails itself failed so nothing to compensate
        execution = backend.load(result.saga_id)
        compensated = [s for s in execution.steps if s.status == StepStatus.COMPENSATED]
        assert len(compensated) == 0

    def test_saga_result_steps_property(self):
        s = (
            Saga("steps_prop", backend=backend)
            .add_step(validate_order)
            .add_step(charge_payment)
        )
        result = s.run(order_id="sp-1", amount=10)
        result.get(timeout=5)

        steps = result.steps
        assert len(steps) == 2
        assert steps[0]["status"] == "success"
        assert steps[1]["status"] == "success"
        assert steps[0]["task_name"] == "tests.test_saga.validate_order"

    def test_multiple_compensations_reverse_order(self):
        """Compensation should run in reverse order of step completion."""
        s = (
            Saga("reverse_saga", backend=backend)
            .add_step(charge_payment)
            .add_step(reserve_inventory)
            .add_step(create_shipping)
            .add_step(charge_payment_fails)  # fails
        )

        result = s.run(order_id="rev-1", amount=100)

        with pytest.raises(SagaCompensated):
            result.get(timeout=5)

        # Find compensation side effects and verify reverse order
        comp_effects = [e for e in side_effects if e.startswith(("refund:", "release:", "undo_ship:"))]
        assert len(comp_effects) == 3
        # Shipping was last to succeed, should compensate first
        assert comp_effects[0].startswith("undo_ship:")
        assert comp_effects[1].startswith("release:")
        assert comp_effects[2].startswith("refund:")
