# Examples

Runnable examples for `celery-saga`. They use Celery's eager mode and the
in-memory backend so no broker or Redis is needed.

## Running

```bash
pip install -e ..
export CELERY_SAGA_SIGNING_KEY=examples-secret  # required for lambdas/transforms
python 01_builder_api.py
```

Or all of them:

```bash
for f in 0*.py; do echo "=== $f ==="; python "$f"; done
```

## What each one shows

| File | Demonstrates |
| --- | --- |
| `01_builder_api.py` | Classic builder API; happy path + compensation on failure |
| `02_functional_api.py` | `@saga` decorator with auto-registration and `transform()` |
| `03_parallel.py` | `add_parallel()` plus reverse-order rollback across parallel siblings |
| `04_child_saga.py` | Atomic child saga: child as a single step; user compensate only runs when child succeeded then parent failed later |
| `05_permanent_failure.py` | `PermanentFailure` carrying partial-state data into the compensation task |
