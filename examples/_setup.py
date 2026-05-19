"""Shared Celery + saga setup used by all examples.

Runs in eager mode so the examples work without a broker. In production you
would point Celery at a real broker (Redis/RabbitMQ) and use RedisSagaBackend.
"""

from celery import Celery

from celery_saga import set_default_backend
from celery_saga.backends.memory import MemorySagaBackend

app = Celery("examples")
app.config_from_object({
    "task_always_eager": True,
    "task_eager_propagates": True,
    "result_backend": "cache+memory://",
})

backend = MemorySagaBackend()
set_default_backend(backend)
