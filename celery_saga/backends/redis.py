from __future__ import annotations

import json
from typing import TYPE_CHECKING

from celery_saga.backends.base import SagaBackend
from celery_saga.state import SagaExecution

if TYPE_CHECKING:
    from redis import Redis


class RedisSagaBackend(SagaBackend):
    """Redis-backed saga state persistence."""

    PREFIX = "celery_saga:"

    def __init__(self, redis_client: Redis | None = None, url: str | None = None, ttl: int = 86400):
        self.ttl = ttl
        if redis_client:
            self._client = redis_client
        elif url:
            import redis
            self._client = redis.Redis.from_url(url)
        else:
            raise ValueError("Provide either redis_client or url")

    def _key(self, saga_id: str) -> str:
        return f"{self.PREFIX}{saga_id}"

    def _idem_key(self, key: str) -> str:
        return f"{self.PREFIX}idem:{key}"

    def save(self, execution: SagaExecution) -> None:
        pipe = self._client.pipeline()
        data = json.dumps(execution.to_dict())
        pipe.set(self._key(execution.saga_id), data, ex=self.ttl)
        if execution.idempotency_key:
            pipe.set(self._idem_key(execution.idempotency_key), execution.saga_id, ex=self.ttl)
        pipe.execute()

    def load(self, saga_id: str) -> SagaExecution | None:
        data = self._client.get(self._key(saga_id))
        if data is None:
            return None
        return SagaExecution.from_dict(json.loads(data))

    def delete(self, saga_id: str) -> None:
        execution = self.load(saga_id)
        if execution:
            pipe = self._client.pipeline()
            pipe.delete(self._key(saga_id))
            if execution.idempotency_key:
                pipe.delete(self._idem_key(execution.idempotency_key))
            pipe.execute()

    def find_by_idempotency_key(self, key: str) -> SagaExecution | None:
        saga_id = self._client.get(self._idem_key(key))
        if saga_id is None:
            return None
        if isinstance(saga_id, bytes):
            saga_id = saga_id.decode()
        return self.load(saga_id)

    def list_all(self) -> list[SagaExecution]:
        results = []
        idem_prefix = f"{self.PREFIX}idem:"
        cursor = 0
        while True:
            cursor, keys = self._client.scan(cursor, match=f"{self.PREFIX}*", count=100)
            for key in keys:
                key_str = key.decode() if isinstance(key, bytes) else key
                if key_str.startswith(idem_prefix):
                    continue
                data = self._client.get(key)
                if data:
                    results.append(SagaExecution.from_dict(json.loads(data)))
            if cursor == 0:
                break
        results.sort(key=lambda e: e.created_at, reverse=True)
        return results
