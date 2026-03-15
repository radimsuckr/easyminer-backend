import logging

import redis

from easyminer.config import celery_backend

logger = logging.getLogger(__name__)


def _get_redis_client() -> redis.Redis:
    return redis.from_url(celery_backend, decode_responses=True)


class PartialResultTracker:
    PARTIAL_RESULT_SHOWN_PREFIX: str = "partial_result_shown:"

    def __init__(self, redis_client: redis.Redis):
        self.redis: redis.Redis = redis_client

    def _get_key(self, task_id: str) -> str:
        return f"{self.PARTIAL_RESULT_SHOWN_PREFIX}{task_id}"

    def try_mark_partial_result_as_shown(self, task_id: str, ttl: int = 86400) -> bool:
        key = self._get_key(task_id)

        if result := self.redis.set(key, "1", nx=True, ex=ttl):
            logger.debug(f"Successfully marked partial result as shown for task {task_id} (first time)")
        else:
            logger.debug(f"Partial result already shown for task {task_id}")

        return bool(result)

    def has_partial_result_been_shown(self, task_id: str) -> bool:
        key = self._get_key(task_id)
        result = self.redis.exists(key)
        return bool(result)

    def clear_partial_result_flag(self, task_id: str) -> None:
        key = self._get_key(task_id)
        _ = self.redis.delete(key)
        logger.debug(f"Cleared partial result flag for task {task_id}")


def get_partial_result_tracker() -> PartialResultTracker:
    redis_client = _get_redis_client()
    return PartialResultTracker(redis_client)
