from aioredis.client import PubSub, Redis
from aioredis.exceptions import ConnectionError, TimeoutError
from threading import Lock
from typing import Dict, Any
from ..decorators.repeat import repeat, repeat_async

connection_pools: Dict[str, Redis] = {}


class RedisClient():
    """
    Wrapper class around the redis python library
    """

    def __init__(
        self,
        redis_url: str,
    ):
        self._connection: Redis = self._get_connection(
            redis_url=redis_url,
        )
        self._pubsub_connection: PubSub = self._get_pubsub_connection()
        self.mutex = Lock()

    def _get_connection(
        self,
        redis_url: str,
    ):
        """
        Create a new connection, if not already found in the connection pool
        """
        global connection_pools
        if (redis_url not in connection_pools):
            connection_pools[redis_url] = self._connect(redis_url)
        return connection_pools[redis_url]

    def _get_pubsub_connection(self):
        return self._connection.pubsub()

    @repeat((ConnectionError, TimeoutError), None, 10)
    def _connect(
        self,
        redis_url: str,
    ) -> Redis:
        return Redis.from_url(redis_url)

    async def close(self):
        await self._connection.close()
        await self._pubsub_connection.close()

    @repeat_async((ConnectionError, TimeoutError), False, 10)
    async def set(self, name: str, obj):
        return await self._connection.set(name, obj)

    @repeat_async((ConnectionError, TimeoutError), None, 10)
    async def get(self, name: str):
        return await self._connection.get(name)

    @repeat_async((ConnectionError, TimeoutError), None, 10)
    async def lpush(self, name: str, obj):
        return await self._connection.lpush(name, obj)

    @repeat_async((ConnectionError, TimeoutError), None, 10)
    async def rpush(self, name: str, obj):
        return await self._connection.rpush(name, obj)

    @repeat_async((ConnectionError, TimeoutError), None, 10)
    async def lpop(self, name: str):
        return await self._connection.lpop(name)

    @repeat_async((ConnectionError, TimeoutError), None, 10)
    async def lrem(self, name: str, obj):
        return await self._connection.lrem(name, 1, obj)

    @repeat_async((ConnectionError, TimeoutError), None, 10)
    async def lindex_rem(self, name: str, index: int):
        await self.lset(name, index, 'DELETED')
        return await self.lrem(name, 'DELETED')

    @repeat_async((ConnectionError, TimeoutError), None, 10)
    async def lindex_obj(self, name: str, index: int, json_model: Any):
        redis_bytes = await self.lindex(name, index)
        if redis_bytes is not None:
            redis_decoded = redis_bytes.decode('utf-8')
            return json_model.from_json(redis_decoded)
        return json_model.from_json('{}')

    @repeat_async((ConnectionError, TimeoutError), None, 10)
    async def lindex(self, name: str, index: int) -> bytes:
        return await self._connection.lindex(name, index)

    @repeat_async((ConnectionError, TimeoutError), None, 10)
    async def llen(self, name: str):
        return await self._connection.llen(name)

    @repeat_async((ConnectionError, TimeoutError), None, 10)
    async def lset(self, name: str, index: int, obj):
        return await self._connection.lset(name, index, obj)

    async def subscribe(self, channel: str):
        return await self._pubsub_connection.subscribe(channel)

    async def listen(self):
        return await self._pubsub_connection.listen()

    async def get_message(self):
        try:
            self.mutex.acquire()
            return await self._pubsub_connection.get_message(
                ignore_subscribe_messages=True)
        except ConnectionError:
            return None
        finally:
            self.mutex.release()

    async def publish(self, channel: str, obj):
        return self._connection.publish(channel, obj)
