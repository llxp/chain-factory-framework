import redis as redis_impl
from redis.client import PubSub, Redis
from redis.exceptions import ConnectionError, TimeoutError, LockError
from typing import Dict, Any
import logging
from ..decorators.repeat import repeat

LOGGER = logging.getLogger(__name__)

connection_pools: Dict[str, redis_impl.Redis] = {}


class RedisClient():
    """
    Wrapper class around the redis python library
    """
    def __init__(
        self,
        host: str,
        password: str = None,
        port: int = 6379,
        db: int = 0
    ):
        self._connection: Redis = self._get_connection(
            host,
            password,
            port,
            db
        )
        self._pubsub_connection: PubSub = self._get_pubsub_connection()

    def _get_connection(
        self,
        host: str,
        password: str = None,
        port: int = 6379,
        db: int = 0
    ):
        """
        Create a new connection, if not already found in the connection pool
        """
        global connection_pools
        if (
            host not in connection_pools
            or (host in connection_pools and connection_pools[host] is None)
        ):
            connection_pools[host] = self._connect(host, password, port, db)
        return connection_pools[host]

    def _get_pubsub_connection(self):
        return self._connection.pubsub()

    @repeat((ConnectionError, TimeoutError), None, 10)
    def _connect(
        self,
        host: str,
        password: str = None,
        port: int = 6379,
        db: int = 0
    ) -> redis_impl.Redis:
        connection: redis_impl.Redis = redis_impl.Redis(
            host=host,
            password=password,
            port=port,
            db=db
        )
        return connection

    @repeat((ConnectionError, TimeoutError, LockError), False, 10)
    def set(self, name: str, obj, lock_key: str = '_lock'):
        # with self._connection.lock(name + lock_key, blocking_timeout=20):
        self._connection.set(name, obj)
        return True
        # return False

    @repeat((ConnectionError, TimeoutError), None, 10)
    def get(self, name: str):
        return self._connection.get(name)

    @repeat((ConnectionError, TimeoutError), None, 10)
    def lpush(self, name: str, obj):
        return self._connection.lpush(name, obj)

    @repeat((ConnectionError, TimeoutError), None, 10)
    def rpush(self, name: str, obj):
        return self._connection.rpush(name, obj)

    @repeat((ConnectionError, TimeoutError), None, 10)
    def lpop(self, name: str):
        return self._connection.lpop(name)

    @repeat((ConnectionError, TimeoutError), None, 10)
    def lrem(self, name: str, obj):
        return self._connection.lrem(name, 1, obj)

    @repeat((ConnectionError, TimeoutError), None, 10)
    def lindex_rem(self, name: str, index: int):
        self.lset(name, index, 'DELETED')
        return self.lrem(name, 'DELETED')

    @repeat((ConnectionError, TimeoutError), None, 10)
    def lindex_obj(self, name: str, index: int, json_model: Any):
        redis_bytes = self.lindex(name, index)
        if redis_bytes is not None:
            redis_decoded = redis_bytes.decode('utf-8')
            return json_model.from_json(redis_decoded)
        return json_model.from_json('{}')

    @repeat((ConnectionError, TimeoutError), None, 10)
    def lindex(self, name: str, index: int):
        return self._connection.lindex(name, index)

    @repeat((ConnectionError, TimeoutError), None, 10)
    def llen(self, name: str):
        return self._connection.llen(name)

    @repeat((ConnectionError, TimeoutError), None, 10)
    def lset(self, name: str, index: int, obj):
        return self._connection.lset(name, index, obj)

    def subscribe(self, channel: str):
        return self._pubsub_connection.subscribe(channel)

    def listen(self):
        return self._pubsub_connection.listen()

    def get_message(self):
        return self._pubsub_connection.get_message(
            ignore_subscribe_messages=True)

    def publish(self, channel: str, obj):
        return self._connection.publish(channel, obj)
