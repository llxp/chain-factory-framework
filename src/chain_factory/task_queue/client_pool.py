from typing import Dict

from .models.mongodb_models import Task, Workflow, WorkflowLog
from .wrapper.mongodb_client import MongoDBClient
from .wrapper.redis_client import RedisClient
from .wrapper.rabbitmq import RabbitMQ


class ClientPool():
    def __init__(
        self,
    ):
        self.mongodb_client: MongoDBClient = None
        self.redis_clients: Dict[str, RedisClient] = {}
        self.rabbitmq_clients: Dict[str, RabbitMQ] = {}

    async def init(
        self,
        redis_url: str,
        mongodb_url: str
    ):
        """
        - initialises the redis client
        - initialises the mongodb client
        """
        self.redis_clients['default'] = await self._init_redis(redis_url)
        self.mongodb_client: MongoDBClient = await self._init_mongodb(
            mongodb_url)

    async def redis_client(self, redis_url: str = 'default') -> RedisClient:
        """
        return a redis client specific to the given redis url
        if no redis url is given, return the default redis client
        if no default redis client exists,
            create a new one with the given redis url
        """
        if redis_url not in self.redis_clients:
            self.redis_clients[redis_url] = await self._init_redis(redis_url)
        return self.redis_clients[redis_url]

    async def _init_mongodb(self, mongodb_url: str) -> MongoDBClient:
        """
        returns a new mongodb client object
        """
        client = MongoDBClient(mongodb_url)
        await client.check_connection()
        await self._init_mongodb_collections(client)
        return client

    async def _init_redis(self, redis_url: str) -> RedisClient:
        """
        returns a new redis client object
        """
        return RedisClient(redis_url=redis_url)

    async def _init_rabbitmq(
        self,
        rabbitmq_url: str,
        rmq_type: str,
        queue_name: str
    ) -> RabbitMQ:
        """
        returns a new rabbitmq client object
        """
        client = RabbitMQ(
            url=rabbitmq_url, rmq_type=rmq_type, queue_name=queue_name)
        await client.init()
        return client

    async def rabbitmq_client(
        self,
        rabbitmq_url: str,
        rmq_type: str,
        queue_name: str
    ) -> RabbitMQ:
        """
        return a rabbitmq client specific to the given rabbitmq url
        if no rabbitmq url is given, return the default rabbitmq client
        if no default rabbitmq client exists,
            create a new one with the given rabbitmq url
        """
        if rabbitmq_url not in self.rabbitmq_clients:
            self.rabbitmq_clients[rabbitmq_url] = await self._init_rabbitmq(
                rabbitmq_url, rmq_type, queue_name)
        return self.rabbitmq_clients[rabbitmq_url]

    async def _init_mongodb_collections(self, client: MongoDBClient):
        """
        initialises the mongodb collections
        """
        motor_client = client.client
        await motor_client.get_collection(Workflow).create_index('workflow_id')
        await motor_client.get_collection(Task).create_index('workflow_id')
        await motor_client.get_collection(WorkflowLog).create_index('task_id')

    async def close(self):
        """
        closes all the redis clients
        """
        for redis_client in self.redis_clients.values():
            await redis_client.close()
        await self.mongodb_client.close()
        for rabbitmq_client in self.rabbitmq_clients.values():
            rabbitmq_client.stop_callback()
            await rabbitmq_client.close()
