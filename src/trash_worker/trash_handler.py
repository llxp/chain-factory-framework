from ..task_queue.queue_handler import QueueHandler
from amqpstorm import Message
from typing import Union

from ..task_queue.models.task import Task
from ..task_queue.wrapper.redis_client import RedisClient


class TrashHandler(QueueHandler):
    def __init__(
        self,
        amqp_host: str,
        queue_name: str,
        amqp_username: str,
        amqp_password: str,
        redis_host: str,
        redis_password: str,
        redis_port: int,
        redis_db: int,
        redis_key: str = 'trash_queue_output'
    ):
        super().__init__(
            self,
            amqp_host,
            amqp_username,
            amqp_password
        )
        self.redis_client = RedisClient(
            redis_host,
            queue_name + '_trash_handler',
            redis_password,
            redis_port,
            redis_db
        )
        self.redis_key = redis_key

    def on_task(self, task: Task, message: Message) -> Union[None, Task]:
        print(task.to_json())
        self.redis_client.rpush(self.redis_key, task.to_json())
