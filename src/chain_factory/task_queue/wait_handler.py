from asyncio import sleep
from datetime import datetime, timedelta
from logging import info, debug, warning
from amqpstorm import Message

from .wrapper.rabbitmq import RabbitMQ
from .wrapper.redis_client import RedisClient
from .models.mongodb_models import Task
from .list_handler import ListHandler
from .queue_handler import QueueHandler
from .common.settings import \
    max_task_age_wait_queue, wait_time, \
    wait_block_list_redis_key


class WaitHandler(QueueHandler):
    def __init__(self):
        QueueHandler.__init__(self)
        self.redis_client = None

    async def init(
        self,
        rabbitmq_url: str,
        node_name: str,
        queue_name: str,
        wait_queue_name: str,
        blocked_queue_name: str,
        redis_client: RedisClient
    ):
        await QueueHandler.init(
            self,
            url=rabbitmq_url,
            queue_name=wait_queue_name
        )
        self.node_name = node_name
        self.wait_queue_name = wait_queue_name
        self.rabbitmq_task_queue: RabbitMQ = RabbitMQ(
            url=rabbitmq_url,
            queue_name=queue_name,
            rmq_type='publisher'
        )
        self.amqp_blocked: RabbitMQ = RabbitMQ(
            url=rabbitmq_url,
            queue_name=blocked_queue_name,
            rmq_type='publisher'
        )
        await self.amqp_blocked.init()
        self.block_list = ListHandler(
            list_name=wait_block_list_redis_key,
            redis_client=redis_client
        )
        await self.block_list.init()

    async def _check_blocklist(self, task: Task, message: Message):
        task_name = task.name
        blocklist = await self.block_list.get()
        if (
            blocklist is None
            or (blocklist is not None and blocklist.list_items is None)
        ):
            warning(
                'blocklist \'%s\' couldn\'t be retrieved from redis. '
                'Reschedulung task \'%s\' to queue \'%s\''
                % task_name, self.queue_name
            )
            await self._reject(message)
            return True
        for blocklist_item in blocklist.list_items:
            if (
                blocklist_item.content == task_name and
                blocklist_item.name == self.node_name
            ):
                # reschedule task, which is in incoming block list
                info('task %s is on block list...' % task_name)
                await self.send_to_queue(task, self.amqp_blocked)
                await sleep(wait_time)
                return True
        return False

    async def _send_to_task_queue(self, task: Task, message: Message):
        await self.ack(message)
        await self.send_to_queue(task, self.rabbitmq_task_queue)
        debug("sent back to task queue")

    async def _reject(self, message: Message):
        debug('waiting...')
        await sleep(wait_time)
        await self.reschedule(message)

    def _seconds_diff(self):
        time_now = datetime.utcnow()
        time_difference = timedelta(seconds=max_task_age_wait_queue)
        return time_now - time_difference

    async def on_task(self, task: Task, message: Message) -> Task:
        if task is not None and len(task.name):
            if await self._check_blocklist(task, message):
                return None
            max_task_age = self._seconds_diff()
            current_task_age = task.received_date
            if current_task_age < max_task_age:
                # task is older then max_task_age
                # reschedule the task to the task_queue
                info(
                    'reschedulung task to queue: %s' % self.queue_name
                )
                await self._send_to_task_queue(task, message)
            else:
                await self._reject(message)
        else:
            await self._reject(message)
        return None
