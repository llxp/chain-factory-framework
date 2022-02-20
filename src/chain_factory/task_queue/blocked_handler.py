from time import sleep
from logging import info, warning, debug

from .wrapper.rabbitmq import RabbitMQ, Message
from .wrapper.redis_client import RedisClient
from .models.mongodb_models import Task
from .list_handler import ListHandler
from .queue_handler import QueueHandler
from .common.settings import wait_time


class BlockedHandler(QueueHandler):
    """
    Checks, if the task is still on the blocklist and sends them back
    -> to the blocked queue if still on the blocklist
    -> or to the task queue if not on the blocklit anymore
    """

    def __init__(self):
        QueueHandler.__init__(self)

    async def init(
        self,
        rabbitmq_url: str,
        node_name: str,
        task_queue_name: str,
        blocked_queue_name: str,
        block_list_name: str,
        redis_client: RedisClient
    ):
        await QueueHandler.init(
            self,
            url=rabbitmq_url,
            queue_name=blocked_queue_name
        )
        self.node_name = node_name
        self.rabbitmq_sender_task_queue: RabbitMQ = RabbitMQ(
            url=rabbitmq_url,
            queue_name=task_queue_name,
            rmq_type='publisher'
        )
        await self.rabbitmq_sender_task_queue.init()
        self.block_list = ListHandler(
            list_name=block_list_name,
            redis_client=redis_client
        )
        await self.block_list.init()

    async def _check_blocklist(self, task: Task, message: Message) -> bool:
        """
        Checks, if the task is still on the blocklist and sends them back
        -> to the blocked queue if still on the blocklist
        -> or to the task queue if not on the blocklit anymore
        """
        task_name = task.name
        blocklist = await self.block_list.get()
        if (
            blocklist is None
            or (blocklist is not None and blocklist.list_items is None)
        ):
            warning(
                'blocklist \'%s\' couldn\'t be retrieved from redis. '
                'Reschedulung task \'%s\' to queue \'%s\''
                % self.block_list.list_name, task_name, self.queue_name
            )
            await self.reschedule(message)
            sleep(wait_time)
            return True
        for blocklist_item in blocklist.list_items:
            if (
                blocklist_item.content == task_name and
                blocklist_item.name == self.node_name
            ):
                debug(
                    'BlockedHandler:_check_blocklist: '
                    'task %s is not in blocklist' % task_name
                )
                return False
        # reschedule task, which is in incoming block list
        info('waiting: task %s is on block list...' % task_name)
        await self._send_to_task_queue(task, message)
        sleep(wait_time)
        return True

    async def _send_to_blocked_queue(self, task: Task, message: Message):
        await self.ack(message)
        await self.send_to_queue(task, self.rabbitmq)
        debug("sent back to blocked queue")

    async def _send_to_task_queue(self, task: Task, message: Message):
        await self.ack(message)
        await self.send_to_queue(task, self.rabbitmq_sender_task_queue)
        debug("sent back to task queue")

    async def on_task(self, task: Task, message: Message) -> Task:
        debug('BlockedHandler:on_task: queue_name: ' + self.queue_name)
        if task is not None and len(task.name):
            if await self._check_blocklist(task, message):
                return None
        else:
            debug('task is empty')
            return None
        debug('waiting...')
        await self._send_to_blocked_queue(task, message)
        sleep(wait_time)
        return None
