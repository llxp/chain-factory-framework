import time
import pytz
import logging
from amqpstorm import Message

from .wrapper.amqp import AMQP
from .wrapper.redis_client import RedisClient
from .models.mongo.task import Task
from .list_handler import ListHandler
from .queue_handler import QueueHandler
from .common.settings import wait_time

LOGGER = logging.getLogger(__name__)
utc = pytz.UTC


class BlockedHandler(QueueHandler):
    def __init__(
        self,
        node_name: str,
        amqp_host: str,
        task_queue_name: str,
        blocked_queue_name: str,
        redis_client: RedisClient,
        amqp_username: str = 'guest',
        amqp_password: str = 'guest',
    ):
        QueueHandler.__init__(
            self,
            amqp_host,
            blocked_queue_name,
            amqp_username,
            amqp_password
        )
        self.node_name = node_name
        self.amqp_task: AMQP = AMQP(
            host=amqp_host,
            queue_name=task_queue_name,
            username=amqp_username,
            password=amqp_password,
            amqp_type='publisher',
            port=5672,
            ssl=False,
            ssl_options=None
        )
        self.block_list = ListHandler(
            list_name='incoming_block_list',
            redis_client=redis_client
        )

    def _check_blocklist(self, task: Task, message: Message) -> bool:
        task_name = task.name
        blocklist = self.block_list.get()
        if (
            blocklist is None
            or (blocklist is not None and blocklist.list_items is None)
        ):
            LOGGER.warning(
                'blocklist \'%s\' couldn\'t be retrieved from redis. '
                'Reschedulung task \'%s\' to queue \'%s\''
                % self.block_list.list_name, task_name, self.queue_name
            )
            self.reschedule(message)
            time.sleep(wait_time)
            return True
        for blocklist_item in blocklist.list_items:
            if (
                blocklist_item.content == task_name and
                blocklist_item.name == self.node_name
            ):
                LOGGER.debug(
                    'BlockedHandler:_check_blocklist: '
                    'task %s is not in blocklist' % task_name
                )
                return False
        # reschedule task, which is in incoming block list
        LOGGER.info('waiting: task %s is on block list...' % task_name)
        self.send_to_queue(task, self.amqp_task)
        self.ack(message)
        time.sleep(wait_time)
        return True

    def _send_to_blocked_queue(self, task: Task, message: Message):
        self.ack(message)
        self.send_to_queue(task, self.amqp)
        LOGGER.debug("sent back to blocked queue")

    def on_task(self, task: Task, message: Message) -> Task:
        LOGGER.debug('BlockedHandler:on_task: queue_name: ' + self.queue_name)
        if task is not None and len(task.name):
            if self._check_blocklist(task, message):
                return None
        else:
            LOGGER.debug('task is empty')
            return None
        LOGGER.debug('waiting...')
        self._send_to_blocked_queue(task, message)
        time.sleep(wait_time)
        return None
