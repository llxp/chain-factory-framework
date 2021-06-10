import time
import pytz
from datetime import datetime, timedelta
import logging
from amqpstorm import Message

from .wrapper.amqp import AMQP
from .wrapper.redis_client import RedisClient
from .models.mongo.task import Task
from .list_handler import ListHandler
from .queue_handler import QueueHandler
from .common.settings import \
    max_task_age_wait_queue, wait_time, \
    wait_block_list_redis_key

LOGGER = logging.getLogger(__name__)


class WaitHandler(QueueHandler):
    def __init__(
        self,
        node_name: str,
        amqp_host: str,
        queue_name: str,
        wait_queue_name: str,
        blocked_queue_name: str,
        amqp_username: str,
        amqp_password: str,
        redis_client: RedisClient,
        namespace: str
    ):
        QueueHandler.__init__(self)
        QueueHandler.init(
            self,
            amqp_host,
            wait_queue_name,
            amqp_username,
            amqp_password,
            namespace
        )
        self.node_name = node_name
        self.wait_queue_name = wait_queue_name
        self.amqp_task: AMQP = AMQP(
            host=amqp_host,
            queue_name=queue_name,
            username=amqp_username,
            password=amqp_password,
            amqp_type='publisher',
            port=5672,
            ssl=False,
            ssl_options=None,
            virtual_host=namespace
        )
        self.amqp_blocked: AMQP = AMQP(
            host=amqp_host,
            queue_name=blocked_queue_name,
            username=amqp_username, password=amqp_password,
            amqp_type='publisher',
            port=5672,
            ssl=False,
            ssl_options=None,
            virtual_host=namespace
        )
        self.block_list = ListHandler(
            list_name=wait_block_list_redis_key,
            redis_client=redis_client
        )

    def _check_blocklist(self, task: Task, message: Message):
        task_name = task.name
        blocklist = self.block_list.get()
        if (
            blocklist is None
            or (blocklist is not None and blocklist.list_items is None)
        ):
            LOGGER.warning(
                'blocklist \'%s\' couldn\'t be retrieved from redis. '
                'Reschedulung task \'%s\' to queue \'%s\''
                % task_name, self.queue_name
            )
            self._reject(message)
            return True
        for blocklist_item in blocklist.list_items:
            if (
                blocklist_item.content == task_name and
                blocklist_item.name == self.node_name
            ):
                # reschedule task, which is in incoming block list
                LOGGER.info('task %s is on block list...' % task_name)
                self.send_to_queue(task, self.amqp_blocked)
                time.sleep(wait_time)
                return True
        return False

    def _send_to_task_queue(self, task: Task, message: Message):
        self.ack(message)
        self.send_to_queue(task, self.amqp_task)
        LOGGER.debug("sent back to task queue")

    def _reject(self, message: Message):
        LOGGER.debug('waiting...')
        time.sleep(wait_time)
        self.reschedule(message)

    def _seconds_diff(self, seconds):
        time_now = datetime.now(tz=pytz.UTC)
        time_difference = timedelta(seconds=max_task_age_wait_queue)
        return time_now - time_difference

    def on_task(self, task: Task, message: Message) -> Task:
        if task is not None and len(task.name):
            if self._check_blocklist(task, message):
                return None
            max_task_age = self._seconds_diff(max_task_age_wait_queue)
            current_task_age = task.received_date
            if current_task_age < max_task_age:
                # task is older then max_task_age
                # reschedule the task to the task_queue
                LOGGER.info(
                    'reschedulung task to queue: %s' % self.queue_name
                )
                self._send_to_task_queue(task, message)
            else:
                self._reject(message)
        else:
            self._reject(message)
        return None
