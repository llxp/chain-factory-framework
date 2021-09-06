import abc
from datetime import datetime
import logging
import pytz
import traceback
import sys
from typing import Union

from amqpstorm.exception import AMQPConnectionError

from .wrapper.amqp import AMQP, Message
from .models.mongo.task import Task
from .decorators.parse_catcher import parse_catcher

LOGGER = logging.getLogger(__name__)


class QueueHandler:
    def __init__(self):
        pass

    def init(
        self,
        amqp_host: str,
        queue_name: str,
        amqp_username: str,
        amqp_password: str,
        virtual_host: str = None,
    ):
        """
        Separate init logic to be able to use lazy initialisation
        """
        self.queue_name = queue_name
        self._connect(
            amqp_host=amqp_host,
            amqp_username=amqp_username,
            amqp_password=amqp_password,
            virtual_host=virtual_host,
        )

    def stop_listening(self):
        self.amqp.stop_callback()

    def _connect(
        self,
        amqp_host: str,
        amqp_username: str,
        amqp_password: str,
        virtual_host: str = None,
    ):
        """
        Connects to amqp
        """
        try:
            self.amqp: AMQP = AMQP(
                host=amqp_host,
                queue_name=self.queue_name,
                port=5672,
                username=amqp_username,
                password=amqp_password,
                amqp_type="consumer",
                callback=self._on_message,
                ssl=False,
                ssl_options=None,
                virtual_host=virtual_host,
            )
        except AMQPConnectionError:
            traceback.print_exc(file=sys.stdout)
            sys.exit(1)

    def listen(self):
        """
        starts listening on the queue
        """
        self.amqp.listen()

    def reschedule(self, message: Message):
        """
        Reschedules or rather rejects the message
        """
        self.nack(message=message)

    @staticmethod
    def _now():
        """
        returns the current time with timezone
        """
        return datetime.now(pytz.UTC)

    @staticmethod
    def send_to_queue(task: Task, amqp_queue: AMQP):
        """
        Send a task to the specified queue
        """
        task.received_date = QueueHandler._now()
        return amqp_queue.send(message=task.to_json())

    def ack(self, message: Message):
        """
        Acknowledges the specified message
        """
        self.amqp.ack(message=message)

    def nack(self, message: Message):
        """
        Rejects the specified message
        """
        self.amqp.nack(message=message)

    @abc.abstractmethod
    def on_task(self, task: Task, message: Message) -> Union[None, Task]:
        """
        abstract method for the overriding clas,
        will be invoked, when a new task comes in
        """
        LOGGER.error(
            "Error: on_task on queue_handler has been called. "
            "Please implement the on_task method "
            "in the derived class of queue_handler"
        )
        raise NotImplementedError(
            "Error: on_task on queue_handler has been called. "
            "Please implement the on_task method in the derived "
            "class of queue_handler"
        )

    def _on_message(self, message: Message) -> str:
        """
        method will be invoked by the amqp library, when a new message comes in
        """
        LOGGER.debug("callback_impl in queue_handler called")
        # parse the message body to Task
        task: Task = self._parse_json(body=message.body)
        LOGGER.debug(
            "task: %s" % task.to_json() if task is not None else "None")
        return self._on_message_check_task(task, message)

    def _on_message_check_task(
        self,
        task: Union[Task, None],
        message: Message
    ):
        if task is not None and len(task.name) > 0:
            return self._on_task(task=task, message=message)
        else:
            return self._on_task_error(message=message)

    @staticmethod
    @parse_catcher((AttributeError, TypeError, Exception))
    def _parse_json(body: str) -> Union[None, Task]:
        if len(body) > 0:
            return Task.from_json(body)
        else:
            return None

    def _on_task(self, task: Task, message: Message) -> str:
        """
        method will be invoked by _on_message, when a new task comes in
        checks the return value
        and returns them after logging to the amqp library
        """
        LOGGER.debug("on_task will be called")
        result: Task = self.on_task(task, message)
        return self._on_task_check_task_result(result)

    def _on_task_check_task_result(self, result: Union[Task, None]):
        if result is None:
            return self._on_none_task_result()
        else:
            return self._on_task_result(result)

    def _on_task_result(self, result: Task):
        LOGGER.debug("result: %s" % result.to_json())
        # return the result as json to the queue
        return result.to_json()

    def _on_none_task_result(self):
        LOGGER.debug("result: None")
        return ""

    def _on_task_error(self, message: Message) -> str:
        """
        will be invoked,
        when an error occured during parsing the message to a task
        """
        LOGGER.error("Error, message is not parsable. Body: %s" % message.body)
        return ""
