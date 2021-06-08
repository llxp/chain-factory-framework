import logging
import traceback
import sys
from typing import Callable, List, Dict, Any, Union
from ssl import SSLContext
from _thread import start_new_thread, interrupt_main
from dataclasses import dataclass

from amqpstorm import Connection, Channel, Message as AMQPStormMessage

from ..common.settings import prefetch_count
from amqpstorm.exception import \
    AMQPChannelError, AMQPConnectionError, \
    AMQPInvalidArgument

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=False)
class Message():
    body: str
    channel: Channel
    delivery_tag: int


@dataclass(frozen=False)
class SSLOptions():
    context: SSLContext
    server_hostname: str


class AMQP():

    def __init__(
        self,
        host: str,
        queue_name: str,
        port: int = 5672,
        username: str = 'guest',
        password: str = 'guest',
        amqp_type: str = 'publisher',
        callback: Callable[[Message], str] = None,
        ssl: bool = False,
        ssl_options: SSLOptions = None,
        queue_options: Dict[str, Any] = None
    ):
        self.callback: Callable[[Message], str] = callback
        self.queue_name: str = queue_name
        self.amqp_type = amqp_type

        self.connection: Connection = AMQP._connect(
            host=host,
            username=username,
            password=password,
            port=port,
            ssl=ssl,
            ssl_options=ssl_options)
        self.consumer_list: List(_Consumer) = []
        self.sender_channel: _Consumer = _Consumer(
            self.connection, self.queue_name, queue_options)
        self.scale()
        self.acked = []
        self.nacked = []

    def __enter__(self):
        """
        Needed for the 'with' clause to work
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Needed for the 'with clause to work',
        closes the connection, when the with clause's scope gets out of scope
        """
        self.close()

    def close(self):
        """
        close all consumers and close the connection
        """
        for consumer in self.consumer_list:
            consumer.close()
        self.connection.close()

    @staticmethod
    def _connect(
        host: str,
        username: str,
        password: str,
        port: int = 5672,
        ssl: bool = False,
        ssl_options: SSLOptions = None
    ) -> Connection:
        """
        Connects to an amqp server
        """
        LOGGER.debug('opening new BlockingConnection to host %s' % host)
        if ssl:
            connection = Connection(
                hostname=host,
                username=username,
                password=password,
                port=port,
                ssl=ssl,
                ssl_options=ssl_options
            )
        else:
            connection = Connection(
                hostname=host,
                username=username,
                password=password,
                port=port,
                timeout=5,
                heartbeat=5
            )
        LOGGER.debug('opened new BlockingConnection to host %s' % host)
        return connection

    @staticmethod
    def _queue_exists(self, channel: Channel, queue_name: str):
        """
        Check, if the declared queue has been declared
        """
        try:
            channel.queue.declare(queue=queue_name, durable=True, passive=True)
            return True
        except (AMQPInvalidArgument, AMQPChannelError, AMQPConnectionError):
            return False

    def message_count(self) -> int:
        """
        Retrieves the current count of messages, waiting in the queue
        """
        return self.sender_channel.message_count()

    def callback_impl(self, message: AMQPStormMessage):
        """
        callback function, which will be called
        everytime a new message is consumed by the pika/amqp library
        """
        LOGGER.debug('body: %s' % message.body)
        if len(message.body) <= 0:
            message.ack()  # ignore empty messages
            return
        new_message: Message = Message(
            message.body,
            message.channel,
            message.delivery_tag
        )
        if self.callback is not None:
            LOGGER.debug('invoking registered callback method')
            # start_new_thread(self._start_callback_thread, (new_message, ))
            self._start_callback_thread(new_message)

    def _start_callback_thread(self, new_message: Message):
        # execute the registered callback
        # print(new_message)
        result: str = self.callback(new_message)
        if result is not None and len(result) > 0:
            # another task has been returned to be scheduled
            self.send(result)
            LOGGER.debug(
                'sent task to same queue, because result is %s' % result)
        else:
            # self.nack(new_message)
            LOGGER.debug('invoked registered callback method, result is None')

    def ack(self, message: Message):
        """
        Acknowledges the specified message
        """
        message.channel.basic.ack(delivery_tag=message.delivery_tag)

    def nack(self, message: Message):
        """
        Nacks/Rejects the specified message
        """
        traceback.print_stack()
        message.channel.basic.nack(
            delivery_tag=message.delivery_tag, requeue=True)

    def reject(self, message: Message):
        """
        Rejects the specified message
        """
        message.channel.basic.reject(
            delivery_tag=message.delivery_tag, requeue=True)

    def listen(self):
        """
        Starts the amqp consumer/listener
        """
        i = 0
        for consumer in self.consumer_list:
            start_new_thread(self._start_consuming, (consumer, ))
            LOGGER.info(
                'starting new consumer %d for queue %s' % (i, self.queue_name))
            i = i + 1

    @staticmethod
    def _start_consuming(consumer: '_Consumer'):
        try:
            consumer.channel.start_consuming()
        except Exception:
            print('start_consuming exception')
            traceback.print_exc(file=sys.stdout)
            interrupt_main()

    def scale(self, worker_count: int = 1):
        """
        Set the worker count
        """
        # used for consuming messages from the queue
        if self.amqp_type == 'consumer':
            self._scale_consumer(worker_count)
        # used for publishing messages on the queue
        elif self.amqp_type == 'publisher':
            pass

    def _scale_consumer(self, worker_count: int):
        if worker_count > len(self.consumer_list):
            self._scale_add_consumer(worker_count)
        elif worker_count == len(self.consumer_list):
            # requested worker count is the same
            # as the current worker count
            return
        else:
            self._scale_remove_consumer(worker_count)
        for consumer in self.consumer_list:
            consumer.consume(self.callback_impl)

    def _consumer_count(self):
        return len(self.consumer_list)

    def _scale_add_consumer(self, worker_count: int):
        # how many need to be added
        add_worker_count = worker_count - self._consumer_count()
        for i in range(0, add_worker_count):
            self._add_consumer(_Consumer(
                self.connection, self.queue_name))

    def _scale_remove_consumer(self, worker_count: int):
        # how many need to be removed
        remove_worker_count = self._consumer_count() - worker_count
        for i in range(0, remove_worker_count):
            self._remove_consumer(i)

    def _add_consumer(self, consumer: '_Consumer'):
        self.consumer_list.append(consumer)

    def _remove_consumer(self, index: int):
        existing_consumer: _Consumer = self.consumer_list.pop(index)
        existing_consumer.close()

    def send(self, message: str) -> Union[bool, None]:
        """
        Publishes a new task on the queue
        """
        new_message = self._create_new_message(message)
        return new_message.publish(self.queue_name)

    def _create_new_message(self, message: str):
        properties = {
            'content_type': 'text/plain',
            'headers': {},
            'delivery_mode': 2
        }
        return AMQPStormMessage.create(
            self.sender_channel.channel, message, properties)

    def delete_queue(self):
        """
        Deletes the queue
        """
        self.sender_channel.delete_queue()

    def clear_queue(self):
        """
        Clears the queue
        """
        self.sender_channel.clear_queue()


class _Consumer():
    def __init__(
        self,
        connection: Connection,
        queue_name: str,
        queue_options: Dict[str, Any] = None
    ):
        self.queue_name = queue_name
        self.channel: Channel = self._open_channel(connection)
        self._declare_queue(queue_options)

    @staticmethod
    def _open_channel(connection: Connection) -> Channel:
        """
        returns the current opened channel of the amqp connection
        """
        LOGGER.debug('opening new BlockingChannel on opened connection')
        channel = connection.channel()
        LOGGER.debug('opened new BlockingChannel on opened connection')
        return channel

    def _declare_queue(self, queue_options: Dict[str, Any]):
        """
        Declare the specified queue
        """
        LOGGER.debug('declaring queue %s' % self.queue_name)
        self.channel.queue.declare(
            queue=self.queue_name, durable=True, arguments=queue_options)
        LOGGER.debug('declared queue %s' % self.queue_name)

    def consume(self, callback: Callable[[Message], str]):
        """
        Specify, that this instance should be used to consume messages
        """
        self.channel.basic.qos(prefetch_count=prefetch_count)
        self.channel.basic.consume(
            queue=self.queue_name, callback=callback)
        # print(' [*] Waiting for messages. To exit press CTRL+C')
        LOGGER.info(' [*] Waiting for messages. To exit press CTRL+C')

    def close(self):
        """
        stop consuming on the channel and close the channel
        """
        self.channel.stop_consuming()
        self.channel.close()

    def message_count(self) -> int:
        """
        Retrieves the current count of messages, waiting in the queue
        """
        res = self.channel.queue.declare(
            queue=self.queue_name,
            durable=True,
            exclusive=False,
            auto_delete=False,
            passive=True
        )
        return res['message_count']

    def delete_queue(self):
        """
        Deletes the queue
        """
        self.channel.queue.delete(self.queue_name)

    def clear_queue(self):
        """
        Clears the queue
        """
        self.delete_queue()
        self._declare_queue()
