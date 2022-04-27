from asyncio import (
    AbstractEventLoop, get_event_loop, new_event_loop
)
# from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from logging import debug, error, info
from traceback import print_exc
from sys import stdout
from typing import Callable, Dict, Any, Union, List
from ssl import SSLContext
from _thread import interrupt_main
from _thread import start_new_thread
from aio_pika import (
    Queue, connect_robust,
    IncomingMessage, Message as AioPikaMessage
)
from aio_pika.connection import ConnectionType
from aio_pika.channel import Channel
from aio_pika.robust_queue import RobustQueue
from aio_pika.exceptions import AMQPConnectionError, AMQPChannelError
from asyncio import ensure_future

from ..common.settings import prefetch_count


@dataclass
class Message():
    body: str
    message: IncomingMessage
    delivery_tag: int


@dataclass
class SSLOptions():
    context: SSLContext
    server_hostname: str


class RabbitMQ:
    def __init__(
        self,
        url: str,
        queue_name: str,
        rmq_type: str = "publisher",
        callback: Callable[[Message], str] = None,
        queue_options: Dict[str, Any] = None,
        loop: AbstractEventLoop = None,
    ):
        self.callback: Callable[[Message], str] = callback
        self.queue_name: str = queue_name
        self.rmq_type = rmq_type
        self.url = url
        self.queue_options = queue_options
        self.loop = loop

    async def init(self):
        self.connection: ConnectionType = await RabbitMQ._connect(
            self.url, loop=self.loop)
        self.consumer_list: List(_Consumer) = []
        # await self.scale()
        if self.rmq_type == "consumer":
            self._consumer = _Consumer(self.connection, self.queue_name)
            await self._consumer.init()
        self.sender_channel: _Consumer = _Consumer(
            self.connection, self.queue_name, self.queue_options)
        await self.sender_channel.init()
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

    def stop_callback(self):
        self.callback = None

    async def close(self):
        """
        close all consumers and close the connection
        """
        self.callback = None
        await self._consumer.close()
        # consumer: _Consumer = None
        # for consumer in self.consumer_list:
        #     await consumer.close()
        await self.connection.close()

    @staticmethod
    async def _connect(
        url: str,
        loop: AbstractEventLoop = None,
    ) -> ConnectionType:
        """
        Connects to an rabbitmq server
        """
        debug("opening new RabbitMQ to host %s" % url)
        if not loop:
            loop = get_event_loop()
        connection = await connect_robust(url, timeout=5, loop=loop)
        debug("opened new RabbitMQ to host %s" % url)
        return connection

    @staticmethod
    async def _queue_exists(self, queue: RobustQueue, queue_name: str):
        """
        Check, if the declared queue has been declared
        """
        try:
            await queue.declare(queue=queue_name, durable=True, passive=True)
            return True
        except (AMQPChannelError, AMQPConnectionError):
            return False

    async def message_count(self) -> int:
        """
        Retrieves the current count of messages, waiting in the queue
        """
        return await self.sender_channel.message_count()

    async def callback_impl(self, message: IncomingMessage):
        """
        callback function, which will be called
        everytime a new message is consumed by the pika/rabbitmq library
        """
        async with message.process(ignore_processed=True):
            debug("body: %s" % message.body)
            if len(message.body) <= 0:
                await message.ack()  # ignore empty messages
                return
            new_message: Message = Message(
                message.body, message, message.delivery_tag
            )
            if self.callback is not None:
                debug("invoking registered callback method")
                await self._start_callback(new_message)

    def _start_callback_thread(self, new_message: Message):
        def callback_thread(new_message: Message):
            try:
                loop = new_event_loop()
                loop.run_until_complete(self._start_callback(new_message))
            except Exception:
                print_exc()
                error("error in callback thread")
                interrupt_main()
        start_new_thread(callback_thread, (new_message, ))

    async def _start_callback(self, new_message: Message):
        # execute the registered callback
        if self.callback:
            result: str = await self.callback(new_message)
            if result is not None and len(result) > 0:
                # another task has been returned to be scheduled
                send_future = self.send(result)
                ensure_future(send_future, loop=self.loop)
                debug(
                    "sent task to same queue, because result is %s" % result)
            else:
                # self.nack(new_message)
                debug(
                    "invoked registered callback method, result is None")

    async def ack(self, message: Message):
        """
        Acknowledges the specified message
        """
        await message.message.ack()

    async def nack(self, message: Message):
        """
        Nacks/Rejects the specified message
        """
        # traceback.print_stack()
        await message.message.nack(
            delivery_tag=message.delivery_tag, requeue=True)

    async def reject(self, message: Message):
        """
        Rejects the specified message
        """
        message.message.reject(
            delivery_tag=message.delivery_tag, requeue=True)

    async def listen(self):
        """
        Starts the rabbitmq consumer/listener
        """
        info(f"starting new consumer for queue {self.queue_name}")
        await self._start_consuming(self._consumer)
        # i = 0
        # executor = ProcessPoolExecutor(len(self.consumer_list))
        # for consumer in self.consumer_list:
        #     # start_new_thread(self._start_consuming, (consumer, ))
        #     future = self._start_consuming
        #     self.loop.run_in_executor(executor, future)
        #     info("starting new consumer %d for queue %s" % (
        #         i, self.queue_name))
        #     i += 1

    async def _start_consuming(self, consumer: '_Consumer' = None):
        try:
            await consumer.consume(self.callback_impl)
        except Exception:
            error("start_consuming exception")
            print_exc(file=stdout)
            interrupt_main()

    async def scale(self, worker_count: int = 1):
        """
        Set the worker count
        """
        # used for consuming messages from the queue
        # if self.rmq_type == "consumer":
        #     await self._scale_consumer(worker_count)
        # # used for publishing messages on the queue
        # elif self.rmq_type == "publisher":
        #     pass
        pass

    # async def _scale_consumer(self, worker_count: int):
    #     if worker_count > len(self.consumer_list):
    #         await self._scale_add_consumer(worker_count)
    #     elif worker_count == len(self.consumer_list):
    #         # requested worker count is the same
    #         # as the current worker count
    #         return
    #     else:
    #         await self._scale_remove_consumer(worker_count)
    #     consumer: _Consumer = None
    #     for consumer in self.consumer_list:
    #         await consumer.consume(self.callback_impl)

    # def _consumer_count(self):
    #     return len(self.consumer_list)

    # async def _scale_add_consumer(self, worker_count: int):
    #     # how many need to be added
    #     add_worker_count = worker_count - self._consumer_count()
    #     debug("adding %d consumers" % add_worker_count)
    #     for _ in range(0, add_worker_count):
    #         new_consumer = _Consumer(self.connection, self.queue_name)
    #         await new_consumer.init()
    #         await self._add_consumer(new_consumer)
    #     debug("added %d consumers" % len(self.consumer_list))

    # async def _scale_remove_consumer(self, worker_count: int):
    #     # how many need to be removed
    #     remove_worker_count = self._consumer_count() - worker_count
    #     for _ in range(0, remove_worker_count):
    #         self._remove_last_consumer()

    # async def _add_consumer(self, consumer: "_Consumer"):
    #     self.consumer_list.append(consumer)

    # def _remove_last_consumer(self):
    #     existing_consumer: _Consumer = self.consumer_list.pop(-1)
    #     existing_consumer.close()

    async def send(self, message: str) -> Union[bool, None]:
        """
        Publishes a new task on the queue
        """
        try:
            new_message = self._create_new_message(message)
            return await self.sender_channel.channel.default_exchange.publish(
                new_message, routing_key=self.queue_name)
        except AMQPConnectionError:
            print_exc(file=stdout)
            interrupt_main()

    def _create_new_message(self, message: str):
        return AioPikaMessage(
            body=message.encode('utf-8'),
            content_type="text/plain",
            delivery_mode=2,
            headers={}
        )

    async def delete_queue(self):
        """
        Deletes the queue
        """
        await self.sender_channel.delete_queue()

    async def clear_queue(self):
        """
        Clears the queue
        """
        await self.sender_channel.clear_queue()


class _Consumer:
    def __init__(
        self,
        connection: ConnectionType,
        queue_name: str,
        queue_options: Dict[str, Any] = None,
    ):
        self.queue_name = queue_name
        self.connection = connection
        self.queue_options = queue_options
        self.channel: Channel = None
        self.queue: Queue = None

    async def init(self):
        self.channel: Channel = await self._open_channel(self.connection)
        self.queue = await self._declare_queue(self.queue_options)

    @staticmethod
    async def _open_channel(connection: ConnectionType) -> Channel:
        """
        returns the current opened channel of the amqp connection
        """
        debug("opening new Channel on opened connection")
        channel = await connection.channel()
        debug("opened new Channel on opened connection")
        return channel

    async def _declare_queue(self, queue_options: Dict[str, Any]):
        """
        Declare the specified queue
        """
        debug("declaring queue %s" % self.queue_name)
        queue = await self.channel.declare_queue(
            name=self.queue_name, durable=True, arguments=queue_options
        )
        debug("declared queue %s" % self.queue_name)
        return queue

    async def consume(self, callback: Callable[[IncomingMessage], str]):
        """
        Specify, that this instance should be used to consume messages
        """
        await self.channel.set_qos(prefetch_count=prefetch_count)
        await self.queue.consume(callback=callback)
        info(
            f"[{self.queue_name}] " +
            "[*] Waiting for messages. To exit press CTRL+C"
        )

    async def close(self):
        """
        stop consuming on the channel and close the channel
        """
        try:
            await self.channel.close()
        except (KeyError, AMQPConnectionError):
            pass

    async def message_count(self) -> int:
        """
        Retrieves the current count of messages, waiting in the queue
        """
        res: Queue = await self.channel.declare_queue(
            queue=self.queue_name,
            durable=True,
            exclusive=False,
            auto_delete=False,
            passive=True,
        )
        return res["message_count"]

    async def delete_queue(self):
        """
        Deletes the queue
        """
        await self.channel.queue_delete(queue=self.queue_name)

    async def clear_queue(self):
        """
        Clears the queue
        """
        await self.delete_queue()
        self.queue = await self._declare_queue()
