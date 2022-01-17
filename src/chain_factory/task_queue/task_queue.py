import logging
import time
from _thread import interrupt_main
from typing import Dict

from .task_handler import TaskHandler
from .task_runner import ControlThread
from .wrapper.interruptable_thread import ThreadAbortException
from .wait_handler import WaitHandler
from .blocked_handler import BlockedHandler
from .cluster_heartbeat import ClusterHeartbeat
from .wrapper.redis_client import RedisClient
from .credentials_pool import CredentialsPool
from .task_starter import TaskStarter
from .task_waiter import TaskWaiter
# import the settings
from .common.settings import \
    worker_count as default_worker_count, \
    redis_host as default_redis_host, \
    redis_password as default_redis_password, \
    redis_port as default_redis_port, \
    redis_db as default_redis_db, \
    mongodb_connection as default_mongodb_connection, \
    amqp_username as default_amqp_username, \
    amqp_password as default_amqp_password, \
    amqp_host as default_amqp_host, \
    task_queue as default_task_queue, \
    wait_queue as default_wait_queue, \
    incoming_blocked_queue as default_incoming_blocked_queue, \
    wait_blocked_queue as default_wait_blocked_queue, \
    incoming_block_list_redis_key as default_incoming_block_list_redis_key, \
    wait_block_list_redis_key as default_wait_block_list_redis_key, \
    task_timeout as default_task_timeout, \
    task_repeat_on_timeout as default_task_repeat_on_timeout, \
    namespace as default_namespace
from .common.generate_random_id import generate_random_id
from .wrapper.mongodb_client import MongoDBClient
from .node_registration import NodeRegistration


class TaskQueue():
    """
    - Initialises the framework
    - Connects to redis, mongodb and the rabbitmq queue broker
    """

    def __init__(self, endpoint: str, username: str, password: str):
        """
        Initialises the properties using the default values from the settings
        """
        self.node_name = generate_random_id()
        self.worker_count = default_worker_count
        self.task_handler = TaskHandler()
        # redis keys
        self.incoming_block_list_redis_key = \
            default_incoming_block_list_redis_key
        self.wait_block_list_redis_key = \
            default_wait_block_list_redis_key
        # task timeout
        self.task_timeout = default_task_timeout
        self.task_repeat_on_timeout = default_task_repeat_on_timeout
        # namespace
        self.namespace = default_namespace
        # redis credentials
        self.redis_host = default_redis_host
        self.redis_port = default_redis_port
        self.redis_db = default_redis_db
        self.redis_password = default_redis_password
        # mongodb credentials
        self.mongodb_connection: str = default_mongodb_connection
        # rabbitmq credentials
        self.amqp_username: str = default_amqp_username
        self.amqp_password: str = default_amqp_password
        self.amqp_host = default_amqp_host
        # queue names
        self.task_queue = default_task_queue
        self.wait_queue = default_wait_queue
        self.incoming_blocked_queue = default_incoming_blocked_queue
        self.wait_blocked_queue = default_wait_blocked_queue
        # task starter
        self._task_starter: Dict[TaskStarter] = {}
        # credentials pool
        self._credentials_pool = CredentialsPool(
            endpoint, username, password, [self.namespace]
        )

    def start_new_task(self, namespace: str, task_name: str, arguments: dict):
        credentials = self._credentials_pool.get_credentials(
            namespace).rabbitmq()
        try:
            self._task_starter[namespace].start_new_task(
                task_name, arguments
            )
        except KeyError:
            self._task_starter[namespace] = TaskStarter(
                namespace,
                self.amqp_host,
                credentials['username'],
                credentials['password'],
            )
            self._task_starter[namespace].start_new_task(
                task_name, arguments
            )

    def _mongodb_client_by_namespace(self, credentials, namespace):
        connection_string = \
            'mongodb://{}:{}@{}/{}?authSource=admin'.format(
                credentials['username'],
                credentials['password'],
                self.amqp_host,  # use the same host as the rabbitmq server
                credentials['database']
            )
        mongodb_client = MongoDBClient(connection_string)
        return mongodb_client

    def wait_for_task(self, namespace: str, task_name: str, arguments: dict):
        """
        - waits for the task to complete
        """
        credentials = self._credentials_pool.get_credentials(
            namespace).mongodb()
        try:
            self._task_waiter[namespace].wait_for_task_name(
                task_name, arguments
            )
        except KeyError:
            self._task_waiter[namespace] = TaskWaiter(
                self._mongodb_client_by_namespace(credentials, namespace)
            )
            self._task_waiter[namespace].wait_for_task_name(
                task_name, arguments
            )

    def init(self):
        """
        - initialises the redis and mongodb clients
        - initialises the queue handlers
        """
        self._init_clients()
        self._init_mongodb_collections()
        self._init_handlers()
        self._init_registration()

    def _init_registration(self):
        self._node_registration = NodeRegistration(
            self.namespace,
            self.mongodb_client,
            self.node_name,
            self.task_handler
        )

    def stop_listening(self):
        print('shutting down node')
        self.task_handler.stop_listening()
        self.wait_thread.stop_listening()

    def _init_redis(self):
        """
        returns a new redis client object
        """
        return RedisClient(
            self.redis_host,
            self.redis_password,
            self.redis_port,
            self.redis_db
        )

    def _init_mongodb(self):
        """
        returns a new mongodb client object
        """
        return MongoDBClient(self.mongodb_connection)

    def _init_clients(self):
        """
        - initialises the redis client
        - initialises the mongodb client
        """
        self.redis_client = self._init_redis()
        self.mongodb_client = self._init_mongodb()

    def stop_node(self):
        self.stop_listening()
        running_workflows_counter = 0
        task_runner_count = len(self.task_handler.registered_tasks)
        while running_workflows_counter < task_runner_count:
            running_workflows_counter = 0
            for registered_task in self.task_handler.registered_tasks:
                task_runner = \
                    self.task_handler.registered_tasks[registered_task]
                if len(task_runner.running_workflows()) <= 0:
                    running_workflows_counter = running_workflows_counter + 1
            time.sleep(0.1)
        if running_workflows_counter >= task_runner_count:
            print('node is dry')
            self.stop_heartbeat()
            self.mongodb_client.client.close()
            self.redis_client._connection.close()
            self.redis_client._pubsub_connection.close()
            self.task_handler.amqp.close()
            self.wait_thread.amqp.close()
            logging.shutdown()
            self.node_control_thread.stop()
            self.node_control_thread.abort()

    def stop_heartbeat(self):
        self.cluster_heartbeat.stop_heartbeat()

    def _listen_control_messages(self):
        try:
            redis_client = self._init_redis()
            self.node_control_thread = ControlThread(
                self.node_name,
                {
                    'stop': interrupt_main
                },
                redis_client,
                self.namespaced('node_control_channel')
            )
            self.node_control_thread.start()
        except ThreadAbortException:
            print('detected abort')
            self.node_control_thread.abort()

    def _init_handlers(self):
        """
        Init all handlers
        -> wait handler
        -> incoming blocked handler
        -> wait blocked handler
        -> task handler
        -> cluster heartbeat
        """
        self._get_credentials()
        self._init_wait_handler()
        self._init_incoming_blocked_handler()
        self._init_wait_blocked_handler()
        self._init_task_handler()
        self.init_cluster_heartbeat()

    def task(
        self,
        name: str = '',
        repeat_on_timeout: bool = default_task_repeat_on_timeout
    ):
        """
        Decorator to simply register a new task
        """
        def wrapper(func):
            temp_name = name
            if len(temp_name) <= 0:
                # get the function name as string
                temp_name = func.__name__
            # register the function
            # using the function name
            self.task_handler.add_task(temp_name, func, repeat_on_timeout)
        return wrapper

    def add_task(self, func, name: str = ''):
        """
        Method to add tasks, which cannot be added using the decorator
        """
        outer_wrapper = self.task(name)
        outer_wrapper(func)

    def _init_mongodb_collections(self):
        self.mongodb_client.db().workflows.create_index('workflow_id')
        self.mongodb_client.db().tasks.create_index('workflow_id')
        self.mongodb_client.db().logs.create_index('task_id')

    def _init_wait_handler(self):
        """
        Start the wait handler queue listener
        """
        self.wait_thread = WaitHandler(
            node_name=self.node_name,
            amqp_host=self.amqp_host,
            amqp_username=self.amqp_username,
            amqp_password=self.amqp_password,
            redis_client=self.redis_client,
            queue_name=self.namespaced(self.task_queue),
            wait_queue_name=self.namespaced(self.wait_queue),
            blocked_queue_name=self.namespaced(self.wait_blocked_queue),
            namespace=self.namespace
        )

    def _init_incoming_blocked_handler(self):
        """
        Init the blocked queue for all blocked tasks,
        which are blocked before even getting to the actual processing
        --> If task is on Blacklist/Blocklist
        --> Node is set to not respond to any of those tasks
        --> Node is in standby mode for those tasks
        """
        self.incoming_blocked_handler = BlockedHandler(
            node_name=self.node_name,
            amqp_host=self.amqp_host,
            amqp_username=self.amqp_username,
            amqp_password=self.amqp_password,
            redis_client=self.redis_client,
            task_queue_name=self.namespaced(self.task_queue),
            blocked_queue_name=self.namespaced(self.incoming_blocked_queue),
            block_list_name=self.namespaced(
                self.incoming_block_list_redis_key),
            namespace=self.namespace
        )

    def _init_wait_blocked_handler(self):
        """
        Init the blocked queue listener for all waiting tasks (failed, etc.)
        """
        self.wait_blocked_handler = BlockedHandler(
            node_name=self.node_name,
            amqp_host=self.amqp_host,
            amqp_username=self.amqp_username,
            amqp_password=self.amqp_password,
            redis_client=self.redis_client,
            task_queue_name=self.namespaced(self.wait_queue),
            blocked_queue_name=self.namespaced(self.wait_blocked_queue),
            block_list_name=self.namespaced(self.wait_block_list_redis_key),
            namespace=self.namespace
        )

    def namespaced(self, var: str):
        if self.namespace:
            return self.namespace + '_' + var
        return var

    def _init_task_handler(self):
        """
        Init the actual task queue listener
        """
        print(self.node_name)
        self.task_handler.init(
            node_name=self.node_name,
            amqp_host=self.amqp_host,
            amqp_username=self.amqp_username,
            amqp_password=self.amqp_password,
            redis_client=self.redis_client,
            queue_name=self.namespaced(self.task_queue),
            wait_queue_name=self.namespaced(self.wait_queue),
            blocked_queue_name=self.namespaced(self.incoming_blocked_queue),
            mongodb_client=self.mongodb_client,
            namespace=self.namespace
        )
        self.task_handler.task_timeout = self.task_timeout
        self.task_handler.update_task_timeout()

    def init_cluster_heartbeat(self):
        """
        Init the ClusterHeartbeat
        """
        self.cluster_heartbeat: ClusterHeartbeat = ClusterHeartbeat(
            self.namespace,
            self.node_name,
            self.redis_client
        )

    def _listen_handlers(self):
        """
        Start all handlers to listen
        """
        self.wait_thread.listen()
        self.incoming_blocked_handler.listen()
        self.wait_blocked_handler.listen()
        self.task_handler.listen()

    def listen(self):
        """
        Initialises the queue and starts listening
        """
        self.init()
        self.task_handler.task_set_redis_client()
        self._listen_control_messages()
        self._node_registration.register()
        self.task_handler.scale(self.worker_count)
        self.cluster_heartbeat.start_heartbeat()
        print('listening')
        self._listen_handlers()

    def run_main_loop(self):
        run_sleep = True
        try:
            while run_sleep:
                time.sleep(0.01)  # keep mainthread running
        except KeyboardInterrupt:
            self.stop_node()
            print('node halted')
            # try:
            #     while run_sleep:
            #         time.sleep(0.01)  # keep mainthread running
            # except KeyboardInterrupt:
            #     exit(0)
