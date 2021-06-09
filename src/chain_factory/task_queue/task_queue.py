from typing import List, Dict, Callable
import inspect
import json

from .task_handler import TaskHandler
from .task_runner import TaskRunner, ControlThread
from .wrapper.interruptable_thread import ThreadAbortException
from .wait_handler import WaitHandler
from .blocked_handler import BlockedHandler
from .cluster_heartbeat import ClusterHeartbeat
from .wrapper.redis_client import RedisClient
# import the settings
from .common.settings import \
    unique_hostnames, \
    force_register, \
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
    wait_block_list_redis_key as default_wait_block_list_redis_key
from .common.generate_random_id import generate_random_id
from .models.mongo.registered_task import RegisteredTask
from .models.mongo.node_tasks import NodeTasks
from .models.mongo.task import Task
from .wrapper.mongodb_client import MongoDBClient


class TaskQueue():
    """
    - Initialises the framework
    - Connects to redis, mongodb and the rabbitmq queue broker
    """
    def __init__(self):
        """
        Initialises the properties using the default values from the settings
        """
        self.node_name = generate_random_id()
        self.worker_count = default_worker_count
        self.redis_host = default_redis_host
        self.redis_port = default_redis_port
        self.redis_db = default_redis_db
        self.redis_password = default_redis_password
        self.mongodb_connection: str = default_mongodb_connection
        self.amqp_username: str = default_amqp_username
        self.amqp_password: str = default_amqp_password
        self.amqp_host = default_amqp_host
        self.task_handler = TaskHandler()
        self.task_queue = default_task_queue
        self.wait_queue = default_wait_queue
        self.incoming_blocked_queue = default_incoming_blocked_queue
        self.wait_blocked_queue = default_wait_blocked_queue
        self.incoming_block_list_redis_key = \
            default_incoming_block_list_redis_key
        self.wait_block_list_redis_key = \
            default_wait_block_list_redis_key

    def init(self):
        """
        - initialises the redis and mongodb clients
        - initialises the queue handlers
        """
        self._init_clients()
        self._init_handlers()

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

    def _listen_control_messages(self):
        try:
            task_control_thread = ControlThread(
                self.node_name,
                self.stop_listening,
                self.stop_listening,
                self.redis_client,
                'node_control_channel'
            )
            task_control_thread.start()
            # task_control_thread.abort()
        except ThreadAbortException:
            task_control_thread.abort()
        except KeyboardInterrupt:
            task_control_thread.abort()

    def _init_handlers(self):
        """
        Init all handlers
        -> wait handler
        -> incoming blocked handler
        -> wait blocked handler
        -> task handler
        -> cluster heartbeat
        """
        self._init_wait_handler()
        self._init_incoming_blocked_handler()
        self._init_wait_blocked_handler()
        self._init_task_handler()
        self.init_cluster_heartbeat()

    def task(self, name: str = ''):
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
            self.task_handler.add_task(temp_name, func)
        return wrapper

    def add_task(self, func, name: str = ''):
        """
        Method to add tasks, which cannot be added using the decorator
        """
        outer_wrapper = self.task(name)
        outer_wrapper(func)

    def _task_arguments(self, function_signature):
        """
        Get arguments from task function signature
        """
        return [
            obj for obj in list(function_signature.parameters)
            if obj != 'self'
        ]

    def _task_argument_types(self, function_signature):
        """
        Get argument types from task function signature
        """
        return [
            function_signature.parameters[d].annotation.__name__
            if hasattr(function_signature.parameters[d].annotation, '__name__')
            else str(function_signature.parameters[d].annotation)
            for d in function_signature.parameters
        ]

    def _registered_task(self, task_name: str, callback: Callable[..., Task]):
        """
        Inspect given callback and return a RegisteredTask object
        needed for _node_tasks to assemble the full list of registered tasks
        """
        function_signature = inspect.signature(callback)
        argument_names = self._task_arguments(function_signature)
        argument_types = self._task_argument_types(function_signature)
        return RegisteredTask(
            name=task_name,
            arguments=dict(zip(argument_names, argument_types))
        )

    def _node_tasks(self):
        """
        Get all registered tasks on this node
        """
        task_runners: Dict[str, TaskRunner] = \
            self.task_handler.registered_tasks
        all_registered_tasks: List[RegisteredTask] = []
        for task_name in task_runners:
            registered_task = self._registered_task(
                task_name, task_runners[task_name].callback)
            all_registered_tasks.append(registered_task)
        return NodeTasks(
            node_name=self.node_name,
            tasks=all_registered_tasks
        )

    def _register_tasks(self):
        """
        Registers all internally registered tasks in the database
        in the form:
            node_name/task_name
        """
        node_tasks = self._node_tasks()
        already_registered_node = \
            self.mongodb_client.db().registered_tasks.find_one(
                {
                    'node_name': self.node_name
                }
            )
        if already_registered_node is not None:
            if unique_hostnames:  # setting
                raise Exception(
                    'Existing node name found in redis list, exiting.\n'
                    'If this is intentional, set unique_hostnames to False'
                )
            if force_register:
                self.mongodb_client.db().registered_tasks.delete_many(
                    {
                        'node_name': self.node_name
                    }
                )
        self.mongodb_client.db().registered_tasks.insert_one(
            dict(json.loads(node_tasks.to_json()))
        )

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
            queue_name=self.task_queue,
            wait_queue_name=self.wait_queue,
            blocked_queue_name=self.wait_blocked_queue
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
            task_queue_name=self.task_queue,
            blocked_queue_name=self.incoming_blocked_queue,
            block_list_name=self.incoming_block_list_redis_key
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
            task_queue_name=self.wait_queue,
            blocked_queue_name=self.wait_blocked_queue,
            block_list_name=self.wait_block_list_redis_key
        )

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
            queue_name=self.task_queue,
            wait_queue_name=self.wait_queue,
            blocked_queue_name=self.incoming_blocked_queue,
            mongodb_client=self.mongodb_client
        )

    def init_cluster_heartbeat(self):
        """
        Init the ClusterHeartbeat
        """
        self.cluster_heartbeat: ClusterHeartbeat = ClusterHeartbeat(
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
        self._register_tasks()
        self.task_handler.scale(self.worker_count)
        self.cluster_heartbeat.start_heartbeat()
        print('listening')
        self._listen_handlers()
