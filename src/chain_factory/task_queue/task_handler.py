from typing import Dict, Tuple, Union, Callable, List
from datetime import datetime
import time
import logging
import pytz
import json
from threading import Lock

from .task_runner import TaskRunner
from .list_handler import ListHandler
from .queue_handler import QueueHandler
# wrapper
from .wrapper.amqp import AMQP, Message
from .wrapper.redis_client import RedisClient
from .wrapper.bytes_io_wrapper import BytesIOWrapper
from .wrapper.mongodb_client import MongoDBClient
# settings
from .common.settings import sticky_tasks, reject_limit
from .common.settings import task_status_redis_key
from .common.settings import wait_time, workflow_status_redis_key
from .common.settings import incoming_block_list_redis_key
# common
from .common.generate_random_id import generate_random_id
# models
from .models.mongo.task import Task
from .models.mongo.workflow import Workflow
from .models.mongo.task_workflow_association import TaskWorkflowAssociation
from .models.redis.task_status import TaskStatus
from .models.redis.workflow_status import WorkflowStatus


LOGGER = logging.getLogger(__name__)


class TaskHandler(QueueHandler):
    def __init__(self):
        QueueHandler.__init__(self)
        self.ack_lock = Lock()
        self.registered_tasks: Dict[str, TaskRunner] = {}
        self.redis_client = None

    def init(
        self,
        node_name: str,
        amqp_host: str,
        amqp_username: str,
        amqp_password: str,
        redis_client: RedisClient,
        mongodb_client: MongoDBClient,
        queue_name: str,
        wait_queue_name: str,
        blocked_queue_name: str
    ):
        QueueHandler.init(
            self,
            amqp_host,
            queue_name,
            amqp_username,
            amqp_password
        )
        self.node_name: str = node_name
        self.redis_client: RedisClient = redis_client
        self.mongo_client: MongoDBClient = mongodb_client
        self.wait_queue_name: str = wait_queue_name
        self.blocked_queue_name: str = blocked_queue_name

        self._init_amqp_publishers(
            amqp_host=amqp_host,
            amqp_username=amqp_username,
            amqp_password=amqp_password
        )
        self.block_list = ListHandler(
            list_name=incoming_block_list_redis_key,
            redis_client=redis_client
        )

    def _init_amqp_publishers(
        self,
        amqp_host: str,
        amqp_username: str,
        amqp_password: str,
        ssl: bool = False,
        ssl_options=None
    ):
        self.amqp_wait: AMQP = AMQP(
            host=amqp_host,
            queue_name=self.wait_queue_name,
            username=amqp_username,
            password=amqp_password,
            amqp_type='publisher',
            port=5672,
            ssl=False,
            ssl_options=None
        )
        self.amqp_planned: AMQP = AMQP(
            host=amqp_host,
            queue_name='dlx.' + self.wait_queue_name,
            username=amqp_username,
            password=amqp_password,
            amqp_type='publisher',
            port=5672,
            ssl=False,
            ssl_options=None,
            queue_options={
                'x-dead-letter-exchange': 'dlx.' + self.queue_name,
                'x-dead-letter-routing-key': self.queue_name
            }
        )
        self.amqp_blocked: AMQP = AMQP(
            host=amqp_host,
            queue_name=self.blocked_queue_name,
            username=amqp_username,
            password=amqp_password,
            amqp_type='publisher',
            port=5672,
            ssl=False,
            ssl_options=None
        )

    def _check_blocklist(self, task: Task, message: Message) -> bool:
        """
        Check the redis blocklist for the node name and task
        """
        blocklist = self.block_list.get()
        if blocklist is None or blocklist.list_items is None:
            # the blocklist couldn't be retrieved from redis,
            # so rejecting every incoming task
            LOGGER.warning(
                'could not retrieve blocklist from redis, rejecting all tasks'
            )
            self.nack(message)
            time.sleep(wait_time)
            return True
        for blocklist_item in blocklist.list_items:
            if (
                blocklist_item.content == task.name and
                blocklist_item.name == self.node_name
            ):
                LOGGER.info(
                    'task \'%s\' is in block list, '
                    'dispatching to blocked_queue' % task.name
                )
                task.received_date = datetime.now(pytz.UTC)
                # reschedule task, which is in incoming block list
                self.send_to_queue(task, self.amqp_blocked)
                self.ack(message)
                return True
        return False

    @staticmethod
    def _dataclass_to_dict(dataclass):
        return dict(json.loads(dataclass.to_json()))

    def _save_workflow(self, workflow_id: str, tags: List[str]):
        """
        Report the workflow id to the mongodb database
        """
        workflow = Workflow(
            created_date=datetime.now(pytz.UTC),
            workflow_id=workflow_id,
            node_name=self.node_name,
            tags=tags
        )
        workflow_dict = TaskHandler._dataclass_to_dict(workflow)
        self.mongo_client.db().workflows.insert_one(workflow_dict)

    def _exclude_sensitive_arguments(
        self,
        arguments: list,
        arguments_copy: list
    ):
        if arguments and 'exclude' in arguments:
            excluded_arguments = arguments['exclude']
            print(json.dumps(excluded_arguments))
            for argument in arguments_copy:
                for excluded_argument in excluded_arguments:
                    if argument != 'exclude' and argument == excluded_argument:
                        del arguments[argument]

    def _save_task_workflow_association(self, workflow_id: str, task: Task):
        """
        Report the assignment from workflow_id to task_id to the redis database
        """
        arguments = task.arguments
        if arguments:
            arguments_copy = dict(arguments)
        else:
            arguments_copy = dict()
        self._exclude_sensitive_arguments(arguments, arguments_copy)
        task.arguments = arguments
        task_to_workflow = TaskWorkflowAssociation(
            workflow_id=workflow_id,
            task=task,
            node_name=self.node_name
        )
        task_to_workflow_dict = \
            TaskHandler._dataclass_to_dict(task_to_workflow)
        self.mongo_client.db().tasks.insert_one(task_to_workflow_dict)
        if arguments_copy:
            task.arguments = arguments_copy

    def _run_task(
        self,
        task: Task,
        task_id: str
    ) -> Union[
        None,
        str, Task, bool,
        Tuple[
            Union[str, Task, bool],
            Dict[str, str]
        ]
    ]:
        """
        Runs the specified task and returns the result of the task function
        """
        # buffer to redirect stdout/stderr to the redis database
        log_buffer = BytesIOWrapper(task_id, self.mongo_client.db())
        # run the task
        return self.registered_tasks[task.name].run(
            task.arguments,
            task.workflow_id,
            log_buffer
        )

    TaskReturnType = Union[
        None, str, Task, bool, Callable[..., 'TaskReturnType']
    ]

    def _new_task_from_result(
        self,
        task_result: TaskReturnType,
        new_arguments: Dict[str, str]
    ) -> Task:
        # check, if result is a string ==> task name
        if isinstance(task_result, str):
            return Task(task_result, new_arguments)
        # check, if result is a registered callable
        if callable(task_result):
            return Task(task_result.__name__, new_arguments)
        return task_result

    def _return_new_task(
        self,
        old_task: Task,
        new_arguments: Dict[str, str],
        task_result: Union[str, Task, Callable[..., Dict[str, str]]]
    ) -> Task:
        """
        add the old task to the current task as the parent and schedule
        the to be scheduled task to the message queue
        """
        # rename task_result to new_task for better readability
        new_task = self._new_task_from_result(task_result, new_arguments)
        new_task.parent_task_id = old_task.task_id
        new_task.workflow_id = old_task.workflow_id
        new_task.node_names = old_task.node_names
        if sticky_tasks:  # settings.sticky_tasks
            # if sticky_tasks option is set,
            # only execute the full workflow on the node it started on
            new_task.node_names = [self.node_name]
        return new_task

    def _return_error_task(
        self,
        message: Message,
        task: Task,
        new_arguments: Dict[str, str]
    ) -> None:
        # update received date
        task.received_date = datetime.now(pytz.UTC)
        # accociate the current task with the next task
        task.parent_task_id = task.task_id
        # remove current task id on error
        # generate a new one on next run
        del task.task_id
        task.arguments = new_arguments
        # send task to wait queue
        self.amqp_wait.send(task.to_json())
        # acknowledge the message to delete it from the task queue
        self.ack(message)
        return None

    @staticmethod
    def _generate_random_id() -> str:
        return generate_random_id()

    @staticmethod
    def _generate_workflow_id(task: Task) -> Task:
        task.workflow_id = TaskHandler._generate_random_id()
        return task

    @staticmethod
    def _generate_task_id(task: Task) -> Task:
        task.task_id = TaskHandler._generate_random_id()
        return task

    @staticmethod
    def _workflow_precheck(task: Task):
        return len(task.parent_task_id) <= 0 and len(task.workflow_id) <= 0

    def _save_first_task_as_workflow(self, task: Task):
        if len(task.parent_task_id) <= 0:
            # report workflow to database,
            # if it is the first task in the workflow task chain
            self._save_workflow(task.workflow_id, task.tags)

    def _save_task_result(self, task_id: str, result: str):
        self.redis_client.rpush(
            task_status_redis_key,
            TaskStatus(status=result, task_id=task_id).to_json())

    def _mark_workflow_as_stopped(self, workflow_id: str):
        self.redis_client.rpush(
            workflow_status_redis_key,
            WorkflowStatus(status='None', workflow_id=workflow_id).to_json())

    def _handle_task_result(
        self,
        task_result: Union[bool, None, Task],
        arguments: Dict[str, str],
        message: Message,
        task: Task
    ) -> Union[Task, None]:
        if task_result is not False:  # task_result now can only be Task/None
            # acknowledge the message, as the task was successful
            self.ack(message)
            if task_result is None:
                self._save_task_result(task.task_id, 'None')
                # None means, the workflow chain stops
                self._mark_workflow_as_stopped(task.workflow_id)
                return None  # do nothing
            else:
                self._save_task_result(task.task_id, 'Task')
                return self._return_new_task(task, arguments, task_result)
        else:  # task_result is now False
            self._save_task_result(task.task_id, 'False')
            # the result is False, indicating an error
            # => schedule the task to the waiting queue
            return self._return_error_task(message, task, arguments)

    def _prepare_task_in_database(self, task: Task):
        self._save_first_task_as_workflow(task)
        # associate task id with workflow id in database
        self._save_task_workflow_association(task.workflow_id, task)

    def _prepare_task(self, task: Task):
        """
        Generates a task id and saves the association to it in the database
        """
        task = self._generate_task_id(task)
        self._prepare_task_in_database(task)
        return task

    def _handle_run_task(
        self,
        task: Task,
        message: Message
    ) -> Union[Task, None]:
        """
        will be executed, when the task is valid,
        which means it has a valid workflow id
        - generates a unique task id and
        - saves the association to the workflow in the database
        - runs the task
        - returns a function to handle the task result
        """
        task = self._prepare_task(task)
        task_result, arguments = self._run_task(task, task.task_id)
        # handle task result and return new Task or None
        return self._handle_task_result(task_result, arguments, message, task)

    def _is_planned_task(self, task: Task):
        return task.planned_date

    def _handle_planned_task(self, task: Task, message: Message):
        """
        sends the task to the delayed queue with an ttl of planned_date - now
        """
        pass

    def _handle_task(self, task: Task, message: Message) -> Union[None, Task]:
        """
        Runs a precheck, to see if the task already has a valid workflow id
        if not, it will generate a unique workflow id
        and return it to the queue
        """
        if self._workflow_precheck(task):
            # workflow id does not exist
            self.ack(message)
            return TaskHandler._generate_workflow_id(task)
        if self._is_planned_task(task):
            return self._handle_planned_task(task, message)
        return self._handle_run_task(task, message)

    @staticmethod
    def _handle_rejected_increase_counter(task: Task) -> Task:
        task.reject_counter = task.reject_counter + 1
        return task

    def _send_to_queue(self, queue: AMQP, message: Message, task: Task):
        queue.send(task.to_json())
        self.ack(message)

    def _handle_rejected(self, requested_task: Task, message: Message):
        """
        Increases the reject counter and requeues the task to the message queue
        """
        # task rejected, increase reject counter
        requested_task = self._handle_rejected_increase_counter(requested_task)
        if requested_task.reject_counter > reject_limit:
            requested_task.reject_counter = 0
            self._send_to_queue(self.amqp_wait, requested_task, message)
        else:
            self._send_to_queue(self.amqp, requested_task, message)
        return None

    def _check_node_filter(self, task: Task):
        return (
            len(task.node_names) > 0 and
            self.node_name not in task.node_names
        )

    def on_task(
        self,
        task: Task,
        message: Message
    ) -> Union[None, Task]:
        """
        The callback function, which will be called from the amqp library
        It deserializes the amqp message and then
        checks, if the requested taks is registered and calls it.
        If the task name is on the blocklist,
        it will be rejected, rescheduled and then wait for some time

        Returns either None or a new task
        """
        task_rejected = self._check_node_filter(task)
        if task_rejected:
            logging.debug(
                'task_rejected, because current node '
                'is not in the node_names list: ' + task.to_json()
            )
            return self._handle_rejected(task, message)

        task_blocked = self._check_blocklist(task, message)
        if not task_blocked:
            # task is not on the block list
            # reset reject_counter when task has been accepted
            task.reject_counter = 0
            # iterate through list of all registered tasks
            for registered_task in self.registered_tasks:
                if registered_task == task.name:
                    return self._handle_task(task, message)
        # task is either
        # => on the block list or
        # => not registered on the current node
        # return None to indicate no next task should be scheduled
        return None

    def add_task(self, name, callback):
        """
        Register a new task/task function
        """
        task = TaskRunner(
            name,
            callback
        )
        task.set_redis_client(self.redis_client)
        self.registered_tasks[name] = task

    def task_set_redis_client(self):
        for task_name in self.registered_tasks:
            task: TaskRunner = self.registered_tasks[task_name]
            task.set_redis_client(self.redis_client)

    def scale(self, worker_count: int = 1):
        """
        Set the worker count
        """
        self.amqp.scale(worker_count)
