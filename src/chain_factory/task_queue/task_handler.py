from typing import Dict, Union, Callable, List
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
from .common.task_return_type import ArgumentType, TaskReturnType, TaskRunnerReturnType

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
        self.task_timeout = None
        self.namespace = ""

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
        blocked_queue_name: str,
        namespace: str,
    ):
        QueueHandler.init(
            self, amqp_host, queue_name, amqp_username, amqp_password, namespace
        )
        self.node_name: str = node_name
        self.redis_client: RedisClient = redis_client
        self.mongo_client: MongoDBClient = mongodb_client
        self.wait_queue_name: str = wait_queue_name
        self.blocked_queue_name: str = blocked_queue_name
        self.namespace = namespace

        self._init_amqp_publishers(
            amqp_host=amqp_host,
            amqp_username=amqp_username,
            amqp_password=amqp_password,
        )
        self.block_list = ListHandler(
            list_name=namespace + "_" + incoming_block_list_redis_key,
            redis_client=redis_client,
        )

    def update_task_timeout(self):
        for task in self.registered_tasks:
            task_runner = self.registered_tasks[task]
            task_runner.task_timeout = self.task_timeout

    def _init_amqp_publishers(
        self,
        amqp_host: str,
        amqp_username: str,
        amqp_password: str,
        ssl: bool = False,
        ssl_options=None,
    ):
        self.amqp_wait: AMQP = AMQP(
            host=amqp_host,
            queue_name=self.wait_queue_name,
            username=amqp_username,
            password=amqp_password,
            amqp_type="publisher",
            port=5672,
            ssl=False,
            ssl_options=None,
            virtual_host=self.namespace,
        )
        self.amqp_planned: AMQP = AMQP(
            host=amqp_host,
            queue_name="dlx." + self.wait_queue_name,
            username=amqp_username,
            password=amqp_password,
            amqp_type="publisher",
            port=5672,
            ssl=False,
            ssl_options=None,
            queue_options={
                "x-dead-letter-exchange": "dlx." + self.queue_name,
                "x-dead-letter-routing-key": self.queue_name,
            },
            virtual_host=self.namespace,
        )
        self.amqp_blocked: AMQP = AMQP(
            host=amqp_host,
            queue_name=self.blocked_queue_name,
            username=amqp_username,
            password=amqp_password,
            amqp_type="publisher",
            port=5672,
            ssl=False,
            ssl_options=None,
            virtual_host=self.namespace,
        )

    def _check_blocklist(self, task: Task, message: Message) -> bool:
        """
        Check the redis blocklist for the node name and task
        """
        blocklist = self.block_list.get()
        # print('task_handler')
        # print(blocklist)
        if blocklist is None or blocklist.list_items is None:
            # the blocklist couldn't be retrieved from redis,
            # so rejecting every incoming task
            LOGGER.warning(
                "could not retrieve blocklist from redis, rejecting all tasks"
            )
            self.nack(message)
            time.sleep(wait_time)
            return True
        for item in blocklist.list_items:
            node_name = item.name
            if (
                item.content == task.name
                and node_name == self.node_name
                or node_name == "default"
            ):
                LOGGER.info(
                    "task '%s' is in block list, "
                    "dispatching to blocked_queue" % task.name
                )
                task.update_time()
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
            namespace=self.namespace,
            tags=tags,
        )
        workflow_dict = TaskHandler._dataclass_to_dict(workflow)
        self.mongo_client.db().workflows.insert_one(workflow_dict)

    class _ArgumentExcluder:
        def __init__(self, arguments):
            self.arguments = arguments
            self.arguments_copy = dict(arguments)

        def argument_excluded(self, argument, excluded_argument):
            return argument != "exclude" and argument == excluded_argument

        def should_exclude_any_arguments(self):
            return self.arguments and "exclude" in self.arguments

        def excluded_arguments(self):
            return self.arguments["exclude"]

        def del_argument(self, argument):
            del self.arguments[argument]

        def check_exclude_argument(self, argument, excluded_argument):
            if self.argument_excluded(argument, excluded_argument):
                self.del_argument(argument)

        def exclude(self):
            if self.should_exclude_any_arguments():
                self._exclude_arguments()

        def _exclude_arguments(self):
            for argument in self.arguments_copy:
                for excluded_argument in self.excluded_arguments():
                    self.check_exclude_argument(argument, excluded_argument)

    def _save_task_workflow_association(self, task: Task):
        """
        Report the assignment from workflow_id to task_id to the redis database
        """
        if task.arguments:
            arguments_excluder = self._ArgumentExcluder(task.arguments)
            arguments_excluder.exclude()
            task.arguments = arguments_excluder.arguments
            association = TaskWorkflowAssociation(
                workflow_id=task.workflow_id, task=task, node_name=self.node_name
            )
            task_to_workflow_dict = TaskHandler._dataclass_to_dict(association)
            self.mongo_client.db().tasks.insert_one(task_to_workflow_dict)
            if arguments_excluder.arguments_copy:
                task.arguments = arguments_excluder.arguments_copy

    def _run_task(self, task: Task) -> TaskRunnerReturnType:
        """
        Runs the specified task and returns the result of the task function
        """
        # buffer to redirect stdout/stderr to the database
        log_buffer = BytesIOWrapper(task.task_id, task.workflow_id, self.mongo_client.db())
        # run the task
        return self.registered_tasks[task.name].run(
            task.arguments, task.workflow_id, log_buffer
        )

    def _new_task_from_result(
        self, task_result: TaskReturnType, new_arguments: Dict[str, str]
    ) -> Task:
        # check, if result is a string ==> task name
        if isinstance(task_result, str):
            return Task(task_result, new_arguments)
        if isinstance(task_result, Task):
            return task_result
        # check, if result is a registered callable
        if callable(task_result):
            return Task(task_result.__name__, new_arguments)
        return None

    def _return_new_task(
        self,
        old_task: Task,
        new_arguments: Dict[str, str],
        task_result: Union[str, Task, Callable[..., Dict[str, str]]],
    ) -> Task:
        """
        add the old task to the current task as the parent and schedule
        the to be scheduled task to the message queue
        """
        # rename task_result to new_task for better readability
        new_task = self._new_task_from_result(task_result, new_arguments)
        new_task.set_parent_task(old_task)
        if sticky_tasks:  # settings.sticky_tasks
            # if sticky_tasks option is set,
            # only execute the full workflow on the node it started on
            new_task.node_names = [self.node_name]
        return new_task

    def _return_error_task(self, task: Task, new_arguments: Dict[str, str]) -> None:
        """
        Reenqueue the current task to the wait queue if the current task failed
        """
        # update received date
        task.update_time()
        # accociate the current task with the next task
        task.set_as_parent_task()
        # remove current task id on error
        # generate a new one on next run
        del task.task_id
        task.arguments = new_arguments
        # send task to wait queue
        self.amqp_wait.send(task.to_json())
        return None

    def _save_first_task_as_workflow(self, task: Task):
        if not task.has_parent_task():
            # report workflow to database,
            # if it is the first task in the workflow task chain
            self._save_workflow(task.workflow_id, task.tags)

    def _save_task_result(self, task_id: str, result: str):
        col = self.mongo_client.db().task_status
        col.insert_one(
            {
                "task_id": task_id,
                "namespace": self.namespace,
                "status": result,
                "created_date": datetime.now(pytz.UTC),
            }
        )

    def _mark_workflow_as_stopped(self, workflow_id: str, status: str):
        col = self.mongo_client.db().workflow_status
        if not (
            col.find_one({"workflow_id": workflow_id, "namespace": self.namespace})
        ):
            col.insert_one(
                {
                    "workflow_id": workflow_id,
                    "namespace": self.namespace,
                    "status": status,
                    "created_date": datetime.now(pytz.UTC),
                }
            )

    def _handle_workflow_stopped(self, result: str, task: Task):
        self._save_task_result(task.task_id, result)
        # None means, the workflow chain stops
        self._mark_workflow_as_stopped(task.workflow_id, result)
        return None  # do nothing

    def _handle_repeat_task(self, task: Task, arguments: ArgumentType, result: str):
        self._save_task_result(task.task_id, result)
        # the result is False, indicating an error
        # => schedule the task to the waiting queue
        return self._return_error_task(task, arguments)

    def _handle_task_result(
        self,
        task_result: Union[bool, None, Task],
        arguments: Dict[str, str],
        message: Message,
        task: Task,
    ) -> Union[Task, None]:
        self.ack(message)
        if task_result is False:
            return self._handle_repeat_task(task, arguments, "False")
        elif task_result is TimeoutError:
            if self.registered_tasks[task.name].task_repeat_on_timeout:
                return self._handle_repeat_task(task, arguments, "Timeout")
            return self._handle_workflow_stopped("Timeout", task)
        else:  # task_result now can only be Task/None/Exception
            if task_result is None:
                return self._handle_workflow_stopped("None", task)
            # Exception means an exception occured during the task run
            elif task_result is Exception:
                return self._handle_workflow_stopped("Exception", task)
            # Task means, a new/next task has been returned, to be scheduled to the queue
            else:
                self._save_task_result(task.task_id, "Task")
                return self._return_new_task(task, arguments, task_result)

    def _prepare_task_in_database(self, task: Task):
        self._save_first_task_as_workflow(task)
        # associate task id with workflow id in database
        self._save_task_workflow_association(task)

    def _prepare_task(self, task: Task):
        """
        Generates a task id and saves the association to it in the database
        """
        task.generate_task_id()
        self._prepare_task_in_database(task)
        return task

    def _handle_run_task(self, task: Task, message: Message) -> Union[Task, None]:
        """
        will be executed, when the task is valid,
        which means it has a valid workflow id
        - generates a unique task id and
        - saves the association to the workflow in the database
        - runs the task
        - returns a function to handle the task result
        """
        task = self._prepare_task(task)
        task_runner_result = self._run_task(task)
        if task_runner_result:
            task_result, arguments = task_runner_result
            # handle task result and return new Task
            return self._handle_task_result(task_result, arguments, message, task)
        else:
            # error occured converting the arguments from Dict[str, str]
            # to Dict[str, Any] -> to the actual type expected from the task
            print("An Error occured during the task run")
            return None

    def _handle_planned_task(self, task: Task, message: Message):
        """
        sends the task to the delayed queue with an ttl of planned_date - now
        """
        # not implemented yet
        print("planning tasks is not implemented yet")
        return None

    def _handle_task(self, task: Task, message: Message) -> Union[None, Task]:
        """
        Runs a precheck, to see if the task already has a valid workflow id
        if not, it will generate a unique workflow id
        and return it to the queue
        """
        if task.workflow_precheck():
            return self._prepare_workflow(task, message)
        if task.is_stopped(self.namespace, self.mongo_client):
            return self._handle_stopped(task, message)
        if task.is_planned_task():
            return self._handle_planned_task(task, message)
        return self._handle_run_task(task, message)

    def _prepare_workflow(self, task: Task, message: Message):
        # workflow id does not exist
        self.ack(message)
        task.generate_workflow_id()
        return task

    def _handle_stopped(self, task: Task, message: Message):
        print("_handle_stopped")
        self._save_task_result(task.task_id, "stopped")
        self.ack(message)
        return None

    def _send_to_queue(self, queue: AMQP, message: Message, task: Task):
        queue.send(task.to_json())
        self.ack(message)

    def _handle_rejected(self, requested_task: Task, message: Message):
        """
        Increases the reject counter and requeues the task to the message queue
        """
        # task rejected, increase reject counter
        requested_task.increase_rejected()
        if requested_task.reject_counter > reject_limit:
            requested_task.reject_counter = 0
            self._send_to_queue(self.amqp_wait, message, requested_task)
        else:
            self._send_to_queue(self.amqp, message, requested_task)
        return None

    def on_task(self, task: Task, message: Message) -> Union[None, Task]:
        """
        The callback function, which will be called from the amqp library
        It deserializes the amqp message and then
        checks, if the requested taks is registered and calls it.
        If the task name is on the blocklist,
        it will be rejected, rescheduled and then wait for some time

        Returns either None or a new task
        """
        task_rejected = task.check_node_filter(self.node_name)
        if task_rejected:
            logging.debug(
                "task_rejected, because current node "
                "is not in the node_names list: " + task.to_json()
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
            # task not found on the current node
            # rejecting task
            # print('rejecting task:', task.name)
            return self._handle_rejected(task, message)
        # task is either
        # => on the block list or
        # => not registered on the current node
        # return None to indicate no next task should be scheduled
        return None

    def add_task(self, name, callback, repeat_on_timeout):
        """
        Register a new task/task function
        """
        task = TaskRunner(name, callback, self.namespace)
        task.task_repeat_on_timeout = repeat_on_timeout
        self.set_redis_client(task)
        self.registered_tasks[name] = task

    def set_redis_client(self, task: Task):
        task.set_redis_client(self.redis_client)
        task.namespace = self.namespace

    def task_set_redis_client(self):
        for task_name in self.registered_tasks:
            task: TaskRunner = self.registered_tasks[task_name]
            self.set_redis_client(task)

    def scale(self, worker_count: int = 1):
        """
        Set the worker count
        """
        self.amqp.scale(worker_count)
