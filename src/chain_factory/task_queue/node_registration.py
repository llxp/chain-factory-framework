import inspect
import json
from typing import Callable, Dict, List
from .models.mongo.task import Task
from .models.mongo.node_tasks import NodeTasks
from .models.mongo.registered_task import RegisteredTask
from .models.mongo.namespace import Namespace
from .task_runner import TaskRunner
from .task_handler import TaskHandler
from .wrapper.mongodb_client import MongoDBClient
from .common.settings import unique_hostnames, force_register


class NodeRegistration():
    def __init__(
        self,
        namespace: str,
        mongodb_client: MongoDBClient,
        node_name: str,
        task_handler: TaskHandler
    ):
        self.namespace = namespace
        self.mongodb_client = mongodb_client
        self.node_name = node_name
        self.task_handler = task_handler

    def register_tasks(self):
        """
        Registers all internally registered tasks in the database
        in the form:
            node_name/task_name
        """
        already_registered_node = self._node_already_registered()
        # print(already_registered_node)
        if already_registered_node is not None:
            if unique_hostnames:  # setting
                self._raise_node_already_registered()
            if force_register:  # setting
                self._remove_node_registration()
        self._register_node()

    def _register_node(self):
        node_tasks = self._node_tasks()
        self.mongodb_client.db().registered_tasks.insert_one(
            dict(json.loads(node_tasks.to_json()))
        )

    def _remove_node_registration(self):
        self.mongodb_client.db().registered_tasks.delete_many(
            {
                '$and': [
                    {'node_name': self.node_name},
                    {'namespace': self.namespace}
                ]
            }
        )

    def _raise_node_already_registered(self):
        raise Exception(
            'Existing node name found in redis list, exiting.\n'
            'If this is intentional, set unique_hostnames to False'
        )

    def _node_already_registered(self):
        return self.mongodb_client.db().registered_tasks.find_one(
            {
                '$and': [
                    {'node_name': self.node_name},
                    {'namespace': self.namespace}
                ]
            }, {
                '_id': 0
            }
        )

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
            namespace=self.namespace,
            tasks=all_registered_tasks
        )

    def _existing_namespaces(self):
        namespaces = self.mongodb_client.db().namespaces
        return namespaces.find_one({'namespace': self.namespace})

    def register_namespace(self):
        if not self._existing_namespaces():
            self.mongodb_client.db().namespaces.insert_one(
                dict(json.loads(Namespace(namespace=self.namespace).to_json()))
            )

    def register(self):
        self.register_namespace()
        self.register_tasks()
