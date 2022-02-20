from .models.mongodb_models import Task, TaskStatus
from .wrapper.mongodb_client import MongoDBClient


class TaskWaiter:
    """
    TaskWaiter is a class that is used to wait for tasks to be completed.
    """

    def __init__(self, mongo_client: MongoDBClient):
        """
        Initialize the TaskWaiter class.
        :param mongo_client: MongoDBClient
        """
        self.mongo_client = mongo_client
        self.database = mongo_client.client

    async def wait_for_task_name(
        self,
        task_name: str,
        arguments: dict
    ) -> bool:
        """
        Wait for a task to be completed.
        :param task_name: str
        :param arguments: dict
        :return: bool
        """
        arg_keys = list(arguments.keys())
        query_args = []
        for key in arg_keys:
            query_args.append({'task.arguments.' + key: arguments[key]})
        task = await self.database.find_one(
            Task, {'name': task_name, '$and': query_args})
        if task is None:
            return False
        else:
            task_status = await self.database.find_one(
                TaskStatus, TaskStatus.task_id == task.task.task_id)
            if task_status is None:
                return False
            else:
                return True

    async def wait_for_task_id(self, task_id: str) -> bool:
        """
        Wait for a task to be completed.
        :param task_id: str
        :return: bool
        """
        task = await self.database.find_one(Task, Task.task_id == task_id)
        if task is None:
            return False
        else:
            task_status = await self.database.find_one(
                TaskStatus, TaskStatus.task_id == task_id)
            if task_status is None:
                return False
            else:
                return True
