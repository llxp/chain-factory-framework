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
        self.database = mongo_client.db()

    def wait_for_task_name(self, task_name: str, arguments: dict) -> bool:
        """
        Wait for a task to be completed.
        :param task_name: str
        :param arguments: dict
        :return: bool
        """
        tasks_collection = self.database.tasks
        arg_keys = list(arguments.keys())
        query_args = []
        for key in arg_keys:
            query_args.append({'task.arguments.' + key: arguments[key]})
        task = tasks_collection.find_one(
            {'name': task_name, '$and': query_args})
        task_status_collection = self.database.tasks_finished
        if task is None:
            return False
        else:
            task_status = task_status_collection.find_one(
                {'task_id': task['task']['task_id']})
            if task_status is None:
                return False
            else:
                return True

    def wait_for_task_id(self, task_id: str) -> bool:
        """
        Wait for a task to be completed.
        :param task_id: str
        :return: bool
        """
        tasks_collection = self.database.tasks
        task = tasks_collection.find_one({'task_id': task_id})
        task_status_collection = self.database.tasks_finished
        if task is None:
            return False
        else:
            task_status = task_status_collection.find_one(
                {'task_id': task_id})
            if task_status is None:
                return False
            else:
                return True
