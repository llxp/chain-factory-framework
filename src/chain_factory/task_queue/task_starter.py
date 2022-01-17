from .models.mongo.task import Task
from .wrapper.amqp import AMQP


class TaskStarter:
    def __init__(
        self,
        namespace: str,
        rabbitmq_host: str,
        username: str,
        password: str
    ):
        self.namespace = namespace
        self.amqp_client = AMQP(
            host=rabbitmq_host,
            queue_name=namespace + "_" + "task_queue",
            username=username,
            password=password,
            amqp_type="publisher",
            virtual_host=namespace,
        )

    def start_task(self, task_name, arguments, node_names=[], tags=[]):
        task = Task(name=task_name, arguments=arguments,
                    node_names=node_names, tags=tags)
        self.amqp_client.send(task.to_json())
