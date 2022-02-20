from .models.mongodb_models import Task
from .wrapper.rabbitmq import RabbitMQ


class TaskStarter:
    def __init__(
        self,
        namespace: str,
        rabbitmq_url: str,
    ):
        self.namespace = namespace
        queue_name = namespace + "_task_queue"
        self.amqp_client = RabbitMQ(
            url=rabbitmq_url,
            queue_name=queue_name,
            rmq_type="publisher"
        )

    async def start_task(self, task_name, arguments, node_names=[], tags=[]):
        task = Task(
            name=task_name,
            arguments=arguments,
            node_names=node_names,
            tags=tags
        )
        await self.amqp_client.send(task.json())
