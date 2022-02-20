import unittest
from unittest.mock import patch

from framework.src.chain_factory.task_queue.task_handler import TaskHandler, ListHandler
from framework.src.chain_factory.task_queue.models.mongodb_models import Task
from framework.src.chain_factory.task_queue.models.redis_models import TaskStatus
from framework.src.chain_factory.task_queue.queue_handler import QueueHandler
from chain_factory.task_queue.wrapper.rabbitmq import Message, RabbitMQ
from framework.src.chain_factory.task_queue.common.settings import \
    task_status_redis_key, incoming_block_list_redis_key
import framework.src.chain_factory.task_queue.common.generate_random_id


def get_mocked_instance():
    with patch.object(
        RabbitMQ, '__init__', return_value=None
    ):
        with patch.object(
            TaskHandler, '_init_amqp_publishers', return_value=None
        ) as mock1:
            with patch.object(
                ListHandler, '__init__', return_value=None
            ) as mock2:
                redis_client = MockedRedisClient()
                mocked_instance = TaskHandler(
                    node_name='',
                    amqp_host='',
                    amqp_username='',
                    amqp_password='',
                    redis_client=redis_client,
                    mongodb_client=None,
                    queue_name='',
                    wait_queue_name='',
                    blocked_queue_name=''
                )
                mock2.assert_called_with(
                    list_name=incoming_block_list_redis_key,
                    redis_client=redis_client
                )
                mock1.assert_called_with(
                    amqp_host='',
                    amqp_username='',
                    amqp_password=''
                )
                return mocked_instance


class MockedRedisClient():
    def rpush():
        pass


class TaskHandlerTest(unittest.TestCase):
    def test__generate_workflow_id(self):
        test_task = Task()
        task_id = '1'
        with patch.object(
            TaskHandler, '_generate_random_id', return_value=task_id
        ) as mock:
            task = TaskHandler._generate_workflow_id(test_task)
            test_task.workflow_id = task_id
            assert task == test_task
            mock.assert_called()

    def test__generate_task_id(self):
        test_task = Task()
        task_id = '1'
        with patch.object(
            TaskHandler, '_generate_random_id', return_value=task_id
        ) as mock:
            task = TaskHandler._generate_task_id(test_task)
            test_task.workflow_id = task_id
            assert task == test_task
            mock.assert_called()

    def test__handle_rejected_increase_counter(self):
        test_task = Task()
        task = TaskHandler._handle_rejected_increase_counter(test_task)
        assert task.reject_counter == 1

    def test__workflow_precheck(self):
        test_task = Task()
        # True check
        test_task.parent_task_id = ''
        precheck = TaskHandler._workflow_precheck(test_task)
        assert precheck is True

        # False check
        test_task.parent_task_id = '1'
        precheck = TaskHandler._workflow_precheck(test_task)
        assert precheck is False

    def test___init__(self):
        with patch.object(
            RabbitMQ, '__init__', return_value=None
        ):
            with patch.object(
                TaskHandler, '_init_amqp_publishers', return_value=None
            ) as mock1:
                with patch.object(
                    ListHandler, '__init__', return_value=None
                ) as mock2:
                    redis_client = MockedRedisClient()
                    mocked_instance = TaskHandler(
                        node_name='',
                        amqp_host='',
                        amqp_username='',
                        amqp_password='',
                        redis_client=redis_client,
                        mongodb_client=None,
                        queue_name='',
                        wait_queue_name='',
                        blocked_queue_name=''
                    )
                    mock2.assert_called_with(
                        list_name=incoming_block_list_redis_key,
                        redis_client=redis_client
                    )
                mock1.assert_called_with(
                    amqp_host='',
                    amqp_username='',
                    amqp_password=''
                )

    def test_init_amqp_publishers(self):
        mocked_instance = get_mocked_instance()
        amqp_host = ''
        amqp_username = ''
        amqp_password = ''
        with patch.object(
            RabbitMQ, '__init__', return_value=None
        ) as mock:
            TaskHandler._init_amqp_publishers(
                self=mocked_instance,
                amqp_host=amqp_host,
                amqp_username=amqp_username,
                amqp_password=amqp_password
            )
            mock.assert_called_with(
                host='',
                queue_name='',
                username='',
                password='',
                amqp_type='publisher',
                port=5672,
                ssl=False,
                ssl_options=None
            )
            assert mocked_instance.rabbitmq_wait is not None
            assert mocked_instance.amqp_blocked is not None

    def test__save_task_result(self):
        with patch.object(
            RabbitMQ, '__init__', return_value=None
        ):
            with patch.object(
                TaskHandler, '_init_amqp_publishers', return_value=None
            ):
                with patch.object(
                    ListHandler, '__init__', return_value=None
                ):
                    mocked_instance = TaskHandler(
                        node_name='',
                        amqp_host='',
                        amqp_username='',
                        amqp_password='',
                        redis_client=MockedRedisClient(),
                        mongodb_client=None,
                        queue_name='',
                        wait_queue_name='',
                        blocked_queue_name=''
                    )
                    with patch.object(
                        mocked_instance.redis_client,
                        'rpush',
                        return_value=None
                    ) as mock:
                        task_id = '1'
                        status = 'None'
                        task_status = TaskStatus(
                            status=status, task_id=task_id)
                        mocked_instance._save_task_result(
                            task_id=task_id, result=status)
                        mock.assert_called_with(
                            task_status_redis_key, task_status.json())

    def test__parse_task_output(self):

        def func_task():
            return Task('func2')

        def func_none():
            return None

        def func_callable():
            return func_task

        def func_callable_args(argument: int):
            return func_task, {'argument': 1}

        def func_false():
            return False

        def func_false_args(argument: int):
            return False, {'argument': 1}

        test_task = Task()
        # True check
        test_task.parent_task_id = ''
        #precheck = TaskHandler._parse_task_output(test_task)
        #assert precheck is True
