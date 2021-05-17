import unittest
import pytz
import time
import sys
from unittest.mock import patch
from datetime import datetime
from typing import Callable

from src.task_queue.queue_handler import QueueHandler
from src.task_queue.wrapper.amqp import Message, AMQP
from amqpstorm import AMQPConnectionError
from src.task_queue.models.mongo.task import Task


test_str_empty = ''
test_str_valid = Task(name='test').to_json()
test_str_invalid_object = '{"123":"123"}'
test_str_invalid_json = '{123:123}'
test_str_empty_object = '{}'
message_empty = Message(
    body=test_str_empty, channel=None, delivery_tag=0)
mock_task_empty = None
message_valid = Message(
    body=test_str_valid, channel=None, delivery_tag=0)
mock_task_valid = Task(name='test')


class MockedQueueHandler(QueueHandler):
    def __init__(self):
        super().__init__(
            amqp_host='',
            queue_name='',
            amqp_username='',
            amqp_password=''
        )

    class AMQP():
        def listen(self):
            pass

        def ack(self):
            pass

        def nack(self):
            pass

        def send(self):
            pass

    def _connect(self, amqp_host: str, amqp_username: str, amqp_password: str):
        self.amqp = MockedQueueHandler.AMQP()


mocked_instance = MockedQueueHandler()


class QueueHandlerTest(unittest.TestCase):

    def test__parse_json(self):
        try:
            result = QueueHandler._parse_json(test_str_empty)
            assert result is None
        except (AttributeError, TypeError, Exception) as e:
            raise e
            assert False

        try:
            result = QueueHandler._parse_json(test_str_valid)
            assert result is not None and isinstance(result, Task)
        except (AttributeError, TypeError, Exception) as e:
            raise e
            assert False

        try:
            result = QueueHandler._parse_json(test_str_invalid_json)
            assert result is None
        except (AttributeError, TypeError, Exception) as e:
            raise e
            assert False

        try:
            result = QueueHandler._parse_json(test_str_empty_object)
            assert result is not None and isinstance(result, Task)
        except (AttributeError, TypeError, Exception) as e:
            raise e
            assert False

        try:
            result = QueueHandler._parse_json(test_str_invalid_object)
            assert result is not None and isinstance(result, Task)
        except (AttributeError, TypeError, Exception) as e:
            raise e
            assert False

    def test__on_message(self):
        # invalid test
        with patch.object(
            mocked_instance, '_parse_json', return_value=mock_task_empty
        ) as mock:
            with patch.object(
                mocked_instance,
                '_on_task_error',
                side_effect=mocked_instance._on_task_error
            ) as mock2:
                mocked_instance._on_message(message_empty)
                mock2.assert_called_once_with(
                    message=message_empty
                )
            mock.assert_called_once_with(body=message_empty.body)

        # valid test
        with patch.object(
            mocked_instance, '_parse_json', return_value=mock_task_valid
        ) as mock:
            with patch.object(
                mocked_instance, '_on_task', return_value=''
            ) as mock2:
                mocked_instance._on_message(message_valid)
                mock2.assert_called_once_with(
                    task=mock_task_valid,
                    message=message_valid
                )
            mock.assert_called_once_with(body=message_valid.body)

    def test__on_task(self):
        message_empty = Message(
            body=test_str_empty, channel=None, delivery_tag=0)
        mock_task_empty = None
        message_valid = Message(
            body=test_str_valid, channel=None, delivery_tag=0)
        mock_task_valid = Task(name='test')

        # Task return test
        with patch.object(
            mocked_instance, 'on_task', return_value=mock_task_valid
        ) as mock:
            return_value = mocked_instance._on_task(
                mock_task_valid,
                message_valid
            )
            mock.assert_called_once_with(
                mock_task_valid,
                message_valid
            )
            assert return_value == mock_task_valid.to_json()

        # None return test
        with patch.object(
            mocked_instance, 'on_task', return_value=mock_task_empty
        ) as mock:
            return_value = mocked_instance._on_task(
                mock_task_empty,
                message_empty
            )
            mock.assert_called_once_with(
                mock_task_empty,
                message_empty
            )
            assert return_value == ''

    def test_listen(self):
        with patch.object(
            mocked_instance.amqp, 'listen', return_value=None
        ) as mock:
            mocked_instance.listen()
            mock.assert_called()

    def test_ack(self):
        with patch.object(
            mocked_instance.amqp, 'ack', return_value=None
        ) as mock:
            mocked_instance.ack(message=message_empty)
            mock.assert_called_with(message=message_empty)

        with patch.object(
            mocked_instance.amqp, 'ack', return_value=None
        ) as mock:
            mocked_instance.ack(message=message_valid)
            mock.assert_called_with(message=message_valid)

    def test_nack(self):
        with patch.object(
            mocked_instance.amqp, 'nack', return_value=None
        ) as mock:
            mocked_instance.nack(message=message_empty)
            mock.assert_called_with(message=message_empty)

        with patch.object(
            mocked_instance.amqp, 'nack', return_value=None
        ) as mock:
            mocked_instance.nack(message=message_valid)
            mock.assert_called_with(message=message_valid)

    def test_reschedule(self):
        with patch.object(
            mocked_instance, 'nack', return_value=None
        ) as mock:
            mocked_instance.reschedule(message=message_empty)
            mock.assert_called_with(message=message_empty)

        with patch.object(
            mocked_instance, 'nack', return_value=None
        ) as mock:
            mocked_instance.reschedule(message=message_valid)
            mock.assert_called_with(message=message_valid)

    def test_send_to_queue(self):
        now_mocked = datetime.now(pytz.UTC)
        mocked_queue = MockedQueueHandler.AMQP()
        updated_mock_task_valid = mock_task_valid
        updated_mock_task_valid.received_date = now_mocked
        with patch.object(
            mocked_queue, 'send', return_value=True
        ) as mock:
            with patch.object(
                QueueHandler, '_now', return_value=now_mocked
            ) as mock2:
                mocked_instance.send_to_queue(
                    task=mock_task_valid,
                    amqp_queue=mocked_queue
                )
                mock2.assert_called()
            mock.assert_called_with(message=updated_mock_task_valid.to_json())

    def test__now(self):
        now = datetime.now(pytz.UTC)
        time.sleep(0.01)
        assert now != mocked_instance._now()

    def test_on_task(self):
        try:
            mocked_instance.on_task(task=None, message=None)
        except NotImplementedError:
            assert True

    def test__connect(self):
        with patch.object(
            AMQP, '__init__', return_value=None
        ) as mock:
            with patch.object(
                QueueHandler, '_on_message', return_value=None
            ):
                QueueHandler(
                    amqp_host='',
                    queue_name='',
                    amqp_username='',
                    amqp_password=''
                )
                mock.assert_called_with(
                    host='',
                    queue_name='',
                    port=5672,
                    username='',
                    password='',
                    amqp_type='consumer',
                    callback=QueueHandler._on_message,
                    ssl=False,
                    ssl_options=None
                )

        def amqp_init(
            host: str,
            queue_name: str,
            port: int,
            username: str,
            password: str,
            amqp_type: str,
            callback: Callable[[Message], str],
            ssl: bool,
            ssl_options
        ):
            raise AMQPConnectionError('Error')

        with patch.object(
            AMQP,
            '__init__',
            return_value=None,
            side_effect=amqp_init
        ) as mock:
            with patch.object(
                QueueHandler, '_on_message', return_value=None
            ):
                with patch.object(
                    sys, 'exit', return_value=None
                ) as mock3:
                    QueueHandler(
                        amqp_host='',
                        queue_name='',
                        amqp_username='',
                        amqp_password=''
                    )
                    mock3.assert_called_with(1)
                    mock.assert_called_with(
                        host='',
                        queue_name='',
                        port=5672,
                        username='',
                        password='',
                        amqp_type='consumer',
                        callback=QueueHandler._on_message,
                        ssl=False,
                        ssl_options=None
                    )
