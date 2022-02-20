#!/usr/bin/env python3

import unittest
from unittest.mock import patch
from time import sleep

from src.task_queue.wrapper.amqp import AMQP, Message

host: str = 'image-builder'
username = 'guest'
password = 'guest'
queue_name = 'amqp_test_queue'


class AMQPTest(unittest.TestCase):

    def test___init___consumer(self):
        def callback(message: Message):
            print(message.body)
        amqp = AMQP(
            host=host,
            queue_name=queue_name,
            username=username,
            password=password,
            amqp_type='consumer',
            callback=callback
        )
        self.assertIsNotNone(amqp.connection)
        self.assertTrue(amqp.connection.is_open)
        self.assertFalse(amqp.connection.is_closed)
        self.assertEqual(len(amqp.consumer_list), 1)
        amqp.close()
        amqp.callback = []

    def test___test___publisher(self):
        amqp = AMQP(
            host=host,
            queue_name=queue_name,
            username=username,
            password=password,
            amqp_type='publisher'
        )
        self.assertIsNotNone(amqp.connection)
        self.assertTrue(amqp.connection.is_open)
        self.assertFalse(amqp.connection.is_closed)
        self.assertCountEqual(amqp.consumer_list, [])

    def test_send_publisher(self):
        amqp = AMQP(
            host=host,
            queue_name=queue_name,
            username=username,
            password=password,
            amqp_type='publisher'
        )
        self.assertIsNotNone(amqp.connection)
        self.assertTrue(amqp.connection.is_open)
        self.assertFalse(amqp.connection.is_closed)
        self.assertCountEqual(amqp.consumer_list, [])
        message: str = '{\'test_key\':\'test_value\'}'
        result = amqp.send(message)
        self.assertIsNone(result)

    def callback(self, message: Message):
        self.assertEqual(message.body, '{\'test_key\':\'test_value\'}')

    def test_receive_consumer(self):
        with patch.object(self, 'callback', side_effect=self.callback) as mock:
            amqp = AMQP(host=host, queue_name=queue_name, username=username, password=password, amqp_type='consumer', callback=mock)
            self.assertIsNotNone(amqp.connection)
            self.assertTrue(amqp.connection.is_open)
            self.assertFalse(amqp.connection.is_closed)
            self.assertEqual(len(amqp.consumer_list), 1)
            amqp.listen()
            amqp.close()
            amqp.callback = []
            mock.assert_called_once_with(Message(body='{\'test_key\':\'test_value\'}', channel=amqp.consumer_list[0].channel, delivery_tag=1))

    def test_scale(self):
        amqp = AMQP(host=host, queue_name=queue_name, username=username, password=password, amqp_type='consumer', callback=self.callback)
        self.assertIsNotNone(amqp.connection)
        self.assertTrue(amqp.connection.is_open)
        self.assertFalse(amqp.connection.is_closed)
        self.assertEqual(len(amqp.consumer_list), 1)
        amqp.scale(2)
        self.assertEqual(len(amqp.consumer_list), 2)
        amqp.scale(1)
        self.assertEqual(len(amqp.consumer_list), 1)
        amqp.scale(3)
        self.assertEqual(len(amqp.consumer_list), 3)
        amqp.scale(1)
        self.assertEqual(len(amqp.consumer_list), 1)
        amqp.close()
        amqp.callback = []

    def test_ack(self):
        amqp: AMQP = None
        def callback_ack(message: Message):
            amqp.ack(message)
            print('callback_ack called')
            self.assertIsNone(sleep(30))
            self.assertEqual(amqp.message_count(), 0)

        with patch.object(self, 'callback', side_effect=callback_ack) as mock:
            amqp = AMQP(host=host, queue_name=queue_name, username=username, password=password, amqp_type='consumer', callback=mock)
            amqp.clear_queue()
            self.assertIsNotNone(amqp.connection)
            self.assertTrue(amqp.connection.is_open)
            self.assertFalse(amqp.connection.is_closed)
            self.assertEqual(len(amqp.consumer_list), 1)
            message: str = '{\'test_key\':\'test_value\'}'
            result = amqp.send(message)
            self.assertIsNone(result)
            amqp.listen()
            amqp.close()
            amqp.callback = []
            mock.assert_called_once_with(Message(body='{\'test_key\':\'test_value\'}', channel=amqp.consumer_list[0].channel, delivery_tag=1))


if __name__ == '__main__':
    unittest.main()
