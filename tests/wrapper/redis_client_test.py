from os.path import dirname
from sys import path
from time import sleep
path.append(dirname(dirname(dirname(dirname(__file__)))))
from framework.src.chain_factory.task_queue.wrapper.redis_client import (
    RedisClient, connection_pools
)
import unittest
from unittest import IsolatedAsyncioTestCase


redis_url = 'redis://localhost:6379/0'
port = 6379


class RedisClientTest(IsolatedAsyncioTestCase):
    def __init__(self, *args, **kwargs):
        super(RedisClientTest, self).__init__(*args, **kwargs)

    # def test_init(self):
    #     redis_client: RedisClient = RedisClient(
    #         redis_url, 'lock-key-test-init')
    #     self.assertIsNotNone(redis_client._connection)
    #     self.assertIsNotNone(connection_pools[redis_url])

    async def test_set(self):
        redis_client: RedisClient = RedisClient(
            redis_url, 'lock-key-test-init')
        self.assertIsNotNone(redis_client._connection)
        self.assertIsNotNone(connection_pools[redis_url])
        result = await redis_client.set('test_key', 'test_value')
        self.assertTrue(result)

        redis_client2: RedisClient = RedisClient(
            redis_url, 'lock-key-test-init')
        self.assertIsNotNone(redis_client2._connection)
        self.assertIsNotNone(connection_pools[redis_url])
        result = await redis_client2.get('test_key')
        self.assertIsNotNone(result)
        self.assertEqual(result, b'test_value')
        result2 = await redis_client2.get('test_key')
        self.assertIsNotNone(result2)
        self.assertEqual(result2, b'test_value')
        result3 = await redis_client2.get('test_key2')
        self.assertIsNone(result3)
        self.assertEqual(result3, None)

    # async def test_get(self):
    #     # sleep(10)
    #     redis_client: RedisClient = RedisClient(
    #         redis_url, 'lock-key-test-init')
    #     self.assertIsNotNone(redis_client._connection)
    #     self.assertIsNotNone(connection_pools[redis_url])
    #     result = await redis_client.get('test_key')
    #     self.assertIsNotNone(result)
    #     self.assertEqual(result, b'test_value')
    #     result2 = await redis_client.get('test_key')
    #     self.assertIsNotNone(result2)
    #     self.assertEqual(result2, b'test_value')
    #     result3 = await redis_client.get('test_key2')
    #     self.assertIsNone(result3)
    #     self.assertEqual(result3, None)


if __name__ == '__main__':
    unittest.main()
