from src.task_queue.redis_client import RedisClient, connection_pools
import unittest
import sys

from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))
sys.path.append(d)


host = 'image-builder'
port = 6379


class RedisClientTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(RedisClientTest, self).__init__(*args, **kwargs)

    def test_init(self):
        redis_client: RedisClient = RedisClient(
            host, 'lock-key-test-init', None, port)
        self.assertIsNotNone(redis_client.connection)
        self.assertIsNotNone(connection_pools[host])

    def test_set(self):
        redis_client: RedisClient = RedisClient(
            host, 'lock-key-test-init', None, port)
        self.assertIsNotNone(redis_client.connection)
        self.assertIsNotNone(connection_pools[host])
        result = redis_client.set('test_key', 'test_value')
        self.assertTrue(result)

    def test_get(self):
        redis_client: RedisClient = RedisClient(
            host, 'lock-key-test-init', None, port)
        self.assertIsNotNone(redis_client.connection)
        self.assertIsNotNone(connection_pools[host])
        result = redis_client.get('test_key')
        self.assertIsNotNone(result)
        self.assertEqual(result, b'test_value')


if __name__ == '__main__':
    unittest.main()
