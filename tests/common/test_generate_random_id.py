import unittest
from src.task_queue.common.generate_random_id import generate_random_id


class GenerateRandomIdTest(unittest.TestCase):
    def test_generate_random_id(self):
        rnd_str = ''
        for i in range(0, 100000):
            rnd_str_new = generate_random_id()
            assert rnd_str_new != rnd_str
            rnd_str = rnd_str_new
