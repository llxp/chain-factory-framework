import unittest
import logging

from src.task_queue.decorators.repeat import repeat


class RepeatDecoratorTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(RepeatDecoratorTest, self).__init__(*args, **kwargs)

    @repeat()
    def repeat_test(self, *args, **kwargs):
        self.assertNotIn(kwargs, 'repeat_counter')
        self.assertIn(kwargs, 'argument')
        self.assertIn(args, 123)
        raise Exception

    def test_decorator_repeat(self):
        result = self.repeat_test(123, argument='123')
        self.assertIsNone(result)
