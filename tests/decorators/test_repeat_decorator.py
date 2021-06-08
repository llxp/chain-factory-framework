import unittest
import logging
import sys
sys.path.append('C:\\Users\\F21987975ADM\\chain-factory\\framework\\src')

from chain_factory.task_queue.decorators.repeat import repeat


class RepeatDecoratorTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

    @repeat((KeyError, ))
    def repeat_test(self, *args, **kwargs):
        self.assertNotIn('repeat_counter', kwargs)
        self.assertIn('argument', kwargs)
        self.assertIn(123, args)
        raise KeyError

    def test_decorator_repeat(self):
        result = self.repeat_test(123, argument='123')
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
