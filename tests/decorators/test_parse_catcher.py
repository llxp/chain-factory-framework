import unittest
from src.task_queue.decorators.parse_catcher import parse_catcher


class ParseCatcherDecoratorTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ParseCatcherDecoratorTest, self).__init__(*args, **kwargs)

    def test_parse_catcher(self):
        try:
            @parse_catcher(errors=(ValueError))
            def test_func():
                raise ValueError('Error during parsing')

            test_func()
        except ValueError:
            assert False
            return
        assert True
