import logging
from typing import Any, Tuple

LOGGER = logging.getLogger(__name__)


def repeat(
    errors: Tuple[Exception] = (Exception, ),
    return_value: Any = None,
    max_counter_value: int = 10
):
    def decorator(func):
        def new_func(*args, **kwargs):
            # used to temporarily store the current error counter
            counter: int = -1
            try:
                if 'repeat_counter' in kwargs:
                    counter = kwargs['repeat_counter']
                    # delete the error counter from kwargs so,
                    # that the value is not visible
                    # in the actual decorated function
                    del kwargs['repeat_counter']
                result = func(*args, **kwargs)
                return result
            except errors as e:
                LOGGER.exception(e)
                if counter >= 0:
                    kwargs['repeat_counter'] = counter  # restore error counter
                if 'repeat_counter' in kwargs:
                    kwargs['repeat_counter'] = kwargs['repeat_counter'] + 1
                else:
                    kwargs['repeat_counter'] = 0
                if kwargs['repeat_counter'] >= 10:
                    return return_value
                return new_func(*args, **kwargs)

        return new_func
    return decorator
