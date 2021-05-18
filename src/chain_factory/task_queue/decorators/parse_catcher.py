import logging
import sys
import traceback

LOGGER = logging.getLogger(__name__)


def parse_catcher(errors=(Exception, )):
    def decorator(func):
        def new_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except errors as e:
                traceback.print_exc(file=sys.stdout)
                # logger.error('Got error! ' % repr(e))
                LOGGER.exception(e)
                return None

        return new_func
    return decorator
