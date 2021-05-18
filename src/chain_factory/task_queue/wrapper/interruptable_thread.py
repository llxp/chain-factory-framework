"""
Source:
https://stackoverflow.com/questions/14482230/why-does-the-python-threading
-thread-object-has-start-but-not-stop?noredirect=1&lq=1
"""

import _thread
import ctypes as _ctypes
import threading as _threading

_PyThreadState_SetAsyncExc = _ctypes.pythonapi.PyThreadState_SetAsyncExc
# noinspection SpellCheckingInspection
_PyThreadState_SetAsyncExc.argtypes = _ctypes.c_ulong, _ctypes.py_object
_PyThreadState_SetAsyncExc.restype = _ctypes.c_int

# noinspection PyUnreachableCode
if __debug__:
    # noinspection PyShadowingBuiltins
    def _set_async_exc(id, exc):
        if not isinstance(id, int):
            raise TypeError(f'{id!r} not an int instance')
        if not isinstance(exc, type):
            raise TypeError(f'{exc!r} not a type instance')
        if not issubclass(exc, BaseException):
            raise SystemError(f'{exc!r} not a BaseException subclass')
        return _PyThreadState_SetAsyncExc(id, exc)
else:
    _set_async_exc = _PyThreadState_SetAsyncExc


# noinspection PyShadowingBuiltins
def set_async_exc(id, exc, *args):
    if args:
        class StateInfo(exc):
            def __init__(self):
                super().__init__(*args)

        return _set_async_exc(id, StateInfo)
    return _set_async_exc(id, exc)


def interrupt(ident=None):
    if ident is None:
        _thread.interrupt_main()
    else:
        set_async_exc(ident, KeyboardInterrupt)


# noinspection PyShadowingBuiltins
def exit(ident=None):
    if ident is None:
        _thread.exit()
    else:
        set_async_exc(ident, SystemExit)


class ThreadAbortException(SystemExit):
    pass


class InterruptableThread(_threading.Thread):
    def __init__(self):
        _threading.Thread.__init__(self)

    def set_async_exc(self, exc, *args):
        return set_async_exc(self.ident, exc, *args)

    def interrupt(self):
        self.set_async_exc(KeyboardInterrupt)

    def exit(self):
        self.set_async_exc(SystemExit)

    def abort(self, *args):
        self.set_async_exc(ThreadAbortException, *args)
