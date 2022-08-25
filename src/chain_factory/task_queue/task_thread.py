from .wrapper.interruptable_thread import (
  InterruptableThread, ThreadAbortException
)
from stdio_proxy import redirect_stdout, redirect_stderr
from asyncio import run as run_asyncio
from logging import exception
from traceback import print_exc
from sys import stdout


class TaskThread(InterruptableThread):
    """
    The thread which actually runs the task
    the output of stdio will be redirected to a buffer
    and later uploaded to the mongodb database
    """

    def __init__(self, callback, arguments, buffer):
        InterruptableThread.__init__(self)
        self.callback = callback
        self.arguments = arguments
        # self.result: TaskRunnerReturnType = None
        # current task status
        # 0 means not run
        # 1 means started
        # 2 means finished
        # 3 means stopped
        # 4 means aborted
        self.status = 0
        self.buffer = buffer

    def run(self):
        # redirect stdout and stderr to the buffer
        with redirect_stdout(self.buffer), redirect_stderr(self.buffer):
            try:
                self.status = 1
                self.result = run_asyncio(self.callback(**self.arguments))
                self.status = 2
            # catch ThreadAbortException,
            # will be raised if the thread should be forcefully aborted
            except ThreadAbortException as e:
                exception(e)
                self.result = ThreadAbortException
                self.status = 3
                return
            # catch all exceptions to prevent a crash of the node
            except Exception as e:
                exception(e)
                print_exc(file=stdout)
                self.result = Exception
                self.status = 2
                return

    def stop(self):
        self.status = 3
        self.result = KeyboardInterrupt
        super().interrupt()
        super().exit()

    def abort(self):
        self.result = ThreadAbortException
        self.status = 4
        super().abort()

    def abort_timeout(self):
        self.result = TimeoutError
        self.status = 5
        super().abort()
