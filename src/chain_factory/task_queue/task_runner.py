from typing import Dict, Any, Union
import json
import logging
import traceback
import sys
import time
import stdio_proxy
import io
import threading

from .models.mongo.task import Task
from .models.redis.task_control_message import TaskControlMessage
from .wrapper.interruptable_thread import InterruptableThread
from .wrapper.interruptable_thread import ThreadAbortException
from .wrapper.redis_client import RedisClient
from .common.task_return_type import \
    ArgumentType, CallbackType, \
    TaskRunnerReturnType, TaskReturnType, \
    NormalizedTaskReturnType

LOGGER = logging.getLogger(__name__)


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
        self.result: TaskRunnerReturnType = None
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
        with \
            stdio_proxy.redirect_stdout(self.buffer), \
                stdio_proxy.redirect_stderr(self.buffer):
            try:
                self.status = 1
                self.result = self.callback(**self.arguments)
                self.status = 2
            # catch ThreadAbortException,
            # will be raised if the thread should be forcefully aborted
            except ThreadAbortException as e:
                LOGGER.exception(e)
                self.result = None
                self.status = 3
                return
            # catch all exceptions to prevent a crash of the node
            except Exception as e:
                LOGGER.exception(e)
                traceback.print_exc(file=sys.stdout)
                self.result = None
                self.status = 2
                return

    def stop(self):
        self.status = 3
        super().interrupt()
        super().exit()

    def abort(self):
        self.status = 4
        super().abort()


class TaskControlThread(InterruptableThread):
    def __init__(
        self,
        workflow_id: str,
        task_thread: TaskThread,
        redis_client: RedisClient
    ):
        InterruptableThread.__init__(self)
        self.workflow_id = workflow_id
        self.task_thread = task_thread
        self.redis_client = redis_client

    def run(self):
        try:
            self.redis_client.subscribe('task_control_channel')
            while True:
                msg = self.redis_client.get_message()
                if msg is not None:
                    if self._control_task_thread_handle_channel(msg):
                        break
                time.sleep(0.001)
        except ThreadAbortException:
            pass

    def _control_task_thread_handle_channel(
        self,
        msg: Union[None, Dict]
    ):
        try:
            if self._control_task_thread_handle_data(msg):
                return True
        except Exception as e:
            LOGGER.exception(e)
            traceback.print_exc(file=sys.stdout)
            return True
        return False

    def _control_task_thread_handle_data(
        self,
        msg: Union[None, Dict]
    ):
        data = msg['data']
        if type(data) == bytes:
            decoded_data = data.decode('utf-8')
            parsed_data = TaskControlMessage.from_json(decoded_data)
            if (
                parsed_data.workflow_id == self.workflow_id and
                parsed_data.command == 'stop'
            ):
                self.task_thread.stop()
                return True
            if (
                parsed_data.workflow_id == self.workflow_id and
                parsed_data.command == 'abort'
            ):
                self.task_thread.abort()
                return True
        return False


class TaskRunner():
    lock = threading.Lock()

    def __init__(
        self,
        name: str,
        callback: CallbackType
    ):
        self.callback: CallbackType = callback
        self.name: str = name
        self.task_threads: Dict[str, TaskThread] = {}

    def set_redis_client(self, redis_client: RedisClient):
        self.redis_client = redis_client

    def run(
        self,
        arguments: Dict[str, str],
        workflow_id: str,
        buffer: io.BytesIO
    ) -> TaskRunnerReturnType:
        try:
            LOGGER.debug(
                'running task function %s '
                'with arguments %s' % (self.name, json.dumps(arguments), ))
            print(workflow_id)
            if arguments is None:
                arguments = {}
            # self.convert_arguments could raise a TypeError
            arguments = self.convert_arguments(arguments)
            with TaskRunner.lock:
                self.task_threads[workflow_id] = self._create_task_thread(
                    arguments,
                    buffer
                )
            # start the task
            with TaskRunner.lock:
                self.task_threads[workflow_id].start()
            # start redis subscribe watcher
            try:
                task_control_thread = TaskControlThread(
                    workflow_id,
                    self.task_threads[workflow_id],
                    self.redis_client
                )
                task_control_thread.start()
                self._control_task_thread(workflow_id)
                # task_control_thread.abort()
            except ThreadAbortException:
                pass
            if self.task_threads[workflow_id].status == 2:
                # wait for task thread to normally exit
                self.task_threads[workflow_id].join()
            print('task finished')
            with TaskRunner.lock:
                task_result = self.task_threads[workflow_id].result
            # parse the result to correctly return a result with arguments
            return TaskRunner._parse_task_output(task_result, arguments)
        except TypeError as e:
            LOGGER.exception(e)
            traceback.print_exc(file=sys.stdout)
            return None

    def _create_task_thread(
        self,
        arguments: Dict[str, str],
        buffer: io.BytesIO
    ):
        return TaskThread(
            self.callback,
            arguments,
            buffer
        )

    def _task_finished(self, workflow_id: str):
        return self.task_threads[workflow_id].status in [2, 3, 4]

    def _control_task_thread(self, workflow_id: str):
        """
        Subscribe to a redis key and listen for control messages
        if there is a 'stop' control message, the thread will be aborted
            => but only, if the 'stop' message
            arrives during the thread's runtime
        """
        while True:
            # check, if exited, stopped or aborted
            if self._task_finished(workflow_id):
                break
            time.sleep(0.001)

    @staticmethod
    def _parse_task_output(
        task_result: TaskReturnType,
        arguments: Dict[str, str]
    ) -> NormalizedTaskReturnType:
        """
        Check, if new parameters have been returned,
        to be able to reschedule the same task with changed parameters
        Returns the result of the task and the arguments,
        either the default arguments or the newly returned arguments
        """
        # tuple means parameters have been returned too
        # result[0] can either be a new task, False or None
        # result[1] is the arguments dict
        if isinstance(task_result, tuple):
            # check, if the first object returned is not None
            # result can be either a new task or False
            if (
                # False, means an error occured
                task_result[0] is False or
                # check for Task type
                isinstance(task_result[0], Task) or
                # check for string type
                isinstance(task_result[0], str) or
                # check for function type
                callable(task_result[0])
            ):
                arguments = task_result[1]
            # override the result to the real result
            # as either Task, False or None
            task_result = task_result[0]
        return task_result, arguments

    def convert_arguments(self, arguments: ArgumentType) -> Dict[str, Any]:
        callback_arguments = list(self.callback.__code__.co_varnames)
        callback_types = self.callback.__annotations__
        for argument in arguments:
            if argument in callback_arguments and argument in callback_types:
                if (
                    callback_types[argument] == int and
                    type(arguments[argument]) == str and
                    len(arguments[argument]) > 0
                ):
                    try:
                        arguments[argument] = int(arguments[argument])
                    except Exception as e:
                        print(e)
        return arguments

    def abort(self, workflow_id: str):
        self.task_threads[workflow_id].abort()

    def stop(self, workflow_id: str):
        self.task_threads[workflow_id].stop()
