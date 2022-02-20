from typing import Callable, Dict, Any, Union
from json import dumps
from logging import error, info, debug, exception, warning
from traceback import print_exc
from sys import stdout
from time import sleep, time
from stdio_proxy import redirect_stdout, redirect_stderr
from io import BytesIO
from threading import Lock
from asyncio import run as run_asyncio

from .models.mongodb_models import Task
from .models.redis_models import TaskControlMessage
from .wrapper.interruptable_thread import (
    InterruptableThread, ThreadAbortException
)
from .wrapper.redis_client import RedisClient
from .common.task_return_type import \
    ArgumentType, CallbackType, \
    TaskRunnerReturnType, TaskReturnType, \
    NormalizedTaskReturnType
from .common.settings import \
    task_control_channel_redis_key


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
        with \
            redirect_stdout(self.buffer), \
                redirect_stderr(self.buffer):
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


class ControlThread(InterruptableThread):
    def __init__(
        self,
        workflow_id: str,
        control_actions: Dict[str, Callable],
        redis_client: RedisClient,
        control_channel: str,
        thread_name: str = ''
    ):
        InterruptableThread.__init__(self)
        self.workflow_id = workflow_id
        self.control_actions = control_actions
        self.redis_client = redis_client
        self.control_channel = control_channel
        self.run_thread = True
        self.thread_name = thread_name
        info(self.control_channel)

    def stop(self):
        self.run_thread = False

    def run(self):
        try:
            self.redis_client.subscribe(self.control_channel)
            while self.run_thread:
                msg = self.redis_client.get_message()
                if msg is not None:
                    info(msg)
                    if self._control_task_thread_handle_channel(msg):
                        break
                sleep(0.001)
            self.redis_client._pubsub_connection.unsubscribe(
                self.control_channel)
        except ThreadAbortException:
            return

    def _control_task_thread_handle_channel(
        self,
        msg: Union[None, Dict]
    ):
        try:
            if self._control_task_thread_handle_data(msg):
                return True
        except Exception as e:
            exception(e)
            print_exc(file=stdout)
            return True
        return False

    def _control_task_thread_handle_data(
        self,
        msg: Union[None, Dict]
    ):
        data = msg['data']
        if type(data) == bytes:
            decoded_data = data.decode('utf-8')
            parsed_data = TaskControlMessage.parse_raw(decoded_data)
            debug(parsed_data)
            if parsed_data.workflow_id == self.workflow_id:
                for command in self.control_actions:
                    if parsed_data.command == command:
                        debug('executing command', command)
                        self.control_actions[command]()
                        return True
        return False


class TaskControlThread(ControlThread):
    def __init__(
        self,
        workflow_id: str,
        task_thread: TaskThread,
        redis_client: RedisClient,
        namespace: str
    ):
        self.task_thread = task_thread

        def stop():
            warning('stopping task')
            self.task_thread.stop()

        def abort():
            warning('aborting task')
            self.task_thread.abort()
        control_channel = (
            ((namespace + '_') if namespace else '') +
            task_control_channel_redis_key
        )
        ControlThread.__init__(
            self,
            workflow_id=workflow_id,
            control_actions={
                'stop': stop,
                'abort': self.task_thread.abort
            },
            redis_client=redis_client,
            control_channel=control_channel,
            thread_name='TaskControlThread'
        )


class TaskRunner():
    lock = Lock()

    def __init__(
        self,
        name: str,
        callback: CallbackType,
        namespace: str
    ):
        self.callback: CallbackType = callback
        self.name: str = name
        self.task_threads: Dict[str, TaskThread] = {}
        self.task_timeout = None
        self.task_repeat_on_timeout = False
        self.namespace = namespace

    def set_redis_client(self, redis_client: RedisClient):
        self.redis_client = redis_client

    def running_workflows(self):
        return self.task_threads

    async def run(
        self,
        arguments: Dict[str, str],
        workflow_id: str,
        buffer: BytesIO
    ) -> TaskRunnerReturnType:
        try:
            debug(
                f"running task function {self.name} "
                f"with arguments {dumps(arguments)}"
            )
            info(workflow_id)
            if arguments is None:
                arguments = dict()
            # self.convert_arguments could raise a TypeError
            arguments = self.convert_arguments(arguments)
            with TaskRunner.lock:
                self.task_threads[workflow_id] = self._create_task_thread(
                    arguments, buffer)
            # start the task
            with TaskRunner.lock:
                self.task_threads[workflow_id].start()
            # start redis subscribe watcher
            try:
                task_control_thread = TaskControlThread(
                    workflow_id,
                    self.task_threads[workflow_id],
                    self.redis_client,
                    self.namespace
                )
                task_control_thread.start()
                self._control_task_thread(workflow_id)
                # task_control_thread.abort()
            except ThreadAbortException:
                pass
            if self.task_threads[workflow_id].status == 2:
                # wait for task thread to normally exit
                self.task_threads[workflow_id].join()
                info('task finished')
            else:
                warning('task aborted or stopped')
            with TaskRunner.lock:
                task_result = self.task_threads[workflow_id].result
                del self.task_threads[workflow_id]
            # parse the result to correctly return a result with arguments
            return TaskRunner._parse_task_output(task_result, arguments)
        except TypeError as e:
            exception(e)
            print_exc(file=stdout)
            del self.task_threads[workflow_id]
            return None

    def _create_task_thread(
        self,
        arguments: Dict[str, str],
        buffer: BytesIO
    ):
        return TaskThread(
            self.callback,
            arguments,
            buffer
        )

    def _task_finished(self, workflow_id: str):
        return self.task_threads[workflow_id].status in [2, 3, 4, 5]

    def _control_task_thread(self, workflow_id: str):
        """
        Subscribe to a redis key and listen for control messages
        if there is a 'stop' control message, the thread will be aborted
            => but only, if the 'stop' message
            arrives during the thread's runtime
        """
        start_time = time()
        while True:
            current_time = time()
            elapsed_time = current_time - start_time
            if (
                self.task_timeout is not None and
                elapsed_time > self.task_timeout
            ):
                self.task_threads[workflow_id].abort_timeout()
                break
            # check, if exited, stopped or aborted
            if self._task_finished(workflow_id):
                break
            sleep(0.001)

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
                        error(e)
        return arguments

    def abort(self, workflow_id: str):
        self.task_threads[workflow_id].abort()

    def stop(self, workflow_id: str):
        self.task_threads[workflow_id].stop()
