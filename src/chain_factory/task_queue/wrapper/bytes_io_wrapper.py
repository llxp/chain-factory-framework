from io import BytesIO
from sys import __stdout__
from re import sub
from typing import Optional, Union
from odmantic import AIOEngine

from ..common.settings import task_log_to_stdout, task_log_to_external
from ..models.mongodb_models import TaskLog


class BytesIOWrapper(BytesIO):
    def __init__(
        self,
        task_id: str,
        workflow_id: str,
        mongodb_database: AIOEngine
    ):
        super().__init__()
        self.task_id = task_id
        self.mongodb_database = mongodb_database
        self.workflow_id = workflow_id

    def read(self, size: Optional[int] = ...) -> bytes:
        return super().read(size)

    async def write(self, b: Union[bytes, bytearray]):
        if task_log_to_stdout:
            __stdout__.write(b.decode('utf-8'))
        decoded_log_line = b.decode('utf-8')
        decoded_log_line = self.remove_secrets(decoded_log_line)
        task_log = TaskLog(
            task_id=self.task_id,
            log_line=decoded_log_line,
            workflow_id=self.workflow_id
        )
        if task_log_to_external:
            # dataclass to json and parse to dict
            self.mongodb_database.save(task_log)
        return super().write(b)

    def remove_secrets(self, string: str) -> str:
        return sub('<s>(.*?)</s>', 'REDACTED', string)
