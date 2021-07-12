import io
import sys
import re
from typing import Optional, Union
from ..common.settings import task_log_to_stdout, task_log_to_external
from pymongo.collection import Collection
from .mongodb_client import Database
from ..models.mongo.task_log import TaskLog
import json


class BytesIOWrapper(io.BytesIO):
    def __init__(self, task_id: str, workflow_id: str, mongodb_database: Database):
        super().__init__()
        self.task_id = task_id
        self.mongodb_database = mongodb_database
        self.workflow_id = workflow_id

    def read(self, size: Optional[int] = ...) -> bytes:
        return super().read(size)

    def write(self, b: Union[bytes, bytearray]):
        if task_log_to_stdout:
            sys.__stdout__.write(b.decode('utf-8'))
        decoded_log_line = b.decode('utf-8')
        decoded_log_line = self.remove_secrets(decoded_log_line)
        task_log = TaskLog(task_id=self.task_id, log_line=decoded_log_line, workflow_id=self.workflow_id)
        logs_table: Collection = self.mongodb_database.logs
        if task_log_to_external:
            # dataclass to json and parse to dict
            logs_table.insert_one(json.loads(task_log.to_json()))
        return super().write(b)

    def remove_secrets(self, string: str) -> str:
        return re.sub('<s>(.*?)</s>', 'REDACTED', string)
