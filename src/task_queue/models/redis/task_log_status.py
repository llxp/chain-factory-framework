from dataclasses import dataclass
from dataclasses_json import dataclass_json
from dataclass_dict import DataclassDict


@dataclass_json
@dataclass(frozen=False)
class TaskLogStatus(DataclassDict):
    task_id: str = ''
    status: int = 0
