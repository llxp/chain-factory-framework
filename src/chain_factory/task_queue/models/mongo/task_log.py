from dataclasses import dataclass
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass(frozen=False)
class TaskLog():
    log_line: str = ''
    task_id: str = ''
    workflow_id: str = ''
