from dataclasses import dataclass
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass(frozen=False)
class TaskControlMessage():
    workflow_id: str = ''
    command: str = ''
