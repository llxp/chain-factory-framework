from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import List


@dataclass_json
@dataclass(frozen=False)
class WorkflowLog():
    log_lines: List[str] = ''
    workflow_id: str = ''
    task_id: str = ''
