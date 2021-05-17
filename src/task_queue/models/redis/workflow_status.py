from dataclasses import dataclass
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass(frozen=False)
class WorkflowStatus():
    workflow_id: str = ''
    status: int = 0
