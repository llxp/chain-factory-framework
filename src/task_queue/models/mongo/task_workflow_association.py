from dataclasses import dataclass, field
from dataclasses_json import dataclass_json

from .task import Task


@dataclass_json
@dataclass(frozen=False)
class TaskWorkflowAssociation():
    task: Task = field(default_factory=lambda: {})
    workflow_id: str = ''
    node_name: str = ''
