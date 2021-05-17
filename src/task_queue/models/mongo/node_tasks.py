from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import List

from .registered_task import RegisteredTask


@dataclass_json
@dataclass(frozen=False)
class NodeTasks():
    node_name: str = ''
    tasks: List[RegisteredTask] = field(default_factory=[])
