from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import Dict


@dataclass_json
@dataclass(frozen=False)
class RegisteredTask():
    name: str = ''
    arguments: Dict[str, str] = field(default_factory=lambda: {})
