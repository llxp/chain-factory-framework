from dataclasses_json import dataclass_json
from dataclasses import dataclass


@dataclass_json
@dataclass(frozen=False)
class Namespace():
    namespace: str = ''
