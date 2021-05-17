from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import List


@dataclass_json
@dataclass(frozen=False)
class ListItem():
    name: str = ''
    content: str = ''


@dataclass_json
@dataclass(frozen=False)
class ListItemContainer():
    list_items: List[ListItem] = field(default_factory=lambda: [])
