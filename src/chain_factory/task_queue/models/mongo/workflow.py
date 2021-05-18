from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
from marshmallow import fields
from typing import List

from datetime import datetime


@dataclass_json
@dataclass(frozen=False)
class Workflow():
    created_date: datetime = field(
        default_factory=lambda: datetime.utcnow(),
        metadata=config(
            encoder=datetime.isoformat,
            decoder=datetime.fromisoformat,
            mm_field=fields.DateTime(format='iso')
        ))
    workflow_id: str = ''
    node_name: str = ''
    tags: List[str] = field(default_factory=lambda: [])
