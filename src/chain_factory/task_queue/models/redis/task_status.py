from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
from datetime import datetime
from marshmallow import fields
import pytz


@dataclass_json
@dataclass(frozen=False)
class TaskStatus():
    task_id: str = ''
    status: str = ''
    finished_date: datetime = field(
        default_factory=lambda: datetime.now(tz=pytz.UTC),
        metadata=config(
            encoder=datetime.isoformat,
            decoder=datetime.fromisoformat,
            mm_field=fields.DateTime(format='iso')
        ))
