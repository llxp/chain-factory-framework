from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
from datetime import datetime
from marshmallow import fields
import pytz


@dataclass_json
@dataclass(frozen=False)
class Heartbeat():
    node_name: str = ''
    namespace: str = ''
    last_time_seen: datetime = field(
        default_factory=lambda: datetime.now(pytz.UTC),
        metadata=config(
            encoder=datetime.isoformat,
            decoder=datetime.fromisoformat,
            mm_field=fields.DateTime(format='iso')
        ))
