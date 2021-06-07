from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from dataclasses_json import dataclass_json, config
from marshmallow import fields
import pytz


def from_iso_time(time):
    if time:
        return datetime.fromisoformat(time)
    else:
        return None


def to_iso_time(time):
    if time:
        return datetime.isoformat(time)
    return ''


def from_none_array(field):
    if field is not None:
        return field
    else:
        return []


def to_none_array(field):
    if field is not None:
        return field
    else:
        return []


@dataclass_json
@dataclass(frozen=False)
class Task():
    # required, name of task to start
    name: str = ''
    # required, arguments of task to start
    arguments: Dict[str, str] = field(default_factory=lambda: {})
    # not required, will be overritten by the task_handler
    received_date: datetime = field(
        default_factory=lambda: datetime.now(tz=pytz.UTC),
        metadata=config(
            encoder=datetime.isoformat,
            decoder=datetime.fromisoformat,
            mm_field=fields.DateTime(format='iso')
        ))
    # not required, should be omitted, when starting a new task
    parent_task_id: Optional[str] = ''
    # not required, should be omitted, when starting a new task
    workflow_id: str = ''
    # not required, will be overritten by the task_handler
    task_id: str = ''
    # a list of names the task can be started on.
    # Required, can be empty.
    # If empty it will be executed on any of the nodes
    # where the task is registered
    node_names: List[str] = field(default_factory=lambda: [])
    # tags to be associated with the new task.
    # Used to query for the workflow logs
    tags: List[str] = field(
        default_factory=lambda: [],
        metadata=config(decoder=from_none_array, encoder=to_none_array)
    )
    # not required, should be omitted, when starting a new taks
    reject_counter: int = 0
    # planned date for timed tasks, can be ommited (optional)
    planned_date: datetime = field(
        default_factory=lambda: '',
        metadata=config(
            encoder=to_iso_time,
            decoder=from_iso_time,
            mm_field=fields.DateTime(format='iso')
        ))
