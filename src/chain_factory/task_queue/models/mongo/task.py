from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from dataclasses_json import dataclass_json, config
from marshmallow import fields
import pytz

from ...common.generate_random_id import generate_random_id
from ...wrapper.redis_client import RedisClient
from ...wrapper.mongodb_client import MongoDBClient


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
        )
    )

    def workflow_precheck(self):
        return (
            len(self.parent_task_id) <= 0 and
            len(self.workflow_id) <= 0
        )

    def is_stopped(self, namespace: str, mongodb_client: MongoDBClient):
        db = mongodb_client.db()
        col = db.workflow_status
        workflow_status = col.find_one({
            'workflow_id': self.workflow_id,
            'namespace': namespace
        })
        if workflow_status:
            return True
        return False

    def generate_workflow_id(self):
        self.workflow_id = generate_random_id()

    def is_planned_task(self):
        return self.planned_date

    def increase_rejected(self):
        self.reject_counter = self.reject_counter + 1

    def check_node_filter(self, node_name: str):
        return (
            len(self.node_names) > 0 and
            node_name not in self.node_names
        )

    def generate_task_id(self):
        self.task_id = generate_random_id()

    def update_time(self):
        self.received_date = datetime.now(tz=pytz.UTC)

    def set_as_parent_task(self):
        self.parent_task_id = self.task_id

    def set_parent_task(self, other_task: 'Task'):
        self.parent_task_id = other_task.task_id
        self.workflow_id = other_task.workflow_id
        self.node_names = other_task.node_names

    def has_parent_task(self):
        return len(self.parent_task_id) > 0

    def cleanup_task(self):
        del self.task_id
