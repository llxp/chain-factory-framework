from _thread import start_new_thread
from datetime import datetime
import time
import pytz

from .wrapper.redis_client import RedisClient
from .common.settings import heartbeat_redis_key, heartbeat_sleep_time
from .models.redis.heartbeat import Heartbeat


class ClusterHeartbeat():
    def __init__(
        self,
        node_name: str,
        redis_client: RedisClient
    ):
        self._redis_client = redis_client
        self.node_name = node_name
        self.heartbeat_running = False

    def start_heartbeat(self):
        """
        starts the heartbeat thread
        """
        self.heartbeat_running = True
        start_new_thread(self.heartbeat_thread, ())

    def stop_heartbeat(self):
        """
        stops the heartbeat thread
        """
        self.heartbeat_running = False

    def heartbeat_thread(self):
        """
        updates a key in redis to show the current uptime of the node
        and waits a specified amount of time
        repeats as long as the node is running
        """
        while self.heartbeat_running:
            current_timestamp: datetime = datetime.now(pytz.UTC)
            redis_key = heartbeat_redis_key + '_' + self.node_name
            self._redis_client.set(
                redis_key,
                Heartbeat(
                    node_name=self.node_name,
                    last_time_seen=current_timestamp
                ).to_json()
            )
            time.sleep(heartbeat_sleep_time)
