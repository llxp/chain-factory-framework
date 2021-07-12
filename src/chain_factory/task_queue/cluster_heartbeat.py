from _thread import start_new_thread
from threading import Thread
from datetime import datetime
import time
import pytz

from .wrapper.redis_client import RedisClient
from .common.settings import heartbeat_redis_key, heartbeat_sleep_time
from .models.redis.heartbeat import Heartbeat


class ClusterHeartbeat():
    def __init__(
        self,
        namespace: str,
        node_name: str,
        redis_client: RedisClient
    ):
        self._redis_client = redis_client
        self.node_name = node_name
        self.namespace = namespace
        self.heartbeat_running = False

    def start_heartbeat(self):
        """
        starts the heartbeat thread
        """
        self.heartbeat_running = True
        # start_new_thread(self.heartbeat_thread, ())
        self.thread = Thread(target=self._heartbeat_thread)
        self.thread.start()

    def stop_heartbeat(self):
        """
        stops the heartbeat thread
        """
        self.heartbeat_running = False
        self.thread.join()

    def _current_timestamp(self):
        return datetime.now(pytz.UTC)

    def _redis_key(self):
        return \
            heartbeat_redis_key + '_' + \
            self.namespace + '_' + \
            self.node_name

    def _json_heartbeat(self):
        return Heartbeat(
            node_name=self.node_name,
            namespace=self.namespace,
            last_time_seen=self._current_timestamp()
        ).to_json()

    def _set_heartbeat(self):
        self._redis_client.set(
            self._redis_key(),
            self._json_heartbeat()
        )

    def _heartbeat_thread(self):
        """
        updates a key in redis to show the current uptime of the node
        and waits a specified amount of time
        repeats as long as the node is running
        """
        while self.heartbeat_running:
            self._set_heartbeat()
            time.sleep(heartbeat_sleep_time)
