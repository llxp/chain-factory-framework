from threading import Thread
from _thread import interrupt_main
from datetime import datetime
from time import sleep

from .wrapper.redis_client import RedisClient
from .common.settings import heartbeat_redis_key, heartbeat_sleep_time
from .models.redis_models import Heartbeat


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
        self.thread = Thread(target=self._heartbeat_thread)
        self.thread.start()

    def stop_heartbeat(self):
        """
        stops the heartbeat thread
        """
        self.heartbeat_running = False
        self.thread.join()

    def _current_timestamp(self):
        return datetime.utcnow()

    def _redis_key(self):
        ns = self.namespace + '_' if self.namespace else ''
        return heartbeat_redis_key + '_' + ns + self.node_name

    def _json_heartbeat(self):
        return Heartbeat(
            node_name=self.node_name,
            namespace=self.namespace,
            last_time_seen=self._current_timestamp()
        ).json()

    def _set_heartbeat(self):
        result = self._redis_client.set(
            self._redis_key(),
            self._json_heartbeat()
        )
        if not result:
            # interrupt the main thread, if the heartbeat fails
            # so that the node can be cleanly shutdown and restarted
            interrupt_main()

    def _heartbeat_thread(self):
        """
        updates a key in redis to show the current uptime of the node
        and waits a specified amount of time
        repeats as long as the node is running
        """
        while self.heartbeat_running:
            self._set_heartbeat()
            sleep(heartbeat_sleep_time)
