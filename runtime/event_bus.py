import json
import time
import uuid
from typing import Any, Callable, Dict, Optional

import redis


class EventBus:
    """基于 Redis Streams 的轻量级 Event Bus

    Usage:
        # 发送事件
        bus = EventBus()
        bus.emit_event(task_id="task-1", agent="planner",
                       event="task.started", payload={"goal": "..."})

        # 订阅事件
        def handle_event(data):
            print(f"收到事件: {data['event']}")

        bus.subscribe(handle_event)
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        stream_key: str = "ai-runtime:events",
        group_name: str = "ai-runtime-group",
        consumer_name: Optional[str] = None,
    ):
        self.redis_client = redis.from_url(redis_url, decode_responses=True)
        self.stream_key = stream_key
        self.group_name = group_name
        self.consumer_name = consumer_name or str(uuid.uuid4())
        self._running = False
        self._ensure_consumer_group()

    def _ensure_consumer_group(self) -> None:
        try:
            self.redis_client.xgroup_create(
                self.stream_key, self.group_name, id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def emit_event(
        self,
        task_id: str,
        agent: str,
        event: str,
        payload: Any = None,
    ) -> str:
        event_data = {
            "task_id": task_id,
            "agent": agent,
            "event": event,
            "payload": json.dumps(payload) if not isinstance(payload, str) else payload,
            "timestamp": str(time.time()),
        }
        return self.redis_client.xadd(self.stream_key, event_data)

    def subscribe(
        self,
        callback: Callable[[Dict[str, str]], None],
        block_ms: int = 5000,
    ) -> None:
        self._running = True
        while self._running:
            try:
                streams = self.redis_client.xreadgroup(
                    self.group_name,
                    self.consumer_name,
                    {self.stream_key: ">"},
                    count=1,
                    block=block_ms,
                )
                for _stream, messages in streams:
                    for msg_id, msg_data in messages:
                        callback(msg_data)
                        self.redis_client.xack(
                            self.stream_key, self.group_name, msg_id
                        )
            except Exception:
                time.sleep(1)

    def stop(self) -> None:
        self._running = False
