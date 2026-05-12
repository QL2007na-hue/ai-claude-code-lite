import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from runtime.event_bus import EventBus
from runtime.task_manager import TaskManager

logger = logging.getLogger("api.server")

STREAM_KEY = "ai-runtime:events"
GROUP_NAME = "api-ws-group"


class ConnectionManager:
    """管理所有 WebSocket 连接，负责广播事件。"""

    def __init__(self):
        self._connections: Dict[str, WebSocket] = {}

    async def connect(self, ws: WebSocket) -> str:
        await ws.accept()
        client_id = str(uuid.uuid4())[:8]
        self._connections[client_id] = ws
        return client_id

    def disconnect(self, client_id: str) -> None:
        self._connections.pop(client_id, None)

    @property
    def active_count(self) -> int:
        return len(self._connections)

    def active_ids(self) -> List[str]:
        return list(self._connections.keys())

    async def broadcast(self, data: dict) -> None:
        dead: List[str] = []
        for cid, ws in self._connections.items():
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(cid)
        for cid in dead:
            self.disconnect(cid)


# ═══════════════════════════════════════════════════════════════
# 生命周期管理
# ═══════════════════════════════════════════════════════════════

manager = ConnectionManager()
_broadcast_task: Optional[asyncio.Task] = None
_redis: Optional[aioredis.Redis] = None
_consumer_name: str = ""


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _broadcast_task, _redis, _consumer_name
    try:
        _redis = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
        await _ensure_ws_group(_redis)
        _consumer_name = str(uuid.uuid4())[:12]
        _broadcast_task = asyncio.create_task(_stream_listener())
        logger.info("Redis Streams 监听已启动")
    except Exception as e:
        logger.warning("Redis 不可用，WebSocket / Timeline 将降级: %s", e)
        _redis = None
    logger.info("API server started")
    yield
    if _broadcast_task:
        _broadcast_task.cancel()
    if _redis:
        await _redis.aclose()
    logger.info("API server stopped")


app = FastAPI(title="AI Runtime API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════════════════════════
# Redis Stream 后台监听线程
# ═══════════════════════════════════════════════════════════════

async def _ensure_ws_group(redis_client: aioredis.Redis) -> None:
    try:
        await redis_client.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def _stream_listener() -> None:
    """从 Redis Streams 读取新事件，广播到所有 WebSocket 客户端。"""
    while True:
        try:
            streams = await _redis.xreadgroup(
                GROUP_NAME,
                _consumer_name,
                {STREAM_KEY: ">"},
                count=10,
                block=3000,
            )
            for _, messages in streams:
                for msg_id, msg_data in messages:
                    await _redis.xack(STREAM_KEY, GROUP_NAME, msg_id)
                    await manager.broadcast({
                        "id": msg_id,
                        "task_id": msg_data.get("task_id", ""),
                        "agent": msg_data.get("agent", ""),
                        "event": msg_data.get("event", ""),
                        "payload": msg_data.get("payload", "{}"),
                        "timestamp": msg_data.get("timestamp", ""),
                    })
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(1)


# ═══════════════════════════════════════════════════════════════
# 工厂函数（延迟初始化 TaskManager / EventBus）
# ═══════════════════════════════════════════════════════════════

_tm: Optional[TaskManager] = None
_bus: Optional[EventBus] = None


def _get_tm() -> TaskManager:
    global _tm
    if _tm is None:
        _tm = TaskManager()
    return _tm


def _get_bus() -> EventBus:
    global _bus
    if _bus is None:
        _bus = EventBus()
    return _bus


# ═══════════════════════════════════════════════════════════════
# WebSocket 端点
# ═══════════════════════════════════════════════════════════════

@app.websocket("/ws/events")
async def ws_events(ws: WebSocket):
    cid = await manager.connect(ws)
    logger.info("WebSocket 连接: %s (活跃: %d)", cid, manager.active_count)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        manager.disconnect(cid)
        logger.info("WebSocket 断开: %s (活跃: %d)", cid, manager.active_count)


# ═══════════════════════════════════════════════════════════════
# REST 端点
# ═══════════════════════════════════════════════════════════════

@app.post("/tasks")
def create_task(
    agent: str = "planner",
    description: str = "",
    payload: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """创建新任务并广播 task.created 事件。"""
    tm = _get_tm()
    bus = _get_bus()

    task_payload: Any = {}
    if payload:
        try:
            task_payload = json.loads(payload)
        except json.JSONDecodeError:
            task_payload = {"raw": payload}
    if description:
        task_payload["description"] = description

    task_id = tm.create_task(agent=agent, payload=task_payload)

    bus.emit_event(task_id, agent, "task.created", task_payload)

    return {"task_id": task_id, "agent": agent, "status": "pending"}


@app.get("/tasks")
def list_tasks(
    status: Optional[str] = Query(None),
) -> List[Dict[str, Any]]:
    """列出所有任务，可选按状态过滤。"""
    return _get_tm().list_tasks(status=status)


@app.get("/tasks/{task_id}")
def get_task(task_id: str) -> Dict[str, Any]:
    """查询单个任务详情。"""
    task = _get_tm().get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return task


@app.get("/timeline/{task_id}")
async def get_timeline(task_id: str) -> List[Dict[str, Any]]:
    """查询指定任务的事件时间线（从 Redis Streams）。"""
    if _redis is None:
        return []
    events: List[Dict[str, Any]] = []
    streams = await _redis.xrange(STREAM_KEY, "-", "+", count=500)
    for msg_id, msg_data in reversed(streams):
        if msg_data.get("task_id") == task_id:
            events.append({
                "id": msg_id,
                "task_id": msg_data.get("task_id", ""),
                "agent": msg_data.get("agent", ""),
                "event": msg_data.get("event", ""),
                "payload": msg_data.get("payload", "{}"),
                "timestamp": msg_data.get("timestamp", ""),
            })
    return events


@app.get("/timeline")
async def get_full_timeline(
    limit: int = Query(100, ge=1, le=1000),
) -> List[Dict[str, Any]]:
    """查询最近 N 条事件（全量时间线）。"""
    if _redis is None:
        return []
    streams = await _redis.xrevrange(STREAM_KEY, "+", "-", count=limit)
    return [
        {
            "id": msg_id,
            "task_id": msg_data.get("task_id", ""),
            "agent": msg_data.get("agent", ""),
            "event": msg_data.get("event", ""),
            "payload": msg_data.get("payload", "{}"),
            "timestamp": msg_data.get("timestamp", ""),
        }
        for msg_id, msg_data in streams
    ]


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "ws_connections": manager.active_count,
        "ws_clients": manager.active_ids(),
    }


@app.get("/")
async def serve_ui():
    ui_file = Path(__file__).resolve().parent.parent / "ui" / "pixel_mission_control.html"
    if not ui_file.is_file():
        raise HTTPException(404, "UI not found")
    return FileResponse(str(ui_file))
