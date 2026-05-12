import json
import logging
import threading
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from runtime.event_bus import EventBus
from runtime.task_manager import TaskManager

logger = logging.getLogger("runtime.orchestrator")


class OrchestratorBus(EventBus):
    """EventBus 子类：修正 subscribe() 的异常吞噬问题。

    原版 EventBus.subscribe() 中 except Exception 会吞掉 StopIteration，
    导致 PluginLoader / Orchestrator 的停止机制失效。
    此子类将 StopIteration 和 KeyboardInterrupt 重新抛出。
    """

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
                    count=10,
                    block=block_ms,
                )
                for _stream, messages in streams:
                    for msg_id, msg_data in messages:
                        callback(msg_data)
                        self.redis_client.xack(
                            self.stream_key, self.group_name, msg_id
                        )
            except (StopIteration, KeyboardInterrupt):
                raise
            except Exception:
                if not self._running:
                    break
                time.sleep(1)


@dataclass
class DagNode:
    task_id: str
    title: str
    depends_on: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)
    status: str = "pending"

    @property
    def ready(self) -> bool:
        return self.status == "pending" and not self.depends_on


class DAG:
    def __init__(self, root_id: str):
        self.root_id = root_id
        self.nodes: Dict[str, DagNode] = {}
        self.root_done = False

    def add(self, task_id: str, title: str, depends_on: List[str]) -> DagNode:
        node = DagNode(task_id=task_id, title=title, depends_on=list(depends_on))
        self.nodes[task_id] = node
        for dep_id in depends_on:
            if dep_id in self.nodes:
                self.nodes[dep_id].dependents.append(task_id)
        return node

    def mark_done(self, task_id: str) -> List[str]:
        if task_id not in self.nodes:
            return []
        self.nodes[task_id].status = "done"
        unblocked: List[str] = []
        for dep_id in self.nodes[task_id].dependents:
            dep_node = self.nodes.get(dep_id)
            if dep_node and dep_node.status == "pending":
                dep_node.depends_on = [d for d in dep_node.depends_on if d != task_id]
                if dep_node.ready:
                    unblocked.append(dep_id)
        if task_id == self.root_id:
            self.root_done = True
        return unblocked

    def mark_failed(self, task_id: str) -> None:
        if task_id in self.nodes:
            self.nodes[task_id].status = "failed"
        if task_id == self.root_id:
            self.root_done = True

    def mark_running(self, task_id: str) -> None:
        if task_id in self.nodes:
            self.nodes[task_id].status = "running"

    def ready_nodes(self) -> List[str]:
        return [nid for nid, n in self.nodes.items() if n.ready]

    def all_done(self) -> bool:
        if self.root_done and self.nodes:
            return all(n.status in ("done", "failed") for n in self.nodes.values())
        return self.root_done and not self.nodes

    @property
    def total(self) -> int:
        return len(self.nodes)

    @property
    def done_count(self) -> int:
        return sum(1 for n in self.nodes.values() if n.status == "done")

    @property
    def failed_count(self) -> int:
        return sum(1 for n in self.nodes.values() if n.status == "failed")


class Orchestrator:
    """AI Runtime 自动编排引擎。

    监听 EventBus 事件，自动串联 Planner → CoderAgent → Reviewer 流水线，
    支持 DAG 依赖任务图、并发编码、review 失败自动重试。

    Usage:
        orchestrator = Orchestrator(
            event_bus=EventBus(),
            task_manager=TaskManager(),
            workspace_mgr=WorkspaceManager(),
            api_key="sk-xxx",
        )
        orchestrator.start()          # 后台线程开始监听事件
        orchestrator.submit("写一个贪吃蛇")  # 提交任务，自动走完整流水线
        orchestrator.stop()           # 优雅停止
    """

    MAX_RETRIES = 3

    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        task_manager: Optional[TaskManager] = None,
        workspace_mgr=None,
        api_key: Optional[str] = None,
        model: str = "deepseek-chat",
        max_workers: int = 4,
    ):
        import os as _os
        self.tm = task_manager or TaskManager()
        self.wm = workspace_mgr
        self.api_key = api_key or _os.environ.get("DEEPSEEK_API_KEY", "")
        self.model = model
        self.max_workers = max_workers

        # EventBus — 使用修正子类避免 StopIteration 被吞
        self.bus = event_bus or OrchestratorBus(group_name="orchestrator-group")
        self._own_bus = event_bus is None

        # 状态
        self._running = False
        self._listen_thread: Optional[threading.Thread] = None
        self._dag_by_root: Dict[str, DAG] = {}
        self._retry_count: Dict[str, int] = defaultdict(int)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

        # 延迟导入 Agent（避免循环依赖 + 启动开销）
        self._planner = None
        self._coder = None
        self._reviewer = None

        # Thread safety
        self._lock = threading.Lock()

    @property
    def planner(self):
        if self._planner is None:
            from agents.planner import Planner
            self._planner = Planner(self.bus, self.tm, api_key=self.api_key, model=self.model)
        return self._planner

    @property
    def coder(self):
        if self._coder is None:
            from agents.coder import CoderAgent
            self._coder = CoderAgent(self.tm, self.bus, self.wm, api_key=self.api_key, model=self.model)
        return self._coder

    @property
    def reviewer(self):
        if self._reviewer is None:
            from agents.reviewer import Reviewer
            self._reviewer = Reviewer(self.bus, self.tm, self.wm)
        return self._reviewer

    # ── 生命周期 ────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._listen_thread = threading.Thread(
            target=self._event_loop, daemon=True, name="orchestrator-listener"
        )
        self._listen_thread.start()
        logger.info("Orchestrator 已启动 (max_workers=%d)", self.max_workers)

    def stop(self) -> None:
        self._running = False
        if self._listen_thread and self._listen_thread.is_alive():
            self._listen_thread.join(timeout=10)
        self._executor.shutdown(wait=False)
        if self._own_bus:
            self.bus.stop()
        logger.info("Orchestrator 已停止")

    # ── 任务提交 ────────────────────────────────────────────

    def submit(
        self,
        goal: str,
        root_id: Optional[str] = None,
        run_async: bool = True,
    ) -> str:
        import uuid as _uuid
        root_id = root_id or str(_uuid.uuid4())
        logger.info("提交任务: %s → %s", root_id, goal)

        payload = {"goal": goal, "description": goal}
        self.tm.create_task(agent="planner", payload=payload, task_id=root_id)
        self.bus.emit_event(root_id, "orchestrator", "task.created", {"goal": goal})

        if run_async:
            self._executor.submit(self._run_plan, root_id, goal)
        return root_id

    # ── 事件循环 ────────────────────────────────────────────

    def _event_loop(self) -> None:
        bus = self.bus if not self._own_bus else OrchestratorBus(group_name="orchestrator-group")

        def handler(data: Dict[str, str]):
            if not self._running:
                raise StopIteration()
            self._dispatch(data)

        try:
            bus.subscribe(handler)
        except StopIteration:
            pass
        except Exception:
            if self._running:
                logger.exception("Orchestrator 事件监听异常")

    # ── 事件分发 ────────────────────────────────────────────

    def _dispatch(self, data: Dict[str, str]) -> None:
        task_id = data.get("task_id", "")
        event = data.get("event", "")
        agent = data.get("agent", "")
        raw_payload = data.get("payload", "{}")

        payload: Any = {}
        try:
            payload = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
        except (json.JSONDecodeError, TypeError):
            payload = {"raw": raw_payload}

        logger.debug("[dispatch] %s | %s | %s", event, agent, task_id)

        # 路由表
        if event == "task.created":
            self._on_task_created(task_id, payload)
        elif event == "task.planned":
            self._on_task_planned(task_id, payload)
        elif event == "subtask.created":
            self._on_subtask_created(task_id, payload)
        elif event == "task.coding_completed":
            self._on_coding_completed(task_id)
        elif event == "task.coding_failed":
            self._on_coding_failed(task_id)
        elif event == "task.review_approved":
            self._on_review_approved(task_id)
        elif event == "task.review_rejected":
            self._on_review_rejected(task_id)

    # ── 事件处理器 ──────────────────────────────────────────

    def _on_task_created(self, task_id: str, payload: dict) -> None:
        goal = payload.get("goal", payload.get("description", ""))
        if not goal:
            return
        self._executor.submit(self._run_plan, task_id, goal)

    def _run_plan(self, root_id: str, goal: str) -> None:
        try:
            result = self.planner.plan(goal, task_id=root_id)
            subtasks = result.get("subtasks", [])
            if not subtasks:
                logger.warning("Plan 未产生子任务，将 root 标记为 done")
                self.tm.update_task(root_id, status="done")
                self.bus.emit_event(root_id, "orchestrator", "task.done_no_subtasks", {})
                return

            dag = DAG(root_id)
            for st in subtasks:
                dag.add(st["id"], st.get("title", ""), st.get("depends_on", []))
            with self._lock:
                self._dag_by_root[root_id] = dag
            logger.info("DAG 构建完成: root=%s nodes=%d", root_id, dag.total)

            # 启动所有就绪节点
            ready = dag.ready_nodes()
            logger.info("初始就绪节点: %s", ready)
            for nid in ready:
                with self._lock:
                    dag.mark_running(nid)
                self._executor.submit(self._run_code, nid)

        except Exception:
            logger.exception("Planner 执行失败: root=%s", root_id)
            self.tm.update_task(root_id, status="failed", result={"error": traceback.format_exc()})
            self.bus.emit_event(root_id, "orchestrator", "task.plan_failed", {})

    def _on_task_planned(self, task_id: str, payload: dict) -> None:
        pass

    def _on_subtask_created(self, subtask_id: str, payload: dict) -> None:
        parent_id = payload.get("parent_task_id", "")
        with self._lock:
            dag = self._dag_by_root.get(parent_id)
        if dag and subtask_id in dag.nodes:
            if dag.nodes[subtask_id].ready:
                with self._lock:
                    dag.mark_running(subtask_id)
                self._executor.submit(self._run_code, subtask_id)

    def _run_code(self, task_id: str) -> None:
        try:
            logger.info("Coder 开始: %s", task_id)
            success = self.coder.execute(task_id)
            if not success:
                logger.warning("Coder 执行失败: %s", task_id)
        except Exception:
            logger.exception("Coder 异常: %s", task_id)
            self.tm.update_task(task_id, status="failed")
            self.bus.emit_event(task_id, "orchestrator", "task.coding_failed",
                               {"error": traceback.format_exc()})

    def _on_coding_completed(self, task_id: str) -> None:
        self._executor.submit(self._run_review, task_id)

    def _run_review(self, task_id: str) -> None:
        try:
            logger.info("Reviewer 开始: %s", task_id)
            self.reviewer.review(task_id)
        except Exception:
            logger.exception("Reviewer 异常: %s", task_id)
            self.tm.update_task(task_id, status="failed")
            self.bus.emit_event(task_id, "orchestrator", "task.review_failed",
                               {"error": traceback.format_exc()})

    def _on_review_approved(self, task_id: str) -> None:
        logger.info("审查通过: %s", task_id)
        self._unblock_dependents(task_id)

    def _on_review_rejected(self, task_id: str) -> None:
        with self._lock:
            count = self._retry_count[task_id] + 1
            self._retry_count[task_id] = count
        if count <= self.MAX_RETRIES:
            logger.info("审查不通过，重试 %d/%d: %s", count, self.MAX_RETRIES, task_id)
            self.tm.update_task(task_id, status="retry")
            self._executor.submit(self._run_code, task_id)
        else:
            logger.warning("达到最大重试次数 %d: %s → failed", self.MAX_RETRIES, task_id)
            self.tm.update_task(task_id, status="failed")
            self.bus.emit_event(task_id, "orchestrator", "task.retry_exhausted",
                               {"retries": count})
            self._unblock_dependents(task_id)

    def _on_coding_failed(self, task_id: str) -> None:
        logger.warning("编码失败: %s", task_id)
        self._unblock_dependents(task_id)

    def _unblock_dependents(self, task_id: str) -> None:
        parent_id = self._find_parent(task_id)
        with self._lock:
            dag = self._dag_by_root.get(parent_id)
        if not dag:
            return
        unblocked = dag.mark_done(task_id)
        for nid in unblocked:
            with self._lock:
                dag.mark_running(nid)
            self._executor.submit(self._run_code, nid)

        if dag.all_done():
            task = self.tm.get_task(parent_id)
            if task and task.get("status") not in ("done", "failed"):
                status = "done" if dag.failed_count == 0 else "failed"
                self.tm.update_task(parent_id, status=status, result={
                    "total": dag.total,
                    "done": dag.done_count,
                    "failed": dag.failed_count,
                })
                self.bus.emit_event(parent_id, "orchestrator",
                    "task.done" if status == "done" else "task.partially_failed",
                    {"total": dag.total, "done": dag.done_count, "failed": dag.failed_count})
                logger.info("DAG 完成: root=%s done=%d fail=%d",
                           parent_id, dag.done_count, dag.failed_count)
                with self._lock:
                    self._dag_by_root.pop(parent_id, None)

    def _find_parent(self, task_id: str) -> Optional[str]:
        with self._lock:
            for root_id, dag in self._dag_by_root.items():
                if task_id in dag.nodes or task_id == root_id:
                    return root_id
        return None

    # ── 状态查询 ────────────────────────────────────────────

    def status(self) -> Dict[str, Any]:
        with self._lock:
            dags = {rid: {"total": d.total, "done": d.done_count, "failed": d.failed_count}
                    for rid, d in self._dag_by_root.items()}
        return {
            "running": self._running,
            "active_dags": len(dags),
            "dags": dags,
            "retries": dict(self._retry_count),
        }

    def is_task_active(self, task_id: str) -> bool:
        return self._find_parent(task_id) is not None
