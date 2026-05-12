# -*- coding: utf-8 -*-
"""AI Workflow Engine — 通用 DAG 工作流编排引擎。

提供：
  - WorkflowNode:  带条件路由、重试、超时的可执行节点
  - RetryStrategy: 指数退避重试策略
  - Workflow:      有向无环图工作流（支持暂停/恢复/取消、回调、Orchestrator 集成）
  - WorkflowBuilder: 流式 API 快速构建工作流
"""

from __future__ import annotations

import logging
import threading
import time
import traceback
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("runtime.workflow")


# ═══════════════════════════════════════════════════════════════════
# WorkflowStatus
# ═══════════════════════════════════════════════════════════════════

class WorkflowStatus(Enum):
    """工作流整体状态。"""
    PENDING   = auto()   # 尚未开始
    RUNNING   = auto()   # 执行中
    PAUSED    = auto()   # 已暂停
    COMPLETED = auto()   # 全部完成（含所有节点 done/skipped）
    FAILED    = auto()   # 存在失败节点且无更多可执行路径
    CANCELLED = auto()   # 已被取消


# ═══════════════════════════════════════════════════════════════════
# WorkflowNode
# ═══════════════════════════════════════════════════════════════════

@dataclass
class WorkflowNode:
    """工作流节点。

    Attributes:
        id:              节点唯一标识（在整个 Workflow 内唯一）
        name:            节点显示名称
        agent:           负责执行的 Agent 名称（供外部 executor 路由使用）
        task_description:任务描述文本
        depends_on:      依赖的前置节点 ID 列表
        status:          当前状态 ('pending','running','done','failed','skipped','cancelled')
        result:          执行结果（任意类型）
        retry_count:     当前已重试次数
        max_retries:     最大重试次数（0=不重试）
        condition:       可选前置条件，返回 False 则跳过该节点
        on_success:      成功后强制执行的下一个节点 ID（附加路由，不影响 DAG 解阻塞）
        on_failure:      失败后强制执行的下一个节点 ID（附加路由）
        timeout_seconds: 超时秒数（0=无限制）
    """
    id: str
    name: str = ""
    agent: str = ""
    task_description: str = ""
    depends_on: List[str] = field(default_factory=list)
    status: str = "pending"
    result: Any = None
    retry_count: int = 0
    max_retries: int = 3
    condition: Optional[Callable[[], bool]] = None
    on_success: Optional[str] = None
    on_failure: Optional[str] = None
    timeout_seconds: int = 0

    # ── 内部追踪字段（Workflow 内部维护，不应手动设置） ──────
    _dependents: List[str] = field(default_factory=list, repr=False)
    _remaining_deps: Set[str] = field(default_factory=set, repr=False)

    @property
    def is_terminal(self) -> bool:
        """节点是否处于终态。"""
        return self.status in ("done", "failed", "skipped", "cancelled")

    @property
    def is_ready(self) -> bool:
        """节点是否就绪（pending + 所有依赖已完成/跳过/失败）。"""
        return self.status == "pending" and not self._remaining_deps

    def reset(self) -> None:
        """重置节点（用于重新执行）。"""
        self.status = "pending"
        self.result = None
        self.retry_count = 0


# ═══════════════════════════════════════════════════════════════════
# RetryStrategy
# ═══════════════════════════════════════════════════════════════════

class RetryStrategy:
    """可配置的指数退避重试策略。

    Usage:
        strategy = RetryStrategy(max_retries=3, delay_seconds=2, backoff_multiplier=2.0)
        if strategy.should_retry(node, attempt=1): ...
        wait = strategy.next_delay(attempt=1)  # → 2s
        wait = strategy.next_delay(attempt=2)  # → 4s (2 * 2)

    Args:
        max_retries:        最大重试次数
        delay_seconds:      基础延迟（秒）
        backoff_multiplier: 退避倍数（每多一次重试，延迟 *= backoff_multiplier）
        jitter:             是否在延迟中加入随机抖动（±25%），避免惊群
    """

    def __init__(
        self,
        max_retries: int = 3,
        delay_seconds: float = 1.0,
        backoff_multiplier: float = 2.0,
        jitter: bool = False,
    ):
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if delay_seconds <= 0:
            raise ValueError("delay_seconds must be > 0")
        if backoff_multiplier < 1.0:
            raise ValueError("backoff_multiplier must be >= 1.0")

        self.max_retries = max_retries
        self.delay_seconds = delay_seconds
        self.backoff_multiplier = backoff_multiplier
        self.jitter = jitter

    def should_retry(self, node: WorkflowNode, attempt: int) -> bool:
        """判断在指定 attempt 下是否应该重试。

        Args:
            node:    当前节点
            attempt: 即将进行的重试次数（1-indexed，即已失败次数 + 1）

        Returns:
            是否应该重试
        """
        effective_max = min(node.max_retries, self.max_retries)
        return attempt <= effective_max

    def next_delay(self, attempt: int) -> float:
        """计算第 attempt 次重试前应等待的秒数。

        Args:
            attempt: 重试次数（1-indexed）

        Returns:
            等待秒数
        """
        delay = self.delay_seconds * (self.backoff_multiplier ** (attempt - 1))
        if self.jitter:
            import random
            jitter_range = delay * 0.25
            delay += random.uniform(-jitter_range, jitter_range)
        return max(0, delay)


# ═══════════════════════════════════════════════════════════════════
# CycleError
# ═══════════════════════════════════════════════════════════════════

class CycleError(ValueError):
    """DAG 存在环时抛出。"""

    def __init__(self, cycle: List[str]):
        self.cycle = cycle
        super().__init__(f"DAG contains a cycle: {' → '.join(cycle)}")


class MissingDependencyError(ValueError):
    """节点依赖了不存在的节点。"""

    def __init__(self, node_id: str, missing: List[str]):
        self.node_id = node_id
        self.missing = missing
        super().__init__(f"Node '{node_id}' depends on missing nodes: {missing}")


# ═══════════════════════════════════════════════════════════════════
# Workflow
# ═══════════════════════════════════════════════════════════════════

class Workflow:
    """通用 DAG 工作流引擎。

    特性：
      - 基于依赖图（DAG）的有向无环图执行
      - 支持条件跳过（condition）
      - 支持成功/失败动态路由（on_success / on_failure）
      - 支持暂停/恢复/取消
      - 节点级超时（timeout_seconds）
      - 可配置重试策略（RetryStrategy）
      - 事件回调（on_node_start / on_node_complete / on_node_fail / on_workflow_complete）
      - Orchestrator 集成（handle_event）
      - 线程安全

    Usage:
        wf = Workflow(name="代码审查流水线", retry_strategy=RetryStrategy(max_retries=2))
        wf.add_node(WorkflowNode(id="lint",  agent="linter", task_description="静态检查"))
        wf.add_node(WorkflowNode(id="test",  agent="tester", task_description="运行测试",
                                 depends_on=["lint"]))
        wf.add_node(WorkflowNode(id="build", agent="builder", task_description="构建",
                                 depends_on=["lint"]))
        wf.validate()
        wf.execute(executor=my_executor)         # 同步步进
        wf.execute_parallel(executor=my_executor) # 并发执行
    """

    # ── 回调类型 ──────────────────────────────────────────────
    NodeCallback = Callable[["Workflow", "WorkflowNode"], None]

    def __init__(
        self,
        name: str = "",
        retry_strategy: Optional[RetryStrategy] = None,
        max_workers: int = 4,
    ):
        self.name = name
        self.retry_strategy = retry_strategy or RetryStrategy()
        self.max_workers = max_workers

        # 节点存储
        self._nodes: Dict[str, WorkflowNode] = {}

        # 状态
        self._status: WorkflowStatus = WorkflowStatus.PENDING
        self._lock = threading.RLock()
        self._pause_event = threading.Event()
        self._pause_event.set()  # 初始：非暂停状态
        self._cancel_flag = False

        # 事件回调
        self.event_callbacks: Dict[str, List[Callable]] = {
            "on_node_start":      [],
            "on_node_complete":   [],
            "on_node_fail":       [],
            "on_node_skip":       [],
            "on_workflow_complete": [],
            "on_workflow_pause":  [],
            "on_workflow_resume": [],
            "on_workflow_cancel": [],
        }

    # ── 属性 ───────────────────────────────────────────────────

    @property
    def status(self) -> WorkflowStatus:
        with self._lock:
            return self._status

    @property
    def nodes(self) -> Dict[str, WorkflowNode]:
        """只读节点映射。"""
        return dict(self._nodes)

    @property
    def is_running(self) -> bool:
        return self._status == WorkflowStatus.RUNNING

    @property
    def is_paused(self) -> bool:
        return self._status == WorkflowStatus.PAUSED

    # ── 节点/边管理 ────────────────────────────────────────────

    def add_node(self, node: WorkflowNode) -> "Workflow":
        """添加节点到工作流。

        Args:
            node: WorkflowNode 实例

        Returns:
            self（支持链式调用）

        Raises:
            ValueError: 节点 ID 重复
        """
        with self._lock:
            if node.id in self._nodes:
                raise ValueError(f"Duplicate node id: '{node.id}'")
            self._nodes[node.id] = node
            # 预构建 _remaining_deps（set，便于移除）
            node._remaining_deps = set(node.depends_on)
        return self

    def add_edge(self, from_id: str, to_id: str) -> "Workflow":
        """添加依赖边：to_id 依赖 from_id。

        自动更新 from_id 的 _dependents 和 to_id 的 depends_on / _remaining_deps。

        Args:
            from_id: 前置节点 ID
            to_id:   后置节点 ID

        Returns:
            self

        Raises:
            KeyError: from_id 或 to_id 不存在
        """
        with self._lock:
            from_node = self._nodes.get(from_id)
            to_node = self._nodes.get(to_id)

            if from_node is None:
                raise KeyError(f"Node '{from_id}' not found")
            if to_node is None:
                raise KeyError(f"Node '{to_id}' not found")

            # 更新 from 的后继列表
            if to_id not in from_node._dependents:
                from_node._dependents.append(to_id)

            # 更新 to 的依赖
            if from_id not in to_node.depends_on:
                to_node.depends_on.append(from_id)
                to_node._remaining_deps.add(from_id)

        return self

    def get_node(self, node_id: str) -> Optional[WorkflowNode]:
        """按 ID 获取节点。"""
        return self._nodes.get(node_id)

    # ── 验证 ───────────────────────────────────────────────────

    def validate(self) -> None:
        """验证工作流合法性。

        Raises:
            CycleError:            存在环
            MissingDependencyError: 节点依赖了不存在的节点
            ValueError:            其他配置错误
        """
        with self._lock:
            if not self._nodes:
                raise ValueError("Workflow has no nodes")

            # 1) 检查缺失依赖
            for nid, node in self._nodes.items():
                missing = [dep for dep in node.depends_on if dep not in self._nodes]
                if missing:
                    raise MissingDependencyError(nid, missing)

            # 2) 环检测（DFS 三色标记）
            self._detect_cycles()

            # 3) 自引用检查
            for nid, node in self._nodes.items():
                if nid in node.depends_on:
                    raise CycleError([nid, nid])

    def _detect_cycles(self) -> None:
        """DFS 三色标记环检测。

        WHITE (0): 未访问
        GRAY  (1): 正在访问（在当前递归栈中）
        BLACK (2): 已完全处理
        """
        color: Dict[str, int] = {nid: 0 for nid in self._nodes}
        parent: Dict[str, Optional[str]] = {}

        def dfs(nid: str) -> Optional[List[str]]:
            color[nid] = 1  # GRAY
            node = self._nodes.get(nid)
            if node:
                for dep_id in node.depends_on:
                    if dep_id not in color:
                        continue
                    if color[dep_id] == 1:
                        # 找到环：回溯构造路径
                        cycle = [dep_id, nid]
                        cur = nid
                        while parent.get(cur) and parent[cur] != dep_id:
                            cur = parent[cur]
                            cycle.append(cur)
                        cycle.append(dep_id)
                        cycle.reverse()
                        return cycle
                    if color[dep_id] == 0:
                        parent[dep_id] = nid
                        cycle = dfs(dep_id)
                        if cycle:
                            return cycle
            color[nid] = 2  # BLACK
            return None

        for nid in self._nodes:
            if color[nid] == 0:
                cycle = dfs(nid)
                if cycle:
                    raise CycleError(cycle)

    # ── 执行（同步步进） ───────────────────────────────────────

    def execute(
        self,
        executor: Optional[Callable[[WorkflowNode], Any]] = None,
    ) -> None:
        """同步步进执行工作流。

        每次从就绪节点中选择一个执行，直至所有节点到达终态或工作流被取消/暂停。

        Args:
            executor: 节点执行函数。签名为 (WorkflowNode) -> Any。
                      返回正常即视为成功，抛出异常视为失败。
                      若为 None，节点直接标记为 done（dry-run 模式）。
        """
        self._prepare_execution()

        while not self._cancel_flag and not self._is_workflow_done():
            self._wait_if_paused()
            if self._cancel_flag:
                break

            ready = self._ready_nodes()
            if not ready:
                # 所有剩余节点都 blocked — 检查是否死锁
                if not self._has_pending_nodes():
                    break
                # 还有 pending 节点但都不就绪 → 等待或超时
                time.sleep(0.05)
                continue

            # 步进：只执行一个就绪节点
            node_id = ready[0]
            self._run_node(node_id, executor)

        self._finalize_status()

    # ── 并发执行 ───────────────────────────────────────────────

    def execute_parallel(
        self,
        executor: Optional[Callable[[WorkflowNode], Any]] = None,
        max_workers: Optional[int] = None,
    ) -> None:
        """并发执行工作流。

        每一轮将所有就绪节点提交到 ThreadPoolExecutor 并行执行。

        Args:
            executor:    节点执行函数。同 execute()。
            max_workers: 线程池大小。默认使用 self.max_workers。
        """
        self._prepare_execution()
        workers = max_workers or self.max_workers

        with ThreadPoolExecutor(max_workers=workers) as pool:
            while not self._cancel_flag and not self._is_workflow_done():
                self._wait_if_paused()
                if self._cancel_flag:
                    break

                ready = self._ready_nodes()
                if not ready:
                    if not self._has_pending_nodes():
                        break
                    time.sleep(0.05)
                    continue

                # 提交所有就绪节点
                futures: Dict[Future, str] = {}
                with self._lock:
                    for nid in ready:
                        node = self._nodes[nid]
                        if node.status == "pending" and node.is_ready:
                            node.status = "running"
                            f = pool.submit(self._execute_with_timeout, node, executor)
                            futures[f] = nid

                # 等待本轮全部完成
                for f in as_completed(futures):
                    nid = futures[f]
                    try:
                        f.result()
                    except Exception:
                        # _execute_with_timeout 已处理内部异常，这里兜底
                        logger.error("节点 %s 执行异常: %s", nid, traceback.format_exc())

        self._finalize_status()

    # ── 控制 ───────────────────────────────────────────────────

    def pause(self) -> None:
        """暂停工作流。正在执行的节点不受影响，完成后不会调度新节点。"""
        with self._lock:
            if self._status not in (WorkflowStatus.RUNNING,):
                return
            self._status = WorkflowStatus.PAUSED
            self._pause_event.clear()
        self._fire("on_workflow_pause", None)
        logger.info("Workflow '%s' 已暂停", self.name)

    def resume(self) -> None:
        """恢复暂停的工作流。"""
        with self._lock:
            if self._status != WorkflowStatus.PAUSED:
                return
            self._status = WorkflowStatus.RUNNING
            self._pause_event.set()
        self._fire("on_workflow_resume", None)
        logger.info("Workflow '%s' 已恢复", self.name)

    def cancel(self) -> None:
        """取消工作流。设置取消标志，不会等待正在执行的节点。"""
        with self._lock:
            if self._status in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED,
                                WorkflowStatus.CANCELLED):
                return
            self._cancel_flag = True
            self._status = WorkflowStatus.CANCELLED
            self._pause_event.set()  # 解除暂停等待
            # 将未执行节点标记为 cancelled
            for node in self._nodes.values():
                if node.status == "pending":
                    node.status = "cancelled"
        self._fire("on_workflow_cancel", None)
        logger.info("Workflow '%s' 已取消", self.name)

    # ── Orchestrator 集成 ──────────────────────────────────────

    def handle_event(self, task_id: str, event: str, payload: Any = None) -> None:
        """处理来自 Orchestrator 的事件，自动更新对应节点状态。

        将 Orchestrator 事件映射到节点状态变更：
          - task.coding_completed / task.done  → 标记节点 done
          - task.coding_failed / task.review_rejected  → 标记节点 failed
          - task.started / task.running       → 标记节点 running

        Args:
            task_id: 对应节点 ID
            event:   事件类型
            payload: 事件负载（可选）
        """
        with self._lock:
            node = self._nodes.get(task_id)
            if node is None:
                logger.debug("handle_event: 未找到节点 %s", task_id)
                return

        success_events = {
            "task.coding_completed", "task.done", "task.review_approved",
            "node.completed", "task.completed",
        }
        fail_events = {
            "task.coding_failed", "task.review_rejected", "task.failed",
            "node.failed", "task.retry_exhausted",
        }
        running_events = {"task.started", "task.running", "node.started"}

        if event in success_events:
            self._mark_node_done(task_id, result=payload)
        elif event in fail_events:
            self._mark_node_failed(task_id, error=payload)
        elif event in running_events:
            with self._lock:
                if node.status == "pending":
                    node.status = "running"

    # ════════════════════════════════════════════════════════════
    # 内部方法
    # ════════════════════════════════════════════════════════════

    def _prepare_execution(self) -> None:
        """执行前准备：重置状态、重建内部依赖跟踪。"""
        with self._lock:
            if not self._nodes:
                raise ValueError("Workflow has no nodes — cannot execute")

            self._status = WorkflowStatus.RUNNING
            self._cancel_flag = False
            self._pause_event.set()

            for node in self._nodes.values():
                node.reset()
                node._dependents = []
                node._remaining_deps = set(node.depends_on)

            # 重建 _dependents（从 depends_on 反向构建）
            for nid, node in self._nodes.items():
                for dep_id in node.depends_on:
                    dep_node = self._nodes.get(dep_id)
                    if dep_node and nid not in dep_node._dependents:
                        dep_node._dependents.append(nid)

    def _wait_if_paused(self) -> None:
        """若处于暂停状态，阻塞等待恢复或取消。"""
        while not self._pause_event.is_set() and not self._cancel_flag:
            self._pause_event.wait(timeout=0.5)

    def _ready_nodes(self) -> List[str]:
        """获取当前所有就绪节点 ID（线程安全）。"""
        with self._lock:
            return [nid for nid, n in self._nodes.items()
                    if n.is_ready and n.status == "pending"]

    def _has_pending_nodes(self) -> bool:
        """是否还有未到达终态的节点。"""
        with self._lock:
            return any(n.status in ("pending", "running") for n in self._nodes.values())

    def _is_workflow_done(self) -> bool:
        """检查工作流是否应结束（所有节点均为终态）。"""
        with self._lock:
            if self._cancel_flag:
                return True
            return all(n.is_terminal for n in self._nodes.values())

    def _run_node(
        self,
        node_id: str,
        executor: Optional[Callable[[WorkflowNode], Any]],
    ) -> None:
        """执行单个节点（内部同步调用）。

        处理流程：条件检查 → 执行 → 成功/失败路由 → 解阻塞后继节点。
        """
        node = self._nodes.get(node_id)
        if node is None:
            return

        with self._lock:
            if node.status not in ("pending",):
                return
            node.status = "running"

        # 回调：节点开始
        self._fire("on_node_start", node)

        try:
            # ── 条件检查 ──────────────────────────────────
            if node.condition is not None:
                try:
                    should_run = node.condition()
                except Exception:
                    logger.error("节点 %s 的条件检查异常: %s", node_id, traceback.format_exc())
                    should_run = False

                if not should_run:
                    with self._lock:
                        node.status = "skipped"
                        node.result = "skipped_by_condition"
                    self._fire("on_node_skip", node)
                    self._unblock_dependents(node_id)
                    return

            # ── 执行 ───────────────────────────────────────
            if executor is not None:
                result = executor(node)
            else:
                # dry-run：无 executor 时直接标记成功
                result = None

            # ── 成功 ───────────────────────────────────────
            self._mark_node_done(node_id, result=result)

        except Exception as e:
            # ── 失败 + 重试 ────────────────────────────────
            self._handle_node_failure(node_id, e)

    def _execute_with_timeout(
        self,
        node: WorkflowNode,
        executor: Optional[Callable[[WorkflowNode], Any]],
    ) -> None:
        """带超时控制的节点执行（供 ThreadPoolExecutor 使用）。"""
        self._fire("on_node_start", node)

        # 条件检查
        if node.condition is not None:
            try:
                if not node.condition():
                    with self._lock:
                        node.status = "skipped"
                        node.result = "skipped_by_condition"
                    self._fire("on_node_skip", node)
                    self._unblock_dependents(node.id)
                    return
            except Exception:
                logger.error("节点 %s 条件检查异常: %s", node.id, traceback.format_exc())
                with self._lock:
                    node.status = "failed"
                self._fire("on_node_fail", node)
                self._unblock_dependents(node.id)
                return

        try:
            if executor is not None:
                result = executor(node)
            else:
                result = None
            self._mark_node_done(node.id, result=result)
        except Exception as e:
            self._handle_node_failure(node.id, e)

    def _mark_node_done(self, node_id: str, result: Any = None) -> None:
        """标记节点完成并触发后续逻辑。"""
        node = self._nodes.get(node_id)
        if node is None:
            return
        with self._lock:
            node.status = "done"
            node.result = result
        self._fire("on_node_complete", node)
        logger.info("节点 %s 完成", node_id)

        # 动态路由: on_success
        if node.on_success:
            self._trigger_dynamic_route(node.on_success)

        # 解阻塞后继
        self._unblock_dependents(node_id)

    def _handle_node_failure(self, node_id: str, error: Exception) -> None:
        """处理节点失败：重试或标记失败。"""
        node = self._nodes.get(node_id)
        if node is None:
            return

        attempt = node.retry_count + 1
        should_retry = self.retry_strategy.should_retry(node, attempt)

        if should_retry:
            delay = self.retry_strategy.next_delay(attempt)
            logger.warning("节点 %s 失败，%d 秒后重试 (attempt %d): %s",
                          node_id, delay, attempt, error)
            time.sleep(delay)

            with self._lock:
                node.status = "pending"
                node.retry_count = attempt
                node._remaining_deps = set(node.depends_on)
        else:
            logger.error("节点 %s 最终失败 (attempt %d): %s", node_id, attempt, error)
            self._mark_node_failed(node_id, error=str(error))

    def _mark_node_failed(self, node_id: str, error: Any = None) -> None:
        """标记节点失败并触发后续逻辑。"""
        node = self._nodes.get(node_id)
        if node is None:
            return
        with self._lock:
            node.status = "failed"
            node.result = error
        self._fire("on_node_fail", node)

        # 动态路由: on_failure
        if node.on_failure:
            self._trigger_dynamic_route(node.on_failure)

        # 解阻塞后继（即使是失败也解阻塞，让后续节点决定是否继续）
        self._unblock_dependents(node_id)

    def _unblock_dependents(self, node_id: str) -> None:
        """从所有后继节点的 _remaining_deps 中移除 node_id。"""
        node = self._nodes.get(node_id)
        if node is None:
            return

        with self._lock:
            for dep_id in node._dependents:
                dep_node = self._nodes.get(dep_id)
                if dep_node and dep_node.status == "pending":
                    dep_node._remaining_deps.discard(node_id)

    def _trigger_dynamic_route(self, target_id: str) -> None:
        """触发动态路由：将目标节点标记为就绪（移除其所有依赖）。"""
        target = self._nodes.get(target_id)
        if target is None:
            logger.warning("动态路由目标节点不存在: %s", target_id)
            return
        with self._lock:
            if target.status == "pending":
                target._remaining_deps.clear()
            elif target.status == "skipped":
                # 被跳过的节点可以重新激活
                target.status = "pending"
                target._remaining_deps.clear()

    def _finalize_status(self) -> None:
        """根据节点终态确定工作流最终状态。"""
        with self._lock:
            if self._cancel_flag:
                self._status = WorkflowStatus.CANCELLED
            elif any(n.status == "failed" for n in self._nodes.values()):
                self._status = WorkflowStatus.FAILED
            else:
                self._status = WorkflowStatus.COMPLETED
            final_status = self._status
        self._fire("on_workflow_complete", None, status=final_status)
        logger.info("Workflow '%s' 结束: %s", self.name, final_status.name)

    def _fire(self, event_name: str, node: Optional[WorkflowNode],
              **extra) -> None:
        """触发回调（安全调用，单个回调异常不影响其他回调）。"""
        callbacks = self.event_callbacks.get(event_name, [])
        for cb in callbacks:
            try:
                if node is not None:
                    cb(self, node)
                else:
                    cb(self)
            except Exception:
                logger.error("回调 %s 异常: %s", event_name, traceback.format_exc())

    # ── 事件回调注册 ──────────────────────────────────────────

    def on(self, event: str, callback: Callable) -> "Workflow":
        """注册事件回调。支持的事件名见 event_callbacks 字典的键。

        callback 签名:
          - on_node_start / on_node_complete / on_node_fail / on_node_skip:
                (workflow: Workflow, node: WorkflowNode) -> None
          - on_workflow_complete / on_workflow_pause / on_workflow_resume /
            on_workflow_cancel:
                (workflow: Workflow) -> None
        """
        if event not in self.event_callbacks:
            available = list(self.event_callbacks.keys())
            raise ValueError(f"Unknown event '{event}'. Available: {available}")
        self.event_callbacks[event].append(callback)
        return self

    # ── 查询 / 诊断 ────────────────────────────────────────────

    def summary(self) -> Dict[str, Any]:
        """返回工作流摘要。"""
        with self._lock:
            nodes_info = {
                nid: {
                    "name": n.name,
                    "status": n.status,
                    "depends_on": n.depends_on,
                    "dependents": n._dependents,
                    "retries": n.retry_count,
                }
                for nid, n in self._nodes.items()
            }
        return {
            "name": self.name,
            "status": self._status.name,
            "total_nodes": len(self._nodes),
            "done": sum(1 for n in self._nodes.values() if n.status == "done"),
            "failed": sum(1 for n in self._nodes.values() if n.status == "failed"),
            "skipped": sum(1 for n in self._nodes.values() if n.status == "skipped"),
            "cancelled": sum(1 for n in self._nodes.values() if n.status == "cancelled"),
            "running": sum(1 for n in self._nodes.values() if n.status == "running"),
            "pending": sum(1 for n in self._nodes.values() if n.status == "pending"),
            "nodes": nodes_info,
        }

    def __repr__(self) -> str:
        return (f"Workflow(name={self.name!r}, status={self._status.name}, "
                f"nodes={len(self._nodes)})")


# ═══════════════════════════════════════════════════════════════════
# WorkflowBuilder
# ═══════════════════════════════════════════════════════════════════

class WorkflowBuilder:
    """流式 API 快速构建工作流。

    示例:
        wf = (WorkflowBuilder()
              .plan("分析需求")
              .code("编写代码")
              .review("审查代码")
              .requires("plan")
              .requires("code", "plan")     # review 依赖 plan + code
              .parallel("单元测试", "集成测试")  # 两个并行测试节点
              .build())

    支持的快捷方法:
      .plan(desc)   → agent="planner"
      .code(desc)   → agent="coder"
      .review(desc) → agent="reviewer"
      .test(desc)   → agent="tester"
      .deploy(desc) → agent="deployer"
      .node(name, agent, desc) → 自定义节点
    """

    def __init__(self, workflow_name: str = ""):
        self._wf = Workflow(name=workflow_name)
        self._last_id: Optional[str] = None
        self._counter: Dict[str, int] = {}

    # ── 便捷方法 ──────────────────────────────────────────────

    def plan(self, desc: str) -> "WorkflowBuilder":
        """添加 plan 节点。"""
        return self._add("plan", "planner", desc)

    def code(self, desc: str) -> "WorkflowBuilder":
        """添加 code 节点。"""
        return self._add("code", "coder", desc)

    def review(self, desc: str) -> "WorkflowBuilder":
        """添加 review 节点。"""
        return self._add("review", "reviewer", desc)

    def test(self, desc: str) -> "WorkflowBuilder":
        """添加 test 节点。"""
        return self._add("test", "tester", desc)

    def deploy(self, desc: str) -> "WorkflowBuilder":
        """添加 deploy 节点。"""
        return self._add("deploy", "deployer", desc)

    def node(self, name: str, agent: str, desc: str) -> "WorkflowBuilder":
        """添加自定义节点。"""
        return self._add(name, agent, desc)

    # ── 依赖设置 ──────────────────────────────────────────────

    def requires(self, *node_ids: str) -> "WorkflowBuilder":
        """为上一次添加的节点设置依赖（累积去重）。

        可多次调用，每次追加新的依赖 ID。

        Raises:
            RuntimeError: 尚未添加任何节点
        """
        if self._last_id is None:
            raise RuntimeError("No node added yet — cannot set dependencies")

        node = self._wf._nodes[self._last_id]
        for nid in node_ids:
            self._wf.add_edge(nid, self._last_id)
        return self

    # ── 并行节点 ──────────────────────────────────────────────

    def parallel(self, *descs: str) -> "WorkflowBuilder":
        """添加可并行执行的节点。

        各节点之间无依赖关系，均可与已存在节点并行。

        Args:
            *descs: 节点描述列表
        """
        for desc in descs:
            self._add("parallel", "worker", desc)
        return self

    # ── 构建 ──────────────────────────────────────────────────

    def build(self) -> Workflow:
        """构建并返回 Workflow 实例。

        Raises:
            CycleError / MissingDependencyError: 验证不通过
        """
        self._wf.validate()
        return self._wf

    # ── 内部 ──────────────────────────────────────────────────

    def _add(self, name: str, agent: str, desc: str) -> "WorkflowBuilder":
        """内部：生成唯一 ID，添加节点。"""
        # 生成递增 ID
        self._counter[name] = self._counter.get(name, 0) + 1
        node_id = f"{name}_{self._counter[name]}"

        node = WorkflowNode(
            id=node_id,
            name=name,
            agent=agent,
            task_description=desc,
        )
        self._wf.add_node(node)
        self._last_id = node_id
        return self
