# AI Runtime Event Protocol Specification

**版本**: v1.0.0  
**最后更新**: 2026-05-12  
**维护者**: AI Runtime Team

---

## 目录

1. [概述](#1-概述)
2. [事件命名规范](#2-事件命名规范)
3. [完整事件目录](#3-完整事件目录)
4. [Pydantic 模型文档](#4-pydantic-模型文档)
5. [事件流图](#5-事件流图)
6. [示例 JSON 载荷](#6-示例-json-载荷)
7. [版本策略](#7-版本策略)
8. [第三方开发指南](#8-第三方开发指南)

---

## 1. 概述

AI Runtime Event Protocol 定义了 AI Runtime 中所有组件之间通信的事件格式。事件通过 **Redis Streams** 传输，由 `runtime/event_bus.py` 中的 `EventBus` 作为传输层，`runtime/events.py` 中的 Pydantic v2 模型提供强类型校验与序列化。

### 核心原则

- **命名空间隔离**: 事件名按前缀分组（`task.*`, `plugin.*`, `system.*`, `review.*`, `subtask.*`）
- **强类型约束**: 每个事件对应一个 Pydantic v2 模型，运行时校验 payload 结构
- **可追溯性**: 每个事件携带 `event_id`（UUID4）和 `correlation_id`（因果链关联）
- **向后兼容**: 新增字段不破坏已有消费者；废弃事件名保留 2 个 minor 版本

---

## 2. 事件命名规范

### 2.1 命名空间前缀

| 前缀       | 含义               | 示例事件名                |
|------------|--------------------|---------------------------|
| `task.`    | 任务生命周期事件   | `task.created`            |
| `subtask.` | 子任务事件         | `subtask.created`         |
| `review.`  | 代码审查事件       | `review.approved`         |
| `plugin.`  | 插件扩展事件       | `plugin.todo_found`       |
| `system.`  | 系统级生命周期事件 | `system.started`          |

### 2.2 保留事件名

以下事件名不要求命名空间前缀：

| 事件名       | 用途             |
|--------------|------------------|
| `heartbeat`  | 系统心跳         |

### 2.3 命名规则

```
{namespace}.{action}[_{qualifier}]
```

- `namespace`: 必须为 `task` / `plugin` / `system` / `review` / `subtask` 之一
- `action`: 动词或状态，如 `created`, `started`, `completed`, `failed`
- `qualifier`: 可选限定词，如 `task.coding_failed` 中的 `coding`

事件名全小写，单词间用下划线分隔。**违反命名规范的事件将被 Pydantic validator 拒绝。**

---

## 3. 完整事件目录

### 3.1 Task 生命周期事件 (`task.*`)

| 事件名                   | 触发者          | 描述                                       | Pydantic 模型          |
|--------------------------|-----------------|--------------------------------------------|------------------------|
| `task.created`           | Orchestrator / REST API | 新任务已创建                         | `TaskCreated`          |
| `task.plan_started`      | Planner         | Planner 开始规划                           | `PlanStarted`          |
| `task.planned`           | Planner         | Planner 完成子任务拆解                      | `PlanCompleted`        |
| `task.plan_failed`       | Orchestrator    | Planner 执行过程中抛出异常                  | `PlanFailed`           |
| `task.coding_started`    | CoderAgent      | Coder 开始编码                              | `CodingStarted`        |
| `task.code_generated`    | CoderAgent      | LLM 调用完成，获得原始响应                   | `CodeGenerated`        |
| `task.code_written`      | CoderAgent      | 单个文件写入 workspace                     | `CodeWritten`          |
| `task.git_committed`     | CoderAgent      | workspace 已 git commit                    | `GitCommitted`         |
| `task.shell_executed`    | CoderAgent      | shell 命令执行完毕                          | `ShellExecuted`        |
| `task.coding_completed`  | CoderAgent      | 编码阶段全部完成，进入 review               | `CodingCompleted`      |
| `task.coding_failed`     | CoderAgent / Orchestrator | 编码阶段失败                     | `CodingFailed`         |
| `task.review_started`    | Reviewer        | 代码审查开始                               | `ReviewStarted`        |
| `task.review_completed`  | Reviewer        | 代码审查完成（通过或不通过）                | `ReviewCompleted`      |
| `task.status_changed`    | TaskManager     | 任务状态在状态机中流转                      | `TaskStatusChanged`    |
| `task.done`              | Orchestrator    | 任务（含所有子任务）全部完成                | `TaskCompleted`        |
| `task.partially_failed`  | Orchestrator    | 部分子任务失败但 DAG 仍结束                 | `TaskPartiallyFailed`  |
| `task.failed`            | Orchestrator    | 任务彻底失败                                | `TaskFailed`           |
| `task.retry_exhausted`   | Orchestrator    | review 不通过且达到最大重试次数             | `TaskRetryExhausted`   |
| `task.done_no_subtasks`  | Orchestrator    | 规划未产生子任务，直接标记完成              | `TaskDoneNoSubtasks`   |

### 3.2 子任务事件 (`subtask.*`)

| 事件名              | 触发者   | 描述                            | Pydantic 模型       |
|---------------------|----------|---------------------------------|---------------------|
| `subtask.created`   | Planner  | Planner 为父任务创建子任务       | `SubtaskCreated`    |

### 3.3 审查事件 (`review.*`)

| 事件名              | 触发者    | 描述                       | Pydantic 模型      |
|---------------------|-----------|----------------------------|--------------------|
| `review.approved`   | Reviewer  | 审查通过（score >= 70）    | `ReviewApproved`   |
| `review.rejected`   | Reviewer  | 审查不通过                 | `ReviewRejected`   |
| `review.failed`     | Orchestrator | Reviewer 抛出异常       | `ReviewFailed`     |

### 3.4 插件事件 (`plugin.*`)

| 事件名                   | 触发者  | 描述                           | Pydantic 模型           |
|--------------------------|---------|--------------------------------|-------------------------|
| `plugin.todo_found`      | Plugin  | 在代码中发现 TODO/FIXME 标记   | `PluginTodoFound`       |
| `plugin.check_complete`  | Plugin  | 插件完成一轮检查               | `PluginCheckComplete`   |

### 3.5 系统事件 (`system.*` + 保留事件)

| 事件名              | 触发者  | 描述                            | Pydantic 模型      |
|---------------------|---------|---------------------------------|--------------------|
| `system.started`    | System  | AI Runtime 完成初始化           | `SystemStarted`    |
| `system.stopped`    | System  | AI Runtime 进入优雅关闭         | `SystemStopped`    |
| `system.error`      | System  | 全局级错误（非特定任务）        | `SystemError`      |
| `heartbeat`         | System  | 周期性心跳（证明 Runtime 存活） | `Heartbeat`        |

---

## 4. Pydantic 模型文档

所有事件模型定义在 `runtime/events.py`，继承链如下：

```
BaseModel
 └── RuntimeEvent          ← 基类（event_id, correlation_id, timestamp）
      ├── AgentEvent        ← Agent 事件基类
      │    ├── PlanStarted
      │    ├── PlanCompleted
      │    ├── PlanFailed
      │    ├── CodeGenerated
      │    ├── CodeWritten
      │    ├── GitCommitted
      │    ├── ShellExecuted
      │    ├── CodingStarted
      │    ├── CodingCompleted
      │    ├── CodingFailed
      │    ├── ReviewStarted
      │    └── ReviewCompleted
      ├── TaskEvent         ← 任务事件基类
      │    ├── TaskCreated
      │    ├── TaskStatusChanged
      │    ├── TaskCompleted
      │    ├── TaskPartiallyFailed
      │    ├── TaskFailed
      │    ├── TaskRetryExhausted
      │    ├── TaskDoneNoSubtasks
      │    └── SubtaskCreated
      ├── ReviewEvent       ← 审查事件基类
      │    ├── ReviewApproved
      │    ├── ReviewRejected
      │    └── ReviewFailed
      ├── PluginEvent       ← 插件事件基类
      │    ├── PluginTodoFound
      │    └── PluginCheckComplete
      └── SystemEvent       ← 系统事件基类
           ├── SystemStarted
           ├── SystemStopped
           ├── SystemError
           └── Heartbeat
```

### 4.1 RuntimeEvent 基类字段

| 字段             | 类型          | 默认值                   | 说明                                          |
|------------------|---------------|--------------------------|-----------------------------------------------|
| `task_id`        | `str`         | `""`                     | 事件关联的任务 ID（系统事件可为空）            |
| `agent`          | `str`         | `"system"`               | 触发者名称（planner/coder/reviewer/system...） |
| `event`          | `str`         | `""`                     | 事件名（须以合法前缀开头）                     |
| `payload`        | `dict \| str` | `{}`                     | 事件载荷，JSON 可序列化                       |
| `timestamp`      | `str`         | `str(time.time())`       | Unix 时间戳字符串                             |
| `event_id`       | `str`         | `uuid4()`                | 事件唯一 ID                                   |
| `correlation_id` | `str`         | `uuid4()`                | 因果链关联 ID                                 |

### 4.2 关键方法

#### `to_eventbus_kwargs() -> dict`

将事件模型转换为可直接传入 `EventBus.emit_event()` 的关键字参数：

```python
evt = TaskCreated(task_id="task-001", agent="orchestrator")
bus.emit_event(**evt.to_eventbus_kwargs())
# 等价于: bus.emit_event(task_id="task-001", agent="orchestrator",
#                         event="task.created", payload="{}")
```

#### `from_eventbus_data(data: dict) -> RuntimeEvent`

从 Redis Stream 原始数据反序列化为具体事件模型：

```python
raw = {
    "task_id": "task-001",
    "agent": "planner",
    "event": "task.planned",
    "payload": '{"goal":"写贪吃蛇","subtasks":[...]}',
    "timestamp": "1700000000.123",
}
evt = RuntimeEvent.from_eventbus_data(raw)
# → PlanCompleted 实例，payload 自动解析为 dict
```

### 4.3 Validator 行为

- **`_validate_event_namespace`**: `model_validator(mode="after")` —— 校验 `event` 字段是否以 `task.` / `plugin.` / `system.` / `review.` / `subtask.` 开头，或为保留事件名。
- **子类专用 validator**: 如 `PlanCompleted` 要求 `payload` 包含 `subtasks` 字段；`CodeWritten` 要求包含 `file` 字段。
- **`extra="forbid"`**: 禁止传入模型未定义的额外字段，防止拼写错误。

---

## 5. 事件流图

### 5.1 任务完整生命周期

```
                         REST API / UI
                              │
                              ▼
                     ┌─────────────────┐
                     │  task.created    │  ◄── TaskCreated
                     └────────┬────────┘
                              │
              ┌───────────────▼───────────────┐
              │       Planner.plan()           │
              │  ┌─────────────────────────┐  │
              │  │  task.plan_started       │  │  ◄── PlanStarted
              │  └───────────┬─────────────┘  │
              │              │                │
              │  ┌───────────▼─────────────┐  │
              │  │  task.planned            │  │  ◄── PlanCompleted
              │  └───────────┬─────────────┘  │
              │              │                │
              │  ┌───────────▼─────────────┐  │
              │  │  subtask.created x N     │  │  ◄── SubtaskCreated
              │  └─────────────────────────┘  │
              └───────────────┬───────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
          ▼                   ▼                   ▼
    ┌────────────┐     ┌────────────┐     ┌────────────┐
    │ CoderAgent │     │ CoderAgent │     │ CoderAgent │
    │ (subtask1) │     │ (subtask2) │     │ (subtask3) │
    └─────┬──────┘     └─────┬──────┘     └─────┬──────┘
          │                   │                   │
          ▼                   ▼                   ▼
   task.coding_started   task.coding_started   task.coding_started
          │                   │                   │
          ▼                   ▼                   ▼
   task.code_generated   task.code_generated   task.code_generated
          │                   │                   │
          ▼                   ▼                   ▼
   task.code_written     task.code_written     task.code_written
          │                   │                   │
          ▼                   ▼                   ▼
   task.coding_completed task.coding_completed task.coding_completed
          │                   │                   │
          ▼                   ▼                   ▼
    ┌────────────┐     ┌────────────┐     ┌────────────┐
    │  Reviewer  │     │  Reviewer  │     │  Reviewer  │
    └─────┬──────┘     └─────┬──────┘     └─────┬──────┘
          │                   │                   │
    ┌─────┴──────┐      ┌─────┴──────┐      ┌─────┴──────┐
    │            │      │            │      │            │
    ▼            ▼      ▼            ▼      ▼            ▼
review.     review.  review.     review.  review.     review.
approved   rejected approved   rejected approved   rejected
    │            │      │            │      │            │
    │       ┌────┘      │       ┌────┘      │       ┌────┘
    │       ▼           │       ▼           │       ▼
    │  retry (≤3)       │  retry (≤3)       │  retry (≤3)
    │       │           │       │           │       │
    │       ▼           │       ▼           │       ▼
    │  ┌─────────┐      │  ┌─────────┐      │  ┌─────────┐
    │  │CoderAgent│      │  │CoderAgent│      │  │CoderAgent│
    │  │  again   │      │  │  again   │      │  │  again   │
    │  └─────────┘      │  └─────────┘      │  └─────────┘
    │                   │                   │
    ▼                   ▼                   ▼
    └───────────────────┼───────────────────┘
                        │
              ┌─────────▼──────────┐
              │   DAG 全部完成？    │
              └──┬────────────┬───┘
                 │            │
         全部通过│            │有失败
                 ▼            ▼
           task.done    task.partially_failed
         (TaskCompleted)  (TaskPartiallyFailed)
```

### 5.2 Agent 流水线

```
┌─────────┐    ┌──────────┐    ┌──────────┐
│ Planner │───▶│CoderAgent│───▶│ Reviewer │
└────┬────┘    └────┬─────┘    └────┬─────┘
     │              │               │
     ▼              ▼               ▼
task.planned   task.code_written  review.approved
subtask.created  task.coding_     review.rejected
                 completed/failed
```

### 5.3 插件钩子

```
                              EventBus
                                 │
                    ┌────────────┼────────────┐
                    │            │            │
                    ▼            ▼            ▼
              ┌──────────┐ ┌──────────┐ ┌──────────┐
              │ Plugin A │ │ Plugin B │ │ Plugin C │
              │ subscribe│ │ subscribe│ │ subscribe│
              │ [task.*] │ │ [plugin.*]│ │ [ALL]   │
              └────┬─────┘ └────┬─────┘ └────┬─────┘
                   │            │            │
                   ▼            ▼            ▼
            plugin.todo_   plugin.check_  custom events
            found          complete
```

### 5.4 系统生命周期

```
  system.started ──▶ [Runtime 运行中] ──▶ system.stopped
       │                    │                    │
       │         ┌──────────┴──────────┐         │
       │         │                     │         │
       │    heartbeat          system.error     │
       │    (每 30s)           (异常时)          │
       │                                         │
       └─────────────────────────────────────────┘
```

---

## 6. 示例 JSON 载荷

### 6.1 TaskCreated

```json
{
  "task_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "agent": "orchestrator",
  "event": "task.created",
  "payload": {
    "goal": "写一个 Python 贪吃蛇游戏",
    "description": "使用 pygame 实现经典贪吃蛇"
  },
  "timestamp": "1700000000.123",
  "event_id": "11111111-2222-3333-4444-555555555555",
  "correlation_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
}
```

### 6.2 PlanCompleted

```json
{
  "task_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "agent": "planner",
  "event": "task.planned",
  "payload": {
    "task_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "goal": "写一个 Python 贪吃蛇游戏",
    "subtasks": [
      {
        "id": "sub-001",
        "title": "初始化 pygame 窗口",
        "description": "创建 800x600 窗口，设置游戏循环骨架",
        "depends_on": []
      },
      {
        "id": "sub-002",
        "title": "实现蛇的移动逻辑",
        "description": "用 deque 管理蛇身，处理方向键输入",
        "depends_on": ["sub-001"]
      }
    ],
    "subtask_count": 2
  },
  "timestamp": "1700000001.456",
  "event_id": "22222222-3333-4444-5555-666666666666",
  "correlation_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
}
```

### 6.3 SubtaskCreated

```json
{
  "task_id": "sub-001",
  "agent": "planner",
  "event": "subtask.created",
  "payload": {
    "parent_task_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "subtask_id": "sub-001",
    "title": "初始化 pygame 窗口",
    "description": "创建 800x600 窗口，设置游戏循环骨架",
    "depends_on": []
  },
  "timestamp": "1700000001.789",
  "event_id": "33333333-4444-5555-6666-777777777777",
  "correlation_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
}
```

### 6.4 CodeWritten

```json
{
  "task_id": "sub-001",
  "agent": "coder",
  "event": "task.code_written",
  "payload": {
    "file": "src/main.py"
  },
  "timestamp": "1700000005.012",
  "event_id": "44444444-5555-6666-7777-888888888888",
  "correlation_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
}
```

### 6.5 CodingCompleted

```json
{
  "task_id": "sub-001",
  "agent": "coder",
  "event": "task.coding_completed",
  "payload": {
    "files_count": 3,
    "shell_count": 1
  },
  "timestamp": "1700000010.345",
  "event_id": "55555555-6666-7777-8888-999999999999",
  "correlation_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
}
```

### 6.6 ReviewApproved

```json
{
  "task_id": "sub-001",
  "agent": "reviewer",
  "event": "review.approved",
  "payload": {
    "score": 92,
    "issues_count": 2
  },
  "timestamp": "1700000012.678",
  "event_id": "66666666-7777-8888-9999-000000000000",
  "correlation_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
}
```

### 6.7 ReviewRejected

```json
{
  "task_id": "sub-002",
  "agent": "reviewer",
  "event": "review.rejected",
  "payload": {
    "score": 45,
    "issues": [
      {
        "category": "syntax_error",
        "file": "src/snake.py",
        "line": 42,
        "message": "语法错误: invalid syntax",
        "snippet": "def move(): retrun None"
      },
      {
        "category": "fake_impl",
        "file": "src/game.py",
        "line": 15,
        "message": "函数 'update' 是桩实现 (raise NotImplementedError)"
      }
    ]
  },
  "timestamp": "1700000015.901",
  "event_id": "77777777-8888-9999-0000-111111111111",
  "correlation_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
}
```

### 6.8 PluginTodoFound

```json
{
  "task_id": "sub-001",
  "agent": "plugin",
  "event": "plugin.todo_found",
  "payload": {
    "plugin": "todo-checker",
    "file": "src/main.py",
    "count": 3,
    "items": [
      {"line": 10, "tag": "TODO", "text": "# TODO: 添加错误处理"},
      {"line": 25, "tag": "FIXME", "text": "# FIXME: 修复内存泄漏"},
      {"line": 50, "tag": "HACK", "text": "# HACK: 临时方案，需重构"}
    ]
  },
  "timestamp": "1700000006.200",
  "event_id": "88888888-9999-0000-1111-222222222222",
  "correlation_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
}
```

### 6.9 SystemStarted

```json
{
  "task_id": "",
  "agent": "system",
  "event": "system.started",
  "payload": {
    "version": "1.0.0",
    "redis_connected": true,
    "plugins_loaded": 3
  },
  "timestamp": "1700000000.001",
  "event_id": "99999999-0000-1111-2222-333333333333",
  "correlation_id": "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
}
```

### 6.10 Heartbeat

```json
{
  "task_id": "",
  "agent": "system",
  "event": "heartbeat",
  "payload": {
    "uptime_seconds": 3600.5,
    "active_tasks": 2,
    "ws_connections": 1
  },
  "timestamp": "1700003600.500",
  "event_id": "00000000-1111-2222-3333-444444444444",
  "correlation_id": "cccccccc-dddd-eeee-ffff-000000000000"
}
```

---

## 7. 版本策略

### 7.1 语义化版本

事件协议采用 **MAJOR.MINOR.PATCH** 语义化版本：

| 变更类型                           | 版本影响 | 示例                        |
|------------------------------------|----------|-----------------------------|
| 移除事件名或必填字段               | MAJOR    | 删除 `task.deprecated`      |
| 新增事件名（非破坏性）             | MINOR    | 新增 `task.benchmark_ran`   |
| 修改字段描述 / 放宽校验            | PATCH    | 将 `min_length` 从 1 改为 0 |
| 新增可选字段到 payload             | PATCH    | 给 `CodeWritten` 加 `size`  |

### 7.2 废弃周期

- 废弃的事件名在代码中标记 `@deprecated` 注释
- 保留至少 **2 个 MINOR 版本**后才能移除
- 废弃期间，事件名在 validator 中产生 `DeprecationWarning` 但不拒绝

### 7.3 兼容性承诺

- **向前兼容**: 新版本 Consumer 必须能解析旧版本 Producer 的事件
- **向后兼容**: 旧版本 Consumer 忽略新版本新增的未知字段
- **破坏性变更**: 仅在 MAJOR 版本中允许，需在 CHANGELOG 中明确标注迁移指南

---

## 8. 第三方开发指南

### 8.1 新增自定义事件

如果要在自己的插件中新增事件，按以下步骤操作：

**Step 1: 定义 Pydantic 模型**

```python
# my_plugin/events.py
from runtime.events import PluginEvent, _EVENT_MODEL_REGISTRY
from pydantic import Field, model_validator

class MyCustomEvent(PluginEvent):
    """我的自定义插件事件。"""
    __event_name__: ClassVar[str] = "plugin.my_custom_event"
    event: str = Field(default="plugin.my_custom_event", frozen=True)

    @model_validator(mode="after")
    def _require_my_field(self) -> "MyCustomEvent":
        p = self.payload if isinstance(self.payload, dict) else {}
        if "my_required_field" not in p:
            raise ValueError("MyCustomEvent.payload 必须包含 'my_required_field'")
        return self
```

**Step 2: 在插件中发送事件**

```python
from my_plugin.events import MyCustomEvent

class MyPlugin(BasePlugin):
    name = "my-plugin"

    def on_event(self, task_id, agent, event, payload):
        if event == "task.code_written":
            evt = MyCustomEvent(
                task_id=task_id,
                payload={"my_required_field": "hello", "extra": 42},
            )
            self.ctx.event_bus.emit_event(**evt.to_eventbus_kwargs())
```

**Step 3: 在其他组件中接收事件**

```python
from runtime.events import RuntimeEvent

raw = {"task_id": "...", "agent": "plugin", "event": "plugin.my_custom_event", ...}
evt = RuntimeEvent.from_eventbus_data(raw)
# → MyCustomEvent 实例
```

### 8.2 事件命名建议

- 插件事件使用 `plugin.{plugin_name}.{action}` 格式（如 `plugin.todo_checker.scan_started`），避免不同插件的事件名冲突
- 遵循全小写 + 下划线分隔的惯例
- 在插件的 `subscribe()` 中精确声明关心的事件列表，避免不必要的 `on_event()` 调用

### 8.3 最佳实践

1. **始终使用 `to_eventbus_kwargs()` 发送事件**，而不是手动构造 dict，享受 Pydantic 校验保护
2. **在 `on_event()` 中使用 `isinstance(payload, dict)` 做防御性检查**
3. **为自定义事件模型添加 `model_validator`**，确保 payload 结构符合预期
4. **不要修改 `__event_name__` 的值**保持事件名的不可变性
5. **利用 `correlation_id` 串联同一请求链路上的所有事件**，便于调试和监控

### 8.4 调试技巧

```python
# 列出所有已注册事件名
from runtime.events import list_registered_events
print(list_registered_events())
# ['heartbeat', 'plugin.check_complete', 'plugin.todo_found',
#  'review.approved', 'review.failed', 'review.rejected',
#  'subtask.created', 'system.error', 'system.started',
#  'system.stopped', 'task.code_generated', 'task.code_written',
#  ...]

# 根据事件名获取模型类
from runtime.events import get_event_model
model = get_event_model("task.planned")
print(model)  # <class 'runtime.events.PlanCompleted'>

# 验证事件名是否合法
from runtime.events import RuntimeEvent
try:
    RuntimeEvent(task_id="t1", agent="test", event="bad.event.name")
except ValueError as e:
    print(e)  # 事件名 'bad.event.name' 不合法...
```
