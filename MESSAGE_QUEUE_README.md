# 消息队列使用说明

## 功能特性

1. **高并发支持**：支持 1000+ 并发任务执行
2. **自动阈值补给**：当队列为空时自动调用阈值回调补充任务
3. **非阻塞执行**：所有任务异步执行，不会阻塞队列
4. **自动停止**：当没有更多任务且队列为空时自动停止
5. **阈值回调排队**：阈值回调函数排队执行，避免并发冲突
6. **统计信息**：提供详细的执行统计信息

## 核心设计

### 并发控制
- 每一时刻保持 `max_concurrent` 个任务在执行
- 当任务完成时，自动从队列中取新任务执行
- 如果队列为空，调用阈值补给

### 阈值补给机制
- 当队列为空且运行任务数 < max_concurrent 时触发
- 阈值回调函数排队执行（使用锁保证）
- 如果阈值回调返回空列表，表示没有更多任务

### 自动停止机制
- 当阈值回调返回空且队列为空且没有正在执行的任务时，队列自动停止

## 使用方法

### 基本使用

```python
from message_queue import MessageQueue

# 任务执行回调
async def task_handler(task):
    # 处理任务（异步）
    print(f"处理任务: {task}")
    await asyncio.sleep(0.1)

# 阈值补给回调
def threshold_handler():
    # 返回新任务列表，如果返回空列表表示没有更多任务
    tasks = get_tasks_from_source()  # 从数据源获取任务
    return tasks if tasks else []

# 创建队列
queue = MessageQueue(
    max_concurrent=1000,  # 1000 并发
    threshold_callback=threshold_handler,
    task_callback=task_handler
)

# 启动队列
queue.start()

# 等待队列完成
queue.wait()

# 获取统计信息
stats = queue.get_stats()
print(f"完成任务: {stats['completed_tasks']}")
```

### 异步阈值回调

```python
async def async_threshold_handler():
    # 异步获取任务（如从数据库查询）
    tasks = await fetch_tasks_from_db()
    return tasks if tasks else []

queue = MessageQueue(
    max_concurrent=1000,
    threshold_callback=async_threshold_handler,  # 支持异步回调
    task_callback=task_handler
)
```

### 手动添加任务

```python
# 创建队列（不设置阈值回调）
queue = MessageQueue(
    max_concurrent=1000,
    task_callback=task_handler
)

queue.start()

# 手动添加任务
queue.add_task("task_1")
queue.add_tasks(["task_2", "task_3", "task_4"])

# 等待完成
queue.wait()
```

## API 文档

### MessageQueue 类

#### 初始化参数

- `max_concurrent` (int): 最大并发数，默认 1000
- `threshold_callback` (Callable): 阈值补给回调函数
  - 可以是同步函数：`def callback() -> List[Any]`
  - 可以是异步函数：`async def callback() -> List[Any]`
- `task_callback` (Callable): 任务执行回调函数
  - 可以是同步函数：`def callback(task: Any)`
  - 可以是异步函数：`async def callback(task: Any)`

#### 主要方法

- `start()`: 启动队列（同步方法）
- `stop()`: 停止队列（同步方法）
- `add_task(task)`: 添加单个任务（同步方法）
- `add_tasks(tasks)`: 批量添加任务（同步方法）
- `wait(timeout=None)`: 等待队列停止（同步方法）
- `get_stats()`: 获取统计信息（同步方法）

#### 统计信息

```python
{
    "total_tasks": 1000,        # 总任务数
    "completed_tasks": 950,     # 完成任务数
    "failed_tasks": 5,          # 失败任务数
    "running_tasks": 45,        # 正在运行的任务数
    "queue_size": 0,            # 队列中等待的任务数
    "is_running": True,         # 是否正在运行
    "is_stopped": False         # 是否已停止
}
```

## 注意事项

1. **阈值回调排队执行**：阈值回调函数使用锁保证排队执行，避免并发冲突
2. **任务不阻塞**：所有任务使用 `asyncio.create_task` 异步执行，不会阻塞队列
3. **自动停止**：当没有更多任务时，队列会自动停止，无需手动调用 `stop()`
4. **线程安全**：队列在独立线程中运行，可以安全地从主线程调用

## 性能特点

- 支持 1000+ 并发任务
- 任务执行速度取决于任务本身的耗时
- 阈值回调排队执行，避免重复调用
- 内存占用低，使用异步队列

## 示例

详细示例请参考 `message_queue_example.py` 文件。

