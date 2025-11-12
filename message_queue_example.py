"""
消息队列使用示例
"""
import asyncio
import time
import random
from message_queue import MessageQueue


# 示例 1: 基本使用
def example_basic():
    """基本使用示例"""
    print("=" * 60)
    print("示例 1: 基本使用")
    print("=" * 60)
    
    # 任务计数器
    task_counter = 0
    
    # 任务执行回调
    async def task_handler(task):
        """处理任务"""
        await asyncio.sleep(0.1)  # 模拟任务执行
        print(f"  执行任务: {task}")
    
    # 阈值补给回调
    def threshold_handler():
        """阈值补给"""
        nonlocal task_counter
        if task_counter < 10:
            tasks = [f"task_{i}" for i in range(task_counter, task_counter + 3)]
            task_counter += 3
            print(f"  阈值补给: 添加 {len(tasks)} 个任务")
            return tasks
        else:
            print("  阈值补给: 没有更多任务")
            return []
    
    # 创建队列
    queue = MessageQueue(
        max_concurrent=5,  # 5 个并发
        threshold_callback=threshold_handler,
        task_callback=task_handler
    )
    
    # 启动队列
    queue.start()
    
    # 等待队列完成
    queue.wait()
    
    # 打印统计信息
    stats = queue.get_stats()
    print(f"\n统计信息: {stats}")


# 示例 2: 高并发测试（1000 并发）
def example_high_concurrency():
    """高并发测试示例"""
    print("\n" + "=" * 60)
    print("示例 2: 高并发测试（1000 并发）")
    print("=" * 60)
    
    # 任务计数器
    task_counter = 0
    max_tasks = 5000
    
    # 任务执行回调
    async def task_handler(task):
        """处理任务"""
        # 模拟任务执行（随机耗时 0.01-0.1 秒）
        await asyncio.sleep(random.uniform(0.01, 0.1))
    
    # 阈值补给回调
    def threshold_handler():
        """阈值补给"""
        nonlocal task_counter
        if task_counter < max_tasks:
            # 每次补给 1000 个任务
            batch_size = 1000
            remaining = max_tasks - task_counter
            batch_size = min(batch_size, remaining)
            
            tasks = [f"task_{i}" for i in range(task_counter, task_counter + batch_size)]
            task_counter += batch_size
            print(f"  阈值补给: 添加 {len(tasks)} 个任务 (总计: {task_counter}/{max_tasks})")
            return tasks
        else:
            print("  阈值补给: 没有更多任务")
            return []
    
    # 创建队列
    queue = MessageQueue(
        max_concurrent=1000,  # 1000 个并发
        threshold_callback=threshold_handler,
        task_callback=task_handler
    )
    
    # 启动队列
    print("启动队列...")
    start_time = time.time()
    queue.start()
    
    # 定期打印统计信息
    def print_stats():
        while queue.is_running:
            time.sleep(2)
            stats = queue.get_stats()
            print(f"  统计: 总计={stats['total_tasks']}, "
                  f"完成={stats['completed_tasks']}, "
                  f"失败={stats['failed_tasks']}, "
                  f"运行中={stats['running_tasks']}, "
                  f"队列={stats['queue_size']}")
    
    import threading
    stats_thread = threading.Thread(target=print_stats, daemon=True)
    stats_thread.start()
    
    # 等待队列完成
    queue.wait()
    
    elapsed = time.time() - start_time
    
    # 打印最终统计信息
    stats = queue.get_stats()
    print(f"\n最终统计:")
    print(f"  总任务数: {stats['total_tasks']}")
    print(f"  完成任务数: {stats['completed_tasks']}")
    print(f"  失败任务数: {stats['failed_tasks']}")
    print(f"  总耗时: {elapsed:.2f} 秒")
    print(f"  平均速度: {stats['completed_tasks'] / elapsed:.2f} 任务/秒")


# 示例 3: 异步阈值回调
def example_async_threshold():
    """异步阈值回调示例"""
    print("\n" + "=" * 60)
    print("示例 3: 异步阈值回调")
    print("=" * 60)
    
    task_counter = 0
    
    # 任务执行回调
    async def task_handler(task):
        await asyncio.sleep(0.05)
        print(f"  执行任务: {task}")
    
    # 异步阈值补给回调
    async def async_threshold_handler():
        """异步阈值补给"""
        nonlocal task_counter
        await asyncio.sleep(0.1)  # 模拟异步操作（如从数据库获取数据）
        
        if task_counter < 20:
            tasks = [f"async_task_{i}" for i in range(task_counter, task_counter + 5)]
            task_counter += 5
            print(f"  异步阈值补给: 添加 {len(tasks)} 个任务")
            return tasks
        else:
            print("  异步阈值补给: 没有更多任务")
            return []
    
    # 创建队列
    queue = MessageQueue(
        max_concurrent=10,
        threshold_callback=async_threshold_handler,
        task_callback=task_handler
    )
    
    # 启动队列
    queue.start()
    
    # 等待队列完成
    queue.wait()
    
    # 打印统计信息
    stats = queue.get_stats()
    print(f"\n统计信息: {stats}")


# 示例 4: 手动添加任务
def example_manual_add():
    """手动添加任务示例"""
    print("\n" + "=" * 60)
    print("示例 4: 手动添加任务")
    print("=" * 60)
    
    # 任务执行回调
    async def task_handler(task):
        await asyncio.sleep(0.1)
        print(f"  执行任务: {task}")
    
    # 创建队列（不设置阈值回调，手动添加任务）
    queue = MessageQueue(
        max_concurrent=5,
        task_callback=task_handler
    )
    
    # 启动队列
    queue.start()
    
    # 手动添加任务
    print("手动添加任务...")
    queue.add_tasks([f"manual_task_{i}" for i in range(10)])
    
    # 等待队列完成
    queue.wait()
    
    # 打印统计信息
    stats = queue.get_stats()
    print(f"\n统计信息: {stats}")


if __name__ == "__main__":
    print("消息队列使用示例")
    print("=" * 60)
    
    # 运行示例
    # example_basic()
    example_high_concurrency()
    # example_async_threshold()
    # example_manual_add()

