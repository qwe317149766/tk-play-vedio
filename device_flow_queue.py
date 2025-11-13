"""
使用消息队列执行设备流程任务
基于 message_queue.py 重构 async_thread_main.py 的业务逻辑
"""
import os
import time
import json
import threading
import traceback
import asyncio
from typing import Any, Dict, Tuple, List, Optional

from message_queue import MessageQueue
from config_loader import ConfigLoader

# ===================== 业务函数导入 =====================
from devices import getANewDevice
from tiktok_api import TikTokAPI

# ===================== 配置加载 =====================
def load_config():
    """加载配置"""
    config = ConfigLoader._load_config_file()
    mq_config = config.get("message_queue", {})
    
    # 默认配置
    defaults = {
        "max_concurrent": 1000,
        "proxy": "",
        "results_log": "state/results.log",
        "device_save_path": "state/devices1.jsonl",
        "sticky_batch": 5,
        "sticky_fetch_timeout": 0.2,
        "global_qps": 0,
        "log_flush_every": 5000,
        "stats_every": 5,
        "pool_initial_size": 1000,
        "pool_max_size": 5000,
        "pool_grow_step": 50
    }
    
    # 合并配置
    for key, default_value in defaults.items():
        if key not in mq_config:
            mq_config[key] = default_value
    
    return mq_config

# 加载配置
CONFIG = load_config()

# 创建必要的目录
os.makedirs("state", exist_ok=True)

# ===================== 文件写入和日志 =====================
_write_lock = threading.Lock()

def append_device_jsonl(dev: Dict[str, Any]):
    """追加设备信息到文件"""
    line = json.dumps(dev, ensure_ascii=False)
    try:
        with _write_lock:
            with open(CONFIG["device_save_path"], "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception as e:
        print(f"[WARN] 写入 {CONFIG['device_save_path']} 失败: {e}")

class SafeLogger:
    """线程安全的日志记录器"""
    def __init__(self, path: str, flush_every: int = 200):
        self.path = path
        self.buf: List[str] = []
        self.flush_every = flush_every
        self.lock = threading.Lock()

    def log(self, line: str):
        with self.lock:
            self.buf.append(line)
            if len(self.buf) >= self.flush_every:
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write("\n".join(self.buf) + "\n")
                self.buf.clear()

    def close(self):
        with self.lock:
            if self.buf:
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write("\n".join(self.buf) + "\n")
                self.buf.clear()

# ===================== 统计信息 =====================
class Stats:
    """统计信息"""
    def __init__(self):
        self.ok = 0
        self.fail = 0
        self.lock = threading.Lock()

    def add_ok(self):
        with self.lock:
            self.ok += 1

    def add_fail(self):
        with self.lock:
            self.fail += 1

    def snapshot(self) -> Tuple[int, int]:
        with self.lock:
            return self.ok, self.fail

# ===================== 业务逻辑 =====================
async def run_device_flow(job: Dict[str, Any], proxy_url: str) -> Tuple[bool, str, Dict]:
    """
    执行设备流程（业务逻辑）- 异步版本
    使用 TikTokAPI 进行请求（异步执行，不阻塞）
    同一个流程复用同一个Session，流程结束后重建Session
    
    Args:
        job: 任务数据
        proxy_url: 代理地址
    
    Returns:
        (成功标志, 消息, 元数据)
    """
    step = "init"
    device_id = None
    seed_type = None
    flow_session = None
    try:
        print(f"[RUN_DEVICE_FLOW] 开始执行设备流程: job={job}, proxy_url={proxy_url}")
        # 使用全局 TikTokAPI 实例
        global _api_instance
        if _api_instance is None:
            raise RuntimeError("TikTokAPI 实例未初始化")
        api = _api_instance
        
        # 获取流程专用Session（同一流程复用同一个Session）
        http_client = api.http_client
        flow_session = http_client.get_flow_session()
        print(f"[RUN_DEVICE_FLOW] 获取流程Session: {id(flow_session)}")
        
        # 1) new device（同步操作，在线程池中执行）
        step = "new_device"
        print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 获取新设备")
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 如果没有运行中的事件循环，尝试获取当前事件循环
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # 事件循环已关闭，无法执行
                if flow_session:
                    http_client.release_flow_session(flow_session)
                return False, "事件循环已关闭，无法执行任务", {"step": step}
        
        try:
            device = await loop.run_in_executor(None, getANewDevice)
        except RuntimeError as e:
            if "cannot schedule new futures" in str(e):
                if flow_session:
                    http_client.release_flow_session(flow_session)
                return False, f"事件循环已关闭: {e}", {"step": step}
            raise

        # 2) did/iid - 使用 TikTokAPI（通过 HttpClient，异步执行，使用流程Session）
        step = "did_iid"
        print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 开始调用 make_did_iid_async")
        try:
            dev1, device_id = await api.make_did_iid_async(device, session=flow_session)
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: make_did_iid_async 完成, device_id={device_id}")
        except Exception as ex:
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: make_did_iid_async 出错: {ex}")
            if flow_session:
                http_client.release_flow_session(flow_session)
            return False, f"did/iid error: {ex}", {"step": step}
        if not device_id:
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: device_id 为空")
            if flow_session:
                http_client.release_flow_session(flow_session)
            return False, "did/iid empty device_id", {"step": step}

        # 3) alert_check - 使用 TikTokAPI（通过 HttpClient，异步执行，使用流程Session）
        step = "alert_check"
        print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 开始调用 alert_check_async")
        try:
            chk = await api.alert_check_async(dev1, session=flow_session)
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: alert_check_async 完成, chk={chk}")
        except Exception as ex:
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: alert_check_async 出错: {ex}")
            if flow_session:
                http_client.release_flow_session(flow_session)
            return False, f"alert_check error: {ex}", {"step": step, "device_id": device_id}
        if str(chk).lower() != "success":
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: alert_check 失败, chk={chk}")
            if flow_session:
                http_client.release_flow_session(flow_session)
            return False, f"alert_check fail ({chk})", {"step": step, "device_id": device_id}

        # 4) seed - 使用 TikTokAPI（异步执行，使用流程Session）
        step = "seed"
        print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 开始调用 get_seed_async")
        try:
            seed, seed_type = await api.get_seed_async(dev1, session=flow_session)
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: get_seed_async 完成, seed={seed[:20] if seed else None}..., seed_type={seed_type}")
        except Exception as ex:
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: get_seed_async 出错: {ex}")
            if flow_session:
                http_client.release_flow_session(flow_session)
            return False, f"seed error: {ex}", {"step": step, "device_id": device_id}
        if not seed:
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: seed 为空")
            if flow_session:
                http_client.release_flow_session(flow_session)
            return False, "seed empty", {"step": step, "device_id": device_id}

        # 5) token - 使用 TikTokAPI（异步执行，使用流程Session）
        step = "token"
        print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 开始调用 get_token_async")
        try:
            token = await api.get_token_async(dev1, session=flow_session)
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: get_token_async 完成, token={token[:20] if token else None}...")
        except Exception as ex:
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: get_token_async 出错: {ex}")
            if flow_session:
                http_client.release_flow_session(flow_session)
            return False, f"token error: {ex}", {"step": step, "device_id": device_id, "seed_type": seed_type}
        if not token:
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: token 为空")
            if flow_session:
                http_client.release_flow_session(flow_session)
            return False, "token empty", {"step": step, "device_id": device_id, "seed_type": seed_type}

        # 6) 回填 & 落盘（文件写入在线程池中执行）
        dev1["seed"] = seed
        dev1["seed_type"] = seed_type
        dev1["token"] = token
        try:
            await loop.run_in_executor(None, append_device_jsonl, dev1)
        except RuntimeError as e:
            if "cannot schedule new futures" in str(e):
                # 事件循环已关闭，直接同步写入（作为后备方案）
                try:
                    append_device_jsonl(dev1)
                except Exception:
                    pass  # 忽略写入错误
            else:
                raise
        
        # 流程成功完成，释放流程Session（流程结束后重建Session）
        if flow_session:
            http_client.release_flow_session(flow_session)
            print(f"[RUN_DEVICE_FLOW] 流程Session已释放: {id(flow_session)}")
        
        return True, "ok", {"device_id": device_id, "seed_type": seed_type}

    except Exception as ex:
        # 发生异常时也要释放Session
        if flow_session:
            try:
                http_client = _api_instance.http_client if _api_instance else None
                if http_client:
                    http_client.release_flow_session(flow_session)
                    print(f"[RUN_DEVICE_FLOW] 异常时释放流程Session: {id(flow_session)}")
            except Exception:
                pass
        return False, f"EX:{type(ex).__name__}:{ex}", {"step": step, "device_id": device_id, "seed_type": seed_type}

# ===================== 任务执行函数 =====================
async def execute_device_flow_task(task_data: Dict[str, Any]):
    """
    执行设备流程任务（异步）
    
    Args:
        task_data: 任务数据字典，包含 job 信息
    """
    global _proxy, _logger, _stats
    
    print(f"[EXECUTE_TASK] 开始执行设备流程任务: {task_data}")
    
    if _proxy is None or _logger is None or _stats is None:
        print(f"[EXECUTE_TASK] 全局变量未初始化: _proxy={_proxy}, _logger={_logger}, _stats={_stats}")
        return
    
    try:
        # 执行任务
        t0 = time.time()
        ok = False
        msg = ""
        meta = {}
        
        try:
            # 执行业务逻辑（现在是异步函数，直接 await）
            print(f"[EXECUTE_TASK] 开始调用 run_device_flow: {task_data}")
            ok, msg, meta = await run_device_flow(task_data, _proxy)
            print(f"[EXECUTE_TASK] run_device_flow 完成: {task_data}, ok={ok}, msg={msg}")
        except Exception as ex:
            ok = False
            msg = f"EX:{type(ex).__name__}:{ex}\n{traceback.format_exc(limit=1)}"
            print(f"[EXECUTE_TASK] run_device_flow 出错: {task_data}, 错误: {ex}")
        
        elapsed = time.time() - t0
        (_stats.add_ok() if ok else _stats.add_fail())
        _logger.log(f"[proxy={_proxy}] job={task_data} -> {msg} {meta} elapsed={elapsed:.3f}s")
        print(f"[EXECUTE_TASK] 任务完成: {task_data}, elapsed={elapsed:.3f}s")
        
    except Exception as ex:
        if _logger:
            _logger.log(f"[proxy={_proxy}] TASK-EX:{type(ex).__name__}:{ex}")

# 全局变量
_proxy: Optional[str] = None
_api_instance: Optional[TikTokAPI] = None
_logger: Optional[SafeLogger] = None
_stats: Optional[Stats] = None
_task_counter = 0
_task_counter_lock = threading.Lock()
_max_tasks = 0  # 0 表示无限制

def threshold_callback():
    """阈值补给回调（同步函数，应该快速执行）"""
    global _task_counter, _max_tasks, CONFIG
    
    try:
        with _task_counter_lock:
            # 检查是否超过最大任务数
            if _max_tasks > 0 and _task_counter >= _max_tasks:
                print(f"[threshold_callback] 已达到最大任务数: {_task_counter}/{_max_tasks}")
                return []  # 没有更多任务
            
            # 每次补给一批任务（减少批次大小，避免一次性生成太多任务）
            max_concurrent = CONFIG.get("max_concurrent", 1000)
            # 每次只补给 max_concurrent 的一半，避免一次性生成太多
            batch_size = min(max_concurrent // 2, 500)  # 最多500个任务
            if batch_size < 100:
                batch_size = 100  # 最少100个任务
            
            # 确保不超过最大任务数
            if _max_tasks > 0:
                remaining = _max_tasks - _task_counter
                if remaining <= 0:
                    print(f"[threshold_callback] 已达到最大任务数: {_task_counter}/{_max_tasks}")
                    return []
                batch_size = min(batch_size, remaining)
            
            if batch_size <= 0:
                return []
            
            # 快速生成任务列表
            tasks = [{"i": _task_counter + i} for i in range(batch_size)]
            _task_counter += batch_size
            
            print(f"[threshold_callback] 补充 {len(tasks)} 个任务，当前计数: {_task_counter}" + (f"/{_max_tasks}" if _max_tasks > 0 else ""))
            return tasks
    except Exception as e:
        print(f"[ERROR] threshold_callback 执行失败: {e}")
        import traceback
        traceback.print_exc()
        return []

async def task_callback(task_data: Dict[str, Any]):
    """任务执行回调（异步）"""
    # 直接执行任务（message_queue 已经使用 create_task 包装，不会阻塞）
    try:
        # 任务回调被调用，说明已经获取到信号量，可以执行
        print(f"[TASK_CALLBACK] 开始执行任务: {task_data}")
        await execute_device_flow_task(task_data)
        print(f"[TASK_CALLBACK] 任务执行完成: {task_data}")
    except Exception as e:
        # 错误会被 _execute_task 捕获并记录，这里不需要重复处理
        print(f"[TASK_CALLBACK] 任务执行出错: {task_data}, 错误: {e}")
        raise

def main():
    """主函数"""
    global _proxy, _api_instance, _logger, _stats, _max_tasks
    
    # 从配置文件读取代理
    _proxy = CONFIG.get("proxy", "")
    if not _proxy:
        raise SystemExit("配置文件中未设置代理 (message_queue.proxy)")
    
    print(f"使用代理: {_proxy}")
    
    # 创建 TikTokAPI 实例（全局单例，使用全局 HttpClient）
    # 增加重试次数和延迟，以应对代理连接不稳定的情况
    _api_instance = TikTokAPI(
        proxy=_proxy,
        timeout=30,
        max_retries=1,  # 增加重试次数到 5 次
        retry_delay=2.0,  # 重试延迟 2 秒
        pool_initial_size=CONFIG.get("pool_initial_size", 100),
        pool_max_size=CONFIG.get("pool_max_size", 2000),
        pool_grow_step=CONFIG.get("pool_grow_step", 10),
        use_global_client=True
    )
    
    # 创建日志和统计
    _logger = SafeLogger(CONFIG["results_log"], CONFIG.get("log_flush_every", 5000))
    _stats = Stats()
    
    # 设置最大任务数（从环境变量或配置读取，0 表示无限制）
    _max_tasks = int(os.getenv("MAX_TASKS", "0"))
    
    # 创建消息队列
    queue = MessageQueue(
        max_concurrent=CONFIG.get("max_concurrent", 1000),
        threshold_callback=threshold_callback,
        task_callback=task_callback
    )
    
    print(f"启动消息队列: 并发数={CONFIG.get('max_concurrent', 1000)}, 最大任务数={_max_tasks if _max_tasks > 0 else '无限制'}")
    
    # 启动队列
    queue.start()
    
    # 定期打印统计信息
    stats_every = CONFIG.get("stats_every", 5)
    last_ok, last_fail = 0, 0
    
    try:
        while queue.is_running:
            time.sleep(stats_every)
            queue_stats = queue.get_stats()
            ok, fail = _stats.snapshot()
            d_ok, d_fail = ok - last_ok, fail - last_fail
            last_ok, last_fail = ok, fail
            
            # 打印并发数信息
            current_running = queue_stats.get("running_tasks", 0)
            max_concurrent = queue_stats.get("max_concurrent", 0)
            queue_size = queue_stats.get("queue_size", 0)
            completed = queue_stats.get("completed_tasks", 0)
            failed = queue_stats.get("failed_tasks", 0)
            
            # 计算并发率
            concurrency_rate = (current_running / max_concurrent * 100) if max_concurrent > 0 else 0
            
            print(f"\n{'='*80}")
            print(f"[并发监控] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  当前并发数: {current_running}/{max_concurrent} ({concurrency_rate:.1f}%)")
            print(f"  队列大小: {queue_size}")
            print(f"  已完成: {completed}, 失败: {failed}")
            print(f"  成功/失败: {d_ok}/{d_fail} (最近{stats_every}秒)")
            print(f"{'='*80}")
            
            # 如果并发数明显低于配置值，发出警告
            if max_concurrent > 0 and current_running < max_concurrent * 0.8:
                print(f"⚠️  警告: 当前并发数({current_running})低于配置值({max_concurrent})的80%")
                print(f"   可能原因: 任务执行过快、队列任务不足、或任务被阻塞")
            
            print(f"[stats] ok={ok} (+{d_ok}) fail={fail} (+{d_fail}) "
                  f"queue_total={queue_stats['total_tasks']} "
                  f"queue_completed={queue_stats['completed_tasks']} "
                  f"queue_running={queue_stats['running_tasks']} "
                  f"queue_size={queue_stats['queue_size']}")
            
            # 检查是否完成
            if _max_tasks > 0 and queue_stats['completed_tasks'] >= _max_tasks:
                print("达到最大任务数，准备停止...")
                break
                
    except KeyboardInterrupt:
        print("收到中断信号，正在停止...")
    
    # 停止队列
    queue.stop()
    queue.wait()
    
    # 关闭日志
    _logger.close()
    
    # 打印最终统计
    final_stats = queue.get_stats()
    ok, fail = _stats.snapshot()
    print(f"\n最终统计:")
    print(f"  成功: {ok}")
    print(f"  失败: {fail}")
    print(f"  队列总任务: {final_stats['total_tasks']}")
    print(f"  队列完成: {final_stats['completed_tasks']}")
    print(f"  队列失败: {final_stats['failed_tasks']}")

if __name__ == "__main__":
    t0 = time.time()
    main()
    t1 = time.time()
    print(f"\n总耗时: {t1 - t0:.2f} 秒")

