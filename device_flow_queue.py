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
try:
    from test_10_24 import make_did_iid, alert_check
except ImportError:
    # 兼容性导入
    try:
        from __main__ import make_did_iid, alert_check
    except ImportError:
        raise ImportError("无法导入 make_did_iid 和 alert_check")

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
def run_device_flow(job: Dict[str, Any], proxy_url: str) -> Tuple[bool, str, Dict]:
    """
    执行设备流程（业务逻辑）
    使用 TikTokAPI 进行请求
    
    Args:
        job: 任务数据
        proxy_url: 代理地址
    
    Returns:
        (成功标志, 消息, 元数据)
    """
    step = "init"
    device_id = None
    seed_type = None
    try:
        # 使用全局 TikTokAPI 实例
        global _api_instance
        if _api_instance is None:
            raise RuntimeError("TikTokAPI 实例未初始化")
        api = _api_instance
        
        # 1) new device
        step = "new_device"
        device = getANewDevice()

        # 2) did/iid
        step = "did_iid"
        try:
            dev1, device_id = make_did_iid(device, proxy=proxy_url)
        except TypeError:
            dev1, device_id = make_did_iid(device, proxy=proxy_url)
        if not device_id:
            return False, "did/iid empty device_id", {"step": step}

        # 3) alert_check
        step = "alert_check"
        try:
            chk = alert_check(dev1, proxy=proxy_url)
        except TypeError:
            chk = alert_check(dev1, proxy=proxy_url)
        if str(chk).lower() != "success":
            return False, f"alert_check fail ({chk})", {"step": step, "device_id": device_id}

        # 4) seed - 使用 TikTokAPI
        step = "seed"
        try:
            seed, seed_type = api.get_seed(dev1)
        except Exception as ex:
            return False, f"seed error: {ex}", {"step": step, "device_id": device_id}
        if not seed:
            return False, "seed empty", {"step": step, "device_id": device_id}

        # 5) token - 使用 TikTokAPI
        step = "token"
        try:
            token = api.get_token(dev1)
        except Exception as ex:
            return False, f"token error: {ex}", {"step": step, "device_id": device_id, "seed_type": seed_type}
        if not token:
            return False, "token empty", {"step": step, "device_id": device_id, "seed_type": seed_type}

        # 6) 回填 & 落盘
        dev1["seed"] = seed
        dev1["seed_type"] = seed_type
        dev1["token"] = token
        append_device_jsonl(dev1)
        return True, "ok", {"device_id": device_id, "seed_type": seed_type}

    except Exception as ex:
        return False, f"EX:{type(ex).__name__}:{ex}", {"step": step, "device_id": device_id, "seed_type": seed_type}

# ===================== 任务执行函数 =====================
async def execute_device_flow_task(task_data: Dict[str, Any]):
    """
    执行设备流程任务（异步）
    
    Args:
        task_data: 任务数据字典，包含 job 信息
    """
    global _proxy, _logger, _stats
    
    if _proxy is None or _logger is None or _stats is None:
        return
    
    try:
        # 执行任务
        t0 = time.time()
        ok = False
        msg = ""
        meta = {}
        
        try:
            # 执行业务逻辑
            ok, msg, meta = await asyncio.to_thread(
                run_device_flow,
                task_data,
                _proxy
            )
        except Exception as ex:
            ok = False
            msg = f"EX:{type(ex).__name__}:{ex}\n{traceback.format_exc(limit=1)}"
        
        elapsed = time.time() - t0
        (_stats.add_ok() if ok else _stats.add_fail())
        _logger.log(f"[proxy={_proxy}] job={task_data} -> {msg} {meta} elapsed={elapsed:.3f}s")
        
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
    """阈值补给回调"""
    global _task_counter, _max_tasks
    
    with _task_counter_lock:
        if _max_tasks > 0 and _task_counter >= _max_tasks:
            return []  # 没有更多任务
        
        # 每次补给一批任务
        batch_size = CONFIG.get("max_concurrent", 1000)
        remaining = _max_tasks - _task_counter if _max_tasks > 0 else batch_size
        batch_size = min(batch_size, remaining) if _max_tasks > 0 else batch_size
        
        tasks = [{"i": _task_counter + i} for i in range(batch_size)]
        _task_counter += batch_size
        
        return tasks if tasks else []

async def task_callback(task_data: Dict[str, Any]):
    """任务执行回调（异步）"""
    # 直接执行任务（message_queue 已经使用 create_task 包装，不会阻塞）
    await execute_device_flow_task(task_data)

def main():
    """主函数"""
    global _proxy, _api_instance, _logger, _stats, _max_tasks
    
    # 从配置文件读取代理
    _proxy = CONFIG.get("proxy", "")
    if not _proxy:
        raise SystemExit("配置文件中未设置代理 (message_queue.proxy)")
    
    print(f"使用代理: {_proxy}")
    
    # 创建 TikTokAPI 实例（全局单例）
    _api_instance = TikTokAPI(
        proxy=_proxy,
        pool_initial_size=CONFIG.get("pool_initial_size", 1000),
        pool_max_size=CONFIG.get("pool_max_size", 5000),
        pool_grow_step=CONFIG.get("pool_grow_step", 50)
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

