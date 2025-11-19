"""
使用消息队列执行设备流程任务
基于 message_queue.py 重构 async_thread_main.py 的业务逻辑
"""
import os
import sys
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
        sys.stdout.flush()
        # 使用全局 TikTokAPI 实例和队列实例
        global _api_instance, _queue_instance
        if _api_instance is None:
            raise RuntimeError("TikTokAPI 实例未初始化")
        api = _api_instance
        
        # 获取流程专用Session（同一流程复用同一个Session）
        # 在线程池中执行，避免阻塞事件循环
        http_client = api.http_client
        print(f"[RUN_DEVICE_FLOW] 准备获取流程Session...")
        sys.stdout.flush()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                return False, "事件循环已关闭，无法执行任务", {"step": "init"}
        
        # 异步获取 flow_session（使用异步方法，不阻塞）
        print(f"[RUN_DEVICE_FLOW] 开始异步获取流程Session（带超时保护，最多等待5秒）...")
        sys.stdout.flush()
        try:
            flow_session = await asyncio.wait_for(
                http_client.get_flow_session_async(),
                timeout=5.0
            )
            print(f"[RUN_DEVICE_FLOW] 获取流程Session成功: {id(flow_session)}")
            sys.stdout.flush()
        except asyncio.TimeoutError:
            print(f"[RUN_DEVICE_FLOW] 获取流程Session超时（5秒），尝试直接创建新Session...")
            sys.stdout.flush()
            # 超时时直接创建新 session，不等待连接池
            # 使用 curl_cffi.requests 而不是标准 requests，以支持 impersonate 参数
            from curl_cffi import requests as curl_requests
            flow_session = curl_requests.Session()
            flow_session.headers.update({"Connection": "keep-alive"})
            if http_client.proxy:
                flow_session.proxies = {"http": http_client.proxy, "https": http_client.proxy}
            print(f"[RUN_DEVICE_FLOW] 已直接创建新Session: {id(flow_session)}")
            sys.stdout.flush()
        except Exception as e:
            print(f"[RUN_DEVICE_FLOW] 获取流程Session失败: {e}")
            import traceback
            print(f"[RUN_DEVICE_FLOW] 异常堆栈: {traceback.format_exc()}")
            sys.stdout.flush()
            # 失败时也尝试直接创建新 session
            # 使用 curl_cffi.requests 而不是标准 requests，以支持 impersonate 参数
            from curl_cffi import requests as curl_requests
            flow_session = curl_requests.Session()
            flow_session.headers.update({"Connection": "keep-alive"})
            if http_client.proxy:
                flow_session.proxies = {"http": http_client.proxy, "https": http_client.proxy}
            print(f"[RUN_DEVICE_FLOW] 已直接创建新Session: {id(flow_session)}")
            sys.stdout.flush()
        
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
                    try:
                        # 事件循环已关闭，直接同步执行
                        http_client.release_flow_session(flow_session)
                    except Exception:
                        pass
                return False, "事件循环已关闭，无法执行任务", {"step": step}
        
        try:
            device = await loop.run_in_executor(None, getANewDevice)
        except RuntimeError as e:
            if "cannot schedule new futures" in str(e):
                if flow_session:
                    try:
                        await loop.run_in_executor(None, http_client.release_flow_session, flow_session)
                    except Exception:
                        pass
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
                try:
                    await loop.run_in_executor(None, http_client.release_flow_session, flow_session)
                except Exception:
                    pass
            return False, f"did/iid error: {ex}", {"step": step}
        if not device_id:
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: device_id 为空")
            if flow_session:
                try:
                    await loop.run_in_executor(None, http_client.release_flow_session, flow_session)
                except Exception:
                    pass
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
                try:
                    await loop.run_in_executor(None, http_client.release_flow_session, flow_session)
                except Exception:
                    pass
            return False, f"alert_check error: {ex}", {"step": step, "device_id": device_id}
        if str(chk).lower() != "success":
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: alert_check 失败, chk={chk}")
            if flow_session:
                try:
                    await loop.run_in_executor(None, http_client.release_flow_session, flow_session)
                except Exception:
                    pass
            return False, f"alert_check fail ({chk})", {"step": step, "device_id": device_id}

        # 4) seed - 使用 TikTokAPI（异步执行，使用流程Session）
        step = "seed"
        print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 开始调用 get_seed_async")
        try:
            seed, seed_type = await api.get_seed_async(dev1, session=flow_session)
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: get_seed_async 完成, seed={seed[:20] if seed else None}..., seed_type={seed_type}")
        except Exception as ex:
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: get_seed_async 出错: {ex}")
            import traceback
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 异常堆栈: {traceback.format_exc()}")
            sys.stdout.flush()
            # 打印队列状态
            try:
                if _queue_instance:
                    queue_stats = _queue_instance.get_stats()
                    queue_size = queue_stats.get("queue_size", 0)
                    running_tasks = queue_stats.get("running_tasks", 0)
                    completed_tasks = queue_stats.get("completed_tasks", 0)
                    print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 队列状态 - 队列大小: {queue_size}, 正在执行: {running_tasks}, 已完成: {completed_tasks}")
            except Exception as e:
                print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 获取队列状态失败: {e}")
            if flow_session:
                try:
                    await loop.run_in_executor(None, http_client.release_flow_session, flow_session)
                    print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 已释放流程Session: {id(flow_session)}")
                except Exception as e:
                    print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 释放流程Session失败: {e}")
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 返回失败结果: seed error: {ex}")
            sys.stdout.flush()
            return False, f"seed error: {ex}", {"step": step, "device_id": device_id}
        if not seed:
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: seed 为空")
            if flow_session:
                try:
                    await loop.run_in_executor(None, http_client.release_flow_session, flow_session)
                except Exception:
                    pass
            return False, "seed empty", {"step": step, "device_id": device_id}

        # 5) token - 使用 TikTokAPI（异步执行，使用流程Session）
        step = "token"
        print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 开始调用 get_token_async")
        try:
            token = await api.get_token_async(dev1, session=flow_session)
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: get_token_async 完成, token={token[:20] if token else None}...")
            # 打印队列状态
            try:
                if _queue_instance:
                    queue_stats = _queue_instance.get_stats()
                    queue_size = queue_stats.get("queue_size", 0)
                    running_tasks = queue_stats.get("running_tasks", 0)
                    completed_tasks = queue_stats.get("completed_tasks", 0)
                    print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 队列状态 - 队列大小: {queue_size}, 正在执行: {running_tasks}, 已完成: {completed_tasks}")
            except Exception as e:
                print(f"[RUN_DEVICE_FLOW] 步骤 {step}: 获取队列状态失败: {e}")
        except Exception as ex:
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: get_token_async 出错: {ex}")
            if flow_session:
                try:
                    await loop.run_in_executor(None, http_client.release_flow_session, flow_session)
                except Exception:
                    pass
            return False, f"token error: {ex}", {"step": step, "device_id": device_id, "seed_type": seed_type}
        if not token:
            print(f"[RUN_DEVICE_FLOW] 步骤 {step}: token 为空")
            if flow_session:
                try:
                    await loop.run_in_executor(None, http_client.release_flow_session, flow_session)
                except Exception:
                    pass
            return False, "token empty", {"step": step, "device_id": device_id, "seed_type": seed_type}

        # 6) 回填 & 落盘（文件写入在线程池中执行）
        print(f"[RUN_DEVICE_FLOW] 步骤 6: 开始回填和落盘, device_id={device_id}")
        sys.stdout.flush()
        dev1["seed"] = seed
        dev1["seed_type"] = seed_type
        dev1["token"] = token
        try:
            print(f"[RUN_DEVICE_FLOW] 步骤 6: 准备写入文件...")
            sys.stdout.flush()
            await loop.run_in_executor(None, append_device_jsonl, dev1)
            print(f"[RUN_DEVICE_FLOW] 步骤 6: 文件写入完成")
            sys.stdout.flush()
        except RuntimeError as e:
            if "cannot schedule new futures" in str(e):
                # 事件循环已关闭，直接同步写入（作为后备方案）
                print(f"[RUN_DEVICE_FLOW] 步骤 6: 事件循环已关闭，使用同步写入")
                sys.stdout.flush()
                try:
                    append_device_jsonl(dev1)
                    print(f"[RUN_DEVICE_FLOW] 步骤 6: 同步写入完成")
                    sys.stdout.flush()
                except Exception as write_error:
                    print(f"[RUN_DEVICE_FLOW] 步骤 6: 同步写入失败: {write_error}")
                    sys.stdout.flush()
                    pass  # 忽略写入错误
            else:
                print(f"[RUN_DEVICE_FLOW] 步骤 6: 写入文件异常: {e}")
                sys.stdout.flush()
                raise
        except Exception as e:
            print(f"[RUN_DEVICE_FLOW] 步骤 6: 写入文件异常: {e}")
            import traceback
            print(f"[RUN_DEVICE_FLOW] 步骤 6: 异常堆栈: {traceback.format_exc()}")
            sys.stdout.flush()
            # 写入失败不影响流程完成，继续执行
        
        # 流程成功完成，释放流程Session（流程结束后重建Session）
        # 在线程池中执行，避免阻塞事件循环，并添加超时保护
        print(f"[RUN_DEVICE_FLOW] 步骤 7: 准备释放流程Session, flow_session={id(flow_session) if flow_session else None}")
        sys.stdout.flush()
        if flow_session:
            try:
                print(f"[RUN_DEVICE_FLOW] 步骤 7: 开始释放流程Session（带超时保护，最多等待2秒）...")
                sys.stdout.flush()
                # 使用 asyncio.wait_for 添加超时保护，避免阻塞
                await asyncio.wait_for(
                    loop.run_in_executor(None, http_client.release_flow_session, flow_session),
                    timeout=2.0
                )
                print(f"[RUN_DEVICE_FLOW] 步骤 7: 流程Session已释放: {id(flow_session)}")
                sys.stdout.flush()
            except asyncio.TimeoutError:
                print(f"[RUN_DEVICE_FLOW] 步骤 7: 释放流程Session超时（2秒），跳过释放，继续执行")
                sys.stdout.flush()
                # 超时时直接关闭 session，不等待连接池操作
                try:
                    flow_session.close()
                    print(f"[RUN_DEVICE_FLOW] 步骤 7: 已直接关闭Session: {id(flow_session)}")
                    sys.stdout.flush()
                except Exception as close_error:
                    print(f"[RUN_DEVICE_FLOW] 步骤 7: 直接关闭Session失败: {close_error}")
                    sys.stdout.flush()
            except Exception as e:
                print(f"[RUN_DEVICE_FLOW] 步骤 7: 释放流程Session失败: {e}")
                import traceback
                print(f"[RUN_DEVICE_FLOW] 步骤 7: 异常堆栈: {traceback.format_exc()}")
                sys.stdout.flush()
                # 释放失败时也尝试直接关闭 session
                try:
                    flow_session.close()
                    print(f"[RUN_DEVICE_FLOW] 步骤 7: 已直接关闭Session: {id(flow_session)}")
                    sys.stdout.flush()
                except Exception as close_error:
                    print(f"[RUN_DEVICE_FLOW] 步骤 7: 直接关闭Session失败: {close_error}")
                    sys.stdout.flush()
                # 释放失败不影响流程完成，继续执行
        
        # 打印队列状态
        print(f"[RUN_DEVICE_FLOW] 步骤 8: 准备获取队列状态...")
        sys.stdout.flush()
        try:
            if _queue_instance:
                queue_stats = _queue_instance.get_stats()
                queue_size = queue_stats.get("queue_size", 0)
                running_tasks = queue_stats.get("running_tasks", 0)
                completed_tasks = queue_stats.get("completed_tasks", 0)
                failed_tasks = queue_stats.get("failed_tasks", 0)
                print(f"[RUN_DEVICE_FLOW] 步骤 8: 流程完成 - 队列状态: 队列大小={queue_size}, 正在执行={running_tasks}, 已完成={completed_tasks}, 失败={failed_tasks}")
                sys.stdout.flush()
            else:
                print(f"[RUN_DEVICE_FLOW] 步骤 8: _queue_instance 为空，无法获取队列状态")
                sys.stdout.flush()
        except Exception as e:
            print(f"[RUN_DEVICE_FLOW] 步骤 8: 获取队列状态失败: {e}")
            import traceback
            print(f"[RUN_DEVICE_FLOW] 步骤 8: 异常堆栈: {traceback.format_exc()}")
            sys.stdout.flush()
        
        print(f"[RUN_DEVICE_FLOW] [流程完成] 设备流程执行成功，准备返回结果, device_id={device_id}, seed_type={seed_type}")
        sys.stdout.flush()
        result = (True, "ok", {"device_id": device_id, "seed_type": seed_type})
        print(f"[RUN_DEVICE_FLOW] [流程完成] 返回值: {result}")
        sys.stdout.flush()
        return result

    except Exception as ex:
        print(f"[RUN_DEVICE_FLOW] [流程异常] 设备流程执行异常: {type(ex).__name__}: {ex}, step={step}")
        # 发生异常时也要释放Session（在线程池中执行，避免阻塞）
        if flow_session:
            try:
                http_client = _api_instance.http_client if _api_instance else None
                if http_client:
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        try:
                            loop = asyncio.get_event_loop()
                        except RuntimeError:
                            loop = None
                    
                    if loop:
                        # 在线程池中执行，避免阻塞
                        try:
                            await loop.run_in_executor(None, http_client.release_flow_session, flow_session)
                            print(f"[RUN_DEVICE_FLOW] 异常时释放流程Session: {id(flow_session)}")
                        except Exception:
                            pass
                    else:
                        # 如果没有事件循环，直接同步执行
                        try:
                            http_client.release_flow_session(flow_session)
                            print(f"[RUN_DEVICE_FLOW] 异常时释放流程Session: {id(flow_session)}")
                        except Exception:
                            pass
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
    
    print(f"[EXECUTE_TASK] [开始] 开始执行设备流程任务: {task_data}")
    
    if _proxy is None or _logger is None or _stats is None:
        print(f"[EXECUTE_TASK] [错误] 全局变量未初始化: _proxy={_proxy}, _logger={_logger}, _stats={_stats}")
        return
    
    try:
        # 执行任务
        t0 = time.time()
        ok = False
        msg = ""
        meta = {}
        
        try:
            # 执行业务逻辑（现在是异步函数，直接 await）
            print(f"[EXECUTE_TASK] [执行] 开始调用 run_device_flow: {task_data}")
            ok, msg, meta = await run_device_flow(task_data, _proxy)
            print(f"[EXECUTE_TASK] [完成] run_device_flow 完成: {task_data}, ok={ok}, msg={msg}")
            
            # 如果任务失败（ok=False），抛出异常，让 message_queue 正确标记为失败
            if not ok:
                elapsed = time.time() - t0
                _stats.add_fail()
                _logger.log(f"[proxy={_proxy}] job={task_data} -> {msg} {meta} elapsed={elapsed:.3f}s")
                print(f"[EXECUTE_TASK] [失败] 任务失败: {task_data}, elapsed={elapsed:.3f}s, msg={msg}")
                sys.stdout.flush()
                # 抛出异常，让 message_queue 的 _worker 能够捕获并标记为失败
                raise RuntimeError(f"任务执行失败: {msg}")
        except Exception as ex:
            # 如果是我们主动抛出的异常（任务失败），重新抛出
            if isinstance(ex, RuntimeError) and "任务执行失败:" in str(ex):
                raise
            # 其他异常（网络错误、超时等）也抛出，让 message_queue 标记为失败
            ok = False
            msg = f"EX:{type(ex).__name__}:{ex}\n{traceback.format_exc(limit=1)}"
            print(f"[EXECUTE_TASK] [异常] run_device_flow 出错: {task_data}, 错误: {ex}")
            elapsed = time.time() - t0
            _stats.add_fail()
            _logger.log(f"[proxy={_proxy}] job={task_data} -> {msg} {meta} elapsed={elapsed:.3f}s")
            print(f"[EXECUTE_TASK] [异常] 任务异常: {task_data}, elapsed={elapsed:.3f}s, 错误: {ex}")
            sys.stdout.flush()
            # 抛出异常，让 message_queue 的 _worker 能够捕获并标记为失败
            raise
        
        # 任务成功
        elapsed = time.time() - t0
        _stats.add_ok()
        _logger.log(f"[proxy={_proxy}] job={task_data} -> {msg} {meta} elapsed={elapsed:.3f}s")
        print(f"[EXECUTE_TASK] [成功] 任务成功: {task_data}, elapsed={elapsed:.3f}s, ok={ok}")
        sys.stdout.flush()
        
        # 打印队列状态，确认任务是否被统计
        try:
            if _queue_instance:
                queue_stats = _queue_instance.get_stats()
                queue_size = queue_stats.get("queue_size", 0)
                running_tasks = queue_stats.get("running_tasks", 0)
                completed_tasks = queue_stats.get("completed_tasks", 0)
                failed_tasks = queue_stats.get("failed_tasks", 0)
                print(f"[EXECUTE_TASK] [任务完成] 队列状态: 队列大小={queue_size}, 正在执行={running_tasks}, 已完成={completed_tasks}, 失败={failed_tasks}")
                sys.stdout.flush()
        except Exception as e:
            print(f"[EXECUTE_TASK] [任务完成] 获取队列状态失败: {e}")
            sys.stdout.flush()
        
    except Exception as ex:
        # 打印错误信息，但必须重新抛出异常，让 message_queue 的 _worker 能够捕获并标记为失败
        print(f"[EXECUTE_TASK] [异常] 任务执行异常: {type(ex).__name__}: {ex}")
        import traceback
        print(f"[EXECUTE_TASK] [异常] 异常堆栈: {traceback.format_exc()}")
        sys.stdout.flush()
        if _logger:
            _logger.log(f"[proxy={_proxy}] TASK-EX:{type(ex).__name__}:{ex}")
        # 重新抛出异常，让 message_queue 的 _worker 能够捕获并标记为失败
        raise

# 全局变量
_proxy: Optional[str] = None
_api_instance: Optional[TikTokAPI] = None
_logger: Optional[SafeLogger] = None
_stats: Optional[Stats] = None
_task_counter = 0
_task_counter_lock = threading.Lock()
_max_tasks = 0  # 0 表示无限制
_queue_instance: Optional[MessageQueue] = None  # 队列实例，用于获取队列状态

def threshold_callback():
    """阈值补给回调（同步函数，应该快速执行）"""
    global _task_counter, _max_tasks, CONFIG
    
    try:
        print(f"[threshold_callback] 开始执行，当前计数: {_task_counter}, 最大任务数: {_max_tasks}")
        with _task_counter_lock:
            # 检查是否超过最大任务数
            if _max_tasks > 0 and _task_counter >= _max_tasks:
                print(f"[threshold_callback] 已达到最大任务数: {_task_counter}/{_max_tasks}")
                return []  # 没有更多任务
            
            # 每次补给一批任务
            # 为了确保队列中始终有足够的任务，每次补充 max_concurrent 个任务
            max_concurrent = CONFIG.get("max_concurrent", 1000)
            # 每次补充 max_concurrent 个任务，确保队列中始终有足够任务
            batch_size = max_concurrent
            print(f"[threshold_callback] 计算批次大小: {batch_size} (max_concurrent={max_concurrent})")
            
            # 确保不超过最大任务数
            if _max_tasks > 0:
                remaining = _max_tasks - _task_counter
                if remaining <= 0:
                    print(f"[threshold_callback] 已达到最大任务数: {_task_counter}/{_max_tasks}")
                    return []
                batch_size = min(batch_size, remaining)
                print(f"[threshold_callback] 限制批次大小: {batch_size} (剩余: {remaining})")
            
            if batch_size <= 0:
                print(f"[threshold_callback] 批次大小 <= 0，返回空列表")
                return []
            
            # 快速生成任务列表
            tasks = [{"i": _task_counter + i} for i in range(batch_size)]
            _task_counter += batch_size
            
            print(f"[threshold_callback] 补充 {len(tasks)} 个任务，当前计数: {_task_counter}" + (f"/{_max_tasks}" if _max_tasks > 0 else "（无限制）"))
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
        sys.stdout.flush()
        await execute_device_flow_task(task_data)
        print(f"[TASK_CALLBACK] 任务执行完成: {task_data}")
        sys.stdout.flush()
        
        # 打印队列状态，确认任务是否被统计
        try:
            if _queue_instance:
                queue_stats = _queue_instance.get_stats()
                queue_size = queue_stats.get("queue_size", 0)
                running_tasks = queue_stats.get("running_tasks", 0)
                completed_tasks = queue_stats.get("completed_tasks", 0)
                failed_tasks = queue_stats.get("failed_tasks", 0)
                print(f"[TASK_CALLBACK] [任务完成] 队列状态: 队列大小={queue_size}, 正在执行={running_tasks}, 已完成={completed_tasks}, 失败={failed_tasks}")
                sys.stdout.flush()
        except Exception as e:
            print(f"[TASK_CALLBACK] [任务完成] 获取队列状态失败: {e}")
            sys.stdout.flush()
    except Exception as e:
        # 打印错误信息，但必须重新抛出异常，让 message_queue 的 _worker 能够捕获并标记为失败
        print(f"[TASK_CALLBACK] 任务执行出错: {task_data}, 错误: {e}")
        import traceback
        print(f"[TASK_CALLBACK] 异常堆栈: {traceback.format_exc()}")
        sys.stdout.flush()
        
        # 即使出错，也打印队列状态
        try:
            if _queue_instance:
                queue_stats = _queue_instance.get_stats()
                queue_size = queue_stats.get("queue_size", 0)
                running_tasks = queue_stats.get("running_tasks", 0)
                completed_tasks = queue_stats.get("completed_tasks", 0)
                failed_tasks = queue_stats.get("failed_tasks", 0)
                print(f"[TASK_CALLBACK] [任务出错] 队列状态: 队列大小={queue_size}, 正在执行={running_tasks}, 已完成={completed_tasks}, 失败={failed_tasks}")
                sys.stdout.flush()
        except Exception as e2:
            print(f"[TASK_CALLBACK] [任务出错] 获取队列状态失败: {e2}")
            sys.stdout.flush()
        
        # 重新抛出异常，让 message_queue 的 _worker 能够捕获并标记为失败
        raise

def main():
    """主函数"""
    global _proxy, _api_instance, _logger, _stats, _max_tasks, _queue_instance
    
    print("="*80)
    print("设备流程队列程序启动")
    print("="*80)
    
    # 从配置文件读取代理
    _proxy = CONFIG.get("proxy", "")
    if not _proxy:
        raise SystemExit("配置文件中未设置代理 (message_queue.proxy)")
    
    print(f"使用代理: {_proxy}")
    sys.stdout.flush()  # 确保输出立即显示
    
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
    _queue_instance = MessageQueue(
        max_concurrent=CONFIG.get("max_concurrent", 1000),
        threshold_callback=threshold_callback,
        task_callback=task_callback,
        task_timeout=1200.0  # 20 分钟超时（设备流程任务可能较慢）
    )
    queue = _queue_instance
    
    print(f"启动消息队列: 并发数={CONFIG.get('max_concurrent', 1000)}, 最大任务数={_max_tasks if _max_tasks > 0 else '无限制'}")
    sys.stdout.flush()
    
    # 启动队列
    queue.start()
    
    # 等待队列真正启动（等待 is_running 被设置）
    print("等待队列启动...")
    sys.stdout.flush()
    max_wait = 10  # 最多等待10秒
    wait_count = 0
    while not queue.is_running and wait_count < max_wait * 10:
        time.sleep(0.1)
        wait_count += 1
        if wait_count % 10 == 0:  # 每秒打印一次
            print(f"等待队列启动... ({wait_count/10:.1f}秒)")
            sys.stdout.flush()
    if not queue.is_running:
        print("警告: 队列启动超时，但继续运行...")
        sys.stdout.flush()
    else:
        print("队列已启动")
        sys.stdout.flush()
    
    # 定期打印统计信息
    stats_every = CONFIG.get("stats_every", 5)
    last_ok, last_fail = 0, 0
    
    # 立即打印一次初始统计
    print(f"\n{'='*80}")
    print(f"[初始状态] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  队列状态: {'运行中' if queue.is_running else '未运行'}")
    print(f"{'='*80}\n")
    
    try:
        loop_count = 0
        while queue.is_running:
            loop_count += 1
            print(f"[MAIN_LOOP] 主循环第 {loop_count} 次，准备等待 {stats_every} 秒...")
            sys.stdout.flush()
            time.sleep(stats_every)
            print(f"[MAIN_LOOP] 主循环第 {loop_count} 次，等待完成，准备获取统计信息...")
            sys.stdout.flush()
            try:
                queue_stats = queue.get_stats()
                print(f"[MAIN_LOOP] 主循环第 {loop_count} 次，获取统计信息成功: {queue_stats}")
                sys.stdout.flush()
            except Exception as e:
                print(f"[MAIN_LOOP] 主循环第 {loop_count} 次，获取统计信息失败: {e}")
                import traceback
                print(f"[MAIN_LOOP] 异常堆栈: {traceback.format_exc()}")
                sys.stdout.flush()
                continue
            ok, fail = _stats.snapshot()
            d_ok, d_fail = ok - last_ok, fail - last_fail
            last_ok, last_fail = ok, fail
            
            # 打印并发数信息
            current_running = queue_stats.get("running_tasks", 0)
            max_concurrent = queue_stats.get("max_concurrent", 0)
            queue_size = queue_stats.get("queue_size", 0)
            completed = queue_stats.get("completed_tasks", 0)
            failed = queue_stats.get("failed_tasks", 0)
            total_tasks = queue_stats.get("total_tasks", 0)
            
            # 计算并发率
            concurrency_rate = (current_running / max_concurrent * 100) if max_concurrent > 0 else 0
            
            # 如果 max_concurrent 为 0，从配置中获取
            if max_concurrent == 0:
                max_concurrent = CONFIG.get("max_concurrent", 1000)
                concurrency_rate = (current_running / max_concurrent * 100) if max_concurrent > 0 else 0
            
            print(f"\n{'='*80}")
            print(f"[并发监控] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  当前并发数: {current_running}/{max_concurrent} ({concurrency_rate:.1f}%)")
            print(f"  队列大小: {queue_size}")
            print(f"  已完成任务总数: {completed} (成功: {ok}, 失败: {fail})")
            print(f"  队列失败数: {failed}")
            print(f"  成功/失败: {d_ok}/{d_fail} (最近{stats_every}秒)")
            print(f"  总任务数: {queue_stats.get('total_tasks', 0)}")
            print(f"{'='*80}")
            
            # 如果并发数明显低于配置值，发出警告
            if max_concurrent > 0 and current_running < max_concurrent * 0.8:
                print(f"⚠️  警告: 当前并发数({current_running})低于配置值({max_concurrent})的80%")
                print(f"   可能原因: 任务执行过快、队列任务不足、或任务被阻塞")
            
            print(f"[stats] 完成总数={completed} | 成功={ok} (+{d_ok}) | 失败={fail} (+{d_fail}) | "
                  f"队列总任务={queue_stats['total_tasks']} | "
                  f"队列完成={queue_stats['completed_tasks']} | "
                  f"队列失败={queue_stats['failed_tasks']} | "
                  f"正在执行={queue_stats['running_tasks']} | "
                  f"队列大小={queue_stats['queue_size']}")
            
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
    total_completed = final_stats.get('completed_tasks', 0)
    print(f"\n{'='*80}")
    print(f"最终统计:")
    print(f"  完成的任务总数: {total_completed}")
    print(f"  成功: {ok}")
    print(f"  失败: {fail}")
    print(f"  队列总任务数: {final_stats['total_tasks']}")
    print(f"  队列完成数: {final_stats['completed_tasks']}")
    print(f"  队列失败数: {final_stats['failed_tasks']}")
    print(f"{'='*80}")

if __name__ == "__main__":
    t0 = time.time()
    main()
    t1 = time.time()
    print(f"\n总耗时: {t1 - t0:.2f} 秒")

