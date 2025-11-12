# per_proxy_async_executor_heap.py —— 小根堆代理池（等待时间最短优先）+ Sticky Batch + 会话复用
# Python 3.9+

import os
import time
import json
import queue
import random
import threading
import traceback
import asyncio
import heapq
from typing import Any, Dict, Tuple, List, Optional
from dataclasses import dataclass, field

# ===================== 可配参数 =====================
PROXIES_PATH                 = "proxies.txt"         # 每行：socks5h://user:pass@host:port
RESULTS_LOG                  = "state/results.log"
DEVICE_SAVE_PATH             = "state/devices1.jsonl"
TOTAL_JOBS                   = 1000
STATS_EVERY                  = 5
os.makedirs("state", exist_ok=True)

# 代理维度
PER_PROXY_MAX_INFLIGHT       = 1                     # 每代理槽位数（并发）
PER_PROXY_BASE_COOLDOWN      = 5.0                   # 正常完成后的基础冷却（秒）——先从 5s 试起

# 失败退避（指数型，可关/可调）
ENABLE_FAIL_BACKOFF          = True
FAIL_BACKOFF_MULT            = 2.0                   # 连败每次 *2
FAIL_BACKOFF_MAX             = 60.0                  # 退避上限秒
COOLDOWN_JITTER_RANGE        = (0.95, 1.10)          # 轻微抖动，避免同刻拥挤醒来

# 全局维度（建议：少线程 × 多槽位）
WORKER_THREADS               = 64                    # 建议 16~64
SLOTS_PER_THREAD             = 128                    # 建议 32~128（总槽位=线程×槽位）
GLOBAL_QPS                   = 0                     # 0=关闭总限速
LOG_FLUSH_EVERY              = 5000                  # 大缓冲减少 I/O 锁争用

# Sticky Batch（一次租约连跑多单，强力提速）
STICKY_BATCH                 = 5                     # 每次租到一个代理，连续跑 5 单再释放
STICKY_FETCH_TIMEOUT         = 0.2                   # 批内取下一单的等待时间（秒）

# 会话复用（大幅减少 TCP/TLS 握手）
ENABLE_SESSION_POOL          = True
SESSION_POOL_CONN            = 200
SESSION_POOL_MAXSIZE         = 200

# ===================== 你的业务函数导入 =====================
from devices import getANewDevice
from seed_test import get_get_seed
from token_test import get_get_token
from test_10_24 import make_did_iid, alert_check
try:
    from __main__ import make_did_iid, alert_check  # 兼容主脚本声明
except Exception:
    pass

# ===================== 可选：每代理 Session 复用 =====================
_sessions_lock = threading.Lock()
_sessions: Dict[str, Any] = {}  # proxy_url -> requests.Session

def get_session_for_proxy(proxy_url: str):
    if not ENABLE_SESSION_POOL:
        return None
    try:
        import requests
        from requests.adapters import HTTPAdapter
    except Exception:
        return None
    with _sessions_lock:
        s = _sessions.get(proxy_url)
        if s is None:
            s = requests.Session()
            s.proxies = {"http": proxy_url, "https": proxy_url}
            s.mount("http://", HTTPAdapter(pool_connections=SESSION_POOL_CONN, pool_maxsize=SESSION_POOL_MAXSIZE))
            s.mount("https://", HTTPAdapter(pool_connections=SESSION_POOL_CONN, pool_maxsize=SESSION_POOL_MAXSIZE))
            _sessions[proxy_url] = s
        return s

# ===================== 全局令牌桶（可选） =====================
class TokenBucket:
    def __init__(self, qps: float):
        self.qps = float(qps)
        self.capacity = max(1.0, self.qps) if self.qps > 0 else 0.0
        self.tokens = self.capacity
        self.updated = time.time()
        self.lock = threading.Lock()

    def acquire(self):
        if self.qps <= 0:
            return
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.updated
                self.updated = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.qps)
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
            time.sleep(0.001)

global_bucket = TokenBucket(GLOBAL_QPS)

# ===================== 安全写文件、日志、统计 =====================
_write_lock = threading.Lock()
def append_device_jsonl(dev: Dict[str, Any]):
    line = json.dumps(dev, ensure_ascii=False)
    try:
        with _write_lock:
            with open(DEVICE_SAVE_PATH, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception as e:
        print(f"[WARN] 写入 {DEVICE_SAVE_PATH} 失败: {e}")

class SafeLogger:
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

class Stats:
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

# ===================== 小根堆代理池 =====================
_heap_seq_counter = 0
def _next_seq() -> int:
    global _heap_seq_counter
    _heap_seq_counter += 1
    return _heap_seq_counter

@dataclass(order=True)
class _HeapItem:
    ready_ts: float
    seq: int
    slot: Any=field(compare=False)

@dataclass
class _ProxySlot:
    proxy_url: str
    base_cooldown: float
    consec_fail: int = 0

class ProxyPool:
    def __init__(self, proxies: List[str], per_proxy_max_inflight: int, base_cooldown: float):
        assert per_proxy_max_inflight >= 1
        if not proxies:
            raise SystemExit("proxies.txt 为空")

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._heap: List[_HeapItem] = []

        random.shuffle(proxies)
        now = time.time()
        for p in proxies:
            for _ in range(per_proxy_max_inflight):
                slot = _ProxySlot(proxy_url=p, base_cooldown=base_cooldown)
                item = _HeapItem(ready_ts=now, seq=_next_seq(), slot=slot)
                self._heap.append(item)
        heapq.heapify(self._heap)

    def acquire(self) -> "_SlotLease":
        with self._cond:
            while True:
                if not self._heap:
                    self._cond.wait(timeout=0.01)
                    continue
                top = self._heap[0]
                now = time.time()
                wait = top.ready_ts - now
                if wait > 0:
                    self._cond.wait(timeout=wait)
                    continue
                item = heapq.heappop(self._heap)
                return _SlotLease(pool=self, slot=item.slot)

    def _release(self, slot: _ProxySlot, ok: bool):
        # 计算下次 ready_ts：成功=基础冷却；失败=指数退避（可选）+ 抖动
        if ok:
            slot.consec_fail = 0
            cooldown = slot.base_cooldown
        else:
            if ENABLE_FAIL_BACKOFF:
                slot.consec_fail += 1
                cooldown = min(slot.base_cooldown * (FAIL_BACKOFF_MULT ** slot.consec_fail), FAIL_BACKOFF_MAX)
            else:
                cooldown = slot.base_cooldown
        jitter = random.uniform(*COOLDOWN_JITTER_RANGE)
        ready_ts = time.time() + cooldown * jitter

        with self._cond:
            heapq.heappush(self._heap, _HeapItem(ready_ts=ready_ts, seq=_next_seq(), slot=slot))
            self._cond.notify(1)

class _SlotLease:
    """代理槽位租约——用完必须 release(ok) 把它压回堆"""
    def __init__(self, pool: ProxyPool, slot: _ProxySlot):
        self._pool = pool
        self.slot = slot
        self.proxy_url = slot.proxy_url
        self._released = False

    def release(self, ok: bool):
        if self._released:
            return
        self._released = True
        self._pool._release(self.slot, ok)

# ===================== 业务链路（带 session 的薄包装） =====================
def run_device_flow_with_session(job: Dict[str, Any], proxy_url: str, session) -> Tuple[bool, str, Dict]:
    step = "init"
    device_id = None
    seed_type = None
    try:
        # 1) new device
        step = "new_device"
        device = getANewDevice()

        # 2) did/iid
        step = "did_iid"
        try:
            dev1, device_id = make_did_iid(device, proxy=proxy_url, session=session)
        except TypeError:
            dev1, device_id = make_did_iid(device, proxy=proxy_url)
        if not device_id:
            return False, "did/iid empty device_id", {"step": step}

        # 3) alert_check
        step = "alert_check"
        try:
            chk = alert_check(dev1, proxy=proxy_url, session=session)
        except TypeError:
            chk = alert_check(dev1, proxy=proxy_url)
        if str(chk).lower() != "success":
            return False, f"alert_check fail ({chk})", {"step": step, "device_id": device_id}

        # 4) seed
        step = "seed"
        try:
            seed, seed_type = get_get_seed(dev1, proxy=proxy_url, session=session)
        except TypeError:
            seed, seed_type = get_get_seed(dev1, proxy=proxy_url)
        if not seed:
            return False, "seed empty", {"step": step, "device_id": device_id}

        # 5) token
        step = "token"
        try:
            token = get_get_token(dev1, proxy=proxy_url, session=session)
        except TypeError:
            token = get_get_token(dev1, proxy=proxy_url)
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

# ===================== Worker：线程里开 asyncio，槽位执行（Sticky 批处理） =====================
class Worker(threading.Thread):
    def __init__(self, worker_id: int, jobs_q: "queue.Queue", proxy_pool: ProxyPool,
                 logger: SafeLogger, stats: Stats, slots: int):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.jobs_q = jobs_q
        self.pool = proxy_pool
        self.logger = logger
        self.stats = stats
        self.slots = max(1, int(slots))
        self.stop_event = threading.Event()

    def run(self):
        try:
            asyncio.run(self._main())
        except Exception as e:
            self.logger.log(f"[Worker-CRASH#{self.worker_id}] EX:{type(e).__name__}:{e}")

    async def _slot_loop(self, slot_idx: int):
        name = f"worker#{self.worker_id}/slot#{slot_idx}"
        while not self.stop_event.is_set():
            lease: Optional[_SlotLease] = None
            used_proxy = "no-proxy"
            session = None
            try:
                # 全局限速（若开启）
                await asyncio.to_thread(global_bucket.acquire)

                # 1) 申请一个最早可用的代理（阻塞等待）
                lease = await asyncio.to_thread(self.pool.acquire)
                used_proxy = lease.proxy_url

                # 2) Sticky：一次租约内连续跑 K 单
                session = get_session_for_proxy(used_proxy)
                batch_done = 0
                while batch_done < STICKY_BATCH and not self.stop_event.is_set():
                    # 2.1 取任务（批内取任务不长等）
                    try:
                        job = await asyncio.to_thread(self.jobs_q.get, True, STICKY_FETCH_TIMEOUT)
                    except queue.Empty:
                        break

                    t0 = time.time()
                    ok = False; msg = ""; meta = {}

                    try:
                        ok, msg, meta = await asyncio.to_thread(run_device_flow_with_session, job, used_proxy, session)
                    except Exception as ex:
                        ok = False
                        msg = f"EX:{type(ex).__name__}:{ex}\n{traceback.format_exc(limit=1)}"
                    finally:
                        self.jobs_q.task_done()

                    elapsed = time.time() - t0
                    (self.stats.add_ok() if ok else self.stats.add_fail())
                    self.logger.log(f"[{name}][{used_proxy}] job={job} -> {msg} {meta} elapsed={elapsed:.3f}s")
                    batch_done += 1

            except Exception as ex:
                # 批级别异常
                self.logger.log(f"[{name}][{used_proxy}] BATCH-EX:{type(ex).__name__}:{ex}")

            finally:
                # 3) 释放租约：默认按成功释放（短冷却）；如果你想更保守可以按失败释放
                if lease is not None:
                    try:
                        await asyncio.to_thread(lease.release, True)
                    except Exception:
                        pass

    async def _main(self):
        # 为本事件循环设置更大的默认线程池（供 to_thread 使用）
        try:
            from concurrent.futures import ThreadPoolExecutor
            loop = asyncio.get_running_loop()
            loop.set_default_executor(ThreadPoolExecutor(max_workers=max(128, self.slots * 4)))
        except Exception:
            pass

        tasks = [asyncio.create_task(self._slot_loop(i + 1)) for i in range(self.slots)]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass

    def stop(self):
        self.stop_event.set()

# ===================== 主控 =====================
def load_proxies(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        items = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    if not items:
        raise SystemExit("proxies.txt 为空")
    random.shuffle(items)
    return items

def main():
    proxies = load_proxies(PROXIES_PATH)

    proxy_pool = ProxyPool(
        proxies=proxies,
        per_proxy_max_inflight=PER_PROXY_MAX_INFLIGHT,
        base_cooldown=PER_PROXY_BASE_COOLDOWN,
    )

    logger = SafeLogger(RESULTS_LOG, LOG_FLUSH_EVERY)
    stats  = Stats()

    # 全局任务队列
    jobs_q: "queue.Queue" = queue.Queue(maxsize=0)
    for i in range(TOTAL_JOBS):
        jobs_q.put_nowait({"i": i})

    # 启动工作线程
    workers: List[Worker] = []
    for w_id in range(1, WORKER_THREADS + 1):
        w = Worker(worker_id=w_id, jobs_q=jobs_q, proxy_pool=proxy_pool,
                   logger=logger, stats=stats, slots=SLOTS_PER_THREAD)
        w.start()
        workers.append(w)

    # 定时打印统计
    last_ok, last_fail = 0, 0
    try:
        while True:
            if jobs_q.unfinished_tasks == 0:
                break
            time.sleep(STATS_EVERY)
            ok, fail = stats.snapshot()
            d_ok, d_fail = ok - last_ok, fail - last_fail
            last_ok, last_fail = ok, fail
            print(f"[stats] ok={ok} (+{d_ok}) fail={fail} (+{d_fail}) "
                  f"inflight_threads={threading.active_count()} remaining_jobs={jobs_q.unfinished_tasks}")
    except KeyboardInterrupt:
        print("Interrupted, stopping workers...")

    for w in workers:
        w.stop()
    logger.close()

if __name__ == "__main__":
    t0 = time.time()
    main()
    t1 = time.time()
    print("总耗时===>", t1 - t0)
