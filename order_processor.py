"""
订单处理脚本
从 uni_order 表拉取订单，并发处理视频播放任务

命令行参数：
    python order_processor.py [table_number]
    例如：
    - python order_processor.py 1  # 使用 uni_devices_1 表
    - python order_processor.py 2  # 使用 uni_devices_2 表
"""
import os
import sys
import json
import time
import random
import asyncio
import threading
import argparse
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from mysql_db import MySQLDB
from mysql_pool import MySQLConnectionPool  # 新增：使用连接池
from config_loader import ConfigLoader
from tiktok_api import TikTokAPI
from message_queue import MessageQueue
from redis_client import RedisClient  # 新增：使用Redis缓存
import http_client_async  # 新增：用于清理HTTP session池
import logging

# 配置日志（同时输出到控制台和文件）
def setup_logging():
    """配置日志：同时输出到控制台和文件"""
    # 创建 logs 目录
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 生成日志文件名（带时间戳）
    log_filename = os.path.join(log_dir, f"order_processor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    # 配置日志格式
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # 创建根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # 设置为 DEBUG 以捕获所有阶段日志
    
    # 清除已有的处理器
    root_logger.handlers.clear()
    
    # 1. 控制台处理器（INFO级别）
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(log_format)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # 2. 文件处理器（DEBUG级别，包含所有详细日志）
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(log_format)
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    
    return log_filename

# 设置日志
log_file = None
try:
    log_file = setup_logging()
except Exception as e:
    # 如果日志设置失败，回退到基本配置
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    print(f"警告: 日志文件设置失败: {e}，使用默认控制台输出")

logger = logging.getLogger(__name__)

# 全局变量
_db: Optional[MySQLDB] = None
_api: Optional[TikTokAPI] = None
_redis: Optional[RedisClient] = None  # Redis 客户端实例
_device_fail_count: Dict[str, int] = {}  # 设备连续失败次数，key: device_id, value: 连续失败次数
_device_fail_lock = threading.Lock()  # 设备失败计数锁
_page_size: int = 1000  # 分页大小（从配置文件读取）
_device_fail_threshold: Optional[int] = None  # 设备连续失败阈值，超过此次数后将设备状态更新为4（从配置文件读取）
_thread_pool: Optional[Any] = None  # 专用线程池，用于数据库操作和阻塞IO
_stats_timeout: float = 45.0  # stats 请求超时时间（从配置文件读取，默认45秒）
_request_delay_min: float = 0.05  # 请求之间的最小延迟（从配置文件读取，默认50ms）
_request_delay_max: float = 0.15  # 请求之间的最大延迟（从配置文件读取，默认150ms）

# Redis 键名前缀
REDIS_DEVICE_PLAY_KEY = "tk_play:device:play_num"  # Hash: field=primary_key_id(主键ID), value=增量播放次数
REDIS_ORDER_COMPLETE_KEY = "tk_play:order:complete_num"  # Hash: field=order_id, value=增量完成次数
REDIS_ORDER_NUM_KEY = "tk_play:order:order_num"  # Hash: field=order_id, value=订单总数（缓存，避免频繁查库）
REDIS_ORDER_INFO_KEY = "tk_play:order:info"  # Hash: field=order_id, value=JSON(订单完整信息)
REDIS_DEVICE_STATUS_KEY = "tk_play:device:status_update"  # Hash: field=primary_key_id, value=target_status（设备状态更新队列）
REDIS_PARENT_ORDER_COMPLETE_KEY = "tk_play:parent_order:complete_num"  # Hash: field=parent_order_id, value=父订单完成次数
REDIS_ORDER_COMPLETE_ORDER_NUM_KEY = "tk_play:order:complete_order_num"  # Hash: field=order_id, value=子订单的complate_order_num
REDIS_PARENT_ORDER_SUB_ORDER_NUM_KEY = "tk_play:parent_order:sub_order_num"  # Hash: field=parent_order_id, value=父订单的sub_order_num

# 任务统计（用于监控）
_task_stats = {
    "total_completed": 0,
    "total_success": 0,
    "total_failed": 0,
    "last_check_time": time.time()
}
_task_stats_lock = threading.Lock()
_monitor_thread = None
_monitor_stop_event = threading.Event()

# 订单完成标志（用于取消其他订单检查）
_order_completed_flag = False
_order_completed_lock = threading.Lock()




def device_status_monitor():
    """
    设备状态监控线程
    每30秒检查一次设备状态和任务执行情况
    同时自动清理僵尸设备（状态=使用中但无对应任务）
    """
    global _db_instance, _device_table_name, _queue_instance, _task_stats, _monitor_stop_event
    
    logger.info("[监控线程] 设备状态监控已启动（包含僵尸设备自动清理）")
    
    while not _monitor_stop_event.is_set():
        try:
            # 等待30秒或直到收到停止信号
            if _monitor_stop_event.wait(30):
                break
            
            if _db_instance is None or _device_table_name is None:
                continue
            
            # 查询设备状态统计
            try:
                status_sql = f"""
                SELECT 
                    status,
                    COUNT(*) as count
                FROM {_device_table_name}
                GROUP BY status
                """
                status_results = _db_instance.execute(status_sql, fetch=True)
                
                device_status_map = {
                    0: "可用",
                    1: "使用中",
                    2: "已失效",
                    3: "已完成",
                    4: "连续失败异常"
                }
                
                status_counts = {}
                total = 0
                for row in status_results:
                    status = row['status']
                    count = row['count']
                    status_counts[status] = count
                    total += count
                
                # 构建状态报告
                status_report = []
                for status in [0, 1, 4, 2, 3]:  # 按优先级排序
                    if status in status_counts:
                        count = status_counts[status]
                        name = device_status_map.get(status, f"状态{status}")
                        percentage = (count / total * 100) if total > 0 else 0
                        status_report.append(f"{name}={count}({percentage:.1f}%)")
                
                # 获取任务统计
                with _task_stats_lock:
                    completed = _task_stats["total_completed"]
                    success = _task_stats["total_success"]
                    failed = _task_stats["total_failed"]
                    success_rate = (success / completed * 100) if completed > 0 else 0
                
                # 获取队列状态
                queue_info = ""
                if _queue_instance:
                    queue_stats = _queue_instance.get_stats()
                    queue_size = queue_stats.get("queue_size", 0)
                    running = queue_stats.get("running_tasks", 0)
                    queue_info = f"队列: {queue_size}待处理, {running}运行中 | "
                
                # 检测"僵尸"设备（使用中但没有对应任务的设备）
                zombie_devices = status_counts.get(1, 0) - running
                zombie_warning = ""
                if zombie_devices > 0:
                    zombie_warning = f" ⚠️ 发现{zombie_devices}个僵尸设备（状态=使用中但无任务）"
                
                # 打印监控报告
                logger.info("=" * 100)
                logger.info(f"📊 [设备监控] {queue_info}任务: {completed}完成({success}成功/{failed}失败, 成功率{success_rate:.1f}%) | 设备总数: {total}")
                logger.info(f"📊 [设备状态] {', '.join(status_report)}{zombie_warning}")
                logger.info("=" * 100)
                
                # 批量处理Redis中的设备状态变更请求
                try:
                    if _redis:
                        status_updates = _redis.hgetall(REDIS_DEVICE_STATUS_KEY)
                        if status_updates:
                            update_count = len(status_updates)
                            logger.info(f"🔄 [设备状态批量更新] 发现 {update_count} 个设备需要更新状态")
                            
                            # 按照批次处理（每批200个）
                            batch_size = 200
                            status_items = list(status_updates.items())
                            total_batches = (update_count + batch_size - 1) // batch_size
                            updated_count = 0
                            
                            for batch_idx in range(0, update_count, batch_size):
                                batch_data = status_items[batch_idx:batch_idx + batch_size]
                                current_batch = batch_idx // batch_size + 1
                                
                                try:
                                    # 获取主键字段名
                                    primary_key_field = get_table_primary_key_field(_db_instance, _device_table_name)
                                    
                                    # 构建CASE WHEN批量更新SQL
                                    case_when_parts = []
                                    primary_key_ids = []
                                    
                                    for primary_key_str, target_status_str in batch_data:
                                        try:
                                            primary_key_id = int(primary_key_str)
                                            target_status = int(target_status_str)
                                            case_when_parts.append(f"WHEN {primary_key_id} THEN {target_status}")
                                            primary_key_ids.append(primary_key_id)
                                        except (ValueError, TypeError) as e:
                                            logger.error(f"[设备状态更新] 数据格式错误: primary_key={primary_key_str}, status={target_status_str}, error={e}")
                                    
                                    if case_when_parts and primary_key_ids:
                                        case_when_sql = " ".join(case_when_parts)
                                        ids_list = ",".join(map(str, primary_key_ids))
                                        
                                        update_sql = f"""
                                        UPDATE {_device_table_name}
                                        SET status = (CASE {primary_key_field}
                                            {case_when_sql}
                                            ELSE status
                                        END)
                                        WHERE {primary_key_field} IN ({ids_list})
                                        """
                                        
                                        _db_instance.execute(update_sql)
                                        _db_instance.commit()
                                        updated_count += len(primary_key_ids)
                                        
                                        logger.info(f"✓ [设备状态批量更新] 批次 {current_batch}/{total_batches} 完成（{len(primary_key_ids)}个设备）")
                                    
                                except Exception as batch_error:
                                    logger.error(f"❌ [设备状态批量更新] 批次 {current_batch} 失败: {batch_error}")
                                    import traceback
                                    logger.error(traceback.format_exc())
                            
                            # 更新完成后，清理Redis中的设备状态队列
                            _redis.delete(REDIS_DEVICE_STATUS_KEY)
                            logger.info(f"✅ [设备状态批量更新] 完成 {updated_count}/{update_count} 个设备状态更新，Redis队列已清理")
                        
                except Exception as status_error:
                    logger.error(f"❌ [设备状态批量更新] 处理失败: {status_error}")
                    import traceback
                    logger.error(traceback.format_exc())
                
                # 自动清理僵尸设备（如果存在且超过阈值）
                # 只有当僵尸设备数量超过一定阈值时才清理，避免误杀正在运行的任务
                zombie_threshold = max(50, int(running * 0.3))  # 阈值：50个或运行中任务的30%
                
                if zombie_devices > zombie_threshold:
                    try:
                        logger.warning(f"🔧 [僵尸设备清理] 僵尸设备数量({zombie_devices})超过阈值({zombie_threshold})，开始清理...")
                        
                        # 策略：重置所有status=1且update_time超过2分钟的设备
                        # 这些设备很可能是任务失败但状态未重置的僵尸设备
                        import time
                        five_minutes_ago = int(time.time()) - 120
                        
                        cleanup_sql = f"""
                        UPDATE {_device_table_name}
                        SET status = 0
                        WHERE status = 1 
                        AND update_time < %s
                        """
                        
                        result = _db_instance.execute(cleanup_sql, params=(five_minutes_ago,), fetch=False)
                        _db_instance.commit()
                        
                        # 获取影响的行数
                        cleaned_count = result if isinstance(result, int) else 0
                        
                        if cleaned_count > 0:
                            logger.info(f"✅ [僵尸设备清理] 成功清理 {cleaned_count} 个僵尸设备（update_time > 5分钟）")
                        else:
                            logger.info(f"💡 [僵尸设备清理] 未发现符合条件的僵尸设备（update_time > 5分钟），可能是任务刚开始")
                        
                    except Exception as cleanup_error:
                        logger.error(f"❌ [僵尸设备清理] 清理失败: {cleanup_error}")
                        import traceback
                        logger.error(traceback.format_exc())
                
            except Exception as e:
                logger.error(f"[监控线程] 查询设备状态失败: {e}")
                
        except Exception as e:
            logger.error(f"[监控线程] 运行异常: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    logger.info("[监控线程] 设备状态监控已停止")


def set_device_status_in_redis(primary_key_value: int, target_status: int) -> bool:
    """
    设置设备目标状态到Redis（批量更新队列）
    
    Args:
        primary_key_value: 设备主键ID
        target_status: 目标状态（0=可用, 4=异常等）
    
    Returns:
        是否成功
    """
    global _redis
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化")
        return False
    
    try:
        # 使用HSET设置设备的目标状态
        # 如果同一设备有多次状态变更，只保留最后一次（覆盖）
        _redis.hset(REDIS_DEVICE_STATUS_KEY, str(primary_key_value), str(target_status))
        logger.debug(f"[Redis] 设备 primary_key={primary_key_value} 目标状态={target_status} 已记录到Redis队列")
        return True
    except Exception as e:
        logger.error(f"[Redis] 记录设备状态失败: primary_key={primary_key_value}, status={target_status}, error={e}")
        return False


def increment_device_play_in_redis(primary_key_value: int, amount: int = 1) -> bool:
    """
    增加设备播放次数到Redis
    
    Args:
        primary_key_value: 设备主键ID
        amount: 增量（默认为1）
    
    Returns:
        是否成功
    """
    global _redis
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化")
        return False
    
    try:
        _redis.hincrby(REDIS_DEVICE_PLAY_KEY, str(primary_key_value), amount)
        return True
    except Exception as e:
        logger.error(f"[Redis] 增加设备播放次数失败: primary_key_id={primary_key_value}, error={e}")
        return False


def increment_order_complete_in_redis(order_id: int, amount: int = 1) -> bool:
    """
    增加订单完成次数到Redis，同时更新父订单完成次数
    
    Args:
        order_id: 订单ID
        amount: 增量（默认为1）
    
    Returns:
        是否成功
    """
    global _redis
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化")
        return False
    
    try:
        _redis.hincrby(REDIS_ORDER_COMPLETE_KEY, str(order_id), amount)
        # 注意：父订单完成次数不应该在这里增加，应该在子订单真正完成（status=2）时增加
        # 这样可以确保每个子订单只计算一次，而不是每次任务完成都计算
        return True
    except Exception as e:
        logger.error(f"[Redis] 增加订单完成次数失败: order_id={order_id}, error={e}")
        return False


def get_order_complete_from_redis(order_id: int) -> int:
    """
    从Redis获取订单完成次数
    
    Args:
        order_id: 订单ID
    
    Returns:
        订单完成次数（如果Redis中没有，返回0）
    """
    global _redis
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化")
        return 0
    
    try:
        value = _redis.hget(REDIS_ORDER_COMPLETE_KEY, str(order_id))
        if value is None:
            return 0
        return int(value)
    except Exception as e:
        logger.error(f"[Redis] 获取订单完成次数失败: order_id={order_id}, error={e}")
        return 0


def increment_parent_order_complete_in_redis(parent_order_id: int, amount: int = 1) -> bool:
    """
    增加父订单完成次数到Redis
    
    Args:
        parent_order_id: 父订单ID
        amount: 增量（默认为1）
    
    Returns:
        是否成功
    """
    global _redis
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化")
        return False
    
    try:
        # 如果不存在则创建，存在则增加
        if not _redis.hexists(REDIS_PARENT_ORDER_COMPLETE_KEY, str(parent_order_id)):
            _redis.hset(REDIS_PARENT_ORDER_COMPLETE_KEY, str(parent_order_id), amount)
        else:
            _redis.hincrby(REDIS_PARENT_ORDER_COMPLETE_KEY, str(parent_order_id), amount)
        return True
    except Exception as e:
        logger.error(f"[Redis] 增加父订单完成次数失败: parent_order_id={parent_order_id}, error={e}")
        return False


def get_parent_order_complete_from_redis(parent_order_id: int) -> int:
    """
    从Redis获取父订单完成次数
    
    Args:
        parent_order_id: 父订单ID
    
    Returns:
        父订单完成次数（如果Redis中没有，返回0）
    """
    global _redis
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化")
        return 0
    
    try:
        value = _redis.hget(REDIS_PARENT_ORDER_COMPLETE_KEY, str(parent_order_id))
        if value is None:
            return 0
        return int(value)
    except Exception as e:
        logger.error(f"[Redis] 获取父订单完成次数失败: parent_order_id={parent_order_id}, error={e}")
        return 0


def check_and_update_parent_order_completion(order_id: int, db: MySQLDB) -> bool:
    """
    检查并更新父订单完成状态
    
    Args:
        order_id: 子订单ID
        db: 数据库实例
    
    Returns:
        是否更新了父订单状态
    """
    global _redis
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化")
        return False
    
    try:
        # 1. 从Redis获取订单信息，查找 parent_order_id
        order_info = get_order_info_from_redis(order_id)
        if not order_info:
            logger.warning(f"[父订单检查] 订单 {order_id} 在Redis中不存在，跳过父订单检查")
            return False
        
        parent_order_id = order_info.get('parent_order_id')
        if not parent_order_id:
            # 没有父订单，不需要检查
            return False
        
        # 2. 从Redis获取父订单的 sub_order_num（这个值来自子订单的 sub_order_num）
        sub_order_num_value = _redis.hget(REDIS_PARENT_ORDER_SUB_ORDER_NUM_KEY, str(parent_order_id))
        if sub_order_num_value is None:
            logger.warning(f"[父订单检查] 父订单 {parent_order_id} 的 sub_order_num 未在Redis中找到，尝试从当前订单读取...")
            # 尝试从当前订单（子订单）读取 sub_order_num
            try:
                sub_order_num = order_info.get('sub_order_num', 0) or 0
                if sub_order_num > 0:
                    _redis.hset(REDIS_PARENT_ORDER_SUB_ORDER_NUM_KEY, str(parent_order_id), sub_order_num)
                    logger.debug(f"[父订单检查] ✓ 从当前订单 {order_id} 读取 sub_order_num={sub_order_num} 并存入 Redis（key=parent_order_id={parent_order_id}）")
                else:
                    logger.warning(f"[父订单检查] 当前订单 {order_id} 的 sub_order_num={sub_order_num} <= 0，跳过父订单检查")
                    return False
            except Exception as e:
                logger.error(f"[父订单检查] 从当前订单 {order_id} 读取 sub_order_num 失败: {e}")
                return False
        else:
            sub_order_num = int(sub_order_num_value)
        
        # 确保 sub_order_num 有值
        if sub_order_num <= 0:
            logger.debug(f"[父订单检查] 父订单 {parent_order_id} 的 sub_order_num={sub_order_num} <= 0，无需检查完成状态")
            return False
        
        # 3. 获取父订单的当前完成次数
        parent_complete_num = get_parent_order_complete_from_redis(parent_order_id)
        
        logger.info(f"[父订单检查] 父订单 {parent_order_id}: 完成次数={parent_complete_num}, 需要完成次数={sub_order_num}")
        
        # 4. 检查是否达到完成条件（完成次数大于等于 sub_order_num）
        if parent_complete_num >= sub_order_num:
            logger.info(f"[父订单检查] ✅ 父订单 {parent_order_id} 达到完成条件（{parent_complete_num} >= {sub_order_num}），开始更新状态...")
            # 5. 更新 uni_order 表中所有 parent_order_id = parent_order_id 的记录的 status = 2
            try:
                db.update("uni_order", {"status": 2}, "parent_order_id = %s", (parent_order_id,))
                db.commit()
                logger.info(f"[父订单检查] ✅ 父订单 {parent_order_id} 已完成（完成次数={parent_complete_num} >= sub_order_num={sub_order_num}），已更新 uni_order 表中所有 parent_order_id={parent_order_id} 的记录状态为 2")
                
                # 6. 同时更新 uni_job_order 表中 order_id = parent_order_id 的记录的 status = 2 和 complate_time
                try:
                    # complate_time 是 int(11) 类型，使用时间戳（整数）
                    # 注意：int(11) 在 MySQL 中是有符号整数，范围是 -2147483648 到 2147483647
                    # 如果字段是无符号整数，范围是 0 到 4294967295
                    # 当前时间戳（2025年）大约是 1735000000，在范围内
                    complate_time = int(datetime.now().timestamp())
                    logger.debug(f"[父订单检查] 准备更新 status=2, complate_time={complate_time} (类型: {type(complate_time).__name__})")
                    
                    # 同时更新 status 和 complate_time
                    update_result = db.update("uni_job_order", {"status": 2, "complate_time": complate_time}, "order_id = %s", (parent_order_id,))
                    db.commit()
                    logger.info(f"[父订单检查] ✅ 已更新 uni_job_order 表中 order_id={parent_order_id} 的记录: status=2, complate_time={complate_time}（影响行数: {update_result if update_result is not None else '未知'}）")
                except Exception as job_e:
                    logger.error(f"[父订单检查] ❌ 更新 uni_job_order 表失败: {job_e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    # 即使更新 uni_job_order 失败，也不影响整体流程，继续执行
                
                return True
            except Exception as e:
                logger.error(f"[父订单检查] 更新父订单 {parent_order_id} 状态失败: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return False
        
        return False
        
    except Exception as e:
        logger.error(f"[父订单检查] 检查父订单完成状态失败: order_id={order_id}, error={e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def set_order_num_to_redis(order_id: int, order_num: int) -> bool:
    """
    设置订单总数到Redis（缓存，避免频繁查库）
    
    Args:
        order_id: 订单ID
        order_num: 订单总数
    
    Returns:
        是否成功
    """
    global _redis
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化")
        return False
    
    try:
        _redis.hset(REDIS_ORDER_NUM_KEY, str(order_id), order_num)
        logger.debug(f"✓ 订单 {order_id} 总数已缓存到Redis: order_num={order_num}")
        return True
    except Exception as e:
        logger.error(f"[Redis] 设置订单总数失败: order_id={order_id}, error={e}")
        return False


def get_order_num_from_redis(order_id: int) -> Optional[int]:
    """
    从Redis获取订单总数
    
    Args:
        order_id: 订单ID
    
    Returns:
        订单总数（如果Redis中没有，返回None，表示需要从数据库获取）
    """
    global _redis
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化")
        return None
    
    try:
        value = _redis.hget(REDIS_ORDER_NUM_KEY, str(order_id))
        if value is None:
            return None
        return int(value)
    except Exception as e:
        logger.error(f"[Redis] 获取订单总数失败: order_id={order_id}, error={e}")
        return None


def load_orders_to_redis(db: MySQLDB) -> int:
    """
    从数据库加载所有待处理订单到Redis（增量加载）
    如果Redis中已有订单数据则保留，没有才从数据库加载
    
    Args:
        db: 数据库实例
    
    Returns:
        加载的订单数量
    """
    global _redis
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化")
        return 0
    
    try:
        # 查询所有待处理订单（status IN (0, 1)）
        orders = db.select(
            "uni_order",
            where="status IN (0, 1)",
            order_by="id ASC"
        )
        
        if not orders:
            logger.warning("[订单加载] 没有找到待处理订单")
            return 0
        
        logger.info(f"[订单加载] 从数据库查询到 {len(orders)} 个待处理订单，开始检查并加载到Redis...")
        
        loaded_count = 0
        completed_count = 0
        skipped_count = 0  # Redis中已有的订单
        
        for order in orders:
            order_id = order['id']
            order_num = order.get('order_num', 0) or 0
            complete_num = order.get('complete_num', 0) or 0
            order_status = order.get('status', 0)
            
            # 🔍 关键：检查Redis中是否已有该订单
            redis_has_order = _redis.hexists(REDIS_ORDER_INFO_KEY, str(order_id))
            
            if redis_has_order:
                # Redis中已有该订单，保留Redis数据（可能包含运行中的进度）
                redis_complete_num = get_order_complete_from_redis(order_id)
                logger.info(f"[订单加载] 🔄 订单 {order_id} 已在Redis中，保留Redis数据（complete_num={redis_complete_num}，数据库={complete_num}）")
                skipped_count += 1
                
                # 如果订单有 parent_order_id，确保父订单相关数据已存入 Redis
                parent_order_id = order.get('parent_order_id')
                if parent_order_id:
                    # 直接读取当前订单（子订单）的 sub_order_num
                    sub_order_num = order.get('sub_order_num', 0) or 0
                    if sub_order_num > 0:
                        # 检查 sub_order_num 是否已存入 Redis（以 parent_order_id 为 key）
                        if not _redis.hexists(REDIS_PARENT_ORDER_SUB_ORDER_NUM_KEY, str(parent_order_id)):
                            _redis.hset(REDIS_PARENT_ORDER_SUB_ORDER_NUM_KEY, str(parent_order_id), sub_order_num)
                            logger.debug(f"[订单加载] ✓ 订单 {order_id} 的 sub_order_num={sub_order_num} 已存入 Redis（key=parent_order_id={parent_order_id}）")
                    
                    # 确保父订单完成次数已初始化（如果不存在则创建）
                    if not _redis.hexists(REDIS_PARENT_ORDER_COMPLETE_KEY, str(parent_order_id)):
                        _redis.hset(REDIS_PARENT_ORDER_COMPLETE_KEY, str(parent_order_id), 0)
                        logger.debug(f"[订单加载] ✓ 初始化父订单 {parent_order_id} 的完成次数为 0")
                
                # 检查Redis中的订单是否已完成
                if order_num > 0 and redis_complete_num >= order_num:
                    logger.info(f"[订单加载] 🎉 订单 {order_id} 在Redis中已完成（{redis_complete_num}/{order_num}），更新数据库状态")
                    db.update("uni_order", {"status": 2}, "id = %s", (order_id,))
                    db.commit()
                    completed_count += 1
                    # 更新Redis中的状态
                    order['status'] = 2
                    order_info_json = json.dumps(order, ensure_ascii=False, default=str)
                    _redis.hset(REDIS_ORDER_INFO_KEY, str(order_id), order_info_json)
                
                continue
            
            # 检查数据库中订单是否已经完成（但状态未更新）
            is_completed = (order_num > 0 and complete_num >= order_num)
            
            if is_completed:
                logger.info(f"[订单加载] 🎉 订单 {order_id} 在数据库中已完成（complete_num={complete_num} >= order_num={order_num}），更新状态为2")
                # 更新数据库状态为2（已完成）
                db.update("uni_order", {"status": 2}, "id = %s", (order_id,))
                db.commit()
                
                # 更新订单对象中的状态
                order['status'] = 2
                order_status = 2
                completed_count += 1
                
                # 已完成的订单不加载到Redis（因为不再需要处理）
                logger.info(f"[订单加载] ⏭️  订单 {order_id} 已完成，跳过加载到Redis")
                continue
            
            # Redis中没有，从数据库加载
            # 1. 存储订单完整信息（JSON格式）
            order_info_json = json.dumps(order, ensure_ascii=False, default=str)
            _redis.hset(REDIS_ORDER_INFO_KEY, str(order_id), order_info_json)
            
            # 2. 存储订单总数
            _redis.hset(REDIS_ORDER_NUM_KEY, str(order_id), order_num)
            
            # 3. 初始化订单完成次数（使用数据库中的值）
            _redis.hset(REDIS_ORDER_COMPLETE_KEY, str(order_id), complete_num)
            
            # 4. 如果订单有 parent_order_id，处理父订单相关数据
            parent_order_id = order.get('parent_order_id')
            if parent_order_id:
                # 4.1. 直接读取当前订单（子订单）的 sub_order_num
                sub_order_num = order.get('sub_order_num', 0) or 0
                if sub_order_num > 0:
                    # 存储子订单的 sub_order_num 到 Redis（以 parent_order_id 为 key，如果不存在则创建，存在则保留）
                    if not _redis.hexists(REDIS_PARENT_ORDER_SUB_ORDER_NUM_KEY, str(parent_order_id)):
                        _redis.hset(REDIS_PARENT_ORDER_SUB_ORDER_NUM_KEY, str(parent_order_id), sub_order_num)
                        logger.debug(f"[订单加载] ✓ 订单 {order_id} 的 sub_order_num={sub_order_num} 已存入 Redis（key=parent_order_id={parent_order_id}）")
                
                # 初始化父订单完成次数（如果不存在则创建，存在则保留）
                if not _redis.hexists(REDIS_PARENT_ORDER_COMPLETE_KEY, str(parent_order_id)):
                    _redis.hset(REDIS_PARENT_ORDER_COMPLETE_KEY, str(parent_order_id), 0)
                    logger.debug(f"[订单加载] ✓ 初始化父订单 {parent_order_id} 的完成次数为 0")
            
            loaded_count += 1
            logger.debug(f"[订单加载] ✓ 订单 {order_id}: order_num={order_num}, complete_num={complete_num}, status={order_status}（从数据库加载）")
        
        logger.info(f"[订单加载] ✅ 加载完成：新加载={loaded_count}, Redis已有={skipped_count}, 已完成={completed_count}, 总计={len(orders)}")
        return loaded_count + skipped_count  # 返回总的有效订单数
        
    except Exception as e:
        logger.error(f"[订单加载] 加载订单到Redis失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


def get_order_info_from_redis(order_id: int) -> Optional[Dict[str, Any]]:
    """
    从Redis获取订单完整信息
    
    Args:
        order_id: 订单ID
    
    Returns:
        订单信息字典，如果不存在返回None
    """
    global _redis
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化")
        return None
    
    try:
        order_info_json = _redis.hget(REDIS_ORDER_INFO_KEY, str(order_id))
        if order_info_json is None:
            return None
        
        order_info = json.loads(order_info_json)
        return order_info
    except Exception as e:
        logger.error(f"[Redis] 获取订单信息失败: order_id={order_id}, error={e}")
        return None


def get_all_pending_orders_from_redis() -> List[Dict[str, Any]]:
    """
    从Redis获取所有待处理订单
    
    Returns:
        订单列表
    """
    global _redis
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化")
        return []
    
    try:
        # 获取所有订单信息
        all_orders_data = _redis.hgetall(REDIS_ORDER_INFO_KEY)
        if not all_orders_data:
            return []
        
        orders = []
        for order_id_str, order_info_json in all_orders_data.items():
            try:
                order_info = json.loads(order_info_json)
                # 只返回待处理订单（status IN (0, 1)）
                if order_info.get('status', 0) in (0, 1):
                    orders.append(order_info)
            except Exception as e:
                logger.error(f"[Redis] 解析订单 {order_id_str} 信息失败: {e}")
        
        # 按订单ID排序
        orders.sort(key=lambda x: x.get('id', 0))
        return orders
        
    except Exception as e:
        logger.error(f"[Redis] 获取所有待处理订单失败: {e}")
        return []


def check_and_update_order_completion(order_id: int, db: MySQLDB) -> Tuple[bool, int, int]:
    """
    检查订单是否完成，如果完成则更新订单状态为2，并刷新Redis到数据库
    完全基于 Redis 数据，不查询数据库
    
    Args:
        order_id: 订单ID
        db: 数据库实例
    
    Returns:
        (是否完成, order_num, complete_num)
    """
    global _order_completed_flag, _order_completed_lock
    
    try:
        # 0. 检查是否已经有其他检查完成了订单
        with _order_completed_lock:
            if _order_completed_flag:
                logger.debug(f"[订单完成检查] 订单已被其他检查标记为完成，跳过本次检查")
                return False, 0, 0
        
        # 1. 从Redis获取订单信息
        order_info = get_order_info_from_redis(order_id)
        if not order_info:
            logger.warning(f"[订单完成检查] 订单 {order_id} 在Redis中不存在，尝试从数据库重新加载...")
            # 从数据库重新加载订单信息到Redis
            try:
                order_info = db.select_one("uni_order", where="id = %s", where_params=(order_id,))
                if order_info:
                    # 保存订单信息到Redis
                    order_info_json = json.dumps(order_info, ensure_ascii=False, default=str)
                    _redis.hset(REDIS_ORDER_INFO_KEY, str(order_id), order_info_json)
                    
                    # 保存order_num到Redis
                    order_num = order_info.get('order_num', 0) or 0
                    _redis.hset(REDIS_ORDER_NUM_KEY, str(order_id), order_num)
                    
                    # 保存complete_num到Redis
                    complete_num = order_info.get('complete_num', 0) or 0
                    _redis.hset(REDIS_ORDER_COMPLETE_KEY, str(order_id), complete_num)
                    
                    logger.info(f"[订单完成检查] ✅ 订单 {order_id} 信息已重新加载到Redis: order_num={order_num}, complete_num={complete_num}")
                else:
                    logger.error(f"[订单完成检查] 订单 {order_id} 在数据库中不存在")
                    return False, 0, 0
            except Exception as reload_error:
                logger.error(f"[订单完成检查] 从数据库重新加载订单 {order_id} 失败: {reload_error}")
                return False, 0, 0
        
        # 2. 从Redis获取订单总数和状态
        order_num = order_info.get('order_num', 0) or 0
        order_status = order_info.get('status', 0)
        
        # 3. 从Redis获取完成次数
        current_complete_num = get_order_complete_from_redis(order_id)
        
        logger.debug(f"[订单完成检查] 订单 {order_id}: order_num={order_num}(Redis), "
                    f"complete_num={current_complete_num}(Redis), 状态={order_status}(Redis)")
        
        # 4. 判断订单是否完成（完全基于 Redis 数据）
        if order_num > 0 and current_complete_num >= order_num and order_status != 2:
            # 设置完成标志，阻止其他检查
            with _order_completed_lock:
                if _order_completed_flag:
                    logger.debug(f"[订单完成检查] 订单已被其他检查标记为完成，跳过更新")
                    return False, 0, 0
                _order_completed_flag = True
                logger.info(f"[订单完成检查] ✓ 设置订单完成标志，取消其他检查")
            logger.info(f"[订单完成检查] ✓ 订单 {order_id} 已完成！"
                       f"完成数={current_complete_num}/{order_num}（来自Redis），开始更新状态...")
            
            # 5. 立即刷新 Redis 设备数据到数据库
            global _device_table_name
            if _device_table_name:
                logger.info(f"[订单完成检查] 正在刷新 Redis 设备数据到数据库...")
                flush_stats = flush_redis_to_mysql(db, _device_table_name)
                logger.info(f"[订单完成检查] Redis 刷新完成: {flush_stats}")
                
                # 刷新后清理所有Redis缓存（clear_orders=True: 包括订单缓存）
                if flush_stats['devices_updated'] > 0:
                    clear_redis_cache(clear_orders=True)
                    logger.info(f"[订单完成检查] Redis缓存已清理")
            else:
                logger.warning(f"[订单完成检查] 设备表名未设置，跳过 Redis 刷新")
            
            # 6. 更新订单状态为2（已完成），同时更新complete_num（从Redis读取）
            db.update("uni_order", {"status": 2, "complete_num": current_complete_num}, "id = %s", (order_id,))
            db.commit()
            logger.info(f"[订单完成检查] ✓ 订单 {order_id} 数据库状态已更新为 2（已完成），complete_num={current_complete_num}（来自Redis）")
            
            # 7. 更新Redis中的订单状态和complete_num
            order_info['status'] = 2
            order_info['complete_num'] = current_complete_num
            order_info_json = json.dumps(order_info, ensure_ascii=False, default=str)
            _redis.hset(REDIS_ORDER_INFO_KEY, str(order_id), order_info_json)
            logger.info(f"[订单完成检查] ✓ 订单 {order_id} Redis状态已更新为 2（已完成），complete_num={current_complete_num}")
            
            # 8. 子订单真正完成时，增加父订单完成次数
            order_info_for_parent = get_order_info_from_redis(order_id)
            if order_info_for_parent:
                parent_order_id = order_info_for_parent.get('parent_order_id')
                if parent_order_id:
                    # 子订单完成时，父订单完成次数+1（每个子订单只计算一次）
                    increment_parent_order_complete_in_redis(parent_order_id, amount=1)
                    updated_count = get_parent_order_complete_from_redis(parent_order_id)
                    logger.info(f"[订单完成] 子订单 {order_id} 已完成，父订单 {parent_order_id} 完成次数+1，当前完成次数={updated_count}")
            
            # 9. 检查并更新父订单完成状态
            check_and_update_parent_order_completion(order_id, db)
            
            # 10. 立即设置订单完成停止标志，停止添加新任务（但保持队列中运行和等待的任务）
            global _order_complete_stop_flag, _queue_instance
            with _order_complete_stop_lock:
                _order_complete_stop_flag = True
                logger.info(f"[订单完成] ✅ 订单 {order_id} 已完成，已立即设置停止标志，将停止添加新任务，等待队列中任务完成...")
            
            # 11. 立即停止队列的阈值补充机制（通过设置 is_stopped 标志）
            # 这样可以确保 MessageQueue 不会再触发补充检查
            if _queue_instance:
                try:
                    # 设置队列的停止标志，阻止阈值补充
                    _queue_instance.is_stopped = True
                    logger.info(f"[订单完成] ✅ 已立即停止队列的阈值补充机制")
                except Exception as e:
                    logger.warning(f"[订单完成] 停止队列补充机制失败: {e}")
            
            return True, order_num, current_complete_num
        
        return False, order_num, current_complete_num
        
    except Exception as e:
        logger.error(f"[订单完成检查] 检查订单 {order_id} 失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False, 0, 0


def flush_redis_to_mysql(db_instance: MySQLDB, device_table_name: str) -> Dict[str, int]:
    """
    批量刷新Redis设备数据到MySQL
    注意：订单的complete_num只在订单完成时更新，不在此批量刷新
    
    Args:
        db_instance: 数据库实例
        device_table_name: 设备表名
    
    Returns:
        刷新统计信息: {"devices_updated": 0, "devices_failed": 0}
    """
    global _redis
    stats = {
        "devices_updated": 0,
        "devices_failed": 0
    }
    
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化，无法刷新数据")
        return stats
    
    start_time = time.time()
    logger.info(f"[Redis刷新] 开始批量刷新Redis设备数据到MySQL...")
    
    # 1. 刷新设备播放次数
    try:
        device_play_data = _redis.hgetall(REDIS_DEVICE_PLAY_KEY)
        if device_play_data:
            total_devices = len(device_play_data)
            logger.info(f"[Redis刷新] 发现 {total_devices} 个设备需要更新播放次数")
            
            # 获取主键字段名
            primary_key_field = get_table_primary_key_field(db_instance, device_table_name)
            
            # 批量更新：使用CASE WHEN语句，每批200个（减小批次避免单次SQL过长）
            batch_size = 1000
            device_items = list(device_play_data.items())
            total_batches = (total_devices + batch_size - 1) // batch_size
            
            logger.info(f"[Redis刷新] 设备数据将分为 {total_batches} 批次处理，每批 {batch_size} 个")
            
            for batch_idx in range(0, total_devices, batch_size):
                batch_data = device_items[batch_idx:batch_idx + batch_size]
                current_batch = batch_idx // batch_size + 1
                batch_start_time = time.time()
                
                try:
                    # 构建CASE WHEN语句进行批量更新
                    case_when_parts = []
                    primary_key_ids = []
                    
                    for primary_key_id_str, increment_str in batch_data:
                        try:
                            primary_key_id = int(primary_key_id_str)
                            increment = int(increment_str)
                            if increment > 0:
                                case_when_parts.append(f"WHEN {primary_key_id} THEN {increment}")
                                primary_key_ids.append(primary_key_id)
                        except (ValueError, TypeError) as e:
                            logger.error(f"[Redis刷新] 数据格式错误: primary_key_id={primary_key_id_str}, increment={increment_str}, error={e}")
                            stats["devices_failed"] += 1
                    
                    if case_when_parts and primary_key_ids:
                        # 构建批量更新SQL
                        case_when_sql = " ".join(case_when_parts)
                        ids_list = ",".join(map(str, primary_key_ids))
                        
                        update_sql = f"""
                        UPDATE {device_table_name}
                        SET play_num = play_num + (CASE {primary_key_field}
                            {case_when_sql}
                            ELSE 0
                        END)
                        WHERE {primary_key_field} IN ({ids_list})
                        """
                        
                        # 执行批量更新
                        logger.info(f"[Redis刷新] 正在处理批次 {current_batch}/{total_batches}（设备数: {len(primary_key_ids)}）")
                        db_instance.execute(update_sql)
                        
                        # 每批立即commit，避免长事务锁定
                        db_instance.commit()
                        
                        batch_elapsed = time.time() - batch_start_time
                        stats["devices_updated"] += len(primary_key_ids)
                        logger.info(f"[Redis刷新] ✓ 批次 {current_batch}/{total_batches} 完成，耗时 {batch_elapsed:.2f}秒（已完成: {stats['devices_updated']}/{total_devices}）")
                        
                        # 批次间短暂延迟，避免数据库压力过大（50ms）
                        if current_batch < total_batches:
                            time.sleep(0.05)
                        
                except Exception as e:
                    logger.error(f"[Redis刷新] ✗ 批量更新设备播放次数失败（批次{current_batch}/{total_batches}）: {e}")
                    stats["devices_failed"] += len(batch_data)
                    import traceback
                    logger.error(traceback.format_exc())
                    
                    # 失败时也尝试commit，避免后续批次受影响
                    try:
                        db_instance.commit()
                    except:
                        pass
            
            logger.info(f"[Redis刷新] 设备播放次数更新完成: 成功={stats['devices_updated']}, 失败={stats['devices_failed']}")
    except Exception as e:
        logger.error(f"[Redis刷新] 刷新设备播放次数时发生异常: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    # 2. 刷新设备状态变更
    try:
        status_updates = _redis.hgetall(REDIS_DEVICE_STATUS_KEY)
        if status_updates:
            total_status_updates = len(status_updates)
            logger.info(f"[Redis刷新] 发现 {total_status_updates} 个设备需要更新状态")
            
            # 按批次处理（每批200个）
            batch_size = 1000
            status_items = list(status_updates.items())
            total_batches = (total_status_updates + batch_size - 1) // batch_size
            status_updated_count = 0
            
            for batch_idx in range(0, total_status_updates, batch_size):
                batch_data = status_items[batch_idx:batch_idx + batch_size]
                current_batch = batch_idx // batch_size + 1
                
                try:
                    # 获取主键字段名
                    primary_key_field = get_table_primary_key_field(db_instance, device_table_name)
                    
                    # 构建CASE WHEN批量更新SQL
                    case_when_parts = []
                    primary_key_ids = []
                    
                    for primary_key_str, target_status_str in batch_data:
                        try:
                            primary_key_id = int(primary_key_str)
                            target_status = int(target_status_str)
                            case_when_parts.append(f"WHEN {primary_key_id} THEN {target_status}")
                            primary_key_ids.append(primary_key_id)
                        except (ValueError, TypeError) as e:
                            logger.error(f"[Redis刷新] 设备状态数据格式错误: primary_key={primary_key_str}, status={target_status_str}, error={e}")
                    
                    if case_when_parts and primary_key_ids:
                        case_when_sql = " ".join(case_when_parts)
                        ids_list = ",".join(map(str, primary_key_ids))
                        
                        update_sql = f"""
                        UPDATE {device_table_name}
                        SET status = (CASE {primary_key_field}
                            {case_when_sql}
                            ELSE status
                        END)
                        WHERE {primary_key_field} IN ({ids_list})
                        """
                        
                        logger.info(f"[Redis刷新] 正在处理设备状态批次 {current_batch}/{total_batches}（设备数: {len(primary_key_ids)}）")
                        db_instance.execute(update_sql)
                        db_instance.commit()
                        status_updated_count += len(primary_key_ids)
                        logger.info(f"[Redis刷新] ✓ 设备状态批次 {current_batch}/{total_batches} 完成（已完成: {status_updated_count}/{total_status_updates}）")
                        
                        # 批次间短暂延迟
                        if current_batch < total_batches:
                            time.sleep(0.05)
                    
                except Exception as batch_error:
                    logger.error(f"[Redis刷新] ✗ 批量更新设备状态失败（批次{current_batch}/{total_batches}）: {batch_error}")
                    import traceback
                    logger.error(traceback.format_exc())
                    
                    try:
                        db_instance.commit()
                    except:
                        pass
            
            stats['status_updated'] = status_updated_count
            logger.info(f"[Redis刷新] 设备状态更新完成: 成功={status_updated_count}/{total_status_updates}")
    except Exception as e:
        logger.error(f"[Redis刷新] 刷新设备状态时发生异常: {e}")
        import traceback
        logger.error(traceback.format_exc())
        stats['status_updated'] = 0
    
    elapsed = time.time() - start_time
    logger.info(f"[Redis刷新] 批量刷新完成，耗时: {elapsed:.2f}秒")
    logger.info(f"[Redis刷新] 统计: 设备播放次数更新={stats['devices_updated']}/{stats['devices_updated']+stats['devices_failed']}, "
                f"设备状态更新={stats.get('status_updated', 0)}")
    
    return stats


def clear_redis_cache(clear_orders: bool = True) -> bool:
    """
    清理Redis缓存（在刷新到MySQL后调用）
    
    Args:
        clear_orders: 是否清理订单相关缓存（默认True）。
                      程序启动时设为False，只清理设备缓存；
                      订单完成时设为True，清理所有缓存。
    
    Returns:
        是否成功
    """
    global _redis
    if _redis is None:
        logger.error("[Redis] Redis客户端未初始化")
        return False
    
    try:
        logger.info(f"[Redis清理] 开始清理Redis缓存...（清理订单: {clear_orders}）")
        
        # 删除设备播放次数缓存
        if _redis.exists(REDIS_DEVICE_PLAY_KEY):
            _redis.delete(REDIS_DEVICE_PLAY_KEY)
            logger.info(f"[Redis清理] 已清理设备播放次数缓存: {REDIS_DEVICE_PLAY_KEY}")
        
        # 删除设备状态更新队列
        if _redis.exists(REDIS_DEVICE_STATUS_KEY):
            _redis.delete(REDIS_DEVICE_STATUS_KEY)
            logger.info(f"[Redis清理] 已清理设备状态更新队列: {REDIS_DEVICE_STATUS_KEY}")
        
        # 只在订单完成时清理订单缓存
        if clear_orders:
            # 删除订单完成次数缓存
            if _redis.exists(REDIS_ORDER_COMPLETE_KEY):
                _redis.delete(REDIS_ORDER_COMPLETE_KEY)
                logger.info(f"[Redis清理] 已清理订单完成次数缓存: {REDIS_ORDER_COMPLETE_KEY}")
            
            # 删除订单总数缓存
            if _redis.exists(REDIS_ORDER_NUM_KEY):
                _redis.delete(REDIS_ORDER_NUM_KEY)
                logger.info(f"[Redis清理] 已清理订单总数缓存: {REDIS_ORDER_NUM_KEY}")
            
            # 删除订单信息缓存
            if _redis.exists(REDIS_ORDER_INFO_KEY):
                _redis.delete(REDIS_ORDER_INFO_KEY)
                logger.info(f"[Redis清理] 已清理订单信息缓存: {REDIS_ORDER_INFO_KEY}")
        else:
            logger.debug("[Redis清理] 保留订单缓存（只在订单完成时清理）")
        
        logger.info("[Redis清理] Redis缓存清理完成")
        return True
    except Exception as e:
        logger.error(f"[Redis清理] 清理Redis缓存失败: {e}")
        return False


def parse_video_ids(order_info: str) -> List[str]:
    """
    解析订单信息中的视频ID列表
    
    Args:
        order_info: 订单信息，格式如 "1,2,3"
    
    Returns:
        视频ID列表
    """
    if not order_info:
        return []
    return [vid.strip() for vid in order_info.split(',') if vid.strip()]


def get_table_create_time_field(db: MySQLDB, table_name: str) -> str:
    """
    获取表的时间字段名（可能是 device_create_time 或 devcie_create_time）
    
    Args:
        db: 数据库连接
        table_name: 表名
    
    Returns:
        字段名
    """
    try:
        sql = f"DESCRIBE {table_name}"
        result = db.execute(sql, fetch=True)
        if result:
            columns = [row['Field'] for row in result]
            # 检查是否存在 device_create_time 或 devcie_create_time
            if 'device_create_time' in columns:
                return 'device_create_time'
            elif 'devcie_create_time' in columns:
                return 'devcie_create_time'
            else:
                logger.warning(f"表 {table_name} 中未找到 device_create_time 或 devcie_create_time 字段")
                return 'device_create_time'  # 默认返回
        return 'device_create_time'
    except Exception as e:
        logger.error(f"获取表 {table_name} 的字段信息失败: {e}")
        return 'device_create_time'  # 默认返回


def get_table_primary_key_field(db: MySQLDB, table_name: str) -> str:
    """
    获取表的主键字段名
    所有表的主键字段都为 'id'
    
    Args:
        db: 数据库连接（保留参数以保持接口兼容性）
        table_name: 表名（保留参数以保持接口兼容性）
    
    Returns:
        主键字段名 'id'
    """
    # 所有表的主键字段都为 'id'，直接返回，避免数据库查询
    return 'id'


def reset_device_status(db: MySQLDB, table_name: str = "uni_devices_1"):
    """
    重置设备状态：将所有 status in (1,3) 的设备更新为 status = 0
    包括：
    - status=1: 进行中的设备
    - status=3: 已完成的设备
    
    Args:
        db: 数据库连接
        table_name: 表名
    """
    try:
        sql = f"""
            UPDATE {table_name}
            SET status = 0
            WHERE status = 1 OR status = 3
        """
        with db.get_cursor() as cursor:
            cursor.execute(sql)
            affected_rows = cursor.rowcount
        logger.info(f"表 {table_name} 重置设备状态完成（status 1,3 -> 0），影响行数: {affected_rows}")
        return affected_rows
    except Exception as e:
        logger.error(f"重置表 {table_name} 设备状态失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


def get_devices_from_table(
    db: MySQLDB,
    table_name: str,
    limit: int = 1000,
    status: int = 0
) -> List[Dict[str, Any]]:
    """
    从设备表中获取设备数据（按 play_num 升序、device_create_time 降序、主键升序排序）
    
    Args:
        db: 数据库连接
        table_name: 表名（如 uni_devices_1）
        limit: 获取数量
        status: 设备状态（默认0）
    
    Returns:
        设备数据列表
    """
    try:
        # 获取时间字段名和主键字段名
        time_field = get_table_create_time_field(db, table_name)
        primary_key_field = get_table_primary_key_field(db, table_name)
        
        # 构建查询SQL：按 play_num 升序、device_create_time 降序、主键升序排序
        # 添加主键排序确保获取不同的设备，避免总是获取同一个设备
        # 优化：只查询需要的字段，而不是 SELECT *
        # 注意：device_id 不是表字段，而是从 device_config JSON 中解析出来的
        # 强制使用复合索引以避免文件排序
        sql = f"""
            SELECT {primary_key_field}, device_config, play_num, status, {time_field}, update_time
            FROM {table_name} USE INDEX (idx_status_playnum_createtime)
            WHERE status = %s
            ORDER BY play_num ASC, {time_field} DESC
            LIMIT %s
        """
        
        result = db.execute(sql, (status, limit), fetch=True)
        return result or []
    except Exception as e:
        logger.error(f"从表 {table_name} 获取设备数据失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []


def update_devices_status(db: MySQLDB, device_ids: List[Any], table_name: str, status: int):
    """
    批量更新设备状态
    
    Args:
        db: 数据库连接
        device_ids: 设备主键ID列表
        table_name: 表名
        status: 要更新的状态值
    """
    if not device_ids:
        return 0
    
    try:
        primary_key_field = get_table_primary_key_field(db, table_name)
        # 使用 IN 子句批量更新
        placeholders = ','.join(['%s'] * len(device_ids))
        sql = f"""
            UPDATE {table_name}
            SET status = %s
            WHERE {primary_key_field} IN ({placeholders})
        """
        
        with db.get_cursor() as cursor:
            cursor.execute(sql, (status, *device_ids))
            affected_rows = cursor.rowcount
        logger.debug(f"表 {table_name} 批量更新设备状态完成，影响行数: {affected_rows}")
        return affected_rows
    except Exception as e:
        logger.error(f"批量更新表 {table_name} 设备状态失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


def get_and_lock_devices(db: MySQLDB, table_name: str, limit: int, status: int = 0, max_retries: int = 3) -> List[Dict[str, Any]]:
    """
    在事务中获取设备并更新状态为1（进行中）
    使用事务确保原子性：先获取设备，然后更新状态
    支持重试机制，应对锁等待超时
    一次性获取所有设备，不再分批处理
    
    Args:
        db: 数据库连接
        table_name: 表名
        limit: 获取数量
        status: 筛选的设备状态（默认0）
        max_retries: 最大重试次数（默认3次）
    
    Returns:
        设备数据列表（已更新状态为1）
    """
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            start_time = time.time()
            # 获取时间字段名和主键字段名
            time_field = get_table_create_time_field(db, table_name)
            primary_key_field = get_table_primary_key_field(db, table_name)
            
            # 开始事务
            conn = db._get_connection()
            # 检查连接是否有效
            try:
                conn.ping(reconnect=False)
            except Exception as e:
                logger.warning(f"[get_and_lock_devices] 数据库连接无效，尝试重新连接: {e}")
                db._close_connection()
                conn = db._get_connection()
            
            original_autocommit = conn.get_autocommit() if hasattr(conn, 'get_autocommit') else (conn.autocommit if isinstance(conn.autocommit, bool) else False)
            conn.autocommit(False)
            
            try:
                with db.get_cursor(conn) as cursor:  # 传递连接给 get_cursor
                    # 步骤1：一次性获取所有设备（按 play_num 升序、device_create_time 降序排序）
                    # 使用乐观锁策略：先 SELECT 再 UPDATE，通过 affected_rows 检查并发冲突
                    # 优化：只查询需要的字段，而不是 SELECT *
                    # 强制使用复合索引以避免文件排序
                    select_sql = f"""
                        SELECT {primary_key_field}, device_config, play_num, status, {time_field}, update_time
                        FROM {table_name} USE INDEX (idx_status_playnum_createtime)
                        WHERE status = %s
                        ORDER BY play_num ASC, {time_field} DESC
                        LIMIT %s
                    """
                    select_start = time.time()
                    logger.info(f"[get_and_lock_devices] 正在一次性获取 {limit} 个设备，表: {table_name}")
                    cursor.execute(select_sql, (status, limit))
                    devices = cursor.fetchall()
                    select_elapsed = time.time() - select_start
                    
                    # 记录查询耗时
                    if select_elapsed > 10.0:
                        logger.warning(f"[get_and_lock_devices] SELECT查询耗时: {select_elapsed:.3f}秒, 数量: {limit}, 表: {table_name}")
                    elif select_elapsed > 5.0:
                        logger.info(f"[get_and_lock_devices] SELECT查询耗时: {select_elapsed:.3f}秒, 数量: {limit}")
                    else:
                        logger.info(f"[get_and_lock_devices] SELECT查询耗时: {select_elapsed:.3f}秒")
                
                    if not devices:
                        conn.rollback()
                        if hasattr(conn, 'close'):
                            conn.close()
                        logger.info(f"[get_and_lock_devices] 没有可用设备 (status={status})，返回空列表")
                        return []
                    
                    # 步骤2：获取设备ID列表
                    device_ids = [device.get(primary_key_field) for device in devices if device.get(primary_key_field) is not None]
                    
                    if not device_ids:
                        conn.rollback()
                        if hasattr(conn, 'close'):
                            conn.close()
                        logger.warning(f"[get_and_lock_devices] 设备主键值为空，返回空列表")
                        return []
                    
                    # 步骤3：更新设备状态为1（进行中）
                    # 添加 status = 0 条件，避免更新已被其他进程改变的设备（乐观锁）
                    placeholders = ','.join(['%s'] * len(device_ids))
                    update_sql = f"""
                        UPDATE {table_name}
                        SET status = 1
                        WHERE {primary_key_field} IN ({placeholders})
                        AND status = 0
                    """
                    cursor.execute(update_sql, device_ids)
                    affected_rows = cursor.rowcount
                    
                    if affected_rows != len(device_ids):
                        logger.warning(f"更新设备状态时，预期更新 {len(device_ids)} 个，实际更新 {affected_rows} 个（可能存在并发竞争）")
                        # 不回滚，提交实际更新的设备
                        if affected_rows == 0:
                            # 所有设备都被其他进程抢占了，回滚并重试
                            conn.rollback()
                            if hasattr(conn, 'close'):
                                conn.close()
                            logger.warning(f"[get_and_lock_devices] 所有设备都被其他进程抢占，将重试 (attempt {retry_count + 1}/{max_retries})")
                            retry_count += 1
                            time.sleep(0.1 * retry_count)  # 指数退避
                            continue
                    
                    # 提交事务
                    conn.commit()
                    
                    # 如果更新的行数少于预期，只返回成功更新的设备
                    if affected_rows < len(devices):
                        logger.warning(f"只有 {affected_rows}/{len(devices)} 个设备成功更新，返回实际成功的设备")
                        # 简化处理：返回前 affected_rows 个设备
                        devices = devices[:affected_rows]
                    
                    total_elapsed = time.time() - start_time
                    logger.info(f"[get_and_lock_devices] 成功获取并锁定 {len(devices)} 个设备，总耗时 {total_elapsed:.2f}秒")
                    
                    return devices
                
            except Exception as e:
                conn.rollback()
                # 检查是否是锁等待超时
                if hasattr(e, 'args') and len(e.args) > 0 and (1205 in str(e.args) or 'Lock wait timeout' in str(e)):
                    logger.warning(f"[get_and_lock_devices] 锁等待超时，将重试 (attempt {retry_count + 1}/{max_retries})")
                    retry_count += 1
                    time.sleep(0.5 * retry_count)  # 指数退避
                    continue
                else:
                    raise e
            finally:
                # 恢复原来的 autocommit 设置
                conn.autocommit(original_autocommit)
                # 归还连接（如果是 ConnectionWrapper）
                if hasattr(conn, 'close'):
                    conn.close()
            
        except Exception as e:
            logger.error(f"在事务中获取并锁定设备失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    # 重试次数用尽
    logger.error(f"[get_and_lock_devices] 重试次数用尽 ({max_retries} 次)，返回空列表")
    return []


def parse_device_config(device_config: str) -> Dict[str, Any]:
    """
    解析设备配置 JSON 字符串
    
    Args:
        device_config: JSON 字符串
    
    Returns:
        解析后的设备配置字典
    """
    try:
        if not device_config:
            return {}
        return json.loads(device_config)
    except Exception as e:
        logger.warning(f"解析 device_config 失败: {e}")
        return {}


async def play_video_task(
    aweme_id: str,
    device: Dict[str, Any],
    device_id: str,
    device_table: str,
    primary_key_value: Any,
    db: MySQLDB,
    api: TikTokAPI,
    http_client,
    order_id: Optional[int] = None
) -> Tuple[bool, str]:
    """
    播放视频任务（异步）
    
    Args:
        aweme_id: 视频ID
        device: 设备信息字典（从 device_config 字段解析得到，包含 seed、seed_type、token 等字段）
        device_id: 设备ID（用于日志标识）
        device_table: 设备所在的表名
        primary_key_value: 主键值（用于数据库更新）
        db: 数据库连接
        api: TikTokAPI 实例
        http_client: HttpClient 实例
        order_id: 订单ID（可选，如果提供则在播放成功后立即更新订单的 complete_num）
    
    Returns:
        Tuple[是否成功, 设备ID]
    """
    flow_session = None
    session_acquired_time = None
    stage_start_time = time.time()
    
    try:
        # 阶段1：获取 flow_session
        logger.debug(f"[阶段1] 开始获取 session, device_id: {device_id}")
        stage_start = time.time()
        try:
            flow_session = await http_client.get_flow_session_async()
            session_acquired_time = time.time()
            stage_elapsed = session_acquired_time - stage_start
            if stage_elapsed > 5.0:
                logger.warning(f"[阶段1] 获取session耗时过长: {stage_elapsed:.2f}秒, device_id: {device_id}")
            else:
                logger.debug(f"[阶段1] 获取session成功, 耗时: {stage_elapsed:.2f}秒")
        except Exception as e:
            logger.error(f"[阶段1] 获取 flow_session 失败，device_id: {device_id}, 错误: {e}")
            return False, device_id
        
        # 标记是否需要更新数据库
        need_update_db = False
        
        try:
            # 阶段2：获取 seed
            logger.debug(f"[阶段2] 检查 seed, device_id: {device_id}")
            stage_start = time.time()
            seed = device.get('seed')
            seed_type = device.get('seed_type')
            
            # 如果 device 中没有 seed 或 seed_type，或者为空，则请求获取
            if not seed or seed_type is None:
                logger.debug(f"[阶段2] 需要获取 seed, device_id: {device_id}")
                try:
                    seed, seed_type = await asyncio.wait_for(
                        api.get_seed_async(device, session=flow_session),
                        timeout=30.0  # 30秒超时
                    )
                    # 更新 device 字典
                    device['seed'] = seed
                    device['seed_type'] = seed_type
                    need_update_db = True
                    stage_elapsed = time.time() - stage_start
                    logger.debug(f"[阶段2] 获取 seed 成功, 耗时: {stage_elapsed:.2f}秒")
                except asyncio.TimeoutError:
                    logger.error(f"[阶段2] 获取 seed 超时（30秒），device_id: {device_id}")
                    return False, device_id
            else:
                logger.debug(f"[阶段2] 使用缓存的 seed")
            
            # 阶段3：获取 token
            logger.debug(f"[阶段3] 检查 token, device_id: {device_id}")
            stage_start = time.time()
            token = device.get('token')
            
            # 如果 device 中没有 token 或为空，则请求获取
            if not token:
                logger.debug(f"[阶段3] 需要获取 token, device_id: {device_id}")
                try:
                    token = await asyncio.wait_for(
                        api.get_token_async(device, session=flow_session),
                        timeout=30.0  # 30秒超时
                    )
                    # 更新 device 字典
                    device['token'] = token
                    need_update_db = True
                    stage_elapsed = time.time() - stage_start
                    logger.debug(f"[阶段3] 获取 token 成功, 耗时: {stage_elapsed:.2f}秒")
                except asyncio.TimeoutError:
                    logger.error(f"[阶段3] 获取 token 超时（30秒），device_id: {device_id}")
                    return False, device_id
            else:
                logger.debug(f"[阶段3] 使用缓存的 token")
            
            # 阶段4：更新数据库（如果需要）
            if need_update_db:
                logger.debug(f"[阶段4] 开始更新数据库, device_id: {device_id}")
                stage_start = time.time()
                try:
                    # 获取表的主键字段名（所有表都是 'id'）
                    primary_key_field = get_table_primary_key_field(db, device_table)
                    
                    # 使用传入的主键值（这是从数据库行中获取的真实主键值）
                    # 验证主键值不为空
                    if primary_key_value is None:
                        logger.warning(f"设备 {device_id} 的主键值为空，无法更新 device_config，primary_key_field={primary_key_field}")
                    else:
                        # 将更新后的 device 字典序列化为 JSON
                        try:
                            device_config_json = json.dumps(device, ensure_ascii=False)
                        except Exception as e:
                            logger.error(f"序列化 device_config 失败: {e}, device_id: {device_id}")
                            # 序列化失败不影响主流程，继续执行
                            device_config_json = "{}"
                        
                        # 获取当前时间戳（秒）
                        update_time = int(datetime.now().timestamp())
                        
                        # 验证 update_time 不为 None
                        if update_time is None:
                            logger.error(f"获取 update_time 失败，device_id: {device_id}")
                            update_time = 0
                        
                        # 更新数据库的 device_config 和 update_time 字段（在线程池中执行，不阻塞事件循环）
                        update_sql = f"""
                            UPDATE {device_table} 
                            SET device_config = %s, update_time = %s
                            WHERE {primary_key_field} = %s
                        """
                        logger.debug(f"执行更新SQL: UPDATE {device_table} SET device_config=..., update_time={update_time} WHERE {primary_key_field}={primary_key_value}")
                        
                        # 定义数据库更新函数（在线程池中执行）
                        # 注意：使用线程本地存储，每个线程复用同一个连接，不需要创建新连接
                        def update_device_config():
                            try:
                                # 直接使用传入的 db 实例，连接会自动使用线程本地存储
                                with db.get_cursor() as cursor:
                                    cursor.execute(update_sql, (device_config_json, update_time, primary_key_value))
                                    affected_rows = cursor.rowcount
                                    return affected_rows
                            except Exception as db_error:
                                # 记录详细的数据库错误信息
                                error_msg = str(db_error)
                                error_type = type(db_error).__name__
                                logger.error(f"数据库更新操作失败: {error_type}: {error_msg}")
                                logger.error(f"SQL: {update_sql}")
                                logger.error(f"参数: device_config_json长度={len(device_config_json)}, update_time={update_time}, primary_key_value={primary_key_value} (类型: {type(primary_key_value).__name__})")
                                logger.error(f"表名: {device_table}, 主键字段: {primary_key_field}")
                                # 重新抛出异常，让外层处理
                                raise
                        
                        # 在线程池中执行数据库更新
                        loop = asyncio.get_running_loop()
                        try:
                            affected_rows = await loop.run_in_executor(None, update_device_config)
                            
                            # 处理 affected_rows 可能为 None 的情况
                            affected_rows = affected_rows if affected_rows is not None else 0
                            stage_elapsed = time.time() - stage_start
                            if affected_rows > 0:
                                logger.debug(f"[阶段4] 更新数据库成功, 耗时: {stage_elapsed:.2f}秒")
                            else:
                                logger.warning(f"[阶段4] 更新数据库失败, 耗时: {stage_elapsed:.2f}秒, 影响行数: {affected_rows}")
                        except Exception as update_error:
                            # 更新失败不影响主流程，只记录错误
                            error_msg = str(update_error)
                            error_type = type(update_error).__name__
                            logger.error(f"更新设备 {device_id} 的 device_config 失败: {error_type}: {error_msg}")
                            logger.error(f"设备信息: device_id={device_id}, device_table={device_table}, primary_key_value={primary_key_value}")
                            import traceback
                            logger.error(traceback.format_exc())
                            # 不抛出异常，继续执行播放流程
                except Exception as e:
                    # 外层异常处理，确保不影响主流程
                    error_msg = str(e)
                    error_type = type(e).__name__
                    logger.error(f"更新设备 {device_id} 的 device_config 时发生异常: {error_type}: {error_msg}")
                    import traceback
                    logger.error(traceback.format_exc())
                    # 不抛出异常，继续执行播放流程
            
            # 阶段5：调用 stats 接口播放视频
            # 添加随机延迟，避免瞬时并发过高（使用全局配置）
            delay = random.uniform(_request_delay_min, _request_delay_max)
            await asyncio.sleep(delay)
            
            logger.debug(f"[阶段5] 开始 stats 请求, device_id: {device_id}, aweme_id: {aweme_id}, 延迟: {delay*1000:.0f}ms")
            stage_start = time.time()
            signcount = random.randint(200, 300)  # 随机签名计数
            try:
                result = await asyncio.wait_for(
                    api.stats_async(
                aweme_id=aweme_id,
                seed=seed,
                seed_type=seed_type,
                token=token,
                device=device,
                signcount=signcount,
                session=flow_session
                    ),
                    timeout=_stats_timeout  # 使用配置的超时时间
                )
                stage_elapsed = time.time() - stage_start
                logger.debug(f"[阶段5] stats 请求完成, 耗时: {stage_elapsed:.2f}秒, 结果: {'成功' if result else '失败'}")
            except asyncio.TimeoutError:
                stage_elapsed = time.time() - stage_start
                logger.error(f"[阶段5] stats 请求超时（{_stats_timeout}秒），device_id: {device_id}, aweme_id: {aweme_id}, 耗时: {stage_elapsed:.2f}秒")
                logger.error(f"[阶段5] 超时分析：可能是HTTP连接卡住或代理无响应")
                result = ""
            
            # 如果返回结果不为空，表示成功
            success = result != "" if result else False
            
            # 阶段6：更新播放统计（使用Redis缓存，减少数据库写压力）
            logger.debug(f"[阶段6] 开始更新播放统计, device_id: {device_id}, primary_key: {primary_key_value}")
            stage_start = time.time()
            try:
                # 1. 更新设备播放次数到Redis（使用主键ID，异步操作，极快）
                increment_device_play_in_redis(primary_key_value, amount=1)
                logger.debug(f"✓ 设备 primary_key={primary_key_value} (device_id={device_id}) 播放次数已记录到Redis")
                
                # 2. 如果播放成功且提供了 order_id，更新订单完成次数到Redis
                if success and order_id is not None:
                    increment_order_complete_in_redis(order_id, amount=1)
                    logger.debug(f"✓ 订单 {order_id} 完成次数已记录到Redis")
                    
                    # 2.1. 立即检查订单是否完成（在线程池中执行，避免阻塞）
                    def check_order_completion():
                        try:
                            is_completed, order_num, complete_num = check_and_update_order_completion(order_id, db)
                            if is_completed:
                                logger.info(f"🎉 订单 {order_id} 已完成！完成数={complete_num}/{order_num}")
                            return is_completed
                        except Exception as e:
                            logger.error(f"检查订单 {order_id} 完成状态失败: {e}")
                            return False
                    
                    loop = asyncio.get_running_loop()
                    try:
                        # 使用超时防止阻塞，订单检查最多等待10秒
                        is_completed = await asyncio.wait_for(
                            loop.run_in_executor(None, check_order_completion),
                            timeout=10.0
                        )
                        if is_completed:
                            logger.info(f"✓ 订单 {order_id} 完成检查通过，状态已更新")
                    except asyncio.TimeoutError:
                        logger.warning(f"⚠️ 订单 {order_id} 完成检查超时（10秒），将在下次检查")
                    except Exception as e:
                        logger.error(f"订单 {order_id} 完成检查异常: {e}")
                
                # 3. 更新设备状态为 0（已完成，恢复可用）- 这个需要立即生效，仍然写入数据库
                # 设置为0而不是3，让设备可以重复使用
                def update_device_status():
                    primary_key_field = get_table_primary_key_field(db, device_table)
                    update_status_sql = f"""
                        UPDATE {device_table} 
                        SET status = 0
                        WHERE {primary_key_field} = %s
                    """
                    with db.get_cursor() as cursor:
                        cursor.execute(update_status_sql, (primary_key_value,))
                    logger.debug(f"设备 {device_id} 状态已更新为 0（已完成，恢复可用状态，可重复使用）")
                
                # 在线程池中执行设备状态更新（不阻塞事件循环）
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, update_device_status)
                
                stage_elapsed = time.time() - stage_start
                logger.debug(f"[阶段6] 更新播放统计完成（Redis缓存模式）, 耗时: {stage_elapsed:.2f}秒")
            except Exception as e:
                stage_elapsed = time.time() - stage_start
                logger.error(f"[阶段6] 更新统计失败: {e}, 耗时: {stage_elapsed:.2f}秒")
                import traceback
                logger.error(traceback.format_exc())
            
            # 任务完成，计算总耗时
            total_elapsed = time.time() - stage_start_time
            logger.debug(f"[任务总耗时] {total_elapsed:.2f}秒, device_id: {device_id}, 结果: {'成功' if success else '失败'}")
            
            return success, device_id
            
        finally:
            # 阶段7：确保设备状态被重置（无论成功或失败）
            # 使用Redis批量更新模式，避免大量数据库写入和线程池阻塞
            try:
                logger.debug(f"[阶段7-Finally] 重置设备状态, device_id: {device_id}, primary_key: {primary_key_value}")
                
                # 将设备状态重置记录到Redis（目标状态=0，可用）
                # 监控线程会定期批量处理这些状态变更
                if primary_key_value is not None:
                    set_device_status_in_redis(primary_key_value, target_status=0)
                    logger.debug(f"✓ [Finally-Redis] 设备 {device_id} 状态重置请求已提交到Redis队列")
                else:
                    logger.warning(f"[Finally] 设备 {device_id} 主键值为None，无法记录状态变更")
                        
            except Exception as e:
                logger.error(f"[Finally] 重置设备状态失败: {e}, device_id: {device_id}")
                import traceback
                logger.error(traceback.format_exc())
            
            # 计算任务持有时间，如果过长则警告
            # 注意：新设计中 session 由 http_client 自动管理，无需手动释放
            if session_acquired_time:
                session_hold_time = time.time() - session_acquired_time
                if session_hold_time > 60:
                    logger.warning(f"[性能警告] 任务执行时间过长: {session_hold_time:.1f}秒, device_id: {device_id}, aweme_id: {aweme_id}")
                    logger.warning(f"[性能警告] 建议检查日志中的各阶段耗时，定位性能瓶颈")
            
    except Exception as e:
        logger.error(f"播放视频任务失败: {e}, aweme_id: {aweme_id}, device_id: {device_id}")
        import traceback
        logger.error(traceback.format_exc())
        return False, device_id


async def process_order_videos(
    order_id: int,
    video_ids: List[str],
    order_num: int,
    db: MySQLDB,
    api: TikTokAPI,
    http_client,
    max_concurrent: int = 1000
) -> int:
    """
    处理订单的视频播放任务
    
    Args:
        order_id: 订单ID
        video_ids: 视频ID列表
        order_num: 需要处理的次数
        db: 数据库连接
        api: TikTokAPI 实例
        http_client: HttpClient 实例
        max_concurrent: 最大并发数（默认1000）
    
    Returns:
        成功处理的次数
    """
    if not video_ids:
        logger.warning(f"订单 {order_id} 没有视频ID")
        return 0
    
    # 创建信号量控制并发数
    semaphore = asyncio.Semaphore(max_concurrent)
    
    # 获取当前完成数
    order_info = db.select_one("uni_order", where="id = %s", where_params=(order_id,))
    if not order_info:
        logger.error(f"订单 {order_id} 不存在")
        return 0
    current_complete = order_info.get('complete_num', 0) or 0
    current_play_num = order_info.get('play_num', 0) or 0
    
    # 如果 order_num > 0，检查是否已完成
    if order_num > 0 and current_complete >= order_num:
        logger.info(f"订单 {order_id} 已完成，current_complete={current_complete}, order_num={order_num}")
        return 0
    
    # 还需要处理的次数
    # 如果 order_num = 0，表示无限制处理，remaining 设为一个很大的数
    if order_num == 0:
        remaining = 999999999  # 无限制处理
        logger.info(f"订单 {order_id} order_num=0，将无限制处理，当前完成: {current_complete}")
    else:
        remaining = order_num - current_complete
        logger.info(f"订单 {order_id} 还需要处理 {remaining} 次，当前完成: {current_complete}/{order_num}")
    
    # 设备表列表
    device_tables = [f"uni_devices_{i}" for i in range(1, 11)]
    
    # 成功计数
    success_count = 0
    processed_count = 0
    
    # 从设备表中获取数据并处理
    for table_name in device_tables:
        if processed_count >= remaining:
            break
        
        page = 1
        while processed_count < remaining:
            # 从表中获取设备数据（使用配置的 page_size）
            devices = get_devices_from_table(db, table_name, limit=_page_size, status=0)
            
            if not devices:
                logger.info(f"表 {table_name} 第 {page} 页没有数据，切换到下一张表")
                break
            
            logger.info(f"从表 {table_name} 第 {page} 页获取到 {len(devices)} 个设备，准备处理")
            
            # 计算还需要处理的次数
            need_process = min(remaining - processed_count, len(devices))
            devices_to_process = devices[:need_process]
            
            # 创建带信号量控制的并发任务
            async def play_with_semaphore(device_dict, aweme_id, device_id, device_table_name, primary_key_val):
                """带信号量控制的播放任务"""
                async with semaphore:
                    return await play_video_task(
                        aweme_id=aweme_id,
                        device=device_dict,
                        device_id=device_id,
                        device_table=device_table_name,
                        primary_key_value=primary_key_val,
                        db=db,
                        api=api,
                        http_client=http_client,
                        order_id=order_id
                    )
            
            tasks = []
            device_id_list = []  # 保存设备ID列表，用于后续处理失败计数
            device_table_map = {}  # 保存设备ID到表名的映射
            device_id_to_primary_key = {}  # 保存设备ID到主键值的映射
            # 先获取主键字段名（只需要获取一次）
            primary_key_field = get_table_primary_key_field(db, table_name)
            logger.debug(f"表 {table_name} 的主键字段: {primary_key_field}")
            
            for device_row in devices_to_process:
                # 从数据库行中获取 device_config 字段（JSON字符串）
                device_config_str = device_row.get('device_config', '')
                
                # 获取主键值（主键字段的值，通常是 id）
                primary_key_value = device_row.get(primary_key_field)
                
                # 验证主键值不为空
                if primary_key_value is None:
                    logger.warning(f"设备行中主键字段 {primary_key_field} 的值为 None，跳过")
                    continue
                
                # 确保主键值的类型正确（如果是数字字符串，转换为整数）
                # 主键通常是整数类型
                try:
                    if isinstance(primary_key_value, str) and primary_key_value.isdigit():
                        primary_key_value = int(primary_key_value)
                    elif isinstance(primary_key_value, (int, float)):
                        primary_key_value = int(primary_key_value)
                except (ValueError, TypeError) as e:
                    logger.warning(f"转换主键值类型失败: {e}, primary_key_value={primary_key_value}, 类型={type(primary_key_value).__name__}")
                    # 如果转换失败，继续使用原值
                
                # 解析 device_config 作为 device 字典
                if device_config_str:
                    device_dict = parse_device_config(device_config_str)
                else:
                    # 如果 device_config 为空，使用空字典
                    device_dict = {}
                    logger.warning(f"设备主键 {primary_key_value} 的 device_config 为空")
                
                # 从解析后的 device_config 中获取 device_id
                device_id = device_dict.get('device_id', '')
                if not device_id:
                    # 如果没有device_id，使用主键值作为标识
                    device_id = str(primary_key_value)
                
                # 保存设备ID到表名的映射
                device_table_map[device_id] = table_name
                # 保存设备ID到主键值的映射（使用主键字段的值，不是 device_id）
                device_id_to_primary_key[device_id] = primary_key_value
                
                logger.debug(f"设备映射: device_id={device_id}, primary_key={primary_key_field}={primary_key_value}, table={table_name}")
                
                # 随机选择一个视频ID
                aweme_id = random.choice(video_ids)
                
                device_id_list.append(device_id)
                task = play_with_semaphore(device_dict, aweme_id, device_id, table_name, primary_key_value)
                tasks.append(task)
            
            # 并发执行任务（由信号量控制并发数）
            logger.info(f"开始并发处理 {len(tasks)} 个任务（订单 {order_id}，最大并发 {max_concurrent}）")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 处理结果并统计成功数，同时跟踪设备失败次数，并更新设备的 play_num
            batch_success = 0
            for idx, result in enumerate(results):
                device_id = device_id_list[idx]
                
                # 处理异常情况
                if isinstance(result, Exception):
                    logger.error(f"任务执行异常: {result}, device_id: {device_id}")
                    success = False
                else:
                    success, _ = result if isinstance(result, tuple) else (result, device_id)
                
                # play_num 的更新已在 play_video_task 中完成，这里不再更新
                
                if success:
                    batch_success += 1
                    # 成功时重置失败计数
                    with _device_fail_lock:
                        if device_id in _device_fail_count:
                            _device_fail_count[device_id] = 0
                else:
                    # 失败时增加失败计数
                    with _device_fail_lock:
                        current_fail_count = _device_fail_count.get(device_id, 0) + 1
                        _device_fail_count[device_id] = current_fail_count
                        
                        # 如果连续失败超过阈值，标记设备为异常状态
                        if current_fail_count >= _device_fail_threshold:
                            logger.warning(f"设备 {device_id} 连续失败 {current_fail_count} 次（阈值: {_device_fail_threshold}），标记为异常状态")
                            # 更新设备状态为 4（连续失败异常状态）
                            try:
                                # 从映射中获取设备所在的表名和主键值
                                device_table = device_table_map.get(device_id)
                                primary_key_value = device_id_to_primary_key.get(device_id)
                                if device_table and primary_key_value:
                                    # 获取表的主键字段名
                                    primary_key_field = get_table_primary_key_field(db, device_table)
                                    logger.info(f"正在更新表 {device_table}，使用主键字段: {primary_key_field}={primary_key_value}，设备ID: {device_id}")
                                    
                                    # 更新设备状态
                                    # 注意：db.execute() 返回 None，需要使用游标获取 rowcount
                                    update_status_sql = f"""
                                        UPDATE {device_table} 
                                        SET status = 4
                                        WHERE {primary_key_field} = %s
                                    """
                                    # 使用游标直接执行以获取影响的行数
                                    with db.get_cursor() as cursor:
                                        cursor.execute(update_status_sql, (primary_key_value,))
                                        affected_rows = cursor.rowcount
                                    # 处理 affected_rows 可能为 None 的情况
                                    affected_rows = affected_rows if affected_rows is not None else 0
                                    if affected_rows > 0:
                                        logger.info(f"✓ 设备 {device_id} 在表 {device_table} 中已标记为连续失败异常状态 (status=4)，影响行数: {affected_rows}")
                                    else:
                                        logger.warning(f"✗ 设备 {device_id} 在表 {device_table} 中状态更新失败，影响行数: {affected_rows}，主键值: {primary_key_value}")
                                elif not device_table:
                                    logger.warning(f"设备 {device_id} 不在映射中，无法更新状态")
                                elif not primary_key_value:
                                    logger.warning(f"设备 {device_id} 的主键值未找到，无法更新状态")
                                else:
                                    # 如果映射中没有，尝试在所有设备表中查找并更新
                                    logger.info(f"设备 {device_id} 不在映射中，尝试在所有设备表中查找并更新状态")
                                    for table_idx in range(1, 11):
                                        table_name = f"uni_devices_{table_idx}"
                                        try:
                                            # 获取表的主键字段名
                                            primary_key_field = get_table_primary_key_field(db, table_name)
                                            logger.debug(f"检查表 {table_name}，使用主键字段: {primary_key_field}")
                                            
                                            # 直接尝试更新设备状态
                                            # 注意：db.execute() 返回 None，需要使用游标获取 rowcount
                                            update_status_sql = f"""
                                                UPDATE {table_name} 
                                                SET status = 4
                                                WHERE {primary_key_field} = %s
                                            """
                                            # 使用游标直接执行以获取影响的行数
                                            with db.get_cursor() as cursor:
                                                cursor.execute(update_status_sql, (device_id,))
                                                affected_rows = cursor.rowcount
                                            # 处理 affected_rows 可能为 None 的情况
                                            affected_rows = affected_rows if affected_rows is not None else 0
                                            if affected_rows > 0:
                                                logger.info(f"✓ 设备 {device_id} 在表 {table_name} 中已标记为连续失败异常状态 (status=4)，影响行数: {affected_rows}")
                                                break
                                            else:
                                                logger.debug(f"设备 {device_id} 在表 {table_name} 中未找到，继续尝试下一个表")
                                        except Exception as e:
                                            # 表不存在或查询失败，记录错误并继续尝试下一个表
                                            logger.debug(f"检查表 {table_name} 时出错: {e}")
                                            continue
                                    else:
                                        logger.error(f"设备 {device_id} 未在任何设备表中找到")
                            except Exception as e:
                                logger.error(f"更新设备 {device_id} 状态失败: {e}")
                                import traceback
                                logger.error(traceback.format_exc())
            
            success_count += batch_success
            processed_count += len(tasks)
            
            # complete_num 的更新已在 play_video_task 中完成，这里只检查订单是否完成
            # 注意：play_num 和 complete_num 都已经在 play_video_task 中立即更新了
            with order_lock:
                # 重新查询订单信息（检查是否已完成）
                    order_info = db.select_one("uni_order", where="id = %s", where_params=(order_id,))
                    if order_info:
                        new_complete = order_info.get('complete_num', 0) or 0
                    logger.debug(f"订单 {order_id} 当前 complete_num={new_complete}")
                    
                    # 如果已完成，退出（order_num=0 时不会退出）
                    if order_num > 0 and new_complete >= order_num:
                        logger.info(f"订单 {order_id} 已完成！complete_num={new_complete}, order_num={order_num}")
                        # 更新状态为2
                        db.update("uni_order", {"status": 2}, "id = %s", (order_id,))
                        db.commit()
                        return success_count
            
            logger.info(f"订单 {order_id} 批次完成: 成功 {batch_success}/{len(tasks)}, 累计成功 {success_count}/{processed_count}")
            
            # 如果还需要处理，继续下一页
            if processed_count < remaining and len(devices) == _page_size:
                page += 1
            else:
                break
    
    return success_count


# 全局变量（用于消息队列）
_queue_instance: Optional[MessageQueue] = None
_db_instance: Optional[MySQLDB] = None
_api_instance: Optional[TikTokAPI] = None
_http_client_instance = None
_device_table_name: str = "uni_devices_1"
_max_concurrent: int = 1000
_threshold_size: int = 3000  # 队列阈值大小（可配置）

# 当前正在处理的订单（全局变量）
_current_order: Optional[Dict[str, Any]] = None
_current_order_lock = threading.Lock()

# 订单完成停止标志（用于停止添加新任务，但保持队列中运行的任务）
_order_complete_stop_flag = False
_order_complete_stop_lock = threading.Lock()

# 阈值回调队列和控制变量
_threshold_callback_queue = None  # 阈值回调任务队列
_threshold_callback_processor_thread = None  # 阈值回调处理器线程
_threshold_callback_stop_event = None  # 停止事件
_threshold_callback_queue_lock = threading.Lock()  # 队列锁
_threshold_callback_processing = False  # 是否正在处理回调队列
_threshold_callback_stopped = False  # 是否已停止处理（当某个回调返回空列表时设置为True）


def _threshold_callback_processor():
    """
    阈值回调处理器（在后台线程中运行）
    按顺序处理队列中的回调任务，每次处理间隔2秒
    如果某个回调返回空列表，则停止处理后续任务并清空队列
    """
    global _threshold_callback_queue, _threshold_callback_stop_event
    global _threshold_callback_processing, _threshold_callback_stopped
    global _queue_instance, _order_complete_stop_flag, _order_complete_stop_lock
    
    logger.info("[阈值回调处理器] 启动")
    
    # 阈值回调间隔时间（秒）
    callback_interval = 2.0
    last_callback_time = 0
    
    while not _threshold_callback_stop_event.is_set():
        try:
            # 从队列中获取回调任务（阻塞等待，最多等待1秒）
            try:
                callback_task = _threshold_callback_queue.get(timeout=1.0)
            except:
                # 超时或队列为空，继续循环
                continue
            
            # 标记为正在处理
            with _threshold_callback_queue_lock:
                _threshold_callback_processing = True
            
            try:
                # 确保间隔时间为2秒
                current_time = time.time()
                time_since_last = current_time - last_callback_time
                if time_since_last < callback_interval:
                    wait_time = callback_interval - time_since_last
                    time.sleep(wait_time)
                
                last_callback_time = time.time()
                
                # 执行实际的回调逻辑（在线程池中执行，避免阻塞）
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(_execute_threshold_callback)
                    try:
                        # 超时时间应该大于内部所有操作的总和：
                        # - 获取订单信息: 15秒
                        # - get_and_lock_devices: 180秒
                        # - 创建任务: 最多30秒
                        # 总计至少需要 225秒，设置为240秒（4分钟）更安全
                        tasks = future.result(timeout=240.0)  # 最多等待240秒（4分钟）
                    except concurrent.futures.TimeoutError:
                        logger.error("[阈值回调] ⚠️ 执行超时（240秒/4分钟）")
                        logger.error("[阈值回调超时原因分析]：")
                        logger.error("  1. get_and_lock_devices 操作超过180秒（数据库锁等待或查询慢）")
                        logger.error("  2. 数据库连接池耗尽，等待可用连接")
                        logger.error("  3. 网络延迟或数据库负载过高")
                        logger.error("  建议：检查数据库慢查询日志和锁等待情况")
                        tasks = []
                    except Exception as e:
                        logger.error(f"[阈值回调] ⚠️ 执行异常: {e}")
                        import traceback
                        logger.error(traceback.format_exc())
                        tasks = []
                
                # 将任务添加到队列
                if tasks:
                    # 获取添加前的队列状态
                    queue_stats_before = _queue_instance.get_stats() if _queue_instance else {}
                    queue_size_before = queue_stats_before.get('queue_size', 0)
                    
                    # 批量添加任务
                    added_count = 0
                    failed_count = 0
                    for task in tasks:
                        if _queue_instance:
                            try:
                                success = _queue_instance.add_task(task)
                                if success:
                                    added_count += 1
                                else:
                                    failed_count += 1
                                    
                                    # 如果失败过多，提前终止
                                    if failed_count > 5:
                                        break
                            except Exception:
                                failed_count += 1
                                if failed_count > 5:
                                    break
                        else:
                            break
                    
                    # 获取添加后的队列状态
                    queue_stats_after = _queue_instance.get_stats() if _queue_instance else {}
                    queue_size_after = queue_stats_after.get('queue_size', 0)
                    completed_after = queue_stats_after.get('completed_tasks', 0)
                    
                    # 简洁的成功日志（一次性补齐模式）
                    queue_increase = queue_size_after - queue_size_before
                    if failed_count > 0:
                        logger.warning(f"[阈值回调] ⚠️ 一次性补齐：成功{added_count}个，失败{failed_count}个 | 队列: {queue_size_before}→{queue_size_after}(+{queue_increase})，已完成: {completed_after}")
                    else:
                        logger.info(f"[阈值回调] ✓ 一次性补齐成功: {added_count}个任务 | 队列: {queue_size_before}→{queue_size_after}(+{queue_increase})，已完成: {completed_after}")
                else:
                    # 返回空列表，可能是临时失败或没有可用设备
                    logger.debug("[阈值回调] 无可用任务（可能原因：没有设备、查询超时、订单完成）")
                
            except Exception as e:
                logger.error(f"[阈值回调处理器] 处理回调任务异常: {e}")
                import traceback
                logger.error(traceback.format_exc())
            finally:
                # 标记处理完成
                with _threshold_callback_queue_lock:
                    _threshold_callback_processing = False
                _threshold_callback_queue.task_done()
        
        except Exception as e:
            logger.error(f"[阈值回调处理器] 循环异常: {e}")
            import traceback
            logger.error(traceback.format_exc())
            time.sleep(0.1)
    
    logger.info("[阈值回调处理器] 已停止")


def _execute_threshold_callback() -> List[Dict[str, Any]]:
    """
    执行实际的阈值回调逻辑（从数据库获取设备并创建任务）
    
    Returns:
        任务列表
    """
    global _db_instance, _queue_instance, _device_table_name, _max_concurrent, _threshold_size
    global _current_order, _current_order_lock
    
    try:
        if not _db_instance or not _queue_instance:
            logger.warning("[阈值回调] 数据库或队列实例未初始化")
            return []
            
        # 检查订单完成停止标志，如果已设置则不再添加新任务
        with _order_complete_stop_lock:
            if _order_complete_stop_flag:
                logger.info("[阈值回调] 订单已完成，停止添加新任务（保持队列中运行和等待的任务）")
                return []  # 返回空列表，不再添加新任务
        
            # 获取队列状态
            queue_stats = _queue_instance.get_stats()
            queue_size = queue_stats.get("queue_size", 0)
            running_tasks = queue_stats.get("running_tasks", 0)
            
            # 计算需要获取的设备数量
        # 总任务数 = 队列中的任务数 + 正在执行的任务数
        # 需要补充的数量 = 阈值数量 - 总任务数
            total_in_queue = queue_size + running_tasks
            need_count = _threshold_size - total_in_queue
        
        if need_count <= 0:
            return []  # 队列充足，不需要补充
        
        # 一次性补齐策略：获取足够的设备数量，添加20%的缓冲避免并发竞争
        # 但限制单次获取的最大数量，避免数据库操作过慢
        need_count_with_buffer = int(need_count * 1.2)
        max_single_request = 2000  # 单次最多请求2000个设备
        
        if need_count_with_buffer > max_single_request:
            logger.info(f"[阈值回调] 需要{need_count}个任务（含缓冲{need_count_with_buffer}个），但单次限制为{max_single_request}个，将分多次补充")
            need_count_with_buffer = max_single_request
        else:
            logger.info(f"[阈值回调] 一次性补齐模式：需要{need_count}个任务，实际获取{need_count_with_buffer}个设备（含20%缓冲）")
            
            # 使用当前正在处理的订单（全局变量）
            order_id = None
            order_num = 0
            complete_num = 0
            video_ids = []
            
            with _current_order_lock:
                if not _current_order:
                    logger.error("[阈值回调] 当前订单为空")
                    return []
                order_id = _current_order.get('id')
        
        import concurrent.futures
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    _db_instance.select_one,
                    "uni_order",
                    where="id = %s",
                    where_params=(order_id,)
                )
                order_info = future.result(timeout=15.0)  # 增加超时时间到15秒
        except concurrent.futures.TimeoutError:
            logger.warning(f"[阈值回调] 获取订单信息超时（15秒），返回空列表等待下次回调")
            return []
        except Exception as e:
            logger.error(f"[阈值回调] 获取订单信息失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
        
        if not order_info:
            logger.error(f"[阈值回调] 订单 {order_id} 不存在，这是不应该发生的情况！")
            logger.error(f"[阈值回调停止原因] 当前订单 {order_id} 已被删除或不存在")
            return []
        
        order_num = order_info.get('order_num', 0) or 0
        complete_num = order_info.get('complete_num', 0) or 0
        
        # 检查订单还需要多少任务
        if order_num > 0:
            remaining_tasks = order_num - complete_num
            
            # 限制获取数量不超过订单剩余任务数
            if remaining_tasks < need_count_with_buffer:
                need_count_with_buffer = remaining_tasks
                logger.info(f"[阈值回调] 订单剩余任务数 {remaining_tasks} 小于需求，调整获取数量为 {need_count_with_buffer}")
            
            # 如果订单已完成，自动切换到下一个订单
            if remaining_tasks <= 0:
                logger.info(f"[阈值回调] 订单 {order_id} 已完成（complete_num={complete_num} >= order_num={order_num}），更新状态为已完成并切换到下一个订单")
                # 更新订单状态为已完成并切换到下一个订单 - 在线程池中执行，避免阻塞
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    def switch_order():
                        _db_instance.update("uni_order", {"status": 2}, "id = %s", (order_id,))
                        _db_instance.commit()
                        return _db_instance.select(
                            "uni_order",
                            where="status IN (0, 1)",
                            order_by="id ASC",
                            limit=1
                        )
                    future = executor.submit(switch_order)
                    try:
                        next_orders = future.result(timeout=15.0)  # 增加超时时间到15秒
                    except concurrent.futures.TimeoutError:
                        logger.warning(f"[阈值回调] 切换订单操作超时（15秒），返回空列表等待下次回调")
                        return []
                    
                    if not next_orders:
                        logger.info("[阈值回调] 没有更多待处理的订单，返回空列表")
                        logger.info("[阈值回调停止原因] 所有订单已完成，没有待处理的订单")
                        with _current_order_lock:
                            _current_order = None
                        return []
                    
                    # 切换到下一个订单
                    with _current_order_lock:
                        _current_order = next_orders[0]
                        order_info = _current_order
                        order_id = order_info['id']
                        order_num = order_info.get('order_num', 0) or 0
                        complete_num = order_info.get('complete_num', 0) or 0
                    
                    # 缓存新订单的 order_num 到 Redis
                    if _redis is not None:
                        set_order_num_to_redis(order_id, order_num)
                        logger.info(f"✓ 切换到新订单 {order_id}，总数已缓存到Redis: order_num={order_num}")
                    
                    # 重新计算剩余任务数
                    if order_num > 0:
                        remaining_tasks = order_num - complete_num
                        if remaining_tasks <= 0:
                            return []
                        need_count_with_buffer = min(need_count_with_buffer, remaining_tasks)
                    else:
                        # 限制获取的设备数量不超过订单剩余任务数
                        need_count_with_buffer = min(need_count_with_buffer, remaining_tasks)
        
        if need_count_with_buffer <= 0:
            logger.warning(f"[阈值回调] 调整后需要补充数量 <= 0，返回空列表")
            return []
        
        # 检查订单是否有有效的视频ID
        order_info_str = order_info.get('order_info', '')
        video_ids = parse_video_ids(order_info_str)
        
        if not video_ids:
            logger.warning(f"[阈值回调] 订单 {order_id} 没有有效的视频ID")
            logger.warning(f"[阈值回调停止原因] 订单 {order_id} 的 order_info 字段为空或格式错误")
            return []
                
        # 在事务中从数据库获取设备并更新状态为1（进行中）
        # 使用缓冲数量确保一次性补齐
        import concurrent.futures
        try:
            logger.debug(f"[阈值回调] 开始获取设备，请求数量: {need_count_with_buffer}")
            get_devices_start = time.time()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    get_and_lock_devices,
                    _db_instance,
                    _device_table_name,
                    need_count_with_buffer,  # 使用带缓冲的数量，一次性补齐
                    0
                )
                devices = future.result(timeout=180.0)  # 增加到180秒（3分钟）
            
            get_devices_elapsed = time.time() - get_devices_start
            logger.debug(f"[阈值回调] 获取设备完成，耗时: {get_devices_elapsed:.2f}秒，获取数量: {len(devices) if devices else 0}")
            
            if get_devices_elapsed > 60:
                logger.warning(f"[阈值回调] ⚠️ get_and_lock_devices 耗时过长: {get_devices_elapsed:.2f}秒")
                logger.warning(f"[阈值回调] 可能原因：1) 数据库锁等待 2) 表扫描过慢 3) 网络延迟")
        except concurrent.futures.TimeoutError:
            get_devices_elapsed = time.time() - get_devices_start
            logger.error(f"[阈值回调] 获取设备操作超时（180秒），实际耗时: {get_devices_elapsed:.2f}秒")
            logger.error(f"[阈值回调] 请求获取 {need_count_with_buffer} 个设备")
            logger.error(f"[阈值回调超时诊断]：")
            logger.error(f"  1. 数据库SELECT查询可能被其他事务锁住")
            logger.error(f"  2. 表 {_device_table_name} 可能没有合适的索引")
            logger.error(f"  3. 数据库连接池可能耗尽")
            logger.error(f"  建议：执行 SHOW PROCESSLIST; 查看数据库当前执行的查询")
            logger.warning(f"[阈值回调停止原因] 数据库获取设备操作超时（180秒），可能需要检查数据库性能或并发锁")
            return []  # 超时返回空列表，但不停止后续处理
        except Exception as e:
            logger.error(f"[阈值回调] 获取设备操作失败: {e}")
            logger.error(f"[阈值回调停止原因] 获取设备时发生异常: {type(e).__name__}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []  # 异常返回空列表，但不停止后续处理
        
        if not devices:
            logger.warning(f"[阈值回调] 没有可用设备 (status=0)，表: {_device_table_name}")
            
            # 获取队列状态，打印当前任务数量
            if _queue_instance:
                queue_stats = _queue_instance.get_stats()
                queue_size = queue_stats.get("queue_size", 0)
                running_tasks = queue_stats.get("running_tasks", 0)
                completed_tasks = queue_stats.get("completed_tasks", 0)
                failed_tasks = queue_stats.get("failed_tasks", 0)
                total_in_queue = queue_size + running_tasks
                
                logger.info(f"[阈值回调] 📊 当前队列状态：")
                logger.info(f"  - 等待中的任务: {queue_size}")
                logger.info(f"  - 正在执行的任务: {running_tasks}")
                logger.info(f"  - 队列中总任务数: {total_in_queue} (等待{queue_size} + 执行中{running_tasks})")
                logger.info(f"  - 已完成: {completed_tasks}, 失败: {failed_tasks}")
                
                if total_in_queue > 0:
                    logger.info(f"[阈值回调] ⏳ 等待队列中 {total_in_queue} 个任务完成后，设备将自动释放并可重复使用")
                else:
                    logger.warning(f"[阈值回调] ⚠️ 队列已空但仍无可用设备，检查是否需要重置设备状态")
                    
                    # 检查是否所有设备都已使用过
                    try:
                        status_sql = f"""
                        SELECT status, COUNT(*) as count
                        FROM {_device_table_name}
                        GROUP BY status
                        """
                        status_results = _db_instance.execute(status_sql, fetch=True)
                        status_counts = {row['status']: row['count'] for row in status_results}
                        
                        total_devices = sum(status_counts.values())
                        completed_devices = status_counts.get(3, 0)
                        failed_devices = status_counts.get(4, 0)
                        
                        # 如果大部分设备都已完成（>80%），则重置设备状态
                        if total_devices > 0 and (completed_devices + failed_devices) / total_devices > 0.8:
                            logger.info(f"[阈值回调] 🔄 检测到 {(completed_devices + failed_devices) / total_devices * 100:.1f}% 的设备已使用过，重置设备状态...")
                            reset_count = reset_device_status(_db_instance, _device_table_name)
                            logger.info(f"[阈值回调] ✅ 设备状态已重置，{reset_count} 个设备可重新使用")
                            
                            # 重新获取设备
                            devices = get_devices_from_table(_db_instance, _device_table_name, limit=need_count_with_buffer, status=0)
                            if devices:
                                logger.info(f"[阈值回调] ✅ 重置后获取到 {len(devices)} 个设备，继续创建任务")
                                # 继续执行后面的任务创建逻辑
                            else:
                                logger.warning(f"[阈值回调] ⚠️ 重置后仍无可用设备")
                                return []
                        else:
                            logger.info(f"[阈值回调] 设备使用率 {(completed_devices + failed_devices) / total_devices * 100:.1f}%，暂不重置")
                    except Exception as e:
                        logger.error(f"[阈值回调] 检查设备状态时出错: {e}")
            
        if not devices:
            logger.info(f"[阈值回调] 本次不添加新任务，等待队列中现有任务完成后释放设备")
            logger.info(f"[阈值回调] 注意：阈值回调返回空列表不会影响队列中已有任务的正常执行")
            return []
        
        # 为每个设备创建任务
        tasks = []
        
        # 获取主键字段名（在线程池中执行，避免阻塞）
        try:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    get_table_primary_key_field,
                    _db_instance,
                    _device_table_name
                )
                primary_key_field = future.result(timeout=5.0)  # 最多等待5秒
        except Exception as e:
            logger.error(f"[阈值回调] 获取主键字段失败: {e}")
            primary_key_field = 'id'  # 默认使用 'id'
        
        skipped_count = 0
        
        for device in devices:
            try:
                # 获取主键值
                primary_key_value = device.get(primary_key_field)
                
                if not primary_key_value:
                    logger.warning(f"[阈值回调] 设备主键值为空，跳过")
                    skipped_count += 1
                    continue
                
                # 解析 device_config（快速操作，不需要在线程池中执行）
                device_config_str = device.get('device_config', '')
                if device_config_str:
                    try:
                        device_dict = parse_device_config(device_config_str)
                    except Exception as e:
                        logger.warning(f"[阈值回调] 解析设备配置失败: {e}，使用空字典")
                        device_dict = {}
                else:
                    device_dict = {}
                
                # 从解析后的 device_config 中获取 device_id
                device_id = device_dict.get('device_id', '')
                if not device_id:
                    # 如果没有device_id，使用主键值作为标识
                    device_id = str(primary_key_value)
                
                # 随机选择一个视频ID
                aweme_id = random.choice(video_ids)
                
                # 创建任务
                task = {
                    "aweme_id": aweme_id,
                    "device": device_dict,
                    "device_id": device_id,
                    "device_table": _device_table_name,
                    "primary_key_value": primary_key_value,
                    "order_id": order_id
                }
                tasks.append(task)
            except Exception as e:
                logger.error(f"[阈值回调] 为设备创建任务时异常: {e}")
                skipped_count += 1
                continue
        
        if skipped_count > 0:
            logger.warning(f"[阈值回调] 跳过了 {skipped_count} 个没有主键值的设备")
        
        if not tasks:
            logger.warning(f"[阈值回调] 没有创建任何任务（跳过了 {skipped_count} 个设备），返回空列表")
            # 将设备状态更新回 0，因为任务创建失败 - 在线程池中执行，避免阻塞
            device_ids = [device.get(primary_key_field) for device in devices if device.get(primary_key_field) is not None]
            if device_ids:
                try:
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(
                            update_devices_status,
                            _db_instance,
                            device_ids,
                            _device_table_name,
                            0
                        )
                        future.result(timeout=10.0)  # 最多等待10秒
                except Exception as e:
                    logger.error(f"[阈值回调] 更新设备状态失败: {e}")
            return []
        
        # 确保返回任务列表，补充到队列中
        if tasks:
            return tasks  # 返回任务列表，补充到队列
        else:
            # 将设备状态更新回 0，因为任务创建失败
            device_ids = [device.get(primary_key_field) for device in devices if device.get(primary_key_field) is not None]
            if device_ids:
                try:
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(
                            update_devices_status,
                            _db_instance,
                            device_ids,
                            _device_table_name,
                            0
                        )
                        future.result(timeout=10.0)  # 最多等待10秒
                except Exception as e:
                    logger.error(f"[阈值回调] 更新设备状态失败: {e}")
            return []  # 任务创建失败，返回空列表
    except Exception as e:
        logger.error(f"阈值回调执行失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []


def threshold_callback() -> List[Dict[str, Any]]:
    """
    阈值补给回调函数（同步函数）
    当队列达到阈值时，将回调任务放入队列，由后台线程按顺序处理
    如果某个回调返回空列表，则停止处理后续任务
    只有在确实需要回调的情况下（有新订单或队列需要补充），才重置停止标志并继续处理
    
    Returns:
        任务列表（总是返回空列表，实际任务由后台线程处理）
    """
    global _threshold_callback_queue, _threshold_callback_stopped
    global _threshold_callback_processing, _threshold_callback_queue_lock
    global _db_instance, _queue_instance, _threshold_size, _current_order, _current_order_lock
    
    # 检查是否真的需要回调（无论是否已停止）
    need_callback = False
    need_count = 0  # 初始化 need_count
    
    # 检查1：队列是否需要补充
    if _queue_instance:
        queue_stats = _queue_instance.get_stats()
        queue_size = queue_stats.get("queue_size", 0)
        running_tasks = queue_stats.get("running_tasks", 0)
        total_in_queue = queue_size + running_tasks
        need_count = _threshold_size - total_in_queue
        
        logger.debug(f"[阈值回调] 检查是否需要回调: 队列大小={queue_size}, 运行中={running_tasks}, 总任务数={total_in_queue}, 阈值={_threshold_size}, 需要补充={need_count}")
        
        if need_count > 0:
            # 检查2：是否有订单需要处理
            with _current_order_lock:
                if _current_order:
                    # 有订单，需要回调
                    need_callback = True
                    logger.debug(f"[阈值回调] 队列需要补充（需要 {need_count} 个任务），继续处理")
                else:
                    # 没有订单，检查是否有新订单
                    if _db_instance:
                        next_orders = _db_instance.select(
                            "uni_order",
                            where="status IN (0, 1)",
                            order_by="id ASC",
                            limit=1
                        )
                        if next_orders:
                            # 有新订单，需要回调
                            _current_order = next_orders[0]
                            need_callback = True
                            logger.info(f"[阈值回调] 发现新订单 {_current_order.get('id')}，继续处理")
                        else:
                            # 没有新订单，不需要回调
                            logger.debug("[阈值回调] 没有新订单，跳过本次回调")
                    else:
                        # 数据库未初始化，不需要回调
                        logger.debug("[阈值回调] 数据库未初始化，跳过本次回调")
        else:
            # 队列不需要补充，不需要回调
            logger.debug(f"[阈值回调] 队列充足（总任务数: {total_in_queue}, 阈值: {_threshold_size}），跳过本次回调")
    
    # 如果不需要回调，直接返回空列表，不放入队列
    if not need_callback:
        reason = ""
        if _queue_instance:
            queue_stats = _queue_instance.get_stats()
            queue_size = queue_stats.get("queue_size", 0)
            running_tasks = queue_stats.get("running_tasks", 0)
            total_in_queue = queue_size + running_tasks
            if total_in_queue >= _threshold_size:
                reason = f"队列充足（总任务数: {total_in_queue} >= 阈值: {_threshold_size}）"
            else:
                with _current_order_lock:
                    if not _current_order:
                        reason = "没有订单需要处理"
                    else:
                        reason = "未知原因"
        else:
            reason = "队列实例未初始化"
        
        logger.debug(f"[阈值回调] 不需要回调。原因: {reason}")
        return []
    
    # 防重入保护：如果正在处理回调或队列中已有待处理的回调，不再添加新的
    with _threshold_callback_queue_lock:
        if _threshold_callback_processing:
            logger.debug("[阈值回调] 已有回调正在处理中，跳过本次回调")
            return []
        
        # 检查队列大小，如果已有待处理的回调，不再添加
        if _threshold_callback_queue is not None:
            queue_size = _threshold_callback_queue.qsize()
            if queue_size > 0:
                logger.debug(f"[阈值回调] 回调队列中已有 {queue_size} 个待处理任务，跳过本次回调")
                return []
    
    # 将回调任务放入队列
    if _threshold_callback_queue is not None:
        try:
            _threshold_callback_queue.put_nowait(True)
            logger.debug("[阈值回调] 已将回调任务放入队列")
        except:
            logger.warning("[阈值回调] 回调队列已满")
            return []
    
    # 总是返回空列表，实际任务由后台线程处理
    # 注意：返回空列表不代表没有任务，而是任务已放入回调队列，由后台线程按顺序处理
            return []


async def task_callback(task_data: Dict[str, Any]):
    """
    任务执行回调函数（异步）
    执行播放视频任务
    
    Args:
        task_data: 任务数据，包含 aweme_id, device, device_id, device_table, primary_key_value, order_id
    """
    global _db_instance, _api_instance, _http_client_instance
    global _device_fail_count, _device_fail_lock, _device_fail_threshold
    
    try:
        if not _db_instance or not _api_instance or not _http_client_instance:
            logger.error("数据库、API 或 HttpClient 实例未初始化")
            return
        
        aweme_id = task_data.get('aweme_id')
        device = task_data.get('device', {})
        device_id = task_data.get('device_id', '')
        device_table = task_data.get('device_table', _device_table_name)
        primary_key_value = task_data.get('primary_key_value')
        order_id = task_data.get('order_id')
        
        # 在执行任务前，检查订单是否已经完成（使用Redis缓存，避免频繁查询数据库）
        # 只有当订单接近完成时才检查，避免不必要的查询
        if order_id and _redis:
            try:
                # 从Redis获取订单进度（快速，不查询数据库）
                redis_complete = get_order_complete_from_redis(order_id)
                redis_order_num = get_order_num_from_redis(order_id)
                
                if redis_order_num and redis_order_num > 0:
                    # 只有当完成度超过90%时才检查（避免不必要的检查）
                    if redis_complete >= redis_order_num * 0.9:
                        # 接近完成，从数据库确认一下（使用线程池执行，避免阻塞事件循环）
                        loop = asyncio.get_running_loop()
                        
                        def get_order_status():
                            return _db_instance.select_one("uni_order", where="id = %s", where_params=(order_id,))
                        
                        order_info = await loop.run_in_executor(None, get_order_status)
                        if order_info:
                            order_num = order_info.get('order_num', 0) or 0
                            complete_num = order_info.get('complete_num', 0) or 0
                            
                            if order_num > 0 and complete_num >= order_num:
                                # 订单已完成，跳过此任务
                                logger.info(f"[任务跳过] 订单 {order_id} 已完成({complete_num}/{order_num})，跳过任务: 主键ID={primary_key_value}, 设备ID={device_id}")
                                
                                # 释放设备状态
                                primary_key_field = get_table_primary_key_field(_db_instance, device_table)
                                update_device_status_to_redis(primary_key_value, 0)  # 状态0：可用
                                
                                return  # 直接返回，不执行任务
            except Exception as e:
                logger.debug(f"[任务检查] 检查订单状态时出错: {e}，继续执行任务")
        
        logger.info(f"[任务开始] 主键ID: {primary_key_value}, 设备ID: {device_id}, 视频ID: {aweme_id}")
        task_start = time.time()
        
        # 执行播放视频任务（添加超时保护，避免任务长时间阻塞）
        # 任务总超时 = stats超时 + 其他阶段预留时间（20秒）
        task_timeout = _stats_timeout + 20.0
        try:
            success, _ = await asyncio.wait_for(
                play_video_task(
            aweme_id=aweme_id,
            device=device,
            device_id=device_id,
            device_table=device_table,
            primary_key_value=primary_key_value,
            db=_db_instance,
            api=_api_instance,
            http_client=_http_client_instance,
            order_id=order_id
                ),
                timeout=task_timeout
            )
        except asyncio.TimeoutError:
            task_elapsed = time.time() - task_start
            logger.error(f"[任务超时] 播放视频任务超时（{task_timeout:.0f}秒），主键ID: {primary_key_value}, 设备ID: {device_id}, 视频ID: {aweme_id}，实际耗时: {task_elapsed:.1f}秒")
            logger.error(f"[超时分析] 可能原因：1) HTTP请求卡住 2) 数据库操作阻塞 3) 代理无响应 4) 线程池耗尽")
            logger.error(f"[超时分析] 配置的stats超时: {_stats_timeout}秒，任务总超时: {task_timeout:.0f}秒")
            success = False
        
        # 请求完成，打印主键ID
        task_elapsed = time.time() - task_start
        logger.info(f"[任务完成] 主键ID: {primary_key_value}, 设备ID: {device_id}, 结果: {'成功' if success else '失败'}, 视频ID: {aweme_id}, 耗时: {task_elapsed:.2f}秒")
        
        # 更新任务统计
        with _task_stats_lock:
            _task_stats["total_completed"] += 1
            if success:
                _task_stats["total_success"] += 1
            else:
                _task_stats["total_failed"] += 1
        
        if success:
            logger.debug(f"任务执行成功: 主键ID={primary_key_value}, device_id={device_id}, aweme_id={aweme_id}")
            # 成功时重置失败计数
            with _device_fail_lock:
                if device_id in _device_fail_count:
                    _device_fail_count[device_id] = 0
        else:
            logger.warning(f"任务执行失败: 主键ID={primary_key_value}, device_id={device_id}, aweme_id={aweme_id}")
            # 失败时增加失败计数
            with _device_fail_lock:
                current_fail_count = _device_fail_count.get(device_id, 0) + 1
                _device_fail_count[device_id] = current_fail_count
                
                # 如果连续失败超过阈值，标记设备为异常状态
                if current_fail_count >= _device_fail_threshold:
                    logger.warning(f"设备 {device_id} 连续失败 {current_fail_count} 次（阈值: {_device_fail_threshold}），标记为异常状态")
                    # 更新设备状态为 4（连续失败异常状态）- 使用Redis批量更新
                    try:
                        if primary_key_value:
                            # 将设备状态变更记录到Redis队列（目标状态=4，连续失败异常）
                            set_device_status_in_redis(primary_key_value, target_status=4)
                            logger.info(f"✓ 设备 {device_id} (primary_key={primary_key_value}) 异常状态标记已提交到Redis队列")
                        else:
                            logger.warning(f"设备 {device_id} 的主键值未找到，无法更新状态")
                    except Exception as e:
                        logger.error(f"更新设备 {device_id} 状态失败: {e}")
                        import traceback
                        logger.error(traceback.format_exc())
                
    except Exception as e:
        device_id = task_data.get('device_id', '')
        primary_key_value = task_data.get('primary_key_value')
        aweme_id = task_data.get('aweme_id', '')
        # 请求完成（异常），打印主键ID
        logger.info(f"[请求完成] 主键ID: {primary_key_value}, 设备ID: {device_id}, 结果: 异常, 视频ID: {aweme_id}")
        logger.error(f"任务执行异常: {e}")
        import traceback
        logger.error(traceback.format_exc())
        # 异常也视为失败，增加失败计数
        device_table = task_data.get('device_table', _device_table_name)
        primary_key_value = task_data.get('primary_key_value')
        
        with _device_fail_lock:
            current_fail_count = _device_fail_count.get(device_id, 0) + 1
            _device_fail_count[device_id] = current_fail_count
            
            # 如果连续失败超过阈值，标记设备为异常状态
            if current_fail_count >= _device_fail_threshold:
                logger.warning(f"设备 {device_id} 连续失败 {current_fail_count} 次（阈值: {_device_fail_threshold}），标记为异常状态")
                try:
                    if primary_key_value:
                        primary_key_field = get_table_primary_key_field(_db_instance, device_table)
                        
                        # 更新设备状态 - 在线程池中执行，避免阻塞事件循环
                        def update_device_status():
                            update_status_sql = f"""
                                UPDATE {device_table} 
                                SET status = 4
                                WHERE {primary_key_field} = %s
                            """
                            with _db_instance.get_cursor() as cursor:
                                cursor.execute(update_status_sql, (primary_key_value,))
                                if not _db_instance.autocommit:
                                    _db_instance._get_connection().commit()
                        
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, update_device_status)
                        logger.info(f"✓ 设备 {device_id} 在表 {device_table} 中已标记为连续失败异常状态 (status=4)")
                except Exception as update_error:
                    logger.error(f"更新设备 {device_id} 状态失败: {update_error}")
        raise


def parse_args():
    """
    解析命令行参数
    
    Returns:
        argparse.Namespace: 解析后的参数对象
    """
    parser = argparse.ArgumentParser(
        description="订单处理脚本 - 从 uni_order 表拉取订单，并发处理视频播放任务",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python order_processor.py 1    # 使用 uni_devices_1 表
  python order_processor.py 2    # 使用 uni_devices_2 表
  python order_processor.py      # 使用配置文件中的 device_table 或默认 uni_devices_1
        """
    )
    parser.add_argument(
        "table_number",
        type=int,
        nargs="?",
        default=None,
        help="设备表编号（例如: 1 表示使用 uni_devices_1 表，2 表示使用 uni_devices_2 表）"
    )
    return parser.parse_args()


def main():
    """主函数（使用消息队列）"""
    global _db_instance, _api_instance, _http_client_instance, _queue_instance, _thread_pool
    global _device_table_name, _max_concurrent, _threshold_size, _device_fail_threshold
    global _threshold_callback_queue, _threshold_callback_processor_thread, _threshold_callback_stop_event
    global _threshold_callback_stopped, _threshold_callback_queue_lock, _order_complete_stop_flag, _order_complete_stop_lock
    global _order_completed_flag, _order_completed_lock, _redis, _current_order, _current_order_lock
    global log_file
    
    # 解析命令行参数
    args = parse_args()
    
    logger.info("=" * 80)
    logger.info("订单处理程序启动（消息队列模式）")
    logger.info("=" * 80)
    
    # 输出日志文件位置
    if log_file:
        logger.info(f"📄 日志文件: {log_file}")
        logger.info(f"📊 控制台显示: INFO 及以上级别")
        logger.info(f"📊 文件记录: DEBUG 及以上级别（包含所有阶段诊断日志）")
        logger.info("=" * 80)
    else:
        logger.warning("⚠️ 日志仅输出到控制台，未写入文件")
    logger.info("=" * 80)
    
    # 初始化阈值回调队列和停止事件
    from queue import Queue
    _threshold_callback_queue = Queue()
    _threshold_callback_stop_event = threading.Event()
    with _threshold_callback_queue_lock:
        _threshold_callback_stopped = False
    
    # 启动阈值回调处理器线程
    _threshold_callback_processor_thread = threading.Thread(
        target=_threshold_callback_processor,
        daemon=True,
        name="ThresholdCallbackProcessor"
    )
    _threshold_callback_processor_thread.start()
    logger.info("阈值回调处理器线程已启动")
    
    # 连接数据库（使用连接池）
    try:
        # 使用连接池：固定数量的连接，多线程共享
        # pool_size: 核心连接数（建议设置为 10-20）
        # max_overflow: 最大溢出连接数（高峰期临时创建）
        # 数据库连接池大小应该 >= 并发数，避免任务等待连接
        # pool_size + max_overflow 应该 >= max_concurrent
        _db_instance = MySQLConnectionPool(
            pool_size=50,  # 核心连接数：50个固定连接
            max_overflow=200,  # 最大溢出：高峰期可增加200个（总共最多250个）
            timeout=30  # 获取连接超时：30秒
        )
        stats = _db_instance.get_stats()
        logger.info(f"数据库连接池初始化成功: {stats}")
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        sys.exit(1)
    
    # 初始化 Redis 客户端
    try:
        global _redis
        _redis = RedisClient()
        logger.info("Redis客户端初始化成功")
        
        # 测试 Redis 连接
        if _redis.ping():
            logger.info("Redis连接测试成功")
    except Exception as e:
        logger.error(f"Redis连接失败: {e}")
        logger.warning("程序将继续运行，但数据更新将直接写入数据库（性能较低）")
        _redis = None
    
    # 加载配置
    config = ConfigLoader._load_config_file()
    mq_config = config.get("message_queue", {})
    order_config = config.get("order_processor", {})
    
    # 从配置文件读取参数
    _max_concurrent = order_config.get("max_concurrent", 1000)
    _threshold_size = order_config.get("threshold_size", 3 * _max_concurrent)  # 默认阈值为并发数的3倍
    
    # 确定设备表名（优先级：命令行参数 > 配置文件 > 默认值）
    if args.table_number is not None:
        # 命令行参数优先
        _device_table_name = f"uni_devices_{args.table_number}"
        logger.info(f"使用命令行参数指定的设备表: {_device_table_name}")
    else:
        # 从配置文件读取，如果没有则使用默认值
        _device_table_name = order_config.get("device_table", "uni_devices_1")
        logger.info(f"使用配置文件中的设备表: {_device_table_name}")
    
    # 从配置文件读取设备连续失败阈值（必须配置）
    if "device_fail_threshold" not in order_config:
        logger.error("配置文件中未设置 device_fail_threshold (order_processor.device_fail_threshold)")
        sys.exit(1)
    _device_fail_threshold = order_config.get("device_fail_threshold")
    if not isinstance(_device_fail_threshold, int) or _device_fail_threshold <= 0:
        logger.error(f"配置文件中 device_fail_threshold 必须为正整数，当前值: {_device_fail_threshold}")
        sys.exit(1)
    
    # 从配置文件读取stats超时和请求延迟配置
    global _stats_timeout, _request_delay_min, _request_delay_max
    _stats_timeout = order_config.get("stats_timeout", 45.0)
    _request_delay_min = order_config.get("request_delay_min", 0.05)
    _request_delay_max = order_config.get("request_delay_max", 0.15)
    
    logger.info(f"并发数配置: {_max_concurrent}")
    logger.info(f"队列阈值配置: {_threshold_size}")
    logger.info(f"设备表名: {_device_table_name}")
    logger.info(f"设备连续失败阈值: {_device_fail_threshold}")
    logger.info(f"Stats请求超时: {_stats_timeout}秒")
    logger.info(f"请求延迟范围: {_request_delay_min*1000:.0f}-{_request_delay_max*1000:.0f}ms")
    
    # 优先从 order_processor 读取代理，如果没有则从 message_queue 读取
    proxy = order_config.get("proxy", "")
    if not proxy:
        proxy = mq_config.get("proxy", "")
    
    if not proxy:
        logger.error("配置文件中未设置代理 (order_processor.proxy 或 message_queue.proxy)")
        sys.exit(1)
    
    logger.info(f"使用代理: {proxy[:50]}..." if len(proxy) > 50 else f"使用代理: {proxy}")
    
    # 从配置文件读取 session 管理参数
    # 优先从 order_processor 读取，如果没有则从 message_queue 读取
    max_session_usage = order_config.get("max_session_usage", mq_config.get("max_session_usage", 100))
    max_pool_size = order_config.get("max_pool_size", mq_config.get("pool_max_size", 5000))
    
    logger.info(f"Session 管理配置: max_session_usage={max_session_usage}, max_pool_size={max_pool_size}")
    
    # 创建 TikTokAPI 实例
    _api_instance = TikTokAPI(
        proxy=proxy,
        timeout=30,
        max_retries=1,
        retry_delay=2.0,
        pool_initial_size=mq_config.get("pool_initial_size", 100),  # 已废弃，仅向后兼容
        pool_max_size=max_pool_size,  # 全局池最大大小（最多支持多少设备）
        pool_grow_step=mq_config.get("pool_grow_step", 10),  # 已废弃，仅向后兼容
        max_session_usage=max_session_usage,  # 每个 session 的最大使用次数，从配置文件读取
        use_global_client=True
    )
    
    _http_client_instance = _api_instance.http_client
    
    try:
        # 步骤0：刷新上次程序遗留在Redis中的设备数据到MySQL（只在程序启动时执行一次）
        # 注意：订单数据不在此刷新，只有在订单完成时才刷新
        logger.info("=" * 80)
        logger.info("步骤0：刷新上次遗留的Redis设备数据到MySQL...")
        logger.info("=" * 80)
        if _redis is not None:
            # 只刷新设备播放次数和状态，订单complete_num只在订单完成时更新
            flush_stats = flush_redis_to_mysql(_db_instance, _device_table_name)
            if flush_stats['devices_updated'] > 0:
                logger.info(f"发现并处理了上次遗留的设备数据: 设备={flush_stats['devices_updated']}")
                # 刷新后只清理设备缓存，订单缓存在步骤1重新加载时会被覆盖
                clear_redis_cache(clear_orders=False)
            else:
                logger.info("没有发现上次遗留的Redis设备数据")
        else:
            logger.info("Redis未连接，跳过步骤0")
        
        # 主循环：持续处理订单
        logger.info("=" * 80)
        logger.info("进入订单处理主循环...")
        logger.info("=" * 80)
        
        while True:  # 外层循环：持续等待和处理订单
            global _order_completed_flag, _order_completed_lock, _current_order
            
            try:
                logger.info("")
                logger.info("🔄" * 40)
                logger.info("🔄 开始新一轮订单处理循环")
                logger.info("🔄" * 40)
                logger.info("")
                
                # 步骤1：加载所有待处理订单到Redis
                logger.info("=" * 80)
                logger.info("步骤1：加载所有待处理订单到Redis...")
                logger.info("=" * 80)
                if _redis is not None:
                    orders_loaded = load_orders_to_redis(_db_instance)
                    if orders_loaded > 0:
                        logger.info(f"✅ 成功加载 {orders_loaded} 个订单到Redis")
                    else:
                        # 没有订单时，等待订单而不是退出
                        logger.warning("没有找到待处理订单，程序将等待新订单...")
                        logger.info("提示：程序将每30秒检查一次是否有新订单，按 Ctrl+C 可退出")
                        
                        # 等待订单出现
                        wait_interval = 30  # 每30秒检查一次
                        while True:
                            try:
                                time.sleep(wait_interval)
                                logger.info(f"[等待订单] 检查是否有新订单...")
                                
                                # 重新加载订单
                                orders_loaded = load_orders_to_redis(_db_instance)
                                if orders_loaded > 0:
                                    logger.info(f"✅ 发现 {orders_loaded} 个新订单，开始处理...")
                                    break
                                else:
                                    logger.info(f"[等待订单] 暂无新订单，{wait_interval}秒后再次检查...")
                            except Exception as e:
                                logger.error(f"检查订单时出错: {e}")
                                logger.info(f"{wait_interval}秒后重试...")
                else:
                    logger.error("Redis未连接，无法加载订单，程序退出")
                    sys.exit(1)
        
                # 步骤2：重置设备状态（将所有 status in (0,1) 的设备更新为 status = 0）
                logger.info("=" * 80)
                logger.info("步骤2：重置设备状态...")
                logger.info("=" * 80)
                reset_count = reset_device_status(_db_instance, _device_table_name)
                logger.info(f"设备状态重置完成，共重置 {reset_count} 个设备")
                
                # 统计设备总数
                try:
                    total_devices_sql = f"SELECT COUNT(*) as total FROM {_device_table_name}"
                    result = _db_instance.execute(total_devices_sql, fetch=True)
                    if result and len(result) > 0:
                        total_devices = result[0].get('total', 0)
                        logger.info(f"设备表 {_device_table_name} 总设备数: {total_devices}")
                        if total_devices > 0:
                            logger.info(f"可用设备占比: {reset_count}/{total_devices} ({reset_count/total_devices*100:.1f}%)")
                except Exception as e:
                    logger.warning(f"统计设备总数失败: {e}")
                
                # 步骤3：在事务中获取并发数数量的设备并更新状态为1（进行中）
                logger.info("=" * 80)
                logger.info(f"步骤3：在事务中获取 {_max_concurrent} 个设备并更新状态为1...")
                logger.info("=" * 80)
                initial_devices = get_and_lock_devices(
                    db=_db_instance,
                    table_name=_device_table_name,
                    limit=_max_concurrent,
                    status=0
                )
                
                if not initial_devices:
                    logger.warning("没有可用的设备（status=0），等待设备释放...")
                    logger.info("提示：程序将每30秒检查一次是否有可用设备，按 Ctrl+C 可退出")
                    
                    # 等待设备释放
                    wait_interval = 30
                    while True:
                        try:
                            time.sleep(wait_interval)
                            logger.info(f"[等待设备] 检查是否有可用设备...")
                            
                            # 重新尝试获取设备
                            initial_devices = get_and_lock_devices(
                                db=_db_instance,
                                table_name=_device_table_name,
                                limit=_max_concurrent,
                                status=0
                            )
                            
                            if initial_devices:
                                logger.info(f"✅ 获取到 {len(initial_devices)} 个可用设备")
                                break
                            else:
                                logger.info(f"[等待设备] 暂无可用设备，{wait_interval}秒后再次检查...")
                        except Exception as e:
                            logger.error(f"检查设备时出错: {e}")
                            logger.info(f"{wait_interval}秒后重试...")
                
                # 步骤4：从Redis获取第一个待处理订单
                logger.info("=" * 80)
                logger.info("步骤4：从Redis获取第一个待处理订单...")
                logger.info("=" * 80)
                orders = get_all_pending_orders_from_redis()
                
                if not orders:
                    logger.warning("Redis中没有待处理的订单（可能订单状态已改变），尝试重新从数据库加载...")
                    # 尝试重新从数据库加载订单
                    orders_loaded = load_orders_to_redis(_db_instance)
                    if orders_loaded > 0:
                        logger.info(f"✅ 重新加载了 {orders_loaded} 个订单到Redis，继续处理")
                        orders = get_all_pending_orders_from_redis()
                    
                    if not orders:
                        logger.warning("重新加载后仍然没有待处理订单，可能所有订单已完成或被其他进程处理")
                    # 将设备状态更新回 0
                    device_ids = [device.get('id') for device in initial_devices if device.get('id') is not None]
                    if device_ids:
                        update_devices_status(_db_instance, device_ids, _device_table_name, status=0)
                    # 回到外层循环重新开始
                    logger.info("返回步骤1重新检查订单...")
                    continue
                
                order = orders[0]
                order_id = order['id']
                order_info_str = order.get('order_info', '')
                order_num = order.get('order_num', 0) or 0
                video_ids = parse_video_ids(order_info_str)
                
                logger.info(f"从Redis获取到第一个订单: ID={order_id}, order_num={order_num}, Redis中共有 {len(orders)} 个待处理订单")
                
                if not video_ids:
                    logger.warning(f"订单 {order_id} 没有有效的视频ID，跳过此订单")
                    # 将设备状态更新回 0
                    device_ids = [device.get('id') for device in initial_devices if device.get('id') is not None]
                    if device_ids:
                        update_devices_status(_db_instance, device_ids, _device_table_name, status=0)
                    # 标记订单为失败状态（可选）
                    _db_instance.update("uni_order", {"status": 3}, "id = %s", (order_id,))
                    _db_instance.commit()
                    # 回到外层循环重新开始
                    logger.info("返回步骤1处理下一个订单...")
                    continue
                
                # 设置当前正在处理的订单（全局变量）
                with _current_order_lock:
                    _current_order = order
                
                # 检查订单的 proxyUrl，如果不为空则更新代理（只判断一次，后续批次复用）
                proxy_url = order.get('proxyUrl', '') or order.get('proxy_url', '')
                # 如果 Redis 中的订单信息没有 proxyUrl，尝试从数据库读取
                if not proxy_url:
                    try:
                        order_from_db = _db_instance.select_one("uni_order", where="id = %s", where_params=(order_id,))
                        if order_from_db:
                            proxy_url = order_from_db.get('proxyUrl', '') or order_from_db.get('proxy_url', '')
                    except Exception as e:
                        logger.debug(f"从数据库读取订单 {order_id} 的 proxyUrl 失败: {e}")
                
                if proxy_url:
                    try:
                        _api_instance.update_proxy(proxy_url)
                        logger.info(f"✅ 订单 {order_id} 的 proxyUrl 不为空，已更新代理为: {proxy_url[:50]}..." if len(proxy_url) > 50 else f"✅ 订单 {order_id} 的 proxyUrl 不为空，已更新代理为: {proxy_url}")
                    except Exception as e:
                        logger.error(f"更新代理失败: {e}")
                else:
                    logger.debug(f"订单 {order_id} 的 proxyUrl 为空，使用默认代理")
                
                logger.info(f"使用订单 {order_id}，视频ID数量: {len(video_ids)}，order_num={order_num}，已设置为当前处理订单")
                
                # 步骤5：创建初始任务列表
                logger.info("=" * 80)
                logger.info("步骤5：创建初始任务列表...")
                logger.info("=" * 80)
                initial_tasks = []
                primary_key_field = get_table_primary_key_field(_db_instance, _device_table_name)
                
                for device in initial_devices:
                    # 获取主键值
                    primary_key_value = device.get(primary_key_field)
                    
                    if not primary_key_value:
                        logger.warning(f"设备主键值为空，跳过")
                        continue
                    
                    # 解析 device_config
                    device_config_str = device.get('device_config', '')
                    if device_config_str:
                        device_dict = parse_device_config(device_config_str)
                    else:
                        device_dict = {}
                    
                    # 从解析后的 device_config 中获取 device_id
                    device_id = device_dict.get('device_id', '')
                    if not device_id:
                        # 如果没有device_id，使用主键值作为标识
                        device_id = str(primary_key_value)
                    
                    # 随机选择一个视频ID
                    aweme_id = random.choice(video_ids)
                    
                    # 创建任务
                    task = {
                        "aweme_id": aweme_id,
                        "device": device_dict,
                        "device_id": device_id,
                        "device_table": _device_table_name,
                        "primary_key_value": primary_key_value,
                        "order_id": order_id
                    }
                    initial_tasks.append(task)
                
                logger.info(f"创建了 {len(initial_tasks)} 个初始任务")
                
                # 步骤6：创建消息队列（如果还没创建）
                logger.info("=" * 80)
                if _queue_instance is None:
                    logger.info("步骤6：创建消息队列...")
                    logger.info("=" * 80)
                    _queue_instance = MessageQueue(
                        max_concurrent=_max_concurrent,
                        threshold_callback=threshold_callback,
                        task_callback=task_callback,
                        task_timeout=300.0  # 5 分钟超时（视频播放任务）
                    )
                    
                    # 步骤7：启动队列
                    logger.info("=" * 80)
                    logger.info("步骤7：启动消息队列...")
                    logger.info("=" * 80)
                    _queue_instance.start()
                    
                    # 等待队列启动
                    logger.info("等待队列启动...")
                    max_wait = 10
                    wait_count = 0
                    while not _queue_instance.is_running and wait_count < max_wait * 10:
                        time.sleep(0.1)
                        wait_count += 1
                        if wait_count % 10 == 0:
                            logger.info(f"等待队列启动... ({wait_count/10:.1f}秒)")
                    
                    if not _queue_instance.is_running:
                        logger.error("队列启动超时，返回步骤1重试")
                        continue
                    
                    logger.info("队列已启动")
                    
                    # 步骤6.5：为事件循环设置专用线程池（解决线程池耗尽问题）
                    logger.info("=" * 80)
                    logger.info("步骤6.5：设置专用线程池...")
                    logger.info("=" * 80)
                    import concurrent.futures
                    # 创建一个足够大的线程池：max_concurrent * 3（每个worker可能同时使用多个线程）
                    thread_pool_size = _max_concurrent * 3
                    _thread_pool = concurrent.futures.ThreadPoolExecutor(
                        max_workers=thread_pool_size,
                        thread_name_prefix="OrderProcessor"
                    )
                    
                    # 将线程池设置为事件循环的默认executor
                    if _queue_instance.loop:
                        _queue_instance.loop.set_default_executor(_thread_pool)
                        logger.info(f"已为事件循环设置专用线程池，大小: {thread_pool_size} 个线程")
                    else:
                        logger.error("队列的事件循环不存在，无法设置线程池")
                        continue
                    
                    # 步骤6.8：启动设备状态监控线程
                    logger.info("=" * 80)
                    logger.info("步骤6.8：启动设备状态监控线程...")
                    logger.info("=" * 80)
                    global _monitor_thread, _monitor_stop_event
                    _monitor_stop_event.clear()
                    _monitor_thread = threading.Thread(
                        target=device_status_monitor,
                        name="DeviceMonitor",
                        daemon=True
                    )
                    _monitor_thread.start()
                    logger.info("设备状态监控线程已启动（每30秒报告一次）")
                else:
                    logger.info("消息队列已存在，跳过创建步骤")
                    logger.info("=" * 80)
                
                # 步骤8：添加初始任务到队列
                logger.info("=" * 80)
                logger.info("步骤8：添加初始任务到队列...")
                logger.info("=" * 80)
                
                initial_added = 0
                initial_failed = 0
                for idx, task in enumerate(initial_tasks):
                    success = _queue_instance.add_task(task)
                    if success:
                        initial_added += 1
                    else:
                        initial_failed += 1
                        logger.error(f"初始任务添加失败（第 {idx+1}/{len(initial_tasks)} 个）")
                
                if initial_failed > 0:
                    logger.warning(f"初始任务添加完成: 成功={initial_added}, 失败={initial_failed}, 总计={len(initial_tasks)}")
                    if initial_added == 0:
                        logger.error("所有初始任务添加失败，返回步骤1重试")
                        continue
                else:
                    logger.info(f"已成功添加 {initial_added} 个初始任务到队列")
                
                # 步骤9：主循环（监控队列状态）
                logger.info("=" * 80)
                logger.info("步骤9：进入内层循环，监控队列状态...")
                logger.info("=" * 80)
                
                stats_every = order_config.get("stats_every", 5)
                
                # Session 池监控变量
                last_session_check = time.time()
                session_check_interval = 60  # 每 60 秒检查一次 session 池
                
                # 性能监控变量
                last_performance_check = time.time()
                performance_check_interval = 60  # 每 60 秒检查一次性能指标
                last_completed_count = 0
                last_failed_count = 0
                
                # 停止原因变量
                stop_reason = "未知原因"
                
                # 无设备等待计数器
                no_device_wait_count = 0
                max_no_device_wait = 6  # 最多等待6个周期（30秒，如果stats_every=5）
                
                try:
                    while _queue_instance.is_running:
                        time.sleep(stats_every)  # 主循环中的 sleep，用于控制统计频率
                        
                        queue_stats = _queue_instance.get_stats()
                        queue_size = queue_stats.get("queue_size", 0)
                        running_tasks = queue_stats.get("running_tasks", 0)
                        completed_tasks = queue_stats.get("completed_tasks", 0)
                        failed_tasks = queue_stats.get("failed_tasks", 0)
                
                        # 获取当前订单的实时进度（从Redis）
                        order_progress_info = ""
                        with _current_order_lock:
                            if _current_order:
                                current_order_id = _current_order.get('id')
                                if current_order_id and _redis:
                                    try:
                                        redis_complete = get_order_complete_from_redis(current_order_id)
                                        redis_order_num = get_order_num_from_redis(current_order_id)
                                        if redis_order_num is not None:
                                            order_progress_info = f", 订单{current_order_id}进度={redis_complete}/{redis_order_num}"
                                        else:
                                            order_progress_info = f", 订单{current_order_id}进度={redis_complete}"
                                    except:
                                        pass
                        
                        logger.info(f"队列状态: 队列大小={queue_size}, 运行中={running_tasks}/{_max_concurrent}, "
                                  f"队列任务完成={completed_tasks}, 失败={failed_tasks}{order_progress_info}")
                        
                        # 检查失败率，如果过高则发出警告
                        total_tasks = completed_tasks + failed_tasks
                        if total_tasks > 50:  # 至少有50个任务后才开始检查
                            failure_rate = (failed_tasks / total_tasks * 100) if total_tasks > 0 else 0
                            if failure_rate > 50:
                                logger.error(f"🔴 任务失败率过高: {failure_rate:.1f}% ({failed_tasks}/{total_tasks})")
                                logger.error(f"🔴 建议：1) 检查代理质量 2) 降低并发数（当前{_max_concurrent}）3) 增加请求延迟")
                                logger.error(f"🔴 如果持续失败，请考虑暂停程序检查配置")
                        
                        # 检查是否队列为空且没有可用设备
                        if queue_size == 0 and running_tasks == 0:
                            # 队列完全空了，检查是否有可用设备
                            try:
                                check_devices = get_devices_from_table(_db_instance, _device_table_name, limit=1, status=0)
                                if not check_devices:
                                    no_device_wait_count += 1
                                    logger.warning(f"⚠️ 队列已空且没有可用设备 (等待 {no_device_wait_count}/{max_no_device_wait})")
                                    
                                    if no_device_wait_count >= max_no_device_wait:
                                        # 检查是否所有设备都已使用过（status=3）
                                        logger.info("检查是否所有设备都已使用过...")
                                        try:
                                            # 统计各状态设备数量
                                            status_sql = f"""
                                            SELECT status, COUNT(*) as count
                                            FROM {_device_table_name}
                                            GROUP BY status
                                            """
                                            status_results = _db_instance.execute(status_sql, fetch=True)
                                            status_counts = {row['status']: row['count'] for row in status_results}
                                            
                                            total_devices = sum(status_counts.values())
                                            completed_devices = status_counts.get(3, 0)  # status=3: 已完成
                                            failed_devices = status_counts.get(4, 0)  # status=4: 连续失败异常
                                            
                                            logger.info(f"设备状态统计: 总数={total_devices}, 已完成={completed_devices}, 失败={failed_devices}")
                                            
                                            # 如果大部分设备都已完成（>80%），则重置设备状态
                                            if total_devices > 0 and (completed_devices + failed_devices) / total_devices > 0.8:
                                                logger.info("=" * 80)
                                                logger.info(f"🔄 检测到 {(completed_devices + failed_devices) / total_devices * 100:.1f}% 的设备已使用过")
                                                logger.info("🔄 重置设备状态，让所有设备可以重新使用...")
                                                logger.info("=" * 80)
                                                
                                                # 重置设备状态：将 status=3 和 status=1 的设备改为 status=0
                                                reset_count = reset_device_status(_db_instance, _device_table_name)
                                                logger.info(f"✅ 设备状态已重置，{reset_count} 个设备可重新使用")
                                                
                                                # 重置等待计数器，继续处理
                                                no_device_wait_count = 0
                                                continue
                                            else:
                                                logger.warning(f"设备使用率较低 ({(completed_devices + failed_devices) / total_devices * 100:.1f}%)，不重置")
                                        except Exception as e:
                                            logger.error(f"检查设备状态时出错: {e}")
                                        
                                        stop_reason = "队列已空且持续无可用设备"
                                        logger.info(f"[队列停止] 原因: {stop_reason}")
                                        logger.info(f"[队列停止] 所有任务已完成，设备表中无可用设备，程序将优雅退出")
                                        break
                                else:
                                    # 有设备了，重置计数器
                                    if no_device_wait_count > 0:
                                        logger.info(f"✅ 发现可用设备，恢复正常运行")
                                        no_device_wait_count = 0
                            except Exception as e:
                                logger.error(f"检查可用设备时出错: {e}")
                        else:
                            # 队列不为空，重置计数器
                            if no_device_wait_count > 0:
                                no_device_wait_count = 0
                        
                        # 定期检查 Session 池状态
                        if time.time() - last_session_check > session_check_interval:
                            try:
                                # 获取 HttpClient 统计信息
                                http_stats = _http_client_instance.get_stats()
                                
                                # 新设计：基于 user_id 的全局池
                                pool_size = http_stats.get('pool_size', 0)
                                pool_max_size = http_stats.get('pool_max_size', 5000)
                                avg_usage = http_stats.get('avg_usage_count', 0)
                                
                                # 计算使用率
                                usage_rate = (pool_size / pool_max_size * 100) if pool_max_size > 0 else 0
                                
                                proxy_close_count = http_stats.get('proxy_close_errors', 0)
                                dead_sessions = http_stats.get('dead_sessions_removed', 0)
                                sessions_created = http_stats.get('sessions_created', 0)
                                sessions_recycled = http_stats.get('sessions_recycled', 0)
                                
                                logger.info(f"Session池状态: 设备数={pool_size}/{pool_max_size}, "
                                          f"池使用率={usage_rate:.1f}%, "
                                          f"平均使用次数={avg_usage:.1f}, "
                                          f"请求={http_stats.get('requests', 0)}, "
                                          f"失败={http_stats.get('failures', 0)}, "
                                          f"重试={http_stats.get('retries', 0)}, "
                                          f"🔴ProxyClose={proxy_close_count}, "
                                          f"创建={sessions_created}, "
                                          f"回收={sessions_recycled}, "
                                          f"失效清理={dead_sessions}")
                                
                                # 警告：池接近满载
                                if pool_size > pool_max_size * 0.9:
                                    logger.warning(f"⚠️ Session池接近满载: {pool_size}/{pool_max_size} ({usage_rate:.1f}%), "
                                                 f"建议增加 max_pool_size 或清理无用设备")
                                
                                # 警告：失败率过高
                                total_requests = http_stats.get('requests', 0)
                                total_failures = http_stats.get('failures', 0)
                                if total_requests > 100 and total_failures > 0:
                                    failure_rate = (total_failures / total_requests * 100)
                                    if failure_rate > 10:
                                        logger.warning(f"⚠️ HTTP请求失败率过高: {failure_rate:.1f}% ({total_failures}/{total_requests})")
                                
                                # 警告：Proxy Close 错误过多
                                if proxy_close_count > 0:
                                    proxy_close_rate = (proxy_close_count / total_failures * 100) if total_failures > 0 else 0
                                    if proxy_close_rate > 30:
                                        logger.warning(f"🔴 Proxy Close 错误占比过高: {proxy_close_rate:.1f}% ({proxy_close_count}/{total_failures})")
                                        logger.warning(f"   建议: 1) 检查代理质量 2) 降低 max_session_usage 3) 缩短 health_check_interval")
                                    elif proxy_close_count > 50:
                                        logger.warning(f"🔴 Proxy Close 错误总数较多: {proxy_close_count} 次")
                                
                                last_session_check = time.time()
                            except Exception as e:
                                logger.error(f"检查 Session 池状态时出错: {e}")
                        
                        # 定期检查性能指标并给出调优建议
                        if time.time() - last_performance_check > performance_check_interval:
                            try:
                                queue_stats = _queue_instance.get_stats()
                                current_completed = queue_stats.get('completed_tasks', 0)
                                current_failed = queue_stats.get('failed_tasks', 0)
                                
                                # 计算最近1分钟的完成数和失败数
                                completed_delta = current_completed - last_completed_count
                                failed_delta = current_failed - last_failed_count
                                total_delta = completed_delta + failed_delta
                                
                                if total_delta > 0:
                                    success_rate = (completed_delta / total_delta * 100)
                                    throughput = total_delta / performance_check_interval  # 每秒处理数
                                    
                                    logger.info(f"📊 性能统计（最近{performance_check_interval:.0f}秒）：")
                                    logger.info(f"   - 处理速度: {throughput:.1f} 任务/秒")
                                    logger.info(f"   - 成功率: {success_rate:.1f}% ({completed_delta}成功/{failed_delta}失败)")
                                    logger.info(f"   - 总计: 完成{current_completed}, 失败{current_failed}")
                                    
                                    # 给出性能调优建议
                                    if success_rate < 50:
                                        logger.warning(f"⚠️ [性能建议] 成功率过低({success_rate:.1f}%)，建议：")
                                        logger.warning(f"   1. 降低并发数（当前{_max_concurrent}）到 {int(_max_concurrent * 0.7)}")
                                        logger.warning(f"   2. 增加请求延迟（当前{_request_delay_min*1000:.0f}-{_request_delay_max*1000:.0f}ms）")
                                        logger.warning(f"   3. 检查代理质量和网络状况")
                                    elif success_rate > 90 and throughput < _max_concurrent * 0.3:
                                        logger.info(f"✅ [性能建议] 成功率很高({success_rate:.1f}%)但吞吐量较低，可以考虑：")
                                        logger.info(f"   1. 适当提高并发数到 {int(_max_concurrent * 1.3)}")
                                        logger.info(f"   2. 减少请求延迟（当前{_request_delay_min*1000:.0f}-{_request_delay_max*1000:.0f}ms）")
                                
                                last_completed_count = current_completed
                                last_failed_count = current_failed
                                last_performance_check = time.time()
                            except Exception as e:
                                logger.error(f"检查性能指标时出错: {e}")
                        
                        # 检查队列是否意外停止
                        if not _queue_instance.is_running:
                            stop_reason = "队列意外停止"
                            logger.error(f"[队列停止] 原因: {stop_reason}")
                            break
                        
                        # 检查队列是否完全空闲（所有任务都已完成）
                        if queue_size == 0 and running_tasks == 0:
                            # 检查是否已经有其他检查完成了订单
                            with _order_completed_lock:
                                if _order_completed_flag:
                                    logger.info("✅ 订单已被标记为完成，跳出循环切换到下一个订单")
                                    stop_reason = "订单已完成"
                                    break
                            
                            logger.info("=" * 80)
                            logger.info("检测到队列完全空闲，检查订单状态...")
                            logger.info("=" * 80)
                            
                            # 立即检查订单是否完成（完全从Redis读取）
                            try:
                                # 从Redis获取订单信息和进度
                                if not _redis:
                                    logger.warning("Redis未连接，无法检查订单状态")
                                    continue
                                
                                redis_complete = get_order_complete_from_redis(order_id)
                                redis_order_num = get_order_num_from_redis(order_id)
                                
                                if redis_order_num is None or redis_order_num == 0:
                                    logger.warning(f"订单 {order_id} 在Redis中没有order_num数据，尝试从数据库重新加载...")
                                    # 从数据库重新加载订单信息到Redis
                                    try:
                                        order_info = _db_instance.select_one("uni_order", where="id = %s", where_params=(order_id,))
                                        if order_info:
                                            # 保存订单信息到Redis
                                            order_info_json = json.dumps(order_info, ensure_ascii=False, default=str)
                                            _redis.hset(REDIS_ORDER_INFO_KEY, str(order_id), order_info_json)
                                            
                                            # 保存order_num到Redis
                                            order_num = order_info.get('order_num', 0) or 0
                                            _redis.hset(REDIS_ORDER_NUM_KEY, str(order_id), order_num)
                                            
                                            # 保存complete_num到Redis
                                            complete_num = order_info.get('complete_num', 0) or 0
                                            _redis.hset(REDIS_ORDER_COMPLETE_KEY, str(order_id), complete_num)
                                            
                                            logger.info(f"✅ 订单 {order_id} 信息已重新加载到Redis: order_num={order_num}, complete_num={complete_num}")
                                            
                                            # 重新获取Redis数据
                                            redis_complete = complete_num
                                            redis_order_num = order_num
                                        else:
                                            logger.error(f"订单 {order_id} 在数据库中不存在")
                                            continue
                                    except Exception as reload_error:
                                        logger.error(f"从数据库重新加载订单 {order_id} 失败: {reload_error}")
                                        continue
                                
                                logger.info(f"当前订单 {order_id} 状态(Redis): complete_num={redis_complete}/{redis_order_num}")
                                
                                # 完全基于Redis数据判断
                                if redis_complete >= redis_order_num:
                                    # 设置订单完成标志，取消其他检查
                                    with _order_completed_lock:
                                        _order_completed_flag = True
                                    logger.info(f"✅ 订单 {order_id} 已完成！已设置完成标志")
                                    
                                    # 更新数据库状态（使用Redis中的完成数）
                                    _db_instance.update("uni_order", {"status": 2, "complete_num": redis_complete}, "id = %s", (order_id,))
                                    _db_instance.commit()
                                    logger.info(f"✅ 订单 {order_id} 数据库状态已更新: status=2, complete_num={redis_complete}")
                                    
                                    # 查找下一个待处理订单
                                    next_orders = _db_instance.select(
                                        "uni_order",
                                        where="status IN (0, 1)",
                                        order_by="id ASC",
                                        limit=1
                                    )
                                    
                                    if next_orders:
                                        next_order = next_orders[0]
                                        next_order_id = next_order['id']
                                        logger.info(f"✅ 发现下一个订单 {next_order_id}，准备快速切换...")
                                        stop_reason = "当前订单完成，切换到下一个订单"
                                        
                                        # 快速切换：等待队列中运行的任务完成，然后立即切换
                                        logger.info(f"[快速切换] 等待队列中运行的任务完成...")
                                        max_wait_time = 30  # 最多等待30秒
                                        wait_start = time.time()
                                        while time.time() - wait_start < max_wait_time:
                                            queue_stats = _queue_instance.get_stats()
                                            running = queue_stats.get("running_tasks", 0)
                                            queue_size = queue_stats.get("queue_size", 0)
                                            if running == 0 and queue_size == 0:
                                                logger.info(f"[快速切换] ✅ 队列中所有任务已完成，立即切换")
                                                break
                                            time.sleep(0.5)  # 每0.5秒检查一次
                                        else:
                                            remaining_running = queue_stats.get("running_tasks", 0)
                                            remaining_queue = queue_stats.get("queue_size", 0)
                                            logger.warning(f"[快速切换] ⚠️ 等待超时，仍有 {remaining_running} 个运行中的任务和 {remaining_queue} 个等待的任务，继续切换")
                                    else:
                                        logger.info("没有更多待处理订单")
                                        stop_reason = "所有订单已完成"
                                    break
                                else:
                                    # 订单未完成，但队列已空
                                    logger.warning(f"⚠️ 订单 {order_id} 未完成但队列已空")
                                    logger.warning(f"   订单进度(Redis): {redis_complete}/{redis_order_num}")
                                    logger.warning(f"   任务统计: 完成={completed_tasks}, 失败={failed_tasks}")
                                    
                                    # 检查是否还有可用设备
                                    available_devices = get_devices_from_table(_db_instance, _device_table_name, limit=1, status=0)
                                    if not available_devices:
                                        logger.warning(f"   没有可用设备，订单无法继续")
                                        logger.info("结束当前订单处理，返回外层循环")
                                        stop_reason = "订单未完成但无可用设备"
                                        break
                                    else:
                                        logger.info(f"   有可用设备，但队列已空，可能是阈值回调未触发")
                                        logger.info(f"   继续等待阈值回调补充任务...")
                            except Exception as e:
                                logger.error(f"检查订单状态时出错: {e}")
                                import traceback
                                logger.error(traceback.format_exc())
                        
                        # 注意：订单完成检查已经在队列空闲时立即执行
                        # 这里不再需要定期检查，避免重复查询数据库
                except Exception as inner_e:
                    stop_reason = f"主循环异常: {inner_e}"
                    logger.error(f"[队列停止] 原因: {stop_reason}")
                    import traceback
                    logger.error(traceback.format_exc())
                    # 发生异常时也跳出内层循环，回到外层循环重试
                    logger.info("发生异常，返回步骤1重新开始...")
                
                # 内层循环结束，记录原因
                logger.info(f"内层循环结束，原因: {stop_reason}")
                
                # 清理队列和资源，准备下一轮循环（强制停止模式）
                logger.info("=" * 80)
                logger.info("强制清理当前循环的资源...")
                logger.info("=" * 80)
                
                # 0. 重置订单完成标志和停止标志
                with _order_completed_lock:
                    _order_completed_flag = False
                logger.info("✓ 订单完成标志已重置")
                
                with _order_complete_stop_lock:
                    _order_complete_stop_flag = False
                logger.info("✓ 订单完成停止标志已重置")
                
                # 恢复队列的阈值补充机制
                if _queue_instance:
                    try:
                        _queue_instance.is_stopped = False
                        logger.info("✓ 队列的阈值补充机制已恢复")
                    except Exception as e:
                        logger.warning(f"恢复队列补充机制失败: {e}")
                
                # 1. 强制停止队列（不等待）
                if _queue_instance:
                    logger.info("强制停止消息队列（不等待任务完成）...")
                    try:
                        # 直接停止，不等待
                        if _queue_instance.is_running:
                            _queue_instance.stop()
                        logger.info("✓ 消息队列停止信号已发送")
                    except Exception as e:
                        logger.warning(f"停止队列时出错: {e}")
                    
                    # 不等待事件循环关闭，直接继续
                    logger.info("✓ 跳过事件循环等待（强制模式）")
                
                # 2. 刷新Redis数据到MySQL
                if _redis is not None:
                    logger.info("刷新Redis缓存数据到MySQL...")
                    try:
                        flush_stats = flush_redis_to_mysql(_db_instance, _device_table_name)
                        logger.info(f"数据刷新完成: 设备更新={flush_stats['devices_updated']}, "
                                  f"设备失败={flush_stats.get('devices_failed', 0)}")
                        
                        # 清理Redis缓存（只清理设备缓存，订单缓存在下一轮重新加载）
                        clear_redis_cache(clear_orders=False)
                        logger.info("Redis设备缓存已清理")
                    except Exception as e:
                        logger.error(f"刷新数据失败: {e}")
                
                # 3. 清理HTTP session池（避免事件循环绑定问题）
                logger.info("清理HTTP session池...")
                try:
                    http_client_async.clear_global_pool()
                    logger.info("✓ HTTP session池已清理")
                except Exception as e:
                    logger.warning(f"清理HTTP session池时出错: {e}")
                
                # 4. 强制关闭线程池（不等待）
                if _thread_pool:
                    logger.info("强制关闭线程池（不等待）...")
                    try:
                        _thread_pool.shutdown(wait=False)
                        logger.info("✓ 线程池已强制关闭")
                    except Exception as e:
                        logger.warning(f"关闭线程池时出错: {e}")
                    _thread_pool = None
                
                # 5. 强制重置队列实例
                if _queue_instance:
                    try:
                        del _queue_instance
                    except:
                        pass
                _queue_instance = None
                logger.info("✓ 队列实例已强制重置")
                
                # 6. 强制停止设备状态监控线程（不等待）
                if _monitor_thread and _monitor_thread.is_alive():
                    logger.info("强制停止设备状态监控线程（不等待）...")
                    _monitor_stop_event.set()
                    # 不等待线程结束，直接继续
                    logger.info("✓ 监控线程停止信号已发送")
                
                logger.info("=" * 80)
                logger.info("资源清理完成，准备开始新一轮循环...")
                logger.info("=" * 80)
                
                # 明确标记：即将回到外层循环开始
                logger.info("🔄 回到外层循环，重新开始步骤1...")
                
            except Exception as outer_e:
                logger.error(f"外层循环异常: {outer_e}")
                import traceback
                logger.error(traceback.format_exc())
                logger.info("30秒后重试...")
                time.sleep(30)
    except Exception as e:
        logger.error(f"程序执行异常: {e}")
        import traceback
        logger.error(traceback.format_exc())
        # 确保清理资源
        if _queue_instance and _queue_instance.is_running:
            _queue_instance.stop()
            _queue_instance.wait()
        if _db_instance:
            _db_instance.close()
        sys.exit(1)
    finally:
        # 先停止队列，等待所有任务处理完毕
        if _queue_instance:
            logger.info("正在停止消息队列（等待所有任务完成）...")
            _queue_instance.stop()
            _queue_instance.wait()
            logger.info("消息队列已停止，所有任务已处理完毕")
        
        # 不再在程序退出时刷新Redis数据
        # 数据刷新策略：
        # 1. 程序启动时刷新设备播放次数（处理上次遗留数据）
        # 2. 订单完成时刷新所有数据（设备 + 订单）
        logger.info("=" * 80)
        logger.info("程序退出，Redis数据保留在缓存中（将在下次启动时刷新）")
        logger.info("=" * 80)
        
        # 再停止阈值回调处理器线程（此时队列已空，不会再有新任务）
        if _threshold_callback_stop_event:
            logger.info("正在停止阈值回调处理器线程...")
            _threshold_callback_stop_event.set()
            if _threshold_callback_processor_thread and _threshold_callback_processor_thread.is_alive():
                _threshold_callback_processor_thread.join(timeout=5)
                if _threshold_callback_processor_thread.is_alive():
                    logger.warning("阈值回调处理器线程未在5秒内停止")
                else:
                    logger.info("阈值回调处理器线程已停止")
        
        # 停止设备状态监控线程
        if _monitor_thread and _monitor_thread.is_alive():
            logger.info("正在停止设备状态监控线程...")
            _monitor_stop_event.set()
            _monitor_thread.join(timeout=3)
            if _monitor_thread.is_alive():
                logger.warning("设备状态监控线程未在3秒内停止")
            else:
                logger.info("设备状态监控线程已停止")
        
        # 打印最终统计
        if _queue_instance:
            final_stats = _queue_instance.get_stats()
            total_tasks = final_stats.get('total_tasks', 0)
            completed_tasks = final_stats.get('completed_tasks', 0)
            failed_tasks = final_stats.get('failed_tasks', 0)
            success_rate = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
            
            logger.info("=" * 80)
            logger.info("📊 最终统计:")
            logger.info(f"  队列总任务数: {total_tasks}")
            logger.info(f"  ✅ 已完成: {completed_tasks}")
            logger.info(f"  ❌ 失败: {failed_tasks}")
            logger.info(f"  📈 成功率: {success_rate:.2f}%")
            logger.info(f"  🛑 停止原因: {stop_reason}")
            logger.info("=" * 80)
            
            # 根据成功率给出评价
            if success_rate >= 80:
                logger.info("🎉 性能评价: 优秀 - 系统运行非常稳定")
            elif success_rate >= 60:
                logger.info("✅ 性能评价: 良好 - 系统运行基本稳定")
            elif success_rate >= 40:
                logger.warning("⚠️ 性能评价: 一般 - 建议优化配置或检查网络")
            else:
                logger.error("❌ 性能评价: 较差 - 需要立即检查代理、网络或降低并发")
            
            logger.info("=" * 80)
        
        # 关闭线程池
        if _thread_pool:
            logger.info("正在关闭线程池...")
            _thread_pool.shutdown(wait=True, cancel_futures=False)
            logger.info("线程池已关闭")
        
        _db_instance.close()
        logger.info("数据库连接池已关闭")
        logger.info("程序已退出")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"程序异常退出: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

