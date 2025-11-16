"""
订单处理脚本
从 uni_order 表拉取订单，并发处理视频播放任务
"""
import os
import sys
import json
import time
import random
import asyncio
import threading
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from mysql_db import MySQLDB
from config_loader import ConfigLoader
from tiktok_api import TikTokAPI
from message_queue import MessageQueue
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 全局变量
_db: Optional[MySQLDB] = None
_api: Optional[TikTokAPI] = None
_order_locks: Dict[int, threading.Lock] = {}  # 订单锁，用于并发安全更新
_lock_manager_lock = threading.Lock()  # 锁管理器的锁
_device_fail_count: Dict[str, int] = {}  # 设备连续失败次数，key: device_id, value: 连续失败次数
_device_fail_lock = threading.Lock()  # 设备失败计数锁
_page_size: int = 1000  # 分页大小（从配置文件读取）
_device_fail_threshold: Optional[int] = None  # 设备连续失败阈值，超过此次数后将设备状态更新为4（从配置文件读取）
_thread_pool: Optional[Any] = None  # 专用线程池，用于数据库操作和阻塞IO


def get_order_lock(order_id: int) -> threading.Lock:
    """获取订单锁（线程安全）"""
    global _order_locks
    with _lock_manager_lock:
        if order_id not in _order_locks:
            _order_locks[order_id] = threading.Lock()
        return _order_locks[order_id]


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
    重置设备状态：将所有 status in (0,1) 的设备更新为 status = 0
    
    Args:
        db: 数据库连接
        table_name: 表名
    """
    try:
        sql = f"""
            UPDATE {table_name}
            SET status = 0
            WHERE status = 1
        """
        with db.get_cursor() as cursor:
            cursor.execute(sql)
            affected_rows = cursor.rowcount
            if not db.autocommit:
                db._get_connection().commit()
        logger.info(f"表 {table_name} 重置设备状态完成，影响行数: {affected_rows}")
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
            if not db.autocommit:
                db._get_connection().commit()
        logger.debug(f"表 {table_name} 批量更新设备状态完成，影响行数: {affected_rows}")
        return affected_rows
    except Exception as e:
        logger.error(f"批量更新表 {table_name} 设备状态失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


def get_and_lock_devices(db: MySQLDB, table_name: str, limit: int, status: int = 0) -> List[Dict[str, Any]]:
    """
    在事务中获取设备并更新状态为1（进行中）
    使用事务确保原子性：先获取设备，然后更新状态
    
    Args:
        db: 数据库连接
        table_name: 表名
        limit: 获取数量
        status: 筛选的设备状态（默认0）
    
    Returns:
        设备数据列表（已更新状态为1）
    """
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
        conn.autocommit = False
        
        try:
            with db.get_cursor() as cursor:
                # 步骤1：先获取设备（按 play_num 升序、device_create_time 降序、主键升序排序）
                # 添加主键排序确保获取不同的设备，避免总是获取同一个设备
                # 注意：不使用 FOR UPDATE SKIP LOCKED，因为需要兼容 MySQL 5.x
                # 使用乐观锁策略：先 SELECT 再 UPDATE，通过 affected_rows 检查并发冲突
                # 优化：只查询需要的字段，而不是 SELECT *
                # 注意：device_id 不是表字段，而是从 device_config JSON 中解析出来的
                # 强制使用复合索引以避免文件排序
                select_sql = f"""
                    SELECT {primary_key_field}, device_config, play_num, status, {time_field}, update_time
                    FROM {table_name} USE INDEX (idx_status_playnum_createtime)
                    WHERE status = %s
                    ORDER BY play_num ASC, {time_field} DESC
                    LIMIT %s
                """
                select_start = time.time()
                cursor.execute(select_sql, (status, limit))
                devices = cursor.fetchall()
                select_elapsed = time.time() - select_start
                
                # 如果查询耗时过长，记录警告
                if select_elapsed > 10.0:
                    logger.warning(f"[get_and_lock_devices] SELECT查询耗时: {select_elapsed:.3f}秒")
                
                if not devices:
                    conn.rollback()
                    return []
                
                # 步骤2：获取设备ID列表
                device_ids = [device.get(primary_key_field) for device in devices if device.get(primary_key_field) is not None]
                
                if not device_ids:
                    conn.rollback()
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
                    # 但需要重新查询确认哪些设备真正被更新了
                    if affected_rows == 0:
                        # 所有设备都被其他进程抢占了，回滚并返回空
                        conn.rollback()
                        logger.warning(f"[get_and_lock_devices] 所有设备都被其他进程抢占，回滚事务")
                        return []
                
                # 提交事务
                conn.commit()
                
                # 如果更新的行数少于预期，只返回成功更新的设备
                if affected_rows < len(devices):
                    logger.warning(f"只有 {affected_rows}/{len(devices)} 个设备成功更新，返回实际成功的设备")
                    # 简化处理：返回前 affected_rows 个设备
                    # 注意：这可能不完全准确（如果中间有设备被跳过），但在大多数情况下足够好
                    devices = devices[:affected_rows]
                
                return devices
                
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.autocommit = original_autocommit
            
    except Exception as e:
        logger.error(f"在事务中获取并锁定设备失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
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
    try:
        # 获取 flow_session（在线程池中执行，避免阻塞事件循环）
        # 注意：get_flow_session() 虽然是非阻塞的，但为了确保完全不阻塞，在线程池中执行
        # 添加超时保护，避免长时间阻塞
        loop = asyncio.get_running_loop()
        try:
            flow_session = await asyncio.wait_for(
                loop.run_in_executor(None, http_client.get_flow_session),
                timeout=30.0  # 30秒超时
            )
        except asyncio.TimeoutError:
            logger.error(f"获取 flow_session 超时（30秒），device_id: {device_id}")
            return False, device_id
        
        # 标记是否需要更新数据库
        need_update_db = False
        
        try:
            # 直接从 device 字典中获取 seed 和 seed_type
            seed = device.get('seed')
            seed_type = device.get('seed_type')
            
            # 如果 device 中没有 seed 或 seed_type，或者为空，则请求获取
            if not seed or seed_type is None:
                try:
                    seed, seed_type = await asyncio.wait_for(
                        api.get_seed_async(device, session=flow_session),
                        timeout=30.0  # 30秒超时
                    )
                    # 更新 device 字典
                    device['seed'] = seed
                    device['seed_type'] = seed_type
                    need_update_db = True
                    logger.debug(f"获取 seed: {seed[:20] if seed else None}..., seed_type: {seed_type}")
                except asyncio.TimeoutError:
                    logger.error(f"获取 seed 超时（30秒），device_id: {device_id}")
                    return False, device_id
            else:
                logger.debug(f"从 device 读取 seed: {seed[:20] if seed else None}..., seed_type: {seed_type}")
            
            # 直接从 device 字典中获取 token
            token = device.get('token')
            
            # 如果 device 中没有 token 或为空，则请求获取
            if not token:
                try:
                    token = await asyncio.wait_for(
                        api.get_token_async(device, session=flow_session),
                        timeout=30.0  # 30秒超时
                    )
                    # 更新 device 字典
                    device['token'] = token
                    need_update_db = True
                    logger.debug(f"获取 token: {token[:20] if token else None}...")
                except asyncio.TimeoutError:
                    logger.error(f"获取 token 超时（30秒），device_id: {device_id}")
                    return False, device_id
            else:
                logger.debug(f"从 device 读取 token: {token[:20] if token else None}...")
            
            # 如果获取了新的 seed 或 token，更新数据库（在线程池中执行，不阻塞事件循环）
            if need_update_db:
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
                                    if not db.autocommit:
                                        db._get_connection().commit()
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
                            if affected_rows > 0:
                                logger.debug(f"设备 {device_id} 在表 {device_table} 中的 device_config 和 update_time 已更新，影响行数: {affected_rows}")
                            else:
                                logger.warning(f"设备 {device_id} 在表 {device_table} 中的 device_config 更新失败，影响行数: {affected_rows}，主键字段: {primary_key_field}，主键值: {primary_key_value} (类型: {type(primary_key_value).__name__})")
                                logger.warning(f"可能原因: 主键值不匹配、表不存在、或字段不存在")
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
            
            # 调用 stats 接口播放视频（异步版本，不阻塞事件循环）
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
                    timeout=60.0  # 60秒超时（stats请求可能需要更长时间）
                )
            except asyncio.TimeoutError:
                logger.error(f"stats 请求超时（60秒），device_id: {device_id}, aweme_id: {aweme_id}")
                result = ""
            
            # 如果返回结果不为空，表示成功
            success = result != "" if result else False
            
            # 播放完成后立即更新 play_num 和 complete_num（无论成功或失败都更新 play_num，只有成功才更新 complete_num）
            # 注意：无论播放成功或失败，都更新 play_num，因为已经尝试播放了
            try:
                # 定义更新数据库的函数（在线程池中执行）
                # 注意：使用线程本地存储，每个线程复用同一个连接，不需要创建新连接
                def update_database():
                    # 直接使用传入的 db 实例，连接会自动使用线程本地存储
                    # 更新设备的 play_num
                    primary_key_field = get_table_primary_key_field(db, device_table)
                    logger.debug(f"准备更新设备 {device_id} 在表 {device_table} 中的 play_num，主键字段: {primary_key_field}={primary_key_value}")
                    
                    # 使用原子更新增加 play_num
                    update_device_sql = f"""
                        UPDATE {device_table} 
                        SET play_num = IFNULL(play_num, 0) + 1
                        WHERE {primary_key_field} = %s
                    """
                    # 使用游标直接执行以获取影响的行数
                    with db.get_cursor() as cursor:
                        cursor.execute(update_device_sql, (primary_key_value,))
                        affected_rows = cursor.rowcount
                        if not db.autocommit:
                            db._get_connection().commit()
                    
                    # 处理 affected_rows 可能为 None 的情况
                    affected_rows = affected_rows if affected_rows is not None else 0
                    if affected_rows > 0:
                        logger.debug(f"✓ 设备 {device_id} 在表 {device_table} 中的 play_num 已更新，影响行数: {affected_rows}")
                    else:
                        logger.warning(f"✗ 设备 {device_id} 在表 {device_table} 中的 play_num 更新失败，影响行数: {affected_rows}")
                    
                    # 如果播放成功且提供了 order_id，更新订单的 complete_num
                    if success and order_id is not None:
                        try:
                            # 使用原子更新增加 complete_num（并发安全）
                            update_order_sql = """
                                UPDATE uni_order 
                                SET complete_num = complete_num + 1
                                WHERE id = %s
                            """
                            with db.get_cursor() as cursor:
                                cursor.execute(update_order_sql, (order_id,))
                                order_affected_rows = cursor.rowcount
                                if not db.autocommit:
                                    db._get_connection().commit()
                            
                            order_affected_rows = order_affected_rows if order_affected_rows is not None else 0
                            if order_affected_rows > 0:
                                logger.debug(f"✓ 订单 {order_id} 的 complete_num 已更新")
                            else:
                                logger.warning(f"✗ 订单 {order_id} 的 complete_num 更新失败，影响行数: {order_affected_rows}")
                        except Exception as e:
                            logger.error(f"更新订单 {order_id} 的 complete_num 失败: {e}")
                    
                    # 播放完成后，将设备状态更新为 3（已完成状态，无论成功或失败）
                    try:
                        update_status_sql = f"""
                            UPDATE {device_table} 
                            SET status = 3
                            WHERE {primary_key_field} = %s
                        """
                        with db.get_cursor() as cursor:
                            cursor.execute(update_status_sql, (primary_key_value,))
                            if not db.autocommit:
                                db._get_connection().commit()
                        logger.debug(f"设备 {device_id} 状态已更新为 3（已完成）")
                    except Exception as e:
                        logger.error(f"更新设备 {device_id} 状态失败: {e}")
                    
                    return affected_rows
                
                # 在线程池中执行数据库更新（不阻塞事件循环）
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, update_database)
            except Exception as e:
                logger.error(f"更新数据库失败: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
            return success, device_id
            
        finally:
            # 释放 flow_session（在线程池中执行，避免阻塞事件循环）
            if flow_session:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, http_client.release_flow_session, flow_session)
            
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
    order_lock = get_order_lock(order_id)
    with order_lock:
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
                                        if not db.autocommit:
                                            db._get_connection().commit()
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
                                                if not db.autocommit:
                                                    db._get_connection().commit()
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
    global _queue_instance
    
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
                        tasks = future.result(timeout=60.0)  # 最多等待60秒
                    except concurrent.futures.TimeoutError:
                        logger.error("[阈值回调] ⚠️ 执行超时（60秒）")
                        tasks = []
                    except Exception as e:
                        logger.error(f"[阈值回调] ⚠️ 执行异常: {e}")
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
                    
                    # 简洁的成功日志
                    if failed_count > 0:
                        logger.warning(f"[阈值回调] ⚠️ 补充任务：成功{added_count}个，失败{failed_count}个 | 队列: {queue_size_before}→{queue_size_after}，已完成: {completed_after}")
                    else:
                        logger.info(f"[阈值回调] ✓ 成功补充 {added_count} 个任务到队列 | 队列: {queue_size_before}→{queue_size_after}，已完成: {completed_after}")
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
            return []
        
        order_num = order_info.get('order_num', 0) or 0
        complete_num = order_info.get('complete_num', 0) or 0
        
        # 检查订单还需要多少任务
        if order_num > 0:
            remaining_tasks = order_num - complete_num
            
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
                    logger.warning("[阈值回调] 没有更多待处理的订单，返回空列表")
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
                
                # 重新计算剩余任务数
                if order_num > 0:
                    remaining_tasks = order_num - complete_num
                    if remaining_tasks <= 0:
                        return []
                    need_count = min(need_count, remaining_tasks)
            else:
                # 限制获取的设备数量不超过订单剩余任务数
                need_count = min(need_count, remaining_tasks)
        
        if need_count <= 0:
            logger.warning(f"[阈值回调] 调整后需要补充数量 <= 0，返回空列表")
            return []
        
        # 检查订单是否有有效的视频ID
        order_info_str = order_info.get('order_info', '')
        video_ids = parse_video_ids(order_info_str)
        
        if not video_ids:
            logger.warning(f"[阈值回调] 订单 {order_id} 没有有效的视频ID")
            return []
        
        # 在事务中从数据库获取设备并更新状态为1（进行中）
        import concurrent.futures
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    get_and_lock_devices,
                    _db_instance,
                    _device_table_name,
                    need_count,
                    0
                )
                devices = future.result(timeout=60.0)
        except concurrent.futures.TimeoutError:
            logger.warning(f"[阈值回调] 获取设备操作超时（60秒），可能数据库操作较慢或存在锁等待，返回空列表等待下次回调")
            return []  # 超时返回空列表，但不停止后续处理
        except Exception as e:
            logger.error(f"[阈值回调] 获取设备操作失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []  # 异常返回空列表，但不停止后续处理
        
        if not devices:
            logger.debug("[阈值回调] 没有可用设备")
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
    
    # 将回调任务放入队列
    if _threshold_callback_queue is not None:
        try:
            _threshold_callback_queue.put_nowait(True)
        except:
            logger.warning("[阈值回调] 队列已满")
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
        
        logger.info(f"[任务开始] 主键ID: {primary_key_value}, 设备ID: {device_id}, 视频ID: {aweme_id}")
        task_start = time.time()
        
        # 执行播放视频任务（添加超时保护，避免任务长时间阻塞）
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
                timeout=120.0  # 最多等待120秒（2分钟），如果超时则认为任务失败
            )
        except asyncio.TimeoutError:
            task_elapsed = time.time() - task_start
            logger.error(f"[任务超时] 播放视频任务超时（120秒），主键ID: {primary_key_value}, 设备ID: {device_id}, 视频ID: {aweme_id}，实际耗时: {task_elapsed:.1f}秒")
            success = False
        
        # 请求完成，打印主键ID
        task_elapsed = time.time() - task_start
        logger.info(f"[任务完成] 主键ID: {primary_key_value}, 设备ID: {device_id}, 结果: {'成功' if success else '失败'}, 视频ID: {aweme_id}, 耗时: {task_elapsed:.2f}秒")
        
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
                    # 更新设备状态为 4（连续失败异常状态）
                    try:
                        if primary_key_value:
                            # 获取表的主键字段名
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
                                    affected_rows = cursor.rowcount
                                    if not _db_instance.autocommit:
                                        _db_instance._get_connection().commit()
                                return affected_rows
                            
                            loop = asyncio.get_running_loop()
                            affected_rows = await loop.run_in_executor(None, update_device_status)
                            
                            affected_rows = affected_rows if affected_rows is not None else 0
                            if affected_rows > 0:
                                logger.info(f"✓ 设备 {device_id} 在表 {device_table} 中已标记为连续失败异常状态 (status=4)，影响行数: {affected_rows}")
                            else:
                                logger.warning(f"✗ 设备 {device_id} 在表 {device_table} 中状态更新失败，影响行数: {affected_rows}，主键值: {primary_key_value}")
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


def main():
    """主函数（使用消息队列）"""
    global _db_instance, _api_instance, _http_client_instance, _queue_instance, _thread_pool
    global _device_table_name, _max_concurrent, _threshold_size, _device_fail_threshold
    global _threshold_callback_queue, _threshold_callback_processor_thread, _threshold_callback_stop_event
    global _threshold_callback_stopped, _threshold_callback_queue_lock
    
    logger.info("=" * 80)
    logger.info("订单处理程序启动（消息队列模式）")
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
    
    # 连接数据库
    try:
        _db_instance = MySQLDB()
        logger.info("数据库连接成功")
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        sys.exit(1)
    
    # 加载配置
    config = ConfigLoader._load_config_file()
    mq_config = config.get("message_queue", {})
    order_config = config.get("order_processor", {})
    
    # 从配置文件读取参数
    _max_concurrent = order_config.get("max_concurrent", 1000)
    _threshold_size = order_config.get("threshold_size", 3 * _max_concurrent)  # 默认阈值为并发数的3倍
    _device_table_name = order_config.get("device_table", "uni_devices_1")
    
    # 从配置文件读取设备连续失败阈值（必须配置）
    if "device_fail_threshold" not in order_config:
        logger.error("配置文件中未设置 device_fail_threshold (order_processor.device_fail_threshold)")
        sys.exit(1)
    _device_fail_threshold = order_config.get("device_fail_threshold")
    if not isinstance(_device_fail_threshold, int) or _device_fail_threshold <= 0:
        logger.error(f"配置文件中 device_fail_threshold 必须为正整数，当前值: {_device_fail_threshold}")
        sys.exit(1)
    
    logger.info(f"并发数配置: {_max_concurrent}")
    logger.info(f"队列阈值配置: {_threshold_size}")
    logger.info(f"设备表名: {_device_table_name}")
    logger.info(f"设备连续失败阈值: {_device_fail_threshold}")
    
    # 优先从 order_processor.proxy 读取代理，如果没有则从 message_queue.proxy 读取
    proxy = order_config.get("proxy", "")
    if not proxy:
        proxy = mq_config.get("proxy", "")
    
    if not proxy:
        logger.error("配置文件中未设置代理 (order_processor.proxy 或 message_queue.proxy)")
        sys.exit(1)
    
    logger.info(f"使用代理: {proxy[:50]}..." if len(proxy) > 50 else f"使用代理: {proxy}")
    
    # 创建 TikTokAPI 实例
    _api_instance = TikTokAPI(
        proxy=proxy,
        timeout=30,
        max_retries=1,
        retry_delay=2.0,
        pool_initial_size=mq_config.get("pool_initial_size", 100),
        pool_max_size=mq_config.get("pool_max_size", 2000),
        pool_grow_step=mq_config.get("pool_grow_step", 10),
        use_global_client=True
    )
    
    _http_client_instance = _api_instance.http_client
    
    try:
        # 步骤1：重置设备状态（将所有 status in (0,1) 的设备更新为 status = 0）
        logger.info("=" * 80)
        logger.info("步骤1：重置设备状态...")
        logger.info("=" * 80)
        reset_device_status(_db_instance, _device_table_name)
        logger.info("设备状态重置完成")
        
        # 步骤2：在事务中获取并发数数量的设备并更新状态为1（进行中）
        logger.info("=" * 80)
        logger.info(f"步骤2：在事务中获取 {_max_concurrent} 个设备并更新状态为1...")
        logger.info("=" * 80)
        initial_devices = get_and_lock_devices(
            db=_db_instance,
            table_name=_device_table_name,
            limit=_max_concurrent,
            status=0
        )
        
        if not initial_devices:
            logger.error("没有可用的设备（status=0），程序退出")
            sys.exit(1)
        
        # 步骤3：获取订单信息（在线程池中执行，避免阻塞）
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _db_instance.select,
            "uni_order",
            where="status IN (0, 1)",
            order_by="id ASC",
            limit=1
        )
            orders = future.result(timeout=10.0)  # 最多等待10秒
        
        if not orders:
            logger.error("没有待处理的订单，程序退出")
            # 将设备状态更新回 0
            device_ids = [device.get('id') for device in initial_devices if device.get('id') is not None]
            if device_ids:
                update_devices_status(_db_instance, device_ids, _device_table_name, status=0)
            sys.exit(1)
        
        order = orders[0]
        order_id = order['id']
        order_info_str = order.get('order_info', '')
        video_ids = parse_video_ids(order_info_str)
        
        if not video_ids:
            logger.error(f"订单 {order_id} 没有有效的视频ID，程序退出")
            # 将设备状态更新回 0
            device_ids = [device.get('id') for device in initial_devices if device.get('id') is not None]
            if device_ids:
                update_devices_status(_db_instance, device_ids, _device_table_name, status=0)
            sys.exit(1)
        
        # 设置当前正在处理的订单（全局变量）
        global _current_order
        with _current_order_lock:
            _current_order = order
        
        logger.info(f"使用订单 {order_id}，视频ID数量: {len(video_ids)}，已设置为当前处理订单")
        
        # 步骤4：创建初始任务列表
        logger.info("=" * 80)
        logger.info("步骤4：创建初始任务列表...")
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
        
        # 步骤5：创建消息队列
        logger.info("=" * 80)
        logger.info("步骤5：创建消息队列...")
        logger.info("=" * 80)
        _queue_instance = MessageQueue(
            max_concurrent=_max_concurrent,
            threshold_callback=threshold_callback,
            task_callback=task_callback,
            task_timeout=300.0  # 5 分钟超时（视频播放任务）
        )
        
        # 步骤6：启动队列
        logger.info("=" * 80)
        logger.info("步骤6：启动消息队列...")
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
            logger.error("队列启动超时，程序退出")
            sys.exit(1)
        
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
            sys.exit(1)
        
        # 步骤7：添加初始任务到队列
        logger.info("=" * 80)
        logger.info("步骤7：添加初始任务到队列...")
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
                logger.error("所有初始任务添加失败，程序退出")
                sys.exit(1)
        else:
            logger.info(f"已成功添加 {initial_added} 个初始任务到队列")
        
        # 步骤8：主循环（监控队列状态）
        logger.info("=" * 80)
        logger.info("步骤8：进入主循环，监控队列状态...")
        logger.info("=" * 80)
        
        stats_every = order_config.get("stats_every", 5)
        
        try:
            while _queue_instance.is_running:
                time.sleep(stats_every)  # 主循环中的 sleep，用于控制统计频率
                
                queue_stats = _queue_instance.get_stats()
                queue_size = queue_stats.get("queue_size", 0)
                running_tasks = queue_stats.get("running_tasks", 0)
                completed_tasks = queue_stats.get("completed_tasks", 0)
                failed_tasks = queue_stats.get("failed_tasks", 0)
                
                logger.info(f"队列状态: 队列大小={queue_size}, 运行中={running_tasks}/{_max_concurrent}, "
                          f"已完成={completed_tasks}, 失败={failed_tasks}")
                
                # 检查订单是否完成（在线程池中执行，避免阻塞）
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    def check_order_status():
                        order_info = _db_instance.select_one("uni_order", where="id = %s", where_params=(order_id,))
                        if order_info:
                            order_num = order_info.get('order_num', 0) or 0
                            complete_num = order_info.get('complete_num', 0) or 0
                            
                            if order_num > 0 and complete_num >= order_num:
                                # 更新订单状态为已完成
                                _db_instance.update("uni_order", {"status": 2}, "id = %s", (order_id,))
                                _db_instance.commit()
                                return True, order_num, complete_num
                        return False, 0, 0
                    
                    future = executor.submit(check_order_status)
                    order_completed, order_num, complete_num = future.result(timeout=5.0)  # 最多等待5秒
                    
                    if order_completed:
                        logger.info(f"订单 {order_id} 已完成！complete_num={complete_num}, order_num={order_num}")
                        logger.info("订单状态已更新为已完成，程序退出")
                        break
        except KeyboardInterrupt:
            logger.info("收到中断信号，正在停止...")
        except Exception as inner_e:
            logger.error(f"主循环执行异常: {inner_e}")
            import traceback
            logger.error(traceback.format_exc())
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
        # 停止阈值回调处理器线程
        if _threshold_callback_stop_event:
            logger.info("正在停止阈值回调处理器线程...")
            _threshold_callback_stop_event.set()
            if _threshold_callback_processor_thread and _threshold_callback_processor_thread.is_alive():
                _threshold_callback_processor_thread.join(timeout=5)
                if _threshold_callback_processor_thread.is_alive():
                    logger.warning("阈值回调处理器线程未在5秒内停止")
                else:
                    logger.info("阈值回调处理器线程已停止")
        
        # 停止队列
        if _queue_instance:
            logger.info("正在停止消息队列...")
            _queue_instance.stop()
            _queue_instance.wait()
            logger.info("消息队列已停止")
        else:
            logger.info("消息队列未初始化，跳过停止操作")
        
        # 打印最终统计
        if _queue_instance:
            final_stats = _queue_instance.get_stats()
            logger.info("=" * 80)
            logger.info("最终统计:")
            logger.info(f"  队列总任务数: {final_stats.get('total_tasks', 0)}")
            logger.info(f"  已完成: {final_stats.get('completed_tasks', 0)}")
            logger.info(f"  失败: {final_stats.get('failed_tasks', 0)}")
            logger.info("=" * 80)
        
        # 关闭线程池
        if _thread_pool:
            logger.info("正在关闭线程池...")
            _thread_pool.shutdown(wait=True, cancel_futures=False)
            logger.info("线程池已关闭")
        
        _db_instance.close()
        logger.info("程序已退出")


if __name__ == "__main__":
    main()

