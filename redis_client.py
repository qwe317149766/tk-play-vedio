"""
Redis 连接类
封装基本的操作函数和分布式锁
"""
import redis
import time
import uuid
import threading
from typing import Optional, Any, List, Dict, Set, Union
from contextlib import contextmanager
import logging
from config_loader import ConfigLoader

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RedisClient:
    """Redis 连接客户端类"""
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: Optional[int] = None,
        password: Optional[str] = None,
        decode_responses: Optional[bool] = None,
        socket_timeout: Optional[int] = None,
        socket_connect_timeout: Optional[int] = None,
        max_connections: Optional[int] = None,
        default_ex: Optional[int] = None,
        config_path: Optional[str] = None,
        **kwargs
    ):
        """
        初始化 Redis 客户端
        
        Args:
            host: Redis 主机地址
            port: Redis 端口
            db: 数据库编号
            password: Redis 密码
            decode_responses: 是否自动解码响应（返回字符串而不是字节）
            socket_timeout: Socket 超时时间（秒）
            socket_connect_timeout: Socket 连接超时时间（秒）
            max_connections: 连接池最大连接数
            default_ex: 默认过期时间（秒），如果 set 函数未指定 ex 或 px 时使用
            config_path: 配置文件路径，如果提供则从配置文件读取配置
            **kwargs: 其他连接参数
        """
        # 从配置文件加载配置（如果提供了 config_path 或存在默认配置文件）
        config = ConfigLoader.get_redis_config(config_path) if config_path else ConfigLoader.get_redis_config()
        
        # 如果参数未提供，则使用配置文件中的值
        host = host or config.get("host", "localhost")
        port = port or config.get("port", 6379)
        db = db if db is not None else config.get("db", 0)
        password = password if password is not None else config.get("password")
        decode_responses = decode_responses if decode_responses is not None else config.get("decode_responses", True)
        socket_timeout = socket_timeout or config.get("socket_timeout", 5)
        socket_connect_timeout = socket_connect_timeout or config.get("socket_connect_timeout", 5)
        max_connections = max_connections or config.get("max_connections", 50)
        default_ex = default_ex if default_ex is not None else config.get("default_ex")
        
        # 合并配置文件中的其他参数
        if config:
            kwargs.update({k: v for k, v in config.items() 
                          if k not in ["host", "port", "db", "password", "decode_responses",
                                       "socket_timeout", "socket_connect_timeout", "max_connections", "default_ex"]})
        
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.default_ex = default_ex
        
        # 创建连接池
        self.pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=decode_responses,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            max_connections=max_connections,
            **kwargs
        )
        
        # 创建 Redis 客户端
        self.client = redis.Redis(connection_pool=self.pool)
        
        # 测试连接
        try:
            self.client.ping()
            logger.info(f"成功连接到 Redis: {host}:{port}/{db}")
        except Exception as e:
            logger.error(f"连接 Redis 失败: {e}")
            raise
    
    # ==================== 字符串操作 ====================
    
    def set(
        self,
        key: str,
        value: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False
    ) -> bool:
        """
        设置键值对
        
        Args:
            key: 键名
            value: 值
            ex: 过期时间（秒），如果未指定且未指定 px，则使用 default_ex
            px: 过期时间（毫秒）
            nx: 仅当键不存在时设置
            xx: 仅当键存在时设置
        
        Returns:
            是否设置成功
        """
        # 如果未指定 ex 和 px，且设置了 default_ex，则使用 default_ex
        if ex is None and px is None and self.default_ex is not None:
            ex = self.default_ex
        
        return self.client.set(key, value, ex=ex, px=px, nx=nx, xx=xx)
    
    def setex(
        self,
        key: str,
        value: Any,
        time: int
    ) -> bool:
        """
        设置键值对并指定过期时间（秒）
        
        Args:
            key: 键名
            value: 值
            time: 过期时间（秒）
        
        Returns:
            是否设置成功
        """
        return self.client.setex(key, time, value)
    
    def setnx(
        self,
        key: str,
        value: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None
    ) -> bool:
        """
        仅当键不存在时设置键值对（SET if Not eXists）
        
        Args:
            key: 键名
            value: 值
            ex: 过期时间（秒），如果未指定且未指定 px，则使用 default_ex
            px: 过期时间（毫秒）
        
        Returns:
            是否设置成功（True: 键不存在且设置成功, False: 键已存在）
        """
        # 如果未指定 ex 和 px，且设置了 default_ex，则使用 default_ex
        if ex is None and px is None and self.default_ex is not None:
            ex = self.default_ex
        
        return self.client.set(key, value, ex=ex, px=px, nx=True)
    
    def get(self, key: str) -> Optional[Any]:
        """获取键值"""
        return self.client.get(key)
    
    def mset(self, mapping: Dict[str, Any]) -> bool:
        """批量设置键值对"""
        return self.client.mset(mapping)
    
    def mget(self, keys: List[str]) -> List[Optional[Any]]:
        """批量获取键值"""
        return self.client.mget(keys)
    
    def getset(self, key: str, value: Any) -> Optional[Any]:
        """设置新值并返回旧值"""
        return self.client.getset(key, value)
    
    def incr(self, key: str, amount: int = 1) -> int:
        """递增键值"""
        return self.client.incr(key, amount)
    
    def decr(self, key: str, amount: int = 1) -> int:
        """递减键值"""
        return self.client.decr(key, amount)
    
    def append(self, key: str, value: Any) -> int:
        """追加值到字符串末尾"""
        return self.client.append(key, value)
    
    def strlen(self, key: str) -> int:
        """获取字符串长度"""
        return self.client.strlen(key)
    
    # ==================== 列表操作 ====================
    
    def lpush(self, key: str, *values: Any) -> int:
        """从列表左侧推入元素"""
        return self.client.lpush(key, *values)
    
    def rpush(self, key: str, *values: Any) -> int:
        """从列表右侧推入元素"""
        return self.client.rpush(key, *values)
    
    def lpop(self, key: str) -> Optional[Any]:
        """从列表左侧弹出元素"""
        return self.client.lpop(key)
    
    def rpop(self, key: str) -> Optional[Any]:
        """从列表右侧弹出元素"""
        return self.client.rpop(key)
    
    def llen(self, key: str) -> int:
        """获取列表长度"""
        return self.client.llen(key)
    
    def lrange(self, key: str, start: int = 0, end: int = -1) -> List[Any]:
        """获取列表范围内的元素"""
        return self.client.lrange(key, start, end)
    
    def lindex(self, key: str, index: int) -> Optional[Any]:
        """获取列表中指定索引的元素"""
        return self.client.lindex(key, index)
    
    def lset(self, key: str, index: int, value: Any) -> bool:
        """设置列表中指定索引的元素"""
        return self.client.lset(key, index, value)
    
    def lrem(self, key: str, count: int, value: Any) -> int:
        """从列表中删除元素"""
        return self.client.lrem(key, count, value)
    
    # ==================== 哈希操作 ====================
    
    def hset(self, key: str, field: str, value: Any) -> int:
        """设置哈希字段值"""
        return self.client.hset(key, field, value)
    
    def hget(self, key: str, field: str) -> Optional[Any]:
        """获取哈希字段值"""
        return self.client.hget(key, field)
    
    def hmset(self, key: str, mapping: Dict[str, Any]) -> bool:
        """批量设置哈希字段"""
        return self.client.hmset(key, mapping)
    
    def hmget(self, key: str, fields: List[str]) -> List[Optional[Any]]:
        """批量获取哈希字段值"""
        return self.client.hmget(key, fields)
    
    def hgetall(self, key: str) -> Dict[str, Any]:
        """获取哈希所有字段和值"""
        return self.client.hgetall(key)
    
    def hdel(self, key: str, *fields: str) -> int:
        """删除哈希字段"""
        return self.client.hdel(key, *fields)
    
    def hexists(self, key: str, field: str) -> bool:
        """检查哈希字段是否存在"""
        return self.client.hexists(key, field)
    
    def hkeys(self, key: str) -> List[str]:
        """获取哈希所有字段名"""
        return self.client.hkeys(key)
    
    def hvals(self, key: str) -> List[Any]:
        """获取哈希所有值"""
        return self.client.hvals(key)
    
    def hlen(self, key: str) -> int:
        """获取哈希字段数量"""
        return self.client.hlen(key)
    
    def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        """递增哈希字段值"""
        return self.client.hincrby(key, field, amount)
    
    # ==================== 集合操作 ====================
    
    def sadd(self, key: str, *values: Any) -> int:
        """向集合添加元素"""
        return self.client.sadd(key, *values)
    
    def srem(self, key: str, *values: Any) -> int:
        """从集合删除元素"""
        return self.client.srem(key, *values)
    
    def smembers(self, key: str) -> Set[Any]:
        """获取集合所有成员"""
        return self.client.smembers(key)
    
    def sismember(self, key: str, value: Any) -> bool:
        """检查元素是否在集合中"""
        return self.client.sismember(key, value)
    
    def scard(self, key: str) -> int:
        """获取集合元素数量"""
        return self.client.scard(key)
    
    def spop(self, key: str, count: int = 1) -> Union[Any, List[Any]]:
        """随机弹出集合元素"""
        return self.client.spop(key, count)
    
    def srandmember(self, key: str, count: int = 1) -> Union[Any, List[Any]]:
        """随机获取集合元素（不删除）"""
        return self.client.srandmember(key, count)
    
    # ==================== 有序集合操作 ====================
    
    def zadd(self, key: str, mapping: Dict[Any, float]) -> int:
        """向有序集合添加元素"""
        return self.client.zadd(key, mapping)
    
    def zrem(self, key: str, *values: Any) -> int:
        """从有序集合删除元素"""
        return self.client.zrem(key, *values)
    
    def zscore(self, key: str, value: Any) -> Optional[float]:
        """获取有序集合元素的分数"""
        return self.client.zscore(key, value)
    
    def zrank(self, key: str, value: Any) -> Optional[int]:
        """获取有序集合元素的排名（从低到高）"""
        return self.client.zrank(key, value)
    
    def zrevrank(self, key: str, value: Any) -> Optional[int]:
        """获取有序集合元素的排名（从高到低）"""
        return self.client.zrevrank(key, value)
    
    def zrange(self, key: str, start: int = 0, end: int = -1, withscores: bool = False) -> List[Any]:
        """获取有序集合范围内的元素"""
        return self.client.zrange(key, start, end, withscores=withscores)
    
    def zrevrange(self, key: str, start: int = 0, end: int = -1, withscores: bool = False) -> List[Any]:
        """获取有序集合范围内的元素（倒序）"""
        return self.client.zrevrange(key, start, end, withscores=withscores)
    
    def zcard(self, key: str) -> int:
        """获取有序集合元素数量"""
        return self.client.zcard(key)
    
    def zcount(self, key: str, min_score: float, max_score: float) -> int:
        """统计有序集合中分数范围内的元素数量"""
        return self.client.zcount(key, min_score, max_score)
    
    def zincrby(self, key: str, amount: float, value: Any) -> float:
        """递增有序集合元素的分数"""
        return self.client.zincrby(key, amount, value)
    
    # ==================== 键操作 ====================
    
    def exists(self, *keys: str) -> int:
        """检查键是否存在"""
        return self.client.exists(*keys)
    
    def delete(self, *keys: str) -> int:
        """删除键"""
        return self.client.delete(*keys)
    
    def expire(self, key: str, time: int) -> bool:
        """设置键的过期时间（秒）"""
        return self.client.expire(key, time)
    
    def pexpire(self, key: str, time: int) -> bool:
        """设置键的过期时间（毫秒）"""
        return self.client.pexpire(key, time)
    
    def ttl(self, key: str) -> int:
        """获取键的剩余过期时间（秒）"""
        return self.client.ttl(key)
    
    def pttl(self, key: str) -> int:
        """获取键的剩余过期时间（毫秒）"""
        return self.client.pttl(key)
    
    def persist(self, key: str) -> bool:
        """移除键的过期时间"""
        return self.client.persist(key)
    
    def keys(self, pattern: str = "*") -> List[str]:
        """获取匹配模式的所有键"""
        return self.client.keys(pattern)
    
    def scan(self, cursor: int = 0, match: Optional[str] = None, count: Optional[int] = None) -> tuple:
        """扫描键（推荐用于生产环境）"""
        return self.client.scan(cursor, match=match, count=count)
    
    def type(self, key: str) -> str:
        """获取键的类型"""
        return self.client.type(key)
    
    def rename(self, src: str, dst: str) -> bool:
        """重命名键"""
        return self.client.rename(src, dst)
    
    # ==================== 分布式锁 ====================
    
    def acquire_lock(
        self,
        lock_key: str,
        timeout: int = 10,
        lock_id: Optional[str] = None,
        sleep: float = 0.1
    ) -> Optional[str]:
        """
        获取分布式锁
        
        Args:
            lock_key: 锁的键名
            timeout: 锁的超时时间（秒）
            lock_id: 锁的唯一标识（如果不提供会自动生成）
            sleep: 获取锁失败时的重试间隔（秒）
        
        Returns:
            锁的唯一标识，如果获取失败返回 None
        """
        if lock_id is None:
            lock_id = str(uuid.uuid4())
        
        end_time = time.time() + timeout
        
        while time.time() < end_time:
            # 尝试获取锁（SET key value NX EX timeout）
            if self.client.set(lock_key, lock_id, nx=True, ex=timeout):
                logger.info(f"成功获取锁: {lock_key}, lock_id: {lock_id}")
                return lock_id
            
            # 等待后重试
            time.sleep(sleep)
        
        logger.warning(f"获取锁超时: {lock_key}")
        return None
    
    def release_lock(self, lock_key: str, lock_id: str) -> bool:
        """
        释放分布式锁
        
        Args:
            lock_key: 锁的键名
            lock_id: 锁的唯一标识
        
        Returns:
            是否释放成功
        """
        # 使用 Lua 脚本确保原子性：只有锁的持有者才能释放锁
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        
        result = self.client.eval(lua_script, 1, lock_key, lock_id)
        success = bool(result)
        
        if success:
            logger.info(f"成功释放锁: {lock_key}, lock_id: {lock_id}")
        else:
            logger.warning(f"释放锁失败: {lock_key}, lock_id: {lock_id} (可能不是锁的持有者)")
        
        return success
    
    @contextmanager
    def lock(self, lock_key: str, timeout: int = 10, sleep: float = 0.1):
        """
        分布式锁上下文管理器
        
        Usage:
            with redis_client.lock("my_lock", timeout=10):
                # 执行需要加锁的代码
                pass
        
        Args:
            lock_key: 锁的键名
            timeout: 锁的超时时间（秒）
            sleep: 获取锁失败时的重试间隔（秒）
        
        Yields:
            锁的唯一标识
        """
        lock_id = self.acquire_lock(lock_key, timeout=timeout, sleep=sleep)
        if lock_id is None:
            raise Exception(f"无法获取锁: {lock_key}")
        
        try:
            yield lock_id
        finally:
            self.release_lock(lock_key, lock_id)
    
    def extend_lock(self, lock_key: str, lock_id: str, additional_time: int) -> bool:
        """
        延长锁的过期时间
        
        Args:
            lock_key: 锁的键名
            lock_id: 锁的唯一标识
            additional_time: 额外的时间（秒）
        
        Returns:
            是否延长成功
        """
        # 使用 Lua 脚本确保原子性
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("expire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        
        result = self.client.eval(lua_script, 1, lock_key, lock_id, additional_time)
        return bool(result)
    
    # ==================== 其他工具方法 ====================
    
    def ping(self) -> bool:
        """测试连接"""
        return self.client.ping()
    
    def flushdb(self) -> bool:
        """清空当前数据库"""
        return self.client.flushdb()
    
    def flushall(self) -> bool:
        """清空所有数据库"""
        return self.client.flushall()
    
    def info(self, section: Optional[str] = None) -> Dict[str, Any]:
        """获取 Redis 服务器信息"""
        return self.client.info(section)
    
    def dbsize(self) -> int:
        """获取当前数据库的键数量"""
        return self.client.dbsize()
    
    def close(self):
        """关闭连接池"""
        self.pool.disconnect()
        logger.info("Redis 连接池已关闭")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        # 不自动关闭连接池，因为可能还有其他地方在使用
        return False

