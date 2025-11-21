"""
MySQL 连接池实现
使用队列管理固定数量的连接，多个线程共享
"""
import pymysql
import threading
import queue
import time
import logging
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
from config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class MySQLConnectionPool:
    """MySQL 连接池（多线程安全）"""
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        charset: Optional[str] = None,
        autocommit: Optional[bool] = None,
        pool_size: int = 10,  # 连接池大小
        max_overflow: int = 5,  # 最大溢出连接数
        timeout: int = 30,  # 获取连接超时时间
        **kwargs
    ):
        """
        初始化连接池
        
        Args:
            pool_size: 连接池大小（核心连接数）
            max_overflow: 最大溢出连接数
            timeout: 获取连接超时时间（秒）
        """
        # 从配置文件加载配置
        config = ConfigLoader.get_mysql_config()
        
        self.host = host or config.get("host", "localhost")
        self.port = port or config.get("port", 3306)
        self.user = user or config.get("user", "root")
        self.password = password if password is not None else config.get("password", "")
        self.database = database or config.get("database", "")
        self.charset = charset or config.get("charset", "utf8mb4")
        self.autocommit = autocommit if autocommit is not None else config.get("autocommit", False)
        self.kwargs = kwargs
        
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.timeout = timeout
        
        # 连接池（使用 Queue 实现）
        self._pool = queue.Queue(maxsize=pool_size + max_overflow)
        self._created_connections = 0  # 已创建的连接数
        self._lock = threading.Lock()  # 用于保护 _created_connections
        
        # 初始化核心连接
        for _ in range(pool_size):
            conn = self._create_connection()
            if conn:
                self._pool.put(conn)
        
        logger.info(f"连接池初始化完成: pool_size={pool_size}, max_overflow={max_overflow}")
    
    def _create_connection(self) -> Optional[pymysql.Connection]:
        """创建新的数据库连接"""
        try:
            connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset=self.charset,
                autocommit=self.autocommit,
                cursorclass=pymysql.cursors.DictCursor,
                **self.kwargs
            )
            with self._lock:
                self._created_connections += 1
                logger.debug(f"创建新连接: 总连接数={self._created_connections}")
            return connection
        except Exception as e:
            logger.error(f"创建数据库连接失败: {e}")
            return None
    
    def get_connection(self) -> pymysql.Connection:
        """
        从连接池获取连接
        
        Returns:
            数据库连接对象
        
        Raises:
            Exception: 如果无法获取连接
        """
        try:
            # 尝试从池中获取连接（非阻塞）
            conn = self._pool.get(block=False)
            
            # 检查连接是否有效
            try:
                conn.ping(reconnect=True)
                return conn
            except Exception:
                # 连接已失效，创建新连接
                conn = self._create_connection()
                if conn:
                    return conn
                raise Exception("无法创建新连接")
        
        except queue.Empty:
            # 池中没有可用连接
            with self._lock:
                if self._created_connections < self.pool_size + self.max_overflow:
                    # 可以创建溢出连接
                    conn = self._create_connection()
                    if conn:
                        return conn
            
            # 无法创建更多连接，等待
            try:
                conn = self._pool.get(block=True, timeout=self.timeout)
                # 检查连接是否有效
                try:
                    conn.ping(reconnect=True)
                    return conn
                except Exception:
                    # 连接已失效，尝试创建新连接
                    conn = self._create_connection()
                    if conn:
                        return conn
                    raise Exception("无法创建新连接")
            except queue.Empty:
                raise Exception(f"获取数据库连接超时（{self.timeout}秒）")
    
    def return_connection(self, conn: pymysql.Connection):
        """
        归还连接到连接池
        
        Args:
            conn: 数据库连接对象
        """
        if conn:
            try:
                # 检查连接是否有效
                if conn.open:
                    self._pool.put(conn, block=False)
                else:
                    # 连接已关闭，减少计数
                    with self._lock:
                        self._created_connections -= 1
            except queue.Full:
                # 池已满（溢出连接），关闭连接
                try:
                    conn.close()
                except Exception:
                    pass
                with self._lock:
                    self._created_connections -= 1
                    logger.debug(f"关闭溢出连接: 总连接数={self._created_connections}")
    
    @contextmanager
    def get_cursor(self, connection=None):
        """
        获取游标的上下文管理器
        
        Args:
            connection: 可选的连接对象，如果提供则使用该连接，否则从池中获取
        
        Usage:
            with pool.get_cursor() as cursor:
                cursor.execute("SELECT * FROM table")
                result = cursor.fetchall()
        """
        # 判断是否使用外部提供的连接
        external_conn = connection is not None
        
        if external_conn:
            # 使用外部连接（通常是事务中）
            # 如果是 ConnectionWrapper，获取实际连接
            if hasattr(connection, 'conn'):
                conn = connection.conn
            else:
                conn = connection
        else:
            # 从池中获取连接
            conn = self.get_connection()
        
        cursor = conn.cursor()
        try:
            yield cursor
            # 只在非外部连接且非自动提交时才提交
            if not external_conn and not self.autocommit:
                conn.commit()
        except Exception as e:
            # 只在非外部连接且非自动提交时才回滚
            if not external_conn and not self.autocommit:
                conn.rollback()
            logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            cursor.close()
            # 只归还从池中获取的连接
            if not external_conn:
                self.return_connection(conn)
    
    def execute(
        self,
        sql: str,
        params: Optional[Tuple] = None,
        fetch: bool = False
    ) -> Optional[List[Dict[str, Any]]]:
        """
        执行 SQL 语句
        
        Args:
            sql: SQL 语句
            params: SQL 参数
            fetch: 是否返回查询结果
        
        Returns:
            如果 fetch=True，返回查询结果列表；否则返回 None
        """
        with self.get_cursor() as cursor:
            cursor.execute(sql, params)
            if fetch:
                return cursor.fetchall()
            return None
    
    def close_all(self):
        """关闭所有连接"""
        closed_count = 0
        while not self._pool.empty():
            try:
                conn = self._pool.get(block=False)
                try:
                    conn.close()
                    closed_count += 1
                except Exception:
                    pass
            except queue.Empty:
                break
        
        logger.info(f"连接池已关闭: 关闭了 {closed_count} 个连接")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取连接池统计信息"""
        with self._lock:
            return {
                "created_connections": self._created_connections,
                "available_connections": self._pool.qsize(),
                "in_use_connections": self._created_connections - self._pool.qsize(),
                "pool_size": self.pool_size,
                "max_overflow": self.max_overflow
            }
    
    # ==================== 向后兼容 MySQLDB 的接口 ====================
    
    def select(
        self,
        table: str,
        fields: str = "*",
        where: Optional[str] = None,
        where_params: Optional[Tuple] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """查询记录（兼容 MySQLDB 接口）"""
        sql = f"SELECT {fields} FROM {table}"
        
        params = None
        if where:
            sql += f" WHERE {where}"
            params = where_params
        
        if order_by:
            sql += f" ORDER BY {order_by}"
        
        if limit is not None:
            sql += f" LIMIT {limit}"
            if offset is not None:
                sql += f" OFFSET {offset}"
        
        return self.execute(sql, params, fetch=True) or []
    
    def select_one(
        self,
        table: str,
        fields: str = "*",
        where: Optional[str] = None,
        where_params: Optional[Tuple] = None,
        order_by: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """查询单条记录（兼容 MySQLDB 接口）"""
        results = self.select(table, fields, where, where_params, order_by, limit=1)
        return results[0] if results else None
    
    def update(
        self,
        table: str,
        data: Dict[str, Any],
        where: str,
        where_params: Optional[Tuple] = None
    ) -> int:
        """更新记录（兼容 MySQLDB 接口）"""
        if not data:
            raise ValueError("更新数据不能为空")
        
        set_clause = ", ".join([f"{key} = %s" for key in data.keys()])
        sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
        
        params = tuple(data.values())
        if where_params:
            params = params + where_params
        
        with self.get_cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.rowcount
    
    def _close_connection(self):
        """
        关闭连接（兼容 MySQLDB 接口）
        连接池模式下不需要手动关闭，什么都不做
        """
        pass
    
    def _get_connection(self):
        """
        获取连接（兼容 MySQLDB 接口）
        注意：连接池模式下不建议直接使用此方法
        返回一个包装对象，commit() 时实际上是归还连接
        """
        class ConnectionWrapper:
            """连接包装类，用于兼容性"""
            def __init__(self, pool, conn):
                self.pool = pool
                self.conn = conn
                self.autocommit_mode = pool.autocommit
            
            def commit(self):
                """提交并归还连接"""
                if self.conn:
                    try:
                        if not self.pool.autocommit:
                            self.conn.commit()
                    except Exception as e:
                        logger.error(f"提交事务失败: {e}")
                    finally:
                        self.pool.return_connection(self.conn)
                        self.conn = None
            
            def rollback(self):
                """回滚并归还连接"""
                if self.conn:
                    try:
                        if not self.pool.autocommit:
                            self.conn.rollback()
                    except Exception as e:
                        logger.error(f"回滚事务失败: {e}")
                    finally:
                        self.pool.return_connection(self.conn)
                        self.conn = None
            
            def close(self):
                """关闭（实际是归还连接）"""
                if self.conn:
                    self.pool.return_connection(self.conn)
                    self.conn = None
            
            def autocommit(self, mode):
                """设置自动提交模式"""
                if self.conn:
                    self.conn.autocommit(mode)
                    self.autocommit_mode = mode
            
            def get_autocommit(self):
                """获取自动提交模式"""
                return self.autocommit_mode
            
            @property
            def open(self):
                """检查连接是否打开"""
                return self.conn and self.conn.open if self.conn else False
            
            def ping(self, reconnect=True):
                """测试连接"""
                if self.conn:
                    return self.conn.ping(reconnect=reconnect)
            
            def __getattr__(self, name):
                """代理其他属性到实际连接"""
                if self.conn:
                    return getattr(self.conn, name)
                raise AttributeError(f"Connection is None")
        
        conn = self.get_connection()
        return ConnectionWrapper(self, conn)
    
    def commit(self):
        """提交事务（兼容接口，连接池模式下每次操作自动提交）"""
        pass
    
    def close(self):
        """关闭连接池（兼容接口）"""
        self.close_all()
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close_all()
        return False

