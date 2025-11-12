"""
MySQL 数据库连接类
封装基础的增删改查和分页查询接口
"""
import pymysql
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
import logging
from config_loader import ConfigLoader

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MySQLDB:
    """MySQL 数据库连接类"""
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        charset: Optional[str] = None,
        autocommit: Optional[bool] = None,
        max_connections: Optional[int] = None,
        config_path: Optional[str] = None,
        **kwargs
    ):
        """
        初始化 MySQL 数据库连接
        
        Args:
            host: 数据库主机地址
            port: 数据库端口
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名称
            charset: 字符集
            autocommit: 是否自动提交
            max_connections: 最大连接数（用于连接池）
            config_path: 配置文件路径，如果提供则从配置文件读取配置
            **kwargs: 其他连接参数
        """
        # 从配置文件加载配置（如果提供了 config_path 或存在默认配置文件）
        config = ConfigLoader.get_mysql_config(config_path) if config_path else ConfigLoader.get_mysql_config()
        
        # 如果参数未提供，则使用配置文件中的值
        host = host or config.get("host", "localhost")
        port = port or config.get("port", 3306)
        user = user or config.get("user", "root")
        password = password if password is not None else config.get("password", "")
        database = database or config.get("database", "")
        charset = charset or config.get("charset", "utf8mb4")
        autocommit = autocommit if autocommit is not None else config.get("autocommit", False)
        max_connections = max_connections or config.get("max_connections", 10)
        
        # 合并配置文件中的其他参数
        if config:
            kwargs.update({k: v for k, v in config.items() 
                          if k not in ["host", "port", "user", "password", "database", 
                                       "charset", "autocommit", "max_connections"]})
        
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        self.autocommit = autocommit
        self.max_connections = max_connections
        self.kwargs = kwargs
        
        # 连接池（简单实现）
        self._connection_pool = []
        self._current_connection = None
    
    def _get_connection(self) -> pymysql.Connection:
        """
        获取数据库连接（从连接池或创建新连接）
        
        Returns:
            pymysql.Connection: 数据库连接对象
        """
        if self._current_connection and self._current_connection.open:
            return self._current_connection
        
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
            self._current_connection = connection
            logger.info(f"成功连接到数据库: {self.host}:{self.port}/{self.database}")
            return connection
        except Exception as e:
            logger.error(f"连接数据库失败: {e}")
            raise
    
    def _close_connection(self):
        """关闭当前连接"""
        if self._current_connection and self._current_connection.open:
            self._current_connection.close()
            self._current_connection = None
            logger.info("数据库连接已关闭")
    
    @contextmanager
    def get_cursor(self):
        """
        获取游标的上下文管理器
        
        Usage:
            with db.get_cursor() as cursor:
                cursor.execute("SELECT * FROM table")
                result = cursor.fetchall()
        """
        connection = self._get_connection()
        cursor = connection.cursor()
        try:
            yield cursor
            if not self.autocommit:
                connection.commit()
        except Exception as e:
            if not self.autocommit:
                connection.rollback()
            logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            cursor.close()
    
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
            params: SQL 参数（元组或字典）
            fetch: 是否返回查询结果
        
        Returns:
            如果 fetch=True，返回查询结果列表；否则返回 None
        """
        with self.get_cursor() as cursor:
            cursor.execute(sql, params)
            if fetch:
                return cursor.fetchall()
            return None
    
    def executemany(
        self,
        sql: str,
        params_list: List[Tuple]
    ) -> int:
        """
        批量执行 SQL 语句
        
        Args:
            sql: SQL 语句
            params_list: 参数列表
        
        Returns:
            影响的行数
        """
        with self.get_cursor() as cursor:
            affected_rows = cursor.executemany(sql, params_list)
            return affected_rows
    
    # ==================== 基础 CRUD 操作 ====================
    
    def insert(
        self,
        table: str,
        data: Dict[str, Any],
        ignore: bool = False
    ) -> int:
        """
        插入单条记录
        
        Args:
            table: 表名
            data: 要插入的数据（字典）
            ignore: 是否使用 INSERT IGNORE
        
        Returns:
            插入的记录 ID
        """
        if not data:
            raise ValueError("插入数据不能为空")
        
        keys = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        insert_type = "INSERT IGNORE" if ignore else "INSERT"
        
        sql = f"{insert_type} INTO {table} ({keys}) VALUES ({placeholders})"
        
        with self.get_cursor() as cursor:
            cursor.execute(sql, tuple(data.values()))
            return cursor.lastrowid
    
    def insert_many(
        self,
        table: str,
        data_list: List[Dict[str, Any]],
        ignore: bool = False
    ) -> int:
        """
        批量插入记录
        
        Args:
            table: 表名
            data_list: 要插入的数据列表
            ignore: 是否使用 INSERT IGNORE
        
        Returns:
            影响的行数
        """
        if not data_list:
            raise ValueError("插入数据列表不能为空")
        
        # 获取所有数据的键（假设所有字典的键相同）
        keys = ", ".join(data_list[0].keys())
        placeholders = ", ".join(["%s"] * len(data_list[0]))
        insert_type = "INSERT IGNORE" if ignore else "INSERT"
        
        sql = f"{insert_type} INTO {table} ({keys}) VALUES ({placeholders})"
        
        # 准备参数列表
        params_list = [tuple(item.values()) for item in data_list]
        
        return self.executemany(sql, params_list)
    
    def update(
        self,
        table: str,
        data: Dict[str, Any],
        where: str,
        where_params: Optional[Tuple] = None
    ) -> int:
        """
        更新记录
        
        Args:
            table: 表名
            data: 要更新的数据（字典）
            where: WHERE 条件（SQL 字符串）
            where_params: WHERE 条件的参数
        
        Returns:
            影响的行数
        """
        if not data:
            raise ValueError("更新数据不能为空")
        
        set_clause = ", ".join([f"{key} = %s" for key in data.keys()])
        sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
        
        # 合并参数
        params = tuple(data.values())
        if where_params:
            params = params + where_params
        
        with self.get_cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.rowcount
    
    def delete(
        self,
        table: str,
        where: str,
        where_params: Optional[Tuple] = None
    ) -> int:
        """
        删除记录
        
        Args:
            table: 表名
            where: WHERE 条件（SQL 字符串）
            where_params: WHERE 条件的参数
        
        Returns:
            影响的行数
        """
        sql = f"DELETE FROM {table} WHERE {where}"
        
        with self.get_cursor() as cursor:
            cursor.execute(sql, where_params)
            return cursor.rowcount
    
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
        """
        查询记录
        
        Args:
            table: 表名
            fields: 要查询的字段（默认 "*"）
            where: WHERE 条件（SQL 字符串）
            where_params: WHERE 条件的参数
            order_by: ORDER BY 子句
            limit: 限制返回的记录数
            offset: 偏移量
        
        Returns:
            查询结果列表
        """
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
        """
        查询单条记录
        
        Args:
            table: 表名
            fields: 要查询的字段（默认 "*"）
            where: WHERE 条件（SQL 字符串）
            where_params: WHERE 条件的参数
            order_by: ORDER BY 子句
        
        Returns:
            查询结果（字典）或 None
        """
        results = self.select(table, fields, where, where_params, order_by, limit=1)
        return results[0] if results else None
    
    def count(
        self,
        table: str,
        where: Optional[str] = None,
        where_params: Optional[Tuple] = None
    ) -> int:
        """
        统计记录数
        
        Args:
            table: 表名
            where: WHERE 条件（SQL 字符串）
            where_params: WHERE 条件的参数
        
        Returns:
            记录数
        """
        sql = f"SELECT COUNT(*) as count FROM {table}"
        
        params = None
        if where:
            sql += f" WHERE {where}"
            params = where_params
        
        result = self.execute(sql, params, fetch=True)
        return result[0]['count'] if result else 0
    
    # ==================== 分页查询 ====================
    
    def select_page(
        self,
        table: str,
        page: int = 1,
        page_size: int = 10,
        fields: str = "*",
        where: Optional[str] = None,
        where_params: Optional[Tuple] = None,
        order_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        分页查询
        
        Args:
            table: 表名
            page: 页码（从 1 开始）
            page_size: 每页记录数
            fields: 要查询的字段（默认 "*"）
            where: WHERE 条件（SQL 字符串）
            where_params: WHERE 条件的参数
            order_by: ORDER BY 子句
        
        Returns:
            包含以下字段的字典:
            - data: 查询结果列表
            - total: 总记录数
            - page: 当前页码
            - page_size: 每页记录数
            - total_pages: 总页数
        """
        if page < 1:
            page = 1
        if page_size < 1:
            page_size = 10
        
        # 计算偏移量
        offset = (page - 1) * page_size
        
        # 查询数据
        data = self.select(
            table=table,
            fields=fields,
            where=where,
            where_params=where_params,
            order_by=order_by,
            limit=page_size,
            offset=offset
        )
        
        # 查询总数
        total = self.count(table, where, where_params)
        
        # 计算总页数
        total_pages = (total + page_size - 1) // page_size if total > 0 else 0
        
        return {
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages
        }
    
    # ==================== 事务管理 ====================
    
    @contextmanager
    def transaction(self):
        """
        事务上下文管理器
        
        Usage:
            with db.transaction():
                db.insert("table", {...})
                db.update("table", {...}, "id = %s", (1,))
        """
        connection = self._get_connection()
        original_autocommit = connection.autocommit_mode
        connection.autocommit(False)
        try:
            yield
            connection.commit()
            logger.info("事务提交成功")
        except Exception as e:
            connection.rollback()
            logger.error(f"事务回滚: {e}")
            raise
        finally:
            connection.autocommit(original_autocommit)
    
    def begin_transaction(self):
        """开始事务"""
        connection = self._get_connection()
        connection.autocommit(False)
        logger.info("开始事务")
    
    def commit(self):
        """提交事务"""
        if self._current_connection:
            self._current_connection.commit()
            logger.info("事务提交成功")
    
    def rollback(self):
        """回滚事务"""
        if self._current_connection:
            self._current_connection.rollback()
            logger.info("事务回滚")
    
    # ==================== 工具方法 ====================
    
    def close(self):
        """关闭数据库连接"""
        self._close_connection()
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()
        return False

