"""
配置文件使用示例
展示如何使用配置文件来初始化 MySQL 和 Redis 连接
"""
from mysql_db import MySQLDB
from redis_client import RedisClient


def example_mysql_from_config():
    """从配置文件加载 MySQL 配置示例"""
    print("=" * 60)
    print("从配置文件加载 MySQL 配置示例")
    print("=" * 60)
    
    # 方式1：自动从默认配置文件（config.json）加载
    print("\n方式1：自动从默认配置文件加载")
    db = MySQLDB()  # 自动从 config.json 加载配置
    print(f"   连接信息: {db.host}:{db.port}/{db.database}")
    
    # 方式2：指定配置文件路径
    print("\n方式2：指定配置文件路径")
    db2 = MySQLDB(config_path="config.json")
    print(f"   连接信息: {db2.host}:{db2.port}/{db2.database}")
    
    # 方式3：配置文件 + 覆盖部分参数
    print("\n方式3：配置文件 + 覆盖部分参数")
    db3 = MySQLDB(config_path="config.json", database="other_db")
    print(f"   连接信息: {db3.host}:{db3.port}/{db3.database}")
    
    # 方式4：不使用配置文件，直接传入参数
    print("\n方式4：不使用配置文件，直接传入参数")
    db4 = MySQLDB(
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database="test_db"
    )
    print(f"   连接信息: {db4.host}:{db4.port}/{db4.database}")


def example_redis_from_config():
    """从配置文件加载 Redis 配置示例"""
    print("\n" + "=" * 60)
    print("从配置文件加载 Redis 配置示例")
    print("=" * 60)
    
    # 方式1：自动从默认配置文件（config.json）加载
    print("\n方式1：自动从默认配置文件加载")
    redis_client = RedisClient()  # 自动从 config.json 加载配置
    print(f"   连接信息: {redis_client.host}:{redis_client.port}/{redis_client.db}")
    
    # 方式2：指定配置文件路径
    print("\n方式2：指定配置文件路径")
    redis_client2 = RedisClient(config_path="config.json")
    print(f"   连接信息: {redis_client2.host}:{redis_client2.port}/{redis_client2.db}")
    
    # 方式3：配置文件 + 覆盖部分参数
    print("\n方式3：配置文件 + 覆盖部分参数")
    redis_client3 = RedisClient(config_path="config.json", db=1, default_ex=300)
    print(f"   连接信息: {redis_client3.host}:{redis_client3.port}/{redis_client3.db}")
    print(f"   默认过期时间: {redis_client3.default_ex} 秒")
    
    # 方式4：不使用配置文件，直接传入参数
    print("\n方式4：不使用配置文件，直接传入参数")
    redis_client4 = RedisClient(
        host="localhost",
        port=6379,
        db=0,
        password=None,
        default_ex=120
    )
    print(f"   连接信息: {redis_client4.host}:{redis_client4.port}/{redis_client4.db}")
    print(f"   默认过期时间: {redis_client4.default_ex} 秒")


def example_config_file_structure():
    """配置文件结构说明"""
    print("\n" + "=" * 60)
    print("配置文件结构说明")
    print("=" * 60)
    
    print("""
配置文件支持 JSON 和 YAML 格式，默认查找顺序：
1. config.json
2. config.yaml
3. config.yml

配置文件结构示例（config.json）：

{
  "mysql": {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "your_password",
    "database": "test_db",
    "charset": "utf8mb4",
    "autocommit": false,
    "max_connections": 10
  },
  "redis": {
    "host": "localhost",
    "port": 6379,
    "db": 0,
    "password": null,
    "decode_responses": true,
    "socket_timeout": 5,
    "socket_connect_timeout": 5,
    "max_connections": 50,
    "default_ex": 120
  }
}

使用方式：
1. 自动加载：MySQLDB() 或 RedisClient() 会自动查找配置文件
2. 指定路径：MySQLDB(config_path="config.json")
3. 覆盖参数：MySQLDB(config_path="config.json", database="other_db")
4. 不使用配置：直接传入所有参数，不使用配置文件
    """)


if __name__ == "__main__":
    print("配置文件使用示例")
    print("=" * 60)
    print("\n注意：运行前请确保：")
    print("1. 已创建 config.json 配置文件")
    print("2. 配置文件中的数据库连接信息正确")
    print("\n" + "=" * 60)
    
    # 显示配置文件结构说明
    example_config_file_structure()
    
    # 取消注释以下行来运行示例
    # example_mysql_from_config()
    # example_redis_from_config()

