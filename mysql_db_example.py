"""
MySQL 数据库连接类使用示例
"""
from mysql_db import MySQLDB

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "your_password",
    "database": "test_db",
    "charset": "utf8mb4"
}


def example_basic_usage():
    """基础使用示例"""
    print("=" * 60)
    print("基础使用示例")
    print("=" * 60)
    
    # 创建数据库连接
    db = MySQLDB(**DB_CONFIG)
    
    try:
        # 1. 插入单条记录
        print("\n1. 插入单条记录:")
        user_id = db.insert("users", {
            "name": "张三",
            "email": "zhangsan@example.com",
            "age": 25
        })
        print(f"   插入成功，ID: {user_id}")
        
        # 2. 批量插入
        print("\n2. 批量插入记录:")
        users_data = [
            {"name": "李四", "email": "lisi@example.com", "age": 28},
            {"name": "王五", "email": "wangwu@example.com", "age": 30},
        ]
        affected = db.insert_many("users", users_data)
        print(f"   批量插入成功，影响行数: {affected}")
        
        # 3. 查询记录
        print("\n3. 查询记录:")
        users = db.select("users", where="age > %s", where_params=(25,))
        print(f"   查询到 {len(users)} 条记录")
        for user in users:
            print(f"   - {user}")
        
        # 4. 查询单条记录
        print("\n4. 查询单条记录:")
        user = db.select_one("users", where="id = %s", where_params=(user_id,))
        if user:
            print(f"   用户信息: {user}")
        
        # 5. 更新记录
        print("\n5. 更新记录:")
        affected = db.update(
            "users",
            {"age": 26},
            "id = %s",
            (user_id,)
        )
        print(f"   更新成功，影响行数: {affected}")
        
        # 6. 统计记录数
        print("\n6. 统计记录数:")
        count = db.count("users")
        print(f"   总记录数: {count}")
        
        # 7. 删除记录
        print("\n7. 删除记录:")
        affected = db.delete("users", "id = %s", (user_id,))
        print(f"   删除成功，影响行数: {affected}")
        
    finally:
        db.close()


def example_page_query():
    """分页查询示例"""
    print("\n" + "=" * 60)
    print("分页查询示例")
    print("=" * 60)
    
    db = MySQLDB(**DB_CONFIG)
    
    try:
        # 分页查询
        result = db.select_page(
            table="users",
            page=1,
            page_size=10,
            where="age > %s",
            where_params=(20,),
            order_by="id DESC"
        )
        
        print(f"\n分页结果:")
        print(f"  - 当前页: {result['page']}")
        print(f"  - 每页记录数: {result['page_size']}")
        print(f"  - 总记录数: {result['total']}")
        print(f"  - 总页数: {result['total_pages']}")
        print(f"  - 数据: {len(result['data'])} 条")
        
        for item in result['data']:
            print(f"    {item}")
            
    finally:
        db.close()


def example_transaction():
    """事务示例"""
    print("\n" + "=" * 60)
    print("事务示例")
    print("=" * 60)
    
    db = MySQLDB(**DB_CONFIG)
    
    try:
        # 使用事务上下文管理器
        with db.transaction():
            # 插入用户
            user_id = db.insert("users", {
                "name": "事务测试",
                "email": "transaction@example.com",
                "age": 25
            })
            print(f"   插入用户 ID: {user_id}")
            
            # 更新用户
            db.update("users", {"age": 26}, "id = %s", (user_id,))
            print(f"   更新用户 ID: {user_id}")
            
            # 如果这里出现异常，所有操作都会回滚
            # raise Exception("测试回滚")
        
        print("   事务提交成功")
        
    except Exception as e:
        print(f"   事务失败: {e}")
    finally:
        db.close()


def example_custom_sql():
    """自定义 SQL 示例"""
    print("\n" + "=" * 60)
    print("自定义 SQL 示例")
    print("=" * 60)
    
    db = MySQLDB(**DB_CONFIG)
    
    try:
        # 执行自定义 SQL
        sql = """
            SELECT u.*, COUNT(o.id) as order_count 
            FROM users u 
            LEFT JOIN orders o ON u.id = o.user_id 
            GROUP BY u.id 
            HAVING order_count > 0
        """
        results = db.execute(sql, fetch=True)
        print(f"   查询结果: {len(results)} 条")
        for result in results:
            print(f"   {result}")
            
    finally:
        db.close()


def example_context_manager():
    """上下文管理器示例"""
    print("\n" + "=" * 60)
    print("上下文管理器示例")
    print("=" * 60)
    
    # 使用 with 语句自动管理连接
    with MySQLDB(**DB_CONFIG) as db:
        users = db.select("users", limit=5)
        print(f"   查询到 {len(users)} 条记录")
        # 退出 with 语句时自动关闭连接


if __name__ == "__main__":
    # 注意：运行前请确保数据库配置正确，并创建相应的表
    
    # 创建测试表的 SQL（示例）
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        age INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    
    print("MySQL 数据库连接类使用示例")
    print("=" * 60)
    print("\n注意：运行前请确保：")
    print("1. 数据库配置正确（修改 DB_CONFIG）")
    print("2. 已创建相应的表")
    print("3. 已安装 pymysql: pip install pymysql")
    print("\n" + "=" * 60)
    
    # 取消注释以下行来运行示例
    # example_basic_usage()
    # example_page_query()
    # example_transaction()
    # example_custom_sql()
    # example_context_manager()

