"""
Redis 客户端使用示例
"""
import time
import threading
from redis_client import RedisClient

# Redis 配置
REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0,
    "password": None,
    "decode_responses": True
}


def example_string_operations():
    """字符串操作示例"""
    print("=" * 60)
    print("字符串操作示例")
    print("=" * 60)
    
    redis_client = RedisClient(**REDIS_CONFIG)
    
    try:
        # 设置键值对
        print("\n1. 设置键值对:")
        redis_client.set("name", "张三")
        redis_client.set("age", 25, ex=60)  # 设置过期时间 60 秒
        print("   设置成功")
        
        # 获取值
        print("\n2. 获取值:")
        name = redis_client.get("name")
        age = redis_client.get("age")
        print(f"   name: {name}, age: {age}")
        
        # 使用 setex 设置键值对并指定过期时间
        print("\n3. 使用 setex 设置键值对并指定过期时间:")
        redis_client.setex("temp_key", 30, "临时值")  # 30 秒后过期
        ttl = redis_client.ttl("temp_key")
        print(f"   设置成功，剩余过期时间: {ttl} 秒")
        
        # 使用 setnx 仅当键不存在时设置
        print("\n4. 使用 setnx 仅当键不存在时设置:")
        result1 = redis_client.setnx("new_key", "新值", ex=60)
        result2 = redis_client.setnx("new_key", "重复设置")  # 键已存在，不会设置
        print(f"   第一次设置: {'成功' if result1 else '失败'}")
        print(f"   第二次设置: {'成功' if result2 else '失败（键已存在）'}")
        value = redis_client.get("new_key")
        print(f"   键的值: {value}")
        
        # 使用默认过期时间
        print("\n5. 使用默认过期时间:")
        redis_client_with_default = RedisClient(default_ex=120, **REDIS_CONFIG)
        redis_client_with_default.set("auto_expire_key", "自动过期值")
        ttl = redis_client_with_default.ttl("auto_expire_key")
        print(f"   设置成功，剩余过期时间: {ttl} 秒（使用默认过期时间）")
        redis_client_with_default.close()
        
        # 批量操作
        print("\n6. 批量操作:")
        redis_client.mset({"key1": "value1", "key2": "value2", "key3": "value3"})
        values = redis_client.mget(["key1", "key2", "key3"])
        print(f"   批量获取: {values}")
        
        # 递增/递减
        print("\n7. 递增/递减:")
        redis_client.set("counter", 0)
        redis_client.incr("counter", 5)
        redis_client.decr("counter", 2)
        counter = redis_client.get("counter")
        print(f"   counter: {counter}")
        
    finally:
        redis_client.close()


def example_list_operations():
    """列表操作示例"""
    print("\n" + "=" * 60)
    print("列表操作示例")
    print("=" * 60)
    
    redis_client = RedisClient(**REDIS_CONFIG)
    
    try:
        # 从左侧推入
        print("\n1. 从左侧推入元素:")
        redis_client.lpush("mylist", "a", "b", "c")
        print("   推入成功")
        
        # 从右侧推入
        print("\n2. 从右侧推入元素:")
        redis_client.rpush("mylist", "d", "e", "f")
        print("   推入成功")
        
        # 获取列表
        print("\n3. 获取列表:")
        mylist = redis_client.lrange("mylist")
        print(f"   列表内容: {mylist}")
        
        # 弹出元素
        print("\n4. 弹出元素:")
        left = redis_client.lpop("mylist")
        right = redis_client.rpop("mylist")
        print(f"   左侧弹出: {left}, 右侧弹出: {right}")
        
        # 获取长度
        print("\n5. 获取列表长度:")
        length = redis_client.llen("mylist")
        print(f"   列表长度: {length}")
        
    finally:
        redis_client.close()


def example_hash_operations():
    """哈希操作示例"""
    print("\n" + "=" * 60)
    print("哈希操作示例")
    print("=" * 60)
    
    redis_client = RedisClient(**REDIS_CONFIG)
    
    try:
        # 设置哈希字段
        print("\n1. 设置哈希字段:")
        redis_client.hset("user:1", "name", "张三")
        redis_client.hset("user:1", "age", 25)
        redis_client.hset("user:1", "email", "zhangsan@example.com")
        print("   设置成功")
        
        # 批量设置
        print("\n2. 批量设置哈希字段:")
        redis_client.hmset("user:2", {
            "name": "李四",
            "age": 28,
            "email": "lisi@example.com"
        })
        print("   批量设置成功")
        
        # 获取哈希字段
        print("\n3. 获取哈希字段:")
        name = redis_client.hget("user:1", "name")
        print(f"   user:1 name: {name}")
        
        # 获取所有字段和值
        print("\n4. 获取所有字段和值:")
        user1 = redis_client.hgetall("user:1")
        print(f"   user:1: {user1}")
        
        # 检查字段是否存在
        print("\n5. 检查字段是否存在:")
        exists = redis_client.hexists("user:1", "name")
        print(f"   name 字段存在: {exists}")
        
        # 递增字段值
        print("\n6. 递增字段值:")
        redis_client.hincrby("user:1", "age", 1)
        age = redis_client.hget("user:1", "age")
        print(f"   年龄递增后: {age}")
        
    finally:
        redis_client.close()


def example_set_operations():
    """集合操作示例"""
    print("\n" + "=" * 60)
    print("集合操作示例")
    print("=" * 60)
    
    redis_client = RedisClient(**REDIS_CONFIG)
    
    try:
        # 添加元素
        print("\n1. 添加元素:")
        redis_client.sadd("myset", "a", "b", "c", "d")
        print("   添加成功")
        
        # 获取所有成员
        print("\n2. 获取所有成员:")
        members = redis_client.smembers("myset")
        print(f"   集合成员: {members}")
        
        # 检查元素是否存在
        print("\n3. 检查元素是否存在:")
        exists = redis_client.sismember("myset", "a")
        print(f"   'a' 是否存在: {exists}")
        
        # 获取集合大小
        print("\n4. 获取集合大小:")
        size = redis_client.scard("myset")
        print(f"   集合大小: {size}")
        
        # 随机弹出元素
        print("\n5. 随机弹出元素:")
        popped = redis_client.spop("myset")
        print(f"   弹出元素: {popped}")
        
    finally:
        redis_client.close()


def example_sorted_set_operations():
    """有序集合操作示例"""
    print("\n" + "=" * 60)
    print("有序集合操作示例")
    print("=" * 60)
    
    redis_client = RedisClient(**REDIS_CONFIG)
    
    try:
        # 添加元素（带分数）
        print("\n1. 添加元素（带分数）:")
        redis_client.zadd("leaderboard", {
            "player1": 100,
            "player2": 200,
            "player3": 150,
            "player4": 300
        })
        print("   添加成功")
        
        # 获取排名（从高到低）
        print("\n2. 获取排名（从高到低）:")
        top_players = redis_client.zrevrange("leaderboard", 0, 2, withscores=True)
        print(f"   前3名: {top_players}")
        
        # 获取分数
        print("\n3. 获取分数:")
        score = redis_client.zscore("leaderboard", "player1")
        print(f"   player1 分数: {score}")
        
        # 获取排名
        print("\n4. 获取排名:")
        rank = redis_client.zrank("leaderboard", "player1")
        rev_rank = redis_client.zrevrank("leaderboard", "player1")
        print(f"   player1 排名（从低到高）: {rank}, （从高到低）: {rev_rank}")
        
        # 递增分数
        print("\n5. 递增分数:")
        new_score = redis_client.zincrby("leaderboard", 50, "player1")
        print(f"   player1 新分数: {new_score}")
        
    finally:
        redis_client.close()


def example_lock_basic():
    """分布式锁基础示例"""
    print("\n" + "=" * 60)
    print("分布式锁基础示例")
    print("=" * 60)
    
    redis_client = RedisClient(**REDIS_CONFIG)
    
    try:
        lock_key = "my_lock"
        
        # 获取锁
        print("\n1. 获取锁:")
        lock_id = redis_client.acquire_lock(lock_key, timeout=10)
        if lock_id:
            print(f"   成功获取锁，lock_id: {lock_id}")
            
            # 执行需要加锁的操作
            print("   执行加锁操作...")
            time.sleep(2)
            
            # 释放锁
            print("\n2. 释放锁:")
            success = redis_client.release_lock(lock_key, lock_id)
            print(f"   释放锁: {'成功' if success else '失败'}")
        else:
            print("   获取锁失败")
            
    finally:
        redis_client.close()


def example_lock_context_manager():
    """分布式锁上下文管理器示例"""
    print("\n" + "=" * 60)
    print("分布式锁上下文管理器示例")
    print("=" * 60)
    
    redis_client = RedisClient(**REDIS_CONFIG)
    
    try:
        lock_key = "my_lock"
        
        # 使用上下文管理器自动管理锁
        print("\n使用上下文管理器:")
        with redis_client.lock(lock_key, timeout=10) as lock_id:
            print(f"   成功获取锁，lock_id: {lock_id}")
            print("   执行加锁操作...")
            time.sleep(2)
            print("   操作完成，自动释放锁")
        
    finally:
        redis_client.close()


def example_lock_multithread():
    """多线程分布式锁示例"""
    print("\n" + "=" * 60)
    print("多线程分布式锁示例")
    print("=" * 60)
    
    redis_client = RedisClient(**REDIS_CONFIG)
    
    def worker(worker_id: int):
        """工作线程"""
        lock_key = "shared_lock"
        
        try:
            with redis_client.lock(lock_key, timeout=5) as lock_id:
                print(f"   线程 {worker_id} 获取锁成功，lock_id: {lock_id}")
                time.sleep(1)  # 模拟工作
                print(f"   线程 {worker_id} 完成工作，释放锁")
        except Exception as e:
            print(f"   线程 {worker_id} 获取锁失败: {e}")
    
    try:
        # 创建多个线程
        threads = []
        for i in range(5):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()
        
        # 等待所有线程完成
        for t in threads:
            t.join()
        
        print("\n所有线程执行完成")
        
    finally:
        redis_client.close()


def example_expire_operations():
    """过期时间操作示例"""
    print("\n" + "=" * 60)
    print("过期时间操作示例")
    print("=" * 60)
    
    redis_client = RedisClient(**REDIS_CONFIG)
    
    try:
        # 设置键和过期时间
        print("\n1. 设置键和过期时间:")
        redis_client.set("temp_key", "temp_value", ex=10)
        ttl = redis_client.ttl("temp_key")
        print(f"   键的剩余过期时间: {ttl} 秒")
        
        # 设置过期时间
        print("\n2. 设置过期时间:")
        redis_client.set("key1", "value1")
        redis_client.expire("key1", 60)
        ttl = redis_client.ttl("key1")
        print(f"   key1 的剩余过期时间: {ttl} 秒")
        
        # 移除过期时间
        print("\n3. 移除过期时间:")
        redis_client.persist("key1")
        ttl = redis_client.ttl("key1")
        print(f"   key1 的剩余过期时间: {ttl} (永久)")
        
    finally:
        redis_client.close()


def example_key_operations():
    """键操作示例"""
    print("\n" + "=" * 60)
    print("键操作示例")
    print("=" * 60)
    
    redis_client = RedisClient(**REDIS_CONFIG)
    
    try:
        # 设置一些键
        redis_client.set("key1", "value1")
        redis_client.set("key2", "value2")
        redis_client.set("key3", "value3")
        
        # 检查键是否存在
        print("\n1. 检查键是否存在:")
        exists = redis_client.exists("key1", "key2", "key4")
        print(f"   存在的键数量: {exists}")
        
        # 获取键的类型
        print("\n2. 获取键的类型:")
        key_type = redis_client.type("key1")
        print(f"   key1 的类型: {key_type}")
        
        # 重命名键
        print("\n3. 重命名键:")
        redis_client.rename("key1", "new_key1")
        value = redis_client.get("new_key1")
        print(f"   重命名后的值: {value}")
        
        # 删除键
        print("\n4. 删除键:")
        deleted = redis_client.delete("key2", "key3")
        print(f"   删除的键数量: {deleted}")
        
    finally:
        redis_client.close()


if __name__ == "__main__":
    print("Redis 客户端使用示例")
    print("=" * 60)
    print("\n注意：运行前请确保：")
    print("1. Redis 服务已启动")
    print("2. Redis 配置正确（修改 REDIS_CONFIG）")
    print("3. 已安装 redis: pip install redis")
    print("\n" + "=" * 60)
    
    # 取消注释以下行来运行示例
    # example_string_operations()
    # example_list_operations()
    # example_hash_operations()
    # example_set_operations()
    # example_sorted_set_operations()
    # example_lock_basic()
    # example_lock_context_manager()
    # example_lock_multithread()
    # example_expire_operations()
    # example_key_operations()

