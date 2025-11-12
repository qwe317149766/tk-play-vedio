"""
检查 config.json 配置文件是否合理
"""
import json
import os

def check_config():
    """检查配置文件"""
    print("=" * 60)
    print("检查 config.json 配置文件")
    print("=" * 60)
    
    if not os.path.exists("config.json"):
        print("❌ 配置文件不存在: config.json")
        return
    
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ JSON 格式错误: {e}")
        return
    except Exception as e:
        print(f"❌ 读取配置文件失败: {e}")
        return
    
    issues = []
    warnings = []
    
    # 检查 MySQL 配置
    print("\n【MySQL 配置检查】")
    mysql_config = config.get("mysql", {})
    
    # 必需字段检查
    required_fields = ["host", "port", "user", "database"]
    for field in required_fields:
        if field not in mysql_config:
            issues.append(f"MySQL 缺少必需字段: {field}")
        else:
            print(f"  ✓ {field}: {mysql_config[field]}")
    
    # 密码检查
    if "password" not in mysql_config:
        warnings.append("MySQL 未设置密码（如果数据库需要密码会连接失败）")
    else:
        password = mysql_config["password"]
        if not password:
            warnings.append("MySQL 密码为空（如果数据库需要密码会连接失败）")
        else:
            print(f"  ✓ password: {'*' * len(password)} (已设置)")
    
    # 端口检查
    port = mysql_config.get("port", 3306)
    if not isinstance(port, int) or port < 1 or port > 65535:
        issues.append(f"MySQL 端口无效: {port} (应为 1-65535 之间的整数)")
    elif port != 3306:
        print(f"  ⚠ 使用非标准端口: {port} (标准端口是 3306)")
    
    # 字符集检查
    charset = mysql_config.get("charset", "utf8mb4")
    if charset not in ["utf8", "utf8mb4", "latin1", "gbk", "gb2312"]:
        warnings.append(f"MySQL 字符集可能不常见: {charset}")
    else:
        print(f"  ✓ charset: {charset}")
    
    # max_connections 检查
    max_conn = mysql_config.get("max_connections", 10)
    if not isinstance(max_conn, int) or max_conn < 1:
        issues.append(f"MySQL max_connections 无效: {max_conn}")
    elif max_conn > 1000:
        warnings.append(f"MySQL max_connections 值很大: {max_conn} (建议值: 10-100)")
        print(f"  ⚠ max_connections: {max_conn} (值较大，建议根据实际需求调整)")
    elif max_conn < 5:
        warnings.append(f"MySQL max_connections 值较小: {max_conn} (建议至少 5)")
    else:
        print(f"  ✓ max_connections: {max_conn}")
    
    # autocommit 检查
    autocommit = mysql_config.get("autocommit", False)
    if not isinstance(autocommit, bool):
        issues.append(f"MySQL autocommit 应为布尔值: {autocommit}")
    else:
        print(f"  ✓ autocommit: {autocommit}")
    
    # 检查 Redis 配置
    print("\n【Redis 配置检查】")
    redis_config = config.get("redis", {})
    
    # 必需字段检查
    required_fields = ["host", "port", "db"]
    for field in required_fields:
        if field not in redis_config:
            issues.append(f"Redis 缺少必需字段: {field}")
        else:
            print(f"  ✓ {field}: {redis_config[field]}")
    
    # 密码检查
    if "password" not in redis_config:
        print(f"  ⚠ password: 未设置（如果 Redis 需要密码会连接失败）")
    else:
        password = redis_config["password"]
        if password is None:
            print(f"  ✓ password: null (无密码)")
        elif not password:
            warnings.append("Redis 密码为空字符串（如果 Redis 需要密码会连接失败）")
        else:
            print(f"  ✓ password: {'*' * len(password)} (已设置)")
    
    # 端口检查
    port = redis_config.get("port", 6379)
    if not isinstance(port, int) or port < 1 or port > 65535:
        issues.append(f"Redis 端口无效: {port} (应为 1-65535 之间的整数)")
    elif port != 6379:
        print(f"  ⚠ 使用非标准端口: {port} (标准端口是 6379)")
    
    # db 检查
    db = redis_config.get("db", 0)
    if not isinstance(db, int) or db < 0 or db > 15:
        issues.append(f"Redis db 无效: {db} (应为 0-15 之间的整数)")
    else:
        print(f"  ✓ db: {db}")
    
    # max_connections 检查
    max_conn = redis_config.get("max_connections", 50)
    if not isinstance(max_conn, int) or max_conn < 1:
        issues.append(f"Redis max_connections 无效: {max_conn}")
    elif max_conn > 500:
        warnings.append(f"Redis max_connections 值很大: {max_conn} (建议值: 50-200)")
        print(f"  ⚠ max_connections: {max_conn} (值较大，建议根据实际需求调整)")
    elif max_conn < 10:
        warnings.append(f"Redis max_connections 值较小: {max_conn} (建议至少 10)")
    else:
        print(f"  ✓ max_connections: {max_conn}")
    
    # timeout 检查
    socket_timeout = redis_config.get("socket_timeout", 5)
    if not isinstance(socket_timeout, (int, float)) or socket_timeout < 1:
        issues.append(f"Redis socket_timeout 无效: {socket_timeout}")
    else:
        print(f"  ✓ socket_timeout: {socket_timeout}")
    
    socket_connect_timeout = redis_config.get("socket_connect_timeout", 5)
    if not isinstance(socket_connect_timeout, (int, float)) or socket_connect_timeout < 1:
        issues.append(f"Redis socket_connect_timeout 无效: {socket_connect_timeout}")
    else:
        print(f"  ✓ socket_connect_timeout: {socket_connect_timeout}")
    
    # decode_responses 检查
    decode_responses = redis_config.get("decode_responses", True)
    if not isinstance(decode_responses, bool):
        issues.append(f"Redis decode_responses 应为布尔值: {decode_responses}")
    else:
        print(f"  ✓ decode_responses: {decode_responses}")
    
    # default_ex 检查
    default_ex = redis_config.get("default_ex")
    if default_ex is not None:
        if not isinstance(default_ex, int) or default_ex < 1:
            issues.append(f"Redis default_ex 无效: {default_ex} (应为正整数或 null)")
        else:
            print(f"  ✓ default_ex: {default_ex} 秒")
    else:
        print(f"  ✓ default_ex: null (不设置默认过期时间)")
    
    # 总结
    print("\n" + "=" * 60)
    print("检查结果总结")
    print("=" * 60)
    
    if issues:
        print(f"\n❌ 发现 {len(issues)} 个问题:")
        for i, issue in enumerate(issues, 1):
            print(f"  {i}. {issue}")
    else:
        print("\n✓ 未发现严重问题")
    
    if warnings:
        print(f"\n⚠ 发现 {len(warnings)} 个警告:")
        for i, warning in enumerate(warnings, 1):
            print(f"  {i}. {warning}")
    else:
        print("\n✓ 未发现警告")
    
    # 建议
    print("\n【建议】")
    if mysql_config.get("max_connections", 10) > 100:
        print("  1. MySQL max_connections 建议设置为 10-100（根据实际并发需求）")
    if redis_config.get("max_connections", 50) > 200:
        print("  2. Redis max_connections 建议设置为 50-200（根据实际并发需求）")
    if mysql_config.get("user") == "root":
        print("  3. 生产环境建议使用非 root 用户连接 MySQL")
    if not mysql_config.get("password"):
        print("  4. 生产环境必须设置 MySQL 密码")
    if redis_config.get("password") is None or redis_config.get("password") == "":
        print("  5. 生产环境建议设置 Redis 密码")
    
    print("\n" + "=" * 60)
    
    return len(issues) == 0

if __name__ == "__main__":
    check_config()

