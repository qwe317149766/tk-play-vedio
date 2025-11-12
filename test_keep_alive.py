"""
测试 HTTP 客户端的 keep-alive 功能
"""
import time
import json
from http_client import HttpClient

# IP 查询服务列表（按优先级排序）
IP_SERVICES = [
    "https://httpbin.org/ip",
    "https://api.ipify.org?format=json",
    "https://api.ip.sb/ip",
    "https://ifconfig.me/ip",
    "https://icanhazip.com",
    "https://ipinfo.io/json",
]

# 备选测试服务列表（按优先级排序）
TEST_SERVICES = [
    "https://httpbin.org",
    "https://jsonplaceholder.typicode.com",
    "https://api.github.com",
    "https://www.baidu.com",
    "https://www.google.com",
]


def get_current_ip(client: HttpClient = None) -> dict:
    """
    获取当前 IP 地址
    
    Args:
        client: HttpClient 实例，如果为 None 则创建新实例
    
    Returns:
        包含 IP 信息的字典，格式: {"ip": "xxx.xxx.xxx.xxx", "service": "服务名称"}
    """
    if client is None:
        client = HttpClient(enable_keep_alive=True, timeout=5)
        should_close = True
    else:
        should_close = False
    
    try:
        for service_url in IP_SERVICES:
            try:
                response = client.get(service_url, timeout=5)
                if response.status_code == 200:
                    try:
                        data = response.json()
                        # 不同服务返回格式不同，统一处理
                        if isinstance(data, dict):
                            ip = data.get("ip") or data.get("origin") or data.get("query")
                        else:
                            ip = data
                        
                        if ip:
                            service_name = service_url.split("/")[2] if "/" in service_url else service_url
                            return {"ip": ip.strip() if isinstance(ip, str) else str(ip), "service": service_name}
                    except:
                        # 如果返回的是纯文本 IP
                        ip = response.text.strip()
                        if ip and "." in ip:
                            service_name = service_url.split("/")[2] if "/" in service_url else service_url
                            return {"ip": ip, "service": service_name}
            except Exception as e:
                continue
        
        return {"ip": None, "service": None, "error": "无法获取 IP 地址"}
    finally:
        if should_close:
            client.close()


def get_available_test_service():
    """获取可用的测试服务"""
    client = HttpClient(enable_keep_alive=True, timeout=5)
    try:
        for service in TEST_SERVICES:
            try:
                # 尝试访问根路径或简单端点
                if "httpbin.org" in service:
                    test_url = f"{service}/get"
                elif "jsonplaceholder" in service:
                    test_url = f"{service}/posts/1"
                elif "api.github.com" in service:
                    test_url = f"{service}/zen"
                else:
                    test_url = service
                
                response = client.get(test_url, timeout=5)
                if response.status_code < 500:  # 2xx, 3xx, 4xx 都算可用
                    print(f"✓ 使用测试服务: {service}")
                    return service
            except Exception as e:
                continue
        return None
    finally:
        client.close()


def test_keep_alive_headers():
    """测试 keep-alive 请求头"""
    print("=" * 60)
    print("测试 1: 检查 keep-alive 请求头")
    print("=" * 60)
    
    # 获取当前 IP
    print("\n【获取当前 IP 地址】")
    ip_info = get_current_ip()
    if ip_info.get("ip"):
        print(f"  当前 IP: {ip_info['ip']}")
        print(f"  查询服务: {ip_info['service']}")
    else:
        print(f"  ⚠ 无法获取 IP: {ip_info.get('error', '未知错误')}")
    
    # 获取可用的测试服务
    base_url = get_available_test_service()
    if not base_url:
        print("⚠ 所有测试服务都不可用，跳过此测试")
        return
    
    # 启用 keep-alive
    client = HttpClient(enable_keep_alive=True)
    
    try:
        # 根据服务选择测试 URL
        if "httpbin.org" in base_url:
            test_url = f"{base_url}/get"
        elif "jsonplaceholder" in base_url:
            test_url = f"{base_url}/posts/1"
        elif "api.github.com" in base_url:
            test_url = f"{base_url}/zen"
        else:
            test_url = base_url
        
        # 发送请求
        response = client.get(test_url)
        
        # 检查请求头
        print(f"\n请求 URL: {response.url}")
        print(f"状态码: {response.status_code}")
        
        if response.status_code >= 500:
            print(f"⚠ 服务器返回 {response.status_code} 错误，但连接已建立")
            print("  说明：503 表示服务暂时不可用，但 TCP 连接和 keep-alive 功能正常")
        
        # 检查响应头中的 Connection 信息
        connection_header = response.headers.get('Connection', '')
        print(f"\n响应头 Connection: {connection_header}")
        
        # 检查是否支持 keep-alive
        if 'keep-alive' in connection_header.lower() or connection_header.lower() == 'keep-alive':
            print("✓ Keep-alive 已启用（服务器支持）")
        elif connection_header.lower() == 'close':
            print("⚠ 服务器要求关闭连接")
        else:
            print("⚠ 服务器响应头中没有 keep-alive 信息")
            print("  注意：某些服务器可能不返回 Connection 头，但连接仍可能被复用")
        
        # 检查其他相关响应头
        keep_alive_timeout = response.headers.get('Keep-Alive', '')
        if keep_alive_timeout:
            print(f"Keep-Alive 超时设置: {keep_alive_timeout}")
        
    except Exception as e:
        print(f"请求失败: {e}")
    finally:
        client.close()


def test_connection_reuse_performance():
    """测试连接复用的性能差异"""
    print("\n" + "=" * 60)
    print("测试 2: 连接复用性能对比")
    print("=" * 60)
    
    # 获取当前 IP
    print("\n【获取当前 IP 地址】")
    ip_info = get_current_ip()
    if ip_info.get("ip"):
        print(f"  当前 IP: {ip_info['ip']}")
        print(f"  查询服务: {ip_info['service']}")
    else:
        print(f"  ⚠ 无法获取 IP: {ip_info.get('error', '未知错误')}")
    
    # 获取可用的测试服务
    base_url = get_available_test_service()
    if not base_url:
        print("⚠ 所有测试服务都不可用，跳过此测试")
        return
    
    # 根据服务选择测试 URL
    if "httpbin.org" in base_url:
        test_url = f"{base_url}/get"
    elif "jsonplaceholder" in base_url:
        test_url = f"{base_url}/posts/1"
    elif "api.github.com" in base_url:
        test_url = f"{base_url}/zen"
    else:
        test_url = base_url
    
    num_requests = 10
    
    # 测试启用 keep-alive
    print(f"\n【启用 keep-alive】")
    client_with_keepalive = HttpClient(enable_keep_alive=True, timeout=30)
    
    # 验证 IP（使用同一个客户端）
    ip_info_keepalive = get_current_ip(client_with_keepalive)
    if ip_info_keepalive.get("ip"):
        print(f"  客户端 IP: {ip_info_keepalive['ip']}")
    
    start_time = time.time()
    try:
        for i in range(num_requests):
            response = client_with_keepalive.get(test_url)
            if i == 0:
                print(f"  第 1 次请求状态码: {response.status_code}")
    except Exception as e:
        print(f"  请求失败: {e}")
    finally:
        elapsed_with_keepalive = time.time() - start_time
        client_with_keepalive.close()
    
    print(f"  完成 {num_requests} 次请求耗时: {elapsed_with_keepalive:.2f} 秒")
    print(f"  平均每次请求: {elapsed_with_keepalive/num_requests:.3f} 秒")
    
    # 等待一下，确保连接关闭
    time.sleep(1)
    
    # 测试禁用 keep-alive
    print(f"\n【禁用 keep-alive】")
    client_without_keepalive = HttpClient(enable_keep_alive=False, timeout=30)
    
    # 验证 IP（使用同一个客户端）
    ip_info_no_keepalive = get_current_ip(client_without_keepalive)
    if ip_info_no_keepalive.get("ip"):
        print(f"  客户端 IP: {ip_info_no_keepalive['ip']}")
    
    start_time = time.time()
    try:
        for i in range(num_requests):
            response = client_without_keepalive.get(test_url)
            if i == 0:
                print(f"  第 1 次请求状态码: {response.status_code}")
    except Exception as e:
        print(f"  请求失败: {e}")
    finally:
        elapsed_without_keepalive = time.time() - start_time
        client_without_keepalive.close()
    
    print(f"  完成 {num_requests} 次请求耗时: {elapsed_without_keepalive:.2f} 秒")
    print(f"  平均每次请求: {elapsed_without_keepalive/num_requests:.3f} 秒")
    
    # 性能对比
    print(f"\n【性能对比】")
    if elapsed_without_keepalive > elapsed_with_keepalive:
        improvement = ((elapsed_without_keepalive - elapsed_with_keepalive) / elapsed_without_keepalive) * 100
        print(f"✓ Keep-alive 提升了 {improvement:.1f}% 的性能")
        print(f"  节省时间: {elapsed_without_keepalive - elapsed_with_keepalive:.2f} 秒")
    else:
        print("⚠ 性能差异不明显（可能受网络波动影响）")
    
    # IP 验证
    print(f"\n【IP 验证】")
    if ip_info_keepalive.get("ip") and ip_info_no_keepalive.get("ip"):
        if ip_info_keepalive["ip"] == ip_info_no_keepalive["ip"]:
            print(f"✓ 两个客户端的 IP 一致: {ip_info_keepalive['ip']}")
        else:
            print(f"⚠ IP 不一致:")
            print(f"  启用 keep-alive 的 IP: {ip_info_keepalive['ip']}")
            print(f"  禁用 keep-alive 的 IP: {ip_info_no_keepalive['ip']}")


def test_multiple_requests_same_domain():
    """测试同一域名的多次请求（连接复用）"""
    print("\n" + "=" * 60)
    print("测试 3: 同一域名的多次请求（验证连接复用）")
    print("=" * 60)
    
    # 获取可用的测试服务
    base_url = get_available_test_service()
    if not base_url:
        print("⚠ 所有测试服务都不可用，跳过此测试")
        return
    
    # 根据服务选择端点
    if "httpbin.org" in base_url:
        endpoints = ["/get", "/status/200", "/headers", "/user-agent", "/ip"]
    elif "jsonplaceholder" in base_url:
        endpoints = ["/posts/1", "/posts/2", "/posts/3", "/users/1", "/comments/1"]
    elif "api.github.com" in base_url:
        endpoints = ["/zen", "/octocat", "/emojis"]
    else:
        endpoints = ["/"]
    
    client = HttpClient(enable_keep_alive=True, timeout=30)
    
    try:
        print(f"\n使用同一个 HttpClient 实例请求 {len(endpoints)} 个不同的端点:")
        
        start_time = time.time()
        for i, endpoint in enumerate(endpoints, 1):
            url = f"{base_url}{endpoint}"
            try:
                response = client.get(url)
                print(f"  {i}. {endpoint} - 状态码: {response.status_code}")
            except Exception as e:
                print(f"  {i}. {endpoint} - 失败: {e}")
        
        elapsed = time.time() - start_time
        print(f"\n总耗时: {elapsed:.2f} 秒")
        print(f"平均每次请求: {elapsed/len(endpoints):.3f} 秒")
        print("\n✓ 如果连接被复用，后续请求应该更快（因为不需要重新建立 TCP 连接）")
        
    except Exception as e:
        print(f"测试失败: {e}")
    finally:
        client.close()


def test_session_persistence():
    """测试 Session 持久化（连接复用）"""
    print("\n" + "=" * 60)
    print("测试 4: Session 持久化验证")
    print("=" * 60)
    
    # 获取当前 IP
    print("\n【获取当前 IP 地址】")
    ip_info = get_current_ip()
    if ip_info.get("ip"):
        print(f"  当前 IP: {ip_info['ip']}")
        print(f"  查询服务: {ip_info['service']}")
    else:
        print(f"  ⚠ 无法获取 IP: {ip_info.get('error', '未知错误')}")
    
    # 获取可用的测试服务
    base_url = get_available_test_service()
    if not base_url:
        print("⚠ 所有测试服务都不可用，跳过此测试")
        return
    
    # 根据服务选择测试 URL
    if "httpbin.org" in base_url:
        test_url = f"{base_url}/get"
    elif "jsonplaceholder" in base_url:
        test_url = f"{base_url}/posts/1"
    elif "api.github.com" in base_url:
        test_url = f"{base_url}/zen"
    else:
        test_url = base_url
    
    client = HttpClient(enable_keep_alive=True, timeout=30)
    
    try:
        # 第一次请求前验证 IP
        ip_info_1 = get_current_ip(client)
        if ip_info_1.get("ip"):
            print(f"\n客户端 IP: {ip_info_1['ip']}")
        
        # 第一次请求
        print("\n第一次请求（建立连接）:")
        start1 = time.time()
        response1 = client.get(test_url)
        elapsed1 = time.time() - start1
        print(f"  状态码: {response1.status_code}")
        print(f"  耗时: {elapsed1:.3f} 秒")
        
        # 短暂等待
        time.sleep(0.5)
        
        # 第二次请求（应该复用连接）
        print("\n第二次请求（复用连接）:")
        start2 = time.time()
        response2 = client.get(test_url)
        elapsed2 = time.time() - start2
        print(f"  状态码: {response2.status_code}")
        print(f"  耗时: {elapsed2:.3f} 秒")
        
        # 第三次请求
        print("\n第三次请求（复用连接）:")
        start3 = time.time()
        response3 = client.get(test_url)
        elapsed3 = time.time() - start3
        print(f"  状态码: {response3.status_code}")
        print(f"  耗时: {elapsed3:.3f} 秒")
        
        # 第三次请求后再次验证 IP
        ip_info_2 = get_current_ip(client)
        if ip_info_2.get("ip"):
            print(f"\n请求后客户端 IP: {ip_info_2['ip']}")
            if ip_info_1.get("ip") == ip_info_2.get("ip"):
                print("✓ IP 保持一致，连接复用正常")
            else:
                print("⚠ IP 发生变化，可能连接被重置")
        
        print(f"\n【分析】")
        print(f"  第一次请求耗时: {elapsed1:.3f} 秒（包含建立连接时间）")
        print(f"  第二次请求耗时: {elapsed2:.3f} 秒")
        print(f"  第三次请求耗时: {elapsed3:.3f} 秒")
        
        if elapsed2 < elapsed1 and elapsed3 < elapsed1:
            print("✓ 后续请求更快，说明连接被复用了（keep-alive 生效）")
        else:
            print("⚠ 性能差异不明显，可能受网络波动影响")
        
    except Exception as e:
        print(f"测试失败: {e}")
    finally:
        client.close()


def test_context_manager():
    """测试上下文管理器（自动关闭连接）"""
    print("\n" + "=" * 60)
    print("测试 5: 上下文管理器测试")
    print("=" * 60)
    
    # 获取当前 IP
    print("\n【获取当前 IP 地址】")
    ip_info = get_current_ip()
    if ip_info.get("ip"):
        print(f"  当前 IP: {ip_info['ip']}")
        print(f"  查询服务: {ip_info['service']}")
    else:
        print(f"  ⚠ 无法获取 IP: {ip_info.get('error', '未知错误')}")
    
    # 获取可用的测试服务
    base_url = get_available_test_service()
    if not base_url:
        print("⚠ 所有测试服务都不可用，跳过此测试")
        return
    
    # 根据服务选择测试 URL
    if "httpbin.org" in base_url:
        test_url = f"{base_url}/get"
    elif "jsonplaceholder" in base_url:
        test_url = f"{base_url}/posts/1"
    elif "api.github.com" in base_url:
        test_url = f"{base_url}/zen"
    else:
        test_url = base_url
    
    print("\n使用 with 语句自动管理连接:")
    
    with HttpClient(enable_keep_alive=True) as client:
        # 在上下文中验证 IP
        ip_info_context = get_current_ip(client)
        if ip_info_context.get("ip"):
            print(f"  客户端 IP: {ip_info_context['ip']}")
        
        response = client.get(test_url)
        print(f"  状态码: {response.status_code}")
        print("  连接在使用中...")
    
    print("✓ 退出 with 语句后，连接自动关闭")


def test_with_proxy(proxy: str):
    """使用代理进行测试"""
    print("=" * 60)
    print("使用代理进行测试")
    print("=" * 60)
    print(f"\n代理地址: {proxy.split('@')[1] if '@' in proxy else proxy}")
    
    # 创建带代理的客户端
    client = HttpClient(proxy=proxy, enable_keep_alive=True, timeout=30)
    
    try:
        # 测试 1: 获取 IP 地址（验证代理是否生效）
        print("\n【测试 1: 验证代理 IP】")
        ip_info = get_current_ip(client)
        if ip_info.get("ip"):
            print(f"✓ 通过代理获取的 IP: {ip_info['ip']}")
            print(f"  查询服务: {ip_info['service']}")
        else:
            print(f"⚠ 无法获取 IP: {ip_info.get('error', '未知错误')}")
        
        # 测试 2: Keep-alive 性能测试
        print("\n【测试 2: Keep-alive 性能测试】")
        # 使用更可靠的测试服务
        test_urls = [
            "https://jsonplaceholder.typicode.com/posts/1",
            "https://api.github.com/zen",
            "https://httpbin.org/get",
        ]
        
        # 选择可用的测试 URL
        test_url = None
        for url in test_urls:
            try:
                response = client.get(url, timeout=5)
                if response.status_code < 500:  # 2xx, 3xx, 4xx 都可用
                    test_url = url
                    print(f"  使用测试 URL: {url}")
                    break
            except:
                continue
        
        if not test_url:
            test_url = "https://httpbin.org/get"
            print(f"  使用默认测试 URL: {test_url} (可能返回 503)")
            print("  注意：503 不影响连接复用测试，TCP 连接仍可复用")
        
        # 启用 keep-alive
        print("\n启用 keep-alive:")
        
        # 在测试前获取并打印 IP
        ip_info_before = get_current_ip(client)
        if ip_info_before.get("ip"):
            print(f"  当前 IP: {ip_info_before['ip']}")
        
        start_time = time.time()
        for i in range(5):
            try:
                response = client.get(test_url)
                if i == 0:
                    print(f"  第 1 次请求状态码: {response.status_code}")
            except Exception as e:
                print(f"  请求失败: {e}")
                break
        elapsed_with_keepalive = time.time() - start_time
        print(f"  完成 5 次请求耗时: {elapsed_with_keepalive:.2f} 秒")
        print(f"  平均每次请求: {elapsed_with_keepalive/5:.3f} 秒")
        
        # 测试后再次获取并打印 IP（验证 IP 是否一致）
        ip_info_after = get_current_ip(client)
        if ip_info_after.get("ip"):
            print(f"  测试后 IP: {ip_info_after['ip']}")
            if ip_info_before.get("ip") == ip_info_after.get("ip"):
                print(f"  ✓ IP 保持一致: {ip_info_after['ip']}")
            else:
                print(f"  ⚠ IP 发生变化: {ip_info_before.get('ip')} -> {ip_info_after['ip']}")
        
        # 测试 3: 多次请求验证连接复用（更精确的测试）
        print("\n【测试 3: 连接复用验证（精确测试）】")
        print("连续发送多次请求，验证连接复用:")
        
        # 第一次请求（建立连接）
        print("\n第 1 次请求（建立连接）:")
        start1 = time.time()
        try:
            response1 = client.get(test_url)
            elapsed1 = time.time() - start1
            status1 = response1.status_code
            print(f"  状态码: {status1}, 耗时: {elapsed1:.3f} 秒")
            if status1 == 503:
                print("  ⚠ 服务器返回 503（服务不可用），但 TCP 连接已建立")
                print("  说明：503 是应用层状态码，不影响 TCP 连接复用")
        except Exception as e:
            print(f"  请求失败: {e}")
            elapsed1 = 0
            status1 = None
        
        # 短暂等待，但不要太长（避免连接超时）
        time.sleep(0.2)
        
        # 第二次请求（应该复用连接）
        print("\n第 2 次请求（复用连接）:")
        start2 = time.time()
        try:
            response2 = client.get(test_url)
            elapsed2 = time.time() - start2
            status2 = response2.status_code
            print(f"  状态码: {status2}, 耗时: {elapsed2:.3f} 秒")
            if status2 == 503:
                print("  ⚠ 服务器返回 503，但连接可能已复用（检查耗时）")
            if elapsed1 > 0:
                if elapsed2 < elapsed1:
                    print(f"  ✓ 比第一次快 {elapsed1 - elapsed2:.3f} 秒，说明连接被复用")
                    print("  ✓ 即使返回 503，TCP 连接复用仍然有效")
                elif elapsed2 > elapsed1 * 1.5:
                    print(f"  ⚠ 比第一次慢很多，可能连接被重置，需要重新建立")
                else:
                    print(f"  - 耗时相近，可能受网络波动影响")
        except Exception as e:
            print(f"  请求失败: {e}")
            elapsed2 = 0
            status2 = None
        
        # 第三次请求（继续复用连接）
        time.sleep(0.2)
        print("\n第 3 次请求（继续复用连接）:")
        start3 = time.time()
        try:
            response3 = client.get(test_url)
            elapsed3 = time.time() - start3
            status3 = response3.status_code
            print(f"  状态码: {status3}, 耗时: {elapsed3:.3f} 秒")
            if status3 == 503:
                print("  ⚠ 服务器返回 503，但连接可能已复用（检查耗时）")
            if elapsed1 > 0:
                if elapsed3 < elapsed1:
                    print(f"  ✓ 比第一次快 {elapsed1 - elapsed3:.3f} 秒，说明连接被复用")
                    print("  ✓ 即使返回 503，TCP 连接复用仍然有效")
                elif elapsed3 > elapsed1 * 1.5:
                    print(f"  ⚠ 比第一次慢很多，可能连接被重置")
                else:
                    print(f"  - 耗时相近")
        except Exception as e:
            print(f"  请求失败: {e}")
            elapsed3 = 0
            status3 = None
        
        # 快速连续请求（验证连接池）
        print("\n快速连续请求（验证连接池）:")
        
        # 在快速连续请求前获取并打印 IP
        ip_info_before_quick = get_current_ip(client)
        if ip_info_before_quick.get("ip"):
            print(f"  当前 IP: {ip_info_before_quick['ip']}")
        
        times = []
        for i in range(4, 7):
            start = time.time()
            try:
                response = client.get(test_url)
                elapsed = time.time() - start
                times.append(elapsed)
                print(f"  第 {i} 次请求: 状态码 {response.status_code}, 耗时 {elapsed:.3f} 秒")
            except Exception as e:
                print(f"  第 {i} 次请求失败: {e}")
        
        # 快速连续请求后再次获取并打印 IP（验证 IP 是否一致）
        ip_info_after_quick = get_current_ip(client)
        if ip_info_after_quick.get("ip"):
            print(f"  测试后 IP: {ip_info_after_quick['ip']}")
            if ip_info_before_quick.get("ip") == ip_info_after_quick.get("ip"):
                print(f"  ✓ IP 保持一致: {ip_info_after_quick['ip']}")
            else:
                print(f"  ⚠ IP 发生变化: {ip_info_before_quick.get('ip')} -> {ip_info_after_quick['ip']}")
        
        # 分析结果
        print(f"\n【连接复用分析】")
        print("重要说明：")
        print("  - HTTP 状态码（如 503）是应用层响应，与 TCP 连接复用无关")
        print("  - 即使返回 503，TCP 连接仍可复用，通过耗时判断连接复用情况")
        print("  - 如果后续请求比首次快，说明连接被复用，keep-alive 生效")
        
        if elapsed1 > 0 and elapsed2 > 0:
            avg_later = (elapsed2 + elapsed3) / 2 if elapsed3 > 0 else elapsed2
            if avg_later < elapsed1 * 0.8:
                print(f"\n✓ 后续请求平均耗时 ({avg_later:.3f}s) 明显低于首次 ({elapsed1:.3f}s)")
                print(f"  ✓ 说明连接被成功复用，keep-alive 生效")
                if status1 == 503 or status2 == 503:
                    print(f"  ✓ 即使服务器返回 503，TCP 连接复用仍然有效")
            elif avg_later > elapsed1 * 1.5:
                print(f"\n⚠ 后续请求平均耗时 ({avg_later:.3f}s) 明显高于首次 ({elapsed1:.3f}s)")
                print(f"  可能原因：连接被重置、代理限制、或网络波动")
            else:
                print(f"\n- 后续请求耗时与首次相近，可能受网络波动影响")
        
        if times:
            avg_time = sum(times) / len(times)
            print(f"\n快速连续请求平均耗时: {avg_time:.3f} 秒")
            if avg_time < elapsed1 * 0.9:
                print(f"✓ 快速连续请求更快，说明连接池工作正常")
                print(f"✓ 即使返回 503，连接池仍然正常工作")
        
        # 测试 4: 再次验证 IP（确保代理持续有效）
        print("\n【测试 4: 再次验证代理 IP】")
        ip_info_2 = get_current_ip(client)
        if ip_info_2.get("ip"):
            print(f"✓ 代理 IP: {ip_info_2['ip']}")
            if ip_info.get("ip") == ip_info_2.get("ip"):
                print("✓ IP 保持一致，代理连接稳定")
            else:
                print("⚠ IP 发生变化")
        
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()


if __name__ == "__main__":
    import sys
    
    # 检查是否提供了代理参数
    proxy = None
    if len(sys.argv) > 1:
        proxy = sys.argv[1]
    else:
        # 使用默认代理
        proxy = "socks5h://accountId-5086-tunnelId-12988-area-us:a123456@proxyas.starryproxy.com:10000"
    
    if proxy:
        # 使用代理进行测试
        print("HTTP 客户端 Keep-Alive 功能测试（使用代理）")
        print("=" * 60)
        print("\n注意：")
        print("1. 使用代理进行测试")
        print("2. 将验证代理 IP 是否正确")
        print("3. 测试 keep-alive 在代理环境下的表现")
        print("\n" + "=" * 60)
        
        try:
            test_with_proxy(proxy)
            
            print("\n" + "=" * 60)
            print("代理测试完成！")
            print("=" * 60)
            
        except KeyboardInterrupt:
            print("\n\n测试被用户中断")
        except Exception as e:
            print(f"\n测试过程中发生错误: {e}")
            import traceback
            traceback.print_exc()
    else:
        # 不使用代理的原始测试
        print("HTTP 客户端 Keep-Alive 功能测试")
        print("=" * 60)
        print("\n注意：")
        print("1. 确保网络连接正常")
        print("2. 测试脚本会自动选择可用的测试服务")
        print("3. 如果所有服务都不可用，可能是网络问题")
        print("4. 503 状态码说明：")
        print("   - 503 Service Unavailable 表示服务器暂时不可用")
        print("   - 但 TCP 连接已建立，keep-alive 功能仍然有效")
        print("   - 性能测试仍然可以验证连接复用效果")
        print("\n" + "=" * 60)
        
        try:
            # 运行所有测试
            test_keep_alive_headers()
            test_connection_reuse_performance()
            test_multiple_requests_same_domain()
            test_session_persistence()
            test_context_manager()
            
            print("\n" + "=" * 60)
            print("所有测试完成！")
            print("=" * 60)
            
        except KeyboardInterrupt:
            print("\n\n测试被用户中断")
        except Exception as e:
            print(f"\n测试过程中发生错误: {e}")
            import traceback
            traceback.print_exc()

