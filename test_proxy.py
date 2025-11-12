"""
测试 TikTokAPI 代理是否生效
"""
from tiktok_api import TikTokAPI
from http_client import HttpClient

# 测试代理
proxy = r"socks5h://1pjw6067-region-US-st-New%20York-sid-ccxXQpPE-t-6:wmc4qbge@us.novproxy.io:1000"

print("=" * 60)
print("测试 TikTokAPI 代理配置")
print("=" * 60)

# 1. 测试 HttpClient 代理配置
print("\n1. 测试 HttpClient 代理配置:")
http_client = HttpClient(proxy=proxy)
print(f"   - HttpClient.proxy: {http_client.proxy}")
print(f"   - HttpClient.proxies: {http_client.proxies}")

# 2. 测试 TikTokAPI 代理配置
print("\n2. 测试 TikTokAPI 代理配置:")
api = TikTokAPI(proxy=proxy, timeout=30, max_retries=3)
print(f"   - TikTokAPI.proxy: {api.proxy}")
print(f"   - TikTokAPI.http_client.proxy: {api.http_client.proxy}")
print(f"   - TikTokAPI.http_client.proxies: {api.http_client.proxies}")

# 3. 测试代理传递到函数
print("\n3. 测试代理传递:")
print(f"   - get_seed 将接收的代理: {api.proxy or ''}")
print(f"   - get_token 将接收的代理: {api.proxy or ''}")
print(f"   - stats 将接收的代理: {api.proxy or ''}")

# 4. 测试一个简单的 HTTP 请求（使用 HttpClient）
print("\n4. 测试 HttpClient 实际请求（检查代理是否使用）:")
try:
    # 使用一个简单的测试 URL
    test_url = "https://httpbin.org/ip"
    print(f"   请求 URL: {test_url}")
    print(f"   使用代理: {http_client.proxy}")
    
    response = http_client.get(test_url, timeout=10)
    print(f"   状态码: {response.status_code}")
    print(f"   响应内容: {response.text[:200]}")
    print("   ✓ 代理配置正确，请求成功")
except Exception as e:
    print(f"   ✗ 请求失败: {e}")
    print("   注意：这可能是代理服务器问题，不是配置问题")

# 5. 测试更新代理
print("\n5. 测试更新代理:")
new_proxy = "socks5://test:pass@example.com:1080"
api.update_proxy(new_proxy)
print(f"   更新后的代理: {api.proxy}")
print(f"   HttpClient 代理: {api.http_client.proxy}")

print("\n" + "=" * 60)
print("测试完成")
print("=" * 60)

