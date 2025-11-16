# tem3-1.py 测试脚本说明

## 文件
- **测试脚本**: `test_tem3_1.py`
- **被测试文件**: `demos/stats/tem3-1.py`

## 使用方法

### 1. 配置测试参数

在 `test_tem3_1.py` 中修改以下参数：

#### 设备信息
```python
device = {
    "device_id": "7572992123600520718",
    "install_id": "7572992571081852727",
    "ua": "com.zhiliaoapp.musically/2024204030...",
    # ... 其他设备参数
    "priv_key": "8ce1a5e5538fc0f09fe4d72b540b6d6c14ad0e642b3437e20acad471bb356f4d",
    "device_guard_data0": "{...}",
    "seed": "MDGnHpzSr3EEIDcx1DBjx4DszbwoSSN2VGJ68K9cTU+GeRqCQ2ZpFAQtEhz4f03r3+gEJAPHxxCf80KCSKTwBAGewTGUGbAAe8R+mUIX8XhgqsAJC+i0py1GYnGB4NaWZfI=",
    "seed_type": 6,
    "token": "A1Ez7PXUjH9jID7vE1x6XM8hp"
}
```

#### 视频ID
```python
aweme_id = "7571031981614451975"  # 修改为要测试的视频ID
```

#### 代理设置
```python
proxy = "socks5h://your-proxy-address:port"  # 修改为你的代理地址
```

#### 测试次数
```python
for i in range(10):  # 修改为你想要的测试次数
```

### 2. 运行测试

```bash
python test_tem3_1.py
```

### 3. 输出示例

```
================================================================================
测试 tem3-1.py - stats_3 函数
================================================================================
设备ID: 7572992123600520718
安装ID: 7572992571081852727
视频ID: 7571031981614451975
Seed: MDGnHpzSr3EEIDcx1DBjx4Dszbwo...
Seed Type: 6
Token: A1Ez7PXUjH9jID7vE1x6...
================================================================================

[测试 1/10] 开始...
[设备: 7572992123600520718] {"status_code":0,"extra":{"now":1763224011000},...}
[测试 1/10] ✓ 成功
[测试 1/10] 等待 4 秒...

[测试 2/10] 开始...
[设备: 7572992123600520718] {"status_code":0,"extra":{"now":1763224015000},...}
[测试 2/10] ✓ 成功
[测试 2/10] 等待 3 秒...

...

================================================================================
测试统计
================================================================================
总测试次数: 10
成功次数: 8
失败次数: 2
成功率: 80.0%
================================================================================
```

## 功能说明

### 测试内容
1. ✅ 调用 `stats_3` 函数发送播放统计请求
2. ✅ 自动递增 `signcount` 参数
3. ✅ 每次测试间随机延迟 3-5 秒
4. ✅ 统计成功率

### 测试参数
- **aweme_id**: 视频ID
- **seed**: 设备 seed（从 device 获取）
- **seed_type**: seed 类型（从 device 获取）
- **token**: 设备 token（从 device 获取）
- **device**: 设备信息字典（包含所有必需字段）
- **signcount**: 签名计数（每次测试递增）
- **proxy**: 代理地址

### 必需的 device 字段
```python
device = {
    # 基本信息
    "device_id": "...",
    "install_id": "...",
    "ua": "...",
    
    # 时间戳
    "apk_first_install_time": 1761856226206,
    "apk_last_update_time": 1761856266384,
    
    # 安全相关
    "priv_key": "...",  # 私钥（hex格式）
    "device_guard_data0": "{...}",  # device guard 数据（JSON字符串）
    "tt_ticket_guard_public_key": "...",  # 公钥（可选）
    
    # 认证信息
    "seed": "...",
    "seed_type": 6,
    "token": "...",
    
    # 其他设备属性
    # ...
}
```

## 注意事项

1. **代理配置**
   - 确保代理地址正确且可用
   - 支持格式：`socks5h://user:pass@host:port`

2. **设备数据**
   - 必须包含所有必需字段
   - `priv_key` 和 `device_guard_data0` 是关键字段
   - `seed`, `seed_type`, `token` 必须有效

3. **测试频率**
   - 建议每次测试间隔 3-5 秒
   - 避免频繁请求导致封禁

4. **错误处理**
   - 脚本会捕获并显示异常
   - 可以使用 Ctrl+C 中断测试

## 高级用法

### 批量测试多个设备

```python
devices = [
    device1,
    device2,
    device3,
    # ...
]

for idx, device in enumerate(devices):
    print(f"\n{'='*80}")
    print(f"测试设备 {idx+1}/{len(devices)}")
    print(f"{'='*80}")
    
    # 测试当前设备
    # ...
```

### 测试不同视频

```python
video_ids = [
    "7571031981614451975",
    "7572772064793283848",
    # ...
]

for video_id in video_ids:
    result = stats_3(
        aweme_id=video_id,
        # ...
    )
```

## 故障排查

### 1. 导入错误
```
ImportError: No module named 'tem3_1'
```
**解决**: 脚本使用动态导入，确保 `demos/stats/tem3-1.py` 文件存在

### 2. 连接错误
```
ConnectionError: ...
```
**解决**: 检查代理配置是否正确

### 3. 认证错误
```
{"status_code": 10000, ...}
```
**解决**: 检查 seed、token 是否有效

### 4. 设备数据错误
```
KeyError: 'priv_key'
```
**解决**: 确保 device 字典包含所有必需字段

