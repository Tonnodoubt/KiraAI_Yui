# B站 Cookie 获取指南

如果遇到 **"账号未登录" (-101)** 错误，可能需要获取额外的 Cookie 字段。

## 🔍 如何获取 Cookie

### 方法1：使用浏览器开发者工具

1. **打开浏览器，登录 Bilibili**
   - 访问 https://www.bilibili.com
   - 确保已成功登录

2. **打开开发者工具**
   - 按 `F12` 或右键页面 -> "检查"
   - 切换到 `Application`（Chrome）或 `存储`（Firefox）标签

3. **找到 Cookies**
   - 左侧菜单：`Storage` -> `Cookies` -> `https://www.bilibili.com`
   - 或直接搜索 `Cookies`

4. **复制需要的 Cookie 值**

### 必需字段（必须配置）

- **SESSDATA**: 登录会话数据
- **bili_jct**: CSRF token
- **DedeUserID**: 用户ID

### 可选字段（如果遇到 -101 错误时添加）

- **buvid3**: 浏览器唯一标识（通常以 `UUID-` 开头）
- **ac_time_value**: 访问时间值

## 📝 配置示例

在 `data/config/adapters.ini` 的 `[bilibili]` 部分添加：

```ini
[bilibili]
enabled = true
platform = Bilibili
desc = B站自动回复机器人
bot_pid = 你的DedeUserID值

# 必需字段
sessdata = 你的SESSDATA值
bili_jct = 你的bili_jct值
dedeuserid = 你的DedeUserID值

# 可选字段（如果发送评论时遇到"账号未登录"错误，请添加）
buvid3 = 你的buvid3值
ac_time_value = 你的ac_time_value值
```

## ⚠️ 注意事项

1. **Cookie 会过期**：B站的 Cookie 通常会在一定时间后过期（通常是一个月左右）
   - 如果遇到 -101 错误，首先尝试重新获取 Cookie

2. **Cookie 安全**：
   - 不要将 Cookie 分享给他人
   - 不要将包含 Cookie 的配置文件上传到公开仓库

3. **如何验证 Cookie 是否有效**：
   ```bash
   python adapters/bilibili/test_credential.py
   ```
   这个脚本会测试凭证是否有效

## 🔧 故障排除

### 问题：获取用户信息成功，但发送评论时返回 -101

**原因**：发送评论可能需要额外的 Cookie 字段（如 `buvid3` 和 `ac_time_value`）

**解决方案**：
1. 在浏览器中打开 Bilibili
2. 打开开发者工具，找到 Cookies
3. 复制 `buvid3` 和 `ac_time_value` 的值
4. 添加到 `adapters.ini` 配置文件中
5. 重启 KiraAI

### 问题：所有操作都返回 -101

**原因**：Cookie 已过期或无效

**解决方案**：
1. 重新登录 Bilibili 网页版
2. 重新获取所有 Cookie 值
3. 更新 `adapters.ini` 配置文件
4. 重启 KiraAI

