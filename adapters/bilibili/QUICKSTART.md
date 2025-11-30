# B站适配器快速开始指南

## 📋 概述

B站适配器是KiraAI的一个适配器模块，可以自动回复B站的私信和评论。

## ✅ 前置条件

1. **不需要git clone任何项目**
   - `bilibili-api` 已经包含在 `requirements.txt` 中
   - 只需要确保已安装依赖：`pip install -r requirements.txt`

2. **KiraAI项目已配置**
   - 确保KiraAI可以正常运行
   - LLM配置已正确设置

## 🚀 快速开始（3步）

### 步骤1：获取B站Cookie

1. 打开浏览器，登录B站（https://www.bilibili.com）
2. 按 `F12` 打开开发者工具
3. 切换到 `Application`（或`存储`）标签
4. 在左侧找到 `Cookies` -> `https://www.bilibili.com`
5. 找到以下三个值并复制：
   - `SESSDATA` - 登录凭证
   - `bili_jct` - CSRF token
   - `DedeUserID` - 用户ID

### 步骤2：配置适配器

编辑 `data/config/adapters.ini`，添加以下配置：

```ini
[bilibili]
enabled = true
platform = Bilibili
desc = B站自动回复机器人
bot_pid = 你的用户ID
sessdata = 你的SESSDATA值
bili_jct = 你的bili_jct值
dedeuserid = 你的DedeUserID值
reply_private_messages = true
reply_comments = true
reply_interval = 5
max_replies_per_hour = 20
monitor_video_ids = 
monitor_dynamic_ids = 
user_list = 
group_list = 
```

### 步骤3：启动KiraAI

```bash
python main.py
```

B站适配器会自动启动并开始监听私信和评论！

## 📝 配置说明

### 基础配置

| 配置项 | 说明 | 示例 |
|--------|------|------|
| `enabled` | 是否启用 | `true` |
| `platform` | 平台标识 | `Bilibili`（必须） |
| `sessdata` | B站登录凭证 | 从Cookie获取 |
| `bili_jct` | CSRF token | 从Cookie获取 |
| `dedeuserid` | 用户ID | 从Cookie获取 |

### 功能开关

- `reply_private_messages = true` - 自动回复私信
- `reply_comments = true` - 自动回复评论

### 频率限制

- `reply_interval = 5` - 回复间隔（秒）
- `max_replies_per_hour = 20` - 每小时最大回复数

### 监听评论

如果要监听特定视频的评论：

```ini
monitor_video_ids = BV1xx411c7mu, BV1xx411c7mv
```

**如何获取BV号？**
- 打开B站视频页面
- 查看URL：`https://www.bilibili.com/video/BV1xx411c7mu`
- `BV1xx411c7mu` 就是BV号

## 🎯 使用示例

### 示例1：只回复私信

```ini
[bilibili]
enabled = true
platform = Bilibili
reply_private_messages = true
reply_comments = false
...
```

### 示例2：只回复特定视频的评论

```ini
[bilibili]
enabled = true
platform = Bilibili
reply_private_messages = false
reply_comments = true
monitor_video_ids = BV1xx411c7mu
...
```

### 示例3：保守的频率设置

```ini
[bilibili]
reply_interval = 10
max_replies_per_hour = 10
...
```

## ⚠️ 注意事项

1. **Cookie会过期**（通常7-30天），需要定期更新
2. **遵守频率限制**，避免被B站限流
3. **建议使用小号测试**，不要在主账号上使用
4. **不要将配置文件提交到Git**（包含敏感信息）

## 🐛 故障排查

### 无法启动

- 检查 `enabled = true`
- 检查Cookie是否正确填写
- 检查Cookie是否过期

### 收不到消息

- 检查功能开关是否启用
- 检查白名单配置
- 对于评论，检查 `monitor_video_ids` 是否配置

### 无法发送回复

- 检查频率限制设置
- 检查KiraAI的消息处理器是否正常
- 查看日志了解详细错误

## 📚 更多信息

详细文档请参考：`adapters/bilibili/README.md`

## 💡 提示

- 首次使用建议在测试环境中充分测试
- 建议使用小号进行测试
- 定期检查日志，了解运行状态
- Cookie过期后及时更新

祝使用愉快！🎉

