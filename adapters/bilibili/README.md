# B站适配器使用说明

## 概述

B站适配器是KiraAI的一个适配器模块，用于监听和自动回复B站的私信和评论。

## 功能特性

- ✅ 自动回复B站私信
- ✅ 自动回复视频/动态评论
- ✅ 消息去重，避免重复回复
- ✅ 频率限制，防止被限流
- ✅ 白名单支持
- ✅ 符合KiraAI适配器架构

## 安装依赖

B站适配器依赖 `bilibili-api` 库，在 `requirements.txt` 中已包含：

```bash
bilibili_api_python==17.3.0
```

如果未安装，请运行：

```bash
pip install bilibili-api
```

## 配置说明

### 1. 获取B站登录凭证

你需要从浏览器中获取以下Cookie信息：

1. 打开浏览器，登录B站（https://www.bilibili.com）
2. 按 `F12` 打开开发者工具
3. 切换到 `Application`（或`存储`）标签
4. 在左侧找到 `Cookies` -> `https://www.bilibili.com`
5. 找到以下三个值并复制：
   - `SESSDATA` - 这是你的登录凭证
   - `bili_jct` - 这是CSRF token
   - `DedeUserID` - 这是你的用户ID

### 2. 编辑适配器配置

编辑 `data/config/adapters.ini`，添加B站适配器配置：

```ini
[bilibili]
enabled = true
platform = Bilibili
desc = B站自动回复机器人
bot_pid = your_user_id
# B站登录凭证
sessdata = 你的SESSDATA值
bili_jct = 你的bili_jct值
dedeuserid = 你的DedeUserID值
# 功能开关
reply_private_messages = true
reply_comments = true
# 频率限制
reply_interval = 5
max_replies_per_hour = 20
# 监听目标（可选）
monitor_video_ids = BV1xx411c7mu, BV1xx411c7mv
monitor_dynamic_ids = 
# 白名单（可选，留空则处理所有消息）
user_list = 
group_list = 
```

### 配置项说明

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `enabled` | 是否启用适配器 | `false` |
| `platform` | 平台标识，必须为 `Bilibili` | `Bilibili` |
| `sessdata` | B站登录凭证（必填） | - |
| `bili_jct` | CSRF token（必填） | - |
| `dedeuserid` | 用户ID（必填） | - |
| `reply_private_messages` | 是否回复私信 | `true` |
| `reply_comments` | 是否回复评论 | `true` |
| `reply_interval` | 回复间隔（秒） | `5` |
| `max_replies_per_hour` | 每小时最大回复数 | `20` |
| `monitor_video_ids` | 监听评论的视频BV号列表（逗号分隔） | - |
| `monitor_dynamic_ids` | 监听评论的动态ID列表（逗号分隔） | - |
| `user_list` | 用户白名单（逗号分隔，留空则处理所有） | - |
| `group_list` | 群组白名单（B站不使用，可留空） | - |

## 使用方法

### 启动KiraAI

配置完成后，正常启动KiraAI即可：

```bash
python main.py
```

B站适配器会自动启动并开始监听私信和评论。

### 监听评论配置

如果要监听特定视频的评论，需要配置 `monitor_video_ids`：

```ini
monitor_video_ids = BV1xx411c7mu, BV1xx411c7mv
```

**如何获取视频BV号？**
- 打开B站视频页面
- 查看URL，例如：`https://www.bilibili.com/video/BV1xx411c7mu`
- `BV1xx411c7mu` 就是BV号

## 工作原理

1. **私信处理**：
   - 每30秒轮询一次会话列表
   - 获取未读私信
   - 将私信发布到KiraAI的事件总线
   - KiraAI的消息处理器会自动生成回复并发送

2. **评论处理**：
   - 每45秒轮询一次指定视频/动态
   - 获取最新评论
   - 将评论发布到KiraAI的事件总线
   - KiraAI的消息处理器会自动生成回复并发送

3. **消息去重**：
   - 使用消息ID/评论ID去重
   - 避免重复处理同一条消息

4. **频率限制**：
   - 检查回复间隔
   - 检查每小时最大回复数
   - 防止被B站限流

5. **长时间运行稳定性**：
   - 完善的异常处理机制，确保单个错误不会导致程序崩溃
   - 自动重试机制，出错后等待一段时间后继续运行
   - 定期检查凭证有效性（每小时检查一次）
   - 自动检测Cookie过期并尝试重载
   - 详细的错误日志，方便排查问题

## 注意事项

1. **Cookie有效期和自动重载**：
   - B站Cookie会过期（通常7-30天）
   - **好消息**：适配器现在支持自动检测Cookie过期并提示重载
   - 当检测到Cookie过期（-101错误）时，适配器会：
     - 自动尝试从配置文件重新加载Cookie（如果已更新）
     - 记录错误次数，超过3次后会尝试自动重载
     - 如果自动重载失败，会在日志中提示手动更新
   - **建议**：当Cookie刷新后，只需更新配置文件中的Cookie值，适配器会自动检测并重载（最多等待5分钟）
   - **长时间运行**：适配器设计为可以长时间运行，具有完善的错误处理和重试机制

2. **频率限制**：
   - B站对API调用有严格限制
   - 建议设置合理的 `reply_interval` 和 `max_replies_per_hour`
   - 过于频繁可能导致账号被限流

3. **账号安全**：
   - 建议使用小号测试
   - 不要在主账号上使用
   - 不要将配置文件提交到Git

4. **白名单配置**：
   - 如果设置了 `user_list`，只有白名单中的用户消息会被处理
   - 如果留空，则处理所有用户的消息

## 故障排查

### 问题1：适配器无法启动

**症状**：日志显示"B站适配器未启用"或"B站凭证未配置"

**解决方法**：
1. 检查 `enabled = true`
2. 检查 `sessdata`、`bili_jct`、`dedeuserid` 是否正确填写
3. 检查Cookie是否过期（重新获取）

### 问题1.1：Cookie过期（-101错误）

**症状**：日志显示"账号未登录"或"-101"错误

**解决方法**：
1. **自动重载**：更新配置文件中的Cookie值，适配器会在5分钟内自动检测并重载
2. **手动重载**：如果自动重载失败，重启KiraAI即可
3. **获取新Cookie**：参考 [COOKIE_GUIDE.md](./COOKIE_GUIDE.md) 获取最新的Cookie值

### 问题2：无法收到消息

**症状**：适配器已启动但收不到私信/评论

**解决方法**：
1. 检查白名单配置（如果设置了白名单）
2. 检查 `reply_private_messages` 和 `reply_comments` 是否为 `true`
3. 对于评论，检查 `monitor_video_ids` 是否配置正确
4. 查看日志了解详细错误

### 问题3：无法发送回复

**症状**：收到消息但不回复

**解决方法**：
1. 检查是否达到频率限制
2. 检查KiraAI的消息处理器是否正常工作
3. 查看日志了解详细错误

## 开发说明

B站适配器遵循KiraAI的适配器架构：

- 继承 `IMAdapter` 基类
- 实现 `start()`, `send_group_message()`, `send_direct_message()` 方法
- 通过 `publish()` 方法将消息发布到事件总线
- 在 `core/lifecycle.py` 中注册适配器

## 参考

- [KiraAI项目](https://github.com/xxynet/KiraAI)
- [bilibili-api文档](https://nemo2011.github.io/bilibili-api/)

