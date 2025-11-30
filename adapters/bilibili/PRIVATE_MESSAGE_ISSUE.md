# B站私信功能问题说明

## 🔴 当前问题

**无法回复私信**，主要原因：

### 1. `bilibili-api-python` 不支持私信功能

当前版本的 `bilibili-api-python` (17.3.0) **没有 `message` 模块**，因此无法使用官方 API 发送和接收私信。

**验证方法**：
```python
from bilibili_api import message  # ImportError: cannot import name 'message'
```

### 2. 代码中的 Bug（已修复）

之前的代码中有两个 bug：
- ❌ `msg_part.content` → ✅ `msg_part.text`
- ❌ `msg_part.reply_id` → ✅ `msg_part.message_id`

这些 bug 已经修复，但即使修复了，由于 `message` 模块不存在，私信功能仍然无法使用。

## 🔍 为什么评论功能可以工作？

- ✅ **评论功能**：`bilibili-api-python` 有完整的 `comment` 模块支持
- ❌ **私信功能**：`bilibili-api-python` 没有 `message` 模块

## 💡 解决方案

### 方案1：暂时禁用私信功能（推荐）

在 `data/config/adapters.ini` 中设置：

```ini
[bilibili]
reply_private_messages = false  # 禁用私信回复
reply_comments = true           # 启用评论回复
```

这样可以专注于评论回复功能，避免私信相关的错误。

### 方案2：等待 bilibili-api-python 更新

关注 `bilibili-api-python` 的 GitHub 仓库，等待官方添加私信功能支持：
- https://github.com/Nemo2011/bilibili-api

### 方案3：使用其他方式（不推荐）

1. **直接调用 B 站网页 API**：
   - 需要逆向分析 B 站网页端的私信 API
   - 可能不稳定，容易被封禁
   - 需要自己维护

2. **使用 Selenium/Playwright 自动化**：
   - 模拟浏览器操作发送私信
   - 性能差，资源消耗大
   - 容易被检测

## 📝 当前状态

- ✅ **评论回复**：完全正常工作
- ❌ **私信回复**：由于 API 限制，暂时无法使用

## 🔧 如果未来 bilibili-api-python 支持私信

如果未来 `bilibili-api-python` 添加了 `message` 模块，代码已经准备好了：

1. 代码会自动检测 `message` 模块是否可用
2. 如果可用，会自动启用私信监听和回复功能
3. 只需要更新 `bilibili-api-python` 到新版本即可

## 📌 建议

**目前建议**：
- 专注于评论回复功能（已完全可用）
- 暂时禁用私信功能，避免错误日志
- 等待 `bilibili-api-python` 官方支持

**配置示例**：
```ini
[bilibili]
enabled = true
reply_private_messages = false  # 暂时禁用
reply_comments = true           # 使用评论功能
monitor_video_ids = BV1FFU5B8EvN
```

