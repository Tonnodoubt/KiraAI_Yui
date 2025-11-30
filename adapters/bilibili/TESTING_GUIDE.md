# B站适配器测试指南

## 📋 测试前准备

### 1. 配置检查

确保 `data/config/adapters.ini` 中的B站配置完整：

```ini
[bilibili]
enabled = true
platform = Bilibili
desc = B站自动回复机器人
bot_pid = 你的用户ID
sessdata = 你的SESSDATA值
bili_jct = 你的bili_jct值
dedeuserid = 你的DedeUserID值  # ⚠️ 重要：必须填写实际值，不能是占位符
reply_private_messages = true
reply_comments = true
reply_interval = 5
max_replies_per_hour = 20
monitor_video_ids = BV1xx411c7mu  # 要监听评论的视频BV号（可选）
monitor_dynamic_ids = 
user_list = 
group_list = 
```

### 2. 运行测试脚本

首先运行测试脚本检查配置：

```bash
python adapters/bilibili/test_adapter.py
```

测试脚本会检查：
- ✅ bilibili-api是否正常导入
- ✅ 凭证配置是否正确
- ✅ 适配器是否能正常导入和初始化

## 🚀 测试步骤

### 步骤1：启动KiraAI

```bash
python main.py
```

### 步骤2：查看启动日志

启动后，查看日志输出，应该看到类似以下信息：

```
[INFO] Starting bot...
[INFO] Started adapter Bilibili
[INFO] B站适配器已启动
[INFO] 评论监听已启动
```

**注意**：如果看到以下警告，说明私信功能暂不可用（这是正常的，因为bilibili-api 17.3.0版本没有message模块）：
```
[WARN] bilibili_api.message模块不可用，私信功能已禁用
```

### 步骤3：测试评论回复功能

1. **配置监听视频**：
   在 `adapters.ini` 中设置 `monitor_video_ids`，例如：
   ```ini
   monitor_video_ids = BV1xx411c7mu
   ```

2. **发送测试评论**：
   - 用另一个B站账号（或小号）
   - 在配置的视频下发表评论
   - 等待机器人回复（通常1分钟内）

3. **检查日志**：
   查看日志应该看到：
   ```
   [INFO] 收到B站评论: 用户名(用户ID) -> 评论内容
   [INFO] 已回复评论: 回复内容
   ```

### 步骤4：验证回复

- 检查视频评论区，确认机器人是否已回复
- 检查回复内容是否符合角色设定

## ⚠️ 已知限制

### 私信功能暂不可用

由于 `bilibili-api-python 17.3.0` 版本中没有 `message` 模块，私信功能暂时无法使用。

**解决方案**：
1. 等待bilibili-api更新支持私信API
2. 或者降级到支持message模块的旧版本（不推荐）
3. 目前只能使用评论回复功能

### 评论功能正常

评论功能可以正常使用，包括：
- ✅ 监听指定视频的评论
- ✅ 自动生成回复
- ✅ 自动发送回复

## 🐛 故障排查

### 问题1：适配器无法启动

**症状**：日志显示"B站适配器未启用"或"B站凭证未配置"

**解决方法**：
1. 检查 `enabled = true`
2. 检查 `sessdata`、`bili_jct`、`dedeuserid` 是否正确填写
3. 运行测试脚本检查配置

### 问题2：收不到评论

**症状**：适配器已启动但收不到评论

**解决方法**：
1. 检查 `monitor_video_ids` 是否配置了正确的BV号
2. 检查 `reply_comments = true`
3. 确认评论是在配置的视频下发表的
4. 查看日志是否有错误信息

### 问题3：无法发送回复

**症状**：收到评论但不回复

**解决方法**：
1. 检查是否达到频率限制（`reply_interval` 和 `max_replies_per_hour`）
2. 检查KiraAI的LLM配置是否正确
3. 查看日志了解详细错误

### 问题4：dedeuserid未配置

**症状**：测试脚本显示"dedeuserid未正确配置"

**解决方法**：
1. 打开浏览器，登录B站
2. 按F12打开开发者工具
3. Application -> Cookies -> https://www.bilibili.com
4. 找到 `DedeUserID` 的值
5. 填入配置文件中的 `dedeuserid` 字段

## 📝 测试检查清单

- [ ] 运行测试脚本，所有测试通过
- [ ] dedeuserid已正确填写（不是占位符）
- [ ] enabled = true
- [ ] monitor_video_ids已配置（如果要测试评论功能）
- [ ] KiraAI正常启动，适配器加载成功
- [ ] 在测试视频下发表评论
- [ ] 查看日志确认收到评论
- [ ] 检查评论区确认收到回复

## 💡 提示

1. **首次测试建议**：
   - 使用小号测试
   - 配置一个测试视频的BV号
   - 设置较长的 `reply_interval`（如10秒）避免频率限制

2. **查看日志**：
   - 日志文件通常在项目根目录的 `log.log`
   - 或者查看控制台输出

3. **调试模式**：
   - 如果遇到问题，可以临时设置 `reply_interval = 1` 加快测试
   - 测试完成后记得改回正常值

祝测试顺利！🎉

