"""
QQ频道（QQ Guild）适配器
使用 qq-botpy SDK 实现
"""
import asyncio
import json
import threading
import random
from typing import Any, Dict, Union, List, Optional

try:
    import botpy
    from botpy import Client, Token
    from botpy.message import Message
    QQ_BOTPY_AVAILABLE = True
except ImportError:
    QQ_BOTPY_AVAILABLE = False
    # 定义占位类型，避免类型注解错误
    botpy = None
    Client = None
    Token = None
    Message = Any

from core.logging_manager import get_logger
from utils.adapter_utils import IMAdapter
from utils.message_utils import BotDirectMessage, BotGroupMessage, MessageSending, MessageType

logger = get_logger("qq_guild_adapter", "cyan")


class QQGuildBotClient(Client):
    """自定义Bot客户端，用于处理消息事件"""
    
    def __init__(self, adapter_instance, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.adapter = adapter_instance
    
    async def on_ready(self):
        """机器人准备就绪事件"""
        logger.info(f"[QQ频道] 机器人已准备就绪")
        if hasattr(self, 'robot') and self.robot:
            robot_id = getattr(self.robot, 'id', 'N/A')
            robot_name = getattr(self.robot, 'username', None) or getattr(self.robot, 'name', None) or getattr(self.robot, 'nickname', None) or 'N/A'
            logger.info(f"[QQ频道] 机器人信息 - ID: {robot_id}, 名称: {robot_name}")
            logger.info(f"[QQ频道] 重要：如果机器人不在频道成员列表中，需要在QQ频道管理后台添加机器人")
            logger.info(f"[QQ频道] 添加方法：频道设置 -> 机器人管理 -> 添加机器人 -> 输入AppID: {self.adapter.app_id}")
    
    async def on_at_message_create(self, message: Any):
        """@消息创建事件（QQ频道主要使用@消息）"""
        logger.info(f"[QQ频道] 收到@消息事件")
        await self.adapter._on_message(None, message)
    
    async def on_message_create(self, message: Any):
        """普通消息创建事件"""
        logger.info(f"[QQ频道] 收到普通消息事件")
        await self.adapter._on_message(None, message)


class QQGuildAdapter(IMAdapter):
    """QQ频道适配器"""
    
    def __init__(self, config: Dict[str, Any], loop: asyncio.AbstractEventLoop, event_bus: asyncio.Queue):
        super().__init__(config, loop, event_bus)
        self.name: str = "QQ Guild"
        
        if not QQ_BOTPY_AVAILABLE:
            logger.error("qq-botpy 未安装，请运行: pip install qq-botpy")
            return
        
        # 配置
        self.app_id = self.config.get("app_id", "")
        self.token = self.config.get("token", "")
        self.secret = self.config.get("secret", "")
        
        # 白名单配置（QQ频道使用频道ID和子频道ID）
        self.guild_list: List[str] = []
        self.channel_list: List[str] = []
        self._init_whitelists()
        
        # 响应限制配置
        self.response_probability = float(self.config.get("response_probability", 1.0))
        self.cooldown_seconds = int(self.config.get("cooldown_seconds", 0))
        self.require_strict_match = self.config.get("require_strict_match", "false").lower() == "true"
        self.waking_keywords = [kw.strip() for kw in self.config.get("waking_keywords", "").split(",") if kw.strip()]
        self.last_response_time: Dict[str, float] = {}
        
        # 消息类型支持
        self.message_types = [MessageType.Text, MessageType.Image, MessageType.At, MessageType.Reply, MessageType.Emoji]
        
        # Emoji字典
        self.emoji_dict = self._load_dict("adapters/qq_guild/emoji.json")
        
        # Bot实例
        self.bot: Optional[Client] = None
    
    @staticmethod
    def _load_dict(path: str) -> Dict[str, Any]:
        """加载字典"""
        try:
            with open(path, 'r', encoding="utf-8") as f:
                emoji_json = f.read()
            return json.loads(emoji_json)
        except Exception as e:
            return {}
        
    def _init_whitelists(self):
        """初始化白名单列表"""
        guild_list_str = self.config.get("guild_list", "")
        channel_list_str = self.config.get("channel_list", "")
        
        if guild_list_str:
            self.guild_list = [item.strip() for item in guild_list_str.split(",") if item.strip()]
        if channel_list_str:
            self.channel_list = [item.strip() for item in channel_list_str.split(",") if item.strip()]
    
    def _start_blocking(self):
        """在独立线程中启动Bot（阻塞）"""
        if not QQ_BOTPY_AVAILABLE:
            logger.error("qq-botpy 未安装，无法启动QQ频道适配器")
            return
            
        try:
            # 在新线程中创建新的事件循环
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # 创建自定义Bot客户端（继承Client并重写消息事件方法）
            # 需要同时监听@消息和普通消息
            intents = botpy.Intents(public_guild_messages=True)
            self.bot = QQGuildBotClient(
                self,  # 传入adapter实例
                intents=intents
            )
            
            # 启动Bot（根据botpy文档，start方法接受appid和secret）
            # bot.run() 会阻塞，直到Bot关闭
            self.bot.run(appid=self.app_id, secret=self.secret)
            
            # 启动后记录机器人信息
            if hasattr(self.bot, 'robot') and self.bot.robot:
                logger.info(f"[QQ频道] 机器人已启动 - ID: {self.bot.robot.id}, 名称: {self.bot.robot.username}")
                logger.info(f"[QQ频道] 在频道中@机器人时，请使用机器人的显示名称或昵称")
        except Exception as e:
            logger.error(f"QQ频道Bot启动失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def start(self):
        """启动适配器"""
        if not QQ_BOTPY_AVAILABLE:
            logger.error("qq-botpy 未安装，跳过启动")
            return
            
        if not self.app_id or not self.token:
            logger.error("QQ频道 app_id 或 token 未配置，跳过启动")
            return
            
        threading.Thread(target=self._start_blocking, daemon=True).start()
        logger.info("QQ频道适配器启动中...")
    
    async def _on_message(self, context: Any, message: Any):
        """处理收到的消息"""
        try:
            # 首先记录所有收到的消息（无论是否处理）
            logger.info(f"[QQ频道] ========== 收到消息 ==========")
            logger.info(f"[QQ频道] 消息对象类型: {type(message)}")
            logger.info(f"[QQ频道] 消息对象属性: {dir(message)}")
            
            # 检查频道白名单
            channel_id = str(message.channel_id) if hasattr(message, 'channel_id') else ""
            guild_id = str(message.guild_id) if hasattr(message, 'guild_id') else ""
            
            # 记录收到的消息信息（用于获取实际ID）
            content_preview = str(message.content)[:50] if hasattr(message, 'content') and message.content else 'N/A'
            author_id = str(message.author.id) if hasattr(message, 'author') and hasattr(message.author, 'id') else 'N/A'
            author_name = str(message.author.username) if hasattr(message, 'author') and hasattr(message.author, 'username') else 'N/A'
            
            logger.info(f"[QQ频道] 收到消息详情:")
            logger.info(f"  - Guild ID: {guild_id}")
            logger.info(f"  - Channel ID: {channel_id}")
            logger.info(f"  - 发送者ID: {author_id}")
            logger.info(f"  - 发送者名称: {author_name}")
            logger.info(f"  - 消息内容: {content_preview}")
            logger.info(f"[QQ频道] 【重要】请将以下ID添加到 adapters.ini 的 [qq_guild] 配置中：")
            logger.info(f"  guild_list = {guild_id}")
            logger.info(f"  channel_list = {channel_id}")
            logger.info(f"[QQ频道] =============================")
            
            if self.guild_list and guild_id not in self.guild_list:
                return
            if self.channel_list and channel_id not in self.channel_list:
                return
            
            # 检查是否需要回复
            should_respond = False
            trigger_type = None  # 'at', 'reply', 'keyword'
            
            # 检查@机器人
            if hasattr(message, 'mentions') and message.mentions:
                for mention in message.mentions:
                    if hasattr(message, 'author') and mention.id == message.author.id:
                        should_respond = True
                        trigger_type = 'at'
                        break
                    # 检查是否@了机器人自己（需要从bot获取机器人ID）
                    # 这里简化处理，如果有mentions就响应
                    if mention:
                        should_respond = True
                        trigger_type = 'at'
                        break
            
            # 检查回复
            if message.message_reference:
                # 检查回复的是否是机器人的消息
                # 这里需要查询原消息，暂时简化处理
                should_respond = True
                trigger_type = 'reply'
            
            # 检查关键词
            if not should_respond and self.waking_keywords:
                content = message.content
                if self.require_strict_match:
                    import re
                    for kw in self.waking_keywords:
                        pattern = r'(^|[^\w])' + re.escape(kw) + r'([^\w]|$)'
                        if re.search(pattern, content):
                            should_respond = True
                            trigger_type = 'keyword'
                            break
                else:
                    if any(kw in content for kw in self.waking_keywords):
                        should_respond = True
                        trigger_type = 'keyword'
            
            # 检查冷却时间和概率
            if should_respond:
                import time
                current_time = time.time()
                last_time = self.last_response_time.get(channel_id, 0)
                
                if trigger_type == 'keyword':
                    if self.cooldown_seconds > 0 and (current_time - last_time) < self.cooldown_seconds:
                        logger.info(f"Channel {channel_id} 关键词触发在冷却中，跳过回复")
                        return
                    if random.random() > self.response_probability:
                        logger.info(f"跳过回复 channel {channel_id} due to probability")
                        return
                
                self.last_response_time[channel_id] = current_time
            
            if should_respond:
                # 转换消息格式
                message_list = await self._process_incoming_message(message)
                
                # 创建BotGroupMessage（QQ频道使用频道概念，类似群聊）
                # 获取用户信息
                user_id = str(message.author.id) if hasattr(message, 'author') and hasattr(message.author, 'id') else ""
                user_nickname = str(message.author.username) if hasattr(message, 'author') and hasattr(message.author, 'username') else user_id
                message_id = str(message.id) if hasattr(message, 'id') else ""
                channel_name = str(message.channel_name) if hasattr(message, 'channel_name') else channel_id
                
                message_obj = BotGroupMessage(
                    platform=self.name,
                    adapter_name=self.config['adapter_name'],
                    message_types=self.message_types,
                    group_id=channel_id,  # 使用channel_id作为group_id
                    group_name=channel_name,
                    user_id=user_id,
                    user_nickname=user_nickname,
                    message_id=message_id,
                    self_id="",  # botpy中需要从其他地方获取机器人ID
                    content=message_list,
                    timestamp=int(message.timestamp) if hasattr(message, 'timestamp') else 0
                )
                self.publish(message_obj)
                
        except Exception as e:
            logger.error(f"处理QQ频道消息时出错: {e}")
    
    async def _process_incoming_message(self, message: Any) -> List:
        """将QQ频道消息转换为项目通用消息格式"""
        message_content = []
        
        # 处理文本内容
        if hasattr(message, 'content') and message.content:
            message_content.append(MessageType.Text(str(message.content)))
        
        # 处理@消息
        if hasattr(message, 'mentions') and message.mentions:
            for mention in message.mentions:
                if hasattr(mention, 'id'):
                    message_content.append(MessageType.At(str(mention.id)))
        
        # 处理图片
        if hasattr(message, 'attachments') and message.attachments:
            for attachment in message.attachments:
                if hasattr(attachment, 'content_type') and attachment.content_type and 'image' in attachment.content_type:
                    if hasattr(attachment, 'url'):
                        message_content.append(MessageType.Image(str(attachment.url)))
        
        # 处理回复
        if hasattr(message, 'message_reference') and message.message_reference:
            if hasattr(message.message_reference, 'message_id'):
                message_content.append(MessageType.Reply(str(message.message_reference.message_id)))
        
        return message_content
    
    async def send_group_message(self, channel_id: Union[int, str], send_message_obj: MessageSending) -> Optional[str]:
        """发送频道消息"""
        if not self.bot:
            return None
            
        try:
            message_chain = self._process_outgoing_message(send_message_obj)
            
            # 构建消息内容
            content = ""
            for msg_type in message_chain:
                if isinstance(msg_type, MessageType.Text):
                    content += msg_type.text
                elif isinstance(msg_type, MessageType.At):
                    content += f"<@!{msg_type.pid}>"
            
            # 发送消息
            result = await self.bot.api.post_message(
                channel_id=str(channel_id),
                content=content
            )
            
            return result.id if result else None
        except Exception as e:
            logger.error(f"发送QQ频道消息失败: {e}")
            return None
    
    async def send_direct_message(self, user_id: Union[int, str], send_message_obj: MessageSending) -> Optional[str]:
        """发送私信（QQ频道支持私信）"""
        if not self.bot:
            return None
            
        try:
            message_chain = self._process_outgoing_message(send_message_obj)
            
            content = ""
            for msg_type in message_chain:
                if isinstance(msg_type, MessageType.Text):
                    content += msg_type.text
            
            # QQ频道私信需要先创建私信会话
            result = await self.bot.api.post_dms(
                recipient_id=str(user_id),
                content=content
            )
            
            return result.id if result else None
        except Exception as e:
            logger.error(f"发送QQ频道私信失败: {e}")
            return None
    
    def _process_outgoing_message(self, send_message_obj: MessageSending) -> List:
        """将项目通用消息格式转换为QQ频道消息格式"""
        message_chain = []
        for msg_type in send_message_obj.message_list:
            message_chain.append(msg_type)
        return message_chain

