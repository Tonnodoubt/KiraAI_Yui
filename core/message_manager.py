import asyncio
import xml.etree.ElementTree as ET
from typing import Union, Dict, Any, List
from asyncio import Semaphore
import random
import re

from core.llm_manager import llm_api
from core.logging_manager import get_logger
from core.config_loader import global_config
from core.tts.siliconflow.sftts import generate_speech, speech_to_text
from core.memory_manager import MemoryManager
from core.prompt_manager import PromptManager
from core.services.runtime import get_adapter_by_name
from core.message_filter import message_filter
from utils.common_utils import image_to_base64
from utils.message_utils import BotDirectMessage, BotGroupMessage, MessageSending, MessageType

logger = get_logger("message_processor", "cyan")

config_max_message_interval = int(global_config["bot_config"].get("bot").get("max_message_interval"))
config_max_buffer_messages = int(global_config["bot_config"].get("bot").get("max_buffer_messages"))


class MessageProcessor:
    """Core message processor, responsible for handling all message sending and receiving logic"""
    
    def __init__(self,
                 max_message_interval: int = config_max_message_interval,
                 max_buffer_messages: int = config_max_buffer_messages,
                 max_concurrent_messages: int = 3):
        self.message_processing_semaphore = Semaphore(max_concurrent_messages)
        self.max_message_interval = max_message_interval
        self.max_buffer_messages = max_buffer_messages
        
        # init managers
        self.memory_manager = MemoryManager()
        self.prompt_manager = PromptManager()

        # message buffer
        self.message_buffer: dict[str, Any] = {}
        self.buffer_locks: dict[str, asyncio.Lock] = {}
        
        # 简化日志：初始化不记录
    
    @staticmethod
    def _remove_emoji_keep_kaomoji(text: str) -> str:
        """
        移除文本中的emoji表情，但保留颜文字（kaomoji）
        
        Args:
            text: 原始文本
            
        Returns:
            过滤后的文本
        """
        if not text:
            return text
        
        original_text = text
        original_len = len(text)
        
        # 简化emoji过滤：只移除明确的emoji Unicode范围
        # 使用更保守的范围，避免误删中文字符和标点符号
        emoji_ranges = [
            '\U0001F600-\U0001F64F',  # 表情符号
            '\U0001F300-\U0001F5FF',  # 符号和象形文字
            '\U0001F680-\U0001F6FF',  # 交通和地图符号
            '\U0001F1E0-\U0001F1FF',  # 旗帜（区域指示符）
            '\U00002600-\U000026FF',  # 杂项符号
            '\U00002700-\U000027BF',  # 装饰符号
            '\U0001F900-\U0001F9FF',  # 补充符号和象形文字
            '\U0001FA00-\U0001FA6F',  # 扩展符号
            '\U0001FA70-\U0001FAFF',  # 扩展符号
        ]
        
        # 构建正则表达式模式
        emoji_pattern = re.compile('[' + ''.join(emoji_ranges) + ']+', flags=re.UNICODE)
        
        # 移除emoji
        text = emoji_pattern.sub('', text)
        
        # 移除零宽连接符和变体选择符（这些通常用于组合emoji）
        text = re.sub(r'[\u200D\uFE0F]', '', text)
        
        result = text.strip()
        
        # 如果过滤后文本大幅缩短或为空，记录严重警告并返回原始文本
        if original_len > 0 and len(result) == 0:
            logger.error(f"[Emoji过滤] 严重错误：文本在过滤后变为空！返回原始文本。原始长度: {original_len}, 原始内容: {original_text[:100]}")
            return original_text  # 如果过滤后为空，返回原始文本
        elif len(result) < original_len * 0.5 and original_len > 10:
            logger.warning(f"[Emoji过滤] 警告：文本长度从 {original_len} 减少到 {len(result)}，可能误删了内容。原始: {original_text[:50]}, 过滤后: {result[:50]}")
        
        return result

    def get_session_list_prompt(self) -> str:
        session_list_prompt = ""
        _group_chat_memory = self.memory_manager.group_chat_memory
        _private_chat_memory = self.memory_manager.private_chat_memory
        for session_id in _group_chat_memory:
            session_list_prompt += f"{session_id}\n"
        for session_id in _private_chat_memory:
            session_list_prompt += f"{session_id}\n"
        return session_list_prompt

    @staticmethod
    async def message_format_to_text(message_list: list[MessageType.Text, MessageType.Image, MessageType.At, MessageType.Reply, MessageType.Emoji, MessageType.Sticker, MessageType.Record, MessageType.Notice]):
        """将平台使用标准消息格式封装的消息转换为LLM可以接收的字符串"""
        message_str = ""
        for ele in message_list:
            if isinstance(ele, MessageType.Text):
                message_str += ele.text
            elif isinstance(ele, MessageType.Emoji):
                message_str += f"[Emoji {ele.emoji_id}]"
            elif isinstance(ele, MessageType.At):
                if ele.nickname:
                    message_str += f"[At {ele.pid}(nickname: {ele.nickname})]"
                else:
                    message_str += f"[At {ele.pid}]"
            elif isinstance(ele, MessageType.Image):
                img_desc = await llm_api.desc_img(ele.url)
                message_str += f"[Image {img_desc}]"
            elif isinstance(ele, MessageType.Reply):
                if ele.message_content:
                    message_str += f"[Reply {ele.message_content}]"
                else:
                    message_str += f"[Reply {ele.message_id}]"
            elif isinstance(ele, MessageType.Record):
                record_text = speech_to_text(ele.bs64)
                message_str += f"[Record {record_text}]"
            elif isinstance(ele, MessageType.Notice):
                message_str += f"{ele.text}"
            else:
                pass
        return message_str
    
    async def handle_message(self, msg: Union[BotDirectMessage, BotGroupMessage]):
        """处理消息，带并发控制"""
        async with self.message_processing_semaphore:
            if isinstance(msg, BotDirectMessage):
                await self._handle_direct_message(msg)
            elif isinstance(msg, BotGroupMessage):
                await self._handle_group_message(msg)
            else:
                logger.warning(f"Unknown message type: {type(msg)}")
    
    async def _handle_direct_message(self, msg: BotDirectMessage):
        """process direct message"""
        # 简化日志：私聊消息不单独记录

        dict_key = f"{msg.adapter_name}:dm:{msg.user_id}"
        if dict_key not in self.buffer_locks:
            self.buffer_locks[dict_key] = asyncio.Lock()
        buffer_lock = self.buffer_locks[dict_key]

        async with buffer_lock:
            if dict_key not in self.message_buffer:
                self.message_buffer[dict_key] = []
            self.message_buffer[dict_key].append(msg)
            msg_amount = len(self.message_buffer[dict_key])

        if msg_amount < self.max_buffer_messages:
            await asyncio.sleep(self.max_message_interval)

        if len(self.message_buffer[dict_key]) == msg_amount:
            # print("no new message coming, processing")
            async with buffer_lock:
                message_processing: list[BotDirectMessage] = self.message_buffer[dict_key][:msg_amount]
                self.message_buffer[dict_key] = self.message_buffer[dict_key][msg_amount:]
        else:
            return None

        # 开始处理消息
        formatted_messages_str = ""
        for message in message_processing:
            message_list = message.content
            message.message_str = await self.message_format_to_text(message_list)
            formatted_message = self.prompt_manager.format_user_message(message)
            formatted_messages_str += f"{formatted_message}\n"

        # 使用filter检查消息并计算回复意愿（不带上下文，只看当前消息）
        should_respond, filter_reason, response_willingness = await message_filter.filter_message(formatted_messages_str)
        
        if not should_respond:
            # 简化日志：只记录被拒绝的情况
            logger.debug(f"[过滤] 拒绝: {filter_reason}, 意愿: {response_willingness:.2f}")
            return None  # 不处理此消息，直接返回

        # 获取存在的会话
        session_list = self.get_session_list_prompt()

        # 构建聊天环境信息
        chat_env = {
            "platform": msg.platform,
            "chat_type": 'DirectMessage',
            "self_id": msg.self_id,
            "session_list": session_list
        }
        
        # 获取历史记忆
        private_memory = self.memory_manager.fetch_private_memory(msg.adapter_name, msg.user_id)

        # 获取核心记忆
        core_memory = self.memory_manager.get_core_memory()

        # emoji_dict
        emoji_dict = get_adapter_by_name(msg.adapter_name).emoji_dict

        # 生成系统提示词
        system_prompt = self.prompt_manager.get_system_prompt(chat_env, core_memory, msg.message_types, emoji_dict)
        messages = [{"role": "system", "content": system_prompt}]

        # 生成工具提示词
        tool_prompt = self.prompt_manager.get_tool_prompt(chat_env, core_memory, msg.message_types, emoji_dict)
        
        private_memory.append({"role": "user", "content": formatted_messages_str})
        new_memory_chunk = [{"role": "user", "content": formatted_messages_str}]
        messages.extend(private_memory)
        
        # 获取工具提示词并调用LLM
        response, tool_messages = await llm_api.chat_with_tools(messages, tool_prompt)
        # logger.info(f"LLM响应: {response}")

        message_ids = await self.send_xml_messages(f"{msg.adapter_name}:dm:{msg.user_id}", response)
        
        # 添加消息ID到响应中
        response_with_ids = self._add_message_ids(response, message_ids)
        # print(response_with_ids)
        # 简化日志：私聊回复不单独记录
        
        # 更新记忆
        if tool_messages:
            for tool_message in tool_messages:
                new_memory_chunk.append(tool_message)
        
        new_memory_chunk.append({"role": "assistant", "content": response_with_ids})
        self.memory_manager.update_private_memory(msg.adapter_name, msg.user_id, new_memory_chunk)
    
    async def _handle_group_message(self, msg: BotGroupMessage):
        """process group message"""
        # 简化日志：群聊消息不单独记录
        dict_key = f"{msg.adapter_name}:gm:{msg.group_id}"
        if dict_key not in self.buffer_locks:
            self.buffer_locks[dict_key] = asyncio.Lock()
        buffer_lock = self.buffer_locks[dict_key]

        async with buffer_lock:
            if dict_key not in self.message_buffer:
                self.message_buffer[dict_key] = []
            self.message_buffer[dict_key].append(msg)
            msg_amount = len(self.message_buffer[dict_key])

        if msg_amount < self.max_buffer_messages:
            await asyncio.sleep(self.max_message_interval)
        if len(self.message_buffer[dict_key]) == msg_amount:
            # print("no new message coming, processing")
            async with buffer_lock:
                message_processing: list[BotGroupMessage] = self.message_buffer[dict_key][:msg_amount]
                self.message_buffer[dict_key] = self.message_buffer[dict_key][msg_amount:]
        else:
            return None

        # 开始处理消息
        formatted_messages_str = ""
        for message in message_processing:
            message_list = message.content
            message.message_str = await self.message_format_to_text(message_list)
            formatted_message = self.prompt_manager.format_user_message(message)
            formatted_messages_str += f"{formatted_message}\n"

        # 使用filter检查消息并计算回复意愿（不带上下文，只看当前消息）
        should_respond, filter_reason, response_willingness = await message_filter.filter_message(formatted_messages_str)
        
        if not should_respond:
            # 简化日志：只记录被拒绝的情况
            logger.debug(f"[过滤] 拒绝: {filter_reason}, 意愿: {response_willingness:.2f}")
            return None  # 不处理此消息，直接返回

        # 获取存在的会话
        session_list = self.get_session_list_prompt()

        # 构建聊天环境信息
        chat_env = {
            "platform": msg.platform,
            "chat_type": 'GroupMessage',
            "self_id": msg.self_id,
            "session_list": session_list
        }
        
        # 获取群组ID
        group_id_str = getattr(msg, "group_id", None)
        
        # 获取历史记忆
        group_memory = self.memory_manager.fetch_group_memory(msg.adapter_name, group_id_str)

        # 获取核心记忆
        core_memory = self.memory_manager.get_core_memory()

        # emoji_dict
        emoji_dict = get_adapter_by_name(msg.adapter_name).emoji_dict

        # 生成系统提示词
        system_prompt = self.prompt_manager.get_system_prompt(chat_env, core_memory, msg.message_types, emoji_dict)
        messages = [{"role": "system", "content": system_prompt}]

        # 生成工具提示词
        tool_prompt = self.prompt_manager.get_tool_prompt(chat_env, core_memory, msg.message_types, emoji_dict)
        
        group_memory.append({"role": "user", "content": formatted_messages_str})
        new_memory_chunk = [{"role": "user", "content": formatted_messages_str}]
        messages.extend(group_memory)
        
        # 按群加锁，防止同群并发
        group_lock = self.memory_manager.get_group_lock(group_id_str)
        
        # 获取工具提示词并调用LLM
        async with group_lock:
            try:
                response, tool_messages = await llm_api.chat_with_tools(messages, tool_prompt)
            except Exception as e:
                logger.error(f"[LLM失败] {e}")
                return

        if not response or not response.strip():
            logger.warning(f"[LLM空回复] 跳过发送")
            return

        try:
            message_ids = await self.send_xml_messages(f"{msg.adapter_name}:gm:{msg.group_id}", response)
        except Exception as e:
            logger.error(f"[发送失败] {e}")
            return
        
        # 添加消息ID到响应中
        response_with_ids = self._add_message_ids(response, message_ids)
        # 简化日志：回复内容已在适配器的回复成功日志中显示，这里不再单独显示
        
        # 更新记忆
        if tool_messages:
            for tool_message in tool_messages:
                new_memory_chunk.append(tool_message)
        
        new_memory_chunk.append({"role": "assistant", "content": response_with_ids})
        async with group_lock:
            self.memory_manager.update_group_memory(msg.adapter_name, group_id_str, new_memory_chunk)
    
    async def _send_response_messages(self, msg: Union[BotDirectMessage, BotGroupMessage], response: str) -> List[str]:
        """send response message"""
        message_ids = []
        resp_list = self._parse_and_generate_messages(response)
        
        for message_list in resp_list:
            message_obj = MessageSending(message_list)
            
            # 根据消息类型选择发送方法
            if isinstance(msg, BotDirectMessage):
                message_id = await get_adapter_by_name(msg.adapter_name).send_direct_message(msg.user_id, message_obj)
            elif isinstance(msg, BotGroupMessage):
                message_id = await get_adapter_by_name(msg.adapter_name).send_group_message(msg.group_id, message_obj)
            else:
                message_id = None
            
            if not message_id:
                message_id = ''
            message_ids.append(message_id)
            
            # 添加随机延迟避免频率限制
            await asyncio.sleep(random.uniform(0.8, 1.5))
        
        return message_ids

    async def send_xml_messages(self, target: str, xml: str) -> List[str]:
        """
        send message via session id & xml data
        :param target: adapter_name:session_type:session_id
        :param xml: xml string
        :return: message id(s)
        """
        try:
            message_ids = []
            resp_list = self._parse_and_generate_messages(xml)
            logger.info(f"[发送消息] 解析XML成功，生成 {len(resp_list)} 条消息")

            parts = target.split(":")
            if len(parts) != 3:
                raise ValueError("target 必须是 <adapter>:<dm|gm>:<id> 格式")
            adapter_name, chat_type, pid = parts[0], parts[1], parts[2]
            logger.info(f"[发送消息] 目标: {adapter_name}:{chat_type}:{pid}")

            for i, message_list in enumerate(resp_list, 1):
                try:
                    message_obj = MessageSending(message_list)
                    logger.debug(f"[发送消息] 正在发送第 {i}/{len(resp_list)} 条消息")

                    # 根据消息类型选择发送方法
                    if chat_type == "dm":
                        message_id = await get_adapter_by_name(adapter_name).send_direct_message(pid, message_obj)
                    elif chat_type == "gm":
                        message_id = await get_adapter_by_name(adapter_name).send_group_message(pid, message_obj)
                    else:
                        logger.warning(f"[发送消息] 未知的消息类型: {chat_type}")
                        message_id = None

                    if not message_id:
                        logger.warning(f"[发送消息] 第 {i} 条消息发送失败，返回的message_id为空")
                        message_id = ''
                    # 简化日志：不记录每条消息的发送成功
                    message_ids.append(message_id)

                    # 添加随机延迟避免频率限制
                    await asyncio.sleep(random.uniform(0.8, 1.5))
                except Exception as e:
                    logger.error(f"[发送消息] 发送第 {i} 条消息时出错: {e}")
                    import traceback
                    logger.debug(traceback.format_exc())
                    message_ids.append('')

            # 简化日志：不记录发送完成信息
            return message_ids
        except Exception as e:
            logger.error(f"[发送消息] send_xml_messages执行失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return []
    
    def _parse_and_generate_messages(self, xml_data: str) -> List[List]:
        """parse xml generated by llm & generate MessageType list"""
        try:
            # 先尝试转义XML中的特殊字符（如果它们不在标签中）
            # 使用CDATA或者转义来处理包含< >的文本内容
            import re
            # 匹配<text>标签内的内容，转义其中的<和>
            def escape_text_content(match):
                text_content = match.group(1)
                # 转义<和>（但保留XML标签）
                # 注意：先转义&，避免重复转义
                text_content = text_content.replace('&', '&amp;')
                text_content = text_content.replace('<', '&lt;')
                text_content = text_content.replace('>', '&gt;')
                return f'<text>{text_content}</text>'
            
            # 如果XML中包含未转义的<或>在text标签内，先转义它们
            xml_data_escaped = re.sub(r'<text>(.*?)</text>', escape_text_content, xml_data, flags=re.DOTALL)
            
            # 只在转义后XML发生变化时记录（减少日志量）
            if xml_data_escaped != xml_data:
                logger.debug(f"[XML解析] XML已转义（原始长度: {len(xml_data)}, 转义后长度: {len(xml_data_escaped)}）")
            
            root = ET.fromstring(f"<root>{xml_data_escaped}</root>")
            message_list = []
            last_text_index = -1  # 记录最后一条包含文本的消息索引

            # 检查是否有msg标签，如果没有，说明LLM返回的是纯文本，需要包装成msg标签
            msg_elements = root.findall("msg")
            if not msg_elements:
                # LLM返回的是纯文本，没有XML标签，直接作为文本处理
                # 提取所有文本内容
                text_content = ''.join(root.itertext()).strip()
                if text_content:
                    logger.warning(f"[XML解析] LLM返回的是纯文本（无XML标签），已自动包装为文本消息。内容: {text_content[:100]}")
                    # 包装成msg标签格式
                    xml_data_escaped = f"<msg><text>{text_content}</text></msg>"
                    root = ET.fromstring(f"<root>{xml_data_escaped}</root>")
                    msg_elements = root.findall("msg")

            for msg in msg_elements:
                message_elements = []
                has_text = False
                for child in msg:
                    tag = child.tag
                    # 获取文本内容（使用itertext()获取所有文本节点，包括子元素的文本）
                    # 这样可以确保获取完整的文本内容，即使有嵌套结构
                    all_text_parts = list(child.itertext())
                    if all_text_parts:
                        value = ''.join(all_text_parts).strip()
                    else:
                        value = ""
                    
                    # 如果text标签包含子元素，记录警告（虽然text标签通常不应该有子元素）
                    if len(list(child)) > 0:
                        logger.warning(f"[XML解析] text标签包含子元素，已使用itertext()提取所有文本。标签: {tag}")
                    # 反转义XML实体
                    if value:
                        value = value.replace('&lt;', '<')
                        value = value.replace('&gt;', '>')
                        value = value.replace('&amp;', '&')
                        # 只在提取的文本为空或异常时记录（减少日志量）
                        if not value or len(value) == 0:
                            logger.warning(f"[XML解析] 从<{tag}>标签提取的文本为空")
                    
                    # build MessageType object
                    if tag == "text":
                        has_text = True
                        # 确保文本以"喵～"结尾，移除末尾标点符号后添加
                        if value:
                            # 记录原始文本（用于调试）
                            original_value = value
                            value = value.rstrip()
                            import string
                            punctuation = "。！？，、；："
                            
                            # 移除emoji表情，保留颜文字
                            value_before_filter = value
                            value = self._remove_emoji_keep_kaomoji(value)
                            
                            # 只在过滤后文本发生变化时记录（减少日志量）
                            if value != value_before_filter:
                                logger.warning(f"[XML解析] 文本在过滤后发生变化！过滤前（长度: {len(value_before_filter)}）: {value_before_filter[:100]}, 过滤后（长度: {len(value)}）: {value[:100] if value else '(空字符串)'}")
                            
                            # 检查是否只包含符号、表情或颜文字，没有实际文字内容
                            # 移除所有标点符号、括号、方括号等，检查是否还有汉字、字母或数字
                            import re
                            text_only = re.sub(r'[【】\[\]()（）≧∇≦～~！!？?。.，,、；;：:""''""''\s]', '', value)
                            # 检查是否包含汉字、字母或数字
                            has_actual_text = bool(re.search(r'[\u4e00-\u9fa5a-zA-Z0-9]', text_only))
                            
                            if not has_actual_text and text_only:
                                # 如果只有符号、表情或颜文字，添加默认文字
                                logger.warning(f"[消息过滤] 检测到纯符号/表情回复，已添加默认文字。原始内容: {value}")
                                value = "好呀" + value
                            
                            # 检查是否已经以"喵～"或"喵~"结尾（全角和半角都要检查）
                            if value.endswith("喵～"):
                                # 如果以全角"喵～"结尾，先移除"喵～"
                                value = value[:-2].rstrip()
                                # 检查"喵～"前面是否有标点符号，如果有就移除
                                while value and (value[-1] in punctuation or value[-1] in string.punctuation):
                                    value = value[:-1].rstrip()
                                # 添加"喵～"
                                value = value + "喵～"
                            elif value.endswith("喵~"):
                                # 如果已经以半角"喵~"结尾，检查前面是否有标点符号
                                temp_value = value[:-2].rstrip()  # 临时移除"喵~"
                                # 如果"喵~"前面是标点符号，移除标点符号
                                while temp_value and (temp_value[-1] in punctuation or temp_value[-1] in string.punctuation):
                                    temp_value = temp_value[:-1].rstrip()
                                # 重新添加"喵～"
                                value = temp_value + "喵～"
                            else:
                                # 如果没有以"喵～"或"喵~"结尾，移除末尾标点符号后添加
                                while value and (value[-1] in punctuation or value[-1] in string.punctuation):
                                    value = value[:-1].rstrip()
                                # 添加"喵～"
                                value = value + "喵～"
                            
                            # 再次验证：确保最终文本包含实际文字内容
                            final_text_check = re.sub(r'[【】\[\]()（）≧∇≦～~！!？?。.，,、；;：:""''""''\s]', '', value)
                            if not bool(re.search(r'[\u4e00-\u9fa5a-zA-Z0-9]', final_text_check)):
                                # 如果最终还是没有文字内容，使用默认回复
                                logger.warning(f"[消息过滤] 最终验证失败，使用默认回复。内容: {value}")
                                value = "好呀喵～"
                                
                        message_elements.append(MessageType.Text(value))
                    elif tag == "emoji":
                        # 直接忽略表情包，不添加（不记录日志以提升性能）
                        continue
                    elif tag == "sticker":
                        # 表情包功能已禁用，直接跳过
                        logger.info(f"检测到sticker标签，但表情包功能已禁用，已忽略: {value}")
                        continue
                    elif tag == "at":
                        message_elements.append(MessageType.At(value))
                    elif tag == "img":
                        img_url = llm_api.generate_img(value)
                        message_elements.append(MessageType.Image(img_url))
                    elif tag == "reply":
                        message_elements.append(MessageType.Reply(value))
                    elif tag == "record":
                        try:
                            record_bs64 = generate_speech(value)
                            message_elements.append(MessageType.Record(record_bs64))
                        except Exception as e:
                            logger.error(f"an error occurred while generating voice message: {e}")
                            message_elements.append(MessageType.Text(f"<record>{value}</record>"))
                    elif tag == "poke":
                        message_elements.append(MessageType.Poke(value))
                    else:
                        # 忽略未知标签，记录警告但不中断处理
                        logger.warning(f"未知的XML标签，已忽略: {tag}")
                        continue
                
                if message_elements:
                    message_list.append(message_elements)
                    if has_text:
                        last_text_index = len(message_list) - 1

            # 限制消息数量：最多只发送2条消息
            if len(message_list) > 2:
                logger.warning(f"LLM生成了{len(message_list)}条消息，已限制为2条")
                message_list = message_list[:2]
                # 更新最后文本消息索引
                if last_text_index >= 2:
                    # 重新查找最后一条包含文本的消息
                    last_text_index = -1
                    for i in range(len(message_list) - 1, -1, -1):
                        for element in message_list[i]:
                            if isinstance(element, MessageType.Text):
                                last_text_index = i
                                break
                        if last_text_index >= 0:
                            break

            # 在最后一条包含文本的消息末尾添加感谢支持的句子
            if last_text_index >= 0:
                for i, element in enumerate(message_list[last_text_index]):
                    if isinstance(element, MessageType.Text):
                        # 在文本末尾添加感谢支持的句子
                        current_text = element.text
                        # 如果已经以"喵～"或"喵~"结尾，在"喵～"之前添加感谢支持的句子
                        if current_text.endswith("喵～"):
                            current_text = current_text[:-2]  # 移除"喵～"
                            current_text = current_text.rstrip()
                            # 添加感谢支持的句子和"喵～"（在"喵～"前添加逗号，使语句更自然）
                            if current_text and not current_text.endswith(("，", "。", "！", "？", "、", "；", "：")):
                                current_text = current_text + "，"
                            current_text = current_text + "感谢对「猫娘计划」的支持喵～"
                        elif current_text.endswith("喵~"):
                            current_text = current_text[:-2]  # 移除"喵~"
                            current_text = current_text.rstrip()
                            # 添加感谢支持的句子和"喵～"（在"喵～"前添加逗号，使语句更自然）
                            if current_text and not current_text.endswith(("，", "。", "！", "？", "、", "；", "：")):
                                current_text = current_text + "，"
                            current_text = current_text + "感谢对「猫娘计划」的支持喵～"
                        else:
                            # 如果没有"喵～"，先添加"喵～"，然后添加感谢支持的句子
                            current_text = current_text.rstrip()
                            if current_text and not current_text.endswith(("，", "。", "！", "？", "、", "；", "：")):
                                current_text = current_text + "，"
                            current_text = current_text + "感谢对「猫娘计划」的支持喵～"
                        # 更新文本内容
                        message_list[last_text_index][i] = MessageType.Text(current_text)
                        break

            return message_list
        except Exception as e:
            logger.error(f"Error parsing message: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            # XML解析失败时，尝试提取text标签中的内容
            import re
            text_matches = re.findall(r'<text>(.*?)</text>', xml_data, re.DOTALL)
            if text_matches:
                # 提取所有text标签中的内容并合并
                extracted_text = ' '.join([match.strip() for match in text_matches if match.strip()])
                if extracted_text:
                    # 反转义
                    extracted_text = extracted_text.replace('&lt;', '<')
                    extracted_text = extracted_text.replace('&gt;', '>')
                    extracted_text = extracted_text.replace('&amp;', '&')
                    logger.warning(f"[XML解析失败] 已从XML中提取文本内容: {extracted_text[:50]}")
                    return [[MessageType.Text(extracted_text)]]
            # 如果无法提取，返回默认回复
            logger.error(f"[XML解析失败] 无法从XML中提取有效文本，使用默认回复。原始XML: {xml_data[:200]}")
            return [[MessageType.Text("好呀喵～")]]

    @staticmethod
    def _add_message_ids(xml_data: str, message_ids: List[str]) -> str:
        """为XML响应添加消息ID"""
        try:
            root = ET.fromstring(f"<root>{xml_data}</root>")

            for i, msg in enumerate(root.findall("msg")):
                if i < len(message_ids):
                    msg.set("message_id", message_ids[i])

            return ET.tostring(root, encoding='unicode', method='xml')[6:-7]

        except Exception as e:
            logger.error(f"Error adding message IDs: {str(e)}")
            return xml_data


# global message processor
message_processor = MessageProcessor()
