"""
消息过滤器模块
用于在发送给主LLM之前，筛选不合适的内容并计算回复意愿
"""
import json
import re
from typing import Dict, Any, Tuple, Optional
from core.llm_client import LLMClient
from core.config_loader import global_config
from core.logging_manager import get_logger

logger = get_logger("message_filter", "yellow")


class MessageFilter:
    """消息过滤器，用于内容检查和回复意愿计算"""
    
    def __init__(self):
        # 获取filter模型配置
        try:
            filter_config = global_config["models"].get("filter_llm", {})
            self.filter_provider = filter_config.get("provider", "siliconflow")
            self.filter_model = filter_config.get("model", "Qwen/Qwen3-8B")
            
            # 获取filter模型的API配置
            provider_config = global_config["providers"].get(self.filter_provider, {})
            self.filter_api_key = provider_config.get("api_key", "")
            self.filter_base_url = provider_config.get("base_url", "")
        except Exception as e:
            logger.warning(f"Filter配置读取失败，使用默认值: {e}")
            self.filter_provider = "siliconflow"
            self.filter_model = "Qwen/Qwen3-8B"
            self.filter_api_key = ""
            self.filter_base_url = ""
        
        # 初始化filter LLM客户端
        self.filter_client = None
        if self.filter_api_key:
            try:
                from openai import AsyncOpenAI
                self.filter_client = AsyncOpenAI(
                    api_key=self.filter_api_key,
                    base_url=self.filter_base_url
                )
                logger.info(f"Filter模型已初始化: {self.filter_model} from {self.filter_provider}")
            except Exception as e:
                logger.error(f"Filter模型初始化失败: {e}")
        else:
            logger.warning("Filter模型API key未配置，将使用基础关键词过滤")
    
    def _extract_text_from_message(self, message_str: str) -> Tuple[str, bool]:
        """从消息字符串中提取纯文本内容，并检测是否@了机器人
        
        Returns:
            Tuple[str, bool]: (提取的文本, 是否@了机器人)
        """
        # 检测是否@了机器人（保留@标记用于判断）
        has_at_bot = bool(re.search(r'\[At\s+\d+\(nickname:', message_str) or 
                                   re.search(r'\[At\s+all\]', message_str, re.IGNORECASE))
        
        # 移除XML标签和特殊标记
        text = message_str
        # 移除常见的消息标记（但保留@信息用于判断）
        text = re.sub(r'\[Reply.*?\]', '', text)
        text = re.sub(r'\[Image.*?\]', '', text)
        text = re.sub(r'\[Emoji.*?\]', '', text)
        text = re.sub(r'\[Poke.*?\]', '', text)
        text = re.sub(r'\[System.*?\]', '', text)
        # 最后移除@标记（但已经记录了has_at_bot）
        text = re.sub(r'\[At.*?\]', '', text)
        return text.strip(), has_at_bot
    
    def _basic_keyword_filter(self, text: str) -> Tuple[bool, str]:
        """基础关键词过滤（当LLM不可用时使用）
        
        这是一个纯粹的内容安全过滤器，不涉及人设判断。
        只检查内容是否包含不合适的关键词（如政治话题、哈基米等）。
        """
        # 政治相关关键词列表（严格过滤）
        political_keywords = [
            '政治', '政府', '政党', '选举', '投票', '总统', '总理', '主席',
            '政策', '法律', '法规', '宪法', '议会', '国会', '人大', '政协',
            '意识形态', '主义', '制度', '体制', '政权', '执政', '反对派',
            '抗议', '示威', '革命', '政变', '镇压', '审查', '言论自由',
            '民主', '专制', '独裁', '自由', '人权', '公民权利',
            '领导人', '官员', '政治人物', '政治事件', '政治观点', '政治立场',
            '政治制度', '政治体制', '政治改革', '政治运动'
        ]
        
        # 哈基米相关关键词列表（严格过滤）
        hakimi_keywords = [
            '哈基米', 'hachimi', 'hachimimi', '哈基咪', '哈吉米'
        ]
        
        # 女权/女拳等争议话题关键词列表（严格过滤）
        feminism_keywords = [
            '女权', '女拳', '女权主义', '女拳主义', 'feminism', '女权运动',
            '女权组织', '女权活动', '女权话题', '女权讨论', '女权争议',
            '女权问题', '女权观点', '女权立场', '女权支持', '女权反对',
            '平权', '性别平等', '性别争议', '性别话题', '性别对立',
            '男女对立', '性别战争', '性别冲突', '性别矛盾'
        ]
        
        text_lower = text.lower()
        
        # 检查政治相关关键词
        for keyword in political_keywords:
            if keyword in text_lower:
                return False, f"检测到政治相关关键词: {keyword}"
        
        # 检查哈基米相关关键词
        for keyword in hakimi_keywords:
            if keyword in text_lower:
                return False, f"检测到哈基米相关关键词: {keyword}"
        
        # 检查女权/女拳相关关键词
        for keyword in feminism_keywords:
            if keyword in text_lower:
                return False, f"检测到女权/女拳相关关键词: {keyword}"
        
        return True, "通过基础过滤"
    
    async def filter_message(self, message_str: str) -> Tuple[bool, str, float]:
        """
        过滤消息并计算回复意愿
        
        Args:
            message_str: 消息文本内容
            
        Returns:
            Tuple[bool, str, float]:
                - should_respond: 是否应该回复
                - reason: 原因说明
                - response_willingness: 回复意愿分数 (0.0-1.0)
        """
        # 提取纯文本，并检测是否@了机器人
        text, has_at_bot = self._extract_text_from_message(message_str)
        
        # 先检查内容安全性（无论是否@了机器人，都要先检查内容安全性）
        is_safe_basic, reason_basic = self._basic_keyword_filter(text)
        if not is_safe_basic:
            # 内容不安全，直接拒绝，不回复
            return False, f"内容不安全: {reason_basic}", 0.0
        
        if not text:
            return False, "消息内容为空", 0.0
        
        # 如果filter LLM可用，使用LLM进行智能过滤
        if self.filter_client:
            try:
                return await self._llm_filter(text, has_at_bot)
            except Exception as e:
                logger.error(f"LLM过滤失败，使用基础过滤: {e}")
                is_safe, reason = self._basic_keyword_filter(text)
                willingness = 0.9 if has_at_bot and is_safe else (0.5 if is_safe else 0.0)
                return is_safe, reason, willingness
        
        # 否则使用基础关键词过滤
        is_safe, reason = self._basic_keyword_filter(text)
        willingness = 0.5 if is_safe else 0.0
        return is_safe, reason, willingness
    
    async def _llm_filter(self, text: str, has_at_bot: bool = False) -> Tuple[bool, str, float]:
        """使用LLM进行智能过滤和回复意愿计算
        
        注意：这是一个纯粹的内容安全过滤器，不涉及人设判断。
        只关注内容是否安全合适，如果内容不合适（如政治话题），直接不回复。
        """
        filter_prompt = f"""你是一个严格的内容安全过滤器，只负责判断消息内容是否安全合适，不涉及任何角色人设判断。

消息内容：
{text}

请严格按照以下JSON格式输出你的判断结果：
{{
    "is_safe": true/false,  // 消息是否安全合适（不涉及政治、敏感话题等）
    "should_respond": true/false,  // 是否应该回复这条消息（基于内容安全性，不涉及人设）
    "response_willingness": 0.0-1.0,  // 回复意愿分数，0.0表示完全不想回复，1.0表示非常想回复
    "reason": "原因说明"  // 简要说明判断原因
}}

【核心判断规则 - 必须严格遵守】：
1. **内容安全性优先**：如果消息涉及任何政治话题、政治人物、政治事件、政治观点、政治制度、政治立场等，is_safe必须为false，should_respond必须为false，response_willingness必须为0.0。不要回复，直接拒绝。
2. **哈基米话题拒绝**：如果消息涉及"哈基米"、"hachimi"、"hachimimi"、"哈基咪"、"哈吉米"等相关话题，is_safe必须为false，should_respond必须为false，response_willingness必须为0.0。不要回复，直接拒绝。
3. **女权/女拳等争议话题拒绝**：如果消息涉及"女权"、"女拳"、"女权主义"、"女拳主义"、"feminism"、"平权"、"性别平等"、"性别争议"、"性别对立"、"男女对立"、"性别战争"等容易引起争议的性别相关话题，is_safe必须为false，should_respond必须为false，response_willingness必须为0.0。不要回复，直接拒绝。
4. **敏感内容拒绝**：如果消息涉及敏感、不当、违法、暴力、色情等内容，is_safe必须为false，should_respond必须为false，response_willingness必须为0.0。不要回复，直接拒绝。
3. **不涉及人设**：你的判断只关注内容安全性，不要考虑机器人的人设、性格、回复风格等因素。如果内容不合适，无论是否@了机器人，都应该拒绝回复。
4. **安全内容判断**：如果消息只是普通聊天、问候、日常交流、技术讨论等安全内容，is_safe应该为true。
5. **回复意愿计算**：只有在is_safe为true时，才计算response_willingness：
   - @机器人的安全消息：0.8-1.0
   - 明确提到机器人的安全消息：0.7-0.9
   - 表达期待、支持、已加入愿望单等积极正面的消息：0.8-1.0（这类消息应该高优先级回复）
   - 普通群聊安全消息：0.3-0.6
   - 其他安全消息：0.1-0.5

【重要】：
- 如果is_safe为false，必须设置should_respond为false，response_willingness为0.0
- 不要因为@了机器人就降低对内容安全性的判断标准
- 政治相关内容必须严格拒绝，不要回复

只输出JSON，不要输出任何其他内容。"""

        try:
            response = await self.filter_client.chat.completions.create(
                model=self.filter_model,
                messages=[
                    {"role": "system", "content": "你是一个严格的消息过滤器，只输出JSON格式的判断结果。"},
                    {"role": "user", "content": filter_prompt}
                ],
                max_tokens=200,
                temperature=0.1  # 低温度确保判断稳定
            )
            
            content = response.choices[0].message.content.strip()
            
            # 尝试提取JSON
            json_match = re.search(r'\{[^}]+\}', content, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                result = json.loads(json_str)
                
                is_safe = result.get("is_safe", False)
                should_respond = result.get("should_respond", False)
                willingness = float(result.get("response_willingness", 0.0))
                reason = result.get("reason", "LLM过滤判断")
                
                # 如果内容不安全，强制不回复（最高优先级）
                if not is_safe:
                    should_respond = False
                    willingness = 0.0
                    reason = f"内容不安全: {reason}"
                
                # 确保逻辑一致性：不安全的内容必须不回复
                if not is_safe and should_respond:
                    logger.warning(f"Filter判断不一致：is_safe={is_safe}但should_respond={should_respond}，强制设为False")
                    should_respond = False
                    willingness = 0.0
                
                return should_respond, reason, willingness
            else:
                logger.warning(f"Filter LLM返回格式不正确: {content}")
                # 回退到基础过滤
                is_safe, reason = self._basic_keyword_filter(text)
                willingness = 0.5 if is_safe else 0.0
                return is_safe, reason, willingness
                
        except json.JSONDecodeError as e:
            logger.error(f"Filter LLM返回的JSON解析失败: {e}")
            # 回退到基础过滤
            is_safe, reason = self._basic_keyword_filter(text)
            willingness = 0.5 if is_safe else 0.0
            return is_safe, reason, willingness
        except Exception as e:
            logger.error(f"Filter LLM调用失败: {e}")
            # 回退到基础过滤
            is_safe, reason = self._basic_keyword_filter(text)
            willingness = 0.5 if is_safe else 0.0
            return is_safe, reason, willingness


# 全局filter实例
message_filter = MessageFilter()

