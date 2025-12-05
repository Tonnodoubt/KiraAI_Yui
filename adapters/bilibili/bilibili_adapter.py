"""
B站适配器 - 符合KiraAI架构
实现B站私信和评论的自动回复功能
"""
import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Set, Union
from datetime import datetime
from collections import defaultdict

from bilibili_api import user, video, Credential
from bilibili_api.comment import Comment, CommentResourceType, send_comment, get_comments, get_comments_lazy, OrderType
from bilibili_api.exceptions import ResponseCodeException

# 注意：bilibili-api 17.3.0 版本可能没有message模块
# 私信功能暂时禁用，等待API更新或使用其他方式实现
try:
    from bilibili_api.message import get_sessions, get_messages, send_message
    MESSAGE_MODULE_AVAILABLE = True
except ImportError:
    MESSAGE_MODULE_AVAILABLE = False
    # 定义占位函数避免错误
    def get_sessions(*args, **kwargs):
        raise NotImplementedError("bilibili_api.message模块不可用，私信功能暂不支持")
    def get_messages(*args, **kwargs):
        raise NotImplementedError("bilibili_api.message模块不可用，私信功能暂不支持")
    def send_message(*args, **kwargs):
        raise NotImplementedError("bilibili_api.message模块不可用，私信功能暂不支持")

from core.logging_manager import get_logger
from utils.adapter_utils import IMAdapter
from utils.message_utils import BotDirectMessage, BotGroupMessage, MessageSending, MessageType

logger = get_logger("bilibili_adapter", "cyan")


class BilibiliAdapter(IMAdapter):
    """B站适配器，负责监听和回复B站私信和评论"""
    
    def __init__(self, config: Dict[str, Any], loop: asyncio.AbstractEventLoop, event_bus: asyncio.Queue):
        super().__init__(config, loop, event_bus)
        self.name: str = "Bilibili"
        
        # 配置
        self.enabled = config.get('enabled', 'false').lower() == 'true'
        self.sessdata = config.get('sessdata', '')
        self.bili_jct = config.get('bili_jct', '')
        self.dedeuserid = config.get('dedeuserid', '')
        # 可选的额外凭证字段（用于发送评论等操作）
        self.buvid3 = config.get('buvid3', '')
        self.ac_time_value = config.get('ac_time_value', '')
        
        # 功能开关
        self.reply_private_messages = config.get('reply_private_messages', 'true').lower() == 'true'
        self.reply_comments = config.get('reply_comments', 'true').lower() == 'true'
        
        # 频率限制
        self.reply_interval = int(config.get('reply_interval', '5'))
        self.max_replies_per_hour = int(config.get('max_replies_per_hour', '20'))
        
        # 评论检查配置
        self.max_comments_per_check = int(config.get('max_comments_per_check', '30'))  # 每次检查的评论数量
        self.comment_time_window_minutes = int(config.get('comment_time_window_minutes', '20'))  # 时间窗口（分钟）
        
        # 监听目标
        monitor_videos = config.get('monitor_video_ids', '')
        monitor_dynamics = config.get('monitor_dynamic_ids', '')
        self.monitor_video_ids = [v.strip() for v in monitor_videos.split(',') if v.strip()] if monitor_videos else []
        self.monitor_dynamic_ids = [d.strip() for d in monitor_dynamics.split(',') if d.strip()] if monitor_dynamics else []
        
        # 消息类型支持
        self.message_types = [MessageType.Text, MessageType.Image, MessageType.Reply]
        
        # B站凭证
        self.credential: Optional[Credential] = None
        if self.enabled and self.sessdata and self.bili_jct and self.dedeuserid:
            try:
                # 构建凭证参数字典
                credential_params = {
                    'sessdata': self.sessdata,
                    'bili_jct': self.bili_jct,
                    'dedeuserid': self.dedeuserid
                }
                # 添加可选的额外字段（如果存在）
                if self.buvid3:
                    credential_params['buvid3'] = self.buvid3
                if self.ac_time_value:
                    credential_params['ac_time_value'] = self.ac_time_value
                
                self.credential = Credential(**credential_params)
                logger.info("B站凭证初始化成功")
            except Exception as e:
                logger.error(f"B站凭证初始化失败: {e}")
                self.credential = None
        
        # 消息去重
        self.processed_messages: Set[str] = set()
        self.processed_comments: Set[int] = set()
        
        # 记录启动时已存在的评论ID（用于过滤历史评论）
        self.initial_comment_ids: Set[int] = set()
        self.initial_comment_ids_loaded: bool = False
        
        # 频率限制
        self.reply_timestamps: List[float] = []
        self.last_reply_time: Dict[str, float] = defaultdict(float)
        
        # 运行标志
        self.running = False
        self.monitor_tasks: List[asyncio.Task] = []
        
        # 评论ID到视频AID的映射（用于回复评论）
        # key: 评论ID (rpid), value: 视频AID (oid)
        self.comment_to_video: Dict[int, int] = {}
        
        # 视频AID到最近收到的评论ID的映射（用于自动回复）
        # key: 视频AID (oid), value: 最近收到的评论ID (rpid)
        self.video_to_latest_comment: Dict[int, int] = {}
        
        # 机器人发送的评论ID集合（用于检测是否有用户回复了机器人的评论）
        # 当机器人回复评论时，会记录被回复的评论ID和机器人发送的评论ID
        self.bot_sent_comments: Set[int] = set()  # 机器人发送的评论ID
        self.comment_to_parent: Dict[int, int] = {}  # 评论ID到父评论ID的映射
        
        # 已经回复过的评论ID集合（用于避免重复回复）
        self.replied_comments: Set[int] = set()  # 已经回复过的评论ID
        
        # 持久化文件路径
        self.data_dir = "data/bilibili"
        self.bot_comments_file = os.path.join(self.data_dir, "bot_sent_comments.json")
        self.replied_comments_file = os.path.join(self.data_dir, "replied_comments.json")
        self.bot_comments_log_file = os.path.join(self.data_dir, "bot_sent_comments.log")
        # 评论和回复记录文件
        self.comments_log_file = os.path.join(self.data_dir, "comments_log.txt")
        
        # 确保数据目录存在
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 加载持久化的数据
        self._load_persistent_data()
        
        # 记录初始状态到日志文件
        self._log_bot_comments("初始化")
        
        # 初始化评论日志文件
        self._init_comments_log_file()
    
    def _load_persistent_data(self):
        """从文件加载持久化的数据"""
        # 加载机器人发送的评论ID
        if os.path.exists(self.bot_comments_file):
            try:
                with open(self.bot_comments_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self.bot_sent_comments = set(data)
                    else:
                        self.bot_sent_comments = set()
                    logger.info(f"[持久化] 从文件加载了 {len(self.bot_sent_comments)} 条机器人发送的评论ID")
            except Exception as e:
                logger.error(f"[持久化] 加载机器人评论ID失败: {e}")
                self.bot_sent_comments = set()
        else:
            logger.debug(f"[持久化] 机器人评论ID文件不存在，使用空集合")
        
        # 加载已回复的评论ID
        if os.path.exists(self.replied_comments_file):
            try:
                with open(self.replied_comments_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self.replied_comments = set(data)
                    else:
                        self.replied_comments = set()
                    logger.info(f"[持久化] 从文件加载了 {len(self.replied_comments)} 条已回复的评论ID")
            except Exception as e:
                logger.error(f"[持久化] 加载已回复评论ID失败: {e}")
                self.replied_comments = set()
        else:
            logger.debug(f"[持久化] 已回复评论ID文件不存在，使用空集合")
    
    def _save_bot_comments(self):
        """保存机器人发送的评论ID到文件"""
        try:
            with open(self.bot_comments_file, "w", encoding="utf-8") as f:
                json.dump(list(self.bot_sent_comments), f, indent=2, ensure_ascii=False)
            logger.debug(f"[持久化] 已保存 {len(self.bot_sent_comments)} 条机器人评论ID到文件")
            # 同时记录到日志文件
            self._log_bot_comments("保存")
        except Exception as e:
            logger.error(f"[持久化] 保存机器人评论ID失败: {e}")
    
    def _log_bot_comments(self, action: str = "更新"):
        """记录bot_sent_comments到日志文件"""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sorted_comments = sorted(list(self.bot_sent_comments))
            with open(self.bot_comments_log_file, "a", encoding="utf-8") as f:
                f.write(f"\n{'='*60}\n")
                f.write(f"[{timestamp}] {action} - bot_sent_comments (共 {len(self.bot_sent_comments)} 条)\n")
                f.write(f"{'='*60}\n")
                if sorted_comments:
                    for i, comment_id in enumerate(sorted_comments, 1):
                        f.write(f"{i:4d}. {comment_id}\n")
                else:
                    f.write("(空)\n")
                f.write(f"{'='*60}\n")
            logger.debug(f"[日志] 已记录bot_sent_comments到日志文件: {self.bot_comments_log_file}")
        except Exception as e:
            logger.error(f"[日志] 记录bot_sent_comments到日志文件失败: {e}")
    
    def _save_replied_comments(self):
        """保存已回复的评论ID到文件"""
        try:
            with open(self.replied_comments_file, "w", encoding="utf-8") as f:
                json.dump(list(self.replied_comments), f, indent=2, ensure_ascii=False)
            logger.debug(f"[持久化] 已保存 {len(self.replied_comments)} 条已回复评论ID到文件")
        except Exception as e:
            logger.error(f"[持久化] 保存已回复评论ID失败: {e}")
    
    def _log_comment(self, comment_id: int, sender_name: str, sender_uid: str, content: str, video_aid: int):
        """记录监听到的评论信息到文件"""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(self.comments_log_file, "a", encoding="utf-8") as f:
                f.write(f"\n{'='*80}\n")
                f.write(f"[{timestamp}] 监听到评论\n")
                f.write(f"{'='*80}\n")
                f.write(f"评论ID: {comment_id}\n")
                f.write(f"发布者昵称: {sender_name}\n")
                f.write(f"发布者UID: {sender_uid}\n")
                f.write(f"视频AID: {video_aid}\n")
                f.write(f"评论内容: {content}\n")
                f.write(f"{'='*80}\n")
        except Exception as e:
            logger.error(f"[日志] 记录评论信息失败: {e}")
    
    def _log_reply(self, reply_to_comment_id: int, reply_id: int, reply_content: str):
        """记录回复内容到文件"""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(self.comments_log_file, "a", encoding="utf-8") as f:
                f.write(f"\n[{timestamp}] 机器人回复\n")
                f.write(f"回复目标评论ID: {reply_to_comment_id}\n")
                f.write(f"回复ID: {reply_id}\n")
                f.write(f"回复内容: {reply_content}\n")
                f.write(f"{'='*80}\n")
        except Exception as e:
            logger.error(f"[日志] 记录回复信息失败: {e}")
    
    def _init_comments_log_file(self):
        """初始化评论日志文件，如果文件不存在则创建并写入头部信息"""
        try:
            if not os.path.exists(self.comments_log_file):
                with open(self.comments_log_file, "w", encoding="utf-8") as f:
                    f.write(f"{'='*80}\n")
                    f.write("B站评论和回复记录日志\n")
                    f.write(f"{'='*80}\n")
                    f.write("此文件记录所有监听到的评论和机器人的回复\n")
                    f.write("格式：\n")
                    f.write("  - 监听到的评论：评论ID、发布者昵称、发布者UID、评论内容\n")
                    f.write("  - 机器人回复：回复目标评论ID、回复ID、回复内容\n")
                    f.write(f"{'='*80}\n\n")
                logger.info(f"[日志] 已创建评论日志文件: {self.comments_log_file}")
        except Exception as e:
            logger.error(f"[日志] 初始化评论日志文件失败: {e}")
    
    def _check_rate_limit(self) -> bool:
        """检查是否超过频率限制"""
        now = time.time()
        
        # 清理过期的回复时间戳（超过1小时）
        self.reply_timestamps = [t for t in self.reply_timestamps if now - t < 3600]
        
        # 检查每小时回复数
        if len(self.reply_timestamps) >= self.max_replies_per_hour:
            logger.warning(f"达到每小时最大回复数限制: {self.max_replies_per_hour}")
            return False
        
        return True
    
    def _update_rate_limit(self):
        """更新频率限制记录"""
        now = time.time()
        self.reply_timestamps.append(now)
    
    async def start(self):
        """启动适配器"""
        if not self.enabled:
            logger.warning("B站适配器未启用")
            return
        
        if not self.credential:
            logger.error("B站凭证未配置，无法启动")
            return
        
        if self.running:
            logger.warning("B站适配器已在运行")
            return
        
        self.running = True
        logger.info("启动B站适配器...")
        
        # 启动监听任务
        if self.reply_private_messages:
            if not MESSAGE_MODULE_AVAILABLE:
                logger.warning("bilibili_api.message模块不可用，私信功能已禁用")
            else:
                task = asyncio.create_task(self._monitor_private_messages())
                self.monitor_tasks.append(task)
                logger.info("私信监听已启动")
        
        if self.reply_comments and (self.monitor_video_ids or self.monitor_dynamic_ids):
            task = asyncio.create_task(self._monitor_comments())
            self.monitor_tasks.append(task)
            logger.info("评论监听已启动")
        elif self.reply_comments:
            logger.warning("评论回复已启用但未配置监听目标（monitor_video_ids 或 monitor_dynamic_ids）")
        
        logger.info("B站适配器已启动")
    
    async def _monitor_private_messages(self):
        """监听私信消息"""
        logger.info("开始监听B站私信...")
        
        while self.running:
            try:
                # 获取会话列表
                sessions = await get_sessions(credential=self.credential)
                
                for session in sessions:
                    if not self.running:
                        break
                    
                    session_id = session.get('session_id')
                    if not session_id:
                        continue
                    
                    # 获取该会话的消息
                    messages = await get_messages(
                        credential=self.credential,
                        session_id=session_id
                    )
                    
                    for message in messages.get('messages', []):
                        if not self.running:
                            break
                        
                        # 只处理未读消息
                        if message.get('is_read', True):
                            continue
                        
                        # 检查是否已处理
                        message_id = str(message.get('message_id', ''))
                        if message_id in self.processed_messages:
                            continue
                        
                        # 检查频率限制
                        if not self._check_rate_limit():
                            continue
                        
                        # 检查回复间隔
                        now = time.time()
                        if now - self.last_reply_time.get('private', 0) < self.reply_interval:
                            continue
                        
                        # 处理消息
                        await self._process_private_message(session_id, message)
                        await asyncio.sleep(1)  # 避免请求过快
                
                # 每30秒检查一次
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"监听私信出错: {e}")
                await asyncio.sleep(60)  # 出错后等待更长时间
    
    async def _load_initial_comments(self, video_id: str, video_aid: int):
        """加载初始评论列表（用于过滤历史评论）"""
        if self.initial_comment_ids_loaded:
            return
        
        try:
            logger.info(f"正在加载视频 {video_id} 的初始评论列表（用于过滤历史评论）...")
            comments = await get_comments_lazy(
                oid=video_aid,
                type_=CommentResourceType.VIDEO,
                order=OrderType.TIME,
                credential=self.credential
            )
            
            # 获取所有评论ID
            replies = comments.get('replies', [])
            top_comment = comments.get('top')
            top_replies = comments.get('top_replies', [])
            
            all_replies = []
            if top_comment and isinstance(top_comment, dict):
                all_replies.append(top_comment)
            if top_replies and isinstance(top_replies, list):
                all_replies.extend(top_replies)
            if replies and isinstance(replies, list):
                all_replies.extend(replies)
            
            # 记录所有现有评论的ID
            for comment_data in all_replies:
                if isinstance(comment_data, dict):
                    comment_id = comment_data.get('rpid')
                    if comment_id:
                        self.initial_comment_ids.add(comment_id)
                        self.processed_comments.add(comment_id)  # 同时标记为已处理
            
            logger.info(f"已加载 {len(self.initial_comment_ids)} 条历史评论，将只处理新评论")
            self.initial_comment_ids_loaded = True
            
        except Exception as e:
            logger.warning(f"加载初始评论列表失败: {e}，将处理所有评论")
            # 如果加载失败，不设置标志，让后续代码处理所有评论
    
    async def _check_and_process_replies_to_bot(self, all_comments: List[Dict], video_aid: int):
        """
        第一条线：检查并处理回复机器人的评论
        
        Args:
            all_comments: 所有评论列表
            video_aid: 视频AID
        """
        # 构建机器人发送的评论ID集合（用于检查回复）
        # 【重要】从bot_sent_comments构建，确保包含所有机器人发送的评论（即使API没有返回）
        bot_comment_ids = set(self.bot_sent_comments) if self.bot_sent_comments else set()
        
        # 从当前获取的评论中，识别机器人发送的评论（补充bot_sent_comments）
        if self.dedeuserid:
            bot_mid = str(self.dedeuserid)
            for comment_data in all_comments:
                if not isinstance(comment_data, dict):
                    continue
                comment_mid = str(comment_data.get('mid', comment_data.get('uid', '')))
                if comment_mid == bot_mid:
                    comment_id = comment_data.get('rpid')
                    if comment_id:
                        bot_comment_ids.add(comment_id)
                        if comment_id not in self.bot_sent_comments:
                            self.bot_sent_comments.add(comment_id)
                            # 保存到文件
                            self._save_bot_comments()
        
        # 确保bot_comment_ids包含所有bot_sent_comments（双重保险）
        if len(bot_comment_ids) != len(self.bot_sent_comments):
            logger.warning(f"[回复检查] bot_comment_ids和bot_sent_comments大小不一致，已同步")
            bot_comment_ids = set(self.bot_sent_comments)
        
        # 构建评论ID到评论数据的映射（用于递归查找）
        comment_id_to_data = {}
        for comment_data in all_comments:
            if isinstance(comment_data, dict):
                comment_id = comment_data.get('rpid')
                if comment_id:
                    comment_id_to_data[comment_id] = comment_data
        
        # 递归检查函数：检查评论是否是回复机器人的评论（包括多层嵌套）
        def is_reply_to_bot_comment(comment_data, checked_ids=None, depth=0, path=None):
            """
            递归检查评论是否是回复机器人的评论
            
            Args:
                comment_data: 评论数据
                checked_ids: 已检查的评论ID集合（避免循环）
                depth: 递归深度（防止无限递归）
                path: 递归路径（用于调试）
            
            Returns:
                bool: 是否是回复机器人的评论
            """
            if checked_ids is None:
                checked_ids = set()
            if path is None:
                path = []
            
            # 防止无限递归
            if depth > 20:  # 增加深度限制，支持更深的嵌套
                logger.warning(f"[回复检查] 递归深度超过限制: {depth}，路径: {' -> '.join(map(str, path))}")
                return False
            
            comment_id = comment_data.get('rpid')
            if not comment_id:
                return False
            
            # 避免循环检查
            if comment_id in checked_ids:
                return False
            checked_ids.add(comment_id)
            path.append(comment_id)
            
            parent_id = comment_data.get('parent', 0) or 0
            root_id = comment_data.get('root', 0) or 0
            
            # 只在深度较深时记录日志（减少冗余）
            if depth > 2:
                logger.debug(f"[回复检查] 递归检查评论 {comment_id} (depth={depth})，parent={parent_id}，root={root_id}")
            
            # 直接检查parent或root是否是机器人发送的评论（不仅检查bot_comment_ids，也检查bot_sent_comments）
            parent_match = (parent_id in bot_comment_ids) or (parent_id in self.bot_sent_comments) if parent_id else False
            root_match = (root_id in bot_comment_ids) or (root_id in self.bot_sent_comments) if root_id else False
            
            if parent_match or root_match:
                logger.info(f"[回复检查] ✓ 识别到回复：评论ID {comment_id}，parent {parent_id}，root {root_id}")
                # 如果parent或root在bot_sent_comments中但不在bot_comment_ids中，添加到bot_comment_ids
                if parent_id and parent_id in self.bot_sent_comments and parent_id not in bot_comment_ids:
                    bot_comment_ids.add(parent_id)
                if root_id and root_id in self.bot_sent_comments and root_id not in bot_comment_ids:
                    bot_comment_ids.add(root_id)
                return True
            
            # 【关键改进】即使parent和root都不匹配，也要检查整个回复链
            # 因为B站的回复机制中，parent可能指向用户自己的评论，但回复链中可能包含机器人的评论
            # 我们需要检查parent的parent，parent的root，以及整个回复链
            
            # 如果parent_id为0，说明这是顶级评论，检查评论本身是否是机器人的评论
            if not parent_id:
                # 检查当前评论的作者是否是机器人
                comment_mid = str(comment_data.get('mid', comment_data.get('uid', '')))
                if comment_mid and self.dedeuserid and comment_mid == str(self.dedeuserid):
                    # 将评论ID添加到bot_comment_ids
                    if comment_id not in bot_comment_ids:
                        bot_comment_ids.add(comment_id)
                        self.bot_sent_comments.add(comment_id)
                    return True
                # 如果root_id存在且与parent_id不同，继续检查root
                if root_id and root_id != comment_id:
                    # 继续下面的root检查逻辑（不在这里return，让代码继续检查root）
                    pass
                else:
                    # 如果parent_id为0且root_id也为0或与comment_id相同，说明这是顶级评论
                    # 但是，如果这是在递归检查中，说明可能是回复链中的一环，需要检查是否有其他评论回复了这个评论
                    # 【关键】如果这是在递归检查中（depth > 0），说明可能是回复链中的一环，需要检查是否有机器人的评论回复了这个评论
                    if depth > 0:
                        # 检查all_comments中是否有回复这个评论的机器人评论
                        for other_comment_data in all_comments:
                            if not isinstance(other_comment_data, dict):
                                continue
                            other_comment_id = other_comment_data.get('rpid')
                            if not other_comment_id or other_comment_id == comment_id:
                                continue
                            # 跳过已经在检查路径中的评论，避免循环
                            if other_comment_id in checked_ids:
                                continue
                            other_comment_parent = other_comment_data.get('parent', 0) or 0
                            other_comment_root = other_comment_data.get('root', 0) or 0
                            # 如果其他评论的parent或root是comment_id，检查其作者是否是机器人
                            if other_comment_parent == comment_id or other_comment_root == comment_id:
                                other_comment_mid = str(other_comment_data.get('mid', other_comment_data.get('uid', '')))
                                if other_comment_mid and self.dedeuserid and other_comment_mid == str(self.dedeuserid):
                                    logger.info(f"[回复检查] ✓ 通过检查回复链发现机器人的评论：评论ID {comment_id}，回复链中的机器人评论 {other_comment_id}，路径: {' -> '.join(map(str, path))}")
                                    if other_comment_id not in bot_comment_ids:
                                        bot_comment_ids.add(other_comment_id)
                                        self.bot_sent_comments.add(other_comment_id)
                                    return True
                    # 如果parent_id为0且root_id也为0或与comment_id相同，说明这是顶级评论，不是回复
                    return False
            
            # 递归检查parent评论
            if parent_id:
                # 先检查parent是否在comment_id_to_data中
                if parent_id in comment_id_to_data:
                    parent_comment = comment_id_to_data[parent_id]
                    # 检查parent评论的作者是否是机器人
                    parent_mid = str(parent_comment.get('mid', parent_comment.get('uid', '')))
                    if parent_mid and self.dedeuserid and parent_mid == str(self.dedeuserid):
                        # 将parent评论ID添加到bot_comment_ids
                        if parent_id not in bot_comment_ids:
                            bot_comment_ids.add(parent_id)
                            self.bot_sent_comments.add(parent_id)
                        return True
                    # 【关键】即使parent不是机器人的评论，也要递归检查parent的parent和root
                    # 因为可能存在多层嵌套：用户A -> 机器人B -> 用户A -> 机器人B
                    # 在这种情况下，用户的回复的parent可能是用户自己的评论，但root可能是机器人的评论
                    # parent不是机器人的评论，继续递归检查
                    # 递归检查parent评论的parent和root（继续向上查找整个回复链）
                    if is_reply_to_bot_comment(parent_comment, checked_ids, depth + 1, path.copy()):
                        return True
                    # 如果递归检查parent失败，还要检查parent的root（因为parent可能不是机器人的评论，但root可能是）
                    parent_root_id = parent_comment.get('root', 0) or 0
                    if parent_root_id and parent_root_id != parent_id:
                        if parent_root_id in bot_comment_ids or parent_root_id in self.bot_sent_comments:
                            return True
                        # 如果parent_root_id在comment_id_to_data中，检查其作者
                        if parent_root_id in comment_id_to_data:
                            parent_root_comment = comment_id_to_data[parent_root_id]
                            parent_root_mid = str(parent_root_comment.get('mid', parent_root_comment.get('uid', '')))
                            if parent_root_mid and self.dedeuserid and parent_root_mid == str(self.dedeuserid):
                                if parent_root_id not in bot_comment_ids:
                                    bot_comment_ids.add(parent_root_id)
                                    self.bot_sent_comments.add(parent_root_id)
                                return True
                    # 【关键改进】如果parent的parent=0，说明parent是顶级评论，但我们需要检查整个回复链
                    # 检查all_comments中是否有回复parent的评论，这些评论可能是机器人的评论
                    parent_parent_id = parent_comment.get('parent', 0) or 0
                    if not parent_parent_id:
                        # parent是顶级评论，检查all_comments中是否有回复parent的评论（这些评论可能是机器人的评论）
                        # 【重要】这里不使用递归函数，而是直接检查all_comments，避免循环检测问题
                        # parent是顶级评论，检查all_comments中是否有回复parent的评论
                        for other_comment_data in all_comments:
                            if not isinstance(other_comment_data, dict):
                                continue
                            other_comment_id = other_comment_data.get('rpid')
                            if not other_comment_id:
                                continue
                            # 跳过已经在检查路径中的评论，避免循环
                            if other_comment_id in checked_ids:
                                continue
                            other_comment_parent = other_comment_data.get('parent', 0) or 0
                            other_comment_root = other_comment_data.get('root', 0) or 0
                            # 如果其他评论的parent或root是parent_id，检查其作者是否是机器人
                            if other_comment_parent == parent_id or other_comment_root == parent_id:
                                 other_comment_mid = str(other_comment_data.get('mid', other_comment_data.get('uid', '')))
                                 if other_comment_mid and self.dedeuserid and other_comment_mid == str(self.dedeuserid):
                                     logger.info(f"[回复检查] ✓ 通过回复链识别到回复：评论ID {comment_id}，机器人评论 {other_comment_id}")
                                     if other_comment_id not in bot_comment_ids:
                                         bot_comment_ids.add(other_comment_id)
                                         self.bot_sent_comments.add(other_comment_id)
                                     return True
                else:
                    # parent不在comment_id_to_data中（可能因为API没有返回该评论）
                    # 先检查parent是否在bot_sent_comments中（即使不在当前获取的评论列表中）
                    if parent_id in self.bot_sent_comments:
                        if parent_id not in bot_comment_ids:
                            bot_comment_ids.add(parent_id)
                        return True
                    # 再检查parent是否在bot_comment_ids中（说明是机器人发送的评论）
                    if parent_id in bot_comment_ids:
                        return True
            
            # 递归检查root评论
            # 【重要】即使root_id与parent_id相同，也要检查root（因为可能存在特殊情况）
            # 在B站的回复机制中，root_id通常指向根评论，可能是机器人的评论
            if root_id:
                # 先检查root是否在comment_id_to_data中
                if root_id in comment_id_to_data:
                    root_comment = comment_id_to_data[root_id]
                    # 检查root评论的作者是否是机器人
                    root_mid = str(root_comment.get('mid', root_comment.get('uid', '')))
                    if root_mid and self.dedeuserid and root_mid == str(self.dedeuserid):
                        # 将root评论ID添加到bot_comment_ids
                        if root_id not in bot_comment_ids:
                            bot_comment_ids.add(root_id)
                            self.bot_sent_comments.add(root_id)
                        return True
                    # 递归检查root评论的parent（继续向上查找）
                    if is_reply_to_bot_comment(root_comment, checked_ids, depth + 1, path.copy()):
                        return True
                else:
                    # root不在comment_id_to_data中（可能因为API没有返回该评论）
                    # 先检查root是否在bot_sent_comments中（即使不在当前获取的评论列表中）
                    if root_id in self.bot_sent_comments:
                        if root_id not in bot_comment_ids:
                            bot_comment_ids.add(root_id)
                        return True
                    # 再检查root是否在bot_comment_ids中
                    if root_id in bot_comment_ids:
                        return True
            
            return False
        
        # 检查所有评论是否是回复机器人的评论（不受时间窗口限制）
        replies_to_bot = []
        
        # 统计信息：检查是否有parent或root是机器人评论但不在当前列表中的情况
        # 这种情况可能发生在B站API没有返回所有层级的嵌套评论时
        missing_parent_count = 0
        missing_root_count = 0
        for comment_data in all_comments:
            if not isinstance(comment_data, dict):
                continue
            comment_id = comment_data.get('rpid')
            if not comment_id:
                continue
            parent_id = comment_data.get('parent', 0) or 0
            root_id = comment_data.get('root', 0) or 0
            # 如果parent或root是机器人的评论，但该评论不在当前列表中，说明API可能没有返回完整的嵌套结构
            if parent_id and (parent_id in bot_comment_ids or parent_id in self.bot_sent_comments) and parent_id not in comment_id_to_data:
                missing_parent_count += 1
            if root_id and (root_id in bot_comment_ids or root_id in self.bot_sent_comments) and root_id not in comment_id_to_data:
                missing_root_count += 1
        
        if missing_parent_count > 0 or missing_root_count > 0:
            logger.debug(f"[回复检查] 发现 {missing_parent_count} 条评论的parent是机器人评论但不在当前列表，{missing_root_count} 条评论的root是机器人评论但不在当前列表（可能API未返回完整嵌套结构）")
        
        for comment_data in all_comments:
            if not isinstance(comment_data, dict):
                continue
            
            comment_id = comment_data.get('rpid')
            if not comment_id:
                continue
            
            # 跳过机器人自己的评论
            comment_mid = comment_data.get('mid') or comment_data.get('uid')
            if comment_mid and self.dedeuserid and str(comment_mid) == str(self.dedeuserid):
                continue
            
            # 【重要】跳过已经回复过的评论（避免重复回复）
            if comment_id in self.replied_comments:
                continue
            
            # 跳过已处理且已回复的评论（避免重复检查）
            # 【重要】如果评论已经在processed_comments中，但还没有回复过，且parent或root是机器人的评论，需要重新检查
            if comment_id in self.processed_comments:
                parent_id = comment_data.get('parent', 0) or 0
                root_id = comment_data.get('root', 0) or 0
                # 如果parent或root在bot_comment_ids或bot_sent_comments中，可能是回复机器人的评论，需要重新检查
                if comment_id not in self.replied_comments and (
                    (parent_id and (parent_id in bot_comment_ids or parent_id in self.bot_sent_comments)) or
                    (root_id and (root_id in bot_comment_ids or root_id in self.bot_sent_comments))
                ):
                    if is_reply_to_bot_comment(comment_data):
                        logger.info(f"[回复检查] 重新识别已处理评论为回复：评论ID {comment_id}")
                        self.processed_comments.discard(comment_id)
                        replies_to_bot.append(comment_data)
                continue
            
            # 检查是否是回复机器人的评论（不受时间窗口限制）
            parent_id = comment_data.get('parent', 0) or 0
            root_id = comment_data.get('root', 0) or 0
            
            # 先快速检查parent和root（直接匹配）
            # 不仅检查bot_comment_ids，也检查bot_sent_comments（因为可能不在当前获取的评论列表中）
            parent_in_bot = (parent_id in bot_comment_ids) if parent_id else False
            parent_in_sent = (parent_id in self.bot_sent_comments) if parent_id else False
            root_in_bot = (root_id in bot_comment_ids) if root_id else False
            root_in_sent = (root_id in self.bot_sent_comments) if root_id else False
            
            if parent_in_bot or parent_in_sent or root_in_bot or root_in_sent:
                replies_to_bot.append(comment_data)
                # 记录详细信息，帮助调试
                match_reason = []
                if parent_in_bot:
                    match_reason.append(f"parent({parent_id})在bot_comment_ids")
                if parent_in_sent:
                    match_reason.append(f"parent({parent_id})在bot_sent_comments")
                if root_in_bot:
                    match_reason.append(f"root({root_id})在bot_comment_ids")
                if root_in_sent:
                    match_reason.append(f"root({root_id})在bot_sent_comments")
                logger.info(f"[回复检查] ✓ 快速识别回复：评论ID {comment_id}，原因: {', '.join(match_reason)}")
                # 如果parent或root在bot_sent_comments中但不在bot_comment_ids中，添加到bot_comment_ids
                if parent_id and parent_in_sent and not parent_in_bot:
                    bot_comment_ids.add(parent_id)
                if root_id and root_in_sent and not root_in_bot:
                    bot_comment_ids.add(root_id)
                continue  # 快速检查成功，跳过递归检查
            else:
                # 【关键改进】在递归检查之前，先检查是否有任何评论回复了当前评论的parent或root
                # 如果parent或root是顶级评论，检查all_comments中是否有机器人的评论回复了它们
                if parent_id and parent_id in comment_id_to_data:
                    parent_comment = comment_id_to_data[parent_id]
                    parent_parent_id = parent_comment.get('parent', 0) or 0
                    if not parent_parent_id:
                        # parent是顶级评论，检查all_comments中是否有机器人的评论回复了parent
                        for other_comment_data in all_comments:
                            if not isinstance(other_comment_data, dict):
                                continue
                            other_comment_id = other_comment_data.get('rpid')
                            if not other_comment_id or other_comment_id == comment_id:
                                continue
                            other_comment_parent = other_comment_data.get('parent', 0) or 0
                            other_comment_root = other_comment_data.get('root', 0) or 0
                            # 如果其他评论的parent或root是parent_id，检查其作者是否是机器人
                            if (other_comment_parent == parent_id or other_comment_root == parent_id):
                                other_comment_mid = str(other_comment_data.get('mid', other_comment_data.get('uid', '')))
                                if other_comment_mid and self.dedeuserid and other_comment_mid == str(self.dedeuserid):
                                    logger.info(f"[回复检查] ✓ 通过回复链识别到回复：评论ID {comment_id}，机器人评论 {other_comment_id}")
                                    if other_comment_id not in bot_comment_ids:
                                        bot_comment_ids.add(other_comment_id)
                                        self.bot_sent_comments.add(other_comment_id)
                                    replies_to_bot.append(comment_data)
                                    break  # 找到后跳出循环，跳过后续的递归检查
                
                # 递归检查（包括多层嵌套和通过作者识别）
                if is_reply_to_bot_comment(comment_data):
                    replies_to_bot.append(comment_data)
                    logger.info(f"[回复检查] ✓ 发现回复机器人的评论（递归检查）：评论ID {comment_id}，parent {parent_id}，root {root_id}")
        
        # 处理回复机器人的评论
        if replies_to_bot:
            logger.info(f"[回复检查] 发现 {len(replies_to_bot)} 条回复机器人的评论，开始处理...")
            # 按时间排序，最新的在前
            try:
                replies_to_bot.sort(key=lambda x: x.get('ctime', x.get('rpid', 0)), reverse=True)
            except Exception:
                pass
            
            for comment_data in replies_to_bot:
                if not self.running:
                    break
                
                comment_id = comment_data.get('rpid')
                if not comment_id:
                    continue
                
                parent_id = comment_data.get('parent', 0) or 0
                root_id = comment_data.get('root', 0) or 0
                logger.info(f"[回复检查] 处理回复机器人的评论：评论ID {comment_id}，parent {parent_id}，root {root_id}")
                
                # 【重要】先标记为已处理，避免重复处理（即使因频率限制跳过也要标记）
                self.processed_comments.add(comment_id)
                
                # 检查频率限制
                if not self._check_rate_limit():
                    continue
                
                # 检查回复间隔
                if time.time() - self.last_reply_time.get('comment', 0) < self.reply_interval:
                    continue
                
                # 【重要】再次检查是否已经回复过（防止并发情况下的重复回复）
                if comment_id in self.replied_comments:
                    continue
                
                # 处理回复机器人的评论
                await self._process_comment_dict(comment_data, video_aid)
                # 【关键】标记为已回复，避免重复回复
                self.replied_comments.add(comment_id)
                # 保存到文件
                self._save_replied_comments()
                await asyncio.sleep(1)
    
    async def _check_and_process_new_comments(self, all_comments: List[Dict], video_aid: int):
        """
        第二条线：检查并处理新评论
        
        Args:
            all_comments: 所有评论列表
            video_aid: 视频AID
        """
        # 基于时间窗口过滤评论
        current_time = int(time.time())
        time_window_seconds = self.comment_time_window_minutes * 60
        filtered_comments = []
        
        for comment_data in all_comments:
            comment_id = comment_data.get('rpid')
            if not comment_id:
                continue
            
            # 跳过已处理的评论
            if comment_id in self.processed_comments:
                continue
            
            # 跳过机器人自己的评论
            comment_mid = comment_data.get('mid') or comment_data.get('uid')
            if comment_mid and self.dedeuserid and str(comment_mid) == str(self.dedeuserid):
                continue
            
            comment_time = comment_data.get('ctime', 0)
            if comment_time > 0:
                time_diff = current_time - comment_time
                if time_diff <= time_window_seconds:
                    filtered_comments.append(comment_data)
            elif comment_time == 0:
                # 如果没有时间戳，保留该评论（可能是新评论）
                filtered_comments.append(comment_data)
        
        # 限制检查数量
        comments_to_check = filtered_comments[:self.max_comments_per_check]
        if len(filtered_comments) > 0:
            logger.info(f"[新评论检查] 时间窗口过滤后剩余 {len(filtered_comments)} 条评论，检查前 {len(comments_to_check)} 条")
        
        # 处理新评论
        new_comments_to_process = []
        
        for comment_data in comments_to_check:
            if not self.running:
                break
            
            try:
                # 检查comment_data是否为有效字典
                if not comment_data or not isinstance(comment_data, dict):
                    continue
                
                comment_id = comment_data.get('rpid')
                if not comment_id:
                    continue
                
                # 检查是否是历史评论（在初始列表中）
                if comment_id in self.initial_comment_ids:
                    self.processed_comments.add(comment_id)  # 标记为已处理，避免重复检查
                    continue
                
                # 这是新评论，先标记为已处理，避免遗漏
                if comment_id not in self.processed_comments:
                    self.processed_comments.add(comment_id)
                new_comments_to_process.append(comment_data)
                
            except Exception as e:
                logger.error(f"预处理新评论失败: {e}")
                import traceback
                logger.debug(traceback.format_exc())
                continue
        
        if new_comments_to_process:
            logger.info(f"[新评论检查] 发现 {len(new_comments_to_process)} 条新评论，开始处理...")
        
        # 处理所有新评论
        for comment_data in new_comments_to_process:
            if not self.running:
                break
            
            try:
                comment_id = comment_data.get('rpid')
                if not comment_id:
                    continue
                
                # 检查频率限制
                if not self._check_rate_limit():
                    logger.warning(f"[新评论检查] 评论 {comment_id} 因频率限制跳过（当前已回复 {len(self.reply_timestamps)}/{self.max_replies_per_hour} 条/小时）")
                    continue
                
                # 检查回复间隔
                now = time.time()
                last_reply_time = self.last_reply_time.get('comment', 0)
                time_since_last_reply = now - last_reply_time
                if time_since_last_reply < self.reply_interval:
                    logger.debug(f"[新评论检查] 评论 {comment_id} 因回复间隔跳过（距离上次回复 {time_since_last_reply:.1f} 秒，需要等待 {self.reply_interval} 秒）")
                    continue
                
                # 处理新评论
                logger.info(f"[新评论检查] 开始处理评论 {comment_id}")
                await self._process_comment_dict(comment_data, video_aid)
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"处理新评论失败: {e}")
                import traceback
                logger.debug(traceback.format_exc())
                continue
    
    async def _check_replies_to_bot(self, all_comments: List[Dict], video_aid: int) -> List[Dict]:
        """
        检查用户回复机器人的评论
        
        Args:
            all_comments: 所有评论列表
            video_aid: 视频AID
            
        Returns:
            回复机器人的评论列表（按时间排序，最新的在前）
        """
        replies_to_bot = []
        
        # 构建机器人发送的评论ID集合
        bot_comment_ids = set(self.bot_sent_comments) if self.bot_sent_comments else set()
        
        # 通过mid识别机器人发送的评论（确保不遗漏）
        if self.dedeuserid:
            bot_mid = str(self.dedeuserid)
            for comment_data in all_comments:
                if not isinstance(comment_data, dict):
                    continue
                comment_mid = str(comment_data.get('mid', comment_data.get('uid', '')))
                if comment_mid == bot_mid:
                    comment_id = comment_data.get('rpid')
                    if comment_id:
                        bot_comment_ids.add(comment_id)
                        if comment_id not in self.bot_sent_comments:
                            self.bot_sent_comments.add(comment_id)
        
        if not bot_comment_ids:
            logger.info(f"[回复检查] 未找到机器人发送的评论记录（可能机器人尚未发送评论）")
            return replies_to_bot
        
        logger.info(f"[回复检查] 机器人已发送 {len(bot_comment_ids)} 条评论，开始检查回复...")
        
        # 时间窗口：只检查最近20分钟内的回复
        current_time = int(time.time())
        time_window_seconds = self.comment_time_window_minutes * 60
        
        # 检查所有评论，找出回复机器人的评论
        for comment_data in all_comments:
            if not isinstance(comment_data, dict):
                continue
            
            comment_id = comment_data.get('rpid')
            if not comment_id:
                continue
            
            # 跳过机器人自己的评论
            comment_mid = comment_data.get('mid') or comment_data.get('uid')
            if comment_mid and self.dedeuserid and str(comment_mid) == str(self.dedeuserid):
                continue
            
            # 跳过已处理的评论
            if comment_id in self.processed_comments:
                continue
            
            # 时间窗口过滤：只检查最近20分钟内的评论
            comment_time = comment_data.get('ctime', 0)
            if comment_time > 0:
                time_diff = current_time - comment_time
                if time_diff > time_window_seconds:
                    continue  # 超过20分钟，跳过
            
            # 检查是否是回复机器人评论的评论
            parent_id = comment_data.get('parent', 0) or 0
            root_id = comment_data.get('root', 0) or 0
            
            # 检查parent或root是否是机器人回复过的评论
            if (parent_id in bot_comment_ids) or (root_id in bot_comment_ids):
                replies_to_bot.append(comment_data)
                logger.info(f"[回复检查] 发现回复机器人的评论：评论ID {comment_id}，parent {parent_id}，root {root_id}")
                continue
            
            # 如果通过parent/root没找到，尝试通过检查parent评论的作者
            if parent_id:
                # 在all_comments中查找parent评论
                for c in all_comments:
                    if c.get('rpid') == parent_id:
                        parent_mid = str(c.get('mid', c.get('uid', '')))
                        if parent_mid and self.dedeuserid and parent_mid == str(self.dedeuserid):
                            # parent评论的作者是机器人
                            replies_to_bot.append(comment_data)
                            # 将parent评论ID添加到bot_comment_ids
                            if parent_id not in bot_comment_ids:
                                bot_comment_ids.add(parent_id)
                                self.bot_sent_comments.add(parent_id)
                            logger.info(f"[回复检查] 通过parent作者识别到回复：评论ID {comment_id}，parent {parent_id}（机器人评论）")
                            break
        
        if replies_to_bot:
            logger.info(f"[回复检查] 发现 {len(replies_to_bot)} 条回复机器人的评论")
            # 按时间排序，最新的在前
            try:
                replies_to_bot.sort(key=lambda x: x.get('ctime', x.get('rpid', 0)), reverse=True)
            except Exception:
                pass
        else:
            logger.info(f"[回复检查] 未发现回复机器人的评论")
        
        return replies_to_bot
    
    async def _monitor_comments(self):
        """监听评论"""
        logger.info("开始监听B站评论...")
        
        # 首次加载时，记录所有现有评论ID
        first_run = True
        
        while self.running:
            try:
                # 监听视频评论
                for video_id in self.monitor_video_ids:
                    if not self.running:
                        break
                    
                    try:
                        # 检查video_id格式
                        if video_id.startswith('BV'):
                            v = video.Video(bvid=video_id, credential=self.credential)
                        else:
                            # 假设是aid
                            v = video.Video(aid=int(video_id), credential=self.credential)
                        
                        # 获取视频aid
                        video_info = await v.get_info()
                        video_aid = video_info.get('aid', 0)
                        video_title = video_info.get('title', '未知')
                        
                        
                        if not video_aid:
                            logger.warning(f"无法获取视频 {video_id} 的aid，跳过")
                            continue
                        
                        # 首次运行时，加载初始评论列表
                        if first_run:
                            await self._load_initial_comments(video_id, video_aid)
                            first_run = False
                        
                        # 使用get_comments_lazy获取评论（这个函数可以正确获取评论）
                        logger.info(f"[轮询] 正在检查视频 {video_id} (aid: {video_aid}) 的评论...")
                        comments = await get_comments_lazy(
                            oid=video_aid,
                            type_=CommentResourceType.VIDEO,
                            order=OrderType.TIME,  # 按时间排序，获取最新评论
                            credential=self.credential  # 传递凭证以获取完整评论
                        )
                        
                        # 检查comments是否为None或不是字典
                        if not comments or not isinstance(comments, dict):
                            logger.warning(f"获取视频 {video_id} 的评论失败，返回结果无效: {comments}")
                            continue
                        
                        # get_comments_lazy返回的数据结构不同，直接获取replies
                        replies = comments.get('replies', [])
                        
                        # 检查是否有置顶评论
                        top_comment = comments.get('top')
                        top_replies = comments.get('top_replies', [])
                        
                        # 合并置顶评论和普通评论
                        all_replies = []
                        if top_comment and isinstance(top_comment, dict):
                            all_replies.append(top_comment)
                        
                        if top_replies and isinstance(top_replies, list):
                            all_replies.extend(top_replies)
                        
                        if replies and isinstance(replies, list):
                            all_replies.extend(replies)
                        
                        # 处理replies为None的情况
                        if not all_replies:
                            logger.info(f"[轮询] 视频 {video_id} 暂无评论")
                            continue
                        
                        # 递归提取所有评论（包括子评论）
                        all_comments = []
                        max_depth = 0
                        def extract_all_comments(comment_list, depth=0):
                             """递归提取所有评论，包括子评论"""
                             nonlocal max_depth
                             if depth > max_depth:
                                 max_depth = depth
                             for comment in comment_list:
                                 if not isinstance(comment, dict):
                                     continue
                                 all_comments.append(comment)
                                 # 如果有子评论，递归处理
                                 sub_replies = comment.get('replies', [])
                                 if sub_replies and isinstance(sub_replies, list):
                                     extract_all_comments(sub_replies, depth + 1)
                         
                        extract_all_comments(all_replies)
                        logger.info(f"[轮询] 获取到 {len(all_comments)} 条评论（顶级: {len(all_replies)}，包含子评论，最大嵌套深度: {max_depth}）")
                        
                        # 【重要】主动获取机器人评论的所有回复（因为API可能不会返回所有层级的嵌套评论）
                        # 只获取最近发送的机器人评论的回复（避免处理所有历史评论）
                        # 注意：get_comments_lazy()已经获取了所有评论和嵌套回复，通常不需要单独获取某个评论的回复
                        # 如果需要确保获取某个评论的所有回复，可以使用Comment类的get_replies()方法
                        # 但为了简化代码和避免API限制，这里暂时注释掉单独获取回复的逻辑
                        # 如果发现遗漏了某些回复，可以取消注释下面的代码
                        # recent_bot_comments = sorted(list(self.bot_sent_comments), reverse=True)[:5]  # 只处理最近5条
                        # for bot_comment_id in recent_bot_comments:
                        #     try:
                        #         # 使用Comment类来获取该评论的所有回复
                        #         comment_obj = Comment(
                        #             oid=video_aid,
                        #             type_=CommentResourceType.VIDEO,
                        #             rpid=bot_comment_id,
                        #             credential=self.credential
                        #         )
                        #         # 获取该评论的回复
                        #         replies = await comment_obj.get_replies()
                        #         if replies and isinstance(replies, list):
                        #             # 提取这些回复（包括嵌套回复）
                        #             additional_comments = []
                        #             def extract_replies(comment_list, depth=0):
                        #                 for comment in comment_list:
                        #                     if not isinstance(comment, dict):
                        #                         continue
                        #                     comment_rpid = comment.get('rpid')
                        #                     # 避免重复添加
                        #                     if not any(c.get('rpid') == comment_rpid for c in all_comments):
                        #                         additional_comments.append(comment)
                        #                     # 递归提取子回复
                        #                     sub_replies = comment.get('replies', [])
                        #                     if sub_replies and isinstance(sub_replies, list):
                        #                         extract_replies(sub_replies, depth + 1)
                        #             
                        #             extract_replies(replies)
                        #             if additional_comments:
                        #                 all_comments.extend(additional_comments)
                        #                 logger.info(f"[轮询] 为机器人评论 {bot_comment_id} 获取到 {len(additional_comments)} 条额外回复（包括深层嵌套）")
                        #         await asyncio.sleep(0.5)  # 避免请求过快
                        #     except Exception as e:
                        #         logger.debug(f"[轮询] 获取机器人评论 {bot_comment_id} 的回复失败: {e}")
                        #         continue
                        
                        # 按时间排序，确保最新的评论在前
                        try:
                            all_comments.sort(key=lambda x: x.get('ctime', x.get('rpid', 0)), reverse=True)
                        except Exception:
                            pass
                        
                        # ========== 第一条线：检查回复机器人的评论 ==========
                        await self._check_and_process_replies_to_bot(all_comments, video_aid)
                        
                        # ========== 第二条线：检查新评论 ==========
                        await self._check_and_process_new_comments(all_comments, video_aid)
                        
                    except Exception as e:
                        logger.error(f"处理视频 {video_id} 的评论失败: {e}")
                
                # 每30秒检查一次
                # 每20秒检查一次
                await asyncio.sleep(20)
                
            except Exception as e:
                logger.error(f"监听评论出错: {e}")
                await asyncio.sleep(60)  # 出错后等待60秒再重试
    
    async def _process_private_message(self, session_id: int, message: Dict):
        """处理私信消息，发布到事件总线"""
        try:
            message_id = str(message.get('message_id', ''))
            content = message.get('content', '')
            sender_uid = str(message.get('sender_uid', 0))
            sender_name = message.get('sender_name', sender_uid)
            
            logger.info(f"收到B站私信: {sender_name}({sender_uid}) -> {content[:50]}")
            
            # 检查白名单
            if self.user_list and sender_uid not in self.user_list:
                logger.debug(f"用户 {sender_uid} 不在白名单中，跳过")
                return
            
            # 构建消息对象
            message_list = [MessageType.Text(content)]
            
            message_obj = BotDirectMessage(
                self.name,
                self.config.get('adapter_name', 'bilibili'),
                self.message_types,
                sender_uid,
                sender_name,
                message_id,
                self.config.get("bot_pid", ""),
                message_list,
                int(time.time())
            )
            
            # 发布到事件总线
            self.publish(message_obj)
            
            # 标记为已处理
            self.processed_messages.add(message_id)
            self.last_reply_time['private'] = time.time()
            self._update_rate_limit()
            
        except Exception as e:
            logger.error(f"处理私信失败: {e}")
    
    async def _process_comment_dict(self, comment_data: Dict, video_aid: int):
        """处理评论字典数据，发布到事件总线"""
        try:
            comment_id = comment_data.get('rpid')
            if not comment_id:
                return
            
            # 再次检查是否是机器人自己的评论（双重保险）
            comment_mid = comment_data.get('mid') or comment_data.get('uid')
            if comment_mid and self.dedeuserid and str(comment_mid) == str(self.dedeuserid):
                logger.debug(f"跳过机器人自己的评论: {comment_id} (mid: {comment_mid})")
                return
            
            # 获取评论内容
            content_obj = comment_data.get('content', {})
            if isinstance(content_obj, dict):
                content = content_obj.get('message', '')
            else:
                content = str(content_obj) if content_obj else ''
            
            if not content:
                logger.warning(f"评论 {comment_id} 内容为空，跳过")
                return
            
            # 获取发送者信息
            sender_uid = str(comment_data.get('mid', comment_data.get('uid', 'unknown')))
            
            # 获取发送者名称
            member = comment_data.get('member', {})
            if isinstance(member, dict):
                sender_name = member.get('uname', member.get('nickname', sender_uid))
            else:
                sender_name = str(member) if member else sender_uid
            
            logger.info(f"收到B站评论: {sender_name}({sender_uid}) -> {content[:50]}")
            
            # 记录评论信息到文件
            self._log_comment(comment_id, sender_name, sender_uid, content, video_aid)
            
            # 检查白名单
            if self.user_list and sender_uid not in self.user_list:
                logger.debug(f"用户 {sender_uid} 不在白名单中，跳过")
                return
            
            # 构建消息对象（评论作为群组消息处理）
            message_list = [MessageType.Text(content)]
            
            # 使用视频AID作为群组ID
            group_id = str(video_aid)
            
            # 保存评论ID到视频AID的映射（用于后续回复）
            self.comment_to_video[comment_id] = video_aid
            # 保存视频AID到最近评论ID的映射（用于自动回复）
            self.video_to_latest_comment[video_aid] = comment_id
            
            message_obj = BotGroupMessage(
                self.name,
                self.config.get('adapter_name', 'bilibili'),
                self.message_types,
                group_id,
                f"视频{group_id}",
                sender_uid,
                sender_name,
                str(comment_id),  # message_id存储评论ID，用于后续回复
                self.config.get("bot_pid", ""),
                message_list,
                int(time.time())
            )
            
            # 发布到事件总线
            self.publish(message_obj)
            
            # 标记为已处理
            self.processed_comments.add(comment_id)
            self.last_reply_time['comment'] = time.time()
            self._update_rate_limit()
            
        except Exception as e:
            logger.error(f"处理评论失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
    
    async def send_group_message(self, group_id: Union[int, str], send_message_obj: MessageSending) -> Optional[str]:
        """
        发送群消息（对于B站，这是回复评论）
        
        Args:
            group_id: 视频AID（作为群组ID）
            send_message_obj: 要发送的消息对象
            
        Returns:
            评论ID（字符串），发送失败返回None
        """
        try:
            if not self.credential:
                logger.error("B站凭证未配置，无法发送评论")
                return None
            
            # 提取文本内容
            text_content = ""
            reply_to_rpid = None  # 要回复的评论ID
            
            for msg_part in send_message_obj.message_list:
                if isinstance(msg_part, MessageType.Text):
                    text_content += msg_part.text
                elif isinstance(msg_part, MessageType.Image):
                    text_content += "[图片]"
                elif isinstance(msg_part, MessageType.Reply):
                    # 如果有Reply类型，说明要回复某条评论
                    reply_to_rpid = int(msg_part.message_id) if msg_part.message_id and msg_part.message_id.isdigit() else None
            
            if not text_content:
                logger.warning("消息内容为空，跳过发送")
                return None
            
            # 获取视频AID
            try:
                video_aid = int(group_id)
            except (ValueError, TypeError):
                logger.error(f"无效的视频AID: {group_id}")
                return None
            
            # 如果没有明确指定回复目标，尝试从视频AID查找最近收到的评论ID
            if not reply_to_rpid:
                reply_to_rpid = self.video_to_latest_comment.get(video_aid)
                if reply_to_rpid:
                    logger.info(f"未指定回复目标，自动使用视频 {video_aid} 最近收到的评论ID: {reply_to_rpid}")
            
            # 如果有回复目标，使用回复API；否则作为新评论发布
            if reply_to_rpid:
                # 回复指定评论
                logger.info(f"正在回复评论 {reply_to_rpid}，内容: {text_content[:50]}")
                try:
                    # B站的回复需要指定root和parent参数
                    # root: 根评论ID（如果是回复子评论，需要找到根评论）
                    # parent: 父评论ID（直接回复的评论ID）
                    result = await send_comment(
                        text=text_content,
                        oid=video_aid,
                        type_=CommentResourceType.VIDEO,
                        root=reply_to_rpid,  # 根评论ID（简化处理，直接使用回复目标）
                        parent=reply_to_rpid,  # 父评论ID
                        credential=self.credential
                    )
                    
                    if result and result.get('rpid'):
                        reply_id = int(result.get('rpid'))
                        logger.info(f"评论回复成功，回复ID: {reply_id}，回复目标: {reply_to_rpid}")
                        # 【关键】记录机器人发送的评论（必须在检查回复之前记录）
                        self.bot_sent_comments.add(reply_id)
                        logger.info(f"[回复检查] ✓ 已记录机器人发送的评论ID {reply_id} 到bot_sent_comments（总数: {len(self.bot_sent_comments)}）")
                        # 保存到文件
                        self._save_bot_comments()
                        # 记录回复关系
                        if reply_to_rpid:
                            self.comment_to_parent[reply_id] = reply_to_rpid
                            # 【重要】记录被回复的评论ID，避免重复回复
                            # 注意：这里记录的是用户回复机器人的评论ID，不是机器人的评论ID
                            self.replied_comments.add(reply_to_rpid)
                            # 保存到文件
                            self._save_replied_comments()
                        # 记录回复内容到文件
                        self._log_reply(reply_to_rpid, reply_id, text_content)
                        return str(reply_id)
                    else:
                        logger.error(f"评论回复失败: {result}")
                        # 如果是重复评论错误，记录到replied_comments，避免重复尝试
                        if result and result.get('code') == 12051:
                            if reply_to_rpid:
                                self.replied_comments.add(reply_to_rpid)
                                logger.warning(f"[回复检查] 检测到重复评论错误，已记录评论ID {reply_to_rpid} 到replied_comments")
                                # 保存到文件
                                self._save_replied_comments()
                        return None
                except ResponseCodeException as e:
                    logger.error(f"回复评论失败: {e}")
                    # 如果是重复评论错误（错误代码12051），记录到replied_comments
                    error_str = str(e)
                    if '12051' in error_str or '重复评论' in error_str or '刷屏' in error_str:
                        if reply_to_rpid:
                            self.replied_comments.add(reply_to_rpid)
                            logger.warning(f"[回复检查] 检测到重复评论错误（ResponseCodeException），已记录评论ID {reply_to_rpid} 到replied_comments，避免重复回复")
                            # 保存到文件
                            self._save_replied_comments()
                    import traceback
                    logger.debug(traceback.format_exc())
                    return None
                except Exception as e:
                    logger.error(f"回复评论失败: {e}")
                    import traceback
                    logger.debug(traceback.format_exc())
                    return None
            else:
                # 作为新评论发布（不回复任何评论）
                logger.info(f"正在发布新评论到视频 {video_aid}，内容: {text_content[:50]}")
                try:
                    result = await send_comment(
                        text=text_content,
                        oid=video_aid,
                        type_=CommentResourceType.VIDEO,
                        credential=self.credential
                    )
                    
                    if result and result.get('rpid'):
                        comment_id = int(result.get('rpid'))
                        logger.info(f"评论发布成功，评论ID: {comment_id}")
                        # 保存映射关系
                        self.comment_to_video[comment_id] = video_aid
                        # 【关键】记录机器人发送的评论（必须在检查回复之前记录）
                        self.bot_sent_comments.add(comment_id)
                        logger.info(f"[回复检查] ✓ 已记录机器人发送的评论ID {comment_id} 到bot_sent_comments（总数: {len(self.bot_sent_comments)}）")
                        # 保存到文件
                        self._save_bot_comments()
                        return str(comment_id)
                    else:
                        logger.error(f"评论发布失败: {result}")
                        return None
                except Exception as e:
                    logger.error(f"发布评论失败: {e}")
                    import traceback
                    logger.debug(traceback.format_exc())
                    return None
                
        except ResponseCodeException as e:
            logger.error(f"B站API错误: {e}")
            return None
        except Exception as e:
            logger.error(f"发送评论失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None
    
    async def send_direct_message(self, user_id: Union[int, str], send_message_obj: MessageSending) -> Optional[str]:
        """
        发送私信
        
        Args:
            user_id: 用户ID
            send_message_obj: 要发送的消息对象
            
        Returns:
            消息ID（字符串），发送失败返回None
        """
        if not MESSAGE_MODULE_AVAILABLE:
            logger.warning("bilibili_api.message模块不可用，无法发送私信")
            return None
        
        try:
            if not self.credential:
                logger.error("B站凭证未配置，无法发送私信")
                return None
            
            # 提取文本内容
            text_content = ""
            for msg_part in send_message_obj.message_list:
                if isinstance(msg_part, MessageType.Text):
                    text_content += msg_part.text  # 修复：使用 text 而不是 content
                elif isinstance(msg_part, MessageType.Image):
                    text_content += "[图片]"
                elif isinstance(msg_part, MessageType.Reply):
                    text_content += f"[回复:{msg_part.message_id}]"  # 修复：使用 message_id 而不是 reply_id
            
            if not text_content:
                logger.warning("消息内容为空，跳过发送")
                return None
            
            # 发送私信
            result = await send_message(
                credential=self.credential,
                receiver_id=int(user_id),
                text=text_content
            )
            
            if result:
                message_id = str(result.get('message_id', ''))
                logger.info(f"已发送B站私信给 {user_id}: {text_content[:50]}")
                return message_id
            else:
                logger.error(f"发送B站私信失败: {result}")
                return None
                
        except ResponseCodeException as e:
            logger.error(f"B站API错误: {e}")
            return None
        except Exception as e:
            logger.error(f"发送私信失败: {e}")
            return None

