"""
B站适配器 - 符合KiraAI架构
实现B站私信和评论的自动回复功能
"""
import asyncio
import logging
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
                        
                        logger.debug(f"视频信息 - BV号: {video_id}, AID: {video_aid}, 标题: {video_title}")
                        
                        if not video_aid:
                            logger.warning(f"无法获取视频 {video_id} 的aid，跳过")
                            continue
                        
                        # 首次运行时，加载初始评论列表
                        if first_run:
                            await self._load_initial_comments(video_id, video_aid)
                            first_run = False
                        
                        # 使用get_comments_lazy获取评论（这个函数可以正确获取评论）
                        logger.debug(f"正在获取视频 {video_id} (aid: {video_aid}) 的评论，凭证: {'已配置' if self.credential else '未配置'}")
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
                        
                        # 调试：打印返回数据的结构
                        logger.debug(f"获取视频 {video_id} 的评论数据，keys: {list(comments.keys())}")
                        
                        # get_comments_lazy返回的数据结构不同，直接获取replies
                        replies = comments.get('replies', [])
                        
                        # 检查是否有置顶评论
                        top_comment = comments.get('top')
                        top_replies = comments.get('top_replies', [])
                        
                        # 合并置顶评论和普通评论
                        all_replies = []
                        if top_comment and isinstance(top_comment, dict):
                            logger.debug(f"发现置顶评论: {top_comment.get('rpid', 'unknown')}")
                            all_replies.append(top_comment)
                        
                        if top_replies and isinstance(top_replies, list):
                            logger.debug(f"发现 {len(top_replies)} 条置顶回复")
                            all_replies.extend(top_replies)
                        
                        if replies and isinstance(replies, list):
                            all_replies.extend(replies)
                        
                        # 处理replies为None的情况
                        if not all_replies:
                            logger.debug(f"视频 {video_id} 暂无评论")
                            continue
                        
                        logger.debug(f"获取到 {len(all_replies)} 条评论（包含置顶评论）")
                        replies = all_replies
                        
                        for comment_data in replies[:10]:  # 只处理最新10条
                            if not self.running:
                                break
                            
                            try:
                                # 检查comment_data是否为有效字典
                                if not comment_data or not isinstance(comment_data, dict):
                                    logger.debug(f"跳过无效的评论数据: {comment_data}")
                                    continue
                                
                                comment_id = comment_data.get('rpid')
                                if not comment_id:
                                    continue
                                
                                # 检查是否已处理（包括历史评论）
                                if comment_id in self.processed_comments:
                                    logger.debug(f"评论 {comment_id} 已处理过，跳过")
                                    continue
                                
                                # 检查是否是历史评论（在初始列表中）
                                if comment_id in self.initial_comment_ids:
                                    logger.debug(f"评论 {comment_id} 是历史评论，跳过")
                                    self.processed_comments.add(comment_id)  # 标记为已处理，避免重复检查
                                    continue
                                
                                logger.info(f"发现新评论: {comment_id}")
                                
                                # 检查频率限制
                                if not self._check_rate_limit():
                                    continue
                                
                                # 检查回复间隔
                                now = time.time()
                                if now - self.last_reply_time.get('comment', 0) < self.reply_interval:
                                    continue
                                
                                # 直接使用字典数据，不创建Comment对象
                                await self._process_comment_dict(comment_data, video_aid)
                                await asyncio.sleep(1)
                            except Exception as e:
                                logger.error(f"处理单条评论失败: {e}")
                                continue
                            
                    except Exception as e:
                        logger.error(f"处理视频 {video_id} 的评论失败: {e}")
                
                # 每60秒检查一次
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"监听评论出错: {e}")
                await asyncio.sleep(120)
    
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
                    if reply_to_rpid:
                        logger.debug(f"检测到回复请求，目标评论ID: {reply_to_rpid}")
            
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
                        reply_id = str(result.get('rpid'))
                        logger.info(f"评论回复成功，回复ID: {reply_id}")
                        return reply_id
                    else:
                        logger.error(f"评论回复失败: {result}")
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
                        comment_id = str(result.get('rpid'))
                        logger.info(f"评论发布成功，评论ID: {comment_id}")
                        # 保存映射关系
                        self.comment_to_video[int(comment_id)] = video_aid
                        return comment_id
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
                    text_content += msg_part.content
                elif isinstance(msg_part, MessageType.Image):
                    text_content += "[图片]"
                elif isinstance(msg_part, MessageType.Reply):
                    text_content += f"[回复:{msg_part.reply_id}]"
            
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

