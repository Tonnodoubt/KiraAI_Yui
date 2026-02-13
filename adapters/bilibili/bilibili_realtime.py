"""
B站实时评论适配器
专门用于视频发布时的实时评论回复
- 手动启动
- 持续监听配置的视频
- 只处理指定时间窗口内的新评论
- 智能频率控制，避免触发风控
"""
import asyncio
import json
import os
import time
import random
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, timedelta
from collections import defaultdict

from bilibili_api import video, Credential
from bilibili_api.comment import get_comments_lazy, send_comment, CommentResourceType, OrderType
from bilibili_api.exceptions import ResponseCodeException

from core.logging_manager import get_logger
from core.message_filter import message_filter
from utils.adapter_utils import IMAdapter
from utils.message_utils import BotDirectMessage, BotGroupMessage, MessageSending, MessageType

logger = get_logger("bilibili_realtime", "green")


class BilibiliRealtimeAdapter(IMAdapter):
    """
    B站实时评论适配器

    使用方式：
    1. 配置 data/config/realtime.ini
    2. 视频发布前运行: python -m adapters.bilibili.bilibili_realtime
    3. 或者通过主程序启动
    """

    def __init__(self, config: Dict[str, Any], loop: asyncio.AbstractEventLoop, event_bus: asyncio.Queue):
        super().__init__(config, loop, event_bus)
        self.name = "BilibiliRealtime"

        # ============ 基础配置 ============
        self.enabled = config.get('enabled', 'false').lower() == 'true'
        self.sessdata = config.get('sessdata', '')
        self.bili_jct = config.get('bili_jct', '')
        self.dedeuserid = config.get('dedeuserid', '')
        self.buvid3 = config.get('buvid3', '')
        self.ac_time_value = config.get('ac_time_value', '')

        # ============ 监听配置 ============
        # 要监听的视频BV号（逗号分隔）
        monitor_videos = config.get('monitor_videos', '')
        self.monitor_videos = [v.strip() for v in monitor_videos.split(',') if v.strip()]

        # 视频发布时间（格式：2024-01-15 15:00:00，逗号分隔，与monitor_videos一一对应）
        # 如果不设置，则使用监听开始时间作为基准
        release_times = config.get('release_times', '')
        self.release_times = [t.strip() for t in release_times.split('|') if t.strip()]

        # ============ 时间窗口配置 ============
        # 只回复发布后多久内的评论（分钟）
        self.time_window_minutes = int(config.get('time_window_minutes', '120'))  # 默认2小时
        # 轮询间隔（秒）
        self.poll_interval = int(config.get('poll_interval', '15'))  # 默认15秒

        # ============ 频率控制配置 ============
        # 每分钟最大回复数
        self.max_replies_per_minute = int(config.get('max_replies_per_minute', '2'))
        # 每小时最大回复数
        self.max_replies_per_hour = int(config.get('max_replies_per_hour', '20'))
        # 最小回复间隔（秒）
        self.min_interval = int(config.get('min_interval', '30'))
        # 最大回复间隔（秒）
        self.max_interval = int(config.get('max_interval', '90'))

        # ============ 回复策略配置 ============
        # 回复意愿阈值（0-1，越高越严格）
        self.response_threshold = float(config.get('response_threshold', '0.5'))
        # 是否只回复顶级评论
        self.top_level_only = config.get('top_level_only', 'true').lower() == 'true'
        # 是否跳过已有回复的评论
        self.skip_replied = config.get('skip_replied', 'true').lower() == 'true'

        # ============ 精选回复策略（降低风控风险）============
        # 每轮轮询最多回复几条（建议1-2条，降低频率）
        self.max_replies_per_poll = int(config.get('max_replies_per_poll', '2'))
        # 时间窗口内最大总回复数（硬上限，比如整个发布期最多回复15条）
        self.max_total_replies = int(config.get('max_total_replies', '15'))
        # 只回复包含关键词的评论（逗号分隔，留空则不限制）
        priority_keywords = config.get('priority_keywords', '')
        self.priority_keywords = [k.strip() for k in priority_keywords.split(',') if k.strip()]
        # 排除关键词（包含这些的不回复，如"粉丝群"、"群号"等）
        exclude_keywords = config.get('exclude_keywords', '粉丝群,群号,加群,微信群,QQ群')
        self.exclude_keywords = [k.strip() for k in exclude_keywords.split(',') if k.strip()]

        # ============ 验证码冷却配置 ============
        self.captcha_cooldown_seconds = int(config.get('captcha_cooldown_seconds', '7200'))  # 2小时

        # ============ B站凭证 ============
        self.credential: Optional[Credential] = None
        if self.enabled and self.sessdata and self.bili_jct and self.dedeuserid:
            try:
                credential_params = {
                    'sessdata': self.sessdata,
                    'bili_jct': self.bili_jct,
                    'dedeuserid': self.dedeuserid
                }
                if self.buvid3:
                    credential_params['buvid3'] = self.buvid3
                if self.ac_time_value:
                    credential_params['ac_time_value'] = self.ac_time_value
                self.credential = Credential(**credential_params)
                logger.info("B站凭证初始化成功")
            except Exception as e:
                logger.error(f"B站凭证初始化失败: {e}")

        # ============ 运行状态 ============
        self.running = False

        # 视频信息缓存: {bvid: {'aid': int, 'release_time': datetime, 'start_time': datetime}}
        self.video_info: Dict[str, Dict] = {}

        # 已处理的评论ID
        self.processed_comments: Set[int] = set()

        # 已发送的评论ID
        self.sent_comments: Set[int] = set()

        # 频率控制
        self.reply_timestamps: List[float] = []  # 所有回复时间戳
        self.last_reply_time: float = 0

        # 验证码状态
        self.last_captcha_error_time: Optional[float] = None

        # 数据持久化目录
        self.data_dir = "data/bilibili/realtime"
        os.makedirs(self.data_dir, exist_ok=True)

        # 加载持久化数据
        self._load_state()

    def _load_state(self):
        """加载持久化状态"""
        state_file = os.path.join(self.data_dir, "state.json")
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_comments = set(data.get('processed_comments', []))
                    self.sent_comments = set(data.get('sent_comments', []))
                    if data.get('last_captcha_error_time'):
                        self.last_captcha_error_time = data['last_captcha_error_time']
                logger.info(f"[持久化] 加载状态: {len(self.processed_comments)} 条已处理, {len(self.sent_comments)} 条已发送")
            except Exception as e:
                logger.error(f"[持久化] 加载状态失败: {e}")

    def _save_state(self):
        """保存持久化状态"""
        state_file = os.path.join(self.data_dir, "state.json")
        try:
            data = {
                'processed_comments': list(self.processed_comments),
                'sent_comments': list(self.sent_comments),
                'last_captcha_error_time': self.last_captcha_error_time,
                'last_update': datetime.now().isoformat()
            }
            with open(state_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"[持久化] 保存状态失败: {e}")

    def _is_in_captcha_cooldown(self) -> bool:
        """检查是否在验证码冷却期内"""
        if self.last_captcha_error_time is None:
            return False

        elapsed = time.time() - self.last_captcha_error_time
        if elapsed >= self.captcha_cooldown_seconds:
            logger.info(f"[验证码冷却] 冷却期已过，恢复回复")
            self.last_captcha_error_time = None
            self._save_state()
            return False

        remaining = self.captcha_cooldown_seconds - elapsed
        logger.warning(f"[验证码冷却] 处于冷却期，剩余 {remaining/60:.1f} 分钟")
        return True

    def _check_rate_limit(self) -> bool:
        """检查是否超过频率限制"""
        now = time.time()

        # 检查每分钟限制
        minute_ago = now - 60
        replies_last_minute = sum(1 for t in self.reply_timestamps if t > minute_ago)
        if replies_last_minute >= self.max_replies_per_minute:
            logger.warning(f"[频率限制] 每分钟回复数已达上限: {replies_last_minute}/{self.max_replies_per_minute}")
            return False

        # 检查每小时限制
        hour_ago = now - 3600
        self.reply_timestamps = [t for t in self.reply_timestamps if t > hour_ago]
        if len(self.reply_timestamps) >= self.max_replies_per_hour:
            logger.warning(f"[频率限制] 每小时回复数已达上限: {len(self.reply_timestamps)}/{self.max_replies_per_hour}")
            return False

        return True

    def _get_random_interval(self) -> float:
        """获取随机回复间隔"""
        return random.uniform(self.min_interval, self.max_interval)

    async def _init_video_info(self):
        """初始化视频信息"""
        for i, bvid in enumerate(self.monitor_videos):
            try:
                v = video.Video(bvid=bvid, credential=self.credential)
                info = await v.get_info()
                aid = info['aid']

                # 获取发布时间
                release_time = None
                if i < len(self.release_times):
                    try:
                        release_time = datetime.strptime(self.release_times[i], '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        logger.warning(f"[视频初始化] 无法解析发布时间: {self.release_times[i]}")

                self.video_info[bvid] = {
                    'aid': aid,
                    'release_time': release_time,
                    'start_time': datetime.now(),
                    'title': info.get('title', '未知标题')
                }

                logger.info(f"[视频初始化] {bvid} - {info.get('title', '未知')} (AID: {aid})")
                if release_time:
                    logger.info(f"[视频初始化] 发布时间: {release_time}")

            except Exception as e:
                logger.error(f"[视频初始化] 获取视频 {bvid} 信息失败: {e}")

    def _is_comment_in_time_window(self, comment_data: Dict, video_bvid: str) -> bool:
        """检查评论是否在时间窗口内"""
        comment_ctime = comment_data.get('ctime', 0)
        if not comment_ctime:
            return False

        video_info = self.video_info.get(video_bvid, {})

        # 确定基准时间（发布时间或启动时间）
        base_time = video_info.get('release_time') or video_info.get('start_time')
        if not base_time:
            return False

        # 如果是datetime对象，转换为timestamp
        if isinstance(base_time, datetime):
            base_timestamp = base_time.timestamp()
        else:
            base_timestamp = base_time

        # 评论时间必须在基准时间之后
        if comment_ctime < base_timestamp:
            return False

        # 评论时间必须在时间窗口内
        elapsed_minutes = (time.time() - base_timestamp) / 60
        if elapsed_minutes > self.time_window_minutes:
            return False

        return True

    async def _get_comments(self, bvid: str, aid: int) -> List[Dict]:
        """获取视频评论"""
        try:
            comments = await get_comments_lazy(
                oid=aid,
                type_=CommentResourceType.VIDEO,
                order=OrderType.TIME,
                credential=self.credential
            )

            all_comments = []

            # 顶级评论
            replies = comments.get('replies', []) or []
            all_comments.extend(replies)

            # 置顶评论
            top_comment = comments.get('top')
            if top_comment and isinstance(top_comment, dict):
                all_comments.append(top_comment)

            return all_comments

        except Exception as e:
            logger.error(f"[获取评论] 获取视频 {bvid} 评论失败: {e}")
            return []

    def _is_bot_comment(self, comment_data: Dict) -> bool:
        """检查是否是机器人自己的评论"""
        comment_mid = str(comment_data.get('mid', comment_data.get('uid', '')))
        if comment_mid and self.dedeuserid and comment_mid == str(self.dedeuserid):
            return True

        member = comment_data.get('member', {})
        if isinstance(member, dict):
            uname = member.get('uname', member.get('nickname', ''))
            # 可以配置机器人名称
            if uname == "林悠怡Yui":
                return True

        return False

    async def _should_reply(self, comment_data: Dict, all_comments: List[Dict]) -> tuple:
        """
        判断是否应该回复该评论（精选策略）

        Returns:
            (should_reply: bool, reason: str, willingness: float)
        """
        comment_id = comment_data.get('rpid')

        # 1. 跳过已处理的评论
        if comment_id in self.processed_comments:
            return False, "已处理过", 0.0

        # 2. 跳过机器人自己的评论
        if self._is_bot_comment(comment_data):
            return False, "机器人自己的评论", 0.0

        # 3. 检查是否是顶级评论
        parent_id = comment_data.get('parent', 0) or 0
        root_id = comment_data.get('root', 0) or 0
        is_top_level = (parent_id == 0 and root_id == 0)

        if self.top_level_only and not is_top_level:
            return False, "非顶级评论", 0.0

        # 4. 检查是否已有回复
        if self.skip_replied:
            replies = comment_data.get('replies', []) or []
            if len(replies) > 0:
                for reply in replies:
                    if self._is_bot_comment(reply):
                        return False, "已有机器人回复", 0.0

        # 5. 获取评论内容
        content = comment_data.get('content', {})
        if isinstance(content, dict):
            message = content.get('message', '')
        else:
            message = str(content)

        if not message.strip():
            return False, "评论内容为空", 0.0

        # 6. 【新增】排除关键词检查（规避广告、引流等）
        message_lower = message.lower()
        for exclude_keyword in self.exclude_keywords:
            if exclude_keyword.lower() in message_lower:
                return False, f"包含排除关键词: {exclude_keyword}", 0.0

        # 7. 【新增】检查总回复数是否已达上限
        if len(self.sent_comments) >= self.max_total_replies:
            return False, f"已达总回复上限 {self.max_total_replies}", 0.0

        # 8. 使用 message_filter 评估回复意愿
        should_respond, filter_reason, willingness = await message_filter.filter_message(message)

        # 9. 【新增】优先关键词加成
        priority_bonus = 0.0
        if self.priority_keywords:
            for keyword in self.priority_keywords:
                if keyword.lower() in message_lower:
                    priority_bonus = 0.3  # 优先关键词加成0.3
                    filter_reason += f"[含关键词:{keyword}]"
                    break

        final_willingness = willingness + priority_bonus

        if final_willingness < self.response_threshold:
            return False, f"回复意愿 {final_willingness:.2f} < 阈值 {self.response_threshold}", final_willingness

        return True, f"通过筛选 ({filter_reason})", final_willingness

    async def _send_reply(self, comment_data: Dict, aid: int, bvid: str) -> bool:
        """发送回复"""
        comment_id = comment_data.get('rpid')
        content = comment_data.get('content', {})
        if isinstance(content, dict):
            message = content.get('message', '')
        else:
            message = str(content)

        member = comment_data.get('member', {})
        user_name = member.get('uname', member.get('nickname', '未知用户')) if isinstance(member, dict) else '未知用户'

        # 构建回复消息
        from core.llm_manager import llm_api

        # 读取人设
        try:
            with open("prompts/persona.txt", "r", encoding="utf-8") as f:
                persona = f.read()
        except:
            persona = "你是一个友善的AI助手"

        prompt = f"""你是一个B站视频作者，收到了一条粉丝评论。

粉丝昵称：{user_name}
评论内容：{message}

请用简短、友善、有趣的方式回复这条评论（30字以内）。
你的性格设定：
{persona[:500]}

直接输出回复内容，不要有其他解释。"""

        try:
            messages = [{"role": "user", "content": prompt}]
            reply_text, _ = await llm_api.chat(messages)

            # 清理回复文本
            reply_text = reply_text.strip()
            if len(reply_text) > 100:
                reply_text = reply_text[:100] + "..."

        except Exception as e:
            logger.error(f"[生成回复] 失败: {e}")
            return False

        if not reply_text:
            logger.warning(f"[生成回复] 生成的回复为空")
            return False

        # 发送评论
        try:
            result = await send_comment(
                text=reply_text,
                oid=aid,
                type_=CommentResourceType.VIDEO,
                root=comment_id,
                parent=comment_id,
                credential=self.credential
            )

            if result and result.get('rpid'):
                reply_id = result.get('rpid')

                # 更新状态
                self.sent_comments.add(reply_id)
                self.processed_comments.add(comment_id)
                self.reply_timestamps.append(time.time())
                self.last_reply_time = time.time()
                self._save_state()

                logger.info(f"\n{'='*60}\n[✓回复成功]\n视频: {bvid}\n用户: {user_name}\n评论: {message[:50]}\n回复: {reply_text}\n{'='*60}\n")
                return True
            else:
                error_msg = result.get('message', 'unknown') if result else 'no_result'
                logger.error(f"[回复失败] {error_msg}")
                return False

        except ResponseCodeException as e:
            error_str = str(e)

            # 检测验证码错误
            if '验证码' in error_str or 'captcha' in error_str.lower() or (hasattr(e, 'code') and e.code in [-105, -400, 1200015]):
                logger.error(f"[验证码错误] 触发验证码，进入冷却期")
                self.last_captcha_error_time = time.time()
                self._save_state()
                return False

            logger.error(f"[回复异常] {error_str}")
            return False

        except Exception as e:
            logger.error(f"[回复异常] {e}")
            return False

    async def _monitor_video(self, bvid: str):
        """监听单个视频的评论"""
        video_info = self.video_info.get(bvid)
        if not video_info:
            logger.error(f"[监听] 视频 {bvid} 信息未初始化")
            return

        aid = video_info['aid']
        title = video_info.get('title', '未知')
        base_time = video_info.get('release_time') or video_info.get('start_time')

        logger.info(f"[监听开始] {bvid} - {title}")
        logger.info(f"[监听开始] 基准时间: {base_time}, 时间窗口: {self.time_window_minutes}分钟")

        while self.running:
            try:
                # 检查时间窗口
                if base_time:
                    if isinstance(base_time, datetime):
                        base_timestamp = base_time.timestamp()
                    else:
                        base_timestamp = base_time

                    elapsed_minutes = (time.time() - base_timestamp) / 60
                    if elapsed_minutes > self.time_window_minutes:
                        logger.info(f"[监听结束] {bvid} 时间窗口已过 ({elapsed_minutes:.1f}分钟)")
                        break

                # 检查验证码冷却
                if self._is_in_captcha_cooldown():
                    await asyncio.sleep(60)
                    continue

                # 获取评论
                comments = await self._get_comments(bvid, aid)

                if not comments:
                    logger.debug(f"[轮询] {bvid} 暂无评论")
                    await asyncio.sleep(self.poll_interval)
                    continue

                # 筛选新评论
                new_comments = []
                for comment in comments:
                    comment_id = comment.get('rpid')
                    if comment_id and comment_id not in self.processed_comments:
                        # 检查时间窗口
                        if self._is_comment_in_time_window(comment, bvid):
                            new_comments.append(comment)

                if new_comments:
                    logger.info(f"[轮询] {bvid} 发现 {len(new_comments)} 条新评论")
                    # 显示统计信息
                    logger.info(f"[统计] 总回复: {len(self.sent_comments)}/{self.max_total_replies}, "
                               f"已处理: {len(self.processed_comments)}, "
                               f"本轮已回: 0/{self.max_replies_per_poll}")

                    # 按热度排序（点赞数多的优先）
                    new_comments.sort(
                        key=lambda c: c.get('like', 0) or 0,
                        reverse=True
                    )
                    top_likes = [c.get('like', 0) for c in new_comments[:3]]
                    logger.info(f"[排序] 按热度排序，前3条点赞数: {top_likes}")

                # 处理新评论
                replied_this_poll = 0  # 本轮已回复数量
                for comment in new_comments:
                    if not self.running:
                        break

                    # 【新增】检查本轮回复上限
                    if replied_this_poll >= self.max_replies_per_poll:
                        logger.info(f"[本轮上限] 本轮已回复 {replied_this_poll} 条，跳过剩余评论")
                        break

                    # 【新增】检查总回复上限
                    if len(self.sent_comments) >= self.max_total_replies:
                        logger.info(f"[总上限] 已达总回复上限 {self.max_total_replies}，停止回复")
                        break

                    # 检查频率限制
                    if not self._check_rate_limit():
                        logger.info("[频率限制] 达到限制，暂停回复")
                        await asyncio.sleep(60)
                        continue

                    # 评估是否回复
                    should_reply, reason, willingness = await self._should_reply(comment, comments)

                    # 标记为已处理（无论是否回复）
                    self.processed_comments.add(comment.get('rpid'))

                    if should_reply:
                        # 随机间隔
                        interval = self._get_random_interval()
                        logger.info(f"[回复] 准备回复评论 {comment.get('rpid')}，等待 {interval:.1f} 秒")
                        await asyncio.sleep(interval)

                        # 再次检查状态
                        if not self.running or self._is_in_captcha_cooldown():
                            break

                        # 发送回复
                        success = await self._send_reply(comment, aid, bvid)

                        if success:
                            replied_this_poll += 1
                            self.last_reply_time = time.time()
                    else:
                        logger.debug(f"[跳过] 评论 {comment.get('rpid')}: {reason}")

                # 保存状态
                self._save_state()

                # 等待下次轮询
                await asyncio.sleep(self.poll_interval)

            except Exception as e:
                logger.error(f"[监听异常] {bvid}: {e}")
                await asyncio.sleep(30)

        logger.info(f"[监听结束] {bvid}")

    async def start(self):
        """启动适配器"""
        if not self.enabled:
            logger.warning("实时评论适配器未启用")
            return

        if not self.credential:
            logger.error("B站凭证未配置")
            return

        if not self.monitor_videos:
            logger.error("未配置监听视频")
            return

        if self.running:
            logger.warning("适配器已在运行")
            return

        self.running = True
        logger.info("=" * 60)
        logger.info("启动B站实时评论适配器")
        logger.info(f"监听视频: {self.monitor_videos}")
        logger.info(f"时间窗口: {self.time_window_minutes} 分钟")
        logger.info(f"频率限制: {self.max_replies_per_minute}条/分钟, {self.max_replies_per_hour}条/小时")
        logger.info(f"回复间隔: {self.min_interval}-{self.max_interval} 秒")
        logger.info("=" * 60)

        # 初始化视频信息
        await self._init_video_info()

        # 启动监听任务
        tasks = []
        for bvid in self.monitor_videos:
            if bvid in self.video_info:
                task = asyncio.create_task(self._monitor_video(bvid))
                tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks)

        logger.info("实时评论适配器已停止")

    async def stop(self):
        """停止适配器"""
        logger.info("正在停止实时评论适配器...")
        self.running = False
        self._save_state()

    # ============ IMAdapter 接口实现 ============

    async def send_group_message(self, group_id, send_message_obj) -> Optional[str]:
        """发送群消息（实时模式不使用）"""
        return None

    async def send_direct_message(self, user_id, send_message_obj) -> Optional[str]:
        """发送私聊消息（实时模式不使用）"""
        return None


# ============ 独立运行入口 ============

def clear_state():
    """清除持久化状态"""
    import shutil
    state_dir = Path("data/bilibili/realtime")
    if state_dir.exists():
        shutil.rmtree(state_dir)
        print(f"已清除状态目录: {state_dir}")
    else:
        print("状态目录不存在，无需清除")


async def main():
    """独立运行入口"""
    import configparser
    import sys
    from pathlib import Path

    # 检查命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] == "--clear":
            clear_state()
            return
        elif sys.argv[1] == "--help":
            print("用法:")
            print("  python -m adapters.bilibili.bilibili_realtime       # 启动监听")
            print("  python -m adapters.bilibili.bilibili_realtime --clear  # 清除状态")
            print("  python -m adapters.bilibili.bilibili_realtime --help   # 显示帮助")
            return

    # 加载配置
    config_path = Path("data/config/realtime.ini")
    if not config_path.exists():
        logger.error(f"配置文件不存在: {config_path}")
        logger.info("请复制 realtime_example.ini 为 realtime.ini 并填入配置")
        return

    config = configparser.RawConfigParser()
    config.read(config_path, encoding='utf-8')

    config_dict = dict(config.items('realtime'))

    # 创建适配器
    loop = asyncio.get_event_loop()
    event_bus = asyncio.Queue()
    adapter = BilibiliRealtimeAdapter(config_dict, loop, event_bus)

    # Windows兼容的信号处理
    import signal
    def signal_handler(sig, frame):
        logger.info("收到停止信号，正在停止...")
        adapter.running = False

    # Windows只支持SIGINT (Ctrl+C)
    signal.signal(signal.SIGINT, signal_handler)
    try:
        signal.signal(signal.SIGTERM, signal_handler)
    except:
        pass  # Windows不支持SIGTERM

    # 启动
    await adapter.start()


if __name__ == "__main__":
    asyncio.run(main())
