#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试获取B站视频评论
"""
import asyncio
import sys
import configparser
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from bilibili_api import video, Credential
from bilibili_api.comment import get_comments, CommentResourceType

async def test_get_comments():
    """测试获取评论"""
    # 加载配置
    config_path = Path("data/config/adapters.ini")
    config = configparser.RawConfigParser()
    config.read(config_path, encoding='utf-8')
    
    sessdata = config.get('bilibili', 'sessdata', fallback='')
    bili_jct = config.get('bilibili', 'bili_jct', fallback='')
    dedeuserid = config.get('bilibili', 'dedeuserid', fallback='')
    
    print(f"Sessdata: {'已配置' if sessdata else '未配置'}")
    print(f"Bili_jct: {'已配置' if bili_jct else '未配置'}")
    print(f"Dedeuserid: {'已配置' if dedeuserid else '未配置'}")
    
    # 创建凭证
    credential = Credential(
        sessdata=sessdata,
        bili_jct=bili_jct,
        dedeuserid=dedeuserid
    )
    
    # 测试视频
    bvid = 'BV1FFU5B8EvN'
    print(f"\n测试视频: {bvid}")
    
    # 获取视频信息
    v = video.Video(bvid=bvid, credential=credential)
    video_info = await v.get_info()
    video_aid = video_info.get('aid', 0)
    video_bvid = video_info.get('bvid', '')
    video_title = video_info.get('title', '未知')
    video_stat = video_info.get('stat', {})
    
    print(f"视频AID: {video_aid}")
    print(f"视频BVID: {video_bvid}")
    print(f"视频标题: {video_title}")
    print(f"视频统计信息:")
    print(f"  播放数: {video_stat.get('view', 0)}")
    print(f"  点赞数: {video_stat.get('like', 0)}")
    print(f"  评论数: {video_stat.get('reply', 0)}")
    print(f"  收藏数: {video_stat.get('favorite', 0)}")
    
    # 检查评论是否被关闭
    rights = video_info.get('rights', {})
    print(f"视频权限:")
    print(f"  评论是否关闭: {rights.get('no_reprint', 0)}")
    print(f"  是否禁止评论: {rights.get('no_comment', 0)}")
    
    # 检查是否有评论相关设置
    print(f"\n视频其他信息:")
    print(f"  cid: {video_info.get('cid', 'N/A')}")
    print(f"  tid: {video_info.get('tid', 'N/A')}")
    
    # 尝试使用Video对象的get_comments方法（如果存在）
    if hasattr(v, 'get_comments'):
        print("\n=== 测试: 使用Video.get_comments ===")
        try:
            video_comments = await v.get_comments()
            print(f"返回类型: {type(video_comments)}")
            if isinstance(video_comments, dict):
                print(f"keys: {list(video_comments.keys())}")
        except Exception as e:
            print(f"Video.get_comments不可用: {e}")
    
    # 获取评论（不带凭证）
    print("\n=== 测试1: 不带凭证获取评论 ===")
    try:
        comments_no_auth = await get_comments(
            oid=video_aid,
            type_=CommentResourceType.VIDEO,
            credential=None
        )
        page_info = comments_no_auth.get('page', {})
        print(f"评论总数: {page_info.get('count', 0)}")
        print(f"实际评论数: {page_info.get('acount', 0)}")
        replies = comments_no_auth.get('replies', [])
        print(f"replies数量: {len(replies) if replies else 0}")
        print(f"replies类型: {type(replies)}")
    except Exception as e:
        print(f"错误: {e}")
    
    # 获取评论（带凭证，指定page_index=1）
    print("\n=== 测试2: 带凭证获取评论 (page_index=1) ===")
    try:
        from bilibili_api.comment import OrderType
        comments_with_auth = await get_comments(
            oid=video_aid,
            type_=CommentResourceType.VIDEO,
            page_index=1,  # 明确指定第一页
            order=OrderType.TIME,  # 按时间排序
            credential=credential
        )
        print(f"返回数据keys: {list(comments_with_auth.keys())}")
        page_info = comments_with_auth.get('page', {})
        print(f"page信息: {page_info}")
        print(f"评论总数: {page_info.get('count', 0)}")
        print(f"实际评论数: {page_info.get('acount', 0)}")
        print(f"当前页: {page_info.get('num', 0)}")
        print(f"每页大小: {page_info.get('size', 0)}")
        
        replies = comments_with_auth.get('replies')
        print(f"replies值: {replies}")
        print(f"replies类型: {type(replies)}")
        
        if replies:
            if isinstance(replies, list):
                print(f"replies数量: {len(replies)}")
                if len(replies) > 0:
                    print(f"\n第一条评论:")
                    first_reply = replies[0]
                    print(f"  rpid: {first_reply.get('rpid', 'N/A')}")
                    content = first_reply.get('content', {})
                    if isinstance(content, dict):
                        print(f"  内容: {content.get('message', 'N/A')[:50]}")
                    else:
                        print(f"  内容: {str(content)[:50]}")
            else:
                print(f"replies不是列表: {replies}")
        else:
            print("replies为None或空")
            
        # 检查是否有top评论
        top = comments_with_auth.get('top')
        if top:
            print(f"\n置顶评论: {top}")
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    
    # 测试使用get_comments_lazy
    print("\n=== 测试3: 使用get_comments_lazy ===")
    try:
        from bilibili_api.comment import get_comments_lazy, OrderType
        comments_lazy = await get_comments_lazy(
            oid=video_aid,
            type_=CommentResourceType.VIDEO,
            order=OrderType.TIME,
            credential=credential
        )
        print(f"get_comments_lazy返回类型: {type(comments_lazy)}")
        if isinstance(comments_lazy, dict):
            print(f"返回数据keys: {list(comments_lazy.keys())}")
            page_info = comments_lazy.get('page', {})
            print(f"评论总数: {page_info.get('count', 0)}")
            replies = comments_lazy.get('replies', [])
            print(f"replies数量: {len(replies) if replies else 0}")
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(test_get_comments())

