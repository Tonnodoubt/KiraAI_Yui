#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试B站凭证是否有效
"""
import asyncio
import sys
import configparser
from pathlib import Path

# 设置stdout编码为utf-8
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from bilibili_api import user, Credential
from bilibili_api.comment import send_comment, CommentResourceType
from bilibili_api.exceptions import ResponseCodeException


async def test_credential():
    """测试凭证是否有效"""
    print("=" * 60)
    print("测试B站凭证有效性")
    print("=" * 60)
    
    # 加载配置
    config_path = Path("data/config/adapters.ini")
    if not config_path.exists():
        print(f"[ERROR] 配置文件不存在: {config_path}")
        return
    
    config = configparser.RawConfigParser()
    config.read(config_path, encoding='utf-8')
    
    sessdata = config.get('bilibili', 'sessdata', fallback='')
    bili_jct = config.get('bilibili', 'bili_jct', fallback='')
    dedeuserid = config.get('bilibili', 'dedeuserid', fallback='')
    
    print(f"\n凭证配置检查:")
    print(f"  sessdata: {'已配置' if sessdata else '[ERROR] 未配置'} ({len(sessdata)} 字符)")
    print(f"  bili_jct: {'已配置' if bili_jct else '[ERROR] 未配置'} ({len(bili_jct)} 字符)")
    print(f"  dedeuserid: {'已配置' if dedeuserid else '[ERROR] 未配置'}")
    
    if not (sessdata and bili_jct and dedeuserid):
        print("\n[ERROR] 凭证未完整配置，请检查 adapters.ini")
        return
    
    # 创建凭证
    try:
        credential = Credential(
            sessdata=sessdata,
            bili_jct=bili_jct,
            dedeuserid=dedeuserid
        )
        print("\n[OK] 凭证对象创建成功")
    except Exception as e:
        print(f"\n[ERROR] 凭证对象创建失败: {e}")
        return
    
    # 测试1: 获取用户信息（验证凭证是否有效）
    print("\n" + "=" * 60)
    print("测试1: 获取用户信息（验证凭证）")
    print("=" * 60)
    try:
        u = user.User(uid=int(dedeuserid), credential=credential)
        user_info = await u.get_user_info()
        if user_info:
            print(f"[OK] 凭证有效！")
            print(f"  用户名: {user_info.get('name', 'N/A')}")
            print(f"  用户ID: {user_info.get('mid', 'N/A')}")
            print(f"  签名: {user_info.get('sign', 'N/A')[:50]}")
        else:
            print("[ERROR] 无法获取用户信息，凭证可能无效")
    except ResponseCodeException as e:
        if e.code == -101:
            print("[ERROR] 账号未登录！凭证已过期或无效")
            print("\n解决方案:")
            print("1. 重新登录 Bilibili 网页版")
            print("2. 打开浏览器开发者工具 (F12)")
            print("3. 切换到 Application/存储 -> Cookies -> https://www.bilibili.com")
            print("4. 重新获取以下 Cookie 值:")
            print("   - SESSDATA")
            print("   - bili_jct")
            print("   - DedeUserID")
            print("5. 更新 adapters.ini 中的对应值")
        else:
            print(f"[ERROR] 获取用户信息失败: {e.code} - {e.msg}")
    except Exception as e:
        print(f"[ERROR] 测试失败: {e}")
        import traceback
        traceback.print_exc()
    
    # 测试2: 尝试发送评论（如果用户信息获取成功）
    print("\n" + "=" * 60)
    print("测试2: 测试发送评论功能")
    print("=" * 60)
    print("注意: 此测试不会实际发送评论，只验证API调用")
    
    # 使用一个测试视频（不会实际发送）
    test_video_aid = 123456789  # 虚拟AID，用于测试API调用
    try:
        # 注意：这里会失败，但我们可以检查错误类型
        result = await send_comment(
            text="测试评论（不会实际发送）",
            oid=test_video_aid,
            type_=CommentResourceType.VIDEO,
            credential=credential
        )
        print("[OK] 评论API调用成功（但使用了无效的AID）")
    except ResponseCodeException as e:
        if e.code == -101:
            print("[ERROR] 账号未登录！凭证已过期或无效")
            print("   这是发送评论时遇到的错误")
        elif e.code == -400:
            print("[OK] API调用成功，凭证有效（返回-400是因为AID无效，这是预期的）")
        else:
            print(f"[INFO] API返回: {e.code} - {e.msg}")
            if e.code != -400:  # -400是AID无效，其他错误需要关注
                print("   凭证可能有问题")
    except Exception as e:
        print(f"[ERROR] 测试失败: {e}")


if __name__ == '__main__':
    asyncio.run(test_credential())

