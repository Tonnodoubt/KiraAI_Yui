#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
B站适配器测试脚本
用于测试适配器是否能正常启动和运行
"""
import asyncio
import sys
import io
from pathlib import Path

# 设置Windows控制台编码
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

async def test_bilibili_api():
    """测试bilibili-api的基本功能"""
    print("=" * 50)
    print("测试1: 检查bilibili-api导入")
    print("=" * 50)
    
    try:
        from bilibili_api import Credential, video, comment
        print("[OK] bilibili_api 基础模块导入成功")
        
        # 检查comment模块
        from bilibili_api.comment import Comment, CommentResourceType, send_comment
        print("[OK] comment 模块导入成功")
        
        # 检查是否有message模块
        try:
            from bilibili_api import message
            print("[OK] message 模块存在")
            print(f"   message模块内容: {dir(message)[:10]}")
        except ImportError:
            print("[WARN] message 模块不存在，可能需要使用其他方式处理私信")
            # 检查是否有其他私信相关API
            try:
                from bilibili_api import user
                print(f"   user模块内容: {[x for x in dir(user) if 'message' in x.lower() or 'msg' in x.lower() or 'whisper' in x.lower()]}")
            except:
                pass
        
    except Exception as e:
        print(f"[ERROR] 导入失败: {e}")
        return False
    
    return True

async def test_credential():
    """测试凭证创建"""
    print("\n" + "=" * 50)
    print("测试2: 检查凭证配置")
    print("=" * 50)
    
    import configparser
    config_path = Path("data/config/adapters.ini")
    
    if not config_path.exists():
        print(f"[ERROR] 配置文件不存在: {config_path}")
        return False
    
    config = configparser.RawConfigParser()
    config.read(config_path, encoding='utf-8')
    
    if not config.has_section('bilibili'):
        print("[ERROR] 配置文件中没有[bilibili]节")
        return False
    
    sessdata = config.get('bilibili', 'sessdata', fallback='')
    bili_jct = config.get('bilibili', 'bili_jct', fallback='')
    dedeuserid = config.get('bilibili', 'dedeuserid', fallback='')
    enabled = config.get('bilibili', 'enabled', fallback='false').lower() == 'true'
    
    print(f"enabled: {enabled}")
    print(f"sessdata: {'已配置' if sessdata and sessdata != '你的SESSDATA值' else '[ERROR] 未配置'}")
    print(f"bili_jct: {'已配置' if bili_jct and bili_jct != '你的bili_jct值' else '[ERROR] 未配置'}")
    print(f"dedeuserid: {'已配置' if dedeuserid and dedeuserid != '你的DedeUserID值' else '[ERROR] 未配置'}")
    
    if not enabled:
        print("[WARN] 适配器未启用 (enabled = false)")
        return False
    
    if not sessdata or sessdata == '你的SESSDATA值':
        print("[ERROR] sessdata 未正确配置")
        return False
    
    if not bili_jct or bili_jct == '你的bili_jct值':
        print("[ERROR] bili_jct 未正确配置")
        return False
    
    if not dedeuserid or dedeuserid == '你的DedeUserID值':
        print("[ERROR] dedeuserid 未正确配置")
        return False
    
    # 尝试创建凭证
    try:
        from bilibili_api import Credential
        credential = Credential(
            sessdata=sessdata,
            bili_jct=bili_jct,
            dedeuserid=dedeuserid
        )
        print("[OK] 凭证创建成功")
        return True
    except Exception as e:
        print(f"[ERROR] 凭证创建失败: {e}")
        return False

async def test_adapter_import():
    """测试适配器导入"""
    print("\n" + "=" * 50)
    print("测试3: 检查适配器导入")
    print("=" * 50)
    
    try:
        from adapters.bilibili import BilibiliAdapter
        print("[OK] BilibiliAdapter 导入成功")
        return True
    except Exception as e:
        print(f"[ERROR] 适配器导入失败: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_adapter_init():
    """测试适配器初始化"""
    print("\n" + "=" * 50)
    print("测试4: 检查适配器初始化")
    print("=" * 50)
    
    try:
        import configparser
        from adapters.bilibili import BilibiliAdapter
        import asyncio
        
        config_path = Path("data/config/adapters.ini")
        config = configparser.RawConfigParser()
        config.read(config_path, encoding='utf-8')
        
        # 构建配置字典
        adapter_config = {}
        for key, value in config.items('bilibili'):
            adapter_config[key] = value
        adapter_config['adapter_name'] = 'bilibili'
        
        # 创建事件循环
        loop = asyncio.get_event_loop()
        event_bus = asyncio.Queue()
        
        # 初始化适配器
        adapter = BilibiliAdapter(adapter_config, loop, event_bus)
        print("[OK] 适配器初始化成功")
        print(f"   适配器名称: {adapter.name}")
        print(f"   是否启用: {adapter.enabled}")
        print(f"   回复私信: {adapter.reply_private_messages}")
        print(f"   回复评论: {adapter.reply_comments}")
        
        return True
    except Exception as e:
        print(f"[ERROR] 适配器初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """主测试函数"""
    print("B站适配器测试")
    print("=" * 50)
    
    results = []
    
    # 测试1: API导入
    results.append(await test_bilibili_api())
    
    # 测试2: 凭证配置
    results.append(await test_credential())
    
    # 测试3: 适配器导入
    results.append(await test_adapter_import())
    
    # 测试4: 适配器初始化
    if all(results[:3]):  # 只有前面的测试都通过才测试初始化
        results.append(await test_adapter_init())
    
    # 总结
    print("\n" + "=" * 50)
    print("测试总结")
    print("=" * 50)
    
    if all(results):
        print("[OK] 所有测试通过！可以启动KiraAI测试适配器了")
        print("\n下一步:")
        print("1. 运行: python main.py")
        print("2. 查看日志确认适配器是否正常启动")
        print("3. 发送测试私信或评论验证功能")
    else:
        print("[ERROR] 部分测试失败，请根据上面的错误信息修复问题")
        print("\n常见问题:")
        print("1. 检查配置文件中的凭证是否正确填写")
        print("2. 检查dedeuserid是否填写（不是'你的DedeUserID值'）")
        print("3. 检查enabled是否为true")

if __name__ == '__main__':
    asyncio.run(main())

