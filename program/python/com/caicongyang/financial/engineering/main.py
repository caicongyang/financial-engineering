#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
金融工程项目统一入口文件
整合了股票聊天API服务和日常数据处理功能
"""

import os
import sys
import threading
import uvicorn
from dotenv import load_dotenv
import traceback

# 获取项目根目录
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../../"))
# 添加 program/python 到 Python 路径
python_root = os.path.join(project_root, "program", "python")
sys.path.append(python_root)

# 多路径加载环境变量
def load_env_from_multiple_locations():
    """从多个位置尝试加载.env文件"""
    paths = [
        os.path.dirname(os.path.abspath(__file__)),  # 当前目录
        project_root,  # 项目根目录
        os.path.abspath('/app'),  # Docker容器根目录
    ]
    
    for path in paths:
        env_path = os.path.join(path, '.env')
        if os.path.exists(env_path):
            print(f"发现并加载 .env 文件: {env_path}")
            load_dotenv(env_path)
            return True
    
    print("警告: 未找到任何 .env 文件!")
    return False

# 加载环境变量
has_env = load_env_from_multiple_locations()

# 打印环境变量以进行调试
print("\n=== 环境变量信息 ===")
db_host = os.getenv('DB_HOST', '[未设置]')
db_port = os.getenv('DB_PORT', '[未设置]')
db_user = os.getenv('DB_USER', '[未设置]')
db_name = os.getenv('DB_NAME', '[未设置]')
api_key = os.getenv('DEEPSEEK_API_KEY', '[未设置]')

print(f"DB_HOST: {db_host}")
print(f"DB_PORT: {db_port}")
print(f"DB_USER: {db_user}")
print(f"DB_NAME: {db_name}")
print(f"DEEPSEEK_API_KEY: {'[已设置]' if api_key != '[未设置]' else '[未设置]'}")

# 导入服务类
try:
    from com.caicongyang.financial.engineering.services.stock_chat_service import StockChatService
    from com.caicongyang.financial.engineering.services.data_processing_service import DataProcessingService
    from com.caicongyang.financial.engineering.services.scheduler_service import SchedulerService
    from com.caicongyang.financial.engineering.api.chat_api_service import ChatAPIService
    from com.caicongyang.financial.engineering.api.data_api_service import DataAPIService
    from com.caicongyang.financial.engineering.api.analysis_api_service import AnalysisAPIService
    from com.caicongyang.financial.engineering.services.app_factory import AppFactory
    print("✅ 所有模块导入成功")
except Exception as e:
    print(f"❌ 模块导入失败: {str(e)}")
    traceback.print_exc()
    raise

def create_application():
    """创建应用实例"""
    try:
        # 创建基础服务实例
        data_service = DataProcessingService()
        scheduler_service = SchedulerService(data_service)
        chat_service = StockChatService(
            api_key=os.getenv("DEEPSEEK_API_KEY"),
            base_url=os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com/v1")
        )
        
        # 创建API服务
        chat_api = ChatAPIService(chat_service)
        data_api = DataAPIService(data_service, scheduler_service)
        analysis_api = AnalysisAPIService()
        
        # 创建FastAPI应用
        app = AppFactory.create_app(
            title="金融工程平台API",
            description="提供股票分析、聊天功能和数据处理的API",
            version="1.0.0"
        )
        
        # 注册路由
        app.include_router(chat_api.router)
        app.include_router(data_api.router)
        app.include_router(analysis_api.router)
        
        print("✅ 应用创建成功")
        return app, scheduler_service
    except Exception as e:
        print(f"❌ 应用创建失败: {str(e)}")
        traceback.print_exc()
        raise

def main():
    """主函数，同时启动API服务和定时任务"""
    try:
        # 创建应用和服务
        app, scheduler_service = create_application()
        
        # 在单独的线程中启动定时任务
        scheduler_thread = threading.Thread(
            target=scheduler_service.start_scheduler, 
            daemon=True
        )
        scheduler_thread.start()
        
        print("✅ 应用启动中...")
        # 启动API服务（主线程）
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8000
        )
    except Exception as e:
        print(f"❌ 应用启动失败: {str(e)}")
        traceback.print_exc()
        raise

# 创建全局应用实例（用于uvicorn直接加载）
try:
    app, _ = create_application()
except Exception as e:
    print(f"❌ 全局应用实例创建失败: {str(e)}")
    app = None  # 设置为None以便于错误处理

if __name__ == "__main__":
    main() 