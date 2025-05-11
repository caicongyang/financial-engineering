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

# 获取项目根目录
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../../"))
# 添加 program/python 到 Python 路径
python_root = os.path.join(project_root, "program", "python")
sys.path.append(python_root)

# 导入服务类
from com.caicongyang.financial.engineering.services.stock_chat_service import StockChatService
from com.caicongyang.financial.engineering.services.data_processing_service import DataProcessingService
from com.caicongyang.financial.engineering.services.scheduler_service import SchedulerService
from com.caicongyang.financial.engineering.services.chat_api_service import ChatAPIService
from com.caicongyang.financial.engineering.services.data_api_service import DataAPIService
from com.caicongyang.financial.engineering.services.analysis_api_service import AnalysisAPIService
from com.caicongyang.financial.engineering.services.app_factory import AppFactory

# 加载环境变量
load_dotenv()

def create_application():
    """创建应用实例"""
    
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
    
    return app, scheduler_service

def main():
    """主函数，同时启动API服务和定时任务"""
    # 创建应用和服务
    app, scheduler_service = create_application()
    
    # 在单独的线程中启动定时任务
    scheduler_thread = threading.Thread(
        target=scheduler_service.start_scheduler, 
        daemon=True
    )
    scheduler_thread.start()
    
    # 启动API服务（主线程）
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )

# 创建全局应用实例（用于uvicorn直接加载）
app, _ = create_application()

if __name__ == "__main__":
    main() 