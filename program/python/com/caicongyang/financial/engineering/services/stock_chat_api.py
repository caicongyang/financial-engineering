#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票聊天API服务，使用FastAPI提供Web接口
"""

import os
import sys
import uvicorn
from dotenv import load_dotenv

# 获取项目根目录（确保能正确导入模块）
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../../../"))
# 添加 program/python 到 Python 路径
python_root = os.path.join(project_root, "program", "python")
sys.path.append(python_root)

# 导入服务类
from com.caicongyang.financial.engineering.services.stock_chat_service import StockChatService
from com.caicongyang.financial.engineering.services.chat_api_service import ChatAPIService
from com.caicongyang.financial.engineering.services.app_factory import AppFactory

# 加载环境变量
load_dotenv()

def create_application():
    """创建聊天API应用实例"""
    
    # 创建聊天服务实例
    chat_service = StockChatService(
        api_key=os.getenv("DEEPSEEK_API_KEY"),
        base_url=os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com/v1")
    )
    
    # 创建聊天API服务
    chat_api = ChatAPIService(chat_service)
    
    # 创建FastAPI应用
    app = AppFactory.create_app(
        title="股票聊天API",
        description="提供股票分析和聊天功能的API",
        version="1.0.0"
    )
    
    # 注册路由
    app.include_router(chat_api.router)
    
    # 添加健康检查端点
    @app.get("/health")
    async def health_check():
        """健康检查"""
        return {"status": "ok"}
    
    return app

# 创建全局应用实例（用于uvicorn直接加载）
app = create_application()

# 如果是主模块，则运行API服务
if __name__ == "__main__":
    # 运行FastAPI服务
    uvicorn.run(
        "stock_chat_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    ) 