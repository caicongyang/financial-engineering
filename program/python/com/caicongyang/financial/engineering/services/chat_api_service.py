#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
聊天API服务类，用于注册和处理聊天相关的API路由
"""

from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from com.caicongyang.financial.engineering.web.stock_chat_service import StockChatService

# 请求/响应模型
class MessageRequest(BaseModel):
    message: str

class MessageResponse(BaseModel):
    status: str
    message: str
    conversation_id: Optional[str] = None
    response: Optional[str] = None
    stock_data: Optional[str] = None

class ConversationInfo(BaseModel):
    id: str
    title: str
    created_at: str
    last_active: str

class ChatAPIService:
    """聊天API服务类，处理所有聊天相关的API路由"""
    
    def __init__(self, chat_service: StockChatService):
        """
        初始化聊天API服务
        
        Args:
            chat_service: 聊天服务实例
        """
        self.chat_service = chat_service
        self.router = APIRouter(tags=["chat"])
        self._register_routes()
    
    def _register_routes(self):
        """注册路由处理函数"""
        
        @self.router.post("/conversations", response_model=Dict[str, str])
        async def create_conversation():
            """创建新的对话"""
            conversation_id = self.chat_service.create_new_conversation()
            return {"conversation_id": conversation_id}
        
        @self.router.get("/conversations", response_model=List[ConversationInfo])
        async def list_conversations():
            """列出所有对话"""
            return self.chat_service.list_conversations()
        
        @self.router.delete("/conversations/{conversation_id}", response_model=Dict[str, bool])
        async def delete_conversation(conversation_id: str):
            """删除对话"""
            success = self.chat_service.delete_conversation(conversation_id)
            if not success:
                raise HTTPException(status_code=404, detail="对话不存在")
            return {"success": True}
        
        @self.router.post("/conversations/{conversation_id}/messages", response_model=MessageResponse)
        async def send_message(conversation_id: str, request: MessageRequest):
            """发送消息并获取回复"""
            result = self.chat_service.send_message(conversation_id, request.message)
            
            if result["status"] == "error" and result["conversation_id"] is None:
                raise HTTPException(status_code=404, detail=result["message"])
            
            return result
        
        @self.router.get("/conversations/{conversation_id}/history", response_model=List[Dict[str, str]])
        async def get_conversation_history(conversation_id: str):
            """获取对话历史记录"""
            history = self.chat_service.get_conversation_history(conversation_id)
            if not history and conversation_id not in self.chat_service.conversations:
                raise HTTPException(status_code=404, detail="对话不存在")
            return history 