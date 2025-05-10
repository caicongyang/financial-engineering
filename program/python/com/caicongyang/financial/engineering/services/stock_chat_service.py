#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票聊天助手服务类，使用langchain实现对话记录功能
支持同一对话的记忆功能和创建新对话
"""

import os
import uuid
import datetime
import akshare as ak
import pandas as pd
from typing import Dict, List, Optional, Any, Union

# langchain相关导入
from langchain.memory import ConversationBufferMemory
from langchain.chains import ConversationChain
from langchain.prompts import PromptTemplate
from langchain.schema import HumanMessage, AIMessage, SystemMessage
from langchain.chat_models.base import BaseChatModel

# deepseek接口
try:
    from openai import OpenAI
except ImportError:
    print("OpenAI client library not installed. Please install it using: pip install openai")


class StockChatService:
    """
    股票聊天服务类，使用langchain实现对话记录功能
    """
    
    def __init__(self, api_key: str = None, base_url: str = None):
        """
        初始化股票聊天服务
        
        Args:
            api_key: API密钥
            base_url: API基础URL，默认为DeepSeek API
        """
        self.api_key = api_key or os.getenv("LLM_API_KEY")
        self.base_url = base_url or os.getenv("LLM_BASE_URL")
        
        # 初始化OpenAI客户端
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
        
        # 系统提示
        self.system_prompt = """
你是 StockBot，一位智能股票助手。你会根据用户提供的股票代码和数据，为用户提供简洁、专业的分析建议。你善于结合技术面、消息面和资金面给出易于理解的分析内容。

在分析时：
- 请先总结用户提供的原始数据。
- 然后指出股票的当前表现、是否处于关键支撑或压力区域。
- 如果有主力资金进出、成交量异动，也请重点指出。
- 最后根据数据给出操作建议，例如：继续观望、短线机会、注意风险等。
"""
        
        # 初始化会话存储
        self.conversations: Dict[str, Dict[str, Any]] = {}
    
    def _get_langchain_model(self) -> BaseChatModel:
        """
        创建一个适配langchain的模型
        
        Returns:
            langchain模型接口
        """
        from langchain.chat_models import ChatOpenAI
        
        # 使用ChatOpenAI作为包装器
        return ChatOpenAI(
            openai_api_key=self.api_key,
            openai_api_base=self.base_url,
            model_name="deepseek-chat"
        )
    
    def create_new_conversation(self) -> str:
        """
        创建新的对话并返回对话ID
        
        Returns:
            对话ID
        """
        conversation_id = str(uuid.uuid4())
        
        # 初始化langchain memory
        memory = ConversationBufferMemory(return_messages=True)
        
        # 添加系统提示
        memory.chat_memory.add_message(SystemMessage(content=self.system_prompt))
        
        # 创建langchain对话链
        prompt_template = """
{history}
Human: {input}
AI: """
        
        prompt = PromptTemplate(
            input_variables=["history", "input"],
            template=prompt_template
        )
        
        conversation = ConversationChain(
            prompt=prompt,
            llm=self._get_langchain_model(),
            memory=memory,
            verbose=False
        )
        
        # 保存对话记录
        self.conversations[conversation_id] = {
            "chain": conversation,
            "memory": memory,
            "created_at": datetime.datetime.now(),
            "last_active": datetime.datetime.now()
        }
        
        return conversation_id
    
    def get_stock_data(self, stock_code: str) -> str:
        """
        获取股票数据并格式化为Markdown表格
        
        Args:
            stock_code: 股票代码
        
        Returns:
            格式化的股票数据
        """
        try:
            # 获取最近5天的数据
            stock_df = ak.stock_zh_a_hist(
                symbol=stock_code, 
                period="daily", 
                start_date=(datetime.datetime.now() - datetime.timedelta(days=10)).strftime('%Y%m%d'), 
                adjust="qfq"
            )
            
            stock_df = stock_df.sort_values(by="日期", ascending=False)
            latest = stock_df.head(5)[["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅"]]
            
            # 格式化为Markdown表格
            markdown_table = latest.to_markdown(index=False)
            return markdown_table
        
        except Exception as e:
            return f"获取股票数据时出错：{str(e)}"
    
    def send_message(self, conversation_id: str, message: str) -> Dict[str, Any]:
        """
        发送消息并获取回复
        
        Args:
            conversation_id: 对话ID
            message: 用户消息
        
        Returns:
            包含回复和状态的字典
        """
        # 检查对话是否存在
        if conversation_id not in self.conversations:
            return {
                "status": "error",
                "message": "对话不存在，请创建新对话",
                "conversation_id": None,
                "response": None
            }
        
        conversation_data = self.conversations[conversation_id]
        conversation = conversation_data["chain"]
        conversation_data["last_active"] = datetime.datetime.now()
        
        # 判断是否是股票代码（6位数字）
        if message.isdigit() and len(message) == 6:
            try:
                # 获取股票数据
                stock_data = self.get_stock_data(message)
                
                # 构建包含股票数据的提示
                enhanced_message = f"这是股票 {message} 的近5日行情数据，请帮我分析一下：\n{stock_data}"
                
                # 使用langchain对话链获取回复
                response = conversation.predict(input=enhanced_message)
                
                return {
                    "status": "success",
                    "message": "已处理股票查询请求",
                    "conversation_id": conversation_id,
                    "stock_data": stock_data,
                    "response": response
                }
            
            except Exception as e:
                error_message = f"处理股票数据时出错：{str(e)}"
                return {
                    "status": "error",
                    "message": error_message,
                    "conversation_id": conversation_id,
                    "response": error_message
                }
        else:
            # 普通聊天消息
            try:
                response = conversation.predict(input=message)
                
                return {
                    "status": "success",
                    "message": "消息发送成功",
                    "conversation_id": conversation_id,
                    "response": response
                }
            
            except Exception as e:
                error_message = f"处理消息时出错：{str(e)}"
                return {
                    "status": "error",
                    "message": error_message,
                    "conversation_id": conversation_id,
                    "response": error_message
                }
    
    def get_conversation_history(self, conversation_id: str) -> List[Dict[str, str]]:
        """
        获取对话历史记录
        
        Args:
            conversation_id: 对话ID
        
        Returns:
            对话历史记录列表
        """
        if conversation_id not in self.conversations:
            return []
        
        conversation_data = self.conversations[conversation_id]
        memory = conversation_data["memory"]
        
        history = []
        for message in memory.chat_memory.messages:
            if isinstance(message, SystemMessage):
                continue
            
            if isinstance(message, HumanMessage):
                history.append({
                    "role": "user",
                    "content": message.content
                })
            elif isinstance(message, AIMessage):
                history.append({
                    "role": "assistant",
                    "content": message.content
                })
        
        return history
    
    def list_conversations(self) -> List[Dict[str, Any]]:
        """
        列出所有对话
        
        Returns:
            对话列表
        """
        result = []
        for conversation_id, data in self.conversations.items():
            # 提取对话的第一条消息作为标题
            messages = self.get_conversation_history(conversation_id)
            title = messages[0]["content"] if messages else "新对话"
            
            # 限制标题长度
            if len(title) > 50:
                title = title[:47] + "..."
            
            result.append({
                "id": conversation_id,
                "title": title,
                "created_at": data["created_at"].isoformat(),
                "last_active": data["last_active"].isoformat()
            })
        
        # 按最后活跃时间排序
        result.sort(key=lambda x: x["last_active"], reverse=True)
        return result
    
    def delete_conversation(self, conversation_id: str) -> bool:
        """
        删除对话
        
        Args:
            conversation_id: 对话ID
        
        Returns:
            是否成功删除
        """
        if conversation_id in self.conversations:
            del self.conversations[conversation_id]
            return True
        return False


# 使用示例
if __name__ == "__main__":
    # 从环境变量获取API密钥
    from dotenv import load_dotenv
    load_dotenv()
    
    # 创建服务实例
    service = StockChatService()
    
    # 创建新对话
    conversation_id = service.create_new_conversation()
    print(f"创建新对话: {conversation_id}")
    
    # 发送消息
    response = service.send_message(conversation_id, "你好，我想了解一下股票市场")
    print(f"AI回复: {response['response']}")
    
    # 查询股票
    response = service.send_message(conversation_id, "000001")
    print(f"股票数据: {response.get('stock_data')}")
    print(f"AI分析: {response['response']}")
    
    # 获取对话历史
    history = service.get_conversation_history(conversation_id)
    print(f"对话历史: {history}") 