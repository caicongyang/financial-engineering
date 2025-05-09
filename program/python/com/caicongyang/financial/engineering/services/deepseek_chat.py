from typing import List, Dict, Any, Optional
from openai import OpenAI
import os
from dotenv import load_dotenv
from pathlib import Path

# 尝试加载项目根目录的 .env 文件
try:
    # 获取当前文件所在目录
    current_dir = Path(__file__).resolve().parent
    
    # 尝试向上查找项目根目录
    project_root = current_dir.parent  # 项目根目录是当前目录的父目录
    env_path = project_root / '.env'
    
    if env_path.exists():
        print(f"从 {env_path} 加载环境变量")
        load_dotenv(dotenv_path=env_path)
    else:
        # 如果在父目录没找到，尝试当前目录
        print(f"在 {project_root} 未找到 .env 文件，尝试从当前目录加载")
        load_dotenv()
except Exception as e:
    print(f"加载 .env 文件时出错: {e}")
    # 如果出错，尝试默认加载
    load_dotenv()


class DeepSeekChat:
    """DeepSeek Chat API 客户端，使用 OpenAI SDK。"""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: str = "deepseek-reasoner",
        temperature: float = 1.0,
        max_tokens: Optional[int] = None,
        top_p: Optional[float] = None
    ):
        """初始化 DeepSeek Chat 客户端。"""
        
        # 设置 API 密钥，优先使用传入的参数，其次使用环境变量
        self.api_key = api_key or os.getenv("LLM_API_KEY")
        if not self.api_key:
            raise ValueError("未找到 API 密钥，请在 .env 文件中设置 LLM_API_KEY，或者在初始化时提供")
        
        # 设置基础 URL，优先使用传入的参数，其次使用环境变量
        self.base_url = base_url or os.getenv("LLM_BASE_URL") or "https://api.deepseek.com"
        
        print(f"使用 API 密钥: {self.api_key}")
        print(f"使用基础 URL: {self.base_url}")
        
        # 设置其他参数
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.top_p = top_p
        
        # 初始化 OpenAI 客户端 - 只使用必要的参数
        # 避免传递任何可能不支持的参数如 proxies
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
    
    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        top_p: Optional[float] = None,
        stream: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        """发送聊天请求并返回响应。"""
        # 准备参数
        params = {
            "model": self.model,
            "messages": messages,
            "stream": stream
        }
        
        # 添加可选参数
        if temperature is not None:
            params["temperature"] = temperature
        elif self.temperature != 1.0:
            params["temperature"] = self.temperature
            
        if max_tokens is not None:
            params["max_tokens"] = max_tokens
        elif self.max_tokens is not None:
            params["max_tokens"] = self.max_tokens
            
        if top_p is not None:
            params["top_p"] = top_p
        elif self.top_p is not None:
            params["top_p"] = self.top_p
            
        # 添加其他传入的参数
        for key, value in kwargs.items():
            params[key] = value
        
        try:
            # 调用 API
            response = self.client.chat.completions.create(**params)
            return response
        except Exception as e:
            print(f"调用 DeepSeek API 出错: {str(e)}")
            raise
    
    def simple_chat(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        """简单的聊天接口，只需提供用户输入即可。"""
        messages = []
        
        # 如果有系统提示词，将其添加到用户提示词前面
        if system_prompt:
            full_prompt = f"{system_prompt}\n\n{prompt}"
        else:
            full_prompt = prompt
        
        # 只添加用户输入
        messages.append({"role": "user", "content": full_prompt})
        
        try:
            response = self.chat(messages)
            return response.choices[0].message.content
        except Exception as e:
            return f"错误: {str(e)}" 