#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
应用工厂，用于创建和配置FastAPI应用
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

class AppFactory:
    """应用工厂类，用于创建和配置FastAPI应用"""
    
    @staticmethod
    def create_app(title: str, description: str, version: str = "1.0.0") -> FastAPI:
        """
        创建并配置FastAPI应用
        
        Args:
            title: 应用标题
            description: 应用描述
            version: 应用版本，默认为"1.0.0"
            
        Returns:
            配置好的FastAPI应用实例
        """
        # 创建FastAPI应用
        app = FastAPI(
            title=title,
            description=description,
            version=version
        )
        
        # 配置CORS
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],  # 在生产环境中应该设置为具体的域名
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        return app 