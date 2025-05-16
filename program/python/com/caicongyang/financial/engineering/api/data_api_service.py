#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理API服务类，用于注册和处理数据处理相关的API路由
"""

import threading
from typing import Dict
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from com.caicongyang.financial.engineering.services.data_processing_service import DataProcessingService
from com.caicongyang.financial.engineering.services.scheduler_service import SchedulerService

class ScheduleTimeRequest(BaseModel):
    time: str

class DataAPIService:
    """数据处理API服务类，处理所有数据处理相关的API路由"""
    
    def __init__(self, data_service: DataProcessingService, scheduler_service: SchedulerService):
        """
        初始化数据处理API服务
        
        Args:
            data_service: 数据处理服务实例
            scheduler_service: 调度器服务实例
        """
        self.data_service = data_service
        self.scheduler_service = scheduler_service
        self.router = APIRouter(tags=["data"])
        self._register_routes()
    
    def _register_routes(self):
        """注册路由处理函数"""
        
        @self.router.post("/process-daily-data/{date}")
        async def trigger_daily_process(date: str):
            """手动触发每日数据处理"""
            try:
                # 在后台线程中执行处理任务，避免阻塞API
                thread = threading.Thread(target=self.data_service.process_daily_data, args=(date,))
                thread.start()
                return {"status": "processing", "message": f"开始处理 {date} 的数据"}
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"触发数据处理失败: {str(e)}")
        
        @self.router.post("/scheduler/run-now")
        async def run_job_now():
            """立即执行一次定时任务"""
            try:
                thread = threading.Thread(target=self.scheduler_service.run_job_now)
                thread.start()
                return {"status": "running", "message": "任务已开始执行"}
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"执行任务失败: {str(e)}")
        
        @self.router.post("/scheduler/set-time")
        async def set_schedule_time(request: ScheduleTimeRequest):
            """设置定时任务执行时间"""
            try:
                # 验证时间格式
                hour, minute = request.time.split(":")
                if not (0 <= int(hour) <= 23 and 0 <= int(minute) <= 59):
                    raise ValueError("时间格式不正确")
                
                success = self.scheduler_service.change_schedule_time(request.time)
                if success:
                    return {"status": "success", "message": f"定时任务已设置为每天 {request.time} 执行"}
                else:
                    raise HTTPException(status_code=500, detail="设置定时任务时间失败")
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"无效的时间格式: {str(e)}")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"设置定时任务时间失败: {str(e)}")
        
        # 健康检查端点
        @self.router.get("/health")
        async def health_check():
            """健康检查"""
            return {"status": "ok"} 