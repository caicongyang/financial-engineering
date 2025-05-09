#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
定时任务调度服务，用于管理系统的定时任务
"""

import time
import schedule
from datetime import datetime

class SchedulerService:
    """定时任务调度服务类，用于管理系统的定时任务"""
    
    def __init__(self, data_service, schedule_time="18:30"):
        """
        初始化定时任务调度服务
        
        Args:
            data_service: 数据处理服务实例
            schedule_time: 每日定时任务执行时间，默认为18:30
        """
        self.data_service = data_service
        self.schedule_time = schedule_time
    
    def start_scheduler(self):
        """启动定时任务调度器"""
        print(f"Setting up scheduled job to run at {self.schedule_time} every day...")
        
        # 设置每天定时执行任务
        schedule.every().day.at(self.schedule_time).do(self.data_service.run_daily_job)
        
        # 运行定时任务
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    
    def run_job_now(self):
        """立即执行一次任务"""
        print("Running job now...")
        self.data_service.run_daily_job()
    
    def change_schedule_time(self, new_time):
        """
        更改定时任务执行时间
        
        Args:
            new_time: 新的执行时间，格式为"HH:MM"
        
        Returns:
            bool: 是否成功更改时间
        """
        try:
            # 清除现有的任务
            schedule.clear()
            
            # 设置新的任务时间
            self.schedule_time = new_time
            schedule.every().day.at(new_time).do(self.data_service.run_daily_job)
            
            print(f"Schedule time changed to {new_time}")
            return True
        except Exception as e:
            print(f"Failed to change schedule time: {e}")
            return False 