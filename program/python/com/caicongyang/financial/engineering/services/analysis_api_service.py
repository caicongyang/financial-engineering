#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
分析报告API服务类，用于获取个股分析和市场分析报告
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from datetime import datetime
from com.caicongyang.financial.engineering.services.db_utils import get_latest_reports_with_content, get_report_by_id, search_reports_by_title

class ReportModel(BaseModel):
    id: int
    report_id: str
    report_type: str
    code: Optional[str] = None
    title: str
    content: str
    created_at: str
    tags: Optional[str] = None

class AnalysisReportResponse(BaseModel):
    stock_reports: List[ReportModel]
    market_reports: List[ReportModel]

class SearchReportResponse(BaseModel):
    total: int
    reports: List[ReportModel]

class AnalysisAPIService:
    """分析报告API服务类，处理所有分析报告相关的API路由"""
    
    def __init__(self):
        """初始化分析报告API服务"""
        self.router = APIRouter(tags=["analysis"])
        self._register_routes()
    
    def _register_routes(self):
        """注册路由处理函数"""
        
        @self.router.get("/recent-analysis", response_model=AnalysisReportResponse)
        async def get_recent_analysis():
            """获取最近的个股分析和市场分析报告"""
            try:
                # 从数据库中获取最近的分析报告（包含完整内容）
                stock_reports = get_latest_reports_with_content('stock', limit=5)
                market_reports = get_latest_reports_with_content('market', limit=5)
                
                # 格式化日期（从datetime到字符串）
                for report in stock_reports + market_reports:
                    if isinstance(report.get('created_at'), datetime):
                        report['created_at'] = report['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                
                return {
                    "stock_reports": stock_reports,
                    "market_reports": market_reports
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"获取分析报告失败: {str(e)}")
        
        @self.router.get("/report/{report_id}", response_model=ReportModel)
        async def get_analysis_report(report_id: str):
            """根据ID获取分析报告详情"""
            try:
                report = get_report_by_id(report_id)
                if not report:
                    raise HTTPException(status_code=404, detail=f"未找到报告: {report_id}")
                
                # 格式化日期
                if isinstance(report.get('created_at'), datetime):
                    report['created_at'] = report['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                
                return report
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"获取报告失败: {str(e)}")
                
        @self.router.get("/search", response_model=SearchReportResponse)
        async def search_reports(keyword: str = Query(..., description="搜索关键字"), limit: int = Query(20, description="返回结果数量限制")):
            """根据标题关键字搜索分析报告"""
            try:
                if not keyword or len(keyword.strip()) < 1:
                    raise HTTPException(status_code=400, detail="搜索关键字不能为空")
                
                # 执行搜索
                reports = search_reports_by_title(keyword.strip(), limit)
                
                # 格式化日期
                for report in reports:
                    if isinstance(report.get('created_at'), datetime):
                        report['created_at'] = report['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                
                return {
                    "total": len(reports),
                    "reports": reports
                }
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"搜索报告失败: {str(e)}") 