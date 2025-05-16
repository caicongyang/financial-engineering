#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
分析报告API服务类，用于获取个股分析和市场分析报告
"""

from typing import List, Dict, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from datetime import datetime
import threading
from com.caicongyang.financial.engineering.services.db_utils import get_latest_reports_with_content, get_report_by_id, search_reports_by_title, get_paginated_reports_by_type
from com.caicongyang.financial.engineering.services.stock_flow import generate_stock_flow_report

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
    stock_reports: List[dict]
    market_reports: List[dict]
    finance_reports: List[dict]

class StockReportPageResponse(BaseModel):
    total: int
    page: int
    page_size: int
    reports: List[ReportModel]

class SearchReportResponse(BaseModel):
    total: int
    reports: List[ReportModel]

class GenerateReportRequest(BaseModel):
    stock_code: str
    date: Optional[str] = None

class GenerateReportResponse(BaseModel):
    message: str
    estimated_time: str
    stock_code: str

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
                finance_reports = get_latest_reports_with_content('finance', limit=5)
                
                # 格式化日期（从datetime到字符串）
                for report in stock_reports + market_reports + finance_reports:
                    if isinstance(report.get('created_at'), datetime):
                        report['created_at'] = report['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                
                return {
                    "stock_reports": stock_reports,
                    "market_reports": market_reports,
                    "finance_reports": finance_reports
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"获取分析报告失败: {str(e)}")
        
        @self.router.get("/stock-reports", response_model=StockReportPageResponse)
        async def get_stock_reports(page: int = Query(1, description="页码，从1开始"), 
                                   page_size: int = Query(10, description="每页记录数", ge=1, le=50)):
            """获取个股分析报告，支持分页"""
            try:
                # 从数据库中获取个股分析报告（带分页）
                total, reports = get_paginated_reports_by_type('stock', page, page_size)
                
                # 格式化日期（从datetime到字符串）
                for report in reports:
                    if isinstance(report.get('created_at'), datetime):
                        report['created_at'] = report['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                
                return {
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "reports": reports
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"获取个股分析报告失败: {str(e)}")
        
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
                
        @self.router.post("/generate-stock-flow", response_model=GenerateReportResponse)
        async def generate_stock_flow_analysis(request: GenerateReportRequest):
            """
            生成个股资金流向报告（异步）
            
            该接口会异步生成股票资金流向分析报告，报告生成后可在报告列表中查看
            """
            try:
                # 验证股票代码格式
                stock_code = request.stock_code
                if not stock_code.startswith(('sh', 'sz')):
                    # 尝试自动添加前缀
                    if stock_code.startswith('6'):
                        stock_code = f"sh{stock_code}"
                    else:
                        stock_code = f"sz{stock_code}"
                
                # 启动异步线程生成报告
                def async_generate_report():
                    try:
                        generate_stock_flow_report(stock_code,request.date)
                    except Exception as e:
                        print(f"生成资金流向报告失败: {str(e)}")
                
                # 创建并启动线程
                report_thread = threading.Thread(target=async_generate_report)
                report_thread.daemon = True
                report_thread.start()
                
                # 返回成功信息
                return {
                    "message": "资金流向报告生成已开始，请稍后在报告列表中查看结果",
                    "estimated_time": "3-5分钟",
                    "stock_code": stock_code
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"启动报告生成失败: {str(e)}")
        
        @self.router.get("/finance-reports", response_model=List[dict])
        async def get_finance_reports():
            """获取财务报告"""
            try:
                # 从数据库中获取最近的财务报告（包含完整内容）
                finance_reports = get_latest_reports_with_content('finance', limit=5)
                
                # 格式化日期（从datetime到字符串）
                for report in finance_reports:
                    if isinstance(report.get('created_at'), datetime):
                        report['created_at'] = report['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                
                return finance_reports
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"获取财务报告失败: {str(e)}")
                
        @self.router.post("/generate-finance-report", response_model=GenerateReportResponse)
        async def generate_finance_report(company_name: str = Query(..., description="公司名称"), 
                                          company_ticker: str = Query(None, description="股票代码")):
            """
            生成财务报告分析（异步）
            
            该接口会异步生成公司财务报告分析，报告生成后可在报告列表中查看
            """
            try:
                # 验证参数
                if not company_name:
                    raise HTTPException(status_code=400, detail="公司名称不能为空")
                
                # 启动异步线程生成报告
                def async_generate_report():
                    try:
                        from com.caicongyang.financial.engineering.services.finance_report import generate_finance_report_html
                        generate_finance_report_html(company_name=company_name, company_ticker=company_ticker)
                    except Exception as e:
                        print(f"生成财务报告分析失败: {str(e)}")
                
                # 创建并启动线程
                report_thread = threading.Thread(target=async_generate_report)
                report_thread.daemon = True
                report_thread.start()
                
                # 返回成功信息
                return {
                    "message": "财务报告分析生成已开始，请稍后在报告列表中查看结果",
                    "estimated_time": "5-10分钟",
                    "stock_code": company_ticker if company_ticker else company_name
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"启动财务报告分析失败: {str(e)}")
        
     