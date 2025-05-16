#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
使用yfinance库下载特斯拉(Tesla)的财务报表数据
支持获取资产负债表、利润表和现金流量表
"""

import os
import pandas as pd
import yfinance as yf
from datetime import datetime
from typing import Dict, Optional, List, Union, Tuple
import requests
import re
from bs4 import BeautifulSoup
import json
import webbrowser

class TeslaFinancialReports:
    """
    特斯拉财务报表下载类，使用yfinance库获取财务数据
    
    支持的报表类型:
    - 资产负债表 (balance_sheet)
    - 利润表 (income_statement)
    - 现金流量表 (cash_flow)
    """
    
    def __init__(self, ticker: str = "TSLA"):
        """
        初始化财务报表下载类
        
        Args:
            ticker: 股票代码，默认为 "TSLA" (特斯拉)
        """
        self.ticker = ticker
        self.ticker_obj = yf.Ticker(ticker)
        self.cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
        
        # 确保缓存目录存在
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            
        # SEC EDGAR相关参数
        self.cik = '0001318605'  # 特斯拉的CIK编号
        self.company_name = 'Tesla Inc'
        
        # 特斯拉官方投资者关系网站和年报URL
        self.ir_website = 'https://ir.tesla.com'
        self.quarterly_url = 'https://ir.tesla.com/quarterly-results'
        self.annual_url = 'https://ir.tesla.com/financial-information/annual-reports'
    
    def get_income_statement(self, period: str = "annual", cache: bool = True) -> pd.DataFrame:
        """
        获取利润表数据
        
        Args:
            period: 数据周期，'annual'表示年报，'quarterly'表示季报
            cache: 是否使用缓存，默认为True
        
        Returns:
            利润表数据的DataFrame
        """
        cache_file = os.path.join(self.cache_dir, f"{self.ticker}_income_{period}.csv")
        
        # 检查缓存
        if cache and os.path.exists(cache_file):
            # 获取文件修改时间
            mod_time = os.path.getmtime(cache_file)
            mod_date = datetime.fromtimestamp(mod_time)
            today = datetime.now()
            
            # 如果缓存文件是今天创建的，直接读取
            if mod_date.date() == today.date():
                return pd.read_csv(cache_file, index_col=0)
        
        # 从yfinance获取数据
        if period == "annual":
            income_df = self.ticker_obj.income_stmt
        else:
            income_df = self.ticker_obj.quarterly_income_stmt
        
        # 保存到缓存
        if cache and not income_df.empty:
            income_df.to_csv(cache_file)
        
        return income_df
    
    def get_balance_sheet(self, period: str = "annual", cache: bool = True) -> pd.DataFrame:
        """
        获取资产负债表数据
        
        Args:
            period: 数据周期，'annual'表示年报，'quarterly'表示季报
            cache: 是否使用缓存，默认为True
        
        Returns:
            资产负债表数据的DataFrame
        """
        cache_file = os.path.join(self.cache_dir, f"{self.ticker}_balance_{period}.csv")
        
        # 检查缓存
        if cache and os.path.exists(cache_file):
            mod_time = os.path.getmtime(cache_file)
            mod_date = datetime.fromtimestamp(mod_time)
            today = datetime.now()
            
            # 如果缓存文件是今天创建的，直接读取
            if mod_date.date() == today.date():
                return pd.read_csv(cache_file, index_col=0)
        
        # 从yfinance获取数据
        if period == "annual":
            balance_df = self.ticker_obj.balance_sheet
        else:
            balance_df = self.ticker_obj.quarterly_balance_sheet
        
        # 保存到缓存
        if cache and not balance_df.empty:
            balance_df.to_csv(cache_file)
        
        return balance_df
    
    def get_cash_flow(self, period: str = "annual", cache: bool = True) -> pd.DataFrame:
        """
        获取现金流量表数据
        
        Args:
            period: 数据周期，'annual'表示年报，'quarterly'表示季报
            cache: 是否使用缓存，默认为True
        
        Returns:
            现金流量表数据的DataFrame
        """
        cache_file = os.path.join(self.cache_dir, f"{self.ticker}_cash_flow_{period}.csv")
        
        # 检查缓存
        if cache and os.path.exists(cache_file):
            mod_time = os.path.getmtime(cache_file)
            mod_date = datetime.fromtimestamp(mod_time)
            today = datetime.now()
            
            # 如果缓存文件是今天创建的，直接读取
            if mod_date.date() == today.date():
                return pd.read_csv(cache_file, index_col=0)
        
        # 从yfinance获取数据
        if period == "annual":
            cash_flow_df = self.ticker_obj.cashflow
        else:
            cash_flow_df = self.ticker_obj.quarterly_cashflow
        
        # 保存到缓存
        if cache and not cash_flow_df.empty:
            cash_flow_df.to_csv(cache_file)
        
        return cash_flow_df
    
    def get_key_financial_metrics(self) -> Dict[str, float]:
        """
        获取特斯拉关键财务指标
        
        Returns:
            包含关键财务指标的字典
        """
        metrics = {
            'PE_Ratio': self.ticker_obj.info.get('trailingPE', None),
            'Market_Cap': self.ticker_obj.info.get('marketCap', None),
            'Revenue_TTM': self.ticker_obj.info.get('totalRevenue', None),
            'Gross_Profit_TTM': self.ticker_obj.info.get('grossProfits', None),
            'EBITDA': self.ticker_obj.info.get('ebitda', None),
            'Profit_Margin': self.ticker_obj.info.get('profitMargins', None),
            'ROE': self.ticker_obj.info.get('returnOnEquity', None),
            'ROA': self.ticker_obj.info.get('returnOnAssets', None),
            'Debt_to_Equity': self.ticker_obj.info.get('debtToEquity', None),
            'Current_Ratio': self.ticker_obj.info.get('currentRatio', None),
            'EPS_TTM': self.ticker_obj.info.get('trailingEps', None),
            'Forward_EPS': self.ticker_obj.info.get('forwardEps', None),
            'PEG_Ratio': self.ticker_obj.info.get('pegRatio', None),
            'Beta': self.ticker_obj.info.get('beta', None),
            'Dividend_Yield': self.ticker_obj.info.get('dividendYield', 0),
        }
        return metrics
    
    def generate_financial_summary(self) -> str:
        """
        生成特斯拉财务摘要报告
        
        Returns:
            财务摘要报告的Markdown格式字符串
        """
        try:
            # 获取最新的财务数据
            income = self.get_income_statement()
            balance = self.get_balance_sheet()
            cash_flow = self.get_cash_flow()
            metrics = self.get_key_financial_metrics()
            
            # 提取最近一期的数据
            latest_period = income.columns[0] if not income.empty else None
            
            if latest_period is None:
                return "无法获取最新财务数据"
            
            # 格式化日期
            if isinstance(latest_period, pd.Timestamp):
                period_str = latest_period.strftime('%Y-%m-%d')
            else:
                period_str = str(latest_period)
            
            # 构建财务摘要报告
            report = f"# 特斯拉 (TSLA) 财务摘要报告\n\n"
            report += f"报告日期: {period_str}\n\n"
            
            # 关键指标
            report += "## 关键财务指标\n\n"
            report += f"- 市值: ${metrics.get('Market_Cap', 'N/A'):,.0f}\n"
            report += f"- 市盈率 (P/E): {metrics.get('PE_Ratio', 'N/A'):.2f}\n"
            report += f"- 市盈增长比率 (PEG): {metrics.get('PEG_Ratio', 'N/A'):.2f}\n"
            report += f"- 每股收益 (TTM): ${metrics.get('EPS_TTM', 'N/A'):.2f}\n"
            report += f"- 前瞻每股收益: ${metrics.get('Forward_EPS', 'N/A'):.2f}\n"
            report += f"- 利润率: {metrics.get('Profit_Margin', 'N/A')*100:.2f}%\n"
            report += f"- 股本回报率 (ROE): {metrics.get('ROE', 'N/A')*100:.2f}%\n"
            report += f"- 资产回报率 (ROA): {metrics.get('ROA', 'N/A')*100:.2f}%\n\n"
            
            # 利润表摘要
            report += "## 利润表摘要\n\n"
            if not income.empty:
                report += f"- 总收入: ${income.loc['Total Revenue', latest_period]/1e9:.2f}十亿\n"
                report += f"- 毛利润: ${income.loc['Gross Profit', latest_period]/1e9:.2f}十亿\n"
                report += f"- 营业利润: ${income.loc['Operating Income', latest_period]/1e9:.2f}十亿\n"
                report += f"- 净利润: ${income.loc['Net Income', latest_period]/1e9:.2f}十亿\n\n"
            
            # 资产负债表摘要
            report += "## 资产负债表摘要\n\n"
            if not balance.empty:
                report += f"- 总资产: ${balance.loc['Total Assets', latest_period]/1e9:.2f}十亿\n"
                report += f"- 总负债: ${balance.loc['Total Liabilities Net Minority Interest', latest_period]/1e9:.2f}十亿\n"
                report += f"- 股东权益: ${balance.loc['Total Equity', latest_period]/1e9:.2f}十亿\n"
                report += f"- 负债权益比: {metrics.get('Debt_to_Equity', 'N/A'):.2f}\n\n"
            
            # 现金流量表摘要
            report += "## 现金流量表摘要\n\n"
            if not cash_flow.empty:
                report += f"- 经营活动现金流: ${cash_flow.loc['Operating Cash Flow', latest_period]/1e9:.2f}十亿\n"
                report += f"- 投资活动现金流: ${cash_flow.loc['Investing Cash Flow', latest_period]/1e9:.2f}十亿\n"
                report += f"- 筹资活动现金流: ${cash_flow.loc['Financing Cash Flow', latest_period]/1e9:.2f}十亿\n"
                report += f"- 自由现金流: ${cash_flow.loc['Free Cash Flow', latest_period]/1e9:.2f}十亿\n"
            
            return report
        except Exception as e:
            return f"生成财务摘要失败: {str(e)}"

    def get_financial_reports_pdf(self, report_type: str = '10-K', year: Optional[int] = None, 
                                 download_dir: Optional[str] = None) -> List[Dict[str, str]]:
        """
        获取特斯拉财务报告的PDF文件或链接
        
        Args:
            report_type: 报告类型，'10-K'表示年报，'10-Q'表示季报
            year: 指定年份，如果为None则获取最近的报告
            download_dir: 下载目录，如果指定则下载PDF，否则仅返回链接
            
        Returns:
            包含报告信息的字典列表，每个字典包含：
            - title: 报告标题
            - filing_date: 提交日期
            - document_url: 文档URL
            - download_path: 下载路径(如果已下载)
        """
        # 构建SEC EDGAR搜索URL
        base_url = f"https://www.sec.gov/cgi-bin/browse-edgar"
        params = {
            'CIK': self.cik,
            'owner': 'exclude',
            'action': 'getcompany',
            'type': report_type,
        }
        
        if year:
            # 如果指定了年份，添加日期范围
            params['datea'] = f"{year}0101"
            params['dateb'] = f"{year}1231"
        
        # 发送请求到SEC EDGAR
        print(f"正在从SEC EDGAR检索{self.company_name}的{report_type}报告...")
        
        # 更新请求头，模拟真实浏览器并添加联系信息
        # SEC要求使用真实的User-Agent和联系方式
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'From': 'your-email@example.com',  # 请替换为您的实际Email，SEC建议提供联系方式
        }
        
        try:
            # 添加延迟，避免请求过于频繁
            import time
            time.sleep(1)  # SEC推荐访问之间至少间隔1秒
            
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            
            # 解析HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 打印更多调试信息
            if "not authorized" in response.text.lower() or "forbidden" in response.text.lower():
                print(f"访问被拒绝，可能需要更新请求头或减慢请求频率")
                print(f"响应代码: {response.status_code}")
                print(f"响应内容预览: {response.text[:500]}...")
                return []
            
            # 查找所有文件表格行
            filing_tables = soup.find_all('table', class_='tableFile')
            if not filing_tables:
                print("未找到报告数据表格")
                print(f"响应内容预览: {response.text[:500]}...")
                return []
                
            reports = []
            
            # 解析表格中的行
            rows = filing_tables[0].find_all('tr')
            for row in rows[1:]:  # 跳过表头
                cols = row.find_all('td')
                if len(cols) >= 4:
                    # 获取文档URL
                    filing_detail_link = cols[1].find('a')
                    if filing_detail_link:
                        filing_detail_url = f"https://www.sec.gov{filing_detail_link['href']}"
                        
                        # 获取报告标题和日期
                        report_title = cols[1].text.strip()
                        filing_date = cols[3].text.strip()
                        
                        print(f"找到报告: {report_title} ({filing_date})")
                        print(f"详情链接: {filing_detail_url}")
                        
                        # 在请求详情页前等待
                        time.sleep(1)
                        
                        # 获取文档页面
                        try:
                            detail_response = requests.get(filing_detail_url, headers=headers)
                            detail_response.raise_for_status()
                            
                            # 解析详情页
                            detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                            
                            # 查找文档表格
                            doc_table = detail_soup.find('table', class_='tableFile', summary='Document Format Files')
                            if doc_table:
                                doc_rows = doc_table.find_all('tr')
                                for doc_row in doc_rows[1:]:  # 跳过表头
                                    doc_cols = doc_row.find_all('td')
                                    if len(doc_cols) >= 3:
                                        doc_type = doc_cols[0].text.strip()
                                        doc_description = doc_cols[2].text.strip()
                                        
                                        print(f"文档类型: {doc_type}, 描述: {doc_description}")
                                        
                                        if doc_type in ('10-K', '10-Q', 'EX-13') or '.pdf' in doc_description.lower():
                                            # 找到主要报告文档
                                            doc_link = doc_cols[2].find('a')
                                            if doc_link and doc_link['href']:
                                                document_url = f"https://www.sec.gov{doc_link['href']}"
                                                
                                                report_info = {
                                                    'title': report_title,
                                                    'filing_date': filing_date,
                                                    'document_url': document_url,
                                                    'download_path': None
                                                }
                                                
                                                # 如果指定了下载目录，下载PDF
                                                if download_dir:
                                                    if not os.path.exists(download_dir):
                                                        os.makedirs(download_dir)
                                                    
                                                    file_name = f"{self.ticker}_{report_type}_{filing_date.replace('/', '_')}.pdf"
                                                    download_path = os.path.join(download_dir, file_name)
                                                    
                                                    # 判断是否已下载
                                                    if os.path.exists(download_path):
                                                        print(f"文件已存在: {download_path}")
                                                        report_info['download_path'] = download_path
                                                    else:
                                                        try:
                                                            print(f"下载报告: {report_title} - {filing_date}")
                                                            # 在下载前等待
                                                            time.sleep(1)
                                                            
                                                            doc_response = requests.get(document_url, headers=headers)
                                                            doc_response.raise_for_status()
                                                            
                                                            with open(download_path, 'wb') as f:
                                                                f.write(doc_response.content)
                                                            
                                                            print(f"报告已下载: {download_path}")
                                                            report_info['download_path'] = download_path
                                                        except Exception as e:
                                                            print(f"下载失败: {str(e)}")
                                                
                                                reports.append(report_info)
                                                break  # 找到一个文档后跳出当前报告的循环
                        except Exception as e:
                            print(f"获取详情页失败: {str(e)}")
                            continue
            
            if not reports:
                print("未找到可下载的报告")
                
            return reports
        
        except Exception as e:
            print(f"获取报告失败: {str(e)}")
            return []

    def get_tesla_ir_reports(self, report_type: str = 'annual', download_dir: Optional[str] = None) -> List[Dict[str, str]]:
        """
        从特斯拉投资者关系网站获取财报PDF
        
        Args:
            report_type: 'annual' 表示年报，'quarterly' 表示季报
            download_dir: 下载目录，如果指定则下载PDF，否则仅返回链接
            
        Returns:
            包含报告信息的字典列表，每个字典包含：
            - title: 报告标题
            - year: 报告年份
            - url: 文档URL
            - download_path: 下载路径(如果已下载)
        """
        # 确定URL
        url = self.annual_url if report_type == 'annual' else self.quarterly_url
        
        # 设置请求头
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://ir.tesla.com/',
            'Sec-Ch-Ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"macOS"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Upgrade-Insecure-Requests': '1',
        }
        
        reports = []
        
        try:
            print(f"正在从特斯拉投资者关系网站获取{report_type}报告...")
            
            # 获取页面内容
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            # 解析HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 寻找报告链接 - 年报页面的结构
            if report_type == 'annual':
                # 查找年报列表
                report_links = []
                report_sections = soup.find_all('div', class_='view-grouping')
                
                for section in report_sections:
                    # 获取年份
                    year_header = section.find('div', class_='view-grouping-header')
                    if not year_header:
                        continue
                    
                    year = year_header.text.strip()
                    
                    # 获取该年的报告
                    items = section.find_all('div', class_='field-items')
                    for item in items:
                        links = item.find_all('a')
                        for link in links:
                            if link.get('href') and ('.pdf' in link.get('href') or '/annual-reports/' in link.get('href')):
                                title = link.text.strip()
                                href = link.get('href')
                                
                                # 确保URL是完整的
                                if href.startswith('/'):
                                    href = f"{self.ir_website}{href}"
                                
                                report_links.append({
                                    'title': title,
                                    'year': year,
                                    'url': href
                                })
            else:
                # 季度报告页面结构不同，可能需要调整
                # 查找所有季度报告区块
                report_links = []
                quarters = soup.find_all('div', class_='views-row')
                
                for quarter in quarters:
                    # 查找标题或年份
                    title_elem = quarter.find('h2')
                    if not title_elem:
                        continue
                    
                    quarter_title = title_elem.text.strip()
                    # 从标题中提取年份
                    year_match = re.search(r'(\d{4})', quarter_title)
                    year = year_match.group(1) if year_match else "Unknown"
                    
                    # 查找所有链接
                    links = quarter.find_all('a')
                    for link in links:
                        if link.get('href') and '.pdf' in link.get('href'):
                            title = link.text.strip()
                            href = link.get('href')
                            
                            # 确保URL是完整的
                            if href.startswith('/'):
                                href = f"{self.ir_website}{href}"
                            
                            report_links.append({
                                'title': title,
                                'year': year,
                                'url': href
                            })
            
            # 如果找不到标准结构，尝试更一般的方法
            if not report_links:
                print("无法通过标准结构找到报告，尝试查找所有PDF链接...")
                all_links = soup.find_all('a')
                for link in all_links:
                    href = link.get('href')
                    if href and '.pdf' in href:
                        title = link.text.strip() or f"Report {len(report_links) + 1}"
                        # 尝试从链接或标题中提取年份
                        year_match = re.search(r'(\d{4})', href + ' ' + title)
                        year = year_match.group(1) if year_match else "Unknown"
                        
                        # 确保URL是完整的
                        if href.startswith('/'):
                            href = f"{self.ir_website}{href}"
                        
                        report_links.append({
                            'title': title,
                            'year': year,
                            'url': href
                        })
            
            print(f"找到 {len(report_links)} 个报告链接")
            
            # 下载报告
            for report in report_links:
                print(f"报告: {report['title']} ({report['year']})")
                print(f"URL: {report['url']}")
                
                report_info = {
                    'title': report['title'],
                    'year': report['year'],
                    'url': report['url'],
                    'download_path': None
                }
                
                # 如果指定了下载目录，下载PDF
                if download_dir:
                    if not os.path.exists(download_dir):
                        os.makedirs(download_dir)
                    
                    # 生成文件名
                    file_name = f"Tesla_{report_type}_{report['year']}_{report['title'].replace(' ', '_')[:30]}.pdf"
                    # 替换非法文件名字符
                    file_name = re.sub(r'[\\/*?:"<>|]', '_', file_name)
                    download_path = os.path.join(download_dir, file_name)
                    
                    # 判断是否已下载
                    if os.path.exists(download_path):
                        print(f"文件已存在: {download_path}")
                        report_info['download_path'] = download_path
                    else:
                        try:
                            print(f"下载报告: {report['title']}")
                            response = requests.get(report['url'], headers=headers)
                            response.raise_for_status()
                            
                            # 验证内容类型是否为PDF
                            content_type = response.headers.get('Content-Type', '')
                            if 'application/pdf' in content_type or report['url'].endswith('.pdf'):
                                with open(download_path, 'wb') as f:
                                    f.write(response.content)
                                
                                print(f"报告已下载: {download_path}")
                                report_info['download_path'] = download_path
                            else:
                                print(f"非PDF内容: {content_type}，可能是网页而非直接PDF")
                                # 如果是HTML页面，尝试查找其中的PDF链接
                                if 'text/html' in content_type:
                                    pdf_soup = BeautifulSoup(response.text, 'html.parser')
                                    pdf_link = None
                                    
                                    for a in pdf_soup.find_all('a'):
                                        href = a.get('href')
                                        if href and href.endswith('.pdf'):
                                            pdf_link = href
                                            if not pdf_link.startswith('http'):
                                                pdf_link = f"{self.ir_website}{pdf_link}" if pdf_link.startswith('/') else f"{'/'.join(report['url'].split('/')[:-1])}/{pdf_link}"
                                            break
                                    
                                    if pdf_link:
                                        print(f"在页面中找到PDF链接: {pdf_link}")
                                        pdf_response = requests.get(pdf_link, headers=headers)
                                        pdf_response.raise_for_status()
                                        
                                        with open(download_path, 'wb') as f:
                                            f.write(pdf_response.content)
                                        
                                        print(f"报告已下载: {download_path}")
                                        report_info['download_path'] = download_path
                                    else:
                                        print("无法在页面中找到PDF链接")
                        except Exception as e:
                            print(f"下载失败: {str(e)}")
                
                reports.append(report_info)
            
            return reports
            
        except Exception as e:
            print(f"获取报告失败: {str(e)}")
            return []
    
    def get_tesla_website_10k(self, year: Optional[int] = None, 
                           download_dir: Optional[str] = None) -> List[Dict[str, str]]:
        """
        直接从Tesla官网获取10-K年报（使用对投资者友好的URL）
        
        Args:
            year: 报告年份，如果为None则获取最新的报告
            download_dir: 下载目录，如果指定则下载PDF，否则仅返回链接
            
        Returns:
            包含报告信息的字典列表，每个字典包含：
            - title: 报告标题
            - year: 报告年份
            - url: 文档URL
            - download_path: 下载路径(如果已下载)
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        
        # 直接访问特斯拉10-K报告的简单URL
        # 这些URL是Tesla投资者关系页面上的简化链接
        base_urls = [
            # 特斯拉通常在这里发布其官方10-K年报
            "https://ir.tesla.com/financial-information/annual-reports",
            # 直接访问TSLA投资者关系网站的文件页面
            "https://ir.tesla.com/sec-filings?field_cat_reference_value=10-K"
        ]
        
        reports = []
        
        # 如果指定了年份，构建更具体的URL
        if year:
            # 特斯拉可能会采用这种格式来存储历史报告
            specific_urls = [
                f"https://ir.tesla.com/sec-filings?year={year}&field_cat_reference_value=10-K"
            ]
            base_urls = specific_urls + base_urls
        
        print(f"尝试从特斯拉官网获取{'最新' if not year else str(year)+'年'}的10-K年报...")
        
        for url in base_urls:
            try:
                print(f"访问URL: {url}")
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                
                # 解析HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 查找表格中的10-K链接
                report_links = []
                
                # 方法1: 查找具有特定类的表格行
                rows = soup.find_all('tr', class_='filing')
                for row in rows:
                    report_type_cell = row.find('td', class_='views-field-field-sec-filing-type')
                    
                    if report_type_cell and '10-K' in report_type_cell.text.strip():
                        link_cell = row.find('td', class_='views-field-field-sec-filing-file')
                        if link_cell:
                            link = link_cell.find('a')
                            if link and link.get('href'):
                                href = link.get('href')
                                # 确保URL是完整的
                                if href.startswith('/'):
                                    href = f"https://ir.tesla.com{href}"
                                
                                # 尝试获取日期
                                date_cell = row.find('td', class_='views-field-field-sec-filing-date')
                                date_text = date_cell.text.strip() if date_cell else ""
                                
                                # 尝试提取年份
                                year_match = re.search(r'(\d{4})', date_text)
                                report_year = year_match.group(1) if year_match else "Unknown"
                                
                                report_links.append({
                                    'title': f"Tesla 10-K Annual Report {report_year}",
                                    'year': report_year,
                                    'url': href
                                })
                
                # 方法2: 如果方法1找不到链接，尝试查找所有链接
                if not report_links:
                    all_links = soup.find_all('a')
                    for link in all_links:
                        href = link.get('href')
                        text = link.text.strip()
                        
                        # 查找10-K相关链接
                        if href and (('10-K' in text or '10-K' in href) or 
                                     ('annual' in text.lower() and 'report' in text.lower() and '.pdf' in href)):
                            # 确保URL是完整的
                            if href.startswith('/'):
                                href = f"https://ir.tesla.com{href}"
                            
                            # 尝试从链接或文本中提取年份
                            year_match = re.search(r'(\d{4})', href + ' ' + text)
                            report_year = year_match.group(1) if year_match else "Unknown"
                            
                            report_links.append({
                                'title': text or f"Tesla 10-K Annual Report {report_year}",
                                'year': report_year,
                                'url': href
                            })
                
                # 方法3: 尝试在页面上找到嵌入的JSON数据
                scripts = soup.find_all('script', {'type': 'application/json'})
                for script in scripts:
                    try:
                        data = json.loads(script.string)
                        # 尝试从JSON数据中提取文件信息
                        if isinstance(data, dict) and 'content' in data:
                            content = data['content']
                            if isinstance(content, list):
                                for item in content:
                                    if isinstance(item, dict):
                                        # 尝试找到10-K文件
                                        filing_type = item.get('field_sec_filing_type', '')
                                        if isinstance(filing_type, str) and '10-K' in filing_type:
                                            file_data = item.get('field_sec_filing_file', {})
                                            if isinstance(file_data, dict):
                                                href = file_data.get('url', '')
                                                if href:
                                                    # 确保URL是完整的
                                                    if href.startswith('/'):
                                                        href = f"https://ir.tesla.com{href}"
                                                    
                                                    # 尝试获取年份
                                                    filing_date = item.get('field_sec_filing_date', '')
                                                    year_match = re.search(r'(\d{4})', filing_date)
                                                    report_year = year_match.group(1) if year_match else "Unknown"
                                                    
                                                    report_links.append({
                                                        'title': f"Tesla 10-K Annual Report {report_year}",
                                                        'year': report_year,
                                                        'url': href
                                                    })
                    except:
                        # 跳过JSON解析错误
                        pass
                
                print(f"在 {url} 中找到 {len(report_links)} 个10-K报告链接")
                
                if report_links:
                    # 只处理找到链接的第一个URL
                    # 下载报告
                    for report in report_links:
                        print(f"报告: {report['title']} ({report['year']})")
                        print(f"URL: {report['url']}")
                        
                        report_info = {
                            'title': report['title'],
                            'year': report['year'],
                            'url': report['url'],
                            'download_path': None
                        }
                        
                        # 如果指定了下载目录，下载PDF
                        if download_dir:
                            if not os.path.exists(download_dir):
                                os.makedirs(download_dir)
                            
                            # 生成文件名
                            file_name = f"Tesla_10K_{report['year']}.pdf"
                            download_path = os.path.join(download_dir, file_name)
                            
                            # 判断是否已下载
                            if os.path.exists(download_path):
                                print(f"文件已存在: {download_path}")
                                report_info['download_path'] = download_path
                            else:
                                try:
                                    print(f"下载报告: {report['title']}")
                                    response = requests.get(report['url'], headers=headers)
                                    response.raise_for_status()
                                    
                                    # 检查内容类型
                                    content_type = response.headers.get('Content-Type', '')
                                    
                                    if 'application/pdf' in content_type or report['url'].endswith('.pdf'):
                                        with open(download_path, 'wb') as f:
                                            f.write(response.content)
                                        
                                        print(f"报告已下载: {download_path}")
                                        report_info['download_path'] = download_path
                                    else:
                                        print(f"非PDF内容: {content_type}，尝试在页面中查找PDF链接")
                                        
                                        # 如果是HTML页面，尝试查找其中的PDF链接
                                        if 'text/html' in content_type:
                                            pdf_soup = BeautifulSoup(response.text, 'html.parser')
                                            pdf_links = []
                                            
                                            # 查找所有PDF链接
                                            for a in pdf_soup.find_all('a'):
                                                href = a.get('href')
                                                if href and (href.endswith('.pdf') or '/viewerjs/' in href):
                                                    pdf_text = a.text.strip().lower()
                                                    # 优先选择包含"10-k"或"annual report"的链接
                                                    priority = 0
                                                    if '10-k' in href.lower() or '10-k' in pdf_text:
                                                        priority += 2
                                                    if 'annual' in pdf_text and 'report' in pdf_text:
                                                        priority += 1
                                                        
                                                    # 确保URL是完整的
                                                    if href.startswith('/'):
                                                        href = f"https://ir.tesla.com{href}"
                                                    elif not href.startswith('http'):
                                                        href = f"{'/'.join(report['url'].split('/')[:-1])}/{href}"
                                                    
                                                    pdf_links.append((href, priority))
                                            
                                            # 按优先级排序
                                            pdf_links.sort(key=lambda x: x[1], reverse=True)
                                            
                                            if pdf_links:
                                                pdf_link = pdf_links[0][0]
                                                print(f"在页面中找到PDF链接: {pdf_link}")
                                                
                                                pdf_response = requests.get(pdf_link, headers=headers)
                                                pdf_response.raise_for_status()
                                                
                                                with open(download_path, 'wb') as f:
                                                    f.write(pdf_response.content)
                                                
                                                print(f"报告已下载: {download_path}")
                                                report_info['download_path'] = download_path
                                            else:
                                                print("无法在页面中找到PDF链接")
                                except Exception as e:
                                    print(f"下载失败: {str(e)}")
                        
                        reports.append(report_info)
                    
                    # 找到报告后退出循环
                    if reports:
                        break
            
            except Exception as e:
                print(f"访问 {url} 失败: {str(e)}")
        
        return reports

    def get_static_tesla_report_links(self) -> Dict[str, Dict[str, str]]:
        """
        提供静态的特斯拉财报直接下载链接
        
        由于网站可能会阻止自动下载，这个方法提供已知的特斯拉财报直接链接，
        可以手动打开浏览器下载。
        
        Returns:
            Dict[str, Dict[str, str]]: 财报年份到链接的映射
        """
        # 特斯拉10-K年报直接下载链接
        annual_reports = {
            "2022": {
                "title": "Tesla 2022 10-K Annual Report",
                "url": "https://digitalassets.tesla.com/tesla-contents/image/upload/v1675254872/IR/TESLA-YEAR-END-2022-10K.pdf",
                "direct_url": "https://ir.tesla.com/download/download-annual-report?path=%2F2022%2Ftesla-fy-2022-10k"
            },
            "2021": {
                "title": "Tesla 2021 10-K Annual Report",
                "url": "https://digitalassets.tesla.com/tesla-contents/image/upload/v1643944936/TSLA-Q4-2021-10K.pdf",
                "direct_url": "https://ir.tesla.com/download/download-annual-report?path=%2F2021%2Ftesla-fy-2021-10k"
            },
            "2020": {
                "title": "Tesla 2020 10-K Annual Report",
                "url": "https://digitalassets.tesla.com/tesla-contents/image/upload/v1614174459/TSLA-Q4-2020-10K.pdf",
                "direct_url": "https://ir.tesla.com/download/download-annual-report?path=%2F2020%2Ftesla-fy-2020-10k"
            },
            "2019": {
                "title": "Tesla 2019 10-K Annual Report",
                "url": "https://digitalassets.tesla.com/tesla-contents/image/upload/v1601954313/TSLA-Q4-2019-10K.pdf",
                "direct_url": "https://ir.tesla.com/download/download-annual-report?path=%2F2019%2Ftesla-fy-2019-10k"
            },
            "2018": {
                "title": "Tesla 2018 10-K Annual Report",
                "url": "https://digitalassets.tesla.com/tesla-contents/image/upload/v1601948590/TSLA-Q4-2018-10K.pdf",
                "direct_url": "https://ir.tesla.com/download/download-annual-report?path=%2F2018%2Ftesla-fy-2018-10k"
            }
        }
        
        # 特斯拉季度报告直接下载链接
        quarterly_reports = {
            "2023-Q3": {
                "title": "Tesla 2023 Q3 10-Q Quarterly Report",
                "url": "https://digitalassets.tesla.com/tesla-contents/image/upload/v1698252123/TSLA-Q3-2023-10Q.pdf",
                "direct_url": "https://ir.tesla.com/download/download-quarterly-report?path=%2Fquarterly-reports%2Ftsla-10q-30sep2023"
            },
            "2023-Q2": {
                "title": "Tesla 2023 Q2 10-Q Quarterly Report",
                "url": "https://digitalassets.tesla.com/tesla-contents/image/upload/v1690318346/TSLA-Q2-2023-10Q.pdf",
                "direct_url": "https://ir.tesla.com/download/download-quarterly-report?path=%2Fquarterly-reports%2Ftsla-10q-30jun2023"
            },
            "2023-Q1": {
                "title": "Tesla 2023 Q1 10-Q Quarterly Report",
                "url": "https://digitalassets.tesla.com/tesla-contents/image/upload/v1683053234/TSLA-Q1-2023-10Q.pdf",
                "direct_url": "https://ir.tesla.com/download/download-quarterly-report?path=%2Fquarterly-reports%2Ftsla-10q-31mar2023"
            }
        }
        
        return {
            "annual": annual_reports,
            "quarterly": quarterly_reports
        }
    
    def download_static_report(self, year: str, report_type: str = "annual", download_dir: Optional[str] = None, open_browser: bool = False) -> Optional[str]:
        """
        下载或打开静态财报链接
        
        Args:
            year: 年份，如 "2022" 或季度 "2023-Q3"
            report_type: 'annual' 表示年报，'quarterly' 表示季报
            download_dir: 下载目录，如果指定则尝试下载PDF
            open_browser: 是否直接在浏览器中打开链接（推荐）
            
        Returns:
            Optional[str]: 下载路径或None
        """
        # 获取静态链接
        reports_dict = self.get_static_tesla_report_links()
        
        if report_type not in reports_dict:
            print(f"不支持的报告类型: {report_type}")
            return None
        
        reports = reports_dict[report_type]
        
        if year not in reports:
            available_years = ", ".join(reports.keys())
            print(f"没有找到 {year} 年的报告。可用年份: {available_years}")
            return None
        
        report = reports[year]
        print(f"找到报告: {report['title']}")
        print(f"URL: {report['url']}")
        
        # 如果要求在浏览器中打开
        if open_browser:
            try:
                # 优先使用direct_url，这个一般是下载触发链接
                open_url = report.get('direct_url', report['url'])
                print(f"在浏览器中打开: {open_url}")
                webbrowser.open(open_url)
                return open_url
            except Exception as e:
                print(f"无法在浏览器中打开链接: {str(e)}")
        
        # 如果指定了下载目录，尝试下载
        if download_dir:
            if not os.path.exists(download_dir):
                os.makedirs(download_dir)
            
            # 生成文件名
            if report_type == 'annual':
                file_name = f"Tesla_10K_{year}.pdf"
            else:
                file_name = f"Tesla_10Q_{year}.pdf"
                
            download_path = os.path.join(download_dir, file_name)
            
            # 判断是否已下载
            if os.path.exists(download_path):
                print(f"文件已存在: {download_path}")
                return download_path
            
            try:
                print(f"下载报告: {report['title']}")
                # 设置请求头，模拟浏览器
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                    'Accept': 'application/pdf',
                    'Referer': 'https://ir.tesla.com/',
                    'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"'
                }
                
                response = requests.get(report['url'], headers=headers)
                
                # 检查是否成功
                if response.status_code == 200 and 'application/pdf' in response.headers.get('Content-Type', ''):
                    with open(download_path, 'wb') as f:
                        f.write(response.content)
                    
                    print(f"报告已下载: {download_path}")
                    return download_path
                else:
                    print(f"下载失败: 状态码 {response.status_code}, 内容类型 {response.headers.get('Content-Type')}")
                    print("推荐改用浏览器直接下载，调用方法时设置 open_browser=True")
                    
                    if open_browser:
                        open_url = report.get('direct_url', report['url'])
                        print(f"在浏览器中打开: {open_url}")
                        webbrowser.open(open_url)
                
            except Exception as e:
                print(f"下载失败: {str(e)}")
                print("推荐使用浏览器直接下载")
        
        return None

if __name__ == "__main__":
    # # 使用示例
    tesla_reports = TeslaFinancialReports()
    
    # # 获取年度财务报表
    # income_stmt = tesla_reports.get_income_statement()
    # balance_sheet = tesla_reports.get_balance_sheet()
    # cash_flow = tesla_reports.get_cash_flow()
    
    # # 获取季度财务报表
    # quarterly_income = tesla_reports.get_income_statement(period="quarterly")
    
    # # 获取关键财务指标
    # metrics = tesla_reports.get_key_financial_metrics()
    
    # # 生成财务摘要报告
    # summary = tesla_reports.generate_financial_summary()
    # print(summary)
    
    print("\n===== 特斯拉财报下载选项 =====")
    print("由于特斯拉网站限制自动下载，提供以下选项：")
    
    # 显示可用的静态报告
    reports_dict = tesla_reports.get_static_tesla_report_links()
    
    print("\n--- 年度报告 ---")
    for year, report in sorted(reports_dict["annual"].items(), key=lambda x: x[0], reverse=True):
        print(f"{year}: {report['title']}")
    
    print("\n--- 季度报告 ---")
    for quarter, report in sorted(reports_dict["quarterly"].items(), key=lambda x: x[0], reverse=True):
        print(f"{quarter}: {report['title']}")
    
    # 尝试在浏览器中打开2022年报
    print("\n尝试使用浏览器打开2022年年报...")
    tesla_reports.download_static_report("2022", "annual", open_browser=True)
    
    # # 尝试下载最新季报
    # print("\n尝试下载2023年第3季度报告...")
    # tesla_reports.download_static_report("2023-Q3", "quarterly", download_dir="./reports")
    
    print("\n可以调用 download_static_report() 方法下载其他年份的报告")
    print("示例: tesla_reports.download_static_report(\"2021\", \"annual\", open_browser=True)") 