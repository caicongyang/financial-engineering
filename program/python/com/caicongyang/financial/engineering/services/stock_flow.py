import os
import pandas as pd
import akshare as ak
from deepseek_chat import DeepSeekChat
from datetime import datetime, timedelta
import json
from db_utils import save_report_to_db, init_db, engine
from sqlalchemy import text

# 初始化数据库
init_db()

def analyze_stock_data(chat: DeepSeekChat, df0: pd.DataFrame, df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame) -> str:
    """分析股票数据
    
    Args:
        chat: DeepSeekChat实例
        df_list: 包含3天数据的DataFrame列表
    """
    
    try:        
        # 构建分析请求，包含角色提示
        analysis_request = f"""
# Role: 主力动向分析师

## Profile
- language: 中文
- description: 主力动向分析师专注于通过分析集合竞价时间的资金明细，识别和预测主力资金的动向，帮助投资者做出更明智的投资决策。
- background: 拥有金融分析、数据科学和人工智能的背景，专注于股票市场的主力资金分析。
- personality: 严谨、细致、逻辑性强
- expertise: 金融数据分析、主力资金动向预测、股票市场分析
- target_audience: 股票投资者、金融分析师、投资机构

## Skills
1. 数据分析
   - 数据清洗: 能够处理和分析大量的历史分笔数据，确保数据的准确性和完整性。
   - 模式识别: 能够识别主力资金的典型操作模式，如大单买入或卖出。
   - 趋势预测: 基于历史数据，预测股票的未来走势。
   - 异常检测: 能够检测数据中的异常点，如异常的大单交易。

2. 金融知识
   - 股票市场分析: 深入理解股票市场的运作机制和影响因素。
   - 主力资金行为分析: 熟悉主力资金的常见操作手法和策略。
   - 投资策略建议: 能够根据分析结果，提供具体的投资策略建议。

## Rules
1. 基本原则：
   - 数据驱动: 所有分析和结论必须基于提供的数据，禁止编造或假设数据。
   - 客观公正: 分析过程中保持客观，不受个人情感或外部因素影响。
   - 透明性: 分析方法和过程必须透明，便于验证和复现。
   - 及时性: 分析结果应及时提供，确保信息的时效性。

2. 行为准则：
   - 保密性: 严格保护用户提供的数据，不泄露任何敏感信息。
   - 专业性: 保持专业态度，提供高质量的分析服务。
   - 用户导向: 以用户需求为中心，提供有针对性的分析建议。
   - 持续学习: 不断更新知识和技能，适应市场变化。

3. 限制条件：
   - 数据限制: 分析结果受限于提供的数据质量和数量。
   - 市场风险: 股票市场存在不确定性，分析结果仅供参考。
   - 时间限制: 分析过程可能需要一定时间，用户需耐心等待。
   - 技术限制: 分析工具和方法可能存在技术限制，影响分析结果。

## Workflows
- 目标: 分析个股的历史分笔数据，识别主力资金的动向，预测未来走势。
- 步骤 1: 数据清洗和预处理，确保数据的准确性和完整性。
- 步骤 2: 分析数据中的大单交易，识别主力资金的典型操作模式。
- 步骤 3: 基于识别出的模式，预测股票的未来走势。
- 预期结果: 提供详细的分析报告，包括主力资金的操作手法和未来走势预测。

## Initialization
作为主力动向分析师，你必须遵守上述Rules，按照Workflows执行任务。

---

以下是个股的历史分笔数据，这是四天的数据{df0},{df1},{df2},{df3}，
你帮我分析一下这只股票是否存在主力操盘的行为，如果是，那么主力是如何操作的，以及接下来的走势如何？
必须根据投喂你的数据进行分析，禁止自己编造假数据。描述当天的股价走势，自证没有虚构数据。你的观点需要数据支撑。
"""
        
        # 发送请求并获取分析结果
        result = chat.simple_chat(analysis_request)
        
        # 返回分析结果
        return result
        
    except Exception as e:
        return f"分析过程中发生错误: {str(e)}"

def save_to_markdown(content: str, stock_code: str) -> str:
    """将分析结果保存为Markdown文件"""
    try:
        # 创建文件名，包含股票代码和当前日期时间
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{stock_code}_analysis_{now}.md"
        
        # 写入文件
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
        
        return filename
    except Exception as e:
        print(f"保存Markdown文件时出错: {e}")
        return None

def save_stock_analysis_to_db(content: str, stock_code: str, stock_data_summary=None) -> str:
    """将股票分析结果保存到MySQL数据库
    
    Args:
        content: 报告内容 (Markdown格式)
        stock_code: 股票代码
        stock_data_summary: 相关股票数据摘要
        
    Returns:
        report_id: 报告ID
    """
    try:
        # 获取报告标题 (默认取内容的第一行，通常是标题)
        title_line = content.strip().split('\n')[0]
        title = title_line.strip('# ')
        
        # 如果没有找到合适的标题，使用默认标题
        if not title or len(title) < 2:
            title = f"股票 {stock_code} 分析报告 {datetime.now().strftime('%Y-%m-%d')}"
        
        # 保存到数据库
        report_id = save_report_to_db(
            report_type='stock',
            code=stock_code,
            title=title,
            content=content,
            related_data=stock_data_summary,  # 直接传入字典，db_utils会处理JSON转换
            tags=f'股票,资金流向,{stock_code}'
        )
        
        return report_id
    except Exception as e:
        print(f"保存股票分析到数据库时出错: {e}")
        return None

def main(stock_code=None):
    try:
        # 初始化 DeepSeekChat
        chat = DeepSeekChat()
        
        # 设置要分析的股票代码
        if stock_code is None:
            stock_code = "sz002564"  # 默认股票代码
        
        print(f"正在获取股票 {stock_code} 的分笔数据...")
        
        # 使用 akshare 获取分笔数据
        try:
            # 从t_stock表中获取最近的4个交易日
            # 去掉前缀以匹配数据库中的股票代码格式
            clean_stock_code = stock_code.replace('sh', '').replace('sz', '')
            
            # 使用SQLAlchemy和text查询，参考inputHistoryEtfDataFromAkShare.py
            from sqlalchemy import text
            
            # 查询最近的4个交易日
            query = text("""
                SELECT trade_date 
                FROM t_stock 
                WHERE stock_code = :code 
                ORDER BY trade_date DESC 
                LIMIT 4
            """)
            
            with engine.connect() as conn:
                result = conn.execute(query, {"code": clean_stock_code})
                trade_dates = result.fetchall()
            
            # 确保我们有足够的交易日期数据
            if len(trade_dates) < 4:
                print(f"警告: 只找到 {len(trade_dates)} 个交易日，少于需要的4个")
                # 如果没有足够的日期数据，使用默认日期
                default_dates = ['20250417', '20250418', '20250421', '20250422']
                date0, date1, date2, date3 = default_dates
            else:
                # 转换为列表并按日期升序排列
                date_list = [date[0] for date in trade_dates]
                date_list.sort()  # 确保日期是按升序排列的
                
                # 确保日期格式正确 (移除任何连字符)
                date_list = [str(d).replace('-', '') for d in date_list]
                
                date0, date1, date2, date3 = date_list
            
            print(f"获取日期 {date0}, {date1}, {date2}, {date3} 的分笔数据")
            
            # 分别获取每一天的数据
            df0 = ak.stock_intraday_sina(symbol=stock_code, date=date0)
            df1 = ak.stock_intraday_sina(symbol=stock_code, date=date1)
            df2 = ak.stock_intraday_sina(symbol=stock_code, date=date2)
            df3 = ak.stock_intraday_sina(symbol=stock_code, date=date3)
            
            # 确保我们至少有一天的数据
            if (df0 is None or df0.empty) and (df1 is None or df1.empty) and (df2 is None or df2.empty) and (df3 is None or df3.empty):
                raise Exception("未能获取到任何分笔数据")
            
            # 确保空值被替换为空DataFrame
            df0 = df0 if df0 is not None and not df0.empty else pd.DataFrame()
            df1 = df1 if df1 is not None and not df1.empty else pd.DataFrame()
            df2 = df2 if df2 is not None and not df2.empty else pd.DataFrame()
            df3 = df3 if df3 is not None and not df3.empty else pd.DataFrame()
            
            # 添加日期信息到每个DataFrame
            if not df0.empty:
                df0['date'] = date0
            if not df1.empty:
                df1['date'] = date1
            if not df2.empty:
                df2['date'] = date2
            if not df3.empty:
                df3['date'] = date3
            
            # 分析股票数据，明确传递DataFrame
            print("开始分析股票数据...")
            result = analyze_stock_data(chat, df0, df1, df2, df3)
            
            # 准备数据摘要
            data_summary = {
                "stock_code": stock_code,
                "data_dates": [date0, date1, date2, date3],
                "data_counts": [len(df0) if not df0.empty else 0,
                               len(df1) if not df1.empty else 0, 
                               len(df2) if not df2.empty else 0, 
                               len(df3) if not df3.empty else 0],
                "columns": df1.columns.tolist() if not df1.empty else df2.columns.tolist() if not df2.empty else df3.columns.tolist() if not df3.empty else [],
                "analysis_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # 保存分析结果到MySQL数据库
            report_id = save_stock_analysis_to_db(result, stock_code, data_summary)
            if report_id:
                print(f"分析报告已保存到MySQL数据库，ID: {report_id}")
            
            # 同时也保存到本地文件
            md_file = save_to_markdown(result, stock_code)
            if md_file:
                print(f"分析报告已同时保存到本地文件: {md_file}")
            
            # 打印分析结果
            print("\n" + "="*50)
            print(f"股票 {stock_code} 分析报告")
            print("="*50 + "\n")
            print(result)
            print("\n" + "="*50)
            
            return result
            
        except Exception as e:
            print(f"获取股票数据失败: {e}")
            raise
    
    except Exception as e:
        print(f"程序运行出错: {str(e)}")
        return None

if __name__ == "__main__":
    main("sz002564")  # Changed from "00300" to standard format for Shenzhen stocks 