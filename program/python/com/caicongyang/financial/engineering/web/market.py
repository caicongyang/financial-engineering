import os
import pandas as pd
import akshare as ak
from deepseek_chat import DeepSeekChat
from datetime import datetime, timedelta
import json
from db_utils import save_report_to_db, init_db
# 导入WXPublisher类用于微信公众号发布
import sys

# 直接导入WXPublisher
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
from WXPublisher import WXPublisher

# 初始化数据库
init_db()

def analyze_market_data(chat: DeepSeekChat, index_data: pd.DataFrame, gdp: pd.DataFrame = None,
                        pmi: pd.DataFrame = None, cpi: pd.DataFrame = None, 
                        huilv: pd.DataFrame = None, sp500: pd.DataFrame = None,
                        lhb: pd.DataFrame = None, board: pd.DataFrame = None,
                        market: pd.DataFrame = None, 
                        fund_flow: pd.DataFrame = None) -> str:
    """分析大盘数据"""
    
    try:
        # 构建分析请求，包含角色提示
        analysis_request = f"""# Role: 大盘趋势分析师

## Profile
- language: 中文
- description: 大盘趋势分析师专注于通过分析市场指数、市场情绪等多维度数据，识别和预测市场整体趋势，帮助投资者把握市场机会和规避系统性风险。
- background: 拥有宏观经济、金融市场、技术分析和人工智能的背景，专注于股票市场的整体趋势分析。
- personality: 冷静、理性、全面、客观
- expertise: 宏观经济分析、指数技术分析、资金流向分析、市场情绪分析
- target_audience: 股票投资者、基金经理、投资顾问、策略交易员

## Skills

1. 技术分析
   - 趋势识别: 能够识别指数的长期、中期和短期趋势，判断市场所处的阶段。
   - 形态分析: 识别头肩顶、双底等经典形态，判断可能的反转或延续信号。
   - 支撑阻力: 确定关键支撑位和阻力位，预测可能的反弹或回调区域。
   - 成交量分析: 分析成交量与价格的配合关系，判断趋势的可信度。

2. 资金面分析
   - 板块轮动: 识别市场资金在不同板块间的流动趋势，预测可能的热点轮动。
   - 融资融券: 分析两融余额变化，判断市场杠杆水平和投资者信心。
   - 新增开户: 评估市场新增投资者数量，判断市场参与热情。

3. 市场情绪分析
   - 恐慌指数: 分析市场波动率指标，判断市场恐慌或贪婪程度。
   - 涨跌家数: 评估上涨/下跌股票的比例，判断市场的广度。
   - 涨跌停数量: 分析涨停/跌停股票数量，判断市场情绪极端程度。
   - 换手率: 分析市场整体换手率，判断交投活跃度和可能的短期顶底。

4. 宏观分析
   - 经济数据: 解读GDP、PMI、CPI等宏观经济数据对市场的影响。
   - 政策面: 分析货币政策、财政政策对股市的潜在影响。
   - 国际因素: 评估全球市场联动性、地缘政治等因素对A股的影响。
   - 流动性: 分析市场整体流动性状况，判断资金面松紧程度。

## Rules

1. 基本原则：
   - 数据驱动: 确保所有分析基于数据，而非主观臆断。
   - 多维视角: 从技术、资金、情绪、基本面等多角度分析市场。
   - 客观中立: 不带有个人偏见，平衡呈现乐观与谨慎的观点。
   - 前瞻视角: 不仅分析当前情况，更要提供对未来走势的预判。

2. 行为准则：
   - 明确区分事实与观点: 清晰标明哪些是数据事实，哪些是分析推断。
   - 避免极端预测: 不做夸张的涨跌预测，保持理性克制。
   - 明确风险提示: 对任何预测都附带可能的风险因素。
   - 历史参考: 适当引用历史相似情形，但不教条地认为历史必然重复。

3. 限制条件：
   - 市场不确定性: 承认市场存在不可预测因素，任何分析都有失准可能。
   - 数据局限性: 分析受限于可获取数据的质量和覆盖范围。
   - 周期适用性: 明确指出分析适用的时间周期（短线、中线或长线）。
   - 免责声明: 分析仅供参考，不构成投资建议，投资者需自担风险。

## Workflows

- 目标: 综合分析市场指数、资金流向、市场情绪等数据，研判大盘趋势，提供操作建议。
- 步骤 1: 接收并处理市场指数、宏观经济、资金流向、市场情绪等数据。
- 步骤 2: 分析指数技术形态，识别趋势、形态、关键价位等。
- 步骤 3: 分析宏观经济数据（GDP、PMI、CPI、汇率等）对市场的影响。
- 步骤 4: 分析板块资金流向等资金面因素。
- 步骤 5: 评估市场情绪指标和赚钱效应，判断市场情绪是否过热或过冷。
- 步骤 6: 分析标普500指数/纳指100指数和国际市场对A股的影响。
- 步骤 7: 结合龙虎榜数据和板块数据，识别主力资金动向和热点板块。
- 步骤 8: 综合研判市场趋势，提供市场前景展望和不同时间周期的操作建议。
- 步骤 9: 明确列出支持分析结论的具体数据点和技术指标。
- 预期结果: 提供客观、全面、前瞻性的市场分析报告，帮助投资者把握市场脉搏。

## Initialization
作为大盘趋势分析师，你必须遵守上述Rules，按照Workflows执行任务。

请以Markdown格式输出你的分析报告，使用适当的标题、列表、表格和强调语法，确保报告结构清晰、易于阅读。

以下是各项市场数据：

1. 大盘指数数据：
{index_data}

{"" if gdp is None else f'''
2. GDP指数数据：
{gdp}
'''}

{"" if pmi is None else f'''
3. PMI指数数据：
{pmi}
'''}

{"" if cpi is None else f'''
4. CPI指数数据：
{cpi}
'''}

{"" if huilv is None else f'''
5. 人民币/美元汇率数据：
{huilv}
'''}

{"" if sp500 is None else f'''
6. 标普500指数数据：
{sp500}
'''}

{"" if fund_flow is None else f'''
7. 概念板块资金流向数据：
{fund_flow}
'''}

{"" if board is None else f'''
8. 板块数据：
{board}
'''}

{"" if lhb is None else f'''
9. 龙虎榜数据：
{lhb}
'''}

{"" if market is None else f'''
10. 赚钱效应数据：
{market}
'''}



"""
        
        # 发送请求并获取分析结果
        result = chat.simple_chat(analysis_request)
        
        # 返回分析结果
        return result
        
    except Exception as e:
        return f"分析过程中发生错误: {str(e)}"

def get_market_index_data(days: int = 30, target_date: str = None) -> pd.DataFrame:
    """获取市场指数数据，默认获取最近30天的数据
    
    Args:
        days: 获取数据的天数
        target_date: 目标日期，格式为YYYY-MM-dd
    """
    try:
        # 如果提供了日期，将其转换为datetime对象
        target_datetime = None
        if target_date:
            target_datetime = datetime.strptime(target_date, "%Y-%m-%d")
        else:
            target_datetime = datetime.now()
            
        # 获取上证指数数据
        df_sh = ak.stock_zh_index_daily(symbol="sh000001")
        # 获取深证成指数据
        df_sz = ak.stock_zh_index_daily(symbol="sz399001")
        # 获取创业板指数据
        df_cyb = ak.stock_zh_index_daily(symbol="sz399006")
        
        # 限制数据范围为最近n天
        df_sh = df_sh.tail(days)
        df_sz = df_sz.tail(days)
        df_cyb = df_cyb.tail(days)
        
        # 添加指数名称列
        df_sh['index_name'] = '上证指数'
        df_sz['index_name'] = '深证成指'
        df_cyb['index_name'] = '创业板指'
        
        # 合并数据
        df_combined = pd.concat([df_sh, df_sz, df_cyb])
        
        return df_combined
    except Exception as e:
        print(f"获取市场指数数据失败: {e}")
        return None

def get_gdp_data(target_date: str = None) -> pd.DataFrame:
    """获取GDP指数数据
    
    Args:
        target_date: 目标日期，格式为YYYY-MM-dd
    """
    try:
        # 获取季度GDP数据
        df = ak.macro_china_gdp()
        return df
    except Exception as e:
        print(f"获取GDP数据失败: {e}")
        return None

def get_pmi_data(target_date: str = None) -> pd.DataFrame:
    """获取PMI指数数据
    
    Args:
        target_date: 目标日期，格式为YYYY-MM-dd
    """
    try:
        # 获取制造业PMI数据
        df = ak.macro_china_pmi_yearly()
        return df
    except Exception as e:
        print(f"获取PMI数据失败: {e}")
        return None

def get_cpi_data(target_date: str = None) -> pd.DataFrame:
    """获取CPI指数数据
    
    Args:
        target_date: 目标日期，格式为YYYY-MM-dd
    """
    try:
        # 获取CPI数据
        df = ak.macro_china_cpi_yearly()
        return df
    except Exception as e:
        print(f"获取CPI数据失败: {e}")
        return None

def get_exchange_rate(target_date: str = None) -> pd.DataFrame:
    """获取人民币/美元汇率数据
    
    Args:
        target_date: 目标日期，格式为YYYY-MM-dd
    """
    try:
        # 获取人民币汇率数据
        df = ak.forex_hist_em(symbol="USDCNH")
        return df
    except Exception as e:
        print(f"获取汇率数据失败: {e}")
        return None

def get_sp500_data(target_date: str = None) -> pd.DataFrame:
    """获取标普500指数数据
    
    Args:
        target_date: 目标日期，格式为YYYY-MM-dd
    """
    try:
        # 获取标普500指数数据 和 纳指100指数数据
        print("获取标普500指数数据...")
        df = ak.index_us_stock_sina(symbol=".INX")
        ndxDf =ak.index_us_stock_sina(symbol=".NDX ")
         # 添加指数名称列
        df['index_name'] = '标普500'
        ndxDf['index_name'] = '纳指100'
        print(df)
        print(ndxDf)
        # 合并数据
        df_combined = pd.concat([df, ndxDf])
        return df_combined
    except Exception as e:
        print(f"获取标普500指数数据失败: {e}")
        return None

def get_dragon_tiger_list(target_date: str = None) -> pd.DataFrame:
    """获取龙虎榜数据
    
    Args:
        target_date: 目标日期，格式为YYYY-MM-dd
    """
    try:
        # 使用提供的日期或当前日期
        current_date = None
        if target_date:
            # 将YYYY-MM-dd格式转换为YYYYMMDD格式
            current_date = target_date.replace("-", "")
        else:
            current_date = datetime.now().strftime("%Y%m%d")
            
        # 龙虎榜数据
        df = ak.stock_lhb_detail_em(start_date=current_date, end_date=current_date)
        return df
    except Exception as e:
        print(f"获取龙虎榜数据失败: {e}")
        return None

def get_sector_data(target_date: str = None) -> pd.DataFrame:
    """获取板块数据
    
    Args:
        target_date: 目标日期，格式为YYYY-MM-dd
    """
    try:
        # 获取行业板块数据
        df_industry = ak.stock_sector_spot()
        # 获取概念板块数据
        df_concept = ak.stock_board_concept_name_em()
        
        # 合并数据
        df_combined = pd.concat([
            df_industry.assign(板块类型='行业板块'),
            df_concept.assign(板块类型='概念板块')
        ])
        
        return df_combined
    except Exception as e:
        print(f"获取板块数据失败: {e}")
        return None

def get_market_effect(target_date: str = None) -> pd.DataFrame:
    """获取赚钱效应数据
    
    Args:
        target_date: 目标日期，格式为YYYY-MM-dd
    """
    try:
        # 获取市场活跃度数据
        print("获取市场活跃度数据...")
        effect_df = ak.stock_market_activity_legu()
        print(effect_df)
    
        return effect_df
        
    except Exception as e:
        print(f"获取赚钱效应数据失败: {e}")
        return None

def get_concept_fund_flow(target_date: str = None) -> pd.DataFrame:
    """获取概念板块资金流入流出数据
    
    Args:
        target_date: 目标日期，格式为YYYY-MM-dd
    """
    try:
        # 获取概念板块资金流向数据
        df = ak.stock_individual_fund_flow_rank(indicator="今日")
        return df
    except Exception as e:
        print(f"获取概念板块资金流向数据失败: {e}")
        return None

def save_to_markdown(content: str, target_date: str = None) -> str:
    """将分析结果保存为Markdown文件
    
    Args:
        content: 分析结果内容
        target_date: 目标日期，格式为YYYY-MM-dd
    """
    try:
        # 使用提供的日期或当前日期
        report_date = None
        if target_date:
            report_date = target_date.replace("-", "")
        else:
            report_date = datetime.now().strftime("%Y%m%d")
            
        # 创建文件名，包含日期时间
        now = datetime.now().strftime("%H%M%S")
        filename = f"market_analysis_{report_date}_{now}.md"
        
        # 写入文件
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
        
        return filename
    except Exception as e:
        print(f"保存Markdown文件时出错: {e}")
        return None

def save_market_analysis_to_db(content: str, data_summary=None, target_date: str = None) -> str:
    """将市场分析结果保存到MySQL数据库
    
    Args:
        content: 报告内容 (Markdown格式)
        data_summary: 数据摘要，用于存储相关数据
        target_date: 目标日期，格式为YYYY-MM-dd
        
    Returns:
        report_id: 报告ID
    """
    try:
        # 使用提供的日期或当前日期
        report_date = target_date if target_date else datetime.now().strftime("%Y-%m-%d")
        
        title = f"大盘分析报告 {report_date}"
        
        # 保存到数据库
        report_id = save_report_to_db(
            report_type='market',
            code=None,  # 市场分析没有具体股票代码
            title=title,
            content=content,
            related_data=data_summary,  # 现在可以直接传入字典，db_utils会处理JSON转换
            tags='大盘,市场分析'
        )
        
        return report_id
    except Exception as e:
        print(f"保存市场分析到数据库时出错: {e}")
        return None

def main(target_date: str = None, should_publish_wechat: bool = False):
    """
    主函数，执行市场数据分析流程
    
    Args:
        target_date: 目标日期，格式为YYYY-MM-dd。如果不提供，则使用当前日期
        should_publish_wechat: 是否发布到微信公众号
    """
    try:
        # 如果未提供日期，使用当前日期
        if target_date is None:
            target_date = datetime.now().strftime("%Y-%m-%d")
            
        print(f"使用分析日期: {target_date}")
            
        # 初始化 DeepSeekChat，只传入必要参数
        api_key = os.environ.get("LLM_API_KEY", "")
        base_url = os.environ.get("LLM_BASE_URL", "")
        print(f"使用 API 密钥: {api_key}")
        print(f"使用基础 URL: {base_url}")
        
        # 去掉 proxies 参数，只使用必要的参数
        chat = DeepSeekChat(api_key=api_key, base_url=base_url)
        
        print("正在获取市场数据...")
        
        # 获取市场指数数据
        print("获取市场指数数据...")
        index_data = get_market_index_data(days=30, target_date=target_date)
        print(index_data)
        if index_data is None:
            print("获取市场指数数据失败，将使用本地数据（如果有）")
        
        # 获取GDP数据
        print("获取GDP数据...")
        gdp = get_gdp_data(target_date=target_date)
        print(gdp)
        if gdp is None:
            print("获取GDP数据失败")
        
        # 获取PMI数据
        print("获取PMI数据...")
        pmi = get_pmi_data(target_date=target_date)
        print(pmi)
        if pmi is None:
            print("获取PMI数据失败")
        
        # 获取CPI数据
        print("获取CPI数据...")
        cpi = get_cpi_data(target_date=target_date)
        print(cpi)
        if cpi is None:
            print("获取CPI数据失败")
        
        # 获取汇率数据
        print("获取汇率数据...")
        huilv = get_exchange_rate(target_date=target_date)
        print(huilv)
        if huilv is None:
            print("获取汇率数据失败")
        
        # 获取标普500指数数据
        print("获取标普500指数数据...")
        sp500 = get_sp500_data(target_date=target_date)
        print(sp500)
        if sp500 is None:
            print("获取标普500指数数据失败")
        
        # 获取概念板块资金流向数据
        print("获取概念板块资金流向数据...")
        fund_flow = get_concept_fund_flow(target_date=target_date)
        print(fund_flow)
        if fund_flow is None:
            print("获取概念板块资金流向数据失败")
        
        # 获取板块数据
        print("获取板块数据...")
        board = get_sector_data(target_date=target_date)
        print(board)
        if board is None:
            print("获取板块数据失败")
        
        # 获取龙虎榜数据
        print("获取龙虎榜数据...")
        lhb = get_dragon_tiger_list(target_date=target_date)
        print(lhb)
        if lhb is None:
            print("获取龙虎榜数据失败")
        
        # 获取赚钱效应数据
        print("获取赚钱效应数据...")
        market = get_market_effect(target_date=target_date)
        print(market)
        if market is None:
            print("获取赚钱效应数据失败")
        
        if index_data is not None:
            # 打印数据基本信息
            print(f"获取到 {len(index_data)} 条市场指数数据")
            
            # 分析大盘数据
            print("开始分析大盘数据...")
            result = analyze_market_data(
                chat=chat,
                index_data=index_data,
                gdp=gdp,
                pmi=pmi,
                cpi=cpi,
                huilv=huilv,
                sp500=sp500,
                lhb=lhb,
                board=board,
                market=market,
                fund_flow=fund_flow
            )
            
            # 准备数据摘要
            data_summary = {
                "analysis_date": target_date,
                "index_data_count": len(index_data) if index_data is not None else 0,
                "gdp_data_available": gdp is not None,
                "pmi_data_available": pmi is not None,
                "cpi_data_available": cpi is not None,
                "huilv_data_available": huilv is not None,
                "sp500_data_available": sp500 is not None,
                "lhb_data_available": lhb is not None,
                "board_data_available": board is not None,
                "market_data_available": market is not None,
                "fund_flow_data_available": fund_flow is not None,
                "analysis_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "main_indices": {
                    "上证指数": index_data[index_data['index_name'] == '上证指数'].iloc[-1]['close'] if index_data is not None else None,
                    "深证成指": index_data[index_data['index_name'] == '深证成指'].iloc[-1]['close'] if index_data is not None else None,
                    "创业板指": index_data[index_data['index_name'] == '创业板指'].iloc[-1]['close'] if index_data is not None else None
                }
            }
            
            # 保存分析结果到MySQL数据库
            report_id = save_market_analysis_to_db(result, data_summary, target_date=target_date)
            if report_id:
                print(f"分析报告已保存到MySQL数据库，ID: {report_id}")
            
            # 同时也保存到本地文件（可选）
            md_file = save_to_markdown(result, target_date=target_date)
            if md_file:
                print(f"分析报告已同时保存到本地文件: {md_file}")
                
            # 如果需要发布到微信公众号
            if should_publish_wechat:
                try:
                    print("开始发布到微信公众号...")
                    # 创建微信发布器实例
                    wx_publisher = WXPublisher()
                    
                    # 准备发布参数
                    title = f"大盘市场分析报告 {target_date}"
                    # 提取报告摘要作为文章摘要
                    # 提取报告前100个字符作为摘要
                    digest = f"大盘市场分析报告 {target_date}"
                    
                    # 异步发布到微信公众号
                    import asyncio
                    publish_result = asyncio.run(wx_publisher.push_recommendation(
                        content=result,
                        title=title,
                        digest=digest
                    ))
                    
                    if publish_result and publish_result.get('status') != 'error':
                        print(f"微信公众号发布成功: {json.dumps(publish_result, ensure_ascii=False)}")
                    else:
                        print(f"微信公众号发布失败: {json.dumps(publish_result, ensure_ascii=False)}")
                except Exception as e:
                    print(f"微信公众号发布过程中发生错误: {str(e)}")
            
            # 打印分析结果
            print("\n" + "="*50)
            print(f"大盘分析报告 {target_date}")
            print("="*50 + "\n")
            print(result)
            print("\n" + "="*50)
        else:
            print("无法获取足够的市场数据进行分析")
        
    except Exception as e:
        print(f"程序运行出错: {str(e)}")

if __name__ == "__main__":
   
    main("2025-04-22", True) 