import akshare as ak
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class StockTickData:
    """
    获取股票分笔交易数据
    """
    
    @staticmethod
    def get_tick_data(stock_code: str, trade_date: str) -> pd.DataFrame:
        """
        获取指定股票在指定日期的分笔交易数据
        
        Args:
            stock_code (str): 股票代码 (e.g., "000001")
            trade_date (str): 交易日期，格式为 YYYY-MM-DD (e.g., "2024-04-15")
            
        Returns:
            pd.DataFrame: 包含以下字段的DataFrame:
                - 成交时间
                - 成交价格
                - 价格变动
                - 成交量
                - 成交额
                - 性质
        """
        try:
            # 验证日期格式
            datetime.strptime(trade_date, '%Y-%m-%d')
            
            # 移除日期中的连字符
            date_str = trade_date.replace('-', '')
            
            # 调用akshare接口获取分笔数据
            # 根据最新文档，date参数应该放在symbol后面
            df = ak.stock_zh_a_tick_tx_js(symbol=stock_code)
            
            if df.empty:
                logger.warning(f"未获取到股票 {stock_code} 在 {trade_date} 的分笔数据")
                return pd.DataFrame()
                
            # 数据清洗和格式化
            df['trade_date'] = trade_date
            df['stock_code'] = stock_code
            
            # 重命名列以便于理解
            df = df.rename(columns={
                '成交时间': 'trade_time',
                '成交价格': 'price',
                '价格变动': 'price_change',
                '成交量': 'volume',
                '成交额': 'amount',
                '性质': 'trade_type'
            })
            
            return df
            
        except ValueError as e:
            logger.error(f"日期格式错误: {trade_date}, 请使用YYYY-MM-DD格式")
            raise ValueError(f"日期格式错误: {trade_date}, 请使用YYYY-MM-DD格式") from e
            
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 在 {trade_date} 的分笔数据时发生错误: {str(e)}")
            raise Exception(f"获取分笔数据失败: {str(e)}") from e

    @staticmethod
    def get_tick_data_batch(stock_codes: list, trade_date: str) -> dict:
        """
        批量获取多个股票的分笔数据
        
        Args:
            stock_codes (list): 股票代码列表
            trade_date (str): 交易日期，格式为 YYYY-MM-DD
            
        Returns:
            dict: 股票代码到对应分笔数据DataFrame的映射
        """
        results = {}
        for code in stock_codes:
            try:
                df = StockTickData.get_tick_data(code, trade_date)
                if not df.empty:
                    results[code] = df
            except Exception as e:
                logger.error(f"获取股票 {code} 分笔数据失败: {str(e)}")
                continue
        return results 

    @staticmethod
    def save_tick_data_to_csv(stock_code: str, trade_date: str, output_dir: str = "./") -> str:
        """
        获取分笔数据并保存为CSV文件
        
        Args:
            stock_code (str): 股票代码
            trade_date (str): 交易日期，格式为 YYYY-MM-DD
            output_dir (str): 输出目录，默认为当前目录
            
        Returns:
            str: CSV文件的完整路径
        """
        try:
            df = StockTickData.get_tick_data(stock_code, trade_date)
            if df.empty:
                logger.warning(f"没有数据可保存")
                return ""
                
            # 生成文件名：股票代码_日期_tick.csv
            filename = f"{stock_code}_{trade_date.replace('-', '')}_tick.csv"
            file_path = f"{output_dir}/{filename}"
            
            # 保存为CSV
            df.to_csv(file_path, index=False, encoding='utf-8')
            logger.info(f"分笔数据已保存到: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"保存分笔数据到CSV时发生错误: {str(e)}")
            raise

if __name__ == "__main__":
    # 设置日志格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 示例参数
        stock_code = "sh603200"  # 平安银行
        trade_date = "2005-03-03"  # 交易日期
        
        # 获取并保存数据
        file_path = StockTickData.save_tick_data_to_csv(stock_code, trade_date)
        if file_path:
            print(f"数据已成功保存到: {file_path}")
            
        # # 批量获取多个股票的数据示例
        # stock_codes = ["000001", "600000", "600036"]
        # for code in stock_codes:
        #     file_path = StockTickData.save_tick_data_to_csv(code, trade_date)
        #     if file_path:
        #         print(f"股票 {code} 的数据已保存到: {file_path}")
                
    except Exception as e:
        print(f"程序执行出错: {str(e)}") 