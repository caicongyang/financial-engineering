#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
ETF平台突破选股策略 - 从MySQL数据库读取数据

策略说明：
1. 识别ETF箱体整理：寻找价格在一定范围内震荡的整理期（至少30天）
2. 检测成交量变化：判断20日内是否出现过显著放量
3. 突破确认：价格突破前期高点
4. 趋势确认：使用均线系统判断趋势
5. 风险控制：设置箱体波动范围限制

选股条件：
1. 前期处于箱体整理
   - 波动幅度在3%-10%之间
   - 前后半段均价变化不超过3%
   - 持续至少30天
2. 20日内出现过放量（成交量超过前一日均量2倍）
3. 当日价格突破前20日最高价
4. 均线呈多头排列（MA5 > MA10 > MA20）

参数说明：
- platform_days: 30天（箱体期观察天数）
- volume_threshold: 2.0（放量判断倍数）
- min_platform_days: 20（最小有效数据天数）
"""

import pandas as pd
import numpy as np
import datetime
import logging
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from com.caicongyang.financial.engineering.utils.env_loader import load_env

# 加载环境变量 - 使用通用加载模块
load_env()

# 配置日志输出格式
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EtfPlatformBreakoutStrategy:
    def __init__(self):
        # 策略参数配置
        self.platform_days = 30        # 平台期观察天数：观察多少天的价格波动
        self.volume_threshold = 2.0    # 放量倍数阈值：成交量超过均量的倍数
        self.min_platform_days = 20    # 最小平台天数：最少需要多少天形成平台
        self.atr_window = 30  # 新增ATR窗口参数
        
        # 数据库连接配置
        self.mysql_user = os.getenv('DB_USER')
        self.mysql_password = os.getenv('DB_PASSWORD')
        self.mysql_host = os.getenv('DB_HOST')
        self.mysql_port = os.getenv('DB_PORT')
        self.mysql_db = os.getenv('DB_NAME')
        # 创建数据库连接引擎
        self.engine = create_engine(
            f'mysql+pymysql://{self.mysql_user}:{self.mysql_password}@'
            f'{self.mysql_host}:{self.mysql_port}/{self.mysql_db}'
        )
        
    def get_etf_data(self, etf_code, start_date=None, end_date=None):
        """
        从MySQL数据库获取ETF历史数据
        
        参数:
            etf_code (str): ETF代码
            start_date (str): 开始日期，格式：YYYY-MM-DD
            end_date (str): 结束日期，格式：YYYY-MM-DD
            
        返回:
            DataFrame: 包含ETF历史交易数据的DataFrame
        """
        try:
            query = f"""
                SELECT 
                    trade_date as date,
                    open,
                    close,
                    high,
                    low,
                    volume,
                    amount
                FROM t_etf 
                WHERE stock_code = '{etf_code}'
                AND trade_date BETWEEN '{start_date}' AND '{end_date}'
                AND close IS NOT NULL
                AND volume > 0
                ORDER BY trade_date
            """
            
            df = pd.read_sql(query, self.engine)
            
            # 检查是否有足够的数据
            if len(df) < self.min_platform_days:
                logging.warning(f"ETF {etf_code} 数据不足 {self.min_platform_days} 天")
                return None
            
            # 数据预处理
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            # 确保数值列为float类型
            numeric_columns = ['open', 'close', 'high', 'low', 'volume', 'amount']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 删除任何包含NaN的行
            df = df.dropna()
            
            # 再次检查数据量是否足够
            if len(df) < self.min_platform_days:
                logging.warning(f"ETF {etf_code} 清洗后数据不足 {self.min_platform_days} 天")
                return None
            
            # 计算日涨跌幅
            df['change_pct'] = df['close'].pct_change() * 100
            
            return df
            
        except Exception as e:
            logging.error(f"获取ETF {etf_code} 数据失败: {str(e)}")
            return None

    def get_all_etfs(self):
        """
        获取所有ETF代码列表，并过滤掉无效数据
        """
        try:
            query = """
                SELECT DISTINCT t1.stock_code, t1.stock_name
                FROM t_etf t1
                WHERE EXISTS (
                    SELECT 1 
                    FROM t_etf t2 
                    WHERE t2.stock_code = t1.stock_code 
                    AND t2.stock_name IS NOT NULL
                    AND t2.close IS NOT NULL
                    AND t2.volume > 0
                )
                GROUP BY t1.stock_code, t1.stock_name
                HAVING t1.stock_name IS NOT NULL
                ORDER BY t1.stock_code
            """
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logging.error(f"获取ETF列表失败: {str(e)}")
            return None

    def check_platform(self, price_series):
        """
        基于ATR的箱体判断逻辑
        
        判断标准：
        1. 价格在30日箱体内震荡（80%以上天数在箱体内）
        2. ATR低于历史平均水平（波动率较低）
        3. 箱体高度适中（5%-15%）
        """
        if len(price_series) < 30:
            return False

        # 1. 计算箱体边界（30日最高价和最低价）
        box_high = price_series.max()
        box_low = price_series.min()
        box_height = box_high - box_low
        box_middle = (box_high + box_low) / 2
        box_height_ratio = box_height / box_middle

        # 2. 计算ATR（30日平均真实波幅）
        # 使用原始价格序列计算TR
        tr_series = pd.Series(index=price_series.index)
        for i in range(1, len(price_series)):
            high = price_series.iloc[i]
            low = price_series.iloc[i]
            prev_close = price_series.iloc[i-1]
            tr = max(
                high - low,  # 当日振幅
                abs(high - prev_close),  # 当日最高与前收差值
                abs(low - prev_close)    # 当日最低与前收差值
            )
            tr_series.iloc[i] = tr
        
        # 计算30日ATR
        atr = tr_series.rolling(window=self.atr_window, min_periods=1).mean()
        current_atr = atr.iloc[-1]
        atr_median = atr.median()

        # 3. 计算价格在箱体内的天数比例
        days_in_box = ((price_series >= box_low) & (price_series <= box_high)).sum()
        in_box_ratio = days_in_box / len(price_series)

        # 打印调试信息
        print("\nATR箱体分析详情:")
        print(f"分析周期: {price_series.index[0]} 到 {price_series.index[-1]}")
        print(f"箱体范围: {box_low:.4f} - {box_high:.4f}")
        print(f"箱体高度: {box_height_ratio:.2%}")
        print(f"当前ATR: {current_atr:.4f} (中位数: {atr_median:.4f})")
        print(f"箱体内天数: {days_in_box}/{len(price_series)} ({in_box_ratio:.2%})")

        # 判断条件
        is_proper_height = 0.05 <= box_height_ratio <= 0.15      # 箱体高度合适
        is_low_volatility = current_atr < atr_median * 0.6        # 波动率低于历史水平
        is_in_box = in_box_ratio >= 0.8                          # 价格主要在箱体内

        print("\n判断结果:")
        print(f"箱体高度合适: {'是' if is_proper_height else '否'} ({box_height_ratio:.2%})")
        print(f"低波动率: {'是' if is_low_volatility else '否'} (ATR比率: {current_atr/atr_median:.2f})")
        print(f"箱体内震荡: {'是' if is_in_box else '否'} ({in_box_ratio:.2%})")

        return is_proper_height and is_low_volatility and is_in_box

    def check_volume_surge(self, current_volume, avg_volume):
        """
        检查是否放量
        """
        return current_volume > avg_volume * self.volume_threshold

    def analyze_etf(self, df):
        """
        分析ETF数据，计算技术指标和信号
        """
        try:
            # 1. 计算价格均线系统
            df['ma5'] = df['close'].rolling(5, min_periods=5).mean()    
            df['ma10'] = df['close'].rolling(10, min_periods=10).mean()  
            df['ma20'] = df['close'].rolling(20, min_periods=20).mean()  
            
            # 2. 计算成交量均线
            df['volume_ma5'] = df['volume'].rolling(5, min_periods=5).mean()    
            df['volume_ma20'] = df['volume'].rolling(20, min_periods=20).mean()  

            # 3. 计算前20日最高价（不包含当日）
            df['high_20d'] = df['close'].shift(1).rolling(20, min_periods=20).max()

            # 4. 判断是否处于平台整理
            # 使用最后一天之前的30天数据判断是否是平台期
            last_30_days = df['close'].iloc[-31:-1]  # 不包含当天
            if len(last_30_days) >= 30:
                df.loc[df.index[-1], 'is_platform'] = self.check_platform(last_30_days)
                # 如果是平台期，计算平台高点
                if df.loc[df.index[-1], 'is_platform']:
                    df.loc[df.index[-1], 'platform_high'] = float(last_30_days.max())
                else:
                    df.loc[df.index[-1], 'platform_high'] = 0.0
            else:
                df.loc[df.index[-1], 'is_platform'] = False
                df.loc[df.index[-1], 'platform_high'] = 0.0

            # 5. 检测20日内是否有放量
            last_21_days_volume = df['volume'].iloc[-21:]  # 包含当天
            df.loc[df.index[-1], 'volume_surge_20d'] = any(
                self.check_volume_surge(v2, v1) 
                for v1, v2 in zip(last_21_days_volume[:-1], last_21_days_volume[1:])
            )

            # 6. 判断趋势（均线多头排列）
            df.loc[df.index[-1], 'trend_up'] = (
                df['ma5'].notna().iloc[-1] &  
                df['ma10'].notna().iloc[-1] & 
                df['ma20'].notna().iloc[-1] & 
                (df['ma5'].iloc[-1] > df['ma10'].iloc[-1]) &  
                (df['ma10'].iloc[-1] > df['ma20'].iloc[-1])   
            )

            return df

        except Exception as e:
            logging.error(f"分析ETF数据时出错: {str(e)}")
            raise

    def generate_signals(self, df):
        """
        生成交易信号
        
        买入条件：
        1. 前期处于平台整理（价格在长方形区间内波动）
        2. 当日收盘价突破前20日最高价
        3. 20日内有过放量（任一日成交量比前一日放大2倍以上）
        4. 趋势向上（均线多头排列）
        """
        try:
            signals = pd.DataFrame(index=df.index)
            
            # 确保所有用于信号生成的列都是正确的类型
            is_platform = df['is_platform'].astype(bool)  # 是否处于平台期
            volume_surge = df['volume_surge_20d'].astype(bool)  # 20日内是否有放量
            price_break = (df['close'] > df['high_20d']).astype(bool)  # 当日收盘价大于前20日最高价
            trend_up = df['trend_up'].astype(bool)  # 均线多头排列
            
            # 生成买入信号
            signals['buy_signal'] = (
                is_platform &                # 前期处于平台整理
                price_break &                # 当日收盘价突破前20日最高价
                volume_surge &               # 20日内有放量
                trend_up                     # 当日趋势向上
            )

            # 记录重要指标
            signals['price'] = df['close'].astype(float)
            signals['volume_ratio'] = (df['volume'] / df['volume_ma5']).astype(float)
            signals['price_change_pct'] = df['change_pct'].astype(float)
            signals['platform_high'] = df['high_20d'].astype(float)  # 使用前20日最高价
            
            return signals

        except Exception as e:
            logging.error(f"生成信号时出错: {str(e)}")
            raise

    def run_strategy(self, etf_code, start_date=None, end_date=None):
        """
        运行策略并返回结果
        """
        try:
            # 获取数据
            df = self.get_etf_data(etf_code, start_date, end_date)
            if df is None:
                return None

            # 分析数据
            df = self.analyze_etf(df)

            # 生成信号
            signals = self.generate_signals(df)

            # 筛选出买入信号日期
            buy_dates = signals[signals['buy_signal']].index

            # 整理结果
            results = []
            for date in buy_dates:
                result = {
                    'date': date.strftime('%Y-%m-%d'),
                    'price': signals.loc[date, 'price'],
                    'volume_ratio': round(signals.loc[date, 'volume_ratio'], 2),
                    'price_change': round(signals.loc[date, 'price_change_pct'], 2)
                }
                results.append(result)

            return results

        except Exception as e:
            logging.error(f"策略运行失败: {str(e)}")
            return None

    def run_strategy_all_etfs(self, start_date=None, end_date=None):
        """
        对所有ETF运行策略分析
        
        参数:
            start_date (str): 开始日期
            end_date (str): 结束日期
            
        返回:
            list: 符合条件的ETF列表及其信号详情
        """
        try:
            # 获取有效的ETF列表
            etfs = self.get_all_etfs()
            if etfs is None or len(etfs) == 0:
                logging.error("没有获取到有效的ETF列表")
                return None

            all_results = []
            total_etfs = len(etfs)
            
            # 遍历每个ETF进行分析
            for idx, (_, row) in enumerate(etfs.iterrows(), 1):
                etf_code = row['stock_code']
                etf_name = row['stock_name']
                
                if not etf_code or not etf_name:  # 跳过代码或名称为空的ETF
                    continue
                
                logging.info(f"分析ETF [{idx}/{total_etfs}]: {etf_code} - {etf_name}")
                
                try:
                    # 运行单个ETF的策略分析
                    results = self.run_strategy(etf_code, start_date, end_date)
                    if results:
                        # 添加ETF信息到结果中
                        for result in results:
                            result['etf_code'] = etf_code
                            result['etf_name'] = etf_name
                            all_results.append(result)
                except Exception as e:
                    logging.error(f"处理ETF {etf_code} 时出错: {str(e)}")
                    continue
            
            return all_results

        except Exception as e:
            logging.error(f"批量策略运行失败: {str(e)}")
            return None

def analyze_single_etf(strategy, etf_code, end_date):
    """
    分析单个ETF在指定日期的表现
    """
    # 设置时间范围（需要前90天的数据来计算指标）
    start_date = (datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    
    try:
        # 获取ETF数据
        df = strategy.get_etf_data(etf_code, start_date, end_date)
        if df is None:
            print(f"无法获取ETF {etf_code} 的数据")
            return
            
        # 分析数据
        df = strategy.analyze_etf(df)
        
        # 生成信号
        signals = strategy.generate_signals(df)
        
        # 打印详细分析结果
        print(f"\nETF {etf_code} 在 {end_date} 的分析结果:")
        print("=" * 80)
        print(f"价格: {signals['price'][-1]:.3f}")
        print(f"20日最高价: {df['high_20d'][-1]:.3f}")
        print(f"量比: {signals['volume_ratio'][-1]:.2f}")
        print(f"涨跌幅: {signals['price_change_pct'][-1]:.2f}%")
        print(f"平台高点: {signals['platform_high'][-1]:.3f}")
        print(f"是否处于平台期: {'是' if df['is_platform'][-1] else '否'}")
        print(f"20日内是否有放量: {'是' if df['volume_surge_20d'][-1] else '否'}")
        print(f"趋势是否向上: {'是' if df['trend_up'][-1] else '否'}")
        print(f"是否满足买入条件: {'是' if signals['buy_signal'][-1] else '否'}")
        
        # 添加更多调试信息
        print("\n调试信息:")
        print(f"最近20天成交量数据:")
        print(df['volume'].tail(20))
        print(f"\n20日均量: {df['volume'].tail(20).mean():.2f}")
        print("-" * 80)
        
    except Exception as e:
        logging.error(f"分析ETF {etf_code} 时出错: {str(e)}")
        print(f"分析过程出错，请查看日志")

def main():
    """
    主函数，支持分析指定ETF和日期
    """
    strategy = EtfPlatformBreakoutStrategy()
    
    # 选择分析模式
    print("\n请选择分析模式：")
    print("1. 分析单个ETF")
    print("2. 分析所有ETF")
    mode = input("请输入选项（1或2）：")
    
    if mode == "1":
        # 获取用户输入
        etf_code = input("\n请输入ETF代码（例如：513560-560690）：")
        end_date = input("请输入分析日期（格式：YYYY-MM-DD）：")
        
        # 验证日期格式
        try:
            datetime.datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            print("日期格式错误，请使用YYYY-MM-DD格式")
            return
            
        # 分析单个ETF
        analyze_single_etf(strategy, etf_code, end_date)
        
    elif mode == "2":
        # 获取用户输入
        end_date = input("\n请输入分析日期（格式：YYYY-MM-DD）：")
        
        # 验证日期格式
        try:
            datetime.datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            print("日期格式错误，请使用YYYY-MM-DD格式")
            return
            
        # 分析所有ETF
        start_date = (datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
        results = strategy.run_strategy_all_etfs(start_date, end_date)
        
        if results is None:
            print("没有获取到ETF数据")
            return
            
        if len(results) == 0:
            print(f"\n{end_date} 没有发现符合条件的ETF")
            return
            
        # 只保留指定日期的信号
        date_results = [r for r in results if r['date'] == end_date]
        
        if date_results:
            # 将结果转换为DataFrame以便更好地展示
            df_results = pd.DataFrame(date_results)
            df_results = df_results.sort_values('volume_ratio', ascending=False)
            
            # 打印结果
            print(f"\n{end_date} 符合条件的ETF:")
            print("=" * 80)
            for _, row in df_results.iterrows():
                print(f"ETF代码: {row['etf_code']} - {row['etf_name']}")
                print(f"价格: {row['price']:.3f}")
                print(f"量比: {row['volume_ratio']:.2f}")
                print(f"涨跌幅: {row['price_change']:.2f}%")
                print("-" * 80)
        else:
            print(f"\n{end_date} 没有发现符合条件的ETF")
    
    else:
        print("无效的选项")

if __name__ == "__main__":
    main() 