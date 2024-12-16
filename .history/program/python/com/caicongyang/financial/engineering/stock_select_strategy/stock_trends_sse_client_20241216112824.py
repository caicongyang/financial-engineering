# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
使用 aiohttp 实现东方财富行情 SSE 客户端
"""

import aiohttp
import asyncio
import json
import random
from datetime import datetime
import logging
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import create_engine  # 保留同步引擎用于 pandas
from sqlalchemy import text

# 配置日志
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StockTrendsSSEClient:
    def __init__(self):
        # 服务器列表
        self.server_list = [f"{i}.push2.eastmoney.com" for i in range(1, 100)]
        
        # 通用参数
        self.common_params = {
            'fields1': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f17',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58',
            'mpi': '1000',
            'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
            'ndays': '1',
            'iscr': '0',
            'iscca': '0',
            'wbp2u': '1849325530509956|0|1|0|web'
        }
        
        # 浏览器 User-Agents 列表
        self.user_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15'
        ]
        
        # 请求头
        self.headers = {
            'Accept': 'text/event-stream',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Origin': 'https://quote.eastmoney.com',
            'Referer': 'https://quote.eastmoney.com',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"'
        }
        
        # 重连相关配置
        self.max_retries = 5  # 最大重试次数
        self.retry_delay = 5  # 重试延迟（秒）
        self.heartbeat_interval = 30  # 心跳检测间隔（秒）
        self.last_data_time = None  # 上次接收数据的时间
        
        # 当前使用的服务器索引
        self.current_server_index = 0
        
        # 数据库连接信息
        self.mysql_user = 'root'
        self.mysql_password = 'root'
        self.mysql_host = '101.43.6.49'
        self.mysql_port = '3333'
        self.mysql_db = 'stock'
        self.table_name = 't_stock_trends'
        
        # 创建同步引擎（用于 pandas）
        self.engine = create_engine(
            f'mysql+pymysql://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}/{self.mysql_db}',
            pool_size=5,
            max_overflow=10
        )
        
        # 创建异步引擎
        self.async_engine = create_async_engine(
            f'mysql+aiomysql://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}/{self.mysql_db}'
        )
        
        # 用于跟踪已处理的数据
        self.processed_records = {}  # 格式: {stock_code: {trade_time: True}}

    def get_next_server(self):
        """获取下一个服务器地址"""
        server = self.server_list[self.current_server_index]
        self.current_server_index = (self.current_server_index + 1) % len(self.server_list)
        return server

    def get_random_server(self):
        """随机获取一个服务器地址"""
        return random.choice(self.server_list)

    def get_base_url(self):
        """获取基础URL（这里使用随机策略，也可以改用轮询策略）"""
        server = self.get_random_server()  # 或使用 self.get_next_server()
        return f"https://{server}/api/qt/stock/trends2/sse"

    def get_secid(self, stock_code):
        """生成 secid"""
        if stock_code.startswith(('000', '002', '300', '301')):
            return f"0.{stock_code}"
        elif stock_code.startswith(('600', '601', '603', '688')):
            return f"1.{stock_code}"
        else:
            raise ValueError(f"Unsupported stock code format: {stock_code}")

    async def heartbeat_check(self, stock_code):
        """心跳检测"""
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            if self.last_data_time:
                time_since_last_data = (datetime.now() - self.last_data_time).seconds
                if time_since_last_data > self.heartbeat_interval:
                    logger.warning(f"No data received for {time_since_last_data} seconds for {stock_code}")
                    return True  # 需要重连
            else:
                logger.warning(f"No data received yet for {stock_code}")
                return True  # 需要重连
        return False

    async def connect_with_retry(self, stock_code):
        """带重试机制的连接"""
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                await self.connect_sse(stock_code)
                retry_count = 0  # 重置重试计数
            except Exception as e:
                retry_count += 1
                logger.error(f"Connection error for {stock_code}: {e}")
                if retry_count < self.max_retries:
                    wait_time = self.retry_delay * (2 ** (retry_count - 1))  # 指数退避
                    logger.info(f"Retrying in {wait_time} seconds... (Attempt {retry_count}/{self.max_retries})")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Max retries reached for {stock_code}")
                    break

    async def connect_sse(self, stock_code):
        """连接 SSE 并处理数据流"""
        try:
            params = self.common_params.copy()
            params['secid'] = self.get_secid(stock_code)
            self.headers['User-Agent'] = random.choice(self.user_agents)
            
            # 获取服务器地址
            base_url = self.get_base_url()
            logger.info(f"Using server: {base_url}")
            
            # 创建心跳检测任务
            heartbeat_task = asyncio.create_task(self.heartbeat_check(stock_code))
            
            async with aiohttp.ClientSession() as session:
                while True:  # 添加无限重试循环
                    try:
                        timeout = aiohttp.ClientTimeout(
                            total=None,      # 总超时时间
                            connect=10,      # 连接超时
                            sock_read=30     # 读取超时
                        )
                        
                        async with session.get(
                            base_url,
                            params=params,
                            headers=self.headers,
                            timeout=timeout,
                            chunked=True,    # 启用分块传输
                            read_bufsize=1024 * 1024  # 增加读取缓冲区大小
                        ) as response:
                            logger.info(f"Connected to SSE stream for stock {stock_code}")
                            
                            if response.status != 200:
                                logger.error(f"Received non-200 status code: {response.status} for stock {stock_code}")
                                await asyncio.sleep(5)
                                continue
                            
                            buffer = ""
                            try:
                                # 使用更大的读取块大小
                                async for chunk in response.content.iter_chunked(1024 * 1024):
                                    try:
                                        chunk_text = chunk.decode('utf-8')
                                        buffer += chunk_text
                                        
                                        # 处理可能包含多个JSON对象的buffer
                                        while '}' in buffer:
                                            try:
                                                end_pos = buffer.index('}') + 1
                                                json_str = buffer[:end_pos]
                                                buffer = buffer[end_pos:].lstrip()
                                                
                                                if json_str.startswith('data:'):
                                                    json_str = json_str[5:].strip()
                                                
                                                if json_str:
                                                    self.last_data_time = datetime.now()
                                                    await self.process_sse_data(json_str)
                                            except ValueError:
                                                # JSON不完整，继续等待更多数据
                                                break
                                                
                                    except UnicodeDecodeError as e:
                                        logger.warning(f"Unicode decode error for stock {stock_code}: {e}")
                                        buffer = ""  # 重置buffer
                                        continue
                                        
                            except aiohttp.ClientPayloadError as e:
                                logger.error(f"Payload error during read for stock {stock_code}: {e}")
                                await asyncio.sleep(5)
                                break  # 跳出内部循环，触发重连
                            
                            # 检查心跳状态
                            if heartbeat_task.done() and heartbeat_task.result():
                                logger.warning(f"Heartbeat check failed for {stock_code}, reconnecting...")
                                break
                                
                    except aiohttp.ClientPayloadError as e:
                        logger.error(f"Payload error for stock {stock_code}: {e}")
                        await asyncio.sleep(5)
                        continue
                    except aiohttp.ClientError as e:
                        logger.error(f"Client error for stock {stock_code}: {e}")
                        await asyncio.sleep(5)
                        continue
                    except asyncio.TimeoutError:
                        logger.error(f"Timeout for stock {stock_code}")
                        await asyncio.sleep(5)
                        continue
                    
        except asyncio.CancelledError:
            logger.info(f"Connection cancelled for {stock_code}")
            raise
        except Exception as e:
            logger.error(f"Connection error for {stock_code}: {e}")
            raise

    async def save_trends_data(self, stock_code, trends_data):
        """保存trends数据到数据库"""
        try:
            # 初始化该股票的记录集
            if stock_code not in self.processed_records:
                self.processed_records[stock_code] = set()
            
            # 解析trends数据，只处理新数据
            new_records = []
            for trend in trends_data:
                fields = trend.split(',')
                if len(fields) == 8:
                    trade_time = fields[0]
                    # 检查是否已处理过这条记录
                    if trade_time not in self.processed_records[stock_code]:
                        record = {
                            'stock_code': stock_code,
                            'trade_time': trade_time,
                            'price': float(fields[1]),
                            'open': float(fields[2]),
                            'high': float(fields[3]),
                            'low': float(fields[4]),
                            'volume': int(fields[5]),
                            'amount': float(fields[6]),
                            'avg_price': float(fields[7])
                        }
                        new_records.append(record)
                        # 添加到已处理集合
                        self.processed_records[stock_code].add(trade_time)

            if new_records:
                # 创建DataFrame
                df = pd.DataFrame(new_records)
                
                # 转换时间格式
                df['trade_time'] = pd.to_datetime(df['trade_time'])
                
                # 直接插入新数据
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: df.to_sql(
                        self.table_name,
                        con=self.engine,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=500
                    )
                )
                logger.info(f"Successfully saved {len(new_records)} new trends records for stock {stock_code}")
                
        except Exception as e:
            logger.error(f"Error saving trends data for stock {stock_code}: {e}")

    async def process_sse_data(self, data):
        """处理 SSE 数据"""
        try:
            if data.startswith('data:'):
                data = data[5:].strip()
            
            if data:
                json_data = json.loads(data)
                
                # 检查数据结构和trends数据是否存在
                if (json_data.get('data') and 
                    isinstance(json_data['data'], dict) and 
                    json_data['data'].get('trends')):
                    
                    stock_code = json_data['data'].get('code')
                    trends = json_data['data']['trends']
                    
                    # 异步保存trends数据
                    await self.save_trends_data(stock_code, trends)
                else:
                    logger.debug("Received data without trends information")
                    
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON data: {e}")
            logger.error(f"Raw data: {data}")
        except Exception as e:
            logger.error(f"Error processing data: {e}")

async def main():
    client = StockTrendsSSEClient()
    stock_codes = ['000001', '000002', '000004', '000006', '000007', '000008', '000009', '000010', '000011', '000012', '000014', '000016', '000017', '000019', '000020', '000021', '000025', '000026', '000027', '000028', '000029', '000030', '000031', '000032', '000034', '000035', '000036', '000037', '000039', '000040', '000042', '000045', '000048', '000049', '000050', '000055', '000056', '000058', '000059', '000060', '000061', '000062', '000063', '000065', '000066', '000068', '000069', '000070', '000078', '000088', '000089', '000090', '000096', '000099', '000100', '000151', '000153', '000155', '000156', '000157', '000158', '000159', '000166', '000301', '000333', '000338', '000400', '000401', '000402', '000403', '000404', '000407', '000408', '000409', '000410', '000411', '000415', '000417', '000419', '000420', '000421', '000422', '000423', '000425', '000426', '000428', '000429', '000430', '000488', '000498', '000501', '000503', '000504', '000505', '000506', '000507', '000509', '000510', '000513', '000514', '000516', '000517', '000518', '000519', '000520', '000521', '000523', '000524', '000525', '000526', '000528', '000529', '000530', '000531', '000532', '000533', '000534', '000536', '000537', '000538', '000539', '000541', '000543', '000544', '000545', '000546', '000547', '000548', '000550', '000551', '000552', '000553', '000554', '000555', '000557', '000558', '000559', '000560', '000561', '000563', '000564', '000565', '000566', '000567', '000568', '000570', '000571', '000572', '000573', '000576', '000581', '000582', '000584', '000586', '000589', '000590', '000591', '000592', '000593', '000595', '000596', '000597', '000598', '000599', '000600', '000601', '000603', '000605', '000607', '000608', '000609', '000610', '000612', '000615', '000617', '000619', '000620', '000622', '000623', '000625', '000626', '000627', '000628', '000629', '000630', '000631', '000632', '000633', '000635', '000636', '000637', '000638', '000639', '000650', '000651', '000652', '000655', '000656', '000657', '000659', '000661', '000663', '000665', '000668', '000669', '000670', '000672', '000676', '000677', '000678', '000679', '000680', '000681', '000682', '000683', '000685', '000686', '000688', '000690', '000691', '000692', '000695', '000697', '000698', '000700', '000701', '000702', '000703', '000705', '000707', '000708', '000709', '000710', '000711', '000712', '000713', '000715', '000716', '000717', '000718', '000719', '000720', '000721', '000722', '000723', '000725', '000726', '000727', '000728', '000729', '000731', '000733', '000735', '000736', '000737', '000738', '000739', '000750', '000751', '000752', '000753', '000755', '000756', '000757', '000758', '000759', '000761', '000762', '000766', '000767', '000768', '000776', '000777', '000778', '000779', '000782', '000783', '000785', '000786', '000788', '000789', '000790', '000791', '000792', '000793', '000795', '000796', '000797', '000798', '000799', '000800', '000801', '000802', '000803', '000807', '000809', '000810', '000811', '000812', '000813', '000815', '000816', '000818', '000819', '000820', '000821', '000822', '000823', '000825', '000826', '000828', '000829', '000830', '000831', '000833', '000837', '000838', '000839', '000848', '000850', '000851', '000852', '000856', '000858', '000860', '000862', '000863', '000868', '000869', '000875', '000876', '000877', '000878', '000880', '000881', '000882', '000883', '000885', '000886', '000887', '000888', '000889', '000890', '000892', '000893', '000895', '000897', '000898', '000899', '000900', '000901', '000902', '000903', '000905', '000906', '000908', '000909', '000910', '000911', '000912', '000913', '000915', '000917', '000919', '000920', '000921', '000922', '000923', '000925', '000926', '000927', '000928', '000929', '000930', '000931', '000932', '000933', '000935', '000936', '000937', '000938', '000948', '000949', '000950', '000951', '000952', '000953', '000955', '000957', '000958', '000959', '000960', '000962', '000963', '000965', '000966', '000967', '000968', '000969', '000970', '000972', '000973', '000975', '000977', '000978', '000980', '000981', '000983', '000985', '000987', '000988', '000989', '000990', '000993', '000995', '000997', '000998', '000999', '001201', '001202', '001203', '001205', '001206', '001207', '001208', '001209', '001210', '001211', '001212', '001213', '001215', '001216', '001217', '001218', '001219', '001222', '001223', '001225', '001226', '001227', '001228', '001229', '001230', '001231', '001234', '001236', '001238', '001239', '001255', '001256', '001258', '001259', '001260', '001266', '001267', '001268', '001269', '001270', '001277', '001278', '001279', '001282', '001283', '001286', '001287', '001288', '001289', '001296', '001298', '001299', '001300', '001301', '001306', '001308', '001309', '001311', '001313', '001314', '001316', '001317', '001318', '001319', '001322', '001323', '001324', '001326', '001328', '001330', '001331', '001332', '001333', '001336', '001337', '001338', '001339', '001358', '001359', '001360', '001366', '001367', '001368', '001373', '001376', '001378', '001379', '001380', '001387', '001389', '001696', '001872', '001896', '001914', '001965', '001979', '002001', '002003', '002004', '002005', '002006', '002007', '002008', '002009', '002010', '002011', '002012', '002014', '002015', '002016', '002017', '002019', '002020', '002021', '002022', '002023', '002024', '002025', '002026', '002027', '002028', '002029', '002030', '002031', '002032', '002033', '002034', '002035', '002036', '002037', '002038', '002039', '002040', '002041', '002042', '002043', '002044', '002045', '002046', '002047', '002048', '002049', '002050', '002051', '002052', '002053', '002054', '002055', '002056', '002057', '002058', '002059', '002060', '002061', '002062', '002063', '002064', '002065', '002066', '002067', '002068', '002069', '002072', '002073', '002074', '002075', '002076', '002077', '002078', '002079', '002080', '002081', '002082', '002083', '002084', '002085', '002086', '002088', '002090', '002091', '002092', '002093', '002094', '002095', '002096', '002097', '002098', '002099', '002100', '002101', '002102', '002103', '002104', '002105', '002106', '002107', '002108', '002109', '002110', '002111', '002112', '002114', '002115', '002116', '002117', '002119', '002120', '002121', '002122', '002123', '002124', '002125', '002126', '002127', '002128', '002129', '002130', '002131', '002132', '002133', '002134', '002135', '002136', '002137', '002138', '002139', '002140', '002141', '002142', '002144', '002145', '002146', '002148', '002149', '002150', '002151', '002152', '002153', '002154', '002155', '002156', '002157', '002158', '002159', '002160', '002161', '002162', '002163', '002164', '002165', '002166', '002167', '002168', '002169', '002170', '002171', '002172', '002173', '002174', '002175', '002176', '002177', '002178', '002179', '002180', '002181', '002182', '002183', '002184', '002185', '002186', '002187', '002188', '002189', '002190', '002191', '002192', '002193']
    tasks = [client.connect_with_retry(code) for code in stock_codes]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main()) 