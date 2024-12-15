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
                async with session.get(
                    base_url,
                    params=params,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=None)  # 永不超时
                ) as response:
                    logger.info(f"Connected to SSE stream for stock {stock_code}")
                    
                    async for line in response.content:
                        line = line.decode('utf-8').strip()
                        if line:
                            self.last_data_time = datetime.now()
                            await self.process_sse_data(line)
                        
                        # 检查心跳状态
                        if heartbeat_task.done() and heartbeat_task.result():
                            logger.warning(f"Heartbeat check failed for {stock_code}, reconnecting...")
                            return  # 触发重连
                    
        except asyncio.CancelledError:
            logger.info(f"Connection cancelled for {stock_code}")
            raise
        except Exception as e:
            logger.error(f"Connection error for {stock_code}: {e}")
            raise

    async def process_sse_data(self, data):
        """处理 SSE 数据"""
        try:
            if data.startswith('data:'):
                data = data[5:].strip()
            
            if data:
                json_data = json.loads(data)
                logger.info(f"Received data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:")
                logger.info(json.dumps(json_data, indent=2, ensure_ascii=False))
                
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON data: {e}")
            logger.error(f"Raw data: {data}")
        except Exception as e:
            logger.error(f"Error processing data: {e}")

async def main():
    client = StockTrendsSSEClient()
    stock_codes = [  '000001', '000002', '000004', '000006', '000007', '000008', '000009', '000010', '000011', '000012', '000014', '000016', '000017', '000019', '000020', '000021', '000025', '000026', '000027', '000028', '000029', '000030', '000031', '000032', '000034', '000035', '000036', '000037', '000039', '000040', '000042', '000045', '000048', '000049', '000050', '000055', '000056', '000058', '000059', '000060', '000061', '000062', '000063', '000065', '000066', '000068', '000069', '000070', '000078', '000088', '000089', '000090', '000096', '000099', '000100', '000151', '000153', '000155', '000156', '000157', '000158', '000159', '000166', '000301', '000333', '000338', '000400', '000401', '000402', '000403', '000404', '000407', '000408', '000409', '000410', '000411', '000415', '000417', '000419', '000420', '000421', '000422', '000423', '000425', '000426', '000428', '000429', '000430', '000488', '000498', '000501', '000503', '000504', '000505', '000506', '000507', '000509', '000510', '000513', '000514', '000516', '000517']
    tasks = [client.connect_with_retry(code) for code in stock_codes]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main()) 