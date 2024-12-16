# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
使用 aiohttp 实现东方财富行情 SSE 客户端，数据存储到Redis

Key设计：
- stock:trends:{stock_code}:latest    # Hash结构，存储最新一条行情
- stock:trends:{stock_code}:today     # Sorted Set结构，存储当天分时数据
- stock:trends:active:stocks          # Set结构，存储活跃股票列表



"""

import aiohttp
import asyncio
import json
import random
from datetime import datetime, timedelta
import logging
import aioredis
from aioredis import Redis
import time
import pandas as pd
from sqlalchemy import create_engine

# 配置日志
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StockTrendsSSEClient:
    def __init__(self):
        # 服务器列表
        self.server_list = [f"{i}.push2.eastmoney.com" for i in range(1, 100)]
        self.current_server_index = 0
        
        # Redis配置
        self.redis_host = '101.43.6.49'
        self.redis_port = 3373
        self.redis_db = 0
        self.redis_password = '24777365ccyCCY!'  # 如果有密码，在这里设置
        self.redis: Redis = None
        
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
        self.max_retries = 5
        self.retry_delay = 5
        self.heartbeat_interval = 30
        self.last_data_time = None
        
        # MySQL配置
        self.mysql_config = {
            'host': '101.43.6.49',
            'port': 3333,
            'user': 'root',
            'password': 'root',
            'db': 'stock'
        }

    async def init_redis(self):
        """初始化Redis连接"""
        self.redis = await aioredis.from_url(
            f"redis://{self.redis_host}:{self.redis_port}",
            db=self.redis_db,
            password=self.redis_password,
            decode_responses=True  # 自动解码响应
        )

    async def save_trends_data(self, stock_code: str, trends_data: list):
        """保存trends数据到Redis"""
        try:
            today = datetime.now().strftime('%Y%m%d')
            pipe = self.redis.pipeline()
            
            for trend in trends_data:
                fields = trend.split(',')
                if len(fields) == 8:
                    trade_time = fields[0]
                    volume_int = int(fields[5])  # 解析为整数用于比较
                    
                    # 构建数据字典（存储到Redis时用字符串）
                    data = {
                        'trade_time': trade_time,
                        'price': fields[1],
                        'open': fields[2],
                        'high': fields[3],
                        'low': fields[4],
                        'volume': str(volume_int),  # 转为字符串存储到Redis
                        'amount': fields[6],
                        'avg_price': fields[7]
                    }
                    
                    # 1. 更新最新行情 (Hash结构)
                    latest_key = f"stock:trends:{stock_code}:latest"
                    pipe.hmset(latest_key, data)
                    
                    # 2. 添加到当天的分时数据 (Sorted Set结构)
                    today_key = f"stock:trends:{stock_code}:today:{today}"
                    score = int(time.mktime(datetime.strptime(trade_time, '%H%M%S').timetuple()))
                    pipe.zadd(today_key, {json.dumps(data): score})
                    
                    # 3. 设置过期时间（次日0点过期）
                    tomorrow = (datetime.now().replace(hour=0, minute=0, second=0) + 
                              timedelta(days=1)).timestamp()
                    pipe.expireat(today_key, int(tomorrow))
                    
                    # 4. 更新活跃股票逻辑
                    VOLUME_THRESHOLD = 50000  # 定义阈值常量
                    if volume_int > VOLUME_THRESHOLD:  # 使用整数比较
                        active_stocks_key = f"stock:trends:active:stocks:{today}"
                        current_count = await self.redis.hget(active_stocks_key, stock_code)
                        
                        if current_count is None:
                            new_count = 1
                        else:
                            new_count = int(current_count) + 1  # 从Redis取出的字符串转回整数
                        
                        # 存储新的计数（自动转为字符串）
                        pipe.hset(active_stocks_key, stock_code, new_count)
                        logger.info(f"Stock {stock_code} active count: {new_count}, volume: {volume_int}")
                        
                        # 设置活跃股票列表的过期时间
                        pipe.expireat(active_stocks_key, int(tomorrow))
            
            # 执行管道命令
            await pipe.execute()
            
            logger.info(f"Successfully saved trends data for stock {stock_code}")
            
        except Exception as e:
            logger.error(f"Error saving trends data for stock {stock_code}: {e}")

    async def get_active_stocks(self, min_count=2):
        """
        获取活跃股票列表
        :param min_count: 最小活跃次数，默认为2表示至少两次成交量大于5万
        :return: 字典，包含股票代码和其活跃次数
        """
        try:
            today = datetime.now().strftime('%Y%m%d')
            active_stocks_key = f"stock:trends:active:stocks:{today}"
            
            # 获取所有股票及其活跃次数
            all_stocks = await self.redis.hgetall(active_stocks_key)
            
            # 筛选并返回活跃次数大于等于min_count的股票及其次数
            active_stocks = {
                stock_code: int(count) 
                for stock_code, count in all_stocks.items()
                if int(count) >= min_count
            }
            
            return active_stocks
        except Exception as e:
            logger.error(f"Error getting active stocks: {e}")
            return {}

    async def process_sse_data(self, data):
        """处理 SSE 数据"""
        try:
            if data.startswith('data:'):
                data = data[5:].strip()
            
            if data:
                json_data = json.loads(data)
                
                if (json_data.get('data') and 
                    isinstance(json_data['data'], dict) and 
                    json_data['data'].get('trends')):
                    
                    stock_code = json_data['data'].get('code')
                    trends = json_data['data']['trends']
                    
                    await self.save_trends_data(stock_code, trends)
                else:
                    logger.debug("Received data without trends information")
                    
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON data: {e}")
        except Exception as e:
            logger.error(f"Error processing data: {e}")

    def get_next_server(self):
        """获下一个服务器地址"""
        server = self.server_list[self.current_server_index]
        self.current_server_index = (self.current_server_index + 1) % len(self.server_list)
        return server

    def get_random_server(self):
        """随机获取一个服务器地址"""
        return random.choice(self.server_list)

    def get_base_url(self):
        """获取基础URL（这里使用随机策略，也可以改用轮询策��）"""
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

    async def init_concept_stocks(self):
        """初始化概念股数据到Redis"""
        try:
            # 创建同步数据库连接
            engine = create_engine(
                f"mysql+pymysql://{self.mysql_config['user']}:{self.mysql_config['password']}@"
                f"{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['db']}"
            )

            # 查询概念股数据
            query = """
                SELECT concept_name, stock_code, stock_name 
                FROM t_concept_stock
            """
            
            df = pd.read_sql(query, engine)
            
            # 使用Redis pipeline批量写入数据
            pipe = self.redis.pipeline()
            
            # 1. 存储股票到概念的映射
            for _, row in df.iterrows():
                stock_key = f"meta:stock:concept:{row['stock_code']}"
                concept_data = {
                    'concept_name': row['concept_name'],
                    'stock_name': row['stock_name']
                }
                pipe.hset(stock_key, mapping=concept_data)
            
            # 2. 存储概念到股票的映射
            for concept_name in df['concept_name'].unique():
                concept_stocks = df[df['concept_name'] == concept_name]
                concept_key = f"meta:concept:stocks:{concept_name}"
                
                # 使用Hash存储该概念下的所有股票
                concept_stocks_data = dict(zip(
                    concept_stocks['stock_code'],
                    concept_stocks['stock_name']
                ))
                pipe.hmset(concept_key, concept_stocks_data)
            
            # 3. 存储所有概念名称集合
            all_concepts_key = "meta:concepts:all"
            pipe.sadd(all_concepts_key, *df['concept_name'].unique())
            
            # 执行所有Redis命令
            await pipe.execute()
            
            logger.info(f"Successfully loaded {len(df)} concept stock records into Redis")
            
        except Exception as e:
            logger.error(f"Error loading concept stocks: {e}")
            raise

    async def get_stock_concepts(self, stock_code):
        """获取股票的概念信息"""
        try:
            stock_key = f"meta:stock:concept:{stock_code}"
            concepts = await self.redis.hgetall(stock_key)
            return concepts
        except Exception as e:
            logger.error(f"Error getting concepts for stock {stock_code}: {e}")
            return {}

    async def get_concept_stocks(self, concept_name):
        """获取概念下的所有股票"""
        try:
            concept_key = f"meta:concept:stocks:{concept_name}"
            stocks = await self.redis.hgetall(concept_key)
            return stocks
        except Exception as e:
            logger.error(f"Error getting stocks for concept {concept_name}: {e}")
            return {}

    async def get_all_concepts(self):
        """获取所有概念名称"""
        try:
            return await self.redis.smembers("meta:concepts:all")
        except Exception as e:
            logger.error(f"Error getting all concepts: {e}")
            return set()

async def main():
    try:
        client = StockTrendsSSEClient()
        
        # 初始化Redis连接
        await client.init_redis()
        
        # 初始化概念股数据
        logger.info("Starting to load concept stocks data...")
        await client.init_concept_stocks()
        logger.info("Concept stocks data loaded successfully")
        
        # 读取股票代码列表
        with open('input.txt', 'r') as f:
            stock_codes = [line.strip() for line in f 
                         if line.strip() and not line.startswith('#')]
        
        if not stock_codes:
            logger.error("No stock codes found in input.txt")
            return
            
        logger.info(f"Loaded {len(stock_codes)} stock codes from input.txt")
        
        # 启动行情监控任务
        tasks = [client.connect_with_retry(code) for code in stock_codes]
        await asyncio.gather(*tasks)
        
    except FileNotFoundError:
        logger.error("input.txt not found in current directory")
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 