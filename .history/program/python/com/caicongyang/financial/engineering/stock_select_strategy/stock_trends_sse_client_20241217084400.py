# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
使用 aiohttp 实现东方财富行情 SSE 客户端，数据存储到Redis

Redis数据结构设计：

1. 实时行情数据：
    - stock:trends:{stock_code}:latest (Hash)
        {
            'trade_time': '时间',
            'price': '当前价',
            'open': '开盘价',
            'high': '最高价',
            'low': '最低价',
            'volume': '成交量',
            'amount': '成交额',
            'avg_price': '均价'
        }

2. 当日分时数据：
    - stock:trends:{stock_code}:today:{date} (Sorted Set)
        score: timestamp
        member: JSON字符串(包含完整行情数据)

3. 活跃股票统计：
    - stock:trends:active:stocks:{date} (Hash)
        {
            'stock_code': '活跃次数',  # 成交量>5万即计数+1
        }

4. 概念股元数据：
    - meta:stock:concept:{stock_code} (Hash)
        {
            'concept_name': '概念名称',
            'stock_name': '股票名称'
        }
    
    - meta:concept:stocks:{concept_name} (Hash)
        {
            'stock_code': 'stock_name',  # 股���代码: 股票名称
        }
    
    - meta:concepts:all (Set)
        ['概念1', '概念2', ...]  # 所有概念名称集合

5. 热点板块统计：
    - hot:concept:stats:{date} (Sorted Set)
        score: 活跃股票数量
        member: 概念名称

    - hot:concept:active:stocks:{concept_name}:{date} (Set)
        members: [stock_code1, stock_code2, ...]  # 该概念下当天的活跃股票集合

数据过期策略：
- 分时数据和活跃股票统计：次日0点自动过期
- 概念股元数据：永久保存

使用示例：
1. 获取股票最新行情：HGETALL stock:trends:000001:latest
2. 获取分时数据：ZRANGE stock:trends:000001:today:20240318 0 -1
3. 查询活跃股票：HGETALL stock:trends:active:stocks:20240318
4. 获取股票概念：HGETALL meta:stock:concept:000001
5. 获取概念股票：HGETALL meta:concept:stocks:新能源
6. 获取所有概念：SMEMBERS meta:concepts:all
"""

import aiohttp
import asyncio
import json
import random
from datetime import datetime, timedelta
import logging
from redis.asyncio import Redis
import time
import pandas as pd
from sqlalchemy import create_engine

# 配置日志
logging.basicConfig(
    level=logging.INFO,  # 改为 DEBUG 级别
    format='%(asctime)s - %(levelname)s - %(message)s'
)
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
        self.redis_password = '24777365ccyCCY!'
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
            'sec-ch-ua-platform': '"macOS"',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache'
        }
        
        # 重连相关配置
        self.max_retries = 5
        self.retry_delay = 5
        self.heartbeat_interval = 60  # 增加心跳间隔
        self.last_data_time = None
        
        # MySQL配置
        self.mysql_config = {
            'host': '101.43.6.49',
            'port': 3333,
            'user': 'root',
            'password': 'root',
            'db': 'stock'
        }
        
        # 企业微信机器人配置
        self.webhook_key = "d981e07d-264c-45b3-949b-fb42f9019751"
        self.webhook_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={self.webhook_key}"
        
        # 推送记录（避免重复推送）
        self.pushed_stocks = set()

    async def init_redis(self):
        """初始化Redis连接"""
        self.redis = Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            password=self.redis_password,
            decode_responses=True  # 自动解码响应
        )
        # 测试连接
        await self.redis.ping()

    async def save_trends_data(self, stock_code: str, trends_data: list):
        """保存trends数据到Redis"""
        try:
            today = datetime.now().strftime('%Y%m%d')
            pipe = self.redis.pipeline()
            
            for trend in trends_data:
                try:
                    fields = trend.split(',')
                    if len(fields) == 8:
                        # 修复时间格式解析
                        trade_time = fields[0].split()[-1]  # 只取时间部分 "HH:MM:SS"
                        trade_time = trade_time.replace(':', '')  # 转换为 "HHMMSS" 格式
                        volume_int = int(fields[5])
                        price = float(fields[1])  # 当前价/收盘价
                        open_price = float(fields[2])  # 开盘价
                        
                        # 构建数据字典
                        data = {
                            'trade_time': trade_time,
                            'price': str(price),
                            'open': str(open_price),
                            'high': fields[3],
                            'low': fields[4],
                            'volume': str(volume_int),
                            'amount': fields[6],
                            'avg_price': fields[7]
                        }
                        
                        # 1. 更新最新行情 (使用hset替代hmset)
                        latest_key = f"stock:trends:{stock_code}:latest"
                        pipe.hset(latest_key, mapping=data)
                        
                        # 2. 添加到当天的分时数据
                        today_key = f"stock:trends:{stock_code}:today:{today}"
                        score = int(time.mktime(datetime.strptime(trade_time, '%H%M%S').timetuple()))
                        pipe.zadd(today_key, {json.dumps(data): score})
                        
                        # 3. 设置过期时间
                        tomorrow = (datetime.now().replace(hour=0, minute=0, second=0) + 
                                  timedelta(days=1)).timestamp()
                        pipe.expireat(today_key, int(tomorrow))
                        
                        # 4. 更新活跃股票逻辑
                        # 条件1: 成交量大于20万
                        # 条件2: 收盘价大于开盘价
                        if volume_int > 200000 and price > open_price:
                            active_stocks_key = f"stock:trends:active:stocks:{today}"
                            current_count = await self.redis.hget(active_stocks_key, stock_code)
                            
                            new_count = 1 if current_count is None else int(current_count) + 1
                            pipe.hset(active_stocks_key, stock_code, new_count)
                            logger.debug(f"Stock {stock_code} active count: {new_count}, volume: {volume_int}, price: {price}, open: {open_price}")
                            
                            await self.update_hot_concepts(stock_code)
                            
                            if new_count >= 3 and stock_code not in self.pushed_stocks:
                                # 更新推送消息内容，添加涨跌信息
                                price_change = ((price - open_price) / open_price) * 100
                                await self.push_to_wechat(stock_code, new_count, volume_int, price_change)
                                self.pushed_stocks.add(stock_code)
                            
                            pipe.expireat(active_stocks_key, int(tomorrow))
                
                except ValueError as ve:
                    logger.error(f"Error processing trend data: {ve}, data: {trend}")
                    continue
            
            await pipe.execute()
            
        except Exception as e:
            logger.error(f"Error saving trends data for stock {stock_code}: {e}")

    async def get_active_stocks(self, min_count=2):
        """
        获取活跃股票列表
        :param min_count: 最小活跃次数，默认为2表示至少两次成交于5万
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
            if isinstance(data, str):
                if data.startswith('data:'):
                    data = data[5:].strip()
                
                try:
                    json_data = json.loads(data)
                    
                    # 处理心跳响应
                    if json_data.get('data') is None:
                        logger.debug("Received heartbeat")
                        return
                    
                    # 处理实际数据
                    if (isinstance(json_data.get('data'), dict) and 
                        json_data['data'].get('trends')):
                        
                        stock_code = json_data['data'].get('code')
                        trends = json_data['data']['trends']
                        
                        if isinstance(trends, list):
                            logger.debug(f"Processing {len(trends)} trends for stock {stock_code}")
                            await self.save_trends_data(stock_code, trends)
                        else:
                            logger.warning(f"Invalid trends format for stock {stock_code}")
                    
                except json.JSONDecodeError as je:
                    logger.error(f"JSON decode error: {je}")
                    logger.error(f"Raw data: {data[:200]}...")
                    
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
        """获取基础URL（这里使用随机策略，也可以改用轮询策略）"""
        server = self.get_random_server()  # 或使用 self.get_next_server()
        return f"https://{server}/api/qt/stock/trends2/sse"

    def get_secid(self, stock_code):
        """生成 secid"""
        if stock_code.startswith(('300', '301','00')):
            return f"0.{stock_code}"
        elif stock_code.startswith(('60', '688')):
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
        while True:
            try:
                await self.connect_sse(stock_code)
            except Exception as e:
                logger.error(f"Connection failed for {stock_code}: {e}")
                await asyncio.sleep(5)
                continue

    async def connect_sse(self, stock_code):
        """连接 SSE 并处理数据流"""
        try:
            base_url = self.get_base_url()
            params = self.common_params.copy()
            params['secid'] = self.get_secid(stock_code)
            self.headers['User-Agent'] = random.choice(self.user_agents)
            
            logger.info(f"Connecting to {base_url} for stock {stock_code}")
            
            while True:
                try:
                    timeout = aiohttp.ClientTimeout(total=60, connect=10)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.get(
                            base_url,
                            params=params,
                            headers=self.headers,
                            proxy=None,
                            ssl=False,
                            timeout=3 * 60 * 60
                        ) as response:
                            if response.status != 200:
                                logger.error(f"Non-200 status code: {response.status} for {stock_code}")
                                await asyncio.sleep(5)
                                continue
                            
                            buffer = ""
                            async for chunk in response.content:
                                try:
                                    chunk_text = chunk.decode('utf-8')
                                    buffer += chunk_text
                                    
                                    # 处理完整的数据块
                                    while '\n\n' in buffer:
                                        parts = buffer.split('\n\n', 1)
                                        data_part = parts[0]
                                        buffer = parts[1]
                                        
                                        if data_part.startswith('data:'):
                                            data_part = data_part[5:].strip()
                                        
                                        if data_part and data_part != 'undefined':
                                            try:
                                                json_data = json.loads(data_part)
                                                if json_data.get('data'):
                                                    if isinstance(json_data['data'], dict) and json_data['data'].get('trends'):
                                                        trends = json_data['data']['trends']
                                                        if isinstance(trends, list):
                                                            await self.save_trends_data(stock_code, trends)
                                                        else:
                                                            logger.warning(f"Invalid trends format for {stock_code}")
                                                    elif json_data['data'] is None:
                                                        logger.debug(f"Received heartbeat for {stock_code}")
                                            except json.JSONDecodeError as je:
                                                logger.error(f"JSON decode error: {je}")
                                                logger.error(f"Problematic data: {data_part[:200]}...")
                                    
                                    # 如果缓冲区太大，清理它
                                    if len(buffer) > 1024 * 1024:  # 1MB
                                        logger.warning(f"Buffer too large for {stock_code}, clearing")
                                        buffer = ""
                                        
                                except UnicodeDecodeError as ude:
                                    logger.error(f"Unicode decode error for {stock_code}: {ude}")
                                    buffer = ""
                                    continue
                                
                except aiohttp.ClientPayloadError as cpe:
                    logger.error(f"Payload error for {stock_code}: {cpe}")
                    await asyncio.sleep(5)
                    continue
                except aiohttp.ClientError as e:
                    logger.error(f"Connection error for {stock_code}: {e}")
                    await asyncio.sleep(5)
                    continue
                except asyncio.TimeoutError:
                    logger.error(f"Connection timeout for {stock_code}")
                    await asyncio.sleep(5)
                    continue
                    
        except Exception as e:
            logger.error(f"Unexpected error for {stock_code}: {e}", exc_info=True)
            raise

    async def init_concept_stocks(self):
        """初始化概念股��据到Redis"""
        try:
            engine = create_engine(
                f"mysql+pymysql://{self.mysql_config['user']}:{self.mysql_config['password']}@"
                f"{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['db']}"
            )

            df = pd.read_sql("SELECT concept_name, stock_code, stock_name FROM t_concept_stock", engine)
            
            pipe = self.redis.pipeline()
            
            # 1. 存储股票到概念的映射
            for _, row in df.iterrows():
                stock_key = f"meta:stock:concept:{row['stock_code']}"
                concept_data = {
                    'concept_name': row['concept_name'],
                    'stock_name': row['stock_name']
                }
                pipe.hset(stock_key, mapping=concept_data)  # 使用hset with mapping
            
            # 2. 存储概念到股票的映射
            for concept_name in df['concept_name'].unique():
                concept_stocks = df[df['concept_name'] == concept_name]
                concept_key = f"meta:concept:stocks:{concept_name}"
                
                concept_stocks_data = dict(zip(
                    concept_stocks['stock_code'],
                    concept_stocks['stock_name']
                ))
                pipe.hset(concept_key, mapping=concept_stocks_data)  # 使用hset with mapping
            
            # 3. 存储所有概念名称集合
            all_concepts_key = "meta:concepts:all"
            pipe.sadd(all_concepts_key, *df['concept_name'].unique())
            
            await pipe.execute()
            logger.debug(f"Successfully loaded {len(df)} concept stock records into Redis")
            
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

    async def update_hot_concepts(self, stock_code: str):
        """
        更新热点板块统计
        每只股票在同一概念中只统计一次，不管触发多少次活跃
        """
        try:
            today = datetime.now().strftime('%Y%m%d')
            
            # 获取股票的所有概念
            concepts = await self.get_stock_concepts(stock_code)
            if not concepts:
                return
            
            pipe = self.redis.pipeline()
            
            # 遍历股票的所有概念
            for concept_name in concepts.get('concept_name', '').split(','):
                if not concept_name:
                    continue
                    
                # 概念活跃股票集合的key
                concept_active_stocks_key = f"hot:concept:active:stocks:{concept_name}:{today}"
                
                # 将股票添加到概念的活跃股票集合中
                # SADD命令：如果股票已存在，不会重复添加
                pipe.sadd(concept_active_stocks_key, stock_code)
                
                # 设置过期时间
                tomorrow = (datetime.now().replace(hour=0, minute=0, second=0) + 
                          timedelta(days=1)).timestamp()
                pipe.expireat(concept_active_stocks_key, int(tomorrow))
                
                # 获取该概念下的活跃股票数量
                active_count = await self.redis.scard(concept_active_stocks_key)
                
                # 更新热点板块排行
                hot_concepts_key = f"hot:concept:stats:{today}"
                pipe.zadd(hot_concepts_key, {concept_name: active_count})
                pipe.expireat(hot_concepts_key, int(tomorrow))
            
            await pipe.execute()
            
        except Exception as e:
            logger.error(f"Error updating hot concepts for stock {stock_code}: {e}")

    async def get_hot_concepts(self, limit=10):
        """
        获取热点板块排行
        :param limit: 返回前N个热点板块
        :return: [(concept_name, active_stocks_count), ...]
        """
        try:
            today = datetime.now().strftime('%Y%m%d')
            hot_concepts_key = f"hot:concept:stats:{today}"
            
            # 获取得分最高的N个概念
            results = await self.redis.zrevrange(
                hot_concepts_key, 
                0, 
                limit-1, 
                withscores=True
            )
            
            # 转换格式并返回
            return [(concept, int(score)) for concept, score in results]
            
        except Exception as e:
            logger.error(f"Error getting hot concepts: {e}")
            return []

    async def get_concept_active_stocks(self, concept_name):
        """
        获取概念下的活跃股票列表
        """
        try:
            today = datetime.now().strftime('%Y%m%d')
            concept_active_stocks_key = f"hot:concept:active:stocks:{concept_name}:{today}"
            
            # 获取该概念下所有活跃股票
            active_stocks = await self.redis.smembers(concept_active_stocks_key)
            return list(active_stocks)
            
        except Exception as e:
            logger.error(f"Error getting active stocks for concept {concept_name}: {e}")
            return []

    async def push_to_wechat(self, stock_code: str, count: int, volume: int, price_change: float):
        """
        推送消息到企业微信机器人
        """
        try:
            # 获取股票的概念信息
            stock_concepts = await self.get_stock_concepts(stock_code)
            stock_name = stock_concepts.get('stock_name', '未知')
            concepts = stock_concepts.get('concept_name', '无')

            # 获取当前时间
            current_time = datetime.now().strftime('%H:%M:%S')
            
            # 构建消息内容
            message = (
                f"🔥 高活跃度股票提醒 \n"
                f"时间: {current_time}\n"
                f"股票: {stock_code} {stock_name}\n"
                f"涨跌幅: {price_change:.2f}%\n"
                f"活跃次数: {count}\n"
                f"当前成交量: {volume}\n"
                f"所属概念: {concepts}\n"
                f"------------------------\n"
            )

            # 构建请求数据
            data = {
                "msgtype": "text",
                "text": {
                    "content": message
                }
            }

            # 发送请求
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.webhook_url,
                    json=data,
                    headers={'Content-Type': 'application/json'}
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get('errcode') == 0:
                            logger.debug(f"Successfully pushed message for stock {stock_code}")
                        else:
                            logger.error(f"Failed to push message: {result}")
                    else:
                        logger.error(f"Failed to push message, status code: {response.status}")

        except Exception as e:
            logger.error(f"Error pushing message to WeChat: {e}")

    # 添加一个方法在每天开始时重置推送记录
    async def reset_push_records(self):
        """重置推送记录"""
        self.pushed_stocks.clear()
        logger.debug("Push records have been reset")

async def main():
    try:
        client = StockTrendsSSEClient()
        
        # 初始化Redis连接
        await client.init_redis()
        
        # 初始化概念股数据
        logger.debug("Starting to load concept stocks data...")
        await client.init_concept_stocks()
        logger.debug("Concept stocks data loaded successfully")
        
        # 读取股票代码列表
        with open('input.txt', 'r') as f:
            stock_codes = [line.strip() for line in f 
                         if line.strip() and not line.startswith('#')]
        
        if not stock_codes:
            logger.error("No stock codes found in input.txt")
            return
            
        logger.debug(f"Loaded {len(stock_codes)} stock codes from input.txt")
        
        # 启动行情监控任务
        tasks = [client.connect_with_retry(code) for code in stock_codes]
        await asyncio.gather(*tasks)
        
    except FileNotFoundError:
        logger.error("input.txt not found in current directory")
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 