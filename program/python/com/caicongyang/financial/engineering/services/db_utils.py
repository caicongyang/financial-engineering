import os
from datetime import datetime
import pandas as pd
import json
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, DateTime, Text, UniqueConstraint

# 数据库连接信息，与check_stock_limit.py保持一致
mysql_user = 'root'
mysql_password = 'root'
mysql_host = '43.133.13.36'
mysql_port = '3333'
mysql_db = 'stock'

# 报告表名
REPORTS_TABLE = 't_analysis_reports'

# 创建数据库连接
engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

def init_db():
    """初始化数据库，创建必要的表"""
    metadata = MetaData()
    
    # 创建分析报告表
    analysis_reports = Table(
        REPORTS_TABLE, metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('report_id', String(64), nullable=False),
        Column('report_type', String(20), nullable=False),
        Column('code', String(20), nullable=True),
        Column('title', String(255), nullable=False),
        Column('content', Text, nullable=False),
        Column('created_at', DateTime, nullable=False),
        Column('related_data', Text, nullable=True),
        Column('tags', String(255), nullable=True),
        UniqueConstraint('report_id', name='uix_report_id')
    )
    
    # 创建表（如果不存在）
    metadata.create_all(engine)
    
    print(f"数据库初始化完成: MySQL {mysql_host}:{mysql_port}/{mysql_db}")

def check_table_exists():
    """检查分析报告表是否存在"""
    with engine.connect() as conn:
        query = text(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = '{mysql_db}'
        AND table_name = '{REPORTS_TABLE}'
        """)
        result = conn.execute(query)
        return result.scalar() > 0

def save_report_to_db(report_type, code, title, content, related_data=None, tags=None):
    """保存分析报告到数据库
    
    Args:
        report_type: 报告类型，例如 'market' 或 'stock'
        code: 股票代码或指数代码，市场分析可为 None
        title: 报告标题
        content: 报告内容 (Markdown格式)
        related_data: 相关数据JSON字符串或字典
        tags: 标签，用逗号分隔
        
    Returns:
        report_id: 报告ID
    """
    # 确保表存在
    if not check_table_exists():
        init_db()
    
    # 生成唯一报告ID
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d%H%M%S")
    
    if code:
        report_id = f"{report_type}_{code}_{timestamp}"
    else:
        report_id = f"{report_type}_{timestamp}"
    
    # 准备相关数据
    if related_data and isinstance(related_data, dict):
        related_data = json.dumps(related_data, ensure_ascii=False)
    
    # 插入数据
    insert_query = text(f"""
    INSERT INTO {REPORTS_TABLE} 
    (report_id, report_type, code, title, content, created_at, related_data, tags)
    VALUES (:report_id, :report_type, :code, :title, :content, :created_at, :related_data, :tags)
    """)
    
    with engine.connect() as conn:
        conn.execute(insert_query, {
            'report_id': report_id,
            'report_type': report_type,
            'code': code,
            'title': title,
            'content': content,
            'created_at': now,
            'related_data': related_data,
            'tags': tags
        })
        conn.commit()
    
    print(f"报告已保存到MySQL数据库，ID: {report_id}")
    return report_id

def get_report_by_id(report_id):
    """根据ID获取报告"""
    query = text(f"""
    SELECT * FROM {REPORTS_TABLE}
    WHERE report_id = :report_id
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {'report_id': report_id})
        row = result.fetchone()
    
    if row:
        # 转换为字典
        columns = result.keys()
        return dict(zip(columns, row))
    return None

def get_reports_by_type(report_type, limit=10):
    """获取指定类型的报告列表"""
    query = text(f"""
    SELECT id, report_id, code, title, created_at, tags 
    FROM {REPORTS_TABLE}
    WHERE report_type = :report_type
    ORDER BY created_at DESC
    LIMIT :limit
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {'report_type': report_type, 'limit': limit})
        rows = result.fetchall()
    
    # 转换为字典列表
    columns = result.keys()
    return [dict(zip(columns, row)) for row in rows]

def get_reports_by_code(code, limit=10):
    """获取指定股票代码的报告列表"""
    query = text(f"""
    SELECT id, report_id, report_type, title, created_at, tags 
    FROM {REPORTS_TABLE}
    WHERE code = :code
    ORDER BY created_at DESC
    LIMIT :limit
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {'code': code, 'limit': limit})
        rows = result.fetchall()
    
    # 转换为字典列表
    columns = result.keys()
    return [dict(zip(columns, row)) for row in rows]

def get_latest_reports_with_content(report_type, limit=1):
    """获取指定类型的最新报告，包含完整内容
    
    Args:
        report_type: 报告类型 ('market' 或 'stock')
        limit: 返回的报告数量
        
    Returns:
        reports: 报告列表，包含完整内容
    """
    query = text(f"""
    SELECT * FROM {REPORTS_TABLE}
    WHERE report_type = :report_type
    ORDER BY created_at DESC
    LIMIT :limit
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {'report_type': report_type, 'limit': limit})
        rows = result.fetchall()
    
    # 转换为字典列表
    columns = result.keys()
    return [dict(zip(columns, row)) for row in rows]

def search_reports_by_title(keyword, limit=20):
    """根据标题关键字搜索报告
    
    Args:
        keyword: 要搜索的关键字
        limit: 返回的最大结果数量
        
    Returns:
        reports: 匹配的报告列表
    """
    # 使用LIKE语句进行模糊匹配
    query = text(f"""
    SELECT * FROM {REPORTS_TABLE}
    WHERE title LIKE :keyword
    ORDER BY created_at DESC
    LIMIT :limit
    """)
    
    with engine.connect() as conn:
        # 添加通配符以进行模糊匹配
        search_pattern = f"%{keyword}%"
        result = conn.execute(query, {'keyword': search_pattern, 'limit': limit})
        rows = result.fetchall()
    
    # 转换为字典列表
    columns = result.keys()
    return [dict(zip(columns, row)) for row in rows]

if __name__ == "__main__":
    # 初始化数据库
    init_db()
    print("数据库表结构已创建")
    
    # 输出表结构说明
    print(f"""
分析报告表 ({REPORTS_TABLE}) 结构:
---------------------------------------
- id: 自增主键
- report_id: 报告唯一ID，格式为 {{类型}}_{{代码}}_{{时间戳}}
- report_type: 报告类型 ('market' 或 'stock')
- code: 股票代码或指数代码，市场分析可为NULL
- title: 报告标题
- content: 报告内容 (Markdown格式)
- created_at: 创建时间
- related_data: 相关数据JSON字符串，可存储分析时使用的原始数据摘要
- tags: 标签，用逗号分隔，便于分类查询

数据库连接信息:
- Host: {mysql_host}
- Port: {mysql_port}
- Database: {mysql_db}
- User: {mysql_user}
    """) 