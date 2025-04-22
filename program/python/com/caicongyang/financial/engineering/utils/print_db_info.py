#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
打印数据库连接信息以便检查配置是否正确
"""

import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import traceback

# 获取项目根目录
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../../.."))
# 添加 program/python 到 Python 路径
python_root = os.path.join(project_root, "program", "python")
sys.path.append(python_root)

# 加载环境变量
load_dotenv(os.path.join(project_root, '.env'))

def print_db_info():
    """打印数据库连接信息"""
    print("\n=== 数据库配置信息 ===")
    
    # 从环境变量获取数据库信息
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    
    print(f"用户名: {db_user}")
    print(f"密码: {'*' * len(db_password) if db_password else 'Not set'}")
    print(f"主机: {db_host}")
    print(f"端口: {db_port}")
    print(f"数据库名: {db_name}")
    
    # 尝试连接数据库
    try:
        connection_url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        print(f"\n连接URL: {connection_url}")
        
        engine = create_engine(connection_url)
        with engine.connect() as connection:
            # 测试连接
            result = connection.execute(text("SELECT 1"))
            rows = result.fetchall()
            if rows:
                print("\n✅ 数据库连接成功!")
            
            # 获取数据库版本信息
            result = connection.execute(text("SELECT VERSION()"))
            version = result.scalar()
            print(f"MySQL 版本: {version}")
            
            # 检查表列表
            result = connection.execute(text("SHOW TABLES"))
            tables = [row[0] for row in result.fetchall()]
            print(f"\n数据库表数量: {len(tables)}")
            print("表列表:")
            for table in tables:
                print(f"  - {table}")
                
    except Exception as e:
        print("\n❌ 数据库连接失败!")
        print(f"错误信息: {str(e)}")
        print("\n详细错误信息:")
        traceback.print_exc()
    
    print("\n=== 其他环境变量信息 ===")
    env_vars = [key for key in os.environ.keys() if key.startswith('DB_') or key.startswith('MYSQL_')]
    for var in env_vars:
        if var != 'DB_PASSWORD':
            print(f"{var}: {os.getenv(var)}")

if __name__ == "__main__":
    print_db_info() 