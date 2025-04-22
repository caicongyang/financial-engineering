#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
检查MySQL连接状态并尝试连接特定服务器
"""

import os
import sys
import time
import socket
import pymysql
from sqlalchemy import create_engine, text
import traceback

# 获取项目根目录
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../../.."))
# 添加 program/python 到 Python 路径
python_root = os.path.join(project_root, "program", "python")
sys.path.append(python_root)

def check_socket_connection(host, port, timeout=5):
    """检查TCP连接是否可以建立"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    
    try:
        sock.connect((host, int(port)))
        print(f"✅ 可以连接到 {host}:{port}")
        result = True
    except Exception as e:
        print(f"❌ 无法连接到 {host}:{port}")
        print(f"  错误: {str(e)}")
        result = False
    finally:
        sock.close()
    
    return result

def check_mysql_connection(host, port, user, password, database, timeout=10):
    """尝试连接MySQL数据库"""
    print(f"\n正在尝试连接到MySQL数据库 {host}:{port}...")
    
    # 首先检查网络连接
    if not check_socket_connection(host, port):
        return False
    
    # 尝试使用pymysql直接连接
    try:
        print("\n使用pymysql尝试连接...")
        connection = pymysql.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database,
            connect_timeout=timeout
        )
        print("✅ pymysql连接成功")
        connection.close()
    except Exception as e:
        print(f"❌ pymysql连接失败")
        print(f"  错误: {str(e)}")
        print("\n详细错误信息:")
        traceback.print_exc()
        return False
    
    # 尝试使用SQLAlchemy连接
    try:
        print("\n使用SQLAlchemy尝试连接...")
        connection_url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
        engine = create_engine(connection_url, connect_args={'connect_timeout': timeout})
        
        with engine.connect() as connection:
            # 测试连接
            result = connection.execute(text("SELECT 1"))
            rows = result.fetchall()
            if rows:
                print("✅ SQLAlchemy连接成功")
                
                # 获取数据库版本信息
                result = connection.execute(text("SELECT VERSION()"))
                version = result.scalar()
                print(f"MySQL版本: {version}")
    except Exception as e:
        print(f"❌ SQLAlchemy连接失败")
        print(f"  错误: {str(e)}")
        print("\n详细错误信息:")
        traceback.print_exc()
        return False
    
    return True

def main():
    """主函数，检查MySQL连接"""
    # 从环境变量或配置文件读取信息
    # 先从web/db_utils.py中读取的配置
    server_info = {
        'main': {
            'host': '43.133.13.36',
            'port': '3333',
            'user': 'root',
            'password': 'root',
            'database': 'stock'
        }
    }
    
    # 检查连接
    for server_name, server_config in server_info.items():
        print(f"\n===== 检查服务器 {server_name} ({server_config['host']}:{server_config['port']}) =====")
        result = check_mysql_connection(
            server_config['host'],
            server_config['port'],
            server_config['user'],
            server_config['password'],
            server_config['database']
        )
        if result:
            print(f"\n✅ 服务器 {server_name} 连接成功!")
        else:
            print(f"\n❌ 服务器 {server_name} 连接失败!")
            
            # 尝试ping
            print(f"\n尝试ping {server_config['host']}...")
            os.system(f"ping -c 4 {server_config['host']}")

if __name__ == "__main__":
    main() 