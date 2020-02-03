#!/usr/bin/python
# -*- coding:utf-8 -*-

"""
Mysql工具箱
支持df写入mysql 和从mysql中读出一个df
"""

__author__ = 'caicongyang'

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class MySQLUtil:
    def __init__(self, host, port, username, password, schema):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db = schema
        self.write_mode = 'append'
        self.connect_url = 'mysql+pymysql://' + username + ':' + password + '@' + host + ':' + port + '/' + schema + '?charset=utf8mb4'
        self.engine = create_engine(self.connect_url, echo=True, pool_size=20)

    def write2mysql(self, table_name, df):
        """
        dataframe操作写入mysql
        :param table_name:
        :param df:
        :return:
        """
        df.to_sql(table_name, self.engine, schema=self.db, if_exists='append', index=False, index_label=False)
        print("write into mysql finish")

    def read_from_mysql(self, sql):
        """
        dateframe操作从mysql从读出来
        :param sql:
        :return:
        """
        return pd.read_sql(sql, self.engine)

    def close(self):
        self.engine.dispose()
        print("close mysql connect successful")

    def insert(self, insert_sql, args):
        """
        插入一条记录
        :param insert_sql: 'insert into T_Stock(stock_code,stock_name,trading_day) values(:stock_code,:stock_name,:trading_day)'
        :param args:  {'stock_code': '000001', 'stock_name': '万科A', "trading_day": '2019-01-01'}
        :return:
        """
        session = sessionmaker(bind=self.engine)
        session = session()
        session.execute(insert_sql, params=args)
        session.commit()
        session.close()

    def update(self, update_sql, args):
        """
        修改一条记录
        :param update_sql:
        :param args:
        :return:
        """
        session = sessionmaker(bind=self.engine)
        session = session()
        session.execute(update_sql, params=args)
        session.close()

    def query(self, query_sql):
        conn = self.engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(
            query_sql
        )
        query_result = cursor.fetchall()
        cursor.close()
        conn.close()
        return query_result

    # # for test
    # data1 = {'stock_code': ['600001', '600002', '600003', '600004', '600005'],
    #          'stock_name': ['广发证券', '兴业证券', '东兴证券', '中信证券', '山西证券'],
    #          'trading_day': ['2019-01-01', '2019-01-01', '2019-01-01', '2019-01-01', '2019-01-01'],
    #          'high': [10.11, 10.11, 10.11, 10.11, 10.11],
    #          'low': [10.11, 10.11, 10.11, 10.11, 10.11],
    #          'open': [10.11, 10.11, 10.11, 10.11, 10.11],
    #          'close': [10.11, 10.11, 10.11, 10.11, 10.11],
    #          'volume':[1111000095.10,451000095.10,1000095.10,31000095.10,21000095.10],
    #          'money':[1111000095.10,451000095.10,1000095.10,31000095.10,21000095.10]
    #          }
    #
    # index = pd.Index(data=data1['stock_code'], name="stock_code")
    #
    # df = pd.DataFrame(data1, index=index)
    #
    # # df = pd.DataFrame(data1)
    #
    # print(df)
    #

# x = MySQLUtil('127.0.0.1', '3306', 'root', 'root', 'stock')
# inser_sql = 'insert into T_Stock(stock_code,stock_name,trading_day,open,close,high,low,volume,money) values(:stock_code,:stock_name,:trading_day,:open,:close,:high,:low,:volume,:money)';
# json_str = {'open': 15.92, 'close': 15.54, 'high': 15.92, 'low': 15.39, 'volume': 110059207.0, 'money': 1723394336.66, 'stock_code': '000001.XSHE', 'trading_day': '2020-01-23','stock_name':''}
# # data2 = json.loads(json_str)

# x.insert(inser_sql, json_str);

# query = x.query('select * from t_stock')
# print(query)

# x.write2mysql('T_Stock', df)
#
# sql = 'select stock_name,stock_name,trading_day,high,low,open,close from T_Stock limit 10'
# mysql = x.read_from_mysql(sql)
# print(mysql)
# x.close()


# x.insert('123', '123')
#
# result = x.query("select * from t_stock where stock_code ='000001'")
# print(result)
# print(result[0])
# print(result)
#
# if len(result) > 0:
#     print("good")
# else:
#     print("sb")
