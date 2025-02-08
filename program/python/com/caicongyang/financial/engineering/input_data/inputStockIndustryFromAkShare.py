"""
把AkShare的数据导入到本地数据库
"""

from sqlalchemy import create_engine
import akshare as ak
import pandas as pd
import pymysql



# 数据库连接信息
mysql_user = 'root'
mysql_password = 'root'
mysql_host = '159.138.152.92'
mysql_port = '3333'
mysql_db = 'stock'
table_name = 't_industry'
table_name2 = 't_industry_stock'


# 数据库表字段和 DataFrame 列名的映射
column_mapping = {
    '板块名称': 'industry_name',
    '板块代码': 'industry_code',
    'source': 'source'
}

# 数据库表字段和 DataFrame 列名的映射
column_mapping2 = {
    '板块代码': 'industry_code',
    '板块名称': 'industry_name',
    '代码': 'stock_code',
    '名称': 'stock_name'
}

engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')


def df_to_mysql(df, table_name, column_mapping, mysql_user, mysql_password, mysql_host, mysql_port, mysql_db):
    # 根据映射关系重命名 DataFrame 列
    df = df.rename(columns=column_mapping)

    # 确保日期列的格式正确
    if 'trade_date' in df.columns:
        df['trade_date'] = pd.to_datetime(df['trade_date'])



    # 将 DataFrame 写入 MySQL
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=100)
        print(f"DataFrame has been successfully inserted into the {table_name} table.")
    except Exception as e:
        print(f"An error occurred: {e}")


stock_board_industry_name_em = ak.stock_board_industry_name_em()
print(stock_board_industry_name_em)

selected_columns_concept_name_em_df = stock_board_industry_name_em[['板块名称', '板块代码']]
selected_columns_concept_name_em_df['source'] = 'eastmoney'
# df_to_mysql(selected_columns_concept_name_em_df, table_name, column_mapping, mysql_user, mysql_password, mysql_host, mysql_port, mysql_db)

industry_list = selected_columns_concept_name_em_df['板块名称'].to_list()

# 获取板块名称和板块代码的字典映射
concept_code_mapping = dict(zip(selected_columns_concept_name_em_df['板块名称'], selected_columns_concept_name_em_df['板块代码']))

for x in industry_list:
        stock_board_industry_cons_em = ak.stock_board_industry_cons_em(symbol=x)
        df2 =stock_board_industry_cons_em[['代码', '名称']]
        # 获取当前板块的板块代码
        block_code = concept_code_mapping.get(x, None)
        # 将板块代码作为新列添加到 df2 中
        df2['板块代码'] = block_code
        df2['板块名称'] = x
        #插入到映射表中
        df_to_mysql(df2, table_name2, column_mapping2, mysql_user, mysql_password, mysql_host, mysql_port, mysql_db)




