#!/bin/bash

echo "==== 环境变量设置 ===="

# 优先检查和加载外部.env文件
if [ -f "/app/.env" ]; then
    echo "发现外部.env文件：/app/.env"
    source /app/.env
    echo "已加载外部.env文件"
elif [ -f ".env" ]; then
    echo "发现当前目录.env文件"
    source .env
    echo "已加载当前目录.env文件"
else
    echo "未找到.env文件，将使用默认值或环境变量"
fi


echo "DB_USER=$DB_USER"
echo "DB_PASSWORD=${DB_PASSWORD:0:1}*****"
echo "DB_HOST=$DB_HOST"
echo "DB_PORT=$DB_PORT"
echo "DB_NAME=$DB_NAME"
echo "数据库连接信息: $DB_HOST:$DB_PORT/$DB_NAME, 用户: $DB_USER"

# 显示工作目录
echo "当前工作目录: $(pwd)"
echo "已有的.env文件位置:"
find /app -name ".env" -type f | head -n 5

echo "==== 启动应用 ===="
# 运行应用
exec python main.py 