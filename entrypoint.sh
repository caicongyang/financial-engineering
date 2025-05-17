#!/bin/bash

# 设置默认环境变量
export DB_USER=${DB_USER:-root}
export DB_PASSWORD=${DB_PASSWORD:-root}
export DB_HOST=${DB_HOST:-mysql}
export DB_PORT=${DB_PORT:-3306}
export DB_NAME=${DB_NAME:-stock}

# 创建.env文件
echo "# Generated .env file" > .env
echo "DB_USER=$DB_USER" >> .env
echo "DB_PASSWORD=$DB_PASSWORD" >> .env
echo "DB_HOST=$DB_HOST" >> .env
echo "DB_PORT=$DB_PORT" >> .env
echo "DB_NAME=$DB_NAME" >> .env

# 添加其他环境变量到.env
if [[ ! -z "$DEEPSEEK_API_KEY" ]]; then
    echo "DEEPSEEK_API_KEY=$DEEPSEEK_API_KEY" >> .env
fi

if [[ ! -z "$DEEPSEEK_API_BASE" ]]; then
    echo "DEEPSEEK_API_BASE=$DEEPSEEK_API_BASE" >> .env
fi

# 打印数据库连接信息进行调试
echo "Database connection info:"
echo "Host: $DB_HOST"
echo "Port: $DB_PORT"
echo "DB: $DB_NAME"
echo "User: $DB_USER"
echo "数据库连接信息: $DB_HOST:$DB_PORT/$DB_NAME, 用户: $DB_USER"

# 测试网络连接
echo "Testing network connection to $DB_HOST:$DB_PORT..."
ping -c 2 $DB_HOST || echo "Cannot ping $DB_HOST - network issue detected"

# 运行应用
exec python main.py 