#!/bin/bash

# 设置默认环境变量
export DB_USER=${DB_USER:-root}
export DB_PASSWORD=${DB_PASSWORD:-root}
export DB_HOST=${DB_HOST:-mysql}
export DB_PORT=${DB_PORT:-3306}
export DB_NAME=${DB_NAME:-stock}

echo "==== 环境变量设置 ===="
echo "DB_USER=$DB_USER"
echo "DB_PASSWORD=${DB_PASSWORD:0:1}*****"
echo "DB_HOST=$DB_HOST"
echo "DB_PORT=$DB_PORT"
echo "DB_NAME=$DB_NAME"
echo "数据库连接信息: $DB_HOST:$DB_PORT/$DB_NAME, 用户: $DB_USER"

# 创建.env文件在主要位置
echo "# Generated .env file" > .env
echo "DB_USER=$DB_USER" >> .env
echo "DB_PASSWORD=$DB_PASSWORD" >> .env
echo "DB_HOST=$DB_HOST" >> .env
echo "DB_PORT=$DB_PORT" >> .env
echo "DB_NAME=$DB_NAME" >> .env

# 添加API密钥
if [[ ! -z "$DEEPSEEK_API_KEY" ]]; then
    echo "DEEPSEEK_API_KEY=$DEEPSEEK_API_KEY" >> .env
fi

if [[ ! -z "$DEEPSEEK_API_BASE" ]]; then
    echo "DEEPSEEK_API_BASE=$DEEPSEEK_API_BASE" >> .env
fi

# 复制.env文件到主要位置 (包括main.py目录)
cp .env /app/.env
cp .env /app/program/python/com/caicongyang/financial/engineering/.env

# 显示工作目录和.env文件位置
echo "当前工作目录: $(pwd)"
echo ".env文件已复制到以下位置:"
find /app -name ".env" -type f | head -n 5

echo "==== 启动应用 ===="
# 运行应用
exec python main.py 