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

# 设置环境变量（如果未在.env中定义，则使用默认值）
export DB_USER=${DB_USER:-root}
export DB_PASSWORD=${DB_PASSWORD:-root}
export DB_HOST=${DB_HOST:-mysql}
export DB_PORT=${DB_PORT:-3306}
export DB_NAME=${DB_NAME:-stock}

echo "DB_USER=$DB_USER"
echo "DB_PASSWORD=${DB_PASSWORD:0:1}*****"
echo "DB_HOST=$DB_HOST"
echo "DB_PORT=$DB_PORT"
echo "DB_NAME=$DB_NAME"
echo "数据库连接信息: $DB_HOST:$DB_PORT/$DB_NAME, 用户: $DB_USER"

# 创建一个内部.env文件以确保Python应用可以加载
cat > .env << EOF
# 自动生成的.env文件，基于加载的环境变量
DB_USER=$DB_USER
DB_PASSWORD=$DB_PASSWORD
DB_HOST=$DB_HOST
DB_PORT=$DB_PORT
DB_NAME=$DB_NAME
EOF

# 添加API密钥
if [[ ! -z "$DEEPSEEK_API_KEY" ]]; then
    echo "DEEPSEEK_API_KEY=$DEEPSEEK_API_KEY" >> .env
fi

if [[ ! -z "$DEEPSEEK_API_BASE" ]]; then
    echo "DEEPSEEK_API_BASE=$DEEPSEEK_API_BASE" >> .env
fi

# 复制.env文件到主要位置 (包括main.py目录)
cp .env /app/program/python/com/caicongyang/financial/engineering/.env

# 显示工作目录和.env文件位置
echo "当前工作目录: $(pwd)"
echo ".env文件已复制到以下位置:"
find /app -name ".env" -type f | head -n 5

echo "==== 启动应用 ===="
# 运行应用
exec python main.py 