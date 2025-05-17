#!/bin/bash

# 设置默认环境变量
export DB_USER=${DB_USER:-root}
export DB_PASSWORD=${DB_PASSWORD:-root}
export DB_HOST=${DB_HOST:-mysql}
export DB_PORT=${DB_PORT:-3306}
export DB_NAME=${DB_NAME:-stock}

# 打印数据库连接信息进行调试
echo "Database connection info:"
echo "Host: $DB_HOST"
echo "Port: $DB_PORT"
echo "DB: $DB_NAME"
echo "User: $DB_USER"

# 运行应用
exec python main.py 