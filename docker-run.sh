#!/bin/bash

# 定义变量
IMAGE_NAME="caicongyang/financial-engineering:latest"
CONTAINER_NAME="ai-backend"

# 检查.env文件是否存在
if [ ! -f ".env" ]; then
    echo "警告：当前目录未找到.env文件。将使用容器内默认配置。"
    echo "建议创建.env文件以正确配置数据库连接。"
    read -p "是否继续？(y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# 停止并移除已存在的容器
echo "停止并移除已存在的容器..."
docker stop $CONTAINER_NAME 2>/dev/null || true
docker rm $CONTAINER_NAME 2>/dev/null || true

# 启动容器，挂载.env文件
echo "启动新容器..."
docker run -d --name $CONTAINER_NAME \
    --network host \
    -v "$(pwd)/.env:/app/.env" \
    -v "$(pwd)/.env:/app/program/python/com/caicongyang/financial/engineering/.env" \
    -p 8000:8000 \
    $IMAGE_NAME

# 检查容器是否成功启动
if [ $? -eq 0 ]; then
    echo "容器成功启动！"
    echo "可以通过以下命令查看日志："
    echo "docker logs -f $CONTAINER_NAME"
else
    echo "容器启动失败，请检查错误信息。"
fi 