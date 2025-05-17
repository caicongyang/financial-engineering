#!/bin/bash

# 定义变量
IMAGE_NAME="caicongyang/financial-engineering:latest"
CONTAINER_NAME="ai-backend"
FIX_DEPENDENCIES=false

# 解析命令行参数
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --fix-dependencies)
      FIX_DEPENDENCIES=true
      shift
      ;;
    *)
      # 其他参数忽略
      shift
      ;;
  esac
done

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

# 检查Docker网络
if ! docker network ls | grep financial-network >/dev/null; then
    echo "创建Docker网络 financial-network..."
    docker network create financial-network
fi

# 检查MySQL容器是否运行
MYSQL_RUNNING=$(docker ps -q -f name=mysql)
if [ -z "$MYSQL_RUNNING" ]; then
    echo "警告: MySQL容器未运行"
    echo "如果您的.env文件配置的是外部MySQL服务器，可以忽略此警告"
    echo "如果需要使用Docker容器中的MySQL，请先启动它"
    read -p "是否继续启动应用容器? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# 构建启动命令
RUN_CMD="docker run -d --name $CONTAINER_NAME"

# 添加网络参数
if [ "$MYSQL_RUNNING" != "" ]; then
    echo "使用financial-network网络..."
    RUN_CMD="$RUN_CMD --network financial-network"
else
    echo "使用host网络..."
    RUN_CMD="$RUN_CMD --network host"
fi

# 添加卷挂载和环境变量
RUN_CMD="$RUN_CMD \
    -v \"$(pwd)/.env:/app/.env\" \
    -v \"$(pwd)/.env:/app/program/python/com/caicongyang/financial/engineering/.env\" \
    -v \"$(pwd)/fix-dependencies.sh:/app/fix-dependencies.sh\" \
    -p 8000:8000"

# 如果需要修复依赖，则覆盖入口点
if [ "$FIX_DEPENDENCIES" = true ]; then
    echo "将使用修复依赖脚本启动容器..."
    RUN_CMD="$RUN_CMD --entrypoint /bin/bash $IMAGE_NAME -c \"chmod +x /app/fix-dependencies.sh && /app/fix-dependencies.sh\""
else
    RUN_CMD="$RUN_CMD $IMAGE_NAME"
fi

# 启动容器
echo "启动新容器..."
echo "运行命令: $RUN_CMD"
eval $RUN_CMD

# 检查容器是否成功启动
if [ $? -eq 0 ]; then
    echo "容器成功启动！"
    echo "可以通过以下命令查看日志："
    echo "docker logs -f $CONTAINER_NAME"
    
    # 立即显示日志
    echo "显示容器日志..."
    sleep 2
    docker logs -f $CONTAINER_NAME
else
    echo "容器启动失败，请检查错误信息。"
fi 