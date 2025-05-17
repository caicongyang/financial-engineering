# 使用Python 3.11作为基础镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 设置环境变量
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    wget \
    build-essential \
    iputils-ping \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 复制requirements.txt
COPY requirements.txt .

# 配置pip使用清华镜像源并安装所有依赖
RUN pip config set global.index-url https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple \
    && pip config set install.trusted-host mirrors.tuna.tsinghua.edu.cn \
    && pip install --upgrade pip \
    && pip install setuptools wheel \
    && pip install -r requirements.txt --no-cache-dir \
    && pip install fastapi uvicorn python-dotenv pandas sqlalchemy pymysql --no-cache-dir

# 创建完整的项目目录结构
RUN mkdir -p /app/program/python/com/caicongyang/financial/engineering/{api,input_data,services,stock_select_strategy,timing_strategy,utils,input_data/cache}

# 复制整个项目结构
COPY program /app/program/
COPY setup.py /app/
COPY entrypoint.sh /app/

# 复制.env文件到多个位置确保被找到
COPY .env /app/
COPY .env /app/program/python/com/caicongyang/financial/engineering/

# 设置entrypoint脚本权限
RUN chmod +x /app/entrypoint.sh

# 安装项目包
RUN pip install -e .

# 设置工作目录 - 更新为main.py所在的目录
WORKDIR /app/program/python/com/caicongyang/financial/engineering

# 设置环境变量
ENV PYTHONPATH=/app

# 部署说明
# =====================================================================
# 本镜像应当在与MySQL容器相同的Docker网络中运行，以便主机名解析工作正常
# 
# 推荐的部署方式:
# 1. 创建Docker网络: docker network create financial-network
# 
# 2. 启动MySQL容器:
#    docker run --name mysql \
#      --network financial-network \
#      -e MYSQL_ROOT_PASSWORD=root \
#      -e MYSQL_DATABASE=stock \
#      -d mysql:8.0
# 
# 3. 启动应用容器:
#    docker run --name ai-backend \
#      --network financial-network \
#      -p 8000:8000 \
#      -d <镜像名称>
#
# 注意: 所有配置将从镜像中的.env文件读取，无需通过-e参数传入环境变量
# =====================================================================

# 使用entrypoint脚本启动
ENTRYPOINT ["/app/entrypoint.sh"] 