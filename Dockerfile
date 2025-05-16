# 使用Python 3.9作为基础镜像
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
    && rm -rf /var/lib/apt/lists/*

# 复制requirements.txt
COPY requirements.txt .

# 配置pip使用清华镜像源并安装依赖
RUN pip config set global.index-url https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple \
    && pip config set install.trusted-host mirrors.tuna.tsinghua.edu.cn \
    && pip install --upgrade pip \
    && pip install setuptools wheel \
    && pip install -r requirements.txt --no-cache-dir

# 创建完整的项目目录结构
RUN mkdir -p /app/program/python/com/caicongyang/financial/engineering/{api,input_data,services,stock_select_strategy,timing_strategy,utils,input_data/cache}

# 复制整个项目结构
COPY program /app/program/
COPY setup.py /app/

# 安装项目包
RUN pip install -e .

# 设置工作目录 - 更新为main.py所在的目录
WORKDIR /app/program/python/com/caicongyang/financial/engineering

# 设置环境变量
ENV PYTHONPATH=/app

# 启动脚本 - 更新为使用main.py
CMD ["python", "main.py"] 