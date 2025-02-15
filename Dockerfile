# 使用Python 3.9作为基础镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

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
    && pip install -r requirements.txt

# 创建项目目录结构
RUN mkdir -p /app/program/python/com/caicongyang/financial/engineering/input_data

# 复制所有相关的Python文件
COPY program/python/com/caicongyang/financial/engineering/input_data/*.py \
     /app/program/python/com/caicongyang/financial/engineering/input_data/

# 设置工作目录
WORKDIR /app/program/python/com/caicongyang/financial/engineering/input_data

# 设置环境变量
ENV PYTHONPATH=/app

# 启动脚本
CMD ["python", "daily_data_process.py"] 