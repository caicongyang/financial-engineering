# 使用Python 3.9作为基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制requirements.txt（我们需要先创建这个文件）
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 创建项目目录结构
RUN mkdir -p /app/program/python/com/caicongyang/financial/engineering/stock_select_strategy

# 复制所有相关的Python文件
COPY program/python/com/caicongyang/financial/engineering/stock_select_strategy/*.py \
     /app/program/python/com/caicongyang/financial/engineering/stock_select_strategy/

# 设置工作目录
WORKDIR /app/program/python/com/caicongyang/financial/engineering/stock_select_strategy

# 设置环境变量
ENV PYTHONPATH=/app

# 启动脚本
CMD ["python", "daily_data_process.py"] 