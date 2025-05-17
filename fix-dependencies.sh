#!/bin/bash

# 修复依赖问题的脚本，可以在容器内运行

echo "==== 检查并修复依赖问题 ===="

# 检查uvicorn是否安装
if ! pip list | grep -i uvicorn > /dev/null; then
  echo "未检测到uvicorn，正在安装..."
  pip install uvicorn
else
  echo "uvicorn已安装"
fi

# 检查fastapi是否安装
if ! pip list | grep -i fastapi > /dev/null; then
  echo "未检测到fastapi，正在安装..."
  pip install fastapi
else
  echo "fastapi已安装"
fi

# 检查python-dotenv是否安装
if ! pip list | grep -i python-dotenv > /dev/null; then
  echo "未检测到python-dotenv，正在安装..."
  pip install python-dotenv
else
  echo "python-dotenv已安装"
fi

# 检查pymysql是否安装
if ! pip list | grep -i pymysql > /dev/null; then
  echo "未检测到pymysql，正在安装..."
  pip install pymysql
else
  echo "pymysql已安装"
fi

# 检查sqlalchemy是否安装
if ! pip list | grep -i sqlalchemy > /dev/null; then
  echo "未检测到sqlalchemy，正在安装..."
  pip install sqlalchemy
else
  echo "sqlalchemy已安装"
fi

echo "==== 重新安装主要依赖 ===="
pip install -r /app/requirements.txt

echo "==== 依赖检查完成 ===="
pip list

echo "==== 启动应用 ===="
cd /app/program/python/com/caicongyang/financial/engineering
python main.py 