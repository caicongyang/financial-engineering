#!/bin/bash

# 构建镜像
docker build -t financial-engineering:latest .

# 打标签
docker tag financial-engineering:latest caicongyang/financial-engineering:latest

# 推送镜像
docker push caicongyang/financial-engineering:latest


